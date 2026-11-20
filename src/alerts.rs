//! Alerts: raised once per condition, cleared when the condition goes away, and
//! handed to the channels a rule names. The webhook gets each alert twice, when
//! it fires and when it clears: one event each, written in the same transaction
//! as the alert, signed, retried and recorded (see `docs/sources.md`).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Result;
use hmac::{KeyInit, Mac};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

use crate::config::AlertConfig;
use crate::db::{AlertRow, Db, NewWebhook, Rule, WebhookClaim, now_ms};

/// One attempt and up to five retries.
pub const MAX_ATTEMPTS: u32 = 6;

/// Rows taken per look at the store.
const DUE_BATCH: usize = 20;

/// How the sender paces itself. The defaults are production's; tests shorten
/// them.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct SenderTuning {
    /// How long a claimed attempt is this instance's before another may take
    /// it again. Longer than `timeout` with a wide margin.
    pub lease: Duration,
    pub connect_timeout: Duration,
    /// The whole request, connecting included.
    pub timeout: Duration,
    /// How often the store is looked at when nothing local wakes the sender:
    /// events written by another instance on the same store.
    pub idle_poll: Duration,
}

impl Default for SenderTuning {
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(3),
            timeout: Duration::from_secs(10),
            idle_poll: Duration::from_secs(5),
        }
    }
}

/// The webhook in force: where it posts and how it signs.
#[derive(Clone)]
struct Webhook {
    url: String,
    secret: Option<SecretString>,
    /// Which URL an event was written for, without keeping the URL.
    destination: String,
    backoff_ms: u64,
}

/// Records every alert and the webhook event of each change, and sends those
/// events from a task of its own (`start`).
pub struct Alerter {
    db: Arc<Db>,
    webhook: Option<Webhook>,
    /// Source names by id, for the events' `source.name`.
    names: RwLock<HashMap<String, String>>,
    wake: Arc<Notify>,
}

/// Owns the sender task: dropping it stops the task.
pub struct SenderGuard(tokio::task::JoinHandle<()>);

impl Drop for SenderGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Alerter {
    /// An alerter whose events are for `webhook_url` (unsigned), when there is
    /// one. They are sent only once `start` runs the sender and its guard is
    /// kept; until then they wait in the store. The URL is not validated here.
    pub fn new(db: Arc<Db>, webhook_url: Option<String>) -> Self {
        Self::from_config(
            db,
            &AlertConfig {
                webhook_url,
                ..AlertConfig::default()
            },
        )
    }

    /// An alerter with the `alerts:` settings (already validated).
    pub fn from_config(db: Arc<Db>, config: &AlertConfig) -> Self {
        let webhook = config.webhook_url.clone().map(|url| Webhook {
            destination: destination(&url),
            url,
            secret: config.webhook_secret.clone(),
            backoff_ms: config.backoff_ms(),
        });
        if webhook.is_none() && config.webhook_secret.is_some() {
            tracing::warn!("alerts.webhook_secret is set without a webhook_url; nothing is sent");
        }
        Self {
            db,
            webhook,
            names: RwLock::new(HashMap::new()),
            wake: Arc::new(Notify::new()),
        }
    }

    /// Whether a webhook URL is configured, and whether its events are signed.
    pub fn webhook_status(&self) -> (bool, bool) {
        match &self.webhook {
            Some(w) => (true, w.secret.is_some()),
            None => (false, false),
        }
    }

    /// The fingerprint of the configured URL, `""` without one: events written
    /// for another URL are parked.
    pub fn destination(&self) -> &str {
        self.webhook.as_ref().map_or("", |w| w.destination.as_str())
    }

    /// Remembers a source's name for the events about it.
    pub fn know_source(&self, id: &str, name: &str) {
        self.names
            .write()
            .expect("names lock")
            .insert(id.to_string(), name.to_string());
    }

    /// Starts sending this instance's pending events, when a URL is configured.
    /// The task stops when the guard is dropped.
    pub fn start(&self, tuning: SenderTuning) -> Option<SenderGuard> {
        let webhook = self.webhook.clone()?;
        let http = reqwest::Client::builder()
            .connect_timeout(tuning.connect_timeout)
            .timeout(tuning.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .user_agent(concat!("hie/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("HTTP client");
        let sender = Sender {
            db: self.db.clone(),
            webhook,
            http,
            tuning,
        };
        let wake = self.wake.clone();
        Some(SenderGuard(tokio::spawn(sender.run(wake))))
    }

    /// An alert that is not governed by a rule: stored, logged, posted to the webhook.
    pub fn raise(&self, feed: Option<&str>, kind: &str, message: &str) -> Result<()> {
        self.fire(feed, kind, message, None).map(drop)
    }

    /// Raises an alert for a rule unless one is already open for that rule and
    /// source. Returns whether a new alert was raised.
    pub fn fire_once(&self, feed: &str, kind: &str, rule: &Rule, message: &str) -> Result<bool> {
        if self.db.open_alert(feed, kind, Some(rule.id))?.is_some() {
            return Ok(false);
        }
        self.fire(Some(feed), kind, message, Some(rule))?;
        Ok(true)
    }

    /// Clears the open alert of a rule for a source, when there is one. Returns
    /// whether this call cleared it.
    pub fn clear_open(
        &self,
        feed: &str,
        kind: &str,
        rule_id: Option<i64>,
        reason: &str,
    ) -> Result<bool> {
        let Some((id, _)) = self.db.open_alert(feed, kind, rule_id)? else {
            return Ok(false);
        };
        let cleared = self.db.clear_alert(id, reason, now_ms(), |alert| {
            let wanted = alert
                .channels
                .as_ref()
                .is_none_or(|c| c.iter().any(|c| c == "webhook"));
            self.event("cleared", alert, wanted)
        })?;
        if cleared.is_none() {
            return Ok(false);
        }
        tracing::info!(alert = id, feed, kind, "cleared: {reason}");
        self.wake.notify_one();
        Ok(true)
    }

    fn fire(
        &self,
        feed: Option<&str>,
        kind: &str,
        message: &str,
        rule: Option<&Rule>,
    ) -> Result<i64> {
        let wanted = rule.is_none_or(|r| r.channels.iter().any(|c| c == "webhook"));
        let alert = self
            .db
            .insert_alert(feed, kind, message, rule, now_ms(), |alert| {
                self.event("fired", alert, wanted)
            })?;
        tracing::warn!(alert = alert.id, feed, kind, "{message}");
        if rule.is_some_and(|r| r.channels.iter().any(|c| c == "email")) {
            tracing::info!(
                alert = alert.id,
                "email channel requested; no mail sink is configured yet"
            );
        }
        self.wake.notify_one();
        Ok(alert.id)
    }

    /// The webhook event of an alert's change: queued for the configured URL,
    /// or recorded as not requested by its rule, or as having no URL to go to.
    fn event(&self, event: &'static str, alert: &AlertRow, wanted: bool) -> NewWebhook {
        let event_id = uuid_v4();
        let name = alert
            .feed_id
            .as_ref()
            .and_then(|id| self.names.read().expect("names lock").get(id).cloned());
        let payload = payload(&event_id, event, alert, name.as_deref());
        let (state, destination) = match (&self.webhook, wanted) {
            (_, false) => ("not_requested", String::new()),
            (None, true) => ("not_configured", String::new()),
            (Some(w), true) => ("pending", w.destination.clone()),
        };
        NewWebhook {
            event_id,
            event,
            destination,
            payload,
            state,
        }
    }
}

/// The body of a webhook event, built once and sent byte for byte on every
/// attempt. `text` is what Slack and Teams incoming webhooks display.
pub fn payload(event_id: &str, event: &str, alert: &AlertRow, source_name: Option<&str>) -> String {
    let text = match event {
        "cleared" => format!(
            "[HIE] cleared: {}",
            alert.cleared_reason.as_deref().unwrap_or_default()
        ),
        _ => format!("[HIE] {}", alert.message),
    };
    let source = alert
        .feed_id
        .as_ref()
        .map_or(Value::Null, |id| json!({"id": id, "name": source_name}));
    json!({
        "event_id": event_id,
        "event": event,
        "alert": {
            "id": alert.id,
            "rule_id": alert.rule_id,
            "kind": alert.kind,
            "message": alert.message,
            "notify": alert.notify,
            "created_at": alert.created_at,
            "cleared_at": alert.cleared_at,
            "cleared_reason": alert.cleared_reason,
        },
        "source": source,
        "text": text,
    })
    .to_string()
}

/// `sha256=` and the lowercase hex HMAC-SHA256 of `body` under `secret`: the
/// `X-HIE-Signature` header.
pub fn signature(secret: &[u8], body: &[u8]) -> String {
    let mut mac =
        hmac::Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts keys of any length");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

/// The first 16 hex digits of the SHA-256 of a URL.
fn destination(url: &str) -> String {
    hex::encode(&Sha256::digest(url.as_bytes())[..8])
}

/// A random (version 4) UUID.
fn uuid_v4() -> String {
    let mut b: [u8; 16] = rand::random();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h = hex::encode(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// The wait before retry `n` (1 for the first): `base`, doubling each time.
pub fn retry_delay_ms(base_ms: u64, n: u32) -> u64 {
    base_ms.saturating_mul(2u64.saturating_pow(n.saturating_sub(1)))
}

/// Sends the pending events of one destination, oldest first, one at a time.
struct Sender {
    db: Arc<Db>,
    webhook: Webhook,
    http: reqwest::Client,
    tuning: SenderTuning,
}

impl Sender {
    async fn run(self, wake: Arc<Notify>) {
        let idle = self.tuning.idle_poll.as_millis() as i64;
        loop {
            let next = match self.pass().await {
                Ok(next) => next,
                Err(e) => {
                    tracing::error!("alert webhook: {e:#}");
                    Some(now_ms() + 1000)
                }
            };
            let wait = next.map_or(idle, |at| (at - now_ms()).clamp(0, idle));
            tokio::select! {
                _ = wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(wait as u64)) => {}
            }
        }
    }

    /// Attempts every event due now; returns when the next one falls due.
    async fn pass(&self) -> Result<Option<i64>> {
        let destination = &self.webhook.destination;
        loop {
            let now = now_ms();
            let expired = self.db.webhook_expire_exhausted(now, MAX_ATTEMPTS)?;
            if expired > 0 {
                tracing::error!(
                    events = expired,
                    "alert webhook: gave up on events whose last attempt recorded no outcome"
                );
            }
            let due = self
                .db
                .webhook_due(destination, now, MAX_ATTEMPTS, DUE_BATCH)?;
            if due.is_empty() {
                break;
            }
            for event_id in due {
                let now = now_ms();
                let lease_until = now + self.tuning.lease.as_millis() as i64;
                if let Some(claim) =
                    self.db
                        .webhook_claim(&event_id, destination, now, lease_until, MAX_ATTEMPTS)?
                {
                    let outcome = self.send(&claim).await;
                    self.record(&claim, outcome).await?;
                }
            }
        }
        self.db.webhook_next_due(destination)
    }

    /// One attempt, and the receiver's HTTP status when it answered. The error
    /// is a fixed category: never the URL, a header or what the receiver answered.
    async fn send(&self, claim: &WebhookClaim) -> Attempt {
        let mut request = self
            .http
            .post(&self.webhook.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("X-HIE-Event-Id", &claim.event_id)
            .header("X-HIE-Event", &claim.event)
            .body(claim.payload.clone());
        if let Some(secret) = &self.webhook.secret {
            request = request.header(
                "X-HIE-Signature",
                signature(secret.expose_secret().as_bytes(), claim.payload.as_bytes()),
            );
        }
        match request.send().await {
            Ok(response) => {
                let status = response.status();
                Attempt {
                    status: Some(status.as_u16()),
                    error: (!status.is_success()).then(|| format!("HTTP {status}")),
                }
            }
            Err(e) => Attempt {
                status: None,
                error: Some(
                    if e.is_timeout() {
                        "timeout"
                    } else if e.is_connect() {
                        "connection failed"
                    } else {
                        "request failed"
                    }
                    .into(),
                ),
            },
        }
    }

    /// Stores what an attempt came to, trying the store a few times while the
    /// claim lasts. A claim that ran out meanwhile changes nothing.
    async fn record(&self, claim: &WebhookClaim, outcome: Attempt) -> Result<()> {
        let mut tries = 0;
        loop {
            let result = match (&outcome.error, outcome.status) {
                (None, Some(status)) => {
                    self.db
                        .webhook_delivered(&claim.event_id, claim.attempt, status, now_ms())
                }
                (error, status) => {
                    let error = error.as_deref().unwrap_or("request failed");
                    let retry_at = (claim.attempt < MAX_ATTEMPTS).then(|| {
                        now_ms() + retry_delay_ms(self.webhook.backoff_ms, claim.attempt) as i64
                    });
                    self.db.webhook_attempt_failed(
                        &claim.event_id,
                        claim.attempt,
                        error,
                        status,
                        retry_at,
                    )
                }
            };
            match result {
                Ok(recorded) => {
                    log_outcome(claim, &outcome, recorded);
                    return Ok(());
                }
                Err(e) if tries < 3 => {
                    tries += 1;
                    tracing::warn!(event_id = %claim.event_id, "alert webhook: recording the outcome: {e:#}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// What one attempt came to.
struct Attempt {
    /// The receiver's HTTP status, when it answered.
    status: Option<u16>,
    /// Why the attempt failed; `None` for a 2xx.
    error: Option<String>,
}

fn log_outcome(claim: &WebhookClaim, outcome: &Attempt, recorded: bool) {
    let (event_id, alert, attempt) = (&claim.event_id, claim.alert_id, claim.attempt);
    match &outcome.error {
        None => {
            tracing::info!(%event_id, alert, attempt, recorded, "alert webhook delivered")
        }
        Some(error) if attempt < MAX_ATTEMPTS => {
            tracing::warn!(%event_id, alert, attempt, recorded, "alert webhook attempt failed: {error}; will retry")
        }
        Some(error) => {
            tracing::error!(%event_id, alert, attempt, recorded, "alert webhook failed: {error}; giving up")
        }
    }
}

/// Checks a rule's shape: a known condition and a threshold that parses for it.
pub fn validate_rule(rule: &Rule) -> Result<(), String> {
    match rule.condition.as_str() {
        "silence" => crate::config::parse_duration(&rule.threshold)
            .map(drop)
            .map_err(|e| format!("silence needs a duration such as 2h: {e}")),
        "rejected_share" => rule
            .threshold
            .trim_end_matches('%')
            .parse::<f64>()
            .ok()
            .filter(|p| (0.0..=100.0).contains(p))
            .map(drop)
            .ok_or_else(|| "rejected_share needs a percentage from 0 to 100".to_string()),
        "missing_by" => parse_clock(&rule.threshold)
            .map(drop)
            .ok_or_else(|| "missing_by needs a time of day as HH:MM (UTC)".to_string()),
        "unreachable" => rule
            .threshold
            .parse::<u32>()
            .ok()
            .filter(|n| *n >= 1)
            .map(drop)
            .ok_or_else(|| "unreachable needs a number of attempts".to_string()),
        other => Err(format!(
            "unknown condition {other:?} (use silence, rejected_share, missing_by or unreachable)"
        )),
    }?;
    for channel in &rule.channels {
        if !matches!(channel.as_str(), "app" | "webhook" | "email") {
            return Err(format!(
                "unknown channel {channel:?} (use app, webhook or email)"
            ));
        }
    }
    Ok(())
}

/// "06:00" as minutes after midnight.
pub fn parse_clock(text: &str) -> Option<u32> {
    let (h, m) = text.trim().split_once(':')?;
    let (h, m): (u32, u32) = (h.parse().ok()?, m.parse().ok()?);
    (h < 24 && m < 60).then_some(h * 60 + m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(condition: &str, threshold: &str) -> Rule {
        Rule {
            id: 0,
            source_id: None,
            condition: condition.into(),
            threshold: threshold.into(),
            notify: vec![],
            channels: vec!["app".into()],
            enabled: true,
            created_by: None,
        }
    }

    #[test]
    fn rules_are_checked_by_condition() {
        assert!(validate_rule(&rule("silence", "2h")).is_ok());
        assert!(validate_rule(&rule("silence", "soon")).is_err());
        assert!(validate_rule(&rule("rejected_share", "1%")).is_ok());
        assert!(validate_rule(&rule("rejected_share", "140")).is_err());
        assert!(validate_rule(&rule("missing_by", "06:00")).is_ok());
        assert!(validate_rule(&rule("missing_by", "25:00")).is_err());
        assert!(validate_rule(&rule("unreachable", "5")).is_ok());
        assert!(validate_rule(&rule("sunspots", "1")).is_err());
        let mut r = rule("silence", "2h");
        r.channels = vec!["pager".into()];
        assert!(validate_rule(&r).is_err());
    }

    #[test]
    fn clock_times_parse() {
        assert_eq!(parse_clock("06:30"), Some(390));
        assert_eq!(parse_clock("6:5"), Some(365));
        assert_eq!(parse_clock("24:00"), None);
    }

    #[test]
    fn the_signature_is_hmac_sha256_in_hex() {
        // RFC 4231, test case 2.
        assert_eq!(
            signature(b"Jefe", b"what do ya want for nothing?"),
            "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn event_ids_are_random_v4_uuids() {
        let (a, b) = (uuid_v4(), uuid_v4());
        assert_ne!(a, b);
        let parts: Vec<&str> = a.split('-').collect();
        assert_eq!(
            parts.iter().map(|p| p.len()).collect::<Vec<_>>(),
            [8, 4, 4, 4, 12]
        );
        assert!(parts[2].starts_with('4'));
        assert!(matches!(
            parts[3].chars().next(),
            Some('8' | '9' | 'a' | 'b')
        ));
    }

    #[test]
    fn retries_wait_twice_as_long_each_time() {
        let waits: Vec<u64> = (1..MAX_ATTEMPTS)
            .map(|n| retry_delay_ms(10_000, n))
            .collect();
        assert_eq!(waits, [10_000, 20_000, 40_000, 80_000, 160_000]);
        assert_eq!(retry_delay_ms(u64::MAX / 2, 5), u64::MAX);
    }

    fn alert(cleared: bool) -> AlertRow {
        AlertRow {
            id: 7,
            feed_id: Some("lab-results".into()),
            kind: "rejected_share".into(),
            message: "too many rejected".into(),
            rule_id: Some(3),
            notify: vec!["ops@example.org".into()],
            channels: Some(vec!["app".into(), "webhook".into()]),
            created_at: "2026-11-20T10:00:00.000Z".into(),
            cleared_at: cleared.then(|| "2026-11-20T11:00:00.000Z".into()),
            cleared_reason: cleared.then(|| "within the limit".into()),
        }
    }

    #[test]
    fn payloads_have_the_documented_shape() {
        let fired: Value =
            serde_json::from_str(&payload("e1", "fired", &alert(false), Some("Lab results")))
                .unwrap();
        assert_eq!(
            fired,
            json!({
                "event_id": "e1",
                "event": "fired",
                "alert": {
                    "id": 7, "rule_id": 3, "kind": "rejected_share", "message": "too many rejected",
                    "notify": ["ops@example.org"], "created_at": "2026-11-20T10:00:00.000Z",
                    "cleared_at": null, "cleared_reason": null
                },
                "source": {"id": "lab-results", "name": "Lab results"},
                "text": "[HIE] too many rejected"
            })
        );
        let cleared: Value =
            serde_json::from_str(&payload("e2", "cleared", &alert(true), None)).unwrap();
        assert_eq!(cleared["event"], "cleared");
        assert_eq!(cleared["alert"]["cleared_reason"], "within the limit");
        assert_eq!(
            cleared["source"],
            json!({"id": "lab-results", "name": null})
        );
        assert_eq!(cleared["text"], "[HIE] cleared: within the limit");
        let mut sourceless = alert(false);
        sourceless.feed_id = None;
        sourceless.rule_id = None;
        sourceless.notify = vec![];
        let v: Value = serde_json::from_str(&payload("e3", "fired", &sourceless, None)).unwrap();
        assert_eq!(v["source"], Value::Null);
        assert_eq!(v["alert"]["notify"], json!([]));
        assert_eq!(v["alert"]["rule_id"], Value::Null);
    }

    #[test]
    fn destinations_name_a_url_without_holding_it() {
        let d = destination("https://hooks.example.org/T000/B000/secret");
        assert_eq!(d.len(), 16);
        assert!(!d.contains("secret"));
        assert_ne!(d, destination("https://hooks.example.org/other"));
    }
}
