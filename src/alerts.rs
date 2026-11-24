//! Alerts: raised once per condition, cleared when the condition goes away, and
//! handed to the channels a rule names. The webhook gets each alert twice, when
//! it fires and when it clears: one event each, written in the same transaction
//! as the alert, signed, retried and recorded (see `docs/sources.md`).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use hmac::{KeyInit, Mac};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

use crate::config::AlertConfig;
use crate::db::{AlertRow, Db, NewWebhook, Rule, RuleGone, WebhookClaim, now_ms};
use crate::zone::Zone;

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
            // HTTP/1.1 only: the HTTP/2 layer logs what the peer sends at
            // `trace`, which a receiver may fill with the request's path.
            .http1_only()
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
    /// Nothing is raised when the rule was turned off, deleted or changed in
    /// what it watches since it was read.
    pub fn fire_once(&self, feed: &str, kind: &str, rule: &Rule, message: &str) -> Result<bool> {
        if self.db.open_alert(feed, kind, Some(rule.id))?.is_some() {
            return Ok(false);
        }
        Ok(self.fire(Some(feed), kind, message, Some(rule))?.is_some())
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
        self.clear(id, feed, kind, reason)
    }

    /// Clears alert `id` if it is still open, and queues the webhook's
    /// `cleared` event only when this call is the one that cleared it.
    pub fn clear(&self, id: i64, feed: &str, kind: &str, reason: &str) -> Result<bool> {
        let cleared = self
            .db
            .clear_alert_with_webhook(id, reason, now_ms(), |alert| self.cleared_event(alert))?;
        if cleared.is_none() {
            return Ok(false);
        }
        tracing::info!(alert = id, feed, kind, "cleared: {reason}");
        self.wake.notify_one();
        Ok(true)
    }

    /// Evaluates a `missing_by` rule for a source at `now`, in the rule's zone:
    /// fires once when its deadline has passed and nothing arrived that day,
    /// clears when a file arrives today.
    pub fn check_missing_by(
        &self,
        feed: &str,
        feed_name: &str,
        rule: &Rule,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let Some(due) = parse_clock(&rule.threshold) else {
            return Ok(());
        };
        let zone = match rule.tz.as_deref().map(Zone::parse) {
            None => Zone::utc(),
            Some(Ok(zone)) => zone,
            Some(Err(_)) => {
                // Checked when the rule was saved; only a later database that
                // dropped the name gets here, on every pass.
                tracing::debug!(
                    rule = rule.id,
                    tz = rule.tz.as_deref(),
                    "missing_by rule skipped: its time zone is not in this engine's database"
                );
                return Ok(());
            }
        };
        let check = missing_by(&zone, due, now);
        // Yesterday's deadline, pushed onto today's start, is checked first and
        // against its own day: a file of today must not hide that yesterday's
        // never came. Once per day: not when an alert of the rule was still open
        // at that deadline or raised since. An alert raised here is left for the
        // next pass to clear, so that it is never cleared before it is sent. A
        // rule made after that deadline does not judge the day before it.
        let made_after = |deadline: i64| {
            rule.created_at
                .as_deref()
                .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
                .is_some_and(|t| t.timestamp_millis() > deadline)
        };
        if let Some((day, from, to)) = check.pushed
            && !made_after(to)
            && !self.db.received_between(feed, from, to)?
            && !self
                .db
                .rule_alert_alive_since(feed, "missing", rule.id, to)?
            && self.fire_once(
                feed,
                "missing",
                rule,
                &format!(
                    "{feed_name}: nothing arrived on {day} and the file was expected by {} {}",
                    rule.threshold,
                    zone.name()
                ),
            )?
        {
            return Ok(());
        }
        if self
            .db
            .received_between(feed, check.today.1, check.today.2)?
        {
            self.clear_open(
                feed,
                "missing",
                Some(rule.id),
                &format!("{feed_name}: the file arrived"),
            )?;
        } else if check.due {
            self.fire_once(
                feed,
                "missing",
                rule,
                &format!(
                    "{feed_name}: nothing has arrived today and the file was expected by {} {}",
                    rule.threshold,
                    zone.name()
                ),
            )?;
        }
        Ok(())
    }

    /// Replaces a rule through the API. Turned off, or changed in its
    /// condition, threshold, time zone or source, the rule ends its open alerts in the
    /// same transaction, each with its `cleared` event for the channels
    /// recorded when it fired; if the changed rule still finds the condition,
    /// its next evaluation raises a new alert with its own id. A change of
    /// recipients or channels alone ends nothing. Returns whether the rule exists.
    pub fn update_rule(&self, id: i64, rule: &Rule) -> Result<bool> {
        let cleared = self.db.update_rule_ending_alerts(
            id,
            rule,
            now_ms(),
            |before| rule_update_ends_alerts(before, rule).map(RuleGone::reason),
            |alert| self.cleared_event(alert),
        )?;
        Ok(self.cleared_with_rule(cleared))
    }

    /// Deletes a rule through the API, ending its open alerts as `update_rule`
    /// does. Returns whether the rule existed.
    pub fn delete_rule(&self, id: i64) -> Result<bool> {
        let cleared = self.db.delete_rule_ending_alerts(
            id,
            RuleGone::Deleted.reason(),
            now_ms(),
            |alert| self.cleared_event(alert),
        )?;
        Ok(self.cleared_with_rule(cleared))
    }

    fn cleared_with_rule(&self, cleared: Option<Vec<AlertRow>>) -> bool {
        let Some(cleared) = cleared else {
            return false;
        };
        for alert in &cleared {
            tracing::info!(
                alert = alert.id,
                feed = alert.feed_id.as_deref().unwrap_or(""),
                kind = %alert.kind,
                "cleared: {}",
                alert.cleared_reason.as_deref().unwrap_or_default()
            );
        }
        if !cleared.is_empty() {
            self.wake.notify_one();
        }
        true
    }

    /// Clears the open alerts whose rule no longer watches them (deleted,
    /// turned off, or changed to another condition or source): left by an
    /// earlier version, or by a change made in the store rather than through
    /// the API. Each clear writes the alert's `cleared` event for the channels
    /// recorded when it fired. Returns how many this call cleared.
    pub fn clear_unwatched(&self) -> Result<usize> {
        let mut cleared = 0;
        for alert in self.db.unwatched_alerts()? {
            if self.clear(
                alert.id,
                alert.feed.as_deref().unwrap_or(""),
                &alert.kind,
                alert.why.reason(),
            )? {
                cleared += 1;
            }
        }
        Ok(cleared)
    }

    /// The `cleared` event of an alert, for the channels recorded when it fired.
    fn cleared_event(&self, alert: &AlertRow) -> NewWebhook {
        let wanted = alert
            .channels
            .as_ref()
            .is_none_or(|c| c.iter().any(|c| c == "webhook"));
        self.event("cleared", alert, wanted)
    }

    fn fire(
        &self,
        feed: Option<&str>,
        kind: &str,
        message: &str,
        rule: Option<&Rule>,
    ) -> Result<Option<i64>> {
        let wanted = rule.is_none_or(|r| r.channels.iter().any(|c| c == "webhook"));
        let Some(alert) =
            self.db
                .insert_alert_with_webhook(feed, kind, message, rule, now_ms(), |alert| {
                    self.event("fired", alert, wanted)
                })?
        else {
            return Ok(None);
        };
        tracing::warn!(alert = alert.id, feed, kind, "{message}");
        if rule.is_some_and(|r| r.channels.iter().any(|c| c == "email")) {
            tracing::info!(
                alert = alert.id,
                "email channel requested; no mail sink is configured yet"
            );
        }
        self.wake.notify_one();
        Ok(Some(alert.id))
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

/// Whether replacing rule `before` with `after` ends the alerts it raised:
/// turned off, or changed in its condition, threshold, time zone or source.
/// Recipients and channels are not part of what an alert describes.
pub fn rule_update_ends_alerts(before: &Rule, after: &Rule) -> Option<RuleGone> {
    if !after.enabled {
        Some(RuleGone::TurnedOff)
    } else if before.condition != after.condition
        || before.threshold != after.threshold
        || before.tz != after.tz
        || before.source_id != after.source_id
    {
        Some(RuleGone::Changed)
    } else {
        None
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

/// The `X-HIE-Signature` header of an attempt sent at `timestamp` (the
/// `X-HIE-Timestamp` header, Unix seconds): `sha256=` and the lowercase hex
/// HMAC-SHA256, under `secret`, of the timestamp, a `.` and the raw body.
pub fn signature(secret: &[u8], timestamp: &str, body: &[u8]) -> String {
    hmac_sha256_hex(secret, &[timestamp.as_bytes(), b".", body])
}

/// The lowercase hex HMAC-SHA256 of `parts`, one after the other, with `sha256=`.
fn hmac_sha256_hex(secret: &[u8], parts: &[&[u8]]) -> String {
    let mut mac =
        hmac::Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts keys of any length");
    for part in parts {
        mac.update(part);
    }
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
        // Each attempt is stamped, and signed, when it leaves: a retry carries
        // the same body with a fresh timestamp, so a receiver's tolerance never
        // has to cover the backoff.
        let timestamp = chrono::Utc::now().timestamp().to_string();
        if let Some(secret) = &self.webhook.secret {
            request = request.header(
                "X-HIE-Signature",
                signature(
                    secret.expose_secret().as_bytes(),
                    &timestamp,
                    claim.payload.as_bytes(),
                ),
            );
        }
        request = request.header("X-HIE-Timestamp", timestamp);
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
            .ok_or_else(|| "missing_by needs a time of day as HH:MM".to_string()),
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
    match (rule.condition.as_str(), rule.tz.as_deref()) {
        (_, None) => {}
        ("missing_by", Some(tz)) => {
            Zone::parse(tz).map_err(str::to_string)?;
        }
        (_, Some(_)) => return Err("tz applies only to missing_by rules".into()),
    }
    for channel in &rule.channels {
        if !matches!(channel.as_str(), "app" | "webhook" | "email") {
            return Err(format!(
                "unknown channel {channel:?} (use app, webhook or email)"
            ));
        }
    }
    Ok(())
}

/// What a `missing_by` rule sees at a moment, in its zone.
#[derive(Debug, PartialEq, Eq)]
pub struct MissingBy {
    /// Today and its `[start, end)` in epoch ms: a file received in it clears the alert.
    pub today: (NaiveDate, i64, i64),
    /// Today's deadline has passed: nothing received today fires the alert.
    pub due: bool,
    /// Yesterday and its bounds, when its deadline fell in a gap that ran into
    /// midnight and so resolved to today's start: nothing received yesterday
    /// fires the alert for it, whatever today brings.
    pub pushed: Option<(NaiveDate, i64, i64)>,
}

/// Where a deadline of `due_minute` (minutes after local midnight) stands at
/// `now` in `zone`. A deadline skipped by a change of offset is the change
/// itself; a repeated one is its first occurrence.
pub fn missing_by(zone: &Zone, due_minute: u32, now: DateTime<Utc>) -> MissingBy {
    let time = NaiveTime::from_hms_opt(due_minute / 60, due_minute % 60, 0).expect("a time of day");
    let (today, start, end) = zone.today_bounds(now);
    let pushed = today.pred_opt().and_then(|yesterday| {
        let deadline = zone.instant(yesterday, time);
        let from = zone.day_start(yesterday).timestamp_millis();
        // A date the zone skipped altogether has no instant to miss a file in.
        (deadline.timestamp_millis() >= start && now >= deadline && from < start)
            .then_some((yesterday, from, start))
    });
    MissingBy {
        today: (today, start, end),
        due: now >= zone.instant(today, time),
        pushed,
    }
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
            tz: None,
            created_at: None,
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
    fn a_change_of_zone_ends_the_rules_alerts() {
        let mut before = rule("missing_by", "06:00");
        before.tz = Some("Europe/Madrid".into());
        let mut after = before.clone();
        assert!(rule_update_ends_alerts(&before, &after).is_none());
        for tz in [Some("Europe/Paris"), None] {
            after.tz = tz.map(str::to_string);
            assert!(matches!(
                rule_update_ends_alerts(&before, &after),
                Some(RuleGone::Changed)
            ));
            assert!(matches!(
                rule_update_ends_alerts(&after, &before),
                Some(RuleGone::Changed)
            ));
        }
    }

    #[test]
    fn a_zone_is_only_for_missing_by_and_must_exist() {
        let mut r = rule("missing_by", "06:00");
        r.tz = Some("America/New_York".into());
        assert!(validate_rule(&r).is_ok());
        r.tz = Some("america/new_york".into());
        assert!(validate_rule(&r).is_ok());
        for bad in ["Mars/Base", "", "Etc/Unknown"] {
            r.tz = Some(bad.into());
            assert_eq!(validate_rule(&r).unwrap_err(), crate::zone::INVALID_ZONE);
        }
        let mut r = rule("silence", "2h");
        r.tz = Some("Europe/Madrid".into());
        assert_eq!(
            validate_rule(&r).unwrap_err(),
            "tz applies only to missing_by rules"
        );
        r.tz = None;
        assert!(validate_rule(&r).is_ok());
    }

    use crate::zone::{date, utc};

    fn ms(text: &str) -> i64 {
        utc(text).timestamp_millis()
    }

    fn zone(name: &str) -> Zone {
        Zone::parse(name).unwrap()
    }

    #[test]
    fn missing_by_in_utc() {
        let z = Zone::utc();
        let before = missing_by(&z, 6 * 60, utc("2026-11-20T05:59:59Z"));
        assert_eq!(
            before.today,
            (
                date("2026-11-20"),
                ms("2026-11-20T00:00:00Z"),
                ms("2026-11-21T00:00:00Z")
            )
        );
        assert!(!before.due);
        assert_eq!(before.pushed, None);
        let after = missing_by(&z, 6 * 60, utc("2026-11-20T06:00:00Z"));
        assert!(after.due);
        assert_eq!(after.today, before.today);
    }
    #[test]
    fn missing_by_in_a_zone_across_both_dst_changes() {
        let ny = zone("America/New_York");
        // 06:00 on a normal day: 11:00Z in winter.
        assert!(!missing_by(&ny, 6 * 60, utc("2026-01-15T10:59:59Z")).due);
        assert!(missing_by(&ny, 6 * 60, utc("2026-01-15T11:00:00Z")).due);
        // 02:30 does not exist on 2026-03-08: due at the change, 03:00 EDT (07:00Z).
        assert!(!missing_by(&ny, 150, utc("2026-03-08T06:59:59Z")).due);
        let spring = missing_by(&ny, 150, utc("2026-03-08T07:00:00Z"));
        assert!(spring.due);
        assert_eq!(
            spring.today,
            (
                date("2026-03-08"),
                ms("2026-03-08T05:00:00Z"),
                ms("2026-03-09T04:00:00Z")
            )
        );
        // 01:30 happens twice on 2026-11-01: due at the first (05:30Z), in a 25-hour day.
        assert!(!missing_by(&ny, 90, utc("2026-11-01T05:29:59Z")).due);
        let fall = missing_by(&ny, 90, utc("2026-11-01T05:30:00Z"));
        assert_eq!(
            fall.today,
            (
                date("2026-11-01"),
                ms("2026-11-01T04:00:00Z"),
                ms("2026-11-02T05:00:00Z")
            )
        );
        assert!(fall.due);
        assert_eq!(fall.pushed, None);
        // At 23:30 local the UTC date is already the next one: still the local day.
        let late = missing_by(&ny, 6 * 60, utc("2026-01-16T04:30:00Z"));
        assert_eq!(late.today.0, date("2026-01-15"));
    }
    #[test]
    fn a_deadline_pushed_to_the_next_midnight_is_due_as_that_day_begins() {
        // Nuuk, 2026-03-28: 23:00 to 24:00 does not exist, so 23:30 is the change,
        // which is also the start of 2026-03-29.
        let nuuk = zone("America/Nuuk");
        let deadline = nuuk.instant(
            date("2026-03-28"),
            chrono::NaiveTime::from_hms_opt(23, 30, 0).unwrap(),
        );
        assert_eq!(deadline, nuuk.day_start(date("2026-03-29")));
        let just_before = missing_by(
            &nuuk,
            23 * 60 + 30,
            deadline - chrono::Duration::milliseconds(1),
        );
        assert_eq!(just_before.today.0, date("2026-03-28"));
        assert!(!just_before.due);
        assert_eq!(just_before.pushed, None);
        let at = missing_by(&nuuk, 23 * 60 + 30, deadline);
        assert_eq!(at.today.0, date("2026-03-29"));
        assert!(!at.due);
        let pushed = Some((
            date("2026-03-28"),
            nuuk.day_start(date("2026-03-28")).timestamp_millis(),
            deadline.timestamp_millis(),
        ));
        assert_eq!(at.pushed, pushed);
        // A pass late on the 29th, after the 29th's own deadline, still sees the 28th's.
        let late = missing_by(&nuuk, 23 * 60 + 30, utc("2026-03-30T00:45:00Z"));
        assert_eq!(late.today.0, date("2026-03-29"));
        assert!(late.due);
        assert_eq!(late.pushed, pushed);
    }
    /// The store-backed check with a fixed clock: what fires and what clears.
    struct Lab {
        db: Arc<Db>,
        alerter: Alerter,
        rule: Rule,
    }

    impl Lab {
        fn new(threshold: &str, tz: Option<&str>) -> Lab {
            let db = Arc::new(Db::in_memory().unwrap());
            let mut rule = rule("missing_by", threshold);
            rule.source_id = Some("lab".into());
            rule.tz = tz.map(str::to_string);
            rule.id = db.insert_rule(&rule).unwrap();
            Lab {
                alerter: Alerter::new(db.clone(), None),
                db,
                rule,
            }
        }

        fn receive(&self, at: &str) {
            let id = self
                .db
                .insert_file(
                    "lab",
                    "lab",
                    at,
                    &format!("sha-{at}"),
                    1,
                    "done",
                    None,
                    None,
                    None,
                )
                .unwrap();
            self.db
                .execute_for_test(
                    "UPDATE files SET received_at = $2 WHERE id = $1",
                    &crate::db::params![id, ms(at)],
                )
                .unwrap();
        }

        /// How many alerts the rule raised, open or cleared.
        fn alerts(&self) -> i64 {
            self.db
                .query_json(
                    "SELECT COUNT(*) AS n FROM alerts WHERE rule_id = $1",
                    crate::db::params![self.rule.id],
                )
                .unwrap()[0]["n"]
                .as_i64()
                .unwrap()
        }

        /// Checks at `now`; the open alert's message, if any.
        fn check(&self, now: &str) -> Option<String> {
            self.alerter
                .check_missing_by("lab", "Lab", &self.rule, utc(now))
                .unwrap();
            let open = self
                .db
                .open_alert("lab", "missing", Some(self.rule.id))
                .unwrap()?;
            let rows = self
                .db
                .query_json(
                    "SELECT message FROM alerts WHERE id = $1",
                    crate::db::params![open.0],
                )
                .unwrap();
            rows[0]["message"].as_str().map(str::to_string)
        }
    }

    #[test]
    fn a_utc_rule_fires_after_its_deadline_and_clears_when_the_file_arrives() {
        let lab = Lab::new("06:00", None);
        lab.receive("2026-11-19T23:59:59Z");
        assert_eq!(lab.check("2026-11-20T05:59:00Z"), None);
        assert_eq!(
            lab.check("2026-11-20T06:00:00Z").as_deref(),
            Some("Lab: nothing has arrived today and the file was expected by 06:00 UTC")
        );
        lab.receive("2026-11-20T07:00:00Z");
        assert_eq!(lab.check("2026-11-20T07:01:00Z"), None);
        assert_eq!(
            lab.check("2026-11-20T08:00:00Z"),
            None,
            "no second alert that day"
        );
    }

    #[test]
    fn a_zoned_rule_counts_receipts_by_the_local_day() {
        // 06:00 in New York is 11:00Z in winter.
        let lab = Lab::new("06:00", Some("America/New_York"));
        // 02:00Z on the 20th is still the 19th in New York.
        lab.receive("2026-11-20T02:00:00Z");
        assert_eq!(lab.check("2026-11-20T10:59:00Z"), None, "not due yet");
        assert_eq!(
            lab.check("2026-11-20T11:00:00Z").as_deref(),
            Some(
                "Lab: nothing has arrived today and the file was expected by 06:00 America/New_York"
            )
        );
        // 05:00Z on the 20th is local midnight: today's file.
        let lab = Lab::new("06:00", Some("America/New_York"));
        lab.receive("2026-11-20T05:00:00Z");
        assert_eq!(lab.check("2026-11-20T11:00:00Z"), None);
        // The 02:00Z receipt that was the 19th in New York is the 20th in UTC.
        let lab = Lab::new("06:00", None);
        lab.receive("2026-11-20T02:00:00Z");
        assert_eq!(lab.check("2026-11-20T06:00:00Z"), None);
    }

    #[test]
    fn a_receipt_after_today_neither_hides_nor_fakes_today() {
        let lab = Lab::new("06:00", None);
        lab.receive("2026-11-21T00:00:00Z"); // tomorrow (another clock, a race)
        assert!(lab.check("2026-11-20T23:00:00Z").is_some());
        let lab = Lab::new("06:00", None);
        lab.receive("2026-11-20T03:00:00Z");
        lab.receive("2026-11-21T00:00:00Z");
        assert_eq!(lab.check("2026-11-20T23:00:00Z"), None);
    }

    #[test]
    fn a_deadline_pushed_to_the_next_midnight_still_fires() {
        let lab = Lab::new("23:30", Some("America/Nuuk"));
        let start = Zone::parse("America/Nuuk")
            .unwrap()
            .day_start(date("2026-03-29"));
        let at =
            |offset_minutes: i64| (start + chrono::Duration::minutes(offset_minutes)).to_rfc3339();
        lab.receive(&at(-24 * 60 - 60)); // the 27th
        assert_eq!(
            lab.check(&at(1)).as_deref(),
            Some(
                "Lab: nothing arrived on 2026-03-28 and the file was expected by 23:30 America/Nuuk"
            )
        );
        lab.receive(&at(5));
        assert_eq!(lab.check(&at(6)), None, "the file of the 29th clears it");
        // With a file on the 28th, nothing fires.
        let lab = Lab::new("23:30", Some("America/Nuuk"));
        lab.receive(&at(-60));
        assert_eq!(lab.check(&at(1)), None);
        assert_eq!(lab.alerts(), 0);
    }

    #[test]
    fn a_pushed_deadline_is_judged_by_the_rule_as_it_is_now() {
        let start = Zone::parse("America/Nuuk")
            .unwrap()
            .day_start(date("2026-03-29"));
        let at =
            |offset_minutes: i64| (start + chrono::Duration::minutes(offset_minutes)).to_rfc3339();
        // A rule made after the pushed deadline does not judge the day before.
        let mut lab = Lab::new("23:30", Some("America/Nuuk"));
        lab.rule.created_at = Some(at(1));
        assert_eq!(lab.check(&at(2)), None);
        assert_eq!(lab.alerts(), 0);
        // One made at the deadline does.
        lab.rule.created_at = Some(at(0));
        assert!(lab.check(&at(3)).is_some());

        // A UTC rule fired for the 28th; changed to Nuuk just after the 29th
        // began there, its alert ends, and the rule as changed raises its own
        // for the 28th: the cleared alert watched the rule as it was.
        let mut lab = Lab::new("23:30", None);
        assert!(lab.check("2026-03-28T23:30:00Z").is_some());
        let mut changed = lab.rule.clone();
        changed.tz = Some("America/Nuuk".into());
        assert!(lab.alerter.update_rule(lab.rule.id, &changed).unwrap());
        assert_eq!(lab.alerts(), 1);
        lab.rule = changed;
        assert_eq!(
            lab.check(&at(2)).as_deref(),
            Some(
                "Lab: nothing arrived on 2026-03-28 and the file was expected by 23:30 America/Nuuk"
            )
        );
        assert_eq!(lab.alerts(), 2);
        lab.check(&at(3));
        assert_eq!(lab.alerts(), 2, "once");
    }

    #[test]
    fn a_file_of_the_next_day_does_not_hide_a_pushed_deadline_missed() {
        let start = Zone::parse("America/Nuuk")
            .unwrap()
            .day_start(date("2026-03-29"));
        let at =
            |offset_minutes: i64| (start + chrono::Duration::minutes(offset_minutes)).to_rfc3339();
        let missed =
            "Lab: nothing arrived on 2026-03-28 and the file was expected by 23:30 America/Nuuk";
        // Nothing on the 28th, and the 29th's file already in at the first check:
        // the 28th's alert is raised, cleared on the next pass, never raised again.
        let lab = Lab::new("23:30", Some("America/Nuuk"));
        lab.receive(&at(1));
        assert_eq!(lab.check(&at(2)).as_deref(), Some(missed));
        assert_eq!(lab.check(&at(3)), None);
        assert_eq!(lab.check(&at(4)), None);
        assert_eq!(lab.alerts(), 1);
        // The same when the first pass of the 29th comes after its own deadline.
        let lab = Lab::new("23:30", Some("America/Nuuk"));
        lab.receive(&at(60));
        assert_eq!(lab.check(&at(24 * 60 - 15)).as_deref(), Some(missed));
        assert_eq!(lab.check(&at(24 * 60 - 14)), None);
        assert_eq!(lab.alerts(), 1);
        // An alert of the rule still open at the deadline (the 27th's) covers the
        // 28th too: cleared by the 29th's file, nothing more is raised.
        let lab = Lab::new("23:30", Some("America/Nuuk"));
        lab.alerter
            .fire_once("lab", "missing", &lab.rule, "the 27th")
            .unwrap();
        lab.receive(&at(1));
        assert_eq!(lab.check(&at(2)), None);
        assert_eq!(lab.check(&at(3)), None);
        assert_eq!(lab.alerts(), 1);
        // One cleared before the deadline does not.
        let lab = Lab::new("23:30", Some("America/Nuuk"));
        lab.alerter
            .fire_once("lab", "missing", &lab.rule, "the 27th")
            .unwrap();
        lab.db
            .execute_for_test(
                "UPDATE alerts SET created_at = $1, cleared_at = $2",
                &crate::db::params![
                    start.timestamp_millis() - 30 * 3_600_000,
                    start.timestamp_millis() - 20 * 3_600_000
                ],
            )
            .unwrap();
        lab.receive(&at(1));
        assert_eq!(lab.check(&at(2)).as_deref(), Some(missed));
        assert_eq!(lab.alerts(), 2);
    }
    #[test]
    fn a_skipped_date_misses_no_deadline() {
        // Apia skipped 2011-12-30: its 06:00 resolves to the start of the 31st.
        let apia = zone("Pacific/Apia");
        let start = apia.day_start(date("2011-12-31"));
        let check = missing_by(&apia, 6 * 60, start);
        assert_eq!(check.today.0, date("2011-12-31"));
        assert_eq!(check.pushed, None);
    }

    #[test]
    fn clock_times_parse() {
        assert_eq!(parse_clock("06:30"), Some(390));
        assert_eq!(parse_clock("6:5"), Some(365));
        assert_eq!(parse_clock("24:00"), None);
    }

    #[test]
    fn the_signature_is_hmac_sha256_in_hex() {
        // RFC 4231, test case 2, its data given in pieces.
        assert_eq!(
            hmac_sha256_hex(b"Jefe", &[b"what do ya ", b"want for nothing?"]),
            "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn the_signature_covers_the_timestamp_a_dot_and_the_body() {
        // Python: hmac.new(b"Jefe", b'1700000000.{"event":"fired"}', sha256).hexdigest()
        assert_eq!(
            signature(b"Jefe", "1700000000", br#"{"event":"fired"}"#),
            "sha256=0722f7348e182a55af4ded8a8a61167dcd847b9ddc53b77a32b666e371df4b4e"
        );
        assert_ne!(
            signature(b"Jefe", "1700000001", br#"{"event":"fired"}"#),
            signature(b"Jefe", "1700000000", br#"{"event":"fired"}"#)
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
