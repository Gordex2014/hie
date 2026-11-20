//! Alerts: raised once per condition, cleared when the condition goes away, and
//! handed to the channels a rule names.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde_json::json;

use crate::db::{Db, Rule};

/// Records every alert and, when a rule (or the engine's default) asks for it,
/// posts it to the webhook. The payload's `text` field is what Slack and Teams
/// incoming webhooks display.
pub struct Alerter {
    db: Arc<Db>,
    webhook_url: Option<String>,
    http: reqwest::Client,
}

impl Alerter {
    pub fn new(db: Arc<Db>, webhook_url: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("HTTP client");
        Self {
            db,
            webhook_url,
            http,
        }
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

    /// Clears the open alert of a rule for a source, when there is one.
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

    /// Clears alert `id` if it is still open, and tells the webhook only when
    /// this call is the one that cleared it.
    pub fn clear(&self, id: i64, feed: &str, kind: &str, reason: &str) -> Result<bool> {
        if !self.db.clear_alert(id, reason)? {
            return Ok(false);
        }
        tracing::info!(alert = id, feed, kind, "cleared: {reason}");
        self.post(json!({"text": format!("[HIE] cleared: {reason}"), "feed": feed, "kind": kind, "alert_id": id, "cleared": true}));
        Ok(true)
    }

    fn fire(
        &self,
        feed: Option<&str>,
        kind: &str,
        message: &str,
        rule: Option<&Rule>,
    ) -> Result<i64> {
        let id = self.db.insert_alert(feed, kind, message, rule)?;
        tracing::warn!(alert = id, feed, kind, "{message}");
        let wants_webhook = rule.is_none_or(|r| r.channels.iter().any(|c| c == "webhook"));
        if wants_webhook {
            self.post(json!({
                "text": format!("[HIE] {message}"),
                "feed": feed, "kind": kind, "alert_id": id,
                "notify": rule.map(|r| r.notify.clone()).unwrap_or_default(),
            }));
        }
        if rule.is_some_and(|r| r.channels.iter().any(|c| c == "email")) {
            tracing::info!(
                alert = id,
                "email channel requested; no mail sink is configured yet"
            );
        }
        Ok(id)
    }

    fn post(&self, payload: serde_json::Value) {
        let Some(url) = self.webhook_url.clone() else {
            return;
        };
        let http = self.http.clone();
        tokio::spawn(async move {
            if let Err(e) = http
                .post(&url)
                .json(&payload)
                .send()
                .await
                .and_then(|r| r.error_for_status())
            {
                tracing::error!("alert webhook failed: {e}");
            }
        });
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
}
