//! What a pass tells people: finished loads and their rejected share, delivery
//! outages and recoveries, silence and files missing by a time of day.

use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;

use chrono::Timelike;

use crate::alerts::parse_clock;
use crate::config::parse_duration;
use crate::db::{Rule, now_ms};

use super::{FeedRunner, human};

impl FeedRunner {
    pub(super) fn finish(&self) -> Result<()> {
        for f in self.db.finish_files(&self.feed.id)? {
            tracing::info!(target: "hie::pipeline", feed = %self.feed.id, file = %f.name, total = f.total, sent = f.sent, rejected = f.rejected, "file done");
            // The rules say whether a few rejected rows are worth an alert; without
            // one, every rejection is.
            let share_rules = self.rules("rejected_share")?;
            if f.newly_rejected > 0 && share_rules.is_empty() {
                let first = f
                    .first_rejection
                    .as_ref()
                    .map(|(line, err)| {
                        format!(
                            " First: line {line}: {}",
                            err.lines().next().unwrap_or_default()
                        )
                    })
                    .unwrap_or_default();
                self.alerts.raise(
                    Some(&self.feed.id),
                    "rejected_records",
                    &format!(
                        "{}: {} of {} records rejected in {}.{first}",
                        self.feed.name, f.rejected, f.total, f.name
                    ),
                )?;
            }
            // "More than N% of the rows in a load fail": fires on this load, clears on
            // the next load within the limit.
            let share = if f.total > 0 {
                f.rejected as f64 * 100.0 / f.total as f64
            } else {
                0.0
            };
            for rule in share_rules {
                let limit: f64 = rule.threshold.trim_end_matches('%').parse().unwrap_or(1.0);
                if share > limit {
                    self.alerts.fire_once(
                        &self.feed.id,
                        "rejected_share",
                        &rule,
                        &format!(
                            "{}: {share:.1}% of the rows in {} failed, more than the {limit}% allowed ({} of {})",
                            self.feed.name, f.name, f.rejected, f.total
                        ),
                    )?;
                } else {
                    self.alerts.clear_open(
                        &self.feed.id,
                        "rejected_share",
                        Some(rule.id),
                        &format!(
                            "{}: {} loaded with {share:.1}% failed, within the limit",
                            self.feed.name, f.name
                        ),
                    )?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn rules(&self, condition: &str) -> Result<Vec<Rule>> {
        Ok(self
            .db
            .rules_for(&self.feed.id)?
            .into_iter()
            .filter(|r| r.condition == condition)
            .collect())
    }

    /// Delivery failed `attempts` times in a row. The unreachable rules say when to
    /// tell someone; without one, the source's `alert_after_attempts` does.
    pub(super) fn outage(&self, attempts: u32, reason: &str) -> Result<()> {
        let message = format!(
            "{}: cannot deliver to HFS after {attempts} attempts ({reason}); data is kept and retried",
            self.feed.name
        );
        let rules = self.rules("unreachable")?;
        if rules.is_empty() {
            if attempts >= self.feed.delivery.alert_after_attempts
                && !self.outage_alerted.swap(true, Ordering::SeqCst)
            {
                self.alerts
                    .raise(Some(&self.feed.id), "delivery", &message)?;
            }
            return Ok(());
        }
        for rule in &rules {
            if attempts >= rule.threshold.parse().unwrap_or(5) {
                self.alerts
                    .fire_once(&self.feed.id, "delivery", rule, &message)?;
            }
        }
        Ok(())
    }

    pub(super) fn recovered(&self) -> Result<()> {
        let message = format!("{}: delivery to HFS recovered", self.feed.name);
        if self.outage_alerted.swap(false, Ordering::SeqCst) {
            self.alerts
                .raise(Some(&self.feed.id), "delivery_recovered", &message)?;
        }
        for rule in self.rules("unreachable")? {
            self.alerts
                .clear_open(&self.feed.id, "delivery", Some(rule.id), &message)?;
        }
        Ok(())
    }

    /// The rules that watch time rather than loads: silence, and a file expected by
    /// a time of day. Each fires once and clears itself when data arrives.
    pub(super) fn check_silence(&self) -> Result<()> {
        let last = self.db.last_received_at(&self.feed.id)?;
        let since = last.unwrap_or(self.started_at);
        let now = now_ms();
        for rule in self.db.rules_for(&self.feed.id)? {
            match rule.condition.as_str() {
                "silence" => {
                    let Ok(window) = parse_duration(&rule.threshold) else {
                        continue;
                    };
                    let silent_for = now - since;
                    if silent_for > window.as_millis() as i64 {
                        self.alerts.fire_once(
                            &self.feed.id,
                            "silence",
                            &rule,
                            &format!(
                                "{}: nothing received for {} (expected at least every {})",
                                self.feed.name,
                                human(Duration::from_millis(silent_for as u64)),
                                human(window)
                            ),
                        )?;
                    } else {
                        self.alerts.clear_open(
                            &self.feed.id,
                            "silence",
                            Some(rule.id),
                            &format!("{}: data arrived", self.feed.name),
                        )?;
                    }
                }
                "missing_by" => {
                    let Some(due_minute) = parse_clock(&rule.threshold) else {
                        continue;
                    };
                    let now_utc = chrono::Utc::now();
                    let today = now_utc
                        .date_naive()
                        .and_hms_opt(0, 0, 0)
                        .expect("midnight")
                        .and_utc()
                        .timestamp_millis();
                    let received_today = last.is_some_and(|t| t >= today);
                    let due = now_utc.hour() * 60 + now_utc.minute() >= due_minute;
                    if received_today {
                        self.alerts.clear_open(
                            &self.feed.id,
                            "missing",
                            Some(rule.id),
                            &format!("{}: the file arrived", self.feed.name),
                        )?;
                    } else if due {
                        self.alerts.fire_once(
                            &self.feed.id,
                            "missing",
                            &rule,
                            &format!(
                                "{}: nothing has arrived today and the file was expected by {} UTC",
                                self.feed.name, rule.threshold
                            ),
                        )?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}
