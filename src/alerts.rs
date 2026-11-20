//! Alerts: raised once per condition, cleared when the condition goes away, and
//! handed to the channels a rule names.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde_json::json;

use crate::db::{Db, Rule};
use crate::zone::Zone;

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
        self.db.clear_alert(id, reason)?;
        tracing::info!(alert = id, feed, kind, "cleared: {reason}");
        self.post(json!({"text": format!("[HIE] cleared: {reason}"), "feed": feed, "kind": kind, "alert_id": id, "cleared": true}));
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
        // at that deadline or raised since.
        if let Some((day, from, to)) = check.due.filter(|(day, ..)| *day != check.today.0)
            && !self.db.received_between(feed, from, to)?
            && !self
                .db
                .rule_alert_alive_since(feed, "missing", rule.id, to)?
        {
            self.fire_once(
                feed,
                "missing",
                rule,
                &format!(
                    "{feed_name}: nothing arrived on {day} and the file was expected by {} {}",
                    rule.threshold,
                    zone.name()
                ),
            )?;
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
        } else if check.due.is_some_and(|(day, ..)| day == check.today.0) {
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
    /// The day whose deadline has passed, with its bounds, if any: nothing
    /// received in it fires the alert. Usually today, after its deadline. It is
    /// yesterday in the first moments of a day when yesterday's deadline fell in a
    /// DST gap that ran into midnight, so that it resolved to today's start.
    pub due: Option<(NaiveDate, i64, i64)>,
}

/// Where a deadline of `due_minute` (minutes after local midnight) stands at
/// `now` in `zone`. A deadline skipped by a DST change is the change itself; a
/// repeated one is its first occurrence.
pub fn missing_by(zone: &Zone, due_minute: u32, now: DateTime<Utc>) -> MissingBy {
    let time = NaiveTime::from_hms_opt(due_minute / 60, due_minute % 60, 0).expect("a time of day");
    let (today, start, end) = zone.today_bounds(now);
    let deadline = zone.instant(today, time);
    let due = if now >= deadline {
        Some((today, start, end))
    } else {
        today.pred_opt().and_then(|yesterday| {
            let deadline = zone.instant(yesterday, time);
            let from = zone.day_start(yesterday).timestamp_millis();
            // A date the zone skipped altogether has no instant to miss a file in.
            (deadline.timestamp_millis() >= start && now >= deadline && from < start)
                .then_some((yesterday, from, start))
        })
    };
    MissingBy {
        today: (today, start, end),
        due,
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
        assert_eq!(before.due, None);
        let after = missing_by(&z, 6 * 60, utc("2026-11-20T06:00:00Z"));
        assert_eq!(after.due, Some(after.today));
    }

    #[test]
    fn missing_by_in_a_zone_across_both_dst_changes() {
        let ny = zone("America/New_York");
        // 06:00 on a normal day: 11:00Z in winter.
        assert_eq!(
            missing_by(&ny, 6 * 60, utc("2026-01-15T10:59:59Z")).due,
            None
        );
        assert!(
            missing_by(&ny, 6 * 60, utc("2026-01-15T11:00:00Z"))
                .due
                .is_some()
        );
        // 02:30 does not exist on 2026-03-08: due at the change, 03:00 EDT (07:00Z).
        assert_eq!(missing_by(&ny, 150, utc("2026-03-08T06:59:59Z")).due, None);
        let spring = missing_by(&ny, 150, utc("2026-03-08T07:00:00Z"));
        assert_eq!(
            spring.due,
            Some((
                date("2026-03-08"),
                ms("2026-03-08T05:00:00Z"),
                ms("2026-03-09T04:00:00Z")
            ))
        );
        // 01:30 happens twice on 2026-11-01: due at the first (05:30Z), in a 25-hour day.
        assert_eq!(missing_by(&ny, 90, utc("2026-11-01T05:29:59Z")).due, None);
        let fall = missing_by(&ny, 90, utc("2026-11-01T05:30:00Z"));
        assert_eq!(
            fall.today,
            (
                date("2026-11-01"),
                ms("2026-11-01T04:00:00Z"),
                ms("2026-11-02T05:00:00Z")
            )
        );
        assert!(fall.due.is_some());
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
        assert_eq!(just_before.due, None);
        let at = missing_by(&nuuk, 23 * 60 + 30, deadline);
        assert_eq!(at.today.0, date("2026-03-29"));
        assert_eq!(
            at.due,
            Some((
                date("2026-03-28"),
                nuuk.day_start(date("2026-03-28")).timestamp_millis(),
                deadline.timestamp_millis()
            ))
        );
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
                .insert_file("lab", at, &format!("sha-{at}"), 1, "done", None, None, None)
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
    fn a_file_of_the_next_day_does_not_hide_a_pushed_deadline_missed() {
        let start = Zone::parse("America/Nuuk")
            .unwrap()
            .day_start(date("2026-03-29"));
        let at =
            |offset_minutes: i64| (start + chrono::Duration::minutes(offset_minutes)).to_rfc3339();
        // Nothing on the 28th, and the 29th's file already in at the first check:
        // the 28th's alert is raised and cleared at once, then never raised again.
        let lab = Lab::new("23:30", Some("America/Nuuk"));
        lab.receive(&at(1));
        assert_eq!(lab.check(&at(2)), None);
        assert_eq!(lab.alerts(), 1);
        assert_eq!(lab.check(&at(3)), None);
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
    }

    #[test]
    fn a_skipped_date_misses_no_deadline() {
        // Apia skipped 2011-12-30: its 06:00 resolves to the start of the 31st.
        let apia = zone("Pacific/Apia");
        let start = apia.day_start(date("2011-12-31"));
        let check = missing_by(&apia, 6 * 60, start);
        assert_eq!(check.today.0, date("2011-12-31"));
        assert_eq!(check.due, None);
    }

    #[test]
    fn clock_times_parse() {
        assert_eq!(parse_clock("06:30"), Some(390));
        assert_eq!(parse_clock("6:5"), Some(365));
        assert_eq!(parse_clock("24:00"), None);
    }
}
