//! Alerts raised for sources, and the rules that raise them.

use anyhow::Result;

use super::{Db, Param, now_ms, params};

/// Every alert raised, open until cleared.
pub(super) const ALERTS: &str = "CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY,
  feed_id TEXT,
  kind TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  rule_id INTEGER,
  cleared_at INTEGER,
  cleared_reason TEXT,
  notify TEXT,
  channels TEXT
);
CREATE INDEX IF NOT EXISTS alerts_feed_kind ON alerts(feed_id, kind, created_at);
";

/// The rules that say when to alert, and to whom.
pub(super) const RULES: &str = "CREATE TABLE IF NOT EXISTS alert_rules (
  id INTEGER PRIMARY KEY,
  source_id TEXT,
  condition TEXT NOT NULL,
  threshold TEXT NOT NULL,
  notify TEXT NOT NULL DEFAULT '[]',
  channels TEXT NOT NULL DEFAULT '[\"app\"]',
  enabled INTEGER NOT NULL DEFAULT 1,
  created_by TEXT,
  created_at INTEGER NOT NULL
);
";

/// Columns added to these tables after a store was first created, in the order they are added.
pub(super) const ADDED_COLUMNS: &[(&str, &str, &str)] = &[
    ("alerts", "rule_id", "INTEGER"),
    ("alerts", "cleared_at", "INTEGER"),
    ("alerts", "cleared_reason", "TEXT"),
    ("alerts", "notify", "TEXT"),
    ("alerts", "channels", "TEXT"),
];

/// A rule in words: "nothing arrives for 2 hours", "more than 1% of a load's rows
/// fail", "the file is not there by 06:00", "HFS cannot be reached after 5 attempts".
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Rule {
    #[serde(default)]
    pub id: i64,
    /// `None` applies to every source.
    #[serde(default)]
    pub source_id: Option<String>,
    pub condition: String,
    pub threshold: String,
    #[serde(default)]
    pub notify: Vec<String>,
    #[serde(default = "default_channels")]
    pub channels: Vec<String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub created_by: Option<String>,
}

fn default_channels() -> Vec<String> {
    vec!["app".into(), "webhook".into()]
}
fn default_enabled() -> bool {
    true
}

impl Db {
    pub fn last_alert_at(&self, feed: &str, kind: &str) -> Result<Option<i64>> {
        Ok(self
            .query_one(
                "SELECT MAX(created_at) FROM alerts WHERE feed_id = $1 AND kind = $2",
                &params![feed, kind],
            )?
            .and_then(|r| r.opt_i64(0)))
    }

    // ---- alerts ----------------------------------------------------------------

    pub fn insert_alert(
        &self,
        feed: Option<&str>,
        kind: &str,
        message: &str,
        rule: Option<&Rule>,
    ) -> Result<i64> {
        self.insert_returning_id(
            "INSERT INTO alerts (feed_id, kind, message, created_at, rule_id, notify, channels)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &params![
                feed,
                kind,
                message,
                now_ms(),
                rule.map(|r| r.id),
                rule.map(|r| serde_json::to_string(&r.notify).unwrap_or_default()),
                rule.map(|r| serde_json::to_string(&r.channels).unwrap_or_default()),
            ],
        )
    }

    /// The open alert (not cleared) of this kind for a source and rule, if any.
    pub fn open_alert(
        &self,
        feed: &str,
        kind: &str,
        rule_id: Option<i64>,
    ) -> Result<Option<(i64, i64)>> {
        Ok(self
            .query_one(
                "SELECT id, created_at FROM alerts
                 WHERE feed_id = $1 AND kind = $2 AND cleared_at IS NULL AND rule_id IS NOT DISTINCT FROM $3
                 ORDER BY id DESC LIMIT 1",
                &params![feed, kind, rule_id],
            )?
            .map(|r| (r.i64(0), r.i64(1))))
    }

    pub fn clear_alert(&self, id: i64, reason: &str) -> Result<()> {
        self.execute(
            "UPDATE alerts SET cleared_at = $2, cleared_reason = $3 WHERE id = $1 AND cleared_at IS NULL",
            &params![id, now_ms(), reason],
        )
        .map(drop)
    }

    // ---- alert rules -------------------------------------------------------

    const RULE_SELECT: &'static str = "SELECT id, source_id, condition, threshold, notify, channels, enabled, created_by FROM alert_rules";

    fn rule_rows(&self, sql: &str, p: &[Param]) -> Result<Vec<Rule>> {
        Ok(self
            .query(sql, p)?
            .iter()
            .map(|r| Rule {
                id: r.i64(0),
                source_id: r.opt_string(1),
                condition: r.string(2),
                threshold: r.string(3),
                notify: r
                    .json(4)
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default(),
                channels: r
                    .json(5)
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default(),
                enabled: r.i64(6) != 0,
                created_by: r.opt_string(7),
            })
            .collect())
    }

    pub fn rules(&self) -> Result<Vec<Rule>> {
        self.rule_rows(&format!("{} ORDER BY id", Self::RULE_SELECT), &[])
    }

    pub fn rule(&self, id: i64) -> Result<Option<Rule>> {
        Ok(self
            .rule_rows(
                &format!("{} WHERE id = $1", Self::RULE_SELECT),
                &params![id],
            )?
            .into_iter()
            .next())
    }

    /// The enabled rules that watch a source: its own and the ones for every source.
    pub fn rules_for(&self, feed: &str) -> Result<Vec<Rule>> {
        self.rule_rows(
            &format!(
                "{} WHERE enabled = 1 AND (source_id IS NULL OR source_id = $1) ORDER BY id",
                Self::RULE_SELECT
            ),
            &params![feed],
        )
    }

    pub fn has_rule(&self, feed: &str, condition: &str) -> Result<bool> {
        Ok(self
            .query_one(
                "SELECT 1 FROM alert_rules WHERE source_id = $1 AND condition = $2 LIMIT 1",
                &params![feed, condition],
            )?
            .is_some())
    }

    pub fn insert_rule(&self, rule: &Rule) -> Result<i64> {
        self.insert_returning_id(
            "INSERT INTO alert_rules (source_id, condition, threshold, notify, channels, enabled, created_by, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &params![
                rule.source_id,
                rule.condition,
                rule.threshold,
                serde_json::to_string(&rule.notify).unwrap_or_default(),
                serde_json::to_string(&rule.channels).unwrap_or_default(),
                rule.enabled,
                rule.created_by,
                now_ms()
            ],
        )
    }

    pub fn update_rule(&self, id: i64, rule: &Rule) -> Result<bool> {
        let n = self.execute(
            "UPDATE alert_rules SET source_id = $2, condition = $3, threshold = $4, notify = $5, channels = $6, enabled = $7
             WHERE id = $1",
            &params![
                id,
                rule.source_id,
                rule.condition,
                rule.threshold,
                serde_json::to_string(&rule.notify).unwrap_or_default(),
                serde_json::to_string(&rule.channels).unwrap_or_default(),
                rule.enabled
            ],
        )?;
        Ok(n > 0)
    }

    pub fn delete_rule(&self, id: i64) -> Result<bool> {
        Ok(self.execute("DELETE FROM alert_rules WHERE id = $1", &params![id])? > 0)
    }
}
