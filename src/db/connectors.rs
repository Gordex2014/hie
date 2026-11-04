//! The store's connection evidence from Tests, and what binds a source to
//! its evidence and credential revision (with the first load its Start
//! created). Evidence holds references (a secret id and revision), never a
//! value, and no source row. Evidence is kept 24 hours, and while any
//! source's binding names it (`evidence.source_id` is no longer used).

use anyhow::Result;
use serde_json::Value;

use super::{Db, Param, now_ms, params};
use crate::evidence::{Binding, EvidenceRow};

/// The DDL of this area's tables (`CREATE TABLE IF NOT EXISTS ...`).
pub(super) const TABLES: &str = "CREATE TABLE IF NOT EXISTS evidence (
  id TEXT PRIMARY KEY,
  verdict TEXT NOT NULL,
  body TEXT NOT NULL,
  definition TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  secret_id TEXT,
  secret_revision INTEGER,
  primary_key TEXT,
  tested_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  source_id TEXT
);
CREATE INDEX IF NOT EXISTS evidence_expires ON evidence(expires_at);
CREATE TABLE IF NOT EXISTS pending_evidence (
  id TEXT PRIMARY KEY,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS start_intents (
  feed_id TEXT PRIMARY KEY,
  evidence_id TEXT,
  secret_id TEXT,
  secret_revision INTEGER,
  first_load INTEGER,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS source_bindings (
  feed_id TEXT PRIMARY KEY,
  evidence_id TEXT,
  secret_id TEXT,
  secret_revision INTEGER,
  first_load INTEGER,
  updated_at INTEGER NOT NULL
);
";

/// Columns added to this area's tables after a store was first created.
/// `passes.owner` (the instance whose pass it is, so an abandoned pass can be
/// recognised) is added here: the passes area has no slot of its own.
pub(super) const ADDED_COLUMNS: &[(&str, &str, &str)] = &[("passes", "owner", "TEXT")];

fn opt_text(value: Option<&str>) -> Param {
    value.map_or(Param::Null, |v| Param::Text(v.into()))
}

fn opt_int(value: Option<i64>) -> Param {
    value.map_or(Param::Null, Param::Int)
}

impl Db {
    pub fn insert_evidence(&self, row: &EvidenceRow) -> Result<()> {
        self.execute(
            "INSERT INTO evidence (id, verdict, body, definition, fingerprint, secret_id, secret_revision, primary_key, tested_at, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            &[
                Param::Text(row.id.clone()),
                Param::Text(row.verdict.clone()),
                Param::Text(row.body.to_string()),
                Param::Text(row.definition.to_string()),
                Param::Text(row.fingerprint.clone()),
                opt_text(row.credential.as_ref().map(|c| c.0.as_str())),
                opt_int(row.credential.as_ref().map(|c| c.1 as i64)),
                row.primary_key
                    .as_ref()
                    .map_or(Param::Null, |k| Param::Text(Value::from(k.clone()).to_string())),
                Param::Int(row.tested_at),
                Param::Int(row.expires_at),
            ],
        )
        .map(drop)
    }

    /// Evidence still kept: not expired, or named by a source's binding.
    pub fn evidence(&self, id: &str) -> Result<Option<EvidenceRow>> {
        Ok(self
            .query_one(
                "SELECT id, verdict, body, definition, fingerprint, secret_id, secret_revision, primary_key, tested_at, expires_at
                 FROM evidence WHERE id = $1
                   AND (expires_at >= $2 OR EXISTS (SELECT 1 FROM source_bindings b WHERE b.evidence_id = evidence.id)
                        OR EXISTS (SELECT 1 FROM start_intents i WHERE i.evidence_id = evidence.id))",
                &params![id, now_ms()],
            )?
            .map(|r| EvidenceRow {
                id: r.string(0),
                verdict: r.string(1),
                body: r.json(2).unwrap_or(Value::Null),
                definition: r.json(3).unwrap_or(Value::Null),
                fingerprint: r.string(4),
                credential: r
                    .opt_string(5)
                    .zip(r.opt_i64(6))
                    .map(|(s, n)| (s, n as u32)),
                primary_key: r.json(7).and_then(|v| serde_json::from_value(v).ok()),
                tested_at: r.i64(8),
                expires_at: r.i64(9),
            }))
    }

    /// Evidence past its time that no source's binding names: deleted; their ids.
    pub fn expire_evidence(&self) -> Result<Vec<String>> {
        self.transaction(|tx| {
            const UNHELD: &str = "expires_at < $1 AND NOT EXISTS (SELECT 1 FROM source_bindings b WHERE b.evidence_id = evidence.id) AND NOT EXISTS (SELECT 1 FROM start_intents i WHERE i.evidence_id = evidence.id)";
            let ids: Vec<String> = tx
                .query_json(&format!("SELECT id FROM evidence WHERE {UNHELD}"), &params![now_ms()])?
                .iter()
                .filter_map(|r| r["id"].as_str().map(str::to_string))
                .collect();
            for id in &ids {
                tx.execute(
                    &format!("DELETE FROM evidence WHERE id = $2 AND {UNHELD}"),
                    &params![now_ms(), id],
                )?;
            }
            Ok(ids)
        })
    }

    /// Records (or replaces) what a source is bound to; a first load already
    /// recorded is kept when `first_load` is `None`.
    pub fn set_binding(&self, binding: &Binding) -> Result<()> {
        self.execute(
            "INSERT INTO source_bindings (feed_id, evidence_id, secret_id, secret_revision, first_load, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (feed_id) DO UPDATE SET evidence_id = excluded.evidence_id, secret_id = excluded.secret_id,
               secret_revision = excluded.secret_revision,
               first_load = COALESCE(excluded.first_load, source_bindings.first_load), updated_at = excluded.updated_at",
            &[
                Param::Text(binding.source.clone()),
                opt_text(binding.evidence.as_deref()),
                opt_text(binding.credential.as_ref().map(|c| c.0.as_str())),
                opt_int(binding.credential.as_ref().map(|c| c.1 as i64)),
                opt_int(binding.first_load),
                Param::Int(now_ms()),
            ],
        )
        .map(drop)
    }

    pub fn binding(&self, feed: &str) -> Result<Option<Binding>> {
        Ok(self
            .query_one(
                "SELECT feed_id, evidence_id, secret_id, secret_revision, first_load FROM source_bindings WHERE feed_id = $1",
                &params![feed],
            )?
            .map(|r| Binding {
                source: r.string(0),
                evidence: r.opt_string(1),
                credential: r
                    .opt_string(2)
                    .zip(r.opt_i64(3))
                    .map(|(s, n)| (s, n as u32)),
                first_load: r.opt_i64(4),
            }))
    }

    pub fn delete_binding(&self, feed: &str) -> Result<()> {
        self.execute(
            "DELETE FROM source_bindings WHERE feed_id = $1",
            &params![feed],
        )
        .map(drop)
    }

    /// Records that a Test is under way for evidence `id`, before it holds
    /// a secret revision under that id.
    pub fn insert_pending_evidence(&self, id: &str) -> Result<()> {
        self.execute(
            "INSERT INTO pending_evidence (id, created_at) VALUES ($1, $2)",
            &params![id, now_ms()],
        )
        .map(drop)
    }

    pub fn delete_pending_evidence(&self, id: &str) -> Result<()> {
        self.execute("DELETE FROM pending_evidence WHERE id = $1", &params![id])
            .map(drop)
    }

    /// Tests begun before `before` (epoch ms) that never finished: their ids,
    /// apart from those whose evidence was kept after all.
    pub fn abandoned_pending_evidence(&self, before: i64) -> Result<Vec<String>> {
        let ids: Vec<String> = self
            .query(
                "SELECT id FROM pending_evidence WHERE created_at < $1",
                &params![before],
            )?
            .iter()
            .map(|r| r.string(0))
            .collect();
        let mut abandoned = Vec::new();
        for id in ids {
            let kept = self
                .query_one("SELECT 1 FROM evidence WHERE id = $1", &params![id])?
                .is_some();
            if kept {
                self.delete_pending_evidence(&id)?;
            } else {
                abandoned.push(id);
            }
        }
        Ok(abandoned)
    }

    /// Records that a Start of `intent.source` is under way, before its file
    /// is written; `false` when one already is.
    pub fn insert_start_intent(&self, intent: &Binding) -> Result<bool> {
        let n = self.execute(
            "INSERT INTO start_intents (feed_id, evidence_id, secret_id, secret_revision, first_load, created_at)
             VALUES ($1, $2, $3, $4, NULL, $5) ON CONFLICT (feed_id) DO NOTHING",
            &[
                Param::Text(intent.source.clone()),
                opt_text(intent.evidence.as_deref()),
                opt_text(intent.credential.as_ref().map(|c| c.0.as_str())),
                opt_int(intent.credential.as_ref().map(|c| c.1 as i64)),
                Param::Int(now_ms()),
            ],
        )?;
        Ok(n > 0)
    }

    /// Records the first load a Start under way created.
    pub fn set_start_intent_load(&self, feed: &str, load: i64) -> Result<()> {
        self.execute(
            "UPDATE start_intents SET first_load = $2 WHERE feed_id = $1",
            &params![feed, load],
        )
        .map(drop)
    }

    /// Starts under way (or left by an engine that stopped).
    pub fn start_intents(&self, feed: Option<&str>) -> Result<Vec<Binding>> {
        let rows = match feed {
            Some(feed) => self.query(
                "SELECT feed_id, evidence_id, secret_id, secret_revision, first_load FROM start_intents WHERE feed_id = $1",
                &params![feed],
            )?,
            None => self.query(
                "SELECT feed_id, evidence_id, secret_id, secret_revision, first_load FROM start_intents ORDER BY feed_id",
                &[],
            )?,
        };
        Ok(rows
            .iter()
            .map(|r| Binding {
                source: r.string(0),
                evidence: r.opt_string(1),
                credential: r
                    .opt_string(2)
                    .zip(r.opt_i64(3))
                    .map(|(s, n)| (s, n as u32)),
                first_load: r.opt_i64(4),
            })
            .collect())
    }

    pub fn delete_start_intent(&self, feed: &str) -> Result<()> {
        self.execute(
            "DELETE FROM start_intents WHERE feed_id = $1",
            &params![feed],
        )
        .map(drop)
    }

    /// The oldest load of `feed` still queued.
    pub fn queued_load(&self, feed: &str) -> Result<Option<i64>> {
        Ok(self
            .query_one(
                "SELECT id FROM files WHERE feed_id = $1 AND status = 'queued' ORDER BY id LIMIT 1",
                &params![feed],
            )?
            .map(|r| r.i64(0)))
    }

    /// A load's status, for a source's `first_load`.
    pub fn load_status(&self, load: i64) -> Result<Option<String>> {
        Ok(self
            .query_one("SELECT status FROM files WHERE id = $1", &params![load])?
            .map(|r| r.string(0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn evidence(id: &str, expires_at: i64) -> EvidenceRow {
        EvidenceRow {
            id: id.into(),
            verdict: "passed".into(),
            body: json!({}),
            definition: json!({}),
            fingerprint: "f".into(),
            credential: None,
            primary_key: None,
            tested_at: 0,
            expires_at,
        }
    }

    fn bind(db: &Db, source: &str, evidence: &str) {
        db.set_binding(&Binding {
            source: source.into(),
            evidence: Some(evidence.into()),
            credential: None,
            first_load: None,
        })
        .unwrap();
    }

    #[test]
    fn evidence_is_kept_while_any_source_names_it() {
        let db = Db::in_memory().unwrap();
        // Past its 24 hours, held only by bindings.
        db.insert_evidence(&evidence("ev_a", now_ms() - 1)).unwrap();
        bind(&db, "a", "ev_a");
        bind(&db, "b", "ev_a");
        // B leaves: A still names it.
        db.delete_binding("b").unwrap();
        assert!(db.expire_evidence().unwrap().is_empty());
        assert!(db.evidence("ev_a").unwrap().is_some());
        // A moves to other evidence: nothing names it any more.
        bind(&db, "a", "ev_b");
        assert!(db.evidence("ev_a").unwrap().is_none());
        assert_eq!(db.expire_evidence().unwrap(), ["ev_a"]);
    }
}
