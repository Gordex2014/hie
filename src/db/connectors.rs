//! The store's connection evidence from Tests, and what binds a source to
//! its evidence and credential revision (with the first load its Start
//! created). Evidence holds references (a secret id and revision), never a
//! value, and no source row.

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
            "INSERT INTO evidence (id, verdict, body, definition, fingerprint, secret_id, secret_revision, primary_key, tested_at, expires_at, source_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL)",
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

    /// Evidence still kept: not expired, or bound to a source.
    pub fn evidence(&self, id: &str) -> Result<Option<EvidenceRow>> {
        Ok(self
            .query_one(
                "SELECT id, verdict, body, definition, fingerprint, secret_id, secret_revision, primary_key, tested_at, expires_at, source_id
                 FROM evidence WHERE id = $1 AND (expires_at >= $2 OR source_id IS NOT NULL)",
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
                source_id: r.opt_string(10),
            }))
    }

    /// Evidence past its time that no source holds: deleted; their ids.
    pub fn expire_evidence(&self) -> Result<Vec<String>> {
        let ids: Vec<String> = self
            .query(
                "SELECT id FROM evidence WHERE expires_at < $1 AND source_id IS NULL",
                &params![now_ms()],
            )?
            .iter()
            .map(|r| r.string(0))
            .collect();
        for id in &ids {
            self.execute(
                "DELETE FROM evidence WHERE id = $1 AND source_id IS NULL",
                &params![id],
            )?;
        }
        Ok(ids)
    }

    /// The source `feed` holds evidence `id` (and only it).
    pub fn hold_evidence(&self, feed: &str, id: &str) -> Result<()> {
        self.transaction(|tx| {
            tx.execute(
                "UPDATE evidence SET source_id = NULL WHERE source_id = $1",
                &params![feed],
            )?;
            tx.execute(
                "UPDATE evidence SET source_id = $2 WHERE id = $1",
                &params![id, feed],
            )?;
            Ok(())
        })
    }

    /// The source holds no evidence any more; it expires as any other.
    pub fn release_evidence(&self, feed: &str) -> Result<()> {
        self.execute(
            "UPDATE evidence SET source_id = NULL WHERE source_id = $1",
            &params![feed],
        )
        .map(drop)
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

    /// A load's status, for a source's `first_load`.
    pub fn load_status(&self, load: i64) -> Result<Option<String>> {
        Ok(self
            .query_one("SELECT status FROM files WHERE id = $1", &params![load])?
            .map(|r| r.string(0)))
    }
}
