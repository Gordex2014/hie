//! A source's operational state: paused or not, its cursors, the lease of the
//! instance running it, and when it last received anything.

use anyhow::Result;

use super::{Db, now_ms, params};

/// Paused or not, by whom; not part of the source's definition file.
pub(super) const STATE: &str = "CREATE TABLE IF NOT EXISTS source_state (
  feed_id TEXT PRIMARY KEY,
  paused INTEGER NOT NULL DEFAULT 0,
  paused_at INTEGER,
  paused_by TEXT
);
";

/// Where a pulling source has read up to, per key.
pub(super) const CURSORS: &str = "CREATE TABLE IF NOT EXISTS source_cursors (
  feed_id TEXT NOT NULL,
  key TEXT NOT NULL,
  cursor TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (feed_id, key)
);
";

/// Which instance runs a source, until when.
pub(super) const LEASES: &str = "CREATE TABLE IF NOT EXISTS source_leases (
  feed_id TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  until INTEGER NOT NULL
);
";

/// Who holds a source and until when.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Lease {
    pub owner: String,
    pub until: i64,
}

impl Db {
    // ---- source state, cursors, leases ------------------------------------------

    /// Pauses or resumes a source. Paused is an operational state kept here, not
    /// in the source's definition file.
    pub fn set_paused(&self, feed: &str, paused: bool, by: Option<&str>) -> Result<()> {
        self.execute(
            "INSERT INTO source_state (feed_id, paused, paused_at, paused_by) VALUES ($1, $2, $3, $4)
             ON CONFLICT(feed_id) DO UPDATE SET paused = excluded.paused, paused_at = excluded.paused_at, paused_by = excluded.paused_by",
            &params![feed, paused, paused.then(now_ms), if paused { by } else { None }],
        )
        .map(drop)
    }

    /// When and by whom a source was paused, if it is.
    pub fn paused_state(&self, feed: &str) -> Result<Option<(i64, Option<String>)>> {
        Ok(self
            .query_one(
                "SELECT paused_at, paused_by FROM source_state WHERE feed_id = $1 AND paused = 1",
                &params![feed],
            )?
            .map(|r| (r.i64(0), r.opt_string(1))))
    }

    pub fn is_paused(&self, feed: &str) -> Result<bool> {
        Ok(self.paused_state(feed)?.is_some())
    }

    /// Where a pulling source left off, per key (a resource type for a FHIR server).
    pub fn cursor(&self, feed: &str, key: &str) -> Result<Option<String>> {
        Ok(self
            .query_one(
                "SELECT cursor FROM source_cursors WHERE feed_id = $1 AND key = $2",
                &params![feed, key],
            )?
            .and_then(|r| r.opt_string(0)))
    }

    pub fn set_cursor(&self, feed: &str, key: &str, cursor: &str) -> Result<()> {
        self.execute(
            "INSERT INTO source_cursors (feed_id, key, cursor, updated_at) VALUES ($1, $2, $3, $4)
             ON CONFLICT(feed_id, key) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at",
            &params![feed, key, cursor, now_ms()],
        )
        .map(drop)
    }

    pub fn cursors(&self, feed: &str) -> Result<Vec<(String, String)>> {
        Ok(self
            .query(
                "SELECT key, cursor FROM source_cursors WHERE feed_id = $1 ORDER BY key",
                &params![feed],
            )?
            .iter()
            .map(|r| (r.string(0), r.string(1)))
            .collect())
    }

    /// Takes or renews the lease on a source for `ttl_ms`: granted when nobody holds
    /// it, when `owner` already does, or when the holder's lease has run out.
    pub fn acquire_lease(&self, feed: &str, owner: &str, ttl_ms: i64) -> Result<bool> {
        let now = now_ms();
        let n = self.execute(
            "INSERT INTO source_leases (feed_id, owner, until) VALUES ($1, $2, $3)
             ON CONFLICT(feed_id) DO UPDATE SET owner = excluded.owner, until = excluded.until
             WHERE source_leases.owner = excluded.owner OR source_leases.until < $4",
            &params![feed, owner, now + ttl_ms, now],
        )?;
        Ok(n > 0)
    }

    /// Lets go of a source, so another instance may take it at once.
    pub fn release_lease(&self, feed: &str, owner: &str) -> Result<()> {
        self.execute(
            "DELETE FROM source_leases WHERE feed_id = $1 AND owner = $2",
            &params![feed, owner],
        )
        .map(drop)
    }

    /// Who holds a source now, if anyone still does.
    pub fn lease(&self, feed: &str) -> Result<Option<Lease>> {
        Ok(self
            .query_one(
                "SELECT owner, until FROM source_leases WHERE feed_id = $1 AND until >= $2",
                &params![feed, now_ms()],
            )?
            .map(|r| Lease {
                owner: r.string(0),
                until: r.i64(1),
            }))
    }

    pub fn last_received_at(&self, feed: &str) -> Result<Option<i64>> {
        Ok(self
            .query_one(
                "SELECT MAX(received_at) FROM files WHERE feed_id = $1 AND status <> 'duplicate'",
                &params![feed],
            )?
            .and_then(|r| r.opt_i64(0)))
    }
}
