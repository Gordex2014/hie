//! Passes: a source's pass asked for (check-now) and what it did, so a client
//! can follow it (`GET /api/sources/{id}/passes/{pass}`). Every pass asked for
//! before a pass begins is served by that pass.

use anyhow::Result;
use serde_json::Value;

use super::{Db, Param, now_ms, params};

pub(super) const PASSES: &str = "CREATE TABLE IF NOT EXISTS passes (
  id TEXT PRIMARY KEY,
  feed_id TEXT NOT NULL,
  status TEXT NOT NULL,
  skipped_reason TEXT,
  error TEXT,
  loads TEXT,
  new_rows INTEGER NOT NULL DEFAULT 0,
  requested_at INTEGER NOT NULL,
  started_at INTEGER,
  finished_at INTEGER
);
CREATE INDEX IF NOT EXISTS passes_feed_status ON passes(feed_id, status);
";

/// Why a pass ended without finishing.
const INTERRUPTED: &str = "the pass was interrupted before it finished (the engine stopped)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassStatus {
    Queued,
    Running,
    Done,
    /// Not run; `skipped_reason` says why (`paused`).
    Skipped,
    /// The source could not be read; `error` is a secret-safe sentence.
    Failed,
}

impl PassStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            PassStatus::Queued => "queued",
            PassStatus::Running => "running",
            PassStatus::Done => "done",
            PassStatus::Skipped => "skipped",
            PassStatus::Failed => "failed",
        }
    }
}

/// A pass as the API answers it.
#[derive(Debug, Clone, PartialEq)]
pub struct Pass {
    pub id: String,
    pub source: String,
    pub status: String,
    pub skipped_reason: Option<String>,
    pub error: Option<String>,
    pub loads: Vec<i64>,
    pub new_rows: i64,
    pub requested_at: i64,
    pub finished_at: Option<i64>,
}

impl Db {
    /// Records that a pass of `feed` was asked for; its `ps_` id.
    pub fn request_pass(&self, feed: &str) -> Result<String> {
        let id = crate::ids::new(crate::ids::Kind::Pass);
        self.execute(
            "INSERT INTO passes (id, feed_id, status, requested_at) VALUES ($1, $2, 'queued', $3)",
            &params![id, feed, now_ms()],
        )?;
        Ok(id)
    }

    /// The passes of `feed` asked for and not begun: now `running`, served by
    /// the pass that begins, owned by the source's lease holder. Passes still
    /// running under another owner were abandoned and are ended as failed.
    pub fn start_passes(&self, feed: &str) -> Result<Vec<String>> {
        let owner = self.lease(feed)?.map(|l| l.owner);
        self.execute(
            "UPDATE passes SET status = 'failed', error = $3, finished_at = $4
             WHERE feed_id = $1 AND status = 'running' AND COALESCE(owner, '') <> $2",
            &[
                Param::Text(feed.into()),
                Param::Text(owner.clone().unwrap_or_default()),
                Param::Text(INTERRUPTED.into()),
                Param::Int(now_ms()),
            ],
        )?;
        let ids: Vec<String> = self
            .query(
                "SELECT id FROM passes WHERE feed_id = $1 AND status = 'queued' ORDER BY requested_at",
                &params![feed],
            )?
            .iter()
            .map(|r| r.string(0))
            .collect();
        for id in &ids {
            self.execute(
                "UPDATE passes SET status = 'running', started_at = $2, owner = $3 WHERE id = $1 AND status = 'queued'",
                &[
                    Param::Text(id.clone()),
                    Param::Int(now_ms()),
                    owner.clone().map_or(Param::Null, Param::Text),
                ],
            )?;
        }
        Ok(ids)
    }

    /// Ends passes with what they did.
    pub fn finish_passes(
        &self,
        ids: &[String],
        status: PassStatus,
        skipped_reason: Option<&str>,
        error: Option<&str>,
        loads: &[i64],
        new_rows: i64,
    ) -> Result<()> {
        let loads = Value::from(loads.to_vec()).to_string();
        for id in ids {
            self.execute(
                "UPDATE passes SET status = $2, skipped_reason = $3, error = $4, loads = $5, new_rows = $6, finished_at = $7
                 WHERE id = $1 AND status = 'running'",
                &[
                    Param::Text(id.clone()),
                    Param::Text(status.as_str().into()),
                    skipped_reason.map_or(Param::Null, |r| Param::Text(r.into())),
                    error.map_or(Param::Null, |e| Param::Text(e.into())),
                    Param::Text(loads.clone()),
                    Param::Int(new_rows),
                    Param::Int(now_ms()),
                ],
            )?;
        }
        Ok(())
    }

    /// Ends as failed the passes of `feed` left running by `owner` (or by no
    /// known owner): called when that instance starts, before it runs any.
    pub fn recover_passes(&self, feed: &str, owner: &str) -> Result<()> {
        self.execute(
            "UPDATE passes SET status = 'failed', error = $3, finished_at = $4
             WHERE feed_id = $1 AND status = 'running' AND (owner IS NULL OR owner = $2)",
            &params![feed, owner, INTERRUPTED, now_ms()],
        )
        .map(drop)
    }

    /// One pass of `feed`.
    pub fn pass(&self, feed: &str, id: &str) -> Result<Option<Pass>> {
        Ok(self
            .query_one(
                "SELECT id, feed_id, status, skipped_reason, error, loads, new_rows, requested_at, finished_at
                 FROM passes WHERE feed_id = $1 AND id = $2",
                &params![feed, id],
            )?
            .map(|r| Pass {
                id: r.string(0),
                source: r.string(1),
                status: r.string(2),
                skipped_reason: r.opt_string(3),
                error: r.opt_string(4),
                loads: r
                    .json(5)
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default(),
                new_rows: r.i64(6),
                requested_at: r.i64(7),
                finished_at: r.opt_i64(8),
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pass_left_running_is_ended_when_another_owner_passes_or_its_own_restarts() {
        let db = Db::in_memory().unwrap();
        assert!(db.acquire_lease("src", "a", 60_000).unwrap());
        let abandoned = db.request_pass("src").unwrap();
        assert_eq!(
            db.start_passes("src").unwrap(),
            std::slice::from_ref(&abandoned)
        );
        // Instance a stops; b takes the lease and begins a pass.
        db.release_lease("src", "a").unwrap();
        assert!(db.acquire_lease("src", "b", 60_000).unwrap());
        let next = db.request_pass("src").unwrap();
        assert_eq!(db.start_passes("src").unwrap(), std::slice::from_ref(&next));
        let pass = db.pass("src", &abandoned).unwrap().unwrap();
        assert_eq!(pass.status, "failed");
        assert!(pass.error.is_some() && pass.finished_at.is_some());
        // An ended pass is not changed again; b's own pass is not touched.
        db.finish_passes(
            &[abandoned.clone(), next.clone()],
            PassStatus::Done,
            None,
            None,
            &[],
            0,
        )
        .unwrap();
        assert_eq!(
            db.pass("src", &abandoned).unwrap().unwrap().status,
            "failed"
        );
        assert_eq!(db.pass("src", &next).unwrap().unwrap().status, "done");
        // b restarts with a pass still running: recovered as failed.
        let left = db.request_pass("src").unwrap();
        db.start_passes("src").unwrap();
        db.recover_passes("src", "b").unwrap();
        assert_eq!(db.pass("src", &left).unwrap().unwrap().status, "failed");
    }
}
