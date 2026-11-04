//! `$bulk-submit` submissions: what the engine handed to HFS and is waiting on.

use anyhow::Result;
use serde_json::Value;

use super::history::append_submission_events;
use super::loads::Column;
use super::{Db, Param, now_ms, params};

/// One row per submission, with its manifest and where its status is read.
pub(super) const SUBMISSIONS: &str = "CREATE TABLE IF NOT EXISTS submissions (
  token TEXT PRIMARY KEY,
  feed_id TEXT NOT NULL,
  submission_id TEXT NOT NULL,
  status TEXT NOT NULL,
  manifest TEXT NOT NULL,
  status_url TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS submissions_feed_status ON submissions(feed_id, status, next_attempt_at);
CREATE TABLE IF NOT EXISTS dispatches (
  id TEXT PRIMARY KEY,
  feed_id TEXT NOT NULL,
  payload TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'sending',
  created_at INTEGER NOT NULL
);
";

/// A `$bulk-submit` submission the engine handed to HFS and is waiting on.
#[derive(Debug, Clone)]
pub struct Submission {
    pub token: String,
    pub feed_id: String,
    pub submission_id: String,
    pub status: String,
    pub manifest: Value,
    pub status_url: Option<String>,
    pub attempts: u32,
}

/// A file with records waiting to be submitted, and what they are.
#[derive(Debug, Clone)]
pub struct SubmittableFile {
    pub file_id: i64,
    pub name: String,
    pub source_key: Option<String>,
    /// The resource type of the file's first pending record (an export holds one type per file).
    pub resource_type: String,
    /// Pending records of the file that no submission holds.
    pub count: usize,
}

impl Db {
    // ---- submissions ($bulk-submit) ------------------------------------------

    /// Files of a feed with pending records that no submission holds yet, as the
    /// manifest needs them: counted in the store, never listed record by record.
    pub fn submittable_files(&self, feed: &str, limit: usize) -> Result<Vec<SubmittableFile>> {
        let rows = self.query(
            "SELECT r.file_id, f.name, f.source_key, COUNT(*)
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.feed_id = $1 AND r.status = 'pending' AND r.submission IS NULL AND r.exec_retry IS NULL
             GROUP BY r.file_id, f.name, f.source_key
             ORDER BY r.file_id LIMIT $2",
            &params![feed, limit],
        )?;
        let mut files = Vec::with_capacity(rows.len());
        for r in &rows {
            let file_id = r.i64(0);
            let resource_type = self
                .query_one(
                    "SELECT id, resources FROM records
                     WHERE file_id = $1 AND status = 'pending' AND submission IS NULL AND exec_retry IS NULL
                     ORDER BY id LIMIT 1",
                    &params![file_id],
                )?
                .and_then(|row| self.open_record(Some(row.i64(0)), Column::Resources, row.get(1)))
                .and_then(|v| {
                    v.get(0)
                        .and_then(|res| res.get("resourceType"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_else(|| "Resource".to_string());
            files.push(SubmittableFile {
                file_id,
                name: r.string(1),
                source_key: r.opt_string(2),
                resource_type,
                count: r.i64(3) as usize,
            });
        }
        Ok(files)
    }

    /// Opens a submission over every pending record of the given files, which move
    /// to `submitted` in one statement per file.
    pub fn create_submission_for_files(
        &self,
        token: &str,
        feed: &str,
        submission_id: &str,
        manifest: &Value,
        file_ids: &[i64],
    ) -> Result<()> {
        self.with(|c| {
            c.batch("BEGIN")?;
            let now = now_ms();
            let result = (|| {
                c.execute(
                    "INSERT INTO submissions (token, feed_id, submission_id, status, manifest, created_at, updated_at)
                     VALUES ($1, $2, $3, 'kickoff', $4, $5, $6)",
                    &params![token, feed, submission_id, manifest.to_string(), now, now],
                )?;
                for file_id in file_ids {
                    c.execute(
                        "UPDATE records SET status = 'submitted', submission = $1, uncertain = 1, updated_at = $2
                         WHERE file_id = $3 AND feed_id = $4 AND status = 'pending' AND submission IS NULL
                           AND exec_retry IS NULL",
                        &params![token, now, file_id, feed],
                    )?;
                }
                append_submission_events(c, token, "outcome_unknown")?;
                Ok::<_, anyhow::Error>(())
            })();
            match result {
                Ok(()) => c.batch("COMMIT"),
                Err(e) => {
                    let _ = c.batch("ROLLBACK");
                    Err(e)
                }
            }
        })
    }

    /// Opens a submission over the given records, which move to `submitted`.
    pub fn create_submission(
        &self,
        token: &str,
        feed: &str,
        submission_id: &str,
        manifest: &Value,
        record_ids: &[i64],
    ) -> Result<()> {
        self.with(|c| {
            c.batch("BEGIN")?;
            let now = now_ms();
            let result = (|| {
                c.execute(
                    "INSERT INTO submissions (token, feed_id, submission_id, status, manifest, created_at, updated_at)
                     VALUES ($1, $2, $3, 'kickoff', $4, $5, $6)",
                    &params![token, feed, submission_id, manifest.to_string(), now, now],
                )?;
                for id in record_ids {
                    c.execute(
                        "UPDATE records SET status = 'submitted', submission = $2, uncertain = 1, updated_at = $3 WHERE id = $1",
                        &params![id, token, now],
                    )?;
                }
                append_submission_events(c, token, "outcome_unknown")?;
                Ok::<_, anyhow::Error>(())
            })();
            match result {
                Ok(()) => c.batch("COMMIT"),
                Err(e) => {
                    let _ = c.batch("ROLLBACK");
                    Err(e)
                }
            }
        })
    }

    fn submissions(&self, sql: &str, p: &[Param]) -> Result<Vec<Submission>> {
        Ok(self
            .query(sql, p)?
            .iter()
            .map(|r| Submission {
                token: r.string(0),
                feed_id: r.string(1),
                submission_id: r.string(2),
                status: r.string(3),
                manifest: r.json(4).unwrap_or(Value::Null),
                status_url: r.opt_string(5),
                attempts: r.i64(6) as u32,
            })
            .collect())
    }

    pub fn submission(&self, token: &str) -> Result<Option<Submission>> {
        Ok(self
            .submissions(
                "SELECT token, feed_id, submission_id, status, manifest, status_url, attempts FROM submissions WHERE token = $1",
                &params![token],
            )?
            .into_iter()
            .next())
    }

    /// Open submissions of a feed whose next attempt is due.
    pub fn submissions_due(&self, feed: &str) -> Result<Vec<Submission>> {
        self.submissions(
            "SELECT token, feed_id, submission_id, status, manifest, status_url, attempts FROM submissions
             WHERE feed_id = $1 AND status IN ('kickoff', 'running') AND next_attempt_at <= $2 ORDER BY created_at",
            &params![feed, now_ms()],
        )
    }

    pub fn update_submission(
        &self,
        token: &str,
        status: &str,
        status_url: Option<&str>,
        attempts: u32,
        next_attempt_at: i64,
        error: Option<&str>,
    ) -> Result<()> {
        self.execute(
            "UPDATE submissions SET status = $2,
                    status_url = COALESCE(CAST($3 AS TEXT), status_url),
                    attempts = $4, next_attempt_at = $5, error = $6, updated_at = $7
             WHERE token = $1",
            &params![
                token,
                status,
                status_url,
                attempts,
                next_attempt_at,
                error,
                now_ms()
            ],
        )
        .map(drop)
    }

    /// The records a submission holds: (record id, attempt, file id, resources).
    pub fn submitted_records(&self, token: &str) -> Result<Vec<(i64, i64, i64, Value)>> {
        self.submitted_records_after(token, 0, usize::MAX)
    }

    /// A page of a submission's records past `after_id`, so a million-record
    /// submission is settled in chunks rather than held in memory at once.
    pub fn submitted_records_after(
        &self,
        token: &str,
        after_id: i64,
        limit: usize,
    ) -> Result<Vec<(i64, i64, i64, Value)>> {
        Ok(self
            .query(
                "SELECT id, run, file_id, resources FROM records
                 WHERE submission = $1 AND status = 'submitted' AND id > $2 ORDER BY id LIMIT $3",
                &params![token, after_id, limit.min(i64::MAX as usize) as i64],
            )?
            .iter()
            .map(|r| {
                let id = r.i64(0);
                (
                    id,
                    r.i64(1),
                    r.i64(2),
                    self.open_record(Some(id), Column::Resources, r.get(3))
                        .unwrap_or(Value::Null),
                )
            })
            .collect())
    }

    /// Takes rows of a settled submission whose receipts did not confirm them
    /// back to `pending`, outcome unknown, to be submitted again when due.
    pub fn resubmit(&self, rows: &[(i64, i64)], next_attempt_at: i64, reason: &str) -> Result<()> {
        let reason = super::loads::bounded(reason);
        for (id, run) in rows {
            self.execute(
                "UPDATE records SET status = 'pending', submission = NULL, attempts = attempts + 1,
                        next_attempt_at = $3, error_kind = 'transient', error = $4, waiting = 'destination_unavailable',
                        uncertain = 1, updated_at = $5
                 WHERE id = $1 AND run = $2 AND status = 'submitted'",
                &params![id, run, next_attempt_at, reason, now_ms()],
            )?;
        }
        Ok(())
    }

    /// The source key of a file, only when that file is part of the submission.
    pub fn submission_file(&self, token: &str, file_id: i64) -> Result<Option<String>> {
        Ok(self
            .query_one(
                "SELECT f.source_key FROM files f
                 WHERE f.id = $2 AND f.source_key IS NOT NULL
                   AND EXISTS (SELECT 1 FROM records r WHERE r.file_id = f.id AND r.submission = $1)",
                &params![token, file_id],
            )?
            .and_then(|r| r.opt_string(0)))
    }

    // ---- stored transactions ---------------------------------------------------

    /// Keeps a transaction before it is sent: its payload sealed for its id
    /// (it holds the rows' resources), its members `(id, run)` and the state
    /// `sending`. A transaction found still `sending` after a restart may have
    /// committed: its rows are outcome unknown, and the same one goes again.
    /// `None` when none of the rows is pending any more.
    pub fn create_dispatch(
        &self,
        feed: &str,
        payload: &Value,
        members: &[(i64, i64)],
    ) -> Result<Option<String>> {
        let id = format!("tx_{}", &crate::ids::new(crate::ids::Kind::Group)[2..]);
        let sealed = self.seal_dispatch(&id, payload);
        self.atomically(|c| {
            c.execute(
                "INSERT INTO dispatches (id, feed_id, payload, state, created_at) VALUES ($1, $2, $3, 'sending', $4)",
                &params![id, feed, sealed, now_ms()],
            )?;
            let mut joined = 0;
            for (record, run) in members {
                joined += c.execute(
                    "UPDATE records SET dispatch = $1 WHERE id = $2 AND run = $3 AND status = 'pending' AND dispatch IS NULL",
                    &params![id, record, run],
                )?;
            }
            if joined == 0 {
                c.execute("DELETE FROM dispatches WHERE id = $1", &params![id])?;
                return Ok(None);
            }
            Ok(Some(id.clone()))
        })
    }

    /// A stored transaction, opened, and its state (`sending` or `held`).
    pub fn dispatch_payload(&self, id: &str) -> Result<Option<(Value, String)>> {
        Ok(self
            .query_one(
                "SELECT payload, state FROM dispatches WHERE id = $1",
                &params![id],
            )?
            .and_then(|r| {
                self.open_dispatch(id, &r.string(0))
                    .map(|payload| (payload, r.string(1)))
            }))
    }

    /// Marks a stored transaction `sending` (about to go) or `held` (it went
    /// without a confirming answer, or never left).
    pub fn set_dispatch_state(&self, id: &str, state: &str) -> Result<()> {
        self.execute(
            "UPDATE dispatches SET state = $2 WHERE id = $1",
            &params![id, state],
        )
        .map(drop)
    }

    /// Drops a stored transaction once no pending row belongs to it.
    pub fn drop_dispatch_if_settled(&self, id: &str) -> Result<()> {
        self.execute(
            "DELETE FROM dispatches WHERE id = $1
               AND NOT EXISTS (SELECT 1 FROM records WHERE dispatch = $1 AND status = 'pending')",
            &params![id],
        )
        .map(drop)
    }

    /// Open submissions' tokens and when they were created, all sources or one.
    pub fn submission_tokens(&self, feed: Option<&str>) -> Result<Vec<(String, String, i64)>> {
        Ok(self
            .query(
                "SELECT token, status, created_at FROM submissions
                 WHERE CAST($1 AS TEXT) IS NULL OR feed_id = CAST($1 AS TEXT)",
                &params![feed],
            )?
            .iter()
            .map(|r| (r.string(0), r.string(1), r.i64(2)))
            .collect())
    }
}
