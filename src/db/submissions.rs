//! `$bulk-submit` submissions: what the engine handed to HFS and is waiting on.

use anyhow::Result;
use serde_json::Value;

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
             WHERE r.feed_id = $1 AND r.status = 'pending' AND r.submission IS NULL
             GROUP BY r.file_id, f.name, f.source_key
             ORDER BY r.file_id LIMIT $2",
            &params![feed, limit],
        )?;
        let mut files = Vec::with_capacity(rows.len());
        for r in &rows {
            let file_id = r.i64(0);
            let resource_type = self
                .query_one(
                    "SELECT resources FROM records
                     WHERE file_id = $1 AND status = 'pending' AND submission IS NULL ORDER BY id LIMIT 1",
                    &params![file_id],
                )?
                .and_then(|row| row.json(0))
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

    /// Pending records of a feed that no submission holds yet, with their
    /// resources: what the engine writes out itself on the transformed bulk path.
    pub fn submittable_records(&self, feed: &str, limit: usize) -> Result<Vec<(i64, Value)>> {
        Ok(self
            .query(
                "SELECT id, resources FROM records
                 WHERE feed_id = $1 AND status = 'pending' AND submission IS NULL
                 ORDER BY id LIMIT $2",
                &params![feed, limit],
            )?
            .iter()
            .map(|r| (r.i64(0), r.json(1).unwrap_or(Value::Null)))
            .collect())
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
                        "UPDATE records SET status = 'submitted', submission = $1, updated_at = $2
                         WHERE file_id = $3 AND feed_id = $4 AND status = 'pending' AND submission IS NULL",
                        &params![token, now, file_id, feed],
                    )?;
                }
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
                        "UPDATE records SET status = 'submitted', submission = $2, updated_at = $3 WHERE id = $1",
                        &params![id, token, now],
                    )?;
                }
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

    /// The records a submission holds: (record id, file id, resources).
    pub fn submitted_records(&self, token: &str) -> Result<Vec<(i64, i64, Value)>> {
        self.submitted_records_after(token, 0, usize::MAX)
    }

    /// A page of a submission's records past `after_id`, so a million-record
    /// submission is settled in chunks rather than held in memory at once.
    pub fn submitted_records_after(
        &self,
        token: &str,
        after_id: i64,
        limit: usize,
    ) -> Result<Vec<(i64, i64, Value)>> {
        Ok(self
            .query(
                "SELECT id, file_id, resources FROM records
                 WHERE submission = $1 AND status = 'submitted' AND id > $2 ORDER BY id LIMIT $3",
                &params![token, after_id, limit.min(i64::MAX as usize) as i64],
            )?
            .iter()
            .map(|r| (r.i64(0), r.i64(1), r.json(2).unwrap_or(Value::Null)))
            .collect())
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
}
