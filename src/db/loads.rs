//! Loads: the files (objects) taken from sources and the records cut from them.

use anyhow::Result;
use serde_json::{Map, Value, json};

use super::{Db, Param, now_ms, params};
use crate::versions::{VersionKey, VersionRef};

/// One row per object taken from a source: a load.
pub(super) const FILES: &str = "CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY,
  feed_id TEXT NOT NULL,
  name TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  size INTEGER NOT NULL,
  status TEXT NOT NULL,
  duplicate_of INTEGER,
  received_at INTEGER NOT NULL,
  finished_at INTEGER,
  error TEXT,
  rejected_reported INTEGER NOT NULL DEFAULT 0,
  source_key TEXT,
  archived INTEGER NOT NULL DEFAULT 0,
  transform_id TEXT,
  transform_version INTEGER,
  environment TEXT
);
CREATE INDEX IF NOT EXISTS files_feed_sha ON files(feed_id, sha256);
CREATE INDEX IF NOT EXISTS files_feed_received ON files(feed_id, received_at);
";

/// One row per record cut from a load, with its fate.
pub(super) const RECORDS: &str = "CREATE TABLE IF NOT EXISTS records (
  id INTEGER PRIMARY KEY,
  file_id INTEGER NOT NULL REFERENCES files(id),
  feed_id TEXT NOT NULL,
  line INTEGER NOT NULL,
  raw TEXT NOT NULL,
  original_raw TEXT,
  status TEXT NOT NULL,
  resources TEXT,
  error_kind TEXT,
  error TEXT,
  response TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  submission TEXT,
  exec_retry TEXT,
  exec_transform_id TEXT,
  exec_version INTEGER
);
CREATE INDEX IF NOT EXISTS records_feed_status ON records(feed_id, status, next_attempt_at);
CREATE INDEX IF NOT EXISTS records_feed_status_id ON records(feed_id, status, id);
CREATE INDEX IF NOT EXISTS records_file ON records(file_id);
";

/// Columns added to these tables after a store was first created, in the order they are added.
pub(super) const ADDED_COLUMNS: &[(&str, &str, &str)] = &[
    ("files", "source_key", "TEXT"),
    ("files", "archived", "INTEGER NOT NULL DEFAULT 0"),
    ("records", "submission", "TEXT"),
    // The version a load runs, fixed when it is admitted (or leaves `queued`).
    ("files", "transform_id", "TEXT"),
    ("files", "transform_version", "INTEGER"),
    ("files", "environment", "TEXT"),
    // Who runs a record's current attempt: a retry and its version, or null
    // for the load's own snapshot.
    ("records", "exec_retry", "TEXT"),
    ("records", "exec_transform_id", "TEXT"),
    ("records", "exec_version", "INTEGER"),
];

#[derive(Debug, Clone)]
pub struct WorkItem {
    pub id: i64,
    pub file_id: i64,
    pub line: i64,
    pub file_name: String,
    pub raw: Value,
    pub resources: Option<Value>,
    pub attempts: u32,
    /// The retry running this record's current attempt; `None` for the load's own.
    pub retry: Option<String>,
    /// The version this attempt runs: the retry's, else the load's snapshot;
    /// `None` for a load admitted before loads recorded one.
    pub version: Option<VersionKey>,
}

/// One failed row whose content is still kept, as Try's `failures` input and
/// the failure samples read it.
#[derive(Debug, Clone, PartialEq)]
pub struct FailedRow {
    pub record_id: i64,
    pub line: i64,
    pub raw: Value,
    /// The transform context the row ran with (`feed`, `file`, `line`).
    pub context: Value,
}

/// Why [`Db::failed_rows`] has no rows to give.
#[derive(Debug)]
pub enum FailedRowsError {
    LoadNotFound,
    GroupNotFound,
    /// A record named in the selection is not one of the group's failing rows.
    RecordNotInGroup(i64),
    /// The group's content was purged or expired (`410 content_expired`).
    ContentExpired,
    Store(anyhow::Error),
}

impl From<anyhow::Error> for FailedRowsError {
    fn from(e: anyhow::Error) -> Self {
        FailedRowsError::Store(e)
    }
}

#[derive(Debug, Clone)]
pub struct FinishedFile {
    pub id: i64,
    pub name: String,
    pub total: i64,
    pub sent: i64,
    pub rejected: i64,
    pub newly_rejected: i64,
    pub first_rejection: Option<(i64, String)>,
}

/// What a work item is read from: the record, its load, and who runs it (the
/// record's retry and version, else the load's snapshot).
const WORK_COLUMNS: &str =
    "r.id, r.file_id, r.line, f.name, r.raw, r.resources, r.attempts, r.exec_retry,
    COALESCE(r.exec_transform_id, f.transform_id),
    CASE WHEN r.exec_transform_id IS NULL THEN f.transform_version ELSE r.exec_version END";

/// Rows per multi-row statement: well under both engines' parameter limits.
const INSERT_ROWS_PER_STATEMENT: usize = 200;

impl Db {
    // ---- files -------------------------------------------------------------

    /// Whether a file of this name and size was already taken (not as a duplicate):
    /// what lets a read-only source be skipped without downloading it again.
    pub fn has_file(&self, feed: &str, name: &str, size: u64) -> Result<bool> {
        Ok(self
            .query_one(
                "SELECT 1 FROM files WHERE feed_id = $1 AND name = $2 AND size = $3 AND status <> 'duplicate' LIMIT 1",
                &params![feed, name, size],
            )?
            .is_some())
    }

    pub fn find_file_by_hash(&self, feed: &str, sha: &str) -> Result<Option<i64>> {
        Ok(self
            .query_one(
                "SELECT id FROM files WHERE feed_id = $1 AND sha256 = $2 AND status <> 'duplicate' ORDER BY id LIMIT 1",
                &params![feed, sha],
            )?
            .map(|r| r.i64(0)))
    }

    /// Records a file. `source_key` is kept when the object must stay at its source
    /// until HFS has fetched it (the `$bulk-submit` path).
    #[allow(clippy::too_many_arguments)]
    pub fn insert_file(
        &self,
        feed: &str,
        name: &str,
        sha: &str,
        size: u64,
        status: &str,
        duplicate_of: Option<i64>,
        source_key: Option<&str>,
    ) -> Result<i64> {
        let now = now_ms();
        let finished_at = if status == "processing" {
            None
        } else {
            Some(now)
        };
        self.insert_returning_id(
            "INSERT INTO files (feed_id, name, sha256, size, status, duplicate_of, received_at, finished_at, source_key)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            &params![feed, name, sha, size, status, duplicate_of, now, finished_at, source_key],
        )
    }

    /// A load created before its pass begins (a database source's first load,
    /// at Start): `queued`, with no content yet. The source's next pass adopts
    /// it ([`Db::open_load`]) instead of creating one.
    pub fn create_queued_load(&self, feed: &str) -> Result<i64> {
        self.insert_returning_id(
            "INSERT INTO files (feed_id, name, sha256, size, status, received_at) VALUES ($1, '', '', 0, 'queued', $2)",
            &params![feed, now_ms()],
        )
    }

    /// Starts a load for an object a pass took: the source's oldest `queued`
    /// load when there is one, else a new load; `processing`, with the version
    /// it runs fixed from now on (`None` on the paths that run no transform).
    pub fn open_load(
        &self,
        feed: &str,
        name: &str,
        sha: &str,
        size: u64,
        source_key: Option<&str>,
        version: Option<&VersionRef>,
    ) -> Result<i64> {
        let (transform_id, transform_version, environment) = match version {
            Some(v) => (
                Some(v.transform_id.as_str()),
                Some(v.version),
                Some(serde_json::to_string(&v.environment)?),
            ),
            None => (None, None, None),
        };
        let now = now_ms();
        if let Some(queued) = self.query_one(
            "SELECT id FROM files WHERE feed_id = $1 AND status = 'queued' ORDER BY id LIMIT 1",
            &params![feed],
        )? {
            let id = queued.i64(0);
            let adopted = self.execute(
                "UPDATE files SET name = $2, sha256 = $3, size = $4, status = 'processing', received_at = $5,
                        source_key = $6, transform_id = $7, transform_version = $8, environment = $9
                 WHERE id = $1 AND status = 'queued'",
                &params![id, name, sha, size, now, source_key, transform_id, transform_version, environment],
            )?;
            if adopted == 1 {
                return Ok(id);
            }
        }
        self.insert_returning_id(
            "INSERT INTO files (feed_id, name, sha256, size, status, received_at, source_key, transform_id, transform_version, environment)
             VALUES ($1, $2, $3, $4, 'processing', $5, $6, $7, $8, $9)",
            &params![feed, name, sha, size, now, source_key, transform_id, transform_version, environment],
        )
    }

    /// The failed rows of one failure group of a load, all or those named, in
    /// line order, at most 100, while their content is kept.
    ///
    /// Stub: failure groups do not exist until track H builds them, so every
    /// existing load answers `GroupNotFound`.
    pub fn failed_rows(
        &self,
        load_id: i64,
        group: &str,
        selection: Option<&[i64]>,
    ) -> std::result::Result<Vec<FailedRow>, FailedRowsError> {
        let _ = (group, selection);
        if self
            .query_one("SELECT 1 FROM files WHERE id = $1", &params![load_id])?
            .is_none()
        {
            return Err(FailedRowsError::LoadNotFound);
        }
        Err(FailedRowsError::GroupNotFound)
    }

    /// The key and name of a file still waiting at its source (not archived yet).
    pub fn file_source(&self, file_id: i64) -> Result<Option<(String, String)>> {
        Ok(self
            .query_one(
                "SELECT source_key, name FROM files WHERE id = $1 AND source_key IS NOT NULL AND archived = 0",
                &params![file_id],
            )?
            .map(|r| (r.string(0), r.string(1))))
    }

    /// The file was moved out of the way; `key` is where it lives now, so a retry
    /// can still hand it to HFS.
    pub fn mark_file_archived(&self, file_id: i64, key: &str) -> Result<()> {
        self.execute(
            "UPDATE files SET source_key = $2, archived = 1 WHERE id = $1",
            &params![file_id, key],
        )
        .map(drop)
    }

    /// Rejected records of a file, grouped by reason, each with a few sample rows.
    pub fn failure_groups(&self, file_id: i64) -> Result<Vec<Value>> {
        let groups = self.query_json(
            "SELECT error_kind AS kind, error AS reason, COUNT(*) AS n, MIN(line) AS first_line
             FROM records WHERE file_id = $1 AND status = 'rejected'
             GROUP BY error_kind, error ORDER BY n DESC, first_line",
            params![file_id],
        )?;
        let mut out = Vec::with_capacity(groups.len());
        for g in groups {
            let samples = self.query_json(
                "SELECT id, line, raw FROM records
                 WHERE file_id = $1 AND status = 'rejected'
                   AND error_kind IS NOT DISTINCT FROM $2 AND error IS NOT DISTINCT FROM $3
                 ORDER BY line LIMIT 3",
                params![file_id, g["kind"].as_str(), g["reason"].as_str()],
            )?;
            out.push(json!({
                "kind": g["kind"],
                "reason": g["reason"],
                "count": g["n"],
                "first_line": g["first_line"],
                "samples": samples,
            }));
        }
        Ok(out)
    }

    /// Puts a file's rejected records back at the start, all of them or those with
    /// one reason. Returns how many.
    pub fn reprocess_rejected(&self, file_id: i64, reason: Option<&str>) -> Result<usize> {
        let n = self.execute(
            "UPDATE records SET status = 'received', resources = NULL, error_kind = NULL, error = NULL,
                    response = NULL, attempts = 0, next_attempt_at = 0, submission = NULL, updated_at = $3
             WHERE file_id = $1 AND status = 'rejected' AND (CAST($2 AS TEXT) IS NULL OR error = CAST($2 AS TEXT))",
            &params![file_id, reason, now_ms()],
        )?;
        if n > 0 {
            self.execute(
                "UPDATE files SET status = 'processing', finished_at = NULL WHERE id = $1",
                &params![file_id],
            )?;
        }
        Ok(n as usize)
    }

    pub fn fail_file(&self, file_id: i64, error: &str) -> Result<()> {
        self.execute(
            "UPDATE files SET status = 'unreadable', error = $2, finished_at = $3 WHERE id = $1",
            &params![file_id, error, now_ms()],
        )
        .map(drop)
    }

    /// Marks files whose records have all reached `sent` or `rejected` as done and returns them.
    pub fn finish_files(&self, feed: &str) -> Result<Vec<FinishedFile>> {
        let rows = self.query(
            "SELECT f.id, f.name,
                    COUNT(r.id),
                    CAST(COALESCE(SUM(CASE WHEN r.status = 'sent' THEN 1 ELSE 0 END), 0) AS BIGINT),
                    CAST(COALESCE(SUM(CASE WHEN r.status = 'rejected' THEN 1 ELSE 0 END), 0) AS BIGINT),
                    f.rejected_reported
             FROM files f LEFT JOIN records r ON r.file_id = f.id
             WHERE f.feed_id = $1 AND f.status IN ('processing', 'unpacked')
             GROUP BY f.id, f.name, f.rejected_reported
             HAVING COALESCE(SUM(CASE WHEN r.status IN ('received', 'pending', 'submitted') THEN 1 ELSE 0 END), 0) = 0",
            &params![feed],
        )?;
        let mut done: Vec<FinishedFile> = rows
            .iter()
            .map(|r| FinishedFile {
                id: r.i64(0),
                name: r.string(1),
                total: r.i64(2),
                sent: r.i64(3),
                rejected: r.i64(4),
                newly_rejected: r.i64(4) - r.i64(5),
                first_rejection: None,
            })
            .collect();
        for f in &mut done {
            self.execute(
                "UPDATE files SET status = 'done', finished_at = $2,
                        rejected_reported = CASE WHEN rejected_reported > $3 THEN rejected_reported ELSE $3 END
                 WHERE id = $1",
                &params![f.id, now_ms(), f.rejected],
            )?;
            f.first_rejection = self
                .query_one(
                    "SELECT line, error FROM records WHERE file_id = $1 AND status = 'rejected' ORDER BY line LIMIT 1",
                    &params![f.id],
                )?
                .map(|r| (r.i64(0), r.string(1)));
        }
        Ok(done)
    }

    // ---- records -----------------------------------------------------------

    pub fn insert_records(
        &self,
        file_id: i64,
        feed: &str,
        rows: &[(i64, Value, Option<String>)],
    ) -> Result<()> {
        self.with(|c| {
            c.batch("BEGIN")?;
            let now = now_ms();
            let result = (|| {
                // Rows go in by the hundred per statement: one round trip each
                // over a million rows is an hour; this is minutes.
                for chunk in rows.chunks(INSERT_ROWS_PER_STATEMENT) {
                    let mut sql = String::from(
                        "INSERT INTO records (file_id, feed_id, line, raw, status, error_kind, error, updated_at) VALUES ",
                    );
                    let mut values: Vec<Param> = Vec::with_capacity(chunk.len() * 8);
                    for (i, (line, raw, parse_error)) in chunk.iter().enumerate() {
                        let (status, kind) = match parse_error {
                            Some(_) => ("rejected", Some("parse")),
                            None => ("received", None),
                        };
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        let b = i * 8;
                        sql.push_str(&format!(
                            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                            b + 1, b + 2, b + 3, b + 4, b + 5, b + 6, b + 7, b + 8
                        ));
                        values.extend(params![file_id, feed, line, raw.to_string(), status, kind, parse_error, now]);
                    }
                    c.execute(&sql, &values)?;
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

    fn work_items(&self, sql: &str, p: &[Param]) -> Result<Vec<WorkItem>> {
        Ok(self
            .query(sql, p)?
            .iter()
            .map(|r| WorkItem {
                id: r.i64(0),
                file_id: r.i64(1),
                line: r.i64(2),
                file_name: r.string(3),
                raw: r.json(4).unwrap_or(Value::Null),
                resources: r.json(5),
                attempts: r.i64(6) as u32,
                retry: r.opt_string(7),
                version: match (r.opt_string(8), r.opt_i64(9)) {
                    (Some(transform_id), Some(version)) => Some(VersionKey {
                        transform_id,
                        version,
                    }),
                    _ => None,
                },
            })
            .collect())
    }

    pub fn received_records(&self, feed: &str, limit: usize) -> Result<Vec<WorkItem>> {
        self.received_records_after(feed, 0, limit)
    }

    /// Received records with ids above `after`, in id order: a round of the
    /// transform walks them chunk by chunk without meeting the ones it skipped.
    pub fn received_records_after(
        &self,
        feed: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<WorkItem>> {
        self.work_items(
            &format!(
                "SELECT {WORK_COLUMNS} FROM records r JOIN files f ON f.id = r.file_id
                 WHERE r.feed_id = $1 AND r.status = 'received' AND r.id > $2 ORDER BY r.id LIMIT $3"
            ),
            &params![feed, after, limit],
        )
    }

    pub fn due_records(&self, feed: &str, limit: usize) -> Result<Vec<WorkItem>> {
        self.work_items(
            &format!(
                "SELECT {WORK_COLUMNS}
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.feed_id = $1 AND r.status = 'pending' AND r.next_attempt_at <= $2
             ORDER BY r.id LIMIT $3"
            ),
            &params![feed, now_ms(), limit],
        )
    }

    /// Marks records whose source row is the resource to deliver as transformed,
    /// in one statement per hundreds of ids: the resource list is built from the
    /// row in the store, so no payload travels.
    pub fn mark_passthrough(&self, ids: &[i64]) -> Result<()> {
        let now = now_ms();
        for chunk in ids.chunks(INSERT_ROWS_PER_STATEMENT) {
            let marks: Vec<String> = (0..chunk.len()).map(|i| format!("${}", i + 2)).collect();
            let mut values: Vec<Param> = Vec::with_capacity(chunk.len() + 1);
            values.push(Param::Int(now));
            values.extend(chunk.iter().map(|id| Param::Int(*id)));
            self.execute(
                &format!(
                    "UPDATE records SET status = 'pending', resources = '[' || raw || ']', error_kind = NULL, error = NULL,
                            attempts = 0, next_attempt_at = 0, updated_at = $1 WHERE id IN ({})",
                    marks.join(", ")
                ),
                &values,
            )?;
        }
        Ok(())
    }

    pub fn mark_transformed(&self, id: i64, resources: &Value) -> Result<()> {
        self.execute(
            "UPDATE records SET status = 'pending', resources = $2, error_kind = NULL, error = NULL,
                    attempts = 0, next_attempt_at = 0, updated_at = $3 WHERE id = $1",
            &params![id, resources.to_string(), now_ms()],
        )
        .map(drop)
    }

    pub fn mark_rejected(
        &self,
        id: i64,
        kind: &str,
        error: &str,
        response: Option<&Value>,
    ) -> Result<()> {
        self.execute(
            "UPDATE records SET status = 'rejected', error_kind = $2, error = $3, response = $4, updated_at = $5 WHERE id = $1",
            &params![id, kind, error, response.map(Value::to_string), now_ms()],
        )
        .map(drop)
    }

    /// Marks many records sent, each with its own response, in one statement per
    /// hundreds of records: `UPDATE ... FROM (VALUES ...)`, which both engines
    /// take with the columns named `column1` and `column2`.
    pub fn mark_sent_many(&self, items: &[(i64, Value)]) -> Result<()> {
        let now = now_ms();
        for chunk in items.chunks(INSERT_ROWS_PER_STATEMENT) {
            let rows: Vec<String> = (0..chunk.len())
                .map(|i| {
                    format!(
                        "(CAST(${} AS BIGINT), CAST(${} AS TEXT))",
                        i * 2 + 2,
                        i * 2 + 3
                    )
                })
                .collect();
            let mut values: Vec<Param> = Vec::with_capacity(chunk.len() * 2 + 1);
            values.push(Param::Int(now));
            for (id, response) in chunk {
                values.push(Param::Int(*id));
                values.push(Param::Text(response.to_string()));
            }
            self.execute(
                &format!(
                    "UPDATE records SET status = 'sent', response = v.column2, error_kind = NULL, error = NULL, updated_at = $1
                     FROM (VALUES {}) AS v WHERE records.id = v.column1",
                    rows.join(", ")
                ),
                &values,
            )?;
        }
        Ok(())
    }

    pub fn mark_sent(&self, id: i64, response: &Value) -> Result<()> {
        self.execute(
            "UPDATE records SET status = 'sent', response = $2, error_kind = NULL, error = NULL, updated_at = $3 WHERE id = $1",
            &params![id, response.to_string(), now_ms()],
        )
        .map(drop)
    }

    pub fn defer(&self, id: i64, attempts: u32, next_attempt_at: i64, error: &str) -> Result<()> {
        self.execute(
            "UPDATE records SET attempts = $2, next_attempt_at = $3, error_kind = 'transient', error = $4, updated_at = $5 WHERE id = $1",
            &params![id, attempts, next_attempt_at, error, now_ms()],
        )
        .map(drop)
    }

    /// Puts a record back at the start of the pipeline, optionally with corrected source data.
    pub fn reprocess(&self, id: i64, raw: Option<&Value>) -> Result<bool> {
        let n = match raw {
            Some(raw) => self.execute(
                "UPDATE records SET status = 'received', original_raw = COALESCE(original_raw, raw), raw = $2, resources = NULL, error_kind = NULL, error = NULL,
                        response = NULL, attempts = 0, next_attempt_at = 0, updated_at = $3 WHERE id = $1",
                &params![id, raw.to_string(), now_ms()],
            )?,
            None => self.execute(
                "UPDATE records SET status = 'received', resources = NULL, error_kind = NULL, error = NULL,
                        response = NULL, attempts = 0, next_attempt_at = 0, updated_at = $2 WHERE id = $1",
                &params![id, now_ms()],
            )?,
        };
        if n > 0 {
            self.execute(
                "UPDATE files SET status = 'processing', finished_at = NULL WHERE id = (SELECT file_id FROM records WHERE id = $1)",
                &params![id],
            )?;
        }
        Ok(n > 0)
    }

    // ---- counts and free queries -------------------------------------------------

    pub fn feed_counts(&self, feed: &str) -> Result<Value> {
        let mut counts = Map::new();
        for status in ["received", "pending", "submitted", "sent", "rejected"] {
            counts.insert(status.into(), json!(0));
        }
        for r in self.query(
            "SELECT status, COUNT(*) FROM records WHERE feed_id = $1 GROUP BY status",
            &params![feed],
        )? {
            counts.insert(r.string(0), json!(r.i64(1)));
        }
        let files = self
            .query_one(
                "SELECT COUNT(*) FROM files WHERE feed_id = $1 AND status <> 'duplicate'",
                &params![feed],
            )?
            .map_or(0, |r| r.i64(0));
        let duplicates = self
            .query_one(
                "SELECT COUNT(*) FROM files WHERE feed_id = $1 AND status = 'duplicate'",
                &params![feed],
            )?
            .map_or(0, |r| r.i64(0));
        counts.insert("files".into(), json!(files));
        counts.insert("duplicate_files".into(), json!(duplicates));
        Ok(Value::Object(counts))
    }
}
