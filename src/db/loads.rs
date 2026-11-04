//! Loads: the files (objects) taken from sources and the records cut from them,
//! and the custody of the records' content.
//!
//! **Custody** (`custody.rs`). A record's content (its raw row, the corrected
//! row's original, the resources it became, the destination's answers) is
//! sealed before it is first written, bound to its record and column, so the
//! store's files (SQLite pages and WAL included) never hold it in plaintext.
//! It is opened only in memory, to transform, deliver or serve it. Failed
//! content goes when its retention runs out, when its row is written by a
//! retry or reprocess, or when its source is deleted (`crate::purge`); the row
//! keeps `raw: null` and its `content` state (`expired` or `purged`).
//!
//! **Attempts.** Every outcome a stage records names the attempt it worked on
//! (`run`): a result of an attempt that a reprocess or retry replaced since is
//! dropped, never written over the newer one.
//!
//! **Stored rows** (`stored.rs`): rows of a load by reference, as Try's
//! `stored` input reads them, on one snapshot of the store.
//!
//! Every change of a record's fate appends to its history (`history.rs`).

mod custody;
mod stored;

use std::collections::BTreeMap;

use anyhow::Result;
use serde_json::{Map, Value, json};

use super::engine::Conn;
use super::history::{Failure, OUTCOME_SQL, append_events, group_for};
use super::{Db, Param, Tx, now_ms, params};
use crate::versions::{VersionKey, VersionRef};
pub(super) use custody::{Column, ContentKeys};
pub use stored::{StoredRef, StoredRow, StoredRowsError};

/// The longest error or reason text kept in plaintext; longer ones are cut.
pub const REASON_MAX_CHARS: usize = 500;

/// `text` cut to [`REASON_MAX_CHARS`] characters.
pub(super) fn bounded(text: &str) -> String {
    match text.char_indices().nth(REASON_MAX_CHARS) {
        None => text.to_string(),
        Some((at, _)) => format!("{}…", &text[..at]),
    }
}

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
  environment TEXT,
  admitted INTEGER NOT NULL DEFAULT 1
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
  exec_version INTEGER,
  run INTEGER NOT NULL DEFAULT 1,
  uncertain INTEGER NOT NULL DEFAULT 0,
  waiting TEXT,
  dispatch TEXT,
  group_id TEXT,
  failed_at INTEGER,
  content TEXT,
  resource_count INTEGER,
  resource_types TEXT,
  validated INTEGER NOT NULL DEFAULT 0
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
    // The attempt number (1 for the load's own; each retry or reprocess adds one).
    ("records", "run", "INTEGER NOT NULL DEFAULT 1"),
    // A write was sent and not confirmed: outcome unknown until settled.
    ("records", "uncertain", "INTEGER NOT NULL DEFAULT 0"),
    // Why the row is held without failing: `validation_unavailable` or
    // `destination_unavailable` (a version held for its environment is in
    // the version store's `version_waits`).
    ("records", "waiting", "TEXT"),
    // The stored transaction an unconfirmed write belongs to (`dispatches`).
    ("records", "dispatch", "TEXT"),
    // The failure group the row is in, and when it last failed.
    ("records", "group_id", "TEXT"),
    ("records", "failed_at", "INTEGER"),
    // Null while the content is kept; `expired` or `purged` once it is gone.
    ("records", "content", "TEXT"),
    ("records", "resource_count", "INTEGER"),
    ("records", "resource_types", "TEXT"),
    ("records", "validated", "INTEGER NOT NULL DEFAULT 0"),
    // Whether a load's rows are all in: set with the rows, in their
    // transaction. A load opened whose rows never went in is neither finished
    // nor taken as admitted (loads from before the marker count as admitted).
    ("files", "admitted", "INTEGER NOT NULL DEFAULT 1"),
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
    /// Whether the destination already took a write of it without confirming.
    pub uncertain: bool,
    /// The stored transaction its unconfirmed write belongs to, re-sent as is.
    pub dispatch: Option<String>,
    /// Whether its resources passed the destination's `$validate`.
    pub validated: bool,
    /// The attempt this is (1 for the load's own; each retry or reprocess
    /// adds one): what an outcome of it is recorded against.
    pub run: i64,
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

/// Why rows are held without failing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Waiting {
    /// The destination cannot validate; nothing of the batch was dispatched.
    Validation,
    /// Writes are not confirmed; they are tried again.
    Destination,
}

impl Waiting {
    pub fn as_str(self) -> &'static str {
        match self {
            Waiting::Validation => "validation_unavailable",
            Waiting::Destination => "destination_unavailable",
        }
    }
}

/// A delivery attempt that did not settle its rows.
#[derive(Debug, Clone)]
pub struct Deferral<'a> {
    pub waiting: Waiting,
    /// Why, for the record's `error` while it waits.
    pub reason: &'a str,
    /// The write was sent and may have been applied.
    pub sent: bool,
    /// The stored transaction the rows belong to from now on.
    pub dispatch: Option<&'a str>,
}

/// What a work item is read from: the record, its load, and who runs it (the
/// record's retry and version, else the load's snapshot).
const WORK_COLUMNS: &str =
    "r.id, r.file_id, r.line, f.name, r.raw, r.resources, r.attempts, r.exec_retry,
    COALESCE(r.exec_transform_id, f.transform_id),
    CASE WHEN r.exec_transform_id IS NULL THEN f.transform_version ELSE r.exec_version END,
    r.uncertain, r.dispatch, r.validated, r.run";

/// Rows per multi-row statement: well under both engines' parameter limits.
const INSERT_ROWS_PER_STATEMENT: usize = 200;

impl Db {
    /// Runs `f` as one transaction on the store's connection, as a writer
    /// from its start (SQLite `BEGIN IMMEDIATE`), so a transaction that read
    /// first never fails to write because another connection wrote meanwhile.
    pub(super) fn atomically<T>(&self, f: impl FnOnce(&mut Conn) -> Result<T>) -> Result<T> {
        self.with(|c| {
            let begin = match c {
                Conn::Sqlite(_) => "BEGIN IMMEDIATE",
                Conn::Postgres(_) => "BEGIN",
            };
            c.batch(begin)?;
            match f(c) {
                Ok(value) => {
                    c.batch("COMMIT")?;
                    Ok(value)
                }
                Err(e) => {
                    let _ = c.batch("ROLLBACK");
                    Err(e)
                }
            }
        })
    }
}

/// `$n, $n+1, ...` for `count` values starting at `first`.
fn marks(first: usize, count: usize) -> String {
    (0..count)
        .map(|i| format!("${}", first + i))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The resource types of a record's resources and how many of each.
fn resource_types(resources: &Value) -> (i64, String) {
    let mut types: BTreeMap<String, i64> = BTreeMap::new();
    let mut n = 0;
    for r in resources.as_array().into_iter().flatten() {
        n += 1;
        let kind = r["resourceType"].as_str().unwrap_or("Resource").to_string();
        *types.entry(kind).or_default() += 1;
    }
    let types: Map<String, Value> = types.into_iter().map(|(k, v)| (k, json!(v))).collect();
    (n, Value::Object(types).to_string())
}

/// ` FOR UPDATE` on PostgreSQL, where a transaction locks the rows it reads
/// only when asked; nothing on SQLite, whose `BEGIN IMMEDIATE` (see
/// [`Db::atomically`]) already makes the whole transaction exclusive.
pub(super) fn for_update(c: &Conn) -> &'static str {
    match c {
        Conn::Postgres(_) => " FOR UPDATE",
        Conn::Sqlite(_) => "",
    }
}

/// A record's eligibility for a new attempt, as a predicate on `records`: no
/// write of it unresolved and no retry running it.
pub(super) const NOT_IN_FLIGHT: &str =
    "status <> 'submitted' AND dispatch IS NULL AND uncertain = 0
     AND NOT EXISTS (SELECT 1 FROM retry_claims k WHERE k.record_id = records.id)";

/// Holds a transform live for the rest of the transaction (the version
/// store's lock, which its deletion takes too); `false` when it was deleted.
/// A transform the version store has never seen (a store used without one)
/// is not held.
pub(super) fn live_transform(tx: &mut Tx<'_>, id: &str) -> Result<bool> {
    match tx.transform(id)? {
        None => Ok(true),
        Some(t) if t.deleted => Ok(false),
        Some(_) => tx.lock_live_transform(id),
    }
}

/// What a reprocess did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reprocess {
    Done,
    NotFound,
    /// A write of the record is unresolved, or a retry runs it (`409 record_in_flight`).
    InFlight,
}

/// The part of a diagnostics `location` kept with a failure: its line and
/// column, nothing else the program reported.
fn stored_location(location: &Value) -> Option<String> {
    let kept: Map<String, Value> = ["line", "column"]
        .into_iter()
        .filter_map(|k| {
            location
                .get(k)
                .and_then(Value::as_i64)
                .map(|n| (k.to_string(), json!(n)))
        })
        .collect();
    (!kept.is_empty()).then(|| Value::Object(kept).to_string())
}

/// Fails one record inside a transaction: its group, its fields, its events.
/// With `run`, only that attempt of it (a newer one is left alone); `false`
/// when nothing was failed.
fn fail_in(
    c: &mut Conn,
    id: i64,
    run: Option<i64>,
    failure: &Failure,
    response: Option<String>,
) -> Result<bool> {
    let Some(row) = c
        .query(
            "SELECT r.file_id, COALESCE(r.exec_transform_id, f.transform_id),
                    CASE WHEN r.exec_transform_id IS NULL THEN f.transform_version ELSE r.exec_version END,
                    r.uncertain, r.status, r.run
             FROM records r JOIN files f ON f.id = r.file_id WHERE r.id = $1",
            &params![id],
        )?
        .into_iter()
        .next()
    else {
        return Ok(false);
    };
    if run.is_some_and(|run| run != row.i64(5)) || matches!(row.string(4).as_str(), "sent") {
        return Ok(false);
    }
    let filtered = failure.kind == "filtered";
    let group = if filtered {
        None
    } else {
        let transform = row.opt_string(1).zip(row.opt_i64(2));
        Some(group_for(
            c,
            row.i64(0),
            failure,
            transform.as_ref().map(|(t, v)| (t.as_str(), *v)),
        )?)
    };
    let now = now_ms();
    c.execute(
        "UPDATE records SET status = 'rejected', error_kind = $2, error = $3, response = $4, group_id = $5,
                failed_at = CASE WHEN CAST($6 AS BIGINT) = 1 THEN failed_at ELSE $7 END,
                uncertain = 0, waiting = NULL, dispatch = NULL, updated_at = $7
         WHERE id = $1 AND run = $8",
        &params![id, failure.kind, failure.reason, response, group, filtered, now, row.i64(5)],
    )?;
    if row.i64(3) == 1 {
        append_events(c, &[id], "settled", None)?;
    }
    append_events(c, &[id], "failed", Some(failure))?;
    Ok(true)
}

impl Db {
    // ---- files -------------------------------------------------------------

    /// Whether a file of this name and size was already taken (not as a duplicate):
    /// what lets a read-only source be skipped without downloading it again.
    pub fn has_file(&self, feed: &str, name: &str, size: u64) -> Result<bool> {
        Ok(self
            .query_one(
                "SELECT 1 FROM files WHERE feed_id = $1 AND name = $2 AND size = $3 AND status NOT IN ('duplicate', 'unpacking') LIMIT 1",
                &params![feed, name, size],
            )?
            .is_some())
    }

    pub fn find_file_by_hash(&self, feed: &str, sha: &str) -> Result<Option<i64>> {
        Ok(self
            .query_one(
                "SELECT id FROM files WHERE feed_id = $1 AND sha256 = $2 AND status NOT IN ('duplicate', 'unpacking') ORDER BY id LIMIT 1",
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
        // The version is fixed in the same transaction that holds the
        // transform live, so its deletion cannot slip between them.
        self.transaction(|tx: &mut Tx<'_>| -> Result<i64> {
            if let Some(v) = version
                && !live_transform(tx, &v.transform_id)?
            {
                anyhow::bail!(
                    "transform {} was deleted; the load waits for a version that can run",
                    v.transform_id
                );
            }
            let now = now_ms();
            if let Some(queued) = tx
                .query_json(
                    "SELECT id FROM files WHERE feed_id = $1 AND status = 'queued' ORDER BY id LIMIT 1",
                    &params![feed],
                )?
                .first()
                .and_then(|r| r["id"].as_i64())
            {
                let adopted = tx.execute(
                    "UPDATE files SET name = $2, sha256 = $3, size = $4, status = 'processing', received_at = $5,
                            source_key = $6, transform_id = $7, transform_version = $8, environment = $9, admitted = 0
                     WHERE id = $1 AND status = 'queued'",
                    &params![queued, name, sha, size, now, source_key, transform_id, transform_version, environment],
                )?;
                if adopted == 1 {
                    return Ok(queued);
                }
            }
            tx.insert_returning_id(
                "INSERT INTO files (feed_id, name, sha256, size, status, received_at, source_key, transform_id, transform_version, environment, admitted)
                 VALUES ($1, $2, $3, $4, 'processing', $5, $6, $7, $8, $9, 0)",
                &params![feed, name, sha, size, now, source_key, transform_id, transform_version, environment],
            )
        })
    }

    /// Every load id of a source.
    pub fn source_load_ids(&self, feed: &str) -> Result<Vec<i64>> {
        Ok(self
            .query("SELECT id FROM files WHERE feed_id = $1", &params![feed])?
            .iter()
            .map(|r| r.i64(0))
            .collect())
    }

    /// A zip of the source still `unpacking` (a pass stopped half-way through it).
    pub fn unfinished_container(&self, feed: &str, name: &str, sha: &str) -> Result<Option<i64>> {
        Ok(self
            .query_one(
                "SELECT id FROM files WHERE feed_id = $1 AND name = $2 AND sha256 = $3 AND status = 'unpacking'
                 ORDER BY id LIMIT 1",
                &params![feed, name, sha],
            )?
            .map(|r| r.i64(0)))
    }

    /// The load an entry of `container` already has (made after it, by an
    /// expansion that stopped half-way), and whether its admission completed
    /// (its rows went in, or it was found unreadable).
    pub fn entry_load(
        &self,
        container: i64,
        feed: &str,
        name: &str,
        sha: &str,
    ) -> Result<Option<(i64, bool)>> {
        Ok(self
            .query_one(
                "SELECT f.id, f.admitted
                 FROM files f WHERE f.feed_id = $1 AND f.name = $2 AND f.sha256 = $3 AND f.id > $4
                   AND f.status NOT IN ('duplicate', 'unpacking')
                 ORDER BY f.id LIMIT 1",
                &params![feed, name, sha, container],
            )?
            .map(|r| (r.i64(0), r.i64(1) == 1)))
    }

    /// Every entry of a zip was admitted: it is `unpacked`, read once.
    pub fn finish_container(&self, container: i64) -> Result<()> {
        self.execute(
            "UPDATE files SET status = 'unpacked', finished_at = $2 WHERE id = $1 AND status = 'unpacking'",
            &params![container, now_ms()],
        )
        .map(drop)
    }

    /// The source of a load; `None` when there is no such load.
    pub fn load_source(&self, load_id: i64) -> Result<Option<String>> {
        Ok(self
            .query_one("SELECT feed_id FROM files WHERE id = $1", &params![load_id])?
            .map(|r| r.string(0)))
    }

    /// The failed rows of one failure group of a load, all or those named, in
    /// line order, at most 100, while their content is kept.
    pub fn failed_rows(
        &self,
        load_id: i64,
        group: &str,
        selection: Option<&[i64]>,
    ) -> std::result::Result<Vec<FailedRow>, FailedRowsError> {
        let Some(file) = self.query_one(
            "SELECT feed_id, name FROM files WHERE id = $1",
            &params![load_id],
        )?
        else {
            return Err(FailedRowsError::LoadNotFound);
        };
        let group = self
            .failure_group(load_id, group)?
            .ok_or(FailedRowsError::GroupNotFound)?;
        if group.count > 0 && group.content != "kept" {
            return Err(FailedRowsError::ContentExpired);
        }
        let (feed, name) = (file.string(0), file.string(1));
        let selected =
            selection.map(|ids| ids.iter().map(|id| Param::Int(*id)).collect::<Vec<_>>());
        if let Some(ids) = &selected
            && !ids.is_empty()
        {
            // Every record named must be one of the group's failing rows.
            let mut p: Vec<Param> = params![group.load_id, group.id].to_vec();
            p.extend(ids.iter().cloned());
            let found: Vec<i64> = self
                .query(
                    &format!(
                        "SELECT id FROM records WHERE file_id = $1 AND group_id = $2 AND status = 'rejected'
                           AND id IN ({})",
                        marks(3, ids.len())
                    ),
                    &p,
                )?
                .iter()
                .map(|r| r.i64(0))
                .collect();
            if let Some(missing) = selection
                .into_iter()
                .flatten()
                .find(|id| !found.contains(id))
            {
                return Err(FailedRowsError::RecordNotInGroup(*missing));
            }
        }
        let mut p: Vec<Param> = params![group.load_id, group.id].to_vec();
        let only = match &selected {
            Some(ids) if !ids.is_empty() => {
                p.extend(ids.iter().cloned());
                format!("AND id IN ({})", marks(3, ids.len()))
            }
            Some(_) => return Ok(Vec::new()),
            None => String::new(),
        };
        Ok(self
            .query(
                &format!(
                    "SELECT id, line, raw FROM records
                     WHERE file_id = $1 AND group_id = $2 AND status = 'rejected' AND content IS NULL {only}
                     ORDER BY line, id LIMIT 100"
                ),
                &p,
            )?
            .iter()
            .map(|r| {
                let (record_id, line) = (r.i64(0), r.i64(1));
                FailedRow {
                    record_id,
                    line,
                    raw: self
                        .open_record(Some(record_id), Column::Raw, r.get(2))
                        .unwrap_or(Value::Null),
                    context: json!({"feed": feed, "file": name, "line": line}),
                }
            })
            .collect())
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

    pub fn fail_file(&self, file_id: i64, error: &str) -> Result<()> {
        self.execute(
            "UPDATE files SET status = 'unreadable', error = $2, finished_at = $3, admitted = 1 WHERE id = $1",
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
             WHERE f.feed_id = $1 AND f.status IN ('processing', 'unpacked') AND f.admitted = 1
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

    // ---- load views ---------------------------------------------------------

    /// Loads of a source as their routes answer them, newest first (or oldest
    /// first with `ascending`), keyed by id: those below `below`, above
    /// `above`, or both, at most `limit`.
    pub fn load_views(
        &self,
        feed: &str,
        below: Option<i64>,
        above: Option<i64>,
        ascending: bool,
        limit: usize,
    ) -> Result<Vec<Value>> {
        self.loads_where(
            "f.feed_id = $1 AND (CAST($2 AS BIGINT) IS NULL OR f.id < CAST($2 AS BIGINT))
               AND (CAST($3 AS BIGINT) IS NULL OR f.id > CAST($3 AS BIGINT))",
            params![feed, below, above].to_vec(),
            if ascending { "ASC" } else { "DESC" },
            limit,
        )
    }

    /// One load as its route answers it.
    pub fn load_view(&self, load_id: i64) -> Result<Option<Value>> {
        Ok(self
            .loads_where("f.id = $1", params![load_id].to_vec(), "DESC", 1)?
            .into_iter()
            .next())
    }

    /// The id of the source's first load.
    pub fn first_load_id(&self, feed: &str) -> Result<Option<i64>> {
        Ok(self
            .query_one(
                "SELECT MIN(id) FROM files WHERE feed_id = $1 AND status NOT IN ('duplicate', 'unpacked', 'unpacking')",
                &params![feed],
            )?
            .and_then(|r| r.opt_i64(0)))
    }

    fn loads_where(
        &self,
        condition: &str,
        mut p: Vec<Param>,
        order: &str,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let n = p.len() + 1;
        p.push(Param::Int(limit.min(i64::MAX as usize) as i64));
        let count = |cond: &str| {
            format!("CAST(COALESCE(SUM(CASE WHEN {cond} THEN 1 ELSE 0 END), 0) AS BIGINT)")
        };
        let flag = |cond: &str| format!("MAX(CASE WHEN {cond} THEN 1 ELSE 0 END)");
        // The page of loads is chosen first, so their rows and history are
        // counted for those loads only, not for every load the page skips.
        let sql = format!(
            "SELECT f.id, f.feed_id, f.name, f.status, f.duplicate_of, f.size, f.error, f.source_key, f.archived,
                    COUNT(r.id),
                    {sent}, {rejected}, {in_progress_01},
                    CASE WHEN f.finished_at IS NULL THEN NULL ELSE f.finished_at - f.received_at END,
                    {received_at}, {finished_at},
                    f.transform_id, f.transform_version, f.environment,
                    {failed}, {filtered}, {unknown}, {received}, {pending},
                    CAST(COALESCE(SUM(CASE WHEN r.status = 'sent' THEN r.resource_count ELSE 0 END), 0) AS BIGINT),
                    {wait_validation}, {wait_environment}, {wait_destination},
                    (SELECT CAST(COUNT(DISTINCT e.record_id) AS BIGINT) FROM record_events e
                      WHERE e.load_id = f.id AND e.kind = 'failed' AND COALESCE(e.error_kind, '') <> 'filtered')
             FROM (SELECT f.id, f.feed_id, f.name, f.status, f.duplicate_of, f.size, f.error, f.source_key,
                          f.archived, f.finished_at, f.received_at, f.transform_id, f.transform_version, f.environment
                   FROM files f WHERE {condition} ORDER BY f.id {order} LIMIT ${n}) f
             LEFT JOIN records r ON r.file_id = f.id
             GROUP BY f.id, f.feed_id, f.name, f.status, f.duplicate_of, f.size, f.error, f.source_key, f.archived,
                      f.finished_at, f.received_at, f.transform_id, f.transform_version, f.environment
             ORDER BY f.id {order}",
            sent = count("r.status = 'sent'"),
            rejected = count("r.status = 'rejected'"),
            in_progress_01 = count("r.status IN ('received', 'pending', 'submitted')"),
            received_at = self.iso("f.received_at"),
            finished_at = self.iso("f.finished_at"),
            failed = count("r.status = 'rejected' AND r.error_kind <> 'filtered'"),
            filtered = count("r.status = 'rejected' AND r.error_kind = 'filtered'"),
            unknown = count("r.status IN ('pending', 'submitted') AND r.uncertain = 1"),
            received = count("r.status = 'received'"),
            pending = count("r.status IN ('pending', 'submitted') AND r.uncertain = 0"),
            wait_validation = flag(
                "r.status IN ('received', 'pending') AND r.waiting = 'validation_unavailable'"
            ),
            wait_environment = flag(
                "r.status IN ('received', 'pending') AND r.waiting = 'environment_unavailable'"
            ),
            wait_destination = flag(
                "r.status IN ('received', 'pending', 'submitted') AND (r.uncertain = 1 OR r.waiting = 'destination_unavailable')"
            ),
        );
        Ok(self
            .query(&sql, &p)?
            .iter()
            .map(|r| {
                let status = r.string(3);
                let read = r.i64(9);
                let (written, failed, filtered, unknown) = (r.i64(10), r.i64(19), r.i64(20), r.i64(21));
                let (received, pending) = (r.i64(22), r.i64(23));
                let processed = written + failed + filtered + unknown;
                let phase = match status.as_str() {
                    "queued" => "queued",
                    "processing" | "unpacked" if read == 0 => "reading",
                    "processing" | "unpacked" if received > 0 => "transforming",
                    "processing" | "unpacked" if pending > 0 => "delivering",
                    "processing" | "unpacked" if unknown > 0 => "settling",
                    "processing" | "unpacked" => "delivering",
                    _ => "done",
                };
                // The version store holds a load whose version's environment
                // is not the executor's (`environment_unavailable`).
                let held = status != "done"
                    && self
                        .version_waiting(r.i64(0), None)
                        .ok()
                        .flatten()
                        .is_some();
                let waiting = if r.i64(25) == 1 {
                    json!("validation_unavailable")
                } else if r.i64(26) == 1 || held {
                    json!("environment_unavailable")
                } else if r.i64(27) == 1 {
                    json!("destination_unavailable")
                } else {
                    Value::Null
                };
                let transform = match (r.opt_string(16), r.opt_i64(17)) {
                    (Some(id), Some(version)) => json!({"id": id, "version": version}),
                    _ => Value::Null,
                };
                json!({
                    "id": r.i64(0),
                    "source_id": r.string(1),
                    "name": r.string(2),
                    "status": status,
                    "duplicate_of": r.get(4),
                    "size": r.i64(5),
                    "error": r.get(6),
                    "source_key": r.get(7),
                    "archived": r.i64(8),
                    "records": read,
                    "sent": written,
                    "rejected": r.i64(11),
                    "in_progress": r.i64(12),
                    "took_ms": r.get(13),
                    "received_at": r.get(14),
                    "finished_at": r.get(15),
                    "transform": transform,
                    "environment": r.json(18),
                    "rows": {
                        "read": read, "written": written, "failed": failed, "failed_ever": r.i64(28),
                        "outcome_unknown": unknown, "in_progress": received + pending, "filtered": filtered,
                    },
                    "resources": {"written": r.i64(24)},
                    "progress": {
                        "phase": phase, "rows_admitted": read, "rows_processed": processed, "total_rows": null,
                    },
                    "waiting": waiting,
                })
            })
            .collect())
    }

    // ---- records -----------------------------------------------------------

    /// Stores a load's rows, each sealed for its own record (ids are taken
    /// first, in the same transaction) with its `admitted` event; rows the
    /// format could not parse fail at once (kind `parse`).
    pub fn insert_records(
        &self,
        file_id: i64,
        feed: &str,
        rows: &[(i64, Value, Option<String>)],
    ) -> Result<()> {
        if rows.is_empty() {
            return self
                .execute(
                    "UPDATE files SET admitted = 1 WHERE id = $1",
                    &params![file_id],
                )
                .map(drop);
        }
        self.atomically(|c| {
            let now = now_ms();
            let ids: Vec<i64> = match c {
                Conn::Postgres(_) => c
                    .query(
                        "SELECT nextval(pg_get_serial_sequence('records', 'id')) FROM generate_series(1, CAST($1 AS BIGINT))",
                        &params![rows.len()],
                    )?
                    .iter()
                    .map(|r| r.i64(0))
                    .collect(),
                // A writer from the transaction's start: nobody else inserts meanwhile.
                Conn::Sqlite(_) => {
                    let top = c
                        .query("SELECT COALESCE(MAX(id), 0) FROM records", &[])?
                        .first()
                        .map_or(0, |r| r.i64(0));
                    (1..=rows.len() as i64).map(|n| top + n).collect()
                }
            };
            // Rows go in by the hundred per statement: one round trip each
            // over a million rows is an hour; this is minutes.
            for (chunk, ids) in rows
                .chunks(INSERT_ROWS_PER_STATEMENT)
                .zip(ids.chunks(INSERT_ROWS_PER_STATEMENT))
            {
                let mut sql = String::from(
                    "INSERT INTO records (id, file_id, feed_id, line, raw, status, error_kind, error, updated_at) VALUES ",
                );
                let mut values: Vec<Param> = Vec::with_capacity(chunk.len() * 9);
                for (i, ((line, raw, parse_error), id)) in chunk.iter().zip(ids).enumerate() {
                    let (status, kind) = match parse_error {
                        Some(_) => ("rejected", Some("parse")),
                        None => ("received", None),
                    };
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("({})", marks(i * 9 + 1, 9)));
                    values.extend(params![
                        id,
                        file_id,
                        feed,
                        line,
                        self.seal_record(*id, Column::Raw, raw),
                        status,
                        kind,
                        parse_error.as_deref().map(bounded),
                        now
                    ]);
                }
                c.execute(&sql, &values)?;
            }
            let (first, last) = (ids[0], ids[ids.len() - 1]);
            c.execute(
                "INSERT INTO record_events (record_id, load_id, feed_id, kind, attempt, transform_id, transform_version, at)
                 SELECT r.id, r.file_id, r.feed_id, 'admitted', r.run, f.transform_id, f.transform_version, $4
                 FROM records r JOIN files f ON f.id = r.file_id
                 WHERE r.file_id = $1 AND r.id >= $2 AND r.id <= $3 ORDER BY r.id",
                &params![file_id, first, last, now],
            )?;
            let unparsed = c.query(
                "SELECT id, error FROM records WHERE file_id = $1 AND id >= $2 AND id <= $3 AND status = 'rejected' ORDER BY id",
                &params![file_id, first, last],
            )?;
            for r in unparsed {
                let reason = r.string(1);
                let failure = Failure {
                    kind: "parse",
                    reason: &reason,
                    location: None,
                };
                fail_in(c, r.i64(0), None, &failure, None)?;
            }
            // With the rows, never before: an admission that rolled back
            // leaves the load unadmitted, to be admitted again.
            c.execute("UPDATE files SET admitted = 1 WHERE id = $1", &params![file_id])?;
            Ok(())
        })
    }

    fn work_items(&self, sql: &str, p: &[Param]) -> Result<Vec<WorkItem>> {
        Ok(self
            .query(sql, p)?
            .iter()
            .map(|r| {
                let id = r.i64(0);
                WorkItem {
                    id,
                    file_id: r.i64(1),
                    line: r.i64(2),
                    file_name: r.string(3),
                    raw: self
                        .open_record(Some(id), Column::Raw, r.get(4))
                        .unwrap_or(Value::Null),
                    resources: self.open_record(Some(id), Column::Resources, r.get(5)),
                    attempts: r.i64(6) as u32,
                    retry: r.opt_string(7),
                    version: match (r.opt_string(8), r.opt_i64(9)) {
                        (Some(transform_id), Some(version)) => Some(VersionKey {
                            transform_id,
                            version,
                        }),
                        _ => None,
                    },
                    uncertain: r.i64(10) == 1,
                    dispatch: r.opt_string(11),
                    validated: r.i64(12) == 1,
                    run: r.i64(13),
                }
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

    /// Pending records due for delivery, but those of `except_load`; only those
    /// a retry runs when `retries_only`.
    pub fn due_records(
        &self,
        feed: &str,
        limit: usize,
        except_load: Option<i64>,
        retries_only: bool,
    ) -> Result<Vec<WorkItem>> {
        self.work_items(
            &format!(
                "SELECT {WORK_COLUMNS}
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.feed_id = $1 AND r.status = 'pending' AND r.next_attempt_at <= $2
               AND (CAST($4 AS BIGINT) IS NULL OR r.file_id <> CAST($4 AS BIGINT))
               AND (CAST($5 AS BIGINT) = 0 OR r.exec_retry IS NOT NULL)
             ORDER BY r.id LIMIT $3"
            ),
            &params![feed, now_ms(), limit, except_load, retries_only],
        )
    }

    /// Pending records of a feed (of `only_load`, when given) that no
    /// submission holds yet and whose next attempt is due, with their
    /// resources: what the engine writes out itself on the transformed bulk path.
    pub fn submittable_items(
        &self,
        feed: &str,
        limit: usize,
        only_load: Option<i64>,
    ) -> Result<Vec<WorkItem>> {
        self.work_items(
            &format!(
                "SELECT {WORK_COLUMNS} FROM records r JOIN files f ON f.id = r.file_id
                 WHERE r.feed_id = $1 AND r.status = 'pending' AND r.submission IS NULL AND r.next_attempt_at <= $2
                   AND (CAST($4 AS BIGINT) IS NULL OR r.file_id = CAST($4 AS BIGINT))
                 ORDER BY r.id LIMIT $3"
            ),
            &params![feed, now_ms(), limit, only_load],
        )
    }

    /// The pending records of one stored transaction, in id order.
    pub fn dispatch_records(&self, dispatch: &str) -> Result<Vec<WorkItem>> {
        self.work_items(
            &format!(
                "SELECT {WORK_COLUMNS} FROM records r JOIN files f ON f.id = r.file_id
                 WHERE r.dispatch = $1 AND r.status = 'pending' ORDER BY r.id"
            ),
            &params![dispatch],
        )
    }

    /// When the feed's next delivery attempt is due (epoch ms): the earliest
    /// deferred record or open submission; `None` when nothing waits.
    pub fn next_attempt_at(&self, feed: &str) -> Result<Option<i64>> {
        let records = self
            .query_one(
                "SELECT MIN(next_attempt_at) FROM records WHERE feed_id = $1 AND status = 'pending' AND next_attempt_at > 0",
                &params![feed],
            )?
            .and_then(|r| r.opt_i64(0));
        let submissions = self
            .query_one(
                "SELECT MIN(next_attempt_at) FROM submissions WHERE feed_id = $1 AND status IN ('kickoff', 'running') AND next_attempt_at > 0",
                &params![feed],
            )?
            .and_then(|r| r.opt_i64(0));
        Ok(match (records, submissions) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        })
    }

    /// Marks records whose source row is the resource to deliver as checked:
    /// each one's resources are its row.
    pub fn mark_passthrough(&self, items: &[&WorkItem]) -> Result<()> {
        let now = now_ms();
        for chunk in items.chunks(INSERT_ROWS_PER_STATEMENT) {
            let mut values: Vec<Param> = vec![Param::Int(now)];
            let mut tuples = Vec::with_capacity(chunk.len());
            for (i, item) in chunk.iter().enumerate() {
                let resources = Value::Array(vec![item.raw.clone()]);
                let (count, types) = resource_types(&resources);
                tuples.push(format!(
                    "(CAST(${} AS BIGINT), CAST(${} AS BIGINT), CAST(${} AS TEXT), CAST(${} AS BIGINT), CAST(${} AS TEXT))",
                    i * 5 + 2,
                    i * 5 + 3,
                    i * 5 + 4,
                    i * 5 + 5,
                    i * 5 + 6
                ));
                values.extend(params![
                    item.id,
                    item.run,
                    self.seal_record(item.id, Column::Resources, &resources),
                    count,
                    types
                ]);
            }
            if tuples.is_empty() {
                continue;
            }
            self.execute(
                &format!(
                    "UPDATE records SET status = 'pending', resources = v.column3, resource_count = v.column4,
                            resource_types = v.column5, error_kind = NULL, error = NULL,
                            attempts = 0, next_attempt_at = 0, validated = 0, updated_at = $1
                     FROM (VALUES {}) AS v
                     WHERE records.id = v.column1 AND records.run = v.column2 AND records.status = 'received'",
                    tuples.join(", ")
                ),
                &values,
            )?;
        }
        Ok(())
    }

    /// The transform made `resources` of attempt `run` of the record: they
    /// wait for delivery. Dropped when a newer attempt replaced that one.
    pub fn mark_transformed(&self, id: i64, run: i64, resources: &Value) -> Result<()> {
        let sealed = self.seal_record(id, Column::Resources, resources);
        let (count, types) = resource_types(resources);
        self.atomically(|c| {
            let n = c.execute(
                "UPDATE records SET status = 'pending', resources = $2, resource_count = $3, resource_types = $4,
                        error_kind = NULL, error = NULL, waiting = NULL, validated = 0,
                        attempts = 0, next_attempt_at = 0, updated_at = $5
                 WHERE id = $1 AND run = $6 AND status = 'received'",
                &params![id, sealed, count, types, now_ms(), run],
            )?;
            if n == 1 {
                append_events(c, &[id], "transformed", None)?;
            }
            Ok(())
        })
    }

    /// Attempt `run` of the record failed: kind, reason (cut to
    /// [`REASON_MAX_CHARS`]) and, for a transform's failure, the diagnostics'
    /// location. It joins its load's failure group for that kind, reason and
    /// version (`filtered` rows join none). The destination's answer, when
    /// there is one, is kept sealed with the row.
    pub fn mark_failed(
        &self,
        id: i64,
        run: i64,
        kind: &str,
        reason: &str,
        response: Option<&Value>,
        location: Option<&Value>,
    ) -> Result<()> {
        let response = response.map(|r| self.seal_record(id, Column::Response, r));
        let reason = bounded(reason);
        let failure = Failure {
            kind,
            reason: &reason,
            location: location.and_then(stored_location),
        };
        self.atomically(|c| fail_in(c, id, Some(run), &failure, response).map(drop))
    }

    /// The resources of these attempts `(id, run)` passed the destination's `$validate`.
    pub fn mark_validated(&self, rows: &[(i64, i64)]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.atomically(|c| {
            for chunk in rows.chunks(INSERT_ROWS_PER_STATEMENT) {
                let mut done = Vec::new();
                for (id, run) in chunk {
                    if c.execute(
                        "UPDATE records SET validated = 1, waiting = NULL WHERE id = $1 AND run = $2 AND status = 'pending'",
                        &params![id, run],
                    )? == 1
                    {
                        done.push(*id);
                    }
                }
                append_events(c, &done, "validated", None)?;
            }
            Ok(())
        })
    }

    /// Marks attempts `(id, run, response)` written, in one statement per
    /// hundreds of records: `UPDATE ... FROM (VALUES ...)`, which both
    /// engines take with the columns named `column1`, `column2`, ... Each gets
    /// its `delivered` event (and `settled` when its outcome was unknown); a
    /// row that had failed before, written now by a retry or reprocess, loses
    /// all its content (counts and types stay).
    pub fn mark_sent_many(&self, items: &[(i64, i64, Value)]) -> Result<()> {
        let sealed: Vec<(i64, i64, String)> = items
            .iter()
            .map(|(id, run, response)| {
                (*id, *run, self.seal_record(*id, Column::Response, response))
            })
            .collect();
        self.atomically(|c| {
            let now = now_ms();
            for chunk in sealed.chunks(INSERT_ROWS_PER_STATEMENT) {
                let keys: Vec<Param> = chunk
                    .iter()
                    .flat_map(|(id, run, _)| [Param::Int(*id), Param::Int(*run)])
                    .collect();
                let pairs: Vec<String> = (0..chunk.len())
                    .map(|i| format!("(id = ${} AND run = ${})", i * 2 + 1, i * 2 + 2))
                    .collect();
                let settled: Vec<i64> = c
                    .query(
                        &format!(
                            "SELECT id FROM records WHERE uncertain = 1 AND status IN ('pending', 'submitted')
                               AND ({}) ORDER BY id",
                            pairs.join(" OR ")
                        ),
                        &keys,
                    )?
                    .iter()
                    .map(|r| r.i64(0))
                    .collect();
                append_events(c, &settled, "settled", None)?;
                let rows: Vec<String> = (0..chunk.len())
                    .map(|i| {
                        format!(
                            "(CAST(${} AS BIGINT), CAST(${} AS BIGINT), CAST(${} AS TEXT))",
                            i * 3 + 2,
                            i * 3 + 3,
                            i * 3 + 4
                        )
                    })
                    .collect();
                let mut values: Vec<Param> = Vec::with_capacity(chunk.len() * 3 + 1);
                values.push(Param::Int(now));
                for (id, run, response) in chunk {
                    values.extend(params![id, run, response]);
                }
                let written: Vec<i64> = c
                    .query(
                        &format!(
                            "UPDATE records SET status = 'sent', response = v.column3, error_kind = NULL, error = NULL,
                                    uncertain = 0, waiting = NULL, dispatch = NULL, updated_at = $1
                             FROM (VALUES {}) AS v
                             WHERE records.id = v.column1 AND records.run = v.column2
                               AND records.status IN ('pending', 'submitted')
                             RETURNING records.id",
                            rows.join(", ")
                        ),
                        &values,
                    )?
                    .iter()
                    .map(|r| r.i64(0))
                    .collect();
                append_events(c, &written, "delivered", None)?;
                if !written.is_empty() {
                    let values: Vec<Param> = written.iter().map(|id| Param::Int(*id)).collect();
                    c.execute(
                        &format!(
                            "UPDATE records SET raw = 'null', original_raw = NULL, resources = NULL, response = NULL,
                                    content = 'purged'
                             WHERE failed_at IS NOT NULL AND content IS NULL AND id IN ({})",
                            marks(1, written.len())
                        ),
                        &values,
                    )?;
                }
            }
            Ok(())
        })
    }

    /// Holds attempts after a delivery attempt that did not settle them, each
    /// `(id, run, attempts, next attempt at)`: they wait, and when the write was
    /// sent their outcome is unknown until a later attempt settles it.
    pub fn defer_many(&self, items: &[(i64, i64, u32, i64)], deferral: &Deferral) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let reason = bounded(deferral.reason);
        self.atomically(|c| {
            let now = now_ms();
            let mut newly = Vec::new();
            for (id, run, attempts, next) in items {
                let updated = c.query(
                    "UPDATE records SET attempts = $3, next_attempt_at = $4, error_kind = 'transient', error = $5,
                            waiting = $6, dispatch = COALESCE(CAST($7 AS TEXT), dispatch),
                            uncertain = CASE WHEN CAST($8 AS BIGINT) = 1 THEN 1 ELSE uncertain END,
                            updated_at = $9
                     WHERE id = $1 AND run = $2 AND status = 'pending'
                     RETURNING uncertain",
                    &params![
                        id,
                        run,
                        attempts,
                        next,
                        reason,
                        deferral.waiting.as_str(),
                        deferral.dispatch,
                        deferral.sent,
                        now
                    ],
                )?;
                if !updated.is_empty() {
                    newly.push(*id);
                }
            }
            if deferral.sent && !newly.is_empty() {
                // `outcome_unknown` once per attempt: when it first became unknown.
                let values: Vec<Param> = newly.iter().map(|id| Param::Int(*id)).collect();
                let first: Vec<i64> = c
                    .query(
                        &format!(
                            "SELECT r.id FROM records r WHERE r.id IN ({}) AND r.uncertain = 1
                               AND NOT EXISTS (SELECT 1 FROM record_events e WHERE e.record_id = r.id
                                                 AND e.attempt = r.run AND e.kind = 'outcome_unknown')
                             ORDER BY r.id",
                            marks(1, newly.len())
                        ),
                        &values,
                    )?
                    .iter()
                    .map(|r| r.i64(0))
                    .collect();
                append_events(c, &first, "outcome_unknown", None)?;
            }
            Ok(())
        })
    }

    /// Takes records out of their stored transaction, as new rows to send.
    pub fn leave_dispatch(&self, ids: &[i64]) -> Result<()> {
        for chunk in ids.chunks(INSERT_ROWS_PER_STATEMENT) {
            let values: Vec<Param> = chunk.iter().map(|id| Param::Int(*id)).collect();
            self.execute(
                &format!(
                    "UPDATE records SET dispatch = NULL, next_attempt_at = 0 WHERE id IN ({})",
                    marks(1, chunk.len())
                ),
                &values,
            )?;
        }
        Ok(())
    }

    /// Puts a record back at the start of the pipeline, optionally with corrected
    /// source data, as a new attempt of its load's version; appended to its
    /// history. Refused while a write of it is unresolved (a stored
    /// transaction, a submission, an unknown outcome) or a retry runs it.
    pub fn reprocess(&self, id: i64, raw: Option<&Value>) -> Result<Reprocess> {
        self.atomically(|c| {
            // Locks in the order retry acceptance takes them (the load, then
            // its records), so the two never wait on each other in a cycle.
            // The record is locked before its eligibility is read, and the
            // update checks it again: a stored transaction or a retry that
            // takes it meanwhile cannot be erased.
            c.execute(
                "UPDATE files SET status = status WHERE id = (SELECT file_id FROM records WHERE id = $1)",
                &params![id],
            )?;
            let lock = for_update(c);
            let Some(row) = c
                .query(
                    &format!("SELECT raw, original_raw FROM records WHERE id = $1{lock}"),
                    &params![id],
                )?
                .into_iter()
                .next()
            else {
                return Ok(Reprocess::NotFound);
            };
            if c.query(
                &format!("SELECT 1 FROM records WHERE id = $1 AND {NOT_IN_FLIGHT}"),
                &params![id],
            )?
            .is_empty()
            {
                return Ok(Reprocess::InFlight);
            }
            let now = now_ms();
            let updated = match raw {
                Some(raw) => {
                    // The row as received is kept once, sealed for its own column.
                    let original = match row.opt_string(1) {
                        Some(original) => Some(original),
                        None => self
                            .open_record(Some(id), Column::Raw, row.get(0))
                            .map(|v| self.seal_record(id, Column::OriginalRaw, &v)),
                    };
                    c.execute(
                        &format!("UPDATE records SET status = 'received', original_raw = $2, raw = $3,
                                content = NULL, resources = NULL, resource_count = NULL, resource_types = NULL,
                                error_kind = NULL, error = NULL, response = NULL, attempts = 0, next_attempt_at = 0,
                                run = run + 1, exec_retry = NULL, exec_transform_id = NULL, exec_version = NULL,
                                uncertain = 0, waiting = NULL, dispatch = NULL, validated = 0, submission = NULL,
                                updated_at = $4
                         WHERE id = $1 AND {NOT_IN_FLIGHT}"),
                        &params![id, original, self.seal_record(id, Column::Raw, raw), now],
                    )?
                }
                None => c.execute(
                    &format!("UPDATE records SET status = 'received', resources = NULL, resource_count = NULL, resource_types = NULL,
                            error_kind = NULL, error = NULL, response = NULL, attempts = 0, next_attempt_at = 0,
                            run = run + 1, exec_retry = NULL, exec_transform_id = NULL, exec_version = NULL,
                            uncertain = 0, waiting = NULL, dispatch = NULL, validated = 0, submission = NULL,
                            updated_at = $2
                     WHERE id = $1 AND {NOT_IN_FLIGHT}"),
                    &params![id, now],
                )?,
            };
            if updated == 0 {
                return Ok(Reprocess::InFlight);
            }
            append_events(c, &[id], "retried", None)?;
            c.execute(
                "UPDATE files SET status = 'processing', finished_at = NULL WHERE id = (SELECT file_id FROM records WHERE id = $1)",
                &params![id],
            )?;
            Ok(Reprocess::Done)
        })
    }

    // ---- failed content ------------------------------------------------------

    /// Removes the content of rows failing now that failed before `before_ms`
    /// (`content: expired`); answers how many.
    pub fn expire_failed_content(&self, before_ms: i64) -> Result<u64> {
        self.execute(
            "UPDATE records SET raw = 'null', original_raw = NULL, resources = NULL, response = NULL, content = 'expired'
             WHERE status = 'rejected' AND failed_at IS NOT NULL AND failed_at < $1 AND content IS NULL",
            &params![before_ms],
        )
    }

    /// Removes the failed content of a source's rows (`content: purged`).
    pub fn purge_source_content(&self, feed: &str) -> Result<u64> {
        self.execute(
            "UPDATE records SET raw = 'null', original_raw = NULL, resources = NULL, response = NULL, content = 'purged'
             WHERE feed_id = $1 AND failed_at IS NOT NULL AND content IS NULL",
            &params![feed],
        )
    }

    // ---- one record ------------------------------------------------------------

    /// One record as `GET /api/records/{id}` answers it, its content opened.
    pub fn record_view(&self, id: i64) -> Result<Option<Value>> {
        Ok(self
            .query_json(
                &format!(
                    "SELECT r.id, r.feed_id AS source_id, r.file_id, f.name AS file, f.sha256 AS file_sha256, r.line, r.status,
                            {OUTCOME_SQL} AS outcome, r.run AS attempt,
                            r.raw, r.original_raw, r.resources, r.response, r.error_kind, r.error, r.attempts,
                            {} AS received_at, {} AS updated_at, r.content AS content_state
                     FROM records r JOIN files f ON f.id = r.file_id WHERE r.id = $1",
                    self.iso("f.received_at"),
                    self.iso("r.updated_at")
                ),
                params![id],
            )?
            .into_iter()
            .next())
    }

    /// Records, newest first, filtered as `GET /api/records` is.
    pub fn record_list(
        &self,
        feed: Option<&str>,
        status: Option<&str>,
        file_id: Option<i64>,
        limit: i64,
    ) -> Result<Vec<Value>> {
        self.query_json(
            &format!(
                "SELECT r.id, r.feed_id AS source_id, r.file_id, f.name AS file, r.line, r.status,
                        {OUTCOME_SQL} AS outcome, r.run AS attempt, r.error_kind, r.error, r.attempts, {} AS updated_at
                 FROM records r JOIN files f ON f.id = r.file_id
                 WHERE (CAST($1 AS TEXT) IS NULL OR r.feed_id = CAST($1 AS TEXT)) AND (CAST($2 AS TEXT) IS NULL OR r.status = CAST($2 AS TEXT)) AND (CAST($3 AS BIGINT) IS NULL OR r.file_id = CAST($3 AS BIGINT))
                 ORDER BY r.id DESC LIMIT $4",
                self.iso("r.updated_at")
            ),
            params![feed, status, file_id, limit],
        )
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A load of `n` rows that all failed in one group.
    fn failed_load(db: &Db, n: i64) -> (i64, super::super::Group) {
        let load = db.open_load("src", "a.csv", "sha", 3, None, None).unwrap();
        let rows: Vec<(i64, Value, Option<String>)> =
            (1..=n).map(|l| (l, json!({"v": l}), None)).collect();
        db.insert_records(load, "src", &rows).unwrap();
        for item in db.received_records("src", 1000).unwrap() {
            db.mark_failed(
                item.id,
                item.run,
                "data",
                "bad",
                Some(&json!({"issue": 1})),
                None,
            )
            .unwrap();
        }
        let failures = db.load_failures(load).unwrap();
        let group = db
            .failure_group(load, failures[0]["group"].as_str().unwrap())
            .unwrap()
            .unwrap();
        (load, group)
    }

    #[test]
    fn a_row_written_after_failing_loses_every_content_column() {
        let db = Db::in_memory().unwrap();
        let (_, _) = failed_load(&db, 1);
        let id = db.query("SELECT id FROM records", &[]).unwrap()[0].i64(0);
        assert_eq!(
            db.reprocess(id, Some(&json!({"v": 9}))).unwrap(),
            Reprocess::Done
        );
        let item = &db.received_records("src", 10).unwrap()[0];
        db.mark_transformed(
            item.id,
            item.run,
            &json!([{"resourceType": "Patient", "id": "p"}]),
        )
        .unwrap();
        db.mark_sent_many(&[(item.id, item.run, json!([{"status": "201"}]))])
            .unwrap();
        let row = &db
            .query(
                "SELECT raw, original_raw, resources, response, content, resource_count, resource_types FROM records",
                &[],
            )
            .unwrap()[0];
        assert_eq!(
            (
                row.string(0),
                row.opt_string(1),
                row.opt_string(2),
                row.opt_string(3)
            ),
            ("null".to_string(), None, None, None)
        );
        assert_eq!(row.string(4), "purged");
        assert_eq!(
            (row.i64(5), row.string(6)),
            (1, r#"{"Patient":1}"#.to_string())
        );
    }

    #[test]
    fn a_record_in_flight_is_not_reprocessed_and_stale_results_are_dropped() {
        let db = Db::in_memory().unwrap();
        let load = db.open_load("src", "a.csv", "sha", 3, None, None).unwrap();
        db.insert_records(load, "src", &[(1, json!({}), None), (2, json!({}), None)])
            .unwrap();
        let items = db.received_records("src", 10).unwrap();
        for i in &items {
            db.mark_transformed(
                i.id,
                i.run,
                &json!([{"resourceType": "Patient", "id": "p"}]),
            )
            .unwrap();
        }
        // In a stored transaction: refused.
        let d = db
            .create_dispatch(
                "src",
                &json!({"bundle": {}}),
                &[(items[0].id, items[0].run)],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            db.reprocess(items[0].id, None).unwrap(),
            Reprocess::InFlight
        );
        // Outcome unknown: refused.
        db.defer_many(
            &[(items[1].id, items[1].run, 1, 0)],
            &Deferral {
                waiting: Waiting::Destination,
                reason: "lost",
                sent: true,
                dispatch: None,
            },
        )
        .unwrap();
        assert_eq!(
            db.reprocess(items[1].id, None).unwrap(),
            Reprocess::InFlight
        );
        db.leave_dispatch(&[items[0].id]).unwrap();
        db.drop_dispatch_if_settled(&d).unwrap();
        assert_eq!(db.reprocess(items[0].id, None).unwrap(), Reprocess::Done);
        // The delivery of the replaced attempt cannot settle the new one.
        db.mark_sent_many(&[(items[0].id, items[0].run, json!([]))])
            .unwrap();
        db.mark_failed(items[0].id, items[0].run, "server", "late", None, None)
            .unwrap();
        let status = db
            .query(
                "SELECT status, run FROM records WHERE id = $1",
                &params![items[0].id],
            )
            .unwrap();
        assert_eq!(
            (status[0].string(0), status[0].i64(1)),
            ("received".into(), 2)
        );
        assert_eq!(db.reprocess(9999, None).unwrap(), Reprocess::NotFound);
    }

    #[test]
    fn failed_rows_are_limited_in_the_store_and_checked_for_membership() {
        let db = Db::in_memory().unwrap();
        let (load, group) = failed_load(&db, 150);
        let rows = db.failed_rows(load, &group.id, None).unwrap();
        assert_eq!(rows.len(), 100);
        assert_eq!(rows[0].raw, json!({"v": 1}));
        assert_eq!(rows[0].context["line"], 1);
        let picked = db
            .failed_rows(load, &group.id, Some(&[rows[5].record_id]))
            .unwrap();
        assert_eq!(picked.len(), 1);
        assert!(matches!(
            db.failed_rows(load, &group.id, Some(&[rows[5].record_id, 999_999])),
            Err(FailedRowsError::RecordNotInGroup(999_999))
        ));
        assert!(matches!(
            db.failed_rows(load, "g_none", None),
            Err(FailedRowsError::GroupNotFound)
        ));
    }

    #[test]
    fn group_records_page_in_line_order_both_ways() {
        let db = Db::in_memory().unwrap();
        let (_, group) = failed_load(&db, 5);
        let lines = |rows: Vec<Value>| -> Vec<i64> {
            rows.iter().map(|r| r["line"].as_i64().unwrap()).collect()
        };
        assert_eq!(
            lines(db.group_records(&group, None, None, 2).unwrap()),
            [1, 2]
        );
        let second = db.group_records(&group, None, None, 2).unwrap()[1].clone();
        let key = (
            second["line"].as_i64().unwrap(),
            second["id"].as_i64().unwrap(),
        );
        assert_eq!(
            lines(db.group_records(&group, Some(key), None, 2).unwrap()),
            [3, 4]
        );
        let fourth = db.group_records(&group, Some(key), None, 2).unwrap()[1].clone();
        let key = (
            fourth["line"].as_i64().unwrap(),
            fourth["id"].as_i64().unwrap(),
        );
        assert_eq!(
            lines(db.group_records(&group, None, Some(key), 2).unwrap()),
            [2, 3]
        );
    }

    #[test]
    fn reasons_are_cut_and_responses_stay_sealed() {
        let db = Db::in_memory().unwrap();
        let load = db.open_load("src", "a.csv", "sha", 3, None, None).unwrap();
        db.insert_records(load, "src", &[(1, json!({}), None)])
            .unwrap();
        let item = &db.received_records("src", 1).unwrap()[0];
        let long = "x".repeat(2000);
        db.mark_failed(
            item.id,
            item.run,
            "server",
            &long,
            Some(&json!({"zq": "body"})),
            None,
        )
        .unwrap();
        let row = &db
            .query("SELECT error, response FROM records", &[])
            .unwrap()[0];
        assert_eq!(row.string(0).chars().count(), REASON_MAX_CHARS + 1);
        assert!(row.string(1).starts_with("c1.") && !row.string(1).contains("zq"));
        let reasons = db
            .query(
                "SELECT reason FROM record_events WHERE kind = 'failed'",
                &[],
            )
            .unwrap();
        assert_eq!(reasons[0].string(0).chars().count(), REASON_MAX_CHARS + 1);
        let groups = db.query("SELECT reason FROM failure_groups", &[]).unwrap();
        assert_eq!(groups[0].string(0).chars().count(), REASON_MAX_CHARS + 1);
    }

    /// A transaction kept and a reprocess of the same row at once, through two
    /// connections (on PostgreSQL with `HIE_TEST_DATABASE_URL`, two
    /// connections to one schema): one of them wins, never both, so no stored
    /// transaction keeps a row a reprocess replaced. The same holds for a retry
    /// accepted while its row is reprocessed.
    #[test]
    fn a_reprocess_never_erases_a_transaction_or_a_retry_that_took_the_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hie.db");
        let a = std::sync::Arc::new(Db::open(&path).unwrap());
        let b = std::sync::Arc::new(Db::open(&path).unwrap());
        let load = a.open_load("src", "a.csv", "sha", 3, None, None).unwrap();
        for n in 0..25 {
            a.insert_records(load, "src", &[(n, json!({"n": n}), None)])
                .unwrap();
            let item = a.received_records("src", 1).unwrap()[0].clone();
            a.mark_transformed(
                item.id,
                item.run,
                &json!([{"resourceType": "Patient", "id": "p"}]),
            )
            .unwrap();
            let (kept, reprocessed) = std::thread::scope(|s| {
                let keep = s.spawn(|| {
                    a.create_dispatch("src", &json!({"bundle": {}}), &[(item.id, item.run)])
                        .unwrap()
                });
                let redo = s.spawn(|| b.reprocess(item.id, None).unwrap());
                (keep.join().unwrap(), redo.join().unwrap())
            });
            let row = &a
                .query(
                    "SELECT dispatch, run, status FROM records WHERE id = $1",
                    &params![item.id],
                )
                .unwrap()[0];
            match (&kept, reprocessed) {
                (Some(d), Reprocess::InFlight) => {
                    assert_eq!(
                        (row.opt_string(0).as_deref(), row.i64(1)),
                        (Some(d.as_str()), item.run)
                    );
                }
                (None, Reprocess::Done) => {
                    assert_eq!((row.opt_string(0), row.i64(1)), (None, item.run + 1));
                }
                other => panic!("both or neither took the row: {other:?}"),
            }
            // Settle the row either way for the next round.
            a.execute(
                "UPDATE records SET status = 'sent', dispatch = NULL WHERE id = $1",
                &params![item.id],
            )
            .unwrap();
        }
        // A retry accepted while its row is reprocessed.
        for n in 100..115 {
            a.insert_records(load, "src", &[(n, json!({"n": n}), None)])
                .unwrap();
            let item = a.received_records("src", 1).unwrap()[0].clone();
            a.mark_failed(item.id, item.run, "data", &format!("bad {n}"), None, None)
                .unwrap();
            let group = a
                .load_failures(load)
                .unwrap()
                .into_iter()
                .find(|g| g["reason"] == format!("bad {n}"))
                .unwrap();
            let group = a
                .failure_group(load, group["group"].as_str().unwrap())
                .unwrap()
                .unwrap();
            let version = crate::versions::VersionRef {
                transform_id: "lab".into(),
                version: 1,
                code_sha256: "a".repeat(64),
                environment: crate::versions::Environment::local(),
            };
            let (retried, reprocessed) = std::thread::scope(|s| {
                let retry = s.spawn(|| a.create_retry(&group, &version));
                let redo = s.spawn(|| b.reprocess(item.id, None).unwrap());
                (retry.join().unwrap(), redo.join().unwrap())
            });
            match (&retried, reprocessed) {
                (Ok(_), Reprocess::InFlight)
                | (Err(super::super::RetryRefusal::NothingToRetry), Reprocess::Done) => {}
                other => panic!("both or neither took the row: {other:?}"),
            }
            a.execute(
                "UPDATE records SET status = 'sent' WHERE id = $1",
                &params![item.id],
            )
            .unwrap();
            a.execute("DELETE FROM retry_claims", &[]).unwrap();
            a.execute("UPDATE retries SET status = 'done'", &[])
                .unwrap();
        }
    }

    /// A transaction is kept only when every row joins it as it was read: a
    /// row a reprocess replaced since keeps the whole of it from being stored.
    #[test]
    fn a_transaction_is_kept_whole_or_not_at_all() {
        let db = Db::in_memory().unwrap();
        let load = db.open_load("src", "a.csv", "sha", 3, None, None).unwrap();
        db.insert_records(load, "src", &[(1, json!({}), None), (2, json!({}), None)])
            .unwrap();
        let items = db.received_records("src", 10).unwrap();
        for i in &items {
            db.mark_transformed(
                i.id,
                i.run,
                &json!([{"resourceType": "Patient", "id": "p"}]),
            )
            .unwrap();
        }
        assert_eq!(db.reprocess(items[1].id, None).unwrap(), Reprocess::Done);
        let members: Vec<(i64, i64)> = items.iter().map(|i| (i.id, i.run)).collect();
        assert_eq!(
            db.create_dispatch("src", &json!({"bundle": {}}), &members)
                .unwrap(),
            None
        );
        assert!(
            db.query("SELECT id FROM dispatches", &[])
                .unwrap()
                .is_empty()
        );
        assert!(
            db.query("SELECT id FROM records WHERE dispatch IS NOT NULL", &[])
                .unwrap()
                .is_empty()
        );
    }
}
