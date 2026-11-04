//! Loads: the files (objects) taken from sources and the records cut from them,
//! and the custody of the records' content.
//!
//! **Custody.** A record's content (its raw row, the corrected row's original,
//! the resources it became, the destination's answers) is sealed with the
//! engine's content key (AES-256-GCM, `crate::crypto::Keyring`) before it is
//! first written, so the store's files (SQLite pages and WAL included) never
//! hold it in plaintext. It is opened only in memory, to transform, deliver or
//! serve it. A store written before custody keeps its plaintext rows readable.
//! Failed content goes when its retention runs out, when its row is written by
//! a retry or reprocess, or when its source is deleted (`crate::purge`); the
//! row keeps `raw: null` and its `content` state (`expired` or `purged`).
//!
//! Every change of a record's fate appends to its history (`history.rs`).

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, RwLock};

use anyhow::Result;
use serde_json::{Map, Value, json};

use super::engine::Conn;
use super::history::{Failure, OUTCOME_SQL, append_events, group_for};
use super::{Db, Param, now_ms, params};
use crate::crypto::{Key, Keyring, OpenError};
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
    // Why the row is held without failing: `validation_unavailable`,
    // `environment_unavailable`, `destination_unavailable`.
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
    /// The executor runs another environment than the version's (track V).
    Environment,
    /// Writes are not confirmed; they are tried again.
    Destination,
}

impl Waiting {
    pub fn as_str(self) -> &'static str {
        match self {
            Waiting::Validation => "validation_unavailable",
            Waiting::Environment => "environment_unavailable",
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
    r.uncertain, r.dispatch, r.validated";

/// Rows per multi-row statement: well under both engines' parameter limits.
const INSERT_ROWS_PER_STATEMENT: usize = 200;

/// What every sealed record column is bound to.
const CONTENT_AD: &[u8] = b"hie:records:content";

/// The record columns that hold content, sealed.
const CONTENT_COLUMNS: [&str; 4] = ["raw", "original_raw", "resources", "response"];

// ---- custody ----------------------------------------------------------------------

/// The keys the store seals content with: the engine's (`content.key_file`,
/// `HIE_CONTENT_KEY`, or the key in the store's directory) once
/// [`Db::use_content_keys`] gives them, and until then a key of this process,
/// for stores opened without an engine (tests, tools).
#[derive(Default)]
pub(super) struct ContentKeys {
    engine: RwLock<Option<Arc<Keyring>>>,
}

fn process_keys() -> &'static Keyring {
    static KEYS: OnceLock<Keyring> = OnceLock::new();
    KEYS.get_or_init(|| Keyring::new(vec![Key::generate()]).expect("one key"))
}

impl ContentKeys {
    fn seal(&self, plaintext: &[u8]) -> String {
        match &*self.engine.read().expect("content keys") {
            Some(keys) => keys.seal(plaintext, CONTENT_AD),
            None => process_keys().seal(plaintext, CONTENT_AD),
        }
    }

    fn open(&self, sealed: &str) -> Result<Vec<u8>, OpenError> {
        if let Some(keys) = &*self.engine.read().expect("content keys") {
            match keys.open(sealed, CONTENT_AD) {
                Err(OpenError::UnknownKey(_)) => {}
                other => return other,
            }
        }
        process_keys().open(sealed, CONTENT_AD)
    }
}

/// Whether a stored value is sealed (`v1.<key id>.<base64url>`); plaintext
/// content is JSON, which never starts so.
fn is_sealed(text: &str) -> bool {
    text.starts_with("v1.")
}

impl Db {
    /// Seals record content with these keys from now on (the engine's content
    /// keys); content sealed before stays readable.
    pub fn use_content_keys(&self, keys: Arc<Keyring>) {
        *self.content.engine.write().expect("content keys") = Some(keys);
    }

    /// Content as it is stored: sealed JSON.
    pub(super) fn seal(&self, value: &Value) -> String {
        self.content.seal(value.to_string().as_bytes())
    }

    /// A stored content value opened: sealed or (from before custody) plain
    /// JSON; `None` for null or what cannot be opened.
    pub(super) fn unseal(&self, stored: &Value) -> Option<Value> {
        let text = stored.as_str()?;
        if is_sealed(text) {
            match self.content.open(text) {
                Ok(bytes) => serde_json::from_slice(&bytes).ok(),
                Err(e) => {
                    tracing::error!(target: "hie::store", "record content cannot be opened: {e}");
                    None
                }
            }
        } else {
            serde_json::from_str(text).ok()
        }
    }

    /// A row read by [`Db::query_json`] with its sealed content columns opened.
    pub(super) fn reveal(&self, mut row: Value) -> Value {
        if let Value::Object(members) = &mut row {
            for column in CONTENT_COLUMNS {
                if let Some(value) = members.get_mut(column)
                    && value.as_str().is_some_and(is_sealed)
                {
                    *value = self.unseal(value).unwrap_or(Value::Null);
                }
            }
        }
        row
    }

    /// Runs `f` as one transaction on the store's connection.
    pub(super) fn atomically<T>(&self, f: impl FnOnce(&mut Conn) -> Result<T>) -> Result<T> {
        self.with(|c| {
            c.batch("BEGIN")?;
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

/// Fails one record inside a transaction: its group, its fields, its events.
fn fail_in(c: &mut Conn, id: i64, failure: &Failure, response: Option<String>) -> Result<()> {
    let Some(row) = c
        .query(
            "SELECT r.file_id, COALESCE(r.exec_transform_id, f.transform_id),
                    CASE WHEN r.exec_transform_id IS NULL THEN f.transform_version ELSE r.exec_version END,
                    r.uncertain, r.status
             FROM records r JOIN files f ON f.id = r.file_id WHERE r.id = $1",
            &params![id],
        )?
        .into_iter()
        .next()
    else {
        return Ok(());
    };
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
         WHERE id = $1",
        &params![id, failure.kind, failure.reason, response, group, filtered, now],
    )?;
    if row.i64(3) == 1 {
        append_events(c, &[id], "settled", None)?;
    }
    append_events(c, &[id], "failed", Some(failure))
}

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
        let failing: Vec<i64> = self
            .group_records(&group, usize::MAX >> 1)?
            .iter()
            .filter_map(|r| r["id"].as_i64())
            .collect();
        if let Some(selection) = selection
            && let Some(missing) = selection.iter().find(|id| !failing.contains(id))
        {
            return Err(FailedRowsError::RecordNotInGroup(*missing));
        }
        let (feed, name) = (file.string(0), file.string(1));
        Ok(self
            .group_rows(&group, usize::MAX >> 1)?
            .into_iter()
            .filter(|(id, _, _)| selection.is_none_or(|s| s.contains(id)))
            .take(100)
            .map(|(record_id, line, raw)| FailedRow {
                record_id,
                line,
                raw,
                context: json!({"feed": feed, "file": name, "line": line}),
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
                "SELECT MIN(id) FROM files WHERE feed_id = $1 AND status NOT IN ('duplicate', 'unpacked')",
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
        let sql = format!(
            "SELECT f.id, f.feed_id, f.name, f.status, f.duplicate_of, f.size, f.error, f.source_key, f.archived,
                    COUNT(r.id),
                    {sent}, {rejected}, {in_progress_01},
                    CASE WHEN f.finished_at IS NULL THEN NULL ELSE f.finished_at - f.received_at END,
                    {received_at}, {finished_at},
                    f.transform_id, f.transform_version, f.environment,
                    {failed}, {filtered}, {unknown}, {received}, {pending},
                    CAST(COALESCE(SUM(CASE WHEN r.status = 'sent' THEN r.resource_count ELSE 0 END), 0) AS BIGINT),
                    {wait_validation}, {wait_environment}, {wait_destination}
             FROM files f LEFT JOIN records r ON r.file_id = f.id
             WHERE {condition}
             GROUP BY f.id, f.feed_id, f.name, f.status, f.duplicate_of, f.size, f.error, f.source_key, f.archived,
                      f.finished_at, f.received_at, f.transform_id, f.transform_version, f.environment
             ORDER BY f.id {order} LIMIT ${n}",
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
                let waiting = if r.i64(25) == 1 {
                    json!("validation_unavailable")
                } else if r.i64(26) == 1 {
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
                        "read": read, "written": written, "failed": failed,
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

    /// Stores a load's rows, sealed, each with its `admitted` event; rows the
    /// format could not parse fail at once (kind `parse`).
    pub fn insert_records(
        &self,
        file_id: i64,
        feed: &str,
        rows: &[(i64, Value, Option<String>)],
    ) -> Result<()> {
        let sealed: Vec<String> = rows.iter().map(|(_, raw, _)| self.seal(raw)).collect();
        self.atomically(|c| {
            let now = now_ms();
            let before = c
                .query("SELECT COALESCE(MAX(id), 0) FROM records", &[])?
                .first()
                .map_or(0, |r| r.i64(0));
            // Rows go in by the hundred per statement: one round trip each
            // over a million rows is an hour; this is minutes.
            for (chunk, raws) in rows
                .chunks(INSERT_ROWS_PER_STATEMENT)
                .zip(sealed.chunks(INSERT_ROWS_PER_STATEMENT))
            {
                let mut sql = String::from(
                    "INSERT INTO records (file_id, feed_id, line, raw, status, error_kind, error, updated_at) VALUES ",
                );
                let mut values: Vec<Param> = Vec::with_capacity(chunk.len() * 8);
                for (i, ((line, _, parse_error), raw)) in chunk.iter().zip(raws).enumerate() {
                    let (status, kind) = match parse_error {
                        Some(_) => ("rejected", Some("parse")),
                        None => ("received", None),
                    };
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("({})", marks(i * 8 + 1, 8)));
                    values.extend(params![file_id, feed, line, raw, status, kind, parse_error, now]);
                }
                c.execute(&sql, &values)?;
            }
            c.execute(
                "INSERT INTO record_events (record_id, load_id, feed_id, kind, attempt, transform_id, transform_version, at)
                 SELECT r.id, r.file_id, r.feed_id, 'admitted', r.run, f.transform_id, f.transform_version, $3
                 FROM records r JOIN files f ON f.id = r.file_id
                 WHERE r.file_id = $1 AND r.id > $2 ORDER BY r.id",
                &params![file_id, before, now],
            )?;
            let unparsed = c.query(
                "SELECT id, error FROM records WHERE file_id = $1 AND id > $2 AND status = 'rejected' ORDER BY id",
                &params![file_id, before],
            )?;
            for r in unparsed {
                let reason = r.string(1);
                let failure = Failure {
                    kind: "parse",
                    reason: &reason,
                    location: None,
                };
                fail_in(c, r.i64(0), &failure, None)?;
            }
            Ok(())
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
                raw: self.unseal(r.get(4)).unwrap_or(Value::Null),
                resources: self.unseal(r.get(5)),
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

    /// Pending records due for delivery, but those of `except_load`.
    pub fn due_records(
        &self,
        feed: &str,
        limit: usize,
        except_load: Option<i64>,
    ) -> Result<Vec<WorkItem>> {
        self.work_items(
            &format!(
                "SELECT {WORK_COLUMNS}
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.feed_id = $1 AND r.status = 'pending' AND r.next_attempt_at <= $2
               AND (CAST($4 AS BIGINT) IS NULL OR r.file_id <> CAST($4 AS BIGINT))
             ORDER BY r.id LIMIT $3"
            ),
            &params![feed, now_ms(), limit, except_load],
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

    /// Marks records whose source row is the resource to deliver as transformed:
    /// each one's resources are its row.
    pub fn mark_passthrough(&self, ids: &[i64]) -> Result<()> {
        let now = now_ms();
        for chunk in ids.chunks(INSERT_ROWS_PER_STATEMENT) {
            let mut values: Vec<Param> = chunk.iter().map(|id| Param::Int(*id)).collect();
            let rows = self.query(
                &format!(
                    "SELECT id, raw FROM records WHERE id IN ({})",
                    marks(1, chunk.len())
                ),
                &values,
            )?;
            values.clear();
            values.push(Param::Int(now));
            let mut tuples = Vec::with_capacity(rows.len());
            for (i, r) in rows.iter().enumerate() {
                let resources = Value::Array(vec![self.unseal(r.get(1)).unwrap_or(Value::Null)]);
                let (count, types) = resource_types(&resources);
                tuples.push(format!(
                    "(CAST(${} AS BIGINT), CAST(${} AS TEXT), CAST(${} AS BIGINT), CAST(${} AS TEXT))",
                    i * 4 + 2,
                    i * 4 + 3,
                    i * 4 + 4,
                    i * 4 + 5
                ));
                values.extend(params![r.i64(0), self.seal(&resources), count, types]);
            }
            if tuples.is_empty() {
                continue;
            }
            self.execute(
                &format!(
                    "UPDATE records SET status = 'pending', resources = v.column2, resource_count = v.column3,
                            resource_types = v.column4, error_kind = NULL, error = NULL,
                            attempts = 0, next_attempt_at = 0, validated = 0, updated_at = $1
                     FROM (VALUES {}) AS v WHERE records.id = v.column1",
                    tuples.join(", ")
                ),
                &values,
            )?;
        }
        Ok(())
    }

    /// The transform made `resources` of the record: they wait for delivery.
    pub fn mark_transformed(&self, id: i64, resources: &Value) -> Result<()> {
        let sealed = self.seal(resources);
        let (count, types) = resource_types(resources);
        self.atomically(|c| {
            c.execute(
                "UPDATE records SET status = 'pending', resources = $2, resource_count = $3, resource_types = $4,
                        error_kind = NULL, error = NULL, waiting = NULL, validated = 0,
                        attempts = 0, next_attempt_at = 0, updated_at = $5 WHERE id = $1",
                &params![id, sealed, count, types, now_ms()],
            )?;
            append_events(c, &[id], "transformed", None)
        })
    }

    /// The record failed: kind, reason and, for a transform's failure, the
    /// diagnostics' location. It joins its load's failure group for that kind,
    /// reason and version (`filtered` rows join none).
    pub fn mark_failed(
        &self,
        id: i64,
        kind: &str,
        reason: &str,
        response: Option<&Value>,
        location: Option<&Value>,
    ) -> Result<()> {
        let response = response.map(|r| self.seal(r));
        let failure = Failure {
            kind,
            reason,
            location: location.filter(|l| !l.is_null()).map(Value::to_string),
        };
        self.atomically(|c| fail_in(c, id, &failure, response))
    }

    pub fn mark_rejected(
        &self,
        id: i64,
        kind: &str,
        error: &str,
        response: Option<&Value>,
    ) -> Result<()> {
        self.mark_failed(id, kind, error, response, None)
    }

    /// The resources of these records passed the destination's `$validate`.
    pub fn mark_validated(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        self.atomically(|c| {
            for chunk in ids.chunks(INSERT_ROWS_PER_STATEMENT) {
                let values: Vec<Param> = chunk.iter().map(|id| Param::Int(*id)).collect();
                c.execute(
                    &format!(
                        "UPDATE records SET validated = 1, waiting = NULL WHERE id IN ({})",
                        marks(1, chunk.len())
                    ),
                    &values,
                )?;
            }
            append_events(c, ids, "validated", None)
        })
    }

    /// Marks many records sent, each with its own response, in one statement per
    /// hundreds of records: `UPDATE ... FROM (VALUES ...)`, which both engines
    /// take with the columns named `column1` and `column2`. Each gets its
    /// `delivered` event (and `settled` when its outcome was unknown); a row
    /// that had failed before, written now by a retry or reprocess, loses its
    /// failed content.
    pub fn mark_sent_many(&self, items: &[(i64, Value)]) -> Result<()> {
        let sealed: Vec<(i64, String)> = items
            .iter()
            .map(|(id, response)| (*id, self.seal(response)))
            .collect();
        self.atomically(|c| {
            let now = now_ms();
            for chunk in sealed.chunks(INSERT_ROWS_PER_STATEMENT) {
                let ids: Vec<Param> = chunk.iter().map(|(id, _)| Param::Int(*id)).collect();
                let settled: Vec<i64> = c
                    .query(
                        &format!(
                            "SELECT id FROM records WHERE uncertain = 1 AND id IN ({}) ORDER BY id",
                            marks(1, chunk.len())
                        ),
                        &ids,
                    )?
                    .iter()
                    .map(|r| r.i64(0))
                    .collect();
                append_events(c, &settled, "settled", None)?;
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
                    values.push(Param::Text(response.clone()));
                }
                c.execute(
                    &format!(
                        "UPDATE records SET status = 'sent', response = v.column2, error_kind = NULL, error = NULL,
                                uncertain = 0, waiting = NULL, dispatch = NULL, updated_at = $1
                         FROM (VALUES {}) AS v WHERE records.id = v.column1",
                        rows.join(", ")
                    ),
                    &values,
                )?;
                let ids: Vec<i64> = chunk.iter().map(|(id, _)| *id).collect();
                append_events(c, &ids, "delivered", None)?;
                let values: Vec<Param> = ids.iter().map(|id| Param::Int(*id)).collect();
                c.execute(
                    &format!(
                        "UPDATE records SET raw = 'null', original_raw = NULL, content = 'purged'
                         WHERE failed_at IS NOT NULL AND content IS NULL AND id IN ({})",
                        marks(1, ids.len())
                    ),
                    &values,
                )?;
            }
            Ok(())
        })
    }

    pub fn mark_sent(&self, id: i64, response: &Value) -> Result<()> {
        self.mark_sent_many(&[(id, response.clone())])
    }

    /// Holds records after a delivery attempt that did not settle them, each
    /// `(id, attempts, next attempt at)`: they wait, and when the write was
    /// sent their outcome is unknown until a later attempt settles it.
    pub fn defer_many(&self, items: &[(i64, u32, i64)], deferral: &Deferral) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        self.atomically(|c| {
            let now = now_ms();
            if deferral.sent {
                let values: Vec<Param> = items.iter().map(|(id, _, _)| Param::Int(*id)).collect();
                let newly: Vec<i64> = c
                    .query(
                        &format!(
                            "SELECT id FROM records WHERE uncertain = 0 AND id IN ({}) ORDER BY id",
                            marks(1, items.len())
                        ),
                        &values,
                    )?
                    .iter()
                    .map(|r| r.i64(0))
                    .collect();
                for chunk in newly.chunks(INSERT_ROWS_PER_STATEMENT) {
                    let values: Vec<Param> = chunk.iter().map(|id| Param::Int(*id)).collect();
                    c.execute(
                        &format!(
                            "UPDATE records SET uncertain = 1 WHERE id IN ({})",
                            marks(1, chunk.len())
                        ),
                        &values,
                    )?;
                }
                append_events(c, &newly, "outcome_unknown", None)?;
            }
            for (id, attempts, next) in items {
                c.execute(
                    "UPDATE records SET attempts = $2, next_attempt_at = $3, error_kind = 'transient', error = $4,
                            waiting = $5, dispatch = COALESCE(CAST($6 AS TEXT), dispatch), updated_at = $7
                     WHERE id = $1",
                    &params![
                        id,
                        attempts,
                        next,
                        deferral.reason,
                        deferral.waiting.as_str(),
                        deferral.dispatch,
                        now
                    ],
                )?;
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

    /// Says why records wait without failing (`None`: they no longer do); for
    /// a stage that holds them, such as a version whose environment is not the
    /// executor's.
    pub fn set_waiting(&self, ids: &[i64], waiting: Option<Waiting>) -> Result<()> {
        for chunk in ids.chunks(INSERT_ROWS_PER_STATEMENT) {
            let mut values: Vec<Param> =
                vec![super::IntoParam::to_param(&waiting.map(Waiting::as_str))];
            values.extend(chunk.iter().map(|id| Param::Int(*id)));
            self.execute(
                &format!(
                    "UPDATE records SET waiting = CAST($1 AS TEXT) WHERE id IN ({})",
                    marks(2, chunk.len())
                ),
                &values,
            )?;
        }
        Ok(())
    }

    /// Puts a record back at the start of the pipeline, optionally with corrected
    /// source data, as a new attempt of its load's version; appended to its history.
    pub fn reprocess(&self, id: i64, raw: Option<&Value>) -> Result<bool> {
        let sealed = raw.map(|r| self.seal(r));
        self.atomically(|c| {
            let now = now_ms();
            let n = match &sealed {
                Some(raw) => c.execute(
                    "UPDATE records SET status = 'received', original_raw = COALESCE(original_raw, raw), raw = $2,
                            content = NULL, resources = NULL, resource_count = NULL, resource_types = NULL,
                            error_kind = NULL, error = NULL, response = NULL, attempts = 0, next_attempt_at = 0,
                            run = run + 1, exec_retry = NULL, exec_transform_id = NULL, exec_version = NULL,
                            uncertain = 0, waiting = NULL, dispatch = NULL, validated = 0, submission = NULL,
                            updated_at = $3
                     WHERE id = $1",
                    &params![id, raw, now],
                )?,
                None => c.execute(
                    "UPDATE records SET status = 'received', resources = NULL, resource_count = NULL, resource_types = NULL,
                            error_kind = NULL, error = NULL, response = NULL, attempts = 0, next_attempt_at = 0,
                            run = run + 1, exec_retry = NULL, exec_transform_id = NULL, exec_version = NULL,
                            uncertain = 0, waiting = NULL, dispatch = NULL, validated = 0, submission = NULL,
                            updated_at = $2
                     WHERE id = $1",
                    &params![id, now],
                )?,
            };
            if n > 0 {
                append_events(c, &[id], "retried", None)?;
                c.execute(
                    "UPDATE files SET status = 'processing', finished_at = NULL WHERE id = (SELECT file_id FROM records WHERE id = $1)",
                    &params![id],
                )?;
            }
            Ok(n > 0)
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
                            {OUTCOME_SQL} AS outcome,
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
                        {OUTCOME_SQL} AS outcome, r.error_kind, r.error, r.attempts, {} AS updated_at
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
