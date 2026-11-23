//! The engine's store: every file, record, alert, rule, cursor and lease. One
//! SQL dialect written once, run on SQLite (a file, the default) or PostgreSQL
//! (`postgres://...`, for more than one instance or more than one machine).
//! Placeholders are `$1`, `$2`, ...; SQLite gets them as `?1`, `?2`.

#[cfg(feature = "secrets-native")]
pub mod secrets;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use postgres::types::{IsNull, ToSql, Type};
use rusqlite::types::ValueRef;
use serde_json::{Map, Value, json};

use crate::events::Event;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS files (
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
  archived INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS files_feed_sha ON files(feed_id, sha256);
CREATE INDEX IF NOT EXISTS files_feed_received ON files(feed_id, received_at);
CREATE INDEX IF NOT EXISTS files_feed_id ON files(feed_id, id);
CREATE TABLE IF NOT EXISTS records (
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
  submission TEXT
);
CREATE INDEX IF NOT EXISTS records_feed_status ON records(feed_id, status, next_attempt_at);
CREATE INDEX IF NOT EXISTS records_feed_status_id ON records(feed_id, status, id);
CREATE INDEX IF NOT EXISTS records_file ON records(file_id);
CREATE TABLE IF NOT EXISTS alerts (
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
CREATE TABLE IF NOT EXISTS source_state (
  feed_id TEXT PRIMARY KEY,
  paused INTEGER NOT NULL DEFAULT 0,
  paused_at INTEGER,
  paused_by TEXT
);
CREATE TABLE IF NOT EXISTS source_cursors (
  feed_id TEXT NOT NULL,
  key TEXT NOT NULL,
  cursor TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (feed_id, key)
);
CREATE TABLE IF NOT EXISTS source_leases (
  feed_id TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  until INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS submissions (
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
CREATE TABLE IF NOT EXISTS alert_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id TEXT,
  condition TEXT NOT NULL,
  threshold TEXT NOT NULL,
  notify TEXT NOT NULL DEFAULT '[]',
  channels TEXT NOT NULL DEFAULT '[\"app\"]',
  enabled INTEGER NOT NULL DEFAULT 1,
  created_by TEXT,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS store_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at INTEGER NOT NULL,
  source_id TEXT,
  kind TEXT NOT NULL,
  load_id INTEGER,
  record_id INTEGER,
  attempt INTEGER,
  error_kind TEXT,
  reason TEXT,
  detail TEXT
);
CREATE INDEX IF NOT EXISTS events_at ON events(at);
CREATE INDEX IF NOT EXISTS events_source ON events(source_id, id);
CREATE INDEX IF NOT EXISTS events_kind ON events(kind, id);
CREATE TABLE IF NOT EXISTS alert_counts_purged (
  feed_id TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL,
  n INTEGER NOT NULL,
  PRIMARY KEY (feed_id, kind)
);
CREATE TABLE IF NOT EXISTS alert_webhooks (
  id INTEGER PRIMARY KEY,
  event_id TEXT NOT NULL UNIQUE,
  alert_id INTEGER NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
  event TEXT NOT NULL,
  destination TEXT NOT NULL,
  payload TEXT NOT NULL,
  state TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  last_status INTEGER,
  delivered_at INTEGER,
  created_at INTEGER NOT NULL,
  UNIQUE (alert_id, event)
);
CREATE INDEX IF NOT EXISTS alert_webhooks_due ON alert_webhooks(state, destination, next_attempt_at);
CREATE INDEX IF NOT EXISTS alert_webhooks_alert ON alert_webhooks(alert_id, id);
CREATE TABLE IF NOT EXISTS checks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  feed_id TEXT NOT NULL,
  status TEXT NOT NULL,
  requested_at INTEGER NOT NULL,
  finished_at INTEGER,
  after_file INTEGER NOT NULL,
  upto_file INTEGER,
  file_count INTEGER,
  reason TEXT
);
CREATE INDEX IF NOT EXISTS checks_feed_status ON checks(feed_id, status);
CREATE INDEX IF NOT EXISTS checks_requested ON checks(requested_at);
";

/// Columns added after a store was first created; each is added when missing.
const ADDED_COLUMNS: &[(&str, &str, &str)] = &[
    ("files", "source_key", "TEXT"),
    ("files", "archived", "INTEGER NOT NULL DEFAULT 0"),
    ("files", "origin", "TEXT"),
    ("files", "source_name", "TEXT"),
    ("records", "submission", "TEXT"),
    // Bumped when a person puts a record back: an outcome computed for an older
    // revision changes nothing.
    ("records", "revision", "INTEGER NOT NULL DEFAULT 0"),
    // A submission's failed attempts and its settled rows, counted per page.
    ("submissions", "failures", "INTEGER NOT NULL DEFAULT 0"),
    ("submissions", "sent", "INTEGER NOT NULL DEFAULT 0"),
    ("submissions", "rejected", "INTEGER NOT NULL DEFAULT 0"),
    ("alerts", "rule_id", "INTEGER"),
    ("alerts", "cleared_at", "INTEGER"),
    ("alerts", "cleared_reason", "TEXT"),
    ("alerts", "notify", "TEXT"),
    ("alerts", "channels", "TEXT"),
];

/// Indexes made after the added columns, since they may name one.
const ADDED_INDEXES: [&str; 2] = [
    "CREATE INDEX IF NOT EXISTS records_submission_id ON records(submission, id)",
    "CREATE INDEX IF NOT EXISTS events_source_kind_at ON events(source_id, kind, at)",
];

/// The indexes the loads list chooses its pages through, each built in a
/// transaction of its own: a build over a large `records` locks that table
/// alone, and never holds `files` while it waits for another instance's
/// writes. The failing ones hold only the rows that make a load fail.
const LOAD_INDEXES: [&str; 4] = [
    "CREATE INDEX IF NOT EXISTS files_status_id ON files(status, id)",
    "CREATE INDEX IF NOT EXISTS files_feed_status_id ON files(feed_id, status, id)",
    "CREATE INDEX IF NOT EXISTS records_failing_file ON records(file_id) WHERE status = 'rejected' AND COALESCE(error_kind, '') <> 'filtered'",
    "CREATE INDEX IF NOT EXISTS records_failing_feed_file ON records(feed_id, file_id) WHERE status = 'rejected' AND COALESCE(error_kind, '') <> 'filtered'",
];

/// On PostgreSQL only: on SQLite `records_file` already orders by id, the rowid.
const ADDED_INDEXES_POSTGRES: [&str; 1] =
    ["CREATE INDEX IF NOT EXISTS records_file_id ON records(file_id, id)"];

/// `SCHEMA`'s statements, its tables apart from its indexes.
fn schema_statements() -> impl Iterator<Item = &'static str> {
    SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty())
}

fn is_index(statement: &str) -> bool {
    statement.starts_with("CREATE INDEX")
}

/// Every index of a PostgreSQL store, in the order they are built: `SCHEMA`'s,
/// then the ones added since. Each is built in a transaction of its own, after
/// the tables and their added columns: `CREATE INDEX IF NOT EXISTS` takes a
/// `SHARE` lock on its table even when the index exists, and a start holding
/// that lock on one table while it waits for another instance's write to a
/// second table, which that write holds while it waits for the first, would
/// deadlock with it.
fn postgres_indexes() -> Vec<&'static str> {
    let mut all: Vec<&'static str> = schema_statements().filter(|s| is_index(s)).collect();
    all.extend(ADDED_INDEXES);
    all.extend(LOAD_INDEXES);
    all.extend(ADDED_INDEXES_POSTGRES);
    all
}

/// When the event log began on this store: set once, at the first start of a
/// version that keeps it, and kept.
const EVENTS_SINCE: &str =
    "INSERT INTO store_meta (key, value) VALUES ('events_since', $1) ON CONFLICT (key) DO NOTHING";

/// A SQLite store made before rule ids came from `AUTOINCREMENT` would give a
/// deleted rule's id again: its `alert_rules` is rebuilt once, in a
/// transaction, with the same columns, rows and ids, and the sequence set past
/// every rule id in use, alerts' included.
fn never_reuse_rule_ids(conn: &rusqlite::Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| {
        let sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'alert_rules'",
            [],
            |r| r.get(0),
        )?;
        if sql.to_ascii_uppercase().contains("AUTOINCREMENT") {
            return Ok(());
        }
        let start = SCHEMA
            .find("CREATE TABLE IF NOT EXISTS alert_rules (")
            .context("alert_rules in the schema")?;
        let end = start + SCHEMA[start..].find(");").context("end of alert_rules")? + 2;
        let ddl = SCHEMA[start..end].replace(
            "CREATE TABLE IF NOT EXISTS alert_rules (",
            "CREATE TABLE alert_rules_rebuilt (",
        );
        let columns = "id, source_id, condition, threshold, notify, channels, enabled, created_by, created_at";
        conn.execute_batch(&format!(
            "{ddl}
             INSERT INTO alert_rules_rebuilt ({columns}) SELECT {columns} FROM alert_rules;
             DROP TABLE alert_rules;
             ALTER TABLE alert_rules_rebuilt RENAME TO alert_rules;
             DELETE FROM sqlite_sequence WHERE name IN ('alert_rules', 'alert_rules_rebuilt');
             INSERT INTO sqlite_sequence (name, seq) VALUES ('alert_rules', MAX(
               COALESCE((SELECT MAX(id) FROM alert_rules), 0),
               COALESCE((SELECT MAX(rule_id) FROM alerts), 0)));"
        ))?;
        Ok::<_, anyhow::Error>(())
    })();
    match result {
        Ok(()) => Ok(conn.execute_batch("COMMIT")?),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

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
    /// When the rule was made, as an instant; never taken from a request.
    #[serde(default, skip_deserializing)]
    pub created_at: Option<String>,
}

fn default_channels() -> Vec<String> {
    vec!["app".into(), "webhook".into()]
}
fn default_enabled() -> bool {
    true
}

/// Why a check failed. Only these fixed sentences are stored and answered: the
/// error behind one, which may carry a URL, a query or a server's answer, goes
/// to the engine log only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckFailure {
    Paused,
    CredentialNotLoaded,
    Listing,
    Fetching,
    Taking,
    Notifications,
    Deleted,
}

impl CheckFailure {
    pub fn reason(self) -> &'static str {
        match self {
            CheckFailure::Paused => "the source is paused",
            CheckFailure::CredentialNotLoaded => {
                "the source's credential is not loaded; see its credential alert"
            }
            CheckFailure::Listing => "listing the source failed; the engine log has the details",
            CheckFailure::Fetching => {
                "reading an object from the source failed; the engine log has the details"
            }
            CheckFailure::Taking => {
                "taking an object in (recording, unpacking or acknowledging it) failed; the engine log has the details"
            }
            CheckFailure::Notifications => {
                "acknowledging the source's notifications failed; the engine log has the details"
            }
            CheckFailure::Deleted => "the source was deleted",
        }
    }
}

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
    /// Where the kept file lives: see `Db::insert_file`.
    pub origin: Option<String>,
    /// The resource type of the file's first pending record (an export holds one type per file).
    pub resource_type: String,
    /// Pending records of the file that no submission holds.
    pub count: usize,
}

/// An open alert whose rule no longer watches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnwatchedAlert {
    pub id: i64,
    pub feed: Option<String>,
    pub kind: String,
    pub why: RuleGone,
}

/// Why a rule no longer watches an alert it raised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleGone {
    Deleted,
    TurnedOff,
    /// Changed in what it watches: its condition, threshold or source.
    Changed,
}

impl RuleGone {
    /// The alert's clearing reason: a fixed sentence.
    pub fn reason(self) -> &'static str {
        match self {
            RuleGone::Deleted => "its rule was deleted",
            RuleGone::TurnedOff => "its rule was turned off",
            RuleGone::Changed => "its rule was changed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkItem {
    pub id: i64,
    pub file_id: i64,
    pub line: i64,
    pub file_name: String,
    pub raw: Value,
    pub resources: Option<Value>,
    pub attempts: u32,
    /// The record's revision when it was selected: its outcome is stored only
    /// while the record is still at it.
    pub revision: i64,
}

/// A row as read from a file: its line, its data, and why it could not be
/// parsed, if it could not.
pub type ReadRow = (i64, Value, Option<String>);

/// A source's name as a load keeps it: a name PostgreSQL cannot hold as text
/// (with a NUL) is not kept, so that it never stops a load.
fn storable_name(name: &str) -> Option<&str> {
    Some(name).filter(|n| !n.contains('\0'))
}

/// A file as it is recorded on arrival: see `Db::insert_file` for `source_key`
/// and `origin`.
#[derive(Debug, Clone, Copy)]
pub struct NewFile<'a> {
    pub feed: &'a str,
    /// The source's name as the load is recorded: see `Db::insert_file`.
    pub source_name: &'a str,
    pub name: &'a str,
    pub sha: &'a str,
    pub size: u64,
    pub source_key: Option<&'a str>,
    pub origin: Option<&'a str>,
}

/// What `Db::admit_file` did with a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Recorded, with this id.
    Admitted(i64),
    /// Its content was already taken, into this file: nothing was written.
    AlreadyTaken(i64),
}

/// What became of one record of a batch sent to the FHIR server.
#[derive(Debug, Clone, Copy)]
pub enum Delivery<'a> {
    Sent(&'a Value),
    Rejected {
        kind: &'a str,
        error: &'a str,
        response: Option<&'a Value>,
    },
    /// Tried again from `next_attempt_at`, as attempt `attempts`.
    Deferred {
        attempts: u32,
        next_attempt_at: i64,
        error: &'a str,
    },
}

/// What `GET /api/events` asks for.
#[derive(Debug, Clone, Default)]
pub struct EventQuery {
    pub source: Option<String>,
    /// Exact kinds and families (`record`, `load`, `submission`); any of them.
    pub kinds: Vec<String>,
    /// Epoch milliseconds: events at or after it.
    pub since: Option<i64>,
    /// Events after this id.
    pub cursor: Option<i64>,
    pub limit: usize,
}

/// A page of events, oldest first, and where the next poll starts.
#[derive(Debug, Clone)]
pub struct EventPage {
    pub events: Vec<Value>,
    /// The id the next poll continues after; `None` when the log is empty and
    /// no cursor was given.
    pub next: Option<i64>,
    pub has_more: bool,
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

/// Who holds a source and until when.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Lease {
    pub owner: String,
    pub until: i64,
}

/// An alert as stored, with its times as the API formats them, for the webhook
/// event written with it.
#[derive(Debug, Clone)]
pub struct AlertRow {
    pub id: i64,
    pub feed_id: Option<String>,
    pub kind: String,
    pub message: String,
    pub rule_id: Option<i64>,
    pub notify: Vec<String>,
    /// The rule's channels when it fired; `None` for an alert without a rule.
    pub channels: Option<Vec<String>>,
    pub created_at: String,
    pub cleared_at: Option<String>,
    pub cleared_reason: Option<String>,
}

/// The webhook event written in the same transaction as an alert's change.
#[derive(Debug, Clone)]
pub struct NewWebhook {
    pub event_id: String,
    pub event: &'static str,
    /// Which webhook URL the event is for (a fingerprint, never the URL), or
    /// `""` when nothing is to be sent.
    pub destination: String,
    pub payload: String,
    pub state: &'static str,
}

/// A webhook attempt this instance claimed: `attempt` fences its outcome.
#[derive(Debug, Clone)]
pub struct WebhookClaim {
    pub event_id: String,
    pub alert_id: i64,
    pub event: String,
    pub attempt: u32,
    pub payload: String,
}

/// A `cleared` event waits while the `fired` event of its alert for the same
/// destination is still pending. Written against the unaliased table.
const WEBHOOK_UNBLOCKED: &str = "NOT (alert_webhooks.event = 'cleared' AND EXISTS (
    SELECT 1 FROM alert_webhooks f
    WHERE f.alert_id = alert_webhooks.alert_id AND f.event = 'fired'
      AND f.state = 'pending' AND f.destination = alert_webhooks.destination))";

/// Rows per multi-row statement: well under both engines' parameter limits.
const INSERT_ROWS_PER_STATEMENT: usize = 200;

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

// ---- parameters and rows ---------------------------------------------------

/// A bound value. Booleans travel as integers so both engines take them in an
/// integer column.
#[derive(Debug, Clone)]
pub enum Param {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
}

#[allow(clippy::wrong_self_convention)]
pub trait IntoParam {
    fn to_param(&self) -> Param;
}

impl IntoParam for i64 {
    fn to_param(&self) -> Param {
        Param::Int(*self)
    }
}
impl IntoParam for i32 {
    fn to_param(&self) -> Param {
        Param::Int(*self as i64)
    }
}
impl IntoParam for u32 {
    fn to_param(&self) -> Param {
        Param::Int(*self as i64)
    }
}
impl IntoParam for u64 {
    fn to_param(&self) -> Param {
        Param::Int(*self as i64)
    }
}
impl IntoParam for usize {
    fn to_param(&self) -> Param {
        Param::Int(*self as i64)
    }
}
impl IntoParam for f64 {
    fn to_param(&self) -> Param {
        Param::Float(*self)
    }
}
impl IntoParam for bool {
    fn to_param(&self) -> Param {
        Param::Int(*self as i64)
    }
}
impl IntoParam for str {
    fn to_param(&self) -> Param {
        Param::Text(self.to_string())
    }
}
impl IntoParam for String {
    fn to_param(&self) -> Param {
        Param::Text(self.clone())
    }
}
impl<T: IntoParam + ?Sized> IntoParam for &T {
    fn to_param(&self) -> Param {
        (**self).to_param()
    }
}
impl<T: IntoParam> IntoParam for Option<T> {
    fn to_param(&self) -> Param {
        match self {
            Some(v) => v.to_param(),
            None => Param::Null,
        }
    }
}
impl IntoParam for Param {
    fn to_param(&self) -> Param {
        self.clone()
    }
}

/// `params![a, b, c]` builds the bound values of a statement.
#[macro_export]
macro_rules! params {
    () => { ([] as [$crate::db::Param; 0]) };
    ($($x:expr),+ $(,)?) => { [$($crate::db::IntoParam::to_param(&$x)),+] };
}
pub use params;

/// A NULL that fits any column type, for PostgreSQL.
#[derive(Debug)]
struct AnyNull;

impl ToSql for AnyNull {
    fn to_sql(
        &self,
        _: &Type,
        _: &mut postgres::types::private::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(IsNull::Yes)
    }
    fn accepts(_: &Type) -> bool {
        true
    }
    postgres::types::to_sql_checked!();
}

impl Param {
    fn pg(&self) -> Box<dyn ToSql + Sync + Send> {
        match self {
            Param::Null => Box::new(AnyNull),
            Param::Int(i) => Box::new(*i),
            Param::Float(f) => Box::new(*f),
            Param::Text(t) => Box::new(t.clone()),
        }
    }
}

impl rusqlite::ToSql for Param {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, Value as V};
        Ok(match self {
            Param::Null => ToSqlOutput::Owned(V::Null),
            Param::Int(i) => ToSqlOutput::Owned(V::Integer(*i)),
            Param::Float(f) => ToSqlOutput::Owned(V::Real(*f)),
            Param::Text(t) => ToSqlOutput::Borrowed(ValueRef::Text(t.as_bytes())),
        })
    }
}

/// One result row: the column names and the values, as JSON.
pub struct Row {
    names: Vec<String>,
    values: Vec<Value>,
}

impl Row {
    fn get(&self, i: usize) -> &Value {
        self.values.get(i).unwrap_or(&Value::Null)
    }
    fn i64(&self, i: usize) -> i64 {
        self.get(i).as_i64().unwrap_or(0)
    }
    fn opt_i64(&self, i: usize) -> Option<i64> {
        self.get(i).as_i64()
    }
    fn string(&self, i: usize) -> String {
        match self.get(i) {
            Value::String(s) => s.clone(),
            Value::Null => String::new(),
            other => other.to_string(),
        }
    }
    fn opt_string(&self, i: usize) -> Option<String> {
        match self.get(i) {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        }
    }
    fn json(&self, i: usize) -> Option<Value> {
        self.opt_string(i)
            .and_then(|s| serde_json::from_str(&s).ok())
    }
    fn object(&self) -> Value {
        let mut obj = Map::new();
        for (name, value) in self.names.iter().zip(&self.values) {
            let v = match value {
                Value::String(s)
                    if matches!(
                        name.as_str(),
                        "raw" | "original_raw" | "resources" | "response"
                    ) =>
                {
                    serde_json::from_str(s).unwrap_or(Value::String(s.clone()))
                }
                other => other.clone(),
            };
            obj.insert(name.clone(), v);
        }
        Value::Object(obj)
    }
}

// ---- the two engines --------------------------------------------------------

enum Conn {
    Sqlite(rusqlite::Connection),
    Postgres(PgHandle),
}

/// What the PostgreSQL thread is asked to do.
enum PgJob {
    Execute(String, Vec<Param>),
    Query(String, Vec<Param>),
    Batch(String),
}

enum PgReply {
    Count(u64),
    Rows(Vec<Row>),
    Done,
}

/// The PostgreSQL client lives on a thread of its own: the synchronous client
/// drives its own runtime, which cannot run on a tokio worker thread, and the
/// store is called from async tasks. Calls block the caller for the round trip,
/// as a SQLite call does.
struct PgHandle {
    jobs: std::sync::mpsc::Sender<(PgJob, std::sync::mpsc::Sender<Result<PgReply>>)>,
}

impl PgHandle {
    fn connect(url: &str) -> Result<Self> {
        let (jobs, inbox) =
            std::sync::mpsc::channel::<(PgJob, std::sync::mpsc::Sender<Result<PgReply>>)>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<()>>();
        let url = url.to_string();
        std::thread::Builder::new()
            .name("hie-postgres".into())
            .spawn(move || {
                let mut client = match postgres::Client::connect(&url, postgres::NoTls) {
                    Ok(c) => {
                        let _ = ready_tx.send(Ok(()));
                        c
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(anyhow::Error::new(e)
                            .context(format!("connecting to PostgreSQL at {}", redact(&url)))));
                        return;
                    }
                };
                while let Ok((job, reply)) = inbox.recv() {
                    let result = match job {
                        PgJob::Execute(sql, params) => {
                            pg_execute(&mut client, &sql, &params).map(PgReply::Count)
                        }
                        PgJob::Query(sql, params) => {
                            pg_query(&mut client, &sql, &params).map(PgReply::Rows)
                        }
                        PgJob::Batch(sql) => client
                            .batch_execute(&sql)
                            .map(|_| PgReply::Done)
                            .map_err(anyhow::Error::new),
                    };
                    let _ = reply.send(result);
                }
            })
            .context("starting the PostgreSQL thread")?;
        ready_rx
            .recv()
            .context("the PostgreSQL thread went away before connecting")??;
        Ok(Self { jobs })
    }

    fn call(&self, job: PgJob) -> Result<PgReply> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.jobs
            .send((job, tx))
            .map_err(|_| anyhow::anyhow!("the PostgreSQL connection thread has stopped"))?;
        rx.recv()
            .map_err(|_| anyhow::anyhow!("the PostgreSQL connection thread has stopped"))?
    }

    fn execute(&self, sql: &str, params: &[Param]) -> Result<u64> {
        match self.call(PgJob::Execute(sql.to_string(), params.to_vec()))? {
            PgReply::Count(n) => Ok(n),
            _ => Ok(0),
        }
    }

    fn query(&self, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
        match self.call(PgJob::Query(sql.to_string(), params.to_vec()))? {
            PgReply::Rows(rows) => Ok(rows),
            _ => Ok(vec![]),
        }
    }

    fn batch(&self, sql: &str) -> Result<()> {
        self.call(PgJob::Batch(sql.to_string())).map(drop)
    }
}

fn pg_execute(client: &mut postgres::Client, sql: &str, params: &[Param]) -> Result<u64> {
    let boxed: Vec<Box<dyn ToSql + Sync + Send>> = params.iter().map(Param::pg).collect();
    let refs: Vec<&(dyn ToSql + Sync)> = boxed
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    Ok(client.execute(sql, refs.as_slice())?)
}

fn pg_query(client: &mut postgres::Client, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
    let boxed: Vec<Box<dyn ToSql + Sync + Send>> = params.iter().map(Param::pg).collect();
    let refs: Vec<&(dyn ToSql + Sync)> = boxed
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let rows = client.query(sql, refs.as_slice())?;
    Ok(rows
        .iter()
        .map(|r| {
            let names: Vec<String> = r.columns().iter().map(|c| c.name().to_string()).collect();
            let values = r
                .columns()
                .iter()
                .enumerate()
                .map(|(i, col)| pg_value(r, i, col.type_()))
                .collect();
            Row { names, values }
        })
        .collect())
}

impl Conn {
    fn execute(&mut self, sql: &str, params: &[Param]) -> Result<u64> {
        match self {
            Conn::Sqlite(c) => {
                let sql = sql.replace('$', "?");
                let refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
                Ok(c.execute(&sql, refs.as_slice())? as u64)
            }
            Conn::Postgres(h) => h.execute(sql, params),
        }
    }

    fn query(&mut self, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
        match self {
            Conn::Sqlite(c) => {
                let sql = sql.replace('$', "?");
                let mut stmt = c.prepare(&sql)?;
                let names: Vec<String> =
                    stmt.column_names().iter().map(|s| s.to_string()).collect();
                let refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
                let rows = stmt.query_map(refs.as_slice(), |r| {
                    let mut values = Vec::with_capacity(names.len());
                    for i in 0..names.len() {
                        values.push(match r.get_ref(i)? {
                            ValueRef::Null => Value::Null,
                            ValueRef::Integer(n) => json!(n),
                            ValueRef::Real(f) => json!(f),
                            ValueRef::Text(t) => {
                                Value::String(String::from_utf8_lossy(t).into_owned())
                            }
                            ValueRef::Blob(_) => Value::Null,
                        });
                    }
                    Ok(Row {
                        names: names.clone(),
                        values,
                    })
                })?;
                Ok(rows.collect::<rusqlite::Result<_>>()?)
            }
            Conn::Postgres(h) => h.query(sql, params),
        }
    }

    fn batch(&mut self, sql: &str) -> Result<()> {
        match self {
            Conn::Sqlite(c) => Ok(c.execute_batch(sql)?),
            Conn::Postgres(h) => h.batch(sql),
        }
    }
}

fn pg_value(row: &postgres::Row, i: usize, kind: &Type) -> Value {
    match *kind {
        Type::INT8 => row
            .try_get::<_, Option<i64>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::from),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |v| Value::from(v as i64)),
        Type::INT2 => row
            .try_get::<_, Option<i16>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |v| Value::from(v as i64)),
        Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::from),
        Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |v| Value::from(v as f64)),
        Type::BOOL => row
            .try_get::<_, Option<bool>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::Bool),
        Type::NUMERIC => row
            .try_get::<_, Option<rust_decimal::Decimal>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |d| {
                serde_json::from_str(&d.to_string()).unwrap_or(Value::String(d.to_string()))
            }),
        _ => row
            .try_get::<_, Option<String>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::String),
    }
}

// ---- transactions and events ------------------------------------------------

/// One transaction on the store, under the connection's lock: see
/// `Db::transaction`. Events are written only through `insert_events`.
struct Tx<'a> {
    conn: &'a mut Conn,
    postgres: bool,
    /// Whether this transaction holds the event lock (PostgreSQL).
    events_locked: bool,
    /// Ends the transaction with a rollback even when it succeeds.
    rollback: bool,
}

impl Tx<'_> {
    fn execute(&mut self, sql: &str, params: &[Param]) -> Result<u64> {
        self.conn.execute(sql, params)
    }

    fn query(&mut self, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
        self.conn.query(sql, params)
    }

    fn query_one(&mut self, sql: &str, params: &[Param]) -> Result<Option<Row>> {
        Ok(self.query(sql, params)?.into_iter().next())
    }

    /// ` FOR UPDATE` on PostgreSQL. On SQLite `BEGIN IMMEDIATE` already holds
    /// the store's only write lock.
    fn for_update(&self) -> &'static str {
        if self.postgres { " FOR UPDATE" } else { "" }
    }

    /// Appends events. On PostgreSQL the first call takes the event lock, held
    /// until the transaction ends, so ids are given in commit order: a reader
    /// that sees an id knows every lower id is committed or gone. Callers make
    /// every other change of the transaction before the first call, so a holder
    /// of the lock never waits for a row lock.
    fn insert_events(&mut self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        if self.postgres && !self.events_locked {
            // Keyed by schema: stores in other schemas of one database do not
            // wait on each other.
            self.conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext('hie.events'), hashtext(current_schema()))",
                &[],
            )?;
            self.events_locked = true;
        }
        let at = now_ms();
        for chunk in events.chunks(INSERT_ROWS_PER_STATEMENT) {
            let mut sql = String::from(
                "INSERT INTO events (at, source_id, kind, load_id, record_id, attempt, error_kind, reason, detail) VALUES ",
            );
            let mut values: Vec<Param> = Vec::with_capacity(chunk.len() * 9);
            for (i, e) in chunk.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                let b = i * 9;
                sql.push_str(&format!(
                    "(CAST(${} AS BIGINT), CAST(${} AS TEXT), CAST(${} AS TEXT), CAST(${} AS BIGINT), CAST(${} AS BIGINT), \
                     CAST(${} AS BIGINT), CAST(${} AS TEXT), CAST(${} AS TEXT), CAST(${} AS TEXT))",
                    b + 1, b + 2, b + 3, b + 4, b + 5, b + 6, b + 7, b + 8, b + 9
                ));
                values.extend(params![
                    at,
                    e.source_id,
                    e.kind,
                    e.load_id,
                    e.record_id,
                    e.attempt,
                    e.error_kind,
                    e.reason,
                    e.detail.as_ref().map(Value::to_string)
                ]);
            }
            self.conn.execute(&sql, &values)?;
        }
        Ok(())
    }
}

impl Tx<'_> {
    /// Appends one event per item, built and written a chunk at a time, so a
    /// large set never holds all its events at once.
    fn insert_each<T>(&mut self, items: &[T], event: impl Fn(&T) -> Event) -> Result<()> {
        for chunk in items.chunks(INSERT_ROWS_PER_STATEMENT) {
            let events: Vec<Event> = chunk.iter().map(&event).collect();
            self.insert_events(&events)?;
        }
        Ok(())
    }
}

/// Stores a file's records, by the hundred per statement: one round trip each
/// over a million rows is an hour; this is minutes. Returns the rows stored as
/// rejected because they could not be parsed: (record id, index in `rows`).
fn insert_record_rows(
    tx: &mut Tx,
    file_id: i64,
    feed: &str,
    rows: &[ReadRow],
) -> Result<Vec<(i64, usize)>> {
    let now = now_ms();
    let mut rejected = Vec::new();
    for (n, chunk) in rows.chunks(INSERT_ROWS_PER_STATEMENT).enumerate() {
        let first = n * INSERT_ROWS_PER_STATEMENT;
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
            sql.push_str(&format!("({})", marks(i * 8 + 1, 8)));
            values.extend(params![
                file_id,
                feed,
                line,
                raw.to_string(),
                status,
                kind,
                parse_error,
                now
            ]);
        }
        // The line tells the rows of one statement apart; the id is the store's.
        sql.push_str(" RETURNING id, line, status");
        let mut by_line: HashMap<i64, Vec<usize>> = HashMap::new();
        for (i, (line, _, parse_error)) in chunk.iter().enumerate() {
            if parse_error.is_some() {
                by_line.entry(*line).or_default().push(first + i);
            }
        }
        let mut stored = tx.query(&sql, &values)?;
        stored.sort_by_key(|r| r.i64(0));
        for r in stored {
            if r.string(2) == "rejected"
                && let Some(indexes) = by_line.get_mut(&r.i64(1))
                && !indexes.is_empty()
            {
                rejected.push((r.i64(0), indexes.remove(0)));
            }
        }
    }
    Ok(rejected)
}

/// Updates the records that match `guard`, `INSERT_ROWS_PER_STATEMENT` at a
/// time, paged by id: each step selects the next ids that match (locked on
/// PostgreSQL), then updates exactly those, with the guard checked again on
/// each row, so a record changed meanwhile is left alone. `guard` uses the
/// parameters `$1..$g` (`guard_params`), `set` the ones after (`set_params`).
/// Only the ids changed are kept, with the second column `returning` names
/// (or 0): never the rows.
fn update_records_paged(
    tx: &mut Tx,
    guard: &str,
    guard_params: &[Param],
    set: &str,
    set_params: &[Param],
    returning: &str,
) -> Result<Vec<(i64, i64)>> {
    let g = guard_params.len();
    let select = format!(
        "SELECT id FROM records WHERE {guard} AND id > ${} ORDER BY id LIMIT {INSERT_ROWS_PER_STATEMENT}{}",
        g + 1,
        tx.for_update()
    );
    let mut changed = Vec::new();
    let mut last = 0i64;
    loop {
        let mut values = guard_params.to_vec();
        values.push(Param::Int(last));
        let ids: Vec<i64> = tx
            .query(&select, &values)?
            .iter()
            .map(|r| r.i64(0))
            .collect();
        let Some(&end) = ids.last() else {
            break;
        };
        let mut values = guard_params.to_vec();
        values.extend_from_slice(set_params);
        values.extend(ids.iter().map(|id| Param::Int(*id)));
        let rows = tx.query(
            &format!(
                "UPDATE records SET {set} WHERE id IN ({}) AND {guard} RETURNING {returning}",
                marks(g + set_params.len() + 1, ids.len())
            ),
            &values,
        )?;
        changed.extend(rows.iter().map(|r| (r.i64(0), r.opt_i64(1).unwrap_or(0))));
        last = end;
        if ids.len() < INSERT_ROWS_PER_STATEMENT {
            break;
        }
    }
    changed.sort_unstable();
    Ok(changed)
}

/// A new submission's row, waiting for its kick-off.
fn insert_submission(
    tx: &mut Tx,
    token: &str,
    feed: &str,
    submission_id: &str,
    manifest: &Value,
    now: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO submissions (token, feed_id, submission_id, status, manifest, created_at, updated_at)
         VALUES ($1, $2, $3, 'kickoff', $4, $5, $6)",
        &params![token, feed, submission_id, manifest.to_string(), now, now],
    )
    .map(drop)
}

/// `submission.opened`: the run's public key, never its token.
fn opened(feed: &str, token: &str, submission_id: &str, records: usize, files: usize) -> Event {
    Event::new("submission.opened", feed).detail(json!({
        "submission": submission_id,
        "run": crate::bulk::run_key(token),
        "records": records,
        "files": files,
    }))
}

/// `$first, $first+1, ...` for `n` values.
fn marks(first: usize, n: usize) -> String {
    (first..first + n)
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// An instant in epoch milliseconds as RFC 3339, for an event's detail.
fn iso_ms(ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

// ---- the store ----------------------------------------------------------------

pub struct Db {
    conn: Mutex<Conn>,
    postgres: bool,
    /// A schema created for one test run on PostgreSQL, dropped with the store.
    test_schema: Option<String>,
    /// What a test runs at a named point (`pause_point!`), to hold a thread there.
    #[cfg(all(test, feature = "secrets-native"))]
    pause: Mutex<Option<PauseHook>>,
}

/// What a test runs at a `pause_point!` of the store.
#[cfg(all(test, feature = "secrets-native"))]
pub(crate) type PauseHook = std::sync::Arc<dyn Fn(&'static str) + Send + Sync>;

/// Whether an error is a lock wait that ran out: SQLite's busy or locked after
/// `busy_timeout`, or PostgreSQL's serialization failure, deadlock or lock
/// not available. The same call may succeed later.
pub fn is_busy(e: &anyhow::Error) -> bool {
    use postgres::error::SqlState;
    e.chain().any(|cause| {
        if let Some(e) = cause.downcast_ref::<rusqlite::Error>() {
            return matches!(
                e.sqlite_error_code(),
                Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
            );
        }
        if let Some(e) = cause.downcast_ref::<postgres::Error>() {
            return e.code().is_some_and(|code| {
                *code == SqlState::T_R_SERIALIZATION_FAILURE
                    || *code == SqlState::T_R_DEADLOCK_DETECTED
                    || *code == SqlState::LOCK_NOT_AVAILABLE
            });
        }
        false
    })
}

impl Db {
    /// Opens the store named by `spec`: a `postgres://` URL, or a SQLite file path.
    pub fn connect(spec: &str) -> Result<Self> {
        if spec.starts_with("postgres://") || spec.starts_with("postgresql://") {
            Self::open_postgres(spec, None)
        } else {
            Self::open(Path::new(spec))
        }
    }

    pub fn open(path: &Path) -> Result<Self> {
        if let Some(url) = test_database_url() {
            // The same path opened twice in a test is the same store, as with a
            // SQLite file: the schema is named after the path.
            use sha2::Digest;
            let digest = sha2::Sha256::digest(path.to_string_lossy().as_bytes());
            return Self::open_postgres(
                &url,
                Some(format!("hie_test_{}", hex::encode(&digest[..8]))),
            );
        }
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let conn = rusqlite::Connection::open(path)
            .with_context(|| format!("opening {}", path.display()))?;
        Self::init_sqlite(conn)
    }

    pub fn in_memory() -> Result<Self> {
        if let Some(url) = test_database_url() {
            return Self::open_postgres(&url, Some(test_schema_name()));
        }
        Self::init_sqlite(rusqlite::Connection::open_in_memory()?)
    }

    pub fn is_postgres(&self) -> bool {
        self.postgres
    }

    fn init_sqlite(conn: rusqlite::Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.execute_batch(SCHEMA)?;
        for (table, column, kind) in ADDED_COLUMNS {
            let exists = conn
                .prepare(&format!("PRAGMA table_info({table})"))?
                .query_map([], |r| r.get::<_, String>(1))?
                .filter_map(Result::ok)
                .any(|c| c == *column);
            if !exists {
                conn.execute(
                    &format!("ALTER TABLE {table} ADD COLUMN {column} {kind}"),
                    [],
                )?;
            }
        }
        // SCHEMA made its own indexes; SQLite has one writer at a time.
        for index in ADDED_INDEXES.into_iter().chain(LOAD_INDEXES) {
            conn.execute(index, [])?;
        }
        never_reuse_rule_ids(&conn)?;
        conn.execute(
            &EVENTS_SINCE.replace('$', "?"),
            rusqlite::params![now_ms().to_string()],
        )?;
        Ok(Self {
            conn: Mutex::new(Conn::Sqlite(conn)),
            postgres: false,
            test_schema: None,
            #[cfg(all(test, feature = "secrets-native"))]
            pause: Mutex::new(None),
        })
    }

    fn open_postgres(url: &str, test_schema: Option<String>) -> Result<Self> {
        let client = PgHandle::connect(url)?;
        let mut test_schema = test_schema;
        if let Some(schema) = test_schema.clone() {
            // Whoever creates the schema drops it; a second handle on the same
            // schema only uses it.
            let existed = !client
                .query(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = $1",
                    &params![schema],
                )?
                .is_empty();
            client.batch(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema}; SET search_path TO {schema}"
            ))?;
            if existed {
                test_schema = None;
            }
        }
        // Instances starting together on one schema build it in turn: two
        // `CREATE INDEX IF NOT EXISTS` at once may both try to build it.
        client
            .batch("SELECT pg_advisory_lock(hashtext('hie.schema'), hashtext(current_schema()))")?;
        let built = (|| {
            // A sequence never gives an id twice, as SQLite's AUTOINCREMENT does not.
            // The tables only: the indexes come after, one at a time (`postgres_indexes`).
            let ddl = schema_statements()
                .filter(|s| !is_index(s))
                .collect::<Vec<_>>()
                .join(";\n")
                .replace(" AUTOINCREMENT", "")
                .replace("INTEGER PRIMARY KEY", "BIGSERIAL PRIMARY KEY")
                .replace("INTEGER", "BIGINT");
            client
                .batch(&ddl)
                .context("creating the schema in PostgreSQL")?;
            for (table, column, kind) in ADDED_COLUMNS {
                client.batch(&format!(
                    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {}",
                    kind.replace("INTEGER", "BIGINT")
                ))?;
            }
            for index in postgres_indexes() {
                client.batch(index)?;
            }
            client.execute(EVENTS_SINCE, &params![now_ms().to_string()])?;
            Ok::<_, anyhow::Error>(())
        })();
        let unlocked = client
            .batch("SELECT pg_advisory_unlock(hashtext('hie.schema'), hashtext(current_schema()))");
        built?;
        unlocked?;
        Ok(Self {
            conn: Mutex::new(Conn::Postgres(client)),
            postgres: true,
            test_schema,
            #[cfg(all(test, feature = "secrets-native"))]
            pause: Mutex::new(None),
        })
    }

    /// How long a SQLite statement waits for another connection's write lock
    /// before it fails as busy (5 s by default). PostgreSQL waits on its locks.
    pub fn set_busy_timeout(&self, wait: std::time::Duration) {
        if let Conn::Sqlite(c) = &mut *self.conn.lock().expect("database mutex poisoned") {
            let _ = c.busy_timeout(wait);
        }
    }

    /// Runs `hook` at every `pause_point!` this store reaches.
    #[cfg(all(test, feature = "secrets-native"))]
    pub(crate) fn on_pause(&self, hook: PauseHook) {
        *self.pause.lock().expect("pause mutex") = Some(hook);
    }

    #[cfg(all(test, feature = "secrets-native"))]
    pub(crate) fn pause_at(&self, name: &'static str) {
        let hook = self.pause.lock().expect("pause mutex").clone();
        if let Some(hook) = hook {
            hook(name);
        }
    }

    fn with<T>(&self, f: impl FnOnce(&mut Conn) -> Result<T>) -> Result<T> {
        let mut conn = self.conn.lock().expect("database mutex poisoned");
        f(&mut conn)
    }

    /// Runs `f` in one transaction: `BEGIN IMMEDIATE` on SQLite (the write lock
    /// at once, so one writer at a time), `BEGIN` on PostgreSQL. Committed when
    /// `f` succeeds, unless it asked for a rollback; rolled back on any error.
    fn transaction<T>(&self, f: impl FnOnce(&mut Tx) -> Result<T>) -> Result<T> {
        let postgres = self.postgres;
        self.with(|c| {
            c.batch(if postgres { "BEGIN" } else { "BEGIN IMMEDIATE" })?;
            let (result, rollback) = {
                let mut tx = Tx {
                    conn: c,
                    postgres,
                    events_locked: false,
                    rollback: false,
                };
                let result = f(&mut tx);
                (result, tx.rollback)
            };
            match result {
                Ok(value) if !rollback => match c.batch("COMMIT") {
                    Ok(()) => Ok(value),
                    Err(e) => {
                        let _ = c.batch("ROLLBACK");
                        Err(e)
                    }
                },
                Ok(value) => c.batch("ROLLBACK").map(|()| value),
                Err(e) => {
                    let _ = c.batch("ROLLBACK");
                    Err(e)
                }
            }
        })
    }

    /// The schema the store's tables are in, on PostgreSQL.
    pub fn schema(&self) -> Result<Option<String>> {
        if !self.postgres {
            return Ok(None);
        }
        Ok(self
            .query_one("SELECT current_schema()", &[])?
            .and_then(|r| r.opt_string(0)))
    }

    fn execute(&self, sql: &str, params: &[Param]) -> Result<u64> {
        self.with(|c| c.execute(sql, params))
    }

    fn query(&self, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
        self.with(|c| c.query(sql, params))
    }

    fn query_one(&self, sql: &str, params: &[Param]) -> Result<Option<Row>> {
        Ok(self.query(sql, params)?.into_iter().next())
    }

    /// An inserted row's id.
    fn insert_returning_id(&self, sql: &str, params: &[Param]) -> Result<i64> {
        self.query_one(&format!("{sql} RETURNING id"), params)?
            .map(|r| r.i64(0))
            .context("INSERT returned no id")
    }

    /// The SQL that formats an epoch-milliseconds column as an ISO instant.
    pub fn iso(&self, col: &str) -> String {
        if self.postgres {
            format!(
                "to_char(to_timestamp({col} / 1000.0) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')"
            )
        } else {
            format!("strftime('%Y-%m-%dT%H:%M:%fZ', {col} / 1000.0, 'unixepoch')")
        }
    }

    /// The SQL that formats an epoch-milliseconds column as a UTC calendar day.
    pub fn day(&self, col: &str) -> String {
        if self.postgres {
            format!("to_char(to_timestamp({col} / 1000.0) AT TIME ZONE 'UTC', 'YYYY-MM-DD')")
        } else {
            format!("date({col} / 1000, 'unixepoch')")
        }
    }

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
    /// until HFS has fetched it (the `$bulk-submit` path), and `origin` says where
    /// that kept file lives:
    /// - `store`: an object in a store, always handed over by its store's URL;
    /// - `local`: a file of a source that reads from disk (a folder, the MLLP
    ///   spool), served by the engine and acknowledged by its source;
    /// - `staged`: an entry the engine unpacked under its bulk directory, served
    ///   by the engine and removed once settled.
    ///
    /// A row without one (written by an earlier version and not backfilled) is
    /// never served by the engine. `source_name` is the source's name as the
    /// load is recorded, kept with it after the source is renamed or deleted
    /// (see `storable_name`).
    #[allow(clippy::too_many_arguments)]
    pub fn insert_file(
        &self,
        feed: &str,
        source_name: &str,
        name: &str,
        sha: &str,
        size: u64,
        status: &str,
        duplicate_of: Option<i64>,
        source_key: Option<&str>,
        origin: Option<&str>,
    ) -> Result<i64> {
        let now = now_ms();
        let source_name = storable_name(source_name);
        let finished_at = if status == "processing" {
            None
        } else {
            Some(now)
        };
        self.insert_returning_id(
            "INSERT INTO files (feed_id, name, sha256, size, status, duplicate_of, received_at, finished_at, source_key, origin, source_name)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            &params![feed, name, sha, size, status, duplicate_of, now, finished_at, source_key, origin, source_name],
        )
    }

    /// The key, name and origin of a file still waiting at its source (not archived yet).
    pub fn file_source(&self, file_id: i64) -> Result<Option<(String, String, Option<String>)>> {
        Ok(self
            .query_one(
                "SELECT source_key, name, origin FROM files WHERE id = $1 AND source_key IS NOT NULL AND archived = 0",
                &params![file_id],
            )?
            .map(|r| (r.string(0), r.string(1), r.opt_string(2))))
    }

    /// Gives the files of a feed recorded before origins were, and that still name
    /// where they live, their origin, decided once by `bulk::historical_origin`.
    /// Archived files are included: a retry hands them over again. Only missing
    /// origins are filled. Returns how many.
    pub fn backfill_file_origins(
        &self,
        feed: &str,
        local_source: bool,
        bulk_dir: &Path,
    ) -> Result<usize> {
        self.with(|c| {
            c.batch("BEGIN")?;
            let result = (|| {
                // Only an unpacked entry (`zip!/entry`) can have been staged by the
                // engine; every other file takes its source's kind.
                let mut n = c.execute(
                    "UPDATE files SET origin = $2
                     WHERE feed_id = $1 AND origin IS NULL AND source_key IS NOT NULL
                       AND name NOT LIKE '%!/%'",
                    &params![feed, if local_source { "local" } else { "store" }],
                )? as usize;
                let entries = c.query(
                    "SELECT id, name, source_key FROM files
                     WHERE feed_id = $1 AND origin IS NULL AND source_key IS NOT NULL",
                    &params![feed],
                )?;
                for r in &entries {
                    let origin = crate::bulk::historical_origin(
                        &r.string(1),
                        &r.string(2),
                        local_source,
                        bulk_dir,
                    );
                    c.execute(
                        "UPDATE files SET origin = $2 WHERE id = $1 AND origin IS NULL",
                        &params![r.i64(0), origin],
                    )?;
                }
                n += entries.len();
                Ok::<_, anyhow::Error>(n)
            })();
            match result {
                Ok(n) => c.batch("COMMIT").map(|()| n),
                Err(e) => {
                    let _ = c.batch("ROLLBACK");
                    Err(e)
                }
            }
        })
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

    /// Records a file and admits what was read from it, in one transaction: its
    /// records, or its unreadable state (`read` is the reason); then
    /// `load.received` (or `load.unreadable`) and a `record.rejected` for each row
    /// that could not be parsed. A failure leaves no file row, so the next scan
    /// takes the object again.
    ///
    /// The source's admissions take turns (on PostgreSQL, a lock per source), and
    /// the content is looked up again under that lock: an instance that took the
    /// source over after a lease lapsed may be reading the same object. When the
    /// content was taken meanwhile nothing is written and the file it went into
    /// comes back, to be handled as a duplicate.
    pub fn admit_file(
        &self,
        file: &NewFile,
        read: std::result::Result<&[ReadRow], &str>,
    ) -> Result<Admission> {
        self.transaction(|tx| {
            if tx.postgres {
                tx.execute(
                    "SELECT pg_advisory_xact_lock(hashtext('hie.admit'), hashtext(current_schema() || ':' || $1))",
                    &params![file.feed],
                )?;
            }
            if let Some(original) = tx
                .query_one(
                    "SELECT id FROM files WHERE feed_id = $1 AND sha256 = $2 AND status <> 'duplicate' ORDER BY id LIMIT 1",
                    &params![file.feed, file.sha],
                )?
                .map(|r| r.i64(0))
            {
                tx.rollback = true;
                return Ok(Admission::AlreadyTaken(original));
            }
            let now = now_ms();
            let (status, finished_at, error) = match read {
                Ok(_) => ("processing", None, None),
                Err(reason) => ("unreadable", Some(now), Some(reason)),
            };
            let file_id = tx
                .query_one(
                    "INSERT INTO files (feed_id, name, sha256, size, status, received_at, finished_at, error, source_key, origin, source_name)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id",
                    &params![
                        file.feed,
                        file.name,
                        file.sha,
                        file.size,
                        status,
                        now,
                        finished_at,
                        error,
                        file.source_key,
                        file.origin,
                        storable_name(file.source_name)
                    ],
                )?
                .map(|r| r.i64(0))
                .context("INSERT returned no id")?;
            match read {
                Ok(rows) => {
                    let mut rejected = insert_record_rows(tx, file_id, file.feed, rows)?;
                    rejected.sort_unstable_by_key(|(id, _)| *id);
                    tx.insert_events(&[Event::new("load.received", file.feed)
                        .load(file_id)
                        .detail(json!({"name": file.name, "size": file.size, "records": rows.len()}))])?;
                    // Each row's own reason; the events go in a chunk at a time.
                    tx.insert_each(&rejected, |(id, row)| {
                        Event::new("record.rejected", file.feed)
                            .load(file_id)
                            .record(*id)
                            .error_kind("parse")
                            .reason(rows[*row].2.as_deref().unwrap_or_default())
                    })?;
                }
                Err(reason) => tx.insert_events(&[Event::new("load.unreadable", file.feed)
                    .load(file_id)
                    .reason(reason)
                    .detail(json!({"name": file.name, "size": file.size}))])?,
            }
            Ok(Admission::Admitted(file_id))
        })
    }

    /// Records a file whose content was already received, with `load.duplicate`.
    pub fn insert_duplicate(
        &self,
        feed: &str,
        source_name: &str,
        name: &str,
        sha: &str,
        size: u64,
        original: i64,
    ) -> Result<i64> {
        self.transaction(|tx| {
            let now = now_ms();
            let id = tx
                .query_one(
                    "INSERT INTO files (feed_id, name, sha256, size, status, duplicate_of, received_at, finished_at, source_name)
                     VALUES ($1, $2, $3, $4, 'duplicate', $5, $6, $7, $8) RETURNING id",
                    &params![feed, name, sha, size, original, now, now, storable_name(source_name)],
                )?
                .map(|r| r.i64(0))
                .context("INSERT returned no id")?;
            tx.insert_events(&[Event::new("load.duplicate", feed)
                .load(id)
                .detail(json!({"name": name, "duplicate_of": original}))])?;
            Ok(id)
        })
    }

    /// Records a zip as `unpacked`, with `load.received` marked as a container;
    /// its entries are loads of their own.
    pub fn insert_container(
        &self,
        feed: &str,
        source_name: &str,
        name: &str,
        sha: &str,
        size: u64,
    ) -> Result<i64> {
        self.transaction(|tx| {
            let now = now_ms();
            let id = tx
                .query_one(
                    "INSERT INTO files (feed_id, name, sha256, size, status, received_at, finished_at, source_name)
                     VALUES ($1, $2, $3, $4, 'unpacked', $5, $6, $7) RETURNING id",
                    &params![feed, name, sha, size, now, now, storable_name(source_name)],
                )?
                .map(|r| r.i64(0))
                .context("INSERT returned no id")?;
            tx.insert_events(&[Event::new("load.received", feed).load(id).detail(
                json!({"name": name, "size": size, "records": 0, "container": true}),
            )])?;
            Ok(id)
        })
    }

    /// Puts a file's rejected records back at the start, all of them or those with
    /// one reason, with `load.retried` and a `record.retried` per record. The
    /// file's row is locked first, so a finish of the same file waits or is seen
    /// waiting. Returns how many; with none, nothing changes.
    pub fn reprocess_rejected(&self, file_id: i64, reason: Option<&str>) -> Result<usize> {
        self.transaction(|tx| {
            let lock = format!(
                "SELECT feed_id FROM files WHERE id = $1{}",
                tx.for_update()
            );
            let Some(feed) = tx.query_one(&lock, &params![file_id])?.map(|r| r.string(0)) else {
                return Ok(0);
            };
            let ids = update_records_paged(
                tx,
                "file_id = $1 AND status = 'rejected' AND (CAST($2 AS TEXT) IS NULL OR error = CAST($2 AS TEXT))",
                &params![file_id, reason],
                "status = 'received', resources = NULL, error_kind = NULL, error = NULL,
                 response = NULL, attempts = 0, next_attempt_at = 0, submission = NULL,
                 revision = revision + 1, updated_at = $3",
                &params![now_ms()],
                "id",
            )?;
            if ids.is_empty() {
                tx.rollback = true;
                return Ok(0);
            }
            tx.execute(
                "UPDATE files SET status = 'processing', finished_at = NULL WHERE id = $1",
                &params![file_id],
            )?;
            let mut retried = Event::new("load.retried", &feed)
                .load(file_id)
                .detail(json!({"retried": ids.len()}));
            if let Some(reason) = reason {
                retried = retried.reason(reason);
            }
            tx.insert_events(&[retried])?;
            let detail = json!({"from_status": "rejected", "corrected": false});
            tx.insert_each(&ids, |(id, _)| {
                Event::new("record.retried", &feed)
                    .load(file_id)
                    .record(*id)
                    .detail(detail.clone())
            })?;
            Ok(ids.len())
        })
    }

    /// A file that could not be read after it was recorded (a zip that does not
    /// open), with `load.unreadable`.
    pub fn fail_file(&self, file_id: i64, error: &str) -> Result<()> {
        self.transaction(|tx| {
            let row = tx.query_one(
                "UPDATE files SET status = 'unreadable', error = $2, finished_at = $3 WHERE id = $1
                 RETURNING feed_id, name, size",
                &params![file_id, error, now_ms()],
            )?;
            if let Some(r) = row {
                tx.insert_events(&[Event::new("load.unreadable", &r.string(0))
                    .load(file_id)
                    .reason(error)
                    .detail(json!({"name": r.string(1), "size": r.i64(2)}))])?;
            }
            Ok(())
        })
    }

    /// Marks files whose records have all reached `sent` or `rejected` as done and
    /// returns them: `finish_candidates`, then `finish_file` for each.
    pub fn finish_files(&self, feed: &str) -> Result<Vec<FinishedFile>> {
        let mut done = Vec::new();
        for candidate in self.finish_candidates(feed)? {
            done.extend(self.finish_file(feed, candidate)?);
        }
        Ok(done)
    }

    /// The files of a feed that look finished: every record sent or rejected.
    pub fn finish_candidates(&self, feed: &str) -> Result<Vec<i64>> {
        Ok(self
            .query(
            "SELECT f.id FROM files f
             WHERE f.feed_id = $1 AND f.status IN ('processing', 'unpacked')
               AND NOT EXISTS (SELECT 1 FROM records r WHERE r.file_id = f.id AND r.status IN ('received', 'pending', 'submitted'))
             ORDER BY f.id",
                &params![feed],
            )?
            .iter()
            .map(|r| r.i64(0))
            .collect())
    }

    /// Finishes one file, with `load.finished`, in a transaction of its own: its
    /// row is locked, then moved to `done` only if no record is in progress, read
    /// after the lock; the counts are read after that. A Retry that put records
    /// back first is seen; one that comes after waits. `None` when the file is
    /// not finished.
    pub fn finish_file(&self, feed: &str, candidate: i64) -> Result<Option<FinishedFile>> {
        self.transaction(|tx| {
                let lock = format!(
                    "SELECT name, status, rejected_reported FROM files
                     WHERE id = $1 AND status IN ('processing', 'unpacked'){}",
                    tx.for_update()
                );
                let Some(file) = tx.query_one(&lock, &params![candidate])? else {
                    return Ok(None);
                };
                let moved = tx.query_one(
                    "UPDATE files SET status = 'done', finished_at = $2,
                            rejected_reported = CASE
                              WHEN rejected_reported > (SELECT COUNT(*) FROM records WHERE file_id = $1 AND status = 'rejected')
                              THEN rejected_reported
                              ELSE (SELECT COUNT(*) FROM records WHERE file_id = $1 AND status = 'rejected') END
                     WHERE id = $1 AND status IN ('processing', 'unpacked')
                       AND NOT EXISTS (SELECT 1 FROM records WHERE file_id = $1 AND status IN ('received', 'pending', 'submitted'))
                     RETURNING id",
                    &params![candidate, now_ms()],
                )?;
                if moved.is_none() {
                    return Ok(None);
                }
                let counts = tx
                    .query_one(
                        "SELECT COUNT(id),
                                CAST(COALESCE(SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END), 0) AS BIGINT),
                                CAST(COALESCE(SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END), 0) AS BIGINT)
                         FROM records WHERE file_id = $1",
                        &params![candidate],
                    )?
                    .context("no counts")?;
                let (total, sent, rejected) = (counts.i64(0), counts.i64(1), counts.i64(2));
                let first_rejection = tx
                    .query_one(
                        "SELECT line, error FROM records WHERE file_id = $1 AND status = 'rejected' ORDER BY line LIMIT 1",
                        &params![candidate],
                    )?
                    .map(|r| (r.i64(0), r.string(1)));
                let mut detail = json!({"records": total, "sent": sent, "rejected": rejected});
                if file.string(1) == "unpacked" {
                    detail["container"] = json!(true);
                }
                tx.insert_events(&[Event::new("load.finished", feed)
                    .load(candidate)
                    .detail(detail)])?;
                Ok(Some(FinishedFile {
                    id: candidate,
                    name: file.string(0),
                    total,
                    sent,
                    rejected,
                    newly_rejected: rejected - file.i64(2),
                    first_rejection,
                }))
            })
    }

    // ---- records -----------------------------------------------------------

    /// Stores a file's records (rows that could not be parsed as rejected), with
    /// no events: the pipeline admits files with `admit_file`.
    pub fn insert_records(
        &self,
        file_id: i64,
        feed: &str,
        rows: &[(i64, Value, Option<String>)],
    ) -> Result<()> {
        self.transaction(|tx| insert_record_rows(tx, file_id, feed, rows).map(drop))
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
                revision: r.i64(7),
            })
            .collect())
    }

    pub fn received_records(&self, feed: &str, limit: usize) -> Result<Vec<WorkItem>> {
        self.work_items(
            "SELECT r.id, r.file_id, r.line, f.name, r.raw, r.resources, r.attempts, r.revision
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.feed_id = $1 AND r.status = 'received' ORDER BY r.id LIMIT $2",
            &params![feed, limit],
        )
    }

    pub fn due_records(&self, feed: &str, limit: usize) -> Result<Vec<WorkItem>> {
        self.work_items(
            "SELECT r.id, r.file_id, r.line, f.name, r.raw, r.resources, r.attempts, r.revision
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.feed_id = $1 AND r.status = 'pending' AND r.next_attempt_at <= $2
             ORDER BY r.id LIMIT $3",
            &params![feed, now_ms(), limit],
        )
    }

    /// Marks records whose source row is the resource to deliver as transformed,
    /// in one statement per hundreds of records: the resource list is built from
    /// the row in the store, so no payload travels. Each is given as (id,
    /// revision) and changes only while still `received` at that revision.
    pub fn mark_passthrough(&self, items: &[(i64, i64)]) -> Result<()> {
        let now = now_ms();
        for chunk in items.chunks(INSERT_ROWS_PER_STATEMENT) {
            let rows: Vec<String> = (0..chunk.len())
                .map(|i| {
                    format!(
                        "(CAST(${} AS BIGINT), CAST(${} AS BIGINT))",
                        i * 2 + 2,
                        i * 2 + 3
                    )
                })
                .collect();
            let mut values: Vec<Param> = Vec::with_capacity(chunk.len() * 2 + 1);
            values.push(Param::Int(now));
            for (id, revision) in chunk {
                values.push(Param::Int(*id));
                values.push(Param::Int(*revision));
            }
            self.execute(
                &format!(
                    "UPDATE records SET status = 'pending', resources = '[' || raw || ']', error_kind = NULL, error = NULL,
                            attempts = 0, next_attempt_at = 0, updated_at = $1
                     FROM (VALUES {}) AS v
                     WHERE records.id = v.column1 AND records.revision = v.column2 AND records.status = 'received'",
                    rows.join(", ")
                ),
                &values,
            )?;
        }
        Ok(())
    }

    /// The transform's resources for a record still `received` at the item's
    /// revision. Returns whether it changed. The transform step writes no event.
    pub fn mark_transformed(&self, item: &WorkItem, resources: &Value) -> Result<bool> {
        Ok(self.execute(
            "UPDATE records SET status = 'pending', resources = $2, error_kind = NULL, error = NULL,
                    attempts = 0, next_attempt_at = 0, updated_at = $3
             WHERE id = $1 AND status = 'received' AND revision = $4",
            &params![item.id, resources.to_string(), now_ms(), item.revision],
        )? > 0)
    }

    /// Rejects a record still in `from` at the item's revision (`received` for the
    /// transform and the bulk preflight), with `record.rejected`. Returns whether
    /// it changed: an outcome for a superseded revision changes nothing.
    pub fn reject_item(
        &self,
        item: &WorkItem,
        from: &str,
        kind: &str,
        error: &str,
    ) -> Result<bool> {
        self.transaction(|tx| {
            let Some(r) = tx.query_one(
                "UPDATE records SET status = 'rejected', error_kind = $2, error = $3, updated_at = $4
                 WHERE id = $1 AND status = $5 AND revision = $6
                 RETURNING feed_id, file_id",
                &params![item.id, kind, error, now_ms(), from, item.revision],
            )?
            else {
                return Ok(false);
            };
            tx.insert_events(&[Event::new("record.rejected", &r.string(0))
                .load(r.i64(1))
                .record(item.id)
                .error_kind(kind)
                .reason(error)])?;
            Ok(true)
        })
    }

    /// The transform could not finish a record now and it stays `received`:
    /// `record.attempt_failed`, while the record is still at the item's revision.
    pub fn transform_deferred(&self, item: &WorkItem, reason: &str) -> Result<bool> {
        self.transaction(|tx| {
            let sql = format!(
                "SELECT feed_id, file_id FROM records WHERE id = $1 AND status = 'received' AND revision = $2{}",
                tx.for_update()
            );
            let Some(r) = tx.query_one(&sql, &params![item.id, item.revision])? else {
                return Ok(false);
            };
            tx.insert_events(&[Event::new("record.attempt_failed", &r.string(0))
                .load(r.i64(1))
                .record(item.id)
                .reason(reason)
                .detail(json!({"phase": "transform"}))])?;
            Ok(true)
        })
    }

    /// Rejects a record whatever its state, with `record.rejected`: an
    /// unguarded helper for tests that seed a store. The pipeline never uses it;
    /// its outcomes go through the guarded writes.
    #[doc(hidden)]
    pub fn mark_rejected(
        &self,
        id: i64,
        kind: &str,
        error: &str,
        response: Option<&Value>,
    ) -> Result<()> {
        self.transaction(|tx| {
            let Some(r) = tx.query_one(
                "UPDATE records SET status = 'rejected', error_kind = $2, error = $3, response = $4, updated_at = $5
                 WHERE id = $1 RETURNING feed_id, file_id",
                &params![id, kind, error, response.map(Value::to_string), now_ms()],
            )?
            else {
                return Ok(());
            };
            tx.insert_events(&[Event::new("record.rejected", &r.string(0))
                .load(r.i64(1))
                .record(id)
                .error_kind(kind)
                .reason(error)])
        })
    }

    /// Stores what one batch's answer decided for each of its records, in one
    /// transaction, with their events: `record.delivered` (or `record.recovered`
    /// after failed attempts), `record.rejected`, `record.attempt_failed`. Each
    /// changes only while its record is still `pending` at the item's revision
    /// and attempt count, so an older runner's answer can neither roll the count
    /// back nor log an attempt twice.
    pub fn delivered(&self, outcomes: &[(&WorkItem, Delivery)]) -> Result<()> {
        self.transaction(|tx| {
            let now = now_ms();
            let mut events = Vec::with_capacity(outcomes.len());
            for (item, outcome) in outcomes {
                let row = match outcome {
                    Delivery::Sent(response) => tx.query_one(
                        "UPDATE records SET status = 'sent', response = $2, error_kind = NULL, error = NULL, updated_at = $3
                         WHERE id = $1 AND status = 'pending' AND revision = $4 AND attempts = $5
                         RETURNING feed_id, file_id, attempts",
                        &params![item.id, response.to_string(), now, item.revision, item.attempts],
                    )?,
                    Delivery::Rejected {
                        kind,
                        error,
                        response,
                    } => tx.query_one(
                        "UPDATE records SET status = 'rejected', error_kind = $2, error = $3, response = $4, updated_at = $5
                         WHERE id = $1 AND status = 'pending' AND revision = $6 AND attempts = $7
                         RETURNING feed_id, file_id, attempts",
                        &params![item.id, kind, error, response.map(Value::to_string), now, item.revision, item.attempts],
                    )?,
                    Delivery::Deferred {
                        attempts,
                        next_attempt_at,
                        error,
                    } => tx.query_one(
                        "UPDATE records SET attempts = $2, next_attempt_at = $3, error_kind = 'transient', error = $4, updated_at = $5
                         WHERE id = $1 AND status = 'pending' AND revision = $6 AND attempts = $7
                         RETURNING feed_id, file_id, attempts",
                        &params![item.id, attempts, next_attempt_at, error, now, item.revision, item.attempts],
                    )?,
                };
                let Some(r) = row else {
                    continue;
                };
                let (feed, file_id, attempts) = (r.string(0), r.i64(1), r.i64(2));
                let base = |kind| Event::new(kind, &feed).load(file_id).record(item.id);
                events.push(match outcome {
                    Delivery::Sent(_) if attempts > 0 => base("record.recovered").attempt(attempts + 1),
                    Delivery::Sent(_) => base("record.delivered").attempt(attempts + 1),
                    Delivery::Rejected { kind, error, .. } => base("record.rejected")
                        .attempt(attempts + 1)
                        .error_kind(kind)
                        .reason(error),
                    Delivery::Deferred {
                        next_attempt_at,
                        error,
                        ..
                    } => base("record.attempt_failed")
                        .attempt(attempts)
                        .reason(error)
                        .detail(json!({"phase": "delivery", "next_attempt_at": iso_ms(*next_attempt_at)})),
                });
            }
            tx.insert_events(&events)
        })
    }

    /// Puts a record back at the start of the pipeline, optionally with corrected
    /// source data, with `record.retried`. Its file's row is locked first, as a
    /// Retry does. The record keeps the submission it was in, if any.
    pub fn reprocess(&self, id: i64, raw: Option<&Value>) -> Result<bool> {
        self.transaction(|tx| {
            let Some(file_id) = tx
                .query_one("SELECT file_id FROM records WHERE id = $1", &params![id])?
                .map(|r| r.i64(0))
            else {
                return Ok(false);
            };
            let lock_file = format!("SELECT id FROM files WHERE id = $1{}", tx.for_update());
            tx.query(&lock_file, &params![file_id])?;
            let lock_record = format!(
                "SELECT status, feed_id FROM records WHERE id = $1{}",
                tx.for_update()
            );
            let Some(before) = tx.query_one(&lock_record, &params![id])? else {
                return Ok(false);
            };
            let now = now_ms();
            match raw {
                Some(raw) => tx.execute(
                    "UPDATE records SET status = 'received', original_raw = COALESCE(original_raw, raw), raw = $2, resources = NULL,
                            error_kind = NULL, error = NULL, response = NULL, attempts = 0, next_attempt_at = 0,
                            revision = revision + 1, updated_at = $3
                     WHERE id = $1",
                    &params![id, raw.to_string(), now],
                )?,
                None => tx.execute(
                    "UPDATE records SET status = 'received', resources = NULL, error_kind = NULL, error = NULL,
                            response = NULL, attempts = 0, next_attempt_at = 0, revision = revision + 1, updated_at = $2
                     WHERE id = $1",
                    &params![id, now],
                )?,
            };
            tx.execute(
                "UPDATE files SET status = 'processing', finished_at = NULL WHERE id = $1",
                &params![file_id],
            )?;
            tx.insert_events(&[Event::new("record.retried", &before.string(1))
                .load(file_id)
                .record(id)
                .detail(json!({"from_status": before.string(0), "corrected": raw.is_some()}))])?;
            Ok(true)
        })
    }

    // ---- submissions ($bulk-submit) ------------------------------------------

    /// Files of a feed with pending records that no submission holds yet, as the
    /// manifest needs them: counted in the store, never listed record by record.
    pub fn submittable_files(&self, feed: &str, limit: usize) -> Result<Vec<SubmittableFile>> {
        let rows = self.query(
            "SELECT r.file_id, f.name, f.source_key, COUNT(*), f.origin
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.feed_id = $1 AND r.status = 'pending' AND r.submission IS NULL
             GROUP BY r.file_id, f.name, f.source_key, f.origin
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
                origin: r.opt_string(4),
                resource_type,
                count: r.i64(3) as usize,
            });
        }
        Ok(files)
    }

    /// Pending records of a feed that no submission holds yet, with their
    /// revision and resources: what the engine writes out itself on the
    /// transformed bulk path.
    pub fn submittable_records(&self, feed: &str, limit: usize) -> Result<Vec<(i64, i64, Value)>> {
        Ok(self
            .query(
                "SELECT id, revision, resources FROM records
                 WHERE feed_id = $1 AND status = 'pending' AND submission IS NULL
                 ORDER BY id LIMIT $2",
                &params![feed, limit],
            )?
            .iter()
            .map(|r| (r.i64(0), r.i64(1), r.json(2).unwrap_or(Value::Null)))
            .collect())
    }

    /// Opens a submission over every pending record of the given files that no
    /// submission holds, which move to `submitted` in one statement per file,
    /// with `submission.opened`. `files` names each file with the number of
    /// records its manifest entry was built for; if any file's claim differs
    /// (a record changed meanwhile), nothing is written and `false` comes back,
    /// so the caller builds the submission again from what is pending now.
    pub fn create_submission_for_files(
        &self,
        token: &str,
        feed: &str,
        submission_id: &str,
        manifest: &Value,
        files: &[(i64, usize)],
    ) -> Result<bool> {
        self.transaction(|tx| {
            let now = now_ms();
            insert_submission(tx, token, feed, submission_id, manifest, now)?;
            let mut records = 0;
            for (file_id, expected) in files {
                let claimed = tx.execute(
                    "UPDATE records SET status = 'submitted', submission = $1, updated_at = $2
                     WHERE file_id = $3 AND feed_id = $4 AND status = 'pending' AND submission IS NULL",
                    &params![token, now, file_id, feed],
                )? as usize;
                if claimed != *expected {
                    tx.rollback = true;
                    return Ok(false);
                }
                records += claimed;
            }
            tx.insert_events(&[opened(feed, token, submission_id, records, files.len())])?;
            Ok(true)
        })
    }

    /// Opens a submission over the given records, given as (id, revision), which
    /// move to `submitted`, with `submission.opened`. Each is claimed only while
    /// still pending at that revision and held by no submission; if any is not,
    /// nothing is written and `false` comes back, so the caller builds the
    /// submission again from what is pending now.
    pub fn create_submission(
        &self,
        token: &str,
        feed: &str,
        submission_id: &str,
        manifest: &Value,
        records: &[(i64, i64)],
    ) -> Result<bool> {
        self.transaction(|tx| {
            let now = now_ms();
            insert_submission(tx, token, feed, submission_id, manifest, now)?;
            let mut claimed = 0;
            let mut files = std::collections::BTreeSet::new();
            for chunk in records.chunks(INSERT_ROWS_PER_STATEMENT) {
                let rows: Vec<String> = (0..chunk.len())
                    .map(|i| {
                        format!(
                            "(CAST(${} AS BIGINT), CAST(${} AS BIGINT))",
                            i * 2 + 3,
                            i * 2 + 4
                        )
                    })
                    .collect();
                let mut values: Vec<Param> = Vec::with_capacity(chunk.len() * 2 + 2);
                values.push(Param::Text(token.to_string()));
                values.push(Param::Int(now));
                for (id, revision) in chunk {
                    values.push(Param::Int(*id));
                    values.push(Param::Int(*revision));
                }
                let taken = tx.query(
                    &format!(
                        "UPDATE records SET status = 'submitted', submission = $1, updated_at = $2
                         FROM (VALUES {}) AS v
                         WHERE records.id = v.column1 AND records.revision = v.column2
                           AND records.status = 'pending' AND records.submission IS NULL
                         RETURNING file_id",
                        rows.join(", ")
                    ),
                    &values,
                )?;
                claimed += taken.len();
                files.extend(taken.iter().map(|r| r.i64(0)));
            }
            if claimed < records.len() {
                tx.rollback = true;
                return Ok(false);
            }
            tx.insert_events(&[opened(feed, token, submission_id, claimed, files.len())])?;
            Ok(true)
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

    /// Moves an open submission on (kicked off, polled, still running). These
    /// steps write no event.
    pub fn update_submission(
        &self,
        token: &str,
        status: &str,
        status_url: Option<&str>,
        attempts: u32,
        next_attempt_at: i64,
        error: Option<&str>,
    ) -> Result<()> {
        self.progress_submission(
            token,
            None,
            status,
            status_url,
            attempts,
            next_attempt_at,
            error,
        )
        .map(drop)
    }

    /// `update_submission`, only while the submission still has the attempt
    /// count `captured` read before its request: the answer to an older request
    /// (a runner that held the source before a lease lapsed) changes nothing.
    /// Returns whether it changed.
    #[allow(clippy::too_many_arguments)]
    pub fn progress_submission(
        &self,
        token: &str,
        captured: Option<u32>,
        status: &str,
        status_url: Option<&str>,
        attempts: u32,
        next_attempt_at: i64,
        error: Option<&str>,
    ) -> Result<bool> {
        Ok(self.execute(
            "UPDATE submissions SET status = $2,
                    status_url = COALESCE(CAST($3 AS TEXT), status_url),
                    attempts = $4, next_attempt_at = $5, error = $6, updated_at = $7
             WHERE token = $1 AND status IN ('kickoff', 'running')
               AND (CAST($8 AS BIGINT) IS NULL OR attempts = CAST($8 AS BIGINT))",
            &params![
                token,
                status,
                status_url,
                attempts,
                next_attempt_at,
                error,
                now_ms(),
                captured
            ],
        )? > 0)
    }

    /// A kick-off or status poll failed for a reason that may pass: the next
    /// attempt is due at `next_attempt_at`, with `submission.attempt_failed`.
    /// `reason` is already scrubbed of URLs and the token. Only while the
    /// submission still has the attempt count `captured` read before the
    /// request; returns whether it changed.
    pub fn submission_attempt_failed(
        &self,
        token: &str,
        captured: u32,
        next_attempt_at: i64,
        reason: &str,
    ) -> Result<bool> {
        let attempts = captured + 1;
        self.transaction(|tx| {
            let Some(r) = tx.query_one(
                "UPDATE submissions SET attempts = $2, next_attempt_at = $3, error = $4, failures = failures + 1, updated_at = $5
                 WHERE token = $1 AND status IN ('kickoff', 'running') AND attempts = $6
                 RETURNING feed_id, submission_id",
                &params![token, attempts, next_attempt_at, reason, now_ms(), captured],
            )?
            else {
                return Ok(false);
            };
            tx.insert_events(&[Event::new("submission.attempt_failed", &r.string(0))
                .attempt(attempts as i64)
                .reason(reason)
                .detail(json!({"submission": r.string(1), "run": crate::bulk::run_key(token),
                               "next_attempt_at": iso_ms(next_attempt_at)}))])?;
            Ok(true)
        })
    }

    /// Settles one page of a submission's records from the receipts, in one
    /// transaction: `sent` with their responses, `missing` rejected with
    /// `error`, each only while still `submitted` under this token; the rows
    /// that changed are added to the submission's tallies, and each gets
    /// `record.delivered` (`record.recovered` when the submission had failed
    /// attempts) or `record.rejected`. Only while the submission is still open
    /// at the attempt count `captured` read before the poll: otherwise nothing
    /// changes and `false` comes back.
    pub fn settle_page(
        &self,
        token: &str,
        captured: u32,
        sent: &[(i64, Value)],
        missing: &[i64],
        error: &str,
    ) -> Result<bool> {
        self.transaction(|tx| {
            // The submission's row first, still open at the attempt count read
            // before the poll, as a refusal takes it: then its records.
            let lock = format!(
                "SELECT feed_id, submission_id, failures FROM submissions
                 WHERE token = $1 AND status IN ('kickoff', 'running') AND attempts = $2{}",
                tx.for_update()
            );
            let Some(sub) = tx.query_one(&lock, &params![token, captured])? else {
                return Ok(false);
            };
            let (feed, submission_id, failures) = (sub.string(0), sub.string(1), sub.i64(2));
            let now = now_ms();
            let mut delivered = Vec::new();
            for chunk in sent.chunks(INSERT_ROWS_PER_STATEMENT) {
                let rows: Vec<String> = (0..chunk.len())
                    .map(|i| {
                        format!(
                            "(CAST(${} AS BIGINT), CAST(${} AS TEXT))",
                            i * 2 + 3,
                            i * 2 + 4
                        )
                    })
                    .collect();
                let mut values: Vec<Param> = Vec::with_capacity(chunk.len() * 2 + 2);
                values.push(Param::Int(now));
                values.push(Param::Text(token.to_string()));
                for (id, response) in chunk {
                    values.push(Param::Int(*id));
                    values.push(Param::Text(response.to_string()));
                }
                delivered.extend(tx.query(
                    &format!(
                        "UPDATE records SET status = 'sent', response = v.column2, error_kind = NULL, error = NULL, updated_at = $1
                         FROM (VALUES {}) AS v
                         WHERE records.id = v.column1 AND records.status = 'submitted' AND records.submission = $2
                         RETURNING id, file_id",
                        rows.join(", ")
                    ),
                    &values,
                )?);
            }
            let mut rejected = Vec::new();
            for chunk in missing.chunks(INSERT_ROWS_PER_STATEMENT) {
                let mut values = params![now, token, error].to_vec();
                values.extend(chunk.iter().map(|id| Param::Int(*id)));
                rejected.extend(tx.query(
                    &format!(
                        "UPDATE records SET status = 'rejected', error_kind = 'server', error = $3, updated_at = $1
                         WHERE status = 'submitted' AND submission = $2 AND id IN ({})
                         RETURNING id, file_id",
                        marks(4, chunk.len())
                    ),
                    &values,
                )?);
            }
            tx.execute(
                "UPDATE submissions SET sent = sent + $2, rejected = rejected + $3 WHERE token = $1",
                &params![token, delivered.len(), rejected.len()],
            )?;
            let run = crate::bulk::run_key(token);
            let mut events: Vec<(i64, Event)> = Vec::with_capacity(delivered.len() + rejected.len());
            for r in &delivered {
                let event = if failures > 0 {
                    Event::new("record.recovered", &feed).detail(
                        json!({"submission": submission_id, "run": run, "failures": failures}),
                    )
                } else {
                    Event::new("record.delivered", &feed)
                        .detail(json!({"submission": submission_id, "run": run}))
                };
                events.push((r.i64(0), event.load(r.i64(1)).record(r.i64(0))));
            }
            for r in &rejected {
                events.push((
                    r.i64(0),
                    Event::new("record.rejected", &feed)
                        .load(r.i64(1))
                        .record(r.i64(0))
                        .error_kind("server")
                        .reason(error)
                        .detail(json!({"submission": submission_id, "run": run})),
                ));
            }
            events.sort_by_key(|(id, _)| *id);
            let events: Vec<Event> = events.into_iter().map(|(_, e)| e).collect();
            tx.insert_events(&events)?;
            Ok(true)
        })
    }

    /// Closes a submission whose receipts are all applied, with
    /// `submission.settled` and its tallies. Only a submission still open at the
    /// attempt count `captured` read before the poll moves, so the event is
    /// written once and an older poll's answer changes nothing. Returns whether
    /// it moved.
    pub fn settle_submission(&self, token: &str, captured: u32) -> Result<bool> {
        self.transaction(|tx| {
            let lock = format!(
                "SELECT token FROM submissions
                 WHERE token = $1 AND status IN ('kickoff', 'running') AND attempts = $2{}",
                tx.for_update()
            );
            if tx.query_one(&lock, &params![token, captured])?.is_none() {
                return Ok(false);
            }
            let Some(r) = tx.query_one(
                "UPDATE submissions SET status = 'done', attempts = 0, next_attempt_at = 0, error = NULL, updated_at = $2
                 WHERE token = $1 AND status IN ('kickoff', 'running')
                 RETURNING feed_id, submission_id, sent, rejected, failures",
                &params![token, now_ms()],
            )?
            else {
                return Ok(false);
            };
            tx.insert_events(&[Event::new("submission.settled", &r.string(0)).detail(json!({
                "submission": r.string(1),
                "run": crate::bulk::run_key(token),
                "sent": r.i64(2),
                "rejected": r.i64(3),
                "failures": r.i64(4),
            }))])?;
            Ok(true)
        })
    }

    /// The server refused a submission: it fails, and every record it still
    /// holds is rejected with `reason` and the server's `response`, in one
    /// transaction, with a `record.rejected` each and one `submission.refused`.
    /// `reason` and `response` are already scrubbed; the events never carry the
    /// response. Nothing happens unless the submission is open at the attempt
    /// count `captured` read before the request (an older request's answer
    /// changes nothing). Returns how many records were rejected, or `None`.
    pub fn reject_submission(
        &self,
        token: &str,
        captured: u32,
        kind: &str,
        reason: &str,
        response: &Value,
    ) -> Result<Option<usize>> {
        self.transaction(|tx| {
            let now = now_ms();
            let Some(sub) = tx.query_one(
                "UPDATE submissions SET status = 'failed', next_attempt_at = 0, error = $2, updated_at = $3
                 WHERE token = $1 AND status IN ('kickoff', 'running') AND attempts = $4
                 RETURNING feed_id, submission_id",
                &params![token, reason, now, captured],
            )?
            else {
                return Ok(None);
            };
            let (feed, submission_id) = (sub.string(0), sub.string(1));
            let rows = update_records_paged(
                tx,
                "submission = $1 AND status = 'submitted'",
                &params![token],
                "status = 'rejected', error_kind = $2, error = $3, response = $4, updated_at = $5",
                &params![kind, reason, response.to_string(), now],
                "id, file_id",
            )?;
            let text = crate::events::reason_text(reason);
            let detail = json!({"submission": submission_id, "run": crate::bulk::run_key(token)});
            tx.insert_each(&rows, |(id, file_id)| {
                Event::new("record.rejected", &feed)
                    .load(*file_id)
                    .record(*id)
                    .error_kind(kind)
                    .prepared_reason(&text)
                    .detail(detail.clone())
            })?;
            let mut refused = detail.clone();
            refused["records"] = json!(rows.len());
            tx.insert_events(&[Event::new("submission.refused", &feed)
                .prepared_reason(&text)
                .detail(refused)])?;
            Ok(Some(rows.len()))
        })
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

    /// The source key and origin of a file, only when that file is part of the submission.
    pub fn submission_file(
        &self,
        token: &str,
        file_id: i64,
    ) -> Result<Option<(String, Option<String>)>> {
        Ok(self
            .query_one(
                "SELECT f.source_key, f.origin FROM files f
                 WHERE f.id = $2 AND f.source_key IS NOT NULL
                   AND EXISTS (SELECT 1 FROM records r WHERE r.file_id = f.id AND r.submission = $1)",
                &params![token, file_id],
            )?
            .map(|r| (r.string(0), r.opt_string(1))))
    }

    /// Rejects a file's pending records that no submission holds: the file cannot
    /// be handed over at all. Each gets `record.rejected`. Returns how many.
    pub fn reject_unsubmitted(&self, file_id: i64, kind: &str, error: &str) -> Result<usize> {
        self.transaction(|tx| {
            let Some(feed) = tx
                .query_one("SELECT feed_id FROM files WHERE id = $1", &params![file_id])?
                .map(|r| r.string(0))
            else {
                return Ok(0);
            };
            let ids = update_records_paged(
                tx,
                "file_id = $1 AND status = 'pending' AND submission IS NULL",
                &params![file_id],
                "status = 'rejected', error_kind = $2, error = $3, updated_at = $4",
                &params![kind, error, now_ms()],
                "id",
            )?;
            let text = crate::events::reason_text(error);
            tx.insert_each(&ids, |(id, _)| {
                Event::new("record.rejected", &feed)
                    .load(file_id)
                    .record(*id)
                    .error_kind(kind)
                    .prepared_reason(&text)
            })?;
            Ok(ids.len())
        })
    }

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

    pub fn last_alert_at(&self, feed: &str, kind: &str) -> Result<Option<i64>> {
        Ok(self
            .query_one(
                "SELECT MAX(created_at) FROM alerts WHERE feed_id = $1 AND kind = $2",
                &params![feed, kind],
            )?
            .and_then(|r| r.opt_i64(0)))
    }

    // ---- alerts ----------------------------------------------------------------

    /// The columns `AlertRow` is read from, as a `RETURNING` or `SELECT` list.
    fn alert_columns(&self) -> String {
        format!(
            "id, feed_id, kind, message, rule_id, notify, channels, {} AS created_at, {} AS cleared_at, cleared_reason",
            self.iso("created_at"),
            self.iso("cleared_at")
        )
    }

    fn alert_row(r: &Row) -> AlertRow {
        let list = |i: usize| -> Option<Vec<String>> {
            r.json(i).and_then(|v| serde_json::from_value(v).ok())
        };
        AlertRow {
            id: r.i64(0),
            feed_id: r.opt_string(1),
            kind: r.string(2),
            message: r.string(3),
            rule_id: r.opt_i64(4),
            notify: list(5).unwrap_or_default(),
            channels: list(6),
            created_at: r.string(7),
            cleared_at: r.opt_string(8),
            cleared_reason: r.opt_string(9),
        }
    }

    fn insert_webhook(c: &mut Conn, alert_id: i64, w: &NewWebhook, now: i64) -> Result<()> {
        c.execute(
            "INSERT INTO alert_webhooks (event_id, alert_id, event, destination, payload, state, attempts, next_attempt_at, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $7)",
            &params![w.event_id, alert_id, w.event, w.destination, w.payload, w.state, now],
        )
        .map(drop)
    }

    /// Records an alert with no webhook event, as alerts were before events
    /// were: for tests and imports. The engine raises alerts through `Alerter`.
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

    /// Clears an open alert with no webhook event: for tests and imports, like
    /// `insert_alert`.
    pub fn clear_alert(&self, id: i64, reason: &str) -> Result<()> {
        self.execute(
            "UPDATE alerts SET cleared_at = $2, cleared_reason = $3 WHERE id = $1 AND cleared_at IS NULL",
            &params![id, now_ms(), reason],
        )
        .map(drop)
    }

    /// Records an alert and, in the same transaction, the webhook event `webhook`
    /// builds from the stored row. An alert of a rule is recorded only while
    /// the rule is still on and watches what it did when it was read (its
    /// condition, threshold and source): `None` when it does not any more. On
    /// PostgreSQL the rule's row stays locked to the commit, so a change of the
    /// rule waits for the alert and then clears it.
    pub fn insert_alert_with_webhook(
        &self,
        feed: Option<&str>,
        kind: &str,
        message: &str,
        rule: Option<&Rule>,
        now: i64,
        webhook: impl FnOnce(&AlertRow) -> NewWebhook,
    ) -> Result<Option<AlertRow>> {
        let columns = self.alert_columns();
        let postgres = self.postgres;
        self.with(|c| {
            // Immediate on SQLite: the rule is read under the write lock.
            c.batch(if postgres { "BEGIN" } else { "BEGIN IMMEDIATE" })?;
            let result = (|| {
                if let Some(rule) = rule {
                    let lock = if postgres { " FOR SHARE" } else { "" };
                    let current = c.query(
                        &format!(
                            "SELECT 1 FROM alert_rules
                             WHERE id = $1 AND enabled = 1 AND condition = $2 AND threshold = $3
                               AND source_id IS NOT DISTINCT FROM $4{lock}"
                        ),
                        &params![rule.id, rule.condition, rule.threshold, rule.source_id],
                    )?;
                    if current.is_empty() {
                        return Ok(None);
                    }
                }
                let row = c
                    .query(
                        &format!(
                            "INSERT INTO alerts (feed_id, kind, message, created_at, rule_id, notify, channels)
                             VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING {columns}"
                        ),
                        &params![
                            feed,
                            kind,
                            message,
                            now,
                            rule.map(|r| r.id),
                            rule.map(|r| serde_json::to_string(&r.notify).unwrap_or_default()),
                            rule.map(|r| serde_json::to_string(&r.channels).unwrap_or_default()),
                        ],
                    )?
                    .first()
                    .map(Self::alert_row)
                    .context("INSERT returned no alert")?;
                Self::insert_webhook(c, row.id, &webhook(&row), now)?;
                Ok::<_, anyhow::Error>(Some(row))
            })();
            match result {
                Ok(row) => match c.batch("COMMIT") {
                    Ok(()) => Ok(row),
                    Err(e) => {
                        let _ = c.batch("ROLLBACK");
                        Err(e)
                    }
                },
                Err(e) => {
                    let _ = c.batch("ROLLBACK");
                    Err(e)
                }
            }
        })
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

    /// Clears an open alert and, in the same transaction, records the webhook
    /// event `webhook` builds from the cleared row. `None` when the alert was
    /// already cleared (by another instance, say): then nothing is written.
    pub fn clear_alert_with_webhook(
        &self,
        id: i64,
        reason: &str,
        now: i64,
        webhook: impl FnOnce(&AlertRow) -> NewWebhook,
    ) -> Result<Option<AlertRow>> {
        let columns = self.alert_columns();
        self.with(|c| {
            c.batch("BEGIN")?;
            let result = (|| {
                let Some(row) = c
                    .query(
                        &format!(
                            "UPDATE alerts SET cleared_at = $2, cleared_reason = $3
                             WHERE id = $1 AND cleared_at IS NULL RETURNING {columns}"
                        ),
                        &params![id, now, reason],
                    )?
                    .first()
                    .map(Self::alert_row)
                else {
                    return Ok(None);
                };
                Self::insert_webhook(c, row.id, &webhook(&row), now)?;
                Ok::<_, anyhow::Error>(Some(row))
            })();
            match result {
                Ok(row) => match c.batch("COMMIT") {
                    Ok(()) => Ok(row),
                    Err(e) => {
                        let _ = c.batch("ROLLBACK");
                        Err(e)
                    }
                },
                Err(e) => {
                    let _ = c.batch("ROLLBACK");
                    Err(e)
                }
            }
        })
    }

    /// The open alerts a rule raised, oldest first: (id, source, kind).
    pub fn open_alerts_of_rule(&self, rule_id: i64) -> Result<Vec<(i64, Option<String>, String)>> {
        Ok(self
            .query(
                "SELECT id, feed_id, kind FROM alerts WHERE rule_id = $1 AND cleared_at IS NULL ORDER BY id",
                &params![rule_id],
            )?
            .iter()
            .map(|r| (r.i64(0), r.opt_string(1), r.string(2)))
            .collect())
    }

    /// The open alerts whose rule no longer watches them, oldest first, each
    /// with why: the rule was deleted, turned off, or changed to another
    /// condition or to a source that is not the alert's, alerts about no
    /// source included. A rule made after the alert is another rule given the
    /// deleted one's id (a SQLite store before ids came from `AUTOINCREMENT`),
    /// as retention reads it. A change of threshold alone cannot be told from
    /// here.
    pub fn unwatched_alerts(&self) -> Result<Vec<UnwatchedAlert>> {
        Ok(self
            .query(
                "SELECT a.id, a.feed_id, a.kind,
                    CASE WHEN r.id IS NULL OR r.created_at > a.created_at THEN 'deleted'
                         WHEN r.enabled = 0 THEN 'off' ELSE 'changed' END
             FROM alerts a LEFT JOIN alert_rules r ON r.id = a.rule_id
             WHERE a.cleared_at IS NULL AND a.rule_id IS NOT NULL
               AND (r.id IS NULL OR r.created_at > a.created_at OR r.enabled = 0
                    OR (r.source_id IS NOT NULL AND (a.feed_id IS NULL OR r.source_id <> a.feed_id))
                    OR a.kind <> CASE r.condition WHEN 'unreachable' THEN 'delivery'
                                                  WHEN 'missing_by' THEN 'missing'
                                                  ELSE r.condition END)
             ORDER BY a.id",
                &[],
            )?
            .iter()
            .map(|r| UnwatchedAlert {
                id: r.i64(0),
                feed: r.opt_string(1),
                kind: r.string(2),
                why: match r.string(3).as_str() {
                    "deleted" => RuleGone::Deleted,
                    "off" => RuleGone::TurnedOff,
                    _ => RuleGone::Changed,
                },
            })
            .collect())
    }

    /// The open alerts of a kind for a source, with a rule or without, oldest first.
    pub fn open_alert_ids(&self, feed: &str, kind: &str) -> Result<Vec<i64>> {
        Ok(self
            .query(
                "SELECT id FROM alerts WHERE feed_id = $1 AND kind = $2 AND cleared_at IS NULL ORDER BY id",
                &params![feed, kind],
            )?
            .iter()
            .map(|r| r.i64(0))
            .collect())
    }

    /// Closes what versions before recovery cleared the `delivery` alert left
    /// open, once per store: every `delivery_recovered` alert (a notice, never
    /// a condition), and every rule-less `delivery` alert shown to be over, by
    /// a later `delivery_recovered` of its source or by a record of the source
    /// delivered after it. A rule-less `delivery` alert with neither may be an
    /// outage still going on: it stays, for the next successful delivery to
    /// clear. Later starts change nothing, so an outage alert this version
    /// raised is cleared only by its recovery. Only the store changes. Returns
    /// how many alerts were closed.
    pub fn close_legacy_recoveries(&self) -> Result<u64> {
        let now = now_ms();
        self.transaction(|tx| {
            let first = tx.execute(
                "INSERT INTO store_meta (key, value) VALUES ('delivery_alerts_reconciled', $1)
                 ON CONFLICT (key) DO NOTHING",
                &params![now.to_string()],
            )?;
            if first == 0 {
                return Ok(0);
            }
            let over = tx.execute(
                "UPDATE alerts SET cleared_at = $1, cleared_reason = $2
                 WHERE kind = 'delivery' AND rule_id IS NULL AND cleared_at IS NULL AND feed_id IS NOT NULL
                   AND (EXISTS (SELECT 1 FROM alerts later WHERE later.feed_id = alerts.feed_id
                                AND later.kind = 'delivery_recovered' AND later.id > alerts.id)
                        OR EXISTS (SELECT 1 FROM records r WHERE r.feed_id = alerts.feed_id
                                   AND r.status = 'sent' AND r.updated_at > alerts.created_at))",
                &params![
                    now,
                    "delivery to HFS recovered (recorded before recovery cleared this alert)"
                ],
            )?;
            let notices = tx.execute(
                "UPDATE alerts SET cleared_at = $1, cleared_reason = $2
                 WHERE kind = 'delivery_recovered' AND cleared_at IS NULL",
                &params![
                    now,
                    "a recovery notice; recovery now clears the delivery alert instead"
                ],
            )?;
            Ok(over + notices)
        })
    }

    // ---- alert webhook deliveries --------------------------------------------

    /// Ends the rows whose last allowed attempt was claimed and never recorded
    /// (the process died during it): `failed`, never sent again. Returns how many.
    pub fn webhook_expire_exhausted(&self, now: i64, max_attempts: u32) -> Result<u64> {
        self.execute(
            "UPDATE alert_webhooks SET state = 'failed', last_error = $1, last_status = NULL
             WHERE state = 'pending' AND attempts >= $2 AND next_attempt_at <= $3",
            &params![
                "no outcome recorded for the last attempt",
                max_attempts,
                now
            ],
        )
    }

    /// The events for `destination` that may be attempted now, oldest first.
    pub fn webhook_due(
        &self,
        destination: &str,
        now: i64,
        max_attempts: u32,
        limit: usize,
    ) -> Result<Vec<String>> {
        Ok(self
            .query(
                &format!(
                    "SELECT event_id FROM alert_webhooks
                     WHERE state = 'pending' AND destination = $1 AND next_attempt_at <= $2
                       AND attempts < $3 AND {WEBHOOK_UNBLOCKED}
                     ORDER BY id LIMIT $4"
                ),
                &params![destination, now, max_attempts, limit],
            )?
            .iter()
            .map(|r| r.string(0))
            .collect())
    }

    /// When the next event for `destination` falls due (or a claim on one runs
    /// out), counting only events the ordering rule does not hold back.
    pub fn webhook_next_due(&self, destination: &str) -> Result<Option<i64>> {
        Ok(self
            .query_one(
                &format!(
                    "SELECT MIN(next_attempt_at) FROM alert_webhooks
                     WHERE state = 'pending' AND destination = $1 AND {WEBHOOK_UNBLOCKED}"
                ),
                &params![destination],
            )?
            .and_then(|r| r.opt_i64(0)))
    }

    /// Claims the next attempt of an event until `lease_until`: one statement,
    /// so of several instances on the store only one gets it. `None` when the
    /// event is not due (any more), is held back, or belongs to another
    /// destination.
    pub fn webhook_claim(
        &self,
        event_id: &str,
        destination: &str,
        now: i64,
        lease_until: i64,
        max_attempts: u32,
    ) -> Result<Option<WebhookClaim>> {
        Ok(self
            .query_one(
                &format!(
                    "UPDATE alert_webhooks SET attempts = attempts + 1, next_attempt_at = $3, last_status = NULL
                     WHERE event_id = $1 AND destination = $2 AND state = 'pending'
                       AND next_attempt_at <= $4 AND attempts < $5 AND {WEBHOOK_UNBLOCKED}
                     RETURNING event_id, alert_id, event, attempts, payload"
                ),
                &params![event_id, destination, lease_until, now, max_attempts],
            )?
            .map(|r| WebhookClaim {
                event_id: r.string(0),
                alert_id: r.i64(1),
                event: r.string(2),
                attempt: r.i64(3) as u32,
                payload: r.string(4),
            }))
    }

    /// Records that a claimed attempt was delivered. `false` when the claim is
    /// no longer this attempt's (it ran out and was taken again, or the alert
    /// was removed): nothing changes.
    pub fn webhook_delivered(
        &self,
        event_id: &str,
        attempt: u32,
        status: u16,
        now: i64,
    ) -> Result<bool> {
        Ok(self.execute(
            "UPDATE alert_webhooks SET state = 'delivered', delivered_at = $3, last_status = $4
             WHERE event_id = $1 AND state = 'pending' AND attempts = $2",
            &params![event_id, attempt, now, status as i64],
        )? > 0)
    }

    /// Records a failed claimed attempt, with the receiver's HTTP status when
    /// it answered: due again at `retry_at`, or `failed` for good when there is
    /// none. Fenced like `webhook_delivered`.
    pub fn webhook_attempt_failed(
        &self,
        event_id: &str,
        attempt: u32,
        error: &str,
        status: Option<u16>,
        retry_at: Option<i64>,
    ) -> Result<bool> {
        let status = status.map(i64::from);
        let n = match retry_at {
            Some(at) => self.execute(
                "UPDATE alert_webhooks SET last_error = $3, last_status = $4, next_attempt_at = $5
                 WHERE event_id = $1 AND state = 'pending' AND attempts = $2",
                &params![event_id, attempt, error, status, at],
            )?,
            None => self.execute(
                "UPDATE alert_webhooks SET state = 'failed', last_error = $3, last_status = $4
                 WHERE event_id = $1 AND state = 'pending' AND attempts = $2",
                &params![event_id, attempt, error, status],
            )?,
        };
        Ok(n > 0)
    }

    /// Alerts raised so far, by source and kind, the purged ones included, so
    /// the totals never go down: (source or "", kind, count).
    pub fn alert_totals(&self) -> Result<Vec<(String, String, i64)>> {
        Ok(self
            .query(
                "SELECT source, kind, CAST(SUM(n) AS BIGINT) FROM (
                   SELECT COALESCE(feed_id, '') AS source, kind, COUNT(*) AS n FROM alerts GROUP BY feed_id, kind
                   UNION ALL
                   SELECT feed_id AS source, kind, n FROM alert_counts_purged
                 ) t GROUP BY source, kind ORDER BY source, kind",
                &[],
            )?
            .iter()
            .map(|r| (r.string(0), r.string(1), r.i64(2)))
            .collect())
    }

    // ---- history: events and their retention --------------------------------

    /// Whether a submission is still open at the attempt count `captured`.
    pub fn submission_open_at(&self, token: &str, captured: u32) -> Result<bool> {
        Ok(self
            .query_one(
                "SELECT 1 FROM submissions WHERE token = $1 AND status IN ('kickoff', 'running') AND attempts = $2",
                &params![token, captured],
            )?
            .is_some())
    }

    /// When the event log began on this store, in epoch milliseconds.
    pub fn events_since(&self) -> Result<Option<i64>> {
        Ok(self
            .query_one(
                "SELECT value FROM store_meta WHERE key = 'events_since'",
                &[],
            )?
            .and_then(|r| r.string(0).parse().ok()))
    }

    /// Distinct records of a source with a `record.rejected` event stored in
    /// `[from, to)`: those rejected for any reason but `filtered`, and those
    /// filtered out. A record rejected twice counts once.
    pub fn rejected_records(&self, source: &str, from: i64, to: i64) -> Result<(i64, i64)> {
        Ok(self
            .query_one(
                "SELECT COUNT(DISTINCT CASE WHEN COALESCE(error_kind, '') <> 'filtered' THEN record_id END),
                        COUNT(DISTINCT CASE WHEN error_kind = 'filtered' THEN record_id END)
                 FROM events
                 WHERE source_id = $1 AND kind = 'record.rejected' AND at >= $2 AND at < $3",
                &params![source, from, to],
            )?
            .map_or((0, 0), |r| (r.i64(0), r.i64(1))))
    }

    /// A page of events, oldest first by id: see `docs/api.md` (Events). The
    /// newest id is read first and nothing past it is listed, so a page never
    /// skips an event that commits later with a lower id.
    pub fn list_events(&self, q: &EventQuery) -> Result<EventPage> {
        let mark = self
            .query_one("SELECT MAX(id) FROM events", &[])?
            .and_then(|r| r.opt_i64(0));
        let Some(mark) = mark else {
            return Ok(EventPage {
                events: vec![],
                next: q.cursor,
                has_more: false,
            });
        };
        let forward = |next: i64| Some(q.cursor.map_or(next, |c| c.max(next)));
        // Without a cursor, `since` gives where to start, itself included.
        let (start, inclusive) = match (q.cursor, q.since) {
            (Some(cursor), _) => (cursor, false),
            (None, Some(since)) => match self
                .query_one("SELECT MIN(id) FROM events WHERE at >= $1", &params![since])?
                .and_then(|r| r.opt_i64(0))
            {
                Some(first) => (first, true),
                None => {
                    return Ok(EventPage {
                        events: vec![],
                        next: forward(mark),
                        has_more: false,
                    });
                }
            },
            (None, None) => (0, false),
        };
        let mut sql = format!(
            "SELECT id, {} AS at, source_id, kind, load_id, record_id, attempt, error_kind, reason, detail
             FROM events WHERE id {} $1 AND id <= $2",
            self.iso("at"),
            if inclusive { ">=" } else { ">" }
        );
        let mut values = params![start, mark].to_vec();
        if let Some(source) = &q.source {
            values.push(Param::Text(source.clone()));
            sql.push_str(&format!(" AND source_id = ${}", values.len()));
        }
        if let Some(since) = q.since {
            values.push(Param::Int(since));
            sql.push_str(&format!(" AND at >= ${}", values.len()));
        }
        if !q.kinds.is_empty() {
            let mut any = Vec::new();
            for kind in &q.kinds {
                if crate::events::FAMILIES.contains(&kind.as_str()) {
                    any.push(format!("kind LIKE '{kind}.%'"));
                } else {
                    values.push(Param::Text(kind.clone()));
                    any.push(format!("kind = ${}", values.len()));
                }
            }
            sql.push_str(&format!(" AND ({})", any.join(" OR ")));
        }
        values.push(Param::Int(q.limit as i64 + 1));
        sql.push_str(&format!(" ORDER BY id LIMIT ${}", values.len()));
        let mut events: Vec<Value> = self
            .query(&sql, &values)?
            .iter()
            .map(|r| {
                let mut e = r.object();
                if let Some(Value::String(text)) = e.get("detail") {
                    e["detail"] = serde_json::from_str(text).unwrap_or(Value::Null);
                }
                e
            })
            .collect();
        let has_more = events.len() > q.limit;
        events.truncate(q.limit);
        let next = match events.last() {
            Some(last) if has_more => last["id"].as_i64(),
            _ => forward(mark),
        };
        Ok(EventPage {
            events,
            next,
            has_more,
        })
    }

    /// Removes up to `limit` events older than `cutoff_ms` in one transaction.
    /// Returns how many went.
    pub fn purge_events(&self, cutoff_ms: i64, limit: usize) -> Result<usize> {
        self.transaction(|tx| {
            Ok(tx
                .query(
                    "DELETE FROM events WHERE id IN (SELECT id FROM events WHERE at < $1 ORDER BY id LIMIT $2)
                     RETURNING id",
                    &params![cutoff_ms, limit],
                )?
                .len())
        })
    }

    /// Removes up to `limit` alerts older than `cutoff_ms` (since they cleared, or
    /// since they were raised while open) in one transaction, and adds what went
    /// to the purged tallies `/metrics` counts. Kept at any age: open alerts of a
    /// rule that exists, is enabled and is older than the alert (a SQLite rule id
    /// may be given again after a rule is deleted), open `credential` and `definition`
    /// alerts, and the alert with the highest id, so no alert id is ever given
    /// again. Returns how many went.
    pub fn purge_alerts(&self, cutoff_ms: i64, limit: usize) -> Result<usize> {
        self.transaction(|tx| {
            // The predicate is checked again on the row deleted: on PostgreSQL an
            // alert cleared meanwhile is read again once its lock is free.
            let eligible = |a: &str| {
                format!(
                    "COALESCE({a}.cleared_at, {a}.created_at) < $1
                     AND {a}.id < (SELECT MAX(id) FROM alerts)
                     AND NOT ({a}.cleared_at IS NULL AND (
                           {a}.kind IN ('credential', 'definition')
                           OR ({a}.kind = 'delivery' AND {a}.rule_id IS NULL)
                           OR EXISTS (SELECT 1 FROM alert_rules r
                                      WHERE r.id = {a}.rule_id AND r.enabled = 1
                                        AND r.created_at <= {a}.created_at)))"
                )
            };
            let gone = tx.query(
                &format!(
                    "DELETE FROM alerts WHERE id IN (
                       SELECT a.id FROM alerts a WHERE {} ORDER BY a.id LIMIT $2)
                     AND {}
                     RETURNING feed_id, kind",
                    eligible("a"),
                    eligible("alerts")
                ),
                &params![cutoff_ms, limit],
            )?;
            let mut tallies: std::collections::BTreeMap<(String, String), i64> = Default::default();
            for r in &gone {
                *tallies.entry((r.string(0), r.string(1))).or_default() += 1;
            }
            for ((feed, kind), n) in tallies {
                tx.execute(
                    "INSERT INTO alert_counts_purged (feed_id, kind, n) VALUES ($1, $2, $3)
                     ON CONFLICT(feed_id, kind) DO UPDATE SET n = alert_counts_purged.n + excluded.n",
                    &params![feed, kind, n],
                )?;
            }
            Ok(gone.len())
        })
    }

    // ---- checks ------------------------------------------------------------

    /// Records a check of a source, pending, with its baseline: the source's
    /// newest load when it was asked for.
    pub fn insert_check(&self, feed: &str) -> Result<i64> {
        // One statement: no load can be recorded between the baseline and the check.
        self.insert_returning_id(
            "INSERT INTO checks (feed_id, status, requested_at, after_file)
             VALUES ($1, 'pending', $2, (SELECT COALESCE(MAX(id), 0) FROM files WHERE feed_id = $1))",
            &params![feed, now_ms()],
        )
    }

    fn max_file_id(&self, feed: &str) -> Result<i64> {
        Ok(self
            .query_one(
                "SELECT MAX(id) FROM files WHERE feed_id = $1",
                &params![feed],
            )?
            .and_then(|r| r.opt_i64(0))
            .unwrap_or(0))
    }

    /// The source's checks waiting for an answer, as they are now: what the scan
    /// about to start answers. Checks recorded after this wait for the next one.
    pub fn pending_checks(&self, feed: &str) -> Result<Vec<i64>> {
        Ok(self
            .query(
                "SELECT id FROM checks WHERE feed_id = $1 AND status = 'pending' ORDER BY id",
                &params![feed],
            )?
            .iter()
            .map(|r| r.i64(0))
            .collect())
    }

    /// Answers the checks of `snapshot` still pending: `failed` with the
    /// failure's fixed reason, or else `found` or `nothing_new` by the loads the
    /// source recorded after each check's baseline, up to now. A check another
    /// pass answered first keeps that answer.
    pub fn answer_checks(
        &self,
        feed: &str,
        snapshot: &[i64],
        failure: Option<CheckFailure>,
    ) -> Result<()> {
        if snapshot.is_empty() {
            return Ok(());
        }
        const COUNT: &str = "(SELECT COUNT(*) FROM files WHERE files.feed_id = checks.feed_id
                               AND files.id > checks.after_file AND files.id <= $3)";
        let upto = self.max_file_id(feed)?;
        let now = now_ms();
        let status = match failure {
            Some(_) => "'failed'".to_string(),
            None => format!("CASE WHEN {COUNT} > 0 THEN 'found' ELSE 'nothing_new' END"),
        };
        let sql = format!(
            "UPDATE checks SET status = {status}, file_count = {COUNT}, upto_file = $3,
               finished_at = CASE WHEN requested_at > $4 THEN requested_at ELSE $4 END,
               reason = $5
             WHERE id = $1 AND feed_id = $2 AND status = 'pending'"
        );
        for &id in snapshot {
            self.execute(
                &sql,
                &params![id, feed, upto, now, failure.map(CheckFailure::reason)],
            )?;
        }
        Ok(())
    }

    /// Fails every pending check of the source with the failure's fixed reason,
    /// with the loads since each was asked for, unless an instance other than
    /// `owner` held the source when the statement ran: that one may still answer
    /// them.
    pub fn fail_pending_checks(
        &self,
        feed: &str,
        owner: &str,
        failure: CheckFailure,
    ) -> Result<u64> {
        self.execute(
            "UPDATE checks SET status = 'failed',
               file_count = (SELECT COUNT(*) FROM files
                             WHERE files.feed_id = checks.feed_id AND files.id > checks.after_file),
               upto_file = (SELECT COALESCE(MAX(id), 0) FROM files WHERE feed_id = $1),
               finished_at = CASE WHEN requested_at > $4 THEN requested_at ELSE $4 END,
               reason = $2
             WHERE feed_id = $1 AND status = 'pending'
               AND NOT EXISTS (SELECT 1 FROM source_leases
                               WHERE feed_id = $1 AND owner <> $3 AND until >= $4)",
            &params![feed, failure.reason(), owner, now_ms()],
        )
    }

    /// One check, with up to `limit` of its loads after load `after` (each with
    /// its current status), or `None` when there is no such check.
    pub fn check(&self, id: i64, after: i64, limit: i64) -> Result<Option<Value>> {
        let Some(mut check) = self
            .query_json(
                &format!(
                    "SELECT id AS check_id, feed_id AS source, status, {} AS requested_at, {} AS finished_at,
                            file_count, reason, after_file, upto_file
                     FROM checks WHERE id = $1",
                    self.iso("requested_at"),
                    self.iso("finished_at")
                ),
                params![id],
            )?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let from = check["after_file"].as_i64().unwrap_or(0).max(after);
        let files = match check["upto_file"].as_i64() {
            Some(upto) => self.query_json(
                "SELECT id, name, status, duplicate_of FROM files
                 WHERE feed_id = $1 AND id > $2 AND id <= $3 ORDER BY id LIMIT $4",
                params![
                    check["source"].as_str().unwrap_or(""),
                    from,
                    upto,
                    limit + 1
                ],
            )?,
            None => Vec::new(),
        };
        let more = files.len() as i64 > limit;
        let files: Vec<Value> = files.into_iter().take(limit as usize).collect();
        let next = if more {
            files.last().and_then(|f| f["id"].as_i64())
        } else {
            None
        };
        let object = check.as_object_mut().expect("a row is an object");
        object.remove("after_file");
        object.remove("upto_file");
        if object["file_count"].is_null() {
            object.insert("file_count".into(), json!(0));
        }
        object.insert("files".into(), Value::Array(files));
        object.insert("next_after".into(), json!(next));
        Ok(Some(check))
    }

    /// Removes up to `limit` checks asked for before `cutoff_ms`, answered or
    /// not, in one transaction; returns how many went. Their ids are never
    /// given again (`AUTOINCREMENT`, a sequence on PostgreSQL).
    pub fn purge_checks(&self, cutoff_ms: i64, limit: usize) -> Result<usize> {
        self.transaction(|tx| {
            // Locked as they are chosen: on PostgreSQL a purge running at the
            // same time waits, then chooses the next rows instead of counting
            // none and stopping early.
            Ok(tx
                .query(
                    &format!(
                        "DELETE FROM checks WHERE id IN (SELECT id FROM checks WHERE requested_at < $1 ORDER BY id LIMIT $2{})
                         RETURNING id",
                        tx.for_update()
                    ),
                    &params![cutoff_ms, limit],
                )?
                .len())
        })
    }

    // ---- alert rules -------------------------------------------------------

    const RULE_SELECT: &'static str = "SELECT id, source_id, condition, threshold, notify, channels, enabled, created_by, created_at FROM alert_rules";

    fn rule_from(r: &Row) -> Rule {
        Rule {
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
            created_at: r.opt_i64(8).and_then(iso_ms),
        }
    }

    fn rule_rows(&self, sql: &str, p: &[Param]) -> Result<Vec<Rule>> {
        Ok(self.query(sql, p)?.iter().map(Self::rule_from).collect())
    }

    /// Replaces a rule and, in the same transaction, clears its open alerts
    /// with the reason `ends` gives from the rule as it was (`None`: they stay),
    /// each with the webhook event `webhook` builds from the cleared row. `None`
    /// when there is no such rule. The rule's row is locked first, so an alert
    /// being raised under it (`insert_alert_with_webhook`) is either committed
    /// before, and cleared here, or checked against the rule as replaced.
    pub fn update_rule_ending_alerts(
        &self,
        id: i64,
        rule: &Rule,
        now: i64,
        ends: impl FnOnce(&Rule) -> Option<&'static str>,
        webhook: impl FnMut(&AlertRow) -> NewWebhook,
    ) -> Result<Option<Vec<AlertRow>>> {
        let columns = self.alert_columns();
        self.transaction(|tx| {
            let lock = tx.for_update();
            let Some(before) = tx
                .query_one(
                    &format!("{} WHERE id = $1{lock}", Self::RULE_SELECT),
                    &params![id],
                )?
                .map(|r| Self::rule_from(&r))
            else {
                return Ok(None);
            };
            tx.execute(
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
            match ends(&before) {
                Some(reason) => Self::clear_rule_alerts(tx, &columns, id, reason, now, webhook).map(Some),
                None => Ok(Some(vec![])),
            }
        })
    }

    /// Deletes a rule and, in the same transaction, clears its open alerts
    /// with `reason`, as `update_rule_ending_alerts` does. `None` when there is
    /// no such rule.
    pub fn delete_rule_ending_alerts(
        &self,
        id: i64,
        reason: &str,
        now: i64,
        webhook: impl FnMut(&AlertRow) -> NewWebhook,
    ) -> Result<Option<Vec<AlertRow>>> {
        let columns = self.alert_columns();
        self.transaction(|tx| {
            if tx.execute("DELETE FROM alert_rules WHERE id = $1", &params![id])? == 0 {
                return Ok(None);
            }
            Self::clear_rule_alerts(tx, &columns, id, reason, now, webhook).map(Some)
        })
    }

    fn clear_rule_alerts(
        tx: &mut Tx,
        columns: &str,
        rule_id: i64,
        reason: &str,
        now: i64,
        mut webhook: impl FnMut(&AlertRow) -> NewWebhook,
    ) -> Result<Vec<AlertRow>> {
        let cleared: Vec<AlertRow> = tx
            .query(
                &format!(
                    "UPDATE alerts SET cleared_at = $2, cleared_reason = $3
                     WHERE rule_id = $1 AND cleared_at IS NULL RETURNING {columns}"
                ),
                &params![rule_id, now, reason],
            )?
            .iter()
            .map(Self::alert_row)
            .collect();
        for row in &cleared {
            Self::insert_webhook(tx.conn, row.id, &webhook(row), now)?;
        }
        Ok(cleared)
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

    /// Runs a read-only query and returns each row as a JSON object keyed by column
    /// name; the JSON columns (`raw`, `original_raw`, `resources`, `response`) come
    /// back parsed.
    pub fn query_json(&self, sql: &str, p: impl Into<Vec<Param>>) -> Result<Vec<Value>> {
        let p = p.into();
        Ok(self.query(sql, &p)?.iter().map(Row::object).collect())
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if let Some(schema) = self.test_schema.take()
            && let Ok(mut conn) = self.conn.lock()
            && let Conn::Postgres(h) = &mut *conn
        {
            let _ = h.batch(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
        }
    }
}

/// `HIE_TEST_DATABASE_URL` runs every store the tests open on that PostgreSQL,
/// each in a schema of its own.
fn test_database_url() -> Option<String> {
    std::env::var("HIE_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
}

fn test_schema_name() -> String {
    use rand::Rng;
    format!("hie_test_{:016x}", rand::thread_rng().r#gen::<u64>())
}

/// A connection URL without its password, for messages.
fn redact(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut u) => {
            if u.password().is_some() {
                let _ = u.set_password(Some("***"));
            }
            u.to_string()
        }
        Err(_) => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- alert webhook events: the store's side, with time passed in -------

    fn hook(event_id: &str, event: &'static str, destination: &str) -> NewWebhook {
        NewWebhook {
            event_id: event_id.into(),
            event,
            destination: destination.into(),
            payload: format!("{{\"event_id\":\"{event_id}\"}}"),
            state: "pending",
        }
    }

    /// Two handles on one store, as two engine instances have.
    fn two_handles() -> (tempfile::TempDir, Db, Db) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hie.db");
        let (a, b) = (Db::open(&path).unwrap(), Db::open(&path).unwrap());
        (dir, a, b)
    }

    fn fire(db: &Db, event_id: &str, destination: &str) -> i64 {
        db.insert_alert_with_webhook(Some("src"), "silence", "quiet", None, 1_000, |_| {
            hook(event_id, "fired", destination)
        })
        .unwrap()
        .unwrap()
        .id
    }

    fn state(db: &Db, event_id: &str) -> (String, i64, Option<String>) {
        let r = db
            .query_one(
                "SELECT state, attempts, last_error FROM alert_webhooks WHERE event_id = $1",
                &params![event_id],
            )
            .unwrap()
            .unwrap();
        (r.string(0), r.i64(1), r.opt_string(2))
    }

    #[test]
    fn one_instance_wins_a_claim() {
        let (_dir, a, b) = two_handles();
        fire(&a, "e1", "d");
        let first = a.webhook_claim("e1", "d", 2_000, 62_000, 6).unwrap();
        let second = b.webhook_claim("e1", "d", 2_000, 62_000, 6).unwrap();
        assert_eq!(first.as_ref().map(|c| c.attempt), Some(1));
        assert!(second.is_none());
        assert!(b.webhook_due("d", 2_000, 6, 10).unwrap().is_empty());
        // Another destination's sender never takes it.
        assert!(
            b.webhook_claim("e1", "other", 70_000, 130_000, 6)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn concurrent_claims_on_two_connections_have_one_winner() {
        let (_dir, a, b) = two_handles();
        let (a, b) = (std::sync::Arc::new(a), std::sync::Arc::new(b));
        for round in 0..40 {
            let event_id = format!("race-{round}");
            fire(&a, &event_id, "d");
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let contenders: Vec<_> = [a.clone(), b.clone()]
                .into_iter()
                .map(|db| {
                    let (barrier, event_id) = (barrier.clone(), event_id.clone());
                    // Both see the event due before either claims it.
                    assert_eq!(
                        db.webhook_due("d", 2_000, 6, 100).unwrap(),
                        std::slice::from_ref(&event_id)
                    );
                    std::thread::spawn(move || {
                        barrier.wait();
                        db.webhook_claim(&event_id, "d", 2_000, 62_000, 6).unwrap()
                    })
                })
                .collect();
            let won: Vec<_> = contenders
                .into_iter()
                .filter_map(|t| t.join().unwrap())
                .collect();
            assert_eq!(won.len(), 1, "round {round}");
            assert_eq!(state(&a, &event_id).1, 1);
            a.webhook_delivered(&event_id, 1, 200, 3_000).unwrap();
        }
    }

    #[test]
    fn a_claimed_attempt_has_no_status_until_it_is_answered() {
        let (_dir, a, _b) = two_handles();
        fire(&a, "e1", "d");
        a.webhook_claim("e1", "d", 2_000, 62_000, 6)
            .unwrap()
            .unwrap();
        a.webhook_attempt_failed(
            "e1",
            1,
            "HTTP 503 Service Unavailable",
            Some(503),
            Some(2_100),
        )
        .unwrap();
        a.webhook_claim("e1", "d", 2_200, 62_200, 6)
            .unwrap()
            .unwrap();
        let r = a
            .query_one(
                "SELECT last_status, last_error FROM alert_webhooks WHERE event_id = 'e1'",
                &[],
            )
            .unwrap()
            .unwrap();
        assert_eq!(r.opt_i64(0), None, "the running attempt has no answer yet");
        assert_eq!(
            r.opt_string(1).as_deref(),
            Some("HTTP 503 Service Unavailable")
        );
    }

    #[test]
    fn a_late_outcome_after_the_claim_ran_out_changes_nothing() {
        let (_dir, a, b) = two_handles();
        fire(&a, "e1", "d");
        let stale = a
            .webhook_claim("e1", "d", 2_000, 62_000, 6)
            .unwrap()
            .unwrap();
        // The lease runs out; the other instance takes the next attempt.
        let fresh = b
            .webhook_claim("e1", "d", 63_000, 123_000, 6)
            .unwrap()
            .unwrap();
        assert_eq!(fresh.attempt, 2);
        // While that attempt runs, the stale one's outcomes change nothing.
        assert!(
            !a.webhook_delivered("e1", stale.attempt, 200, 64_000)
                .unwrap()
        );
        assert!(
            !a.webhook_attempt_failed("e1", stale.attempt, "timeout", None, Some(70_000))
                .unwrap()
        );
        assert!(
            !a.webhook_attempt_failed("e1", stale.attempt, "timeout", None, None)
                .unwrap()
        );
        let r = a
            .query_one(
                "SELECT state, attempts, next_attempt_at, delivered_at, last_error FROM alert_webhooks WHERE event_id = 'e1'",
                &[],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                r.string(0),
                r.i64(1),
                r.i64(2),
                r.opt_i64(3),
                r.opt_string(4)
            ),
            ("pending".to_string(), 2, 123_000, None, None)
        );
        assert!(
            b.webhook_delivered("e1", fresh.attempt, 200, 65_000)
                .unwrap()
        );
        assert_eq!(state(&a, "e1").0, "delivered");
    }

    #[test]
    fn a_late_outcome_never_touches_a_replacement_event() {
        let (_dir, a, _b) = two_handles();
        let alert = fire(&a, "old", "d");
        let stale = a
            .webhook_claim("old", "d", 2_000, 62_000, 6)
            .unwrap()
            .unwrap();
        a.execute("DELETE FROM alerts WHERE id = $1", &params![alert])
            .unwrap();
        assert!(state_missing(&a, "old"), "the event goes with its alert");
        fire(&a, "new", "d");
        let fresh = a
            .webhook_claim("new", "d", 3_000, 63_000, 6)
            .unwrap()
            .unwrap();
        assert_eq!(fresh.attempt, stale.attempt);
        assert!(
            !a.webhook_delivered("old", stale.attempt, 200, 4_000)
                .unwrap()
        );
        assert_eq!(state(&a, "new").0, "pending");
    }

    fn state_missing(db: &Db, event_id: &str) -> bool {
        db.query_one(
            "SELECT 1 FROM alert_webhooks WHERE event_id = $1",
            &params![event_id],
        )
        .unwrap()
        .is_none()
    }

    #[test]
    fn a_last_attempt_with_no_outcome_ends_failed_and_frees_the_clear() {
        let (_dir, a, _b) = two_handles();
        let alert = fire(&a, "f", "d");
        let mut now = 2_000;
        for attempt in 1..=5 {
            let c = a
                .webhook_claim("f", "d", now, now + 60_000, 6)
                .unwrap()
                .unwrap();
            assert_eq!(c.attempt, attempt);
            a.webhook_attempt_failed(
                "f",
                attempt,
                "HTTP 500 Internal Server Error",
                Some(500),
                Some(now + 1),
            )
            .unwrap();
            now += 10;
        }
        a.clear_alert_with_webhook(alert, "back", now, |_| hook("c", "cleared", "d"))
            .unwrap()
            .unwrap();
        assert_eq!(
            a.webhook_due("d", now, 6, 10).unwrap(),
            ["f"],
            "the clear waits"
        );
        // The sixth attempt is claimed and the process dies before recording it.
        a.webhook_claim("f", "d", now, now + 60_000, 6)
            .unwrap()
            .unwrap();
        assert!(a.webhook_due("d", now + 61_000, 6, 10).unwrap().is_empty());
        assert_eq!(a.webhook_expire_exhausted(now + 61_000, 6).unwrap(), 1);
        let (st, attempts, error) = state(&a, "f");
        assert_eq!((st.as_str(), attempts), ("failed", 6));
        assert_eq!(
            error.as_deref(),
            Some("no outcome recorded for the last attempt")
        );
        assert!(
            a.webhook_claim("f", "d", now + 62_000, now + 122_000, 6)
                .unwrap()
                .is_none()
        );
        assert_eq!(a.webhook_due("d", now + 62_000, 6, 10).unwrap(), ["c"]);
    }

    #[test]
    fn a_clear_waits_only_for_its_fire_at_the_same_destination() {
        let (_dir, a, _b) = two_handles();
        let one = fire(&a, "f1", "d");
        a.clear_alert_with_webhook(one, "back", 2_000, |_| hook("c1", "cleared", "d"))
            .unwrap()
            .unwrap();
        assert!(
            a.webhook_claim("c1", "d", 3_000, 63_000, 6)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            a.webhook_next_due("d").unwrap(),
            Some(1_000),
            "only the fire counts"
        );
        // Fired for an earlier URL, parked; cleared for the current one: not held back.
        let two = fire(&a, "f2", "old");
        a.clear_alert_with_webhook(two, "back", 2_000, |_| hook("c2", "cleared", "new"))
            .unwrap()
            .unwrap();
        assert_eq!(a.webhook_due("new", 3_000, 6, 10).unwrap(), ["c2"]);
    }

    #[test]
    fn an_alert_clears_once_and_rolls_back_whole() {
        let (_dir, a, b) = two_handles();
        let id = fire(&a, "f", "");
        let cleared = a
            .clear_alert_with_webhook(id, "back", 2_000, |row| {
                assert_eq!(row.cleared_reason.as_deref(), Some("back"));
                assert!(row.cleared_at.is_some());
                hook("c", "cleared", "")
            })
            .unwrap();
        assert!(cleared.is_some());
        assert!(
            b.clear_alert_with_webhook(id, "again", 3_000, |_| hook("c2", "cleared", ""))
                .unwrap()
                .is_none()
        );
        assert!(state_missing(&a, "c2"));
        // An event that cannot be written (its id is taken) leaves no alert behind.
        let before = a.query("SELECT id FROM alerts", &[]).unwrap().len();
        assert!(
            a.insert_alert_with_webhook(Some("src"), "silence", "quiet", None, 4_000, |_| hook(
                "f", "fired", ""
            ))
            .is_err()
        );
        assert_eq!(a.query("SELECT id FROM alerts", &[]).unwrap().len(), before);
    }

    #[test]
    fn params_take_the_usual_values() {
        let p = params![
            1i64,
            "a",
            String::from("b"),
            Some(2i64),
            None::<String>,
            true,
            3usize
        ];
        assert!(matches!(p[0], Param::Int(1)));
        assert!(matches!(&p[1], Param::Text(t) if t == "a"));
        assert!(matches!(&p[2], Param::Text(t) if t == "b"));
        assert!(matches!(p[3], Param::Int(2)));
        assert!(matches!(p[4], Param::Null));
        assert!(matches!(p[5], Param::Int(1)));
        assert!(matches!(p[6], Param::Int(3)));
    }

    #[test]
    fn leases_are_held_renewed_and_taken_over() {
        let db = Db::in_memory().unwrap();
        assert!(db.acquire_lease("lab", "a", 60_000).unwrap());
        assert!(!db.acquire_lease("lab", "b", 60_000).unwrap());
        assert!(db.acquire_lease("lab", "a", 60_000).unwrap());
        assert_eq!(db.lease("lab").unwrap().unwrap().owner, "a");
        // Expired: b takes it.
        assert!(db.acquire_lease("lab", "a", -1).unwrap());
        assert!(db.lease("lab").unwrap().is_none());
        assert!(db.acquire_lease("lab", "b", 60_000).unwrap());
        assert!(!db.acquire_lease("lab", "a", 60_000).unwrap());
        db.release_lease("lab", "b").unwrap();
        assert!(db.acquire_lease("lab", "a", 60_000).unwrap());
    }

    fn file(db: &Db, feed: &str, name: &str) -> i64 {
        db.insert_file(feed, feed, name, name, 1, "processing", None, None, None)
            .unwrap()
    }

    fn status(db: &Db, check: i64) -> Value {
        db.check(check, 0, 1000).unwrap().unwrap()
    }

    #[test]
    fn a_check_is_answered_by_the_scan_whose_snapshot_holds_it() {
        let db = Db::in_memory().unwrap();
        file(&db, "lab", "old.csv");
        let first = db.insert_check("lab").unwrap();
        let snapshot = db.pending_checks("lab").unwrap();
        assert_eq!(snapshot, vec![first]);
        // Asked for while that scan runs: not in its snapshot.
        let second = db.insert_check("lab").unwrap();
        let new = file(&db, "lab", "new.csv");
        file(&db, "other", "elsewhere.csv");
        db.answer_checks("lab", &snapshot, None).unwrap();

        let answered = status(&db, first);
        assert_eq!(answered["status"], "found");
        assert_eq!(answered["source"], "lab");
        assert_eq!(answered["file_count"], 1);
        assert_eq!(answered["files"][0]["id"], new);
        assert_eq!(answered["files"][0]["name"], "new.csv");
        assert_eq!(answered["files"][0]["status"], "processing");
        assert!(answered["reason"].is_null());
        assert!(
            answered["finished_at"].as_str().unwrap() >= answered["requested_at"].as_str().unwrap()
        );
        assert!(answered["next_after"].is_null());

        let waiting = status(&db, second);
        assert_eq!(waiting["status"], "pending");
        assert!(waiting["finished_at"].is_null());
        assert_eq!(waiting["files"], json!([]));
        assert_eq!(waiting["file_count"], 0);

        // The next scan answers it with the load taken after it was asked for,
        // though an earlier scan recorded that load.
        let snapshot = db.pending_checks("lab").unwrap();
        assert_eq!(snapshot, vec![second]);
        db.answer_checks("lab", &snapshot, None).unwrap();
        assert_eq!(status(&db, second)["status"], "found");
        assert_eq!(status(&db, second)["files"][0]["id"], new);

        // Nothing since.
        let third = db.insert_check("lab").unwrap();
        db.answer_checks("lab", &[third], None).unwrap();
        let empty = status(&db, third);
        assert_eq!(empty["status"], "nothing_new");
        assert_eq!(empty["files"], json!([]));
    }

    #[test]
    fn the_first_answer_stays_and_a_failure_keeps_what_was_taken() {
        let db = Db::in_memory().unwrap();
        let check = db.insert_check("lab").unwrap();
        let taken = file(&db, "lab", "a.csv");
        db.answer_checks("lab", &[check], Some(CheckFailure::Listing))
            .unwrap();
        db.answer_checks("lab", &[check], None).unwrap();
        db.fail_pending_checks("lab", "me", CheckFailure::Deleted)
            .unwrap();
        let answered = status(&db, check);
        assert_eq!(answered["status"], "failed");
        assert_eq!(answered["reason"], CheckFailure::Listing.reason());
        assert_eq!(answered["files"][0]["id"], taken);

        // Another instance holding the source keeps its checks; once nobody
        // else holds it, they fail with the loads since they were asked for.
        let deleted = db.insert_check("lab").unwrap();
        let since = file(&db, "lab", "b.csv");
        assert!(db.acquire_lease("lab", "other", 60_000).unwrap());
        assert_eq!(
            db.fail_pending_checks("lab", "me", CheckFailure::Deleted)
                .unwrap(),
            0
        );
        assert_eq!(status(&db, deleted)["status"], "pending");
        db.release_lease("lab", "other").unwrap();
        assert!(db.acquire_lease("lab", "me", 60_000).unwrap());
        assert_eq!(
            db.fail_pending_checks("lab", "me", CheckFailure::Deleted)
                .unwrap(),
            1
        );
        let failed = status(&db, deleted);
        assert_eq!(failed["reason"], "the source was deleted");
        assert_eq!(failed["file_count"], 1);
        assert_eq!(failed["files"][0]["id"], since);
        // An unknown check.
        assert!(db.check(deleted + 100, 0, 10).unwrap().is_none());
    }

    #[test]
    fn a_check_with_many_loads_lists_them_in_pages() {
        let db = Db::in_memory().unwrap();
        let check = db.insert_check("lab").unwrap();
        let ids: Vec<i64> = (0..5)
            .map(|i| file(&db, "lab", &format!("{i}.csv")))
            .collect();
        db.answer_checks("lab", &[check], None).unwrap();
        // Recorded after the answer: never part of it.
        file(&db, "lab", "later.csv");
        let page = db.check(check, 0, 2).unwrap().unwrap();
        assert_eq!(page["file_count"], 5);
        assert_eq!(page["files"].as_array().unwrap().len(), 2);
        assert_eq!(page["next_after"], ids[1]);
        let rest = db.check(check, ids[1], 10).unwrap().unwrap();
        let rest: Vec<i64> = rest["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["id"].as_i64().unwrap())
            .collect();
        assert_eq!(rest, ids[2..]);
    }

    #[test]
    fn checks_are_purged_by_age_and_their_ids_never_come_back() {
        let db = Db::in_memory().unwrap();
        let old = db.insert_check("lab").unwrap();
        let newest = db.insert_check("lab").unwrap();
        db.execute(
            "UPDATE checks SET requested_at = 1000 WHERE id = $1",
            &params![old],
        )
        .unwrap();
        db.execute(
            "UPDATE checks SET requested_at = 2000 WHERE id = $1",
            &params![newest],
        )
        .unwrap();
        // The cutoff itself is kept.
        assert_eq!(db.purge_checks(1000, 10).unwrap(), 0);
        assert_eq!(db.purge_checks(1001, 10).unwrap(), 1);
        assert!(db.check(old, 0, 10).unwrap().is_none());
        assert_eq!(status(&db, newest)["status"], "pending");
        // Purging the newest does not hand its id to the next check.
        assert_eq!(db.purge_checks(i64::MAX, 10).unwrap(), 1);
        assert!(db.insert_check("lab").unwrap() > newest);
    }

    #[test]
    fn urls_are_redacted() {
        assert_eq!(
            redact("postgres://hie:secret@db:5432/hie"),
            "postgres://hie:***@db:5432/hie"
        );
    }

    // ---- starting on a store in use ------------------------------------------

    /// The tables `postgres_indexes` builds an index on.
    fn indexed_tables() -> Vec<&'static str> {
        let mut tables: Vec<&'static str> = postgres_indexes()
            .into_iter()
            .filter_map(|i| i.split(" ON ").nth(1)?.split('(').next())
            .collect();
        tables.sort_unstable();
        tables.dedup();
        tables
    }

    /// A second instance starts while a write on another holds one table and
    /// then takes every other one, as a write spanning tables does. The start
    /// waits for the held table without holding any other, so the write goes
    /// through and neither is ended as a deadlock. Once for each indexed table.
    /// A table with added columns stops the start at its `ALTER TABLE`; the
    /// others stop it at an index, which the test checks by the lock awaited.
    #[test]
    fn a_start_on_postgres_waits_for_a_write_without_holding_what_it_needs() {
        let Some(url) = test_database_url() else {
            return;
        };
        let schema = test_schema_name();
        let _first = Db::open_postgres(&url, Some(schema.clone())).unwrap();
        let tables = indexed_tables();
        assert!(tables.len() >= 7, "{tables:?}");
        let mut watch = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        for held in &tables {
            let mut write = postgres::Client::connect(&url, postgres::NoTls).unwrap();
            write
                .batch_execute(&format!("SET search_path TO {schema}"))
                .unwrap();
            write.batch_execute("BEGIN").unwrap();
            write
                .batch_execute(&format!("LOCK TABLE {held} IN ROW EXCLUSIVE MODE"))
                .unwrap();
            let start = {
                let (url, schema) = (url.clone(), schema.clone());
                std::thread::spawn(move || Db::open_postgres(&url, Some(schema)).map(drop))
            };
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            loop {
                let waiting = watch
                    .query(
                        "SELECT l.mode FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
                           JOIN pg_namespace n ON n.oid = c.relnamespace
                         WHERE NOT l.granted AND n.nspname = $1 AND c.relname = $2",
                        &[&schema, held],
                    )
                    .unwrap();
                if let Some(row) = waiting.first() {
                    let mode: String = row.get(0);
                    let altered = ADDED_COLUMNS.iter().any(|(table, ..)| table == held);
                    let expected = if altered {
                        "AccessExclusiveLock"
                    } else {
                        "ShareLock"
                    };
                    assert_eq!(mode, expected, "{held}");
                    break;
                }
                assert!(!start.is_finished(), "the start did not wait for {held}");
                assert!(
                    std::time::Instant::now() < deadline,
                    "the start never reached {held}"
                );
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            for other in tables.iter().filter(|t| *t != held) {
                write
                    .batch_execute(&format!("LOCK TABLE {other} IN ROW EXCLUSIVE MODE"))
                    .unwrap_or_else(|e| panic!("holding {held}, taking {other}: {e}"));
            }
            write.batch_execute("COMMIT").unwrap();
            start
                .join()
                .unwrap()
                .unwrap_or_else(|e| panic!("starting while {held} was held: {e:#}"));
        }
    }
}
