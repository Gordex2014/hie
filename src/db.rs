//! The engine's store: every file, record, alert, rule, cursor and lease. One
//! SQL dialect written once, run on SQLite (a file, the default) or PostgreSQL
//! (`postgres://...`, for more than one instance or more than one machine).
//! Placeholders are `$1`, `$2`, ...; SQLite gets them as `?1`, `?2`.

#[cfg(feature = "secrets-native")]
pub mod secrets;

use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use postgres::types::{IsNull, ToSql, Type};
use rusqlite::types::ValueRef;
use serde_json::{Map, Value, json};

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
  id INTEGER PRIMARY KEY,
  source_id TEXT,
  condition TEXT NOT NULL,
  threshold TEXT NOT NULL,
  notify TEXT NOT NULL DEFAULT '[]',
  channels TEXT NOT NULL DEFAULT '[\"app\"]',
  enabled INTEGER NOT NULL DEFAULT 1,
  created_by TEXT,
  created_at INTEGER NOT NULL
);
";

/// Columns added after a store was first created; each is added when missing.
const ADDED_COLUMNS: &[(&str, &str, &str)] = &[
    ("files", "source_key", "TEXT"),
    ("files", "archived", "INTEGER NOT NULL DEFAULT 0"),
    ("files", "origin", "TEXT"),
    ("records", "submission", "TEXT"),
    ("alerts", "rule_id", "INTEGER"),
    ("alerts", "cleared_at", "INTEGER"),
    ("alerts", "cleared_reason", "TEXT"),
    ("alerts", "notify", "TEXT"),
    ("alerts", "channels", "TEXT"),
];

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
}

fn default_channels() -> Vec<String> {
    vec!["app".into(), "webhook".into()]
}
fn default_enabled() -> bool {
    true
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

#[derive(Debug, Clone)]
pub struct WorkItem {
    pub id: i64,
    pub file_id: i64,
    pub line: i64,
    pub file_name: String,
    pub raw: Value,
    pub resources: Option<Value>,
    pub attempts: u32,
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
        let ddl = SCHEMA
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
    /// never served by the engine.
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
        origin: Option<&str>,
    ) -> Result<i64> {
        let now = now_ms();
        let finished_at = if status == "processing" {
            None
        } else {
            Some(now)
        };
        self.insert_returning_id(
            "INSERT INTO files (feed_id, name, sha256, size, status, duplicate_of, received_at, finished_at, source_key, origin)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            &params![feed, name, sha, size, status, duplicate_of, now, finished_at, source_key, origin],
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
            })
            .collect())
    }

    pub fn received_records(&self, feed: &str, limit: usize) -> Result<Vec<WorkItem>> {
        self.work_items(
            "SELECT r.id, r.file_id, r.line, f.name, r.raw, r.resources, r.attempts
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.feed_id = $1 AND r.status = 'received' ORDER BY r.id LIMIT $2",
            &params![feed, limit],
        )
    }

    pub fn due_records(&self, feed: &str, limit: usize) -> Result<Vec<WorkItem>> {
        self.work_items(
            "SELECT r.id, r.file_id, r.line, f.name, r.raw, r.resources, r.attempts
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.feed_id = $1 AND r.status = 'pending' AND r.next_attempt_at <= $2
             ORDER BY r.id LIMIT $3",
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
    /// be handed over at all. Returns how many.
    pub fn reject_unsubmitted(&self, file_id: i64, kind: &str, error: &str) -> Result<usize> {
        Ok(self.execute(
            "UPDATE records SET status = 'rejected', error_kind = $2, error = $3, updated_at = $4
             WHERE file_id = $1 AND status = 'pending' AND submission IS NULL",
            &params![file_id, kind, error, now_ms()],
        )? as usize)
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

    pub fn clear_alert(&self, id: i64, reason: &str) -> Result<()> {
        self.execute(
            "UPDATE alerts SET cleared_at = $2, cleared_reason = $3 WHERE id = $1 AND cleared_at IS NULL",
            &params![id, now_ms(), reason],
        )
        .map(drop)
    }

    // ---- alert rules -------------------------------------------------------

    const RULE_SELECT: &'static str = "SELECT id, source_id, condition, threshold, notify, channels, enabled, created_by FROM alert_rules";

    fn rule_rows(&self, sql: &str, p: &[Param]) -> Result<Vec<Rule>> {
        Ok(self
            .query(sql, p)?
            .iter()
            .map(|r| Rule {
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
            })
            .collect())
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

    #[test]
    fn urls_are_redacted() {
        assert_eq!(
            redact("postgres://hie:secret@db:5432/hie"),
            "postgres://hie:***@db:5432/hie"
        );
    }
}
