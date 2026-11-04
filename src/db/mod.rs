//! The engine's store: every file, record, alert, rule, cursor and lease. One
//! SQL dialect written once, run on SQLite (a file, the default) or PostgreSQL
//! (`postgres://...`, for more than one instance or more than one machine).
//! Placeholders are `$1`, `$2`, ...; SQLite gets them as `?1`, `?2`.
//!
//! `mod.rs` opens the store and creates or upgrades its schema; each area
//! (`loads`, `submissions`, `sources`, `alerts`, `idempotency`, `settings`,
//! `passes`, `versions`, `secrets`, `connectors`, `history`) owns its tables,
//! the columns added to them since, and its queries. `engine` holds the two
//! drivers; [`Db::transaction`] runs several statements as one.

mod alerts;
mod connectors;
mod engine;
mod history;
mod idempotency;
mod loads;
mod passes;
mod secrets;
mod settings;
mod sources;
mod submissions;
mod versions;

use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use serde_json::Value;

pub use alerts::Rule;
use engine::{Conn, PgHandle};
pub use engine::{IntoParam, Param, Row};
pub use history::{Group, PeriodCounts, RetryRefusal, VersionRun};
pub use idempotency::Claim;
pub use loads::{
    Deferral, FailedRow, FailedRowsError, FinishedFile, REASON_MAX_CHARS, Reprocess, Waiting,
    WorkItem,
};
pub use passes::{Pass, PassStatus};
pub use sources::Lease;
pub use submissions::{Submission, SubmittableFile};

// ---- the schema ---------------------------------------------------------------

/// Every table, in the order a store has always created them; each area owns its
/// own DDL. They run as one batch, as they always have. A new area appends here.
const TABLES: &[&str] = &[
    loads::FILES,
    loads::RECORDS,
    alerts::ALERTS,
    sources::STATE,
    sources::CURSORS,
    sources::LEASES,
    submissions::SUBMISSIONS,
    alerts::RULES,
    idempotency::IDEMPOTENCY,
    settings::SETTINGS,
    passes::PASSES,
    versions::TABLES,
    secrets::TABLES,
    connectors::TABLES,
    history::TABLES,
];

/// Columns added after a store was first created, per area, in the order they
/// are added; each is added when missing, after the tables exist. A new area's
/// list appends here.
const ADDED_COLUMNS: &[&[(&str, &str, &str)]] = &[
    loads::ADDED_COLUMNS,
    alerts::ADDED_COLUMNS,
    versions::ADDED_COLUMNS,
    secrets::ADDED_COLUMNS,
    connectors::ADDED_COLUMNS,
    history::ADDED_COLUMNS,
];

/// The DDL of every table, as one batch.
fn schema() -> String {
    format!("\n{}", TABLES.concat())
}

fn added_columns() -> impl Iterator<Item = &'static (&'static str, &'static str, &'static str)> {
    ADDED_COLUMNS.iter().flat_map(|area| area.iter())
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// `params![a, b, c]` builds the bound values of a statement.
#[macro_export]
macro_rules! params {
    () => { ([] as [$crate::db::Param; 0]) };
    ($($x:expr),+ $(,)?) => { [$($crate::db::IntoParam::to_param(&$x)),+] };
}
pub use params;

/// Creates what a SQLite store lacks: every table, then every added column.
fn migrate_sqlite(conn: &rusqlite::Connection) -> Result<()> {
    conn.execute_batch(&schema())?;
    for (table, column, kind) in added_columns() {
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
    Ok(())
}

/// The same for PostgreSQL, with its integer and serial types.
fn migrate_postgres(client: &PgHandle) -> Result<()> {
    let ddl = schema()
        .replace("INTEGER PRIMARY KEY", "BIGSERIAL PRIMARY KEY")
        .replace("INTEGER", "BIGINT");
    client
        .batch(&ddl)
        .context("creating the schema in PostgreSQL")?;
    for (table, column, kind) in added_columns() {
        client.batch(&format!(
            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {}",
            kind.replace("INTEGER", "BIGINT")
        ))?;
    }
    Ok(())
}

// ---- the store ----------------------------------------------------------------

pub struct Db {
    conn: Mutex<Conn>,
    postgres: bool,
    /// A schema created for one test run on PostgreSQL, dropped with the store.
    test_schema: Option<String>,
    /// The keys record content is sealed with (`loads.rs`).
    content: loads::ContentKeys,
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
        migrate_sqlite(&conn)?;
        Ok(Self {
            conn: Mutex::new(Conn::Sqlite(conn)),
            postgres: false,
            test_schema: None,
            content: Default::default(),
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
        migrate_postgres(&client)?;
        Ok(Self {
            conn: Mutex::new(Conn::Postgres(client)),
            postgres: true,
            test_schema,
            content: Default::default(),
        })
    }

    /// Runs the schema creation and upgrade again on an open store.
    #[cfg(test)]
    fn migrate(&self) -> Result<()> {
        self.with(|c| match c {
            Conn::Sqlite(c) => migrate_sqlite(c),
            Conn::Postgres(h) => migrate_postgres(h),
        })
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

    /// Runs `f` as one transaction: committed when it returns `Ok`, rolled back
    /// when it returns `Err`. The store is held for the whole of it, so `f`
    /// must use the [`Tx`] it is given, never `self`.
    pub fn transaction<T, E: From<anyhow::Error>>(
        &self,
        f: impl FnOnce(&mut Tx<'_>) -> std::result::Result<T, E>,
    ) -> std::result::Result<T, E> {
        let mut conn = self.conn.lock().expect("database mutex poisoned");
        conn.batch("BEGIN").map_err(E::from)?;
        let mut tx = Tx { conn: &mut conn };
        match f(&mut tx) {
            Ok(value) => {
                conn.batch("COMMIT").map_err(E::from)?;
                Ok(value)
            }
            Err(e) => {
                let _ = conn.batch("ROLLBACK");
                Err(e)
            }
        }
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

    /// Runs a read-only query and returns each row as a JSON object keyed by column
    /// name; the JSON columns (`raw`, `original_raw`, `resources`, `response`) come
    /// back parsed, and opened when sealed.
    pub fn query_json(&self, sql: &str, p: impl Into<Vec<Param>>) -> Result<Vec<Value>> {
        let p = p.into();
        Ok(self
            .query(sql, &p)?
            .iter()
            .map(|r| self.reveal(r.object()))
            .collect())
    }
}

/// Statements inside [`Db::transaction`].
pub struct Tx<'a> {
    conn: &'a mut Conn,
}

impl Tx<'_> {
    pub fn execute(&mut self, sql: &str, params: &[Param]) -> Result<u64> {
        self.conn.execute(sql, params)
    }

    /// Rows as JSON objects, as [`Db::query_json`] returns them.
    pub fn query_json(&mut self, sql: &str, params: &[Param]) -> Result<Vec<Value>> {
        Ok(self
            .conn
            .query(sql, params)?
            .iter()
            .map(Row::object)
            .collect())
    }

    /// An inserted row's id.
    pub fn insert_returning_id(&mut self, sql: &str, params: &[Param]) -> Result<i64> {
        self.conn
            .query(&format!("{sql} RETURNING id"), params)?
            .first()
            .map(|r| r.i64(0))
            .context("INSERT returned no id")
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
    fn a_load_runs_its_snapshot_unless_a_retry_names_another_version() {
        use crate::versions::{Environment, VersionKey, VersionRef};
        let db = Db::in_memory().unwrap();
        let v1 = VersionRef {
            transform_id: "lab".into(),
            version: 1,
            code_sha256: "a".repeat(64),
            environment: Environment::local(),
        };
        // A queued load is adopted by the next object, and fixes its version then.
        let queued = db.create_queued_load("src").unwrap();
        let load = db
            .open_load("src", "a.csv", "sha", 3, None, Some(&v1))
            .unwrap();
        assert_eq!(load, queued);
        let other = db.open_load("src", "b.csv", "sha2", 3, None, None).unwrap();
        assert_ne!(other, load);
        db.insert_records(
            load,
            "src",
            &[
                (1, serde_json::json!({}), None),
                (2, serde_json::json!({}), None),
            ],
        )
        .unwrap();
        db.insert_records(other, "src", &[(1, serde_json::json!({}), None)])
            .unwrap();
        let second = db.received_records("src", 10).unwrap()[1].id;
        db.execute(
            "UPDATE records SET exec_retry = 'rt_x', exec_transform_id = 'lab', exec_version = 2 WHERE id = $1",
            &params![second],
        )
        .unwrap();
        let items = db.received_records_after("src", 0, 10).unwrap();
        let runs: Vec<(Option<String>, Option<VersionKey>)> = items
            .iter()
            .map(|i| (i.retry.clone(), i.version.clone()))
            .collect();
        let key = |version| {
            Some(VersionKey {
                transform_id: "lab".into(),
                version,
            })
        };
        assert_eq!(
            runs,
            [(None, key(1)), (Some("rt_x".into()), key(2)), (None, None)]
        );
        assert_eq!(
            db.received_records_after("src", items[1].id, 10)
                .unwrap()
                .len(),
            1
        );
        let environment = db
            .query_json("SELECT environment FROM files WHERE id = $1", params![load])
            .unwrap();
        assert_eq!(
            environment[0]["environment"],
            serde_json::to_string(&Environment::local()).unwrap()
        );
    }

    #[test]
    fn passes_asked_for_are_served_by_the_next_pass() {
        let db = Db::in_memory().unwrap();
        let a = db.request_pass("src").unwrap();
        let b = db.request_pass("src").unwrap();
        assert!(a.starts_with("ps_") && a != b);
        let started = db.start_passes("src").unwrap();
        assert_eq!(started, [a.clone(), b.clone()]);
        assert!(db.start_passes("src").unwrap().is_empty());
        assert_eq!(db.pass("src", &a).unwrap().unwrap().status, "running");
        db.finish_passes(&started, PassStatus::Done, None, None, &[7], 3)
            .unwrap();
        let pass = db.pass("src", &b).unwrap().unwrap();
        assert_eq!(
            (pass.status.as_str(), pass.loads, pass.new_rows),
            ("done", vec![7], 3)
        );
        assert!(db.pass("other", &b).unwrap().is_none());
    }

    #[test]
    fn urls_are_redacted() {
        assert_eq!(
            redact("postgres://hie:secret@db:5432/hie"),
            "postgres://hie:***@db:5432/hie"
        );
    }

    #[test]
    fn a_store_missing_the_added_columns_is_upgraded_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(&dir.path().join("hie.db")).unwrap();
        let file = db
            .insert_file("lab", "a.csv", "abc", 3, "done", None, None)
            .unwrap();
        db.insert_records(file, "lab", &[(1, serde_json::json!({"a": 1}), None)])
            .unwrap();
        db.insert_alert(Some("lab"), "silence", "quiet", None)
            .unwrap();
        // The store as it was before the added columns existed, with its rows.
        for (table, column, _) in added_columns() {
            db.execute(&format!("ALTER TABLE {table} DROP COLUMN {column}"), &[])
                .unwrap();
        }
        db.migrate().unwrap();
        for (table, column, _) in added_columns() {
            db.query(&format!("SELECT {column} FROM {table}"), &[])
                .unwrap_or_else(|e| panic!("{table}.{column}: {e:#}"));
        }
        let row = db
            .query_json(
                "SELECT f.name, f.archived, f.source_key, r.raw, r.submission FROM files f JOIN records r ON r.file_id = f.id",
                [],
            )
            .unwrap();
        assert_eq!(row.len(), 1);
        assert_eq!(row[0]["name"], "a.csv");
        assert_eq!(row[0]["archived"], 0);
        assert!(row[0]["source_key"].is_null() && row[0]["submission"].is_null());
        assert_eq!(row[0]["raw"]["a"], 1);
        assert_eq!(
            db.query_json("SELECT kind, rule_id, channels FROM alerts", [])
                .unwrap()[0],
            serde_json::json!({"kind": "silence", "rule_id": null, "channels": null})
        );
        let indexes = if db.is_postgres() {
            "SELECT indexname AS name FROM pg_indexes WHERE schemaname = current_schema()"
        } else {
            "SELECT name FROM sqlite_master WHERE type = 'index'"
        };
        let names: Vec<String> = db
            .query_json(indexes, [])
            .unwrap()
            .iter()
            .filter_map(|r| r["name"].as_str().map(str::to_string))
            .collect();
        for index in [
            "files_feed_sha",
            "files_feed_received",
            "records_feed_status",
            "records_feed_status_id",
            "records_file",
            "alerts_feed_kind",
            "submissions_feed_status",
        ] {
            assert!(names.iter().any(|n| n == index), "{index} in {names:?}");
        }
        // A new file still gets the default.
        let again = db
            .insert_file("lab", "b.csv", "def", 3, "done", None, None)
            .unwrap();
        assert!(db.file_source(again).unwrap().is_none());
    }
}
