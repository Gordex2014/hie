//! The engine's store: every file, record, alert, rule, cursor and lease. One
//! SQL dialect written once, run on SQLite (a file, the default) or PostgreSQL
//! (`postgres://...`, for more than one instance or more than one machine).
//! Placeholders are `$1`, `$2`, ...; SQLite gets them as `?1`, `?2`.
//!
//! `mod.rs` opens the store and creates or upgrades its schema; each area
//! (`loads`, `submissions`, `sources`, `alerts`) owns its tables, the columns
//! added to them since, and its queries. `engine` holds the two drivers.

mod alerts;
mod engine;
mod loads;
mod sources;
mod submissions;

use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use serde_json::Value;

pub use alerts::Rule;
use engine::{Conn, PgHandle};
pub use engine::{IntoParam, Param, Row};
pub use loads::{FinishedFile, WorkItem};
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
];

/// Columns added after a store was first created, per area, in the order they
/// are added; each is added when missing, after the tables exist. A new area's
/// list appends here.
const ADDED_COLUMNS: &[&[(&str, &str, &str)]] = &[loads::ADDED_COLUMNS, alerts::ADDED_COLUMNS];

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
