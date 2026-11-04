//! A SQL database as a source, on SQLite so no server is needed: runs of the
//! query past a cursor, the cursor moving once the rows are stored, nothing read
//! twice, a cursor_start, and the connection test.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use hie::alerts::Alerter;
use hie::db::Db;
use hie::fhir::FhirClient;
use hie::pipeline::FeedRunner;
use hie::source::Source;
use serde_json::{Value, json};
use sqlx::sqlite::SqlitePoolOptions;

type Urls = Arc<Mutex<Vec<String>>>;

async fn batch(State(urls): State<Urls>, Json(bundle): Json<Value>) -> Json<Value> {
    let mut entries = Vec::new();
    for e in bundle["entry"].as_array().unwrap() {
        urls.lock()
            .unwrap()
            .push(e["request"]["url"].as_str().unwrap().to_string());
        entries.push(json!({"response": {"status": "201 Created"}}));
    }
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

fn python() -> Option<&'static str> {
    ["py", "python3", "python"].into_iter().find(|p| {
        std::process::Command::new(p)
            .arg("-c")
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

const QUERY: &str = "SELECT id, patient, code, display, value, unit, taken_at FROM lab_results WHERE id > {cursor} ORDER BY id LIMIT 2";

fn lab_feed(
    repo: &std::path::Path,
    id: &str,
    cursor_start: Option<&str>,
) -> hie::config::FeedConfig {
    let start = cursor_start
        .map(|c| format!("  cursor_start: \"{c}\"\n"))
        .unwrap_or_default();
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
        "id: {id}\nname: Lab database\nsource:\n  type: database\n  url_env: HIE_TEST_DB_URL\n  query: \"{QUERY}\"\n  cursor_column: id\n  max_runs: 2\n{start}transform: transforms/sql_lab_results.py\n"
    ))
    .unwrap();
    feed.resolve(repo).unwrap();
    feed
}

#[tokio::test]
async fn a_table_is_read_past_a_cursor_and_nothing_twice() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("lab.db")
        .display()
        .to_string()
        .replace('\\', "/");
    let url = format!("sqlite:{path}?mode=rwc");
    let pool = SqlitePoolOptions::new().connect(&url).await.unwrap();
    sqlx::query(
        "CREATE TABLE lab_results (id INTEGER PRIMARY KEY, patient TEXT, code TEXT, display TEXT, value REAL, unit TEXT, taken_at TEXT)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let insert = |pool: sqlx::SqlitePool, id: i64, value: f64| async move {
        sqlx::query(
            "INSERT INTO lab_results VALUES ($1, $2, '2345-7', 'Glucose', $3, 'mg/dL', $4)",
        )
        .bind(id)
        .bind(format!("Patient/{id}"))
        .bind(value)
        .bind(format!("2026-10-{:02}T08:00:00Z", id))
        .execute(&pool)
        .await
        .unwrap();
    };
    for id in 1..=5 {
        insert(pool.clone(), id, 90.0 + id as f64).await;
    }
    // The URL lives in the environment, named by the source, never in the file.
    unsafe { std::env::set_var("HIE_TEST_DB_URL", &url) };

    let urls: Urls = Arc::default();
    let hfs = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(urls.clone()),
    )
    .await;
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let feed = lab_feed(&repo, "lab-db", None);
    assert_eq!(feed.format, hie::config::Format::Ndjson);
    assert_eq!(feed.source.describe(), "database $HIE_TEST_DB_URL by id");

    let db = Arc::new(Db::in_memory().unwrap());
    let runner = FeedRunner::with_source(
        feed.clone(),
        Source::from_config(&feed).unwrap().with_db(db.clone()),
        db.clone(),
        Arc::new(FhirClient::new(&hfs)),
        Arc::new(Alerter::new(db.clone(), None)),
        python,
        &repo.join("hie_runtime"),
        &hfs,
        "http://127.0.0.1:0",
    );

    // First pass: two runs of two rows (LIMIT 2, max_runs 2), one load, cursor at 4.
    runner.tick().await.unwrap();
    let counts = db.feed_counts("lab-db").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["files"].as_i64()),
        (Some(4), Some(1))
    );
    assert_eq!(
        *urls.lock().unwrap(),
        [
            "Observation/lab-1",
            "Observation/lab-2",
            "Observation/lab-3",
            "Observation/lab-4"
        ]
    );
    assert_eq!(
        db.cursor("lab-db", "id").unwrap().as_deref(),
        Some(r#"{"kind":"int","value":4}"#)
    );
    let record = db
        .query_json("SELECT raw, resources FROM records WHERE line = 1", [])
        .unwrap();
    assert_eq!(record[0]["raw"]["value"], 91.0);
    assert_eq!(record[0]["raw"]["patient"], "Patient/1");
    assert_eq!(record[0]["resources"][0]["valueQuantity"]["value"], 91.0);

    // Second pass: the fifth row. Third: nothing, so no load.
    runner.tick().await.unwrap();
    runner.tick().await.unwrap();
    let counts = db.feed_counts("lab-db").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["files"].as_i64()),
        (Some(5), Some(2))
    );

    // A new row arrives, and only it comes through.
    insert(pool.clone(), 6, 96.0).await;
    runner.tick().await.unwrap();
    let counts = db.feed_counts("lab-db").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["files"].as_i64()),
        (Some(6), Some(3))
    );
    assert_eq!(urls.lock().unwrap().last().unwrap(), "Observation/lab-6");

    // The connection test: connects, runs the query once past the cursor.
    let probe = Source::from_config(&feed)
        .unwrap()
        .with_db(db.clone())
        .probe()
        .await
        .unwrap();
    assert!(probe.reachable);
    assert_eq!(probe.objects, 0);
    insert(pool.clone(), 7, 97.0).await;
    let probe = Source::from_config(&feed)
        .unwrap()
        .with_db(db.clone())
        .probe()
        .await
        .unwrap();
    assert_eq!(
        (probe.objects, probe.newest.as_deref()),
        (1, Some("id = 7"))
    );

    // A source that starts later skips what is before its cursor_start.
    let later = lab_feed(&repo, "lab-db-late", Some("5"));
    let source = Source::from_config(&later).unwrap().with_db(db.clone());
    assert_eq!(source.list().await.unwrap(), ["query"]);
    let incoming = source.fetch("query").await.unwrap();
    let lines: Vec<Value> = String::from_utf8(incoming.bytes)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        lines
            .iter()
            .map(|l| l["id"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        [6, 7]
    );
    assert_eq!(source.acknowledge("query", "").await.unwrap(), "cursor 7");

    // A query that cannot run fails the pass with the database's words.
    unsafe { std::env::set_var("HIE_TEST_DB_URL", "sqlite::memory:") };
    let err = Source::from_config(&feed)
        .unwrap()
        .list()
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("no such table"), "{err:#}");
}

#[test]
fn a_database_source_is_checked_on_resolve() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let parse = |source: &str, delivery: &str| -> Result<hie::config::FeedConfig, String> {
        let mut feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
            "id: x\nname: X\nsource:\n  type: database\n{source}\ntransform: transforms/sql_lab_results.py\n{delivery}"
        ))
        .map_err(|e| e.to_string())?;
        feed.resolve(&repo).map_err(|e| format!("{e:#}"))?;
        Ok(feed)
    };
    let err = parse("  url_env: X\n  query: SELECT 1\n  cursor_column: id", "").unwrap_err();
    assert!(err.contains("{cursor}"), "{err}");
    let err = parse(
        "  url_env: X\n  query: SELECT 1 WHERE id > {cursor}\n  cursor_column: \"id; drop\"",
        "",
    )
    .unwrap_err();
    assert!(err.contains("column name"), "{err}");
    let err = parse(
        "  url_env: X\n  query: SELECT 1 WHERE id > {cursor}\n  cursor_column: id",
        "delivery:\n  method: bulk_submit\n  bulk_from: source_objects\n",
    )
    .unwrap_err();
    assert!(
        err.contains("database source has no file for HFS to fetch"),
        "{err}"
    );
    let ok = parse(
        "  url_env: X\n  query: SELECT 1 WHERE id > {cursor}\n  cursor_column: id",
        "",
    )
    .unwrap();
    assert_eq!(ok.format, hie::config::Format::Ndjson);
    // The variable is read when the source opens, not when the file is parsed.
    unsafe { std::env::remove_var("X") };
    let err = Source::from_config(&ok).map(drop).unwrap_err();
    assert!(format!("{err:#}").contains("X"), "{err:#}");
}

/// A table read by parts follows the keyset (`new_rows_by`, `key`): rows that
/// share a `new_rows_by` value across a run's boundary (1000 rows) are all
/// read once, and rows added later with that same value are read next.
#[tokio::test]
async fn a_table_read_never_skips_rows_that_share_new_rows_by() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("labs.db").display().to_string();
    let pool = SqlitePoolOptions::new()
        .connect(&format!("sqlite:{path}?mode=rwc"))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE lab_results (id INTEGER NOT NULL PRIMARY KEY, batch INTEGER NOT NULL, value TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    // 1500 rows in one batch value: a run of 1000 ends inside it.
    let insert = |from: i64, to: i64, batch: i64| {
        let pool = pool.clone();
        async move {
            for id in from..=to {
                sqlx::query("INSERT INTO lab_results (id, batch, value) VALUES (?1, ?2, 'x')")
                    .bind(id)
                    .bind(batch)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        }
    };
    insert(1, 1500, 7).await;

    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut feed: hie::config::FeedConfig = serde_json::from_value(json!({
        "id": "labs", "name": "Labs",
        "source": {"type": "database",
                   "connection": {"engine": "sqlite", "database": path},
                   "read": {"mode": "table", "table": {"name": "lab_results"}, "key": ["id"], "new_rows_by": "batch"}},
        "transform": "transforms/sql_lab_results.py",
    }))
    .unwrap();
    feed.resolve(&repo).unwrap();
    let db = Arc::new(Db::in_memory().unwrap());
    let source = Source::from_config_in(&feed, &hie::connectors::Context::default())
        .unwrap()
        .with_db(db);

    let ids = |bytes: Vec<u8>| -> Vec<i64> {
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|l| {
                serde_json::from_str::<Value>(l).unwrap()["id"]
                    .as_i64()
                    .unwrap()
            })
            .collect()
    };
    let key = source.list().await.unwrap().remove(0);
    let first = ids(source.fetch(&key).await.unwrap().bytes);
    assert_eq!(first, (1..=1500).collect::<Vec<_>>());
    source.acknowledge(&key, "").await.unwrap();
    assert!(source.list().await.unwrap().is_empty());

    // Later rows in the same batch, and in a later one.
    insert(1501, 1502, 7).await;
    insert(1503, 1503, 8).await;
    let key = source.list().await.unwrap().remove(0);
    assert_eq!(
        ids(source.fetch(&key).await.unwrap().bytes),
        [1501, 1502, 1503]
    );
    source.acknowledge(&key, "").await.unwrap();
    assert!(source.list().await.unwrap().is_empty());
}

/// A column that may be null cannot order a table read: its rows would be
/// skipped by the keyset.
#[tokio::test]
async fn a_nullable_ordering_column_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("labs.db").display().to_string();
    let pool = SqlitePoolOptions::new()
        .connect(&format!("sqlite:{path}?mode=rwc"))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE lab_results (id INTEGER NOT NULL PRIMARY KEY, seen TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut feed: hie::config::FeedConfig = serde_json::from_value(json!({
        "id": "labs", "name": "Labs",
        "source": {"type": "database",
                   "connection": {"engine": "sqlite", "database": path},
                   "read": {"mode": "table", "table": {"name": "lab_results"}, "new_rows_by": "seen"}},
        "transform": "transforms/sql_lab_results.py",
    }))
    .unwrap();
    feed.resolve(&repo).unwrap();
    let source = Source::from_config_in(&feed, &hie::connectors::Context::default()).unwrap();
    let error = format!("{:#}", source.list().await.unwrap_err());
    assert!(error.contains("may be null"), "{error}");
}

/// On PostgreSQL (`HIE_TEST_DATABASE_URL`): a table read by parts whose
/// `new_rows_by` is a `numeric` shared across a run's boundary, with a
/// `uuid` key, reads every row once, binding each position in its own type
/// and exactly; the password is a kept secret.
#[tokio::test]
async fn a_postgres_table_reads_by_exact_native_positions() {
    let Ok(url) = std::env::var("HIE_TEST_DATABASE_URL") else {
        eprintln!("skipping: HIE_TEST_DATABASE_URL is not set");
        return;
    };
    let parsed = url::Url::parse(&url).unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&url)
        .await
        .unwrap();
    let table = format!("keyset_{}", std::process::id());
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE public.{table} (u uuid NOT NULL PRIMARY KEY, amount numeric(30, 2) NOT NULL, note text)"
    )))
    .execute(&pool)
    .await
    .unwrap();
    // 1200 rows share one amount that a float cannot hold, then two later ones.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO public.{table} (u, amount) SELECT md5(i::text)::uuid, 12345678901234567.89 FROM generate_series(1, 1200) i"
    )))
    .execute(&pool)
    .await
    .unwrap();

    let db = Arc::new(Db::in_memory().unwrap());
    let keys = hie::crypto::Keyring::parse("Y29uZm9ybWFuY2Utc2VjcmV0cy1rZXktMzItYnl0ZXM=").unwrap();
    let secrets = Arc::new(hie::secrets::Secrets::new(db.clone(), Some(keys)));
    let password = percent(parsed.password().unwrap_or_default());
    let secret = db
        .transaction(|tx| {
            secrets
                .create_in(tx, "pg", &password)
                .map_err(|e| anyhow::anyhow!("{e:?}"))
        })
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut feed: hie::config::FeedConfig = serde_json::from_value(json!({
        "id": "pgkeys", "name": "Keys",
        "source": {"type": "database",
                   "connection": {"engine": "postgres", "host": parsed.host_str().unwrap(),
                                  "port": parsed.port().unwrap_or(5432),
                                  "database": parsed.path().trim_start_matches('/'),
                                  "user": percent(parsed.username()),
                                  "password": {"secret": secret, "revision": 1},
                                  "encrypt": false},
                   "read": {"mode": "table", "table": {"schema": "public", "name": table},
                            "key": ["u"], "new_rows_by": "amount"}},
        "transform": "transforms/sql_lab_results.py",
    }))
    .unwrap();
    feed.resolve(&repo).unwrap();
    let ctx = hie::connectors::Context {
        admission: Default::default(),
        secrets: Some(secrets),
    };
    let source = Source::from_config_in(&feed, &ctx)
        .unwrap()
        .with_db(db.clone());
    let read = |bytes: Vec<u8>| -> Vec<String> {
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|l| {
                serde_json::from_str::<Value>(l).unwrap()["u"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect()
    };
    let key = source.list().await.unwrap().remove(0);
    let first = read(source.fetch(&key).await.unwrap().bytes);
    source.acknowledge(&key, "").await.unwrap();
    let mut unique = first.clone();
    unique.sort();
    unique.dedup();
    assert_eq!((first.len(), unique.len()), (1200, 1200));
    assert!(source.list().await.unwrap().is_empty());

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO public.{table} (u, amount) VALUES ('ffffffff-ffff-ffff-ffff-ffffffffffff', 12345678901234567.89), ('00000000-0000-0000-0000-000000000001', 12345678901234567.90)"
    )))
    .execute(&pool)
    .await
    .unwrap();
    let key = source.list().await.unwrap().remove(0);
    let later = read(source.fetch(&key).await.unwrap().bytes);
    source.acknowledge(&key, "").await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP TABLE public.{table}")))
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        later,
        [
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "00000000-0000-0000-0000-000000000001"
        ]
    );
}

/// URL percent-decoding, without form rules (a `+` stays a `+`).
fn percent(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(b) = u8::from_str_radix(&text[i + 1..i + 3], 16)
        {
            out.push(b);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
