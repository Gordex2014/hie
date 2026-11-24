//! Alert rules end to end: the rule a source is born with, the CRUD, and the
//! rule kinds a test can provoke quickly — too many rejected rows, silence, and a
//! file missing by midnight (in UTC and in a zone) — each firing once and
//! clearing itself.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};

async fn batch(Json(bundle): Json<Value>) -> Json<Value> {
    let entries: Vec<Value> = bundle["entry"]
        .as_array()
        .unwrap()
        .iter()
        .map(|_| json!({"response": {"status": "201 Created"}}))
        .collect();
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
}

async fn serve(router: Router) -> String {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hie=warn")
        .with_test_writer()
        .try_init();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

fn python() -> Option<String> {
    let candidates = match std::env::var("HIE_TEST_PYTHON") {
        Ok(p) => vec![p],
        Err(_) => vec!["py".into(), "python3".into(), "python".into()],
    };
    candidates.into_iter().find(|p| {
        std::process::Command::new(p)
            .arg("-c")
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

struct Lab {
    _dir: tempfile::TempDir,
    root: PathBuf,
    api: String,
    http: reqwest::Client,
    db: std::sync::Arc<hie::db::Db>,
}

/// An engine with one folder source, `lab-results`, polling every minute so only
/// `check-now` moves it.
async fn lab(python: &str) -> Lab {
    lab_with(python, "").await
}

/// `lab`, with more lines for `hie.yaml` (an `alerts:` block).
async fn lab_with(python: &str, more_config: &str) -> Lab {
    let fhir = serve(Router::new().route("/", post(batch))).await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::create_dir_all(root.join("inbox")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {fhir}\ndatabase: data/hie.db\npython: {python}\n{more_config}"),
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let db = state.db.clone();
    let api = serve(hie::api::router(state)).await;
    let http = reqwest::Client::new();
    let created = http
        .post(format!("{api}/api/sources"))
        .json(&json!({
            "id": "lab-results", "name": "Lab results",
            "source": {"type": "folder", "path": "inbox", "pattern": "*.csv", "poll_seconds": 60, "min_file_age_ms": 0},
            "transform": "transforms/lab_results.py",
            "expect_every": "24h"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    Lab {
        _dir: dir,
        root,
        api,
        http,
        db,
    }
}

impl Lab {
    async fn get(&self, path: &str) -> Value {
        self.http
            .get(format!("{}{path}", self.api))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn post(&self, path: &str, body: Value) -> reqwest::Response {
        self.http
            .post(format!("{}{path}", self.api))
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    async fn check_now(&self) {
        self.post("/api/sources/lab-results/check-now", json!({}))
            .await;
    }

    async fn drop_file(&self, name: &str) {
        std::fs::copy(
            repo().join("examples/data").join(name),
            self.root.join("inbox").join(name),
        )
        .unwrap();
        self.check_now().await;
    }

    /// Reads `path` until `ok` holds, passing the source meanwhile.
    async fn wait_until(&self, path: &str, mut ok: impl FnMut(&Value) -> bool) -> Value {
        let started = Instant::now();
        loop {
            let value = self.get(path).await;
            if ok(&value) {
                return value;
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "timed out waiting on {path}: {value}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.check_now().await;
        }
    }

    async fn wait_for(&self, what: &str, mut ok: impl FnMut(&Value) -> bool) -> Value {
        let started = Instant::now();
        loop {
            let alerts = self.get("/api/alerts?source=lab-results").await;
            if ok(&alerts) {
                return alerts;
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "timed out waiting for {what}: {alerts}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.check_now().await;
        }
    }
}

fn alert<'a>(alerts: &'a Value, kind: &str) -> Option<&'a Value> {
    alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["kind"] == kind)
}

#[tokio::test]
async fn a_source_is_born_with_a_silence_rule_and_rules_are_managed() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;

    // The default rule follows expect_every.
    let rules = lab.get("/api/alert-rules?source=lab-results").await;
    let rules = rules["rules"].as_array().unwrap();
    assert_eq!(rules.len(), 1, "{rules:?}");
    assert_eq!(rules[0]["condition"], "silence");
    assert_eq!(rules[0]["threshold"], "24h");
    assert_eq!(rules[0]["channels"], json!(["app", "webhook"]));
    let default_id = rules[0]["id"].as_i64().unwrap();

    // Bad rules are refused with the reason.
    for (body, reason) in [
        (
            json!({"condition": "sunspots", "threshold": "1"}),
            "unknown condition",
        ),
        (
            json!({"condition": "missing_by", "threshold": "25:00"}),
            "HH:MM",
        ),
        (
            json!({"condition": "silence", "threshold": "2h", "source_id": "nope"}),
            "source nope not found",
        ),
        (
            json!({"condition": "silence", "threshold": "2h", "channels": ["pager"]}),
            "unknown channel",
        ),
    ] {
        let r = lab.post("/api/alert-rules", body).await;
        assert_eq!(r.status(), 400);
        let err = r.json::<Value>().await.unwrap();
        assert!(err["error"].as_str().unwrap().contains(reason), "{err}");
    }

    // Create, read back, replace, delete.
    let created = lab
        .post(
            "/api/alert-rules",
            json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "10%", "notify": ["ops@example.org"], "channels": ["app", "email"]}),
        )
        .await;
    assert_eq!(created.status(), 201);
    let created: Value = created.json().await.unwrap();
    let id = created["id"].as_i64().unwrap();
    assert_ne!(id, default_id);
    assert_eq!(created["notify"], json!(["ops@example.org"]));
    let read = lab.get(&format!("/api/alert-rules/{id}")).await;
    assert_eq!(read["threshold"], "10%");
    let replaced = lab
        .http
        .put(format!("{}/api/alert-rules/{id}", lab.api))
        .json(&json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "25", "enabled": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(replaced.status(), 200);
    let read = lab.get(&format!("/api/alert-rules/{id}")).await;
    assert_eq!(
        (read["threshold"].as_str(), read["enabled"].as_bool()),
        (Some("25"), Some(false))
    );
    let all = lab.get("/api/alert-rules").await;
    assert_eq!(all["rules"].as_array().unwrap().len(), 2);
    let deleted = lab
        .http
        .delete(format!("{}/api/alert-rules/{id}", lab.api))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    let gone = lab
        .http
        .get(format!("{}/api/alert-rules/{id}", lab.api))
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), 404);

    // The default rule, once deleted, stays deleted: a replace of the whole
    // definition, as a client saving one member sends it, adds none.
    let deleted = lab
        .http
        .delete(format!("{}/api/alert-rules/{default_id}", lab.api))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    let mut definition = lab.get("/api/sources/lab-results").await["definition"].clone();
    definition["contact"] = json!("Lab IT on-call");
    let replaced = lab
        .http
        .put(format!("{}/api/sources/lab-results", lab.api))
        .json(&definition)
        .send()
        .await
        .unwrap();
    assert_eq!(replaced.status(), 200);
    let rules = lab.get("/api/alert-rules?source=lab-results").await;
    assert_eq!(rules["rules"], json!([]), "{rules}");
}

#[tokio::test]
async fn too_many_rejected_rows_fire_once_and_clear_on_a_clean_load() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;
    let r = lab
        .post(
            "/api/alert-rules",
            json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "10%"}),
        )
        .await;
    assert_eq!(r.status(), 201);
    let rule_id = r.json::<Value>().await.unwrap()["id"].clone();

    // messy.csv: 5 of 6 rows fail, far more than 10%.
    lab.drop_file("messy.csv").await;
    let alerts = lab
        .wait_for("the rejected_share alert", |a| {
            alert(a, "rejected_share").is_some()
        })
        .await;
    let fired = alert(&alerts, "rejected_share").unwrap();
    assert_eq!(fired["rule_id"], rule_id);
    assert!(fired["cleared_at"].is_null());
    assert!(
        fired["message"].as_str().unwrap().contains("83.3%"),
        "{fired}"
    );
    assert_eq!(
        lab.get("/api/alerts?source=lab-results&open=true").await["alerts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // A clean load clears it, and only once.
    lab.drop_file("labs.csv").await;
    let alerts = lab
        .wait_for("the alert to clear", |a| {
            alert(a, "rejected_share").is_some_and(|x| !x["cleared_at"].is_null())
        })
        .await;
    let cleared = alert(&alerts, "rejected_share").unwrap();
    assert!(
        cleared["cleared_reason"]
            .as_str()
            .unwrap()
            .contains("within the limit"),
        "{cleared}"
    );
    let share_alerts = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["kind"] == "rejected_share")
        .count();
    assert_eq!(share_alerts, 1);
    assert!(
        lab.get("/api/alerts?source=lab-results&open=true").await["alerts"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn silence_fires_once_and_clears_when_data_arrives() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;
    // Tighten the default rule to one second.
    let rules = lab.get("/api/alert-rules?source=lab-results").await;
    let id = rules["rules"][0]["id"].as_i64().unwrap();
    let r = lab
        .http
        .put(format!("{}/api/alert-rules/{id}", lab.api))
        .json(&json!({"source_id": "lab-results", "condition": "silence", "threshold": "1s"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let alerts = lab
        .wait_for("the silence alert", |a| alert(a, "silence").is_some())
        .await;
    let fired = alert(&alerts, "silence").unwrap();
    assert_eq!(fired["rule_id"], id);
    assert!(
        fired["message"].as_str().unwrap().contains("every 1s"),
        "{fired}"
    );

    // Still one alert after more passes.
    lab.check_now().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    lab.check_now().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let alerts = lab.get("/api/alerts?source=lab-results").await;
    assert_eq!(alerts["alerts"].as_array().unwrap().len(), 1, "{alerts}");

    lab.drop_file("labs.csv").await;
    let alerts = lab
        .wait_for("silence to clear", |a| {
            alert(a, "silence").is_some_and(|x| !x["cleared_at"].is_null())
        })
        .await;
    assert_eq!(
        alert(&alerts, "silence").unwrap()["cleared_reason"],
        "Lab results: data arrived"
    );
}

// ---- rule ids and when rules were made -------------------------------------

fn new_rule(db: &hie::db::Db) -> i64 {
    let rule: hie::db::Rule = serde_json::from_value(
        json!({"source_id": "s", "condition": "silence", "threshold": "1h"}),
    )
    .unwrap();
    db.insert_rule(&rule).unwrap()
}

#[test]
fn a_rule_id_is_never_given_again_even_after_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hie.db");
    let db = hie::db::Db::open(&path).unwrap();
    let ids: Vec<i64> = (0..3).map(|_| new_rule(&db)).collect();
    assert!(db.delete_rule(ids[2]).unwrap());
    let next = new_rule(&db);
    assert!(next > ids[2], "{next} after {ids:?}");
    assert!(db.delete_rule(next).unwrap());
    // Opened again: still past every id used. On PostgreSQL the first handle
    // owns the test schema, so it stays open.
    let keep = std::env::var("HIE_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
        .map(|_| db);
    let again = hie::db::Db::open(&path).unwrap();
    assert!(new_rule(&again) > next);
    drop(keep);
}

#[test]
fn a_store_whose_rules_table_was_made_the_old_way_stops_reusing_ids() {
    if std::env::var("HIE_TEST_DATABASE_URL").is_ok_and(|u| !u.trim().is_empty()) {
        eprintln!("skipping: the old table is SQLite's");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hie.db");
    {
        let old = rusqlite::Connection::open(&path).unwrap();
        old.execute_batch(
            "CREATE TABLE alert_rules (
               id INTEGER PRIMARY KEY, source_id TEXT, condition TEXT NOT NULL, threshold TEXT NOT NULL,
               notify TEXT NOT NULL DEFAULT '[]', channels TEXT NOT NULL DEFAULT '[\"app\"]',
               enabled INTEGER NOT NULL DEFAULT 1, created_by TEXT, created_at INTEGER NOT NULL);
             INSERT INTO alert_rules (id, condition, threshold, created_at) VALUES
               (1, 'silence', '1h', 1000), (2, 'silence', '2h', 2000), (3, 'silence', '3h', 3000);",
        )
        .unwrap();
    }
    let db = hie::db::Db::open(&path).unwrap();
    // The rows and ids are kept.
    let rules = db.rules().unwrap();
    assert_eq!(
        rules
            .iter()
            .map(|r| (r.id, r.threshold.as_str()))
            .collect::<Vec<_>>(),
        [(1, "1h"), (2, "2h"), (3, "3h")]
    );
    assert_eq!(
        rules[0].created_at.as_deref(),
        Some("1970-01-01T00:00:01.000Z")
    );
    assert!(db.delete_rule(3).unwrap());
    assert!(new_rule(&db) > 3);
    drop(db);
    let sql: String = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name = 'alert_rules'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(sql.contains("AUTOINCREMENT"), "{sql}");
}

#[tokio::test]
async fn a_rule_answer_says_when_it_was_made_and_a_body_cannot() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("hie.yaml"),
        "fhir_base_url: http://127.0.0.1:9\ndatabase: data/hie.db\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&dir.path().join("hie.yaml")).unwrap();
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let api = serve(hie::api::router(state)).await;
    let http = reqwest::Client::new();
    let body =
        json!({"condition": "silence", "threshold": "1h", "created_at": "2001-01-01T00:00:00Z"});
    let created: Value = http
        .post(format!("{api}/api/alert-rules"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let made = created["created_at"].as_str().unwrap().to_string();
    assert!(
        made.ends_with('Z') && !made.starts_with("2001"),
        "{created}"
    );
    let id = created["id"].as_i64().unwrap();
    let replaced: Value = http
        .put(format!("{api}/api/alert-rules/{id}"))
        .json(&json!({"condition": "silence", "threshold": "2h", "created_at": "2001-01-01T00:00:00Z"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replaced["created_at"], made.as_str());
    assert_eq!(replaced["threshold"], "2h");
    let one: Value = reqwest::get(format!("{api}/api/alert-rules/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(one["created_at"], made.as_str());
    let all: Value = reqwest::get(format!("{api}/api/alert-rules"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        all["rules"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["created_at"].is_string())
    );
}

// ---- Reading alerts by period and in pages ------------------------------------

/// An engine with no source and no Python, whose alerts a test writes itself.
struct Store {
    _dir: tempfile::TempDir,
    _manager: std::sync::Arc<hie::manager::SourceManager>,
    db: std::sync::Arc<hie::db::Db>,
    api: String,
    http: reqwest::Client,
}

async fn store() -> Store {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("hie.yaml"),
        "fhir_base_url: http://127.0.0.1:9/fhir\ndatabase: data/hie.db\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&dir.path().join("hie.yaml")).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let db = state.db.clone();
    let api = serve(hie::api::router(state)).await;
    Store {
        _dir: dir,
        _manager: manager,
        db,
        api,
        http: reqwest::Client::new(),
    }
}

/// A whole second an hour ago: recent enough for any retention, and exact.
fn base_ms() -> i64 {
    (chrono::Utc::now().timestamp() - 3600) * 1000
}

fn iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

#[derive(Clone, Copy)]
struct Seeded {
    id: i64,
    source: &'static str,
    at: i64,
    open: bool,
}

impl Store {
    /// An alert created at exactly `at`. The store's write helpers are private,
    /// so the time is set through `query_json`, which runs any statement.
    fn seed(&self, source: &'static str, at: i64, open: bool) -> Seeded {
        let id = self
            .db
            .insert_alert(Some(source), "test", "a test alert", None)
            .unwrap();
        let updated = self
            .db
            .query_json(
                "UPDATE alerts SET created_at = $1 WHERE id = $2 RETURNING id",
                hie::db::params![at, id],
            )
            .unwrap();
        assert_eq!(updated, vec![json!({"id": id})]);
        if !open {
            self.db.clear_alert(id, "cleared by the test").unwrap();
        }
        Seeded {
            id,
            source,
            at,
            open,
        }
    }

    async fn list(&self, query: &[(&str, &str)]) -> (u16, Value) {
        let r = self
            .http
            .get(format!("{}/api/alerts", self.api))
            .query(query)
            .send()
            .await
            .unwrap();
        (r.status().as_u16(), r.json().await.unwrap())
    }

    /// Every page of a query, following `next_cursor` to its end: the ids in
    /// order. Each page has at most `limit` alerts, carries `next_cursor`, and
    /// only the first may be empty.
    async fn walk(&self, query: &[(&str, &str)], limit: usize) -> Vec<i64> {
        self.walk_from(query, limit, None).await
    }

    /// `walk`, starting after a cursor already in hand.
    async fn walk_from(
        &self,
        query: &[(&str, &str)],
        limit: usize,
        mut cursor: Option<String>,
    ) -> Vec<i64> {
        let limit_text = limit.to_string();
        let mut ids = Vec::new();
        for page in 0.. {
            assert!(page < 1000, "the walk does not end");
            let mut q = query.to_vec();
            q.push(("limit", &limit_text));
            if let Some(c) = &cursor {
                q.push(("cursor", c));
            }
            let (status, body) = self.list(&q).await;
            assert_eq!(status, 200, "{body}");
            let alerts = body["alerts"].as_array().unwrap();
            assert!(alerts.len() <= limit, "{body}");
            assert!(page == 0 || !alerts.is_empty(), "an empty page: {body}");
            ids.extend(alerts.iter().map(|a| a["id"].as_i64().unwrap()));
            match body.get("next_cursor") {
                Some(Value::String(c)) => cursor = Some(c.clone()),
                Some(Value::Null) => return ids,
                _ => panic!("no next_cursor: {body}"),
            }
        }
        unreachable!()
    }
}

fn newest_first(seeded: &[Seeded], keep: impl Fn(&Seeded) -> bool) -> Vec<i64> {
    let mut ids: Vec<i64> = seeded.iter().filter(|a| keep(a)).map(|a| a.id).collect();
    ids.sort_unstable_by(|a, b| b.cmp(a));
    ids
}

#[tokio::test]
async fn alerts_are_read_by_period_and_in_pages() {
    let store = store().await;
    let b = base_ms();
    // Ids follow this order; times do not always: a later id may be older, and
    // some share a millisecond. Two sources, open and cleared, on both sides
    // of each `since` below.
    let seeded: Vec<Seeded> = [
        ("lab", 0, true),
        ("imaging", 100, false),
        ("lab", 123, false),
        ("lab", 123, true),
        ("imaging", 200, true),
        ("lab", 50, true),
        ("lab", 300, true),
        ("lab", 300, false),
        ("imaging", 122, true),
        ("lab", 124, true),
        ("lab", 10, false),
    ]
    .into_iter()
    .map(|(source, offset, open)| store.seed(source, b + offset, open))
    .collect();

    // The boundary as a consumer has it: the `created_at` the list returned.
    let (_, all) = store.list(&[]).await;
    let third = seeded[2];
    let returned = all["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == third.id)
        .unwrap()["created_at"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(returned, iso(b + 123));

    // Each `since` and the first stored millisecond it keeps.
    let at_123 = iso(b + 123);
    let a_bit_after_123 = iso(b + 123).replace("Z", "1Z");
    let offset_form = chrono::DateTime::from_timestamp_millis(b + 123)
        .unwrap()
        .with_timezone(&chrono::FixedOffset::east_opt(2 * 3600).unwrap())
        .format("%Y-%m-%dT%H:%M:%S%.3f%:z")
        .to_string();
    let after_all = iso(b + 301);
    let sinces: [(Option<&str>, i64); 6] = [
        (None, i64::MIN),
        (Some(&returned), b + 123),
        (Some(&at_123), b + 123),
        (Some(&a_bit_after_123), b + 124),
        (Some(&offset_form), b + 123),
        (Some(&after_all), b + 301),
    ];
    assert!(offset_form.contains('+'), "{offset_form}");

    for (since, from) in sinces {
        for source in [None, Some("lab"), Some("imaging")] {
            for open in [false, true] {
                let mut query = Vec::new();
                if let Some(s) = source {
                    query.push(("source", s));
                }
                if open {
                    query.push(("open", "true"));
                }
                if let Some(s) = since {
                    query.push(("since", s));
                }
                let expected = newest_first(&seeded, |a| {
                    a.at >= from && source.is_none_or(|s| a.source == s) && (!open || a.open)
                });
                for limit in [1, 2, 3, 100] {
                    assert_eq!(
                        store.walk(&query, limit).await,
                        expected,
                        "{query:?} limit {limit}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn a_walk_keeps_its_place_while_alerts_change() {
    let store = store().await;
    let b = base_ms();
    let s: Vec<Seeded> = (0..6)
        .map(|i| store.seed("lab", b + i * 10, true))
        .collect();
    let open = [("open", "true"), ("limit", "2")];

    let (_, first) = store.list(&open).await;
    let page: Vec<i64> = first["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["id"].as_i64().unwrap())
        .collect();
    assert_eq!(page, vec![s[5].id, s[4].id]);
    let cursor = first["next_cursor"].as_str().unwrap().to_string();

    // Between pages: a new alert, one further down cleared, and the cursor's
    // own alert removed, as a purge would.
    let new = store.seed("lab", b + 1000, true);
    store
        .db
        .clear_alert(s[2].id, "cleared by the test")
        .unwrap();
    let deleted = store
        .db
        .query_json(
            "DELETE FROM alerts WHERE id = $1 RETURNING id",
            hie::db::params![s[4].id],
        )
        .unwrap();
    assert_eq!(deleted, vec![json!({"id": s[4].id})]);

    let rest = store
        .walk_from(&[("open", "true")], 2, Some(cursor.clone()))
        .await;
    assert_eq!(rest, vec![s[3].id, s[1].id, s[0].id]);
    // Without `open`, the cleared alert stays where it was.
    let rest = store.walk_from(&[], 2, Some(cursor)).await;
    assert_eq!(rest, vec![s[3].id, s[2].id, s[1].id, s[0].id]);

    // The new alert is on a fresh first page.
    let (_, fresh) = store.list(&open).await;
    assert_eq!(fresh["alerts"][0]["id"], new.id);

    // A cursor whose remaining alerts all left leads to an empty last page.
    let (_, four) = store.list(&[("open", "true"), ("limit", "4")]).await;
    assert_eq!(four["alerts"][3]["id"], s[1].id);
    let cursor = four["next_cursor"].as_str().unwrap().to_string();
    store
        .db
        .clear_alert(s[0].id, "cleared by the test")
        .unwrap();
    let (status, empty) = store
        .list(&[("open", "true"), ("limit", "4"), ("cursor", &cursor)])
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        empty,
        json!({"alerts": [], "next_cursor": null, "webhook": {"configured": false, "signed": false}})
    );
}

#[tokio::test]
async fn the_default_page_holds_a_hundred_alerts() {
    let store = store().await;
    let b = base_ms();
    let s: Vec<Seeded> = (0..101).map(|i| store.seed("lab", b + i, true)).collect();

    let (status, first) = store.list(&[]).await;
    assert_eq!(status, 200);
    let alerts = first["alerts"].as_array().unwrap();
    assert_eq!(alerts.len(), 100);
    assert_eq!(alerts[0]["id"], s[100].id);
    assert_eq!(alerts[99]["id"], s[1].id);
    let cursor = first["next_cursor"].as_str().unwrap();
    let (_, last) = store.list(&[("cursor", cursor)]).await;
    assert_eq!(last["alerts"].as_array().unwrap().len(), 1);
    assert_eq!(last["alerts"][0]["id"], s[0].id);
    assert_eq!(last.get("next_cursor"), Some(&Value::Null));

    let (_, none) = store.list(&[("limit", "0")]).await;
    assert_eq!(
        none,
        json!({"alerts": [], "next_cursor": null, "webhook": {"configured": false, "signed": false}})
    );
    // At most 1000 a page: every alert is read by following the cursor.
    let (status, most) = store.list(&[("limit", "1000")]).await;
    assert_eq!(status, 200);
    assert_eq!(most["alerts"].as_array().unwrap().len(), 101);
    assert_eq!(most.get("next_cursor"), Some(&Value::Null));
    // Empty values are absent ones.
    let (status, empty) = store.list(&[("since", ""), ("cursor", "")]).await;
    assert_eq!(status, 200);
    assert_eq!(empty["alerts"].as_array().unwrap().len(), 100);
}

#[tokio::test]
async fn a_malformed_query_is_refused_without_echoing_it() {
    let store = store().await;
    store.seed("lab", base_ms(), true);
    type Case<'a> = (&'a [(&'a str, &'a str)], Option<&'a str>);
    let cases: [Case; 17] = [
        (&[("since", "yesterday")], Some("since")),
        (&[("since", "2025-01-02")], Some("since")),
        (&[("since", "2016-12-31T23:59:60Z")], Some("since")),
        (
            &[("since", "2025-01-02T00:00:00.1230000001Z")],
            Some("since"),
        ),
        (&[("cursor", "abc")], Some("cursor")),
        (&[("cursor", "17")], Some("cursor")),
        (&[("cursor", "a1.-1")], Some("cursor")),
        (&[("cursor", "a1.0")], Some("cursor")),
        (&[("cursor", "a1.+5")], Some("cursor")),
        (&[("cursor", "a1.99999999999999999999")], Some("cursor")),
        (&[("limit", "0"), ("since", "garbage")], Some("since")),
        (&[("limit", "abc")], None),
        // A negative limit once meant every alert; a page is now bounded.
        (&[("limit", "-1")], Some("limit")),
        (&[("limit", "1001")], Some("limit")),
        (&[("limit", "-12345")], Some("limit")),
        (&[("open", "yes")], None),
        (&[("limit", "1"), ("limit", "2")], None),
    ];
    for (query, field) in cases {
        let (status, body) = store.list(query).await;
        assert_eq!(status, 400, "{query:?}: {body}");
        assert_eq!(body["code"], "invalid_query", "{query:?}: {body}");
        assert_eq!(body["field"].as_str(), field, "{query:?}: {body}");
        assert!(body["error"].is_string(), "{body}");
        for (_, value) in query {
            if value.len() > 2 {
                assert!(!body.to_string().contains(value), "echoed: {body}");
            }
        }
    }
    // The rules list still checks the types of what it ignores.
    for query in ["limit=abc", "open=yes"] {
        let r = store
            .http
            .get(format!("{}/api/alert-rules?{query}", store.api))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "{query}");
    }
}

/// What a webhook receiver got: headers and raw body of each request.
type Received = Arc<Mutex<Vec<(HeaderMap, Bytes)>>>;

async fn receive(State(got): State<Received>, headers: HeaderMap, body: Bytes) {
    got.lock().unwrap().push((headers, body));
}

fn webhook_of<'a>(alerts: &'a Value, id: &Value) -> &'a Value {
    &alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == *id)
        .unwrap()["webhook"]
}

#[tokio::test]
async fn the_webhook_gets_each_alert_change_and_the_api_shows_its_delivery() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let got = Received::default();
    let receiver = serve(
        Router::new()
            .route("/hook/urlcanary", post(receive))
            .with_state(got.clone()),
    )
    .await;
    let url = format!("{receiver}/hook/urlcanary");
    let lab = lab_with(
        &python,
        &format!(
            "alerts:\n  webhook_url: {url}\n  webhook_secret: secretcanary\n  webhook_backoff_ms: 50\n"
        ),
    )
    .await;
    let rule = |threshold: &str, channels: Value| json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": threshold, "channels": channels});
    let posted = lab
        .post("/api/alert-rules", rule("10%", json!(["app", "webhook"])))
        .await;
    let webhook_rule = posted.json::<Value>().await.unwrap()["id"].clone();
    let posted = lab
        .post("/api/alert-rules", rule("20%", json!(["app"])))
        .await;
    let app_rule = posted.json::<Value>().await.unwrap()["id"].clone();

    // An alert left open by an earlier version: no webhook event recorded.
    let earlier = lab.db.query_json(
        "INSERT INTO alerts (feed_id, kind, message, created_at, rule_id, notify, channels)
         VALUES ('lab-results', 'rejected_share', 'from before', 1700000000123, $1, '[]', '[\"app\",\"webhook\"]')
         RETURNING id",
        vec![hie::db::Param::Int(webhook_rule.as_i64().unwrap())],
    ).unwrap()[0]["id"].clone();
    // Two events that are not due: one for this URL, one for another.
    use sha2::Digest;
    let ours = hex::encode(&sha2::Sha256::digest(url.as_bytes())[..8]);
    let mut waiting = vec![];
    for (event_id, destination) in [
        ("waits-here", ours.as_str()),
        ("waits-elsewhere", "0123456789abcdef"),
    ] {
        let id = lab.db.query_json(
            "INSERT INTO alerts (feed_id, kind, message, created_at) VALUES ('other', 'unreadable_file', 'x', 1) RETURNING id",
            vec![],
        ).unwrap()[0]["id"].clone();
        lab.db.query_json(
            "INSERT INTO alert_webhooks (event_id, alert_id, event, destination, payload, state, attempts, next_attempt_at, created_at)
             VALUES ($1, $2, 'fired', $3, '{}', 'pending', 0, 9000000000000000, 1) RETURNING id",
            vec![
                hie::db::Param::Text(event_id.into()),
                hie::db::Param::Int(id.as_i64().unwrap()),
                hie::db::Param::Text(destination.into()),
            ],
        ).unwrap();
        waiting.push(id);
    }
    let all = lab.get("/api/alerts").await;
    assert_eq!(webhook_of(&all, &waiting[0])["state"], "pending");
    assert_eq!(webhook_of(&all, &waiting[1])["state"], "parked");
    assert_eq!(
        webhook_of(&all, &waiting[1])["events"],
        json!([{"event": "fired", "event_id": "waits-elsewhere", "state": "parked", "attempts": 0,
                "last_error": null, "last_status": null, "delivered_at": null}])
    );
    // A later `cleared` delivered to this URL does not hide the `fired` still
    // parked for the other one: the latest event is the clear, and `events`
    // lists both, oldest first.
    lab.db
        .query_json(
            "INSERT INTO alert_webhooks (event_id, alert_id, event, destination, payload, state, attempts, last_status, delivered_at, next_attempt_at, created_at)
             VALUES ('cleared-here', $1, 'cleared', $2, '{}', 'delivered', 1, 200, 1700000000456, 0, 2) RETURNING id",
            vec![
                hie::db::Param::Int(waiting[1].as_i64().unwrap()),
                hie::db::Param::Text(ours.clone()),
            ],
        )
        .unwrap();
    let all = lab.get("/api/alerts").await;
    let hook = webhook_of(&all, &waiting[1]);
    assert_eq!(
        (hook["event"].as_str(), hook["state"].as_str()),
        (Some("cleared"), Some("delivered"))
    );
    assert_eq!(hook["delivered_at"], "2023-11-14T22:13:20.456Z");
    let states: Vec<_> = hook["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["event_id"].as_str().unwrap(),
                e["state"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        states,
        [("waits-elsewhere", "parked"), ("cleared-here", "delivered")]
    );
    assert_eq!(hook["events"][1]["delivered_at"], hook["delivered_at"]);
    // Two events of one alert still make one alert of the page.
    let paged = lab.get("/api/alerts?source=other&limit=1").await;
    assert_eq!(paged["alerts"].as_array().unwrap().len(), 1);
    assert_eq!(paged["alerts"][0]["id"], waiting[1]);
    assert!(paged["next_cursor"].is_string());
    let alerts = lab.get("/api/alerts?source=lab-results").await;
    assert_eq!(
        alerts["webhook"],
        json!({"configured": true, "signed": true})
    );
    assert_eq!(*webhook_of(&alerts, &earlier), Value::Null);

    // messy.csv: the webhook rule's alert is still open, so only the app-only rule fires.
    lab.drop_file("messy.csv").await;
    let alerts = lab
        .wait_for("the app-only alert", |a| {
            a["alerts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|x| x["rule_id"] == app_rule)
        })
        .await;
    let app_alert = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["rule_id"] == app_rule)
        .unwrap()
        .clone();
    let hook = webhook_of(&alerts, &app_alert["id"]);
    assert_eq!(
        (hook["event"].as_str(), hook["state"].as_str()),
        (Some("fired"), Some("not_requested"))
    );
    assert_eq!(hook["attempts"], 0);

    // labs.csv clears both; the earlier alert's clear goes out, the app-only one's does not.
    lab.drop_file("labs.csv").await;
    let alerts = lab
        .wait_for("the clear to be delivered", |a| {
            webhook_of(a, &earlier)["state"] == "delivered"
                && !webhook_of(a, &app_alert["id"]).is_null()
                && webhook_of(a, &app_alert["id"])["event"] == "cleared"
        })
        .await;
    let hook = webhook_of(&alerts, &earlier);
    assert_eq!(hook["event"], "cleared");
    assert_eq!(hook["attempts"], 1);
    assert_eq!(hook["last_status"], 200);
    assert_eq!(hook["last_error"], Value::Null);
    assert!(hook["delivered_at"].is_string());
    assert_eq!(
        webhook_of(&alerts, &app_alert["id"])["state"],
        "not_requested"
    );

    let got = got.lock().unwrap().clone();
    assert_eq!(got.len(), 1, "only the webhook rule's clear is posted");
    let (headers, body) = &got[0];
    assert_eq!(
        headers["x-hie-signature"].to_str().unwrap(),
        hie::alerts::signature(
            b"secretcanary",
            headers["x-hie-timestamp"].to_str().unwrap(),
            body
        )
    );
    let payload: Value = serde_json::from_slice(body).unwrap();
    assert_eq!(payload["event_id"], hook["event_id"]);
    assert_eq!(
        payload["source"],
        json!({"id": "lab-results", "name": "Lab results"})
    );
    let listed = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == earlier)
        .unwrap();
    // The times are the strings the API shows.
    assert_eq!(payload["alert"]["created_at"], listed["created_at"]);
    assert_eq!(payload["alert"]["created_at"], "2023-11-14T22:13:20.123Z");
    assert_eq!(payload["alert"]["cleared_at"], listed["cleared_at"]);
    assert_eq!(payload["alert"]["message"], "from before");
    // Neither the URL nor the secret is in any answer, nor is the stored fingerprint.
    let answer = lab.get("/api/alerts").await.to_string();
    for canary in ["urlcanary", "secretcanary", ours.as_str()] {
        assert!(!answer.contains(canary), "{canary} in {answer}");
    }
}

#[tokio::test]
async fn without_a_webhook_the_api_says_so() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;
    lab.post(
        "/api/alert-rules",
        json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "10%"}),
    )
    .await;
    lab.drop_file("messy.csv").await;
    let alerts = lab
        .wait_for("the alert", |a| alert(a, "rejected_share").is_some())
        .await;
    assert_eq!(
        alerts["webhook"],
        json!({"configured": false, "signed": false})
    );
    let hook = &alert(&alerts, "rejected_share").unwrap()["webhook"];
    assert_eq!(
        (hook["event"].as_str(), hook["state"].as_str()),
        (Some("fired"), Some("not_configured"))
    );
    assert_eq!(hook["attempts"], 0);
    assert_eq!(hook["last_status"], Value::Null);
}

/// The alert a rule raised, by rule id.
fn alert_of(alerts: &Value, rule: i64) -> Option<&Value> {
    alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["rule_id"] == rule)
}

#[tokio::test]
async fn missing_by_rules_take_a_time_zone_and_fire_in_it() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;

    // Refusals: a zone that does not exist, and a zone on a rule without a time of day.
    for (body, reason) in [
        (
            json!({"condition": "missing_by", "threshold": "06:00", "tz": "Mars/Base"}),
            "tz must be an IANA time zone name, such as Europe/Madrid or UTC",
        ),
        (
            json!({"condition": "missing_by", "threshold": "06:00", "tz": "Etc/Unknown"}),
            "tz must be an IANA time zone name, such as Europe/Madrid or UTC",
        ),
        (
            json!({"condition": "silence", "threshold": "2h", "tz": "Europe/Madrid"}),
            "tz applies only to missing_by rules",
        ),
    ] {
        let r = lab.post("/api/alert-rules", body).await;
        assert_eq!(r.status(), 400);
        assert_eq!(r.json::<Value>().await.unwrap()["error"], reason);
    }

    // 00:00 has always passed, in any zone: both rules are due at once. The
    // zone is kept as the database spells it; no zone reads back as null (UTC).
    // Of two zones 8½ hours apart, the one not near its midnight, so that the
    // file dropped below is today's when it is checked.
    let now = chrono::Utc::now();
    let (asked, zone) = [
        ("pacific/kiritimati", "Pacific/Kiritimati"),
        ("asia/kolkata", "Asia/Kolkata"),
    ]
    .into_iter()
    .find(|(_, name)| {
        let (_, start, end) = hie::zone::Zone::parse(name).unwrap().today_bounds(now);
        let at = now.timestamp_millis();
        at - start > 3_600_000 && end - at > 3_600_000
    })
    .unwrap();
    let created = lab
        .post(
            "/api/alert-rules",
            json!({"source_id": "lab-results", "condition": "missing_by", "threshold": "00:00", "tz": asked}),
        )
        .await;
    assert_eq!(created.status(), 201);
    let zoned: Value = created.json().await.unwrap();
    assert_eq!(zoned["tz"], zone);
    let zoned = zoned["id"].as_i64().unwrap();
    assert_eq!(
        lab.get(&format!("/api/alert-rules/{zoned}")).await["tz"],
        zone
    );
    let created = lab
        .post(
            "/api/alert-rules",
            json!({"source_id": "lab-results", "condition": "missing_by", "threshold": "00:00"}),
        )
        .await;
    assert_eq!(created.status(), 201);
    let plain: Value = created.json().await.unwrap();
    assert_eq!(plain["tz"], Value::Null);
    let plain = plain["id"].as_i64().unwrap();

    let alerts = lab
        .wait_for("both missing_by alerts", |a| {
            alert_of(a, zoned).is_some() && alert_of(a, plain).is_some()
        })
        .await;
    assert_eq!(
        alert_of(&alerts, plain).unwrap()["message"],
        "Lab results: nothing has arrived today and the file was expected by 00:00 UTC"
    );
    assert_eq!(
        alert_of(&alerts, zoned).unwrap()["message"],
        format!("Lab results: nothing has arrived today and the file was expected by 00:00 {zone}")
    );

    // A file now is today's in every zone: both clear.
    lab.drop_file("labs.csv").await;
    let alerts = lab
        .wait_for("both missing_by alerts to clear", |a| {
            [zoned, plain]
                .iter()
                .all(|id| alert_of(a, *id).is_some_and(|x| !x["cleared_at"].is_null()))
        })
        .await;
    assert_eq!(
        alert_of(&alerts, zoned).unwrap()["cleared_reason"],
        format!(
            "Lab results: a file arrived on {}",
            hie::zone::Zone::parse(zone)
                .unwrap()
                .today_bounds(chrono::Utc::now())
                .0
        )
    );

    // Replacing the rule without a zone puts it back on UTC.
    let r = lab
        .http
        .put(format!("{}/api/alert-rules/{zoned}", lab.api))
        .json(&json!({"source_id": "lab-results", "condition": "missing_by", "threshold": "06:00"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        lab.get(&format!("/api/alert-rules/{zoned}")).await["tz"],
        Value::Null
    );
}

#[tokio::test]
async fn stats_count_local_days_and_echo_the_zone() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;
    let received_after = chrono::Utc::now();
    lab.drop_file("labs.csv").await;
    // The file is in the day it was received in, in each zone, and the last day
    // is that zone's today; both may move on by one if a local midnight passes
    // during the test.
    for (query, zone) in [
        ("days=3", "UTC"),
        ("days=3&tz=pacific/kiritimati", "Pacific/Kiritimati"),
        ("days=3&tz=Pacific/Pago_Pago", "Pacific/Pago_Pago"),
        ("days=3&tz=Etc/GMT%2B5", "Etc/GMT+5"),
    ] {
        let stats = lab
            .wait_until(&format!("/api/sources/lab-results/stats?{query}"), |s| {
                s["by_day"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|d| d["files"] == 1)
            })
            .await;
        let z = hie::zone::Zone::parse(zone).unwrap();
        let (earliest, latest) = (
            z.today(received_after).to_string(),
            z.today(chrono::Utc::now()).to_string(),
        );
        let within = |day: &Value| {
            let day = day.as_str().unwrap();
            earliest.as_str() <= day && day <= latest.as_str()
        };
        assert_eq!(stats["tz"], zone, "{query}");
        let days = stats["by_day"].as_array().unwrap();
        assert_eq!(days.len(), 3);
        let with_file = days.iter().find(|d| d["files"] == 1).unwrap();
        assert!(within(&with_file["day"]), "{query}: {stats}");
        assert!(within(&days[2]["day"]), "{query}: {stats}");
        assert_eq!(stats["today"]["day"], days[2]["day"], "{query}");
        assert_eq!(stats["today"]["files"], days[2]["files"], "{query}");
    }
    // Kiritimati (+14) and Pago Pago (-11) are a day and more apart.
    let east = lab
        .get("/api/sources/lab-results/stats?days=1&tz=Pacific/Kiritimati")
        .await;
    let west = lab
        .get("/api/sources/lab-results/stats?days=1&tz=Pacific/Pago_Pago")
        .await;
    assert_ne!(east["by_day"][0]["day"], west["by_day"][0]["day"]);

    // An unknown zone is refused before the source is looked up.
    let r = lab
        .http
        .get(format!(
            "{}/api/sources/no-such-source/stats?tz=Mars/Base",
            lab.api
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    for zone in ["Mars/Base", "", "Etc/Unknown"] {
        let r = lab
            .http
            .get(format!("{}/api/sources/lab-results/stats", lab.api))
            .query(&[("tz", zone)])
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "{zone:?}");
        assert_eq!(
            r.json::<Value>().await.unwrap(),
            json!({
                "error": "tz must be an IANA time zone name, such as Europe/Madrid or UTC",
                "code": "invalid_query",
                "field": "tz"
            })
        );
    }
}

/// A `rejected_share` rule weighs the failing rows among those a load kept:
/// rows its transform filtered out on purpose count on neither side, and a
/// load that kept none neither fires nor clears it.
#[tokio::test]
async fn rejected_share_counts_failing_rows_among_those_kept() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;
    let r = lab
        .post(
            "/api/alert-rules",
            json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "10%"}),
        )
        .await;
    assert_eq!(r.status(), 201);
    let sieve = "from hie import DataError\n\n\ndef transform(record, context):\n    if record['kind'] == 'skip':\n        return None\n    if record['kind'] == 'bad':\n        raise DataError('a bad row')\n    return {'resourceType': 'Patient', 'id': 'p' + record['n']}\n";
    let r = lab
        .http
        .put(format!("{}/api/transforms/lab_results", lab.api))
        .json(&json!({ "code": sieve }))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    let rows = |kinds: &[&str]| {
        let mut csv = String::from("n,kind\n");
        for (n, kind) in kinds.iter().enumerate() {
            csv.push_str(&format!("{n},{kind}\n"));
        }
        csv
    };
    let drop = |name: &str, content: String| {
        std::fs::write(lab.root.join("inbox").join(name), content).unwrap();
    };
    let done = |name: &'static str| {
        move |l: &Value| {
            l["loads"]
                .as_array()
                .unwrap()
                .iter()
                .any(|f| f["name"] == name && f["status"] == "done" && f["in_progress"] == 0)
        }
    };
    let share_alerts = |alerts: &Value| -> Vec<Value> {
        alerts["alerts"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| a["kind"] == "rejected_share")
            .cloned()
            .collect()
    };

    // Every row filtered out: the load kept nothing, so the rule says nothing.
    drop("all-filtered.csv", rows(&["skip"; 4]));
    lab.check_now().await;
    lab.wait_until("/api/sources/lab-results/loads", done("all-filtered.csv"))
        .await;

    // Nine rows filtered out and one sent: nothing failed.
    let mut kept_one = vec!["skip"; 9];
    kept_one.push("keep");
    drop("kept.csv", rows(&kept_one));
    lab.check_now().await;
    lab.wait_until("/api/sources/lab-results/loads", done("kept.csv"))
        .await;

    // Nine filtered out and one failing: all of what the load kept failed. Over
    // every row it would be 10%, within the limit.
    let mut failed_one = vec!["skip"; 9];
    failed_one.push("bad");
    drop("failed.csv", rows(&failed_one));
    let alerts = lab
        .wait_for("the rejected_share alert", |a| !share_alerts(a).is_empty())
        .await;
    let fired = share_alerts(&alerts);
    assert_eq!(
        fired.len(),
        1,
        "all-filtered.csv and kept.csv fired nothing: {alerts}"
    );
    assert_eq!(
        fired[0]["message"],
        "Lab results: 100.0% of the rows kept from failed.csv failed, more than the 10% allowed (1 of 1, 9 others were filtered out by its transform)"
    );

    // A load that kept nothing says nothing either way.
    drop("none-kept.csv", rows(&["skip", "skip", "skip"]));
    lab.check_now().await;
    lab.wait_until("/api/sources/lab-results/loads", done("none-kept.csv"))
        .await;

    // A load within the limit clears it, named in the reason.
    drop("clean.csv", rows(&["keep", "keep"]));
    let alerts = lab
        .wait_for("the alert to clear", |a| {
            share_alerts(a)
                .first()
                .is_some_and(|x| !x["cleared_at"].is_null())
        })
        .await;
    let cleared = share_alerts(&alerts);
    assert_eq!(cleared.len(), 1);
    assert_eq!(
        cleared[0]["cleared_reason"],
        "Lab results: clean.csv loaded with 0.0% failed, within the limit",
        "none-kept.csv did not clear it"
    );
}

#[tokio::test]
async fn rows_filtered_out_are_not_failures() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;
    let put_transform = |code: String| {
        lab.http
            .put(format!("{}/api/transforms/lab_results", lab.api))
            .json(&json!({ "code": code }))
            .send()
    };
    // A transform that keeps nothing: every row is rejected as `filtered`.
    let r = put_transform("def transform(record, context):\n    return []\n".into())
        .await
        .unwrap();
    assert!(r.status().is_success());
    let rows = std::fs::read_to_string(repo().join("examples/data/labs.csv"))
        .unwrap()
        .lines()
        .skip(1)
        .filter(|l| !l.trim().is_empty())
        .count() as i64;
    lab.drop_file("labs.csv").await;
    let stats = lab
        .wait_until("/api/sources/lab-results/stats?days=1", |s| {
            s["today"]["rejected"].as_i64() == Some(rows)
        })
        .await;
    let filtered = rows;
    assert_eq!(stats["today"]["failing"], 0, "{stats}");
    assert_eq!(stats["by_day"][0]["failing"], 0, "{stats}");
    let status = lab.get("/api/monitoring/status").await;
    let source = &status["sources"][0];
    assert_eq!(source["rejected_last_24h"], filtered, "{status}");
    assert_eq!(source["failing_last_24h"], 0, "{status}");
    assert_eq!(status["last_24h"]["rejected"], filtered);
    assert_eq!(status["last_24h"]["failing"], 0);
    assert_eq!(status["needs_you"], json!([]), "{status}");
    // The load does not fail; its filtered rows are still listed by reason, to
    // be read and retried.
    let loads = lab.get("/api/sources/lab-results/loads").await;
    let load = lab
        .get(&format!("/api/loads/{}", loads["loads"][0]["id"]))
        .await;
    assert_eq!(
        (&load["failed"], &load["failing"]),
        (&json!(false), &json!(0))
    );
    assert_eq!(load["failures"][0]["kind"], "filtered", "{load}");
    assert_eq!(load["failures"][0]["count"], filtered, "{load}");

    // Rows that fail are a reason to look.
    let code = std::fs::read_to_string(repo().join("transforms/lab_results.py")).unwrap();
    assert!(put_transform(code).await.unwrap().status().is_success());
    lab.drop_file("messy.csv").await;
    lab.wait_until("/api/sources/lab-results/loads", |l| {
        let loads = l["loads"].as_array().unwrap();
        loads.len() == 2
            && loads
                .iter()
                .all(|f| f["in_progress"] == 0 && f["status"] != "processing")
    })
    .await;
    let status = lab.get("/api/monitoring/status").await;
    assert!(
        status["sources"][0]["failing_last_24h"].as_i64() > Some(0),
        "{status}"
    );
    let source = &status["sources"][0];
    let failing = source["failing_last_24h"].as_i64().unwrap();
    assert_eq!(
        source["rejected_last_24h"].as_i64().unwrap(),
        filtered + failing,
        "{status}"
    );
    assert_eq!(status["last_24h"]["failing"], failing);
    assert_eq!(
        status["needs_you"][0]["reason"],
        format!("{failing} records rejected in the last 24 hours")
    );
    let stats = lab.get("/api/sources/lab-results/stats?days=1").await;
    assert_eq!(stats["today"]["failing"], failing, "{stats}");
    // Without a `rejected_share` rule a load with failing rows raises an alert;
    // the earlier load, whose rows were all filtered out, raised none.
    let alerts = lab
        .wait_for("the rejected_records alert", |a| {
            alert(a, "rejected_records").is_some()
        })
        .await;
    let raised: Vec<&Value> = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["kind"] == "rejected_records")
        .collect();
    assert_eq!(raised.len(), 1, "{alerts}");
    assert!(
        raised[0]["message"].as_str().unwrap().starts_with(&format!(
            "Lab results: {failing} of 6 records failed in messy.csv."
        )),
        "{alerts}"
    );
}
