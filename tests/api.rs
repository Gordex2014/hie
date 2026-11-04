//! The API end to end over a real listener: a source created, tested, fed, paused,
//! resumed and deleted, with a stand-in FHIR server behind it.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};

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
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

async fn wait_until(
    mut f: impl FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = bool>>>,
    what: &str,
) {
    let started = Instant::now();
    while !f().await {
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn a_source_lives_through_the_api() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let urls: Urls = Arc::default();
    let fhir = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(urls.clone()),
    )
    .await;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {fhir}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    assert!(feeds.is_empty());
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let api = serve(hie::api::router(state)).await;
    let http = reqwest::Client::new();

    let health: Value = http
        .get(format!("{api}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["status"], "ok");
    assert_eq!(health["service"], "hie");
    assert_eq!(health["version"], env!("CARGO_PKG_VERSION"));
    assert!(health["uptime_seconds"].as_f64().unwrap() >= 0.0);
    assert!(health["started_at"].is_string() && health["timestamp"].is_string());

    // Nothing yet.
    let listed: Value = http
        .get(format!("{api}/api/sources"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["sources"].as_array().unwrap().len(), 0);

    // Test the connection before creating anything: the inbox is empty.
    std::fs::create_dir_all(root.join("inbox")).unwrap();
    let probe: Value = http
        .post(format!("{api}/api/sources/test"))
        .json(&json!({"source": {"type": "folder", "path": "inbox", "pattern": "*.csv"}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (probe["reachable"].as_bool(), probe["objects"].as_u64()),
        (Some(true), Some(0))
    );

    // A bad definition is refused with the reason.
    let bad = http
        .post(format!("{api}/api/sources"))
        .json(&json!({"id": "Bad", "name": "x", "source": {"type": "folder", "path": "inbox"}, "transform": "transforms/lab_results.py"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    assert!(
        bad.json::<Value>().await.unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("lowercase")
    );

    // Create it: the definition is written as a file and the source starts.
    let definition = json!({
        "id": "lab-results", "name": "Lab results",
        "source": {"type": "folder", "path": "inbox", "pattern": "*.csv", "poll_seconds": 60, "min_file_age_ms": 0},
        "transform": "transforms/lab_results.py",
        "expect_every": "24h"
    });
    let created = http
        .post(format!("{api}/api/sources"))
        .json(&definition)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let created: Value = created.json().await.unwrap();
    assert_eq!(created["state"], "waiting");
    assert_eq!(created["definition"]["source"]["path"], "inbox");
    let file = std::fs::read_to_string(root.join("sources/lab-results.yaml")).unwrap();
    assert!(file.contains("id: lab-results") && file.contains("expect_every: 24h"));
    let again = http
        .post(format!("{api}/api/sources"))
        .json(&definition)
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), 409);

    // Drop a file and ask for a pass now, instead of waiting for the 60 s poll.
    std::fs::copy(
        repo().join("examples/data/labs.csv"),
        root.join("inbox/labs.csv"),
    )
    .unwrap();
    let probe: Value = http
        .post(format!("{api}/api/sources/test"))
        .json(&definition)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (probe["objects"].as_u64(), probe["newest"].as_str()),
        (Some(1), Some("labs.csv"))
    );
    let checking = http
        .post(format!("{api}/api/sources/lab-results/check-now"))
        .send()
        .await
        .unwrap();
    assert_eq!(checking.status(), 202);
    let api2 = api.clone();
    let http2 = http.clone();
    wait_until(
        move || {
            let (api, http) = (api2.clone(), http2.clone());
            Box::pin(async move {
                let s: Value = http
                    .get(format!("{api}/api/sources/lab-results"))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                s["records"]["sent"] == 3
            })
        },
        "3 records sent",
    )
    .await;
    assert_eq!(urls.lock().unwrap().len(), 3);

    // Pause: a new file is left where it is.
    let paused: Value = http
        .post(format!("{api}/api/sources/lab-results/pause"))
        .json(&json!({"by": "sam"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (paused["state"].as_str(), paused["paused"]["by"].as_str()),
        (Some("paused"), Some("sam"))
    );
    std::fs::copy(
        repo().join("examples/data/labs.csv"),
        root.join("inbox/labs-2.csv"),
    )
    .unwrap();
    http.post(format!("{api}/api/sources/lab-results/check-now"))
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(root.join("inbox/labs-2.csv").exists());
    assert_eq!(urls.lock().unwrap().len(), 3);

    // Resume: it is taken on the next pass, and recognized as the same content.
    let resumed: Value = http
        .post(format!("{api}/api/sources/lab-results/resume"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resumed["state"], "receiving");
    let (api3, http3) = (api.clone(), http.clone());
    wait_until(
        move || {
            let (api, http) = (api3.clone(), http3.clone());
            Box::pin(async move {
                let s: Value = http
                    .get(format!("{api}/api/sources/lab-results"))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                s["records"]["duplicate_files"] == 1
            })
        },
        "the duplicate file",
    )
    .await;
    assert_eq!(urls.lock().unwrap().len(), 3);

    // Replace the definition: the name changes, the id cannot.
    let mut renamed = definition.clone();
    renamed["name"] = json!("Lab results (Quest)");
    let replaced: Value = http
        .put(format!("{api}/api/sources/lab-results"))
        .json(&renamed)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replaced["name"], "Lab results (Quest)");
    renamed["id"] = json!("other");
    let mismatch = http
        .put(format!("{api}/api/sources/lab-results"))
        .json(&renamed)
        .send()
        .await
        .unwrap();
    assert_eq!(mismatch.status(), 400);

    // Delete: the definition goes, the history stays.
    let deleted = http
        .delete(format!("{api}/api/sources/lab-results"))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    assert!(!root.join("sources/lab-results.yaml").exists());
    assert_eq!(
        http.get(format!("{api}/api/sources/lab-results"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let records: Value = http
        .get(format!("{api}/api/records?source=lab-results"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(records["records"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn transforms_and_monitoring_are_served() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let urls: Urls = Arc::default();
    let fhir = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(urls.clone()),
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {fhir}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/lab-results.yaml"),
        "id: lab-results\nname: Lab results\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  poll_seconds: 60\n  min_file_age_ms: 0\ntransform: transforms/lab_results.py\nexpect_every: 1s\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let api = serve(hie::api::router(state)).await;
    let http = reqwest::Client::new();

    // The transform is listed with the source that uses it, and its code is served.
    let listed: Value = http
        .get(format!("{api}/api/transforms"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let t = &listed["transforms"][0];
    assert_eq!(
        (t["name"].as_str(), t["path"].as_str()),
        (Some("lab_results"), Some("transforms/lab_results.py"))
    );
    assert_eq!(t["used_by"], json!(["lab-results"]));
    let code: Value = http
        .get(format!("{api}/api/transforms/lab_results"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(code["code"].as_str().unwrap().contains("def transform("));

    // Try it: a good row becomes an Observation, a bad one is explained, nothing is loaded.
    let tried: Value = http
        .post(format!("{api}/api/transforms/lab_results/try"))
        .json(&json!({"records": [
            {"patient": "Patient/1", "code": "GLU", "display": "", "value": "95", "unit": "mg/dL", "date": "2026-10-15"},
            {"patient": "Patient/1", "code": "2345-7", "display": "Glucose", "value": "alto", "unit": "mg/dL", "date": "2026-10-15"}
        ]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let results = tried["results"].as_array().unwrap();
    assert_eq!(
        results[0]["resources"][0]["code"]["coding"][0]["code"],
        "2345-7"
    );
    assert!(results[0].get("error").is_none());
    assert_eq!(results[1]["error"]["kind"], "data");
    assert!(
        results[1]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not a number")
    );
    assert!(urls.lock().unwrap().is_empty());

    // A new transform is created through the API and shows up as unused.
    let created = http
        .put(format!("{api}/api/transforms/passthrough-copy"))
        .json(&json!({"code": "def transform(record, context):\n    return record\n"}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let listed: Value = http
        .get(format!("{api}/api/transforms"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["transforms"].as_array().unwrap().len(), 2);
    assert_eq!(
        http.put(format!("{api}/api/transforms/bad name"))
            .json(&json!({"code": ""}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );

    // Monitoring: nothing yet, then a silent source needs someone.
    let status: Value = http
        .get(format!("{api}/api/monitoring/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["state"], "all_receiving");
    assert_eq!(status["last_24h"]["files"], 0);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    http.post(format!("{api}/api/sources/lab-results/check-now"))
        .send()
        .await
        .unwrap();
    let (api2, http2) = (api.clone(), http.clone());
    wait_until(
        move || {
            let (api, http) = (api2.clone(), http2.clone());
            Box::pin(async move {
                let s: Value = http
                    .get(format!("{api}/api/monitoring/status"))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                s["state"] == "needs_you"
            })
        },
        "the silent source",
    )
    .await;
    let status: Value = http
        .get(format!("{api}/api/monitoring/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["needs_you"][0]["id"], "lab-results");
    assert!(
        status["needs_you"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("nothing received")
    );
    assert_eq!(status["last_24h"]["alerts"], 1);

    // Stats: the shape a source's page draws, with today last.
    let stats: Value = http
        .get(format!("{api}/api/sources/lab-results/stats?days=3"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats["by_day"].as_array().unwrap().len(), 3);
    assert_eq!(stats["today"]["files"], 0);
}

#[tokio::test]
async fn a_load_shows_its_failures_grouped_and_retries_them() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let urls: Urls = Arc::default();
    let fhir = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(urls.clone()),
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::create_dir_all(root.join("inbox")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {fhir}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/lab-results.yaml"),
        "id: lab-results\nname: Lab results\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  poll_seconds: 60\n  min_file_age_ms: 0\ntransform: transforms/lab_results.py\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let api = serve(hie::api::router(state)).await;
    let http = reqwest::Client::new();

    std::fs::copy(
        repo().join("examples/data/messy.csv"),
        root.join("inbox/messy.csv"),
    )
    .unwrap();
    http.post(format!("{api}/api/sources/lab-results/check-now"))
        .send()
        .await
        .unwrap();
    let (api2, http2) = (api.clone(), http.clone());
    wait_until(
        move || {
            let (api, http) = (api2.clone(), http2.clone());
            Box::pin(async move {
                let s: Value = http
                    .get(format!("{api}/api/sources/lab-results"))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                s["records"]["rejected"] == 5 && s["records"]["sent"] == 1
            })
        },
        "the messy file to finish",
    )
    .await;

    // The loads list: one load, its method, what it read, wrote and failed, how long it took.
    let loads: Value = http
        .get(format!("{api}/api/sources/lab-results/loads"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let load = &loads["loads"][0];
    assert_eq!(
        (
            load["name"].as_str(),
            load["method"].as_str(),
            load["status"].as_str()
        ),
        (Some("messy.csv"), Some("rest"), Some("done"))
    );
    assert_eq!(
        (
            load["records"].as_i64(),
            load["sent"].as_i64(),
            load["rejected"].as_i64()
        ),
        (Some(6), Some(1), Some(5))
    );
    assert!(load["took_ms"].as_i64().unwrap() >= 0);
    assert_eq!(load["archived"], 1);
    let load_id = load["id"].as_i64().unwrap();

    // One load: the failures grouped by reason, with sample rows and the first line.
    let detail: Value = http
        .get(format!("{api}/api/loads/{load_id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let failures = detail["failures"].as_array().unwrap();
    assert_eq!(failures.len(), 5, "{failures:?}");
    let alto = failures
        .iter()
        .find(|f| f["reason"].as_str().unwrap().contains("'alto'"))
        .unwrap();
    assert_eq!(
        (
            alto["kind"].as_str(),
            alto["count"].as_i64(),
            alto["first_line"].as_i64()
        ),
        (Some("data"), Some(1), Some(3))
    );
    assert_eq!(alto["samples"][0]["raw"]["value"], "alto");
    let parse = failures.iter().find(|f| f["kind"] == "parse").unwrap();
    assert!(parse["reason"].as_str().unwrap().contains("found 7"));

    // Retry the rows with one reason: they go back through the transform (and fail again, unchanged).
    let retried: Value = http
        .post(format!("{api}/api/loads/{load_id}/retry"))
        .json(&json!({"reason": alto["reason"]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retried["retried"], 1);
    let (api3, http3) = (api.clone(), http.clone());
    wait_until(
        move || {
            let (api, http) = (api3.clone(), http3.clone());
            Box::pin(async move {
                let l: Value = http
                    .get(format!("{api}/api/loads/{load_id}"))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                l["status"] == "done" && l["rejected"] == 5
            })
        },
        "the retried row to settle",
    )
    .await;

    // Retry them all.
    let retried: Value = http
        .post(format!("{api}/api/loads/{load_id}/retry"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retried["retried"], 5);
    assert_eq!(
        http.get(format!("{api}/api/loads/999"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
}

/// A Start that stopped after writing its source file (the engine died) is
/// finished from its intent when the engine starts again: the source holds
/// its evidence and has its queued first load.
#[tokio::test]
async fn a_start_stopped_after_its_file_is_finished_when_the_engine_starts() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let urls: Urls = Arc::default();
    let fhir = serve(Router::new().route("/", post(batch)).with_state(urls)).await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    let labs = root.join("labs.db");
    rusqlite::Connection::open(&labs)
        .unwrap()
        .execute_batch("CREATE TABLE lab_results (id INTEGER NOT NULL PRIMARY KEY, value TEXT)")
        .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {fhir}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    let source = json!({
        "type": "database",
        "connection": {"engine": "sqlite", "database": labs.display().to_string()},
        "read": {"mode": "table", "table": {"name": "lab_results"}, "new_rows_by": "id"},
        "poll_seconds": 3600,
    });

    // The first engine tests the connection.
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (_first, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let api = serve(hie::api::router(state)).await;
    let http = reqwest::Client::new();
    let tested: Value = http
        .post(format!("{api}/api/sources/test"))
        .json(&json!({"source": source}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let evidence = tested["evidence"]["id"].as_str().unwrap().to_string();
    assert_eq!(tested["evidence"]["verdict"], "passed", "{tested}");

    // A Start stopped between writing its file and binding it.
    let db = hie::db::Db::open(&root.join("data/hie.db")).unwrap();
    assert!(
        db.insert_start_intent(&hie::evidence::Binding {
            source: "labs".into(),
            evidence: Some(evidence.clone()),
            credential: None,
            first_load: None,
        })
        .unwrap()
    );
    let mut definition = json!({"id": "labs", "name": "Labs", "source": source,
                                "format": "ndjson", "transform": "transforms/lab_results.py"});
    definition["source"]["read"]["key"] = json!(["id"]);
    std::fs::write(
        root.join("sources/labs.yaml"),
        serde_yaml::to_string(&definition).unwrap(),
    )
    .unwrap();

    // The engine starts again and finishes it.
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (_second, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let api = serve(hie::api::router(state)).await;
    let view: Value = http
        .get(format!("{api}/api/sources/labs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(view["evidence"], evidence.as_str(), "{view}");
    assert!(view["first_load"]["id"].is_i64(), "{view}");
    assert!(db.start_intents(None).unwrap().is_empty());
    assert!(db.evidence(&evidence).unwrap().is_some());
}

#[tokio::test]
async fn a_definition_is_its_file_whatever_the_file_is_named() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::create_dir_all(root.join("inbox")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: http://127.0.0.1:9\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    let definition = |id: &str, name: &str| {
        json!({"id": id, "name": name,
               "source": {"type": "folder", "path": "inbox", "poll_seconds": 3600},
               "transform": "transforms/lab_results.py"})
    };
    let yaml = |id: &str| serde_yaml::to_string(&definition(id, "As written")).unwrap();
    // `.yml`, `.yaml`, and a file not named for its source: the loader reads all three.
    std::fs::write(root.join("sources/short.yml"), yaml("short")).unwrap();
    std::fs::write(root.join("sources/long.yaml"), yaml("long")).unwrap();
    std::fs::write(root.join("sources/by-hand.yml"), yaml("other")).unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    assert_eq!(feeds.len(), 3);
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let api = serve(hie::api::router(state)).await;
    let http = reqwest::Client::new();
    let get = |path: String| {
        let http = http.clone();
        async move {
            let response = http.get(path).send().await.unwrap();
            assert_eq!(response.status(), 200);
            response.json::<Value>().await.unwrap()
        }
    };

    // Every answer that carries a source carries its definition.
    let listed = get(format!("{api}/api/sources")).await;
    let monitoring = get(format!("{api}/api/monitoring/status")).await;
    for sources in [&listed["sources"], &monitoring["sources"]] {
        let sources = sources.as_array().unwrap();
        assert_eq!(sources.len(), 3, "{sources:?}");
        for source in sources {
            assert_eq!(source["definition"]["id"], source["id"], "{source}");
        }
    }
    for id in ["short", "long", "other"] {
        let view = get(format!("{api}/api/sources/{id}")).await;
        assert_eq!(view["definition"]["name"], "As written", "{view}");
    }
    let files = || {
        let mut names: Vec<String> = std::fs::read_dir(root.join("sources"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    };

    // A replace writes over the file it was read from, whatever its extension.
    for id in ["short", "other"] {
        let replaced = http
            .put(format!("{api}/api/sources/{id}"))
            .json(&definition(id, "Replaced"))
            .send()
            .await
            .unwrap();
        assert_eq!(replaced.status(), 200, "{}", replaced.text().await.unwrap());
        let view = get(format!("{api}/api/sources/{id}")).await;
        assert_eq!(view["definition"]["name"], "Replaced", "{view}");
    }
    assert_eq!(files(), ["by-hand.yml", "long.yaml", "short.yml"]);
    let file = std::fs::read_to_string(root.join("sources/short.yml")).unwrap();
    assert!(file.contains("name: Replaced"), "{file}");

    // A source exists while a file named for it does, `.yml` or `.yaml`.
    std::fs::write(root.join("sources/late.yml"), yaml("late")).unwrap();
    for id in ["short", "late"] {
        let again = http
            .post(format!("{api}/api/sources"))
            .json(&definition(id, "Again"))
            .send()
            .await
            .unwrap();
        assert_eq!(again.status(), 409, "{}", again.text().await.unwrap());
    }
    assert_eq!(
        files(),
        ["by-hand.yml", "late.yml", "long.yaml", "short.yml"]
    );

    // A delete removes the file the source was read from.
    for id in ["short", "long", "other"] {
        let deleted = http
            .delete(format!("{api}/api/sources/{id}"))
            .send()
            .await
            .unwrap();
        assert!(deleted.status().is_success(), "{}", deleted.status());
    }
    assert_eq!(files(), ["late.yml"]);

    // A new source is written as `<id>.yaml`.
    let created = http
        .post(format!("{api}/api/sources"))
        .json(&definition("short", "New"))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201, "{}", created.text().await.unwrap());
    assert_eq!(files(), ["late.yml", "short.yaml"]);
}
