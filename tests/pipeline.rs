//! End-to-end: a folder feed, the real Python transform, and a stand-in FHIR server
//! that can be taken down and that refuses some resources.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use hie::pipeline::FeedRunner;
use serde_json::{Value, json};

#[derive(Default)]
struct Fhir {
    down: bool,
    versions: HashMap<String, u32>,
    requests: u32,
}

type Shared = Arc<Mutex<Fhir>>;

async fn batch(
    State(s): State<Shared>,
    Json(bundle): Json<Value>,
) -> (axum::http::StatusCode, Json<Value>) {
    let mut s = s.lock().unwrap();
    s.requests += 1;
    if s.down {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"resourceType": "OperationOutcome"})),
        );
    }
    let mut entries = Vec::new();
    for e in bundle["entry"].as_array().unwrap() {
        let url = e["request"]["url"].as_str().unwrap().to_string();
        if e["resource"]["subject"]["reference"] == "Patient/999" {
            entries.push(json!({"response": {"status": "400 Bad Request", "outcome": {
                "resourceType": "OperationOutcome",
                "issue": [{"severity": "error", "code": "invalid", "details": {"text": "subject Patient/999 does not exist"}}]}}}));
            continue;
        }
        let v = s.versions.entry(url).or_insert(0);
        *v += 1;
        let status = if *v == 1 { "201 Created" } else { "200 OK" };
        entries.push(json!({"response": {"status": status, "etag": format!("W/\"{v}\"")}}));
    }
    (
        axum::http::StatusCode::OK,
        Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries})),
    )
}

async fn fhir_server() -> (String, Shared) {
    let state: Shared = Arc::default();
    let app = Router::new()
        .route("/", post(batch))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), state)
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

fn drop_file(inbox: &Path, name: &str, content: &str) {
    std::fs::create_dir_all(inbox).unwrap();
    std::fs::write(inbox.join(name), content).unwrap();
}

fn count(state: &hie::api::AppState, status: &str) -> i64 {
    state.db.feed_counts("lab-results").unwrap()[status]
        .as_i64()
        .unwrap()
}

fn alert_kinds(state: &hie::api::AppState) -> Vec<String> {
    state
        .db
        .query_json("SELECT kind FROM alerts ORDER BY id", [])
        .unwrap()
        .into_iter()
        .map(|r| r["kind"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn a_feed_survives_an_outage_bad_rows_and_resends() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (base, fhir) = fhir_server().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {base}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/lab-results.yaml"),
        "id: lab-results\nname: Lab results\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  min_file_age_ms: 0\n\
         transform: transforms/lab_results.py\ndelivery:\n  initial_backoff_ms: 10\n  alert_after_attempts: 2\nexpect_every: 1s\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let runner: Arc<FeedRunner> = manager.runner("lab-results").unwrap();
    manager.pause("lab-results", None).unwrap(); // the test drives the passes itself
    let inbox = root.join("inbox");
    let labs = std::fs::read_to_string(repo().join("examples/data/labs.csv")).unwrap();

    // HFS is down when the file arrives: rows are stored, the file leaves the inbox, nothing is lost.
    fhir.lock().unwrap().down = true;
    drop_file(&inbox, "labs.csv", &labs);
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "pending"), 3);
    assert!(!inbox.join("labs.csv").exists());
    tokio::time::sleep(Duration::from_millis(30)).await;
    runner.tick().await.unwrap();
    assert_eq!(alert_kinds(&state), ["delivery"]);

    // HFS comes back: the same rows are delivered on the next retry.
    fhir.lock().unwrap().down = false;
    tokio::time::sleep(Duration::from_millis(60)).await;
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 3);
    assert_eq!(fhir.lock().unwrap().versions.len(), 3);
    assert_eq!(alert_kinds(&state), ["delivery", "delivery_recovered"]);

    // The identical file again: recognized by its hash and not reprocessed.
    let before = fhir.lock().unwrap().requests;
    drop_file(&inbox, "labs.csv", &labs);
    runner.tick().await.unwrap();
    assert_eq!(fhir.lock().unwrap().requests, before);
    assert_eq!(
        state.db.feed_counts("lab-results").unwrap()["duplicate_files"],
        1
    );

    // The same rows in a different file: delivered again, but as updates, not new resources.
    drop_file(&inbox, "labs-resent.csv", &format!("{labs}\n"));
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 6);
    {
        let f = fhir.lock().unwrap();
        assert_eq!(f.versions.len(), 3);
        assert!(f.versions.values().all(|v| *v == 2));
    }

    // A messy file: each bad row is rejected on its own with its reason; good rows go through.
    let messy = std::fs::read_to_string(repo().join("examples/data/messy.csv")).unwrap();
    drop_file(
        &inbox,
        "messy.csv",
        &format!("{messy}Patient/999,2345-7,Glucose,77,mg/dL,2026-10-16\n"),
    );
    runner.tick().await.unwrap();
    let rejected = state
        .db
        .query_json(
            "SELECT line, error_kind, error FROM records WHERE status = 'rejected' ORDER BY line",
            [],
        )
        .unwrap();
    let summary: Vec<(i64, String)> = rejected
        .iter()
        .map(|r| {
            (
                r["line"].as_i64().unwrap(),
                r["error_kind"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (3, "data".into()),
            (4, "parse".into()),
            (5, "data".into()),
            (6, "data".into()),
            (7, "data".into()),
            (8, "server".into())
        ]
    );
    assert!(
        rejected[0]["error"]
            .as_str()
            .unwrap()
            .contains("'alto' is not a number")
    );
    assert!(
        rejected[5]["error"]
            .as_str()
            .unwrap()
            .contains("Patient/999 does not exist")
    );
    assert_eq!(
        count(&state, "sent"),
        7,
        "the GLU local code row is mapped to LOINC and sent"
    );
    assert_eq!(alert_kinds(&state).last().unwrap(), "rejected_records");

    // Someone fixes the value and reprocesses that one row.
    let alto = state
        .db
        .query_json(
            "SELECT id, raw FROM records WHERE line = 3 AND status = 'rejected'",
            [],
        )
        .unwrap();
    let mut raw = alto[0]["raw"].clone();
    raw["value"] = json!("101");
    assert!(
        state
            .db
            .reprocess(alto[0]["id"].as_i64().unwrap(), Some(&raw))
            .unwrap()
    );
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 8);
    assert_eq!(count(&state, "rejected"), 5);
    // Written by the reprocess, the row's failed content goes (0.2.0: failed
    // content is purged once a retry or reprocess writes the row); its history
    // keeps both attempts.
    let id = alto[0]["id"].as_i64().unwrap();
    let fixed = state
        .db
        .query_json(
            "SELECT raw, original_raw, content FROM records WHERE id = $1",
            hie::db::params![id],
        )
        .unwrap();
    assert_eq!(fixed[0]["raw"], Value::Null);
    assert_eq!(fixed[0]["original_raw"], Value::Null);
    assert_eq!(fixed[0]["content"], "purged");
    let kinds: Vec<String> = state.db.record_history(id).unwrap().unwrap()["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["kind"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        kinds,
        ["admitted", "failed", "retried", "transformed", "delivered"]
    );
    assert_eq!(
        alert_kinds(&state)
            .iter()
            .filter(|k| *k == "rejected_records")
            .count(),
        1,
        "reprocessing a row does not repeat the file's alert"
    );

    // Nothing arrives for longer than `expect_every`: one silence alert, not one per pass.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    runner.tick().await.unwrap();
    runner.tick().await.unwrap();
    assert_eq!(
        alert_kinds(&state)
            .iter()
            .filter(|k| *k == "silence")
            .count(),
        1
    );
}

/// A stand-in destination that records every Bundle (its type and entry urls)
/// and answers `503` while `down`, resolving `Patient?identifier=` references
/// in transactions against a seeded patient.
#[derive(Default)]
struct Recorder {
    down: bool,
    bundles: Vec<(String, Vec<String>)>,
    stored: HashMap<String, Value>,
}

type Recorded = Arc<Mutex<Recorder>>;

async fn record_bundle(
    State(s): State<Recorded>,
    Json(bundle): Json<Value>,
) -> (axum::http::StatusCode, Json<Value>) {
    let mut s = s.lock().unwrap();
    let kind = bundle["type"].as_str().unwrap_or_default().to_string();
    let entries = bundle["entry"].as_array().cloned().unwrap_or_default();
    let urls = entries
        .iter()
        .map(|e| e["request"]["url"].as_str().unwrap_or_default().to_string())
        .collect();
    s.bundles.push((kind.clone(), urls));
    if s.down {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"resourceType": "OperationOutcome"})),
        );
    }
    let mut answers = Vec::new();
    for e in entries {
        let mut resource = e["resource"].clone();
        if kind == "transaction"
            && resource["subject"]["reference"]
                .as_str()
                .is_some_and(|r| r.starts_with("Patient?"))
        {
            resource["subject"]["reference"] = json!("Patient/pt-1");
        }
        s.stored.insert(
            e["request"]["url"].as_str().unwrap_or_default().to_string(),
            resource,
        );
        answers.push(json!({"response": {"status": "201 Created"}}));
    }
    (
        axum::http::StatusCode::OK,
        Json(
            json!({"resourceType": "Bundle", "type": format!("{kind}-response"), "entry": answers}),
        ),
    )
}

async fn recorder() -> (String, Recorded) {
    let state: Recorded = Arc::default();
    let app = Router::new()
        .route("/", post(record_bundle))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), state)
}

/// An engine over `root`, its source paused so the test drives the passes.
fn engine(
    root: &Path,
    base: &str,
    python: &str,
) -> (Arc<hie::manager::SourceManager>, hie::api::AppState) {
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {base}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    manager.pause("lab", None).unwrap();
    (manager, state)
}

const LAB_TRANSFORM: &str = "from hie import DataError\n\n\ndef transform(record, context):\n    value = str(record[\"value\"])\n    try:\n        number = float(value)\n    except ValueError:\n        raise DataError(f\"value {value!r} is not a number\")\n    return {\"resourceType\": \"Observation\", \"id\": f\"obs-{record['id']}\", \"status\": \"final\",\n            \"code\": {\"text\": \"glucose\"}, \"valueQuantity\": {\"value\": number}}\n";

/// A transaction sent without a confirming answer is kept as it was sent and
/// the same one goes again after a restart, never split nor re-built.
#[tokio::test]
async fn an_unconfirmed_transaction_is_sent_again_after_a_restart() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (base, dest) = recorder().await;
    dest.lock().unwrap().down = true;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::write(
        root.join("transforms/cond.py"),
        "def transform(record, context):\n    return {\"resourceType\": \"Observation\", \"id\": f\"obs-{record['id']}\", \"status\": \"final\",\n            \"code\": {\"text\": \"glucose\"}, \"subject\": {\"reference\": f\"Patient?identifier=urn:mrn|{record['mrn']}\"}}\n",
    )
    .unwrap();
    std::fs::write(
        root.join("sources/lab.yaml"),
        "id: lab\nname: Lab\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  min_file_age_ms: 0\n\
         transform: transforms/cond.py\ndelivery:\n  initial_backoff_ms: 10\n  max_backoff_ms: 20\n",
    )
    .unwrap();
    drop_file(&root.join("inbox"), "a.csv", "id,mrn\n1,123\n2,123\n");

    let (manager, state) = engine(root, &base, &python);
    manager.runner("lab").unwrap().tick().await.unwrap();
    let load = state.db.load_views("lab", None, None, false, 1).unwrap()[0].clone();
    assert_eq!(load["rows"]["outcome_unknown"], 2);
    assert_eq!(load["waiting"], "destination_unavailable");
    drop((manager, state));

    // A new engine on the same store; the destination answers again.
    dest.lock().unwrap().down = false;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (manager, state) = engine(root, &base, &python);
    manager.runner("lab").unwrap().tick().await.unwrap();
    let load = state.db.load_views("lab", None, None, false, 1).unwrap()[0].clone();
    assert_eq!(load["rows"]["written"], 2, "{load}");
    assert_eq!(load["rows"]["outcome_unknown"], 0);
    let bundles = dest.lock().unwrap().bundles.clone();
    // At least the attempt before the restart and the one after it (the
    // engine's own loop may have made one more before the source was paused).
    assert!(bundles.len() >= 2, "{bundles:?}");
    assert!(bundles.iter().all(|(kind, _)| kind == "transaction"));
    assert!(
        bundles
            .iter()
            .all(|(_, urls)| *urls == bundles[0].1 && urls.len() == 2),
        "the same transaction every time, never split: {bundles:?}"
    );
    assert_eq!(
        state
            .db
            .query_json("SELECT id FROM dispatches", [])
            .unwrap(),
        Vec::<Value>::new(),
        "a settled transaction is not kept"
    );
    let facts = state.db.destination_written("lab").unwrap();
    assert_eq!(facts["resources_written"], 2);
    assert_eq!(facts["by_type"], json!({"Observation": 2}));
    assert!(facts["last_written_at"].is_string());
    let record = load["id"].as_i64().unwrap();
    let rec = state
        .db
        .query_json(
            "SELECT id FROM records WHERE file_id = $1 ORDER BY id",
            hie::db::params![record],
        )
        .unwrap()[0]["id"]
        .as_i64()
        .unwrap();
    let kinds: Vec<String> = state.db.record_history(rec).unwrap().unwrap()["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["kind"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        kinds,
        [
            "admitted",
            "transformed",
            "outcome_unknown",
            "settled",
            "delivered"
        ]
    );
}

/// Retries form a chain per group, run the version given at acceptance, never
/// cover a row twice, and a later retry never lowers the day's failures.
#[tokio::test]
async fn retries_chain_and_keep_the_failures_of_their_day() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (base, _dest) = recorder().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::write(root.join("transforms/lab_csv.py"), LAB_TRANSFORM).unwrap();
    std::fs::write(
        root.join("sources/lab.yaml"),
        "id: lab\nname: Lab\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  min_file_age_ms: 0\n\
         transform: transforms/lab_csv.py\n",
    )
    .unwrap();
    drop_file(&root.join("inbox"), "a.csv", "id,value\n1,alto\n2,5.4\n");
    let (manager, state) = engine(root, &base, &python);
    let runner = manager.runner("lab").unwrap();
    runner.tick().await.unwrap();
    let db = &state.db;
    let load = db.load_views("lab", None, None, false, 1).unwrap()[0]["id"]
        .as_i64()
        .unwrap();
    let group = db
        .failure_group(
            load,
            db.load_failures(load).unwrap()[0]["group"]
                .as_str()
                .unwrap(),
        )
        .unwrap()
        .unwrap();
    let day = (hie::db::now_ms() - 3_600_000, hie::db::now_ms() + 3_600_000);
    assert_eq!(
        db.period_counts(Some("lab"), day.0, day.1).unwrap().failed,
        1
    );

    // The same program again: the row fails again, in the same group.
    let v1 = state.versions.latest("lab_csv").unwrap().unwrap();
    let first = db.create_retry(&group, &v1).unwrap();
    assert!(matches!(
        db.create_retry(&group, &v1),
        Err(hie::db::RetryRefusal::InProgress(id)) if id == first["id"]
    ));
    runner.tick().await.unwrap();
    let first = db
        .retry(load, first["id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        (first["status"].as_str(), first["failed"].as_i64()),
        (Some("done"), Some(1))
    );
    let again = db.failure_group(load, &group.id).unwrap().unwrap();
    assert_eq!(again.count, 1);
    assert_eq!(
        db.period_counts(Some("lab"), day.0, day.1).unwrap().failed,
        1
    );

    // A fixed program: the retry writes the row; the day keeps its failure.
    let fixed_code = LAB_TRANSFORM.replace(
        "raise DataError(f\"value {value!r} is not a number\")",
        "number = 0.0",
    );
    let (_, saved) = state
        .versions
        .save_legacy("lab_csv", &fixed_code)
        .await
        .unwrap();
    assert_eq!(saved, 2);
    let fixed = state.versions.latest("lab_csv").unwrap().unwrap();
    assert_eq!(fixed.version, 2);
    let second = db.create_retry(&again, &fixed).unwrap();
    assert_eq!(second["previous"], first["id"]);
    runner.tick().await.unwrap();
    let second = db
        .retry(load, second["id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        (second["status"].as_str(), second["written"].as_i64()),
        (Some("done"), Some(1))
    );
    let counts = db.period_counts(Some("lab"), day.0, day.1).unwrap();
    assert_eq!((counts.failed, counts.written, counts.rows_read), (1, 2, 2));
    assert_eq!(db.failure_group(load, &group.id).unwrap().unwrap().count, 0);
    assert!(db.load_failures(load).unwrap().is_empty());
    assert!(matches!(
        db.create_retry(&again, &fixed),
        Err(hie::db::RetryRefusal::NothingToRetry)
    ));
    let runs = |version| {
        db.version_runs("lab_csv", version, 10)
            .unwrap()
            .iter()
            .map(|r| format!("{} {}", r.kind, r.outcome))
            .collect::<Vec<_>>()
    };
    assert_eq!(runs(2), ["retry written"]);
    assert_eq!(runs(1), ["retry failed", "load partial"]);
}
