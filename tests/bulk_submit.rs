//! The $bulk-submit path end to end: a stand-in HFS that behaves as the Data
//! Consumer (fetches the manifest and the file from the engine, answers the status
//! kick-off, publishes receipts), and the engine mapping those receipts back.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

#[derive(Default)]
struct Submission {
    polls: u32,
    references: Vec<String>,
    errors: Vec<String>,
}

#[derive(Clone)]
struct Consumer {
    base: Arc<Mutex<String>>,
    submissions: Arc<Mutex<HashMap<String, Submission>>>,
}

fn param<'a>(body: &'a Value, name: &str) -> Option<&'a Value> {
    body["parameter"]
        .as_array()?
        .iter()
        .find(|p| p["name"] == name)
}

/// `POST /$bulk-submit`: fetch the manifest and every output file right away.
async fn kickoff(State(c): State<Consumer>, Json(body): Json<Value>) -> impl IntoResponse {
    let id = param(&body, "submissionId")
        .and_then(|p| p["valueString"].as_str())
        .unwrap()
        .to_string();
    let manifest_url = param(&body, "manifestUrl")
        .and_then(|p| p["valueUrl"].as_str())
        .unwrap()
        .to_string();
    let manifest: Value = reqwest::get(&manifest_url)
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut sub = Submission::default();
    for entry in manifest["output"].as_array().unwrap() {
        let text = reqwest::get(entry["url"].as_str().unwrap())
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            match serde_json::from_str::<Value>(line) {
                Ok(v) if v["resourceType"].is_string() && v["id"].is_string() => {
                    sub.references.push(format!(
                        "{}/{}",
                        v["resourceType"].as_str().unwrap(),
                        v["id"].as_str().unwrap()
                    ))
                }
                Ok(_) => sub.errors.push("resource has no id".into()),
                Err(e) => sub.errors.push(format!("invalid JSON: {e}")),
            }
        }
    }
    c.submissions.lock().unwrap().insert(id.clone(), sub);
    (
        StatusCode::OK,
        Json(
            json!({"resourceType": "OperationOutcome", "issue": [{"severity": "information", "code": "informational",
            "details": {"text": format!("submission {id} accepted")}}]}),
        ),
    )
}

async fn status_kickoff(State(c): State<Consumer>, Json(body): Json<Value>) -> impl IntoResponse {
    let id = param(&body, "submissionId")
        .and_then(|p| p["valueString"].as_str())
        .unwrap()
        .to_string();
    let base = c.base.lock().unwrap().clone();
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-location",
        format!("{base}/bulk-submit-status/{id}").parse().unwrap(),
    );
    (StatusCode::ACCEPTED, headers)
}

/// The first poll is in progress; the second publishes the receipts.
async fn poll(State(c): State<Consumer>, Path(id): Path<String>) -> impl IntoResponse {
    let base = c.base.lock().unwrap().clone();
    let mut subs = c.submissions.lock().unwrap();
    let sub = subs.get_mut(&id).unwrap();
    sub.polls += 1;
    if sub.polls == 1 {
        return (
            StatusCode::ACCEPTED,
            [("x-progress", "Downloading file 1 of 1")],
            Json(json!({})),
        )
            .into_response();
    }
    let mut outcome = Vec::new();
    if !sub.errors.is_empty() {
        outcome.push(json!({"url": format!("{base}/bulk-submit-file/{id}/error"), "count": sub.errors.len()}));
    }
    Json(json!({
        "submissionId": id,
        "transactionTime": "2026-10-20T00:00:00Z",
        "requiresAccessToken": false,
        "output": [{"type": "Patient", "url": format!("{base}/bulk-submit-file/{id}/output"), "count": sub.references.len()}],
        "outcome": outcome,
        "deleted": [],
        "link": [],
    }))
    .into_response()
}

async fn artifact(
    State(c): State<Consumer>,
    Path((id, part)): Path<(String, String)>,
) -> impl IntoResponse {
    let subs = c.submissions.lock().unwrap();
    let sub = subs.get(&id).unwrap();
    let body: String = match part.as_str() {
        "output" => sub.references.iter().map(|r| json!({"reference": r}).to_string() + "\n").collect(),
        _ => sub
            .errors
            .iter()
            .map(|e| json!({"resourceType": "OperationOutcome", "issue": [{"severity": "error", "code": "processing", "diagnostics": e}]}).to_string() + "\n")
            .collect(),
    };
    ([(header::CONTENT_TYPE, "application/fhir+ndjson")], body)
}

async fn bind() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (listener, url)
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

#[tokio::test]
async fn an_ndjson_drop_goes_through_bulk_submit_and_its_receipts_come_back() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };

    // The stand-in HFS.
    let consumer = Consumer {
        base: Arc::default(),
        submissions: Arc::default(),
    };
    let (hfs_listener, hfs_url) = bind().await;
    *consumer.base.lock().unwrap() = hfs_url.clone();
    let hfs = Router::new()
        .route("/$bulk-submit", post(kickoff))
        .route("/$bulk-submit-status", post(status_kickoff))
        .route("/bulk-submit-status/{id}", get(poll))
        .route("/bulk-submit-file/{id}/{part}", get(artifact))
        .with_state(consumer.clone());
    tokio::spawn(async move { axum::serve(hfs_listener, hfs).await.unwrap() });

    // The engine, reachable by the stand-in at its public URL.
    let (hie_listener, hie_url) = bind().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::copy(
        repo().join("transforms/fhir_passthrough.py"),
        root.join("transforms/fhir_passthrough.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {hfs_url}\npublic_url: {hie_url}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/exports.yaml"),
        "id: exports\nname: Exports\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.ndjson\"\n  poll_seconds: 60\n  min_file_age_ms: 0\n\
         format: ndjson\ntransform: transforms/fhir_passthrough.py\ndelivery:\n  method: bulk_submit\n  initial_backoff_ms: 10\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    manager.pause("exports", None).unwrap(); // the test drives the passes itself
    let runner = manager.runner("exports").unwrap();
    let db = state.db.clone();
    let router = hie::api::router(state);
    tokio::spawn(async move { axum::serve(hie_listener, router).await.unwrap() });

    let inbox = root.join("inbox");
    std::fs::create_dir_all(&inbox).unwrap();
    std::fs::write(
        inbox.join("export.ndjson"),
        concat!(
            "{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n",
            "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"subject\":{\"reference\":\"Patient/p1\"}}\n",
            "{\"resourceType\":\"Observation\"}\n",
            "not json\n",
        ),
    )
    .unwrap();

    // Pass 1: the rows are stored, two are predicted to fail, the drop is submitted,
    // HFS fetches the manifest and the file from the engine, and the status URL is known.
    runner.tick().await.unwrap();
    let counts = db.feed_counts("exports").unwrap();
    assert_eq!(
        (counts["submitted"].as_i64(), counts["rejected"].as_i64()),
        (Some(2), Some(2))
    );
    assert!(
        inbox.join("export.ndjson").exists(),
        "the object stays until HFS has read it"
    );
    {
        let subs = consumer.submissions.lock().unwrap();
        let sub = subs.values().next().expect("HFS received the submission");
        assert_eq!(sub.references, ["Patient/p1", "Observation/o1"]);
        assert_eq!(sub.errors.len(), 2);
    }
    let open = db
        .query_json("SELECT status, status_url FROM submissions", [])
        .unwrap();
    assert_eq!(open[0]["status"], "running");
    assert!(
        open[0]["status_url"]
            .as_str()
            .unwrap()
            .contains("/bulk-submit-status/hie-")
    );

    // Pass 2: HFS is still working.
    runner.tick().await.unwrap();
    assert_eq!(db.feed_counts("exports").unwrap()["submitted"], 2);

    // Pass 3, after the engine's 2 s wait between polls: the receipts are in; the
    // records get their dispositions and the object is archived.
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    runner.tick().await.unwrap();
    let counts = db.feed_counts("exports").unwrap();
    assert_eq!(
        (
            counts["sent"].as_i64(),
            counts["rejected"].as_i64(),
            counts["submitted"].as_i64()
        ),
        (Some(2), Some(2), Some(0))
    );
    let sent = db
        .query_json(
            "SELECT response FROM records WHERE status = 'sent' ORDER BY line",
            [],
        )
        .unwrap();
    assert_eq!(sent[0]["response"][0]["reference"], "Patient/p1");
    assert_eq!(sent[0]["response"][0]["via"], "$bulk-submit");
    let rejected = db
        .query_json(
            "SELECT line, error FROM records WHERE status = 'rejected' ORDER BY line",
            [],
        )
        .unwrap();
    assert!(
        rejected[0]["error"]
            .as_str()
            .unwrap()
            .contains("neither id nor identifier")
    );
    assert_eq!(rejected[1]["line"], 4);
    assert!(!inbox.join("export.ndjson").exists());
    assert!(inbox.join("processed").read_dir().unwrap().any(|e| {
        e.unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with("-export.ndjson")
    }));
    assert_eq!(
        db.query_json("SELECT status FROM submissions", []).unwrap()[0]["status"],
        "done"
    );
    assert_eq!(
        db.query_json("SELECT status FROM files", []).unwrap()[0]["status"],
        "done"
    );
}

/// A lab CSV through the transform and then `$bulk-submit`: the engine writes the
/// Observations as NDJSON, gives each one an id derived from its identifier, serves
/// the file, and maps the receipts back; the CSV itself is archived at once.
#[tokio::test]
async fn a_csv_is_transformed_and_submitted_in_bulk() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let consumer = Consumer {
        base: Arc::default(),
        submissions: Arc::default(),
    };
    let (hfs_listener, hfs_url) = bind().await;
    *consumer.base.lock().unwrap() = hfs_url.clone();
    let hfs = Router::new()
        .route("/$bulk-submit", post(kickoff))
        .route("/$bulk-submit-status", post(status_kickoff))
        .route("/bulk-submit-status/{id}", get(poll))
        .route("/bulk-submit-file/{id}/{part}", get(artifact))
        .with_state(consumer.clone());
    tokio::spawn(async move { axum::serve(hfs_listener, hfs).await.unwrap() });

    let (hie_listener, hie_url) = bind().await;
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
        format!("fhir_base_url: {hfs_url}\npublic_url: {hie_url}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/labs.yaml"),
        "id: labs\nname: Labs\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  poll_seconds: 60\n  min_file_age_ms: 0\n\
         transform: transforms/lab_results.py\ndelivery:\n  method: bulk_submit\n  initial_backoff_ms: 10\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    assert!(
        feeds[0].bulk_transformed(),
        "a CSV with a transform takes the transformed path"
    );
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    manager.pause("labs", None).unwrap();
    let runner = manager.runner("labs").unwrap();
    let db = state.db.clone();
    let bulk_dir = state.bulk_dir.clone();
    let router = hie::api::router(state);
    tokio::spawn(async move { axum::serve(hie_listener, router).await.unwrap() });

    let inbox = root.join("inbox");
    std::fs::create_dir_all(&inbox).unwrap();
    std::fs::copy(
        repo().join("examples/data/messy.csv"),
        inbox.join("messy.csv"),
    )
    .unwrap();

    // Pass 1: the transform runs (5 bad rows rejected with their reason), the good
    // row gets a derived id, the engine writes and serves the NDJSON, HFS reads it.
    runner.tick().await.unwrap();
    let counts = db.feed_counts("labs").unwrap();
    assert_eq!(
        (counts["submitted"].as_i64(), counts["rejected"].as_i64()),
        (Some(1), Some(5)),
        "{counts}"
    );
    assert!(
        !inbox.join("messy.csv").exists(),
        "the CSV is archived at once; HFS never reads it"
    );
    let submitted = db
        .query_json(
            "SELECT resources FROM records WHERE status = 'submitted'",
            [],
        )
        .unwrap();
    let id = submitted[0]["resources"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(id.len(), 48, "{id}");
    assert_eq!(
        submitted[0]["resources"][0]["identifier"][0]["system"],
        "urn:helios:hie:lab-results"
    );
    let token = db.query_json("SELECT token FROM submissions", []).unwrap()[0]["token"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(bulk_dir.join(&token).join("1.ndjson").is_file());
    {
        let subs = consumer.submissions.lock().unwrap();
        let sub = subs.values().next().expect("HFS received the submission");
        assert_eq!(sub.references, [format!("Observation/{id}")]);
        assert!(sub.errors.is_empty());
    }

    // Passes 2 and 3: the receipts arrive, the record is sent, the engine's file goes.
    runner.tick().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    runner.tick().await.unwrap();
    let counts = db.feed_counts("labs").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["submitted"].as_i64()),
        (Some(1), Some(0))
    );
    let sent = db
        .query_json("SELECT response FROM records WHERE status = 'sent'", [])
        .unwrap();
    assert_eq!(
        sent[0]["response"][0]["reference"],
        format!("Observation/{id}")
    );
    assert!(
        !bulk_dir.join(&token).exists(),
        "served file removed after the receipts"
    );

    // The same row again, in another file, derives the same id: an update, not a duplicate.
    std::fs::copy(
        repo().join("examples/data/messy.csv"),
        inbox.join("messy-again.csv"),
    )
    .unwrap();
    std::fs::write(
        inbox.join("messy-again.csv"),
        "patient,code,display,value,unit,date\nPatient/3,GLU,,99,mg/dL,2026-10-16\n",
    )
    .unwrap();
    runner.tick().await.unwrap();
    let again = db
        .query_json(
            "SELECT resources FROM records WHERE status = 'submitted'",
            [],
        )
        .unwrap();
    assert_eq!(again[0]["resources"][0]["id"], id);
}

/// A zipped NDJSON export on the source_objects bulk path: the engine unpacks it,
/// stages each entry where HFS can read it (here: served by the engine, since a
/// folder cannot sign URLs), submits them, and removes the staged files once the
/// receipts are in.
#[tokio::test]
async fn a_zipped_export_is_staged_and_submitted() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let consumer = Consumer {
        base: Arc::default(),
        submissions: Arc::default(),
    };
    let (hfs_listener, hfs_url) = bind().await;
    *consumer.base.lock().unwrap() = hfs_url.clone();
    let hfs = Router::new()
        .route("/$bulk-submit", post(kickoff))
        .route("/$bulk-submit-status", post(status_kickoff))
        .route("/bulk-submit-status/{id}", get(poll))
        .route("/bulk-submit-file/{id}/{part}", get(artifact))
        .with_state(consumer.clone());
    tokio::spawn(async move { axum::serve(hfs_listener, hfs).await.unwrap() });

    let (hie_listener, hie_url) = bind().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::copy(
        repo().join("transforms/fhir_passthrough.py"),
        root.join("transforms/fhir_passthrough.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {hfs_url}\npublic_url: {hie_url}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/exports.yaml"),
        "id: exports\nname: Exports\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.zip\"\n  poll_seconds: 60\n  min_file_age_ms: 0\n\
         format: ndjson\ntransform: transforms/fhir_passthrough.py\ndelivery:\n  method: bulk_submit\n  initial_backoff_ms: 10\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    assert!(
        !feeds[0].bulk_transformed(),
        "NDJSON with the passthrough takes the source_objects path"
    );
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    manager.pause("exports", None).unwrap();
    let runner = manager.runner("exports").unwrap();
    let db = state.db.clone();
    let bulk_dir = state.bulk_dir.clone();
    let router = hie::api::router(state);
    tokio::spawn(async move { axum::serve(hie_listener, router).await.unwrap() });

    // The zip the way an export tool writes it: a folder, NDJSON per type, a manifest.
    let zip_bytes = {
        use std::io::Write;
        let mut buf = std::io::Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(&mut buf);
        let options = zip::write::SimpleFileOptions::default();
        zip.start_file("out/Patient.ndjson", options).unwrap();
        zip.write_all(b"{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n{\"resourceType\":\"Patient\",\"id\":\"p2\"}\n").unwrap();
        zip.start_file("out/Observation.ndjson", options).unwrap();
        zip.write_all(b"{\"resourceType\":\"Observation\",\"id\":\"o1\"}\n{\"resourceType\":\"Observation\"}\n").unwrap();
        zip.start_file("out/manifest.json", options).unwrap();
        zip.write_all(b"{\"output\": []}\n").unwrap();
        zip.finish().unwrap();
        buf.into_inner()
    };
    let inbox = root.join("inbox");
    std::fs::create_dir_all(&inbox).unwrap();
    std::fs::write(inbox.join("export.zip"), zip_bytes).unwrap();

    // Pass 1: unpacked, staged under bulk_dir, submitted; HFS fetched both entries from the engine.
    runner.tick().await.unwrap();
    let counts = db.feed_counts("exports").unwrap();
    assert_eq!(
        (
            counts["files"].as_i64(),
            counts["submitted"].as_i64(),
            counts["rejected"].as_i64()
        ),
        (Some(3), Some(3), Some(1)),
        "{counts}"
    );
    assert!(
        !inbox.join("export.zip").exists(),
        "the zip itself is archived at once"
    );
    let staged: Vec<String> = db
        .query_json(
            "SELECT source_key FROM files WHERE source_key IS NOT NULL AND archived = 0 ORDER BY id",
            [],
        )
        .unwrap()
        .iter()
        .map(|r| r["source_key"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(staged.len(), 2, "{staged:?}");
    assert!(
        staged
            .iter()
            .all(|k| k.starts_with(&bulk_dir.to_string_lossy().to_string())
                && std::path::Path::new(k).is_file()),
        "{staged:?}"
    );
    {
        let subs = consumer.submissions.lock().unwrap();
        let sub = subs.values().next().expect("HFS received the submission");
        let mut refs = sub.references.clone();
        refs.sort();
        assert_eq!(refs, ["Observation/o1", "Patient/p1", "Patient/p2"]);
    }

    // Passes 2 and 3: receipts in, records settled, staged entries removed.
    runner.tick().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    runner.tick().await.unwrap();
    let counts = db.feed_counts("exports").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["submitted"].as_i64()),
        (Some(3), Some(0)),
        "{counts}"
    );
    assert!(
        staged.iter().all(|k| !std::path::Path::new(k).exists()),
        "staged entries removed: {staged:?}"
    );
}

/// `POST /` as a batch: every entry stored; counts the Bundles.
async fn rest_batch(
    State(c): State<Consumer>,
    Json(bundle): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let entries: Vec<Value> = bundle["entry"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|_| json!({"response": {"status": "201 Created"}}))
        .collect();
    c.submissions
        .lock()
        .unwrap()
        .entry("rest".into())
        .or_default()
        .polls += 1;
    (
        StatusCode::OK,
        Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries})),
    )
}

/// `delivery.initial_method: bulk_submit`: the source's first load goes by
/// `$bulk-submit` over its transformed resources, the next one by REST.
#[tokio::test]
async fn the_first_load_goes_in_bulk_and_the_next_by_rest() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let consumer = Consumer {
        base: Arc::default(),
        submissions: Arc::default(),
    };
    let (hfs_listener, hfs_url) = bind().await;
    *consumer.base.lock().unwrap() = hfs_url.clone();
    let hfs = Router::new()
        .route("/", post(rest_batch))
        .route("/$bulk-submit", post(kickoff))
        .route("/$bulk-submit-status", post(status_kickoff))
        .route("/bulk-submit-status/{id}", get(poll))
        .route("/bulk-submit-file/{id}/{part}", get(artifact))
        .with_state(consumer.clone());
    tokio::spawn(async move { axum::serve(hfs_listener, hfs).await.unwrap() });

    let (hie_listener, hie_url) = bind().await;
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
        format!("fhir_base_url: {hfs_url}\npublic_url: {hie_url}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/labs.yaml"),
        "id: labs\nname: Labs\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  poll_seconds: 60\n  min_file_age_ms: 0\n\
         transform: transforms/lab_results.py\ndelivery:\n  method: rest\n  initial_method: bulk_submit\n  initial_backoff_ms: 10\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    manager.pause("labs", None).unwrap();
    let runner = manager.runner("labs").unwrap();
    let db = state.db.clone();
    let router = hie::api::router(state);
    tokio::spawn(async move { axum::serve(hie_listener, router).await.unwrap() });

    let inbox = root.join("inbox");
    std::fs::create_dir_all(&inbox).unwrap();
    std::fs::copy(repo().join("examples/data/labs.csv"), inbox.join("a.csv")).unwrap();
    runner.tick().await.unwrap();
    assert_eq!(db.feed_counts("labs").unwrap()["submitted"], 3);
    runner.tick().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    runner.tick().await.unwrap();
    assert_eq!(db.feed_counts("labs").unwrap()["sent"], 3);
    assert!(!consumer.submissions.lock().unwrap().contains_key("rest"));

    std::fs::write(
        inbox.join("b.csv"),
        "patient,code,display,value,unit,date\nPatient/3,2345-7,Glucose,99,mg/dL,2026-10-16\n",
    )
    .unwrap();
    runner.tick().await.unwrap();
    assert_eq!(db.feed_counts("labs").unwrap()["sent"], 4);
    assert_eq!(consumer.submissions.lock().unwrap()["rest"].polls, 1);
    assert_eq!(
        consumer.submissions.lock().unwrap().len(),
        2,
        "one submission, one batch"
    );
}
