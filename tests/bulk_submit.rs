//! The $bulk-submit path end to end: a stand-in HFS that behaves as the Data
//! Consumer (fetches the manifest and the file from the engine, answers the status
//! kick-off, publishes receipts), and the engine mapping those receipts back.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
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
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)")
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
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
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
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
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
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
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

// ---- public buckets and kept files ------------------------------------------

/// The stand-in FHIR server, serving at the returned URL.
async fn consumer() -> (Consumer, String) {
    let consumer = Consumer {
        base: Arc::default(),
        submissions: Arc::default(),
    };
    let (listener, url) = bind().await;
    *consumer.base.lock().unwrap() = url.clone();
    let app = Router::new()
        .route("/$bulk-submit", post(kickoff))
        .route("/$bulk-submit-status", post(status_kickoff))
        .route("/bulk-submit-status/{id}", get(poll))
        .route("/bulk-submit-file/{id}/{part}", get(artifact))
        .with_state(consumer.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (consumer, url)
}

const BUCKET: &str = "open-data";

/// A stand-in for a public S3 bucket, path style: lists its raw keys
/// (ListObjectsV2), answers GET and HEAD, refuses anything else, and records
/// every request as (method, URI as sent, signed).
#[derive(Clone, Default)]
struct Bucket {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    requests: Arc<Mutex<Vec<(String, String, bool)>>>,
}

impl Bucket {
    fn put(&self, key: &str, body: impl Into<Vec<u8>>) {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), body.into());
    }

    async fn serve(&self) -> String {
        let (listener, url) = bind().await;
        let app = Router::new()
            .fallback(bucket_request)
            .with_state(self.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    fn requests(&self) -> Vec<(String, String, bool)> {
        self.requests.lock().unwrap().clone()
    }
}

async fn bucket_request(
    State(b): State<Bucket>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let signed = headers.contains_key(header::AUTHORIZATION)
        || uri.query().is_some_and(|q| q.contains("X-Amz-Signature"));
    b.requests
        .lock()
        .unwrap()
        .push((method.to_string(), uri.to_string(), signed));
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let root = format!("/{BUCKET}");
    if uri.path() == root || uri.path() == format!("{root}/") {
        let query: HashMap<String, String> =
            url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                .into_owned()
                .collect();
        if query.get("list-type").map(String::as_str) != Some("2") {
            return StatusCode::BAD_REQUEST.into_response();
        }
        let prefix = query.get("prefix").cloned().unwrap_or_default();
        let mut xml = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated>"#,
        );
        for (key, body) in b.objects.lock().unwrap().iter() {
            if !key.starts_with(&prefix) {
                continue;
            }
            let key = key
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;");
            xml.push_str(&format!(
                "<Contents><Key>{key}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>\"0\"</ETag><Size>{}</Size></Contents>",
                body.len()
            ));
        }
        xml.push_str("</ListBucketResult>");
        return ([(header::CONTENT_TYPE, "application/xml")], xml).into_response();
    }
    let Some(encoded) = uri.path().strip_prefix(&format!("{root}/")) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let key = percent_encoding::percent_decode_str(encoded)
        .decode_utf8()
        .unwrap()
        .into_owned();
    match b.objects.lock().unwrap().get(&key) {
        Some(body) => (
            [
                (header::LAST_MODIFIED, "Thu, 01 Jan 2026 00:00:00 GMT"),
                (header::ETAG, "\"0\""),
            ],
            body.clone(),
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// A read-only anonymous source over the whole stand-in bucket, on the bulk path.
fn public_source(bucket_url: &str, pattern: &str) -> String {
    format!(
        "id: open\nname: Open data\nsource:\n  type: s3\n  bucket: {BUCKET}\n  endpoint: {bucket_url}\n  region: us-east-1\n  \
         anonymous: true\n  read_only: true\n  archive_prefix: \"\"\n  pattern: \"{pattern}\"\n  poll_seconds: 60\n  min_file_age_ms: 0\n\
         format: ndjson\ntransform: transforms/fhir_passthrough.py\ndelivery:\n  method: bulk_submit\n  initial_backoff_ms: 10\n"
    )
}

/// An engine root: `hie.yaml` with its store at `data/hie.db`, the passthrough
/// transform, and one source.
fn write_root(root: &std::path::Path, hfs_url: &str, hie_url: &str, python: &str, source: &str) {
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
    std::fs::write(root.join("sources/open.yaml"), source).unwrap();
}

struct Engine {
    manager: Arc<hie::manager::SourceManager>,
    runner: Arc<hie::pipeline::FeedRunner>,
    db: Arc<hie::db::Db>,
    url: String,
    /// Requests to the file route (`/bulk/*/files/*`); the manifest route is not counted.
    file_requests: Arc<AtomicUsize>,
}

/// Starts the engine of `config`, its source `open` paused (the test drives the
/// passes), serving on `listener`.
fn start_engine(listener: tokio::net::TcpListener, url: &str, config: &std::path::Path) -> Engine {
    let (config, feeds) = hie::config::load(config).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    manager.pause("open", None).unwrap();
    let runner = manager.runner("open").unwrap();
    let db = state.db.clone();
    let file_requests = Arc::new(AtomicUsize::new(0));
    let counter = file_requests.clone();
    let router = hie::api::router(state).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let counter = counter.clone();
            async move {
                if request.uri().path().contains("/files/") {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                next.run(request).await
            }
        },
    ));
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Engine {
        manager,
        runner,
        db,
        url: url.to_string(),
        file_requests,
    }
}

/// Every output URL of every manifest the engine opened.
fn manifest_urls(db: &hie::db::Db) -> Vec<String> {
    let mut urls: Vec<String> = db
        .query_json("SELECT manifest FROM submissions ORDER BY created_at", [])
        .unwrap()
        .iter()
        .flat_map(|r| {
            let manifest: Value = match &r["manifest"] {
                Value::String(text) => serde_json::from_str(text).unwrap(),
                other => other.clone(),
            };
            manifest["output"]
                .as_array()
                .unwrap()
                .iter()
                .map(|o| o["url"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    urls.sort();
    urls
}

fn file_id(db: &hie::db::Db, name: &str) -> i64 {
    db.query_json(
        "SELECT id FROM files WHERE name = $1",
        hie::db::params![name],
    )
    .unwrap()[0]["id"]
        .as_i64()
        .unwrap()
}

/// `to` relative to the working directory, through `..` where the two part.
fn relative_to_cwd(to: &std::path::Path) -> PathBuf {
    let cwd = std::env::current_dir().unwrap();
    let from: Vec<_> = cwd.components().collect();
    let to: Vec<_> = to.components().collect();
    let common = from.iter().zip(&to).take_while(|(a, b)| a == b).count();
    let mut relative = PathBuf::new();
    for _ in common..from.len() {
        relative.push("..");
    }
    for part in &to[common..] {
        relative.push(part);
    }
    relative
}

fn zip_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
    use std::io::Write;
    let mut buf = std::io::Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(&mut buf);
    for (name, body) in entries {
        zip.start_file(*name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(body).unwrap();
    }
    zip.finish().unwrap();
    buf.into_inner()
}

/// Ticks until the feed has `sent` records sent, across the engine's 2 s wait between polls.
async fn settle_until_sent(
    runner: &hie::pipeline::FeedRunner,
    db: &hie::db::Db,
    feed: &str,
    sent: i64,
) {
    for _ in 0..5 {
        if db.feed_counts(feed).unwrap()["sent"].as_i64() == Some(sent) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2100)).await;
        runner.tick().await.unwrap();
    }
    panic!("not settled: {}", db.feed_counts(feed).unwrap());
}

/// A public bucket read as a whole: the engine lists it by raw keys, reads each
/// object unsigned, and hands the FHIR server each object's public URL, its key
/// encoded once. The engine's own file route is never used.
#[tokio::test]
async fn an_anonymous_bucket_is_handed_over_by_its_public_urls() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let bucket = Bucket::default();
    bucket.put(
        "Patient.ndjson",
        "{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n{\"resourceType\":\"Patient\",\"id\":\"p2\"}\n",
    );
    // A whole bucket with an empty archive value: these folders are read too.
    bucket.put(
        "processed/Observation.ndjson",
        "{\"resourceType\":\"Observation\",\"id\":\"o1\"}\n{\"resourceType\":\"Observation\"}\n",
    );
    bucket.put(
        "_archive/Encounter.ndjson",
        "{\"resourceType\":\"Encounter\",\"id\":\"e1\"}\n",
    );
    bucket.put(
        "a b/Obs+1.ndjson",
        "{\"resourceType\":\"Observation\",\"id\":\"o2\"}\n",
    );
    bucket.put(
        "x/é 50%~y.ndjson",
        "{\"resourceType\":\"Observation\",\"id\":\"o3\"}\n",
    );
    bucket.put("parameters.json", "{}");
    let bucket_url = bucket.serve().await;
    let (consumer, hfs_url) = consumer().await;

    let (listener, hie_url) = bind().await;
    let dir = tempfile::tempdir().unwrap();
    write_root(
        dir.path(),
        &hfs_url,
        &hie_url,
        &python,
        &public_source(&bucket_url, "*.ndjson"),
    );
    let engine = start_engine(listener, &hie_url, &dir.path().join("hie.yaml"));

    // The connection test sees every matching object of the bucket.
    let probe = engine
        .manager
        .test(
            &json!({"type": "s3", "bucket": BUCKET, "endpoint": bucket_url, "region": "us-east-1",
            "anonymous": true, "read_only": true, "archive_prefix": "", "pattern": "*.ndjson"}),
        )
        .await
        .unwrap();
    assert!(probe.reachable, "{probe:?}");
    assert_eq!(probe.objects, 5, "{probe:?}");

    engine.runner.tick().await.unwrap();
    let encoded = [
        "Patient.ndjson",
        "_archive/Encounter.ndjson",
        "a%20b/Obs%2B1.ndjson",
        "processed/Observation.ndjson",
        "x/%C3%A9%2050%25~y.ndjson",
    ];
    let mut expected: Vec<String> = encoded
        .iter()
        .map(|k| format!("{bucket_url}/{BUCKET}/{k}"))
        .collect();
    expected.sort();
    assert_eq!(manifest_urls(&engine.db), expected);

    engine.runner.tick().await.unwrap();
    settle_until_sent(&engine.runner, &engine.db, "open", 6).await;
    let counts = engine.db.feed_counts("open").unwrap();
    assert_eq!(
        (
            counts["sent"].as_i64(),
            counts["rejected"].as_i64(),
            counts["submitted"].as_i64()
        ),
        (Some(6), Some(1), Some(0)),
        "{counts}"
    );
    {
        let subs = consumer.submissions.lock().unwrap();
        let mut refs: Vec<String> = subs.values().flat_map(|s| s.references.clone()).collect();
        refs.sort();
        assert_eq!(
            refs,
            [
                "Encounter/e1",
                "Observation/o1",
                "Observation/o2",
                "Observation/o3",
                "Patient/p1",
                "Patient/p2"
            ]
        );
    }

    let requests = bucket.requests();
    assert!(
        requests.iter().all(|(_, _, signed)| !signed),
        "{requests:?}"
    );
    assert!(
        requests.iter().all(|(m, _, _)| m == "GET" || m == "HEAD"),
        "{requests:?}"
    );
    for key in encoded {
        let path = format!("/{BUCKET}/{key}");
        let reads = requests
            .iter()
            .filter(|(m, uri, _)| m == "GET" && *uri == path)
            .count();
        assert!(
            reads >= 2,
            "the engine and the FHIR server each read {path}: {requests:?}"
        );
    }
    assert_eq!(engine.file_requests.load(Ordering::SeqCst), 0);
}

/// The engine serves only files it holds itself: a store object answers 404 at
/// the engine's file route even when a local file has the same path as its key,
/// while an entry the engine unpacked from a zip is served.
#[tokio::test]
async fn the_engine_never_serves_a_store_object_from_its_file_endpoint() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    // The key names a file that exists relative to the engine's working directory.
    assert!(std::path::Path::new("examples/data/labs.csv").is_file());
    let entry = b"{\"resourceType\":\"Patient\",\"id\":\"z1\"}\n";
    let bucket = Bucket::default();
    bucket.put(
        "examples/data/labs.csv",
        "{\"resourceType\":\"Patient\",\"id\":\"s1\"}\n",
    );
    bucket.put("examples/drop.zip", zip_of(&[("Patient.ndjson", entry)]));
    let bucket_url = bucket.serve().await;
    let (_consumer, hfs_url) = consumer().await;

    let (listener, hie_url) = bind().await;
    let dir = tempfile::tempdir().unwrap();
    write_root(
        dir.path(),
        &hfs_url,
        &hie_url,
        &python,
        &public_source(&bucket_url, "*"),
    );
    let engine = start_engine(listener, &hie_url, &dir.path().join("hie.yaml"));
    engine.runner.tick().await.unwrap();

    let token = engine
        .db
        .query_json("SELECT token FROM submissions", [])
        .unwrap()[0]["token"]
        .as_str()
        .unwrap()
        .to_string();
    let store_object = file_id(&engine.db, "labs.csv");
    let staged = file_id(&engine.db, "drop.zip!/Patient.ndjson");
    let get = |id: i64| reqwest::get(format!("{}/bulk/{token}/files/{id}", engine.url));
    let refused = get(store_object).await.unwrap();
    assert_eq!(refused.status(), 404);
    let served = get(staged).await.unwrap();
    assert_eq!(served.status(), 200);
    assert_eq!(served.bytes().await.unwrap().as_ref(), entry);
    assert!(
        manifest_urls(&engine.db)
            .contains(&format!("{bucket_url}/{BUCKET}/examples/data/labs.csv"))
    );
}

/// Under a relative bulk directory a store key is still shared from its store,
/// and an entry the engine unpacked is still known as staged and served.
#[tokio::test]
async fn store_keys_and_zip_entries_under_a_relative_bulk_directory() {
    let bucket = Bucket::default();
    bucket.put(
        "data/bulk/Patient.ndjson",
        "{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n",
    );
    bucket.put(
        "drop.zip",
        zip_of(&[(
            "Observation.ndjson",
            b"{\"resourceType\":\"Observation\",\"id\":\"o1\"}\n",
        )]),
    );
    let bucket_url = bucket.serve().await;
    // A FHIR server that is not there: the submissions stay open.
    let closed = {
        let (listener, url) = bind().await;
        drop(listener);
        url
    };
    let feed: hie::config::FeedConfig =
        serde_yaml::from_str(&public_source(&bucket_url, "*")).unwrap();
    let source = hie::source::Source::from_config(&feed).unwrap();
    let staging = tempfile::tempdir().unwrap();
    let relative = relative_to_cwd(staging.path());
    assert!(relative.is_relative());
    let db = Arc::new(hie::db::Db::in_memory().unwrap());
    let runner = hie::pipeline::FeedRunner::with_source(
        feed,
        source,
        db.clone(),
        Arc::new(hie::fhir::FhirClient::new(&closed)),
        Arc::new(hie::alerts::Alerter::new(db.clone(), None)),
        "python3",
        &repo().join("hie_runtime"),
        &closed,
        "http://engine.test",
    )
    .with_bulk_dir(&relative);
    runner.tick().await.unwrap();

    let entry = file_id(&db, "drop.zip!/Observation.ndjson");
    let rows = db
        .query_json(
            "SELECT origin, source_key FROM files WHERE id = $1",
            hie::db::params![entry],
        )
        .unwrap();
    assert_eq!(rows[0]["origin"], "staged");
    assert!(
        rows[0]["source_key"]
            .as_str()
            .unwrap()
            .starts_with(&*relative.to_string_lossy())
    );
    let urls = manifest_urls(&db);
    assert!(
        urls.contains(&format!("{bucket_url}/{BUCKET}/data/bulk/Patient.ndjson")),
        "{urls:?}"
    );
    assert!(
        urls.iter()
            .any(|u| u.starts_with("http://engine.test/bulk/")
                && u.ends_with(&format!("/files/{entry}"))),
        "{urls:?}"
    );
}

/// A signer that fails its second call, as while credentials are refreshed; its
/// URLs point at a stand-in that serves the store's objects.
#[derive(Debug)]
struct FlakySigner {
    base: String,
    calls: AtomicUsize,
}

impl object_store::signer::Signer for FlakySigner {
    fn signed_url<'a, 'b, 'c>(
        &'a self,
        _method: http::Method,
        path: &'b object_store::path::Path,
        _expires_in: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<url::Url>> + Send + 'c>,
    >
    where
        'a: 'c,
        'b: 'c,
        Self: 'c,
    {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let url = format!("{}/objects/{path}", self.base);
        Box::pin(async move {
            if call == 2 {
                return Err(object_store::Error::Generic {
                    store: "test",
                    source: "credentials are being refreshed".into(),
                });
            }
            Ok(url::Url::parse(&url).unwrap())
        })
    }
}

#[derive(Clone)]
struct Served {
    store: Arc<object_store::memory::InMemory>,
    db: Arc<hie::db::Db>,
}

async fn served_object(State(s): State<Served>, Path(key): Path<String>) -> Response {
    use object_store::ObjectStore;
    let path = object_store::path::Path::parse(&key).unwrap();
    match s.store.get(&path).await {
        Ok(found) => found.bytes().await.unwrap().to_vec().into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn served_manifest(State(s): State<Served>, Path(token): Path<String>) -> Response {
    match s.db.submission(&token).unwrap() {
        Some(sub) => Json(sub.manifest).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn object_feed() -> hie::config::FeedConfig {
    serde_yaml::from_str(
        "id: drops\nname: Drops\nsource:\n  type: s3\n  bucket: unused\nformat: ndjson\ntransform: transforms/fhir_passthrough.py\n\
         delivery:\n  method: bulk_submit\n  bulk_from: source_objects\n  initial_backoff_ms: 10\n",
    )
    .unwrap()
}

async fn put_object(store: &object_store::memory::InMemory, key: &str, id: &str) {
    use object_store::ObjectStore;
    store
        .put(
            &object_store::path::Path::parse(key).unwrap(),
            format!("{{\"resourceType\":\"Patient\",\"id\":\"{id}\"}}\n").into(),
        )
        .await
        .unwrap();
}

fn statuses(db: &hie::db::Db, name: &str) -> Vec<String> {
    db.query_json(
        "SELECT r.status FROM records r JOIN files f ON f.id = r.file_id WHERE f.name = $1",
        hie::db::params![name],
    )
    .unwrap()
    .iter()
    .map(|r| r["status"].as_str().unwrap().to_string())
    .collect()
}

/// A URL that cannot be signed yet leaves its file's records pending for the next
/// pass, and the submissions already open are still polled in that pass.
#[tokio::test]
async fn a_signing_failure_is_retried_while_open_submissions_keep_polling() {
    let (consumer, hfs_url) = consumer().await;
    let store = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(hie::db::Db::in_memory().unwrap());
    let (listener, served_url) = bind().await;
    let app = Router::new()
        .route("/objects/{*key}", get(served_object))
        .route("/bulk/{token}/manifest.json", get(served_manifest))
        .with_state(Served {
            store: store.clone(),
            db: db.clone(),
        });
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let signer = Arc::new(FlakySigner {
        base: served_url.clone(),
        calls: AtomicUsize::new(0),
    });
    let source = hie::source::Source::Object(
        hie::source::ObjectSource::new(store.clone(), "", "processed", "*.ndjson", Duration::ZERO)
            .with_signer(signer.clone()),
    );
    let runner = hie::pipeline::FeedRunner::with_source(
        object_feed(),
        source,
        db.clone(),
        Arc::new(hie::fhir::FhirClient::new(&hfs_url)),
        Arc::new(hie::alerts::Alerter::new(db.clone(), None)),
        "python3",
        &repo().join("hie_runtime"),
        &hfs_url,
        &served_url,
    );

    // Pass 1: A is signed, submitted and kicked off.
    put_object(&store, "a.ndjson", "a").await;
    runner.tick().await.unwrap();
    assert_eq!(consumer.submissions.lock().unwrap().len(), 1);
    let polls = || {
        consumer
            .submissions
            .lock()
            .unwrap()
            .values()
            .map(|s| s.polls)
            .sum::<u32>()
    };
    assert_eq!(polls(), 0);

    // Pass 2: B cannot be signed. The pass goes on: B stays pending, A is polled.
    put_object(&store, "b.ndjson", "b").await;
    runner.tick().await.unwrap();
    assert_eq!(signer.calls.load(Ordering::SeqCst), 2);
    assert_eq!(statuses(&db, "b.ndjson"), ["pending"]);
    assert_eq!(polls(), 1, "the open submission was polled");

    // Pass 3: B is signed and submitted; then both are settled.
    runner.tick().await.unwrap();
    assert_eq!(consumer.submissions.lock().unwrap().len(), 2);
    settle_until_sent(&runner, &db, "drops", 2).await;
    assert_eq!(statuses(&db, "a.ndjson"), ["sent"]);
    assert_eq!(statuses(&db, "b.ndjson"), ["sent"]);
}

/// A store object that no configuration can hand over is settled as rejected
/// with its reason, and the pass goes on.
#[tokio::test]
async fn an_unshareable_object_is_rejected_without_stalling() {
    let closed = {
        let (listener, url) = bind().await;
        drop(listener);
        url
    };
    let store = Arc::new(object_store::memory::InMemory::new());
    put_object(&store, "a.ndjson", "a").await;
    let db = Arc::new(hie::db::Db::in_memory().unwrap());
    let runner = hie::pipeline::FeedRunner::with_source(
        object_feed(),
        hie::source::Source::Object(hie::source::ObjectSource::new(
            store,
            "",
            "processed",
            "*.ndjson",
            Duration::ZERO,
        )),
        db.clone(),
        Arc::new(hie::fhir::FhirClient::new(&closed)),
        Arc::new(hie::alerts::Alerter::new(db.clone(), None)),
        "python3",
        &repo().join("hie_runtime"),
        &closed,
        "http://engine.test",
    );
    runner.tick().await.unwrap();
    let rows = db
        .query_json("SELECT status, error FROM records", [])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["status"], "rejected");
    assert!(
        rows[0]["error"]
            .as_str()
            .unwrap()
            .contains("cannot be handed over to the FHIR server"),
        "{rows:?}"
    );
    assert!(
        db.query_json("SELECT token FROM submissions", [])
            .unwrap()
            .is_empty()
    );
}

/// The store an engine opens for `config`, opened the same way (in PostgreSQL test
/// mode the schema follows the spelling of the path). The first handle owns a test
/// schema, so it must live as long as the test.
fn seed_store(config: &std::path::Path) -> hie::db::Db {
    let (config, _) = hie::config::load(config).unwrap();
    hie::db::Db::connect(&config.database.to_string_lossy()).unwrap()
}

/// An upgrade: entries an earlier version unpacked and recorded by a path relative
/// to its working directory, with no origin, are recognised as staged at start-up.
/// One already in an open submission is served and settled; one with rejected
/// records can be retried and is served again. An entry row whose path is outside
/// the bulk directory is never served. Here the engine starts from the same
/// relative configuration path as the earlier version.
#[tokio::test]
async fn entries_staged_by_an_earlier_version_are_served_settled_and_retried() {
    staged_by_an_earlier_version(true).await;
}

/// The same upgrade started from the absolute path of the same configuration, the
/// working directory unchanged: the recorded paths, with their `..`, still point
/// into the bulk directory.
#[tokio::test]
async fn entries_staged_by_an_earlier_version_are_recognised_from_an_absolute_configuration() {
    staged_by_an_earlier_version(false).await;
}

async fn staged_by_an_earlier_version(relative_config: bool) {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let bucket = Bucket::default();
    let bucket_url = bucket.serve().await;
    let (consumer, hfs_url) = consumer().await;
    let (listener, hie_url) = bind().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_root(
        root,
        &hfs_url,
        &hie_url,
        &python,
        &public_source(&bucket_url, "*.ndjson"),
    );
    // The earlier version ran with a relative configuration path, so what it
    // staged is recorded relative to the working directory.
    let config = if relative_config {
        relative_to_cwd(&root.join("hie.yaml"))
    } else {
        root.join("hie.yaml")
    };
    let staged_dir = root.join("data/bulk/unpacked/7");
    std::fs::create_dir_all(&staged_dir).unwrap();
    let e1_bytes = "{\"resourceType\":\"Patient\",\"id\":\"u1\"}\n{\"resourceType\":\"Patient\",\"id\":\"u2\"}\n";
    let e2_bytes = "{\"resourceType\":\"Observation\",\"id\":\"v1\"}\n";
    std::fs::write(staged_dir.join("1.ndjson"), e1_bytes).unwrap();
    std::fs::write(staged_dir.join("2.ndjson"), e2_bytes).unwrap();
    let e1_key = relative_to_cwd(&staged_dir.join("1.ndjson"));
    let e2_key = relative_to_cwd(&staged_dir.join("2.ndjson"));
    assert!(e1_key.is_relative() && e1_key.is_file());
    assert!(
        e1_key
            .components()
            .any(|c| c == std::path::Component::ParentDir),
        "{e1_key:?}"
    );

    // The store as the earlier version left it: no origin on any row.
    let seed = seed_store(&config);
    let (e1, e2, e3, e1_token, e3_token) = {
        let db = &seed;
        let file = |name: &str, key: &str, body: &str| {
            let id = db
                .insert_file(
                    "open",
                    name,
                    &format!("sha-{name}"),
                    body.len() as u64,
                    "processing",
                    None,
                    Some(key),
                    None,
                )
                .unwrap();
            let rows: Vec<_> = body
                .lines()
                .enumerate()
                .map(|(n, l)| (n as i64 + 1, serde_json::from_str(l).unwrap(), None))
                .collect();
            db.insert_records(id, "open", &rows).unwrap();
            let records: Vec<i64> = db
                .query_json(
                    "SELECT id FROM records WHERE file_id = $1",
                    hie::db::params![id],
                )
                .unwrap()
                .iter()
                .map(|r| r["id"].as_i64().unwrap())
                .collect();
            (id, records)
        };
        let open = |token: &str, file_id: i64, records: &[i64], kind: &str| {
            db.mark_passthrough(records).unwrap();
            let manifest = json!({"output": [{"type": kind,
                "url": format!("{hie_url}/bulk/{token}/files/{file_id}"), "count": records.len()}]});
            db.create_submission_for_files(token, "open", token, &manifest, &[file_id])
                .unwrap();
        };
        let (e1, e1_records) = file(
            "drop.zip!/Patient.ndjson",
            &e1_key.to_string_lossy(),
            e1_bytes,
        );
        open("hie-earlier-e1", e1, &e1_records, "Patient");
        let (e2, e2_records) = file(
            "drop.zip!/Observation.ndjson",
            &e2_key.to_string_lossy(),
            e2_bytes,
        );
        for r in e2_records {
            db.mark_rejected(r, "server", "HTTP 503", None).unwrap();
        }
        let (e3, e3_records) = file(
            "drop.zip!/Encounter.ndjson",
            "examples/data/labs.csv",
            "{\"resourceType\":\"Encounter\",\"id\":\"w1\"}\n",
        );
        open("hie-earlier-e3", e3, &e3_records, "Encounter");
        (e1, e2, e3, "hie-earlier-e1", "hie-earlier-e3")
    };

    let engine = start_engine(listener, &hie_url, &config);
    let origin = |id: i64| {
        engine
            .db
            .query_json(
                "SELECT origin FROM files WHERE id = $1",
                hie::db::params![id],
            )
            .unwrap()[0]["origin"]
            .clone()
    };
    assert_eq!(
        (origin(e1), origin(e2), origin(e3)),
        (json!("staged"), json!("staged"), json!("store"))
    );

    let get = |token: &str, id: i64| reqwest::get(format!("{hie_url}/bulk/{token}/files/{id}"));
    let served = get(e1_token, e1).await.unwrap();
    assert_eq!(served.status(), 200);
    assert_eq!(served.text().await.unwrap(), e1_bytes);
    assert_eq!(get(e3_token, e3).await.unwrap().status(), 404);

    // The open submissions are kicked off and settled; the staged file goes.
    engine.runner.tick().await.unwrap();
    engine.runner.tick().await.unwrap();
    settle_until_sent(&engine.runner, &engine.db, "open", 2).await;
    assert_eq!(
        statuses(&engine.db, "drop.zip!/Patient.ndjson"),
        ["sent", "sent"]
    );
    assert!(!e1_key.exists(), "the staged entry is removed once settled");
    {
        let subs = consumer.submissions.lock().unwrap();
        let mut refs: Vec<String> = subs.values().flat_map(|s| s.references.clone()).collect();
        refs.sort();
        assert_eq!(refs, ["Patient/u1", "Patient/u2"]);
    }

    // Retry of the rejected entry: handed over by the engine again, and settled.
    let retried: Value = reqwest::Client::new()
        .post(format!("{hie_url}/api/loads/{e2}/retry"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retried["retried"], 1);
    engine.runner.tick().await.unwrap();
    let reopened = format!("/files/{e2}");
    let urls = manifest_urls(&engine.db);
    let url = urls.iter().find(|u| u.ends_with(&reopened)).unwrap();
    assert!(url.starts_with(&format!("{hie_url}/bulk/")), "{urls:?}");
    let token = url
        .trim_start_matches(&format!("{hie_url}/bulk/"))
        .split('/')
        .next()
        .unwrap();
    let served = get(token, e2).await.unwrap();
    assert_eq!(served.status(), 200);
    assert_eq!(served.text().await.unwrap(), e2_bytes);
    engine.runner.tick().await.unwrap();
    settle_until_sent(&engine.runner, &engine.db, "open", 3).await;
    assert_eq!(
        statuses(&engine.db, "drop.zip!/Observation.ndjson"),
        ["sent"]
    );
    drop(engine);
    drop(seed);
}

/// A folder source on the bulk path that hands its own files over.
fn folder_source() -> &'static str {
    "id: open\nname: Open data\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.ndjson\"\n  poll_seconds: 60\n  min_file_age_ms: 0\n\
     format: ndjson\ntransform: transforms/fhir_passthrough.py\ndelivery:\n  method: bulk_submit\n  initial_backoff_ms: 10\n"
}

/// An upgrade from a store without file origins: a folder file an earlier version
/// archived after a load with a rejected record is retried, and the engine serves
/// it from where it was archived.
#[tokio::test]
async fn an_archived_folder_file_from_an_earlier_version_is_served_on_retry() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (_consumer, hfs_url) = consumer().await;
    let (listener, hie_url) = bind().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_root(root, &hfs_url, &hie_url, &python, folder_source());
    let body = "{\"resourceType\":\"Patient\",\"id\":\"f1\"}\n";
    let archived = root.join("inbox/processed/1-export.ndjson");
    std::fs::create_dir_all(archived.parent().unwrap()).unwrap();
    std::fs::write(&archived, body).unwrap();

    // The store as the earlier version left it: the file archived, its record
    // rejected, and no origin column at all.
    let config = root.join("hie.yaml");
    let seed = seed_store(&config);
    let file = seed
        .insert_file(
            "open",
            "export.ndjson",
            "sha-export",
            body.len() as u64,
            "done",
            None,
            None,
            None,
        )
        .unwrap();
    seed.insert_records(
        file,
        "open",
        &[(1, serde_json::from_str(body).unwrap(), None)],
    )
    .unwrap();
    let record = seed
        .query_json(
            "SELECT id FROM records WHERE file_id = $1",
            hie::db::params![file],
        )
        .unwrap()[0]["id"]
        .as_i64()
        .unwrap();
    seed.mark_rejected(record, "server", "HTTP 503", None)
        .unwrap();
    seed.mark_file_archived(file, &archived.to_string_lossy())
        .unwrap();
    seed.query_json("ALTER TABLE files DROP COLUMN origin", [])
        .unwrap();

    let engine = start_engine(listener, &hie_url, &config);
    let retried: Value = reqwest::Client::new()
        .post(format!("{hie_url}/api/loads/{file}/retry"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retried["retried"], 1);
    engine.runner.tick().await.unwrap();
    let urls = manifest_urls(&engine.db);
    assert_eq!(urls.len(), 1, "{urls:?}");
    assert!(urls[0].starts_with(&format!("{hie_url}/bulk/")), "{urls:?}");
    let served = reqwest::get(&urls[0]).await.unwrap();
    assert_eq!(served.status(), 200);
    assert_eq!(served.text().await.unwrap(), body);
    drop(engine);
    drop(seed);
}

/// A FHIR server that refuses every submission.
async fn refusing_server() -> String {
    async fn refuse() -> impl IntoResponse {
        (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"resourceType": "OperationOutcome", "issue": [{"severity": "error", "code": "invalid"}]}),
            ),
        )
    }
    let (listener, url) = bind().await;
    let app = Router::new().route("/$bulk-submit", post(refuse));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

/// A source replaced by one of another kind keeps its records. A file taken from
/// a bucket and retried after the replacement cannot be read from its store any
/// more: its records are rejected, and the engine never offers to serve it.
#[tokio::test]
async fn a_store_file_retried_after_its_source_became_a_folder_is_rejected() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let bucket = Bucket::default();
    bucket.put("a.ndjson", "{\"resourceType\":\"Patient\",\"id\":\"a\"}\n");
    let bucket_url = bucket.serve().await;
    let hfs_url = refusing_server().await;
    let (listener, hie_url) = bind().await;
    let dir = tempfile::tempdir().unwrap();
    write_root(
        dir.path(),
        &hfs_url,
        &hie_url,
        &python,
        &public_source(&bucket_url, "*.ndjson"),
    );
    let engine = start_engine(listener, &hie_url, &dir.path().join("hie.yaml"));
    // Taken from the bucket, handed over, refused: the record is rejected.
    engine.runner.tick().await.unwrap();
    let file = file_id(&engine.db, "a.ndjson");
    assert_eq!(statuses(&engine.db, "a.ndjson"), ["rejected"]);

    let replaced = reqwest::Client::new()
        .put(format!("{hie_url}/api/sources/open"))
        .json(&json!({"id": "open", "name": "Open data",
            "source": {"type": "folder", "path": "inbox", "pattern": "*.ndjson", "poll_seconds": 60, "min_file_age_ms": 0},
            "format": "ndjson", "transform": "transforms/fhir_passthrough.py",
            "delivery": {"method": "bulk_submit", "initial_backoff_ms": 10}}))
        .send()
        .await
        .unwrap();
    assert!(replaced.status().is_success(), "{}", replaced.status());
    let runner = engine.manager.runner("open").unwrap();
    let retried: Value = reqwest::Client::new()
        .post(format!("{hie_url}/api/loads/{file}/retry"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retried["retried"], 1);
    runner.tick().await.unwrap();

    let rows = engine
        .db
        .query_json("SELECT status, error FROM records", [])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["status"], "rejected");
    assert!(
        rows[0]["error"]
            .as_str()
            .unwrap()
            .contains("cannot be handed over to the FHIR server"),
        "{rows:?}"
    );
    let urls = manifest_urls(&engine.db);
    assert!(urls.iter().all(|u| !u.contains("/files/")), "{urls:?}");
    assert_eq!(urls, [format!("{bucket_url}/{BUCKET}/a.ndjson")]);
}

/// A configuration reached through a symbolic link and `..` (`outer/link/..` where
/// `outer/link -> actual/nested`): a kept folder file is recorded by that path, and
/// the engine serves the file the filesystem resolves it to, the one it took, never
/// a different file that a by-name reading of `link/..` would point at.
#[cfg(unix)]
#[tokio::test]
async fn a_file_recorded_through_a_symlink_and_parent_is_served_as_the_filesystem_resolves_it() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let original = "{\"resourceType\":\"Patient\",\"id\":\"ORIGINAL\"}\n";
    let decoy = "{\"resourceType\":\"Patient\",\"id\":\"DECOY\"}\n";
    let (_consumer, hfs_url) = consumer().await;
    let (listener, hie_url) = bind().await;
    let dir = tempfile::tempdir().unwrap();
    let actual = dir.path().join("actual");
    let outer = dir.path().join("outer");
    std::fs::create_dir_all(actual.join("nested")).unwrap();
    std::fs::create_dir_all(outer.join("inbox")).unwrap();
    std::os::unix::fs::symlink(actual.join("nested"), outer.join("link")).unwrap();
    write_root(&actual, &hfs_url, &hie_url, &python, folder_source());
    std::fs::create_dir_all(actual.join("inbox")).unwrap();
    std::fs::write(actual.join("inbox/a.ndjson"), original).unwrap();
    std::fs::write(outer.join("inbox/a.ndjson"), decoy).unwrap();

    let engine = start_engine(listener, &hie_url, &outer.join("link/../hie.yaml"));
    engine.runner.tick().await.unwrap();
    let key = engine
        .db
        .query_json("SELECT source_key FROM files WHERE name = 'a.ndjson'", [])
        .unwrap()[0]["source_key"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(key.contains("link/../inbox"), "{key}");
    let urls = manifest_urls(&engine.db);
    assert_eq!(urls.len(), 1, "{urls:?}");
    let served = reqwest::get(&urls[0]).await.unwrap();
    let status = served.status();
    let body = served.text().await.unwrap();
    assert!(!body.contains("DECOY"), "{status}: {body}");
    assert!(status == 404 || body == original, "{status}: {body}");
}
