//! A zip is a drop of several files: each entry that fits the source's format is a
//! load of its own, the rest are skipped, and the zip itself is archived. On a
//! read-only object store nothing is moved and nothing is read twice.

use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use hie::alerts::Alerter;
use hie::db::Db;
use hie::fhir::FhirClient;
use hie::pipeline::FeedRunner;
use hie::source::{ObjectSource, Source};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
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

fn python() -> Option<&'static str> {
    ["py", "python3", "python"].into_iter().find(|p| {
        std::process::Command::new(p)
            .arg("-c")
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

/// A zip the way an export tool writes one: a folder, NDJSON per type, a manifest.
fn export_zip() -> Vec<u8> {
    let mut buf = std::io::Cursor::new(Vec::new());
    {
        let mut zip = zip::ZipWriter::new(&mut buf);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.add_directory("out/", options).unwrap();
        zip.start_file("out/Patient.ndjson", options).unwrap();
        zip.write_all(b"{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n{\"resourceType\":\"Patient\",\"id\":\"p2\"}\n")
            .unwrap();
        zip.start_file("out/Observation.ndjson", options).unwrap();
        zip.write_all(b"{\"resourceType\":\"Observation\",\"id\":\"o1\",\"subject\":{\"reference\":\"Patient/p1\"}}\n")
            .unwrap();
        zip.start_file("out/manifest.json", options).unwrap();
        zip.write_all(
            b"{\n  \"transactionTime\": \"2026-10-22T00:00:00Z\",\n  \"output\": []\n}\n",
        )
        .unwrap();
        zip.start_file("out/answer_key.csv", options).unwrap();
        zip.write_all(b"order,should_alert\n1,true\n").unwrap();
        zip.finish().unwrap();
    }
    buf.into_inner()
}

#[tokio::test]
async fn a_zip_in_a_read_only_bucket_is_unpacked_once_and_left_in_place() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found");
        return;
    };
    let urls: Urls = Arc::default();
    let app = Router::new()
        .route("/", post(batch))
        .with_state(urls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hfs = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
        "id: tpad\nname: TPAD\nsource:\n  type: s3\n  bucket: unused\n  read_only: true\nformat: ndjson\ntransform: {}\n",
        repo.join("transforms/fhir_passthrough.py").display().to_string().replace('\\', "/")
    ))
    .unwrap();

    // Someone else's bucket: a zip we may read but must not move.
    let store = Arc::new(InMemory::new());
    store
        .put(
            &ObjectPath::from("public/tpad/v2/tpad_ndjson_v2.zip"),
            PutPayload::from(export_zip()),
        )
        .await
        .unwrap();
    let source = Source::Object(
        ObjectSource::new(
            store.clone(),
            "public/tpad",
            "processed",
            "*.zip",
            Duration::ZERO,
        )
        .read_only(true),
    );
    assert!(source.is_read_only());

    let db = Arc::new(Db::in_memory().unwrap());
    let runner = FeedRunner::with_source(
        feed,
        source,
        db.clone(),
        Arc::new(FhirClient::new(&hfs)),
        Arc::new(Alerter::new(db.clone(), None)),
        python,
        &repo.join("hie_runtime"),
        &hfs,
        "http://127.0.0.1:0",
    );
    runner.tick().await.unwrap();

    // Three resources delivered from two entries; the manifest and the CSV skipped.
    let counts = db.feed_counts("tpad").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["rejected"].as_i64()),
        (Some(3), Some(0)),
        "{counts}"
    );
    let mut sent = urls.lock().unwrap().clone();
    sent.sort();
    assert_eq!(sent, ["Observation/o1", "Patient/p1", "Patient/p2"]);
    let files = db
        .query_json("SELECT name, status FROM files ORDER BY id", [])
        .unwrap();
    let names: Vec<(String, String)> = files
        .iter()
        .map(|f| {
            (
                f["name"].as_str().unwrap().to_string(),
                f["status"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        names,
        [
            ("tpad_ndjson_v2.zip".to_string(), "done".to_string()),
            (
                "tpad_ndjson_v2.zip!/out/Patient.ndjson".to_string(),
                "done".to_string()
            ),
            (
                "tpad_ndjson_v2.zip!/out/Observation.ndjson".to_string(),
                "done".to_string()
            ),
        ]
    );

    // The zip is still where it was, and the next pass does not read it again.
    assert!(
        store
            .head(&ObjectPath::from("public/tpad/v2/tpad_ndjson_v2.zip"))
            .await
            .is_ok()
    );
    assert!(
        store
            .head(&ObjectPath::from("processed/1-tpad_ndjson_v2.zip"))
            .await
            .is_err()
    );
    runner.tick().await.unwrap();
    let counts = db.feed_counts("tpad").unwrap();
    assert_eq!(
        (
            counts["files"].as_i64(),
            counts["duplicate_files"].as_i64(),
            counts["sent"].as_i64()
        ),
        (Some(3), Some(0), Some(3))
    );
}

#[tokio::test]
async fn a_zip_in_a_folder_is_unpacked_and_archived() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found");
        return;
    };
    let urls: Urls = Arc::default();
    let app = Router::new()
        .route("/", post(batch))
        .with_state(urls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hfs = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let dir = tempfile::tempdir().unwrap();
    let inbox = dir.path().join("inbox");
    std::fs::create_dir_all(&inbox).unwrap();
    std::fs::write(inbox.join("drop.zip"), export_zip()).unwrap();
    std::fs::write(inbox.join("broken.zip"), b"not a zip at all").unwrap();
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(
        "id: drops\nname: Drops\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.zip\"\n  min_file_age_ms: 0\nformat: ndjson\ntransform: transforms/fhir_passthrough.py\n",
    )
    .unwrap();
    feed.resolve(&repo).unwrap();
    if let hie::config::SourceConfig::Folder { path, .. } = &mut feed.source {
        *path = inbox.clone();
    }
    let db = Arc::new(Db::in_memory().unwrap());
    let runner = FeedRunner::with_source(
        feed.clone(),
        Source::from_config(&feed).unwrap(),
        db.clone(),
        Arc::new(FhirClient::new(&hfs)),
        Arc::new(Alerter::new(db.clone(), None)),
        python,
        &repo.join("hie_runtime"),
        &hfs,
        "http://127.0.0.1:0",
    );
    runner.tick().await.unwrap();

    let counts = db.feed_counts("drops").unwrap();
    assert_eq!(counts["sent"], 3, "{counts}");
    assert!(!inbox.join("drop.zip").exists() && !inbox.join("broken.zip").exists());
    let processed: Vec<String> = inbox
        .join("processed")
        .read_dir()
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        processed.iter().any(|n| n.ends_with("-drop.zip")),
        "{processed:?}"
    );
    let broken = db
        .query_json(
            "SELECT status, error FROM files WHERE name = 'broken.zip'",
            [],
        )
        .unwrap();
    assert_eq!(broken[0]["status"], "unreadable");
    let alerts = db
        .query_json("SELECT kind FROM alerts WHERE feed_id = 'drops'", [])
        .unwrap();
    assert_eq!(alerts[0]["kind"], "unreadable_file");
}

/// A pass that stopped half-way through a zip (the container `unpacking`, one
/// entry admitted) is resumed by the next: the entry already in is not taken
/// again, the rest are, and the zip is not mistaken for a duplicate of itself.
#[tokio::test]
async fn a_zip_unpacked_half_way_is_resumed_not_skipped() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found");
        return;
    };
    let urls: Urls = Arc::default();
    let app = Router::new()
        .route("/", post(batch))
        .with_state(urls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hfs = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
        "id: drops\nname: Drops\nsource:\n  type: s3\n  bucket: unused\nformat: ndjson\ntransform: {}\n",
        repo.join("transforms/fhir_passthrough.py").display().to_string().replace('\\', "/")
    ))
    .unwrap();
    let zip = export_zip();
    let store = Arc::new(InMemory::new());
    store
        .put(
            &ObjectPath::from("in/drop.zip"),
            PutPayload::from(zip.clone()),
        )
        .await
        .unwrap();
    let source = Source::Object(ObjectSource::new(
        store.clone(),
        "in",
        "processed",
        "*.zip",
        Duration::ZERO,
    ));
    let db = Arc::new(Db::in_memory().unwrap());

    // What the stopped pass left: the container, and its first entry admitted.
    let container = db
        .insert_file(
            "drops",
            "drop.zip",
            &hie::formats::sha256_hex(&zip),
            zip.len() as u64,
            "unpacking",
            None,
            None,
        )
        .unwrap();
    let patients = b"{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n{\"resourceType\":\"Patient\",\"id\":\"p2\"}\n";
    let entry = db
        .open_load(
            "drops",
            "drop.zip!/out/Patient.ndjson",
            &hie::formats::sha256_hex(patients),
            patients.len() as u64,
            None,
            None,
        )
        .unwrap();
    db.insert_records(
        entry,
        "drops",
        &[
            (1, json!({"resourceType": "Patient", "id": "p1"}), None),
            (2, json!({"resourceType": "Patient", "id": "p2"}), None),
        ],
    )
    .unwrap();

    // And the second entry's load opened, its rows never in (their insert
    // rolled back): not admitted, so neither finished nor skipped.
    let observations = b"{\"resourceType\":\"Observation\",\"id\":\"o1\",\"subject\":{\"reference\":\"Patient/p1\"}}\n";
    let unadmitted = db
        .open_load(
            "drops",
            "drop.zip!/out/Observation.ndjson",
            &hie::formats::sha256_hex(observations),
            observations.len() as u64,
            None,
            None,
        )
        .unwrap();
    assert!(
        db.finish_files("drops")
            .unwrap()
            .iter()
            .all(|f| f.id != unadmitted),
        "a load whose rows never went in is not finished"
    );
    let runner = FeedRunner::with_source(
        feed,
        source,
        db.clone(),
        Arc::new(FhirClient::new(&hfs)),
        Arc::new(Alerter::new(db.clone(), None)),
        python,
        &repo.join("hie_runtime"),
        &hfs,
        "http://127.0.0.1:0",
    );
    runner.tick().await.unwrap();
    let admitted = db
        .query_json(
            "SELECT COUNT(*) AS n FROM records WHERE file_id = $1",
            hie::db::params![unadmitted],
        )
        .unwrap();
    assert_eq!(
        admitted[0]["n"], 1,
        "the unadmitted entry is admitted into its load"
    );

    let files = db
        .query_json("SELECT id, name, status FROM files ORDER BY id", [])
        .unwrap();
    assert!(
        files.iter().all(|f| f["status"] != "duplicate"),
        "nothing is a duplicate of the zip itself: {files:?}"
    );
    let named = |n: &str| files.iter().filter(|f| f["name"] == n).count();
    assert_eq!(named("drop.zip"), 1);
    assert_eq!(named("drop.zip!/out/Patient.ndjson"), 1, "{files:?}");
    assert_eq!(named("drop.zip!/out/Observation.ndjson"), 1, "{files:?}");
    assert_ne!(files[0]["status"], "unpacking");
    assert_eq!(files[0]["id"], container);
    let counts = db.feed_counts("drops").unwrap();
    assert_eq!(counts["sent"], 3, "{counts}");
    assert!(
        store.head(&ObjectPath::from("in/drop.zip")).await.is_err(),
        "the zip is archived once every entry is in"
    );
}
