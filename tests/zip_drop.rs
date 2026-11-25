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

/// What the FHIR server was sent: each entry's URL and resource.
type Urls = Arc<Mutex<Vec<(String, Value)>>>;

async fn batch(State(urls): State<Urls>, Json(bundle): Json<Value>) -> Json<Value> {
    let mut entries = Vec::new();
    for e in bundle["entry"].as_array().unwrap() {
        urls.lock().unwrap().push((
            e["request"]["url"].as_str().unwrap().to_string(),
            e["resource"].clone(),
        ));
        entries.push(json!({"response": {"status": "201 Created"}}));
    }
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
}

fn python() -> Option<&'static str> {
    ["py", "python3", "python"].into_iter().find(|p| {
        std::process::Command::new(p)
            .arg("-c")
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

fn sent_urls(urls: &Urls) -> Vec<String> {
    let mut sent: Vec<String> = urls
        .lock()
        .unwrap()
        .iter()
        .map(|(u, _)| u.clone())
        .collect();
    sent.sort();
    sent
}

/// A zip of these entries, in order.
fn zip_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut buf = std::io::Cursor::new(Vec::new());
    {
        let mut zip = zip::ZipWriter::new(&mut buf);
        let options = zip::write::SimpleFileOptions::default();
        for (name, data) in entries {
            zip.start_file(*name, options).unwrap();
            zip.write_all(data).unwrap();
        }
        zip.finish().unwrap();
    }
    buf.into_inner()
}

/// A folder source of format `fhir` reading `inbox`, with every file it holds.
async fn fhir_drops(inbox: &std::path::Path) -> Option<(FeedRunner, Arc<Db>, Urls)> {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found");
        return None;
    };
    let urls: Urls = Arc::default();
    let app = Router::new()
        .route("/", post(batch))
        .with_state(urls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hfs = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(
        "id: export\nname: Export\nsource:\n  type: folder\n  path: inbox\n  min_file_age_ms: 0\nformat: fhir\ntransform: transforms/fhir_passthrough.py\n",
    )
    .unwrap();
    feed.resolve(&repo).unwrap();
    if let hie::config::SourceConfig::Folder { path, .. } = &mut feed.source {
        *path = inbox.to_path_buf();
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
    Some((runner, db, urls))
}

fn loads(db: &Db) -> Vec<(String, String)> {
    db.query_json("SELECT name, status FROM files ORDER BY id", [])
        .unwrap()
        .iter()
        .map(|f| {
            (
                f["name"].as_str().unwrap().to_string(),
                f["status"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn alert_kinds(db: &Db) -> Vec<String> {
    db.query_json("SELECT kind FROM alerts ORDER BY id", [])
        .unwrap()
        .iter()
        .map(|a| a["kind"].as_str().unwrap().to_string())
        .collect()
}

const MANIFEST: &[u8] =
    b"{\"transactionTime\": \"2026-10-22T00:00:00Z\", \"request\": \"$export\", \"output\": []}\n";

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
        eprintln!("skipping: no Python 3.9+ found");
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
    assert_eq!(
        sent_urls(&urls),
        ["Observation/o1", "Patient/p1", "Patient/p2"]
    );
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
        eprintln!("skipping: no Python 3.9+ found");
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

#[tokio::test]
async fn a_fhir_zip_reads_ndjson_and_bundle_entries_by_their_extension() {
    let dir = tempfile::tempdir().unwrap();
    let inbox = dir.path().join("inbox");
    std::fs::create_dir_all(&inbox).unwrap();
    let bundle = json!({
        "resourceType": "Bundle", "type": "transaction",
        "entry": [
            {"fullUrl": "urn:uuid:p3", "resource": {"resourceType": "Patient", "id": "p3"}},
            {"fullUrl": "urn:uuid:o2", "resource": {"resourceType": "Observation", "id": "o2",
                "subject": {"reference": "urn:uuid:p3"}}},
        ]
    })
    .to_string();
    std::fs::write(
        inbox.join("drop.zip"),
        zip_of(&[
            (
                "Patient.ndjson",
                b"{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n{\"resourceType\":\"Patient\",\"id\":\"p2\"}\n",
            ),
            (
                "Observation.jsonl",
                b"{\"resourceType\":\"Observation\",\"id\":\"o1\"}\n",
            ),
            ("bundle.json", bundle.as_bytes()),
            ("manifest.json", MANIFEST),
            ("README.txt", b"An export.\n"),
        ]),
    )
    .unwrap();
    let Some((runner, db, urls)) = fhir_drops(&inbox).await else {
        return;
    };
    runner.tick().await.unwrap();

    let done = |name: &str| (name.to_string(), "done".to_string());
    assert_eq!(
        loads(&db),
        [
            done("drop.zip"),
            done("drop.zip!/Patient.ndjson"),
            done("drop.zip!/Observation.jsonl"),
            done("drop.zip!/bundle.json"),
        ]
    );
    assert_eq!(
        sent_urls(&urls),
        [
            "Observation/o1",
            "Observation/o2",
            "Patient/p1",
            "Patient/p2",
            "Patient/p3"
        ]
    );
    let o2 = urls
        .lock()
        .unwrap()
        .iter()
        .find(|(u, _)| u == "Observation/o2")
        .map(|(_, r)| r.clone())
        .unwrap();
    assert_eq!(o2["subject"]["reference"], "Patient/p3");
    assert!(alert_kinds(&db).is_empty());
    assert!(!inbox.join("drop.zip").exists());
    assert!(inbox.join("processed/1-drop.zip").exists());

    // A `.json` entry that is neither a resource nor a manifest cannot be read.
    std::fs::write(
        inbox.join("second.zip"),
        zip_of(&[("bundle.json", br#"{"type":"transaction","entry":[]}"#)]),
    )
    .unwrap();
    runner.tick().await.unwrap();
    let unreadable = db
        .query_json(
            "SELECT status, error FROM files WHERE name = 'second.zip!/bundle.json'",
            [],
        )
        .unwrap();
    assert_eq!(unreadable[0]["status"], "unreadable", "{unreadable:?}");
    assert_eq!(unreadable[0]["error"], "JSON has no resourceType");
    assert_eq!(alert_kinds(&db), ["unreadable_file"]);
}

#[tokio::test]
async fn loose_fhir_files_are_read_by_their_extension_and_a_foreign_one_is_unreadable() {
    let dir = tempfile::tempdir().unwrap();
    let inbox = dir.path().join("inbox");
    std::fs::create_dir_all(&inbox).unwrap();
    std::fs::write(
        inbox.join("a.ndjson"),
        "{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n",
    )
    .unwrap();
    std::fs::write(
        inbox.join("b.json"),
        "{\"resourceType\":\"Patient\",\"id\":\"p2\"}",
    )
    .unwrap();
    std::fs::write(inbox.join("manifest.json"), MANIFEST).unwrap();
    std::fs::write(inbox.join("notes.txt"), "Delivered by the lab.\n").unwrap();
    let Some((runner, db, urls)) = fhir_drops(&inbox).await else {
        return;
    };
    runner.tick().await.unwrap();

    assert_eq!(sent_urls(&urls), ["Patient/p1", "Patient/p2"]);
    let files = db
        .query_json(
            "SELECT name, status, error FROM files WHERE name IN ('manifest.json', 'notes.txt') ORDER BY name",
            [],
        )
        .unwrap();
    // The manifest is a load with nothing in it; the foreign file is refused.
    assert_eq!(files[0]["status"], "done", "{files:?}");
    assert!(files[0]["error"].is_null());
    assert_eq!(files[1]["status"], "unreadable");
    assert_eq!(
        files[1]["error"],
        "not an NDJSON (.ndjson, .jsonl) or JSON (.json) file"
    );
    assert_eq!(alert_kinds(&db), ["unreadable_file"]);
    let mut left: Vec<String> = inbox
        .read_dir()
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    left.sort();
    assert_eq!(left, ["processed"]);
    let processed: Vec<String> = inbox
        .join("processed")
        .read_dir()
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        processed.iter().any(|n| n.ends_with("-notes.txt")),
        "{processed:?}"
    );
}

#[tokio::test]
async fn a_fhir_file_sent_again_under_a_name_read_another_way_is_read_again() {
    let dir = tempfile::tempdir().unwrap();
    let inbox = dir.path().join("inbox");
    std::fs::create_dir_all(&inbox).unwrap();
    let lines = "{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n";
    std::fs::write(inbox.join("x.txt"), lines).unwrap();
    let Some((runner, db, urls)) = fhir_drops(&inbox).await else {
        return;
    };
    runner.tick().await.unwrap();
    assert!(sent_urls(&urls).is_empty());

    // Renamed to an extension that reads it: not a duplicate of the unreadable one.
    std::fs::write(inbox.join("x.ndjson"), lines).unwrap();
    runner.tick().await.unwrap();
    assert_eq!(sent_urls(&urls), ["Patient/p1"]);

    // Under another name read the same way, the same bytes are a duplicate.
    std::fs::write(inbox.join("y.jsonl"), lines).unwrap();
    runner.tick().await.unwrap();
    assert_eq!(sent_urls(&urls), ["Patient/p1"]);
    assert_eq!(
        loads(&db),
        [
            ("x.txt".to_string(), "unreadable".to_string()),
            ("x.ndjson".to_string(), "done".to_string()),
            ("y.jsonl".to_string(), "duplicate".to_string()),
        ]
    );
}
