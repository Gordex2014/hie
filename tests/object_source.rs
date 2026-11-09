//! End-to-end over an object store (what the S3 source uses), with the in-memory
//! store standing in for a bucket: an NDJSON export is listed, read, passed through
//! and delivered by resource id, then archived under the bucket's processed prefix.

use std::path::PathBuf;
use std::sync::Arc;
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

type Urls = Arc<std::sync::Mutex<Vec<String>>>;

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
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

#[tokio::test]
async fn an_ndjson_object_is_delivered_by_id_and_archived() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.11+ found");
        return;
    };
    let urls: Urls = Arc::default();
    let app = Router::new()
        .route("/", post(batch))
        .with_state(urls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
        "id: export\nname: Export\nsource:\n  type: s3\n  bucket: unused\nformat: ndjson\ntransform: {}\n",
        repo.join("transforms/fhir_passthrough.py").display().to_string().replace('\\', "/")
    ))
    .unwrap();

    let store = Arc::new(InMemory::new());
    let ndjson = concat!(
        "{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n",
        "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"subject\":{\"reference\":\"Patient/p1\"}}\n",
        "{\"resourceType\":\"Observation\"}\n",
        "{\"resourceType\":\"Bundle\",\"id\":\"b\"}\n",
    );
    store
        .put(
            &ObjectPath::from("exports/Observation.ndjson"),
            PutPayload::from_static(ndjson.as_bytes()),
        )
        .await
        .unwrap();
    let source = Source::Object(ObjectSource::new(
        store.clone(),
        "exports",
        "exports/processed",
        "*.ndjson",
        Duration::ZERO,
    ));

    let db = Arc::new(Db::in_memory().unwrap());
    let runner = FeedRunner::with_source(
        feed,
        source,
        db.clone(),
        Arc::new(FhirClient::new(&base)),
        Arc::new(Alerter::new(db.clone(), None)),
        python,
        &repo.join("hie_runtime"),
        &base,
        "http://127.0.0.1:0",
    );
    runner.tick().await.unwrap();

    let counts = db.feed_counts("export").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["rejected"].as_i64()),
        (Some(2), Some(2))
    );
    assert_eq!(*urls.lock().unwrap(), ["Patient/p1", "Observation/o1"]);

    let rejected = db
        .query_json(
            "SELECT line, error FROM records WHERE status = 'rejected' ORDER BY line",
            [],
        )
        .unwrap();
    assert_eq!(rejected[0]["line"], 3);
    assert!(
        rejected[0]["error"]
            .as_str()
            .unwrap()
            .contains("neither id nor identifier")
    );
    assert_eq!(rejected[1]["line"], 4);
    assert!(
        rejected[1]["error"]
            .as_str()
            .unwrap()
            .contains("nested Bundle")
    );

    assert!(
        store
            .head(&ObjectPath::from("exports/Observation.ndjson"))
            .await
            .is_err()
    );
    assert!(
        store
            .head(&ObjectPath::from("exports/processed/1-Observation.ndjson"))
            .await
            .is_ok()
    );
}
