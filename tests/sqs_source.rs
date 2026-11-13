//! An object source with a notification queue: the bucket is listed once at
//! start, then each pass takes only what the queue announced, acknowledging the
//! messages after the objects are in the store. The queue is a stand-in that
//! speaks SQS's JSON protocol and checks the SigV4 signature headers.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use hie::alerts::Alerter;
use hie::db::Db;
use hie::fhir::FhirClient;
use hie::pipeline::FeedRunner;
use hie::source::{ObjectSource, Source};
use hie::sqs::SqsQueue;
use object_store::aws::AwsCredential;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload, StaticCredentialProvider};
use serde_json::{Value, json};

#[derive(Default)]
struct Queue {
    /// Messages waiting: (receipt, body).
    waiting: Vec<(String, String)>,
    /// Messages handed out and not yet deleted.
    in_flight: Vec<(String, String)>,
    deleted: Vec<String>,
    unsigned_calls: u32,
}

type Shared = Arc<Mutex<Queue>>;

async fn sqs(State(q): State<Shared>, headers: HeaderMap, body: String) -> Json<Value> {
    // SQS speaks `application/x-amz-json-1.0`, which the JSON extractor refuses.
    let body: Value = serde_json::from_str(&body).unwrap();
    let mut q = q.lock().unwrap();
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if !(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIATEST/")
        && auth.contains("/us-east-1/sqs/aws4_request")
        && headers.contains_key("x-amz-date"))
    {
        q.unsigned_calls += 1;
    }
    assert!(body["QueueUrl"].as_str().unwrap().contains("/queue/drops"));
    match headers.get("x-amz-target").unwrap().to_str().unwrap() {
        "AmazonSQS.ReceiveMessage" => {
            let max = body["MaxNumberOfMessages"].as_u64().unwrap() as usize;
            let take = q.waiting.len().min(max);
            let batch: Vec<(String, String)> = q.waiting.drain(..take).collect();
            q.in_flight.extend(batch.iter().cloned());
            let messages: Vec<Value> = batch
                .iter()
                .map(|(r, b)| json!({"MessageId": r, "ReceiptHandle": r, "Body": b}))
                .collect();
            Json(if messages.is_empty() {
                json!({})
            } else {
                json!({"Messages": messages})
            })
        }
        "AmazonSQS.DeleteMessageBatch" => {
            let mut ok = Vec::new();
            for e in body["Entries"].as_array().unwrap() {
                let receipt = e["ReceiptHandle"].as_str().unwrap().to_string();
                q.in_flight.retain(|(r, _)| *r != receipt);
                q.deleted.push(receipt);
                ok.push(json!({"Id": e["Id"]}));
            }
            Json(json!({"Successful": ok, "Failed": []}))
        }
        other => panic!("unexpected target {other}"),
    }
}

fn event(key: &str, size: u64) -> String {
    json!({"Records": [{"eventName": "ObjectCreated:Put", "s3": {"object": {"key": key, "size": size}}}]})
        .to_string()
}

async fn batch(
    State(urls): State<Arc<Mutex<Vec<String>>>>,
    Json(bundle): Json<Value>,
) -> Json<Value> {
    let mut entries = Vec::new();
    for e in bundle["entry"].as_array().unwrap() {
        urls.lock()
            .unwrap()
            .push(e["request"]["url"].as_str().unwrap().to_string());
        entries.push(json!({"response": {"status": "201 Created"}}));
    }
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
}

async fn serve(app: Router) -> String {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hie=warn")
        .with_test_writer()
        .try_init();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
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

async fn put(store: &InMemory, key: &str, id: &str) {
    store
        .put(
            &ObjectPath::from(key),
            PutPayload::from(format!(
                "{{\"resourceType\":\"Patient\",\"id\":\"{id}\"}}\n"
            )),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn a_queue_announces_objects_between_listings() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found");
        return;
    };
    let queue: Shared = Arc::default();
    let sqs_base = serve(
        Router::new()
            .route("/queue/drops", post(sqs))
            .with_state(queue.clone()),
    )
    .await;
    let urls: Arc<Mutex<Vec<String>>> = Arc::default();
    let fhir = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(urls.clone()),
    )
    .await;

    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
        "id: export\nname: Export\nsource:\n  type: s3\n  bucket: unused\nformat: ndjson\ntransform: {}\n",
        repo.join("transforms/fhir_passthrough.py").display().to_string().replace('\\', "/")
    ))
    .unwrap();

    let store = Arc::new(InMemory::new());
    put(&store, "drops/a.ndjson", "a").await;
    put(&store, "drops/b.ndjson", "b").await;
    let credentials = Arc::new(StaticCredentialProvider::new(AwsCredential {
        key_id: "AKIATEST".into(),
        secret_key: "secret".into(),
        token: None,
    }));
    let source = Source::Object(
        ObjectSource::new(
            store.clone(),
            "drops",
            "drops/processed",
            "*.ndjson",
            Duration::ZERO,
        )
        .with_queue(SqsQueue::new(
            &format!("{sqs_base}/queue/drops"),
            "us-east-1",
            credentials,
        ))
        .relist_every(Duration::from_secs(3600)),
    );
    let db = Arc::new(Db::in_memory().unwrap());
    let runner = FeedRunner::with_source(
        feed,
        source,
        db.clone(),
        Arc::new(FhirClient::new(&fhir)),
        Arc::new(Alerter::new(db.clone(), None)),
        python,
        &repo.join("hie_runtime"),
        &fhir,
        "http://127.0.0.1:0",
    );

    // First pass: the bucket is listed in full, the queue is not asked.
    runner.tick().await.unwrap();
    assert_eq!(*urls.lock().unwrap(), ["Patient/a", "Patient/b"]);
    assert!(queue.lock().unwrap().deleted.is_empty());

    // Then only what the queue announces is taken: c (announced), not d (silent);
    // a test event, an announcement outside the prefix and one for an object
    // already gone are consumed without effect.
    put(&store, "drops/c.ndjson", "c").await;
    put(&store, "drops/d.ndjson", "d").await;
    put(&store, "elsewhere/e.ndjson", "e").await;
    {
        let mut q = queue.lock().unwrap();
        q.waiting.push(("m-c".into(), event("drops/c.ndjson", 40)));
        q.waiting.push((
            "m-test".into(),
            r#"{"Service":"Amazon S3","Event":"s3:TestEvent"}"#.into(),
        ));
        q.waiting
            .push(("m-e".into(), event("elsewhere/e.ndjson", 40)));
        q.waiting
            .push(("m-gone".into(), event("drops/gone.ndjson", 40)));
    }
    runner.tick().await.unwrap();
    assert_eq!(
        *urls.lock().unwrap(),
        ["Patient/a", "Patient/b", "Patient/c"]
    );
    {
        let q = queue.lock().unwrap();
        let mut deleted = q.deleted.clone();
        deleted.sort();
        assert_eq!(deleted, ["m-c", "m-e", "m-gone", "m-test"]);
        assert!(q.in_flight.is_empty());
        assert_eq!(q.unsigned_calls, 0, "every call to the queue is signed");
    }
    assert!(
        store
            .head(&ObjectPath::from("drops/processed/3-c.ndjson"))
            .await
            .is_ok()
    );
    assert!(
        store
            .head(&ObjectPath::from("drops/d.ndjson"))
            .await
            .is_ok()
    );

    // A quiet queue: nothing is listed, nothing moves.
    runner.tick().await.unwrap();
    assert_eq!(urls.lock().unwrap().len(), 3);
    assert_eq!(db.feed_counts("export").unwrap()["files"], 3);
}

/// A notice for a key that is not an object path (an empty segment) is consumed
/// without effect: the valid objects of the pass are still taken.
#[tokio::test]
async fn an_announced_key_that_is_not_an_object_path_is_skipped() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found");
        return;
    };
    let queue: Shared = Arc::default();
    let sqs_base = serve(
        Router::new()
            .route("/queue/drops", post(sqs))
            .with_state(queue.clone()),
    )
    .await;
    let urls: Arc<Mutex<Vec<String>>> = Arc::default();
    let fhir = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(urls.clone()),
    )
    .await;

    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
        "id: export\nname: Export\nsource:\n  type: s3\n  bucket: unused\nformat: ndjson\ntransform: {}\n",
        repo.join("transforms/fhir_passthrough.py").display().to_string().replace('\\', "/")
    ))
    .unwrap();
    let store = Arc::new(InMemory::new());
    let credentials = Arc::new(StaticCredentialProvider::new(AwsCredential {
        key_id: "AKIATEST".into(),
        secret_key: "secret".into(),
        token: None,
    }));
    let source = Source::Object(
        ObjectSource::new(
            store.clone(),
            "drops",
            "drops/processed",
            "*.ndjson",
            Duration::ZERO,
        )
        .with_queue(SqsQueue::new(
            &format!("{sqs_base}/queue/drops"),
            "us-east-1",
            credentials,
        ))
        .relist_every(Duration::from_secs(3600)),
    );
    let db = Arc::new(Db::in_memory().unwrap());
    let runner = FeedRunner::with_source(
        feed,
        source,
        db.clone(),
        Arc::new(FhirClient::new(&fhir)),
        Arc::new(Alerter::new(db.clone(), None)),
        python,
        &repo.join("hie_runtime"),
        &fhir,
        "http://127.0.0.1:0",
    );
    // The first pass lists the (empty) bucket; the next ones read the queue.
    runner.tick().await.unwrap();

    put(&store, "drops/c.ndjson", "c").await;
    {
        let mut q = queue.lock().unwrap();
        q.waiting
            .push(("m-bad".into(), event("drops//b.ndjson", 40)));
        q.waiting.push(("m-c".into(), event("drops/c.ndjson", 40)));
    }
    runner.tick().await.unwrap();
    assert_eq!(*urls.lock().unwrap(), ["Patient/c"]);
    let q = queue.lock().unwrap();
    let mut deleted = q.deleted.clone();
    deleted.sort();
    assert_eq!(deleted, ["m-bad", "m-c"]);
    assert!(q.in_flight.is_empty());
}
