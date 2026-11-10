//! Two engine instances on one store: a source is run by whoever holds its lease,
//! the other stands by, and a lease that lapses is taken over.

use std::path::PathBuf;
use std::sync::Arc;

use axum::routing::post;
use axum::{Json, Router};
use hie::alerts::Alerter;
use hie::db::Db;
use hie::fhir::FhirClient;
use hie::pipeline::FeedRunner;
use hie::source::Source;
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

fn python() -> Option<&'static str> {
    ["py", "python3", "python"].into_iter().find(|p| {
        std::process::Command::new(p)
            .arg("-c")
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

#[tokio::test]
async fn one_instance_runs_a_source_while_the_other_stands_by() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found");
        return;
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hfs = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", post(batch)))
            .await
            .unwrap()
    });
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let dir = tempfile::tempdir().unwrap();
    let inbox = dir.path().join("inbox");
    std::fs::create_dir_all(&inbox).unwrap();
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(
        "id: lab\nname: Lab\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  min_file_age_ms: 0\ntransform: transforms/lab_results.py\n",
    )
    .unwrap();
    feed.resolve(&repo).unwrap();
    if let hie::config::SourceConfig::Folder { path, .. } = &mut feed.source {
        *path = inbox.clone();
    }

    // Two instances share one store (one SQLite file here; PostgreSQL in production).
    let path = dir.path().join("hie.db");
    let db_a = Arc::new(Db::open(&path).unwrap());
    let db_b = Arc::new(Db::open(&path).unwrap());
    let runner = |db: &Arc<Db>, id: &str| {
        FeedRunner::with_source(
            feed.clone(),
            Source::from_config(&feed).unwrap(),
            db.clone(),
            Arc::new(FhirClient::new(&hfs)),
            Arc::new(Alerter::new(db.clone(), None)),
            python,
            &repo.join("hie_runtime"),
            &hfs,
            "http://127.0.0.1:0",
        )
        .as_instance(id)
    };
    let a = runner(&db_a, "a");
    let b = runner(&db_b, "b");

    std::fs::copy(repo.join("examples/data/labs.csv"), inbox.join("labs.csv")).unwrap();
    // a takes the lease and runs; b finds it held and does nothing.
    assert!(a.pass().await.unwrap());
    assert!(!b.pass().await.unwrap());
    assert_eq!(db_a.feed_counts("lab").unwrap()["sent"], 3);
    let lease = db_b.lease("lab").unwrap().unwrap();
    assert_eq!(lease.owner, "a");

    // a keeps renewing; b keeps standing by.
    assert!(a.pass().await.unwrap());
    assert!(!b.pass().await.unwrap());

    // a goes away without releasing (a crash): once its lease lapses, b takes over
    // and processes what arrived meanwhile.
    db_a.acquire_lease("lab", "a", -1).unwrap();
    std::fs::copy(
        repo.join("examples/data/messy.csv"),
        inbox.join("messy.csv"),
    )
    .unwrap();
    assert!(b.pass().await.unwrap());
    assert_eq!(db_b.lease("lab").unwrap().unwrap().owner, "b");
    let counts = db_b.feed_counts("lab").unwrap();
    assert_eq!(
        (counts["files"].as_i64(), counts["sent"].as_i64()),
        (Some(2), Some(4))
    );
    // And a, back, now stands by.
    assert!(!a.pass().await.unwrap());

    // A clean release hands the source over at once.
    db_b.release_lease("lab", "b").unwrap();
    assert!(a.pass().await.unwrap());
}
