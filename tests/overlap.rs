//! Delivery runs while the transform is still working: with a transform that
//! takes a few milliseconds per record, HFS sees the first batch long before
//! the last record is transformed.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use hie::pipeline::FeedRunner;
use serde_json::{Value, json};

type Seen = Arc<Mutex<Vec<Instant>>>;

async fn batch(State(seen): State<Seen>, Json(bundle): Json<Value>) -> Json<Value> {
    seen.lock().unwrap().push(Instant::now());
    let entries: Vec<Value> = bundle["entry"]
        .as_array()
        .unwrap()
        .iter()
        .map(|_| json!({"response": {"status": "201 Created"}}))
        .collect();
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
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

#[tokio::test]
async fn delivery_starts_before_the_transform_ends() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let seen: Seen = Arc::default();
    let app = Router::new()
        .route("/", post(batch))
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::write(
        root.join("transforms/slow.py"),
        "import time\n\ndef transform(record, context):\n    time.sleep(0.005)\n    return record\n",
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {base}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/export.yaml"),
        "id: export\nname: Export\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.ndjson\"\n  min_file_age_ms: 0\n\
         format: ndjson\ntransform: transforms/slow.py\n",
    )
    .unwrap();
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo.join("hie_runtime"), None, None).unwrap();
    let runner: Arc<FeedRunner> = manager.runner("export").unwrap();
    manager.pause("export", None).unwrap();

    // 600 records at 5 ms each: about three seconds of transform.
    let lines: Vec<String> = (0..600)
        .map(|i| format!("{{\"resourceType\":\"Patient\",\"id\":\"p{i}\"}}"))
        .collect();
    std::fs::create_dir_all(root.join("inbox")).unwrap();
    std::fs::write(root.join("inbox/patients.ndjson"), lines.join("\n")).unwrap();

    runner.tick().await.unwrap();
    let finished = Instant::now();
    assert_eq!(state.db.feed_counts("export").unwrap()["sent"], 600);
    let first = seen.lock().unwrap()[0];
    let lead = finished.duration_since(first);
    assert!(
        lead.as_millis() >= 1000,
        "the first Bundle reached HFS only {lead:?} before the pass ended; delivery waited for the whole transform"
    );
}
