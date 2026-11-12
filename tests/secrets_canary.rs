//! A stored secret key never leaves the engine: with every log at TRACE under
//! the production filter, a canary secret access key is stored, refused in a
//! malformed body, and used by a source, and it appears in no API answer, no
//! log line, no source file and no byte of the SQLite database or its WAL. The
//! engine's key is in no log line either.
#![cfg(feature = "secrets-native")]

use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Where the log goes: a buffer the test reads.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn serve(router: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

fn random_hex(n: usize) -> String {
    let mut b = vec![0u8; n];
    getrandom::fill(&mut b).unwrap();
    hex::encode(b)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_canary_secret_appears_nowhere() {
    let log = Captured::default();
    {
        use tracing_subscriber::layer::{Layer, SubscriberExt};
        use tracing_subscriber::util::SubscriberInitExt;
        let writer = log.clone();
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(move || writer.clone())
                    .with_filter(hie::log_filter(Some("trace"))),
            )
            .init();
    }
    let canary = format!("CANARY-{}", random_hex(16));
    let key = {
        use base64::Engine;
        let mut b = [0u8; 32];
        getrandom::fill(&mut b).unwrap();
        base64::engine::general_purpose::STANDARD.encode(b)
    };

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/fhir_passthrough.py"),
        root.join("transforms/fhir_passthrough.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        "fhir_base_url: http://127.0.0.1:9/fhir\ndatabase: data/hie.db\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (_manager, state) = hie::build_with(
        &config,
        feeds,
        &repo().join("hie_runtime"),
        None,
        None,
        hie::secrets::KeySettings::inline(&key),
    )
    .unwrap();
    let api = serve(hie::api::router(state.clone())).await;
    // A bucket that refuses everything, and records that it was asked.
    let asked = Arc::new(Mutex::new(0u32));
    let bucket = {
        let asked = asked.clone();
        serve(axum::Router::new().fallback(move || {
            let asked = asked.clone();
            async move {
                *asked.lock().unwrap() += 1;
                axum::http::StatusCode::FORBIDDEN
            }
        }))
        .await
    };
    let http = reqwest::Client::new();
    let mut answers = Vec::new();

    // Stored, then refused in a malformed body, then replaced.
    let r = http
        .post(format!("{api}/api/secrets"))
        .json(&json!({"name": "lab", "kind": "aws",
            "value": {"access_key_id": "AKIDCANARY", "secret_access_key": canary}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let text = r.text().await.unwrap();
    let id = serde_json::from_str::<Value>(&text).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    answers.push(text);
    for body in [
        format!(
            r#"{{"name": "bad", "kind": "aws", "value": {{"secret_access_key": "{canary}", "x": 1}}}}"#
        ),
        format!(r#"{{"name": "bad", "kind": "aws", "value": {{"secret_access_key": "{canary}""#),
    ] {
        let r = http
            .post(format!("{api}/api/secrets"))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400);
        answers.push(r.text().await.unwrap());
    }
    let r = http
        .put(format!("{api}/api/secrets/{id}"))
        .json(&json!({"value": {"access_key_id": "AKIDCANARY", "secret_access_key": canary}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    answers.push(r.text().await.unwrap());

    // A source signs with it against a bucket that refuses it.
    let definition = json!({"id": "lab-s3", "name": "Lab",
        "source": {"type": "s3", "bucket": "b", "region": "us-east-1", "endpoint": bucket,
                   "credential": "lab", "poll_seconds": 3600},
        "format": "ndjson", "transform": "transforms/fhir_passthrough.py"});
    let r = http
        .post(format!("{api}/api/sources/test"))
        .json(&json!({"source": definition["source"]}))
        .send()
        .await
        .unwrap();
    answers.push(r.text().await.unwrap());
    let r = http
        .post(format!("{api}/api/sources"))
        .json(&definition)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    answers.push(r.text().await.unwrap());
    let started = Instant::now();
    while *asked.lock().unwrap() < 2 {
        assert!(started.elapsed() < Duration::from_secs(30), "no pass");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    for path in [
        "/api/secrets",
        &format!("/api/secrets/{id}"),
        "/api/sources",
        "/api/sources/lab-s3",
        "/api/alerts",
        "/health",
        "/metrics",
    ] {
        answers.push(
            http.get(format!("{api}{path}"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
        );
    }
    for answer in &answers {
        assert!(
            !answer.contains(&canary),
            "an answer carries the canary: {answer}"
        );
    }

    let logged = String::from_utf8_lossy(&log.0.lock().unwrap()).into_owned();
    assert!(
        logged.contains("TRACE") || logged.contains("DEBUG"),
        "the log is at TRACE"
    );
    assert!(logged.contains("using the credential's current revision"));
    assert!(!logged.contains(&canary), "the log carries the canary");
    assert!(!logged.contains(&key), "the log carries the engine's key");

    for entry in std::fs::read_dir(root.join("sources")).unwrap() {
        let text = std::fs::read(entry.unwrap().path()).unwrap();
        assert!(!contains(&text, canary.as_bytes()));
    }
    if !state.db.is_postgres() {
        for file in ["hie.db", "hie.db-wal"] {
            let path = root.join("data").join(file);
            if let Ok(bytes) = std::fs::read(&path) {
                assert!(
                    !contains(&bytes, canary.as_bytes()),
                    "{file} holds the canary"
                );
                assert!(!contains(&bytes, key.as_bytes()), "{file} holds the key");
            }
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}
