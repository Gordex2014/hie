//! Two engine instances on one store, each with its own sources directory: a
//! credential either instance's source uses cannot be deleted through either.
#![cfg(feature = "secrets-native")]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

async fn serve(router: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

/// An instance rooted at `root` on the store `database` (the same path string
/// for every instance: one SQLite file, or one PostgreSQL test schema).
async fn instance(root: &Path, database: &Path, key: &str) -> (String, hie::api::AppState) {
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/fhir_passthrough.py"),
        root.join("transforms/fhir_passthrough.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!(
            "fhir_base_url: http://127.0.0.1:9/fhir\ndatabase: {}\n",
            database.display()
        ),
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    assert_eq!(config.database, database);
    let (_manager, state) = hie::build_with(
        &config,
        feeds,
        &repo().join("hie_runtime"),
        None,
        None,
        hie::secrets::KeySettings::inline(key),
    )
    .unwrap();
    (serve(hie::api::router(state.clone())).await, state)
}

fn source(bucket: &str) -> Value {
    json!({"id": "s", "name": "S",
        "source": {"type": "s3", "bucket": "b", "region": "us-east-1", "endpoint": bucket,
                   "credential": "x", "poll_seconds": 3600},
        "format": "ndjson", "transform": "transforms/fhir_passthrough.py"})
}

async fn delete(api: &str, id: &str) -> (u16, Value) {
    let r = reqwest::Client::new()
        .delete(format!("{api}/api/secrets/{id}"))
        .send()
        .await
        .unwrap();
    let status = r.status().as_u16();
    (status, r.json().await.unwrap_or(Value::Null))
}

async fn eventually(what: &str, mut done: impl AsyncFnMut() -> bool) {
    let started = Instant::now();
    while !done().await {
        assert!(started.elapsed() < Duration::from_secs(30), "{what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn a_credential_used_on_either_instance_cannot_be_deleted_on_either() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("shared").join("hie.db");
    // Kept for the test's length: under PostgreSQL it owns the test schema.
    let _keeper = hie::db::Db::connect(&database.to_string_lossy()).unwrap();
    let key = {
        use base64::Engine;
        let mut b = [0u8; 32];
        getrandom::fill(&mut b).unwrap();
        base64::engine::general_purpose::STANDARD.encode(b)
    };
    let (a, a_state) = instance(&dir.path().join("a"), &database, &key).await;
    let (b, b_state) = instance(&dir.path().join("b"), &database, &key).await;
    assert_ne!(a_state.secrets.owner(), b_state.secrets.owner());
    let bucket =
        serve(axum::Router::new().fallback(|| async { axum::http::StatusCode::FORBIDDEN })).await;
    let http = reqwest::Client::new();

    // One store: what A writes, B reads.
    let x = http
        .post(format!("{a}/api/secrets"))
        .json(&json!({"name": "x", "kind": "aws",
            "value": {"access_key_id": "AKIDSHARED", "secret_access_key": "generated"}}))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let listed: Value = http
        .get(format!("{b}/api/secrets"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["secrets"][0]["id"], x.as_str());

    // A's source uses x: B cannot delete it.
    let r = http
        .post(format!("{a}/api/sources"))
        .json(&source(&bucket))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let (status, body) = delete(&b, &x).await;
    assert_eq!(status, 409);
    assert_eq!(body["code"], "secret_in_use");
    assert_eq!(body["sources"], json!(["s"]));

    // B defines its own source s with x: one source id, listed once.
    let r = http
        .post(format!("{b}/api/sources"))
        .json(&source(&bucket))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let meta: Value = http
        .get(format!("{a}/api/secrets/{x}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(meta["used_by"], json!(["s"]));

    // A deletes its source: its registration goes, B's stays.
    let r = http
        .delete(format!("{a}/api/sources/s"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let owner = a_state.secrets.owner().to_string();
    eventually("a's registration released", async || {
        a_state.db.secret_holds_of(&owner).unwrap().is_empty()
    })
    .await;
    let (status, body) = delete(&a, &x).await;
    assert_eq!(status, 409);
    assert_eq!(body["sources"], json!(["s"]));

    // B deletes its source: now x can go.
    let r = http
        .delete(format!("{b}/api/sources/s"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    eventually("x deleted once no source uses it", async || {
        delete(&a, &x).await.0 == 204
    })
    .await;
}
