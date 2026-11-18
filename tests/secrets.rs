//! `/api/secrets` as a client sees it: shapes, errors, the 409 while a source is
//! registered for a credential; and, in a build without a secret store, the
//! 503s and the refused definitions.

use std::path::PathBuf;

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

/// A bucket that refuses every request, so a source's passes end at once.
async fn refusing_bucket() -> String {
    serve(axum::Router::new().fallback(|| async { axum::http::StatusCode::FORBIDDEN })).await
}

/// An engine with no sources and a SQLite store; the API's base URL.
async fn engine() -> (String, hie::api::AppState, tempfile::TempDir) {
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
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let api = serve(hie::api::router(state.clone())).await;
    (api, state, dir)
}

fn s3_source(id: &str, bucket: &str, access: Value) -> Value {
    let mut source = json!({"type": "s3", "bucket": "b", "region": "us-east-1",
        "endpoint": bucket, "poll_seconds": 3600});
    source
        .as_object_mut()
        .unwrap()
        .extend(access.as_object().unwrap().clone());
    json!({"id": id, "name": id, "source": source, "format": "ndjson",
        "transform": "transforms/fhir_passthrough.py"})
}

#[cfg(feature = "secrets-native")]
mod native {
    use super::*;

    fn canary() -> String {
        let mut b = [0u8; 16];
        getrandom::fill(&mut b).unwrap();
        format!("CANARY-{}", hex::encode(b))
    }

    #[tokio::test]
    async fn credentials_are_created_listed_replaced_and_deleted_without_their_values() {
        let (api, _state, _dir) = engine().await;
        let http = reqwest::Client::new();
        let secret = canary();
        let r = http
            .post(format!("{api}/api/secrets"))
            .json(&json!({"name": "lab-bucket", "kind": "aws",
                "value": {"access_key_id": "AKIAIOSFODNN7EXAMPLE", "secret_access_key": secret}}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201);
        let text = r.text().await.unwrap();
        assert!(!text.contains(&secret) && !text.contains("AKIAIOSFODNN7EXAMPLE"));
        let created: Value = serde_json::from_str(&text).unwrap();
        let id = created["id"].as_str().unwrap().to_string();
        assert!(id.starts_with("sec_") && id.len() == 20, "{id}");
        assert_eq!(created["name"], "lab-bucket");
        assert_eq!(created["kind"], "aws");
        assert_eq!(created["hint"], "MPLE");
        assert_eq!(created["revision"], 1);
        assert_eq!(created["used_by"], json!([]));
        assert!(created["created_at"].as_str().unwrap().ends_with('Z'));
        assert!(created.get("value").is_none());

        // A second name, to see the list sorted by name.
        let r = http
            .post(format!("{api}/api/secrets"))
            .json(&json!({"name": "archive", "kind": "aws",
                "value": {"access_key_id": "AKIDARCHIVE", "secret_access_key": canary()}}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201);
        let r = http
            .post(format!("{api}/api/secrets"))
            .json(&json!({"name": "lab-bucket", "kind": "aws",
                "value": {"access_key_id": "AKIDOTHER", "secret_access_key": canary()}}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 409);
        assert_eq!(r.json::<Value>().await.unwrap()["code"], "name_in_use");

        let list: Value = http
            .get(format!("{api}/api/secrets"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let names: Vec<&str> = list["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["archive", "lab-bucket"]);
        let one: Value = http
            .get(format!("{api}/api/secrets/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(one, created);

        let r = http
            .put(format!("{api}/api/secrets/{id}"))
            .json(
                &json!({"value": {"access_key_id": "AKIDROTATED2", "secret_access_key": canary()}}),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let replaced: Value = r.json().await.unwrap();
        assert_eq!(replaced["revision"], 2);
        assert_eq!(replaced["hint"], "TED2");
        assert_eq!(replaced["name"], "lab-bucket");
        assert_eq!(replaced["created_at"], created["created_at"]);

        let unknown = "sec_ffffffffffffffff";
        for r in [
            http.get(format!("{api}/api/secrets/{unknown}"))
                .send()
                .await
                .unwrap(),
            http.put(format!("{api}/api/secrets/{unknown}"))
                .json(&json!({"value": {"access_key_id": "A", "secret_access_key": "b"}}))
                .send()
                .await
                .unwrap(),
            http.delete(format!("{api}/api/secrets/{unknown}"))
                .send()
                .await
                .unwrap(),
        ] {
            assert_eq!(r.status(), 404);
            assert_eq!(r.json::<Value>().await.unwrap()["code"], "secret_not_found");
        }

        let r = http
            .delete(format!("{api}/api/secrets/{id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 204);
        let r = http
            .get(format!("{api}/api/secrets/{id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 404);
    }

    #[tokio::test]
    async fn a_bad_body_is_refused_with_a_fixed_sentence_that_never_echoes_it() {
        let (api, _state, _dir) = engine().await;
        let http = reqwest::Client::new();
        let c = canary();
        let posts = [
            json!({"name": "lab", "kind": "aws"}),
            json!({"name": "lab", "kind": "aws",
                "value": {"access_key_id": "AKID", "secret_access_key": "s", "session_token": c}}),
            json!({"name": "lab", "kind": "gcp",
                "value": {"access_key_id": "AKID", "secret_access_key": c}}),
            json!({"name": "lab", "kind": "aws",
                "value": {"access_key_id": ["AKID"], "secret_access_key": c}}),
            json!({"name": c, "kind": "aws",
                "value": {"access_key_id": 5, "secret_access_key": "s"}}),
            json!({c.clone(): 1}),
            json!({"name": "sec_lab", "kind": "aws",
                "value": {"access_key_id": "AKID", "secret_access_key": c}}),
            json!({"name": " ", "kind": "aws",
                "value": {"access_key_id": "AKID", "secret_access_key": c}}),
            json!({"name": "lab", "kind": "aws",
                "value": {"access_key_id": "AKID", "secret_access_key": format!("{c} with spaces")}}),
        ];
        for body in posts {
            let r = http
                .post(format!("{api}/api/secrets"))
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), 400, "{body}");
            let text = r.text().await.unwrap();
            assert!(!text.contains(&c), "{text}");
            let answer: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(answer["code"], "invalid_body", "{text}");
        }
        let r = http
            .post(format!("{api}/api/secrets"))
            .header("content-type", "application/json")
            .body(format!("{{\"name\": \"{c}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400);
        assert!(!r.text().await.unwrap().contains(&c));

        // A field the value lacks is named.
        let r = http
            .post(format!("{api}/api/secrets"))
            .json(&json!({"name": "lab", "kind": "aws", "value": {"access_key_id": "AKID"}}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            r.json::<Value>().await.unwrap()["field"],
            "secret_access_key"
        );

        // The same rules for a replace.
        let id = http
            .post(format!("{api}/api/secrets"))
            .json(&json!({"name": "lab", "kind": "aws",
                "value": {"access_key_id": "AKID", "secret_access_key": canary()}}))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        let r = http
            .put(format!("{api}/api/secrets/{id}"))
            .json(
                &json!({"value": {"access_key_id": "AKID", "secret_access_key": "s",
                "session_token": c}}),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400);
        assert!(!r.text().await.unwrap().contains(&c));
        let r = http
            .put(format!("{api}/api/secrets/{id}"))
            .json(&json!({"name": "renamed", "value": {"access_key_id": "AKID", "secret_access_key": "s"}}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "a name cannot change");
    }

    #[tokio::test]
    async fn a_credential_a_source_uses_cannot_be_deleted() {
        let (api, _state, _dir) = engine().await;
        let bucket = refusing_bucket().await;
        let http = reqwest::Client::new();
        let id = http
            .post(format!("{api}/api/secrets"))
            .json(&json!({"name": "lab", "kind": "aws",
                "value": {"access_key_id": "AKIDLAB", "secret_access_key": canary()}}))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        // A definition naming a credential that does not exist is refused.
        let r = http
            .post(format!("{api}/api/sources"))
            .json(&s3_source(
                "drops",
                &bucket,
                json!({"credential": "missing"}),
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400);
        let body: Value = r.json().await.unwrap();
        assert_eq!(body["code"], "invalid_definition");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("no credential named missing")
        );

        let r = http
            .post(format!("{api}/api/sources"))
            .json(&s3_source("drops", &bucket, json!({"credential": "lab"})))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201, "{}", r.text().await.unwrap());
        let used: Value = http
            .get(format!("{api}/api/secrets/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(used["used_by"], json!(["drops"]));
        let r = http
            .delete(format!("{api}/api/secrets/{id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 409);
        let body: Value = r.json().await.unwrap();
        assert_eq!(body["code"], "secret_in_use");
        assert_eq!(body["sources"], json!(["drops"]));
        assert_eq!(body["error"], "sources use this credential: drops");

        // Once the source is deleted and its runner has ended, it can go.
        let r = http
            .delete(format!("{api}/api/sources/drops"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 204);
        let started = std::time::Instant::now();
        loop {
            let r = http
                .delete(format!("{api}/api/secrets/{id}"))
                .send()
                .await
                .unwrap();
            if r.status() == 204 {
                break;
            }
            assert_eq!(r.status(), 409);
            assert!(
                started.elapsed() < std::time::Duration::from_secs(30),
                "still registered after its source was deleted"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn a_busy_store_answers_503_with_retry_after_and_no_detail() {
        let (api, state, dir) = engine().await;
        if state.db.is_postgres() {
            eprintln!("SQLite only: the lock is a SQLite write lock");
            return;
        }
        let http = reqwest::Client::new();
        state
            .db
            .set_busy_timeout(std::time::Duration::from_millis(50));
        let blocker = rusqlite::Connection::open(dir.path().join("data/hie.db")).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let r = http
            .post(format!("{api}/api/secrets"))
            .json(&json!({"name": "lab", "kind": "aws",
                "value": {"access_key_id": "AKID", "secret_access_key": canary()}}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 503);
        assert_eq!(r.headers()["retry-after"], "1");
        let body: Value = r.json().await.unwrap();
        assert_eq!(
            body,
            json!({"error": "the store is busy; try again shortly", "code": "store_busy"})
        );
        blocker.execute_batch("ROLLBACK").unwrap();
    }
}

#[cfg(not(feature = "secrets-native"))]
mod no_backend {
    use super::*;

    #[tokio::test]
    async fn every_secret_route_answers_503_and_credential_sources_are_refused() {
        let (api, _state, _dir) = engine().await;
        let http = reqwest::Client::new();
        let id = "sec_0123456789abcdef";
        let body = json!({"name": "lab", "kind": "aws",
            "value": {"access_key_id": "AKID", "secret_access_key": "s"}});
        for r in [
            http.get(format!("{api}/api/secrets")).send().await.unwrap(),
            http.post(format!("{api}/api/secrets"))
                .json(&body)
                .send()
                .await
                .unwrap(),
            http.get(format!("{api}/api/secrets/{id}"))
                .send()
                .await
                .unwrap(),
            http.put(format!("{api}/api/secrets/{id}"))
                .json(&body)
                .send()
                .await
                .unwrap(),
            http.delete(format!("{api}/api/secrets/{id}"))
                .send()
                .await
                .unwrap(),
        ] {
            assert_eq!(r.status(), 503);
            let answer: Value = r.json().await.unwrap();
            assert_eq!(answer["code"], "secrets_unavailable");
            assert!(
                answer["error"]
                    .as_str()
                    .unwrap()
                    .contains("no secret backend")
            );
        }

        let bucket = refusing_bucket().await;
        let r = http
            .post(format!("{api}/api/sources"))
            .json(&s3_source("drops", &bucket, json!({"credential": "lab"})))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400);
        let answer: Value = r.json().await.unwrap();
        assert_eq!(answer["code"], "invalid_definition");
        assert!(
            answer["error"]
                .as_str()
                .unwrap()
                .contains("needs a secret store")
        );

        let r = http
            .post(format!("{api}/api/sources"))
            .json(&s3_source("open", &bucket, json!({"anonymous": true})))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201);

        let health: Value = http
            .get(format!("{api}/health"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            health["secrets"],
            json!({"backend": "none", "available": false, "key_id": null})
        );
    }
}
