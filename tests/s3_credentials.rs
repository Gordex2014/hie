//! How an S3 source reaches its bucket, against a stand-in bucket that records
//! the access key id of every request and refuses revoked ones: a definition
//! must say how; an anonymous source is never signed; a stored credential signs
//! the Test, the source, its queue, and its next revision once replaced.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

/// One request the stand-in saw: its path, the access key id it was signed
/// with (`None` when unsigned), and the status it got.
#[derive(Clone, Debug)]
struct Seen {
    path: String,
    key_id: Option<String>,
    status: u16,
}

/// An object's key and body.
type Object = (String, Vec<u8>);

/// A path-style bucket named `b`: lists (ListObjectsV2) and serves its objects,
/// refusing requests signed with a revoked key id.
#[derive(Clone, Default)]
struct Bucket {
    objects: Arc<Mutex<Vec<Object>>>,
    revoked: Arc<Mutex<HashSet<String>>>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

fn key_id(headers: &HeaderMap, uri: &Uri) -> Option<String> {
    if let Some(auth) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        return auth
            .split("Credential=")
            .nth(1)
            .and_then(|c| c.split('/').next())
            .map(str::to_string);
    }
    url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .find(|(k, _)| k == "X-Amz-Credential")
        .and_then(|(_, v)| v.split('/').next().map(str::to_string))
}

impl Bucket {
    async fn serve(&self) -> String {
        serve(Router::new().fallback(bucket).with_state(self.clone())).await
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// Listings that succeeded with `key` (or unsigned, for `None`).
    fn listings(&self, key: Option<&str>) -> usize {
        self.seen()
            .iter()
            .filter(|s| s.path == "/b" && s.status == 200 && s.key_id.as_deref() == key)
            .count()
    }
}

async fn bucket(State(b): State<Bucket>, method: Method, uri: Uri, headers: HeaderMap) -> Response {
    let key = key_id(&headers, &uri);
    let status = if key
        .as_ref()
        .is_some_and(|k| b.revoked.lock().unwrap().contains(k))
    {
        StatusCode::FORBIDDEN
    } else if method != Method::GET {
        StatusCode::METHOD_NOT_ALLOWED
    } else {
        StatusCode::OK
    };
    let path = uri.path().trim_end_matches('/').to_string();
    b.seen.lock().unwrap().push(Seen {
        path: path.clone(),
        key_id: key,
        status: status.as_u16(),
    });
    if status != StatusCode::OK {
        return status.into_response();
    }
    if path == "/b" {
        let mut xml = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated>"#,
        );
        for (k, body) in b.objects.lock().unwrap().iter() {
            xml.push_str(&format!(
                "<Contents><Key>{k}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>\"0\"</ETag><Size>{}</Size></Contents>",
                body.len()
            ));
        }
        xml.push_str("</ListBucketResult>");
        return ([(header::CONTENT_TYPE, "application/xml")], xml).into_response();
    }
    let wanted = path.strip_prefix("/b/").unwrap_or_default().to_string();
    match b.objects.lock().unwrap().iter().find(|(k, _)| *k == wanted) {
        Some((_, body)) => (
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

/// An S3 definition on the stand-in, read-only, polled only when asked.
fn definition(id: &str, bucket: &str, access: Value) -> Value {
    let mut source = json!({"type": "s3", "bucket": "b", "region": "us-east-1",
        "endpoint": bucket, "read_only": true, "pattern": "*.ndjson", "poll_seconds": 3600});
    source
        .as_object_mut()
        .unwrap()
        .extend(access.as_object().unwrap().clone());
    json!({"id": id, "name": id, "source": source, "format": "ndjson",
        "transform": "transforms/fhir_passthrough.py"})
}

async fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(started.elapsed() < Duration::from_secs(30), "{what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn a_definition_must_say_how_it_reaches_its_bucket() {
    let (api, _state, _dir) = engine().await;
    let bucket = Bucket::default().serve().await;
    let http = reqwest::Client::new();
    for (access, says) in [
        (json!({}), "say how to reach the bucket"),
        (
            json!({"anonymous": true, "credential": "lab"}),
            "do not go together",
        ),
    ] {
        let body = definition("s", &bucket, access);
        for (path, body) in [
            ("/api/sources", body.clone()),
            ("/api/sources/test", json!({"source": body["source"]})),
        ] {
            let r = http
                .post(format!("{api}{path}"))
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), 400, "{path}");
            let answer: Value = r.json().await.unwrap();
            assert_eq!(answer["code"], "invalid_definition", "{path}");
            assert!(answer["error"].as_str().unwrap().contains(says), "{answer}");
        }
    }
}

#[tokio::test]
async fn an_anonymous_source_is_never_signed() {
    let (api, _state, _dir) = engine().await;
    let bucket = Bucket::default();
    let url = bucket.serve().await;
    let http = reqwest::Client::new();
    let body = definition("open", &url, json!({"anonymous": true}));
    let probe: Value = http
        .post(format!("{api}/api/sources/test"))
        .json(&json!({"source": body["source"]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(probe["reachable"], true, "{probe}");
    assert_eq!(probe["objects"], 0, "an empty listing parses");
    let r = http
        .post(format!("{api}/api/sources"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    eventually("the source listed its bucket", || {
        bucket.listings(None) >= 2
    })
    .await;
    assert!(
        bucket.seen().iter().all(|s| s.key_id.is_none()),
        "{:?}",
        bucket.seen()
    );
}

#[cfg(feature = "secrets-native")]
mod stored {
    use super::*;

    async fn store_credential(api: &str, name: &str, key_id: &str) -> String {
        let r = reqwest::Client::new()
            .post(format!("{api}/api/secrets"))
            .json(&json!({"name": name, "kind": "aws",
                "value": {"access_key_id": key_id, "secret_access_key": "generated-secret"}}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201);
        r.json::<Value>().await.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    async fn open_alerts(api: &str, source: &str) -> Vec<Value> {
        let alerts: Value = reqwest::get(format!("{api}/api/alerts?source={source}&open=true"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        alerts["alerts"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| a["kind"] == "credential")
            .cloned()
            .collect()
    }

    #[tokio::test]
    async fn the_stored_credential_signs_the_test_the_source_and_its_next_revision() {
        let (api, _state, _dir) = engine().await;
        let bucket = Bucket::default();
        let url = bucket.serve().await;
        let http = reqwest::Client::new();
        let id = store_credential(&api, "lab", "AKIDALPHA").await;
        let body = definition("lab-s3", &url, json!({"credential": "lab"}));

        // The Test proves the credential, and registers nothing.
        let probe: Value = http
            .post(format!("{api}/api/sources/test"))
            .json(&json!({"source": body["source"]}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(probe["reachable"], true, "{probe}");
        assert_eq!(bucket.listings(Some("AKIDALPHA")), 1);
        let meta: Value = http
            .get(format!("{api}/api/secrets/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(meta["used_by"], json!([]));

        let r = http
            .post(format!("{api}/api/sources"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201);
        eventually("the source listed with the stored key", || {
            bucket.listings(Some("AKIDALPHA")) >= 2
        })
        .await;

        // A new revision; the old key stops working at the bucket.
        let r = http
            .put(format!("{api}/api/secrets/{id}"))
            .json(&json!({"value": {"access_key_id": "AKIDBRAVO", "secret_access_key": "generated-two"}}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        bucket.revoked.lock().unwrap().insert("AKIDALPHA".into());
        let r = http
            .post(format!("{api}/api/sources/lab-s3/check-now"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 202);
        eventually("the next pass listed with the new key", || {
            bucket.listings(Some("AKIDBRAVO")) >= 1
        })
        .await;
        assert!(
            !bucket
                .seen()
                .iter()
                .any(|s| s.key_id.as_deref() == Some("AKIDALPHA") && s.status == 403),
            "the old key was not used after the replace"
        );
        assert!(open_alerts(&api, "lab-s3").await.is_empty());
    }

    #[tokio::test]
    async fn a_queue_is_read_with_the_sources_credential() {
        let (api, _state, _dir) = engine().await;
        let bucket = Bucket::default();
        let url = bucket.serve().await;
        let calls: Arc<Mutex<Vec<Option<String>>>> = Arc::default();
        let queue = {
            let calls = calls.clone();
            serve(Router::new().fallback(move |headers: HeaderMap, uri: Uri| {
                let calls = calls.clone();
                async move {
                    calls.lock().unwrap().push(key_id(&headers, &uri));
                    ([(header::CONTENT_TYPE, "application/x-amz-json-1.0")], "{}")
                }
            }))
            .await
        };
        store_credential(&api, "lab", "AKIDQUEUE").await;
        let mut body = definition("drops", &url, json!({"credential": "lab"}));
        body["source"]["queue"] = json!(format!("{queue}/000000000000/drops"));
        let http = reqwest::Client::new();
        let r = http
            .post(format!("{api}/api/sources"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201, "{}", r.text().await.unwrap());
        // The first pass lists the bucket; the next reads the queue.
        eventually("the first listing", || {
            bucket.listings(Some("AKIDQUEUE")) >= 1
        })
        .await;
        http.post(format!("{api}/api/sources/drops/check-now"))
            .send()
            .await
            .unwrap();
        eventually("a queue call", || !calls.lock().unwrap().is_empty()).await;
        assert!(
            calls
                .lock()
                .unwrap()
                .iter()
                .all(|k| k.as_deref() == Some("AKIDQUEUE")),
            "{:?}",
            calls.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn a_credential_that_cannot_be_loaded_raises_one_alert_until_a_pass_loads_it() {
        let (api, state, _dir) = engine().await;
        let bucket = Bucket::default();
        let url = bucket.serve().await;
        let http = reqwest::Client::new();
        let id = store_credential(&api, "lab", "AKIDALPHA").await;
        let r = http
            .post(format!("{api}/api/sources"))
            .json(&definition("lab-s3", &url, json!({"credential": "lab"})))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201);
        eventually("the first listing", || {
            bucket.listings(Some("AKIDALPHA")) >= 1
        })
        .await;

        // The stored revision no longer opens (its row was moved on by hand):
        // the source keeps its client and says so once, pass after pass.
        state
            .db
            .query_json(
                &format!("UPDATE secrets SET revision = revision + 1 WHERE id = '{id}'"),
                Vec::new(),
            )
            .unwrap();
        for pass in 2..=3 {
            http.post(format!("{api}/api/sources/lab-s3/check-now"))
                .send()
                .await
                .unwrap();
            eventually("another pass", || {
                bucket.listings(Some("AKIDALPHA")) >= pass
            })
            .await;
        }
        let alerts = open_alerts(&api, "lab-s3").await;
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        assert!(
            alerts[0]["message"]
                .as_str()
                .unwrap()
                .contains("credential lab")
        );

        // Back as it was, with no replace: the next pass clears the alert.
        state
            .db
            .query_json(
                &format!("UPDATE secrets SET revision = revision - 1 WHERE id = '{id}'"),
                Vec::new(),
            )
            .unwrap();
        http.post(format!("{api}/api/sources/lab-s3/check-now"))
            .send()
            .await
            .unwrap();
        let started = Instant::now();
        while !open_alerts(&api, "lab-s3").await.is_empty() {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "alert not cleared"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// A stand-in FHIR server whose submissions never finish: it counts polls.
    #[derive(Clone, Default)]
    struct Hfs {
        base: Arc<Mutex<String>>,
        kickoffs: Arc<Mutex<u32>>,
        polls: Arc<Mutex<u32>>,
    }

    async fn hfs() -> (Hfs, String) {
        use axum::routing::{get, post};
        let h = Hfs::default();
        let app = Router::new()
            .route(
                "/$bulk-submit",
                post(|State(h): State<Hfs>| async move {
                    *h.kickoffs.lock().unwrap() += 1;
                    axum::Json(json!({"resourceType": "OperationOutcome", "issue": []}))
                }),
            )
            .route(
                "/$bulk-submit-status",
                post(|State(h): State<Hfs>| async move {
                    let base = h.base.lock().unwrap().clone();
                    (
                        StatusCode::ACCEPTED,
                        [(header::CONTENT_LOCATION, format!("{base}/status/1"))],
                    )
                }),
            )
            .route(
                "/status/{id}",
                get(|State(h): State<Hfs>| async move {
                    *h.polls.lock().unwrap() += 1;
                    (StatusCode::ACCEPTED, axum::Json(json!({})))
                }),
            )
            .with_state(h.clone());
        let url = serve(app).await;
        *h.base.lock().unwrap() = url.clone();
        (h, url)
    }

    /// Until its credential exists, a source lists nothing and hands nothing
    /// over, but what is already open is still polled and nothing is rejected;
    /// once the credential is stored, the next pass loads it.
    #[tokio::test]
    async fn a_source_waits_for_its_credential_while_open_submissions_keep_polling() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("hie.yaml"),
            "fhir_base_url: http://127.0.0.1:9/fhir\ndatabase: data/hie.db\n",
        )
        .unwrap();
        let (config, _) = hie::config::load(&dir.path().join("hie.yaml")).unwrap();
        let db = Arc::new(hie::db::Db::open(&config.database).unwrap());
        let key = {
            use base64::Engine;
            let mut b = [0u8; 32];
            getrandom::fill(&mut b).unwrap();
            base64::engine::general_purpose::STANDARD.encode(b)
        };
        let secrets =
            hie::secrets::open(&config, &db, hie::secrets::KeySettings::inline(&key)).unwrap();
        let store = secrets.store().unwrap().clone();
        let value = |key_id: &str| {
            hie::secrets::SecretValue::new(
                hie::secrets::SecretKind::Aws,
                [
                    ("access_key_id".to_string(), key_id.to_string().into()),
                    (
                        "secret_access_key".to_string(),
                        "generated".to_string().into(),
                    ),
                ],
            )
            .unwrap()
        };
        let bucket = Bucket::default();
        bucket.objects.lock().unwrap().push((
            "a.ndjson".into(),
            b"{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n".to_vec(),
        ));
        let bucket_url = bucket.serve().await;
        let (hfs, hfs_url) = hfs().await;
        let feed = |credential: &str| -> hie::config::FeedConfig {
            serde_yaml::from_str(&format!(
                "id: drops\nname: Drops\nsource:\n  type: s3\n  bucket: b\n  region: us-east-1\n  endpoint: {bucket_url}\n  credential: {credential}\n  read_only: true\n  pattern: \"*.ndjson\"\n  min_file_age_ms: 0\nformat: ndjson\ntransform: {}\ndelivery:\n  method: bulk_submit\n  bulk_from: source_objects\n  initial_backoff_ms: 10\n",
                repo().join("transforms/fhir_passthrough.py").display()
            ))
            .unwrap()
        };
        let runner = |feed: hie::config::FeedConfig| {
            hie::pipeline::FeedRunner::new(
                feed,
                db.clone(),
                Arc::new(hie::fhir::FhirClient::new(&hfs_url)),
                Arc::new(hie::alerts::Alerter::new(db.clone(), None)),
                "python3",
                &repo().join("hie_runtime"),
                &hfs_url,
                "http://engine.test",
            )
            .unwrap()
            .with_secrets(secrets.clone())
        };

        // A first definition opens a submission for the bucket's object.
        store
            .create(hie::secrets::NewSecret {
                name: "first".into(),
                kind: hie::secrets::SecretKind::Aws,
                value: value("AKIDFIRST"),
            })
            .await
            .unwrap();
        assert!(runner(feed("first")).pass().await.unwrap());
        assert_eq!(*hfs.kickoffs.lock().unwrap(), 1);
        let polls = *hfs.polls.lock().unwrap();

        // The definition now names a credential that does not exist yet.
        let waiting = runner(feed("later"));
        let seen = bucket.seen().len();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(waiting.pass().await.unwrap());
        assert_eq!(bucket.seen().len(), seen, "nothing listed or fetched");
        assert!(
            *hfs.polls.lock().unwrap() > polls,
            "the open submission was polled"
        );
        let rejected = db
            .query_json(
                "SELECT COUNT(*) AS n FROM records WHERE status = 'rejected'",
                Vec::new(),
            )
            .unwrap();
        assert_eq!(rejected[0]["n"], 0);
        assert!(
            db.open_alert("drops", "credential", None)
                .unwrap()
                .is_some()
        );

        // Stored now: the next pass loads it and lists with it.
        store
            .create(hie::secrets::NewSecret {
                name: "later".into(),
                kind: hie::secrets::SecretKind::Aws,
                value: value("AKIDLATER"),
            })
            .await
            .unwrap();
        assert!(waiting.pass().await.unwrap());
        assert_eq!(bucket.listings(Some("AKIDLATER")), 1);
        assert!(
            db.open_alert("drops", "credential", None)
                .unwrap()
                .is_none()
        );
    }
}
