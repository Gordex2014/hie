//! A feed whose transform runs at the shared runtime: the engine stores the code
//! under the transform's name, sends batches, applies one reply per record, and
//! keeps records waiting when the runtime is busy, has forgotten the code or
//! runs another environment than the version's. The runtime is a stand-in
//! that speaks python-runtime's HIE facade (0.1.3: capabilities, environment
//! headers).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use hie::pipeline::FeedRunner;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Default)]
struct Facade {
    transforms: HashMap<String, String>,
    puts: u32,
    runs: u32,
    tries: u32,
    busy: bool,
    tokens: Vec<String>,
    largest_batch: usize,
    /// The environment id runs and tries report.
    environment: String,
    /// The environments named in run bodies.
    named: Vec<String>,
    /// Answers without the environment headers, as a proxy that strips them.
    no_headers: bool,
    /// `POST /transforms/try` answers `307` to itself.
    redirect_try: bool,
}

impl Facade {
    fn headers(&self) -> HeaderMap {
        if self.no_headers {
            HeaderMap::new()
        } else {
            env_headers(&self.environment)
        }
    }
}

const ENV: &str = "cpython-3.14@2-env-0123456789abcdef0123456789abcdef";

fn env_headers(id: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("python-runtime-environment-id", id.parse().unwrap());
    h.insert(
        "python-runtime-environment-digest",
        "a".repeat(64).parse().unwrap(),
    );
    h
}

async fn capabilities() -> Json<Value> {
    Json(json!({"default_environment": ENV,
        "environments": [{"id": ENV, "digest": "a".repeat(64), "state": "ready"}],
        "aliases": {"cpython-3.14@2": ENV}}))
}

type Shared = Arc<Mutex<Facade>>;

fn token(headers: &HeaderMap) -> String {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

async fn put_transform(
    State(s): State<Shared>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let mut s = s.lock().unwrap();
    s.puts += 1;
    s.tokens.push(token(&headers));
    let code = body["code"].as_str().unwrap().to_string();
    let sha = hex::encode(Sha256::digest(code.as_bytes()));
    let status = if s.transforms.insert(name.clone(), sha.clone()).is_some() {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    (status, Json(json!({"name": name, "sha256": sha})))
}

/// Echoes each record as its resource; a record with `"bad": true` is a data
/// error, one with `"crash": true` a transform error.
fn reply(request: &Value) -> Value {
    let id = &request["id"];
    let record = &request["record"];
    if record["bad"] == true {
        return json!({"id": id, "error": {"kind": "data", "message": "flagged bad"}});
    }
    if record["crash"] == true {
        return json!({"id": id, "error": {"kind": "transform", "message": "KeyError: 'x'", "trace": "  File \"t.py\", line 2"}});
    }
    json!({"id": id, "resources": [record]})
}

async fn run_transform(
    State(s): State<Shared>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> (StatusCode, HeaderMap, Json<Value>) {
    let mut s = s.lock().unwrap();
    s.runs += 1;
    if let Some(env) = body["environment"].as_str() {
        s.named.push(env.to_string());
    }
    if s.busy {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            HeaderMap::new(),
            Json(json!({"error": "busy"})),
        );
    }
    if !s.transforms.contains_key(&name) {
        return (
            StatusCode::NOT_FOUND,
            HeaderMap::new(),
            Json(json!({"error": "transform_unknown"})),
        );
    }
    let requests = body["requests"].as_array().unwrap();
    s.largest_batch = s.largest_batch.max(requests.len());
    let replies: Vec<Value> = requests.iter().map(reply).collect();
    (
        StatusCode::OK,
        s.headers(),
        Json(json!({"replies": replies})),
    )
}

async fn try_transform(
    State(s): State<Shared>,
    Json(body): Json<Value>,
) -> (StatusCode, HeaderMap, Json<Value>) {
    let mut s = s.lock().unwrap();
    s.tries += 1;
    assert!(body["code"].is_string());
    if s.redirect_try {
        let mut h = HeaderMap::new();
        h.insert("location", "/transforms/try".parse().unwrap());
        return (StatusCode::TEMPORARY_REDIRECT, h, Json(json!({})));
    }
    let replies: Vec<Value> = body["requests"]
        .as_array()
        .unwrap()
        .iter()
        .map(reply)
        .collect();
    (
        StatusCode::OK,
        s.headers(),
        Json(json!({"replies": replies})),
    )
}

async fn batch(Json(bundle): Json<Value>) -> Json<Value> {
    let entries: Vec<Value> = bundle["entry"]
        .as_array()
        .unwrap()
        .iter()
        .map(|_| json!({"response": {"status": "201 Created"}}))
        .collect();
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
}

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn count(state: &hie::api::AppState, status: &str) -> i64 {
    state.db.feed_counts("export").unwrap()[status]
        .as_i64()
        .unwrap()
}

#[tokio::test]
async fn transforms_run_at_the_shared_runtime() {
    let facade: Shared = Arc::default();
    facade.lock().unwrap().environment = ENV.into();
    let runtime = serve(
        Router::new()
            .route("/v1/capabilities", get(capabilities))
            .route("/transforms/{name}", put(put_transform))
            .route("/transforms/{name}/run", post(run_transform))
            .route("/transforms/try", post(try_transform))
            .with_state(facade.clone()),
    )
    .await;
    let fhir = serve(Router::new().route("/", post(batch))).await;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/fhir_passthrough.py"),
        root.join("transforms/fhir_passthrough.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!(
            "fhir_base_url: {fhir}\ndatabase: data/hie.db\npython: nonexistent-python\n\
             runtime:\n  url: {runtime}\n  token_env: HIE_TEST_RUNTIME_TOKEN\n  batch_size: 150\n"
        ),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/export.yaml"),
        "id: export\nname: Export\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.ndjson\"\n  min_file_age_ms: 0\n\
         format: ndjson\ntransform: transforms/fhir_passthrough.py\ndelivery:\n  initial_backoff_ms: 10\n",
    )
    .unwrap();
    // SAFETY: the test process sets its own variable before anything reads it.
    unsafe { std::env::set_var("HIE_TEST_RUNTIME_TOKEN", "dev-token-1") };
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let runner: Arc<FeedRunner> = manager.runner("export").unwrap();
    manager.pause("export", None).unwrap();
    // The runtime's environment is read (and the files found at start saved
    // in it) before the first pass; until then no load would be admitted.
    state.versions.ready().await;

    // 400 records: more than one call at batch_size 150, two of them rejected by the transform.
    let mut lines = Vec::new();
    for i in 0..400 {
        let mut record = json!({"resourceType": "Patient", "id": format!("p{i}")});
        if i == 7 {
            record["bad"] = json!(true);
        }
        if i == 300 {
            record["crash"] = json!(true);
        }
        lines.push(record.to_string());
    }
    std::fs::create_dir_all(root.join("inbox")).unwrap();
    std::fs::write(root.join("inbox/patients.ndjson"), lines.join("\n")).unwrap();
    runner.tick().await.unwrap();
    assert_eq!((count(&state, "sent"), count(&state, "rejected")), (398, 2));
    {
        let f = facade.lock().unwrap();
        assert_eq!(f.puts, 1, "the code is stored once");
        assert_eq!(f.largest_batch, 150);
        assert!(f.tokens.iter().all(|t| t == "Bearer dev-token-1"));
        assert!(
            f.named.iter().all(|e| e == ENV),
            "every run names the version's environment"
        );
        let names: Vec<&String> = f.transforms.keys().collect();
        assert_eq!(names.len(), 1);
        assert!(names[0].starts_with("fhir_passthrough-v1-"), "{names:?}");
    }
    let rejected = state
        .db
        .query_json(
            "SELECT line, error_kind, error FROM records WHERE status = 'rejected' ORDER BY line",
            [],
        )
        .unwrap();
    assert_eq!(rejected[0]["line"], 8);
    assert_eq!(rejected[0]["error_kind"], "data");
    assert_eq!(rejected[1]["line"], 301);
    assert_eq!(rejected[1]["error_kind"], "transform");
    assert!(rejected[1]["error"].as_str().unwrap().contains("KeyError"));

    // The runtime is busy: the records stay received and go through on a later pass.
    facade.lock().unwrap().busy = true;
    std::fs::write(
        root.join("inbox/more.ndjson"),
        "{\"resourceType\":\"Patient\",\"id\":\"q1\"}\n",
    )
    .unwrap();
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "received"), 1);
    facade.lock().unwrap().busy = false;
    runner.tick().await.unwrap();
    assert_eq!((count(&state, "received"), count(&state, "sent")), (0, 399));

    // The runtime restarted without its store: the code is stored again and the batch run.
    facade.lock().unwrap().transforms.clear();
    std::fs::write(
        root.join("inbox/after-restart.ndjson"),
        "{\"resourceType\":\"Patient\",\"id\":\"r1\"}\n",
    )
    .unwrap();
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 400);
    assert_eq!(facade.lock().unwrap().puts, 2);

    // A changed transform file is stored under the same name before the next batch.
    let script = root.join("transforms/fhir_passthrough.py");
    let code = std::fs::read_to_string(&script).unwrap();
    std::fs::write(&script, format!("{code}\n# edited\n")).unwrap();
    std::fs::write(
        root.join("inbox/after-edit.ndjson"),
        "{\"resourceType\":\"Patient\",\"id\":\"s1\"}\n",
    )
    .unwrap();
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 401);
    assert_eq!(facade.lock().unwrap().puts, 3);

    // The runtime now runs another environment than the version's: nothing it
    // returns is kept, the load waits, and goes through once it is back.
    facade.lock().unwrap().environment =
        "cpython-3.14@2-env-fedcba9876543210fedcba9876543210".into();
    std::fs::write(
        root.join("inbox/elsewhere.ndjson"),
        "{\"resourceType\":\"Patient\",\"id\":\"u1\"}\n",
    )
    .unwrap();
    runner.tick().await.unwrap();
    assert_eq!((count(&state, "received"), count(&state, "sent")), (1, 401));
    let load = state
        .db
        .query_json("SELECT MAX(id) AS id FROM files", [])
        .unwrap()[0]["id"]
        .as_i64()
        .unwrap();
    assert_eq!(
        state.db.version_waiting(load, None).unwrap().as_deref(),
        Some("environment_unavailable")
    );
    facade.lock().unwrap().environment = ENV.into();
    runner.tick().await.unwrap();
    assert_eq!((count(&state, "received"), count(&state, "sent")), (0, 402));
    assert_eq!(state.db.version_waiting(load, None).unwrap(), None);

    // Try runs the saved code inline at the runtime and stores nothing.
    let api = serve(hie::api::router(state.clone())).await;
    let out: Value = reqwest::Client::new()
        .post(format!("{api}/api/transforms/fhir_passthrough/try"))
        .json(&json!({"records": [{"resourceType": "Patient", "id": "t1"}, {"bad": true}]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(out["results"][0]["resources"][0]["id"], "t1");
    assert_eq!(out["results"][1]["error"]["kind"], "data");
    assert_eq!(out["run"]["environment"]["id"], ENV);
    assert_eq!(out["run"]["transform"]["version"], 2);
    assert_eq!(facade.lock().unwrap().tries, 1);

    let try_saved = |run_id: &'static str| {
        let api = api.clone();
        async move {
            let response = reqwest::Client::new()
                .post(format!("{api}/api/transforms/fhir_passthrough/try"))
                .json(&json!({"run_id": run_id, "input": {"records": [{"resourceType": "Patient", "id": "t2"}]}}))
                .send()
                .await
                .unwrap();
            let status = response.status().as_u16();
            (status, response.json::<Value>().await.unwrap())
        }
    };
    // Ran in another environment: refused, nothing shown, and the run id
    // stays used (it was dispatched).
    facade.lock().unwrap().environment =
        "cpython-3.14@2-env-fedcba9876543210fedcba9876543210".into();
    let (status, body) = try_saved("e-1").await;
    assert_eq!(
        (status, body["code"].as_str()),
        (409, Some("environment_unavailable"))
    );
    assert_eq!(try_saved("e-1").await.1["code"], "run_id_in_use");
    assert_eq!(facade.lock().unwrap().tries, 2, "never sent again");
    let run: Value = reqwest::get(format!("{api}/api/transforms/try-runs/e-1"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(run["run"]["status"], "finished");
    assert_eq!(run["coverage"]["reported"], 0);
    facade.lock().unwrap().environment = ENV.into();

    // An answer without complete environment evidence applies nothing: the
    // load waits; a saved version's Try is refused.
    facade.lock().unwrap().no_headers = true;
    std::fs::write(
        root.join("inbox/unproven.ndjson"),
        "{\"resourceType\":\"Patient\",\"id\":\"w1\"}\n",
    )
    .unwrap();
    runner.tick().await.unwrap();
    assert_eq!((count(&state, "received"), count(&state, "sent")), (1, 402));
    let load = state
        .db
        .query_json("SELECT MAX(id) AS id FROM files", [])
        .unwrap()[0]["id"]
        .as_i64()
        .unwrap();
    assert_eq!(
        state.db.version_waiting(load, None).unwrap().as_deref(),
        Some("environment_unavailable")
    );
    assert_eq!(try_saved("h-1").await.1["code"], "environment_unavailable");
    facade.lock().unwrap().no_headers = false;
    runner.tick().await.unwrap();
    assert_eq!((count(&state, "received"), count(&state, "sent")), (0, 403));

    // A redirect is not the facade's answer and is not followed.
    facade.lock().unwrap().redirect_try = true;
    let (status, body) = try_saved("r-1").await;
    assert_eq!(status, 200);
    assert_eq!(body["run"]["outcome"], "indeterminate");
    assert_eq!(body["run"]["termination"], "unconfirmed");
    let f = facade.lock().unwrap();
    assert_eq!((f.tries, f.puts), (4, 3), "the redirect was not followed");
    drop(f);
    drop(manager);
}
