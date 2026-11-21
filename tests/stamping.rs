//! Delivered resources tagged with their source (`delivery.stamp_source`), and
//! what the engine reports about deliveries that were not: end to end, with a
//! stand-in FHIR server that takes REST batches and `$bulk-submit` manifests and
//! keeps what it was sent.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

const SYSTEM: &str = "urn:helios:hie:source";

#[derive(Default)]
struct Hfs {
    base: String,
    /// Every REST Bundle received, refused ones included.
    bundles: Vec<Value>,
    rest_down: bool,
    /// A Bundle of more than one entry is refused as a whole.
    refuse_multi: bool,
    /// Submission ids kicked off; a repeat is answered `409`.
    submitted: Vec<String>,
    /// The NDJSON lines fetched for each submission id.
    fetched: HashMap<String, Vec<Value>>,
    /// `$bulk-submit-status` is refused (`404`) after the kick-off was taken.
    refuse_status: bool,
    /// Polls answer "in progress" for ever.
    never_done: bool,
    /// Receipts leave out every resource.
    no_receipts: bool,
    polls: HashMap<String, u32>,
}

type Shared = Arc<Mutex<Hfs>>;

async fn rest(State(h): State<Shared>, Json(bundle): Json<Value>) -> Response {
    let mut h = h.lock().unwrap();
    h.bundles.push(bundle.clone());
    let entries = bundle["entry"].as_array().unwrap();
    if h.rest_down {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if h.refuse_multi && entries.len() > 1 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"resourceType": "OperationOutcome", "issue": [{"severity": "error", "code": "invalid", "diagnostics": "batch refused"}]})),
        )
            .into_response();
    }
    let answers: Vec<Value> = entries
        .iter()
        .map(|_| json!({"response": {"status": "201 Created"}}))
        .collect();
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": answers}))
        .into_response()
}

fn param(body: &Value, name: &str) -> Option<Value> {
    body["parameter"]
        .as_array()?
        .iter()
        .find(|p| p["name"] == name)
        .cloned()
}

/// Fetches the manifest and every file at once, as the server would.
async fn kickoff(State(h): State<Shared>, Json(body): Json<Value>) -> Response {
    let id = param(&body, "submissionId").unwrap()["valueString"]
        .as_str()
        .unwrap()
        .to_string();
    if h.lock().unwrap().submitted.contains(&id) {
        return StatusCode::CONFLICT.into_response();
    }
    let manifest_url = param(&body, "manifestUrl").unwrap()["valueUrl"]
        .as_str()
        .unwrap()
        .to_string();
    let manifest: Value = reqwest::get(&manifest_url)
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut lines = Vec::new();
    for entry in manifest["output"].as_array().unwrap() {
        let text = reqwest::get(entry["url"].as_str().unwrap())
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        lines.extend(
            text.lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str::<Value>(l).unwrap()),
        );
    }
    let mut h = h.lock().unwrap();
    h.submitted.push(id.clone());
    h.fetched.insert(id, lines);
    StatusCode::OK.into_response()
}

async fn status_kickoff(State(h): State<Shared>, Json(body): Json<Value>) -> Response {
    let id = param(&body, "submissionId").unwrap()["valueString"]
        .as_str()
        .unwrap()
        .to_string();
    let h = h.lock().unwrap();
    if h.refuse_status {
        return StatusCode::NOT_FOUND.into_response();
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-location",
        format!("{}/status/{id}", h.base).parse().unwrap(),
    );
    (StatusCode::ACCEPTED, headers).into_response()
}

async fn poll(State(h): State<Shared>, UrlPath(id): UrlPath<String>) -> Response {
    let mut h = h.lock().unwrap();
    let polls = h.polls.entry(id.clone()).or_default();
    *polls += 1;
    if *polls == 1 || h.never_done {
        return StatusCode::ACCEPTED.into_response();
    }
    Json(json!({
        "output": [{"type": "Patient", "url": format!("{}/receipts/{id}", h.base)}],
        "outcome": [], "link": [],
    }))
    .into_response()
}

async fn receipts(State(h): State<Shared>, UrlPath(id): UrlPath<String>) -> String {
    let h = h.lock().unwrap();
    if h.no_receipts {
        return String::new();
    }
    h.fetched[&id]
        .iter()
        .map(|r| {
            json!({"reference": format!("{}/{}", r["resourceType"].as_str().unwrap(), r["id"].as_str().unwrap())})
                .to_string()
                + "\n"
        })
        .collect()
}

async fn bind() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (listener, url)
}

async fn hfs() -> (String, Shared) {
    let (listener, url) = bind().await;
    let state: Shared = Arc::default();
    state.lock().unwrap().base = url.clone();
    let app = Router::new()
        .route("/", post(rest))
        .route("/$bulk-submit", post(kickoff))
        .route("/$bulk-submit-status", post(status_kickoff))
        .route("/status/{id}", get(poll))
        .route("/receipts/{id}", get(receipts))
        .with_state(state.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, state)
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

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// A transform that returns what its record carries under `out`.
const EMIT: &str = "def transform(record, context):\n    return record[\"out\"]\n";

/// A REST source over `inbox/*.ndjson` with the `emit` transform.
const REST_SOURCE: &str = "id: lab\nname: Lab\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.ndjson\"\n  min_file_age_ms: 0\n\
     format: ndjson\ntransform: transforms/emit.py\ndelivery:\n  initial_backoff_ms: 10\n";

/// The same through `$bulk-submit` from transformed records.
const TRANSFORMED_SOURCE: &str = "id: lab\nname: Lab\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.ndjson\"\n  min_file_age_ms: 0\n\
     format: ndjson\ntransform: transforms/emit.py\ndelivery:\n  method: bulk_submit\n  bulk_from: transformed\n  initial_backoff_ms: 10\n";

/// FHIR NDJSON handed over as it arrived.
const DIRECT_SOURCE: &str = "id: lab\nname: Lab\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.ndjson\"\n  min_file_age_ms: 0\n\
     format: ndjson\ntransform: transforms/fhir_passthrough.py\ndelivery:\n  method: bulk_submit\n  bulk_from: source_objects\n  initial_backoff_ms: 10\n";

struct Engine {
    api: String,
    state: hie::api::AppState,
    manager: Arc<hie::manager::SourceManager>,
}

impl Engine {
    /// Builds the engine over `root` (its store, sources and transforms), serves
    /// its API, and pauses the source: the test drives the passes.
    async fn start(root: &Path, hfs: &str, python: &str, stamp: bool) -> Engine {
        let (listener, api) = bind().await;
        std::fs::write(
            root.join("hie.yaml"),
            format!(
                "fhir_base_url: {hfs}\npublic_url: {api}\ndatabase: data/hie.db\npython: {python}\n\
                 delivery:\n  stamp_source: {stamp}\n"
            ),
        )
        .unwrap();
        let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
        let (manager, state) =
            hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
        let app = hie::api::router(state.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        if manager.runner("lab").is_some() {
            manager.pause("lab", None).unwrap();
        }
        Engine {
            api,
            state,
            manager,
        }
    }

    /// One pass; open submissions are polled now rather than after their back-off.
    async fn tick(&self) {
        self.state
            .db
            .query_json(
                "UPDATE submissions SET next_attempt_at = 0 RETURNING token",
                [],
            )
            .unwrap();
        self.manager.runner("lab").unwrap().tick().await.unwrap();
    }

    /// Passes until no record waits any more (a submission takes a kick-off and
    /// two polls), at most a few.
    async fn settle(&self) {
        for _ in 0..6 {
            if !self
                .statuses()
                .iter()
                .any(|s| matches!(s.as_str(), "received" | "pending" | "submitted"))
            {
                return;
            }
            self.tick().await;
        }
        panic!("records still waiting: {:?}", self.statuses());
    }

    async fn get(&self, path: &str) -> Value {
        reqwest::get(format!("{}{path}", self.api))
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn stamping(&self) -> Value {
        self.get("/api/sources/lab").await["stamping"].clone()
    }

    fn statuses(&self) -> Vec<String> {
        self.state
            .db
            .query_json("SELECT status FROM records ORDER BY id", [])
            .unwrap()
            .iter()
            .map(|r| r["status"].as_str().unwrap().to_string())
            .collect()
    }
}

fn root(source: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for d in ["sources", "transforms", "inbox"] {
        std::fs::create_dir_all(root.join(d)).unwrap();
    }
    std::fs::write(root.join("transforms/emit.py"), EMIT).unwrap();
    std::fs::copy(
        repo().join("transforms/fhir_passthrough.py"),
        root.join("transforms/fhir_passthrough.py"),
    )
    .unwrap();
    std::fs::write(root.join("sources/lab.yaml"), source).unwrap();
    dir
}

fn drop_records(root: &Path, name: &str, records: &[Value]) {
    let text: String = records.iter().map(|r| r.to_string() + "\n").collect();
    std::fs::write(root.join("inbox").join(name), text).unwrap();
}

fn source_tags(resource: &Value) -> Vec<Value> {
    resource["meta"]["tag"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|t| t["system"] == SYSTEM)
        .cloned()
        .collect()
}

fn sent_resources(bundle: &Value) -> Vec<Value> {
    bundle["entry"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["resource"].clone())
        .collect()
}

fn instant(v: &Value) -> i64 {
    chrono::DateTime::parse_from_rfc3339(v.as_str().unwrap())
        .unwrap()
        .timestamp_millis()
}

const MARGIN_MS: i64 = hie::db::UNTAGGED_MARGIN_MS;

#[tokio::test]
async fn rest_deliveries_carry_one_source_tag_and_keep_the_rest_of_meta() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (hfs_url, hfs) = hfs().await;
    let dir = root(REST_SOURCE);
    let engine = Engine::start(dir.path(), &hfs_url, &python, true).await;
    assert_eq!(
        engine.stamping().await,
        json!({"active": true, "reason": null, "untagged": "none", "untagged_until": null})
    );
    let patient = json!({"resourceType": "Patient", "id": "p1", "meta": {
        "source": "urn:lab:analyser-7",
        "profile": ["http://example.org/p"],
        "tag": [{"system": "urn:a", "code": "1"}, {"system": SYSTEM, "code": "forged"}]
    }});
    let observation = json!({"resourceType": "Observation",
        "identifier": [{"system": "urn:lab", "value": "o1"}], "status": "final"});
    drop_records(
        dir.path(),
        "a.ndjson",
        &[
            json!({"out": [patient, observation]}),
            json!({"out": {"resourceType": "Patient", "id": "p2"}}),
        ],
    );
    engine.tick().await;
    assert_eq!(engine.statuses(), ["sent", "sent"]);
    let sent: Vec<Value> = {
        let h = hfs.lock().unwrap();
        assert_eq!(h.bundles.len(), 1);
        sent_resources(&h.bundles[0])
    };
    assert_eq!(sent.len(), 3);
    for r in &sent {
        assert_eq!(
            source_tags(r),
            [json!({"system": SYSTEM, "code": "lab"})],
            "{r}"
        );
    }
    assert_eq!(sent[0]["meta"]["source"], "urn:lab:analyser-7");
    assert_eq!(sent[0]["meta"]["profile"], json!(["http://example.org/p"]));
    assert_eq!(
        sent[0]["meta"]["tag"][0],
        json!({"system": "urn:a", "code": "1"})
    );
    assert_eq!(
        engine.stamping().await,
        json!({"active": true, "reason": null, "untagged": "none", "untagged_until": null})
    );

    // What the record keeps is what the transform returned, untagged; sent
    // again, it carries the tag once, not twice.
    let first = engine
        .state
        .db
        .query_json("SELECT MIN(id) AS id FROM records", [])
        .unwrap()[0]["id"]
        .clone();
    let record = engine.get(&format!("/api/records/{first}")).await;
    assert_eq!(
        source_tags(&record["resources"][0]),
        [json!({"system": SYSTEM, "code": "forged"})]
    );
    let reprocessed = reqwest::Client::new()
        .post(format!("{}/api/records/{first}/reprocess", engine.api))
        .send()
        .await
        .unwrap();
    assert!(reprocessed.status().is_success());
    engine.tick().await;
    {
        let h = hfs.lock().unwrap();
        assert_eq!(h.bundles.len(), 2);
        let again = sent_resources(&h.bundles[1]);
        assert_eq!(again.len(), 2);
        assert_eq!(again[0], sent[0], "the same JSON as the first delivery");
        for r in &again {
            assert_eq!(source_tags(r).len(), 1);
        }
    }

    // Try never writes and is not tagged.
    let tried: Value = reqwest::Client::new()
        .post(format!("{}/api/transforms/emit/try", engine.api))
        .json(&json!({"records": [{"out": {"resourceType": "Patient", "id": "t"}}]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tried = tried.to_string();
    assert!(tried.contains("\"id\":\"t\""), "{tried}");
    assert!(!tried.contains(SYSTEM), "{tried}");

    // The setting is the engine's: a source that names it is refused.
    let refused = reqwest::Client::new()
        .post(format!("{}/api/sources", engine.api))
        .json(&json!({"id": "other", "name": "Other",
            "source": {"type": "folder", "path": "inbox2"},
            "transform": "transforms/emit.py", "delivery": {"stamp_source": true}}))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 400);
    assert!(refused.text().await.unwrap().contains("engine setting"));
}

#[tokio::test]
async fn switched_off_nothing_is_tagged_and_a_restart_with_it_on_tags_what_waited() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (hfs_url, hfs) = hfs().await;
    let dir = root(REST_SOURCE);
    let off = Engine::start(dir.path(), &hfs_url, &python, false).await;

    // Delivered untagged: the bound covers the request and the margin after it.
    drop_records(
        dir.path(),
        "a.ndjson",
        &[json!({"out": {"resourceType": "Patient", "id": "p1"}})],
    );
    let before = chrono::Utc::now().timestamp_millis();
    off.tick().await;
    assert_eq!(off.statuses(), ["sent"]);
    assert!(source_tags(&sent_resources(&hfs.lock().unwrap().bundles[0])[0]).is_empty());
    let stamping = off.stamping().await;
    assert_eq!(stamping["active"], false);
    assert_eq!(stamping["reason"], "off");
    assert_eq!(stamping["untagged"], "open", "the margin has not passed");
    assert!(instant(&stamping["untagged_until"]) >= before + MARGIN_MS);

    // HFS is down: the next record is tried, untagged, and waits.
    hfs.lock().unwrap().rest_down = true;
    drop_records(
        dir.path(),
        "b.ndjson",
        &[json!({"out": {"resourceType": "Patient", "id": "p2"}})],
    );
    off.tick().await;
    assert_eq!(off.statuses(), ["sent", "pending"]);
    let bound = off.stamping().await["untagged_until"].clone();

    // Restarted with stamping on, over the same store: what waited goes out
    // tagged, and the bound stays where the untagged attempts left it.
    let on = Engine::start(dir.path(), &hfs_url, &python, true).await;
    hfs.lock().unwrap().rest_down = false;
    tokio::time::sleep(Duration::from_millis(50)).await;
    on.tick().await;
    assert_eq!(on.statuses(), ["sent", "sent"]);
    let last = sent_resources(hfs.lock().unwrap().bundles.last().unwrap())[0].clone();
    assert_eq!(last["id"], "p2");
    assert_eq!(
        source_tags(&last),
        [json!({"system": SYSTEM, "code": "lab"})]
    );
    let stamping = on.stamping().await;
    assert_eq!(stamping["active"], true);
    assert_eq!(stamping["untagged_until"], bound);
}

#[tokio::test]
async fn a_record_that_cannot_carry_the_tag_is_rejected_alone_even_in_the_fallback() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (hfs_url, hfs) = hfs().await;
    hfs.lock().unwrap().refuse_multi = true;
    let dir = root(REST_SOURCE);
    let engine = Engine::start(dir.path(), &hfs_url, &python, true).await;
    drop_records(
        dir.path(),
        "a.ndjson",
        &[
            json!({"out": {"resourceType": "Patient", "id": "v1"}}),
            json!({"out": [{"resourceType": "Patient", "id": "bad-ok"}, {"resourceType": "Patient", "id": "bad", "meta": "x"}]}),
            json!({"out": {"resourceType": "Patient", "id": "v2"}}),
        ],
    );
    engine.tick().await;
    assert_eq!(engine.statuses(), ["sent", "rejected", "sent"]);
    let rejected = &engine.get("/api/records?source=lab&status=rejected").await["records"][0];
    assert_eq!(rejected["error_kind"], "validation");
    assert!(
        rejected["error"]
            .as_str()
            .unwrap()
            .contains("meta is not an object")
    );
    let h = hfs.lock().unwrap();
    // The two valid records as one Bundle, refused, then one by one, tagged.
    let ids: Vec<Vec<String>> = h
        .bundles
        .iter()
        .map(|b| {
            sent_resources(b)
                .iter()
                .map(|r| {
                    assert_eq!(source_tags(r).len(), 1);
                    r["id"].as_str().unwrap().to_string()
                })
                .collect()
        })
        .collect();
    assert_eq!(ids, [vec!["v1", "v2"], vec!["v1"], vec!["v2"]]);
}

#[tokio::test]
async fn transformed_submissions_are_tagged_and_an_untagged_one_bounds_the_source_when_it_ends() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (hfs_url, hfs) = hfs().await;
    let dir = root(TRANSFORMED_SOURCE);
    let off = Engine::start(dir.path(), &hfs_url, &python, false).await;

    // Off: a one-record load is submitted untagged; HFS sends no receipt, so
    // the record will be rejected. Open while the submission runs.
    hfs.lock().unwrap().no_receipts = true;
    drop_records(
        dir.path(),
        "a.ndjson",
        &[
            json!({"out": {"resourceType": "Patient", "identifier": [{"system": "urn:p", "value": "1"}]}}),
        ],
    );
    off.tick().await;
    let first_id = hfs.lock().unwrap().submitted[0].clone();
    assert!(source_tags(&hfs.lock().unwrap().fetched[&first_id][0]).is_empty());
    assert_eq!(off.stamping().await["untagged"], "open");

    // Restarted with stamping on: the untagged submission is settled; its end
    // is the bound.
    let on = Engine::start(dir.path(), &hfs_url, &python, true).await;
    on.settle().await;
    assert_eq!(on.statuses(), ["rejected"]);
    let done_at = on
        .state
        .db
        .query_json(
            "SELECT updated_at FROM submissions WHERE status = 'done'",
            [],
        )
        .unwrap()[0]["updated_at"]
        .as_i64()
        .unwrap();
    let stamping = on.stamping().await;
    assert_eq!(stamping["active"], true);
    assert_eq!(stamping["untagged"], "until");
    assert_eq!(instant(&stamping["untagged_until"]), done_at);

    // The load is retried: the same record, the same span, now tagged, under an
    // id of its own (HFS answers 409 to a repeated one).
    hfs.lock().unwrap().no_receipts = false;
    let file_id = on.get("/api/sources/lab/loads").await["loads"][0]["id"].clone();
    let retried: Value = reqwest::Client::new()
        .post(format!("{}/api/loads/{file_id}/retry", on.api))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retried["retried"], 1);
    on.settle().await;
    assert_eq!(on.statuses(), ["sent"]);
    {
        let h = hfs.lock().unwrap();
        assert_eq!(h.submitted.len(), 2);
        assert_ne!(h.submitted[0], h.submitted[1]);
        let tagged = &h.fetched[&h.submitted[1]][0];
        assert_eq!(
            source_tags(tagged),
            [json!({"system": SYSTEM, "code": "lab"})]
        );
    }
    let stamped: Vec<i64> = on
        .state
        .db
        .query_json("SELECT stamped FROM submissions ORDER BY created_at", [])
        .unwrap()
        .iter()
        .map(|r| r["stamped"].as_i64().unwrap())
        .collect();
    assert_eq!(stamped, [0, 1]);
    // The tagged submission moved nothing.
    assert_eq!(instant(&on.stamping().await["untagged_until"]), done_at);
}

#[tokio::test]
async fn a_page_that_cannot_carry_the_tag_opens_no_submission() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (hfs_url, hfs) = hfs().await;
    let dir = root(TRANSFORMED_SOURCE);
    let engine = Engine::start(dir.path(), &hfs_url, &python, true).await;
    drop_records(
        dir.path(),
        "a.ndjson",
        &[
            json!({"out": {"resourceType": "Patient", "id": "a", "meta": "x"}}),
            json!({"out": {"resourceType": "Patient", "id": "b", "meta": {"tag": null}}}),
        ],
    );
    engine.tick().await;
    assert_eq!(engine.statuses(), ["rejected", "rejected"]);
    assert!(hfs.lock().unwrap().submitted.is_empty());
    let n = engine
        .state
        .db
        .query_json("SELECT COUNT(*) AS n FROM submissions", [])
        .unwrap()[0]["n"]
        .as_i64()
        .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn a_tagged_submission_is_served_only_the_file_the_engine_wrote() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    // No FHIR server: the submission is opened and waits for its kick-off.
    let (closed, hfs_url) = bind().await;
    drop(closed);
    let dir = root(TRANSFORMED_SOURCE);
    let engine = Engine::start(dir.path(), &hfs_url, &python, true).await;
    drop_records(
        dir.path(),
        "a.ndjson",
        &[json!({"out": {"resourceType": "Patient", "id": "a"}})],
    );
    engine.tick().await;
    let token = engine
        .state
        .db
        .query_json("SELECT token FROM submissions", [])
        .unwrap()[0]["token"]
        .as_str()
        .unwrap()
        .to_string();
    let url = format!("{}/bulk/{token}/files/1", engine.api);
    let served = reqwest::get(&url).await.unwrap();
    assert_eq!(served.status(), 200);
    assert!(served.text().await.unwrap().contains(SYSTEM));
    // The engine's file is gone: nothing else is served in its place, though
    // the submission's source file has the same number.
    std::fs::remove_file(dir.path().join("data/bulk").join(&token).join("1.ndjson")).unwrap();
    assert_eq!(reqwest::get(&url).await.unwrap().status(), 404);
}

#[tokio::test]
async fn direct_bulk_is_never_tagged_and_an_unresolved_submission_stays_open() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (hfs_url, hfs) = hfs().await;
    let dir = root(DIRECT_SOURCE);
    let engine = Engine::start(dir.path(), &hfs_url, &python, true).await;
    let stamping = engine.stamping().await;
    assert_eq!(
        (stamping["active"].clone(), stamping["reason"].clone()),
        (json!(false), json!("direct_bulk"))
    );

    // The object as it arrived, byte for byte; open until HFS is done.
    let object = "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"meta\":{\"tag\":[{\"system\":\"urn:a\",\"code\":\"1\"}]}}\n";
    std::fs::write(dir.path().join("inbox/a.ndjson"), object).unwrap();
    engine.tick().await;
    let id = hfs.lock().unwrap().submitted[0].clone();
    assert_eq!(
        hfs.lock().unwrap().fetched[&id],
        [serde_json::from_str::<Value>(object).unwrap()]
    );
    assert_eq!(engine.stamping().await["untagged"], "open");
    engine.settle().await;
    assert_eq!(engine.statuses(), ["sent"]);
    let stamping = engine.stamping().await;
    assert_eq!(stamping["untagged"], "until");
    let bound = stamping["untagged_until"].clone();

    // Accepted, then its status refused: HFS may still be importing, and the
    // engine cannot know when it stops. Open for good.
    hfs.lock().unwrap().refuse_status = true;
    std::fs::write(
        dir.path().join("inbox/b.ndjson"),
        "{\"resourceType\":\"Patient\",\"id\":\"p2\"}\n",
    )
    .unwrap();
    engine.tick().await;
    assert_eq!(engine.statuses(), ["sent", "rejected"]);
    let stamping = engine.stamping().await;
    assert_eq!(stamping["untagged"], "open");
    assert_eq!(stamping["untagged_until"], bound, "the last known bound");
}

#[tokio::test]
async fn an_untagged_submission_left_behind_by_a_replaced_source_stays_open() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (hfs_url, hfs) = hfs().await;
    hfs.lock().unwrap().never_done = true;
    let dir = root(DIRECT_SOURCE);
    let engine = Engine::start(dir.path(), &hfs_url, &python, true).await;
    std::fs::write(
        dir.path().join("inbox/a.ndjson"),
        "{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n",
    )
    .unwrap();
    engine.tick().await;
    engine.tick().await;
    assert_eq!(engine.stamping().await["untagged"], "open");

    // Replaced by a REST definition: nothing polls the old submission again.
    let rest: Value = serde_yaml::from_str(REST_SOURCE).unwrap();
    let replaced = reqwest::Client::new()
        .put(format!("{}/api/sources/lab", engine.api))
        .json(&rest)
        .send()
        .await
        .unwrap();
    assert!(
        replaced.status().is_success(),
        "{}",
        replaced.text().await.unwrap()
    );
    let stamping = engine.stamping().await;
    assert_eq!(stamping["active"], true);
    assert_eq!(stamping["untagged"], "open");
}
