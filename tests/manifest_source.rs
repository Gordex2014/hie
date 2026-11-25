//! The `manifest` source end to end: a stand-in FHIR server that behaves as the
//! Data Consumer of `$bulk-submit` (takes the kick-off, fetches the manifest and
//! the provider's files after answering, publishes receipts on status pages),
//! a stand-in provider, and the engine over a real listener, its runner
//! polling on its own.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

// ---- what the process logs -------------------------------------------------

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The process's log at INFO through the production filter, installed once;
/// also where the kick-off timeout of these tests is set.
fn logs() -> &'static Captured {
    static LOGS: OnceLock<Captured> = OnceLock::new();
    LOGS.get_or_init(|| {
        hie::manifest::set_kickoff_timeout(KICKOFF_TIMEOUT);
        let captured = Captured::default();
        let writer = captured.clone();
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(move || writer.clone())
                    .with_filter(hie::log_filter(Some("info"))),
            )
            .init();
        captured
    })
}

fn logged() -> String {
    String::from_utf8_lossy(&logs().0.lock().unwrap()).into_owned()
}

/// Shorter than the engine's 30 s, so a lost answer is a test of seconds.
const KICKOFF_TIMEOUT: Duration = Duration::from_secs(3);

// ---- the stand-in FHIR server and provider ---------------------------------

/// How the stand-in answers the next kick-off.
#[derive(Clone)]
enum Answer {
    /// `200`, then the manifest and its files are fetched.
    Accept,
    /// `200` for a manifest the server completes with no files.
    AcceptEmpty,
    /// A refusal, as given.
    Refuse {
        status: u16,
        body: String,
        content_type: &'static str,
        retry_after: Option<&'static str>,
    },
    /// Registers the submission (with its manifest when `manifest`, and
    /// completed when `completed`), then answers `status` after `after`.
    Late {
        after: Duration,
        status: u16,
        register: bool,
        manifest: bool,
        completed: bool,
    },
}

#[derive(Default)]
struct Sub {
    manifest_url: String,
    /// Whether the server registered the manifest, not just the submission.
    manifest: bool,
    /// Whether the submission was completed (a later kick-off is a `409`).
    completed: bool,
    fetched: bool,
    polls: u32,
    output: BTreeMap<String, Vec<String>>,
    outcomes: Vec<Value>,
    deleted: i64,
    page2_tripped: bool,
}

#[derive(Default)]
struct World {
    /// The server's base (kick-off, status request).
    hfs: String,
    /// Where status pages are (the server's public URL).
    pages: String,
    /// Where receipt files are (a pre-signed store).
    store: String,
    /// The provider: manifests and NDJSON files.
    provider: String,
    kickoffs: Vec<(HeaderMap, Value)>,
    /// Status-only kick-offs: the submission ids asked about.
    probes: Vec<String>,
    script: VecDeque<Answer>,
    /// Holds the next kick-off until notified.
    gate: Option<Arc<tokio::sync::Notify>>,
    subs: HashMap<String, Sub>,
    /// What fetching each manifest gave: (status, content type, body).
    fetches: Vec<(u16, Option<String>, String)>,
    files: HashMap<String, String>,
    manifests: HashMap<String, Value>,
    /// Polls answered `202` before the receipts.
    in_progress: u32,
    two_pages: bool,
    /// Page 2 answers `429` once.
    page2_429: bool,
    /// Polls answered `500` from now on, one each.
    poll_500: u32,
    /// Every poll answers `202` while set.
    hold: bool,

    /// Requests of status pages and receipt files: (path, had a bearer).
    page_requests: Vec<(String, bool)>,
    receipt_requests: Vec<(String, bool)>,
    /// One receipt line is not an OperationOutcome.
    malformed: bool,
    /// The manifest is fetched with a provider token, as a server may do.
    provider_token: bool,
}

type W = Arc<Mutex<World>>;

fn param<'a>(body: &'a Value, name: &str) -> Option<&'a Value> {
    body["parameter"]
        .as_array()?
        .iter()
        .find(|p| p["name"] == name)
}

fn outcome(severity: &str, code: &str, text: &str) -> Value {
    json!({"resourceType": "OperationOutcome", "issue": [{"severity": severity, "code": code, "diagnostics": text}]})
}

/// Fetches the manifest and every file it names, as the Data Consumer does.
async fn fetch(w: W, id: String, manifest_url: String) {
    let token = w.lock().unwrap().provider_token;
    let mut request = reqwest::Client::new().get(&manifest_url);
    if token {
        request = request.bearer_auth("PROVIDER-CANARY-TOKEN");
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_string());
    let text = response.text().await.unwrap();
    w.lock()
        .unwrap()
        .fetches
        .push((status, content_type, text.clone()));
    let mut output: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut outcomes = Vec::new();
    let mut deleted = 0;
    if status != 200 {
        outcomes.push(outcome(
            "error",
            "not-found",
            &format!("fetching the manifest {manifest_url} failed: HTTP {status}"),
        ));
    } else {
        let manifest: Value = serde_json::from_str(&text).unwrap();
        for entry in manifest["output"].as_array().unwrap() {
            let url = entry["url"].as_str().unwrap();
            let file = reqwest::get(url).await.unwrap();
            if !file.status().is_success() {
                outcomes.push(outcome(
                    "error",
                    "not-found",
                    &format!("fetching {url} failed: HTTP {}", file.status().as_u16()),
                ));
                continue;
            }
            for line in file
                .text()
                .await
                .unwrap()
                .lines()
                .filter(|l| !l.trim().is_empty())
            {
                let v: Value = serde_json::from_str(line).unwrap();
                match (v["resourceType"].as_str(), v["id"].as_str()) {
                    (Some(t), Some(i)) => {
                        output.entry(t.into()).or_default().push(format!("{t}/{i}"))
                    }
                    _ => outcomes.push(outcome("error", "processing", "resource has no id")),
                }
            }
        }
        for entry in manifest["deleted"].as_array().into_iter().flatten() {
            deleted += entry["count"].as_i64().unwrap_or(1);
        }
        outcomes.push(outcome("warning", "informational", "a warning only"));
    }
    let mut w = w.lock().unwrap();
    let sub = w.subs.get_mut(&id).unwrap();
    sub.output = output;
    sub.outcomes = outcomes;
    sub.deleted = deleted;
    sub.fetched = true;
}

fn register(w: &W, id: &str, manifest_url: &str, manifest: bool, completed: bool) {
    w.lock().unwrap().subs.insert(
        id.to_string(),
        Sub {
            manifest_url: manifest_url.to_string(),
            manifest,
            completed,
            fetched: !manifest,
            ..Sub::default()
        },
    );
    if manifest {
        tokio::spawn(fetch(w.clone(), id.to_string(), manifest_url.to_string()));
    }
}

fn accepted() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/fhir+json")],
        Json(outcome("information", "informational", "accepted")),
    )
        .into_response()
}

async fn kickoff(State(w): State<W>, headers: HeaderMap, body: String) -> Response {
    let body: Value = serde_json::from_str(&body).unwrap();
    let id = param(&body, "submissionId").unwrap()["valueString"]
        .as_str()
        .unwrap()
        .to_string();
    // A status-only kick-off, as HFS answers one: a completed submission
    // refuses it, an open one takes it and nothing changes.
    let Some(manifest_url) = param(&body, "manifestUrl") else {
        let mut w = w.lock().unwrap();
        w.probes.push(id.clone());
        assert!(
            param(&body, "fhirBaseUrl").is_none()
                && param(&body, "submissionStatus").unwrap()["valueCoding"]["code"]
                    == "in-progress"
        );
        return match w.subs.get(&id) {
            Some(sub) if sub.completed => (
                StatusCode::CONFLICT,
                Json(outcome("error", "conflict", "submission is complete")),
            )
                .into_response(),
            _ => accepted(),
        };
    };
    let manifest_url = manifest_url["valueUrl"].as_str().unwrap().to_string();
    let (answer, gate, repeat) = {
        let mut w = w.lock().unwrap();
        w.kickoffs.push((headers, body.clone()));
        let repeat = w.subs.contains_key(&id);
        (
            w.script.pop_front().unwrap_or(Answer::Accept),
            w.gate.take(),
            repeat,
        )
    };
    if let Some(gate) = gate {
        gate.notified().await;
    }
    match answer {
        // As HFS: a submission it already has is refused.
        Answer::Accept if repeat => (
            StatusCode::CONFLICT,
            Json(outcome("error", "conflict", "submission already exists")),
        )
            .into_response(),
        Answer::Accept => {
            register(&w, &id, &manifest_url, true, true);
            accepted()
        }
        Answer::AcceptEmpty => {
            register(&w, &id, &manifest_url, false, true);
            accepted()
        }
        Answer::Refuse {
            status,
            body,
            content_type,
            retry_after,
        } => {
            let mut response = (
                StatusCode::from_u16(status).unwrap(),
                [(header::CONTENT_TYPE, content_type)],
                body,
            )
                .into_response();
            if let Some(r) = retry_after {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, r.parse().unwrap());
            }
            response
        }
        Answer::Late {
            after,
            status,
            register: registers,
            manifest,
            completed,
        } => {
            if registers {
                register(&w, &id, &manifest_url, manifest, completed);
            }
            tokio::time::sleep(after).await;
            if status == 200 {
                accepted()
            } else {
                (
                    StatusCode::from_u16(status).unwrap(),
                    Json(outcome("error", "exception", "server trouble")),
                )
                    .into_response()
            }
        }
    }
}

async fn status_request(State(w): State<W>, body: String) -> Response {
    let body: Value = serde_json::from_str(&body).unwrap();
    let id = param(&body, "submissionId").unwrap()["valueString"]
        .as_str()
        .unwrap()
        .to_string();
    let (known, pages) = {
        let w = w.lock().unwrap();
        (w.subs.contains_key(&id), w.pages.clone())
    };
    if !known {
        return (
            StatusCode::NOT_FOUND,
            Json(outcome("error", "not-found", "no such submission")),
        )
            .into_response();
    }
    (
        StatusCode::ACCEPTED,
        [(
            "content-location",
            format!("{pages}/bulk-submit-status/{id}"),
        )],
    )
        .into_response()
}

fn bearer(headers: &HeaderMap) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .is_some_and(|v| v.to_str().unwrap().starts_with("Bearer "))
}

async fn poll(
    State(w): State<W>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let mut w = w.lock().unwrap();
    let page = q.get("page").cloned().unwrap_or_else(|| "1".into());
    w.page_requests.push((
        format!("/bulk-submit-status/{id}?page={page}"),
        bearer(&headers),
    ));
    if w.hold {
        return StatusCode::ACCEPTED.into_response();
    }
    if w.poll_500 > 0 {
        w.poll_500 -= 1;
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let (pages, store, in_progress, two_pages, page2_429) = (
        w.pages.clone(),
        w.store.clone(),
        w.in_progress,
        w.two_pages,
        w.page2_429,
    );
    let Some(sub) = w.subs.get_mut(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    sub.polls += 1;
    if !sub.fetched || sub.polls <= in_progress {
        return (StatusCode::ACCEPTED, [("x-progress", "fetching")]).into_response();
    }
    let manifest_url = sub.manifest_url.clone();
    let mut output = Vec::new();
    let mut outcome_entries = Vec::new();
    if sub.manifest {
        for (t, refs) in &sub.output {
            output.push(
                json!({"type": t, "url": format!("{store}/bulk-submit-file/{id}/output-{t}"),
                               "count": refs.len(), "manifestUrl": manifest_url}),
            );
        }
        outcome_entries.push(
            json!({"url": format!("{store}/bulk-submit-file/{id}/error"),
                                    "count": sub.outcomes.len(), "manifestUrl": manifest_url}),
        );
    }
    let deleted = if sub.deleted > 0 {
        vec![json!({"url": format!("{store}/bulk-submit-file/{id}/deleted"), "count": sub.deleted})]
    } else {
        vec![]
    };
    let body = if two_pages && page == "1" {
        json!({"submissionId": id, "output": output, "outcome": outcome_entries, "deleted": [],
               "link": [{"relation": "next", "url": format!("{pages}/bulk-submit-status/{id}?page=2")}]})
    } else if two_pages {
        if page2_429 && !sub.page2_tripped {
            sub.page2_tripped = true;
            return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "1")]).into_response();
        }
        json!({"submissionId": id, "output": [], "outcome": [], "deleted": deleted, "link": []})
    } else {
        json!({"submissionId": id, "output": output, "outcome": outcome_entries, "deleted": deleted, "link": []})
    };
    ([(header::CONTENT_TYPE, "application/json")], Json(body)).into_response()
}

async fn artifact(
    State(w): State<W>,
    Path((id, part)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let mut w = w.lock().unwrap();
    w.receipt_requests
        .push((format!("/bulk-submit-file/{id}/{part}"), bearer(&headers)));
    let malformed = w.malformed;
    let sub = w.subs.get(&id).unwrap();
    let mut body = String::new();
    if let Some(t) = part.strip_prefix("output-") {
        for r in sub.output.get(t).into_iter().flatten() {
            body.push_str(&json!({"reference": r}).to_string());
            body.push('\n');
        }
    } else if part == "error" {
        for o in &sub.outcomes {
            body.push_str(&o.to_string());
            body.push('\n');
        }
        if malformed {
            body.push_str("{\"resourceType\": \"Patient\", \"id\": \"oops\"}\n");
        }
    }
    ([(header::CONTENT_TYPE, "application/fhir+ndjson")], body).into_response()
}

async fn provider_file(State(w): State<W>, Path(name): Path<String>) -> Response {
    match w.lock().unwrap().files.get(&name) {
        Some(text) => (
            [(header::CONTENT_TYPE, "application/fhir+ndjson")],
            text.clone(),
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn provider_manifest(State(w): State<W>, Path(name): Path<String>) -> Response {
    match w.lock().unwrap().manifests.get(&name) {
        Some(m) => Json(m.clone()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// The engine's identity provider: one fixed token.
async fn idp_token() -> Json<Value> {
    Json(json!({"access_token": "ENGINE-CANARY-TOKEN", "token_type": "Bearer", "expires_in": 3600}))
}

async fn bind() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (listener, url)
}

/// The stand-in FHIR server, on three origins (its base, its public URL for
/// status pages, a store for receipt files), and the provider.
async fn fake() -> W {
    let w: W = Arc::default();
    let router = Router::new()
        .route("/$bulk-submit", post(kickoff))
        .route("/$bulk-submit-status", post(status_request))
        .route("/bulk-submit-status/{id}", get(poll))
        .route("/bulk-submit-file/{id}/{part}", get(artifact))
        .route("/provider/files/{name}", get(provider_file))
        .route("/provider/manifests/{name}", get(provider_manifest))
        .route("/token", post(idp_token))
        .with_state(w.clone());
    let mut urls = Vec::new();
    for _ in 0..3 {
        let (listener, url) = bind().await;
        let router = router.clone();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        urls.push(url);
    }
    {
        let mut world = w.lock().unwrap();
        world.hfs = urls[0].clone();
        world.pages = urls[1].clone();
        world.store = urls[2].clone();
        world.provider = format!("{}/provider", urls[2]);
        world.in_progress = 1;
        world.files.insert(
            "patients.ndjson".into(),
            "{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n{\"resourceType\":\"Patient\",\"id\":\"p2\"}\n".into(),
        );
        world.files.insert(
            "observations.ndjson".into(),
            "{\"resourceType\":\"Observation\",\"id\":\"o1\"}\n{\"resourceType\":\"Observation\"}\n".into(),
        );
    }
    w
}

// ---- the engine ------------------------------------------------------------

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

struct Engine {
    api: String,
    state: hie::api::AppState,
    manager: Arc<hie::manager::SourceManager>,
    root: tempfile::TempDir,
    http: reqwest::Client,
}

/// An engine calling the stand-in, with the engine's own credentials when
/// `credentials`, its definitions taken from `sources` (file name, YAML).
async fn engine_with(w: &W, credentials: bool, sources: &[(&str, &str)]) -> Engine {
    engine_config(w, credentials, sources, "").await
}

/// `engine_with`, with more of `hie.yaml`.
async fn engine_config(w: &W, credentials: bool, sources: &[(&str, &str)], more: &str) -> Engine {
    logs();
    let hfs = w.lock().unwrap().hfs.clone();
    let (listener, api) = bind().await;
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("sources")).unwrap();
    std::fs::create_dir_all(root.path().join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/fhir_passthrough.py"),
        root.path().join("transforms/fhir_passthrough.py"),
    )
    .unwrap();
    for (name, yaml) in sources {
        std::fs::write(root.path().join("sources").join(name), yaml).unwrap();
    }
    std::fs::write(
        root.path().join("hie.yaml"),
        format!("fhir_base_url: {hfs}\npublic_url: {api}\ndatabase: data/hie.db\n{more}"),
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.path().join("hie.yaml")).unwrap();
    let credentials = credentials.then(|| {
        Arc::new(hie::credentials::TokenSource::new(
            hie::credentials::CredentialSettings {
                token_url: format!("{hfs}/token"),
                client_id: "hie".into(),
                client_secret: "ENGINE-CANARY-SECRET".into(),
                scope: Some("system/bulk-submit".into()),
            },
        ))
    });
    let (manager, state) = hie::build(
        &config,
        feeds,
        &repo().join("hie_runtime"),
        None,
        credentials,
    )
    .unwrap();
    let router = hie::api::router(state.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Engine {
        api,
        state,
        manager,
        root,
        http: reqwest::Client::new(),
    }
}

async fn engine(w: &W) -> Engine {
    engine_with(w, false, &[]).await
}

impl Engine {
    async fn post(&self, path: &str, body: Value) -> (StatusCode, HeaderMap, Value) {
        let r = self
            .http
            .post(format!("{}{path}", self.api))
            .json(&body)
            .send()
            .await
            .unwrap();
        let (status, headers) = (r.status(), r.headers().clone());
        let text = r.text().await.unwrap();
        (
            status,
            headers,
            serde_json::from_str(&text).unwrap_or(Value::Null),
        )
    }

    async fn put(&self, path: &str, body: Value) -> (StatusCode, Value) {
        let r = self
            .http
            .put(format!("{}{path}", self.api))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = r.status();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        let r = self
            .http
            .get(format!("{}{path}", self.api))
            .send()
            .await
            .unwrap();
        let status = r.status();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    /// A manifest source, polling every second.
    async fn source(&self, id: &str, extra: Value) -> Value {
        let mut source = json!({"type": "manifest", "poll_seconds": 1});
        for (k, v) in extra.as_object().into_iter().flatten() {
            source[k] = v.clone();
        }
        let (status, _, body) = self
            .post(
                "/api/sources",
                json!({"id": id, "name": format!("Source {id}"), "source": source}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        body
    }

    async fn send(&self, id: &str, body: Value) -> (StatusCode, HeaderMap, Value) {
        self.post(&format!("/api/sources/{id}/manifests"), body)
            .await
    }

    /// The load once `done` says so, within 30 s.
    async fn wait_load(&self, id: i64, done: impl Fn(&Value) -> bool) -> Value {
        let started = Instant::now();
        loop {
            let (status, load) = self.get(&format!("/api/loads/{id}")).await;
            if status == StatusCode::OK && done(&load) {
                return load;
            }
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "load {id} never got there: {load}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn settled(&self, id: i64) -> Value {
        self.wait_load(id, |l| l["status"] != "processing").await
    }

    /// The source's events, oldest first, details parsed.
    fn events(&self, source: &str) -> Vec<Value> {
        self.state
            .db
            .query_json(
                "SELECT kind, load_id, record_id, reason, detail FROM events WHERE source_id = $1 ORDER BY id",
                hie::db::params![source],
            )
            .unwrap()
            .into_iter()
            .map(|mut e| {
                if let Some(Value::String(text)) = e.get("detail") {
                    e["detail"] = serde_json::from_str(text).unwrap();
                }
                e
            })
            .collect()
    }

    fn sql(&self, sql: &str) -> Vec<Value> {
        self.state.db.query_json(sql, []).unwrap()
    }
}

fn kinds(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .map(|e| e["kind"].as_str().unwrap().to_string())
        .collect()
}

fn url_manifest(w: &W, name: &str, files: &[(&str, &str)]) -> String {
    let mut w = w.lock().unwrap();
    let output: Vec<Value> = files
        .iter()
        .map(|(t, f)| json!({"type": t, "url": format!("{}/files/{f}", w.provider)}))
        .collect();
    w.manifests.insert(
        name.into(),
        json!({"transactionTime": "2026-11-25T10:00:00Z", "requiresAccessToken": false,
               "output": output, "error": [], "deleted": [{"type": "Patient", "url": "x", "count": 1}]}),
    );
    format!("{}/manifests/{name}", w.provider)
}

// ---- the definition ---------------------------------------------------------

#[tokio::test]
async fn a_manifest_source_is_defined_in_one_canonical_form() {
    let w = fake().await;
    let e = engine(&w).await;

    // Refused on what the definition holds, defaults and nulls included.
    for (definition, member) in [
        (
            json!({"transform": "transforms/fhir_passthrough.py"}),
            "transform",
        ),
        (json!({"transform": null}), "transform"),
        (json!({"expect_every": "1h"}), "expect_every"),
        (json!({"expect_every": null}), "expect_every"),
        (json!({"format": "csv"}), "format"),
        (json!({"delivery": {"method": "rest"}}), "delivery.method"),
        (
            json!({"delivery": {"method": "bulk_submit", "bulk_from": "auto"}}),
            "delivery.bulk_from",
        ),
        (
            json!({"delivery": {"batch_size": 100}}),
            "delivery.batch_size",
        ),
        (
            json!({"source": {"type": "manifest", "path": "inbox"}}),
            "source.path",
        ),
        (
            json!({"source": {"type": "manifest", "oauth_metadata_url": "/relative"}}),
            "oauth_metadata_url",
        ),
        (
            json!({"source": {"type": "manifest", "poll_seconds": 0}}),
            "poll_seconds",
        ),
        (
            json!({"source": {"type": "manifest", "poll_seconds": 3601}}),
            "poll_seconds",
        ),
    ] {
        let mut body = json!({"id": "refused", "name": "Refused", "source": {"type": "manifest"}});
        for (k, v) in definition.as_object().unwrap() {
            body[k] = v.clone();
        }
        let (status, _, answer) = e.post("/api/sources", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body} -> {answer}");
        assert_eq!(answer["code"], "invalid_definition", "{answer}");
        assert!(
            answer["error"].as_str().unwrap().contains(member),
            "{member} not named: {answer}"
        );
    }

    // The least definition and the fullest write the same file, read back as
    // `definition`, which PUT takes as it is.
    let canonical = "id: export\nname: Provider export\nsource:\n  type: manifest\n  poll_seconds: 10\nformat: ndjson\ndelivery:\n  method: bulk_submit\n";
    let (status, _, created) = e
        .post(
            "/api/sources",
            json!({"id": "export", "name": "Provider export", "source": {"type": "manifest"}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let file = e.root.path().join("sources/export.yaml");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), canonical);
    assert_eq!(created["source"], "manifest");
    assert_eq!(created["format"], "ndjson");
    assert_eq!(created["transform"], Value::Null);
    assert_eq!(created["poll_seconds"], 10);
    assert_eq!(created["expect_every"], Value::Null);
    assert_eq!(created["freshness"], "not_monitored");
    assert_eq!(created["state"], "receiving");
    assert_eq!(created["last_received_at"], Value::Null);
    assert_eq!(created["stamping"]["active"], false);
    assert_eq!(created["stamping"]["reason"], "off");
    let (status, _, _) = e
        .post(
            "/api/sources",
            json!({"id": "full", "name": "Provider export", "contact": "Data team, +1 555 0100",
                   "source": {"type": "manifest", "poll_seconds": 10, "oauth_metadata_url": "https://provider.example/.well-known/smart-configuration"},
                   "format": "ndjson", "delivery": {"method": "bulk_submit"}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let full = std::fs::read_to_string(e.root.path().join("sources/full.yaml")).unwrap();
    assert_eq!(
        full,
        "id: full\nname: Provider export\ncontact: Data team, +1 555 0100\nsource:\n  type: manifest\n  poll_seconds: 10\n  oauth_metadata_url: https://provider.example/.well-known/smart-configuration\nformat: ndjson\ndelivery:\n  method: bulk_submit\n"
    );
    let (_, read) = e.get("/api/sources/full").await;
    let mut definition = read["definition"].clone();
    definition["contact"] = json!("Data team, +1 555 0199");
    let (status, replaced) = e.put("/api/sources/full", definition).await;
    assert_eq!(status, StatusCode::OK, "{replaced}");
    assert_eq!(
        std::fs::read_to_string(e.root.path().join("sources/full.yaml")).unwrap(),
        full.replace("0100", "0199")
    );

    // No rule is made for it, at its creation or at a start.
    let (_, rules) = e.get("/api/alert-rules?source=export").await;
    assert_eq!(rules["rules"], json!([]), "{rules}");

    // The kind stays: no replace to or from manifest.
    let (status, refused) = e
        .put(
            "/api/sources/export",
            json!({"id": "export", "name": "X", "source": {"type": "folder", "path": "inbox"},
                   "transform": "transforms/fhir_passthrough.py"}),
        )
        .await;
    assert_eq!(
        (status, refused["code"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid_definition"))
    );
    let (status, _, _) = e
        .post(
            "/api/sources",
            json!({"id": "folder", "name": "F", "source": {"type": "folder", "path": "inbox"},
                   "transform": "transforms/fhir_passthrough.py"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, refused) = e
        .put(
            "/api/sources/folder",
            json!({"id": "folder", "name": "F", "source": {"type": "manifest"}}),
        )
        .await;
    assert_eq!(
        (status, refused["code"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid_definition"))
    );

    // There is no connection to test.
    let (status, _, refused) = e
        .post("/api/sources/test", json!({"source": {"type": "manifest"}}))
        .await;
    assert_eq!(
        (status, refused["code"].as_str()),
        (StatusCode::BAD_REQUEST, Some("not_supported"))
    );

    // The files load again at a start, with no rule; one with a transform stops it.
    let (_, feeds) = hie::config::load(&e.root.path().join("hie.yaml")).unwrap();
    assert!(feeds.iter().any(|f| f.id == "export" && f.is_manifest()));
    let born = fake().await;
    let again = engine_config(
        &born,
        false,
        &[("disk.yaml", canonical.replace("export", "disk").as_str())],
        "delivery:\n  stamp_source: true\n",
    )
    .await;
    let (_, rules) = again.get("/api/alert-rules?source=disk").await;
    assert_eq!(rules["rules"], json!([]), "{rules}");
    // With stamping on, what the FHIR server loads is still never tagged.
    let (_, disk) = again.get("/api/sources/disk").await;
    assert_eq!(disk["stamping"]["active"], false);
    assert_eq!(disk["stamping"]["reason"], "direct_bulk");
    std::fs::write(
        again.root.path().join("sources/bad.yaml"),
        "id: bad\nname: Bad\nsource:\n  type: manifest\ntransform: t.py\n",
    )
    .unwrap();
    let e2 = hie::config::load(&again.root.path().join("hie.yaml")).unwrap_err();
    let text = format!("{e2:#}");
    assert!(
        text.contains("bad.yaml") && text.contains("transform"),
        "{text}"
    );
}

// ---- by URL -------------------------------------------------------------------

#[tokio::test]
async fn a_manifest_url_is_forwarded_unchanged_and_loads_from_its_receipts() {
    let w = fake().await;
    w.lock().unwrap().two_pages = true;
    w.lock().unwrap().page2_429 = true;
    let e = engine(&w).await;
    e.source("export", json!({})).await;
    let url = url_manifest(
        &w,
        "m1",
        &[
            ("Patient", "patients.ndjson"),
            ("Observation", "observations.ndjson"),
            ("Patient", "missing.ndjson"),
        ],
    );
    let signed = format!("{url}?sig=SIGNED-CANARY&expires=1");
    let (status, headers, load) = e
        .send(
            "export",
            json!({"manifest_url": signed, "request_id": "first"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{load}");
    let id = load["id"].as_i64().unwrap();
    assert_eq!(headers[header::LOCATION], format!("/api/loads/{id}"));
    assert_eq!(load["status"], "processing");
    assert_eq!(load["manifest"]["kickoff"], "accepted");
    assert_eq!(load["manifest"]["by"], "url");
    assert_eq!(load["manifest"]["request_id"], "first");
    assert_eq!(load["name"], format!("{url}?…"));
    assert_eq!(load["size"], Value::Null);
    assert_eq!(load["method"], "bulk_submit");

    // What the FHIR server was handed.
    let (kickoff_headers, kickoff) = w.lock().unwrap().kickoffs[0].clone();
    assert_eq!(
        param(&kickoff, "submitter").unwrap()["valueIdentifier"],
        json!({"system": "urn:helios:hie:source", "value": "export"})
    );
    let submission = param(&kickoff, "submissionId").unwrap()["valueString"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        submission.starts_with("hie-manifest-") && submission.len() == 13 + 16,
        "{submission}"
    );
    assert_eq!(load["manifest"]["submission_id"], submission);
    assert_eq!(param(&kickoff, "manifestUrl").unwrap()["valueUrl"], signed);
    let provider_origin = w.lock().unwrap().store.clone();
    assert_eq!(
        param(&kickoff, "fhirBaseUrl").unwrap()["valueUrl"],
        provider_origin
    );
    assert_eq!(
        param(&kickoff, "submissionStatus").unwrap()["valueCoding"]["code"],
        "completed"
    );
    assert!(param(&kickoff, "oauthMetadataUrl").is_none());
    assert!(kickoff_headers.get("x-tenant-id").is_none());

    // Its receipts: two pages, the second throttled once; the first page's
    // groups are kept across it.
    let load = e.settled(id).await;
    assert_eq!(load["status"], "done", "{load}");
    assert_eq!(load["sent"], 3);
    assert_eq!(
        load["rejected"], 2,
        "a missing file and a line without id; the warning is not counted"
    );
    assert_eq!(load["failing"], 2);
    assert_eq!(load["records"], 5);
    assert_eq!(load["in_progress"], 0);
    assert_eq!(load["failed"], true);
    assert_eq!(
        load["manifest"]["sent_by_type"],
        json!({"Observation": 1, "Patient": 2})
    );
    assert_eq!(load["manifest"]["deleted"], 1);
    assert_eq!(load["duplicate_of"], Value::Null);
    assert_eq!(load["archived"], Value::Null);
    let failures = load["failures"].as_array().unwrap();
    assert_eq!(failures.len(), 2, "{failures:?}");
    let missing = failures.iter().find(|f| f["code"] == "not-found").unwrap();
    assert_eq!(missing["kind"], "server");
    assert_eq!(missing["severity"], "error");
    assert_eq!(missing["count"], 1);
    assert_eq!(missing["first_line"], Value::Null);
    assert_eq!(missing["samples"], json!([]));
    assert!(
        missing["reason"]
            .as_str()
            .unwrap()
            .contains(&format!("{provider_origin}/…")),
        "{missing}"
    );
    assert!(w.lock().unwrap().subs[&submission].page2_tripped);

    // Its events: no per-record ones.
    let events = e.events("export");
    assert_eq!(
        kinds(&events),
        [
            "load.received",
            "submission.opened",
            "submission.settled",
            "load.finished"
        ]
    );
    assert_eq!(events[0]["detail"]["name"], format!("{url}?…"));
    assert_eq!(events[0]["detail"]["size"], Value::Null);
    assert_eq!(events[2]["detail"]["sent"], 3);
    assert_eq!(events[2]["detail"]["rejected"], 2);
    let (_, records) = e.get(&format!("/api/records?file_id={id}")).await;
    assert_eq!(records["records"], json!([]));

    // Where it is counted.
    let (_, source) = e.get("/api/sources/export").await;
    assert_eq!(source["failing_last_24h"], 2);
    assert_eq!(source["rejected_last_24h"], 2);
    assert!(source["last_received_at"].is_string());
    for key in ["received", "pending", "submitted", "sent", "rejected"] {
        assert_eq!(source["records"][key], 0, "{key}: {source}");
    }
    let (_, failed) = e.get("/api/loads?status=failed").await;
    assert_eq!(failed["loads"][0]["id"], id);
    let (_, failed) = e.get("/api/loads?status=failed&source=export").await;
    assert_eq!(failed["loads"][0]["id"], id);
    let (_, stats) = e.get("/api/sources/export/stats").await;
    assert_eq!(stats["today"]["files"], 1);
    assert_eq!(stats["today"]["sent"], 3);
    assert_eq!(stats["today"]["rejected"], 2);
    assert_eq!(stats["today"]["failing"], 2);
    assert_eq!(stats["today"]["pending"], 0);
    let (_, monitoring) = e.get("/api/monitoring/status").await;
    assert_eq!(monitoring["state"], "needs_you");
    assert!(
        monitoring["needs_you"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["id"] == "export")
    );
    assert_eq!(monitoring["last_24h"]["sent"], 3);
    assert_eq!(monitoring["last_24h"]["failing"], 2);

    // No Retry: it is sent again instead; the same key answers the same load.
    let (status, _, refused) = e.post(&format!("/api/loads/{id}/retry"), json!({})).await;
    assert_eq!(
        (status, refused["code"].as_str()),
        (StatusCode::CONFLICT, Some("not_retryable"))
    );
    let (status, _, again) = e
        .send(
            "export",
            json!({"manifest_url": signed, "request_id": "first"}),
        )
        .await;
    assert_eq!((status, again["id"].as_i64()), (StatusCode::OK, Some(id)));
    let (status, _, conflict) = e
        .send(
            "export",
            json!({"manifest_url": url, "request_id": "first"}),
        )
        .await;
    assert_eq!(
        (status, conflict["code"].as_str()),
        (StatusCode::CONFLICT, Some("request_id_conflict"))
    );
    assert_eq!(
        w.lock().unwrap().kickoffs.len(),
        1,
        "a replay sends nothing"
    );

    // The signature is never readable: not in the stored name, the events or the log.
    let stored = e.sql("SELECT name FROM files UNION ALL SELECT COALESCE(detail, '') FROM events UNION ALL SELECT COALESCE(error, '') FROM submissions");
    assert!(
        !format!("{stored:?}").contains("SIGNED-CANARY"),
        "{stored:?}"
    );
    assert!(!logged().contains("SIGNED-CANARY"));
}

#[tokio::test]
async fn every_send_is_its_own_submission() {
    let w = fake().await;
    let e = engine(&w).await;
    e.source(
        "export",
        json!({"oauth_metadata_url": "https://provider.example/.well-known/smart-configuration"}),
    )
    .await;
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    let (s1, _, first) = e.send("export", json!({"manifest_url": url})).await;
    let (s2, _, second) = e.send("export", json!({"manifest_url": url})).await;
    assert_eq!(
        (s1, s2),
        (StatusCode::CREATED, StatusCode::CREATED),
        "{first} {second}"
    );
    assert_ne!(
        first["manifest"]["submission_id"],
        second["manifest"]["submission_id"]
    );
    for load in [&first, &second] {
        let done = e.settled(load["id"].as_i64().unwrap()).await;
        assert_eq!(
            (
                done["status"].as_str(),
                done["sent"].as_i64(),
                done["failed"].as_bool()
            ),
            (Some("done"), Some(2), Some(false)),
            "{done}"
        );
    }
    for (_, kickoff) in &w.lock().unwrap().kickoffs {
        assert_eq!(
            param(kickoff, "oauthMetadataUrl").unwrap()["valueUrl"],
            "https://provider.example/.well-known/smart-configuration"
        );
    }
    let (_, loads) = e.get("/api/sources/export/loads").await;
    assert_eq!(loads["loads"].as_array().unwrap().len(), 2);
}

// ---- by upload ------------------------------------------------------------------

#[tokio::test]
async fn an_uploaded_manifest_is_served_while_its_load_is_open() {
    let w = fake().await;
    w.lock().unwrap().provider_token = true;
    let e = engine(&w).await;
    e.source("upload", json!({})).await;
    let provider = w.lock().unwrap().provider.clone();
    let manifest = json!({"transactionTime": "2026-11-25T10:00:00Z", "requiresAccessToken": false,
        "output": [{"type": "Patient", "url": format!("{provider}/files/patients.ndjson")}], "error": []});
    let r = e
        .http
        .post(format!("{}/api/sources/upload/manifests", e.api))
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
        .body(json!({"manifest": manifest, "file_name": "export.json"}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);
    let load: Value = r.json().await.unwrap();
    let id = load["id"].as_i64().unwrap();
    assert_eq!(load["name"], "export.json");
    assert_eq!(load["manifest"]["by"], "upload");
    let kickoff = w.lock().unwrap().kickoffs[0].1.clone();
    let served_at = param(&kickoff, "manifestUrl").unwrap()["valueUrl"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        served_at.starts_with(&format!("{}/bulk/", e.api)) && served_at.ends_with("/manifest.json"),
        "{served_at}"
    );
    assert_eq!(
        param(&kickoff, "fhirBaseUrl").unwrap()["valueUrl"],
        provider.trim_end_matches("/provider")
    );
    let done = e.settled(id).await;
    assert_eq!(
        (done["status"].as_str(), done["sent"].as_i64()),
        (Some("done"), Some(2)),
        "{done}"
    );
    // Fetched by the server while the load was open, as the engine stores it.
    let (status, content_type, body) = w.lock().unwrap().fetches[0].clone();
    assert_eq!(status, 200);
    assert_eq!(content_type.as_deref(), Some("application/json"));
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap(), manifest);
    assert_eq!(done["size"], body.len());
    // Gone once the receipts are in.
    let gone = reqwest::get(&served_at).await.unwrap();
    assert_eq!(gone.status(), StatusCode::NOT_FOUND);
    // A token the server presents to the engine's URL is not read or logged.
    assert!(!logged().contains("PROVIDER-CANARY-TOKEN"));
}

// ---- refusals ---------------------------------------------------------------------

#[tokio::test]
async fn a_refused_kickoff_comes_back_as_the_fhir_server_sent_it() {
    let w = fake().await;
    let e = engine(&w).await;
    e.source("export", json!({})).await;
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    let cases = [
        (
            429,
            "{ \"resourceType\" : \"OperationOutcome\", \"issue\": [{\"severity\":\"error\",\"code\":\"throttled\"}] }\n",
            "application/fhir+json",
            Some("7"),
        ),
        (
            501,
            "{\"resourceType\":\"OperationOutcome\",\"issue\":[{\"severity\":\"error\",\"code\":\"not-supported\"}]}",
            "application/json",
            None,
        ),
        (
            409,
            "{\"resourceType\":\"OperationOutcome\",\"issue\":[{\"severity\":\"error\",\"code\":\"conflict\"}]}",
            "application/fhir+json",
            None,
        ),
        (
            422,
            "{\"resourceType\":\"OperationOutcome\",\"issue\":[{\"severity\":\"error\",\"code\":\"invalid\"}]}",
            "application/fhir+json",
            None,
        ),
    ];
    for (status, body, content_type, retry_after) in cases {
        w.lock().unwrap().script.push_back(Answer::Refuse {
            status,
            body: body.into(),
            content_type,
            retry_after,
        });
        let r = e
            .http
            .post(format!("{}/api/sources/export/manifests", e.api))
            .json(&json!({"manifest_url": url, "request_id": format!("r{status}")}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status().as_u16(), status);
        assert_eq!(r.headers()["x-hie-refused-by"], "destination");
        assert_eq!(r.headers()[header::CONTENT_TYPE], content_type);
        assert_eq!(
            r.headers()
                .get(header::RETRY_AFTER)
                .map(|v| v.to_str().unwrap().to_string()),
            retry_after.map(str::to_string)
        );
        assert_eq!(r.text().await.unwrap(), body, "byte for byte");
    }
    // No load is left, nothing is recorded, and the key is free again.
    assert_eq!(e.sql("SELECT COUNT(*) AS n FROM files")[0]["n"], 0);
    assert_eq!(e.sql("SELECT COUNT(*) AS n FROM submissions")[0]["n"], 0);
    assert!(e.events("export").is_empty());
    let (_, loads) = e.get("/api/loads").await;
    assert_eq!(loads["loads"], json!([]));
    let (status, _, load) = e
        .send("export", json!({"manifest_url": url, "request_id": "r429"}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{load}");
}

#[tokio::test]
async fn the_engines_own_token_refused_is_its_own_answer_and_logged_safely() {
    let w = fake().await;
    let e = engine_with(&w, true, &[]).await;
    e.source("export", json!({})).await;
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    for (status, body) in [
        (
            401,
            json!({"resourceType": "OperationOutcome", "issue": [{"severity": "error", "code": "security",
                "details": {"text": "token for client hie (Bearer ENGINE-CANARY-TOKEN) rejected by https://idp.example/realms/x/token?client_secret=abc"}}]})
            .to_string(),
        ),
        (403, "<html>PAGE-CANARY forbidden</html>".to_string()),
    ] {
        w.lock().unwrap().script.push_back(Answer::Refuse {
            status,
            body,
            content_type: "application/fhir+json",
            retry_after: None,
        });
        let (code, headers, body) = e.send("export", json!({"manifest_url": url})).await;
        assert_eq!(code, StatusCode::BAD_GATEWAY);
        assert_eq!(body["code"], "destination_auth", "{body}");
        assert!(headers.get("x-hie-refused-by").is_none());
        assert!(body.get("resourceType").is_none());
    }
    // The engine sent its own token to the FHIR server.
    assert!(bearer(&w.lock().unwrap().kickoffs[0].0));
    let log = logged();
    let warned: Vec<&str> = log
        .lines()
        .filter(|l| l.contains("refused the engine's token"))
        .collect();
    assert!(
        warned.iter().any(|l| l.contains("HTTP 401: error security:") && l.contains("https://idp.example/…")),
        "{warned:?}"
    );
    assert!(
        warned
            .iter()
            .any(|l| l.contains("HTTP 403, a body of 34 bytes")),
        "{warned:?}"
    );
    for leak in [
        "client_secret=abc",
        "realms/x",
        "ENGINE-CANARY-TOKEN",
        "ENGINE-CANARY-SECRET",
        "PAGE-CANARY",
        "authorization",
    ] {
        assert!(
            !log.to_ascii_lowercase()
                .contains(&leak.to_ascii_lowercase()),
            "{leak} logged"
        );
    }
    assert_eq!(e.sql("SELECT COUNT(*) AS n FROM files")[0]["n"], 0);
}

#[tokio::test]
async fn the_engines_own_errors_answer_with_their_codes() {
    let w = fake().await;
    let e = engine(&w).await;
    e.source("export", json!({})).await;
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    let r = e
        .http
        .post(format!("{}/api/sources/export/manifests", e.api))
        .header(header::CONTENT_TYPE, "text/plain")
        .body(json!({"manifest_url": url}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(
        r.json::<Value>().await.unwrap()["code"],
        "unsupported_media_type"
    );
    let big = format!(
        "{{\"manifest_url\": \"{url}\", \"pad\": \"{}\"}}",
        "x".repeat(4 << 20)
    );
    let r = e
        .http
        .post(format!("{}/api/sources/export/manifests", e.api))
        .header(header::CONTENT_TYPE, "application/json")
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(r.json::<Value>().await.unwrap()["code"], "body_too_large");
    let r = e
        .http
        .post(format!("{}/api/sources/export/manifests", e.api))
        .header(header::CONTENT_TYPE, "application/json")
        .body("not json")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert_eq!(r.json::<Value>().await.unwrap()["code"], "invalid_body");
    let output = json!([{"type": "Patient", "url": "https://p.example/f.ndjson"}]);
    for (body, code, field) in [
        (json!({}), "invalid_body", Value::Null),
        (
            json!({"manifest_url": url, "manifest": {"output": output}}),
            "invalid_body",
            Value::Null,
        ),
        (
            json!({"manifest_url": url, "extra": 1}),
            "invalid_body",
            Value::Null,
        ),
        (
            json!({"manifest_url": "ftp://p.example/m.json"}),
            "invalid_body",
            json!("manifest_url"),
        ),
        (
            json!({"manifest_url": url, "file_name": "m.json"}),
            "invalid_body",
            json!("file_name"),
        ),
        (
            json!({"manifest": {"output": output}, "file_name": ""}),
            "invalid_body",
            json!("file_name"),
        ),
        (
            json!({"manifest": {"output": output}, "file_name": "a\nb"}),
            "invalid_body",
            json!("file_name"),
        ),
        (
            json!({"manifest_url": url, "request_id": "has space"}),
            "invalid_body",
            json!("request_id"),
        ),
        (
            json!({"manifest_url": url, "request_id": "x".repeat(129)}),
            "invalid_body",
            json!("request_id"),
        ),
        (
            json!({"manifest": {"output": []}}),
            "invalid_manifest",
            Value::Null,
        ),
        (
            json!({"manifest": {"output": [{"url": "https://p.example/f"}]}}),
            "invalid_manifest",
            Value::Null,
        ),
        (
            json!({"manifest": {"output": [{"type": "Patient", "url": "/relative"}]}}),
            "invalid_manifest",
            Value::Null,
        ),
        (json!({"manifest": []}), "invalid_manifest", Value::Null),
    ] {
        let (status, _, answer) = e.send("export", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body} -> {answer}");
        assert_eq!(answer["code"], code, "{body} -> {answer}");
        if code == "invalid_body" {
            assert_eq!(answer["field"], field, "{body} -> {answer}");
        }
    }
    let (status, _, answer) = e.send("nope", json!({"manifest_url": url})).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{answer}");
    let (status, _, _) = e
        .post(
            "/api/sources",
            json!({"id": "folder", "name": "F", "source": {"type": "folder", "path": "inbox", "poll_seconds": 3600},
                   "transform": "transforms/fhir_passthrough.py"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _, answer) = e.send("folder", json!({"manifest_url": url})).await;
    assert_eq!(
        (status, answer["code"].as_str()),
        (StatusCode::BAD_REQUEST, Some("not_a_manifest_source"))
    );
    e.manager.pause("export", None).unwrap();
    let (status, _, answer) = e.send("export", json!({"manifest_url": url})).await;
    assert_eq!(
        (status, answer["code"].as_str()),
        (StatusCode::CONFLICT, Some("source_paused"))
    );
    e.manager.resume("export").unwrap();

    // A refusal over 64 KiB is not passed through.
    w.lock().unwrap().script.push_back(Answer::Refuse {
        status: 400,
        body: "x".repeat(70 << 10),
        content_type: "text/plain",
        retry_after: None,
    });
    let (status, _, answer) = e.send("export", json!({"manifest_url": url})).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(answer["code"], "destination_answer_too_large");
    assert_eq!(answer["destination_status"], 400);
    assert!(
        w.lock().unwrap().kickoffs.len() == 1,
        "only the last one was sent"
    );
    assert_eq!(
        e.sql("SELECT COUNT(*) AS n FROM files WHERE feed_id = 'export'")[0]["n"],
        0
    );
}

#[tokio::test]
async fn nothing_sent_and_no_token_are_engine_errors() {
    // A FHIR server that cannot be reached.
    let w = fake().await;
    let (dead, dead_url) = bind().await;
    drop(dead);
    w.lock().unwrap().hfs = dead_url;
    let e = engine(&w).await;
    e.source("export", json!({})).await;
    let (status, _, answer) = e
        .send(
            "export",
            json!({"manifest_url": "https://p.example/m.json"}),
        )
        .await;
    assert_eq!(
        (status, answer["code"].as_str()),
        (StatusCode::BAD_GATEWAY, Some("destination_unreachable")),
        "{answer}"
    );
    assert_eq!(e.sql("SELECT COUNT(*) AS n FROM files")[0]["n"], 0);

    // An engine whose token endpoint cannot be reached.
    let w = fake().await;
    let e = engine_with(&w, true, &[]).await;
    e.source("export", json!({})).await;
    let hfs = w.lock().unwrap().hfs.clone();
    let (dead, dead_url) = bind().await;
    drop(dead);
    // The token endpoint is the server's `/token`: move the server's base away
    // from it by making a new engine against a dead identity provider.
    let _ = (hfs, dead_url.clone());
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("hie.yaml"),
        format!(
            "fhir_base_url: {}\npublic_url: http://127.0.0.1:9\ndatabase: data/hie.db\n",
            w.lock().unwrap().hfs
        ),
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.path().join("hie.yaml")).unwrap();
    let credentials = Some(Arc::new(hie::credentials::TokenSource::new(
        hie::credentials::CredentialSettings {
            token_url: format!("{dead_url}/token?client_secret=QUERY-CANARY"),
            client_id: "hie".into(),
            client_secret: "s".into(),
            scope: None,
        },
    )));
    let (_m, state) = hie::build(
        &config,
        feeds,
        &repo().join("hie_runtime"),
        None,
        credentials,
    )
    .unwrap();
    let (listener, api) = bind().await;
    let router = hie::api::router(state);
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let http = reqwest::Client::new();
    let r = http
        .post(format!("{api}/api/sources"))
        .json(&json!({"id": "export", "name": "E", "source": {"type": "manifest"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);
    let r = http
        .post(format!("{api}/api/sources/export/manifests"))
        .json(&json!({"manifest_url": "https://p.example/m.json"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(r.json::<Value>().await.unwrap()["code"], "destination_auth");
    assert!(w.lock().unwrap().kickoffs.is_empty(), "nothing was sent");
    assert!(!logged().contains("QUERY-CANARY"));
}

// ---- lost answers ------------------------------------------------------------------

#[tokio::test]
async fn a_lost_answer_is_confirmed_with_the_status_request() {
    let w = fake().await;
    let e = engine(&w).await;
    e.source("export", json!({})).await;
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    // The server took it, then: did not answer in time, answered 503, 408.
    for (after, status) in [
        (KICKOFF_TIMEOUT + Duration::from_secs(1), 200),
        (Duration::ZERO, 503),
        (Duration::ZERO, 408),
    ] {
        w.lock().unwrap().script.push_back(Answer::Late {
            after,
            status,
            register: true,
            manifest: true,
            completed: true,
        });
        let (code, headers, load) = e.send("export", json!({"manifest_url": url})).await;
        assert_eq!(code, StatusCode::ACCEPTED, "{status}: {load}");
        let id = load["id"].as_i64().unwrap();
        assert_eq!(headers[header::LOCATION], format!("/api/loads/{id}"));
        assert_eq!(load["manifest"]["kickoff"], "unconfirmed");
        assert_eq!(load["status"], "processing");
        let done = e.settled(id).await;
        assert_eq!(
            (done["status"].as_str(), done["sent"].as_i64()),
            (Some("done"), Some(2)),
            "{done}"
        );
        assert_eq!(done["manifest"]["kickoff"], "accepted");
        let events: Vec<String> = kinds(
            &e.events("export")
                .into_iter()
                .filter(|ev| ev["load_id"] == id)
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            events,
            [
                "load.received",
                "submission.opened",
                "submission.settled",
                "load.finished"
            ]
        );
    }

    // Never received: once the server has had time to finish, the load is unreadable.
    w.lock().unwrap().script.push_back(Answer::Late {
        after: Duration::ZERO,
        status: 503,
        register: false,
        manifest: false,
        completed: false,
    });
    let (code, _, load) = e.send("export", json!({"manifest_url": url})).await;
    assert_eq!(code, StatusCode::ACCEPTED);
    let id = load["id"].as_i64().unwrap();
    // Asked, found unknown, and asked again later rather than believed.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (_, still) = e.get(&format!("/api/loads/{id}")).await;
    assert_eq!(still["status"], "processing", "{still}");
    e.sql(&format!("UPDATE submissions SET created_at = created_at - 600000, next_attempt_at = 0 WHERE load_id = {id} RETURNING token"));
    let gone = e.settled(id).await;
    assert_eq!(gone["status"], "unreadable");
    assert_eq!(gone["failed"], true);
    assert_eq!(
        gone["error"],
        "the FHIR server did not receive this manifest; send it again"
    );

    assert!(
        w.lock().unwrap().probes.is_empty(),
        "an unknown submission is never probed"
    );

    // Known, with no file listed, and completed: a manifest with no files.
    w.lock().unwrap().script.push_back(Answer::Late {
        after: Duration::ZERO,
        status: 503,
        register: true,
        manifest: false,
        completed: true,
    });
    let (_, _, load) = e.send("export", json!({"manifest_url": url})).await;
    let done = e.settled(load["id"].as_i64().unwrap()).await;
    assert_eq!(
        (
            done["status"].as_str(),
            done["sent"].as_i64(),
            done["failed"].as_bool()
        ),
        (Some("done"), Some(0), Some(false)),
        "{done}"
    );
    assert_eq!(
        w.lock().unwrap().probes,
        [done["manifest"]["submission_id"].as_str().unwrap()]
    );

    // Known, with no file listed, and still open: a kick-off cut before its
    // manifest. Asked again until the grace period ends, then unreadable.
    w.lock().unwrap().script.push_back(Answer::Late {
        after: Duration::ZERO,
        status: 503,
        register: true,
        manifest: false,
        completed: false,
    });
    let (_, _, load) = e.send("export", json!({"manifest_url": url})).await;
    let id = load["id"].as_i64().unwrap();
    let started = Instant::now();
    while w.lock().unwrap().probes.len() < 2 {
        assert!(started.elapsed() < Duration::from_secs(15), "never probed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (_, open) = e.get(&format!("/api/loads/{id}")).await;
    assert_eq!(open["status"], "processing", "{open}");
    e.sql(&format!("UPDATE submissions SET created_at = created_at - 600000, next_attempt_at = 0 WHERE load_id = {id} RETURNING token"));
    let gone = e.settled(id).await;
    assert_eq!(gone["status"], "unreadable", "{gone}");
    assert_eq!(
        gone["error"],
        "the FHIR server did not receive this manifest; send it again"
    );
}

#[tokio::test]
async fn an_accepted_manifest_with_no_files_is_done_without_a_probe() {
    let w = fake().await;
    let e = engine(&w).await;
    e.source("export", json!({})).await;
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    w.lock().unwrap().script.push_back(Answer::AcceptEmpty);
    let (status, _, load) = e.send("export", json!({"manifest_url": url})).await;
    assert_eq!(status, StatusCode::CREATED);
    let done = e.settled(load["id"].as_i64().unwrap()).await;
    assert_eq!(
        (
            done["status"].as_str(),
            done["records"].as_i64(),
            done["failed"].as_bool()
        ),
        (Some("done"), Some(0), Some(false)),
        "{done}"
    );
    assert!(w.lock().unwrap().probes.is_empty());
}

#[tokio::test]
async fn a_load_is_invisible_while_its_kickoff_is_sent_and_a_repeat_waits() {
    let w = fake().await;
    let e = engine(&w).await;
    e.source("export", json!({})).await;
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    let gate = Arc::new(tokio::sync::Notify::new());
    w.lock().unwrap().gate = Some(gate.clone());
    let sending = {
        let (http, api, url) = (e.http.clone(), e.api.clone(), url.clone());
        tokio::spawn(async move {
            http.post(format!("{api}/api/sources/export/manifests"))
                .json(&json!({"manifest_url": url, "request_id": "held"}))
                .send()
                .await
                .unwrap()
                .status()
        })
    };
    let started = Instant::now();
    while w.lock().unwrap().kickoffs.is_empty() {
        assert!(started.elapsed() < Duration::from_secs(5));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let id = e.sql("SELECT id FROM files")[0]["id"].as_i64().unwrap();
    let (status, _, answer) = e
        .send("export", json!({"manifest_url": url, "request_id": "held"}))
        .await;
    assert_eq!(
        (status, answer["code"].as_str()),
        (StatusCode::CONFLICT, Some("request_in_progress"))
    );
    for path in [
        "/api/sources/export/loads",
        "/api/loads",
        "/api/loads?status=processing",
        "/api/loads?source=export",
    ] {
        let (_, page) = e.get(path).await;
        assert_eq!(page["loads"], json!([]), "{path}: {page}");
    }
    assert_eq!(
        e.get(&format!("/api/loads/{id}")).await.0,
        StatusCode::NOT_FOUND
    );
    let (_, source) = e.get("/api/sources/export").await;
    assert_eq!(source["last_received_at"], Value::Null);
    assert_eq!(source["records"]["files"], 0);
    let (_, stats) = e.get("/api/sources/export/stats").await;
    assert_eq!(stats["today"]["files"], 0);
    let (_, monitoring) = e.get("/api/monitoring/status").await;
    assert_eq!(monitoring["last_24h"]["files"], 0);
    let (status, _, _) = e.post(&format!("/api/loads/{id}/retry"), json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    gate.notify_one();
    assert_eq!(sending.await.unwrap(), StatusCode::CREATED);
    let (_, page) = e.get("/api/sources/export/loads").await;
    assert_eq!(page["loads"][0]["id"], id);
}

#[tokio::test]
async fn a_load_left_while_it_was_sent_is_listed_and_confirmed() {
    let w = fake().await;
    let e = engine(&w).await;
    e.source("export", json!({})).await;
    // What a request that stopped while it sent leaves: the load, unlisted.
    let token = "0123456789abcdef0123456789abcdef";
    let admitted = e
        .state
        .db
        .admit_manifest(&hie::db::manifest::NewLoad {
            feed: "export",
            source_name: "Source export",
            name: "https://p.example/m.json",
            sha: "00",
            size: 0,
            by: "url",
            request_id: Some("lost"),
            request_hash: "h",
            token,
            served: None,
        })
        .unwrap();
    let hie::db::manifest::Admission::New(admitted) = admitted else {
        panic!("not admitted");
    };
    let id = admitted.load_id;
    // Past the time its request could still be sending, it is listed as
    // unconfirmed and asked about; the server never had it.
    e.sql(&format!("UPDATE submissions SET created_at = created_at - 600000 WHERE load_id = {id} RETURNING token"));
    let gone = e.settled(id).await;
    assert_eq!(gone["status"], "unreadable", "{gone}");
    assert_eq!(gone["manifest"]["kickoff"], "unconfirmed");
    assert_eq!(
        gone["error"],
        "the FHIR server did not receive this manifest; send it again"
    );
    let events = kinds(&e.events("export"));
    assert_eq!(events, ["load.received", "load.unreadable"]);
    // A repeat of its key answers with it.
    let db = e.state.db.clone();
    let again = db
        .admit_manifest(&hie::db::manifest::NewLoad {
            feed: "export",
            source_name: "Source export",
            name: "https://p.example/m.json",
            sha: "00",
            size: 0,
            by: "url",
            request_id: Some("lost"),
            request_hash: "h",
            token: "fedcba9876543210fedcba9876543210",
            served: None,
        })
        .unwrap();
    assert!(matches!(again, hie::db::manifest::Admission::Existing(found) if found == id));
}

// ---- the runner --------------------------------------------------------------------

#[tokio::test]
async fn a_paused_source_polls_nothing_and_a_replace_keeps_its_loads() {
    let w = fake().await;
    w.lock().unwrap().hold = true;
    let e = engine(&w).await;
    e.source("export", json!({})).await;
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    let (_, _, load) = e.send("export", json!({"manifest_url": url})).await;
    let id = load["id"].as_i64().unwrap();
    let started = Instant::now();
    while w.lock().unwrap().page_requests.is_empty() {
        assert!(started.elapsed() < Duration::from_secs(10), "never polled");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    e.manager.pause("export", None).unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let polled = w.lock().unwrap().page_requests.len();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        w.lock().unwrap().page_requests.len(),
        polled,
        "a paused source polls nothing"
    );
    let (_, paused) = e.get(&format!("/api/loads/{id}")).await;
    assert_eq!(paused["status"], "processing");
    e.manager.resume("export").unwrap();
    // A replace leaves the load open, and the new runner polls it.
    let (_, read) = e.get("/api/sources/export").await;
    let mut definition = read["definition"].clone();
    definition["contact"] = json!("Data team");
    let (status, _) = e.put("/api/sources/export", definition).await;
    assert_eq!(status, StatusCode::OK);
    w.lock().unwrap().hold = false;
    let done = e.settled(id).await;
    assert_eq!(done["status"], "done", "{done}");
}

#[tokio::test]
async fn a_failing_poll_is_an_attempt_and_alerts_until_it_recovers() {
    let w = fake().await;
    let e = engine(&w).await;
    e.source("export", json!({})).await;
    let (status, _, _) = e
        .post(
            "/api/alert-rules",
            json!({"source_id": "export", "condition": "unreachable", "threshold": "1", "channels": ["app"]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    w.lock().unwrap().poll_500 = 1;
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    let (_, _, load) = e.send("export", json!({"manifest_url": url})).await;
    let id = load["id"].as_i64().unwrap();
    let done = e.settled(id).await;
    assert_eq!(done["status"], "done", "{done}");
    let events = e.events("export");
    let failed: Vec<&Value> = events
        .iter()
        .filter(|ev| ev["kind"] == "submission.attempt_failed")
        .collect();
    assert_eq!(failed.len(), 1, "{events:?}");
    assert!(failed[0]["reason"].as_str().unwrap().contains("HTTP 500"));
    let alerts = e.sql("SELECT kind, cleared_at FROM alerts WHERE feed_id = 'export'");
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert_eq!(alerts[0]["kind"], "delivery");
    assert!(
        alerts[0]["cleared_at"].is_i64(),
        "cleared once the poll worked: {alerts:?}"
    );
    assert!(!kinds(&events).iter().any(|k| k.starts_with("record.")));
}

#[tokio::test]
async fn a_malformed_receipt_makes_the_load_unreadable() {
    let w = fake().await;
    w.lock().unwrap().malformed = true;
    let e = engine(&w).await;
    e.source("export", json!({})).await;
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    let (_, _, load) = e.send("export", json!({"manifest_url": url})).await;
    let gone = e.settled(load["id"].as_i64().unwrap()).await;
    assert_eq!(gone["status"], "unreadable", "{gone}");
    assert!(
        gone["error"]
            .as_str()
            .unwrap()
            .contains("not an OperationOutcome"),
        "{gone}"
    );
    assert_eq!(
        kinds(&e.events("export")).last().unwrap(),
        "load.unreadable"
    );
}

#[tokio::test]
async fn the_token_goes_only_to_the_fhir_server_and_its_status_url() {
    let w = fake().await;
    let e = engine_with(&w, true, &[]).await;
    e.source("export", json!({})).await;
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    let (status, _, load) = e.send("export", json!({"manifest_url": url})).await;
    assert_eq!(status, StatusCode::CREATED, "{load}");
    let done = e.settled(load["id"].as_i64().unwrap()).await;
    assert_eq!(done["status"], "done", "{done}");
    let w = w.lock().unwrap();
    assert!(
        !w.page_requests.is_empty() && w.page_requests.iter().all(|(_, auth)| *auth),
        "{:?}",
        w.page_requests
    );
    assert!(
        !w.receipt_requests.is_empty() && w.receipt_requests.iter().all(|(_, auth)| !*auth),
        "{:?}",
        w.receipt_requests
    );
}

#[tokio::test]
async fn a_check_answers_with_the_loads_recorded_before_its_pass_ends() {
    let w = fake().await;
    let e = engine(&w).await;
    // A source that passes only when asked to.
    e.source("export", json!({"poll_seconds": 3600})).await;
    let (status, headers, _) = e.post("/api/sources/export/check-now", json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let location = headers[header::LOCATION].to_str().unwrap().to_string();
    let started = Instant::now();
    let check = loop {
        let (_, check) = e.get(&location).await;
        if check["status"] != "pending" {
            break check;
        }
        assert!(started.elapsed() < Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(check["status"], "nothing_new", "{check}");

    // A manifest recorded after a check is asked for, before the pass that
    // answers it ends, is what it found.
    let check = e.state.db.insert_check("export").unwrap();
    let url = url_manifest(&w, "m", &[("Patient", "patients.ndjson")]);
    let (_, _, load) = e.send("export", json!({"manifest_url": url})).await;
    e.manager
        .runner("export")
        .unwrap()
        .tick_with(&[check])
        .await
        .unwrap();
    let (_, answered) = e.get(&format!("/api/checks/{check}")).await;
    assert_eq!(answered["status"], "found", "{answered}");
    assert_eq!(answered["file_count"], 1);
    assert_eq!(answered["files"][0]["id"], load["id"]);
}

// ---- the outbound path and the store --------------------------------------------------

/// The outbound `$bulk-submit` path is unchanged: a kick-off HFS refuses as
/// a repeat (`409`) goes on to the status request and settles.
#[tokio::test]
async fn an_outbound_kickoff_refused_as_a_repeat_still_settles() {
    let w = fake().await;
    let e = engine_with(
        &w,
        false,
        &[(
            "drop.yaml",
            "id: drop\nname: Drop\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.ndjson\"\n  poll_seconds: 1\n  min_file_age_ms: 0\n\
             format: ndjson\ntransform: transforms/fhir_passthrough.py\ndelivery:\n  method: bulk_submit\n",
        )],
    )
    .await;
    let inbox = e.root.path().join("inbox");
    std::fs::create_dir_all(&inbox).unwrap();
    // The server already holds this drop's submission.
    let submission = {
        let content = "{\"resourceType\":\"Patient\",\"id\":\"p1\"}\n";
        let sha = hie::reader::sha256_hex(content.as_bytes());
        let id = hie::bulk::submission_id("drop", &[&sha]);
        std::fs::write(inbox.join("drop.ndjson"), content).unwrap();
        id
    };
    {
        let mut world = w.lock().unwrap();
        world.subs.insert(
            submission.clone(),
            Sub {
                manifest: true,
                fetched: true,
                output: BTreeMap::from([("Patient".to_string(), vec!["Patient/p1".to_string()])]),
                ..Sub::default()
            },
        );
    }
    let started = Instant::now();
    loop {
        let done = e.sql("SELECT status FROM submissions");
        if done.first().is_some_and(|s| s["status"] == "done") {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(30), "{done:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(e.state.db.feed_counts("drop").unwrap()["sent"], 1);
    let (_, loads) = e.get("/api/sources/drop/loads").await;
    assert_eq!(loads["loads"][0]["manifest"], Value::Null);
}

#[test]
fn a_store_from_before_manifest_loads_reopens() {
    if std::env::var("HIE_TEST_DATABASE_URL").is_ok() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hie.db");
    drop(hie::db::Db::open(&path).unwrap());
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        for index in [
            "files_feed_request",
            "files_receipts_failing",
            "files_feed_receipts_failing",
            "submissions_load",
        ] {
            conn.execute(&format!("DROP INDEX {index}"), []).unwrap();
        }
        for (table, column) in [
            ("files", "manifest"),
            ("files", "request_id"),
            ("files", "listed"),
            ("files", "receipts_sent"),
            ("files", "receipts_rejected"),
            ("files", "receipt_failures"),
            ("submissions", "load_id"),
            ("submissions", "progress"),
        ] {
            conn.execute(&format!("ALTER TABLE {table} DROP COLUMN {column}"), [])
                .unwrap();
        }
        conn.execute(
            "INSERT INTO files (feed_id, name, sha256, size, status, received_at) VALUES ('old', 'a.ndjson', 'x', 1, 'done', 1)",
            [],
        )
        .unwrap();
    }
    let db = hie::db::Db::open(&path).unwrap();
    let rows = db
        .query_json(
            "SELECT listed, manifest, receipts_sent FROM files WHERE feed_id = 'old'",
            [],
        )
        .unwrap();
    assert_eq!(rows[0]["listed"], 1);
    assert_eq!(rows[0]["manifest"], Value::Null);
    assert_eq!(rows[0]["receipts_sent"], 0);
    assert_eq!(db.feed_counts("old").unwrap()["files"], 1);
}
