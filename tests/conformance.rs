//! The conformance corpus (`tests/conformance/`) run against the in-process API,
//! built the way `tests/api.rs` builds it. Each case boots its own engine in a
//! temporary directory with the stand-ins it asks for: a destination FHIR
//! server, python-runtime's HIE facade, TCP listeners that count connections,
//! and SQLite databases. The format is described in `tests/conformance/README.md`.
//!
//! `known-red.txt` lists the cases expected to fail until the front named next
//! to each implements them. The test fails on an unlisted failure, and on a
//! listed case that passes, so the list only shrinks honestly. `SHA256SUMS` must
//! match the corpus files exactly.
//!
//! `HIE_CONFORMANCE_ONLY=<text>` runs only the cases whose id contains it;
//! `HIE_CONFORMANCE_VERBOSE=1` prints why each known-red case fails.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path as UrlPath, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const FRONTS: [&str; 4] = ["H-versions", "H-connectors", "H-history", "H-secrets"];
const CASE_BUDGET: Duration = Duration::from_secs(90);
const DEFAULT_UNTIL_MS: u64 = 8000;
const PARALLEL_CASES: usize = 4;

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn corpus_dir() -> PathBuf {
    repo().join("tests/conformance")
}

fn python() -> Option<String> {
    let candidates = match std::env::var("HIE_TEST_PYTHON") {
        Ok(p) => vec![p],
        Err(_) => vec!["py".into(), "python3".into(), "python".into()],
    };
    candidates.into_iter().find(|p| {
        std::process::Command::new(p)
            .arg("-c")
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

// ---- The corpus on disk ---------------------------------------------------------

struct Case {
    id: String,
    doc: Value,
}

fn corpus_files(dir: &Path) -> Vec<String> {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .collect();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                walk(base, &path, out);
            } else {
                let rel = path
                    .strip_prefix(base)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                if rel != "SHA256SUMS" {
                    out.push(rel);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// Every corpus file is listed with its SHA-256, and nothing else is.
fn verify_sums(dir: &Path) -> Result<(), String> {
    let text =
        std::fs::read_to_string(dir.join("SHA256SUMS")).map_err(|e| format!("SHA256SUMS: {e}"))?;
    let mut listed = BTreeMap::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let (hash, path) = line
            .split_once("  ")
            .ok_or_else(|| format!("SHA256SUMS: malformed line {line:?}"))?;
        listed.insert(path.to_string(), hash.to_string());
    }
    let mut problems = Vec::new();
    let files = corpus_files(dir);
    for file in &files {
        let bytes = std::fs::read(dir.join(file)).unwrap();
        let actual = hex::encode(Sha256::digest(&bytes));
        match listed.get(file) {
            None => problems.push(format!("{file} is not in SHA256SUMS")),
            Some(h) if *h != actual => problems.push(format!("{file} does not match SHA256SUMS")),
            _ => {}
        }
    }
    for file in listed.keys() {
        if !files.contains(file) {
            problems.push(format!("SHA256SUMS lists {file}, which does not exist"));
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{}\nregenerate with: cd tests/conformance && find . -type f ! -name SHA256SUMS | sed 's|^\\./||' | LC_ALL=C sort | xargs sha256sum > SHA256SUMS",
            problems.join("\n")
        ))
    }
}

fn load_cases(dir: &Path) -> Vec<Case> {
    let mut cases = Vec::new();
    for rel in corpus_files(dir) {
        let Some(id) = rel
            .strip_prefix("cases/")
            .and_then(|r| r.strip_suffix(".json"))
        else {
            continue;
        };
        let text = std::fs::read_to_string(dir.join(&rel)).unwrap();
        let doc: Value =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("{rel} is not JSON: {e}"));
        if let Some(declared) = doc.get("id").and_then(Value::as_str) {
            assert_eq!(declared, id, "{rel}: its id must match its path");
        }
        assert!(
            doc.get("steps").and_then(Value::as_array).is_some(),
            "{rel}: no steps"
        );
        cases.push(Case {
            id: id.to_string(),
            doc,
        });
    }
    cases
}

fn load_known_red(dir: &Path, cases: &[Case]) -> BTreeMap<String, String> {
    let text = std::fs::read_to_string(dir.join("known-red.txt")).unwrap_or_default();
    let ids: BTreeSet<&str> = cases.iter().map(|c| c.id.as_str()).collect();
    let mut out = BTreeMap::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap().trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let (id, front) = (parts.next().unwrap(), parts.next().unwrap_or(""));
        assert!(
            FRONTS.contains(&front),
            "known-red.txt:{}: {id} names front {front:?}; use one of {FRONTS:?}",
            n + 1
        );
        assert!(ids.contains(id), "known-red.txt:{}: no case {id}", n + 1);
        assert!(
            out.insert(id.to_string(), front.to_string()).is_none(),
            "known-red.txt: {id} listed twice"
        );
    }
    out
}

/// Every route in `endpoints.txt` is requested by some case's steps. A request
/// counts for the most specific route its method and path match.
fn check_coverage(dir: &Path, cases: &[Case]) -> Result<(), String> {
    let text = std::fs::read_to_string(dir.join("endpoints.txt")).unwrap_or_default();
    let routes: Vec<(String, Vec<String>)> = text
        .lines()
        .map(|l| l.split('#').next().unwrap().trim())
        .filter(|l| !l.is_empty())
        .filter_map(|l| l.split_once(' '))
        .map(|(m, p)| {
            let segments = p
                .trim()
                .trim_start_matches('/')
                .split('/')
                .map(str::to_string)
                .collect();
            (m.to_string(), segments)
        })
        .collect();
    let is_param = |t: &str| t.starts_with('{') && t.ends_with('}');
    let mut covered = BTreeSet::new();
    for case in cases {
        for step in case.doc["steps"].as_array().into_iter().flatten() {
            let Some(request) = step.get("request") else {
                continue;
            };
            let method = request["method"].as_str().unwrap_or("GET");
            let path = request["path"].as_str().unwrap_or_default();
            let path = path.split('?').next().unwrap_or_default();
            let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
            let best = routes
                .iter()
                .enumerate()
                .filter(|(_, (m, t))| {
                    m == method
                        && t.len() == segments.len()
                        && t.iter().zip(&segments).all(|(t, s)| is_param(t) || t == s)
                })
                .max_by_key(|(_, (_, t))| t.iter().filter(|t| !is_param(t)).count());
            if let Some((i, _)) = best {
                covered.insert(i);
            }
        }
    }
    let missing: Vec<String> = routes
        .iter()
        .enumerate()
        .filter(|(i, _)| !covered.contains(i))
        .map(|(_, (m, t))| format!("{m} /{}", t.join("/")))
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!("routes no case requests: {missing:?}"))
    }
}

// ---- The stand-in destination -----------------------------------------------

#[derive(Default)]
struct Destination {
    validate: String,
    invalid: Vec<Value>,
    writes_mode: String,
    resources: BTreeMap<String, Value>,
    writes: usize,
    validations: usize,
    bundle_types: Vec<String>,
    next_id: usize,
}

type Dest = Arc<Mutex<Destination>>;

impl Destination {
    fn configure(&mut self, cfg: &Value) {
        if let Some(v) = cfg.get("validate").and_then(Value::as_str) {
            self.validate = v.to_string();
        }
        if let Some(v) = cfg.get("invalid").and_then(Value::as_array) {
            self.invalid = v.clone();
        }
        if let Some(v) = cfg.get("writes").and_then(Value::as_str) {
            self.writes_mode = v.to_string();
        }
        for r in cfg["seed"].as_array().cloned().unwrap_or_default() {
            let key = format!(
                "{}/{}",
                r["resourceType"].as_str().unwrap_or_default(),
                r["id"].as_str().unwrap_or_default()
            );
            self.resources.insert(key, r);
        }
    }

    fn find_by_identifier(&self, rtype: &str, query: &str) -> Vec<String> {
        let query = url::form_urlencoded::parse(query.as_bytes())
            .find(|(k, _)| k == "identifier")
            .map(|(_, v)| v.into_owned());
        let Some(query) = query else { return vec![] };
        let (system, value) = query.split_once('|').unwrap_or(("", &query));
        self.resources
            .iter()
            .filter(|(key, r)| {
                key.starts_with(&format!("{rtype}/"))
                    && r["identifier"].as_array().is_some_and(|ids| {
                        ids.iter().any(|i| {
                            i["value"] == value && (system.is_empty() || i["system"] == system)
                        })
                    })
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Stores one resource from a write; answers the entry status.
    fn write(&mut self, method: &str, url: &str, mut resource: Value) -> &'static str {
        self.writes += 1;
        let url = url.trim_start_matches('/');
        let (rtype, rest) = url.split_once(['/', '?']).unwrap_or((url, ""));
        let key = if url.contains('?') {
            match self.find_by_identifier(rtype, rest).first() {
                Some(k) => k.clone(),
                None => {
                    self.next_id += 1;
                    format!("{rtype}/s{}", self.next_id)
                }
            }
        } else if method == "POST" || rest.is_empty() {
            self.next_id += 1;
            format!("{rtype}/s{}", self.next_id)
        } else {
            format!("{rtype}/{rest}")
        };
        resource["id"] = json!(key.split_once('/').unwrap().1);
        let existed = self.resources.insert(key, resource).is_some();
        if existed { "200 OK" } else { "201 Created" }
    }

    /// Rewrites `Type?identifier=...` references to the one match, as a
    /// destination does inside a transaction.
    fn resolve_conditional(&self, value: &mut Value) -> Result<(), String> {
        match value {
            Value::Object(map) => {
                for (k, v) in map.iter_mut() {
                    if k == "reference"
                        && let Some(text) = v.as_str()
                        && let Some((rtype, query)) = text.split_once('?')
                    {
                        let found = self.find_by_identifier(rtype, query);
                        if found.len() != 1 {
                            return Err(format!("{text} matches {} resources", found.len()));
                        }
                        *v = json!(found[0]);
                    } else {
                        self.resolve_conditional(v)?;
                    }
                }
                Ok(())
            }
            Value::Array(items) => items
                .iter_mut()
                .try_for_each(|v| self.resolve_conditional(v)),
            _ => Ok(()),
        }
    }

    fn outcome_for(&self, resource: &Value) -> Value {
        let issues: Vec<Value> = self
            .invalid
            .iter()
            .filter(|rule| {
                rule["resourceType"] == resource["resourceType"]
                    && rule["field"]
                        .as_str()
                        .is_some_and(|f| resource.get(f) == Some(&rule["equals"]))
            })
            .map(|rule| {
                json!({"severity": "error", "code": "code-invalid",
                       "diagnostics": rule["message"].as_str().unwrap_or("invalid"),
                       "expression": [rule["expression"]]})
            })
            .collect();
        let issues = if issues.is_empty() {
            vec![
                json!({"severity": "information", "code": "informational", "diagnostics": "All OK"}),
            ]
        } else {
            issues
        };
        json!({"resourceType": "OperationOutcome", "issue": issues})
    }
}

async fn dest_bundle(State(d): State<Dest>, Json(bundle): Json<Value>) -> Response {
    let mut d = d.lock().unwrap();
    let kind = bundle["type"].as_str().unwrap_or("batch").to_string();
    d.bundle_types.push(kind.clone());
    if d.writes_mode == "unavailable" {
        return (StatusCode::SERVICE_UNAVAILABLE, "unavailable").into_response();
    }
    let mut staged = Vec::new();
    for e in bundle["entry"].as_array().cloned().unwrap_or_default() {
        let mut resource = e["resource"].clone();
        // A transaction resolves its conditional references against what the
        // server holds before anything executes, and commits all or nothing.
        if kind == "transaction"
            && let Err(message) = d.resolve_conditional(&mut resource)
        {
            let outcome = json!({"resourceType": "OperationOutcome",
                "issue": [{"severity": "error", "code": "not-found", "diagnostics": message}]});
            return (StatusCode::BAD_REQUEST, Json(outcome)).into_response();
        }
        let method = e["request"]["method"].as_str().unwrap_or("PUT").to_string();
        let url = e["request"]["url"].as_str().unwrap_or_default().to_string();
        staged.push((method, url, resource));
    }
    let entries: Vec<Value> = staged
        .into_iter()
        .map(|(method, url, resource)| json!({"response": {"status": d.write(&method, &url, resource)}}))
        .collect();
    Json(json!({"resourceType": "Bundle", "type": format!("{kind}-response"), "entry": entries}))
        .into_response()
}

async fn dest_put(
    State(d): State<Dest>,
    UrlPath((rtype, id)): UrlPath<(String, String)>,
    Json(resource): Json<Value>,
) -> Response {
    let mut d = d.lock().unwrap();
    if d.writes_mode == "unavailable" {
        return (StatusCode::SERVICE_UNAVAILABLE, "unavailable").into_response();
    }
    let status = d.write("PUT", &format!("{rtype}/{id}"), resource.clone());
    let code = if status.starts_with("201") {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    (code, Json(resource)).into_response()
}

async fn dest_conditional_put(
    State(d): State<Dest>,
    UrlPath(rtype): UrlPath<String>,
    RawQuery(query): RawQuery,
    Json(resource): Json<Value>,
) -> Response {
    let mut d = d.lock().unwrap();
    if d.writes_mode == "unavailable" {
        return (StatusCode::SERVICE_UNAVAILABLE, "unavailable").into_response();
    }
    let url = format!("{rtype}?{}", query.unwrap_or_default());
    d.write("PUT", &url, resource.clone());
    (StatusCode::OK, Json(resource)).into_response()
}

async fn dest_operation(
    State(d): State<Dest>,
    UrlPath((_rtype, op)): UrlPath<(String, String)>,
    body: Bytes,
) -> Response {
    let mut d = d.lock().unwrap();
    if op != "$validate" {
        return (StatusCode::NOT_FOUND, "unknown operation").into_response();
    }
    d.validations += 1;
    if d.validate == "unavailable" {
        return (StatusCode::SERVICE_UNAVAILABLE, "validation unavailable").into_response();
    }
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let resource = if body["resourceType"] == "Parameters" {
        body["parameter"]
            .as_array()
            .and_then(|ps| ps.iter().find(|p| p["name"] == "resource"))
            .map(|p| p["resource"].clone())
            .unwrap_or(Value::Null)
    } else {
        body
    };
    Json(d.outcome_for(&resource)).into_response()
}

async fn dest_metadata() -> Json<Value> {
    Json(
        json!({"resourceType": "CapabilityStatement", "status": "active", "kind": "instance",
                "fhirVersion": "4.0.1", "software": {"name": "Conformance stand-in", "version": "1.0"},
                "format": ["json"]}),
    )
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

async fn start_destination(cfg: &Value) -> (String, Dest) {
    let mut state = Destination {
        validate: "valid".into(),
        writes_mode: "ok".into(),
        ..Default::default()
    };
    state.configure(cfg);
    let dest: Dest = Arc::new(Mutex::new(state));
    let router = Router::new()
        .route("/", post(dest_bundle))
        .route("/metadata", get(dest_metadata))
        .route(
            "/{rtype}/{id}",
            axum::routing::put(dest_put).post(dest_operation),
        )
        .route("/{rtype}", axum::routing::put(dest_conditional_put))
        .with_state(dest.clone());
    (serve(router).await, dest)
}

// ---- The stand-in python-runtime facade ---------------------------------------

/// Scripted answers for `POST /transforms/try`, and call counts per route. Raw
/// TCP, so an answer can be lost: the connection closes after the request.
#[derive(Default)]
struct Runtime {
    tries: VecDeque<Value>,
    calls: BTreeMap<String, usize>,
    /// The `Python-Runtime-Environment-*` headers it answers with.
    environment: Map<String, Value>,
    /// How `/run` answers: absent is `503 busy`; `{"marker", "diagnostics"?}`
    /// runs a fixed lab program (see the README).
    run: Value,
}

impl Runtime {
    fn configure(&mut self, cfg: &Value) {
        if let Some(env) = cfg["environment"].as_object() {
            self.environment = env.clone();
        }
        if let Some(run) = cfg.get("run") {
            self.run = run.clone();
        }
    }

    fn capabilities(&self) -> Value {
        let id = self
            .environment
            .get("Python-Runtime-Environment-Id")
            .cloned()
            .unwrap_or(Value::Null);
        let digest = self
            .environment
            .get("Python-Runtime-Environment-Digest")
            .cloned()
            .unwrap_or(Value::Null);
        json!({"protocols": [1], "bindings": ["hie-record@1"], "default_environment": id,
               "environments": [{"id": id, "digest": digest, "bindings": ["hie-record@1"], "state": "ready"}]})
    }

    /// The fixed lab program: `{id, value, status?}` to one Observation, or a
    /// data error when `value` is not a number.
    fn run_one(&self, record: &Value) -> Value {
        let value = match &record["value"] {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let mut reply = match value.parse::<f64>() {
            Ok(n) => json!({"resources": [{
                "resourceType": "Observation", "id": format!("obs-{}", match &record["id"] { Value::String(s) => s.clone(), v => v.to_string() }),
                "status": record["status"].as_str().filter(|s| !s.is_empty()).unwrap_or("final"),
                "code": {"text": "glucose"}, "valueQuantity": {"value": n},
                "note": [{"text": self.run["marker"].as_str().unwrap_or("runtime")}]}]}),
            Err(_) => {
                json!({"error": {"kind": "data", "message": format!("value '{value}' is not a number")}})
            }
        };
        if let Some(diagnostics) = self.run.get("diagnostics") {
            reply["diagnostics"] = diagnostics.clone();
        }
        reply
    }
}

type Rt = Arc<Mutex<Runtime>>;

async fn start_runtime(cfg: &Value) -> (String, Rt) {
    let mut state = Runtime {
        tries: cfg["try"].as_array().cloned().unwrap_or_default().into(),
        ..Default::default()
    };
    state.configure(cfg);
    let rt: Rt = Arc::new(Mutex::new(state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = rt.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(runtime_connection(stream, state.clone()));
        }
    });
    (format!("http://{addr}"), rt)
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<(String, String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let header_end = loop {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.lines();
    let mut first = lines.next()?.split_whitespace();
    let (method, path) = (first.next()?.to_string(), first.next()?.to_string());
    let length = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < length {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    Some((method, path, body))
}

async fn write_response(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    headers: &Map<String, Value>,
    body: &Value,
) {
    let text = body.to_string();
    let mut head = format!(
        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
        text.len()
    );
    for (k, v) in headers {
        head.push_str(&format!("{k}: {}\r\n", v.as_str().unwrap_or_default()));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(text.as_bytes()).await;
    let _ = stream.shutdown().await;
}

async fn runtime_connection(mut stream: tokio::net::TcpStream, rt: Rt) {
    let Some((method, path, body)) = read_request(&mut stream).await else {
        return;
    };
    let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let kind = if method == "PUT" {
        "put"
    } else if path.ends_with("/v1/capabilities") {
        "capabilities"
    } else if path.ends_with("/transforms/try") {
        "try"
    } else if path.ends_with("/run") {
        "run"
    } else {
        "other"
    };
    let (script, environment) = {
        let mut rt = rt.lock().unwrap();
        *rt.calls.entry(kind.to_string()).or_default() += 1;
        let script = if kind == "try" {
            rt.tries.pop_front()
        } else {
            None
        };
        (script, rt.environment.clone())
    };
    match kind {
        "capabilities" => {
            let caps = rt.lock().unwrap().capabilities();
            write_response(&mut stream, 200, &Map::new(), &caps).await;
        }
        "run" => {
            let answer = {
                let rt = rt.lock().unwrap();
                if rt.run.is_null() {
                    None
                } else {
                    let replies: Vec<Value> = request["requests"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default()
                        .iter()
                        .map(|r| {
                            let mut reply = rt.run_one(&r["record"]);
                            reply["id"] = r["id"].clone();
                            reply
                        })
                        .collect();
                    Some(json!({"replies": replies}))
                }
            };
            match answer {
                Some(body) => write_response(&mut stream, 200, &environment, &body).await,
                None => {
                    write_response(&mut stream, 503, &Map::new(), &json!({"error": "busy"})).await
                }
            }
        }
        "put" => {
            let code = request["code"].as_str().unwrap_or_default();
            let name = path.rsplit('/').next().unwrap_or_default();
            let sha = hex::encode(Sha256::digest(code.as_bytes()));
            write_response(
                &mut stream,
                200,
                &Map::new(),
                &json!({"name": name, "sha256": sha}),
            )
            .await;
        }
        "try" => {
            let Some(script) = script else {
                write_response(&mut stream, 500, &Map::new(), &json!({"error": "internal"})).await;
                return;
            };
            if let Some(ms) = script["delay_ms"].as_u64() {
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            if script["drop"] == true {
                drop(stream);
                return;
            }
            let mut answer = script["body"].clone();
            // Replies match requests by position; give them the ids HIE sent.
            if let (Some(replies), Some(requests)) = (
                answer["replies"].as_array_mut(),
                request["requests"].as_array(),
            ) {
                for (reply, req) in replies.iter_mut().zip(requests) {
                    reply["id"] = req["id"].clone();
                }
            }
            let status = script["status"].as_u64().unwrap_or(200) as u16;
            // A scripted answer names its own headers; a 200 without them
            // carries the runtime's current environment.
            let headers = match script["headers"].as_object() {
                Some(h) => h.clone(),
                None if status == 200 => environment,
                None => Map::new(),
            };
            write_response(&mut stream, status, &headers, &answer).await;
        }
        _ => {
            write_response(&mut stream, 503, &Map::new(), &json!({"error": "busy"})).await;
        }
    }
}

// ---- Patterns -------------------------------------------------------------------

type Captures = HashMap<String, Value>;

/// What placeholders resolve to outside captures.
struct Env {
    vars: HashMap<String, Value>,
}

impl Env {
    fn lookup(&self, name: &str, caps: &Captures) -> Option<Value> {
        if let Some(cap) = name.strip_prefix('$') {
            return caps.get(cap).cloned();
        }
        self.vars.get(name).cloned()
    }

    /// Replaces `{{destination}}`, `{{port:x}}`, `{{$x}}`, ... A string that is
    /// one placeholder becomes its value (a number stays a number); otherwise
    /// placeholders are interpolated as text. Unknown placeholders are left.
    fn subst_str(&self, s: &str, caps: &Captures) -> Result<Value, String> {
        if let Some(inner) = s.strip_prefix("{{").and_then(|r| r.strip_suffix("}}"))
            && !inner.contains("{{")
            && let Some(v) = self.lookup(inner, caps)
        {
            return Ok(v);
        }
        let mut out = String::new();
        let mut rest = s;
        while let Some(start) = rest.find("{{") {
            out.push_str(&rest[..start]);
            let after = &rest[start + 2..];
            let Some(end) = after.find("}}") else {
                out.push_str(&rest[start..]);
                rest = "";
                break;
            };
            let name = &after[..end];
            match self.lookup(name, caps) {
                Some(Value::String(t)) => out.push_str(&t),
                Some(v) => out.push_str(&v.to_string()),
                None if name.starts_with('$') => {
                    return Err(format!("placeholder {{{{{name}}}}} is not captured yet"));
                }
                None => out.push_str(&rest[start..start + 2 + end + 2]),
            }
            rest = &after[end + 2..];
        }
        out.push_str(rest);
        Ok(Value::String(out))
    }

    fn subst(&self, v: &Value, caps: &Captures) -> Result<Value, String> {
        Ok(match v {
            Value::String(s) => self.subst_str(s, caps)?,
            Value::Array(items) => Value::Array(
                items
                    .iter()
                    .map(|i| self.subst(i, caps))
                    .collect::<Result<_, _>>()?,
            ),
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(k, v)| {
                        Ok((
                            self.subst_str(k, caps)?.as_str().unwrap_or(k).to_string(),
                            self.subst(v, caps)?,
                        ))
                    })
                    .collect::<Result<_, String>>()?,
            ),
            other => other.clone(),
        })
    }
}

fn short(v: Option<&Value>) -> String {
    match v {
        None => "(absent)".into(),
        Some(v) => {
            let t = v.to_string();
            if t.len() > 300 {
                format!("{}…", &t[..300])
            } else {
                t
            }
        }
    }
}

fn numbers_equal(a: &Value, b: &Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}

/// Matches `actual` (None: the member is absent) against a pattern, binding
/// captures into `caps`. Objects match as subsets; arrays exactly, or with a
/// leading `{{contains}}` or `{{each}}`.
fn matches(
    pat: &Value,
    actual: Option<&Value>,
    caps: &mut Captures,
    env: &Env,
    at: &str,
) -> Result<(), String> {
    let fail = |why: &str| Err(format!("{at}: {why}; got {}", short(actual)));
    if let Value::String(p) = pat
        && p.starts_with("{{")
        && p.ends_with("}}")
        && !p[2..].contains("{{")
    {
        let token = &p[2..p.len() - 2];
        if token == "absent" {
            return if actual.is_none() {
                Ok(())
            } else {
                fail("expected absent")
            };
        }
        let Some(a) = actual else {
            return fail(&format!("expected {p}"));
        };
        let ok = match token {
            "any" => true,
            "string" => a.is_string(),
            "nonempty" => {
                a.as_str().is_some_and(|s| !s.is_empty())
                    || a.as_array().is_some_and(|x| !x.is_empty())
            }
            "int" => a.is_i64() || a.is_u64(),
            "number" => a.is_number(),
            "bool" => a.is_boolean(),
            "object" => a.is_object(),
            "array" => a.is_array(),
            "time" => a
                .as_str()
                .is_some_and(|s| chrono::DateTime::parse_from_rfc3339(s).is_ok()),
            t if t.starts_with("gte:") => {
                let n: f64 = t[4..]
                    .parse()
                    .map_err(|_| format!("{at}: bad pattern {p}"))?;
                a.as_f64().is_some_and(|x| x >= n)
            }
            t if t.starts_with("hex:") => {
                let n: usize = t[4..]
                    .parse()
                    .map_err(|_| format!("{at}: bad pattern {p}"))?;
                a.as_str()
                    .is_some_and(|s| s.len() == n && s.chars().all(|c| c.is_ascii_hexdigit()))
            }
            t if t.starts_with("contains:") => a.as_str().is_some_and(|s| s.contains(&t[9..])),
            t if t.starts_with('$') => {
                let name = &t[1..];
                match caps.get(name) {
                    Some(bound) => bound == a || numbers_equal(bound, a),
                    None if a.is_null() => false,
                    None => {
                        caps.insert(name.to_string(), a.clone());
                        true
                    }
                }
            }
            _ => match env.lookup(token, caps) {
                Some(v) => &v == a || numbers_equal(&v, a),
                None => return Err(format!("{at}: unknown pattern {p}")),
            },
        };
        return if ok {
            Ok(())
        } else {
            fail(&format!("expected {p}"))
        };
    }
    let Some(a) = actual else {
        return fail(&format!("expected {}", short(Some(pat))));
    };
    match pat {
        Value::String(p) if p.contains("{{") => {
            let want = env.subst_str(p, caps)?;
            if &want == a {
                Ok(())
            } else {
                fail(&format!("expected {want}"))
            }
        }
        Value::Object(map) => {
            let Some(obj) = a.as_object() else {
                return fail("expected an object");
            };
            // `"{{exact}}": true` asks for exactly these members, no others.
            if map.get("{{exact}}") == Some(&Value::Bool(true))
                && let Some(extra) = obj.keys().find(|k| !map.contains_key(*k))
            {
                return fail(&format!("unexpected member {extra}"));
            }
            for (k, pv) in map.iter().filter(|(k, _)| *k != "{{exact}}") {
                matches(pv, obj.get(k), caps, env, &format!("{at}.{k}"))?;
            }
            Ok(())
        }
        Value::Array(items) => {
            let Some(arr) = a.as_array() else {
                return fail("expected an array");
            };
            match items.first().and_then(Value::as_str) {
                Some("{{contains}}") => {
                    for (i, p) in items[1..].iter().enumerate() {
                        let found = arr.iter().enumerate().find_map(|(j, el)| {
                            let mut trial = caps.clone();
                            matches(p, Some(el), &mut trial, env, &format!("{at}[{j}]"))
                                .ok()
                                .map(|_| trial)
                        });
                        match found {
                            Some(trial) => *caps = trial,
                            None => {
                                return fail(&format!(
                                    "no element matches pattern {i}: {}",
                                    short(Some(p))
                                ));
                            }
                        }
                    }
                    Ok(())
                }
                Some("{{each}}") => {
                    let p = items.get(1).cloned().unwrap_or(json!("{{any}}"));
                    for (j, el) in arr.iter().enumerate() {
                        matches(&p, Some(el), caps, env, &format!("{at}[{j}]"))?;
                    }
                    Ok(())
                }
                _ => {
                    if arr.len() != items.len() {
                        return fail(&format!("expected {} elements", items.len()));
                    }
                    for (j, (p, el)) in items.iter().zip(arr).enumerate() {
                        matches(p, Some(el), caps, env, &format!("{at}[{j}]"))?;
                    }
                    Ok(())
                }
            }
        }
        Value::Number(_) if numbers_equal(pat, a) => Ok(()),
        _ if pat == a => Ok(()),
        _ => fail(&format!("expected {}", short(Some(pat)))),
    }
}

/// Matches into a copy of the captures and keeps them only on success.
fn try_match(
    pat: &Value,
    actual: Option<&Value>,
    caps: &mut Captures,
    env: &Env,
    at: &str,
) -> Result<(), String> {
    let mut trial = caps.clone();
    matches(pat, actual, &mut trial, env, at)?;
    *caps = trial;
    Ok(())
}

// ---- One case -------------------------------------------------------------------

struct Answer {
    status: u16,
    headers: Map<String, Value>,
    body: Value,
    raw: String,
}

struct Run {
    api: String,
    http: reqwest::Client,
    root: PathBuf,
    env: Env,
    caps: Captures,
    dest: Dest,
    runtime: Option<Rt>,
    tcp: HashMap<String, Arc<AtomicUsize>>,
    sqlite: HashMap<String, PathBuf>,
    forbid: Vec<String>,
    /// Values that may be answered but must not be stored in plaintext, and
    /// the directories (under the engine's) that are checked.
    at_rest: (Vec<String>, Vec<String>),
    background: HashMap<String, tokio::task::JoinHandle<Result<Answer, String>>>,
}

async fn send(http: reqwest::Client, api: String, request: Value) -> Result<Answer, String> {
    let method = request["method"].as_str().unwrap_or("GET");
    let path = request["path"].as_str().ok_or("request without path")?;
    let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?;
    let mut req = http.request(method, format!("{api}{path}"));
    if let Some(headers) = request["headers"].as_object() {
        for (k, v) in headers {
            req = req.header(
                k,
                v.as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| v.to_string()),
            );
        }
    }
    if let Some(raw) = request["raw_body"].as_str() {
        req = req
            .header("content-type", "application/json")
            .body(raw.to_string());
    } else if let Some(body) = request.get("body") {
        req = req.json(body);
    }
    let res = req
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))?;
    let status = res.status().as_u16();
    let mut headers = Map::new();
    for (k, v) in res.headers() {
        headers.insert(
            k.as_str().to_string(),
            json!(v.to_str().unwrap_or_default()),
        );
    }
    let raw = res.text().await.map_err(|e| e.to_string())?;
    let body = if raw.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&raw).unwrap_or_else(|_| Value::String(raw.clone()))
    };
    Ok(Answer {
        status,
        headers,
        body,
        raw,
    })
}

impl Run {
    fn check_forbidden(&self, what: &str, text: &str) -> Result<(), String> {
        for secret in &self.forbid {
            if text.contains(secret.as_str()) {
                return Err(format!("{what} contains a forbidden value"));
            }
        }
        Ok(())
    }

    fn check_answer(&mut self, answer: &Answer, expect: &Value, at: &str) -> Result<(), String> {
        let headers_text: String = answer.headers.values().map(|v| v.to_string()).collect();
        self.check_forbidden(&format!("{at}: the answer"), &answer.raw)?;
        self.check_forbidden(&format!("{at}: the answer's headers"), &headers_text)?;
        let mut trial = self.caps.clone();
        if let Some(status) = expect.get("status")
            && let Err(e) = matches(
                status,
                Some(&json!(answer.status)),
                &mut trial,
                &self.env,
                &format!("{at} status"),
            )
        {
            return Err(format!("{e}; body {}", short(Some(&answer.body))));
        }
        if let Some(headers) = expect["headers"].as_object() {
            for (k, p) in headers {
                matches(
                    p,
                    answer.headers.get(&k.to_ascii_lowercase()),
                    &mut trial,
                    &self.env,
                    &format!("{at} header {k}"),
                )?;
            }
        }
        if let Some(body) = expect.get("body") {
            matches(
                body,
                Some(&answer.body),
                &mut trial,
                &self.env,
                &format!("{at} body"),
            )?;
        }
        self.caps = trial;
        Ok(())
    }

    async fn http_step(&mut self, step: &Value, at: &str) -> Result<(), String> {
        let request = self.env.subst(&step["request"], &self.caps)?;
        let expect = step.get("expect").cloned().unwrap_or(json!({}));
        if let Some(label) = step["background"].as_str() {
            let handle = tokio::spawn(send(self.http.clone(), self.api.clone(), request));
            self.background.insert(label.to_string(), handle);
            return Ok(());
        }
        let until = step["until_ms"]
            .as_u64()
            .or_else(|| (step["until"] == true).then_some(DEFAULT_UNTIL_MS));
        let started = Instant::now();
        loop {
            let answer = send(self.http.clone(), self.api.clone(), request.clone()).await;
            let result = match answer {
                Ok(a) => self.check_answer(&a, &expect, at),
                Err(e) => Err(format!("{at}: {e}")),
            };
            match (result, until) {
                (Ok(()), _) => return Ok(()),
                (Err(e), Some(ms)) if started.elapsed() < Duration::from_millis(ms) => {
                    if e.contains("forbidden") {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_millis(150)).await;
                }
                (Err(e), _) => return Err(e),
            }
        }
    }

    fn destination_check(&mut self, expect: &Value, at: &str) -> Result<(), String> {
        let d = self.dest.lock().unwrap();
        let mut trial = self.caps.clone();
        if let Some(by_type) = expect["resources"].as_object() {
            for (rtype, p) in by_type {
                let n = d
                    .resources
                    .keys()
                    .filter(|k| k.starts_with(&format!("{rtype}/")))
                    .count();
                matches(
                    p,
                    Some(&json!(n)),
                    &mut trial,
                    &self.env,
                    &format!("{at} resources.{rtype}"),
                )?;
            }
        }
        let stored: Vec<Value> = d.resources.values().cloned().collect();
        if let Some(patterns) = expect["stored"].as_array() {
            let mut pats = vec![json!("{{contains}}")];
            pats.extend(patterns.iter().cloned());
            matches(
                &Value::Array(pats),
                Some(&Value::Array(stored.clone())),
                &mut trial,
                &self.env,
                &format!("{at} stored"),
            )?;
        }
        if let Some(patterns) = expect["not_stored"].as_array() {
            for (i, p) in patterns.iter().enumerate() {
                for r in &stored {
                    let mut scratch = trial.clone();
                    if matches(p, Some(r), &mut scratch, &self.env, "").is_ok() {
                        return Err(format!(
                            "{at} not_stored[{i}]: the destination holds {}",
                            short(Some(r))
                        ));
                    }
                }
            }
        }
        for (key, value) in [
            ("writes", json!(d.writes)),
            ("validations", json!(d.validations)),
            ("bundle_types", json!(d.bundle_types)),
        ] {
            if let Some(p) = expect.get(key) {
                matches(
                    p,
                    Some(&value),
                    &mut trial,
                    &self.env,
                    &format!("{at} {key}"),
                )?;
            }
        }
        self.caps = trial;
        Ok(())
    }

    async fn step(&mut self, step: &Value, at: &str) -> Result<(), String> {
        if step.get("request").is_some() {
            return self.http_step(step, at).await;
        }
        if let Some(label) = step["await"].as_str() {
            let handle = self
                .background
                .remove(label)
                .ok_or_else(|| format!("{at}: no background request {label}"))?;
            let answer = handle
                .await
                .map_err(|e| format!("{at}: {e}"))?
                .map_err(|e| format!("{at}: {e}"))?;
            let expect = step.get("expect").cloned().unwrap_or(json!({}));
            return self.check_answer(&answer, &expect, at);
        }
        if let Some(expect) = step.get("destination") {
            let until = step["until_ms"]
                .as_u64()
                .or_else(|| (step["until"] == true).then_some(DEFAULT_UNTIL_MS));
            let started = Instant::now();
            loop {
                match (self.destination_check(expect, at), until) {
                    (Ok(()), _) => return Ok(()),
                    (Err(_), Some(ms)) if started.elapsed() < Duration::from_millis(ms) => {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                    }
                    (Err(e), _) => return Err(e),
                }
            }
        }
        if let Some(expect) = step.get("runtime") {
            let rt = self
                .runtime
                .as_ref()
                .ok_or_else(|| format!("{at}: the case has no runtime"))?;
            let calls = json!(rt.lock().unwrap().calls);
            return try_match(
                &expect["calls"],
                Some(&calls),
                &mut self.caps,
                &self.env,
                &format!("{at} runtime calls"),
            );
        }
        if let Some(expect) = step["tcp"].as_object() {
            for (name, p) in expect {
                let n = self
                    .tcp
                    .get(name)
                    .ok_or_else(|| format!("{at}: no listener {name}"))?
                    .load(Ordering::SeqCst);
                try_match(
                    p,
                    Some(&json!(n)),
                    &mut self.caps,
                    &self.env,
                    &format!("{at} tcp.{name}"),
                )?;
            }
            return Ok(());
        }
        if let Some(sql) = step.get("sql") {
            let db = sql["db"].as_str().unwrap_or_default();
            let path = self
                .sqlite
                .get(db)
                .ok_or_else(|| format!("{at}: no database {db}"))?;
            let conn = rusqlite::Connection::open(path).map_err(|e| e.to_string())?;
            for s in sql["statements"].as_array().cloned().unwrap_or_default() {
                conn.execute_batch(s.as_str().unwrap_or_default())
                    .map_err(|e| format!("{at}: {e}"))?;
            }
            return Ok(());
        }
        if let Some(cfg) = step.get("set_destination") {
            self.dest.lock().unwrap().configure(cfg);
            return Ok(());
        }
        if let Some(cfg) = step.get("set_runtime") {
            let rt = self
                .runtime
                .as_ref()
                .ok_or_else(|| format!("{at}: the case has no runtime"))?;
            rt.lock().unwrap().configure(cfg);
            return Ok(());
        }
        if let Some(files) = step["write_files"].as_object() {
            for (rel, content) in files {
                let content = self.env.subst(content, &self.caps)?;
                write_file(&self.root, rel, content.as_str().unwrap_or_default())?;
            }
            return Ok(());
        }
        if let Some(ms) = step["sleep_ms"].as_u64() {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            return Ok(());
        }
        Err(format!("{at}: unknown step {}", short(Some(step))))
    }

    /// Nothing the case marks as secret may be written anywhere under the
    /// engine's directory, nor logged; values forbidden at rest may not be in
    /// the directories named, nor logged.
    fn check_custody(&self) -> Result<(), String> {
        let log = LOG.lock().unwrap();
        for value in self.forbid.iter().chain(&self.at_rest.0) {
            if log.windows(value.len()).any(|w| w == value.as_bytes()) {
                return Err("the engine's log holds a forbidden value".into());
            }
        }
        drop(log);
        self.check_files()
    }

    fn check_files(&self) -> Result<(), String> {
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out)
                } else {
                    out.push(p)
                }
            }
        }
        let holds = |f: &Path, values: &[String]| -> Result<(), String> {
            let bytes = std::fs::read(f).unwrap_or_default();
            for value in values {
                if bytes.windows(value.len()).any(|w| w == value.as_bytes()) {
                    return Err(format!(
                        "{} holds a forbidden value in plaintext",
                        f.display()
                    ));
                }
            }
            Ok(())
        };
        let mut files = Vec::new();
        walk(&self.root, &mut files);
        for f in &files {
            holds(f, &self.forbid)?;
        }
        let (values, under) = &self.at_rest;
        for dir in under {
            let mut files = Vec::new();
            walk(&self.root.join(dir), &mut files);
            for f in &files {
                holds(f, values)?;
            }
        }
        Ok(())
    }
}

fn write_file(root: &Path, rel: &str, content: &str) -> Result<(), String> {
    let path = root.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    std::fs::write(&path, content).map_err(|e| e.to_string())
}

fn merge(base: &mut Value, extra: &Value) {
    match (base, extra) {
        (Value::Object(b), Value::Object(e)) => {
            for (k, v) in e {
                merge(b.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (b, e) => *b = e.clone(),
    }
}

/// `Ok(true)` passed, `Ok(false)` skipped for a missing requirement.
async fn run_case(case: &Case, python: &str, root: PathBuf) -> Result<bool, String> {
    let fixtures = case.doc.get("fixtures").cloned().unwrap_or(json!({}));

    let mut vars: HashMap<String, Value> = HashMap::new();
    vars.insert("root".into(), json!(root.to_string_lossy()));
    for requirement in fixtures["requires"].as_array().cloned().unwrap_or_default() {
        match requirement.as_str() {
            Some("mssql") => match mssql_vars() {
                Some(v) => vars.extend(v),
                None => return Ok(false),
            },
            other => return Err(format!("unknown requirement {other:?}")),
        }
    }

    let (dest_url, dest) =
        start_destination(fixtures.get("destination").unwrap_or(&json!({}))).await;
    vars.insert("destination".into(), json!(dest_url));

    let runtime = match fixtures.get("runtime") {
        Some(cfg) => {
            let (url, rt) = start_runtime(cfg).await;
            vars.insert("runtime".into(), json!(url));
            Some(rt)
        }
        None => None,
    };

    let mut tcp = HashMap::new();
    for name in fixtures["tcp"].as_array().cloned().unwrap_or_default() {
        let name = name.as_str().unwrap_or_default().to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                c.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });
        vars.insert(format!("port:{name}"), json!(port));
        tcp.insert(name, count);
    }

    let mut sqlite = HashMap::new();
    if let Some(dbs) = fixtures["sqlite"].as_object() {
        for (name, statements) in dbs {
            let path = root.join(format!("{name}.db"));
            let conn = rusqlite::Connection::open(&path).map_err(|e| e.to_string())?;
            for s in statements.as_array().cloned().unwrap_or_default() {
                conn.execute_batch(s.as_str().unwrap_or_default())
                    .map_err(|e| format!("fixture {name}: {e}"))?;
            }
            vars.insert(format!("sqlite:{name}"), json!(path.to_string_lossy()));
            sqlite.insert(name.clone(), path);
        }
    }

    let env = Env { vars };
    let caps = Captures::new();
    if let Some(files) = fixtures["files"].as_object() {
        for (rel, content) in files {
            let content = env.subst(content, &caps)?;
            write_file(&root, rel, content.as_str().unwrap_or_default())?;
        }
    }

    let mut config =
        json!({"fhir_base_url": dest_url, "database": "data/hie.db", "python": python});
    if fixtures["secrets_key"] == true {
        // A fixed 32-byte key, base64; custody is configured only when a case asks.
        write_file(
            &root,
            "secrets.key",
            "Y29uZm9ybWFuY2Utc2VjcmV0cy1rZXktMzItYnl0ZXM=\n",
        )?;
        merge(
            &mut config,
            &json!({"secrets": {"key_file": "secrets.key"}}),
        );
    }
    if let Some(extra) = fixtures.get("hie") {
        merge(&mut config, &env.subst(extra, &caps)?);
    }
    let yaml = serde_yaml::to_string(&config).map_err(|e| e.to_string())?;
    write_file(&root, "hie.yaml", &yaml)?;

    let (config, feeds) = hie::config::load(&root.join("hie.yaml"))
        .map_err(|e| format!("loading hie.yaml: {e:#}"))?;
    let (_manager, state) = hie::build(&config, feeds, &repo().join("hie_runtime"), None, None)
        .map_err(|e| format!("building the engine: {e:#}"))?;
    let api = serve(hie::api::router(state)).await;

    let strings = |v: &Value| -> Result<Vec<String>, String> {
        v.as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|v| {
                env.subst(v, &caps)
                    .map(|v| v.as_str().unwrap_or_default().to_string())
            })
            .collect()
    };
    let forbid = strings(&case.doc["forbid"])?;
    let at_rest = {
        let spec = &case.doc["forbid_at_rest"];
        let mut under = strings(&spec["under"])?;
        if under.is_empty() {
            under.push(".".into());
        }
        (strings(&spec["values"])?, under)
    };
    let mut run = Run {
        api,
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap(),
        root,
        env,
        caps,
        dest,
        runtime,
        tcp,
        sqlite,
        forbid,
        at_rest,
        background: HashMap::new(),
    };
    let mut failure = None;
    for (i, step) in case.doc["steps"].as_array().unwrap().iter().enumerate() {
        let label = step["name"]
            .as_str()
            .map(|n| format!("step {i} ({n})"))
            .unwrap_or(format!("step {i}"));
        if let Err(e) = run.step(step, &label).await {
            failure = Some(e);
            break;
        }
    }
    // Custody is checked whether the steps passed or not.
    match (failure, run.check_custody()) {
        (None, Ok(())) => Ok(true),
        (Some(e), Ok(())) => Err(e),
        (None, Err(c)) => Err(c),
        (Some(e), Err(c)) => Err(format!("{e}; and {c}")),
    }
}

/// Placeholders for a SQL Server named by `HIE_TEST_MSSQL_URL`
/// (`sqlserver://user:password@host:port/database?trust_cert=true`).
fn mssql_vars() -> Option<HashMap<String, Value>> {
    let url = url::Url::parse(&std::env::var("HIE_TEST_MSSQL_URL").ok()?).ok()?;
    let decode = |s: &str| {
        url::form_urlencoded::parse(format!("x={s}").as_bytes())
            .next()
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default()
    };
    let query: HashMap<String, String> = url.query_pairs().into_owned().collect();
    let flag = |k: &str, default: bool| {
        query
            .get(k)
            .map(|v| matches!(v.as_str(), "true" | "on" | "yes" | "1"))
            .unwrap_or(default)
    };
    Some(HashMap::from([
        ("mssql:host".to_string(), json!(url.host_str()?)),
        ("mssql:port".to_string(), json!(url.port().unwrap_or(1433))),
        (
            "mssql:database".to_string(),
            json!(url.path().trim_start_matches('/')),
        ),
        ("mssql:user".to_string(), json!(decode(url.username()))),
        ("mssql:password".to_string(), json!(decode(url.password()?))),
        ("mssql:encrypt".to_string(), json!(flag("encrypt", true))),
        (
            "mssql:trust_cert".to_string(),
            json!(flag("trust_cert", false)),
        ),
    ]))
}

/// The engine's log, from every case; scanned for forbidden values.
static LOG: Mutex<Vec<u8>> = Mutex::new(Vec::new());

struct LogWriter;

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        LOG.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// One case on a runtime of its own, so that when it ends, or runs out of time,
/// every task it started (the engine's runners, its API, the stand-ins, their
/// Python workers) goes with the runtime, before its directory is removed.
fn run_isolated(case: Case, python: Arc<String>) -> Result<bool, String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let root = dir.path().to_path_buf();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    let outcome = runtime.block_on(async {
        match tokio::time::timeout(CASE_BUDGET, run_case(&case, &python, root)).await {
            Ok(r) => r,
            Err(_) => Err(format!("ran past {}s", CASE_BUDGET.as_secs())),
        }
    });
    runtime.shutdown_timeout(Duration::from_secs(5));
    drop(dir);
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_conformance_corpus() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hie=debug")
        .with_ansi(false)
        .with_writer(|| LogWriter)
        .try_init();
    let dir = corpus_dir();
    let mut problems = Vec::new();
    if let Err(e) = verify_sums(&dir) {
        problems.push(e);
    }
    let cases = load_cases(&dir);
    let known = load_known_red(&dir, &cases);
    if let Err(e) = check_coverage(&dir, &cases) {
        problems.push(e);
    }
    let only = std::env::var("HIE_CONFORMANCE_ONLY").ok();
    let verbose = std::env::var("HIE_CONFORMANCE_VERBOSE").is_ok();
    let selected: Vec<Case> = cases
        .into_iter()
        .filter(|c| only.as_ref().is_none_or(|o| c.id.contains(o.as_str())))
        .collect();
    let python = Arc::new(python);
    let results: Vec<(String, Result<bool, String>)> = futures::stream::iter(selected)
        .map(|case| {
            let python = python.clone();
            async move {
                let id = case.id.clone();
                let outcome = tokio::task::spawn_blocking(move || run_isolated(case, python))
                    .await
                    .unwrap_or_else(|e| Err(format!("panicked: {e}")));
                (id, outcome)
            }
        })
        .buffer_unordered(PARALLEL_CASES)
        .collect()
        .await;

    let mut results = results;
    results.sort_by(|a, b| a.0.cmp(&b.0));
    let (mut green, mut red, mut skipped) = (0, 0, 0);
    for (id, outcome) in &results {
        match (outcome, known.get(id)) {
            (Ok(false), _) => skipped += 1,
            (Ok(true), None) => green += 1,
            (Err(e), Some(front)) => {
                red += 1;
                if verbose {
                    eprintln!("known red ({front}) {id}: {e}");
                }
            }
            (Ok(true), Some(front)) => problems.push(format!(
                "{id} passes but known-red.txt lists it for {front}: remove it from the list"
            )),
            (Err(e), None) => problems.push(format!("{id} failed: {e}")),
        }
    }
    eprintln!(
        "conformance: {} cases run, {green} green, {red} known red, {skipped} skipped for a missing requirement",
        results.len()
    );
    assert!(problems.is_empty(), "\n{}", problems.join("\n"));
}
