//! The destination partition: with `destination.tenant` set, every request the
//! engine makes to the destination carries the tenant header (REST batch and
//! transaction delivery with conditional updates, `$validate`, the
//! `$bulk-submit` kick-off, status poll and receipt fetch, and the destination
//! facts' `/metadata`); a receipt URL on another origin does not get it; unset,
//! no request carries it.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

/// One request a stand-in received.
#[derive(Debug, Clone)]
struct Seen {
    method: Method,
    path: String,
    headers: HeaderMap,
    /// The Bundle's `type`, for a Bundle.
    bundle: Option<String>,
    /// The Bundle's entry request URLs, for a Bundle.
    urls: Vec<String>,
}

#[derive(Clone, Default)]
struct StandIn {
    /// The destination's base URL and the other origin receipts are put on.
    base: Arc<Mutex<String>>,
    storage: Arc<Mutex<String>>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

/// The stand-in destination: records every request and answers each kind.
async fn destination(
    State(s): State<StandIn>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_string();
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let entries = body["entry"].as_array().cloned().unwrap_or_default();
    let is_bundle = body["resourceType"] == "Bundle";
    s.seen.lock().unwrap().push(Seen {
        method: method.clone(),
        path: path.clone(),
        headers,
        bundle: is_bundle.then(|| body["type"].as_str().unwrap_or_default().to_string()),
        urls: entries
            .iter()
            .filter_map(|e| e["request"]["url"].as_str().map(str::to_string))
            .collect(),
    });
    let base = s.base.lock().unwrap().clone();
    let storage = s.storage.lock().unwrap().clone();
    let outcome = json!({"resourceType": "OperationOutcome",
        "issue": [{"severity": "information", "code": "informational"}]});
    match (method.as_str(), path.as_str()) {
        ("GET", "/metadata") => axum::Json(json!({
            "resourceType": "CapabilityStatement", "fhirVersion": "4.0.1",
            "software": {"name": "Stand-in", "version": "1"}}))
        .into_response(),
        ("POST", "/") if is_bundle => {
            let kind = format!("{}-response", body["type"].as_str().unwrap_or("batch"));
            let entry: Vec<Value> = entries
                .iter()
                .map(|_| json!({"response": {"status": "201 Created", "etag": "W/\"1\""}}))
                .collect();
            axum::Json(json!({"resourceType": "Bundle", "type": kind, "entry": entry}))
                .into_response()
        }
        ("POST", "/$bulk-submit") => axum::Json(outcome).into_response(),
        ("POST", "/$bulk-submit-status") => (
            StatusCode::ACCEPTED,
            [("content-location", format!("{base}/status/1"))],
        )
            .into_response(),
        ("GET", "/status/1") => axum::Json(json!({
            "transactionTime": "2026-11-04T00:00:00Z",
            "requiresAccessToken": false,
            "output": [
                {"type": "Patient", "url": format!("{base}/receipts/1"), "count": 1},
                {"type": "Patient", "url": format!("{storage}/signed/1?sig=abc"), "count": 0},
            ],
            "outcome": [], "deleted": [], "link": [],
        }))
        .into_response(),
        ("GET", "/receipts/1") => "{\"reference\":\"Patient/b1\"}\n".into_response(),
        ("GET", "/signed/1") => "".into_response(),
        ("POST", p) if p.ends_with("/$validate") => axum::Json(outcome).into_response(),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
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

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// What one engine run sent: to the destination, to the other origin, and what
/// `GET /api/destination` answered.
struct Run {
    destination: Vec<Seen>,
    storage: Vec<Seen>,
    facts: Value,
}

/// Runs an engine with validation on and `destination` (YAML under
/// `destination:`) over three sources: a batch, a transaction with a
/// conditional update and a conditional reference, and a `$bulk-submit` load
/// polled to its receipts; then reads `GET /api/destination`.
async fn run(destination_yaml: &str) -> Option<Run> {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return None;
    };
    let stand_in = StandIn::default();
    let base = serve(
        Router::new()
            .fallback(destination)
            .with_state(stand_in.clone()),
    )
    .await;
    let other = StandIn::default();
    let storage = serve(
        Router::new()
            .fallback(destination)
            .with_state(other.clone()),
    )
    .await;
    *stand_in.base.lock().unwrap() = base.clone();
    *stand_in.storage.lock().unwrap() = storage.clone();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hie_url = format!("http://{}", listener.local_addr().unwrap());
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::copy(
        repo().join("transforms/fhir_passthrough.py"),
        root.join("transforms/fhir_passthrough.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!(
            "fhir_base_url: {base}\npublic_url: {hie_url}\ndatabase: data/hie.db\npython: {python}\n\
             validation: required\ndestination:\n  display_name: Stand-in\n{destination_yaml}"
        ),
    )
    .unwrap();
    let source = |id: &str, delivery: &str| {
        format!(
            "id: {id}\nname: {id}\nsource:\n  type: folder\n  path: inbox-{id}\n  pattern: \"*.ndjson\"\n  min_file_age_ms: 0\n\
             format: ndjson\ntransform: transforms/fhir_passthrough.py\ndelivery:\n  initial_backoff_ms: 10\n{delivery}"
        )
    };
    let sources = [
        (
            "batched",
            "",
            "{\"resourceType\":\"Patient\",\"id\":\"a1\"}\n",
        ),
        (
            "linked",
            "",
            concat!(
                "{\"resourceType\":\"Patient\",\"identifier\":[{\"system\":\"urn:x\",\"value\":\"1\"}]}\n",
                "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\",\"code\":{\"text\":\"x\"},",
                "\"subject\":{\"reference\":\"Patient?identifier=urn:x|1\"}}\n",
            ),
        ),
        (
            "bulk",
            "  method: bulk_submit\n  bulk_from: transformed\n",
            "{\"resourceType\":\"Patient\",\"id\":\"b1\"}\n",
        ),
    ];
    for (id, delivery, data) in sources {
        std::fs::write(
            root.join(format!("sources/{id}.yaml")),
            source(id, delivery),
        )
        .unwrap();
        let inbox = root.join(format!("inbox-{id}"));
        std::fs::create_dir_all(&inbox).unwrap();
        std::fs::write(inbox.join("drop.ndjson"), data).unwrap();
    }
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    for (id, _, _) in sources {
        manager.pause(id, None).unwrap(); // the test drives the passes itself
    }
    let router = hie::api::router(state);
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    for (id, _, _) in sources {
        manager.runner(id).unwrap().tick().await.unwrap();
    }
    // The receipt poll waits the delivery's initial backoff.
    tokio::time::sleep(Duration::from_millis(50)).await;
    manager.runner("bulk").unwrap().tick().await.unwrap();

    let facts: Value = reqwest::get(format!("{hie_url}/api/destination"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let destination = stand_in.seen.lock().unwrap().clone();
    let storage = other.seen.lock().unwrap().clone();
    drop(manager);
    Some(Run {
        destination,
        storage,
        facts,
    })
}

/// Every kind of destination request the run must have made, as
/// `(method, path, bundle type)`; panics naming a missing one.
fn assert_every_kind(seen: &[Seen]) {
    let has = |m: &str, p: &dyn Fn(&str) -> bool| seen.iter().any(|s| s.method == m && p(&s.path));
    for (what, found) in [
        ("GET /metadata", has("GET", &|p| p == "/metadata")),
        (
            "POST $validate",
            has("POST", &|p| p.ends_with("/$validate")),
        ),
        (
            "a batch",
            seen.iter().any(|s| s.bundle.as_deref() == Some("batch")),
        ),
        (
            "a transaction with a conditional update",
            seen.iter().any(|s| {
                s.bundle.as_deref() == Some("transaction")
                    && s.urls.iter().any(|u| u.starts_with("Patient?identifier="))
            }),
        ),
        ("$bulk-submit", has("POST", &|p| p == "/$bulk-submit")),
        (
            "$bulk-submit-status",
            has("POST", &|p| p == "/$bulk-submit-status"),
        ),
        ("the status poll", has("GET", &|p| p == "/status/1")),
        ("the receipt fetch", has("GET", &|p| p == "/receipts/1")),
    ] {
        assert!(found, "no {what} among {:#?}", paths(seen));
    }
}

fn paths(seen: &[Seen]) -> Vec<String> {
    seen.iter()
        .map(|s| format!("{} {}", s.method, s.path))
        .collect()
}

#[tokio::test]
async fn every_destination_request_carries_the_tenant() {
    let Some(run) = run("  tenant: \" north \"\n").await else {
        return;
    };
    assert_every_kind(&run.destination);
    for s in &run.destination {
        assert_eq!(
            s.headers.get("x-tenant-id").and_then(|v| v.to_str().ok()),
            Some("north"),
            "{} {} without the tenant",
            s.method,
            s.path
        );
        assert_eq!(s.headers.get_all("x-tenant-id").iter().count(), 1);
    }
    // A receipt on another origin is fetched, without the destination's tenant.
    assert_eq!(paths(&run.storage), ["GET /signed/1"]);
    assert!(run.storage[0].headers.get("x-tenant-id").is_none());

    assert_eq!(run.facts["tenant"], "north");
    assert_eq!(run.facts["tenant_header"], "X-Tenant-ID");
    assert_eq!(run.facts["display_name"], "Stand-in");
    assert_eq!(run.facts["reachable"], true);
}

#[tokio::test]
async fn a_custom_tenant_header_is_honored() {
    let Some(run) = run("  tenant: south\n  tenant_header: X-Partition\n").await else {
        return;
    };
    assert_every_kind(&run.destination);
    for s in &run.destination {
        assert_eq!(
            s.headers.get("x-partition").and_then(|v| v.to_str().ok()),
            Some("south"),
            "{} {} without the tenant",
            s.method,
            s.path
        );
        assert!(s.headers.get("x-tenant-id").is_none());
    }
    assert!(run.storage[0].headers.get("x-partition").is_none());
    assert_eq!(run.facts["tenant"], "south");
    assert_eq!(run.facts["tenant_header"], "X-Partition");
}

#[tokio::test]
async fn without_a_tenant_no_request_carries_one() {
    let Some(run) = run("").await else {
        return;
    };
    assert_every_kind(&run.destination);
    for s in run.destination.iter().chain(&run.storage) {
        assert!(
            s.headers.get("x-tenant-id").is_none(),
            "{} {} carries a tenant",
            s.method,
            s.path
        );
    }
    assert_eq!(run.facts["tenant"], Value::Null);
    assert_eq!(run.facts["tenant_header"], "X-Tenant-ID");
}
