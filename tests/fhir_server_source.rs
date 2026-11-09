//! Pulling from another FHIR server: pages followed, a cursor per resource type
//! that moves only once the records are stored, nothing pulled twice, a bearer
//! token on every call, and the connection test's evidence.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use hie::alerts::Alerter;
use hie::db::Db;
use hie::fhir::FhirClient;
use hie::pipeline::FeedRunner;
use hie::source::Source;
use serde_json::{Value, json};

/// The remote server: resources with a `meta.lastUpdated`, searchable by
/// `_lastUpdated=gt`, sorted, paged with a `page` parameter.
#[derive(Default)]
struct Remote {
    base: String,
    resources: Vec<Value>,
    requests: Vec<String>,
}

type Shared = Arc<Mutex<Remote>>;

fn authorized(headers: &HeaderMap) -> bool {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == "Bearer remote-token")
}

async fn metadata(headers: HeaderMap) -> (StatusCode, Json<Value>) {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({})));
    }
    (
        StatusCode::OK,
        Json(json!({"resourceType": "CapabilityStatement", "fhirVersion": "4.0.1"})),
    )
}

async fn search(
    State(s): State<Shared>,
    Path(resource_type): Path<String>,
    Query(q): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    if !authorized(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({})));
    }
    let mut s = s.lock().unwrap();
    s.requests.push(format!("{resource_type}?{:?}", {
        let mut keys: Vec<_> = q.iter().map(|(k, v)| format!("{k}={v}")).collect();
        keys.sort();
        keys
    }));
    let after = q
        .get("_lastUpdated")
        .and_then(|v| v.strip_prefix("gt"))
        .map(str::to_string);
    let count: usize = q.get("_count").and_then(|c| c.parse().ok()).unwrap_or(10);
    let page: usize = q.get("page").and_then(|p| p.parse().ok()).unwrap_or(0);
    let descending = q.get("_sort").is_some_and(|v| v == "-_lastUpdated");
    let mut matching: Vec<Value> = s
        .resources
        .iter()
        .filter(|r| r["resourceType"] == resource_type.as_str())
        .filter(|r| {
            after
                .as_ref()
                .is_none_or(|a| r["meta"]["lastUpdated"].as_str().unwrap() > a.as_str())
        })
        .cloned()
        .collect();
    matching.sort_by(|a, b| {
        a["meta"]["lastUpdated"]
            .as_str()
            .cmp(&b["meta"]["lastUpdated"].as_str())
    });
    if descending {
        matching.reverse();
    }
    let total = matching.len();
    let slice: Vec<Value> = matching
        .iter()
        .skip(page * count)
        .take(count)
        .map(|r| json!({"resource": r}))
        .collect();
    let mut links = vec![];
    if (page + 1) * count < total {
        let mut next = q.clone();
        next.insert("page".into(), (page + 1).to_string());
        let query: Vec<String> = next.iter().map(|(k, v)| format!("{k}={v}")).collect();
        links.push(json!({"relation": "next", "url": format!("{}/{resource_type}?{}", s.base, query.join("&"))}));
    }
    (
        StatusCode::OK,
        Json(
            json!({"resourceType": "Bundle", "type": "searchset", "total": total, "entry": slice, "link": links}),
        ),
    )
}

type Urls = Arc<Mutex<Vec<String>>>;

async fn batch(State(urls): State<Urls>, Json(bundle): Json<Value>) -> Json<Value> {
    let mut entries = Vec::new();
    for e in bundle["entry"].as_array().unwrap() {
        urls.lock()
            .unwrap()
            .push(e["request"]["url"].as_str().unwrap().to_string());
        entries.push(json!({"response": {"status": "201 Created"}}));
    }
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

fn python() -> Option<&'static str> {
    ["py", "python3", "python"].into_iter().find(|p| {
        std::process::Command::new(p)
            .arg("-c")
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

fn resource(resource_type: &str, id: &str, at: &str) -> Value {
    json!({"resourceType": resource_type, "id": id, "meta": {"lastUpdated": at}})
}

#[tokio::test]
async fn a_fhir_server_is_pulled_by_cursor_and_nothing_twice() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found");
        return;
    };
    let remote: Shared = Arc::default();
    remote.lock().unwrap().resources = vec![
        resource("Patient", "p1", "2026-10-01T10:00:00Z"),
        resource("Patient", "p2", "2026-10-02T10:00:00Z"),
        resource("Observation", "o1", "2026-10-01T11:00:00Z"),
        resource("Observation", "o2", "2026-10-02T11:00:00Z"),
        resource("Observation", "o3", "2026-10-03T11:00:00Z"),
    ];
    let remote_url = serve(
        Router::new()
            .route("/metadata", get(metadata))
            .route("/{resource_type}", get(search))
            .with_state(remote.clone()),
    )
    .await;
    remote.lock().unwrap().base = remote_url.clone();
    let urls: Urls = Arc::default();
    let hfs = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(urls.clone()),
    )
    .await;

    // The token lives in the environment, named by the source, never in the file.
    unsafe { std::env::set_var("HIE_TEST_REMOTE_TOKEN", "remote-token") };
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
        "id: other-fhir\nname: Other FHIR\nsource:\n  type: fhir_server\n  url: {remote_url}\n  types: [Patient, Observation]\n  page_size: 2\n  auth:\n    method: bearer\n    token_env: HIE_TEST_REMOTE_TOKEN\ntransform: transforms/fhir_passthrough.py\n"
    ))
    .unwrap();
    feed.resolve(&repo).unwrap();
    assert_eq!(feed.format, hie::config::Format::Ndjson);

    let db = Arc::new(Db::in_memory().unwrap());
    let source = Source::from_config(&feed).unwrap().with_db(db.clone());
    let runner = FeedRunner::with_source(
        feed.clone(),
        source,
        db.clone(),
        Arc::new(FhirClient::new(&hfs)),
        Arc::new(Alerter::new(db.clone(), None)),
        python,
        &repo.join("hie_runtime"),
        &hfs,
        "http://127.0.0.1:0",
    );

    // First pass: everything, Observations over two pages, one load per type.
    runner.tick().await.unwrap();
    let counts = db.feed_counts("other-fhir").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["files"].as_i64()),
        (Some(5), Some(2))
    );
    {
        let mut sent = urls.lock().unwrap().clone();
        sent.sort();
        assert_eq!(
            sent,
            [
                "Observation/o1",
                "Observation/o2",
                "Observation/o3",
                "Patient/p1",
                "Patient/p2"
            ]
        );
    }
    assert_eq!(
        db.cursors("other-fhir").unwrap(),
        [
            (
                "Observation".to_string(),
                "2026-10-03T11:00:00Z".to_string()
            ),
            ("Patient".to_string(), "2026-10-02T10:00:00Z".to_string()),
        ]
    );
    let loads = db
        .query_json("SELECT name, source_key FROM files ORDER BY id", [])
        .unwrap();
    assert!(loads[0]["name"].as_str().unwrap().starts_with("Patient-"));
    assert_eq!(loads[1]["source_key"], "cursor 2026-10-03T11:00:00Z");
    {
        let r = remote.lock().unwrap();
        let observation_pages = r
            .requests
            .iter()
            .filter(|q| q.starts_with("Observation?"))
            .count();
        assert_eq!(observation_pages, 2, "{:?}", r.requests);
    }

    // Second pass: nothing new, so no load and no request to HFS.
    runner.tick().await.unwrap();
    let counts = db.feed_counts("other-fhir").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["files"].as_i64()),
        (Some(5), Some(2))
    );
    {
        let r = remote.lock().unwrap();
        assert!(
            r.requests.iter().any(|q| q.starts_with("Observation?")
                && q.contains("_lastUpdated=gt2026-10-03T11:00:00Z")),
            "{:?}",
            r.requests
        );
    }

    // A newer Observation and a re-saved Patient: only those come through.
    remote
        .lock()
        .unwrap()
        .resources
        .push(resource("Observation", "o4", "2026-10-04T11:00:00Z"));
    remote
        .lock()
        .unwrap()
        .resources
        .push(resource("Patient", "p1", "2026-10-05T10:00:00Z"));
    runner.tick().await.unwrap();
    let counts = db.feed_counts("other-fhir").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["files"].as_i64()),
        (Some(7), Some(4))
    );
    assert_eq!(
        urls.lock().unwrap()[5..],
        ["Patient/p1".to_string(), "Observation/o4".to_string()]
    );
    assert_eq!(
        db.cursor("other-fhir", "Patient").unwrap().as_deref(),
        Some("2026-10-05T10:00:00Z")
    );

    // The connection test: reachable, and what is waiting past the cursors.
    let probe = Source::from_config(&feed)
        .unwrap()
        .with_db(db.clone())
        .probe()
        .await
        .unwrap();
    assert!(probe.reachable);
    assert_eq!(probe.objects, 0);
    remote
        .lock()
        .unwrap()
        .resources
        .push(resource("Observation", "o5", "2026-10-06T11:00:00Z"));
    let probe = Source::from_config(&feed)
        .unwrap()
        .with_db(db.clone())
        .probe()
        .await
        .unwrap();
    assert_eq!(
        (
            probe.objects,
            probe.newest.as_deref(),
            probe.newest_at.as_deref()
        ),
        (1, Some("Observation"), Some("2026-10-06T11:00:00Z"))
    );

    // Without the token the remote refuses, and the pass fails rather than silently pulling nothing.
    unsafe { std::env::set_var("HIE_TEST_REMOTE_TOKEN", "wrong") };
    let err = Source::from_config(&feed)
        .unwrap()
        .with_db(db.clone())
        .list()
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("401"), "{err:#}");
}

#[test]
fn a_fhir_server_source_is_checked_on_resolve() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let parse = |source: &str, delivery: &str| -> Result<hie::config::FeedConfig, String> {
        let mut feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
            "id: x\nname: X\nsource:\n{source}\ntransform: transforms/fhir_passthrough.py\n{delivery}"
        ))
        .map_err(|e| e.to_string())?;
        feed.resolve(&repo).map_err(|e| format!("{e:#}"))?;
        Ok(feed)
    };
    let err = parse("  type: fhir_server\n  url: http://x\n  types: []", "").unwrap_err();
    assert!(err.contains("at least one resource type"), "{err}");
    let err = parse("  type: fhir_server\n  url: x\n  types: [Patient]", "").unwrap_err();
    assert!(err.contains("http://"), "{err}");
    let err = parse(
        "  type: fhir_server\n  url: http://x\n  types: [Patient]",
        "delivery:\n  method: bulk_submit\n  bulk_from: source_objects\n",
    )
    .unwrap_err();
    assert!(err.contains("has no file for HFS to fetch"), "{err}");
    let ok = parse(
        "  type: fhir_server\n  url: http://x\n  types: [Patient]",
        "",
    )
    .unwrap();
    assert_eq!(ok.source.describe(), "http://x (Patient)");
}
