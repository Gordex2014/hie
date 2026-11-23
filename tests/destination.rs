//! The FHIR server's reachability in the monitoring status: from deliveries,
//! from the idle probe of `/metadata`, and the delivery alert around an outage.
//! Records are seeded already transformed, so no transform runs.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::routing::{get, post};
use axum::{Json, Router};
use hie::credentials::{CredentialSettings, TokenSource};
use hie::destination::Destination;
use hie::pipeline::FeedRunner;
use serde_json::{Value, json};

/// A stand-in FHIR server under `/fhir`.
#[derive(Default)]
struct Hfs {
    /// A batch answers this status instead of 200.
    post_status: Option<u16>,
    /// A batch answers 200 with every entry 503.
    entries_unavailable: bool,
    /// `/metadata` answers this status instead of 200.
    metadata_status: Option<u16>,
    posts: u32,
    metadata_hits: u32,
    /// The path and whether an `Authorization` header came, per probe.
    probes: Vec<(String, bool)>,
    /// What the alert webhook received.
    hooks: Vec<Value>,
}

type Shared = Arc<Mutex<Hfs>>;

async fn batch(State(s): State<Shared>, Json(bundle): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut s = s.lock().unwrap();
    s.posts += 1;
    if let Some(status) = s.post_status {
        return (
            StatusCode::from_u16(status).unwrap(),
            Json(json!({"resourceType": "OperationOutcome"})),
        );
    }
    let status = if s.entries_unavailable {
        "503 Service Unavailable"
    } else {
        "201 Created"
    };
    let entries: Vec<Value> = bundle["entry"]
        .as_array()
        .unwrap()
        .iter()
        .map(|_| json!({"response": {"status": status}}))
        .collect();
    (
        StatusCode::OK,
        Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries})),
    )
}

async fn metadata(State(s): State<Shared>, uri: Uri, headers: HeaderMap) -> StatusCode {
    let mut s = s.lock().unwrap();
    s.metadata_hits += 1;
    s.probes.push((
        uri.path_and_query().unwrap().to_string(),
        headers.contains_key("authorization"),
    ));
    StatusCode::from_u16(s.metadata_status.unwrap_or(200)).unwrap()
}

async fn bulk_submit(State(s): State<Shared>) -> StatusCode {
    StatusCode::from_u16(s.lock().unwrap().post_status.unwrap_or(202)).unwrap()
}

async fn status_page() -> StatusCode {
    StatusCode::SERVICE_UNAVAILABLE
}

async fn hook(State(s): State<Shared>, Json(payload): Json<Value>) -> StatusCode {
    s.lock().unwrap().hooks.push(payload);
    StatusCode::OK
}

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

/// The stand-in's origin; its FHIR base is `{origin}/fhir`.
async fn hfs() -> (String, Shared) {
    let state: Shared = Arc::default();
    let app = Router::new()
        .route("/fhir", post(batch))
        .route("/fhir/metadata", get(metadata))
        .route("/fhir/$bulk-submit", post(bulk_submit))
        .route("/fhir/$bulk-submit-status", post(bulk_submit))
        .route("/fhir/status/{id}", get(status_page))
        .route("/hook", post(hook))
        .with_state(state.clone());
    (serve(app).await, state)
}

/// An address nothing listens on.
async fn closed() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("127.0.0.1:{}", addr.port())
}

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

struct Engine {
    _root: tempfile::TempDir,
    runner: Arc<FeedRunner>,
    state: hie::api::AppState,
    api: String,
    _manager: Arc<hie::manager::SourceManager>,
}

/// The engine's files: `extra` is appended to `hie.yaml`.
fn write_config(root: &Path, base: &str, probe_seconds: u64, extra: &str) {
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::create_dir_all(root.join("inbox")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!(
            "fhir_base_url: {base}\ndatabase: data/hie.db\ndestination:\n  probe_seconds: {probe_seconds}\n{extra}"
        ),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/lab-results.yaml"),
        "id: lab-results\nname: Lab results\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  min_file_age_ms: 0\n\
         transform: transforms/lab_results.py\ndelivery:\n  initial_backoff_ms: 3600000\n  max_backoff_ms: 3600000\n  alert_after_attempts: 1\n",
    )
    .unwrap();
}

async fn start(root: tempfile::TempDir, credentials: hie::credentials::Credentials) -> Engine {
    let (config, feeds) = hie::config::load(&root.path().join("hie.yaml")).unwrap();
    let (manager, state) = hie::build(
        &config,
        feeds,
        &repo().join("hie_runtime"),
        None,
        credentials,
    )
    .unwrap();
    let runner = manager.runner("lab-results").unwrap();
    manager.pause("lab-results", None).unwrap(); // the test drives the passes
    let api = serve(hie::api::router(state.clone())).await;
    Engine {
        _root: root,
        runner,
        state,
        api,
        _manager: manager,
    }
}

async fn engine(base: &str, probe_seconds: u64) -> Engine {
    let root = tempfile::tempdir().unwrap();
    write_config(root.path(), base, probe_seconds, "");
    start(root, None).await
}

impl Engine {
    /// One record of the source, transformed and due for delivery.
    fn seed(&self) {
        let db = &self.state.db;
        let n = db
            .query_json("SELECT COUNT(*) AS n FROM files", [])
            .unwrap()[0]["n"]
            .as_i64()
            .unwrap();
        let file = db
            .insert_file(
                "lab-results",
                "Lab results",
                &format!("seed-{n}.csv"),
                &format!("sha-{n}"),
                1,
                "processing",
                None,
                None,
                None,
            )
            .unwrap();
        db.insert_records(file, "lab-results", &[(1, json!({"row": n}), None)])
            .unwrap();
        for item in db.received_records("lab-results", 100).unwrap() {
            let id = item.id;
            db.mark_transformed(
                &item,
                &json!([{"resourceType": "Observation", "id": format!("o{id}"), "status": "final"}]),
            )
            .unwrap();
        }
    }

    /// The records waiting on a retry are due now.
    fn make_due(&self) {
        self.state
            .db
            .query_json(
                "UPDATE records SET next_attempt_at = 0 WHERE status = 'pending' RETURNING id",
                [],
            )
            .unwrap();
    }

    async fn destination(&self) -> Value {
        let status: Value = reqwest::get(format!("{}/api/monitoring/status", self.api))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        status["destination"].clone()
    }

    /// `(kind, rule_id, cleared_at, cleared_reason)` of every alert of a source.
    fn alerts(&self, source: &str) -> Vec<(String, Option<i64>, bool, Option<String>)> {
        self.state
            .db
            .query_json(
                "SELECT kind, rule_id, cleared_at, cleared_reason FROM alerts WHERE feed_id = $1 ORDER BY id",
                [hie::db::Param::Text(source.to_string())],
            )
            .unwrap()
            .into_iter()
            .map(|a| {
                (
                    a["kind"].as_str().unwrap().to_string(),
                    a["rule_id"].as_i64(),
                    !a["cleared_at"].is_null(),
                    a["cleared_reason"].as_str().map(str::to_string),
                )
            })
            .collect()
    }

    fn pending(&self) -> i64 {
        self.state.db.feed_counts("lab-results").unwrap()["pending"]
            .as_i64()
            .unwrap()
    }
}

/// Waits, up to five seconds, for `done` to hold.
async fn until(mut done: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done().await {
        assert!(Instant::now() < deadline, "timed out");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn instant(v: &Value) -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::parse_from_rfc3339(v.as_str().unwrap()).unwrap()
}

const RECOVERED: &str = "Lab results: delivery to HFS recovered";

#[tokio::test]
async fn an_unreachable_server_is_reported_with_a_fixed_reason_and_no_url() {
    let addr = closed().await;
    let e = engine(&format!("http://hie:CANARY-PASSWORD@{addr}/CANARY-PATH"), 0).await;
    assert_eq!(
        e.destination().await,
        json!({"reachable": null, "since": null, "last_error": null, "consecutive_failures": 0, "observed_at": null})
    );
    e.seed();
    e.runner.tick().await.unwrap();
    let d = e.destination().await;
    assert_eq!(d["reachable"], false, "{d}");
    assert_eq!(d["last_error"], "could not connect to the FHIR server");
    assert_eq!(d["consecutive_failures"], 1);
    assert_eq!(d["since"], d["observed_at"]);
    instant(&d["since"]);
    let text = d.to_string();
    assert!(
        !text.contains("CANARY") && !text.contains("127.0.0.1"),
        "{d}"
    );
    // The record's error and the delivery alert name the server, not its URL.
    for row in e
        .state
        .db
        .query_json(
            "SELECT error AS text FROM records UNION ALL SELECT message FROM alerts",
            [],
        )
        .unwrap()
    {
        let text = row["text"].as_str().unwrap();
        assert!(
            text.contains("could not connect to the FHIR server"),
            "{text}"
        );
        assert!(!text.contains("CANARY") && !text.contains(&addr), "{text}");
    }
}

#[tokio::test]
async fn a_server_that_answers_503_and_comes_back() {
    let (origin, hfs) = hfs().await;
    hfs.lock().unwrap().post_status = Some(503);
    let e = engine(&format!("{origin}/fhir"), 0).await;
    e.seed();
    e.runner.tick().await.unwrap();
    let down = e.destination().await;
    assert_eq!(down["reachable"], false, "{down}");
    assert_eq!(down["last_error"], "the FHIR server answered HTTP 503");
    assert_eq!(
        e.alerts("lab-results"),
        [("delivery".into(), None, false, None)]
    );

    hfs.lock().unwrap().post_status = None;
    tokio::time::sleep(Duration::from_millis(5)).await;
    e.make_due();
    e.runner.tick().await.unwrap();
    assert_eq!(e.pending(), 0);
    let up = e.destination().await;
    assert_eq!(up["reachable"], true, "{up}");
    assert_eq!(up["last_error"], Value::Null);
    assert_eq!(up["consecutive_failures"], 0);
    assert!(instant(&up["since"]) > instant(&down["since"]));
    // The outage alert is cleared by the recovery, and nothing else is raised.
    assert_eq!(
        e.alerts("lab-results"),
        [("delivery".into(), None, true, Some(RECOVERED.into()))]
    );
}

#[tokio::test]
async fn a_batch_whose_every_entry_is_to_be_retried_does_not_end_the_outage() {
    let (origin, hfs) = hfs().await;
    hfs.lock().unwrap().post_status = Some(503);
    let e = engine(&format!("{origin}/fhir"), 0).await;
    e.seed();
    e.runner.tick().await.unwrap();
    {
        let mut h = hfs.lock().unwrap();
        h.post_status = None;
        h.entries_unavailable = true;
    }
    e.make_due();
    e.runner.tick().await.unwrap();
    // The server answered, so it is reachable; but nothing was delivered.
    assert_eq!(e.destination().await["reachable"], true);
    assert_eq!(e.pending(), 1);
    assert_eq!(
        e.alerts("lab-results"),
        [("delivery".into(), None, false, None)]
    );

    hfs.lock().unwrap().entries_unavailable = false;
    e.make_due();
    e.runner.tick().await.unwrap();
    assert_eq!(
        e.alerts("lab-results"),
        [("delivery".into(), None, true, Some(RECOVERED.into()))]
    );
}

#[tokio::test]
async fn the_idle_probe_reports_the_server_with_no_delivery_and_no_credentials() {
    let (origin, hfs) = hfs().await;
    let base = format!("{origin}/fhir?CANARY=1").replace("http://", "http://hie:CANARY@");
    let e = engine(&base, 1).await;
    // Nothing observed yet: the first probe goes out at once.
    until(async || e.destination().await["reachable"] == true).await;
    {
        let h = hfs.lock().unwrap();
        assert_eq!(h.posts, 0);
        assert_eq!(h.probes[0], ("/fhir/metadata".to_string(), false));
    }
    let first = e.destination().await;

    hfs.lock().unwrap().metadata_status = Some(503);
    until(async || e.destination().await["reachable"] == false).await;
    let down = e.destination().await;
    assert_eq!(down["last_error"], "the FHIR server answered HTTP 503");
    assert!(instant(&down["observed_at"]) > instant(&first["observed_at"]));
    until(async || e.destination().await["consecutive_failures"].as_u64() >= Some(2)).await;
    let still = e.destination().await;
    assert_eq!(still["since"], down["since"], "the same outage");

    hfs.lock().unwrap().metadata_status = None;
    until(async || e.destination().await["reachable"] == true).await;
    assert!(instant(&e.destination().await["since"]) > instant(&down["since"]));
    assert!(hfs.lock().unwrap().probes.iter().all(|(_, auth)| !auth));
}

#[tokio::test]
async fn with_the_probe_off_nothing_is_probed() {
    let (off_origin, off) = hfs().await;
    let (on_origin, on) = hfs().await;
    let quiet = engine(&format!("{off_origin}/fhir"), 0).await;
    let probing = engine(&format!("{on_origin}/fhir"), 1).await;
    // The control: the same build with the probe on probes at once, then again
    // a second later.
    until(async || on.lock().unwrap().metadata_hits >= 2).await;
    assert_eq!(probing.destination().await["reachable"], true);
    assert_eq!(off.lock().unwrap().metadata_hits, 0);
    assert_eq!(
        quiet.destination().await,
        json!({"reachable": null, "since": null, "last_error": null, "consecutive_failures": 0, "observed_at": null})
    );
}

#[tokio::test]
async fn a_probe_that_gets_an_answer_does_not_contradict_a_delivery_answered_503() {
    let (origin, hfs) = hfs().await;
    let e = engine(&format!("{origin}/fhir"), 1).await;
    until(async || e.destination().await["reachable"] == true).await;
    hfs.lock().unwrap().post_status = Some(503);
    e.seed();
    e.runner.tick().await.unwrap();
    let down = e.destination().await;
    assert_eq!(down["reachable"], false, "{down}");
    // The probe keeps going and `/metadata` answers, but the outage stands.
    let hits = hfs.lock().unwrap().metadata_hits;
    until(async || hfs.lock().unwrap().metadata_hits >= hits + 2).await;
    let after = e.destination().await;
    assert_eq!(after["reachable"], false, "{after}");
    assert_eq!(after["since"], down["since"]);
    assert_eq!(after["last_error"], "the FHIR server answered HTTP 503");
}

#[tokio::test]
async fn a_token_that_cannot_be_fetched_says_nothing_about_the_server() {
    let (origin, hfs) = hfs().await;
    let idp = closed().await;
    let root = tempfile::tempdir().unwrap();
    write_config(root.path(), &format!("{origin}/fhir"), 0, "");
    let tokens = TokenSource::new(CredentialSettings {
        token_url: format!("http://{idp}/token"),
        client_id: "hie".into(),
        client_secret: "s3cret".into(),
        scope: None,
    });
    let e = start(root, Some(Arc::new(tokens))).await;
    e.seed();
    e.runner.tick().await.unwrap();
    assert_eq!(e.pending(), 1);
    assert_eq!(hfs.lock().unwrap().posts, 0);
    assert_eq!(e.destination().await["reachable"], Value::Null);
}

#[tokio::test]
async fn bulk_requests_under_the_base_are_observed_and_status_pages_are_not() {
    let (origin, hfs) = hfs().await;
    let base = format!("{origin}/fhir");
    let destination = Arc::new(Destination::new(&base));
    let bulk =
        hie::bulk::BulkSubmitter::new(&base, "http://hie").with_destination(destination.clone());
    hfs.lock().unwrap().post_status = Some(503);
    assert!(
        bulk.kickoff("lab-results", "s1", "http://hie/m")
            .await
            .is_err()
    );
    let s = destination.snapshot();
    assert_eq!(s.reachable, Some(false));
    assert_eq!(s.last_error, Some("the FHIR server answered HTTP 503"));
    hfs.lock().unwrap().post_status = None;
    bulk.kickoff("lab-results", "s1", "http://hie/m")
        .await
        .unwrap();
    assert_eq!(destination.snapshot().reachable, Some(true));
    // The status request is a POST under the base: observed.
    hfs.lock().unwrap().post_status = Some(503);
    assert!(bulk.status_url("lab-results", "s1").await.is_err());
    assert_eq!(destination.snapshot().reachable, Some(false));
    hfs.lock().unwrap().post_status = None;
    bulk.kickoff("lab-results", "s1", "http://hie/m")
        .await
        .unwrap();
    // A status page is wherever HFS says, so it is not observed: neither one
    // on the server's own origin answering 503, nor one that cannot be reached.
    assert!(bulk.poll(&format!("{base}/status/1")).await.is_err());
    assert_eq!(destination.snapshot().reachable, Some(true));
    let elsewhere = closed().await;
    assert!(
        bulk.poll(&format!("http://{elsewhere}/status/1"))
            .await
            .is_err()
    );
    let s = destination.snapshot();
    assert_eq!(s.reachable, Some(true));
    assert_eq!(s.consecutive_failures, 0);
}

#[tokio::test]
async fn delivery_alerts_left_open_by_earlier_versions_are_closed_or_cleared() {
    let (origin, hfs) = hfs().await;
    let root = tempfile::tempdir().unwrap();
    write_config(root.path(), &format!("{origin}/fhir"), 0, "");
    // Kept open to the end: on PostgreSQL the handle that made the test's
    // schema drops it when it closes.
    let legacy = {
        // As an older version left the store: an outage and its recovery
        // notice, both open, for this source; an outage with no notice after
        // it for another.
        std::fs::create_dir_all(root.path().join("data")).unwrap();
        let db = hie::db::Db::connect(&root.path().join("data/hie.db").to_string_lossy()).unwrap();
        db.insert_alert(Some("lab-results"), "delivery", "outage", None)
            .unwrap();
        db.insert_alert(Some("lab-results"), "delivery_recovered", "recovered", None)
            .unwrap();
        db.insert_alert(Some("old-source"), "delivery", "outage", None)
            .unwrap();
        // An outage alert whose source delivered a record after it, though
        // no recovery notice was written (the engine restarted in between):
        // the record retried and delivered after it is not the newest one,
        // which was delivered before the outage.
        let file = db
            .insert_file(
                "sent-source",
                "Sent source",
                "a.csv",
                "sha-a",
                1,
                "processing",
                None,
                None,
                None,
            )
            .unwrap();
        db.insert_records(
            file,
            "sent-source",
            &[(1, json!({}), None), (2, json!({}), None)],
        )
        .unwrap();
        for item in db.received_records("sent-source", 2).unwrap() {
            db.mark_transformed(&item, &json!([])).unwrap();
        }
        let mut due = db.due_records("sent-source", 2).unwrap();
        let (retried, newest) = (due.remove(0), due.remove(0));
        assert!(newest.id > retried.id);
        db.delivered(&[(&newest, hie::db::Delivery::Sent(&json!([])))])
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
        db.insert_alert(Some("sent-source"), "delivery", "outage", None)
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
        db.delivered(&[(&retried, hie::db::Delivery::Sent(&json!([])))])
            .unwrap();
        db
    };
    let e = start(root, None).await;
    assert_eq!(
        e.alerts("lab-results"),
        [
            (
                "delivery".into(),
                None,
                true,
                Some(
                    "delivery to HFS recovered (recorded before recovery cleared this alert)"
                        .into()
                )
            ),
            (
                "delivery_recovered".into(),
                None,
                true,
                Some("a recovery notice; recovery now clears the delivery alert instead".into())
            ),
        ]
    );
    assert_eq!(
        e.alerts("old-source"),
        [("delivery".into(), None, false, None)],
        "maybe still going on"
    );
    assert_eq!(
        e.alerts("sent-source"),
        [(
            "delivery".into(),
            None,
            true,
            Some("delivery to HFS recovered (recorded before recovery cleared this alert)".into())
        )]
    );

    // Once per store: alerts of that shape written later stay, for recovery
    // to clear with its event.
    let db = &e.state.db;
    let later = db
        .insert_alert(Some("later-source"), "delivery", "outage", None)
        .unwrap();
    db.insert_alert(
        Some("later-source"),
        "delivery_recovered",
        "recovered",
        None,
    )
    .unwrap();
    assert_eq!(db.close_legacy_recoveries().unwrap(), 0);
    assert!(
        e.alerts("later-source").iter().all(|a| !a.2),
        "{later}: {:?}",
        e.alerts("later-source")
    );

    // A new outage is alerted, though the old one was never followed by a success.
    hfs.lock().unwrap().post_status = Some(503);
    e.seed();
    e.runner.tick().await.unwrap();
    assert_eq!(e.alerts("lab-results").len(), 3);
    assert_eq!(
        e.alerts("lab-results")[2],
        ("delivery".into(), None, false, None)
    );
    // A rule-governed outage alert whose rule is gone is cleared by the recovery too.
    let gone = hie::db::Rule {
        id: 999,
        source_id: Some("lab-results".into()),
        condition: "unreachable".into(),
        threshold: "1".into(),
        notify: vec![],
        channels: vec!["app".into()],
        enabled: true,
        created_by: None,
        created_at: None,
        tz: None,
    };
    e.state
        .db
        .insert_alert(Some("lab-results"), "delivery", "outage", Some(&gone))
        .unwrap();
    hfs.lock().unwrap().post_status = None;
    e.make_due();
    e.runner.tick().await.unwrap();
    let after = e.alerts("lab-results");
    assert_eq!(
        after[2],
        ("delivery".into(), None, true, Some(RECOVERED.into()))
    );
    assert_eq!(
        after[3],
        ("delivery".into(), Some(999), true, Some(RECOVERED.into()))
    );
    assert!(!e.alerts("old-source")[0].2, "another source's alert stays");
    drop(e);
    drop(legacy);
}

/// An open rule-less outage alert is what keeps a second one from being
/// raised for the same outage: retention keeps it whatever its age.
#[tokio::test]
async fn retention_keeps_an_open_outage_alert() {
    let e = engine("http://127.0.0.1:9/fhir", 0).await;
    let db = &e.state.db;
    let outage = db
        .insert_alert(Some("lab-results"), "delivery", "outage", None)
        .unwrap();
    let old = db
        .insert_alert(Some("lab-results"), "silence", "quiet", None)
        .unwrap();
    db.clear_alert(old, "done").unwrap();
    // Newer than both, so neither is kept as the newest.
    db.insert_alert(Some("lab-results"), "unreadable_file", "x", None)
        .unwrap();
    db.query_json(
        "UPDATE alerts SET created_at = 1, cleared_at = CASE WHEN cleared_at IS NULL THEN NULL ELSE 1 END
         WHERE id IN ($1, $2) RETURNING id",
        [hie::db::Param::Int(outage), hie::db::Param::Int(old)],
    )
    .unwrap();
    assert_eq!(db.purge_alerts(hie::db::now_ms(), 100).unwrap(), 1);
    let left: Vec<i64> = db
        .query_json("SELECT id FROM alerts ORDER BY id", [])
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert!(left.contains(&outage), "{left:?}");
    assert!(!left.contains(&old), "{left:?}");
}

#[test]
fn the_probe_interval_is_checked() {
    let dir = tempfile::tempdir().unwrap();
    let load = |destination: &str| {
        std::fs::write(
            dir.path().join("hie.yaml"),
            format!("fhir_base_url: http://hfs\ndatabase: data/hie.db\n{destination}"),
        )
        .unwrap();
        hie::config::load(&dir.path().join("hie.yaml")).map(|(c, _)| c.destination.probe_seconds)
    };
    assert_eq!(load("").unwrap(), 60);
    assert_eq!(load("destination:\n  probe_seconds: 0\n").unwrap(), 0);
    assert_eq!(
        load("destination:\n  probe_seconds: 86400\n").unwrap(),
        86400
    );
    let too_long = load("destination:\n  probe_seconds: 86401\n").unwrap_err();
    assert!(
        format!("{too_long:#}").contains("at most 86400"),
        "{too_long:#}"
    );
    assert!(load("destination:\n  probe_every: 5\n").is_err());
}

#[tokio::test]
async fn a_connection_closed_before_the_answer_is_a_lost_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            drop(socket);
        }
    });
    let client = hie::fhir::FhirClient::new(&format!("http://{addr}/fhir"));
    let sent = client
        .post_bundle(&json!({"resourceType": "Bundle", "type": "batch", "entry": []}))
        .await;
    assert!(
        matches!(sent, Err(hie::fhir::SendError::Transient(_))),
        "{sent:?}"
    );
    let s = client.destination().snapshot();
    assert_eq!(s.reachable, Some(false));
    assert_eq!(
        s.last_error,
        Some("the connection to the FHIR server was lost")
    );
}

#[tokio::test]
async fn the_webhook_hears_one_clear_per_recovered_outage_and_nothing_at_start() {
    let (origin, hfs) = hfs().await;
    let root = tempfile::tempdir().unwrap();
    write_config(
        root.path(),
        &format!("{origin}/fhir"),
        0,
        &format!("alerts:\n  webhook_url: {origin}/hook\n"),
    );
    // Kept open to the end, as above.
    let legacy = {
        std::fs::create_dir_all(root.path().join("data")).unwrap();
        let db = hie::db::Db::connect(&root.path().join("data/hie.db").to_string_lossy()).unwrap();
        db.insert_alert(Some("lab-results"), "delivery", "outage", None)
            .unwrap();
        db.insert_alert(Some("lab-results"), "delivery_recovered", "recovered", None)
            .unwrap();
        db
    };
    let e = start(root, None).await;
    hfs.lock().unwrap().post_status = Some(503);
    e.seed();
    e.runner.tick().await.unwrap();
    until(async || hfs.lock().unwrap().hooks.len() == 1).await;
    hfs.lock().unwrap().post_status = None;
    e.make_due();
    e.runner.tick().await.unwrap();
    until(async || hfs.lock().unwrap().hooks.len() == 2).await;
    // A recovery with nothing open posts nothing more.
    e.seed();
    e.runner.tick().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let hooks = hfs.lock().unwrap().hooks.clone();
    assert_eq!(hooks.len(), 2, "{hooks:?}");
    assert_eq!(hooks[0]["event"], "fired", "{hooks:?}");
    assert_eq!(hooks[0]["alert"]["kind"], "delivery");
    assert_eq!(hooks[1]["event"], "cleared", "{hooks:?}");
    assert_eq!(hooks[1]["alert"]["kind"], "delivery");
    assert_eq!(hooks[1]["alert"]["cleared_reason"], RECOVERED);
    // Both name the new outage's alert.
    let outage = e
        .state
        .db
        .query_json(
            "SELECT id FROM alerts WHERE kind = 'delivery' AND cleared_reason = $1",
            [hie::db::Param::Text(RECOVERED.into())],
        )
        .unwrap();
    assert_eq!(outage.len(), 1, "{outage:?}");
    assert_eq!(hooks[0]["alert"]["id"], outage[0]["id"]);
    assert_eq!(hooks[1]["alert"]["id"], outage[0]["id"]);
    drop(e);
    drop(legacy);
}
