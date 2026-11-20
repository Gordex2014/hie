//! The alert webhook against a local receiver: the signature over the raw body,
//! retries with one stable `event_id`, giving up after five retries, redirects
//! refused, two instances on one store sending each event once, an attempt whose
//! outcome was lost sent again with the same bytes, and events parked while
//! their URL is not the configured one. Driven through `Alerter` on a store of
//! its own, no transform involved. A test binary of its own, so its log
//! subscriber is the process's: no URL, secret or receiver answer may reach it.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use hie::alerts::{Alerter, SenderGuard, SenderTuning};
use hie::config::AlertConfig;
use hie::db::Db;
use serde_json::{Value, json};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

/// Values that must never appear in a log line or a stored error.
const URL_CANARY: &str = "urlcanary7f3a";
const SECRET_CANARY: &str = "secretcanary91bd";
const BODY_CANARY: &str = "bodycanary44e0";

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

/// Everything the process logs at TRACE, through the production filter.
fn logs() -> &'static Captured {
    static LOGS: OnceLock<Captured> = OnceLock::new();
    LOGS.get_or_init(|| {
        let captured = Captured::default();
        let writer = captured.clone();
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(move || writer.clone())
                    .with_filter(hie::log_filter(Some("trace"))),
            )
            .init();
        captured
    })
}

fn assert_no_canary_logged() {
    let logged = String::from_utf8_lossy(&logs().0.lock().unwrap()).into_owned();
    assert!(
        logged.contains("alert webhook"),
        "the capture sees the sender"
    );
    for canary in [URL_CANARY, SECRET_CANARY, BODY_CANARY] {
        assert!(!logged.contains(canary), "{canary} was logged");
    }
}

/// One request as the receiver got it.
#[derive(Clone, Debug)]
struct Received {
    headers: HeaderMap,
    body: Bytes,
}

impl Received {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
    fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .map(|v| v.to_str().unwrap().to_string())
    }
}

/// A receiver answering from a script of statuses, then 200; a 302 sends the
/// client to `/elsewhere`, which counts its own hits.
#[derive(Clone, Default)]
struct Receiver {
    got: Arc<Mutex<Vec<Received>>>,
    script: Arc<Mutex<Vec<u16>>>,
    always: Arc<Mutex<Option<u16>>>,
    elsewhere: Arc<Mutex<usize>>,
}

impl Receiver {
    fn got(&self) -> Vec<Received> {
        self.got.lock().unwrap().clone()
    }
}

async fn receive(State(r): State<Receiver>, headers: HeaderMap, body: Bytes) -> Response {
    r.got.lock().unwrap().push(Received { headers, body });
    let status = {
        let mut script = r.script.lock().unwrap();
        if script.is_empty() {
            r.always.lock().unwrap().unwrap_or(200)
        } else {
            script.remove(0)
        }
    };
    if status == 302 {
        return (StatusCode::FOUND, [(header::LOCATION, "/elsewhere")]).into_response();
    }
    (
        StatusCode::from_u16(status).unwrap(),
        format!("receiver says {BODY_CANARY}"),
    )
        .into_response()
}

async fn elsewhere(State(r): State<Receiver>) -> StatusCode {
    *r.elsewhere.lock().unwrap() += 1;
    StatusCode::OK
}

/// Starts a receiver; its URL carries the URL canary in its path.
async fn receiver() -> (Receiver, String) {
    logs();
    let r = Receiver::default();
    let app = Router::new()
        .route(&format!("/hook/{URL_CANARY}"), post(receive))
        .route("/elsewhere", post(elsewhere).get(elsewhere))
        .with_state(r.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (r, format!("http://{addr}/hook/{URL_CANARY}"))
}

fn tuning() -> SenderTuning {
    SenderTuning {
        lease: Duration::from_millis(800),
        connect_timeout: Duration::from_millis(500),
        timeout: Duration::from_millis(600),
        idle_poll: Duration::from_millis(100),
    }
}

fn settings(url: Option<&str>, secret: Option<&str>, backoff_ms: u64) -> AlertConfig {
    AlertConfig {
        webhook_url: url.map(str::to_string),
        webhook_secret: secret.map(|s| secrecy::SecretString::from(s.to_string())),
        webhook_backoff_ms: Some(backoff_ms),
    }
}

struct Engine {
    db: Arc<Db>,
    alerts: Arc<Alerter>,
    _sender: Option<SenderGuard>,
}

fn engine(path: &std::path::Path, config: &AlertConfig, start: bool) -> Engine {
    let db = Arc::new(Db::open(path).unwrap());
    let alerts = Arc::new(Alerter::from_config(db.clone(), config));
    alerts.know_source("lab-results", "Lab results");
    let sender = if start { alerts.start(tuning()) } else { None };
    Engine {
        db,
        alerts,
        _sender: sender,
    }
}

/// The stored webhook events of an alert, oldest first.
fn events(db: &Db, alert: i64) -> Vec<Value> {
    db.query_json(
        "SELECT event_id, event, state, attempts, last_error, last_status, delivered_at, payload
         FROM alert_webhooks WHERE alert_id = $1 ORDER BY id",
        vec![hie::db::Param::Int(alert)],
    )
    .unwrap()
}

fn last_alert(db: &Db) -> i64 {
    db.query_json("SELECT MAX(id) AS id FROM alerts", vec![])
        .unwrap()[0]["id"]
        .as_i64()
        .unwrap()
}

async fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
    let started = Instant::now();
    while !ok() {
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn fired_and_cleared_events_are_signed_over_the_raw_body() {
    let (receiver, url) = receiver().await;
    let dir = tempfile::tempdir().unwrap();
    let e = engine(
        &dir.path().join("hie.db"),
        &settings(Some(&url), Some(SECRET_CANARY), 50),
        true,
    );
    e.alerts
        .raise(Some("lab-results"), "unreadable_file", "cannot read a.csv")
        .unwrap();
    let alert = last_alert(&e.db);
    assert!(
        e.alerts
            .clear_open("lab-results", "unreadable_file", None, "read again")
            .unwrap()
    );
    wait_for("two events", || receiver.got().len() == 2).await;

    let got = receiver.got();
    for r in &got {
        assert_eq!(
            r.header("x-hie-signature").unwrap(),
            hie::alerts::signature(SECRET_CANARY.as_bytes(), &r.body)
        );
        assert_eq!(r.header("content-type").unwrap(), "application/json");
        assert!(r.header("user-agent").unwrap().starts_with("hie/"));
        let body = r.json();
        assert_eq!(r.header("x-hie-event-id").unwrap(), body["event_id"]);
        assert_eq!(r.header("x-hie-event").unwrap(), body["event"]);
        let mut keys: Vec<_> = body.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, ["alert", "event", "event_id", "source", "text"]);
        let mut alert_keys: Vec<_> = body["alert"].as_object().unwrap().keys().cloned().collect();
        alert_keys.sort();
        assert_eq!(
            alert_keys,
            [
                "cleared_at",
                "cleared_reason",
                "created_at",
                "id",
                "kind",
                "message",
                "notify",
                "rule_id"
            ]
        );
        assert_eq!(body["alert"]["id"], alert);
        assert_eq!(
            body["source"],
            json!({"id": "lab-results", "name": "Lab results"})
        );
    }
    let (fired, cleared) = (got[0].json(), got[1].json());
    assert_eq!(
        (fired["event"].as_str(), cleared["event"].as_str()),
        (Some("fired"), Some("cleared"))
    );
    assert_ne!(fired["event_id"], cleared["event_id"]);
    assert_eq!(fired["alert"]["cleared_at"], Value::Null);
    assert_eq!(cleared["alert"]["cleared_reason"], "read again");
    assert_eq!(cleared["alert"]["created_at"], fired["alert"]["created_at"]);
    assert!(cleared["alert"]["cleared_at"].is_string());

    wait_for("both recorded", || {
        events(&e.db, alert)
            .iter()
            .all(|ev| ev["state"] == "delivered")
    })
    .await;
    for ev in events(&e.db, alert) {
        assert_eq!(ev["attempts"], 1);
        assert_eq!(ev["last_status"], 200);
        assert!(ev["delivered_at"].is_i64());
    }
    assert_no_canary_logged();
}

#[tokio::test]
async fn a_failing_receiver_gets_the_same_event_until_it_answers() {
    let (receiver, url) = receiver().await;
    receiver.script.lock().unwrap().extend([500, 503]);
    let dir = tempfile::tempdir().unwrap();
    let e = engine(
        &dir.path().join("hie.db"),
        &settings(Some(&url), None, 50),
        true,
    );
    e.alerts
        .raise(Some("lab-results"), "unreadable_file", "cannot read b.csv")
        .unwrap();
    let alert = last_alert(&e.db);
    wait_for("delivered", || {
        events(&e.db, alert)[0]["state"] == "delivered"
    })
    .await;

    let got = receiver.got();
    assert_eq!(got.len(), 3);
    assert!(
        got.iter().all(|r| r.body == got[0].body),
        "one body, byte for byte"
    );
    assert!(
        got.iter().all(|r| r.header("x-hie-signature").is_none()),
        "unsigned without a secret"
    );
    // The receiver's side of de-duplication: one event, however many requests.
    let mut ids: Vec<String> = got
        .iter()
        .map(|r| r.header("x-hie-event-id").unwrap())
        .collect();
    ids.dedup();
    assert_eq!(ids.len(), 1);
    let ev = &events(&e.db, alert)[0];
    assert_eq!(ev["attempts"], 3);
    assert_eq!(ev["last_status"], 200);
    assert_eq!(ev["last_error"], "HTTP 503 Service Unavailable");
    assert_no_canary_logged();
}

#[tokio::test]
async fn the_engine_gives_up_after_five_retries() {
    let (receiver, url) = receiver().await;
    *receiver.always.lock().unwrap() = Some(500);
    let dir = tempfile::tempdir().unwrap();
    let e = engine(
        &dir.path().join("hie.db"),
        &settings(Some(&url), None, 20),
        true,
    );
    e.alerts
        .raise(Some("lab-results"), "unreadable_file", "cannot read c.csv")
        .unwrap();
    let alert = last_alert(&e.db);
    wait_for("failed", || events(&e.db, alert)[0]["state"] == "failed").await;
    // Longer than the next backoff (20 ms × 2⁵) would have been.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(receiver.got().len(), 6);
    let ev = &events(&e.db, alert)[0];
    assert_eq!(ev["attempts"], 6);
    assert_eq!(ev["last_status"], 500);
    assert_eq!(ev["last_error"], "HTTP 500 Internal Server Error");
    assert!(!ev["last_error"].as_str().unwrap().contains(BODY_CANARY));
    assert_no_canary_logged();
}

#[tokio::test]
async fn a_redirect_is_a_failed_attempt_and_is_not_followed() {
    let (receiver, url) = receiver().await;
    receiver.script.lock().unwrap().push(302);
    let dir = tempfile::tempdir().unwrap();
    let e = engine(
        &dir.path().join("hie.db"),
        &settings(Some(&url), Some(SECRET_CANARY), 60_000),
        true,
    );
    e.alerts
        .raise(Some("lab-results"), "unreadable_file", "cannot read d.csv")
        .unwrap();
    let alert = last_alert(&e.db);
    wait_for("one attempt", || {
        events(&e.db, alert)[0]["attempts"] == 1 && events(&e.db, alert)[0]["last_status"].is_i64()
    })
    .await;
    let ev = &events(&e.db, alert)[0];
    assert_eq!(ev["state"], "pending");
    assert_eq!(ev["last_status"], 302);
    assert_eq!(ev["last_error"], "HTTP 302 Found");
    assert_eq!(*receiver.elsewhere.lock().unwrap(), 0);
    assert_no_canary_logged();
}

#[tokio::test]
async fn a_receiver_that_cannot_be_reached_leaves_no_url_or_secret_behind() {
    logs();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let url = format!("http://127.0.0.1:{port}/hook/{URL_CANARY}");
    let dir = tempfile::tempdir().unwrap();
    let e = engine(
        &dir.path().join("hie.db"),
        &settings(Some(&url), Some(SECRET_CANARY), 60_000),
        true,
    );
    e.alerts
        .raise(Some("lab-results"), "unreadable_file", "cannot read e.csv")
        .unwrap();
    let alert = last_alert(&e.db);
    wait_for("one attempt", || {
        events(&e.db, alert)[0]["last_error"].is_string()
    })
    .await;
    let ev = &events(&e.db, alert)[0];
    assert_eq!(ev["last_error"], "connection failed");
    assert_eq!(ev["last_status"], Value::Null);
    let stored =
        e.db.query_json("SELECT * FROM alert_webhooks", vec![])
            .unwrap();
    let stored = serde_json::to_string(&stored).unwrap();
    assert!(!stored.contains(URL_CANARY) && !stored.contains(SECRET_CANARY));
    assert_no_canary_logged();
}

#[tokio::test]
async fn two_instances_on_one_store_send_each_event_once() {
    let (receiver, url) = receiver().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hie.db");
    let config = settings(Some(&url), None, 50);
    let (a, b) = (engine(&path, &config, true), engine(&path, &config, true));
    for i in 0..10 {
        let e = if i % 2 == 0 { &a } else { &b };
        e.alerts
            .raise(
                Some("lab-results"),
                "unreadable_file",
                &format!("cannot read {i}.csv"),
            )
            .unwrap();
    }
    wait_for("all delivered", || {
        a.db.query_json(
            "SELECT COUNT(*) AS n FROM alert_webhooks WHERE state = 'delivered'",
            vec![],
        )
        .unwrap()[0]["n"]
            == 10
    })
    .await;
    let mut ids: Vec<String> = receiver
        .got()
        .iter()
        .map(|r| r.header("x-hie-event-id").unwrap())
        .collect();
    assert_eq!(ids.len(), 10);
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 10);
}

#[tokio::test]
async fn an_attempt_whose_outcome_was_lost_is_sent_again_unchanged() {
    let (receiver, url) = receiver().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hie.db");
    let config = settings(Some(&url), Some(SECRET_CANARY), 50);
    // Instance A records the alert, claims the attempt and posts it; the
    // receiver applies it; A stops before storing the outcome.
    let a = engine(&path, &config, false);
    a.alerts
        .raise(Some("lab-results"), "unreadable_file", "cannot read f.csv")
        .unwrap();
    let alert = last_alert(&a.db);
    let event_id = events(&a.db, alert)[0]["event_id"]
        .as_str()
        .unwrap()
        .to_string();
    let now = hie::db::now_ms();
    let claim =
        a.db.webhook_claim(&event_id, a.alerts.destination(), now, now + 300, 6)
            .unwrap()
            .unwrap();
    reqwest::Client::new()
        .post(&url)
        .header("X-HIE-Event-Id", &claim.event_id)
        .body(claim.payload.clone())
        .send()
        .await
        .unwrap();
    drop(a);
    // Instance B takes the event once A's claim has run out.
    let b = engine(&path, &config, true);
    wait_for("delivered by B", || {
        events(&b.db, alert)[0]["state"] == "delivered"
    })
    .await;
    let got = receiver.got();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].body, got[1].body);
    assert_eq!(got[1].header("x-hie-event-id").unwrap(), event_id);
    let mut applied: Vec<String> = got
        .iter()
        .map(|r| r.header("x-hie-event-id").unwrap())
        .collect();
    applied.dedup();
    assert_eq!(applied.len(), 1, "the receiver applies it once");
    assert_eq!(events(&b.db, alert)[0]["attempts"], 2);
}

#[tokio::test]
async fn an_event_waits_for_its_own_url() {
    let (first, first_url) = receiver().await;
    let (second, second_url) = receiver().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hie.db");
    // Written while the first URL was configured; the engine stops before sending.
    let a = engine(&path, &settings(Some(&first_url), None, 50), false);
    a.alerts
        .raise(Some("lab-results"), "unreadable_file", "cannot read g.csv")
        .unwrap();
    let alert = last_alert(&a.db);
    let stored = events(&a.db, alert)[0].clone();
    drop(a);
    // A start without a URL, then one with another URL: it stays where it is.
    let none = engine(&path, &settings(None, None, 50), true);
    assert!(none._sender.is_none());
    let other = engine(&path, &settings(Some(&second_url), None, 50), true);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(second.got().is_empty());
    let ev = &events(&other.db, alert)[0];
    assert_eq!(
        (ev["state"].as_str(), ev["attempts"].as_i64()),
        (Some("pending"), Some(0))
    );
    drop(other);
    drop(none);
    // Its URL is back: sent as written.
    let back = engine(&path, &settings(Some(&first_url), None, 50), true);
    wait_for("delivered", || {
        events(&back.db, alert)[0]["state"] == "delivered"
    })
    .await;
    let got = first.got();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].body, stored["payload"].as_str().unwrap().as_bytes());
    assert_eq!(got[0].header("x-hie-event-id").unwrap(), stored["event_id"]);
}
