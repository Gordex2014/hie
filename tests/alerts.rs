//! Alert rules end to end: the rule a source is born with, the CRUD, and the two
//! rule kinds a test can provoke quickly — too many rejected rows, and silence —
//! each firing once and clearing itself.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};

async fn batch(Json(bundle): Json<Value>) -> Json<Value> {
    let entries: Vec<Value> = bundle["entry"]
        .as_array()
        .unwrap()
        .iter()
        .map(|_| json!({"response": {"status": "201 Created"}}))
        .collect();
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
}

async fn serve(router: Router) -> String {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hie=warn")
        .with_test_writer()
        .try_init();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
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

struct Lab {
    _dir: tempfile::TempDir,
    root: PathBuf,
    api: String,
    http: reqwest::Client,
    db: std::sync::Arc<hie::db::Db>,
}

/// An engine with one folder source, `lab-results`, polling every minute so only
/// `check-now` moves it.
async fn lab(python: &str) -> Lab {
    lab_with(python, "").await
}

/// `lab`, with more lines for `hie.yaml` (an `alerts:` block).
async fn lab_with(python: &str, more_config: &str) -> Lab {
    let fhir = serve(Router::new().route("/", post(batch))).await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::create_dir_all(root.join("inbox")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {fhir}\ndatabase: data/hie.db\npython: {python}\n{more_config}"),
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let db = state.db.clone();
    let api = serve(hie::api::router(state)).await;
    let http = reqwest::Client::new();
    let created = http
        .post(format!("{api}/api/sources"))
        .json(&json!({
            "id": "lab-results", "name": "Lab results",
            "source": {"type": "folder", "path": "inbox", "pattern": "*.csv", "poll_seconds": 60, "min_file_age_ms": 0},
            "transform": "transforms/lab_results.py",
            "expect_every": "24h"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    Lab {
        _dir: dir,
        root,
        api,
        http,
        db,
    }
}

impl Lab {
    async fn get(&self, path: &str) -> Value {
        self.http
            .get(format!("{}{path}", self.api))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn post(&self, path: &str, body: Value) -> reqwest::Response {
        self.http
            .post(format!("{}{path}", self.api))
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    async fn check_now(&self) {
        self.post("/api/sources/lab-results/check-now", json!({}))
            .await;
    }

    async fn drop_file(&self, name: &str) {
        std::fs::copy(
            repo().join("examples/data").join(name),
            self.root.join("inbox").join(name),
        )
        .unwrap();
        self.check_now().await;
    }

    async fn wait_for(&self, what: &str, mut ok: impl FnMut(&Value) -> bool) -> Value {
        let started = Instant::now();
        loop {
            let alerts = self.get("/api/alerts?source=lab-results").await;
            if ok(&alerts) {
                return alerts;
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "timed out waiting for {what}: {alerts}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.check_now().await;
        }
    }
}

fn alert<'a>(alerts: &'a Value, kind: &str) -> Option<&'a Value> {
    alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["kind"] == kind)
}

#[tokio::test]
async fn a_source_is_born_with_a_silence_rule_and_rules_are_managed() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;

    // The default rule follows expect_every.
    let rules = lab.get("/api/alert-rules?source=lab-results").await;
    let rules = rules["rules"].as_array().unwrap();
    assert_eq!(rules.len(), 1, "{rules:?}");
    assert_eq!(rules[0]["condition"], "silence");
    assert_eq!(rules[0]["threshold"], "24h");
    assert_eq!(rules[0]["channels"], json!(["app", "webhook"]));
    let default_id = rules[0]["id"].as_i64().unwrap();

    // Bad rules are refused with the reason.
    for (body, reason) in [
        (
            json!({"condition": "sunspots", "threshold": "1"}),
            "unknown condition",
        ),
        (
            json!({"condition": "missing_by", "threshold": "25:00"}),
            "HH:MM",
        ),
        (
            json!({"condition": "silence", "threshold": "2h", "source_id": "nope"}),
            "source nope not found",
        ),
        (
            json!({"condition": "silence", "threshold": "2h", "channels": ["pager"]}),
            "unknown channel",
        ),
    ] {
        let r = lab.post("/api/alert-rules", body).await;
        assert_eq!(r.status(), 400);
        let err = r.json::<Value>().await.unwrap();
        assert!(err["error"].as_str().unwrap().contains(reason), "{err}");
    }

    // Create, read back, replace, delete.
    let created = lab
        .post(
            "/api/alert-rules",
            json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "10%", "notify": ["ops@example.org"], "channels": ["app", "email"]}),
        )
        .await;
    assert_eq!(created.status(), 201);
    let created: Value = created.json().await.unwrap();
    let id = created["id"].as_i64().unwrap();
    assert_ne!(id, default_id);
    assert_eq!(created["notify"], json!(["ops@example.org"]));
    let read = lab.get(&format!("/api/alert-rules/{id}")).await;
    assert_eq!(read["threshold"], "10%");
    let replaced = lab
        .http
        .put(format!("{}/api/alert-rules/{id}", lab.api))
        .json(&json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "25", "enabled": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(replaced.status(), 200);
    let read = lab.get(&format!("/api/alert-rules/{id}")).await;
    assert_eq!(
        (read["threshold"].as_str(), read["enabled"].as_bool()),
        (Some("25"), Some(false))
    );
    let all = lab.get("/api/alert-rules").await;
    assert_eq!(all["rules"].as_array().unwrap().len(), 2);
    let deleted = lab
        .http
        .delete(format!("{}/api/alert-rules/{id}", lab.api))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    let gone = lab
        .http
        .get(format!("{}/api/alert-rules/{id}", lab.api))
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), 404);
}

#[tokio::test]
async fn too_many_rejected_rows_fire_once_and_clear_on_a_clean_load() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;
    let r = lab
        .post(
            "/api/alert-rules",
            json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "10%"}),
        )
        .await;
    assert_eq!(r.status(), 201);
    let rule_id = r.json::<Value>().await.unwrap()["id"].clone();

    // messy.csv: 5 of 6 rows fail, far more than 10%.
    lab.drop_file("messy.csv").await;
    let alerts = lab
        .wait_for("the rejected_share alert", |a| {
            alert(a, "rejected_share").is_some()
        })
        .await;
    let fired = alert(&alerts, "rejected_share").unwrap();
    assert_eq!(fired["rule_id"], rule_id);
    assert!(fired["cleared_at"].is_null());
    assert!(
        fired["message"].as_str().unwrap().contains("83.3%"),
        "{fired}"
    );
    assert_eq!(
        lab.get("/api/alerts?source=lab-results&open=true").await["alerts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // A clean load clears it, and only once.
    lab.drop_file("labs.csv").await;
    let alerts = lab
        .wait_for("the alert to clear", |a| {
            alert(a, "rejected_share").is_some_and(|x| !x["cleared_at"].is_null())
        })
        .await;
    let cleared = alert(&alerts, "rejected_share").unwrap();
    assert!(
        cleared["cleared_reason"]
            .as_str()
            .unwrap()
            .contains("within the limit"),
        "{cleared}"
    );
    let share_alerts = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["kind"] == "rejected_share")
        .count();
    assert_eq!(share_alerts, 1);
    assert!(
        lab.get("/api/alerts?source=lab-results&open=true").await["alerts"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn silence_fires_once_and_clears_when_data_arrives() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;
    // Tighten the default rule to one second.
    let rules = lab.get("/api/alert-rules?source=lab-results").await;
    let id = rules["rules"][0]["id"].as_i64().unwrap();
    let r = lab
        .http
        .put(format!("{}/api/alert-rules/{id}", lab.api))
        .json(&json!({"source_id": "lab-results", "condition": "silence", "threshold": "1s"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let alerts = lab
        .wait_for("the silence alert", |a| alert(a, "silence").is_some())
        .await;
    let fired = alert(&alerts, "silence").unwrap();
    assert_eq!(fired["rule_id"], id);
    assert!(
        fired["message"].as_str().unwrap().contains("every 1s"),
        "{fired}"
    );

    // Still one alert after more passes.
    lab.check_now().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    lab.check_now().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let alerts = lab.get("/api/alerts?source=lab-results").await;
    assert_eq!(alerts["alerts"].as_array().unwrap().len(), 1, "{alerts}");

    lab.drop_file("labs.csv").await;
    let alerts = lab
        .wait_for("silence to clear", |a| {
            alert(a, "silence").is_some_and(|x| !x["cleared_at"].is_null())
        })
        .await;
    assert_eq!(
        alert(&alerts, "silence").unwrap()["cleared_reason"],
        "Lab results: data arrived"
    );
}

/// What a webhook receiver got: headers and raw body of each request.
type Received = Arc<Mutex<Vec<(HeaderMap, Bytes)>>>;

async fn receive(State(got): State<Received>, headers: HeaderMap, body: Bytes) {
    got.lock().unwrap().push((headers, body));
}

fn webhook_of<'a>(alerts: &'a Value, id: &Value) -> &'a Value {
    &alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == *id)
        .unwrap()["webhook"]
}

#[tokio::test]
async fn the_webhook_gets_each_alert_change_and_the_api_shows_its_delivery() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let got = Received::default();
    let receiver = serve(
        Router::new()
            .route("/hook", post(receive))
            .with_state(got.clone()),
    )
    .await;
    let lab = lab_with(
        &python,
        &format!(
            "alerts:\n  webhook_url: {receiver}/hook\n  webhook_secret: test-secret\n  webhook_backoff_ms: 50\n"
        ),
    )
    .await;
    let rule = |threshold: &str, channels: Value| json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": threshold, "channels": channels});
    let posted = lab
        .post("/api/alert-rules", rule("10%", json!(["app", "webhook"])))
        .await;
    let webhook_rule = posted.json::<Value>().await.unwrap()["id"].clone();
    let posted = lab
        .post("/api/alert-rules", rule("20%", json!(["app"])))
        .await;
    let app_rule = posted.json::<Value>().await.unwrap()["id"].clone();

    // An alert left open by an earlier version: no webhook event recorded.
    let earlier = lab.db.query_json(
        "INSERT INTO alerts (feed_id, kind, message, created_at, rule_id, notify, channels)
         VALUES ('lab-results', 'rejected_share', 'from before', 1700000000123, $1, '[]', '[\"app\",\"webhook\"]')
         RETURNING id",
        vec![hie::db::Param::Int(webhook_rule.as_i64().unwrap())],
    ).unwrap()[0]["id"].clone();
    let alerts = lab.get("/api/alerts?source=lab-results").await;
    assert_eq!(
        alerts["webhook"],
        json!({"configured": true, "signed": true})
    );
    assert_eq!(*webhook_of(&alerts, &earlier), Value::Null);

    // messy.csv: the webhook rule's alert is still open, so only the app-only rule fires.
    lab.drop_file("messy.csv").await;
    let alerts = lab
        .wait_for("the app-only alert", |a| {
            a["alerts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|x| x["rule_id"] == app_rule)
        })
        .await;
    let app_alert = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["rule_id"] == app_rule)
        .unwrap()
        .clone();
    let hook = webhook_of(&alerts, &app_alert["id"]);
    assert_eq!(
        (hook["event"].as_str(), hook["state"].as_str()),
        (Some("fired"), Some("not_requested"))
    );
    assert_eq!(hook["attempts"], 0);

    // labs.csv clears both; the earlier alert's clear goes out, the app-only one's does not.
    lab.drop_file("labs.csv").await;
    let alerts = lab
        .wait_for("the clear to be delivered", |a| {
            webhook_of(a, &earlier)["state"] == "delivered"
                && !webhook_of(a, &app_alert["id"]).is_null()
                && webhook_of(a, &app_alert["id"])["event"] == "cleared"
        })
        .await;
    let hook = webhook_of(&alerts, &earlier);
    assert_eq!(hook["event"], "cleared");
    assert_eq!(hook["attempts"], 1);
    assert_eq!(hook["last_status"], 200);
    assert_eq!(hook["last_error"], Value::Null);
    assert!(hook["delivered_at"].is_string());
    assert_eq!(
        webhook_of(&alerts, &app_alert["id"])["state"],
        "not_requested"
    );

    let got = got.lock().unwrap().clone();
    assert_eq!(got.len(), 1, "only the webhook rule's clear is posted");
    let (headers, body) = &got[0];
    assert_eq!(
        headers["x-hie-signature"].to_str().unwrap(),
        hie::alerts::signature(b"test-secret", body)
    );
    let payload: Value = serde_json::from_slice(body).unwrap();
    assert_eq!(payload["event_id"], hook["event_id"]);
    assert_eq!(
        payload["source"],
        json!({"id": "lab-results", "name": "Lab results"})
    );
    let listed = alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == earlier)
        .unwrap();
    // The times are the strings the API shows.
    assert_eq!(payload["alert"]["created_at"], listed["created_at"]);
    assert_eq!(payload["alert"]["created_at"], "2023-11-14T22:13:20.123Z");
    assert_eq!(payload["alert"]["cleared_at"], listed["cleared_at"]);
    assert_eq!(payload["alert"]["message"], "from before");
}

#[tokio::test]
async fn without_a_webhook_the_api_says_so() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let lab = lab(&python).await;
    lab.post(
        "/api/alert-rules",
        json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "10%"}),
    )
    .await;
    lab.drop_file("messy.csv").await;
    let alerts = lab
        .wait_for("the alert", |a| alert(a, "rejected_share").is_some())
        .await;
    assert_eq!(
        alerts["webhook"],
        json!({"configured": false, "signed": false})
    );
    let hook = &alert(&alerts, "rejected_share").unwrap()["webhook"];
    assert_eq!(
        (hook["event"].as_str(), hook["state"].as_str()),
        (Some("fired"), Some("not_configured"))
    );
    assert_eq!(hook["attempts"], 0);
    assert_eq!(hook["last_status"], Value::Null);
}
