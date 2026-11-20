//! Alert rules end to end: the rule a source is born with, the CRUD, and the two
//! rule kinds a test can provoke quickly — too many rejected rows, and silence —
//! each firing once and clearing itself.

use std::path::PathBuf;
use std::time::{Duration, Instant};

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
}

/// An engine with one folder source, `lab-results`, polling every minute so only
/// `check-now` moves it.
async fn lab(python: &str) -> Lab {
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
        format!("fhir_base_url: {fhir}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
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

// ---- Reading alerts by period and in pages ------------------------------------

/// An engine with no source and no Python, whose alerts a test writes itself.
struct Store {
    _dir: tempfile::TempDir,
    _manager: std::sync::Arc<hie::manager::SourceManager>,
    db: std::sync::Arc<hie::db::Db>,
    api: String,
    http: reqwest::Client,
}

async fn store() -> Store {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("hie.yaml"),
        "fhir_base_url: http://127.0.0.1:9/fhir\ndatabase: data/hie.db\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&dir.path().join("hie.yaml")).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let db = state.db.clone();
    let api = serve(hie::api::router(state)).await;
    Store {
        _dir: dir,
        _manager: manager,
        db,
        api,
        http: reqwest::Client::new(),
    }
}

/// A whole second an hour ago: recent enough for any retention, and exact.
fn base_ms() -> i64 {
    (chrono::Utc::now().timestamp() - 3600) * 1000
}

fn iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

#[derive(Clone, Copy)]
struct Seeded {
    id: i64,
    source: &'static str,
    at: i64,
    open: bool,
}

impl Store {
    /// An alert created at exactly `at`. The store's write helpers are private,
    /// so the time is set through `query_json`, which runs any statement.
    fn seed(&self, source: &'static str, at: i64, open: bool) -> Seeded {
        let id = self
            .db
            .insert_alert(Some(source), "test", "a test alert", None)
            .unwrap();
        let updated = self
            .db
            .query_json(
                "UPDATE alerts SET created_at = $1 WHERE id = $2 RETURNING id",
                hie::db::params![at, id],
            )
            .unwrap();
        assert_eq!(updated, vec![json!({"id": id})]);
        if !open {
            self.db.clear_alert(id, "cleared by the test").unwrap();
        }
        Seeded {
            id,
            source,
            at,
            open,
        }
    }

    async fn list(&self, query: &[(&str, &str)]) -> (u16, Value) {
        let r = self
            .http
            .get(format!("{}/api/alerts", self.api))
            .query(query)
            .send()
            .await
            .unwrap();
        (r.status().as_u16(), r.json().await.unwrap())
    }

    /// Every page of a query, following `next_cursor` to its end: the ids in
    /// order. Each page has at most `limit` alerts, carries `next_cursor`, and
    /// only the first may be empty.
    async fn walk(&self, query: &[(&str, &str)], limit: usize) -> Vec<i64> {
        self.walk_from(query, limit, None).await
    }

    /// `walk`, starting after a cursor already in hand.
    async fn walk_from(
        &self,
        query: &[(&str, &str)],
        limit: usize,
        mut cursor: Option<String>,
    ) -> Vec<i64> {
        let limit_text = limit.to_string();
        let mut ids = Vec::new();
        for page in 0.. {
            assert!(page < 1000, "the walk does not end");
            let mut q = query.to_vec();
            q.push(("limit", &limit_text));
            if let Some(c) = &cursor {
                q.push(("cursor", c));
            }
            let (status, body) = self.list(&q).await;
            assert_eq!(status, 200, "{body}");
            let alerts = body["alerts"].as_array().unwrap();
            assert!(alerts.len() <= limit, "{body}");
            assert!(page == 0 || !alerts.is_empty(), "an empty page: {body}");
            ids.extend(alerts.iter().map(|a| a["id"].as_i64().unwrap()));
            match body.get("next_cursor") {
                Some(Value::String(c)) => cursor = Some(c.clone()),
                Some(Value::Null) => return ids,
                _ => panic!("no next_cursor: {body}"),
            }
        }
        unreachable!()
    }
}

fn newest_first(seeded: &[Seeded], keep: impl Fn(&Seeded) -> bool) -> Vec<i64> {
    let mut ids: Vec<i64> = seeded.iter().filter(|a| keep(a)).map(|a| a.id).collect();
    ids.sort_unstable_by(|a, b| b.cmp(a));
    ids
}

#[tokio::test]
async fn alerts_are_read_by_period_and_in_pages() {
    let store = store().await;
    let b = base_ms();
    // Ids follow this order; times do not always: a later id may be older, and
    // some share a millisecond. Two sources, open and cleared, on both sides
    // of each `since` below.
    let seeded: Vec<Seeded> = [
        ("lab", 0, true),
        ("imaging", 100, false),
        ("lab", 123, false),
        ("lab", 123, true),
        ("imaging", 200, true),
        ("lab", 50, true),
        ("lab", 300, true),
        ("lab", 300, false),
        ("imaging", 122, true),
        ("lab", 124, true),
        ("lab", 10, false),
    ]
    .into_iter()
    .map(|(source, offset, open)| store.seed(source, b + offset, open))
    .collect();

    // The boundary as a consumer has it: the `created_at` the list returned.
    let (_, all) = store.list(&[]).await;
    let third = seeded[2];
    let returned = all["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == third.id)
        .unwrap()["created_at"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(returned, iso(b + 123));

    // Each `since` and the first stored millisecond it keeps.
    let at_123 = iso(b + 123);
    let a_bit_after_123 = iso(b + 123).replace("Z", "1Z");
    let offset_form = chrono::DateTime::from_timestamp_millis(b + 123)
        .unwrap()
        .with_timezone(&chrono::FixedOffset::east_opt(2 * 3600).unwrap())
        .format("%Y-%m-%dT%H:%M:%S%.3f%:z")
        .to_string();
    let after_all = iso(b + 301);
    let sinces: [(Option<&str>, i64); 6] = [
        (None, i64::MIN),
        (Some(&returned), b + 123),
        (Some(&at_123), b + 123),
        (Some(&a_bit_after_123), b + 124),
        (Some(&offset_form), b + 123),
        (Some(&after_all), b + 301),
    ];
    assert!(offset_form.contains('+'), "{offset_form}");

    for (since, from) in sinces {
        for source in [None, Some("lab"), Some("imaging")] {
            for open in [false, true] {
                let mut query = Vec::new();
                if let Some(s) = source {
                    query.push(("source", s));
                }
                if open {
                    query.push(("open", "true"));
                }
                if let Some(s) = since {
                    query.push(("since", s));
                }
                let expected = newest_first(&seeded, |a| {
                    a.at >= from && source.is_none_or(|s| a.source == s) && (!open || a.open)
                });
                for limit in [1, 2, 3, 100] {
                    assert_eq!(
                        store.walk(&query, limit).await,
                        expected,
                        "{query:?} limit {limit}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn a_walk_keeps_its_place_while_alerts_change() {
    let store = store().await;
    let b = base_ms();
    let s: Vec<Seeded> = (0..6)
        .map(|i| store.seed("lab", b + i * 10, true))
        .collect();
    let open = [("open", "true"), ("limit", "2")];

    let (_, first) = store.list(&open).await;
    let page: Vec<i64> = first["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["id"].as_i64().unwrap())
        .collect();
    assert_eq!(page, vec![s[5].id, s[4].id]);
    let cursor = first["next_cursor"].as_str().unwrap().to_string();

    // Between pages: a new alert, one further down cleared, and the cursor's
    // own alert removed, as a purge would.
    let new = store.seed("lab", b + 1000, true);
    store
        .db
        .clear_alert(s[2].id, "cleared by the test")
        .unwrap();
    let deleted = store
        .db
        .query_json(
            "DELETE FROM alerts WHERE id = $1 RETURNING id",
            hie::db::params![s[4].id],
        )
        .unwrap();
    assert_eq!(deleted, vec![json!({"id": s[4].id})]);

    let rest = store
        .walk_from(&[("open", "true")], 2, Some(cursor.clone()))
        .await;
    assert_eq!(rest, vec![s[3].id, s[1].id, s[0].id]);
    // Without `open`, the cleared alert stays where it was.
    let rest = store.walk_from(&[], 2, Some(cursor)).await;
    assert_eq!(rest, vec![s[3].id, s[2].id, s[1].id, s[0].id]);

    // The new alert is on a fresh first page.
    let (_, fresh) = store.list(&open).await;
    assert_eq!(fresh["alerts"][0]["id"], new.id);
}

#[tokio::test]
async fn the_default_page_holds_a_hundred_alerts() {
    let store = store().await;
    let b = base_ms();
    let s: Vec<Seeded> = (0..101).map(|i| store.seed("lab", b + i, true)).collect();

    let (status, first) = store.list(&[]).await;
    assert_eq!(status, 200);
    let alerts = first["alerts"].as_array().unwrap();
    assert_eq!(alerts.len(), 100);
    assert_eq!(alerts[0]["id"], s[100].id);
    assert_eq!(alerts[99]["id"], s[1].id);
    let cursor = first["next_cursor"].as_str().unwrap();
    let (_, last) = store.list(&[("cursor", cursor)]).await;
    assert_eq!(last["alerts"].as_array().unwrap().len(), 1);
    assert_eq!(last["alerts"][0]["id"], s[0].id);
    assert_eq!(last.get("next_cursor"), Some(&Value::Null));

    let (_, none) = store.list(&[("limit", "0")]).await;
    assert_eq!(none, json!({"alerts": [], "next_cursor": null}));
    // A negative limit has always meant every alert.
    let (_, every) = store.list(&[("limit", "-1")]).await;
    assert_eq!(every["alerts"].as_array().unwrap().len(), 101);
    assert_eq!(every.get("next_cursor"), Some(&Value::Null));
    // Empty values are absent ones.
    let (status, empty) = store.list(&[("since", ""), ("cursor", "")]).await;
    assert_eq!(status, 200);
    assert_eq!(empty["alerts"].as_array().unwrap().len(), 100);
}

#[tokio::test]
async fn a_malformed_query_is_refused_without_echoing_it() {
    let store = store().await;
    store.seed("lab", base_ms(), true);
    type Case<'a> = (&'a [(&'a str, &'a str)], Option<&'a str>);
    let cases: [Case; 12] = [
        (&[("since", "yesterday")], Some("since")),
        (&[("since", "2025-01-02")], Some("since")),
        (&[("since", "2016-12-31T23:59:60Z")], Some("since")),
        (
            &[("since", "2025-01-02T00:00:00.1230000001Z")],
            Some("since"),
        ),
        (&[("cursor", "abc")], Some("cursor")),
        (&[("cursor", "-1")], Some("cursor")),
        (&[("cursor", "0")], Some("cursor")),
        (&[("cursor", "+5")], Some("cursor")),
        (&[("cursor", "99999999999999999999")], Some("cursor")),
        (&[("limit", "0"), ("since", "garbage")], Some("since")),
        (&[("limit", "abc")], None),
        (&[("open", "yes")], None),
    ];
    for (query, field) in cases {
        let (status, body) = store.list(query).await;
        assert_eq!(status, 400, "{query:?}: {body}");
        assert_eq!(body["code"], "invalid_query", "{query:?}: {body}");
        assert_eq!(body["field"].as_str(), field, "{query:?}: {body}");
        assert!(body["error"].is_string(), "{body}");
        for (_, value) in query {
            if value.len() > 2 {
                assert!(!body.to_string().contains(value), "echoed: {body}");
            }
        }
    }
}
