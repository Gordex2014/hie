//! An open alert ends with its rule: when the rule is turned off, deleted, or
//! changed in its condition, threshold or source. Not when only its
//! recipients or channels change. No transform runs: the source's folder stays
//! empty.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use hie::db::Rule;
use hie::pipeline::FeedRunner;
use serde_json::{Value, json};

type Hooks = Arc<Mutex<Vec<Value>>>;

async fn hook(State(hooks): State<Hooks>, Json(payload): Json<Value>) {
    hooks.lock().unwrap().push(payload);
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

struct Engine {
    root: Arc<tempfile::TempDir>,
    runner: Arc<FeedRunner>,
    state: hie::api::AppState,
    api: String,
    http: reqwest::Client,
    hooks: Hooks,
    _manager: Arc<hie::manager::SourceManager>,
}

/// An engine with one source, `lab-results`, whose passes the test drives,
/// posting its alerts to a receiver the test reads.
async fn engine() -> Engine {
    let hooks = Hooks::default();
    let receiver = serve(
        Router::new()
            .route("/hook", post(hook))
            .with_state(hooks.clone()),
    )
    .await;
    let root = tempfile::tempdir().unwrap();
    for dir in ["sources", "transforms", "inbox"] {
        std::fs::create_dir_all(root.path().join(dir)).unwrap();
    }
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.path().join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.path().join("hie.yaml"),
        format!(
            "fhir_base_url: http://127.0.0.1:9/fhir\ndatabase: data/hie.db\ndestination:\n  probe_seconds: 0\n\
             alerts:\n  webhook_url: {receiver}/hook\n  webhook_backoff_ms: 50\n"
        ),
    )
    .unwrap();
    std::fs::write(
        root.path().join("sources/lab-results.yaml"),
        "id: lab-results\nname: Lab results\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n\
         transform: transforms/lab_results.py\n",
    )
    .unwrap();
    start(Arc::new(root), hooks).await
}

/// An engine on the files in `root`, as a start of the process.
async fn start(root: Arc<tempfile::TempDir>, hooks: Hooks) -> Engine {
    let (config, feeds) = hie::config::load(&root.path().join("hie.yaml")).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let runner = manager.runner("lab-results").unwrap();
    manager.pause("lab-results", None).unwrap(); // the test drives the passes
    let api = serve(hie::api::router(state.clone())).await;
    Engine {
        root,
        runner,
        state,
        api,
        http: reqwest::Client::new(),
        hooks,
        _manager: manager,
    }
}

impl Engine {
    /// Another start on the same files and store, this one still running (on
    /// PostgreSQL the first handle keeps the test's schema).
    async fn again(&self) -> Engine {
        start(self.root.clone(), self.hooks.clone()).await
    }

    /// Makes a rule through the API; returns it as stored.
    async fn rule(&self, body: Value) -> Rule {
        let r = self
            .http
            .post(format!("{}/api/alert-rules", self.api))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201, "{body}");
        let id = r.json::<Value>().await.unwrap()["id"].as_i64().unwrap();
        self.state.db.rule(id).unwrap().unwrap()
    }

    /// Replaces a rule through the API with `change` applied to it.
    async fn change(&self, rule: &Rule, change: Value) {
        let mut body = json!(rule);
        for (key, value) in change.as_object().unwrap() {
            body[key] = value.clone();
        }
        let r = self
            .http
            .put(format!("{}/api/alert-rules/{}", self.api, rule.id))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{body}");
    }

    async fn delete(&self, rule: &Rule) {
        let r = self
            .http
            .delete(format!("{}/api/alert-rules/{}", self.api, rule.id))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 204);
    }

    /// An open alert of `rule` for `feed`, as an earlier pass raised it.
    fn raise(&self, feed: &str, kind: &str, rule: Option<&Rule>) -> i64 {
        self.state
            .db
            .insert_alert(Some(feed), kind, "raised by the test", rule)
            .unwrap()
    }

    /// `(cleared, cleared_reason)` of an alert.
    fn state_of(&self, id: i64) -> (bool, Option<String>) {
        let row = &self
            .state
            .db
            .query_json(
                "SELECT cleared_at, cleared_reason FROM alerts WHERE id = $1",
                [hie::db::Param::Int(id)],
            )
            .unwrap()[0];
        (
            !row["cleared_at"].is_null(),
            row["cleared_reason"].as_str().map(str::to_string),
        )
    }

    /// The open alerts of a rule, by id.
    fn open_of(&self, rule: &Rule) -> Vec<i64> {
        self.state
            .db
            .open_alerts_of_rule(rule.id)
            .unwrap()
            .into_iter()
            .map(|(id, ..)| id)
            .collect()
    }

    /// The latest webhook event of each alert, as `GET /api/alerts` shows it.
    async fn webhook_of(&self, id: i64) -> Value {
        let alerts: Value = self
            .http
            .get(format!("{}/api/alerts", self.api))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        alerts["alerts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["id"] == id)
            .unwrap()["webhook"]
            .clone()
    }

    /// The `cleared` events the receiver got, by alert id, once `n` came.
    async fn cleared_posts(&self, n: usize) -> BTreeSet<i64> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let posts: BTreeSet<i64> = self
                .hooks
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p["event"] == "cleared")
                .map(|p| p["alert"]["id"].as_i64().unwrap())
                .collect();
            if posts.len() >= n || Instant::now() > deadline {
                return posts;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

const OFF: &str = "its rule was turned off";
const DELETED: &str = "its rule was deleted";
const CHANGED: &str = "its rule was changed";

#[tokio::test]
async fn an_alert_ends_when_its_rule_is_turned_off_deleted_or_changed() {
    let e = engine().await;
    let webhook = || json!(["app", "webhook"]);
    let silence = |threshold: &str, channels: Value| json!({"source_id": "lab-results", "condition": "silence", "threshold": threshold, "channels": channels});
    let off = e.rule(silence("2h", webhook())).await;
    let deleted = e.rule(silence("3h", json!(["app"]))).await;
    let threshold = e
        .rule(json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "10%", "channels": webhook()}))
        .await;
    let condition = e
        .rule(json!({"source_id": "lab-results", "condition": "missing_by", "threshold": "08:00", "channels": webhook()}))
        .await;
    let source = e
        .rule(json!({"condition": "silence", "threshold": "4h", "channels": webhook()}))
        .await;
    let recipients = e
        .rule(json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "20%", "channels": ["app"]}))
        .await;
    let untouched = e.rule(silence("5h", webhook())).await;

    let a_off = e.raise("lab-results", "silence", Some(&off));
    let a_deleted = e.raise("lab-results", "silence", Some(&deleted));
    let a_threshold = e.raise("lab-results", "rejected_share", Some(&threshold));
    let a_condition = e.raise("lab-results", "missing", Some(&condition));
    // An all-sources rule's alert about another source.
    let a_source = e.raise("elsewhere", "silence", Some(&source));
    let a_recipients = e.raise("lab-results", "rejected_share", Some(&recipients));
    let a_untouched = e.raise("lab-results", "silence", Some(&untouched));
    let a_ruleless = e.raise("lab-results", "unreadable_file", None);

    e.change(&off, json!({"enabled": false})).await;
    e.delete(&deleted).await;
    e.change(&threshold, json!({"threshold": "15%"})).await;
    e.change(
        &condition,
        json!({"condition": "silence", "threshold": "1h"}),
    )
    .await;
    e.change(&source, json!({"source_id": "lab-results"})).await;
    e.change(
        &recipients,
        json!({"notify": ["ops@example.org"], "channels": ["app", "webhook"]}),
    )
    .await;

    assert_eq!(e.state_of(a_off), (true, Some(OFF.into())));
    assert_eq!(e.state_of(a_deleted), (true, Some(DELETED.into())));
    assert_eq!(e.state_of(a_threshold), (true, Some(CHANGED.into())));
    assert_eq!(e.state_of(a_condition), (true, Some(CHANGED.into())));
    assert_eq!(e.state_of(a_source), (true, Some(CHANGED.into())));
    assert_eq!(e.state_of(a_recipients), (false, None), "recipients only");
    assert_eq!(e.state_of(a_untouched), (false, None));
    assert_eq!(e.state_of(a_ruleless), (false, None));

    // Each clear is the alert's `cleared` event, for the channels it fired
    // with: posted when they held the webhook, recorded as not requested when not.
    let posted = e.cleared_posts(4).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        e.cleared_posts(4).await,
        BTreeSet::from([a_off, a_threshold, a_condition, a_source])
    );
    assert_eq!(posted.len(), 4);
    let hook = e.webhook_of(a_deleted).await;
    assert_eq!(
        (hook["event"].as_str(), hook["state"].as_str()),
        (Some("cleared"), Some("not_requested"))
    );
    let hook = e.webhook_of(a_off).await;
    assert_eq!(hook["event"], "cleared");
    // The recipients change sent nothing about its alert.
    assert!(
        !e.hooks
            .lock()
            .unwrap()
            .iter()
            .any(|p| p["alert"]["id"] == a_recipients)
    );
}

/// The rule changed, the condition still holds: the next evaluation raises a
/// new alert, with its own id. Turned off, it raises nothing.
#[tokio::test]
async fn a_changed_rule_that_still_fails_raises_a_new_alert() {
    let e = engine().await;
    let rule = e
        .rule(json!({"source_id": "lab-results", "condition": "silence", "threshold": "1s", "channels": ["app"]}))
        .await;
    tokio::time::sleep(Duration::from_millis(2_200)).await;
    e.runner.tick().await.unwrap();
    let first = e.open_of(&rule);
    assert_eq!(first.len(), 1);

    e.change(&rule, json!({"threshold": "2s"})).await;
    assert_eq!(e.state_of(first[0]), (true, Some(CHANGED.into())));
    e.runner.tick().await.unwrap();
    let second = e.open_of(&rule);
    assert_eq!(second.len(), 1);
    assert_ne!(second, first);

    let rule = e.state.db.rule(rule.id).unwrap().unwrap();
    e.change(&rule, json!({"enabled": false})).await;
    assert_eq!(e.state_of(second[0]), (true, Some(OFF.into())));
    e.runner.tick().await.unwrap();
    assert!(e.open_of(&rule).is_empty());
}

/// A pass that read a rule just before it was turned off, deleted or changed
/// raises nothing after the change: the alert is written only while the rule
/// still watches what the pass read. An alert raised under the rule as changed
/// stays.
#[tokio::test]
async fn an_alert_from_a_rule_read_before_its_change_is_not_raised() {
    let e = engine().await;
    let alerts = &e.state.alerts;
    let rule = e
        .rule(json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "10%", "channels": ["app"]}))
        .await;
    let read_before = rule.clone();
    e.change(&rule, json!({"threshold": "50%"})).await;
    assert!(
        !alerts
            .fire_once("lab-results", "rejected_share", &read_before, "stale")
            .unwrap()
    );
    assert!(e.open_of(&rule).is_empty());

    let changed = e.state.db.rule(rule.id).unwrap().unwrap();
    assert!(
        alerts
            .fire_once("lab-results", "rejected_share", &changed, "current")
            .unwrap()
    );
    assert_eq!(e.open_of(&rule).len(), 1);

    // Turned off, then deleted: nothing more is raised under it.
    e.change(&changed, json!({"enabled": false})).await;
    let off = e.state.db.rule(rule.id).unwrap().unwrap();
    for read in [&changed, &off] {
        assert!(
            !alerts
                .fire_once("lab-results", "rejected_share", read, "stale")
                .unwrap()
        );
    }
    e.delete(&off).await;
    assert!(
        !alerts
            .fire_once("lab-results", "rejected_share", &off, "stale")
            .unwrap()
    );
    assert!(e.open_of(&rule).is_empty());
}

/// Alerts of rules an earlier version turned off, deleted or changed, which
/// it left open, are cleared at the next start. Here the store is changed
/// directly, as that version did.
#[tokio::test]
async fn a_start_clears_the_alerts_their_rules_no_longer_watch() {
    let e = engine().await;
    let db = &e.state.db;
    let silence = |threshold: &str| json!({"source_id": "lab-results", "condition": "silence", "threshold": threshold, "channels": ["app"]});
    let off = e.rule(silence("2h")).await;
    let deleted = e.rule(silence("3h")).await;
    let condition = e.rule(silence("4h")).await;
    let source = e
        .rule(json!({"condition": "silence", "threshold": "5h", "channels": ["app"]}))
        .await;
    let watching = e
        .rule(json!({"source_id": "lab-results", "condition": "rejected_share", "threshold": "10%", "channels": ["app"]}))
        .await;
    let a_off = e.raise("lab-results", "silence", Some(&off));
    let a_deleted = e.raise("lab-results", "silence", Some(&deleted));
    let a_condition = e.raise("lab-results", "silence", Some(&condition));
    let a_source = e.raise("lab-results", "silence", Some(&source));
    let a_watching = e.raise("lab-results", "rejected_share", Some(&watching));
    // Every condition's own kind is watched: `unreachable` raises `delivery`
    // and `missing_by` raises `missing`.
    let unreachable = e
        .rule(json!({"source_id": "lab-results", "condition": "unreachable", "threshold": "5", "channels": ["app"]}))
        .await;
    let missing_by = e
        .rule(json!({"source_id": "lab-results", "condition": "missing_by", "threshold": "06:00", "channels": ["app"]}))
        .await;
    let all_sources = e
        .rule(json!({"condition": "silence", "threshold": "7h", "channels": ["app"]}))
        .await;
    let a_unreachable = e.raise("lab-results", "delivery", Some(&unreachable));
    let a_missing = e.raise("lab-results", "missing", Some(&missing_by));
    let a_all_sources = e.raise("elsewhere", "silence", Some(&all_sources));
    // Its id given to a rule made after it (as a SQLite store did before ids
    // came from AUTOINCREMENT): the rule that raised it is gone.
    let reused = e.raise("lab-results", "rejected_share", Some(&watching));
    db.query_json(
        "UPDATE alerts SET created_at = 1 WHERE id = $1 RETURNING id",
        [hie::db::Param::Int(reused)],
    )
    .unwrap();
    let a_ruleless = e.raise("lab-results", "delivery", None);

    db.update_rule(
        off.id,
        &Rule {
            enabled: false,
            ..off.clone()
        },
    )
    .unwrap();
    db.delete_rule(deleted.id).unwrap();
    db.update_rule(
        condition.id,
        &Rule {
            condition: "rejected_share".into(),
            threshold: "10%".into(),
            ..condition.clone()
        },
    )
    .unwrap();
    db.update_rule(
        source.id,
        &Rule {
            source_id: Some("elsewhere".into()),
            ..source.clone()
        },
    )
    .unwrap();

    let _next = e.again().await;
    assert_eq!(e.state_of(a_off), (true, Some(OFF.into())));
    assert_eq!(e.state_of(a_deleted), (true, Some(DELETED.into())));
    assert_eq!(e.state_of(a_condition), (true, Some(CHANGED.into())));
    assert_eq!(e.state_of(a_source), (true, Some(CHANGED.into())));
    assert_eq!(e.state_of(a_watching), (false, None));
    assert_eq!(e.state_of(a_unreachable), (false, None));
    assert_eq!(e.state_of(a_missing), (false, None));
    assert_eq!(e.state_of(a_all_sources), (false, None));
    assert_eq!(e.state_of(reused), (true, Some(DELETED.into())));
    assert_eq!(e.state_of(a_ruleless), (false, None));
}
