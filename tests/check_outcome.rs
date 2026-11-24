//! Check now over the API: each request gets its own check, and
//! `GET /api/checks/{id}` says what the scan that answered it found.
//!
//! Every source here is created while another owner holds its lease, so the
//! engine's own passes stand by and the test drives the scans by hand: what a
//! check is answered with never depends on timing.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};

const SOURCE: &str = "lab-results";

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

struct Engine {
    api: String,
    http: reqwest::Client,
    manager: Arc<hie::manager::SourceManager>,
    db: Arc<hie::db::Db>,
    root: PathBuf,
    _dir: tempfile::TempDir,
}

/// Another engine instance's name, holding the lease of the test's sources.
const OTHER: &str = "another-instance";

impl Engine {
    /// An engine with no sources, behind a stand-in FHIR server, or `None`
    /// without Python.
    async fn start() -> Option<Engine> {
        let Some(python) = python() else {
            eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
            return None;
        };
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
        let (manager, state) =
            hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
        let db = state.db.clone();
        let api = serve(hie::api::router(state)).await;
        Some(Engine {
            api,
            http: reqwest::Client::new(),
            manager,
            db,
            root,
            _dir: dir,
        })
    }

    fn definition(id: &str) -> Value {
        json!({
            "id": id, "name": "Lab results",
            "source": {"type": "folder", "path": "inbox", "pattern": "*.csv", "poll_seconds": 3600, "min_file_age_ms": 0},
            "transform": "transforms/lab_results.py"
        })
    }

    /// Creates a folder source over `inbox/` while another instance holds it,
    /// so only the test's own `tick()` scans it.
    async fn create_held(&self, id: &str) {
        assert!(self.db.acquire_lease(id, OTHER, 3_600_000).unwrap());
        let created = self
            .http
            .post(format!("{}/api/sources", self.api))
            .json(&Self::definition(id))
            .send()
            .await
            .unwrap();
        assert_eq!(created.status(), 201);
    }

    async fn check_now(&self, id: &str) -> i64 {
        let answer = self
            .http
            .post(format!("{}/api/sources/{id}/check-now", self.api))
            .send()
            .await
            .unwrap();
        assert_eq!(answer.status(), 202);
        let location = answer.headers()["location"].to_str().unwrap().to_string();
        let body: Value = answer.json().await.unwrap();
        assert_eq!(
            (body["id"].as_str(), body["checking"].as_bool()),
            (Some(id), Some(true))
        );
        let check = body["check_id"].as_i64().expect("a check id");
        assert_eq!(location, format!("/api/checks/{check}"));
        check
    }

    async fn check(&self, check: i64) -> Value {
        let answer = self
            .http
            .get(format!("{}/api/checks/{check}", self.api))
            .send()
            .await
            .unwrap();
        assert_eq!(answer.status(), 200);
        assert_eq!(answer.headers()["cache-control"], "no-store");
        answer.json().await.unwrap()
    }

    /// The check once answered, waiting at most 20 s.
    async fn answered(&self, check: i64) -> Value {
        let started = Instant::now();
        loop {
            let answer = self.check(check).await;
            if answer["status"] != "pending" {
                return answer;
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "check {check} still pending"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn tick(&self, id: &str) {
        self.manager.runner(id).unwrap().tick().await.unwrap();
    }

    fn drop_file(&self, name: &str) {
        std::fs::copy(
            repo().join("examples/data/labs.csv"),
            self.root.join("inbox").join(name),
        )
        .unwrap();
    }

    async fn loads(&self, id: &str) -> Vec<Value> {
        let listed: Value = self
            .http
            .get(format!("{}/api/sources/{id}/loads", self.api))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        listed["loads"].as_array().unwrap().clone()
    }
}

fn load_ids(check: &Value) -> Vec<i64> {
    check["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["id"].as_i64().unwrap())
        .collect()
}

#[tokio::test]
async fn each_check_answers_what_the_scan_after_it_found() {
    let Some(engine) = Engine::start().await else {
        return;
    };
    engine.create_held(SOURCE).await;

    // Pending: nothing has scanned the source since.
    let first = engine.check_now(SOURCE).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let waiting = engine.check(first).await;
    assert_eq!(waiting["status"], "pending");
    assert_eq!(waiting["source"], SOURCE);
    assert!(waiting["requested_at"].is_string());
    assert!(waiting["finished_at"].is_null() && waiting["reason"].is_null());
    assert_eq!(waiting["files"], json!([]));

    // Concurrent checks of one source: each gets its own id, and the one scan
    // after them answers them all with the one file it took.
    engine.drop_file("labs.csv");
    let requests: Vec<_> = (0..8)
        .map(|_| {
            let (api, http) = (engine.api.clone(), engine.http.clone());
            tokio::spawn(async move {
                let answer: Value = http
                    .post(format!("{api}/api/sources/{SOURCE}/check-now"))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                answer["check_id"].as_i64().unwrap()
            })
        })
        .collect();
    let mut checks = vec![first];
    for request in requests {
        checks.push(request.await.unwrap());
    }
    let distinct: std::collections::BTreeSet<i64> = checks.iter().copied().collect();
    assert_eq!(distinct.len(), checks.len(), "{checks:?}");
    for &check in &checks {
        assert_eq!(engine.check(check).await["status"], "pending");
    }
    engine.tick(SOURCE).await;
    let loads = engine.loads(SOURCE).await;
    assert_eq!(loads.len(), 1, "{loads:?}");
    let load = loads[0]["id"].as_i64().unwrap();
    for &check in &checks {
        let found = engine.check(check).await;
        assert_eq!(found["status"], "found", "{found}");
        assert_eq!(load_ids(&found), vec![load]);
        assert_eq!(found["file_count"], 1);
        assert_eq!(found["files"][0]["name"], "labs.csv");
        assert!(found["reason"].is_null());
        assert!(found["finished_at"].as_str().unwrap() >= found["requested_at"].as_str().unwrap());
    }

    // A check asked for once a scan has started waits for the next one, and
    // still reports what the earlier scan took after it was asked for.
    let runner = engine.manager.runner(SOURCE).unwrap();
    let started = runner.pending_checks();
    let late = engine.check_now(SOURCE).await;
    engine.drop_file("labs-again.csv");
    runner.tick_with(&started).await.unwrap();
    assert_eq!(engine.check(late).await["status"], "pending");
    engine.tick(SOURCE).await;
    let found = engine.check(late).await;
    assert_eq!(found["status"], "found", "{found}");
    assert_eq!(found["files"][0]["name"], "labs-again.csv");
    // The same content as before: a duplicate load, and says of which.
    assert_eq!(found["files"][0]["status"], "duplicate");
    assert_eq!(found["files"][0]["duplicate_of"], load);

    // Two loads, read a page at a time with the cursor the answer gives.
    let paged = engine.check_now(SOURCE).await;
    engine.drop_file("two-a.csv");
    engine.drop_file("two-b.csv");
    engine.tick(SOURCE).await;
    let whole = engine.check(paged).await;
    assert_eq!(
        (whole["file_count"].clone(), whole["has_more"].clone()),
        (json!(2), json!(false))
    );
    assert!(whole["next_cursor"].is_null());
    let page = |query: String| {
        let (api, http) = (engine.api.clone(), engine.http.clone());
        async move {
            http.get(format!("{api}/api/checks/{paged}?{query}"))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        }
    };
    let first_page = page("limit=1".into()).await;
    assert_eq!(first_page["has_more"], true);
    assert_eq!(load_ids(&first_page), load_ids(&whole)[..1]);
    let cursor = first_page["next_cursor"].as_str().unwrap().to_string();
    let second_page = page(format!("cursor={cursor}&limit=1")).await;
    assert_eq!(load_ids(&second_page), load_ids(&whole)[1..]);
    assert_eq!(second_page["has_more"], false);

    // Nothing new.
    let empty = engine.check_now(SOURCE).await;
    engine.tick(SOURCE).await;
    let empty = engine.check(empty).await;
    assert_eq!(empty["status"], "nothing_new", "{empty}");
    assert_eq!(
        (empty["files"].clone(), empty["file_count"].clone()),
        (json!([]), json!(0))
    );

    // The folder cannot be listed: failed, with a fixed reason that names no path.
    let inbox = engine.root.join("inbox");
    std::fs::remove_dir_all(&inbox).unwrap();
    std::fs::write(&inbox, "not a folder").unwrap();
    let broken = engine.check_now(SOURCE).await;
    engine.tick(SOURCE).await;
    let failed = engine.check(broken).await;
    assert_eq!(failed["status"], "failed", "{failed}");
    assert_eq!(
        failed["reason"],
        "listing the source failed; the engine log has the details"
    );
    assert!(!failed.to_string().contains("inbox"), "{failed}");
    assert!(failed["finished_at"].is_string());
    assert_eq!(failed["files"], json!([]));
}

#[tokio::test]
async fn unknown_checks_and_sources_are_not_found() {
    let Some(engine) = Engine::start().await else {
        return;
    };
    let missing = engine
        .http
        .get(format!("{}/api/checks/999999", engine.api))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(
        missing.json::<Value>().await.unwrap()["error"],
        "check 999999 not found"
    );
    let not_a_number = engine
        .http
        .get(format!("{}/api/checks/latest", engine.api))
        .send()
        .await
        .unwrap();
    assert_eq!(not_a_number.status(), 400);
    for (query, field) in [
        ("limit=0", "limit"),
        ("limit=-1", "limit"),
        ("cursor=31", "cursor"),
        ("after=31", "after"),
        ("limit=1&limit=2", "limit"),
    ] {
        let refused = engine
            .http
            .get(format!("{}/api/checks/1?{query}", engine.api))
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), 400, "{query}");
        let body: Value = refused.json().await.unwrap();
        assert_eq!(
            (body["code"].as_str(), body["field"].as_str()),
            (Some("invalid_query"), Some(field)),
            "{query}: {body}"
        );
    }
    let no_source = engine
        .http
        .post(format!("{}/api/sources/nope/check-now", engine.api))
        .send()
        .await
        .unwrap();
    assert_eq!(no_source.status(), 404);
}

#[tokio::test]
async fn a_replaced_source_answers_earlier_checks_and_a_paused_or_deleted_one_fails_them() {
    let Some(engine) = Engine::start().await else {
        return;
    };
    engine.create_held(SOURCE).await;

    // Replaced: the new runner answers what was asked of the old one.
    let before = engine.check_now(SOURCE).await;
    let replaced = engine
        .http
        .put(format!("{}/api/sources/{SOURCE}", engine.api))
        .json(&Engine::definition(SOURCE))
        .send()
        .await
        .unwrap();
    assert_eq!(replaced.status(), 200);
    engine.drop_file("labs.csv");
    engine.tick(SOURCE).await;
    let found = engine.check(before).await;
    assert_eq!(found["status"], "found", "{found}");
    assert_eq!(found["files"][0]["name"], "labs.csv");

    // Paused: the source's own pass fails the waiting checks, as soon as this
    // instance may run it.
    let paused = engine
        .http
        .post(format!("{}/api/sources/{SOURCE}/pause", engine.api))
        .send()
        .await
        .unwrap();
    assert_eq!(paused.status(), 200);
    let waiting = engine.check_now(SOURCE).await;
    engine.db.release_lease(SOURCE, OTHER).unwrap();
    let poked = engine.check_now(SOURCE).await;
    for check in [waiting, poked] {
        let failed = engine.answered(check).await;
        assert_eq!(failed["status"], "failed", "{failed}");
        assert_eq!(failed["reason"], "the source is paused");
    }

    // Deleted: a check nobody scanned for fails once the source is gone here.
    // Recorded without a poke while the other instance holds the source, then
    // released: with nothing poking it and an hour between polls, this
    // instance's runner, whose first pass stood by, does not scan before the
    // delete.
    let gone = "gone";
    engine.create_held(gone).await;
    let orphan = engine.db.insert_check(gone).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    engine.db.release_lease(gone, OTHER).unwrap();
    let deleted = engine
        .http
        .delete(format!("{}/api/sources/{gone}", engine.api))
        .send()
        .await
        .unwrap();
    assert!(deleted.status().is_success(), "{}", deleted.status());
    let failed = engine.check(orphan).await;
    assert_eq!(failed["status"], "failed", "{failed}");
    assert_eq!(failed["reason"], "the source was deleted");
}

#[tokio::test]
async fn a_deleted_source_another_instance_holds_keeps_its_checks_for_it() {
    let Some(engine) = Engine::start().await else {
        return;
    };
    engine.create_held(SOURCE).await;
    let check = engine.check_now(SOURCE).await;
    let deleted = engine
        .http
        .delete(format!("{}/api/sources/{SOURCE}", engine.api))
        .send()
        .await
        .unwrap();
    assert!(deleted.status().is_success());
    assert_eq!(engine.check(check).await["status"], "pending");
}

/// The engine's retention purge removes checks older than its cutoff, answered
/// or not, and keeps the newer ones.
#[test]
fn the_retention_purge_removes_old_checks() {
    let db = hie::db::Db::in_memory().unwrap();
    let day = 86_400_000i64;
    let now = hie::db::now_ms();
    let answered = db.insert_check(SOURCE).unwrap();
    db.answer_checks(SOURCE, &[answered], None).unwrap();
    let pending = db.insert_check(SOURCE).unwrap();
    let recent = db.insert_check(SOURCE).unwrap();
    db.query_json(
        "UPDATE checks SET requested_at = CAST($1 AS BIGINT) WHERE id <> CAST($2 AS BIGINT)",
        hie::db::params![now - 40 * day, recent],
    )
    .unwrap();
    let purged = hie::retention::purge(&db, now - 30 * day).unwrap();
    assert_eq!(purged.checks, 2);
    for gone in [answered, pending] {
        assert!(db.check(gone, 0, 10).unwrap().is_none());
    }
    assert_eq!(
        db.check(recent, 0, 10).unwrap().unwrap()["status"],
        "pending"
    );
    assert_eq!(
        hie::retention::purge(&db, now - 30 * day).unwrap().checks,
        0
    );
}
