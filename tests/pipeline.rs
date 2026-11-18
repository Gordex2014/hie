//! End-to-end: a folder feed, the real Python transform, and a stand-in FHIR server
//! that can be taken down and that refuses some resources.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use hie::pipeline::FeedRunner;
use serde_json::{Value, json};

#[derive(Default)]
struct Fhir {
    down: bool,
    versions: HashMap<String, u32>,
    requests: u32,
    /// The next batch is written at once but answered only after a minute.
    hold_next: bool,
}

type Shared = Arc<Mutex<Fhir>>;

async fn batch(
    State(s): State<Shared>,
    Json(bundle): Json<Value>,
) -> (axum::http::StatusCode, Json<Value>) {
    let (hold, answer) = {
        let mut s = s.lock().unwrap();
        s.requests += 1;
        (std::mem::take(&mut s.hold_next), write(&mut s, &bundle))
    };
    if hold {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
    answer
}

fn write(s: &mut Fhir, bundle: &Value) -> (axum::http::StatusCode, Json<Value>) {
    if s.down {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"resourceType": "OperationOutcome"})),
        );
    }
    let mut entries = Vec::new();
    for e in bundle["entry"].as_array().unwrap() {
        let url = e["request"]["url"].as_str().unwrap().to_string();
        if e["resource"]["subject"]["reference"] == "Patient/999" {
            entries.push(json!({"response": {"status": "400 Bad Request", "outcome": {
                "resourceType": "OperationOutcome",
                "issue": [{"severity": "error", "code": "invalid", "details": {"text": "subject Patient/999 does not exist"}}]}}}));
            continue;
        }
        let v = s.versions.entry(url).or_insert(0);
        *v += 1;
        let status = if *v == 1 { "201 Created" } else { "200 OK" };
        entries.push(json!({"response": {"status": status, "etag": format!("W/\"{v}\"")}}));
    }
    (
        axum::http::StatusCode::OK,
        Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries})),
    )
}

async fn fhir_server() -> (String, Shared) {
    let state: Shared = Arc::default();
    let app = Router::new()
        .route("/", post(batch))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), state)
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

fn drop_file(inbox: &Path, name: &str, content: &str) {
    std::fs::create_dir_all(inbox).unwrap();
    std::fs::write(inbox.join(name), content).unwrap();
}

fn count(state: &hie::api::AppState, status: &str) -> i64 {
    state.db.feed_counts("lab-results").unwrap()[status]
        .as_i64()
        .unwrap()
}

fn alert_kinds(state: &hie::api::AppState) -> Vec<String> {
    state
        .db
        .query_json("SELECT kind FROM alerts ORDER BY id", [])
        .unwrap()
        .into_iter()
        .map(|r| r["kind"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn a_feed_survives_an_outage_bad_rows_and_resends() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (base, fhir) = fhir_server().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {base}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/lab-results.yaml"),
        "id: lab-results\nname: Lab results\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  min_file_age_ms: 0\n\
         transform: transforms/lab_results.py\ndelivery:\n  initial_backoff_ms: 10\n  alert_after_attempts: 2\nexpect_every: 1s\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let runner: Arc<FeedRunner> = manager.runner("lab-results").unwrap();
    manager.pause("lab-results", None).unwrap(); // the test drives the passes itself
    let inbox = root.join("inbox");
    let labs = std::fs::read_to_string(repo().join("examples/data/labs.csv")).unwrap();

    // HFS is down when the file arrives: rows are stored, the file leaves the inbox, nothing is lost.
    fhir.lock().unwrap().down = true;
    drop_file(&inbox, "labs.csv", &labs);
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "pending"), 3);
    assert!(!inbox.join("labs.csv").exists());
    tokio::time::sleep(Duration::from_millis(30)).await;
    runner.tick().await.unwrap();
    assert_eq!(alert_kinds(&state), ["delivery"]);

    // HFS comes back: the same rows are delivered on the next retry.
    fhir.lock().unwrap().down = false;
    tokio::time::sleep(Duration::from_millis(60)).await;
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 3);
    assert_eq!(fhir.lock().unwrap().versions.len(), 3);
    assert_eq!(alert_kinds(&state), ["delivery", "delivery_recovered"]);

    // The identical file again: recognized by its hash and not reprocessed.
    let before = fhir.lock().unwrap().requests;
    drop_file(&inbox, "labs.csv", &labs);
    runner.tick().await.unwrap();
    assert_eq!(fhir.lock().unwrap().requests, before);
    assert_eq!(
        state.db.feed_counts("lab-results").unwrap()["duplicate_files"],
        1
    );

    // The same rows in a different file: delivered again, but as updates, not new resources.
    drop_file(&inbox, "labs-resent.csv", &format!("{labs}\n"));
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 6);
    {
        let f = fhir.lock().unwrap();
        assert_eq!(f.versions.len(), 3);
        assert!(f.versions.values().all(|v| *v == 2));
    }

    // A messy file: each bad row is rejected on its own with its reason; good rows go through.
    let messy = std::fs::read_to_string(repo().join("examples/data/messy.csv")).unwrap();
    drop_file(
        &inbox,
        "messy.csv",
        &format!("{messy}Patient/999,2345-7,Glucose,77,mg/dL,2026-10-16\n"),
    );
    runner.tick().await.unwrap();
    let rejected = state
        .db
        .query_json(
            "SELECT line, error_kind, error FROM records WHERE status = 'rejected' ORDER BY line",
            [],
        )
        .unwrap();
    let summary: Vec<(i64, String)> = rejected
        .iter()
        .map(|r| {
            (
                r["line"].as_i64().unwrap(),
                r["error_kind"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (3, "data".into()),
            (4, "parse".into()),
            (5, "data".into()),
            (6, "data".into()),
            (7, "data".into()),
            (8, "server".into())
        ]
    );
    assert!(
        rejected[0]["error"]
            .as_str()
            .unwrap()
            .contains("'alto' is not a number")
    );
    assert!(
        rejected[5]["error"]
            .as_str()
            .unwrap()
            .contains("Patient/999 does not exist")
    );
    assert_eq!(
        count(&state, "sent"),
        7,
        "the GLU local code row is mapped to LOINC and sent"
    );
    assert_eq!(alert_kinds(&state).last().unwrap(), "rejected_records");

    // Someone fixes the value and reprocesses that one row.
    let alto = state
        .db
        .query_json(
            "SELECT id, raw FROM records WHERE line = 3 AND status = 'rejected'",
            [],
        )
        .unwrap();
    let mut raw = alto[0]["raw"].clone();
    raw["value"] = json!("101");
    assert!(
        state
            .db
            .reprocess(alto[0]["id"].as_i64().unwrap(), Some(&raw))
            .unwrap()
    );
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 8);
    assert_eq!(count(&state, "rejected"), 5);
    let fixed = state
        .db
        .query_json(
            "SELECT raw, original_raw FROM records WHERE id = $1",
            hie::db::params![alto[0]["id"].as_i64().unwrap()],
        )
        .unwrap();
    assert_eq!(fixed[0]["raw"]["value"], "101");
    assert_eq!(
        fixed[0]["original_raw"]["value"], "alto",
        "the data as received is kept"
    );
    assert_eq!(
        alert_kinds(&state)
            .iter()
            .filter(|k| *k == "rejected_records")
            .count(),
        1,
        "reprocessing a row does not repeat the file's alert"
    );

    // Nothing arrives for longer than `expect_every`: one silence alert, not one per pass.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    runner.tick().await.unwrap();
    runner.tick().await.unwrap();
    assert_eq!(
        alert_kinds(&state)
            .iter()
            .filter(|k| *k == "silence")
            .count(),
        1
    );
}

// ---- the transform worker's failures, as the pipeline sees them ----------

/// A probe transform: every record it is sent is noted in `seen`, and its
/// `mode` says what to do. Its resources carry the worker's process id.
const PROBE: &str = r#"
import os, time
from hie import TerminologyError

STATE = __STATE__
if os.path.exists(os.path.join(STATE, "broken")):
    raise RuntimeError("the probe cannot load while broken")

def transform(record, context):
    with open(os.path.join(STATE, "seen"), "a") as f:
        f.write(record["id"] + "\n")
    with open(os.path.join(STATE, "calls"), "a") as f:
        f.write(f"{record['id']} {os.getpid()}\n")
    mode = record["mode"]
    if mode == "gate":
        open(os.path.join(STATE, "entered-" + record["id"]), "w").close()
        deadline = time.monotonic() + 60
        while not os.path.exists(os.path.join(STATE, "release-" + record["id"])):
            if time.monotonic() > deadline:
                raise RuntimeError("never released")
            time.sleep(0.05)
    if mode == "hang":
        time.sleep(3600)
    if mode == "hang-then-break":
        open(os.path.join(STATE, "broken"), "w").close()
        time.sleep(3600)
    if mode == "exit":
        os._exit(3)
    if mode == "alloc":
        bytearray(1 << 30)
    if mode == "bigfile":
        with open("big.bin", "wb") as f:
            f.write(b"x" * (2 << 20))
    if mode == "transient":
        raise TerminologyError("the terminology server is down")
    if mode == "slow":
        time.sleep(float(record["seconds"]))
    return {"resourceType": "Observation", "id": "obs-" + record["id"], "status": "final",
            "code": {"text": "probe"}, "valueString": str(os.getpid())}
"#;

struct Probe {
    _dir: tempfile::TempDir,
    state: PathBuf,
    manager: Arc<hie::manager::SourceManager>,
    app: hie::api::AppState,
    feed: hie::config::FeedConfig,
    fhir: Shared,
}

impl Probe {
    /// An engine with one folder source over the probe transform. `paused`: the
    /// test drives the passes itself.
    async fn new(python: &str, worker: &str, paused: bool, csv: &str) -> Self {
        let (base, fhir) = fhir_server().await;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let state = root.join("state");
        for d in ["sources", "transforms", "state", "inbox"] {
            std::fs::create_dir_all(root.join(d)).unwrap();
        }
        std::fs::write(
            root.join("transforms/probe.py"),
            PROBE.replace("__STATE__", &format!("{:?}", state.to_str().unwrap())),
        )
        .unwrap();
        std::fs::write(
            root.join("hie.yaml"),
            format!(
                "fhir_base_url: {base}\ndatabase: data/hie.db\npython: {python}\nworker:\n{worker}"
            ),
        )
        .unwrap();
        std::fs::write(
            root.join("sources/probe.yaml"),
            "id: probe\nname: Probe\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  poll_seconds: 1\n  min_file_age_ms: 0\n\
             transform: transforms/probe.py\ndelivery:\n  initial_backoff_ms: 10\n",
        )
        .unwrap();
        if !paused {
            drop_file(&root.join("inbox"), "rows.csv", csv);
        }
        let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
        let feed = feeds[0].clone();
        let (manager, app) =
            hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
        if paused {
            manager.pause("probe", None).unwrap();
            drop_file(&root.join("inbox"), "rows.csv", csv);
        }
        Self {
            _dir: dir,
            state,
            manager,
            app,
            feed,
            fhir,
        }
    }

    /// The engine's API, served on a free port.
    async fn api(&self) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = hie::api::router(self.app.clone());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{addr}")
    }

    /// Every call of the transform: the record's id and the worker's pid.
    fn calls(&self) -> Vec<(String, String)> {
        std::fs::read_to_string(self.state.join("calls"))
            .unwrap_or_default()
            .lines()
            .map(|l| {
                let (id, pid) = l.split_once(' ').unwrap();
                (id.to_string(), pid.to_string())
            })
            .collect()
    }

    fn calls_of(&self, id: &str) -> usize {
        self.calls().iter().filter(|(i, _)| i == id).count()
    }

    /// Waits until a gated record's transform is running.
    async fn wait_entered(&self, id: &str) {
        let started = std::time::Instant::now();
        while !self.state.join(format!("entered-{id}")).exists() {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "record {id} never reached the transform"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Lets a gated record go once the replacement is waiting for the runner that
    /// holds it, or once a second runner has reached the record too.
    async fn release(&self, id: &str) {
        let started = std::time::Instant::now();
        while self.calls_of(id) < 2 && self.manager.unfinished_runners("probe") < 2 {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "no replacement is waiting"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        std::fs::write(self.state.join(format!("release-{id}")), "").unwrap();
    }

    async fn wait_sent(&self, ids: &[&str]) {
        let started = std::time::Instant::now();
        loop {
            let rows = self.rows();
            if ids
                .iter()
                .all(|id| rows.get(*id).is_some_and(|r| r.0 == "sent"))
            {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "not all sent: {rows:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Each record transformed once and each of its resources written to FHIR once.
    fn assert_once(&self, ids: &[&str]) {
        for id in ids {
            assert_eq!(self.calls_of(id), 1, "record {id}: {:?}", self.calls());
        }
        let fhir = self.fhir.lock().unwrap();
        assert_eq!(fhir.versions.len(), ids.len(), "{:?}", fhir.versions);
        assert!(
            fhir.versions.values().all(|v| *v == 1),
            "a resource written twice: {:?}",
            fhir.versions
        );
    }

    async fn tick(&self) {
        self.manager.runner("probe").unwrap().tick().await.unwrap();
    }

    fn seen(&self) -> Vec<String> {
        std::fs::read_to_string(self.state.join("seen"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    /// Each row by its `id`: status, error kind, error, and the worker's pid.
    fn rows(&self) -> HashMap<String, (String, String, String, String)> {
        self.app
            .db
            .query_json(
                "SELECT raw, status, error_kind, error, resources FROM records WHERE feed_id = 'probe'",
                [],
            )
            .unwrap()
            .into_iter()
            .map(|r| {
                let text = |v: &Value| v.as_str().unwrap_or_default().to_string();
                let resources = match &r["resources"] {
                    Value::String(s) => serde_json::from_str(s).unwrap(),
                    v => v.clone(),
                };
                (
                    text(&r["raw"]["id"]),
                    (
                        text(&r["status"]),
                        text(&r["error_kind"]),
                        text(&r["error"]),
                        text(&resources[0]["valueString"]),
                    ),
                )
            })
            .collect()
    }
}

#[tokio::test]
async fn a_record_that_hangs_crashes_or_runs_out_of_memory_is_rejected_once() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    // The memory and file-size limits are enforced on Linux only; elsewhere
    // the rows end at 5.
    let memory = cfg!(target_os = "linux");
    let mut csv = String::from("id,mode\n1,ok\n2,hang\n3,ok\n4,exit\n5,ok\n");
    if memory {
        csv.push_str("6,alloc\n7,ok\n8,bigfile\n9,ok\n");
    }
    let csv = csv.as_str();
    let probe = Probe::new(
        &python,
        "  request_seconds: 2\n  memory_mib: 512\n  file_mib: 1\n",
        true,
        csv,
    )
    .await;
    probe.tick().await;
    let rows = probe.rows();
    let mut failures = vec![
        ("2", "did not answer within 2 s"),
        ("4", "exited with code 3"),
    ];
    let mut good = vec!["1", "3", "5"];
    if memory {
        failures.push(("6", "exceeded its memory limit (512 MiB)"));
        good.push("7");
        // EFBIG ends the worker with the launcher's file code (66).
        failures.push(("8", "wrote a file past its size limit (1 MiB)"));
        good.push("9");
    }
    for (id, reason) in failures {
        let (status, kind, error, _) = &rows[id];
        assert_eq!((status.as_str(), kind.as_str()), ("rejected", "transform"));
        assert!(error.contains(reason), "row {id}: {error}");
    }
    if memory {
        // The launcher's exit codes, 65 and 66, as the contract classifies them.
        assert!(
            rows["6"].2.contains("verdict limit_exceeded, cause memory"),
            "{:?}",
            rows["6"]
        );
        assert!(
            rows["8"].2.contains("verdict limit_exceeded, cause file"),
            "{:?}",
            rows["8"]
        );
    }
    let pids: Vec<&str> = good
        .iter()
        .map(|id| {
            assert_eq!(rows[*id].0, "sent", "row {id}: {:?}", rows[*id]);
            rows[*id].3.as_str()
        })
        .collect();
    let distinct: std::collections::HashSet<_> = pids.iter().collect();
    assert_eq!(
        distinct.len(),
        good.len(),
        "a new worker after each failure: {pids:?}"
    );
    let all: Vec<String> = (1..=if memory { 9 } else { 5 })
        .map(|i| i.to_string())
        .collect();
    assert_eq!(probe.seen(), all);

    // Later passes send nothing again.
    probe.tick().await;
    probe.tick().await;
    assert_eq!(probe.seen(), all);

    // An operator's reprocess is the one way it runs again: once.
    let api = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = hie::api::router(probe.app.clone());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{addr}")
    };
    let hung = probe
        .app
        .db
        .query_json(
            "SELECT id FROM records WHERE feed_id = 'probe' AND line = 3",
            [],
        )
        .unwrap()[0]["id"]
        .as_i64()
        .unwrap();
    let status = reqwest::Client::new()
        .post(format!("{api}/api/records/{hung}/reprocess"))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 202);
    probe.tick().await;
    probe.tick().await;
    let mut again = all.clone();
    again.push("2".into());
    assert_eq!(probe.seen(), again);
    assert_eq!(probe.rows()["2"].0, "rejected");
}

#[tokio::test]
async fn a_timed_out_record_is_not_sent_again_when_the_worker_then_cannot_start() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let probe = Probe::new(
        &python,
        "  request_seconds: 1\n",
        true,
        "id,mode\n1,hang-then-break\n2,ok\n",
    )
    .await;
    probe.tick().await;
    probe.tick().await;
    probe.tick().await;
    let rows = probe.rows();
    assert_eq!(rows["1"].0, "rejected");
    assert_eq!(
        rows["2"].0, "received",
        "left for when the worker starts again"
    );
    assert_eq!(probe.seen(), ["1"]);

    std::fs::remove_file(probe.state.join("broken")).unwrap();
    probe.tick().await;
    assert_eq!(probe.rows()["2"].0, "sent");
    assert_eq!(probe.seen(), ["1", "2"]);
}

#[tokio::test]
async fn a_transient_record_ends_the_pass_before_the_next_is_sent() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let probe = Probe::new(
        &python,
        "  request_seconds: 1\n",
        true,
        "id,mode\n1,transient\n2,hang\n",
    )
    .await;
    probe.tick().await;
    assert_eq!(probe.seen(), ["1"]);
    let rows = probe.rows();
    assert_eq!(
        (rows["1"].0.as_str(), rows["2"].0.as_str()),
        ("received", "received")
    );
}

#[tokio::test]
async fn a_source_replaced_mid_record_does_not_resend_the_one_already_decided() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let probe = Probe::new(
        &python,
        "  request_seconds: 1\n",
        false,
        "id,mode,seconds\n1,hang,\n2,slow,3\n3,ok,\n",
    )
    .await;
    let started = std::time::Instant::now();
    while !probe.seen().contains(&"2".to_string()) {
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "record 2 never sent"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Record 1 timed out and is decided; record 2 is in flight.
    probe.manager.delete("probe").await.unwrap();
    probe.manager.start(probe.feed.clone()).unwrap();
    while probe.rows()["3"].0 != "sent" {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "record 3 never sent"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(probe.rows()["1"].0, "rejected");
    for id in ["1", "2"] {
        assert_eq!(
            probe.seen().iter().filter(|seen| *seen == id).count(),
            1,
            "record {id}: {:?}",
            probe.seen()
        );
    }
}

// ---- a replaced source: one runner at a time --------------------------------

const GATED: &str = "id,mode\n1,gate\n2,ok\n3,ok\n";

async fn gated_probe() -> Option<Probe> {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return None;
    };
    let probe = Probe::new(&python, "  request_seconds: 30\n", false, GATED).await;
    probe.wait_entered("1").await;
    Some(probe)
}

#[tokio::test]
async fn a_source_replaced_mid_record_runs_each_record_once() {
    let Some(probe) = gated_probe().await else {
        return;
    };
    let api = probe.api().await;
    let definition = probe.manager.definition("probe").unwrap().unwrap();
    let status = reqwest::Client::new()
        .put(format!("{api}/api/sources/probe"))
        .json(&definition)
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 200);
    probe.release("1").await;
    probe.wait_sent(&["1", "2", "3"]).await;
    probe.assert_once(&["1", "2", "3"]);
}

#[tokio::test]
async fn a_source_replaced_twice_mid_record_runs_its_runners_in_turn() {
    let Some(probe) = gated_probe().await else {
        return;
    };
    probe.manager.start(probe.feed.clone()).unwrap();
    probe.manager.start(probe.feed.clone()).unwrap();
    probe.release("1").await;
    probe.wait_sent(&["1", "2", "3"]).await;
    probe.assert_once(&["1", "2", "3"]);
    // The first runner finished its record; the middle one never ran; the last did the rest.
    let calls = probe.calls();
    assert_ne!(calls[0].1, calls[1].1, "{calls:?}");
    assert_eq!(calls[1].1, calls[2].1, "{calls:?}");
}

#[tokio::test]
async fn a_source_deleted_and_created_again_mid_record_runs_each_record_once() {
    let Some(probe) = gated_probe().await else {
        return;
    };
    let api = probe.api().await;
    let definition = probe.manager.definition("probe").unwrap().unwrap();
    let client = reqwest::Client::new();
    let status = client
        .delete(format!("{api}/api/sources/probe"))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 204);
    let status = client
        .post(format!("{api}/api/sources"))
        .json(&definition)
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 201);
    probe.release("1").await;
    probe.wait_sent(&["1", "2", "3"]).await;
    probe.assert_once(&["1", "2", "3"]);
}

#[tokio::test]
async fn a_transform_saved_mid_record_runs_each_record_once() {
    let Some(probe) = gated_probe().await else {
        return;
    };
    let api = probe.api().await;
    let code = probe.manager.transform_code("probe").unwrap().unwrap();
    let status = reqwest::Client::new()
        .put(format!("{api}/api/transforms/probe"))
        .json(&json!({"code": code}))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 200);
    probe.release("1").await;
    probe.wait_sent(&["1", "2", "3"]).await;
    probe.assert_once(&["1", "2", "3"]);
}

#[tokio::test]
async fn a_replacement_aborts_a_runner_that_does_not_stop_within_the_grace() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let probe = Probe::new(&python, "  request_seconds: 30\n", true, "id,mode\n1,ok\n").await;
    probe.fhir.lock().unwrap().hold_next = true;
    probe.manager.resume("probe").unwrap();
    // The runner has sent its record's batch, and FHIR keeps it waiting.
    let started = std::time::Instant::now();
    while probe.fhir.lock().unwrap().requests == 0 {
        assert!(started.elapsed() < Duration::from_secs(30), "nothing sent");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let grace = Duration::from_secs(2);
    probe.manager.set_handover_grace(grace);
    let replaced = std::time::Instant::now();
    probe.manager.start(probe.feed.clone()).unwrap();
    probe.wait_sent(&["1"]).await;
    let waited = replaced.elapsed();
    assert!(
        waited >= grace && waited < Duration::from_secs(30),
        "{waited:?}"
    );
    assert_eq!(probe.calls_of("1"), 1, "{:?}", probe.calls());
    // FHIR had written the batch the aborted runner was waiting on: only a forced
    // abort sends a batch again, and the second write is an update by id.
    let fhir = probe.fhir.lock().unwrap();
    assert_eq!(fhir.requests, 2);
    assert_eq!(
        fhir.versions,
        HashMap::from([("Observation/obs-1".to_string(), 2)])
    );
}

// A regression test for the handover itself, not for the overlap fixed before
// it: an older runner that times out must never abort a newer one. It passes on
// a build without any handover, which has no aborts at all.
#[tokio::test(flavor = "multi_thread")]
async fn runners_replaced_in_a_row_never_abort_a_newer_one() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let probe = Probe::new(&python, "  request_seconds: 30\n", false, "id,mode\n0,ok\n").await;
    probe.wait_sent(&["0"]).await;
    let grace = Duration::from_millis(500);
    probe.manager.set_handover_grace(grace);
    let inbox = probe.state.parent().unwrap().join("inbox");
    let mut ids = vec!["0".to_string()];
    for round in 1..=5 {
        // Replaced twice from a runtime worker, as an API call does.
        let manager = probe.manager.clone();
        let feed = probe.feed.clone();
        tokio::spawn(async move {
            manager.start(feed.clone()).unwrap();
            manager.start(feed).unwrap();
        })
        .await
        .unwrap();
        // Past the grace, the last runner is the one left, and it still works.
        tokio::time::sleep(grace * 3).await;
        drop_file(
            &inbox,
            &format!("round-{round}.csv"),
            &format!("id,mode\n{round},ok\n"),
        );
        ids.push(round.to_string());
        let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
        probe.wait_sent(&ids).await;
    }
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    probe.assert_once(&ids);
}

#[tokio::test]
async fn a_halted_runner_delivers_nothing_more() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let probe = Probe::new(
        &python,
        "  request_seconds: 30\n",
        true,
        "id,mode\n1,ok\n2,ok\n",
    )
    .await;
    // FHIR is down for one pass: both records are transformed and wait to be delivered.
    probe.fhir.lock().unwrap().down = true;
    probe.tick().await;
    let requests = probe.fhir.lock().unwrap().requests;
    assert!(requests > 0);
    probe.fhir.lock().unwrap().down = false;
    let runner = probe.manager.runner("probe").unwrap();
    probe.manager.delete("probe").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await; // past the 10 ms backoff
    runner.tick().await.unwrap();
    assert_eq!(probe.fhir.lock().unwrap().requests, requests);
    let rows = probe.rows();
    assert!(rows.values().all(|r| r.0 != "sent"), "{rows:?}");
}

#[tokio::test]
async fn deleted_sources_leave_no_runner_slots_behind() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let probe = Probe::new(&python, "  request_seconds: 30\n", true, "id,mode\n").await;
    let definition = probe.manager.definition("probe").unwrap().unwrap();
    let ids: Vec<String> = (0..10).map(|i| format!("extra-{i}")).collect();
    for id in &ids {
        let mut body = definition.clone();
        body["id"] = json!(id);
        probe.manager.create(&body, None).await.unwrap();
    }
    assert_eq!(probe.manager.runner_slots(), 11);
    for id in &ids {
        probe.manager.delete(id).await.unwrap();
    }
    let started = std::time::Instant::now();
    while probe.manager.runner_slots() != probe.manager.sources().len() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "{} slots for {} sources",
            probe.manager.runner_slots(),
            probe.manager.sources().len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(probe.manager.runner_slots(), 1);
}
