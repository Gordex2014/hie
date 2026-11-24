//! The event log: each outcome stored with its event before the next record is
//! sent or transformed, nothing stored when either fails, outcomes for a record
//! put back meanwhile discarded, the bulk path's submission events, the API's
//! paging and cursor, ordering across writers on PostgreSQL, the retention
//! purge, and a restart.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use hie::db::{Admission, Db, Delivery, EventQuery, NewFile, Rule};
use serde_json::{Value, json};

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

fn test_database_url() -> Option<String> {
    std::env::var("HIE_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
}

async fn bind() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (listener, url)
}

async fn serve(router: Router) -> String {
    let (listener, url) = bind().await;
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    url
}

/// Every event, oldest first, with its detail parsed.
fn events(db: &Db) -> Vec<Value> {
    db.query_json(
        "SELECT id, kind, source_id, load_id, record_id, attempt, error_kind, reason, detail FROM events ORDER BY id",
        [],
    )
    .unwrap()
    .into_iter()
    .map(|mut e| {
        if let Some(Value::String(text)) = e.get("detail") {
            e["detail"] = serde_json::from_str(text).unwrap();
        }
        e
    })
    .collect()
}

fn kinds(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .map(|e| e["kind"].as_str().unwrap().to_string())
        .collect()
}

fn of_record(events: &[Value], record: i64) -> Vec<Value> {
    events
        .iter()
        .filter(|e| e["record_id"].as_i64() == Some(record))
        .cloned()
        .collect()
}

fn record_id(db: &Db, feed: &str, line: i64) -> i64 {
    db.query_json(
        "SELECT id FROM records WHERE feed_id = $1 AND line = $2 ORDER BY id DESC",
        hie::db::params![feed, line],
    )
    .unwrap()[0]["id"]
        .as_i64()
        .unwrap()
}

fn status(db: &Db, record: i64) -> String {
    db.query_json(
        "SELECT status FROM records WHERE id = $1",
        hie::db::params![record],
    )
    .unwrap()[0]["status"]
        .as_str()
        .unwrap()
        .to_string()
}

/// An engine over the files in `root`: `hie.yaml` with `extra` lines, and one
/// source file per `(id, yaml)`. Every source is paused, so the test drives the
/// passes itself.
fn engine(
    root: &Path,
    extra: &str,
    sources: &[(&str, String)],
) -> (Arc<hie::manager::SourceManager>, hie::api::AppState) {
    for d in ["sources", "transforms", "inbox", "state"] {
        std::fs::create_dir_all(root.join(d)).unwrap();
    }
    std::fs::copy(
        repo().join("transforms/fhir_passthrough.py"),
        root.join("transforms/fhir_passthrough.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("database: data/hie.db\n{extra}"),
    )
    .unwrap();
    for (id, yaml) in sources {
        std::fs::write(root.join(format!("sources/{id}.yaml")), yaml).unwrap();
    }
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let ids: Vec<String> = feeds.iter().map(|f| f.id.clone()).collect();
    let (manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    for id in ids {
        manager.pause(&id, None).unwrap();
    }
    (manager, state)
}

/// A folder source of NDJSON resources, delivered by REST through the
/// pass-through transform.
fn ndjson_source(id: &str, delivery: &str) -> String {
    format!(
        "id: {id}\nname: {id}\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.ndjson\"\n  min_file_age_ms: 0\n\
         format: ndjson\ntransform: transforms/fhir_passthrough.py\ndelivery:\n  initial_backoff_ms: 10\n{delivery}"
    )
}

fn patients(ids: &[&str]) -> String {
    ids.iter()
        .map(|id| format!("{{\"resourceType\":\"Patient\",\"id\":\"{id}\"}}\n"))
        .collect()
}

// ---- a stand-in FHIR server for the REST path --------------------------------

#[derive(Default)]
struct Fhir {
    /// The next requests answered 503.
    fail: u32,
    down: bool,
    /// The next request is written, then answered only once notified.
    hold: Option<Arc<tokio::sync::Notify>>,
    /// The resources of every request, in the order received.
    requests: Vec<Vec<String>>,
    writes: HashMap<String, u32>,
    /// What the server read from its own store handle as each request came in.
    seen: Vec<Vec<(String, i64)>>,
    store: Option<Db>,
}

type Shared = Arc<Mutex<Fhir>>;

async fn batch(State(s): State<Shared>, Json(bundle): Json<Value>) -> Response {
    let (hold, down) = {
        let mut s = s.lock().unwrap();
        let urls: Vec<String> = bundle["entry"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["request"]["url"].as_str().unwrap().to_string())
            .collect();
        if let Some(store) = &s.store {
            let seen = store
                .query_json(
                    "SELECT e.kind, r.line FROM events e JOIN records r ON r.id = e.record_id ORDER BY e.id",
                    [],
                )
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r["kind"].as_str().unwrap().to_string(),
                        r["line"].as_i64().unwrap(),
                    )
                })
                .collect();
            s.seen.push(seen);
        }
        let down = s.down || s.fail > 0;
        s.fail = s.fail.saturating_sub(1);
        if !down {
            for url in &urls {
                *s.writes.entry(url.clone()).or_default() += 1;
            }
        }
        s.requests.push(urls);
        (s.hold.take(), down)
    };
    if let Some(hold) = hold {
        hold.notified().await;
    }
    if down {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let entries: Vec<Value> = bundle["entry"]
        .as_array()
        .unwrap()
        .iter()
        .map(|_| json!({"response": {"status": "201 Created"}}))
        .collect();
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
        .into_response()
}

async fn fhir_server() -> (String, Shared) {
    let state: Shared = Arc::default();
    let url = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(state.clone()),
    )
    .await;
    (url, state)
}

/// The resource each request carried, as the line of its record (`pN` is line N).
fn request_lines(fhir: &Fhir) -> Vec<i64> {
    fhir.requests
        .iter()
        .map(|urls| urls[0].trim_start_matches("Patient/p").parse().unwrap())
        .collect()
}

#[tokio::test]
async fn each_outcome_is_stored_with_its_event_before_the_next_record_is_sent() {
    if python().is_none() {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    }
    let (base, fhir) = fhir_server().await;
    let dir = tempfile::tempdir().unwrap();
    let (manager, state) = engine(
        dir.path(),
        &format!("fhir_base_url: {base}\npython: {}\n", python().unwrap()),
        &[("one", ndjson_source("one", "  batch_size: 1\n"))],
    );
    let (config, _) = hie::config::load(&dir.path().join("hie.yaml")).unwrap();
    {
        let mut f = fhir.lock().unwrap();
        // Its own handle on the store, as another reader would have.
        f.store = Some(Db::open(&config.database).unwrap());
        f.fail = 1;
    }
    std::fs::write(
        dir.path().join("inbox/p.ndjson"),
        patients(&["p1", "p2", "p3"]),
    )
    .unwrap();
    let runner = manager.runner("one").unwrap();
    for _ in 0..20 {
        runner.tick().await.unwrap();
        if state.db.feed_counts("one").unwrap()["sent"] == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(state.db.feed_counts("one").unwrap()["sent"], 3);

    // Each request saw the event of every earlier request's outcome: the failed
    // attempt before that record went out again, each delivery before the next
    // record went out.
    let f = fhir.lock().unwrap();
    let lines = request_lines(&f);
    assert!(lines.len() >= 4, "{lines:?}");
    let mut expected: Vec<(String, i64)> = Vec::new();
    let mut failed = std::collections::HashSet::new();
    for (i, line) in lines.iter().enumerate() {
        for event in &expected {
            let wanted = expected.iter().filter(|e| *e == event).count();
            let seen = f.seen[i].iter().filter(|e| *e == event).count();
            assert!(
                seen >= wanted,
                "request {i} ({line}) did not see {event:?}: {:?}",
                f.seen[i]
            );
        }
        let kind = if i == 0 {
            failed.insert(*line);
            "record.attempt_failed"
        } else if failed.contains(line) {
            "record.recovered"
        } else {
            "record.delivered"
        };
        expected.push((kind.to_string(), *line));
    }
    drop(f);
    let all = events(&state.db);
    let p1 = record_id(&state.db, "one", 1);
    let p1_events = of_record(&all, p1);
    assert_eq!(
        kinds(&p1_events),
        ["record.attempt_failed", "record.recovered"]
    );
    assert_eq!(p1_events[0]["attempt"], 1);
    assert_eq!(p1_events[0]["detail"]["phase"], "delivery");
    assert_eq!(p1_events[1]["attempt"], 2);
}

/// The probe transform, for the transform step: it counts the rejections the
/// store holds each time it is called, then does what the row's `mode` says.
const COUNTING_PROBE: &str = r#"
import sqlite3
from hie import DataError

DB = __DB__
OUT = __OUT__

def transform(record, context):
    con = sqlite3.connect("file:" + DB + "?mode=ro", uri=True)
    n = con.execute("SELECT COUNT(*) FROM events WHERE kind = 'record.rejected'").fetchone()[0]
    con.close()
    with open(OUT, "a") as f:
        f.write(f"{record['id']} {n}\n")
    if record["mode"] == "reject":
        raise DataError("rejected by the probe")
    return {"resourceType": "Observation", "id": "obs-" + record["id"], "status": "final",
            "code": {"text": "probe"}}
"#;

fn csv_source(id: &str, transform: &str) -> String {
    format!(
        "id: {id}\nname: {id}\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  poll_seconds: 1\n  min_file_age_ms: 0\n\
         transform: transforms/{transform}\ndelivery:\n  initial_backoff_ms: 10\n"
    )
}

#[tokio::test]
async fn each_rejection_is_stored_with_its_event_before_the_next_record_is_transformed() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    if test_database_url().is_some() {
        eprintln!("skipping: the probe reads the SQLite file");
        return;
    }
    let (base, _fhir) = fhir_server().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    let out = root.join("state/counts");
    std::fs::write(
        root.join("transforms/counting.py"),
        COUNTING_PROBE
            .replace(
                "__DB__",
                &format!("{:?}", root.join("data/hie.db").to_str().unwrap()),
            )
            .replace("__OUT__", &format!("{:?}", out.to_str().unwrap())),
    )
    .unwrap();
    let (manager, _state) = engine(
        root,
        &format!("fhir_base_url: {base}\npython: {python}\n"),
        &[("probe", csv_source("probe", "counting.py"))],
    );
    std::fs::write(
        root.join("inbox/rows.csv"),
        "id,mode\n1,reject\n2,reject\n3,ok\n4,reject\n5,ok\n",
    )
    .unwrap();
    manager.runner("probe").unwrap().tick().await.unwrap();
    let counts: Vec<String> = std::fs::read_to_string(&out)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(counts, ["1 0", "2 1", "3 2", "4 2", "5 3"]);
}

/// A SQLite trigger that refuses every event, or its removal.
/// A SQLite trigger that refuses the events of one kind.
fn refuse_events_of(db_path: &Path, kind: &str) {
    let c = rusqlite::Connection::open(db_path).unwrap();
    c.busy_timeout(Duration::from_secs(5)).unwrap();
    c.execute_batch(&format!(
        "CREATE TRIGGER refuse_events BEFORE INSERT ON events WHEN NEW.kind = '{kind}' \
         BEGIN SELECT RAISE(ABORT, 'events refused'); END;"
    ))
    .unwrap();
}

fn refuse_events(db_path: &Path, refuse: bool) {
    let c = rusqlite::Connection::open(db_path).unwrap();
    c.busy_timeout(Duration::from_secs(5)).unwrap();
    if refuse {
        c.execute_batch(
            "CREATE TRIGGER refuse_events BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'events refused'); END;",
        )
        .unwrap();
    } else {
        c.execute_batch("DROP TRIGGER refuse_events").unwrap();
    }
}

#[tokio::test]
async fn an_outcome_whose_event_cannot_be_stored_is_not_stored_either() {
    if python().is_none() {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    }
    if test_database_url().is_some() {
        eprintln!("skipping: the trigger is SQLite's");
        return;
    }
    let (base, fhir) = fhir_server().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let (manager, state) = engine(
        root,
        &format!("fhir_base_url: {base}\npython: {}\n", python().unwrap()),
        &[("one", ndjson_source("one", ""))],
    );
    let db_path = root.join("data/hie.db");
    let runner = manager.runner("one").unwrap();
    let files = || {
        state
            .db
            .query_json("SELECT COUNT(*) AS n FROM files", [])
            .unwrap()[0]["n"]
            .as_i64()
            .unwrap()
    };

    // Admission: without its events, no file row; the file stays in the inbox.
    refuse_events(&db_path, true);
    std::fs::write(root.join("inbox/a.ndjson"), patients(&["p1"])).unwrap();
    runner.tick().await.unwrap();
    assert_eq!(files(), 0);
    assert!(root.join("inbox/a.ndjson").exists());
    assert!(events(&state.db).is_empty());

    // Once events can be stored, the next pass takes the same file, not as a
    // duplicate, and delivers it once.
    refuse_events(&db_path, false);
    runner.tick().await.unwrap();
    assert_eq!(files(), 1);
    assert_eq!(state.db.feed_counts("one").unwrap()["duplicate_files"], 0);
    assert_eq!(fhir.lock().unwrap().writes["Patient/p1"], 1);
    assert_eq!(
        kinds(&events(&state.db)),
        ["load.received", "record.delivered", "load.finished"]
    );

    // A delivery: the server took the record, but the outcome and its event
    // cannot be stored. The record stays pending, with no event.
    fhir.lock().unwrap().down = true;
    std::fs::write(root.join("inbox/b.ndjson"), patients(&["p2"])).unwrap();
    runner.tick().await.unwrap();
    let p2 = record_id(&state.db, "one", 1);
    assert_eq!(status(&state.db, p2), "pending");
    refuse_events(&db_path, true);
    fhir.lock().unwrap().down = false;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(runner.tick().await.is_err());
    assert_eq!(status(&state.db, p2), "pending");
    assert_eq!(fhir.lock().unwrap().writes["Patient/p2"], 1);
    // Only the failed attempts made while the server was down (a pass may try
    // a due record twice).
    let failed = kinds(&of_record(&events(&state.db), p2));
    assert!(
        !failed.is_empty() && failed.iter().all(|k| k == "record.attempt_failed"),
        "{failed:?}"
    );

    refuse_events(&db_path, false);
    runner.tick().await.unwrap();
    assert_eq!(status(&state.db, p2), "sent");
    let mut expected = failed.clone();
    expected.push("record.recovered".into());
    assert_eq!(kinds(&of_record(&events(&state.db), p2)), expected);
}

fn zip_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
    use std::io::Write;
    let mut buf = std::io::Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(&mut buf);
    for (name, body) in entries {
        zip.start_file(*name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(body).unwrap();
    }
    zip.finish().unwrap();
    buf.into_inner()
}

#[tokio::test]
async fn a_file_that_cannot_be_read_and_a_zip_are_loads_in_the_log() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (base, _fhir) = fhir_server().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let (manager, state) = engine(
        root,
        &format!("fhir_base_url: {base}\npython: {python}\n"),
        &[(
            "drops",
            ndjson_source("drops", "").replace("\"*.ndjson\"", "\"*\""),
        )],
    );
    // Neither UTF-8 nor UTF-16 with a byte-order mark.
    std::fs::write(root.join("inbox/empty.ndjson"), [0xc3u8, 0x28, 0x0a]).unwrap();
    std::fs::write(root.join("inbox/broken.zip"), "not a zip").unwrap();
    std::fs::write(
        root.join("inbox/drop.zip"),
        zip_of(&[("a.ndjson", patients(&["z1"]).as_bytes())]),
    )
    .unwrap();
    manager.runner("drops").unwrap().tick().await.unwrap();

    let all = events(&state.db);
    let by_name = |name: &str| -> Vec<Value> {
        let load = state
            .db
            .query_json(
                "SELECT id FROM files WHERE name = $1",
                hie::db::params![name],
            )
            .unwrap()[0]["id"]
            .clone();
        all.iter()
            .filter(|e| e["load_id"] == load)
            .cloned()
            .collect()
    };
    let unreadable = by_name("empty.ndjson");
    assert_eq!(kinds(&unreadable), ["load.unreadable"]);
    assert!(!unreadable[0]["reason"].as_str().unwrap().is_empty());
    assert_eq!(unreadable[0]["detail"]["name"], "empty.ndjson");
    let broken = by_name("broken.zip");
    assert_eq!(kinds(&broken), ["load.received", "load.unreadable"]);
    assert_eq!(broken[0]["detail"]["container"], true);
    let container = by_name("drop.zip");
    assert_eq!(kinds(&container), ["load.received", "load.finished"]);
    assert_eq!(
        container[1]["detail"],
        json!({"records": 0, "sent": 0, "rejected": 0, "container": true})
    );
    let entry = by_name("drop.zip!/a.ndjson");
    assert_eq!(
        kinds(&entry),
        ["load.received", "record.delivered", "load.finished"]
    );
    assert_eq!(entry[0]["detail"]["records"], 1);
}

// ---- a record put back while its outcome is being worked out -----------------

/// A probe transform: `gate` waits for its release file, then rejects the row
/// when `then` says so; `transient` cannot finish now.
const GATE_PROBE: &str = r#"
import os, time
from hie import DataError, TerminologyError

STATE = __STATE__

def transform(record, context):
    with open(os.path.join(STATE, "calls"), "a") as f:
        f.write(record["id"] + "\n")
    if record["mode"] == "transient":
        raise TerminologyError("the terminology server is down")
    if record["mode"] == "gate":
        open(os.path.join(STATE, "entered-" + record["id"]), "w").close()
        deadline = time.monotonic() + 60
        while not os.path.exists(os.path.join(STATE, "release-" + record["id"])):
            if time.monotonic() > deadline:
                raise RuntimeError("never released")
            time.sleep(0.05)
        if record.get("then") == "reject":
            raise DataError("rejected for a revision put back since")
    return {"resourceType": "Observation", "id": "obs-" + record["id"], "status": "final",
            "code": {"text": "probe"}}
"#;

fn gate_engine(
    root: &Path,
    base: &str,
    python: &str,
) -> (Arc<hie::manager::SourceManager>, hie::api::AppState) {
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::write(
        root.join("transforms/gate.py"),
        GATE_PROBE.replace(
            "__STATE__",
            &format!("{:?}", root.join("state").to_str().unwrap()),
        ),
    )
    .unwrap();
    engine(
        root,
        &format!("fhir_base_url: {base}\npython: {python}\n"),
        &[("gate", csv_source("gate", "gate.py"))],
    )
}

async fn wait_for(what: &str, mut ready: impl FnMut() -> bool) {
    let started = std::time::Instant::now();
    while !ready() {
        assert!(started.elapsed() < Duration::from_secs(30), "{what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_transform_that_cannot_finish_now_is_logged_once_per_reason_and_runner() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (base, _fhir) = fhir_server().await;
    let dir = tempfile::tempdir().unwrap();
    let (manager, state) = gate_engine(dir.path(), &base, &python);
    std::fs::write(dir.path().join("inbox/rows.csv"), "id,mode\n1,transient\n").unwrap();
    let runner = manager.runner("gate").unwrap();
    if test_database_url().is_none() {
        // The event cannot be stored on the first pass: it is not taken as
        // logged, and the next pass logs it.
        let db_path = dir.path().join("data/hie.db");
        refuse_events_of(&db_path, "record.attempt_failed");
        assert!(runner.tick().await.is_err());
        assert!(!kinds(&events(&state.db)).contains(&"record.attempt_failed".to_string()));
        refuse_events(&db_path, false);
    }
    runner.tick().await.unwrap();
    runner.tick().await.unwrap();
    // Line 1 is the header.
    let record = record_id(&state.db, "gate", 2);
    assert_eq!(status(&state.db, record), "received");
    let mine = of_record(&events(&state.db), record);
    assert_eq!(kinds(&mine), ["record.attempt_failed"]);
    assert_eq!(mine[0]["detail"]["phase"], "transform");
    assert!(mine[0]["attempt"].is_null());
    assert!(
        mine[0]["reason"]
            .as_str()
            .unwrap()
            .contains("terminology server is down")
    );

    // A runner that starts again logs it again, once.
    manager.start(manager.source("gate").unwrap()).unwrap();
    let runner = manager.runner("gate").unwrap();
    runner.tick().await.unwrap();
    runner.tick().await.unwrap();
    assert_eq!(
        kinds(&of_record(&events(&state.db), record)),
        ["record.attempt_failed", "record.attempt_failed"]
    );
}

#[tokio::test]
async fn an_outcome_for_a_record_put_back_meanwhile_changes_nothing() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (base, fhir) = fhir_server().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let (manager, state) = gate_engine(root, &base, &python);
    std::fs::write(root.join("inbox/rows.csv"), "id,mode,then\n1,gate,reject\n").unwrap();
    manager.resume("gate").unwrap();

    // The transform holds row 1 while a person corrects it: the rejection it
    // then gives is for the old revision and changes nothing.
    wait_for("row 1 never reached the transform", || {
        root.join("state/entered-1").exists()
    })
    .await;
    let one = record_id(&state.db, "gate", 2);
    assert!(
        state
            .db
            .reprocess(one, Some(&json!({"id": "1", "mode": "ok", "then": ""})))
            .unwrap()
    );
    std::fs::write(root.join("state/release-1"), "").unwrap();
    wait_for("row 1 never sent", || status(&state.db, one) == "sent").await;
    let mine = of_record(&events(&state.db), one);
    assert_eq!(kinds(&mine), ["record.retried", "record.delivered"]);
    assert_eq!(
        mine[0]["detail"],
        json!({"from_status": "received", "corrected": true})
    );

    // The server holds row 2's batch while a person puts it back: the delivery
    // it then answers is for the old revision; the new one goes out again.
    let hold = Arc::new(tokio::sync::Notify::new());
    fhir.lock().unwrap().hold = Some(hold.clone());
    let requests = fhir.lock().unwrap().requests.len();
    std::fs::write(root.join("inbox/more.csv"), "id,mode,then\n2,ok,\n").unwrap();
    wait_for("row 2 never sent", || {
        fhir.lock().unwrap().requests.len() > requests
    })
    .await;
    let two = record_id(&state.db, "gate", 2);
    assert_ne!(one, two);
    assert!(state.db.reprocess(two, None).unwrap());
    hold.notify_one();
    wait_for("row 2 never sent again", || {
        fhir.lock().unwrap().writes.get("Observation/obs-2") == Some(&2)
            && status(&state.db, two) == "sent"
    })
    .await;
    let mine = of_record(&events(&state.db), two);
    assert_eq!(kinds(&mine), ["record.retried", "record.delivered"]);
    assert_eq!(mine[0]["detail"]["from_status"], "pending");
    manager.pause("gate", None).unwrap();
}

// ---- the bulk path ------------------------------------------------------------

#[derive(Clone, Default)]
struct Consumer {
    base: Arc<Mutex<String>>,
    /// Kick-offs refuse, echoing what they were given.
    refuse: bool,
    polls: Arc<Mutex<u32>>,
}

fn param(body: &Value, name: &str) -> String {
    body["parameter"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == name)
        .map(|p| {
            p["valueString"]
                .as_str()
                .or(p["valueUrl"].as_str())
                .unwrap()
                .to_string()
        })
        .unwrap()
}

/// The diagnostics of a refusal: the engine's own path with the token, a signed
/// URL on a bracketed IPv6 host, and a path with parentheses.
fn echoed(manifest_url: &str) -> String {
    let path = &manifest_url[manifest_url.find("/bulk/").unwrap()..];
    format!(
        "could not fetch {path} (also tried https://[::1]:8443/b/a.ndjson?X-Amz-Signature=CANARY-V6 \
         and https://h.test/a(CANARY-PAREN)/b?sig=CANARY-SIG); receipts at \
         /receipts/1?X-Amz-Signature=CANARY-RELATIVE, receipts?X-Amz-Signature=CANARY-QUERY \
         and ?sig=CANARY-BARE"
    )
}

async fn consumer_kickoff(State(c): State<Consumer>, Json(body): Json<Value>) -> Response {
    if c.refuse {
        let diagnostics = echoed(&param(&body, "manifestUrl"));
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"resourceType": "OperationOutcome", "issue": [
                {"severity": "error", "code": "processing", "diagnostics": diagnostics}]})),
        )
            .into_response();
    }
    Json(json!({"resourceType": "OperationOutcome", "issue": []})).into_response()
}

async fn consumer_status(State(c): State<Consumer>, Json(body): Json<Value>) -> Response {
    let id = param(&body, "submissionId");
    let base = c.base.lock().unwrap().clone();
    (
        StatusCode::ACCEPTED,
        [("content-location", format!("{base}/status/{id}"))],
    )
        .into_response()
}

/// The first poll fails; the next publishes a receipt for `Patient/a` only.
async fn consumer_poll(State(c): State<Consumer>) -> Response {
    let polls = {
        let mut p = c.polls.lock().unwrap();
        *p += 1;
        *p
    };
    if polls == 1 {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let base = c.base.lock().unwrap().clone();
    Json(
        json!({"output": [{"type": "Patient", "url": format!("{base}/receipts"), "count": 1}],
                "outcome": [], "link": []}),
    )
    .into_response()
}

async fn consumer_server(refuse: bool) -> (String, Consumer) {
    let consumer = Consumer {
        refuse,
        ..Consumer::default()
    };
    let app = Router::new()
        .route("/$bulk-submit", post(consumer_kickoff))
        .route("/$bulk-submit-status", post(consumer_status))
        .route("/status/{id}", get(consumer_poll))
        .route(
            "/receipts",
            get(|| async { "{\"reference\":\"Patient/a\"}\n" }),
        )
        .with_state(consumer.clone());
    let url = serve(app).await;
    *consumer.base.lock().unwrap() = url.clone();
    (url, consumer)
}

fn bulk_source() -> String {
    "id: drops\nname: Drops\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.ndjson\"\n  poll_seconds: 60\n  min_file_age_ms: 0\n\
     format: ndjson\ntransform: transforms/fhir_passthrough.py\n\
     delivery:\n  method: bulk_submit\n  bulk_from: source_objects\n  initial_backoff_ms: 10\n"
        .to_string()
}

fn token(db: &Db) -> String {
    db.query_json("SELECT token FROM submissions", []).unwrap()[0]["token"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn a_submission_logs_its_opening_failed_attempts_receipts_and_settling() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (hfs, _consumer) = consumer_server(false).await;
    let dir = tempfile::tempdir().unwrap();
    let (manager, state) = engine(
        dir.path(),
        &format!("fhir_base_url: {hfs}\npublic_url: http://engine.test\npython: {python}\n"),
        &[("drops", bulk_source())],
    );
    std::fs::write(dir.path().join("inbox/a.ndjson"), patients(&["a", "b"])).unwrap();
    let runner = manager.runner("drops").unwrap();
    // Opened and kicked off; the first poll fails; the next one settles.
    runner.tick().await.unwrap();
    runner.tick().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    runner.tick().await.unwrap();
    let counts = state.db.feed_counts("drops").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["rejected"].as_i64()),
        (Some(1), Some(1))
    );

    let all = events(&state.db);
    assert_eq!(
        kinds(&all),
        [
            "load.received",
            "submission.opened",
            "submission.attempt_failed",
            "record.recovered",
            "record.rejected",
            "submission.settled",
            "load.finished"
        ]
    );
    let token = token(&state.db);
    let run = all[1]["detail"]["run"].as_str().unwrap().to_string();
    assert_eq!(run.len(), 16);
    assert!(!token.contains(&run));
    for e in &all[1..6] {
        assert_eq!(e["detail"]["run"], run.as_str(), "{e}");
        assert_eq!(e["source_id"], "drops");
    }
    assert_eq!(all[1]["detail"]["records"], 2);
    assert_eq!(all[1]["detail"]["files"], 1);
    assert_eq!(all[2]["attempt"], 1);
    assert!(all[2]["reason"].as_str().unwrap().contains("HTTP 500"));
    assert_eq!(all[3]["detail"]["failures"], 1);
    assert!(
        all[4]["reason"]
            .as_str()
            .unwrap()
            .contains("no receipt from HFS")
    );
    assert_eq!(
        (
            &all[5]["detail"]["sent"],
            &all[5]["detail"]["rejected"],
            &all[5]["detail"]["failures"]
        ),
        (&json!(1), &json!(1), &json!(1))
    );
    assert!(!serde_json::to_string(&all).unwrap().contains(&token));

    // A later pass settles nothing twice.
    runner.tick().await.unwrap();
    assert_eq!(events(&state.db).len(), all.len());
}

#[tokio::test]
async fn a_refused_submission_rejects_its_records_and_keeps_what_was_echoed_out() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (hfs, _consumer) = consumer_server(true).await;
    let dir = tempfile::tempdir().unwrap();
    let (manager, state) = engine(
        dir.path(),
        &format!("fhir_base_url: {hfs}\npublic_url: http://engine.test\npython: {python}\n"),
        &[("drops", bulk_source())],
    );
    let api = serve(hie::api::router(state.clone())).await;
    std::fs::write(dir.path().join("inbox/a.ndjson"), patients(&["a", "b"])).unwrap();
    let runner = manager.runner("drops").unwrap();
    runner.tick().await.unwrap();
    runner.tick().await.unwrap();

    let all = events(&state.db);
    assert_eq!(
        kinds(&all),
        [
            "load.received",
            "submission.opened",
            "record.rejected",
            "record.rejected",
            "submission.refused",
            "load.finished"
        ]
    );
    assert_eq!(all[4]["detail"]["records"], 2);
    for e in &all[2..4] {
        assert_eq!(e["error_kind"], "server");
        assert!(e["reason"].as_str().unwrap().contains("HTTP 400"), "{e}");
    }
    let token = token(&state.db);
    let rows = state
        .db
        .query_json("SELECT id, error, response FROM records ORDER BY id", [])
        .unwrap();
    // The server's answer is kept on each record, with its structure, scrubbed.
    for row in &rows {
        assert_eq!(row["response"]["resourceType"], "OperationOutcome");
        let diagnostics = row["response"]["issue"][0]["diagnostics"].as_str().unwrap();
        assert!(
            diagnostics.contains("/bulk/<token>/manifest.json")
                && diagnostics.contains("/receipts/1?…")
                && diagnostics.contains("receipts?…")
                && diagnostics.contains(" ?…")
                && diagnostics.contains("https://[::1]:8443/…")
                && diagnostics.contains("https://h.test/…"),
            "{diagnostics}"
        );
    }

    let http = reqwest::Client::new();
    let mut texts = vec![
        serde_json::to_string(&all).unwrap(),
        serde_json::to_string(&rows).unwrap(),
        http.get(format!("{api}/api/events"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
    ];
    for row in &rows {
        texts.push(
            http.get(format!("{api}/api/records/{}", row["id"]))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
        );
    }
    for text in &texts {
        assert!(!text.contains(&token), "{text}");
        assert!(!text.contains("CANARY"), "{text}");
    }
    assert!(texts[2].contains("submission.refused"));

    // The refusal is written once.
    runner.tick().await.unwrap();
    assert_eq!(events(&state.db).len(), all.len());
}

// ---- the store's contracts ----------------------------------------------------

fn new_file<'a>(feed: &'a str, name: &'a str) -> NewFile<'a> {
    NewFile {
        feed,
        source_name: feed,
        name,
        sha: name,
        size: 1,
        source_key: None,
        origin: None,
    }
}

/// A load of `bad` rows that could not be parsed and `good` rows: one
/// `load.received` and one `record.rejected` per bad row.
fn load(db: &Db, feed: &str, name: &str, bad: usize, good: usize) -> i64 {
    let rows: Vec<_> = (0..bad + good)
        .map(|i| {
            (
                i as i64 + 1,
                json!({"resourceType": "Patient", "id": format!("{name}-{i}")}),
                (i < bad).then(|| "not JSON".to_string()),
            )
        })
        .collect();
    match db.admit_file(&new_file(feed, name), Ok(&rows)).unwrap() {
        Admission::Admitted(id) => id,
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_submission_whose_records_changed_meanwhile_is_not_opened() {
    let db = Db::in_memory().unwrap();
    let file = load(&db, "s", "a", 0, 2);
    let ids: Vec<i64> = db
        .query_json(
            "SELECT id FROM records WHERE file_id = $1 ORDER BY id",
            hie::db::params![file],
        )
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    db.mark_passthrough(&[(ids[0], 0), (ids[1], 0)]).unwrap();
    let before = events(&db).len();
    // The second record was put back and passed through again since it was staged.
    assert!(db.reprocess(ids[1], None).unwrap());
    db.mark_passthrough(&[(ids[1], 1)]).unwrap();
    let opened = db
        .create_submission("t1", "s", "hie-1", &json!({}), &[(ids[0], 0), (ids[1], 0)])
        .unwrap();
    assert!(!opened);
    assert!(
        db.query_json("SELECT token FROM submissions", [])
            .unwrap()
            .is_empty()
    );
    assert_eq!(status(&db, ids[0]), "pending");
    assert_eq!(kinds(&events(&db)[before..]), ["record.retried"]);
    // Built again from what is pending now, it opens over both.
    assert!(
        db.create_submission("t2", "s", "hie-2", &json!({}), &[(ids[0], 0), (ids[1], 1)])
            .unwrap()
    );
    assert_eq!(status(&db, ids[1]), "submitted");
}

#[test]
fn a_retry_that_finds_nothing_to_put_back_changes_nothing() {
    let db = Db::in_memory().unwrap();
    let file = load(&db, "s", "a", 1, 0);
    assert_eq!(db.finish_files("s").unwrap().len(), 1);
    let before = events(&db).len();
    assert_eq!(
        db.reprocess_rejected(file, Some("another reason")).unwrap(),
        0
    );
    assert_eq!(events(&db).len(), before);
    let file_status = db
        .query_json(
            "SELECT status FROM files WHERE id = $1",
            hie::db::params![file],
        )
        .unwrap();
    assert_eq!(file_status[0]["status"], "done");
    // With rows to put back: the load is retried, and finishes again.
    assert_eq!(db.reprocess_rejected(file, None).unwrap(), 1);
    let retried = &events(&db)[before..];
    assert_eq!(kinds(retried), ["load.retried", "record.retried"]);
    assert_eq!(retried[0]["detail"]["retried"], 1);
    assert_eq!(
        retried[1]["detail"],
        json!({"from_status": "rejected", "corrected": false})
    );
}

// ---- the API ------------------------------------------------------------------

async fn get_json(url: &str) -> (u16, Value) {
    let r = reqwest::get(url).await.unwrap();
    (r.status().as_u16(), r.json().await.unwrap())
}

fn ids(page: &Value) -> Vec<i64> {
    page["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_i64().unwrap())
        .collect()
}

#[tokio::test]
async fn events_are_paged_in_order_and_a_cursor_only_moves_forward() {
    let dir = tempfile::tempdir().unwrap();
    let (_manager, state) = engine(
        dir.path(),
        "fhir_base_url: http://127.0.0.1:9\nretention:\n  history: 3650d\n",
        &[],
    );
    let db = state.db.clone();
    let api = serve(hie::api::router(state.clone())).await;
    let (_, empty) = get_json(&format!("{api}/api/events")).await;
    assert_eq!(
        empty,
        json!({"events": [], "next_cursor": null, "has_more": false})
    );

    // Six events over two sources: a's load and two rejections, b's duplicate,
    // b's load and one rejection.
    let a = load(&db, "a", "a1", 2, 1);
    db.insert_duplicate("b", "b", "copy", "a1", 1, a).unwrap();
    load(&db, "b", "b1", 1, 0);

    let mut cursor = String::new();
    let mut seen = Vec::new();
    loop {
        let url = if cursor.is_empty() {
            format!("{api}/api/events?limit=2")
        } else {
            format!("{api}/api/events?limit=2&cursor={cursor}")
        };
        let (code, page) = get_json(&url).await;
        assert_eq!(code, 200, "{page}");
        seen.extend(ids(&page));
        cursor = page["next_cursor"].as_str().unwrap().to_string();
        if !page["has_more"].as_bool().unwrap() {
            break;
        }
    }
    let all: Vec<i64> = events(&db)
        .iter()
        .map(|e| e["id"].as_i64().unwrap())
        .collect();
    assert_eq!(seen, all);
    assert!(seen.windows(2).all(|w| w[0] < w[1]));
    assert!(cursor.starts_with("e1."), "{cursor}");

    // One page in full: every field, the detail as an object, the time as an instant.
    let (_, first) = get_json(&format!("{api}/api/events?limit=1")).await;
    let e = &first["events"][0];
    assert_eq!(e["kind"], "load.received");
    assert_eq!(e["source_id"], "a");
    assert_eq!(e["load_id"], a);
    assert_eq!(e["detail"]["records"], 3);
    assert!(e["at"].as_str().unwrap().ends_with('Z'), "{e}");
    for key in ["record_id", "attempt", "error_kind", "reason"] {
        assert!(e.get(key).is_some_and(Value::is_null), "{key}: {e}");
    }

    // Nothing new: the same cursor back. Something new: only that.
    let (_, page) = get_json(&format!("{api}/api/events?cursor={cursor}")).await;
    assert_eq!(
        (ids(&page), page["next_cursor"].as_str()),
        (vec![], Some(cursor.as_str()))
    );
    let record = record_id(&db, "a", 3);
    db.mark_rejected(record, "data", "corrected later", None)
        .unwrap();
    let (_, page) = get_json(&format!("{api}/api/events?cursor={cursor}")).await;
    let newest = ids(&page);
    assert_eq!(newest.len(), 1);
    let cursor = page["next_cursor"].as_str().unwrap().to_string();

    // A filtered poll that finds nothing still moves forward, and a later
    // matching event is returned from there.
    load(&db, "a", "a2", 0, 1);
    let (_, page) = get_json(&format!("{api}/api/events?source=b&cursor={cursor}")).await;
    assert!(ids(&page).is_empty());
    let moved = page["next_cursor"].as_str().unwrap().to_string();
    assert_ne!(moved, cursor);
    load(&db, "b", "b2", 0, 1);
    let (_, page) = get_json(&format!("{api}/api/events?source=b&cursor={moved}")).await;
    assert_eq!(ids(&page).len(), 1);
    assert_eq!(page["events"][0]["detail"]["name"], "b2");

    // Filters, alone and together.
    let count = |q: &str| {
        let url = format!("{api}/api/events?{q}");
        async move { ids(&get_json(&url).await.1).len() }
    };
    assert_eq!(count("kind=load").await, 5);
    assert_eq!(count("kind=record.rejected,load.duplicate").await, 5);
    assert_eq!(count("kind=submission").await, 0);
    assert_eq!(count("source=a&kind=record").await, 3);
    assert_eq!(count("source=a&kind=load.received").await, 2);

    // `since`: at or after the instant, by id. An event exactly at it counts.
    let all = events(&db);
    for (i, e) in all.iter().enumerate() {
        db.query_json(
            "UPDATE events SET at = CAST($1 AS BIGINT) WHERE id = CAST($2 AS BIGINT)",
            hie::db::params![
                1_800_000_000_000i64 + i as i64 * 1000,
                e["id"].as_i64().unwrap()
            ],
        )
        .unwrap();
    }
    let since = "2027-01-15T08:00:03Z"; // the fourth event's instant
    let (_, page) = get_json(&format!("{api}/api/events?since={since}")).await;
    let from: Vec<i64> = all[3..].iter().map(|e| e["id"].as_i64().unwrap()).collect();
    assert_eq!(ids(&page), from);
    let (_, page) = get_json(&format!(
        "{api}/api/events?since={since}&cursor=e1.{}",
        from[1]
    ))
    .await;
    assert_eq!(ids(&page), &from[2..]);
    // A finer instant starts at the next millisecond: the event at :03.000 is
    // before it.
    let (_, page) = get_json(&format!("{api}/api/events?since=2027-01-15T08:00:03.0001Z")).await;
    assert_eq!(ids(&page), &from[1..]);
    let (_, page) = get_json(&format!(
        "{api}/api/events?since=2027-01-15T08:00:03.0000000001Z"
    ))
    .await;
    assert_eq!(
        ids(&page),
        &from[1..],
        "digits past the nanosecond count too"
    );
    let (_, page) = get_json(&format!("{api}/api/events?since=2027-01-15T08:00:03.000Z")).await;
    assert_eq!(ids(&page), from);
    let (_, page) = get_json(&format!("{api}/api/events?since=2030-01-01T00:00:00Z")).await;
    assert!(ids(&page).is_empty());
    let last = format!("e1.{}", all.last().unwrap()["id"]);
    assert_eq!(page["next_cursor"], last.as_str());

    // Every event purged: a cursor is a position and stays where it was; one
    // past the log does not move back.
    db.query_json("UPDATE events SET at = 0", []).unwrap();
    hie::retention::purge(&db, 1).unwrap();
    let (_, page) = get_json(&format!("{api}/api/events?cursor={last}")).await;
    assert_eq!(page["next_cursor"], last.as_str());
    let (_, page) = get_json(&format!("{api}/api/events")).await;
    assert_eq!(page["next_cursor"], Value::Null);
    let (_, page) = get_json(&format!("{api}/api/events?cursor=e1.99999")).await;
    assert_eq!(page["next_cursor"], "e1.99999");
}

#[tokio::test]
async fn an_events_query_that_cannot_be_read_is_refused_naming_the_parameter() {
    let dir = tempfile::tempdir().unwrap();
    let (_manager, state) = engine(dir.path(), "fhir_base_url: http://127.0.0.1:9\n", &[]);
    let api = serve(hie::api::router(state)).await;
    for (query, field) in [
        ("kind=records", "kind"),
        ("kind=load,", "kind"),
        ("kind=load.archived", "kind"),
        ("since=yesterday", "since"),
        ("since=2026-11-20", "since"),
        ("cursor=v1.3", "cursor"),
        ("cursor=e1.", "cursor"),
        ("cursor=e1.-3", "cursor"),
        ("cursor=e1.3x", "cursor"),
        ("limit=0", "limit"),
        ("limit=1001", "limit"),
        ("limit=ten", "limit"),
        ("limit=+5", "limit"),
        ("source=", "source"),
        ("limit=1&limit=2", "limit"),
        ("other=1", "other"),
        ("limit=10&Kind=load", "Kind"),
    ] {
        let (code, body) = get_json(&format!("{api}/api/events?{query}")).await;
        assert_eq!(code, 400, "{query}: {body}");
        assert_eq!(body["code"], "invalid_query", "{query}: {body}");
        assert_eq!(body["field"], field, "{query}: {body}");
        assert!(body["error"].is_string(), "{query}: {body}");
    }
    let (code, _) = get_json(&format!(
        "{api}/api/events?limit=1000&kind=record,load,record"
    ))
    .await;
    assert_eq!(code, 200);
}

// ---- order across writers on PostgreSQL ---------------------------------------

/// Waits until some session is blocked by the session `pid`, as seen from a
/// connection of its own.
fn wait_until_blocked_by(pid: i32) {
    let mut monitor =
        postgres::Client::connect(&test_database_url().unwrap(), postgres::NoTls).unwrap();
    let started = std::time::Instant::now();
    loop {
        let waiting: i64 = monitor
            .query_one(
                "SELECT COUNT(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
                &[&pid],
            )
            .unwrap()
            .get(0);
        if waiting > 0 {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "nothing waits for session {pid}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn backend_pid(client: &mut postgres::Client) -> i32 {
    client
        .query_one("SELECT pg_backend_pid()", &[])
        .unwrap()
        .get(0)
}

/// A raw client on the store's schema, as another instance's transaction.
fn raw_client(db: &Db) -> Option<postgres::Client> {
    let url = test_database_url()?;
    let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    client
        .batch_execute(&format!(
            "SET search_path TO {}",
            db.schema().unwrap().unwrap()
        ))
        .unwrap();
    Some(client)
}

#[test]
fn an_event_committed_later_with_a_lower_id_is_never_skipped_on_postgres() {
    if test_database_url().is_none() {
        eprintln!("skipping: HIE_TEST_DATABASE_URL is not set");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hie.db");
    let db = Arc::new(Db::open(&path).unwrap());
    let reader = Db::open(&path).unwrap();
    let file = load(&db, "s", "a", 0, 1);
    let record = record_id(&db, "s", 1);
    let start = reader
        .list_events(&EventQuery {
            limit: 100,
            ..Default::default()
        })
        .unwrap()
        .next;

    // Another writer holds the event lock with an event not committed yet.
    let mut other = raw_client(&db).unwrap();
    other.batch_execute("BEGIN").unwrap();
    other
        .execute(
            "SELECT pg_advisory_xact_lock(hashtext('hie.events'), hashtext(current_schema()))",
            &[],
        )
        .unwrap();
    let first: i64 = other
        .query_one(
            "INSERT INTO events (at, source_id, kind, load_id) VALUES ($1, 's', 'load.finished', $2) RETURNING id",
            &[&hie::db::now_ms(), &file],
        )
        .unwrap()
        .get(0);
    let writer = {
        let db = db.clone();
        std::thread::spawn(move || db.mark_rejected(record, "data", "decided meanwhile", None))
    };
    wait_until_blocked_by(backend_pid(&mut other));
    assert!(
        !writer.is_finished(),
        "the engine's event waits for the lock"
    );
    let during = reader
        .list_events(&EventQuery {
            cursor: start,
            limit: 100,
            ..Default::default()
        })
        .unwrap();
    assert!(during.events.is_empty(), "{:?}", during.events);

    other.batch_execute("COMMIT").unwrap();
    writer.join().unwrap().unwrap();
    let after = reader
        .list_events(&EventQuery {
            cursor: during.next,
            limit: 100,
            ..Default::default()
        })
        .unwrap();
    let got: Vec<i64> = after
        .events
        .iter()
        .map(|e| e["id"].as_i64().unwrap())
        .collect();
    assert_eq!(got.len(), 2, "{got:?}");
    assert_eq!(got[0], first);
    assert!(got[1] > first);
}

#[test]
fn a_finish_that_waits_for_a_retry_sees_the_rows_put_back_on_postgres() {
    if test_database_url().is_none() {
        eprintln!("skipping: HIE_TEST_DATABASE_URL is not set");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(&dir.path().join("hie.db")).unwrap());
    // Every row rejected: the load can finish.
    let file = load(&db, "s", "a", 2, 0);
    let before = events(&db).len();

    // A Retry on another instance: the file's row locked, its rows put back,
    // not committed yet.
    let mut other = raw_client(&db).unwrap();
    other.batch_execute("BEGIN").unwrap();
    other
        .query("SELECT id FROM files WHERE id = $1 FOR UPDATE", &[&file])
        .unwrap();
    other
        .execute(
            "UPDATE records SET status = 'received', revision = revision + 1 WHERE file_id = $1 AND status = 'rejected'",
            &[&file],
        )
        .unwrap();
    let finisher = {
        let db = db.clone();
        std::thread::spawn(move || db.finish_files("s"))
    };
    wait_until_blocked_by(backend_pid(&mut other));
    assert!(
        !finisher.is_finished(),
        "the finish waits for the file's row"
    );
    other
        .execute(
            "UPDATE files SET status = 'processing', finished_at = NULL WHERE id = $1",
            &[&file],
        )
        .unwrap();
    other.batch_execute("COMMIT").unwrap();

    assert!(finisher.join().unwrap().unwrap().is_empty());
    let row = db
        .query_json(
            "SELECT status FROM files WHERE id = $1",
            hie::db::params![file],
        )
        .unwrap();
    assert_eq!(row[0]["status"], "processing");
    assert_eq!(events(&db).len(), before, "no load.finished");
}

#[test]
fn an_alert_cleared_while_the_purge_waits_for_it_is_kept_on_postgres() {
    if test_database_url().is_none() {
        eprintln!("skipping: HIE_TEST_DATABASE_URL is not set");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(&dir.path().join("hie.db")).unwrap());
    let day = 86_400_000i64;
    let now = hie::db::now_ms();
    // Open and without a rule: purged by age (an outage alert would be kept).
    let open = db
        .insert_alert(Some("s"), "unreadable_file", "m", None)
        .unwrap();
    let cleared = db.insert_alert(Some("s"), "delivery", "m", None).unwrap();
    db.clear_alert(cleared, "done").unwrap();
    let newest = db.insert_alert(Some("s"), "delivery", "m", None).unwrap();
    db.query_json(
        "UPDATE alerts SET created_at = CAST($1 AS BIGINT),
                cleared_at = CASE WHEN cleared_at IS NULL THEN NULL ELSE CAST($1 AS BIGINT) END",
        hie::db::params![now - 40 * day],
    )
    .unwrap();

    // Another instance clears the old open alert, not committed yet.
    let mut other = raw_client(&db).unwrap();
    other.batch_execute("BEGIN").unwrap();
    other
        .execute(
            "UPDATE alerts SET cleared_at = $1, cleared_reason = 'data arrived' WHERE id = $2",
            &[&now, &open],
        )
        .unwrap();
    let purger = {
        let db = db.clone();
        std::thread::spawn(move || hie::retention::purge(&db, now - 30 * day))
    };
    wait_until_blocked_by(backend_pid(&mut other));
    other.batch_execute("COMMIT").unwrap();

    // Read again once free, the alert cleared just now is not old any more.
    let purged = purger.join().unwrap().unwrap();
    assert_eq!(purged.alerts, 1);
    assert_eq!(alert_ids(&db), [open, newest]);
}

/// A purge of checks that waits for another instance's purge chooses the next
/// old rows once free, instead of counting none and stopping with old checks left.
#[test]
fn a_check_purge_waiting_for_another_moves_on_to_the_next_rows_on_postgres() {
    if test_database_url().is_none() {
        eprintln!("skipping: HIE_TEST_DATABASE_URL is not set");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(&dir.path().join("hie.db")).unwrap());
    let day = 86_400_000i64;
    let now = hie::db::now_ms();
    let first = db.insert_check("s").unwrap();
    let second = db.insert_check("s").unwrap();
    db.query_json(
        "UPDATE checks SET requested_at = CAST($1 AS BIGINT)",
        hie::db::params![now - 40 * day],
    )
    .unwrap();

    // Another instance's purge holds the first, not committed yet.
    let mut other = raw_client(&db).unwrap();
    other.batch_execute("BEGIN").unwrap();
    other
        .execute("DELETE FROM checks WHERE id = $1", &[&first])
        .unwrap();
    let purger = {
        let db = db.clone();
        std::thread::spawn(move || db.purge_checks(now - 30 * day, 1))
    };
    wait_until_blocked_by(backend_pid(&mut other));
    other.batch_execute("COMMIT").unwrap();

    assert_eq!(purger.join().unwrap().unwrap(), 1);
    assert!(db.check(second, 0, 10).unwrap().is_none());
}

#[test]
fn set_writes_over_more_than_one_chunk_change_and_log_every_row_once() {
    let db = Db::in_memory().unwrap();
    let ids_of = |file: i64| -> Vec<i64> {
        db.query_json(
            "SELECT id FROM records WHERE file_id = $1 ORDER BY id",
            hie::db::params![file],
        )
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect()
    };
    let logged = |kind: &str, from: usize| -> Vec<i64> {
        let mut ids: Vec<i64> = events(&db)[from..]
            .iter()
            .filter(|e| e["kind"] == kind)
            .map(|e| e["record_id"].as_i64().unwrap())
            .collect();
        ids.sort_unstable();
        ids
    };

    // A Retry of 450 rejected rows.
    let file = load(&db, "s", "retried", 450, 0);
    let before = events(&db).len();
    assert_eq!(db.reprocess_rejected(file, None).unwrap(), 450);
    assert_eq!(logged("record.retried", before), ids_of(file));
    assert_eq!(events(&db)[before]["detail"]["retried"], 450);

    // 450 pending rows of a file that cannot be handed over.
    let file = load(&db, "s", "unsubmitted", 0, 450);
    let ids = ids_of(file);
    db.mark_passthrough(&ids.iter().map(|id| (*id, 0)).collect::<Vec<_>>())
        .unwrap();
    let before = events(&db).len();
    assert_eq!(
        db.reject_unsubmitted(file, "server", "cannot be handed over")
            .unwrap(),
        450
    );
    assert_eq!(logged("record.rejected", before), ids);

    // A refused submission of 450 rows.
    let file = load(&db, "s", "refused", 0, 450);
    let ids = ids_of(file);
    db.mark_passthrough(&ids.iter().map(|id| (*id, 0)).collect::<Vec<_>>())
        .unwrap();
    assert!(
        db.create_submission_for_files("t", "s", "sub", &json!({}), &[(file, 450)])
            .unwrap()
    );
    let before = events(&db).len();
    assert_eq!(
        db.reject_submission("t", 0, "server", "HTTP 400", &json!({}))
            .unwrap(),
        Some(450)
    );
    assert_eq!(logged("record.rejected", before), ids);
    let refused = events(&db).pop().unwrap();
    assert_eq!(refused["kind"], "submission.refused");
    assert_eq!(refused["detail"]["records"], 450);
    let statuses = db
        .query_json(
            "SELECT DISTINCT status FROM records WHERE file_id = $1",
            hie::db::params![file],
        )
        .unwrap();
    assert_eq!(statuses, [json!({"status": "rejected"})]);
}

#[test]
fn a_record_put_back_while_its_file_is_rejected_stays_put_back_on_postgres() {
    if test_database_url().is_none() {
        eprintln!("skipping: HIE_TEST_DATABASE_URL is not set");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(&dir.path().join("hie.db")).unwrap());
    let file = load(&db, "s", "a", 0, 2);
    let (held, other_id) = (record_id(&db, "s", 1), record_id(&db, "s", 2));
    db.mark_passthrough(&[(held, 0), (other_id, 0)]).unwrap();

    // A person's reprocess on another instance, not committed yet.
    let mut other = raw_client(&db).unwrap();
    other.batch_execute("BEGIN").unwrap();
    other
        .execute(
            "UPDATE records SET status = 'received', revision = revision + 1 WHERE id = $1",
            &[&held],
        )
        .unwrap();
    let rejecting = {
        let db = db.clone();
        std::thread::spawn(move || db.reject_unsubmitted(file, "server", "cannot be handed over"))
    };
    wait_until_blocked_by(backend_pid(&mut other));
    other.batch_execute("COMMIT").unwrap();

    assert_eq!(rejecting.join().unwrap().unwrap(), 1);
    assert_eq!(status(&db, held), "received");
    assert_eq!(status(&db, other_id), "rejected");
    assert!(of_record(&events(&db), held).is_empty());
    assert_eq!(
        kinds(&of_record(&events(&db), other_id)),
        ["record.rejected"]
    );
}

#[test]
fn a_source_object_submission_is_not_opened_when_a_file_changed_meanwhile() {
    let db = Db::in_memory().unwrap();
    let file = load(&db, "s", "a", 0, 2);
    let ids: Vec<i64> = db
        .query_json(
            "SELECT id FROM records WHERE file_id = $1 ORDER BY id",
            hie::db::params![file],
        )
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    db.mark_passthrough(&[(ids[0], 0), (ids[1], 0)]).unwrap();
    let before = events(&db).len();
    // The manifest was built for three records; two are pending now.
    assert!(
        !db.create_submission_for_files("t1", "s", "sub-1", &json!({}), &[(file, 3)])
            .unwrap()
    );
    assert!(
        db.query_json("SELECT token FROM submissions", [])
            .unwrap()
            .is_empty()
    );
    assert_eq!(status(&db, ids[0]), "pending");
    assert_eq!(events(&db).len(), before);
    assert!(
        db.create_submission_for_files("t2", "s", "sub-2", &json!({}), &[(file, 2)])
            .unwrap()
    );
    assert_eq!(status(&db, ids[1]), "submitted");
    let opened = events(&db).pop().unwrap();
    assert_eq!(opened["kind"], "submission.opened");
    assert_eq!(opened["detail"]["records"], 2);
}

#[test]
fn an_older_answer_cannot_roll_back_a_records_attempts() {
    let db = Db::in_memory().unwrap();
    let file = load(&db, "s", "a", 0, 1);
    let id = record_id(&db, "s", 1);
    db.mark_passthrough(&[(id, 0)]).unwrap();
    let selected = db.due_records("s", 10).unwrap().remove(0);
    let deferred = Delivery::Deferred {
        attempts: 1,
        next_attempt_at: 0,
        error: "HTTP 503",
    };
    db.delivered(&[(&selected, deferred)]).unwrap();
    // The same selection answered again, as a runner that held it before would.
    db.delivered(&[(&selected, deferred)]).unwrap();
    db.delivered(&[(&selected, Delivery::Sent(&json!([])))])
        .unwrap();
    let row = db
        .query_json(
            "SELECT status, attempts FROM records WHERE id = $1",
            hie::db::params![id],
        )
        .unwrap();
    assert_eq!(
        (&row[0]["status"], &row[0]["attempts"]),
        (&json!("pending"), &json!(1))
    );
    let mine = of_record(&events(&db), id);
    assert_eq!(kinds(&mine), ["record.attempt_failed"]);

    // Selected again, it goes through as the next attempt.
    let selected = db.due_records("s", 10).unwrap().remove(0);
    db.delivered(&[(&selected, Delivery::Sent(&json!([])))])
        .unwrap();
    let mine = of_record(&events(&db), id);
    assert_eq!(kinds(&mine), ["record.attempt_failed", "record.recovered"]);
    assert_eq!(mine[1]["attempt"], 2);
    assert_eq!(mine[1]["load_id"], file);
}

#[test]
fn content_taken_meanwhile_is_not_admitted_twice() {
    let db = Db::in_memory().unwrap();
    let first = load(&db, "s", "a", 0, 1);
    let before = events(&db).len();
    let rows = [(1, json!({"resourceType": "Patient", "id": "x"}), None)];
    let again = db
        .admit_file(
            &NewFile {
                sha: "a",
                ..new_file("s", "copy-of-a")
            },
            Ok(&rows),
        )
        .unwrap();
    assert_eq!(again, Admission::AlreadyTaken(first));
    let files = db.query_json("SELECT id FROM files", []).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(events(&db).len(), before);
}

// ---- retention ----------------------------------------------------------------

fn rule(source: &str, enabled: bool) -> Rule {
    serde_json::from_value(
        json!({"source_id": source, "condition": "silence", "threshold": "1h",
        "channels": ["app"], "enabled": enabled}),
    )
    .unwrap()
}

async fn alert_totals(api: &str) -> Vec<String> {
    let text = reqwest::get(format!("{api}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    text.lines()
        .filter(|l| l.starts_with("hie_alerts_total{"))
        .map(str::to_string)
        .collect()
}

fn alert_ids(db: &Db) -> Vec<i64> {
    db.query_json("SELECT id FROM alerts ORDER BY id", [])
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_history_is_purged_except_what_the_engine_still_tracks() {
    let dir = tempfile::tempdir().unwrap();
    // The engine's own purge keeps ten years; this test purges at 30 days.
    let (_manager, state) = engine(
        dir.path(),
        "fhir_base_url: http://127.0.0.1:9\nretention:\n  history: 3650d\n",
        &[],
    );
    let db = state.db.clone();
    let api = serve(hie::api::router(state.clone())).await;
    let day = 86_400_000i64;
    let now = hie::db::now_ms();
    let old = now - 40 * day;
    let cutoff = hie::retention::Retention::from_config(&hie::config::RetentionConfig::default())
        .cutoff(now);

    // Alerts: what stays open while the engine tracks it, and what does not.
    let enabled = Rule {
        id: db.insert_rule(&rule("s", true)).unwrap(),
        ..rule("s", true)
    };
    let disabled = Rule {
        id: db.insert_rule(&rule("s", false)).unwrap(),
        ..rule("s", false)
    };
    let deleted = Rule {
        id: db.insert_rule(&rule("s", true)).unwrap(),
        ..rule("s", true)
    };
    let raise =
        |kind: &str, rule: Option<&Rule>| db.insert_alert(Some("s"), kind, "m", rule).unwrap();
    let open_enabled = raise("silence", Some(&enabled));
    let credential = raise("credential", None);
    let definition = raise("definition", None);
    let open_disabled = raise("silence", Some(&disabled));
    let open_deleted = raise("silence", Some(&deleted));
    let open_no_rule = raise("unreadable_file", None);
    // An outage still going on: what stops a second alert for it.
    let open_outage = raise("delivery", None);
    let cleared_old = raise("delivery_recovered", None);
    db.clear_alert(cleared_old, "done").unwrap();
    let cleared_recent = db.insert_alert(None, "listener", "m", None).unwrap();
    db.clear_alert(cleared_recent, "done").unwrap();
    let highest = raise("delivery", None);
    db.clear_alert(highest, "done").unwrap();
    db.delete_rule(deleted.id).unwrap();
    db.query_json(
        "UPDATE alerts SET created_at = CAST($1 AS BIGINT),
                cleared_at = CASE WHEN cleared_at IS NULL THEN NULL ELSE CAST($1 AS BIGINT) END
         WHERE id <> CAST($2 AS BIGINT)",
        hie::db::params![old, cleared_recent],
    )
    .unwrap();
    // The rules were there before their alerts.
    db.query_json(
        "UPDATE alert_rules SET created_at = CAST($1 AS BIGINT)",
        hie::db::params![old - day],
    )
    .unwrap();

    // Events: more than one chunk of them old, one recent.
    load(&db, "s", "big", 5_001, 0);
    db.query_json(
        "UPDATE events SET at = CAST($1 AS BIGINT)",
        hie::db::params![old],
    )
    .unwrap();
    load(&db, "s", "recent", 0, 1);
    let total = events(&db).len();
    assert_eq!(total, 5_003);

    // Two purges at once, on two connections: each row is counted once.
    let totals = alert_totals(&api).await;
    let (config, _) = hie::config::load(&dir.path().join("hie.yaml")).unwrap();
    let other = Arc::new(Db::open(&config.database).unwrap());
    let purges: Vec<_> = [db.clone(), other.clone()]
        .into_iter()
        .map(|store| std::thread::spawn(move || hie::retention::purge(&store, cutoff).unwrap()))
        .collect();
    let purged: Vec<_> = purges.into_iter().map(|p| p.join().unwrap()).collect();
    assert_eq!(purged.iter().map(|p| p.events).sum::<usize>(), 5_002);
    assert_eq!(purged.iter().map(|p| p.alerts).sum::<usize>(), 4);
    assert_eq!(events(&db).len(), 1);
    assert_eq!(
        alert_ids(&db),
        [
            open_enabled,
            credential,
            definition,
            open_outage,
            cleared_recent,
            highest
        ]
    );
    assert_eq!(
        alert_totals(&api).await,
        totals,
        "the counter never goes down"
    );
    for gone in [open_disabled, open_deleted, open_no_rule, cleared_old] {
        assert!(!alert_ids(&db).contains(&gone));
    }

    // Everything old, every alert cleared: all go but the one with the highest
    // id, and the next alert gets an id above every one purged.
    for id in [open_enabled, credential, definition, open_outage] {
        db.clear_alert(id, "done").unwrap();
    }
    db.query_json(
        "UPDATE alerts SET created_at = CAST($1 AS BIGINT), cleared_at = CAST($1 AS BIGINT)",
        hie::db::params![old],
    )
    .unwrap();
    db.query_json(
        "UPDATE events SET at = CAST($1 AS BIGINT)",
        hie::db::params![old],
    )
    .unwrap();
    let last_event = events(&db)[0]["id"].as_i64().unwrap();
    let purged = hie::retention::purge(&db, cutoff).unwrap();
    assert_eq!((purged.events, purged.alerts), (1, 5));
    assert_eq!(alert_ids(&db), [highest]);
    assert_eq!(alert_totals(&api).await, totals);
    let next_alert = db.insert_alert(Some("s"), "delivery", "m", None).unwrap();
    assert!(next_alert > highest);
    load(&db, "s", "after", 0, 1);
    assert!(events(&db)[0]["id"].as_i64().unwrap() > last_event);
    drop(other);
}

/// An engine and its API, which `stop` shuts down for good.
struct Running {
    manager: Arc<hie::manager::SourceManager>,
    state: hie::api::AppState,
    api: String,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
}

async fn start_engine(root: &Path, yaml: &str) -> Running {
    let (manager, state) = engine(root, yaml, &[]);
    let (listener, api) = bind().await;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let router = hie::api::router(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap()
    });
    Running {
        manager,
        state,
        api,
        stop,
        server,
    }
}

/// Shuts the API down and drops the engine, until nothing holds its store.
async fn stop_engine(running: Running) {
    let Running {
        manager,
        state,
        stop,
        server,
        ..
    } = running;
    let _ = stop.send(());
    server.await.unwrap();
    let store = Arc::downgrade(&state.db);
    drop((manager, state));
    wait_for("the store is still held after the engine stopped", || {
        store.upgrade().is_none()
    })
    .await;
}

#[test]
fn an_open_alert_is_not_kept_by_a_rule_made_after_it() {
    let db = Db::in_memory().unwrap();
    let day = 86_400_000i64;
    let now = hie::db::now_ms();
    let first = Rule {
        id: db.insert_rule(&rule("s", true)).unwrap(),
        ..rule("s", true)
    };
    let open = db
        .insert_alert(Some("s"), "silence", "m", Some(&first))
        .unwrap();
    let newest = db.insert_alert(Some("s"), "delivery", "m", None).unwrap();
    db.query_json(
        "UPDATE alerts SET created_at = CAST($1 AS BIGINT)",
        hie::db::params![now - 40 * day],
    )
    .unwrap();
    // The rule goes; a new one is made, and the old alert names it, as it
    // would on a store that gave a deleted rule's id again.
    db.delete_rule(first.id).unwrap();
    let second = db.insert_rule(&rule("s", true)).unwrap();
    db.query_json(
        "UPDATE alerts SET rule_id = CAST($1 AS BIGINT) WHERE id = CAST($2 AS BIGINT)",
        hie::db::params![second, open],
    )
    .unwrap();
    let purged = hie::retention::purge(&db, now - 30 * day).unwrap();
    assert_eq!(purged.alerts, 1);
    assert_eq!(alert_ids(&db), [newest]);
    assert!(!alert_ids(&db).contains(&open));
}

#[test]
fn an_older_poll_answer_cannot_move_a_submission() {
    let db = Db::in_memory().unwrap();
    let file = load(&db, "s", "a", 0, 1);
    db.mark_passthrough(&[(record_id(&db, "s", 1), 0)]).unwrap();
    assert!(
        db.create_submission_for_files("t", "s", "sub", &json!({}), &[(file, 1)])
            .unwrap()
    );
    // A poll failed once: the submission is at attempt 1.
    assert!(db.submission_attempt_failed("t", 0, 0, "HTTP 503").unwrap());
    // Answers to requests made at attempt 0 change nothing.
    assert!(!db.submission_attempt_failed("t", 0, 0, "HTTP 503").unwrap());
    assert!(
        !db.progress_submission("t", Some(0), "running", None, 0, 0, None)
            .unwrap()
    );
    let row = db
        .query_json("SELECT status, attempts, failures FROM submissions", [])
        .unwrap();
    assert_eq!(
        row[0],
        json!({"status": "kickoff", "attempts": 1, "failures": 1})
    );
    let failed = events(&db)
        .iter()
        .filter(|e| e["kind"] == "submission.attempt_failed")
        .count();
    assert_eq!(failed, 1);
    assert!(
        db.progress_submission("t", Some(1), "running", None, 0, 0, None)
            .unwrap()
    );
}

/// A FHIR server whose status poll is held until released, then answers as
/// `answer` says: a refusal, or done with no receipts.
#[derive(Clone)]
struct HeldPoll {
    base: Arc<Mutex<String>>,
    polled: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    refuse: bool,
}

async fn held_poll_server(refuse: bool) -> (String, HeldPoll) {
    let held = HeldPoll {
        base: Arc::default(),
        polled: Arc::default(),
        release: Arc::default(),
        refuse,
    };
    let app = Router::new()
        .route(
            "/$bulk-submit",
            post(|| async { Json(json!({"resourceType": "OperationOutcome", "issue": []})) }),
        )
        .route(
            "/$bulk-submit-status",
            post(|State(h): State<HeldPoll>| async move {
                let base = h.base.lock().unwrap().clone();
                (
                    StatusCode::ACCEPTED,
                    [("content-location", format!("{base}/status/1"))],
                )
            }),
        )
        .route(
            "/status/1",
            get(|State(h): State<HeldPoll>| async move {
                h.polled.notify_one();
                h.release.notified().await;
                if h.refuse {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"resourceType": "OperationOutcome", "issue": []})),
                    )
                        .into_response()
                } else {
                    Json(json!({"output": [], "outcome": [], "link": []})).into_response()
                }
            }),
        )
        .with_state(held.clone());
    let url = serve(app).await;
    *held.base.lock().unwrap() = url.clone();
    (url, held)
}

/// A poll made at attempt 0 is held; meanwhile a newer poll fails (attempt 1);
/// then the old answer comes back: it changes nothing, writes no event, and
/// archives nothing. The current runner's own next poll decides.
async fn an_old_terminal_answer_changes_nothing(refuse: bool) {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (hfs, held) = held_poll_server(refuse).await;
    let dir = tempfile::tempdir().unwrap();
    let (manager, state) = engine(
        dir.path(),
        &format!("fhir_base_url: {hfs}\npublic_url: http://engine.test\npython: {python}\n"),
        &[("drops", bulk_source())],
    );
    std::fs::write(dir.path().join("inbox/a.ndjson"), patients(&["a"])).unwrap();
    let runner = manager.runner("drops").unwrap();
    // Opened and kicked off, at attempt 0.
    runner.tick().await.unwrap();
    let db = state.db.clone();
    let token = token(&db);
    let before = events(&db).len();

    let polling = tokio::spawn({
        let runner = runner.clone();
        async move { runner.tick().await }
    });
    held.polled.notified().await;
    assert!(
        db.submission_attempt_failed(&token, 0, 0, "HTTP 503")
            .unwrap()
    );
    held.release.notify_one();
    polling.await.unwrap().unwrap();

    let sub = db
        .query_json("SELECT status, attempts FROM submissions", [])
        .unwrap();
    assert_eq!(sub[0], json!({"status": "running", "attempts": 1}));
    assert_eq!(db.feed_counts("drops").unwrap()["submitted"], 1);
    let after: Vec<String> = kinds(&events(&db)[before..]);
    assert_eq!(
        after,
        ["submission.attempt_failed"],
        "only the newer failure"
    );
    assert!(
        dir.path().join("inbox/a.ndjson").exists(),
        "nothing archived for an old answer"
    );
}

#[tokio::test]
async fn an_old_refusal_after_a_newer_failure_changes_nothing() {
    an_old_terminal_answer_changes_nothing(true).await;
}

#[tokio::test]
async fn an_old_empty_settlement_after_a_newer_failure_changes_nothing() {
    an_old_terminal_answer_changes_nothing(false).await;
}

#[test]
fn an_older_answer_cannot_settle_or_refuse_a_submission() {
    let db = Db::in_memory().unwrap();
    let file = load(&db, "s", "a", 0, 1);
    let record = record_id(&db, "s", 1);
    db.mark_passthrough(&[(record, 0)]).unwrap();
    assert!(
        db.create_submission_for_files("t", "s", "sub", &json!({}), &[(file, 1)])
            .unwrap()
    );
    assert!(db.submission_attempt_failed("t", 0, 0, "HTTP 503").unwrap());
    // Answers to the poll made at attempt 0.
    assert_eq!(
        db.reject_submission("t", 0, "server", "HTTP 400", &json!({}))
            .unwrap(),
        None
    );
    assert!(
        !db.settle_page("t", 0, &[(record, json!([]))], &[], "")
            .unwrap()
    );
    assert!(!db.settle_submission("t", 0).unwrap());
    assert!(!db.submission_open_at("t", 0).unwrap());
    assert_eq!(status(&db, record), "submitted");
    // The current attempt's answer settles it.
    assert!(
        db.settle_page("t", 1, &[(record, json!([]))], &[], "")
            .unwrap()
    );
    assert!(db.settle_submission("t", 1).unwrap());
    assert_eq!(status(&db, record), "sent");
}

#[tokio::test]
async fn events_and_cursors_survive_a_restart_and_ids_are_never_given_again() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let yaml = "fhir_base_url: http://127.0.0.1:9\nretention:\n  history: 3650d\n";
    // On PostgreSQL this handle owns the test schema, so it outlives each
    // engine; on SQLite the file does, and nothing else stays open.
    std::fs::write(
        root.join("hie.yaml"),
        format!("database: data/hie.db\n{yaml}"),
    )
    .unwrap();
    let (config, _) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let seed =
        test_database_url().map(|_| Db::connect(&config.database.to_string_lossy()).unwrap());

    let first = start_engine(root, yaml).await;
    load(&first.state.db, "s", "a", 1, 1);
    let (_, before) = get_json(&format!("{}/api/events", first.api)).await;
    let cursor = before["next_cursor"].as_str().unwrap().to_string();
    stop_engine(first).await;

    let second = start_engine(root, yaml).await;
    let api = second.api.clone();
    let (_, again) = get_json(&format!("{api}/api/events")).await;
    assert_eq!(again["events"], before["events"]);
    let (_, page) = get_json(&format!("{api}/api/events?cursor={cursor}")).await;
    assert!(ids(&page).is_empty());
    load(&second.state.db, "s", "b", 0, 1);
    let (_, page) = get_json(&format!("{api}/api/events?cursor={cursor}")).await;
    assert_eq!(page["events"].as_array().unwrap().len(), 1);
    assert_eq!(page["events"][0]["detail"]["name"], "b");
    let highest = page["events"][0]["id"].as_i64().unwrap();

    // Every event purged, then a restart: the next id is still above them all.
    second
        .state
        .db
        .query_json("UPDATE events SET at = 0", [])
        .unwrap();
    hie::retention::purge(&second.state.db, 1).unwrap();
    assert!(events(&second.state.db).is_empty());
    stop_engine(second).await;
    let third = start_engine(root, yaml).await;
    load(&third.state.db, "s", "c", 0, 1);
    assert!(events(&third.state.db)[0]["id"].as_i64().unwrap() > highest);
    stop_engine(third).await;
    drop(seed);
}

// ---- rejected records over a period --------------------------------------------

/// Stores `record.rejected` for `record` at `at`.
fn rejected_at(db: &Db, record: i64, kind: &str, at: i64) {
    db.mark_rejected(record, kind, "rejected", None).unwrap();
    let last = events(db).pop().unwrap()["id"].as_i64().unwrap();
    db.query_json(
        "UPDATE events SET at = CAST($1 AS BIGINT) WHERE id = CAST($2 AS BIGINT)",
        hie::db::params![at, last],
    )
    .unwrap();
}

fn set_events_since(db: &Db, at: i64) {
    db.query_json(
        "UPDATE store_meta SET value = CAST($1 AS TEXT) WHERE key = 'events_since'",
        hie::db::params![at.to_string()],
    )
    .unwrap();
}

/// The window of a day in an answer, by its start.
fn day_of(answer: &Value, from: i64) -> Value {
    let iso = chrono::DateTime::from_timestamp_millis(from)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    answer["days"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["from"] == iso.as_str())
        .unwrap_or_else(|| panic!("no day from {iso}: {answer}"))
        .clone()
}

#[tokio::test]
async fn rejected_records_are_counted_once_per_day_with_their_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let (_manager, state) = engine(
        dir.path(),
        "fhir_base_url: http://127.0.0.1:9\n",
        &[("one", ndjson_source("one", ""))],
    );
    let db = state.db.clone();
    let api = serve(hie::api::router(state.clone())).await;
    let day = 86_400_000i64;
    let today = hie::db::now_ms().div_euclid(day) * day;
    load(&db, "one", "a", 0, 4);
    let [a, b, c, d] = [1, 2, 3, 4].map(|line| record_id(&db, "one", line));
    // Today, from its midnight: one record rejected twice, one filtered out.
    rejected_at(&db, a, "data", today);
    rejected_at(&db, a, "validation", today);
    rejected_at(&db, c, "filtered", today);
    // Yesterday, up to its last millisecond: two records.
    rejected_at(&db, b, "server", today - 1);
    rejected_at(&db, d, "parse", today - 1);
    // Another source's rejection is not this one's.
    load(&db, "two", "b", 1, 0);

    let url = format!("{api}/api/sources/one/failures");
    let (code, answer) = get_json(&url).await;
    assert_eq!(code, 200, "{answer}");
    assert_eq!(answer["source_id"], "one");
    assert_eq!(answer["tz"], "UTC");
    let days = answer["days"].as_array().unwrap();
    assert_eq!(days.len(), 14);
    let today_day = chrono::DateTime::from_timestamp_millis(today)
        .unwrap()
        .format("%Y-%m-%d")
        .to_string();
    assert_eq!(days[13]["day"], today_day.as_str());
    assert!(
        days.windows(2)
            .all(|w| w[0]["from"].as_str() < w[1]["from"].as_str())
    );
    let t = day_of(&answer, today);
    assert_eq!(
        (&t["records"], &t["filtered"]),
        (&json!(1), &json!(1)),
        "{t}"
    );
    let y = day_of(&answer, today - day);
    assert_eq!(
        (&y["records"], &y["filtered"]),
        (&json!(2), &json!(0)),
        "{y}"
    );
    assert_eq!(days[0]["records"], 0);
    // The last 24 hours hold both: three records and the one filtered out.
    let last = &answer["last_24h"];
    assert_eq!(
        (&last["records"], &last["filtered"]),
        (&json!(3), &json!(1)),
        "{last}"
    );

    // The log began on this store just now: no window is covered yet.
    assert!(days.iter().all(|d| d["complete"] == false), "{answer}");
    assert_eq!(last["complete"], false);

    // Begun two days ago: covered from then.
    set_events_since(&db, today - 2 * day);
    let (_, answer) = get_json(&url).await;
    assert_eq!(
        answer["covered_from"],
        day_of(&answer, today - 2 * day)["from"]
    );
    assert_eq!(day_of(&answer, today - 2 * day)["complete"], true);
    assert_eq!(day_of(&answer, today - 3 * day)["complete"], false);
    assert_eq!(day_of(&answer, today)["complete"], true);
    assert_eq!(answer["last_24h"]["complete"], true);
    // Begun long ago: the retention (30 days) is the limit. The counts are
    // still given over what is kept.
    set_events_since(&db, today - 60 * day);
    let (_, answer) = get_json(&format!("{url}?days=40")).await;
    assert_eq!(answer["days"].as_array().unwrap().len(), 40);
    assert_eq!(day_of(&answer, today - 29 * day)["complete"], true);
    assert_eq!(day_of(&answer, today - 31 * day)["complete"], false);
    assert_eq!(day_of(&answer, today - 39 * day)["records"], 0);
    let (_, answer) = get_json(&format!("{url}?days=90")).await;
    assert_eq!(answer["days"].as_array().unwrap().len(), 90);

    for (query, field) in [
        ("days=0", "days"),
        ("days=91", "days"),
        ("days=ten", "days"),
        ("days=+5", "days"),
        ("days=1&days=2", "days"),
        ("since=2026-11-20T00:00:00Z", "since"),
    ] {
        let (code, body) = get_json(&format!("{url}?{query}")).await;
        assert_eq!(code, 400, "{query}: {body}");
        assert_eq!(body["code"], "invalid_query", "{query}: {body}");
        assert_eq!(body["field"], field, "{query}: {body}");
    }
    // As `/stats` answers an unknown source.
    for path in ["failures", "stats"] {
        let (code, body) = get_json(&format!("{api}/api/sources/nobody/{path}")).await;
        assert_eq!(code, 404, "{body}");
        assert_eq!(body["error"], "source nobody not found");
    }
}

#[tokio::test]
async fn rejected_records_are_counted_in_local_days_of_the_zone_asked_for() {
    let dir = tempfile::tempdir().unwrap();
    let (_manager, state) = engine(
        dir.path(),
        "fhir_base_url: http://127.0.0.1:9\n",
        &[("one", ndjson_source("one", ""))],
    );
    let db = state.db.clone();
    let api = serve(hie::api::router(state.clone())).await;
    // Fourteen hours ahead of UTC, so its days are not UTC's.
    let zone = hie::zone::Zone::parse("Pacific/Kiritimati").unwrap();
    // The test reads the clock apart from the handler: kept clear of a
    // midnight, in the zone (10:00Z) or in UTC, that could fall between them.
    // Neither zone changes its offset, so their days are 24 hours.
    loop {
        let now = chrono::Utc::now();
        let near = |start: i64| {
            let ms = now.timestamp_millis();
            (start - ms).abs() < 30_000 || (start + 86_400_000 - ms).abs() < 30_000
        };
        let (_, from, _) = zone.today_bounds(now);
        let (_, utc_from, _) = hie::zone::Zone::utc().today_bounds(now);
        if !near(from) && !near(utc_from) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    let (today, from, to) = zone.today_bounds(chrono::Utc::now());
    load(&db, "one", "a", 0, 3);
    let [a, b, c] = [1, 2, 3].map(|line| record_id(&db, "one", line));
    // One at local midnight, one the millisecond before, one filtered out today.
    rejected_at(&db, a, "data", from);
    rejected_at(&db, b, "validation", from - 1);
    rejected_at(&db, c, "filtered", to - 1);

    let url = format!("{api}/api/sources/one/failures");
    // Matched without regard to case, echoed as the database spells it.
    let (code, answer) = get_json(&format!("{url}?tz=pacific/kiritimati&days=2")).await;
    assert_eq!(code, 200, "{answer}");
    assert_eq!(answer["tz"], "Pacific/Kiritimati");
    let days = answer["days"].as_array().unwrap();
    assert_eq!(days.len(), 2);
    let iso = |ms: i64| {
        chrono::DateTime::from_timestamp_millis(ms)
            .unwrap()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    };
    let yesterday = today.pred_opt().unwrap().to_string();
    assert_eq!(
        (&days[1]["day"], &days[1]["from"], &days[1]["to"]),
        (
            &json!(today.to_string()),
            &json!(iso(from)),
            &json!(iso(to))
        ),
        "{answer}"
    );
    assert_eq!(days[0]["day"], yesterday.as_str());
    assert_eq!(days[0]["to"], days[1]["from"]);
    assert_eq!(
        (&days[1]["records"], &days[1]["filtered"]),
        (&json!(1), &json!(1)),
        "{answer}"
    );
    assert_eq!(
        (&days[0]["records"], &days[0]["filtered"]),
        (&json!(1), &json!(0)),
        "{answer}"
    );

    // Without a zone the days are UTC's, and say so.
    let (_, answer) = get_json(&format!("{url}?days=1")).await;
    assert_eq!(answer["tz"], "UTC");
    let utc_today = chrono::Utc::now().date_naive().to_string();
    assert_eq!(answer["days"][0]["day"], utc_today.as_str());
    assert!(
        answer["days"][0]["from"]
            .as_str()
            .unwrap()
            .ends_with("T00:00:00.000Z")
    );

    // An unknown zone is refused with the same answer as on `/stats`, before
    // the source is looked up; so is a second `tz`.
    for path in [
        "one/failures",
        "nobody/failures",
        "one/stats",
        "nobody/stats",
    ] {
        for tz in ["Mars/Olympus", "Factory", "%20UTC", ""] {
            let (code, body) = get_json(&format!("{api}/api/sources/{path}?tz={tz}")).await;
            assert_eq!(code, 400, "{path} {tz}: {body}");
            assert_eq!(
                body,
                json!({
                    "error": "tz must be an IANA time zone name, such as Europe/Madrid or UTC",
                    "code": "invalid_query",
                    "field": "tz"
                }),
                "{path} {tz}"
            );
        }
    }
    let (code, body) = get_json(&format!("{url}?tz=UTC&tz=UTC")).await;
    assert_eq!(code, 400, "{body}");
    assert_eq!(
        (&body["code"], &body["field"]),
        (&json!("invalid_query"), &json!("tz"))
    );
}

#[test]
fn the_event_log_start_is_set_once_and_kept() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hie.db");
    let first = Db::open(&path).unwrap();
    let since = first.events_since().unwrap().unwrap();
    assert!((hie::db::now_ms() - since).abs() < 60_000, "{since}");
    set_events_since(&first, since - 1000);
    // Opened again, as at a restart (on PostgreSQL the first handle owns the
    // schema, so it stays open).
    let keep = test_database_url().map(|_| first);
    std::thread::sleep(Duration::from_millis(5));
    let again = Db::open(&path).unwrap();
    assert_eq!(again.events_since().unwrap(), Some(since - 1000));
    drop(keep);
}
