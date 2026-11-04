//! What the corpus does not cover: two files with one stem, local module
//! state kept per source, concurrent saves on one base, and a Try run whose
//! engine stopped.

use std::path::Path;
use std::sync::{Arc, Barrier};

use serde_json::{Value, json};

use super::{Environment, ExecutorSettings, NewVersion, Saved, Versions};
use crate::config::FeedConfig;
use crate::db::Db;

fn python() -> Option<String> {
    std::env::var("HIE_TEST_PYTHON")
        .ok()
        .filter(|p| !p.trim().is_empty())
}

fn versions(db: Arc<Db>, transforms: &Path, python: &str) -> Versions {
    Versions::new(
        db,
        ExecutorSettings {
            python: python.into(),
            runtime_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("hie_runtime"),
            terminology: None,
            runtime: None,
        },
        transforms,
    )
}

fn feed(base: &Path, id: &str, transform: &str) -> FeedConfig {
    let yaml = format!(
        "id: {id}\nname: {id}\nsource:\n  type: folder\n  path: inbox\ntransform: {transform}\n"
    );
    let mut feed: FeedConfig = serde_yaml::from_str(&yaml).unwrap();
    feed.resolve(base).unwrap();
    feed
}

async fn run_one(versions: &Versions, source: &str, feed: &FeedConfig) -> Value {
    let version = versions.resolve_for_source(feed).unwrap();
    let executor = versions.executor(source, &version).unwrap();
    let execution = executor
        .run(&[(json!({}), json!({"feed": source}))])
        .await
        .unwrap();
    match &execution.items[0].outcome {
        crate::transform::Outcome::Resources(r) => r[0].clone(),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn two_files_with_one_stem_are_two_transforms_each_running_its_own_code() {
    let Some(python) = python() else {
        eprintln!("skipping: set HIE_TEST_PYTHON");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    for (sub, marker) in [("a", "A"), ("b", "B")] {
        std::fs::create_dir_all(base.join(sub)).unwrap();
        std::fs::write(
            base.join(sub).join("script.py"),
            format!("def transform(record, context):\n    return {{\"resourceType\": \"Basic\", \"id\": \"{marker}\"}}\n"),
        )
        .unwrap();
    }
    let db = Arc::new(Db::in_memory().unwrap());
    let versions = versions(db, &base.join("transforms"), &python);
    let a = feed(base, "src-a", "a/script.py");
    let b = feed(base, "src-b", "b/script.py");
    let va = versions.resolve_for_source(&a).unwrap();
    let vb = versions.resolve_for_source(&b).unwrap();
    assert_eq!(va.transform_id, "script");
    assert_ne!(
        va.transform_id, vb.transform_id,
        "B never replaces A's identity"
    );
    assert_ne!(va.code_sha256, vb.code_sha256);
    // Resolving B did not move A: A still runs A's code, with A's version.
    assert_eq!(versions.resolve_for_source(&a).unwrap(), va);
    assert_eq!(run_one(&versions, "src-a", &a).await["id"], "A");
    assert_eq!(run_one(&versions, "src-b", &b).await["id"], "B");
    assert_eq!(run_one(&versions, "src-a", &a).await["id"], "A");
}

#[tokio::test]
async fn local_module_state_never_crosses_sources_and_goes_with_the_source() {
    let Some(python) = python() else {
        eprintln!("skipping: set HIE_TEST_PYTHON");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    std::fs::create_dir_all(base.join("transforms")).unwrap();
    std::fs::write(
        base.join("transforms/counter.py"),
        "seen = []\n\ndef transform(record, context):\n    seen.append(context['feed'])\n    return {\"resourceType\": \"Basic\", \"id\": str(len(seen)), \"text\": seen[0]}\n",
    )
    .unwrap();
    let db = Arc::new(Db::in_memory().unwrap());
    let versions = versions(db, &base.join("transforms"), &python);
    let a = feed(base, "src-a", "transforms/counter.py");
    let b = feed(base, "src-b", "transforms/counter.py");
    assert_eq!(run_one(&versions, "src-a", &a).await["id"], "1");
    assert_eq!(run_one(&versions, "src-a", &a).await["id"], "2");
    // Same version, another source: a worker of its own.
    let first_b = run_one(&versions, "src-b", &b).await;
    assert_eq!(
        (first_b["id"].as_str(), first_b["text"].as_str()),
        (Some("1"), Some("src-b"))
    );
    // A deleted source's worker goes; a source of that id starts afresh.
    versions.source_deleted("src-a").unwrap();
    assert_eq!(run_one(&versions, "src-a", &a).await["id"], "1");
}

#[test]
fn of_two_saves_on_one_base_exactly_one_wins() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hie.db");
    let first = Arc::new(Db::open(&path).unwrap());
    let env = Environment::local();
    let content = |code: &'static str| NewVersion {
        code,
        code_sha256: "0",
        note: None,
        author: None,
        environment: &env,
    };
    first
        .transaction(|tx| {
            tx.insert_transform("t", "T", "t", &content("v1"), None, None)
                .map(drop)
        })
        .unwrap();
    for round in 1..=5 {
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                // A store handle each, as two engines on one store.
                let db = Db::open(&path).unwrap();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let env = Environment::local();
                    barrier.wait();
                    db.transaction(|tx| {
                        tx.save_version(
                            "t",
                            Some(round),
                            &NewVersion {
                                code: "next",
                                code_sha256: "1",
                                note: None,
                                author: None,
                                environment: &env,
                            },
                        )
                    })
                    .map(|saved| match saved {
                        Saved::Version(v) => Ok(v.version),
                        Saved::Conflict { current_version } => Err(current_version),
                        Saved::NotFound => panic!("not found"),
                    })
                })
            })
            .collect();
        let mut outcomes: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().unwrap().expect("no store error"))
            .collect();
        outcomes.sort();
        assert_eq!(outcomes, [Ok(round + 1), Err(round + 1)], "round {round}");
    }
    assert_eq!(first.version_infos("t").unwrap().len(), 6);
}

#[test]
fn a_try_run_whose_engine_stopped_reads_lost_and_keeps_its_id() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::in_memory().unwrap());
    let versions = versions(db.clone(), dir.path(), "python3");
    let run = json!({"id": "r1", "status": "running", "outcome": null,
        "termination": null, "cleanup": null, "started_at": "2026-11-03T10:00:00.000Z"});
    let coverage = json!({"requested": 2, "reported": 0, "complete": false});
    // Reserved by an engine that stopped before it had the answer.
    assert!(db.try_reserve("r1", &run, &coverage, 0).unwrap());
    let read = crate::tryrun::get(&versions, "r1").unwrap();
    assert_eq!(read["run"]["status"], "finished");
    assert_eq!(read["run"]["outcome"], "indeterminate");
    assert_eq!(read["run"]["termination"], "unconfirmed");
    assert!(!db.try_reserve("r1", &run, &coverage, 0).unwrap());
    assert_eq!(
        crate::tryrun::cancel(&versions, "r1").unwrap_err().code(),
        "run_finished"
    );
}

#[test]
fn two_same_stem_files_imported_at_once_never_share_a_transform() {
    for round in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        for sub in ["a", "b"] {
            std::fs::create_dir_all(base.join(sub)).unwrap();
            std::fs::write(
                base.join(sub).join("script.py"),
                format!("def transform(record, context):\n    return None  # {sub}\n"),
            )
            .unwrap();
        }
        let path = base.join("hie.db");
        // Held for the whole round: on PostgreSQL the first handle on a
        // store owns its test schema.
        let db = Db::open(&path).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|sub| {
                // Two engines on one store.
                let db = Arc::new(Db::open(&path).unwrap());
                let feed = feed(base, &format!("src-{sub}"), &format!("{sub}/script.py"));
                let transforms = base.join("transforms");
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let versions = versions(db, &transforms, "python3");
                    barrier.wait();
                    versions.resolve_for_source(&feed).unwrap()
                })
            })
            .collect();
        let refs: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_ne!(refs[0].transform_id, refs[1].transform_id, "round {round}");
        assert_ne!(refs[0].code_sha256, refs[1].code_sha256, "round {round}");
        for (r, sub) in refs.iter().zip(["a", "b"]) {
            let file = db
                .transform_info(&r.transform_id)
                .unwrap()
                .unwrap()
                .file_path;
            let own = base.join(sub).join("script.py").canonicalize().unwrap();
            assert_eq!(file.as_deref(), Some(own.to_string_lossy().as_ref()));
            assert_eq!(db.version_infos(&r.transform_id).unwrap().len(), 1);
        }
    }
}

#[test]
fn a_cold_runtime_fixes_no_version_so_no_load_is_admitted() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    std::fs::create_dir_all(base.join("transforms")).unwrap();
    std::fs::write(
        base.join("transforms/t.py"),
        "def transform(r, c):\n    return None\n",
    )
    .unwrap();
    let db = Arc::new(Db::in_memory().unwrap());
    let versions = Versions::new(
        db.clone(),
        ExecutorSettings {
            python: "python3".into(),
            runtime_dir: base.to_path_buf(),
            terminology: None,
            // Nothing listens there: the runtime's environment is unknown.
            runtime: Some(crate::runtime::RuntimeSettings {
                url: "http://127.0.0.1:9".into(),
                token: None,
                batch_size: 10,
            }),
        },
        &base.join("transforms"),
    );
    let feed = feed(base, "src", "transforms/t.py");
    assert!(versions.resolve_for_source(&feed).is_err());
    assert!(db.transform_infos().unwrap().is_empty());
    assert!(db.pin_row("src").unwrap().is_none());
}

#[test]
fn a_transform_referred_to_in_the_store_or_by_unfinished_work_is_not_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::in_memory().unwrap());
    let versions = versions(db.clone(), dir.path(), "python3");
    let env = Environment::local();
    let content = NewVersion {
        code: "x",
        code_sha256: "0",
        note: None,
        author: None,
        environment: &env,
    };
    db.transaction(|tx| {
        tx.insert_transform("t", "T", "t", &content, None, None)
            .map(drop)
    })
    .unwrap();
    // A pin in the store (a source the manager does not hold right now).
    let pin = super::PinRow {
        transform_id: "t".into(),
        version: None,
        declared: "t@current".into(),
    };
    assert!(db.set_pin_row("gone-from-memory", &pin).unwrap());
    let refused = versions.delete("t", &[]).unwrap_err();
    assert_eq!(refused.code(), "transform_in_use");
    assert_eq!(refused.members()["sources"], json!(["gone-from-memory"]));
    db.remove_pin_row("gone-from-memory").unwrap();
    // A load still being read with one of its versions.
    let v = db.version_info("t", 1).unwrap().unwrap().reference();
    let load = db
        .open_load("src", "a.csv", "s", 1, None, Some(&v))
        .unwrap();
    assert_eq!(
        versions.delete("t", &[]).unwrap_err().code(),
        "transform_in_use"
    );
    db.fail_file(load, "x").unwrap();
    versions.delete("t", &[]).unwrap();
    // Gone: nothing can refer to it any more.
    assert!(!db.set_pin_row("late", &pin).unwrap());
    assert_eq!(
        versions.delete("t", &[]).unwrap_err().code(),
        "transform_not_found"
    );
}

#[tokio::test]
async fn zero_one_names_keep_their_case_and_get_ids_of_their_own() {
    let dir = tempfile::tempdir().unwrap();
    let transforms = dir.path().join("transforms");
    let db = Arc::new(Db::in_memory().unwrap());
    let versions = versions(db.clone(), &transforms, "python3");
    let code = "def transform(r, c):\n    return None\n";
    assert_eq!(
        versions.save_legacy("Lab_Results", code).await.unwrap(),
        (true, 1)
    );
    assert_eq!(
        versions.save_legacy("Lab_Results", code).await.unwrap(),
        (false, 1)
    );
    let id = versions.resolve_name("Lab_Results").unwrap().unwrap();
    assert!(id.starts_with("lab_results-"), "{id}");
    assert!(
        transforms.join("Lab_Results.py").is_file(),
        "its file keeps the name"
    );
    // The 0.1 name that is the lower case of it is another transform, its own id.
    assert_eq!(
        versions.save_legacy("lab_results", "# b\n").await.unwrap(),
        (true, 1)
    );
    assert_eq!(
        versions.resolve_name("lab_results").unwrap().as_deref(),
        Some("lab_results")
    );
    assert_eq!(
        versions.resolve_name("Lab_Results").unwrap(),
        Some(id.clone())
    );
    assert_eq!(db.version_infos(&id).unwrap().len(), 1);
    assert!(versions.save_legacy("bad name", code).await.is_err());
    assert!(versions.save_legacy("try", code).await.is_err());
}

#[tokio::test]
async fn a_local_try_captures_each_records_output_and_loads_discard_it() {
    let Some(python) = python() else {
        eprintln!("skipping: set HIE_TEST_PYTHON");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("t.py");
    std::fs::write(
        &script,
        "import sys\n\ndef transform(record, context):\n    print('zq-out', record['n'])\n    print('zq-err', file=sys.stderr)\n    return None\n",
    )
    .unwrap();
    let command = |capture_streams| crate::transform::WorkerCommand {
        python: python.clone(),
        worker_script: Path::new(env!("CARGO_MANIFEST_DIR")).join("hie_runtime/worker.py"),
        transform_script: script.clone(),
        terminology: None,
        capture_streams,
    };
    let mut tried = crate::transform::Worker::spawn(&command(true))
        .await
        .unwrap();
    let reply = tried.call(&json!({"n": 1}), &json!({})).await.unwrap();
    assert_eq!(
        reply["diagnostics"],
        json!({"stdout": "zq-out 1\n", "stderr": "zq-err\n"})
    );
    tried.kill().await.unwrap();
    let mut loading = crate::transform::Worker::spawn(&command(false))
        .await
        .unwrap();
    let reply = loading.call(&json!({"n": 2}), &json!({})).await.unwrap();
    assert!(reply.get("diagnostics").is_none());
    loading.kill().await.unwrap();
}

/// A destination whose `$validate` never answers.
async fn silent_destination() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    format!("http://{addr}")
}

fn try_input(
    run_id: &str,
    code: &str,
    deadline_ms: u64,
    validate: bool,
) -> crate::tryrun::TryInput {
    crate::tryrun::TryInput {
        run_id: run_id.into(),
        code: code.into(),
        saved: None,
        records: vec![json!({"n": 1})],
        references: Vec::new(),
        deadline: std::time::Duration::from_millis(deadline_ms),
        validate,
    }
}

#[tokio::test]
async fn a_try_stops_at_its_deadline_also_while_validating_or_loading() {
    let Some(python) = python() else {
        eprintln!("skipping: set HIE_TEST_PYTHON");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::in_memory().unwrap());
    let versions = Arc::new(versions(db, dir.path(), &python));
    let fhir = Arc::new(crate::fhir::FhirClient::new(&silent_destination().await));

    // Validation that never answers: stopped at the deadline, results kept.
    let started = std::time::Instant::now();
    let code = "def transform(r, c):\n    return {'resourceType': 'Basic', 'id': 'b'}\n";
    let body = crate::tryrun::start(
        versions.clone(),
        fhir.clone(),
        1,
        try_input("v", code, 1000, true),
    )
    .await
    .unwrap();
    assert!(
        started.elapsed() < std::time::Duration::from_millis(1400),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(body["run"]["outcome"], "stopped");
    assert_eq!(body["run"]["reason"], "deadline");
    assert_eq!(body["run"]["validation"], "unavailable");
    assert_eq!(body["items"][0]["validation"], Value::Null);
    assert_eq!(body["items"][0]["result"]["resources"][0]["id"], "b");

    // A program that never finishes loading: killed and reaped at the deadline.
    let started = std::time::Instant::now();
    let slow = "import time\ntime.sleep(30)\n\ndef transform(r, c):\n    return None\n";
    let body = crate::tryrun::start(versions.clone(), fhir, 1, try_input("l", slow, 1000, false))
        .await
        .unwrap();
    assert!(
        started.elapsed() < std::time::Duration::from_millis(1400),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(body["run"]["outcome"], "stopped");
    assert_eq!(body["run"]["termination"], "confirmed");
    assert_eq!(body["items"][0]["disposition"], "not_started");
}

#[cfg(unix)]
#[tokio::test]
async fn a_save_never_writes_through_a_symlink_to_another_transforms_file() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    std::fs::create_dir_all(base.join("a")).unwrap();
    std::fs::create_dir_all(base.join("transforms")).unwrap();
    let original = "def transform(r, c):\n    return None  # script\n";
    std::fs::write(base.join("a/script.py"), original).unwrap();
    // transforms/foo.py is a link to a file another transform is.
    std::os::unix::fs::symlink(base.join("a/script.py"), base.join("transforms/foo.py")).unwrap();
    let db = Arc::new(Db::in_memory().unwrap());
    let versions = versions(db.clone(), &base.join("transforms"), "python3");
    let script = versions
        .resolve_for_source(&feed(base, "src", "a/script.py"))
        .unwrap();
    assert_eq!(script.transform_id, "script");

    versions.save_legacy("foo", "# foo\n").await.unwrap();
    assert_eq!(
        std::fs::read_to_string(base.join("a/script.py")).unwrap(),
        original
    );
    let link = std::fs::symlink_metadata(base.join("transforms/foo.py")).unwrap();
    assert!(link.file_type().is_symlink(), "the link is left as it was");
    let foo = db
        .transform_info(&versions.resolve_name("foo").unwrap().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(foo.file_path, None, "a link is never the transform's file");

    // A regular destination is written whole, by a rename.
    versions.save_legacy("bar", "# bar\n").await.unwrap();
    let bar = base.join("transforms/bar.py");
    assert!(
        std::fs::symlink_metadata(&bar)
            .unwrap()
            .file_type()
            .is_file()
    );
    assert_eq!(std::fs::read_to_string(&bar).unwrap(), "# bar\n");
    let leftovers: Vec<_> = std::fs::read_dir(base.join("transforms"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty());
}

#[test]
fn a_definition_whose_transform_cannot_be_read_yet_is_reconciled_once_it_can() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    std::fs::create_dir_all(base.join("t")).unwrap();
    let b_code = "def transform(r, c):\n    return None  # b\n";
    std::fs::write(
        base.join("t/a.py"),
        "def transform(r, c):\n    return None  # a\n",
    )
    .unwrap();
    std::fs::write(base.join("t/b.py"), b_code).unwrap();
    let db = Arc::new(Db::in_memory().unwrap());
    // An earlier process ran the source on A.
    let before = versions(db.clone(), &base.join("transforms"), "python3");
    assert_eq!(
        before
            .resolve_for_source(&feed(base, "src", "t/a.py"))
            .unwrap()
            .transform_id,
        "a"
    );
    drop(before);
    // Its definition now names B, whose file cannot be read at first.
    let on_b = feed(base, "src", "t/b.py");
    std::fs::remove_file(base.join("t/b.py")).unwrap();
    let versions = versions(db.clone(), &base.join("transforms"), "python3");
    assert!(versions.resolve_for_source(&on_b).is_err());
    std::fs::write(base.join("t/b.py"), b_code).unwrap();
    assert_eq!(
        versions.resolve_for_source(&on_b).unwrap().transform_id,
        "b"
    );
    assert_eq!(db.pin_row("src").unwrap().unwrap().transform_id, "b");
}
