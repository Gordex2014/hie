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
        .transaction(|tx| tx.insert_transform("t", "T", "t", &content("v1")).map(drop))
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
