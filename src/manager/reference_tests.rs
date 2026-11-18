//! Which sources are registered for which credential, as sources are created,
//! replaced and deleted on one or two engine instances sharing a store, through
//! failures and restarts. Every interleaving is forced with pause points; the
//! runners read a stand-in bucket that refuses every request.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};

use super::*;
use crate::db::Db;
use crate::secrets::{self, KeySettings, NewSecret, SecretValue};

/// A pause point that holds the task the `nth` time it is reached.
struct Gate {
    name: &'static str,
    nth: usize,
    seen: AtomicUsize,
    reached: Notify,
    release: Notify,
}

impl Gate {
    async fn reached(&self) {
        self.reached.notified().await;
    }
    fn release(&self) {
        self.release.notify_one();
    }
}

/// Every pause point reached, in order, and the gates that hold some of them.
struct Hooks {
    gates: Mutex<Vec<Arc<Gate>>>,
    events: tokio::sync::Mutex<mpsc::UnboundedReceiver<&'static str>>,
}

impl Hooks {
    fn new() -> (Arc<Self>, crate::AsyncPause) {
        let (tx, rx) = mpsc::unbounded_channel();
        let hooks = Arc::new(Self {
            gates: Mutex::new(Vec::new()),
            events: tokio::sync::Mutex::new(rx),
        });
        let h = hooks.clone();
        let pause: crate::AsyncPause = Arc::new(move |at| {
            let _ = tx.send(at);
            let gate = h
                .gates
                .lock()
                .unwrap()
                .iter()
                .find(|g| g.name == at)
                .cloned();
            Box::pin(async move {
                if let Some(gate) = gate
                    && gate.seen.fetch_add(1, Ordering::SeqCst) + 1 == gate.nth
                {
                    gate.reached.notify_one();
                    gate.release.notified().await;
                }
            })
        });
        (hooks, pause)
    }

    /// Holds the `nth` arrival at `name`, counted from now.
    fn hold(&self, name: &'static str, nth: usize) -> Arc<Gate> {
        let gate = Arc::new(Gate {
            name,
            nth,
            seen: AtomicUsize::new(0),
            reached: Notify::new(),
            release: Notify::new(),
        });
        self.gates.lock().unwrap().push(gate.clone());
        gate
    }

    /// Waits until a pause point named `name` is reached.
    async fn next(&self, name: &str) {
        let mut events = self.events.lock().await;
        loop {
            let at = tokio::time::timeout(std::time::Duration::from_secs(60), events.recv())
                .await
                .unwrap_or_else(|_| panic!("{name} was never reached"))
                .expect("hooks alive");
            if at == name {
                return;
            }
        }
    }
}

/// An engine instance: its own root and sources directory, on the shared store.
struct Instance {
    root: PathBuf,
    db: Arc<Db>,
    secrets: Secrets,
    manager: Arc<SourceManager>,
    hooks: Arc<Hooks>,
}

impl Instance {
    fn start(root: &Path, database: &Path, key: &str) -> Self {
        std::fs::create_dir_all(root.join("sources")).unwrap();
        std::fs::create_dir_all(root.join("transforms")).unwrap();
        std::fs::write(
            root.join("transforms/t.py"),
            "def transform(r, c):\n    return []\n",
        )
        .unwrap();
        std::fs::write(
            root.join("hie.yaml"),
            format!(
                "fhir_base_url: http://127.0.0.1:9/fhir\ndatabase: {}\n",
                database.display()
            ),
        )
        .unwrap();
        let (config, feeds) = crate::config::load(&root.join("hie.yaml")).unwrap();
        let db = Arc::new(Db::connect(&config.database.to_string_lossy()).unwrap());
        let secrets = secrets::open(&config, &db, KeySettings::inline(key)).unwrap();
        let alerts = Arc::new(Alerter::new(db.clone(), None));
        let fhir = Arc::new(FhirClient::with_credentials(&config.fhir_base_url, None));
        let manager = SourceManager::new(&config, root, db.clone(), fhir, alerts, secrets.clone());
        manager.set_handover_grace(std::time::Duration::from_secs(120));
        let (hooks, pause) = Hooks::new();
        manager.on_pause(pause);
        manager.start_all(feeds).unwrap();
        Self {
            root: root.to_path_buf(),
            db,
            secrets,
            manager,
            hooks,
        }
    }

    /// Stops every runner, as the process ending would.
    fn stop(self) -> PathBuf {
        for managed in self.manager.sources.lock().unwrap().values() {
            SourceManager::halt(managed);
        }
        self.root
    }

    fn owner(&self) -> &str {
        self.secrets.owner()
    }

    /// The secrets this instance has registered source `source` for.
    fn refs(&self, source: &str) -> BTreeSet<String> {
        self.db
            .secret_holds_of(self.owner())
            .unwrap()
            .into_iter()
            .filter(|(s, _)| s == source)
            .map(|(_, secret)| secret)
            .collect()
    }

    /// Waits for the cleanup of a start or delete of `source` to finish.
    async fn settled(&self, source: &str) {
        self.hooks.next("manager:cleanup:before").await;
        drop(self.manager.source_lock(source).lock().await);
    }

    async fn secret(&self, name: &str) -> String {
        self.secrets
            .store()
            .unwrap()
            .create(NewSecret {
                name: name.into(),
                kind: SecretKind::Aws,
                value: value(&format!("AKID{}", name.to_uppercase())),
            })
            .await
            .unwrap()
            .id
    }

    async fn delete_secret(&self, id: &str) -> Result<(), SecretError> {
        self.secrets.store().unwrap().delete(id).await
    }

    fn definition(&self, id: &str, credential: &str, bucket: &str) -> Value {
        json!({
            "id": id, "name": id,
            "source": {"type": "s3", "bucket": "b", "region": "us-east-1",
                       "endpoint": bucket, "credential": credential, "poll_seconds": 3600},
            "format": "ndjson", "transform": "transforms/t.py",
        })
    }

    fn file_text(&self, id: &str) -> String {
        std::fs::read_to_string(self.root.join("sources").join(format!("{id}.yaml"))).unwrap()
    }
}

fn value(key_id: &str) -> SecretValue {
    SecretValue::new(
        SecretKind::Aws,
        [
            ("access_key_id".to_string(), key_id.to_string().into()),
            (
                "secret_access_key".to_string(),
                "generated-secret".to_string().into(),
            ),
        ],
    )
    .unwrap()
}

fn key() -> String {
    let k = crate::crypto::MasterKey::generate().unwrap();
    String::from_utf8(k.to_line()[..44].to_vec()).unwrap()
}

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|s| s.to_string()).collect()
}

fn in_use(r: Result<(), SecretError>) -> Vec<String> {
    match r {
        Err(SecretError::InUse(sources)) => sources,
        other => panic!("expected InUse, got {other:?}"),
    }
}

/// A bucket that refuses every request at once, so passes end quickly.
async fn refusing_bucket() -> String {
    let app = axum::Router::new().fallback(|| async { axum::http::StatusCode::FORBIDDEN });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

/// One store for the whole test: a SQLite file, or, under
/// `HIE_TEST_DATABASE_URL`, the PostgreSQL schema named after this path. The
/// first handle keeps the schema for the test's length.
fn store(dir: &Path) -> (PathBuf, Db) {
    let path = dir.join("shared").join("hie.db");
    let keeper = Db::connect(&path.to_string_lossy()).unwrap();
    (path, keeper)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_replaces_and_a_credential_delete_in_between() {
    let dir = tempfile::tempdir().unwrap();
    let (database, _keeper) = store(dir.path());
    let bucket = refusing_bucket().await;
    let a = Instance::start(&dir.path().join("a"), &database, &key());
    let (x, y, z) = (
        a.secret("x").await,
        a.secret("y").await,
        a.secret("z").await,
    );
    a.manager
        .create(&a.definition("s", "x", &bucket), None)
        .await
        .unwrap();
    a.settled("s").await;

    let gate = a.hooks.hold("manager:after-hold", 1);
    let first = tokio::spawn({
        let (m, body) = (a.manager.clone(), a.definition("s", "y", &bucket));
        async move { m.replace("s", &body).await }
    });
    gate.reached().await;
    let second = tokio::spawn({
        let (m, body) = (a.manager.clone(), a.definition("s", "z", &bucket));
        async move { m.replace("s", &body).await }
    });
    // Replace 1 holds y and the source's lock; replace 2 waits for the lock.
    assert_eq!(in_use(a.delete_secret(&y).await), ["s"]);
    gate.release();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    a.settled("s").await;
    a.settled("s").await;

    assert_eq!(a.refs("s"), set(&[&z]));
    assert!(a.file_text("s").contains("credential: z"));
    a.delete_secret(&y).await.unwrap();
    a.delete_secret(&x).await.unwrap();
    assert_eq!(in_use(a.delete_secret(&z).await), ["s"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_source_on_two_instances_keeps_each_instances_registration() {
    let dir = tempfile::tempdir().unwrap();
    let (database, _keeper) = store(dir.path());
    let bucket = refusing_bucket().await;
    let k = key();
    let a = Instance::start(&dir.path().join("a"), &database, &k);
    let b = Instance::start(&dir.path().join("b"), &database, &k);
    assert_ne!(a.owner(), b.owner());
    let x = a.secret("x").await;
    // Visible through the other instance: one store.
    let y = b.secret("y").await;
    assert!(a.secrets.store().unwrap().find(&y).await.unwrap().is_some());

    a.manager
        .create(&a.definition("s", "x", &bucket), None)
        .await
        .unwrap();
    a.settled("s").await;
    b.manager
        .create(&b.definition("s", "x", &bucket), None)
        .await
        .unwrap();
    b.settled("s").await;
    b.manager
        .replace("s", &b.definition("s", "y", &bucket))
        .await
        .unwrap();
    b.settled("s").await;

    assert_eq!(in_use(a.delete_secret(&x).await), ["s"], "a's registration");
    assert_eq!(in_use(b.delete_secret(&y).await), ["s"]);
    let used_by = a
        .secrets
        .store()
        .unwrap()
        .find(&x)
        .await
        .unwrap()
        .unwrap()
        .used_by;
    assert_eq!(used_by, ["s"]);

    a.manager.delete("s").await.unwrap();
    a.settled("s").await;
    b.delete_secret(&x).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_runner_still_registering_its_credential_delays_the_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let (database, _keeper) = store(dir.path());
    let bucket = refusing_bucket().await;
    let a = Instance::start(&dir.path().join("a"), &database, &key());
    let (x, y) = (a.secret("x").await, a.secret("y").await);
    // The first runner stops just before it registers the credential it is
    // about to use.
    let gate = a.hooks.hold("runner:credential:before-hold", 1);
    a.manager
        .create(&a.definition("s", "x", &bucket), None)
        .await
        .unwrap();
    a.settled("s").await;
    gate.reached().await;

    // Meanwhile the source moves to y; the new runner waits for the old one.
    a.manager
        .replace("s", &a.definition("s", "y", &bucket))
        .await
        .unwrap();
    assert_eq!(a.manager.unfinished_runners("s"), 2);
    assert_eq!(in_use(a.delete_secret(&x).await), ["s"]);

    // The old runner registers x again, sees it was halted and ends; only then
    // is x released.
    gate.release();
    a.settled("s").await;
    assert_eq!(a.refs("s"), set(&[&y]));
    a.delete_secret(&x).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_that_fails_after_publishing_keeps_both_until_the_next_start() {
    let dir = tempfile::tempdir().unwrap();
    let (database, _keeper) = store(dir.path());
    let bucket = refusing_bucket().await;
    let k = key();
    let a = Instance::start(&dir.path().join("a"), &database, &k);
    let (x, y) = (a.secret("x").await, a.secret("y").await);
    a.manager
        .create(&a.definition("s", "x", &bucket), None)
        .await
        .unwrap();
    a.settled("s").await;

    a.manager.fail_next_start.store(true, Ordering::SeqCst);
    let err = a
        .manager
        .replace("s", &a.definition("s", "y", &bucket))
        .await
        .unwrap_err();
    assert!(matches!(err, ManagerError::NotStarted(_)), "{err:?}");
    assert!(a.file_text("s").contains("credential: y"));
    assert_eq!(a.refs("s"), set(&[&x, &y]));
    assert_eq!(in_use(a.delete_secret(&x).await), ["s"]);
    assert_eq!(in_use(a.delete_secret(&y).await), ["s"]);

    // The same for a new source.
    a.manager.fail_next_start.store(true, Ordering::SeqCst);
    let err = a
        .manager
        .create(&a.definition("t", "y", &bucket), None)
        .await
        .unwrap_err();
    assert!(matches!(err, ManagerError::NotStarted(_)), "{err:?}");
    assert_eq!(a.refs("t"), set(&[&y]));

    // A restart loads what was saved, and reconciles to it.
    let root = a.stop();
    let a = Instance::start(&root, &database, &k);
    assert_eq!(a.refs("s"), set(&[&y]));
    assert_eq!(a.refs("t"), set(&[&y]));
    a.delete_secret(&x).await.unwrap();
    assert_eq!(in_use(a.delete_secret(&y).await), ["s", "t"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_registration_that_fails_keeps_the_old_one() {
    let dir = tempfile::tempdir().unwrap();
    let (database, _keeper) = store(dir.path());
    let bucket = refusing_bucket().await;
    let a = Instance::start(&dir.path().join("a"), &database, &key());
    let (x, y) = (a.secret("x").await, a.secret("y").await);
    a.manager
        .create(&a.definition("s", "x", &bucket), None)
        .await
        .unwrap();
    a.settled("s").await;

    // y is found, then deleted before the replace registers it.
    let gate = a.hooks.hold("manager:after-find", 1);
    let replace = tokio::spawn({
        let (m, body) = (a.manager.clone(), a.definition("s", "y", &bucket));
        async move { m.replace("s", &body).await }
    });
    gate.reached().await;
    a.delete_secret(&y).await.unwrap();
    gate.release();
    match replace.await.unwrap() {
        Err(ManagerError::InvalidDefinition(m)) => {
            assert!(m.contains("no credential named y"), "{m}")
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert!(a.file_text("s").contains("credential: x"));
    assert_eq!(a.refs("s"), set(&[&x]));
    assert_eq!(in_use(a.delete_secret(&x).await), ["s"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_cleans_what_a_crash_left_and_only_its_own_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (database, _keeper) = store(dir.path());
    let bucket = refusing_bucket().await;
    let k = key();
    let a = Instance::start(&dir.path().join("a"), &database, &k);
    let (x, y) = (a.secret("x").await, a.secret("y").await);
    a.manager
        .create(&a.definition("s", "y", &bucket), None)
        .await
        .unwrap();
    a.settled("s").await;
    // Left by a crash: a registration without a definition, one for a credential
    // the definition no longer names; and another instance's row.
    let owner = a.owner().to_string();
    a.db.secret_hold(&x, "ghost", &owner).unwrap();
    a.db.secret_hold(&x, "s", &owner).unwrap();
    a.db.secret_hold(&x, "t", "another-instance").unwrap();

    let root = a.stop();
    let a = Instance::start(&root, &database, &k);
    assert_eq!(a.owner(), owner, "the instance id survives a restart");
    assert!(a.refs("ghost").is_empty());
    assert_eq!(a.refs("s"), set(&[&y]));
    assert_eq!(
        a.db.secret_holds_of("another-instance").unwrap(),
        [("t".to_string(), x.clone())]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_write_keeps_what_an_earlier_runner_still_uses() {
    let dir = tempfile::tempdir().unwrap();
    let (database, _keeper) = store(dir.path());
    let bucket = refusing_bucket().await;
    let a = Instance::start(&dir.path().join("a"), &database, &key());
    let (x, y, z) = (
        a.secret("x").await,
        a.secret("y").await,
        a.secret("z").await,
    );
    // The first runner stops before it registers x, so it stays alive.
    let gate = a.hooks.hold("runner:credential:before-hold", 1);
    a.manager
        .create(&a.definition("s", "x", &bucket), None)
        .await
        .unwrap();
    a.settled("s").await;
    gate.reached().await;
    // s moves to y; the y runner waits for the x runner.
    a.manager
        .replace("s", &a.definition("s", "y", &bucket))
        .await
        .unwrap();
    assert_eq!(a.refs("s"), set(&[&x, &y]));

    // Moving on to z fails to publish: z's new registration is taken back,
    // and nothing else goes.
    a.manager.fail_next_write.store(true, Ordering::SeqCst);
    let err = a
        .manager
        .replace("s", &a.definition("s", "z", &bucket))
        .await
        .unwrap_err();
    assert!(matches!(err, ManagerError::Internal(_)), "{err:?}");
    assert!(a.file_text("s").contains("credential: y"));
    assert_eq!(a.refs("s"), set(&[&x, &y]));
    assert_eq!(in_use(a.delete_secret(&x).await), ["s"]);
    a.delete_secret(&z).await.unwrap();

    // Once the x runner ends, its credential is released.
    gate.release();
    a.settled("s").await;
    assert_eq!(a.refs("s"), set(&[&y]));
    a.delete_secret(&x).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transform_save_after_a_failed_start_runs_the_saved_definition() {
    let dir = tempfile::tempdir().unwrap();
    let (database, _keeper) = store(dir.path());
    let bucket = refusing_bucket().await;
    let a = Instance::start(&dir.path().join("a"), &database, &key());
    let (x, y) = (a.secret("x").await, a.secret("y").await);
    a.manager
        .create(&a.definition("s", "x", &bucket), None)
        .await
        .unwrap();
    a.settled("s").await;
    a.manager.fail_next_start.store(true, Ordering::SeqCst);
    let err = a
        .manager
        .replace("s", &a.definition("s", "y", &bucket))
        .await
        .unwrap_err();
    assert!(matches!(err, ManagerError::NotStarted(_)), "{err:?}");
    assert_eq!(a.refs("s"), set(&[&x, &y]));

    // Saving the transform restarts the source with the saved definition.
    a.manager
        .save_transform("t", "def transform(r, c):\n    return []\n")
        .await
        .unwrap();
    a.settled("s").await;
    assert!(
        a.manager
            .source("s")
            .unwrap()
            .source
            .credential()
            .is_some_and(|c| c == "y")
    );
    assert_eq!(in_use(a.delete_secret(&y).await), ["s"]);
    assert_eq!(a.refs("s"), set(&[&y]));
    a.delete_secret(&x).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_is_released_while_the_saved_and_running_definitions_differ() {
    let dir = tempfile::tempdir().unwrap();
    let (database, _keeper) = store(dir.path());
    let bucket = refusing_bucket().await;
    let a = Instance::start(&dir.path().join("a"), &database, &key());
    let (x, y) = (a.secret("x").await, a.secret("y").await);
    a.manager
        .create(&a.definition("s", "x", &bucket), None)
        .await
        .unwrap();
    a.settled("s").await;
    a.manager.fail_next_start.store(true, Ordering::SeqCst);
    a.manager
        .replace("s", &a.definition("s", "y", &bucket))
        .await
        .unwrap_err();
    // A cleanup that runs now, for any reason, keeps both.
    a.manager.cleanup_refs("s").await;
    assert_eq!(a.refs("s"), set(&[&x, &y]));
    assert_eq!(in_use(a.delete_secret(&y).await), ["s"]);
    assert_eq!(in_use(a.delete_secret(&x).await), ["s"]);
}

/// A source loaded from a file not named after its id, as `config::load`
/// accepts, is found by transform saves and cleanups like any other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sources_loaded_from_any_yaml_name_are_restarted_and_cleaned() {
    for file in ["s.yml", "hand-authored.yaml"] {
        let dir = tempfile::tempdir().unwrap();
        let (database, _keeper) = store(dir.path());
        let bucket = refusing_bucket().await;
        let k = key();
        let a = Instance::start(&dir.path().join("a"), &database, &k);
        let (x, y) = (a.secret("x").await, a.secret("y").await);
        let body = a.definition("s", "y", &bucket);
        let yaml = serde_yaml::to_string(&body).unwrap();
        let root = a.stop();
        std::fs::write(root.join("sources").join(file), yaml).unwrap();
        let a = Instance::start(&root, &database, &k);
        a.settled("s").await;
        assert_eq!(a.refs("s"), set(&[&y]), "{file}");
        // A registration left from before, which the definition no longer needs.
        a.db.secret_hold(&x, "s", a.owner()).unwrap();

        let before = a.manager.runner("s").unwrap();
        a.manager
            .save_transform("t", "def transform(r, c):\n    return []\n")
            .await
            .unwrap();
        let after = a.manager.runner("s").unwrap();
        assert!(!Arc::ptr_eq(&before, &after), "{file}: not restarted");
        a.settled("s").await;
        assert_eq!(a.refs("s"), set(&[&y]), "{file}: not cleaned");
        a.delete_secret(&x).await.unwrap();
        assert_eq!(in_use(a.delete_secret(&y).await), ["s"], "{file}");
    }
}
