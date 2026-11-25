//! A build without a secret store starts with a source file that names a
//! credential: the file is skipped with the reason, the other sources run, and
//! `hie check` passes while saying what it skipped.
#![cfg(not(feature = "secrets-native"))]

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

/// Stops the engine whatever happens to the test.
struct Running(std::process::Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn root(dir: &Path, port: u16) {
    std::fs::write(
        dir.join("hie.yaml"),
        format!(
            "fhir_base_url: http://127.0.0.1:9/fhir\ndatabase: data/hie.db\nlisten: 127.0.0.1:{port}\n"
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("t.py"),
        "def transform(record, context):\n    return []\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.join("sources")).unwrap();
    std::fs::write(
        dir.join("sources/private.yaml"),
        "id: private\nname: Private\nsource:\n  type: s3\n  bucket: b\n  credential: lab\nformat: ndjson\ntransform: t.py\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("sources/inbox.yaml"),
        "id: inbox\nname: Inbox\nsource:\n  type: folder\n  path: inbox\ntransform: t.py\n",
    )
    .unwrap();
}

fn engine(dir: &Path, command: &str) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_hie"));
    c.arg("--config")
        .arg(dir.join("hie.yaml"))
        .arg(command)
        .env_remove("HIE_CONFIG")
        .env_remove("HIE_RUNTIME_URL")
        .env_remove("HIE_DATABASE_URL")
        .env_remove("HIE_SECRET_KEY")
        .env_remove("HIE_SECRET_KEY_FILE")
        .env("HIE_LOG", "info");
    c
}

#[tokio::test]
async fn a_credential_file_is_skipped_and_the_engine_starts() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    root(dir.path(), port);

    let check = engine(dir.path(), "check").output().unwrap();
    let stdout = String::from_utf8_lossy(&check.stdout);
    assert!(
        check.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&check.stderr)
    );
    assert!(
        stdout.contains("skipped ")
            && stdout.contains("private.yaml")
            && stdout.contains("needs a secret store"),
        "{stdout}"
    );

    let log = dir.path().join("engine.log");
    // The engine logs to standard output; a refusal goes to standard error.
    let output = std::fs::File::create(&log).unwrap();
    let mut child = Running(
        engine(dir.path(), "run")
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .spawn()
            .unwrap(),
    );
    let base = format!("http://127.0.0.1:{port}");
    let http = reqwest::Client::new();
    let started = Instant::now();
    let health = loop {
        if let Ok(r) = http.get(format!("{base}/health")).send().await {
            break r;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the engine did not start: {}",
            std::fs::read_to_string(&log).unwrap_or_default()
        );
        if let Some(status) = child.0.try_wait().unwrap() {
            panic!(
                "the engine exited ({status}): {}",
                std::fs::read_to_string(&log).unwrap_or_default()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(health.status(), 200);
    let health: serde_json::Value = health.json().await.unwrap();
    assert_eq!(health["secrets"]["backend"], "none");
    assert_eq!(health["secrets"]["available"], false);

    let sources: serde_json::Value = http
        .get(format!("{base}/api/sources"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ids: Vec<&str> = sources["sources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["inbox"]);
    let alerts: serde_json::Value = http
        .get(format!("{base}/api/alerts"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        alerts.to_string().contains("needs a secret store"),
        "{alerts}"
    );

    drop(child);
    let logged = std::fs::read_to_string(&log).unwrap();
    assert!(
        logged.contains("private.yaml")
            && logged.contains("needs a secret store")
            && logged.contains("the file is skipped"),
        "{logged}"
    );
}

/// Runs the engine on `dir` until dropped, once it answers.
async fn serve(dir: &Path, port: u16, log: &str) -> Running {
    let log = dir.join(log);
    let output = std::fs::File::create(&log).unwrap();
    let mut child = Running(
        engine(dir, "run")
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .spawn()
            .unwrap(),
    );
    let started = Instant::now();
    while reqwest::get(format!("http://127.0.0.1:{port}/health"))
        .await
        .is_err()
    {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the engine did not start: {}",
            std::fs::read_to_string(&log).unwrap_or_default()
        );
        if let Some(status) = child.0.try_wait().unwrap() {
            panic!(
                "the engine exited ({status}): {}",
                std::fs::read_to_string(&log).unwrap_or_default()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    child
}

/// The `definition` alerts of source `private`, newest first.
async fn definition_alerts(port: u16) -> Vec<serde_json::Value> {
    let alerts: serde_json::Value =
        reqwest::get(format!("http://127.0.0.1:{port}/api/alerts?source=private"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    alerts["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["kind"] == "definition")
        .cloned()
        .collect()
}

/// The `cleared` events of an alert's webhook, as it lists them.
fn clears(alert: &serde_json::Value) -> usize {
    alert["webhook"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["event"] == "cleared")
        .count()
}

/// The alert a skipped file raised clears at the first start that no longer
/// skips it, the file fixed or removed, with one `cleared` event.
#[tokio::test]
async fn a_skipped_file_alert_clears_once_the_file_is_not_skipped() {
    for change in ["fixed", "removed"] {
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        root(dir.path(), port);
        let raised = {
            let _engine = serve(dir.path(), port, "first.log").await;
            definition_alerts(port).await
        };
        assert_eq!(raised.len(), 1, "{raised:?}");
        assert!(raised[0]["cleared_at"].is_null(), "{raised:?}");
        let file = dir.path().join("sources/private.yaml");
        if change == "fixed" {
            std::fs::write(&file, PRIVATE_FOLDER).unwrap();
        } else {
            std::fs::remove_file(&file).unwrap();
        }
        let _engine = serve(dir.path(), port, "second.log").await;
        let after = definition_alerts(port).await;
        eprintln!("{change}: the definition alert after the second start: {after:?}");
        assert_eq!(after.len(), 1, "{change}: {after:?}");
        assert_eq!(after[0]["id"], raised[0]["id"], "{change}");
        assert!(
            !after[0]["cleared_at"].is_null(),
            "{change}: still open: {after:?}"
        );
        assert_eq!(
            after[0]["cleared_reason"], "private: its definition is no longer skipped",
            "{change}"
        );
        assert_eq!(clears(&after[0]), 1, "{change}: {after:?}");
    }
}

const PRIVATE_FOLDER: &str = "id: private
name: Private
source:
  type: folder
  path: private-inbox
transform: t.py
";

/// A start that still skips a file of the source keeps its alert open, also
/// when another file of the same id loads.
#[tokio::test]
async fn a_skipped_file_alert_stays_while_a_file_of_the_source_is_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    root(dir.path(), port);
    drop(serve(dir.path(), port, "first.log").await);
    drop(serve(dir.path(), port, "unchanged.log").await);
    std::fs::write(
        dir.path().join("sources/private-folder.yaml"),
        PRIVATE_FOLDER,
    )
    .unwrap();
    let _engine = serve(dir.path(), port, "sibling.log").await;
    let alerts = definition_alerts(port).await;
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert!(alerts[0]["cleared_at"].is_null(), "{alerts:?}");
    assert_eq!(clears(&alerts[0]), 0, "{alerts:?}");
}

/// Another instance on the store, which does not skip the file, leaves the
/// alert open: it is the instance that skips it that clears it. An alert an
/// earlier version raised names no instance: it stays until an instance that
/// skips the file has taken it as its own, then clears as that one's.
#[tokio::test]
async fn only_the_instance_that_skips_a_file_clears_its_alert() {
    let a = tempfile::tempdir().unwrap();
    let a_port = free_port();
    root(a.path(), a_port);
    drop(serve(a.path(), a_port, "first.log").await);
    // Instance b: its own sources, the same store.
    let b = tempfile::tempdir().unwrap();
    let b_port = free_port();
    root(b.path(), b_port);
    std::fs::remove_file(b.path().join("sources/private.yaml")).unwrap();
    let database = a.path().join("data/hie.db");
    std::fs::write(
        b.path().join("hie.yaml"),
        format!(
            "fhir_base_url: http://127.0.0.1:9/fhir
database: {}
listen: 127.0.0.1:{b_port}
",
            database.display()
        ),
    )
    .unwrap();
    let open = |port| async move {
        let alerts = definition_alerts(port).await;
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        alerts[0]["cleared_at"].is_null()
    };
    {
        let _b = serve(b.path(), b_port, "b.log").await;
        assert!(open(b_port).await, "b cleared a's alert");
    }
    // As an earlier version left it.
    rusqlite::Connection::open(&database)
        .unwrap()
        .execute("UPDATE alerts SET raised_by = NULL", [])
        .unwrap();
    {
        let _b = serve(b.path(), b_port, "b-legacy.log").await;
        assert!(
            open(b_port).await,
            "b cleared an alert of an earlier version"
        );
    }
    // a still skips it: it takes the alert, and clears it once fixed.
    drop(serve(a.path(), a_port, "a-adopts.log").await);
    std::fs::write(a.path().join("sources/private.yaml"), PRIVATE_FOLDER).unwrap();
    let _a = serve(a.path(), a_port, "a-fixed.log").await;
    let alerts = definition_alerts(a_port).await;
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert!(!alerts[0]["cleared_at"].is_null(), "{alerts:?}");
    assert_eq!(clears(&alerts[0]), 1, "{alerts:?}");
}
