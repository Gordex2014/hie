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
