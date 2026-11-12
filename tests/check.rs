//! `hie check` refuses to start while `HIE_RUNTIME_URL` or a `runtime:` key in
//! `hie.yaml` still points transforms at the remote runtime service, which the
//! engine no longer talks to; and both `check` and `run` refuse an S3 source
//! that does not say how it reaches its bucket.

use std::process::{Command, Output};

fn check(runtime_url: Option<&str>) -> Output {
    check_os(runtime_url.map(std::ffi::OsStr::new))
}

fn check_os(runtime_url: Option<&std::ffi::OsStr>) -> Output {
    check_config("fhir_base_url: http://127.0.0.1:1/fhir\n", runtime_url)
}

fn check_config(yaml: &str, runtime_url: Option<&std::ffi::OsStr>) -> Output {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("prod-engine.yaml");
    std::fs::write(&config, yaml).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_hie"));
    command
        .arg("--config")
        .arg(&config)
        .arg("check")
        .env_remove("HIE_CONFIG")
        .env_remove("HIE_RUNTIME_URL");
    if let Some(url) = runtime_url {
        command.env("HIE_RUNTIME_URL", url);
    }
    command.output().unwrap()
}

#[test]
fn check_fails_while_hie_runtime_url_is_set() {
    let out = check(Some("http://127.0.0.1:8790"));
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("HIE_RUNTIME_URL is no longer supported"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn check_passes_when_hie_runtime_url_is_unset_or_blank() {
    for url in [None, Some(""), Some("  ")] {
        let out = check(url);
        assert!(
            out.status.success(),
            "{url:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

#[test]
fn check_fails_while_hie_yaml_has_a_runtime_key() {
    let out = check_config(
        "fhir_base_url: http://127.0.0.1:1/fhir\nruntime:\n  url: http://127.0.0.1:8790\n",
        None,
    );
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("`runtime:` in ")
            && stderr.contains("prod-engine.yaml")
            && stderr.contains("is no longer supported"),
        "{stderr}"
    );
}

#[cfg(unix)]
#[test]
fn check_fails_while_hie_runtime_url_is_not_utf8() {
    use std::os::unix::ffi::OsStrExt;
    let out = check_os(Some(std::ffi::OsStr::from_bytes(
        b"http://127.0.0.1:8790/\xff",
    )));
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("HIE_RUNTIME_URL is no longer supported"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `hie <command>` on `dir/hie.yaml`, without any inherited key or store
/// variable, waiting at most a minute for it to exit.
fn engine(dir: &std::path::Path, command: &str, env: &[(&str, &str)]) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_hie"));
    c.arg("--config")
        .arg(dir.join("hie.yaml"))
        .arg(command)
        .env_remove("HIE_CONFIG")
        .env_remove("HIE_RUNTIME_URL")
        .env_remove("HIE_DATABASE_URL")
        .env_remove("HIE_SECRET_KEY")
        .env_remove("HIE_SECRET_KEY_FILE")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (k, v) in env {
        c.env(k, v);
    }
    let mut child = c.spawn().unwrap();
    let started = std::time::Instant::now();
    while child.try_wait().unwrap().is_none() {
        if started.elapsed() > std::time::Duration::from_secs(60) {
            let _ = child.kill();
            panic!("hie {command} did not exit");
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    child.wait_with_output().unwrap()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A root with `hie.yaml` (on a free port) and a transform.
fn root() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("hie.yaml"),
        format!(
            "fhir_base_url: http://127.0.0.1:1/fhir\ndatabase: data/hie.db\nlisten: 127.0.0.1:{}\n",
            free_port()
        ),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("t.py"),
        "def transform(r, c):\n    return []\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("sources")).unwrap();
    dir
}

#[test]
fn an_s3_file_without_credential_or_anonymous_refuses_check_and_run() {
    let dir = root();
    std::fs::write(
        dir.path().join("sources/synthea-s3.yaml"),
        "id: synthea-s3\nname: Synthea\nsource:\n  type: s3\n  bucket: b\nformat: ndjson\ntransform: t.py\n",
    )
    .unwrap();
    for command in ["check", "run"] {
        let out = engine(dir.path(), command, &[("AWS_ACCESS_KEY_ID", "AKIDFROMENV")]);
        assert!(!out.status.success(), "hie {command} started");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("synthea-s3.yaml")
                && stderr.contains("source synthea-s3")
                && stderr.contains("say how to reach the bucket"),
            "{command}: {stderr}"
        );
    }
}
