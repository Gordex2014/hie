//! `hie check` refuses to start while `HIE_RUNTIME_URL` or a `runtime:` key in
//! `hie.yaml` still points transforms at the remote runtime service, which the
//! engine no longer talks to.

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
