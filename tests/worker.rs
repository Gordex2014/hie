//! The transform worker as a host of the `limited-process@1` contract's
//! long-lived worker pattern: the real `hie_runtime/limits.py` runs a small test
//! program that speaks HIE's worker protocol (`{"ready": true}`, then one
//! `{"id", "resources"}` reply per request) and misbehaves on request.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hie::transform::{Outcome, Transformer};
use hie::worker::{Verdict, WorkerLimits};
use serde_json::{Value, json};

/// The test program, installed as the runtime's `worker.py`. Its "transform"
/// argument is a JSON file: `{"mode": ..., "state": dir}`. Each request's
/// record says what to do with it.
const PROGRAM: &str = r#"
import json, os, subprocess, sys, threading, time

config = json.load(open(sys.argv[1]))
mode = config.get("mode", "normal")
state = config["state"]

def note(name, value):
    path = os.path.join(state, name)
    with open(path + ".tmp", "w") as f:
        json.dump(value, f)
    os.replace(path + ".tmp", path)

note("started-%d" % os.getpid(), {
    "pid": os.getpid(),
    "pgid": os.getpgid(0) if hasattr(os, "getpgid") else None,
    "cwd": os.getcwd(),
})
if mode == "marker":
    open(os.path.join(state, "marker"), "w").close()
if mode == "exit-before-ready":
    sys.stderr.write("boom before ready\n")
    sys.exit(0)
if mode != "never-ready":
    print(json.dumps({"ready": True}))
if mode in ("never-ready", "not-reading"):
    time.sleep(60)

linger = 0
for line in sys.stdin:
    request = json.loads(line)
    record = request["record"]
    with open(os.path.join(state, "seen"), "a") as f:
        f.write(json.dumps(record.get("echo")) + "\n")
    if record.get("spawn"):
        # A child that keeps stdin, stdout and stderr open after this process goes.
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        note("grandchild", child.pid)
    if record.get("escape"):
        # A child outside the process group, holding the pipes for a while.
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(8)"],
                                 start_new_session=True)
        note("escaped", child.pid)
    if "linger" in record:
        linger = record["linger"]
    if "exit_after" in record:
        threading.Timer(record["exit_after"], os._exit, [record.get("code", 0)]).start()
    if "sleep" in record:
        time.sleep(record["sleep"])
    if record.get("hang"):
        time.sleep(3600)
    if record.get("grandchild"):
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        note("grandchild", child.pid)
        time.sleep(3600)
    if "exit" in record:
        os._exit(record["exit"])
    if record.get("stderr_mib"):
        chunk = "e" * (1024 * 1024)
        for _ in range(record["stderr_mib"]):
            sys.stderr.write(chunk)
        sys.stderr.write("end-of-flood\n")
    if record.get("overlong"):
        sys.stdout.write("x" * (33 * 1024 * 1024))
        time.sleep(60)
    if "stray" in record:
        # Not the protocol: a line on stdout before the reply.
        sys.stdout.write(record["stray"] + "\n")
    resource = {"echo": record.get("echo"), "pid": os.getpid(), "cwd": os.getcwd(),
                "listing": sorted(os.listdir("."))}
    if record.get("env"):
        resource["env"] = dict(os.environ)
    if record.get("rlimits"):
        import resource as r
        resource["rlimits"] = {name: list(r.getrlimit(getattr(r, name)))
                               for name in ("RLIMIT_AS", "RLIMIT_FSIZE", "RLIMIT_NOFILE", "RLIMIT_CPU")}
    reply_id = request["id"] + (100 if record.get("wrong_id") else 0)
    reply = json.dumps({"id": reply_id, "resources": [resource]})
    if mode == "split-writes":
        sys.stdout.write(reply[:5])
        time.sleep(0.1)
        sys.stdout.write(reply[5:] + "\n")
    else:
        sys.stdout.write(reply + "\n")
# Asked not to exit when stdin closes.
time.sleep(linger)
"#;

/// Slack for the scheduler on top of a deadline and the cleanup budget.
const SLACK: Duration = Duration::from_millis(2500);

fn python() -> Option<String> {
    let candidates = match std::env::var("HIE_TEST_PYTHON") {
        Ok(p) => vec![p],
        Err(_) => vec!["python3".into(), "python".into(), "py".into()],
    };
    candidates.into_iter().find(|p| {
        std::process::Command::new(p)
            .arg("-c")
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// A runtime directory with the real launcher and the test program, a state
/// directory the program writes to, and the root the run directories go in.
struct Rig {
    _dir: tempfile::TempDir,
    root: PathBuf,
    runtime: PathBuf,
    state: PathBuf,
    runs: PathBuf,
    python: String,
}

impl Rig {
    fn new(python: String) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let runtime = root.join("runtime");
        let state = root.join("state");
        let runs = root.join("runs");
        for d in [&runtime, &state, &runs] {
            std::fs::create_dir_all(d).unwrap();
        }
        std::fs::copy(
            repo().join("hie_runtime/limits.py"),
            runtime.join("limits.py"),
        )
        .unwrap();
        std::fs::write(runtime.join("worker.py"), PROGRAM).unwrap();
        Self {
            _dir: dir,
            root,
            runtime,
            state,
            runs,
            python,
        }
    }

    fn transformer(&self, mode: &str, limits: WorkerLimits) -> Transformer {
        let config = self.root.join(format!("{mode}.json"));
        std::fs::write(
            &config,
            json!({"mode": mode, "state": self.state}).to_string(),
        )
        .unwrap();
        let mut transformer = Transformer::new(&self.python, &self.runtime, &config);
        transformer.set_limits(limits);
        transformer.set_run_root(&self.runs);
        transformer
    }

    fn seen(&self) -> Vec<Value> {
        std::fs::read_to_string(self.state.join("seen"))
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn run_dirs(&self) -> Vec<PathBuf> {
        std::fs::read_dir(&self.runs)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect()
    }

    /// The `started-*` notes of every process so far.
    fn started(&self) -> Vec<Value> {
        std::fs::read_dir(&self.state)
            .unwrap()
            .filter_map(|e| {
                let path = e.unwrap().path();
                let name = path.file_name()?.to_str()?.to_string();
                (name.starts_with("started-") && !name.ends_with(".tmp"))
                    .then(|| serde_json::from_str(&std::fs::read_to_string(&path).ok()?).ok())
                    .flatten()
            })
            .collect()
    }
}

/// The conformance cases' budgets: 1.5 s per request, 5 s to start.
fn limits() -> WorkerLimits {
    WorkerLimits {
        request: Duration::from_millis(1500),
        startup: Duration::from_secs(5),
        ..WorkerLimits::default()
    }
}

fn ctx() -> Value {
    json!({"feed": "t", "file": "t", "line": 1})
}

fn reply(outcome: Outcome) -> Value {
    match outcome {
        Outcome::Resources(mut resources) => resources.remove(0),
        other => panic!("expected a reply, got {other:?}"),
    }
}

fn rejection(outcome: Outcome) -> String {
    match outcome {
        Outcome::Rejected { kind, message } => {
            assert_eq!(kind, "transform", "{message}");
            message
        }
        other => panic!("expected a rejection, got {other:?}"),
    }
}

macro_rules! need_python {
    () => {
        match python() {
            Some(p) => Rig::new(p),
            None => {
                eprintln!("skipping: no Python 3.11+ found (set HIE_TEST_PYTHON)");
                return;
            }
        }
    };
}

#[cfg(unix)]
async fn poll_until(what: &str, within: Duration, mut done: impl FnMut() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(started.elapsed() < within, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The engine's log lines, for the reports it writes.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Logs {
    /// Captures this thread's log lines (the tests run on one thread) until
    /// the guard is dropped.
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let logs = Logs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer({
                let logs = logs.clone();
                move || logs.clone()
            })
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (logs, guard)
    }

    fn lines(&self, needle: &str) -> Vec<String> {
        String::from_utf8_lossy(&self.0.lock().unwrap())
            .lines()
            .filter(|l| l.contains(needle))
            .map(str::to_string)
            .collect()
    }
}

#[tokio::test]
async fn echo_answers_in_order_from_one_process() {
    let rig = need_python!();
    let t = rig.transformer("normal", limits());
    let mut pids = Vec::new();
    for echo in ["a", "b", "c"] {
        let r = reply(t.transform(&json!({"echo": echo}), &ctx()).await.unwrap());
        assert_eq!(r["echo"], echo);
        pids.push(r["pid"].clone());
    }
    assert!(pids.iter().all(|p| p == &pids[0]), "{pids:?}");
    let report = t.close().await.expect("a worker was running");
    assert_eq!(report.verdict, Verdict::Completed);
    assert_eq!(report.exit_code, Some(0));
    assert_eq!(report.signal, None);
    assert_eq!(report.cleanup, "complete");
    assert_eq!(report.contract, "limited-process@1");
    assert_eq!(
        report.reference_sha256.as_deref(),
        Some(hie::worker::pin().sha256)
    );
    assert!(rig.run_dirs().is_empty(), "{:?}", rig.run_dirs());
}

#[tokio::test]
async fn buffering_a_reply_in_two_pieces_is_one_reply() {
    let rig = need_python!();
    let t = rig.transformer("split-writes", limits());
    for echo in ["a", "b"] {
        let r = reply(t.transform(&json!({"echo": echo}), &ctx()).await.unwrap());
        assert_eq!(r["echo"], echo);
    }
    assert_eq!(t.close().await.unwrap().verdict, Verdict::Completed);
}

#[tokio::test]
async fn deadline_a_request_past_it_is_rejected_and_a_new_process_answers_the_next() {
    let rig = need_python!();
    let t = rig.transformer("normal", limits());
    let first = reply(t.transform(&json!({"echo": "a"}), &ctx()).await.unwrap());
    let started = Instant::now();
    let message = rejection(t.transform(&json!({"hang": true}), &ctx()).await.unwrap());
    let took = started.elapsed();
    assert!(
        message.contains("did not answer within 1.5 s") && message.contains("verdict timed_out"),
        "{message}"
    );
    assert!(
        took >= Duration::from_millis(1400) && took < Duration::from_millis(1500) + SLACK,
        "{took:?}"
    );
    let next = reply(t.transform(&json!({"echo": "b"}), &ctx()).await.unwrap());
    assert_eq!(next["echo"], "b");
    assert_ne!(next["pid"], first["pid"]);
    assert_eq!(t.close().await.unwrap().exit_code, Some(0));
}

#[tokio::test]
async fn late_reply_is_never_taken_for_the_next_request() {
    let rig = need_python!();
    let t = rig.transformer("normal", limits());
    let message = rejection(
        t.transform(&json!({"echo": "late", "sleep": 2.5}), &ctx())
            .await
            .unwrap(),
    );
    assert!(message.contains("did not answer"), "{message}");
    let next = reply(t.transform(&json!({"echo": "b"}), &ctx()).await.unwrap());
    assert_eq!(next["echo"], "b");
    // Past the moment the first process would have answered: still no stray reply.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let after = reply(t.transform(&json!({"echo": "c"}), &ctx()).await.unwrap());
    assert_eq!(after["echo"], "c");
    assert_eq!(after["pid"], next["pid"]);
    t.close().await;
}

#[tokio::test]
async fn eof_before_ready_is_unavailable_with_its_stderr() {
    let rig = need_python!();
    let t = rig.transformer("exit-before-ready", limits());
    let err = t
        .transform(&json!({"echo": "a"}), &ctx())
        .await
        .unwrap_err();
    let message = format!("{err:#}");
    assert!(
        message.contains("exited before it was ready") && message.contains("boom before ready"),
        "{message}"
    );
    assert!(rig.seen().is_empty());
    assert!(rig.run_dirs().is_empty());
}

#[tokio::test]
async fn ready_never_is_unavailable_within_the_startup_budget() {
    let rig = need_python!();
    let budget = Duration::from_millis(1500);
    let t = rig.transformer(
        "never-ready",
        WorkerLimits {
            startup: budget,
            ..limits()
        },
    );
    for echo in ["a", "b"] {
        let started = Instant::now();
        let err = t
            .transform(&json!({"echo": echo}), &ctx())
            .await
            .unwrap_err();
        let took = started.elapsed();
        assert!(
            format!("{err:#}").contains("not ready within 1.5 s"),
            "{err:#}"
        );
        assert!(
            took >= Duration::from_millis(1400) && took < budget + SLACK,
            "{took:?}"
        );
    }
    assert!(rig.run_dirs().is_empty());
}

#[tokio::test]
async fn stdin_not_read_times_out_while_the_write_is_blocked() {
    let rig = need_python!();
    let t = rig.transformer("not-reading", limits());
    let big = "x".repeat(8 * 1024 * 1024);
    let started = Instant::now();
    let message = rejection(t.transform(&json!({"echo": big}), &ctx()).await.unwrap());
    let took = started.elapsed();
    assert!(message.contains("did not answer within 1.5 s"), "{message}");
    assert!(
        took >= Duration::from_millis(1400) && took < Duration::from_millis(1500) + SLACK,
        "{took:?}"
    );
}

#[tokio::test]
async fn an_overlong_reply_or_a_wrong_id_ends_the_process() {
    let rig = need_python!();
    let t = rig.transformer("normal", limits());
    let first = reply(t.transform(&json!({"echo": "a"}), &ctx()).await.unwrap());
    let message = rejection(
        t.transform(&json!({"overlong": true}), &ctx())
            .await
            .unwrap(),
    );
    assert!(message.contains("exceeded 32 MiB"), "{message}");
    let second = reply(t.transform(&json!({"echo": "b"}), &ctx()).await.unwrap());
    assert_ne!(second["pid"], first["pid"]);
    let message = rejection(
        t.transform(&json!({"wrong_id": true}), &ctx())
            .await
            .unwrap(),
    );
    assert!(message.contains("out of order"), "{message}");
    let third = reply(t.transform(&json!({"echo": "c"}), &ctx()).await.unwrap());
    assert_ne!(third["pid"], second["pid"]);
    t.close().await;
}

#[tokio::test]
async fn a_line_that_is_not_json_ends_the_process_and_the_next_record_gets_its_own_reply() {
    let rig = need_python!();
    let t = rig.transformer("normal", limits());
    let first = reply(t.transform(&json!({"echo": "a"}), &ctx()).await.unwrap());
    let message = rejection(
        t.transform(&json!({"echo": "stray", "stray": "stray"}), &ctx())
            .await
            .unwrap(),
    );
    assert!(
        message.starts_with(
            "something wrote to the worker's stdout: \"stray\"; its worker was restarted (verdict user_error, cause unknown)"
        ),
        "{message}"
    );
    // The reply the old process wrote after the stray line is never read as
    // this one's.
    let next = reply(t.transform(&json!({"echo": "b"}), &ctx()).await.unwrap());
    assert_eq!(next["echo"], "b");
    assert_ne!(next["pid"], first["pid"]);
    let after = reply(t.transform(&json!({"echo": "c"}), &ctx()).await.unwrap());
    assert_eq!(after["echo"], "c");
    assert_eq!(after["pid"], next["pid"]);
    // A long stray line is cut short in the message.
    let long = "y".repeat(10_000);
    let message = rejection(t.transform(&json!({"stray": long}), &ctx()).await.unwrap());
    assert!(message.len() < 1000, "{} bytes: {message}", message.len());
    t.close().await;
}

#[tokio::test]
async fn a_process_that_dies_on_a_record_rejects_it_once() {
    let rig = need_python!();
    let t = rig.transformer("normal", limits());
    let message = rejection(
        t.transform(&json!({"echo": "dies", "exit": 3}), &ctx())
            .await
            .unwrap(),
    );
    assert!(
        message.contains("exited with code 3") && message.contains("verdict user_error"),
        "{message}"
    );
    let next = reply(t.transform(&json!({"echo": "b"}), &ctx()).await.unwrap());
    assert_eq!(next["echo"], "b");
    assert_eq!(rig.seen(), [json!("dies"), json!("b")]);
    t.close().await;
}

/// Watches every run directory's `control/ack` while `f` runs.
async fn watching_acks<T>(runs: &Path, f: impl std::future::Future<Output = T>) -> (T, bool) {
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watcher = {
        let (stop, seen, runs) = (stop.clone(), seen.clone(), runs.to_path_buf());
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                for run in std::fs::read_dir(&runs).into_iter().flatten().flatten() {
                    if run.path().join("control/ack").exists() {
                        seen.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };
    let out = f.await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    watcher.join().unwrap();
    (out, seen.load(std::sync::atomic::Ordering::SeqCst))
}

/// `limits.py` with one byte changed, and its SHA-256.
fn one_byte_changed(launcher: &Path) -> (String, String) {
    let original = std::fs::read_to_string(launcher).unwrap();
    let changed = original.replacen("Reference launcher", "reference launcher", 1);
    assert_eq!(changed.len(), original.len());
    assert_ne!(changed, original);
    let sha = {
        use sha2::Digest;
        hex::encode(sha2::Sha256::digest(changed.as_bytes()))
    };
    (changed, sha)
}

/// Replaces the rig's interpreter with a shell script that runs `body` and
/// then `exec`s the real one, keeping its process id.
#[cfg(unix)]
fn wrap_interpreter(rig: &mut Rig, body: &str) {
    use std::os::unix::fs::PermissionsExt;
    let wrapper = rig.root.join("python-wrapper");
    let real = resolve(&rig.python);
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\n{}\n",
            body.replace("PYTHON", &format!("\"{real}\""))
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    rig.python = wrapper.to_str().unwrap().to_string();
}

/// The interpreter's absolute path, as the engine would find it on `PATH`.
#[cfg(unix)]
fn resolve(python: &str) -> String {
    let out = std::process::Command::new(python)
        .args(["-c", "import sys; print(sys.executable)"])
        .output()
        .unwrap();
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

#[tokio::test]
async fn a_launcher_that_does_not_match_the_pin_never_runs_the_program() {
    #[allow(unused_mut)]
    let mut rig = need_python!();
    let launcher = rig.runtime.join("limits.py");
    let (changed, changed_sha) = one_byte_changed(&launcher);
    std::fs::write(&launcher, changed).unwrap();
    // Notes every time the interpreter is started.
    #[cfg(unix)]
    let invoked = {
        let invoked = rig.root.join("invoked");
        wrap_interpreter(
            &mut rig,
            &format!(
                "echo started >> \"{}\"\nexec PYTHON \"$@\"",
                invoked.display()
            ),
        );
        invoked
    };
    let (logs, _guard) = Logs::capture();
    let t = rig.transformer("marker", limits());
    for _ in 0..2 {
        let message = format!(
            "{:#}",
            t.transform(&json!({"echo": "a"}), &ctx())
                .await
                .unwrap_err()
        );
        assert!(
            message.contains(&format!(
                "has SHA-256 {changed_sha}, which does not match the pinned {}",
                hie::worker::pin().sha256
            )),
            "{message}"
        );
    }
    // No process was started: the interpreter never ran, so neither did the
    // launcher nor the program, and there is no process to report.
    #[cfg(unix)]
    assert!(!invoked.exists(), "the interpreter was started");
    assert!(
        logs.lines("transform worker").is_empty(),
        "{:?}",
        logs.lines("")
    );
    assert!(!rig.state.join("marker").exists(), "the program ran");
    assert!(rig.started().is_empty());
    assert!(rig.run_dirs().is_empty());
}

/// The file on disk matches the pin, but the interpreter runs another
/// launcher: the launcher's own report of its contract and hash is checked too.
#[cfg(unix)]
#[tokio::test]
async fn a_launcher_that_reports_another_contract_or_hash_is_unavailable() {
    let mut rig = need_python!();
    let original = std::fs::read_to_string(rig.runtime.join("limits.py")).unwrap();
    let other_contract = rig.root.join("other-contract.py");
    std::fs::write(
        &other_contract,
        original.replacen(
            "CONTRACT = \"limited-process@1\"",
            "CONTRACT = \"limited-process@2\"",
            1,
        ),
    )
    .unwrap();
    let other_hash = rig.root.join("other-hash.py");
    let (changed, changed_sha) = one_byte_changed(&rig.runtime.join("limits.py"));
    std::fs::write(&other_hash, changed).unwrap();
    let choice = rig.root.join("substitute");
    // `python -I -B -u -X utf8 <launcher> …`: the sixth argument is swapped.
    wrap_interpreter(
        &mut rig,
        &format!(
            "a1=$1 a2=$2 a3=$3 a4=$4 a5=$5\nshift 6\nexec PYTHON \"$a1\" \"$a2\" \"$a3\" \"$a4\" \"$a5\" \"$(cat \"{}\")\" \"$@\"",
            choice.display()
        ),
    );
    let t = rig.transformer("marker", limits());
    for (substitute, expected) in [
        (
            &other_contract,
            "follows \"limited-process@2\", not the pinned limited-process@1".to_string(),
        ),
        (
            &other_hash,
            format!(
                "the launcher's hash {changed_sha} does not match the pinned {}",
                hie::worker::pin().sha256
            ),
        ),
    ] {
        std::fs::write(&choice, substitute.to_str().unwrap()).unwrap();
        let (result, acked) =
            watching_acks(&rig.runs, t.transform(&json!({"echo": "a"}), &ctx())).await;
        let message = format!("{:#}", result.unwrap_err());
        assert!(message.contains(&expected), "{message}");
        assert!(!acked, "the host acknowledged a launcher it does not trust");
    }
    assert!(!rig.state.join("marker").exists(), "the program ran");
    assert!(rig.started().is_empty());
    assert!(rig.run_dirs().is_empty());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_limit_the_launcher_cannot_apply_is_unavailable_with_its_message() {
    let rig = need_python!();
    let (logs, _guard) = Logs::capture();
    let t = rig.transformer(
        "marker",
        WorkerLimits {
            open_files: 1 << 40,
            ..limits()
        },
    );
    let err = t
        .transform(&json!({"echo": "a"}), &ctx())
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("could not set up the worker: limit_open_files"),
        "{err:#}"
    );
    assert!(!rig.state.join("marker").exists());
    assert!(rig.run_dirs().is_empty());
    let reports = logs.lines("transform worker retired");
    assert_eq!(reports.len(), 1, "{reports:?}");
    assert!(
        reports[0].contains(&format!(
            "\"reference_sha256\":\"{}\"",
            hie::worker::pin().sha256
        )) && reports[0].contains("\"not_enforced\":[]")
            && reports[0].contains("\"verdict\":\"unavailable\""),
        "{}",
        reports[0]
    );
}

#[tokio::test]
async fn a_stderr_flood_neither_blocks_the_worker_nor_grows_without_bound() {
    let rig = need_python!();
    let t = rig.transformer("normal", limits());
    let r = reply(
        t.transform(&json!({"echo": "after", "stderr_mib": 10}), &ctx())
            .await
            .unwrap(),
    );
    assert_eq!(r["echo"], "after");
    let report = t.close().await.unwrap();
    assert_eq!(report.verdict, Verdict::Completed);
    assert!(report.stderr_truncated);
    assert_eq!(report.stderr_tail_bytes, 64 * 1024);
    // Windows writes the line end as \r\n.
    assert!(
        report.stderr_tail.trim_end().ends_with("eeeeend-of-flood"),
        "{:?}",
        &report.stderr_tail[report.stderr_tail.len() - 40..]
    );
}

#[tokio::test]
async fn each_process_works_in_a_fresh_directory_that_goes_with_it() {
    let rig = need_python!();
    let t = rig.transformer("normal", limits());
    let first = reply(t.transform(&json!({"echo": "a"}), &ctx()).await.unwrap());
    let first_cwd = PathBuf::from(first["cwd"].as_str().unwrap());
    assert!(first_cwd.ends_with("work"), "{first_cwd:?}");
    assert_eq!(first["listing"], json!([]));
    let run_dir = first_cwd.parent().unwrap().to_path_buf();
    assert!(run_dir.join("control").is_dir());
    assert_eq!(run_dir.parent().unwrap(), rig.runs);
    #[cfg(unix)]
    for dir in [&run_dir, &first_cwd, &run_dir.join("control")] {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "{dir:?} is {mode:o}");
    }

    rejection(t.transform(&json!({"hang": true}), &ctx()).await.unwrap());
    assert!(
        !run_dir.exists(),
        "the timed-out process's run directory stayed"
    );
    let second = reply(t.transform(&json!({"echo": "b"}), &ctx()).await.unwrap());
    let second_cwd = PathBuf::from(second["cwd"].as_str().unwrap());
    assert_ne!(second_cwd, first_cwd);
    assert!(second_cwd.exists());
    t.close().await;
    assert!(!second_cwd.exists());
    assert!(rig.run_dirs().is_empty());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_launcher_applies_the_configured_limits_and_no_cpu_limit() {
    let rig = need_python!();
    let t = rig.transformer(
        "normal",
        WorkerLimits {
            memory_mib: 1024,
            file_mib: 64,
            open_files: 128,
            ..limits()
        },
    );
    let r = reply(
        t.transform(&json!({"rlimits": true}), &ctx())
            .await
            .unwrap(),
    );
    let mib = 1024 * 1024u64;
    let pair = |name: &str| -> (u64, u64) {
        let v = &r["rlimits"][name];
        (v[0].as_u64().unwrap(), v[1].as_u64().unwrap())
    };
    assert_eq!(pair("RLIMIT_AS"), (1024 * mib, 1024 * mib));
    assert_eq!(pair("RLIMIT_FSIZE"), (64 * mib, 64 * mib));
    assert_eq!(pair("RLIMIT_NOFILE"), (128, 128));
    let mut own = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit fills the struct it is given.
    assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_CPU, &mut own) }, 0);
    // Python reports RLIM_INFINITY as -1.
    let as_reported = |v: libc::rlim_t| {
        if v == libc::RLIM_INFINITY {
            json!(-1)
        } else {
            json!(v)
        }
    };
    assert_eq!(
        r["rlimits"]["RLIMIT_CPU"],
        json!([as_reported(own.rlim_cur), as_reported(own.rlim_max)])
    );
    t.close().await;
}

#[cfg(target_os = "linux")]
fn gone_or_zombie(pid: i64) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Err(_) => true,
        // `pid (comm) state …`: the state follows the last ')'.
        Ok(stat) => stat
            .rsplit_once(')')
            .is_some_and(|(_, rest)| rest.trim_start().starts_with('Z')),
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_timeout_kills_what_the_program_started() {
    let rig = need_python!();
    let t = rig.transformer("normal", limits());
    rejection(
        t.transform(&json!({"grandchild": true}), &ctx())
            .await
            .unwrap(),
    );
    let grandchild: i64 =
        serde_json::from_str(&std::fs::read_to_string(rig.state.join("grandchild")).unwrap())
            .unwrap();
    poll_until(
        "the grandchild to be killed",
        Duration::from_secs(2),
        || gone_or_zombie(grandchild),
    )
    .await;
}

/// The child's environment, probed from a separate test process so that the
/// variables this test sets never touch the test runner's own environment.
#[test]
fn the_worker_environment_is_built_from_nothing() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.11+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let out = tempfile::NamedTempFile::new().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "env_probe", "--ignored", "--test-threads=1"])
        .env("HIE_ENV_PROBE_OUT", out.path())
        .env("HIE_TEST_PYTHON", python)
        .env("HIE_ENGINE_ONLY", "not for the transform")
        .env("PYTHONPATH", "/nowhere")
        .env("PYTHONSTARTUP", "/nowhere/startup.py")
        .env("LANG", "C.UTF-8")
        .env("HIE_TERMINOLOGY_TOKEN", "token-for-hts")
        .status()
        .unwrap();
    assert!(status.success());
    let env: serde_json::Map<String, Value> =
        serde_json::from_str(&std::fs::read_to_string(out.path()).unwrap()).unwrap();
    assert!(!env.contains_key("HIE_ENGINE_ONLY"), "{env:?}");
    assert!(!env.keys().any(|k| k.starts_with("PYTHON")), "{env:?}");
    assert!(env.contains_key("PATH"));
    assert_eq!(env["LANG"], "C.UTF-8");
    assert_eq!(env["HIE_TERMINOLOGY_SERVER"], "http://hts.test/fhir");
    assert_eq!(env["HIE_TERMINOLOGY_TOKEN"], "token-for-hts");
    assert_eq!(env["OMP_NUM_THREADS"], "1");
    let allowed = [
        "PATH",
        "LANG",
        "LC_ALL",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "OPENBLAS_NUM_THREADS",
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "HIE_TERMINOLOGY_SERVER",
        "HIE_TERMINOLOGY_TOKEN",
    ];
    let unexpected: Vec<_> = env
        .keys()
        .filter(|k| !allowed.contains(&k.as_str()))
        .collect();
    assert!(unexpected.is_empty(), "{unexpected:?}");
}

/// The other half of the test above: runs only when that test starts it.
#[test]
#[ignore = "run by the_worker_environment_is_built_from_nothing"]
fn env_probe() {
    let Some(out) = std::env::var_os("HIE_ENV_PROBE_OUT") else {
        return;
    };
    let rig = Rig::new(python().expect("the parent found Python"));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let env = runtime.block_on(async {
        let mut t = rig.transformer("normal", limits());
        t.set_terminology(Some("http://hts.test/fhir"));
        let r = reply(t.transform(&json!({"env": true}), &ctx()).await.unwrap());
        t.close().await;
        r["env"].clone()
    });
    std::fs::write(out, env.to_string()).unwrap();
}

// ---- cancellation: a dropped call takes its process and directory with it ----

#[cfg(unix)]
fn group_gone(pgid: i64) -> bool {
    // SAFETY: signal 0 only checks whether the group exists.
    unsafe { libc::killpg(pgid as libc::pid_t, 0) != 0 }
}

/// Drops the call once `ready` holds, then checks that the process group and
/// the run directory are gone and that one cancelled report was logged.
#[cfg(unix)]
async fn cancel_when(rig: &Rig, mode: &str, record: Value, ready: impl Fn(&Rig) -> bool) {
    let (logs, _guard) = Logs::capture();
    let t = rig.transformer(mode, limits());
    {
        let context = ctx();
        let call = t.transform(&record, &context);
        tokio::pin!(call);
        let started = Instant::now();
        loop {
            tokio::select! {
                outcome = &mut call => panic!("the call finished: {outcome:?}"),
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
            if ready(rig) {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(4),
                "never got ready to cancel"
            );
        }
    }
    let note = rig.started().pop().expect("the program started");
    let pgid = note["pgid"].as_i64().unwrap();
    let run_dir = PathBuf::from(note["cwd"].as_str().unwrap())
        .parent()
        .unwrap()
        .to_path_buf();
    poll_until(
        "the group and its run directory to go",
        Duration::from_secs(3),
        || group_gone(pgid) && !run_dir.exists(),
    )
    .await;
    poll_until("the report", Duration::from_secs(3), || {
        logs.lines("transform worker retired").len() == 1
    })
    .await;
    assert_eq!(logs.lines("\"verdict\":\"cancelled\"").len(), 1);
    // Nothing else of that process is reported later.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(logs.lines("transform worker retired").len(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn cancelling_during_startup_leaves_nothing_behind() {
    let rig = need_python!();
    cancel_when(&rig, "never-ready", json!({"echo": "a"}), |rig| {
        !rig.started().is_empty()
    })
    .await;
}

#[cfg(unix)]
#[tokio::test]
async fn cancelling_during_a_blocked_write_leaves_nothing_behind() {
    let rig = need_python!();
    let big = "x".repeat(8 * 1024 * 1024);
    let since = Mutex::new(None::<Instant>);
    cancel_when(&rig, "not-reading", json!({"echo": big}), |rig| {
        // Started, then a moment for the write to fill the pipe and block.
        let mut since = since.lock().unwrap();
        if rig.started().is_empty() {
            return false;
        }
        since.get_or_insert_with(Instant::now).elapsed() > Duration::from_millis(300)
    })
    .await;
}

#[cfg(unix)]
#[tokio::test]
async fn cancelling_during_a_reply_leaves_nothing_behind() {
    let rig = need_python!();
    cancel_when(
        &rig,
        "normal",
        json!({"echo": "slow", "sleep": 30}),
        |rig| !rig.seen().is_empty(),
    )
    .await;
}

// ---- the real worker: a write to file descriptor 1 is not a reply ----------

/// The real `hie_runtime/` (launcher, `worker.py`, `hie.py`) running `script`.
fn real_worker(python: &str, dir: &Path, script: &str) -> Transformer {
    let path = dir.join("transform.py");
    std::fs::write(&path, script).unwrap();
    let mut t = Transformer::new(python, &repo().join("hie_runtime"), &path);
    t.set_limits(limits());
    t
}

/// Sends records 1..=5 and expects each one's own resource back.
async fn every_record_gets_its_own_reply(t: &Transformer) -> Vec<Value> {
    let mut resources = Vec::new();
    for n in 1..=5 {
        let outcome = t.transform(&json!({"n": n}), &ctx()).await.unwrap();
        let Outcome::Resources(found) = outcome else {
            panic!("record {n}: {outcome:?}");
        };
        assert_eq!(found.len(), 1, "record {n}: {found:?}");
        assert_eq!(found[0]["id"], n.to_string(), "record {n}: {found:?}");
        resources.push(found[0].clone());
    }
    resources
}

#[tokio::test]
async fn a_transform_that_writes_to_fd_1_once_still_gets_every_reply() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.11+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let t = real_worker(
        &python,
        dir.path(),
        r#"
import os

wrote = False

def transform(record, context):
    global wrote
    if not wrote:
        os.write(1, b"stray\n")
        wrote = True
    return {"resourceType": "Observation", "id": str(record["n"]), "pid": os.getpid()}
"#,
    );
    let resources = every_record_gets_its_own_reply(&t).await;
    assert!(
        resources.iter().all(|r| r["pid"] == resources[0]["pid"]),
        "the worker was restarted: {resources:?}"
    );
    let report = t.close().await.unwrap();
    assert_eq!(report.verdict, Verdict::Completed);
    assert!(report.stderr_tail.contains("stray"), "{report:?}");
}

#[cfg(unix)]
#[tokio::test]
async fn a_transform_whose_child_writes_to_fd_1_every_time_still_gets_every_reply() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.11+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let t = real_worker(
        &python,
        dir.path(),
        r#"
import os, subprocess

def transform(record, context):
    os.write(1, b"from-fd1\n")
    subprocess.run(["sh", "-c", "echo from-child"], check=True)
    return {"resourceType": "Observation", "id": str(record["n"]), "pid": os.getpid()}
"#,
    );
    let resources = every_record_gets_its_own_reply(&t).await;
    assert!(
        resources.iter().all(|r| r["pid"] == resources[0]["pid"]),
        "the worker was restarted: {resources:?}"
    );
    let report = t.close().await.unwrap();
    assert_eq!(report.verdict, Verdict::Completed);
    assert_eq!(
        report.stderr_tail.matches("from-child").count(),
        5,
        "{report:?}"
    );
    assert_eq!(
        report.stderr_tail.matches("from-fd1").count(),
        5,
        "{report:?}"
    );
}

// ---- a process that exits on its own ----------------------------------------

#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_exit_mid_request_is_reported_as_the_exit_while_a_child_holds_stdout() {
    let rig = need_python!();
    let t = rig.transformer("normal", limits());
    let first = reply(t.transform(&json!({"echo": "a"}), &ctx()).await.unwrap());
    let started = Instant::now();
    let message = rejection(
        t.transform(&json!({"echo": "dies", "spawn": true, "exit": 3}), &ctx())
            .await
            .unwrap(),
    );
    assert!(
        message.contains("exited with code 3") && message.contains("verdict user_error"),
        "{message}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(1200),
        "waited for the deadline: {:?}",
        started.elapsed()
    );
    let grandchild: i64 =
        serde_json::from_str(&std::fs::read_to_string(rig.state.join("grandchild")).unwrap())
            .unwrap();
    poll_until(
        "the grandchild to be killed",
        Duration::from_secs(2),
        || gone_or_zombie(grandchild),
    )
    .await;
    let next = reply(t.transform(&json!({"echo": "b"}), &ctx()).await.unwrap());
    assert_ne!(next["pid"], first["pid"]);
    t.close().await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_worker_that_exits_while_idle_is_closed_at_once() {
    let rig = need_python!();
    let (logs, _guard) = Logs::capture();
    let t = rig.transformer("normal", limits());
    let first = reply(
        t.transform(
            &json!({"echo": "a", "spawn": true, "exit_after": 0.3, "code": 3}),
            &ctx(),
        )
        .await
        .unwrap(),
    );
    let pid = first["pid"].as_i64().unwrap();
    let run_dir = PathBuf::from(first["cwd"].as_str().unwrap())
        .parent()
        .unwrap()
        .to_path_buf();
    let grandchild: i64 =
        serde_json::from_str(&std::fs::read_to_string(rig.state.join("grandchild")).unwrap())
            .unwrap();
    // Reaped (no zombie), what it started killed, its directory removed: with
    // no record in flight.
    poll_until(
        "the idle worker to be closed",
        Duration::from_secs(2),
        || {
            !Path::new(&format!("/proc/{pid}")).exists()
                && gone_or_zombie(grandchild)
                && !run_dir.exists()
        },
    )
    .await;
    let reports = logs.lines("transform worker retired");
    assert_eq!(reports.len(), 1, "{reports:?}");
    assert!(reports[0].contains("\"exit_code\":3"), "{}", reports[0]);
    let next = reply(t.transform(&json!({"echo": "b"}), &ctx()).await.unwrap());
    assert_ne!(next["pid"], first["pid"]);
    t.close().await;
}

// ---- stopping the engine ------------------------------------------------------

/// A transform that notes where it runs, starts a child, and never answers.
#[cfg(target_os = "linux")]
const STUCK: &str = r#"
import json, os, subprocess, sys, time

NOTE = __NOTE__

def transform(record, context):
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    with open(NOTE + ".tmp", "w") as f:
        json.dump({"pid": os.getpid(), "grandchild": child.pid, "cwd": os.getcwd()}, f)
    os.replace(NOTE + ".tmp", NOTE)
    time.sleep(3600)
"#;

#[cfg(target_os = "linux")]
#[tokio::test]
async fn sigterm_stops_the_engine_and_takes_its_workers_with_it() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.11+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for d in ["sources", "transforms", "inbox"] {
        std::fs::create_dir_all(root.join(d)).unwrap();
    }
    let note = root.join("note.json");
    std::fs::write(
        root.join("transforms/stuck.py"),
        STUCK.replace("__NOTE__", &format!("{:?}", note.to_str().unwrap())),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!(
            "fhir_base_url: http://127.0.0.1:9\ndatabase: data/hie.db\nlisten: 127.0.0.1:0\npython: {python}\n"
        ),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/stuck.yaml"),
        "id: stuck\nname: Stuck\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  min_file_age_ms: 0\n\
         transform: transforms/stuck.py\n",
    )
    .unwrap();
    std::fs::write(root.join("inbox/rows.csv"), "a\n1\n").unwrap();
    let log = std::fs::File::create(root.join("hie.log")).unwrap();
    let mut engine = std::process::Command::new(env!("CARGO_BIN_EXE_hie"))
        .arg("--config")
        .arg(root.join("hie.yaml"))
        .arg("run")
        .env("HIE_RUNTIME_DIR", repo().join("hie_runtime"))
        .env("HIE_LOG", "info")
        .env_remove("HIE_CONFIG")
        .env_remove("HIE_RUNTIME_URL")
        .env_remove("HIE_DATABASE_URL")
        .env_remove("HIE_AUTH_ENABLED")
        .stdout(std::process::Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap();
    let log = || std::fs::read_to_string(root.join("hie.log")).unwrap_or_default();
    let started = Instant::now();
    while !note.exists() {
        if started.elapsed() > Duration::from_secs(20) {
            let _ = engine.kill();
            panic!("the transform never ran:\n{}", log());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let note: Value = serde_json::from_str(&std::fs::read_to_string(&note).unwrap()).unwrap();

    // SAFETY: a signal to the engine this test started.
    unsafe { libc::kill(engine.id() as libc::pid_t, libc::SIGTERM) };
    let stopping = Instant::now();
    let status = loop {
        if let Some(status) = engine.try_wait().unwrap() {
            break status;
        }
        if stopping.elapsed() > Duration::from_secs(10) {
            let _ = engine.kill();
            panic!("the engine did not stop:\n{}", log());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(status.success(), "{status:?}\n{}", log());
    let worker = note["pid"].as_i64().unwrap();
    let grandchild = note["grandchild"].as_i64().unwrap();
    let run_dir = PathBuf::from(note["cwd"].as_str().unwrap())
        .parent()
        .unwrap()
        .to_path_buf();
    poll_until(
        "the worker, its child and its run directory to go",
        Duration::from_secs(3),
        || gone_or_zombie(worker) && gone_or_zombie(grandchild) && !run_dir.exists(),
    )
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_exit_just_before_the_deadline_is_still_reported_as_the_exit() {
    let rig = need_python!();
    let t = rig.transformer("normal", limits());
    // Exits 70 ms before the 1.5 s deadline, within the reply grace, while its
    // child holds stdout: the deadline passing during the grace changes nothing.
    let message = rejection(
        t.transform(
            &json!({"echo": "dies", "spawn": true, "exit_after": 1.43, "code": 3, "hang": true}),
            &ctx(),
        )
        .await
        .unwrap(),
    );
    assert!(
        message.contains("exited with code 3") && message.contains("verdict user_error"),
        "{message}"
    );
    let next = reply(t.transform(&json!({"echo": "b"}), &ctx()).await.unwrap());
    assert_eq!(next["echo"], "b");
    t.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_close_cancelled_during_cleanup_keeps_the_decision_it_made() {
    let rig = need_python!();
    let (logs, _guard) = Logs::capture();
    let t = rig.transformer("normal", limits());
    reply(
        t.transform(&json!({"echo": "a", "escape": true, "linger": 30}), &ctx())
            .await
            .unwrap(),
    );
    let escaped: i64 =
        serde_json::from_str(&std::fs::read_to_string(rig.state.join("escaped")).unwrap()).unwrap();
    // The worker ignores the closed stdin: after its 2 s it is killed and the
    // close is `timed_out`; the escaped child keeps the pipes open, so the
    // cleanup is still draining when the close is dropped.
    let dropped = tokio::time::timeout(Duration::from_millis(2400), t.close()).await;
    assert!(dropped.is_err(), "the close finished: {dropped:?}");
    poll_until("the finalizer's report", Duration::from_secs(3), || {
        logs.lines("transform worker retired").len() == 1
    })
    .await;
    let report = &logs.lines("transform worker retired")[0];
    // SAFETY: a signal to the child this test's program started.
    unsafe { libc::kill(escaped as libc::pid_t, libc::SIGKILL) };
    assert!(
        report.contains("\"verdict\":\"timed_out\"")
            && report.contains("\"cause\":null")
            && report.contains("\"exit_code\":null")
            && report.contains("\"signal\":null")
            && report.contains("\"cleanup\":\"incomplete\""),
        "{report}"
    );
    assert!(rig.run_dirs().is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn an_interpreter_that_starts_python_as_its_child_is_refused() {
    let mut rig = need_python!();
    let wrapper = rig.root.join("python-wrapper");
    std::fs::write(&wrapper, format!("#!/bin/sh\n\"{}\" \"$@\"\n", rig.python)).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    rig.python = wrapper.to_str().unwrap().to_string();
    // A short startup budget: the launcher the wrapper started gives up waiting
    // for its acknowledgement soon after.
    let t = rig.transformer(
        "marker",
        WorkerLimits {
            startup: Duration::from_secs(2),
            ..limits()
        },
    );
    let (result, acked) =
        watching_acks(&rig.runs, t.transform(&json!({"echo": "a"}), &ctx())).await;
    let message = format!("{:#}", result.unwrap_err());
    assert!(
        message.contains(
            "the interpreter started another process; set python: to the interpreter itself"
        ),
        "{message}"
    );
    assert!(!acked, "the host acknowledged a launcher it did not start");
    assert!(!rig.state.join("marker").exists(), "the program ran");
    assert!(rig.run_dirs().is_empty());
}
