//! The host side of the `limited-process@1` contract for a transform's
//! long-lived worker.
//!
//! The worker runs as `python -I -B -u -X utf8 limits.py --control … -- worker.py
//! <transform>`, where `limits.py` is the contract's reference launcher, kept
//! unchanged in `hie_runtime/` and pinned by its hash in `limits.pin`. The
//! launcher puts the process in its own group and applies the memory, file-size
//! and open-files limits; this module owns everything else: a fresh run
//! directory and an environment built from nothing for every process, the
//! control handshake, the startup and per-request deadlines, bounded output,
//! killing the whole group, and one report per process.
//!
//! One process is a *generation*. A request that times out, a process that
//! dies or breaks the protocol, ends its generation; the next request starts a
//! new one, so nothing an old process prints is ever read as a newer reply.
//! Requests are never retried here.

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// The child's exit status and when it was seen, once it has been reaped.
pub(crate) type Exit = watch::Receiver<Option<(ExitStatus, Instant)>>;

/// The contract this host follows.
pub(crate) const CONTRACT: &str = "limited-process@1";
/// The longest line the worker may send: one reply.
pub(crate) const MAX_LINE: usize = 32 * 1024 * 1024;
/// How much of the worker's stderr is kept: the last 64 KiB.
const STDERR_TAIL: usize = 64 * 1024;
/// How much of that tail goes into a rejected record's message.
const MESSAGE_TAIL: usize = 2 * 1024;
const POLL: Duration = Duration::from_millis(5);
/// Draining and reaping after the group is killed; separate from every deadline.
const CLEANUP_BUDGET: Duration = Duration::from_millis(1000);
/// How long a worker whose stdin was closed, or whose pipes broke, gets to exit.
const EXIT_WAIT: Duration = Duration::from_secs(2);
/// After the process exits mid-request: how long a reply it wrote before it
/// went may take to be read, when something it started still holds stdout.
const EXIT_GRACE: Duration = Duration::from_millis(100);

const PLATFORM: &str = if cfg!(target_os = "linux") {
    "linux"
} else if cfg!(target_os = "macos") {
    "macos"
} else if cfg!(windows) {
    "windows"
} else {
    std::env::consts::OS
};

/// The launcher this engine was built for: `hie_runtime/limits.pin`.
#[derive(Debug)]
pub struct Pin {
    pub contract: &'static str,
    pub sha256: &'static str,
}

pub fn pin() -> &'static Pin {
    static PIN: OnceLock<Pin> = OnceLock::new();
    PIN.get_or_init(|| {
        let text = include_str!("../hie_runtime/limits.pin");
        let field = |name: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
                .map(str::trim)
                .unwrap_or_else(|| panic!("hie_runtime/limits.pin has no {name} line"))
        };
        Pin {
            contract: field("contract"),
            sha256: field("sha256"),
        }
    })
}

/// What each worker process may use, and how long it may take.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerLimits {
    /// Address space per process (`RLIMIT_AS` on Linux), in MiB.
    pub memory_mib: u64,
    /// The largest file a process may write, in MiB.
    pub file_mib: u64,
    /// Open file descriptors per process.
    pub open_files: u64,
    /// One record: from before the request is written until its reply line.
    pub request: Duration,
    /// From starting the process until the worker's `{"ready": true}`.
    pub startup: Duration,
}

impl Default for WorkerLimits {
    fn default() -> Self {
        Self {
            memory_mib: 2048,
            file_mib: 256,
            open_files: 256,
            request: Duration::from_secs(60),
            startup: Duration::from_secs(10),
        }
    }
}

/// How a generation ended, as the contract names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Completed,
    TimedOut,
    Cancelled,
    LimitExceeded,
    UserError,
    Unavailable,
}

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = serde_json::to_value(self).ok();
        f.write_str(text.as_ref().and_then(Value::as_str).unwrap_or("?"))
    }
}

/// The one result of a generation, with the contract's fields. stdout carries
/// the worker protocol, so its tail is always empty.
#[derive(Debug, Clone, Serialize)]
pub struct GenerationReport {
    pub contract: &'static str,
    pub platform: &'static str,
    pub verdict: Verdict,
    pub cause: Option<&'static str>,
    pub exit_code: Option<i64>,
    pub signal: Option<i32>,
    pub not_enforced: Vec<String>,
    pub reference_sha256: Option<String>,
    pub cleanup: &'static str,
    pub elapsed_ms: u64,
    pub stdout_tail: String,
    pub stdout_tail_bytes: usize,
    pub stdout_truncated: bool,
    pub stderr_tail: String,
    pub stderr_tail_bytes: usize,
    pub stderr_truncated: bool,
}

impl GenerationReport {
    /// The last couple of KiB of stderr, for a message a person reads.
    pub(crate) fn short_tail(&self) -> &str {
        let tail = self.stderr_tail.trim_end();
        let mut start = tail.len().saturating_sub(MESSAGE_TAIL);
        while !tail.is_char_boundary(start) {
            start += 1;
        }
        &tail[start..]
    }
}

/// Everything needed to start one generation.
pub(crate) struct Launch {
    /// The interpreter, absolute; or why it could not be found.
    pub python: Result<PathBuf, String>,
    pub launcher: PathBuf,
    pub program: PathBuf,
    pub args: Vec<PathBuf>,
    /// Variables passed on purpose, on top of the contract's own.
    pub extra_env: Vec<(String, String)>,
    pub limits: WorkerLimits,
    /// Where run directories are made; the system temporary directory when unset.
    pub run_root: Option<PathBuf>,
    /// What the logs call this worker.
    pub label: String,
}

/// What became of one request.
pub(crate) enum Exchange {
    /// A JSON reply carrying the request's id.
    Reply(Value),
    /// A line that is not JSON: framing is intact, the worker stays.
    NotJson(String),
    /// JSON with another id.
    OutOfOrder,
    TimedOut,
    Overlong,
    /// stdout closed or stdin broke: the process is going or gone.
    Ended,
}

/// The stderr ring: the last bytes, and how many there were in all.
#[derive(Default)]
struct Ring {
    bytes: VecDeque<u8>,
    total: u64,
}

impl Ring {
    fn push(&mut self, chunk: &[u8]) {
        self.total += chunk.len() as u64;
        let keep = &chunk[chunk.len().saturating_sub(STDERR_TAIL)..];
        let overflow = (self.bytes.len() + keep.len()).saturating_sub(STDERR_TAIL);
        self.bytes.drain(..overflow);
        self.bytes.extend(keep);
    }
}

#[derive(Debug, Default, Deserialize)]
struct Ready {
    #[serde(default)]
    contract: String,
    /// The launcher's process, which must be the one the host started (checked
    /// on Unix; see the handshake).
    #[cfg(unix)]
    pid: Option<i64>,
    /// Its group; Windows has none (a Job Object ends the launcher's children
    /// with it).
    #[cfg(unix)]
    pgid: Option<i64>,
    #[serde(default)]
    applied: Vec<String>,
    #[serde(default)]
    not_enforced: Vec<String>,
    reference_sha256: Option<String>,
    error: Option<ReadyError>,
}

#[derive(Debug, Default, Deserialize)]
struct ReadyError {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

enum Line {
    Text(Vec<u8>),
    Overlong,
    Eof,
}

/// Reads one line, failing it once it passes `max` bytes.
async fn read_line(stdout: &mut BufReader<ChildStdout>, max: usize) -> Line {
    let mut line = Vec::new();
    loop {
        let available = match stdout.fill_buf().await {
            Ok(bytes) if !bytes.is_empty() => bytes,
            _ => return Line::Eof,
        };
        if let Some(end) = available.iter().position(|&b| b == b'\n') {
            if line.len() + end > max {
                return Line::Overlong;
            }
            line.extend_from_slice(&available[..end]);
            stdout.consume(end + 1);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Line::Text(line);
        }
        let n = available.len();
        line.extend_from_slice(available);
        stdout.consume(n);
        if line.len() > max {
            return Line::Overlong;
        }
    }
}

/// Reads and drops one chunk; `false` at end of file.
async fn discard_some(stdout: &mut BufReader<ChildStdout>) -> bool {
    let mut buf = [0u8; 8192];
    matches!(stdout.read(&mut buf).await, Ok(n) if n > 0)
}

pub(crate) fn seconds(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s.fract() == 0.0 {
        format!("{s:.0} s")
    } else {
        format!("{s} s")
    }
}

/// `(verdict, cause, exit_code, signal)` for a process that ended on its own.
fn classify(status: ExitStatus) -> (Verdict, Option<&'static str>, Option<i64>, Option<i32>) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            let verdict = if signal == libc::SIGKILL || signal == libc::SIGXCPU {
                Verdict::LimitExceeded
            } else {
                Verdict::UserError
            };
            return (verdict, Some("unknown"), None, Some(signal));
        }
    }
    let Some(code) = status.code() else {
        return (Verdict::UserError, Some("unknown"), None, None);
    };
    // Windows exit codes are unsigned 32-bit.
    let code = if cfg!(windows) {
        i64::from(code as u32)
    } else {
        i64::from(code)
    };
    let (verdict, cause) = match code {
        0 => (Verdict::Completed, None),
        65 => (Verdict::LimitExceeded, Some("memory")),
        66 => (Verdict::LimitExceeded, Some("file")),
        67 => (Verdict::LimitExceeded, Some("cpu")),
        1..=63 => (Verdict::UserError, None),
        _ => (Verdict::UserError, Some("unknown")),
    };
    (verdict, cause, Some(code), None)
}

/// Finds the interpreter the way the contract wants it: absolute, symbolic
/// links kept. A name with a path separator is made absolute; a bare name is
/// looked up on `path` (the engine's `PATH`), skipping empty and relative entries.
pub(crate) fn resolve_interpreter(python: &str, path: Option<&OsStr>) -> Result<PathBuf, String> {
    let has_separator = python.contains('/') || (cfg!(windows) && python.contains('\\'));
    if has_separator {
        return std::path::absolute(python)
            .map_err(|e| format!("cannot make the interpreter path {python} absolute: {e}"));
    }
    let names: Vec<String> = if cfg!(windows) && !python.to_ascii_lowercase().ends_with(".exe") {
        vec![python.to_string(), format!("{python}.exe")]
    } else {
        vec![python.to_string()]
    };
    for dir in path.map(std::env::split_paths).into_iter().flatten() {
        if dir.as_os_str().is_empty() || !dir.is_absolute() {
            continue;
        }
        for name in &names {
            let candidate = dir.join(name);
            if is_executable(&candidate) {
                return Ok(candidate);
            }
        }
    }
    Err(format!("the interpreter {python} was not found on PATH"))
}

/// The interpreter that actually runs Python. On Windows the configured one is
/// often the `py` launcher, which starts `python.exe` as a child of its own:
/// killing the launcher would leave the worker running. Asked once per worker;
/// on Unix the configured path is used as it is, links and all.
#[cfg(windows)]
async fn real_interpreter(python: &Path, budget: Duration) -> Result<PathBuf, String> {
    let mut command = Command::new(python);
    command
        .args(["-I", "-c", "import sys; print(sys.executable)"])
        .env_clear()
        .stdin(Stdio::null())
        .kill_on_drop(true);
    for name in ["PATH", "LANG", "LC_ALL", "SYSTEMROOT", "TEMP", "TMP"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    let output = tokio::time::timeout(budget, command.output())
        .await
        .map_err(|_| format!("{} did not say which Python it runs", python.display()))?
        .map_err(|e| format!("cannot start {}: {e}", python.display()))?;
    let path = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    if !output.status.success() || !path.is_absolute() {
        return Err(format!(
            "{} did not say which Python it runs: {}",
            python.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(path)
}

fn is_executable(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.is_file() && meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        meta.is_file()
    }
}

/// How a generation is brought to its end.
#[derive(Clone, Copy)]
enum Finish {
    /// Done with it: close stdin and let it exit.
    Close,
    /// It closed stdout or stopped reading: find out how it ended.
    Ended,
    /// The host ends it: the verdict is already decided.
    Forced(Verdict, Option<&'static str>),
    /// It never became ready.
    Unavailable,
}

struct Inner {
    /// Asks the task that owns the child to kill it.
    kill_tx: mpsc::UnboundedSender<()>,
    /// Set by that task when it reaps the child.
    exited: Exit,
    reaper: Option<JoinHandle<()>>,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    stderr: Arc<Mutex<Ring>>,
    stderr_task: Option<JoinHandle<()>>,
    run_dir: Option<tempfile::TempDir>,
    control: PathBuf,
    started: Instant,
    pid: Option<u32>,
    pgid: Option<i32>,
    /// The host acknowledged the launcher: from here on its group is the run's.
    #[cfg(unix)]
    acked: bool,
    ready: Option<Ready>,
}

/// Owns the child: reaps it as soon as it exits, whatever the host is doing,
/// and kills it when asked. Dropping the task kills the child (`kill_on_drop`).
fn reaper(mut child: Child) -> (mpsc::UnboundedSender<()>, Exit, JoinHandle<()>) {
    let (kill_tx, mut kill_rx) = mpsc::unbounded_channel::<()>();
    let (exit_tx, exited) = watch::channel(None);
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                status = child.wait() => {
                    if let Ok(status) = status {
                        let _ = exit_tx.send(Some((status, Instant::now())));
                    }
                    return;
                }
                Some(()) = kill_rx.recv() => {
                    let _ = child.start_kill();
                }
            }
        }
    });
    (kill_tx, exited, task)
}

/// Waits until the child has been reaped; `None` if that can no longer happen.
async fn exit_of(exited: &mut Exit) -> Option<ExitStatus> {
    let seen = exited.wait_for(Option::is_some).await.ok()?;
    seen.map(|(status, _)| status)
}

impl Inner {
    /// SIGKILL to the group once the launcher has made it, then to the process.
    fn kill(&mut self) {
        self.kill_group();
        let _ = self.kill_tx.send(());
    }

    /// Only once acknowledged: before that the launcher has no children. On
    /// Windows the Job Object ends them when the launcher is killed.
    fn kill_group(&self) {
        #[cfg(unix)]
        if self.acked
            && let Some(pgid) = self.pgid
        {
            // SAFETY: plain syscall; `pgid` was checked to be the launcher's own group.
            unsafe {
                libc::killpg(pgid, libc::SIGKILL);
            }
        }
    }

    fn observe_exit(&self) -> Option<ExitStatus> {
        self.exited.borrow().map(|(status, _)| status)
    }

    fn ended_at(&self) -> Option<Instant> {
        self.exited.borrow().map(|(_, at)| at)
    }

    /// Waits up to `budget` for the process to exit, reading stdout meanwhile so
    /// a full pipe cannot hold it.
    async fn wait_for_exit(&mut self, budget: Duration) -> Option<ExitStatus> {
        if let Some(status) = self.observe_exit() {
            return Some(status);
        }
        let deadline = tokio::time::Instant::now() + budget;
        let mut stdout_open = true;
        let mut exited = self.exited.clone();
        loop {
            let stdout = &mut self.stdout;
            tokio::select! {
                status = exit_of(&mut exited) => return status,
                open = discard_some(stdout), if stdout_open => stdout_open = open,
                _ = tokio::time::sleep_until(deadline) => return None,
            }
        }
    }

    fn stderr_snapshot(&self) -> (String, usize, bool) {
        let ring = self.stderr.lock().expect("stderr ring");
        let bytes: Vec<u8> = ring.bytes.iter().copied().collect();
        let truncated = ring.total > bytes.len() as u64;
        (
            String::from_utf8_lossy(&bytes).into_owned(),
            bytes.len(),
            truncated,
        )
    }
}

/// One worker process, from its start to its report.
pub(crate) struct Generation {
    inner: Option<Inner>,
    /// Tells this generation from the next one in the same slot.
    serial: u64,
    label: String,
    next_id: u64,
    /// In the middle of a start or a request: dropping it now is a cancellation.
    busy: bool,
    /// The decision, once the host has made it.
    frozen: Option<Frozen>,
    /// This generation is being finished by its own finalizer task.
    finalizing: bool,
}

/// How a generation ended, decided once: a finalizer that resumes a cleanup
/// takes it as it is and never classifies again.
#[derive(Clone, Copy)]
struct Frozen {
    verdict: Verdict,
    cause: Option<&'static str>,
    /// The host killed it, so its exit status is not its own.
    host_killed: bool,
}

static NOT_ENFORCED_WARNED: AtomicBool = AtomicBool::new(false);
static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

impl Generation {
    /// Starts a process and waits until it is ready: the control handshake,
    /// the pin check, then the worker's own `{"ready": true}`, all within the
    /// startup budget. An error is the reason it is unavailable, with the end
    /// of its stderr.
    pub(crate) async fn start(launch: &Launch) -> Result<Generation, String> {
        let python = launch.python.clone()?;
        #[cfg(windows)]
        let python = real_interpreter(&python, launch.limits.startup).await?;
        // Private to the engine's user: the worker's files and the control
        // handshake are nobody else's to read or to fake.
        let mut builder = tempfile::Builder::new();
        builder.prefix("hie-worker-");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o700));
        }
        let run_dir = match &launch.run_root {
            Some(root) => builder.tempdir_in(root),
            None => builder.tempdir(),
        }
        .map_err(|e| format!("cannot create a run directory: {e}"))?;
        let work = run_dir.path().join("work");
        let control = run_dir.path().join("control");
        #[cfg(unix)]
        let dirs = {
            use std::os::unix::fs::DirBuilderExt;
            let mut dirs = std::fs::DirBuilder::new();
            dirs.mode(0o700);
            dirs
        };
        #[cfg(not(unix))]
        let dirs = std::fs::DirBuilder::new();
        for dir in [&work, &control] {
            dirs.create(dir)
                .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
        }

        let limits = &launch.limits;
        let mut command = Command::new(&python);
        command
            .args(["-I", "-B", "-u", "-X", "utf8"])
            .arg(&launch.launcher)
            .arg("--control")
            .arg(&control)
            .arg("--startup-ms")
            .arg(limits.startup.as_millis().max(1).to_string())
            .arg("--memory")
            .arg(limits.memory_mib.to_string())
            .arg("--file")
            .arg(limits.file_mib.to_string())
            .arg("--open-files")
            .arg(limits.open_files.to_string())
            .arg("--")
            .arg(&launch.program)
            .args(&launch.args)
            .env_clear();
        let inherited: &[&str] = if cfg!(windows) {
            &["PATH", "LANG", "LC_ALL", "SYSTEMROOT", "TEMP", "TMP"]
        } else {
            &["PATH", "LANG", "LC_ALL"]
        };
        for name in inherited {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        // Numeric libraries reserve address space per thread; under the memory
        // limit that alone can fail an import.
        for name in [
            "OPENBLAS_NUM_THREADS",
            "OMP_NUM_THREADS",
            "MKL_NUM_THREADS",
            "NUMEXPR_NUM_THREADS",
        ] {
            command.env(name, "1");
        }
        command.envs(launch.extra_env.iter().map(|(k, v)| (k, v)));
        command
            .current_dir(&work)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let started = Instant::now();
        let mut child = command
            .spawn()
            .map_err(|e| format!("cannot start {}: {e}", python.display()))?;
        let stdin = child.stdin.take();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let ring = Arc::new(Mutex::new(Ring::default()));
        let stderr_task = stderr.map(|mut stderr| {
            let ring = ring.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 16 * 1024];
                while let Ok(n) = stderr.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    ring.lock().expect("stderr ring").push(&buf[..n]);
                }
            })
        });
        let Some(stdout) = stdout else {
            let _ = child.start_kill();
            return Err("the worker has no stdout".into());
        };
        let pid = child.id();
        let (kill_tx, exited, reaper) = reaper(child);
        let mut generation = Generation {
            inner: Some(Inner {
                kill_tx,
                exited,
                reaper: Some(reaper),
                stdin,
                stdout: BufReader::new(stdout),
                stderr: ring,
                stderr_task,
                run_dir: Some(run_dir),
                control,
                started,
                pid,
                pgid: None,
                #[cfg(unix)]
                acked: false,
                ready: None,
            }),
            serial: SERIAL.fetch_add(1, Ordering::Relaxed),
            label: launch.label.clone(),
            next_id: 1,
            busy: true,
            frozen: None,
            finalizing: false,
        };
        match generation.handshake(launch).await {
            Ok(()) => {
                generation.busy = false;
                Ok(generation)
            }
            Err(reason) => {
                let report = generation.finish(Finish::Unavailable).await;
                let status = match (report.exit_code, report.signal) {
                    (Some(code), _) => format!(" (exit code {code})"),
                    (_, Some(signal)) => format!(" (signal {signal})"),
                    _ => String::new(),
                };
                let tail = report.short_tail();
                Err(if tail.is_empty() {
                    format!("{reason}{status}")
                } else {
                    format!("{reason}{status}\n{tail}")
                })
            }
        }
    }

    async fn handshake(&mut self, launch: &Launch) -> Result<(), String> {
        let startup = launch.limits.startup;
        let inner = self.inner.as_mut().expect("generation running");
        let deadline = inner.started + startup;
        let ready_path = inner.control.join("ready.json");
        let read_ready = || -> Option<Result<Ready, String>> {
            let bytes = std::fs::read(&ready_path).ok()?;
            Some(
                serde_json::from_slice(&bytes)
                    .map_err(|e| format!("the launcher's ready.json cannot be read: {e}")),
            )
        };
        let ready = loop {
            if let Some(ready) = read_ready() {
                break ready?;
            }
            if inner.observe_exit().is_some() {
                // It may have written ready.json and exited between two polls.
                match read_ready() {
                    Some(ready) => break ready?,
                    None => return Err("the launcher exited before its handshake".into()),
                }
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "the launcher did not complete its handshake within {}",
                    seconds(startup)
                ));
            }
            tokio::time::sleep(POLL).await;
        };
        // Kept from here on, so the report carries the launcher's evidence
        // (its hash, what it could not enforce) even when it is refused.
        let ready = inner.ready.insert(ready);
        if let Some(error) = &ready.error {
            return Err(format!(
                "the launcher could not set up the worker: {}: {}",
                error.code, error.message
            ));
        }
        let pin = pin();
        if ready.contract != pin.contract {
            return Err(format!(
                "the launcher follows {:?}, not the pinned {}",
                ready.contract, pin.contract
            ));
        }
        if ready.reference_sha256.as_deref() != Some(pin.sha256) {
            return Err(format!(
                "the launcher's hash {} does not match the pinned {}",
                ready.reference_sha256.as_deref().unwrap_or("(none)"),
                pin.sha256
            ));
        }
        // A wrapper that starts Python as its own child would leave the
        // launcher out of the host's reach. Unix only: on Windows the `py`
        // launcher and a virtual environment's `python.exe` start the real
        // interpreter as a child tied to them by a Job Object, so it ends
        // with them.
        #[cfg(unix)]
        if ready.pid != inner.pid.map(i64::from) {
            return Err(format!(
                "the launcher runs as process {:?}, not the {:?} the engine started: the interpreter started another process; set python: to the interpreter itself",
                ready.pid, inner.pid
            ));
        }
        #[cfg(unix)]
        {
            // The group must be the launcher's own, never this engine's.
            // SAFETY: plain syscall without arguments.
            let own = i64::from(unsafe { libc::getpgrp() });
            let pgid = ready
                .pgid
                .filter(|&g| g > 1 && Some(g) == ready.pid && g != own)
                .and_then(|g| i32::try_from(g).ok())
                .ok_or_else(|| {
                    format!(
                        "the launcher reported process group {:?} for process {:?}",
                        ready.pgid, ready.pid
                    )
                })?;
            inner.pgid = Some(pgid);
        }
        std::fs::File::create(inner.control.join("ack"))
            .map_err(|e| format!("cannot acknowledge the launcher: {e}"))?;
        #[cfg(unix)]
        {
            inner.acked = true;
        }
        if !ready.not_enforced.is_empty() && !NOT_ENFORCED_WARNED.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                not_enforced = ?ready.not_enforced,
                platform = PLATFORM,
                "transform worker limits not enforced on this platform"
            );
        }
        tracing::info!(
            transform = %self.label,
            pid = ?inner.pid,
            pgid = ?inner.pgid,
            run_dir = %inner.control.parent().unwrap_or(Path::new("")).display(),
            applied = ?ready.applied,
            "transform worker started"
        );

        let line =
            tokio::time::timeout_at(deadline.into(), read_line(&mut inner.stdout, MAX_LINE)).await;
        match line {
            Err(_) => Err(format!(
                "the transform was not ready within {}",
                seconds(startup)
            )),
            Ok(Line::Eof) => Err("the worker exited before it was ready".into()),
            Ok(Line::Overlong) => Err("the worker's first line was too long".into()),
            Ok(Line::Text(text)) => {
                let ready = serde_json::from_slice::<Value>(&text)
                    .ok()
                    .and_then(|v| v.get("ready").cloned());
                if ready == Some(json!(true)) {
                    Ok(())
                } else {
                    let shown = String::from_utf8_lossy(&text);
                    Err(format!(
                        "the worker's first line was not {{\"ready\": true}}: {}",
                        shown.chars().take(200).collect::<String>()
                    ))
                }
            }
        }
    }

    /// Sends one request and waits for its reply. The deadline is taken before
    /// the write, so a worker that stops reading cannot hold the engine.
    pub(crate) async fn exchange(
        &mut self,
        record: &Value,
        context: &Value,
        timeout: Duration,
    ) -> Exchange {
        let deadline = tokio::time::Instant::now() + timeout;
        let id = self.next_id;
        self.next_id += 1;
        let mut request = json!({"id": id, "record": record, "context": context}).to_string();
        request.push('\n');
        self.busy = true;
        let inner = self.inner.as_mut().expect("generation running");
        let mut exited = inner.exited.clone();
        let io = async {
            let Some(stdin) = inner.stdin.as_mut() else {
                return Exchange::Ended;
            };
            if stdin.write_all(request.as_bytes()).await.is_err() || stdin.flush().await.is_err() {
                return Exchange::Ended;
            }
            match read_line(&mut inner.stdout, MAX_LINE).await {
                Line::Eof => Exchange::Ended,
                Line::Overlong => Exchange::Overlong,
                Line::Text(text) => match serde_json::from_slice::<Value>(&text) {
                    Ok(reply) if reply.get("id") == Some(&json!(id)) => Exchange::Reply(reply),
                    Ok(_) => Exchange::OutOfOrder,
                    Err(e) => Exchange::NotJson(e.to_string()),
                },
            }
        };
        tokio::pin!(io);
        // The pipes alone do not say the process is gone: something it started
        // may still hold them open. Its exit ends the request too.
        let first = tokio::time::timeout_at(deadline, async {
            tokio::select! {
                biased;
                exchange = &mut io => Some(exchange),
                _ = exit_of(&mut exited) => None,
            }
        })
        .await;
        let exchange = match first {
            Ok(Some(exchange)) => exchange,
            Err(_) => Exchange::TimedOut,
            // Exited before the deadline: that is how the request ended, even
            // if the deadline passes during the grace. A reply it wrote before
            // it went is still read, but never one after the deadline.
            Ok(None) => {
                let grace = (tokio::time::Instant::now() + EXIT_GRACE).min(deadline);
                tokio::time::timeout_at(grace, &mut io)
                    .await
                    .unwrap_or(Exchange::Ended)
            }
        };
        if matches!(exchange, Exchange::Reply(_) | Exchange::NotJson(_)) {
            self.busy = false;
        }
        exchange
    }

    pub(crate) fn serial(&self) -> u64 {
        self.serial
    }

    /// Becomes ready when the process has exited.
    pub(crate) fn exit_watch(&self) -> Exit {
        self.inner
            .as_ref()
            .map(|i| i.exited.clone())
            .unwrap_or_else(|| watch::channel(None).1)
    }

    pub(crate) fn has_exited(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|i| i.observe_exit().is_some())
    }

    /// Ends a healthy generation: stdin closed, the exit awaited, then cleanup.
    pub(crate) async fn close(mut self) -> GenerationReport {
        self.finish(Finish::Close).await
    }

    /// Ends a generation the host gave up on, with the verdict already decided.
    pub(crate) async fn retire(
        mut self,
        verdict: Verdict,
        cause: Option<&'static str>,
    ) -> GenerationReport {
        self.finish(Finish::Forced(verdict, cause)).await
    }

    /// Ends a generation whose pipes closed mid-request, classified by how the
    /// process actually ended.
    pub(crate) async fn ended(mut self) -> GenerationReport {
        self.finish(Finish::Ended).await
    }

    async fn finish(&mut self, how: Finish) -> GenerationReport {
        let inner = self.inner.as_mut().expect("generation running");
        let frozen = |verdict, cause, host_killed| Frozen {
            verdict,
            cause,
            host_killed,
        };
        match self.frozen {
            // Resumed after the first attempt was cancelled: decided already.
            Some(decided) => {
                if decided.host_killed {
                    inner.kill();
                }
            }
            None => match how {
                Finish::Forced(verdict, cause) => {
                    self.frozen = Some(frozen(verdict, cause, true));
                    inner.kill();
                }
                Finish::Unavailable => {
                    let killed = inner.observe_exit().is_none();
                    self.frozen = Some(frozen(Verdict::Unavailable, None, killed));
                    if killed {
                        inner.kill();
                    }
                }
                Finish::Close | Finish::Ended => {
                    inner.stdin.take();
                    match inner.wait_for_exit(EXIT_WAIT).await {
                        Some(status) => {
                            let (verdict, cause, _, _) = classify(status);
                            self.frozen = Some(frozen(verdict, cause, false));
                        }
                        None => {
                            // A process that does not exit when asked, or that
                            // broke the protocol and kept running.
                            self.frozen = Some(if matches!(how, Finish::Close) {
                                frozen(Verdict::TimedOut, None, true)
                            } else {
                                frozen(Verdict::UserError, Some("unknown"), true)
                            });
                            inner.kill();
                        }
                    }
                }
            },
        }

        // Cleanup, every time: the group goes, then the pipes drain to their end.
        inner.kill_group();
        let complete = tokio::time::timeout(CLEANUP_BUDGET, async {
            exit_of(&mut inner.exited).await;
            while discard_some(&mut inner.stdout).await {}
            if let Some(task) = inner.stderr_task.as_mut() {
                let _ = task.await;
                inner.stderr_task = None;
            }
        })
        .await
        .is_ok();
        if let Some(task) = inner.stderr_task.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = inner.reaper.take() {
            // Not reaped within the budget: dropping the child kills it, and
            // the runtime reaps it later.
            task.abort();
            let _ = task.await;
        }
        if let Some(dir) = inner.run_dir.take()
            && let Err(e) = dir.close()
        {
            tracing::warn!(transform = %self.label, "cannot remove the worker's run directory: {e}");
        }

        let Frozen {
            verdict,
            cause,
            host_killed,
        } = self.frozen.expect("verdict decided");
        let (exit_code, signal) = match (host_killed, inner.observe_exit()) {
            (false, Some(status)) => {
                let (_, _, code, signal) = classify(status);
                (code, signal)
            }
            _ => (None, None),
        };
        let (stderr_tail, stderr_tail_bytes, stderr_truncated) = inner.stderr_snapshot();
        let ready = inner.ready.as_ref();
        let mut not_enforced = ready.map(|r| r.not_enforced.clone()).unwrap_or_default();
        not_enforced.sort();
        let ended = inner.ended_at().unwrap_or_else(Instant::now);
        let report = GenerationReport {
            contract: CONTRACT,
            platform: PLATFORM,
            verdict,
            cause,
            exit_code,
            signal,
            not_enforced,
            reference_sha256: ready.and_then(|r| r.reference_sha256.clone()),
            cleanup: if complete { "complete" } else { "incomplete" },
            elapsed_ms: ended.duration_since(inner.started).as_millis() as u64,
            stdout_tail: String::new(),
            stdout_tail_bytes: 0,
            stdout_truncated: false,
            stderr_tail,
            stderr_tail_bytes,
            stderr_truncated,
        };
        let pid = inner.pid;
        self.inner = None;
        let json = serde_json::to_string(&report).unwrap_or_default();
        if verdict == Verdict::Completed {
            tracing::info!(transform = %self.label, pid = ?pid, report = %json, "transform worker closed");
        } else {
            tracing::warn!(transform = %self.label, pid = ?pid, report = %json, "transform worker retired");
        }
        report
    }
}

impl Drop for Generation {
    /// A generation nobody finished: an idle one is closed normally, one in the
    /// middle of a call is killed at once and reported as cancelled. A task owns
    /// the rest (reaping, draining, the run directory, the report), so it
    /// happens even though the caller is gone.
    fn drop(&mut self) {
        let Some(inner) = self.inner.as_mut() else {
            return;
        };
        // A decision already made is kept; `finish` resumes from it.
        let how = if self.busy {
            Finish::Forced(Verdict::Cancelled, None)
        } else {
            Finish::Close
        };
        if self.busy || self.finalizing || self.frozen.is_some_and(|f| f.host_killed) {
            inner.kill();
        }
        if !self.finalizing
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let mut rest = Generation {
                inner: self.inner.take(),
                serial: self.serial,
                label: std::mem::take(&mut self.label),
                next_id: self.next_id,
                busy: self.busy,
                frozen: self.frozen,
                finalizing: true,
            };
            handle.spawn(async move {
                rest.finish(how).await;
            });
            return;
        }
        // No runtime to finish it (the engine is exiting): the process is
        // killed above and the run directory goes with its `TempDir`.
        if let Some(mut inner) = self.inner.take() {
            inner.kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    #[test]
    fn the_pin_matches_the_vendored_launcher() {
        let launcher =
            std::fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("hie_runtime/limits.py"))
                .unwrap();
        assert_eq!(pin().contract, CONTRACT);
        assert_eq!(hex::encode(sha2::Sha256::digest(&launcher)), pin().sha256);
    }

    #[test]
    fn the_stderr_ring_keeps_the_last_bytes() {
        let mut ring = Ring::default();
        ring.push(&vec![b'a'; STDERR_TAIL - 1]);
        ring.push(b"bc");
        assert_eq!(ring.bytes.len(), STDERR_TAIL);
        assert_eq!(ring.total, STDERR_TAIL as u64 + 1);
        assert_eq!(ring.bytes.back(), Some(&b'c'));
        ring.push(&vec![b'z'; STDERR_TAIL * 3]);
        assert!(ring.bytes.iter().all(|&b| b == b'z'));
        assert_eq!(ring.bytes.len(), STDERR_TAIL);
    }

    #[cfg(unix)]
    #[test]
    fn a_bare_interpreter_name_is_found_on_path_without_resolving_links() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let interpreter = real.join("python-real");
        std::fs::write(&interpreter, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&interpreter, std::fs::Permissions::from_mode(0o755)).unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        std::os::unix::fs::symlink(&interpreter, bin.join("python-link")).unwrap();
        let path = std::env::join_paths(["", "relative/bin", bin.to_str().unwrap()]).unwrap();

        let found = resolve_interpreter("python-link", Some(&path)).unwrap();
        assert_eq!(found, bin.join("python-link"));
        assert!(found.is_absolute());
        assert!(resolve_interpreter("python-missing", Some(&path)).is_err());
        assert!(resolve_interpreter("python-link", None).is_err());
        // A name with a separator is made absolute as written.
        let relative = resolve_interpreter("venv/bin/python", None).unwrap();
        assert!(relative.is_absolute() && relative.ends_with("venv/bin/python"));
    }
}
