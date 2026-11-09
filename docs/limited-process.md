# The `limited-process@1` contract

This document is normative. The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

`limited-process@1` describes how a **host** program starts a Python program as a child process with operating-system limits, a deadline, a clean environment and bounded capture of its output, and how it reports exactly one result for that run. The reference launcher implements the part that runs inside the child; the host implements the rest in its own language.

In HIE the launcher is [`hie_runtime/limits.py`](../hie_runtime/limits.py), vendored unchanged and pinned in [`hie_runtime/limits.pin`](../hie_runtime/limits.pin) (section 10). Its docstring points to `docs/contract.md`: that is this document. The engine is the host, in the long-lived worker pattern (section 11): `src/worker.rs` implements the host's duties, and the program is `hie_runtime/worker.py` running a source's transform. How this shows up for someone writing a transform is in [Transforms](transforms.md#where-transforms-run).

The contract protects against accidents: endless loops, runaway memory, huge files, floods of output and leftover child processes. It is not a security boundary. See the [trust model](#12-trust-model).

## 1. Roles

| Role | Owns |
|---|---|
| **Host** | Starting the child, its environment and working directory, the deadline, cancellation, killing the process group, draining and capping output, concurrency, and the result. The values of every limit, the interpreter and its packages, network policy, retries and persistence. |
| **Launcher** (`limits.py`) | Creating the process group, applying the limits inside the child, the control handshake, running the program and translating how it ended into an exit code. |
| **Program** | The user's Python code. It owns nothing in this contract. |
| **Consumer protocol** | Whatever the host and program exchange on stdin and stdout. This contract does not define it. |

## 2. Platform profiles

| Platform | Profile | Guarantee |
|---|---|---|
| Linux | **production** | Every requested limit is applied, or the run fails before the program starts. |
| macOS | development | Deadline, process-group kill and bounded output are guaranteed. Other limits are applied where the operating system accepts them and reported in `not_enforced` where it does not. |
| Windows | development | Same as macOS. |

The command line, the control files, the result and the verdicts are identical on all three platforms. Only enforcement differs, as the matrix in section 4 states.

## 3. Launch

The host MUST start the child with an argument vector, never through a shell:

```
<python> -I -B -u -X utf8 <limits.py> --control <control-dir> [--startup-ms MS]
         [--memory MiB] [--cpu SECONDS] [--file MiB] [--open-files N]
         -- <program.py> [program arguments...]
```

- `-I` isolates the interpreter from user site-packages, the current directory and every `PYTHON*` environment variable.
- `-X utf8` makes standard streams UTF-8. Because `-I` ignores `PYTHONUTF8` and `PYTHONIOENCODING`, this flag is the only way to get UTF-8 on every platform.
- `-u` makes standard streams unbuffered, so a line the program writes reaches the host immediately. The long-lived worker pattern depends on it.
- `-B` writes no bytecode caches.

Paths:

- The host MUST make `<python>`, `<limits.py>`, `<control-dir>` and `<program.py>` absolute before it changes the child's working directory.
- The host MUST NOT resolve symbolic links in `<python>`: a virtual environment's interpreter is usually a link, and resolving it would select the base interpreter without the environment's packages.
- Program arguments are passed unchanged.

The minimum interpreter version is **Python 3.11**.

The launcher MUST NOT write to stdout. Its diagnostics go to the control directory and, as human-readable text, to stderr.

## 4. Limits

| Limit id | Flag | Linux (production) | macOS (development) | Windows (development) |
|---|---|---|---|---|
| deadline | host | applied by the host | applied by the host | applied by the host |
| process group | — | new session (`setsid`) | new session (`setsid`) | Job Object with `KILL_ON_JOB_CLOSE` |
| output | host | bounded by the host | bounded by the host | bounded by the host |
| `memory` | `--memory MiB` | `RLIMIT_AS` | not enforced: `setrlimit` rejects `RLIMIT_AS` | `JOB_OBJECT_LIMIT_PROCESS_MEMORY` |
| `cpu` | `--cpu SECONDS` | `RLIMIT_CPU`, soft N, hard N+1 | `RLIMIT_CPU`, soft N; no kill at the hard limit | not enforced |
| `file` | `--file MiB` | `RLIMIT_FSIZE` | `RLIMIT_FSIZE` | not enforced |
| `open_files` | `--open-files N` | `RLIMIT_NOFILE` | `RLIMIT_NOFILE` | not enforced |

The macOS column was measured on macOS 27.0.1 (Apple silicon) with Python 3.13. On that system `setrlimit(RLIMIT_AS)` fails, so `memory` is reported in `not_enforced`; and a program that ignores `SIGXCPU` keeps running past the hard CPU limit until the deadline stops it. The launcher reports what it could apply on the machine it runs on, so other macOS versions may differ; `not_enforced` is always the authority.

What each limit means:

- `memory` limits each process's **address space** on Linux and macOS, and each process's **committed memory** on Windows. Neither is resident memory. Numeric libraries that reserve address space for many threads can hit it early; hosts SHOULD set their thread-count variables (`OPENBLAS_NUM_THREADS`, `OMP_NUM_THREADS`, `MKL_NUM_THREADS`, `NUMEXPR_NUM_THREADS`) to 1, as HIE does.
- `cpu` limits user plus system CPU time of each process. At N seconds the launcher receives `SIGXCPU` and exits with 67; if Python cannot run the handler because native code holds the interpreter lock, the kernel kills the process at N+1 seconds.
- `file` limits the size of each file a process writes. It is not a disk quota: many small files can still fill a disk.
- `open_files` limits each process's open file descriptors.

Rules:

- Limits apply **per process**, not to the process tree as a whole. A child process gets its own allowance.
- There is no limit on the number of processes.
- On Linux, if the launcher cannot apply a requested limit, setup fails (section 5) and the program does not run.
- On macOS and Windows, a requested limit the launcher could not apply is listed in `not_enforced`, and the run continues. A limit applied with different semantics (memory on Windows) is not listed; this table records the difference.

## 5. The control directory and the handshake

### Why the control directory exists

The program owns stdin and stdout for its consumer protocol, and stderr is cut down to its last bytes. None of them can carry a message from the launcher to the host reliably. The control directory is the one channel between the launcher and the host that the program does not use, and it does four jobs:

1. **It is a barrier.** The launcher does not run the program until the host has read the child's process id and process group and has answered. A child process created by the program is therefore always inside a group the host already knows, and the host can kill it.
2. **It is the evidence of setup.** The ready file records which limits were applied, which were not (`not_enforced`) and the hash of the launcher that ran (`reference_sha256`). The host copies that evidence into the result instead of guessing.
3. **It separates "could not start" from "the program failed".** If no ready file arrives, or it reports an error, the run is `unavailable`. Once the host has acknowledged, every exit belongs to the program. Without the barrier, an interpreter message such as "can't open file" printed by the program would look like an infrastructure failure.
4. **It is portable.** Plain files and an atomic rename work the same way on Linux, macOS and Windows and from every host language. Inherited file descriptors, signals and named pipes do not.

The control directory is a sibling of the working directory, so it is not part of what the program sees as its current directory.

### Layout

The host MUST create, for each run, a fresh temporary directory with two empty subdirectories:

```
<run-dir>/
  work/      the program's working directory
  control/   passed to the launcher with --control
```

### Sequence

1. The launcher validates its arguments and checks that the program exists and is readable.
2. It creates the process group:
   - On Linux and macOS it calls `os.setsid()` unless the process is already a process-group leader, in which case it keeps the existing group.
   - On Windows it creates a Job Object, with a handle that child processes do not inherit and `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, and assigns itself to it.
3. It applies the requested limits (section 4) and installs its signal dispositions: a `SIGXCPU` handler that exits with 67, and `SIGXFSZ` ignored so that an oversized write raises `OSError` with `errno.EFBIG`.
4. It writes `control/ready.json` atomically, by writing a temporary file in the same directory and renaming it:

   ```json
   {
     "contract": "limited-process@1",
     "pid": 4242,
     "pgid": 4242,
     "platform": "linux",
     "applied": ["cpu", "memory"],
     "not_enforced": [],
     "reference_sha256": "<sha256 of limits.py as it exists on disk>",
     "error": null
   }
   ```

   `pgid` is `null` on Windows. `applied` and `not_enforced` contain limit ids sorted in ascending order. If step 1, 2 or 3 failed, the launcher writes the same file with `error` set to `{"code": "<id>", "message": "<text>"}` and exits with 64.
5. It waits for the file `control/ack` to exist, polling every 5 ms. If the file does not appear within the startup budget (`--startup-ms`, 10000 by default), it exits with 64.
6. It inserts the program's directory at the front of `sys.path`, sets `sys.argv` to the program path followed by its arguments, and runs the program with `runpy.run_path(program, run_name="__main__")`.

The host:

1. Starts the child and waits for `control/ready.json`, within the same startup budget.
2. Before the acknowledgement, kills only the child's process id when it needs to; no other process of the run exists yet.
3. Reads `ready.json` and keeps `pid`, `pgid`, `not_enforced` and `reference_sha256`.
4. If `error` is set, records the run as `unavailable`.
5. Otherwise creates `control/ack` (empty) and from then on treats `pgid` as the run's process group.

The startup budget is part of the deadline: the deadline runs from the moment the host starts the child.

## 6. Exit codes of the launcher

| How the program ended | Exit code |
|---|---|
| Returned normally, `SystemExit(None)` or `SystemExit(0)` | 0 |
| `SystemExit(n)` with 1 ≤ n ≤ 63 | n |
| `SystemExit` with any other value: greater than 63, negative, or not an integer | 1 |
| An uncaught exception | 1, with the traceback on stderr |
| Setup failed, or the acknowledgement did not arrive | 64 |
| `MemoryError` | 65 |
| `OSError` with `errno.EFBIG` | 66 |
| `SIGXCPU` | 67 |

Codes 64 to 79 belong to the launcher. A program that calls `os._exit()` bypasses this translation, and the interpreter itself exits with 120 when flushing standard streams fails at shutdown. Exit codes are advisory: a program can always produce any code it wants.

## 7. Host duties

### Environment

The host MUST build the child's environment from nothing. It contains:

- `PATH`, `LANG` and `LC_ALL`;
- on Windows, `SYSTEMROOT`, `TEMP` and `TMP`, without which the interpreter cannot start or create temporary files;
- the extra variables the host chooses to pass explicitly.

The host MUST NOT copy its own environment. `PYTHON*` variables have no effect because of `-I`.

### Working directory

The program's working directory is `<run-dir>/work`, created empty for the run. The host deletes `<run-dir>` after the run. A failed deletion does not change the result.

### Output

- stdin and stdout belong to the consumer protocol. In the one-shot pattern the host closes the child's stdin and captures stdout like stderr.
- The host MUST drain every captured stream continuously and concurrently, from the start of the run until the end of cleanup, into a ring buffer per stream (64 KiB by default; HIE keeps 64 KiB of stderr). It MUST NOT read a stream to its end before trimming, and it MUST NOT stop draining when a buffer is full: the program keeps running and the oldest bytes are discarded.
- `*_truncated` is `true` when the stream produced more bytes than the buffer holds.
- The tail is the last bytes of everything drained until the end of cleanup, untrimmed, decoded as UTF-8 with invalid sequences replaced by U+FFFD.
- `*_tail_bytes` is the number of raw bytes in the tail before decoding.

### Deadline, cancellation and the terminal transition

The host uses a monotonic clock. The deadline runs from the moment it starts the child.

A run is in exactly one state: `starting`, `running`, `finished`. The result is decided once, at the transition out of `running` (or out of `starting` for `unavailable`), and is then frozen:

1. If the host started a kill because the deadline expired, the verdict is `timed_out`.
2. If the host started a kill because the run was cancelled, the verdict is `cancelled`.
3. Otherwise the verdict comes from how the child ended (section 8).

A deadline that expires after a cancellation was decided does not change the result. Kills made during cleanup never change the verdict.

The host MUST offer a way to cancel a run. HIE cancels the call in flight when the engine stops (Ctrl-C or `SIGTERM`) or when the call is dropped.

### Killing

- Before the acknowledgement: kill the child's process id.
- After it, on Linux and macOS: `killpg(pgid, SIGKILL)`, then `kill(pid, SIGKILL)`, ignoring "no such process".
- After it, on Windows: terminate the child process. Closing the last handle of its Job Object kills every process in it.

### Cleanup

Cleanup runs after every run, including one that ended normally:

1. Kill the process group, so no descendant outlives the run.
2. Keep draining the captured streams until both reach end of file, for at most **1000 ms**. This budget is separate from the deadline.
3. Delete `<run-dir>`.

`cleanup` is `"complete"` when both streams reached end of file within the budget, and `"incomplete"` otherwise.

### Concurrency

The host limits how many runs execute at the same time. Limits on the program's own output protocol, such as the size of a result, belong to the consumer protocol.

## 8. The result

A host reports exactly one result per run. Version 1 has these fields and no others; consumers MUST ignore fields they do not know, so later versions can add some.

| Field | Type | Meaning |
|---|---|---|
| `contract` | string | `"limited-process@1"` |
| `platform` | string | `"linux"`, `"macos"` or `"windows"` |
| `verdict` | string | See below |
| `cause` | string or null | `"memory"`, `"cpu"`, `"file"`, `"unknown"` or `null` |
| `exit_code` | integer or null | See normalization |
| `signal` | integer or null | See normalization |
| `not_enforced` | array of strings | From `ready.json`, sorted; `[]` when none or when no ready file arrived |
| `reference_sha256` | string or null | From `ready.json`; `null` when no ready file arrived |
| `cleanup` | string | `"complete"` or `"incomplete"` |
| `elapsed_ms` | integer | From starting the child until the host first observed that it ended |
| `stdout_tail`, `stderr_tail` | string | Decoded tails |
| `stdout_tail_bytes`, `stderr_tail_bytes` | integer | Raw bytes in each tail |
| `stdout_truncated`, `stderr_truncated` | boolean | Whether bytes were discarded |

### Normalization of `exit_code` and `signal`

| Situation | `exit_code` | `signal` |
|---|---|---|
| The child could not be started | `null` | `null` |
| The host killed the run (`timed_out`, `cancelled`) | `null` | `null` |
| Linux or macOS, the child exited | its exit status | `null` |
| Linux or macOS, the child was killed by a signal | `null` | the signal number |
| Windows | the exit code as an unsigned 32-bit integer | `null` |

Some runtimes report a death by signal N as exit code 128 + N and cannot tell the two apart; Java and .NET do. A host on such a runtime MUST report an exit code between 129 and 192 as `signal: code − 128` and `exit_code: null`. A program that deliberately exits with 128 + N is reported as a signal by those hosts; the launcher never produces such codes.

### Verdicts

The host applies section 7's precedence first. Otherwise it takes the first matching row:

| Condition | `verdict` | `cause` |
|---|---|---|
| The child could not be started; no `ready.json` within the startup budget; or `ready.json` has an `error` | `unavailable` | `null` |
| Exit code 0 | `completed` | `null` |
| Exit code 65 | `limit_exceeded` | `memory` |
| Exit code 66 | `limit_exceeded` | `file` |
| Exit code 67 | `limit_exceeded` | `cpu` |
| Signal `SIGXCPU` or `SIGKILL` | `limit_exceeded` | `unknown` |
| Exit code 1 to 63 | `user_error` | `null` |
| Any other exit code or signal | `user_error` | `unknown` |

`SIGKILL` without a host kill usually means the CPU hard limit, but the kernel's out-of-memory killer and other processes send it too; `cause` says `unknown` for that reason.

## 9. Network

The contract leaves the network open. A host that needs to restrict it does so outside the launcher, for example by starting the child in a network namespace or a container. HIE does not restrict it.

## 10. Pinning

A consumer records two values: the contract id, `limited-process@1`, and the SHA-256 of the reference launcher it ships. HIE keeps both in `hie_runtime/limits.pin`, which the engine compiles in, and checks them twice:

1. **Before every start**, the engine reads `hie_runtime/limits.py` from disk and computes its SHA-256. If it differs from the pin, the worker is unavailable, with both hashes in the message, and no process is started.
2. **In the handshake**, `ready.json` must carry the pinned contract id and, as `reference_sha256`, the pinned hash: the hash of the launcher that actually ran. Otherwise the run is `unavailable` and the program never runs.

`.gitattributes` marks `limits.py` and `limits.pin` as binary (`-text`), so a checkout never changes their line endings, and with them the hash. How to re-vendor the launcher is in [`hie_runtime/README.md`](../hie_runtime/README.md).

### Revisions of the reference launcher

| SHA-256 | Date | Change |
|---|---|---|
| `1e6c8a988d9e6a40b4d5e0014206af094713cde74dc2685d22c6473e9911c17f` | 2026-11-05 | First version |

## 11. The long-lived worker

One process serves many requests, one at a time. Use it when starting the interpreter and importing libraries costs more than a request, and the requests are small. HIE runs every transform this way: the protocol is `{"ready": true}` once, then one `{"id": ..., "resources": [...]}` or `{"id": ..., "error": {...}}` line per request line.

- **Startup.** The startup budget runs from starting the child until it is ready: the control handshake and, if the protocol has one, the program's own ready line (for example `ready` or `{"ready": true}`). A program that exits or stays silent past the budget is `unavailable`.
- **Requests.** The host writes one request and waits for one reply. The request's deadline starts **before the first attempt to write it**, so a program that stops reading stdin cannot block the host: the write waits, the deadline expires, the request is `timed_out`.
- **Framing.** One line per message, with a maximum line length. Without the interpreter's `-u`, a reply could sit in a buffer while the host waits for it; the launch command always includes `-u`. In HIE, `worker.py` writes the protocol to a duplicate of stdout and points file descriptor 1 at stderr, so nothing else the transform runs can write into it; a line that is not a reply anyway ends the process like an out-of-order reply.
- **Generations.** After a timed-out request the host kills the process group and starts a new process for the next request. Each process is a generation, and output from an old generation is discarded, so a late reply can never be taken as the answer to a newer request.
- **No retries.** A request that timed out may have had effects. The host reports it and moves on; whoever sent it decides whether to send it again.
- **State.** Anything the program kept in memory, such as caches, is lost when a new generation starts.
- **Limits.** Memory, file size and open files apply for the whole life of the process. Leave `--cpu` out: CPU time accumulates, so a limit would eventually kill a healthy worker.
- **Credentials.** A worker that calls another service gets its token as an explicit environment extra. The program can read it, which is accepted by the [trust model](#12-trust-model).

## 12. Trust model

The people who write the programs are known and authenticated, and they do not try to break out. The contract protects the host and the other runs against **accidents**: a loop that never ends, memory that keeps growing, a file that fills the disk, output that never stops, child processes left behind. It is not a security boundary against hostile code.

| Accident | What stops it |
|---|---|
| A program that never ends | The host's deadline, then a kill of the process group |
| Runaway memory | `memory` (Linux; Windows with different semantics) |
| A busy loop | `cpu` (Linux, macOS) and the deadline everywhere |
| A huge file | `file` (Linux, macOS) |
| Too many open files | `open_files` (Linux, macOS) |
| A flood of output | The host's ring buffers: the program keeps running and only the last bytes are kept |
| Child processes left behind | The process group (Linux, macOS) or the Job Object (Windows), killed after every run |
| Settings leaking from the host | An environment built from nothing and an isolated interpreter (`-I`) |
| A run that never started looking like a failed program | The control handshake |

What the contract does not protect against:

- **Escaping the process group.** A program that calls `setsid()` itself, or detaches a child twice, leaves the group and survives the kill. The host's cleanup cannot see it.
- **Aggregate use.** Limits apply to each process. A program that starts several children can use several times each limit, and nothing limits how many processes it starts.
- **Disk space.** `file` limits the size of each file, not the total. Many smaller files can still fill a disk.
- **The filesystem.** The working directory is fresh and empty, but the program can read and write anything the host's user can, anywhere.
- **The network.** It is open unless the host closes it (section 9).
- **Secrets passed on purpose.** Every environment extra is readable by the program.
- **Honest exit codes.** The launcher's codes are advisory. A program can exit with any code, including the launcher's own, through `os._exit()`.
- **Locked limits on Windows.** The Job Object's handle lives in the child, so a program could change its limits. On Linux and macOS a program cannot raise a limit above its hard value, which the launcher sets (for `cpu`, one second above the soft value).
- **Memory on macOS.** `setrlimit(RLIMIT_AS)` is rejected there, so `memory` is reported in `not_enforced`, and a program that ignores `SIGXCPU` keeps running until the deadline.
- **A host that dies.** If the host process is killed, nothing kills the child's process group for it. In production the host and its children usually run in the same container, so stopping the container stops both.
