"""Reference launcher for the limited-process@1 contract.

Run it as the child process of a host:

    python -I -B -u -X utf8 limits.py --control DIR [--startup-ms MS]
        [--memory MiB] [--cpu SECONDS] [--file MiB] [--open-files N]
        -- program.py [arguments...]

It creates a process group, applies the requested limits, reports them in
DIR/ready.json, waits for the host to create DIR/ack, runs the program and
translates how the program ended into an exit code. It never writes to
stdout, which belongs to the program. See docs/contract.md.

Standard library only. Python 3.9 or later.
"""

import errno
import hashlib
import json
import os
import runpy
import signal
import sys
import time
import traceback

CONTRACT = "limited-process@1"

EXIT_SETUP = 64
EXIT_MEMORY = 65
EXIT_FILE = 66
EXIT_CPU = 67

USER_EXIT_MAX = 63
ACK_POLL_SECONDS = 0.005
DEFAULT_STARTUP_MS = 10_000
MIB = 1024 * 1024

# Limit ids, the flag that sets each one, and whether its value is in MiB.
LIMITS = {
    "memory": ("--memory", True),
    "cpu": ("--cpu", False),
    "file": ("--file", True),
    "open_files": ("--open-files", False),
}

if sys.platform.startswith("linux"):
    PLATFORM = "linux"
elif sys.platform == "darwin":
    PLATFORM = "macos"
elif sys.platform == "win32":
    PLATFORM = "windows"
else:
    PLATFORM = sys.platform

PRODUCTION = PLATFORM == "linux"


class SetupError(Exception):
    """A failure before the program runs; reported in ready.json."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def warn(message):
    try:
        sys.stderr.write("limits.py: " + message + "\n")
    except Exception:
        pass


# --- arguments ---------------------------------------------------------------

def parse_arguments(argv):
    """Return (control, startup_ms, limits, program, program_args)."""
    control = None
    startup_ms = DEFAULT_STARTUP_MS
    limits = {}
    flags = {flag: limit_id for limit_id, (flag, _) in LIMITS.items()}
    index = 0
    while index < len(argv):
        argument = argv[index]
        if argument == "--":
            index += 1
            break
        if index + 1 >= len(argv):
            raise SetupError("usage", f"{argument} needs a value")
        value = argv[index + 1]
        if argument == "--control":
            control = value
        elif argument == "--startup-ms":
            startup_ms = positive_integer(argument, value)
        elif argument in flags:
            limits[flags[argument]] = positive_integer(argument, value)
        else:
            raise SetupError("usage", f"unknown argument {argument}")
        index += 2
    else:
        raise SetupError("usage", "missing -- before the program")
    if index >= len(argv):
        raise SetupError("usage", "missing the program after --")
    return control, startup_ms, limits, argv[index], argv[index + 1:]


def positive_integer(flag, value):
    try:
        number = int(value, 10)
    except ValueError:
        raise SetupError("usage", f"{flag} must be an integer, got {value!r}") from None
    if number <= 0:
        raise SetupError("usage", f"{flag} must be greater than zero, got {number}")
    return number


# --- process group -------------------------------------------------------------

_job_handle = None  # kept for the life of the process: closing it kills the job


def create_process_group():
    """Return the process group id, or None on Windows."""
    if PLATFORM == "windows":
        create_job_object()
        return None
    try:
        if os.getpgid(0) != os.getpid():
            os.setsid()
        return os.getpgid(0)
    except OSError as error:
        raise SetupError("process_group", f"cannot create a process group: {error}") from None


def windows_job_api():
    import ctypes
    from ctypes import wintypes

    class BasicLimits(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class IoCounters(ctypes.Structure):
        _fields_ = [(name, ctypes.c_uint64) for name in (
            "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
            "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]

    class ExtendedLimits(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", BasicLimits),
            ("IoInfo", IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    return ctypes, kernel32, ExtendedLimits


JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x00000100
JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION = 0x00000400
JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000

_windows_memory_bytes = None


def create_job_object():
    global _job_handle
    ctypes, kernel32, ExtendedLimits = windows_job_api()
    # No security attributes: the handle is not inherited by child processes,
    # so this process holds the only handle and its exit closes the job.
    handle = kernel32.CreateJobObjectW(None, None)
    if not handle:
        raise SetupError("process_group",
                         f"CreateJobObjectW failed with error {ctypes.get_last_error()}")
    _job_handle = handle
    set_job_limits(None)
    if not kernel32.AssignProcessToJobObject(handle, kernel32.GetCurrentProcess()):
        raise SetupError("process_group",
                         f"AssignProcessToJobObject failed with error {ctypes.get_last_error()}")


def set_job_limits(memory_bytes):
    ctypes, kernel32, ExtendedLimits = windows_job_api()
    info = ExtendedLimits()
    info.BasicLimitInformation.LimitFlags = (
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION)
    if memory_bytes is not None:
        info.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_PROCESS_MEMORY
        info.ProcessMemoryLimit = memory_bytes
    if not kernel32.SetInformationJobObject(
            _job_handle, JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(info), ctypes.sizeof(info)):
        return ctypes.get_last_error()
    return 0


# --- limits ----------------------------------------------------------------------

def apply_limits(limits):
    """Apply each requested limit. Return (applied, not_enforced)."""
    applied, not_enforced = [], []
    for limit_id in sorted(limits):
        value = limits[limit_id]
        failure = apply_limit(limit_id, value)
        if failure is None:
            applied.append(limit_id)
        elif PRODUCTION:
            raise SetupError("limit_" + limit_id, failure)
        else:
            not_enforced.append(limit_id)
            warn(f"{limit_id} is not enforced on {PLATFORM}: {failure}")
    return applied, not_enforced


def apply_limit(limit_id, value):
    """Apply one limit. Return None on success or the reason it failed."""
    if PLATFORM == "windows":
        if limit_id != "memory":
            return "Windows has no per-process mechanism for this limit"
        error = set_job_limits(value * MIB)
        return None if error == 0 else f"SetInformationJobObject failed with error {error}"

    import resource
    if limit_id == "memory":
        return set_rlimit(resource.RLIMIT_AS, value * MIB, value * MIB)
    if limit_id == "cpu":
        # SIGXCPU at the soft limit; the kernel kills at the hard limit if the
        # handler cannot run because native code holds the interpreter lock.
        return set_rlimit(resource.RLIMIT_CPU, value, value + 1)
    if limit_id == "file":
        return set_rlimit(resource.RLIMIT_FSIZE, value * MIB, value * MIB)
    if limit_id == "open_files":
        return set_rlimit(resource.RLIMIT_NOFILE, value, value)
    return f"unknown limit {limit_id}"


def set_rlimit(which, soft, hard):
    import resource
    try:
        resource.setrlimit(which, (soft, hard))
    except (ValueError, OSError) as error:
        return f"setrlimit failed: {error}"
    return None


def install_signal_dispositions():
    if PLATFORM == "windows":
        return
    import resource
    try:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    except (ValueError, OSError):
        pass
    signal.signal(signal.SIGXCPU, lambda signum, frame: os._exit(EXIT_CPU))
    # An oversized write then fails with OSError(EFBIG) instead of killing us.
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)


# --- control directory -------------------------------------------------------------

def reference_sha256():
    try:
        with open(os.path.abspath(__file__), "rb") as source:
            return hashlib.sha256(source.read()).hexdigest()
    except OSError:
        return None


def write_ready(control, record):
    path = os.path.join(control, "ready.json")
    temporary = path + ".tmp"
    with open(temporary, "w", encoding="utf-8") as ready:
        json.dump(record, ready)
    os.replace(temporary, path)


def ready_record(pgid, applied, not_enforced, error=None):
    return {
        "contract": CONTRACT,
        "pid": os.getpid(),
        "pgid": pgid,
        "platform": PLATFORM,
        "applied": applied,
        "not_enforced": not_enforced,
        "reference_sha256": reference_sha256(),
        "error": None if error is None else {"code": error.code, "message": error.message},
    }


def wait_for_ack(control, startup_ms):
    ack = os.path.join(control, "ack")
    deadline = time.monotonic() + startup_ms / 1000
    while not os.path.exists(ack):
        if time.monotonic() >= deadline:
            return False
        time.sleep(ACK_POLL_SECONDS)
    return True


# --- running the program -------------------------------------------------------------

def is_internal_frame(filename):
    if filename.startswith("<frozen runpy"):
        return True
    path = os.path.normcase(os.path.abspath(filename))
    return path in (os.path.normcase(os.path.abspath(__file__)),
                    os.path.normcase(os.path.abspath(runpy.__file__)))


def print_user_traceback(error):
    """Print the traceback without the launcher's and runpy's own frames."""
    tb = error.__traceback__
    while tb is not None and is_internal_frame(tb.tb_frame.f_code.co_filename):
        tb = tb.tb_next
    try:
        traceback.print_exception(type(error), error, tb, file=sys.stderr)
    except Exception:
        warn(f"{type(error).__name__}: {error}")


def run_program(program, program_args):
    sys.path.insert(0, os.path.dirname(program))
    sys.argv = [program, *program_args]
    try:
        runpy.run_path(program, run_name="__main__")
    except SystemExit as exit_request:
        return user_exit_code(exit_request.code)
    except MemoryError as error:
        print_user_traceback(error)
        return EXIT_MEMORY
    except OSError as error:
        print_user_traceback(error)
        return EXIT_FILE if error.errno == errno.EFBIG else 1
    except BaseException as error:  # noqa: BLE001 - every other ending is the program's
        print_user_traceback(error)
        return 1
    return 0


def user_exit_code(code):
    if code is None:
        return 0
    if isinstance(code, int) and 0 <= code <= USER_EXIT_MAX:
        return code
    if not isinstance(code, int):
        try:
            sys.stderr.write(f"{code}\n")
        except Exception:
            pass
    return 1


def setup(argv):
    """Everything before the program runs. Return (control, ...) or exit."""
    control = None
    try:
        control, startup_ms, limits, program, program_args = parse_arguments(argv)
        if control is None:
            raise SetupError("usage", "--control is required")
        if not os.path.isdir(control):
            raise SetupError("usage", f"control directory does not exist: {control}")
        control = os.path.abspath(control)
        if sys.version_info < (3, 9):
            raise SetupError("python_version", "Python 3.9 or later is required")
        program = os.path.abspath(program)
        if not os.path.isfile(program) or not os.access(program, os.R_OK):
            raise SetupError("program_missing", f"cannot read the program: {program}")
        pgid = create_process_group()
        applied, not_enforced = apply_limits(limits)
        install_signal_dispositions()
    except SetupError as error:
        warn(error.message)
        if control is not None and os.path.isdir(control):
            try:
                write_ready(control, ready_record(None, [], [], error))
            except OSError:
                pass
        sys.exit(EXIT_SETUP)
    write_ready(control, ready_record(pgid, applied, not_enforced))
    if not wait_for_ack(control, startup_ms):
        warn("the host did not acknowledge within the startup budget")
        sys.exit(EXIT_SETUP)
    return program, program_args


def main():
    program, program_args = setup(sys.argv[1:])
    sys.exit(run_program(program, program_args))


if __name__ == "__main__":
    main()
