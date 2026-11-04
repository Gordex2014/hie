"""Runs a transform script for HIE.

Protocol, one JSON object per line. The first line the worker writes is
{"ready": true}, or {"ready": false, "load_error": {"message", "trace"}} when the
transform did not load (then it exits). After that:
  request  {"id": 1, "record": {...}, "context": {"feed": ..., "file": ..., "line": ...}}
  response {"id": 1, "resources": [...]}
        or {"id": 1, "error": {"kind": "data" | "transform" | "transient", "message": ...}}

`data` rejects the record, `transform` rejects it as a bug in the script, and
`transient` (a terminology server that cannot be reached) makes the engine try
the record again later.

The script defines `transform(record, context)` returning a resource, a list of
resources or None. It raises `hie.DataError` for bad source data.
"""

import importlib.util
import io
import json
import os
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from hie import DataError, TerminologyError  # noqa: E402


def load(path):
    spec = importlib.util.spec_from_file_location("hie_transform", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not callable(getattr(module, "transform", None)):
        raise SystemExit(f"{path} does not define transform(record, context)")
    return module


# With HIE_WORKER_CAPTURE=1 (Try) each record's own stdout and stderr come back
# in its reply's diagnostics, at most this many bytes of UTF-8 each; otherwise
# they go to the worker's stderr, which the engine discards.
STREAM_LIMIT = 4096


class BoundedStream(io.TextIOBase):
    """A text stream that keeps only the first `limit` bytes of UTF-8 written
    to it, cut on a character boundary; the rest is dropped as it is written,
    and `truncated` says so."""

    def __init__(self, limit=STREAM_LIMIT):
        super().__init__()
        self.limit = limit
        self.size = 0
        self.parts = []
        self.truncated = False

    @property
    def encoding(self):
        return "utf-8"

    def writable(self):
        return True

    def write(self, text):
        if not isinstance(text, str):
            raise TypeError(f"write() argument must be str, not {type(text).__name__}")
        if self.truncated or not text:
            if text:
                self.truncated = True
            return len(text)
        data = text.encode("utf-8", "replace")
        room = self.limit - self.size
        if len(data) <= room:
            self.parts.append(text)
            self.size += len(data)
        else:
            # Whole characters only: a cut inside one drops that character.
            kept = data[:room].decode("utf-8", "ignore")
            self.parts.append(kept)
            self.size += len(kept.encode("utf-8"))
            self.truncated = True
        return len(text)

    def getvalue(self):
        return "".join(self.parts)


def captured(stdout, stderr):
    diagnostics = {}
    for name, stream in (("stdout", stdout), ("stderr", stderr)):
        text = stream.getvalue()
        if text:
            diagnostics[name] = text
    if stdout.truncated or stderr.truncated:
        diagnostics["truncated"] = True
    return diagnostics


def main():
    # The protocol is UTF-8 whatever the locale says: on Windows Python would
    # otherwise read the engine's UTF-8 as cp1252 and turn every accented letter
    # into a lone surrogate that the engine cannot parse back.
    sys.stdin.reconfigure(encoding="utf-8", errors="strict")
    sys.stdout.reconfigure(encoding="utf-8", errors="strict")
    protocol = sys.stdout
    sys.stdout = sys.stderr
    try:
        module = load(sys.argv[1])
    except BaseException as e:  # noqa: BLE001 - the engine shows why the code did not load
        message = str(e) if isinstance(e, SystemExit) else f"{type(e).__name__}: {e}"
        load_error = {"message": message, "trace": traceback.format_exc(limit=5)}
        protocol.write(json.dumps({"ready": False, "load_error": load_error}) + "\n")
        protocol.flush()
        sys.exit(1)
    protocol.write(json.dumps({"ready": True}) + "\n")
    protocol.flush()
    capture = os.environ.get("HIE_WORKER_CAPTURE") == "1"
    streams = (sys.stdout, sys.stderr)
    for line in sys.stdin:
        if not line.strip():
            continue
        request = json.loads(line)
        if capture:
            sys.stdout, sys.stderr = BoundedStream(), BoundedStream()
        try:
            result = module.transform(request["record"], request["context"])
            if result is None:
                result = []
            elif isinstance(result, dict):
                result = [result]
            reply = {"id": request["id"], "resources": result}
        except DataError as e:
            reply = {"id": request["id"], "error": {"kind": "data", "message": str(e)}}
        except TerminologyError as e:  # the server, not the row: the engine retries later
            reply = {"id": request["id"], "error": {"kind": "transient", "message": str(e)}}
        except Exception as e:  # a bug in the transform, kept with its traceback
            reply = {
                "id": request["id"],
                "error": {
                    "kind": "transform",
                    "message": f"{type(e).__name__}: {e}",
                    "trace": traceback.format_exc(limit=5),
                },
            }
        if capture:
            diagnostics = captured(sys.stdout, sys.stderr)
            sys.stdout, sys.stderr = streams
            if diagnostics:
                reply["diagnostics"] = diagnostics
        protocol.write(json.dumps(reply) + "\n")
        protocol.flush()


if __name__ == "__main__":
    main()
