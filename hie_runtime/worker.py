"""Runs a transform script for HIE.

Protocol, one JSON object per line:
  request  {"id": 1, "record": {...}, "context": {"feed": ..., "file": ..., "line": ...}}
  response {"id": 1, "resources": [...]}
        or {"id": 1, "error": {"kind": "data" | "transform" | "transient", "message": ...}}

`data` rejects the record, `transform` rejects it as a bug in the script, and
`transient` (a terminology server that cannot be reached) makes the engine try
the record again later.

The script defines `transform(record, context)` returning a resource, a list of
resources or None. It raises `hie.DataError` for bad source data.
"""

import errno
import importlib.util
import json
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


def main():
    # The protocol is UTF-8 whatever the locale says: on Windows Python would
    # otherwise read the engine's UTF-8 as cp1252 and turn every accented letter
    # into a lone surrogate that the engine cannot parse back.
    sys.stdin.reconfigure(encoding="utf-8", errors="strict")
    sys.stdout.reconfigure(encoding="utf-8", errors="strict")
    protocol = sys.stdout
    sys.stdout = sys.stderr
    module = load(sys.argv[1])
    protocol.write(json.dumps({"ready": True}) + "\n")
    protocol.flush()
    for line in sys.stdin:
        if not line.strip():
            continue
        request = json.loads(line)
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
        except MemoryError:  # past the memory limit: the process ends and the engine says why
            raise
        except Exception as e:  # a bug in the transform, kept with its traceback
            if isinstance(e, OSError) and e.errno == errno.EFBIG:
                raise  # a file past the size limit: likewise
            reply = {
                "id": request["id"],
                "error": {
                    "kind": "transform",
                    "message": f"{type(e).__name__}: {e}",
                    "trace": traceback.format_exc(limit=5),
                },
            }
        protocol.write(json.dumps(reply) + "\n")
        protocol.flush()


if __name__ == "__main__":
    main()
