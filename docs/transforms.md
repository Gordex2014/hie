# Transforms

Transforms are Python: one record in, FHIR resources out. Back to the [README](../README.md).

## Transforms

```python
from hie import DataError

def transform(record, context):
    # record: one CSV row as a dict of strings, one NDJSON line, or one Bundle entry's resource
    # context: {"feed", "file", "line"}
    if not record["value"]:
        raise DataError("missing value")           # rejected with this reason
    return {"resourceType": "Observation",
            "identifier": [{"system": "urn:...", "value": "..."}],  # makes re-sends updates
            ...}
```

- A transform returns a resource, a list of resources, or `None` (filtered out).
- Every resource needs an `id` (delivered as `PUT Type/id`) or an `identifier` with system and value (delivered as `PUT Type?identifier=system|value`), so sending the same record twice updates instead of duplicating.
- `DataError` rejects the row with its message. Any other exception is recorded with its traceback as a transform bug.
- Data that already is FHIR (NDJSON exports, Bundle files) goes through `transforms/fhir_passthrough.py`. A Bundle is split into one record per entry, and references between entries (`urn:uuid:...`) are rewritten to `Type/id`.
- On the REST path the engine transforms and delivers **at the same time**: each chunk of transformed records goes out in batch Bundles while Python works on the next, so a large file takes about the longer of the two rather than their sum.

### Where transforms run

Each source runs its transform in a Python process of its own, started by the engine from `hie_runtime/worker.py` and fed one record at a time over stdin/stdout as JSON lines. The worker keeps that stdout to itself: whatever else writes to standard output (a `print()`, a program the transform starts, a native library) goes to the worker's stderr. Try (`POST /api/transforms/{name}/try`) runs the code the same way, in a worker of its own; at most four tries run at once, and one that cannot get a turn within 10 seconds is answered `503`.

The worker runs under the `limited-process@1` contract: the engine starts it as `python -I -B -u -X utf8 hie_runtime/limits.py --control … -- hie_runtime/worker.py <transform>`, where `limits.py` is the contract's reference launcher, kept unchanged and pinned by its SHA-256 in `hie_runtime/limits.pin`. The launcher puts the worker in a process group of its own and applies the limits in the `worker:` block of `hie.yaml` (see [Running HIE](running.md#the-transform-worker)); the engine hashes `limits.py` before it starts each worker, checks the contract and hash the launcher reports before it lets the transform run, and refuses a launcher that does not match. In practice:

- **Python 3.11 or later.** `-I` isolates the interpreter: user site-packages, the working directory and every `PYTHON*` variable are ignored. A transform can import the standard library, the packages installed in the interpreter (point `python:` at a virtual environment's interpreter for more) and `hie`, the helpers in `hie_runtime/`. Modules next to the transform file are not on its path.
- **Limits per process.** Address space (`memory_mib`, 2048 MiB by default), the size of any file it writes (`file_mib`, 256 MiB) and open files (`open_files`, 256). There is no CPU-time limit: CPU time adds up over the life of a long-lived worker, so a limit would end a healthy one; the per-record deadline stops a transform that spins instead.
- **A deadline per record.** `request_seconds` (60 s) runs from before the record is written to the worker until its reply; `startup_seconds` (10 s) covers starting the worker and importing the transform.
- **A clean environment.** The worker sees `PATH`, `LANG` and `LC_ALL` (and on Windows `SYSTEMROOT`, `TEMP`, `TMP`), `OPENBLAS_NUM_THREADS`, `OMP_NUM_THREADS`, `MKL_NUM_THREADS` and `NUMEXPR_NUM_THREADS` set to 1 (numeric libraries reserve address space per thread), `HIE_TERMINOLOGY_SERVER` when a terminology server is configured, and `HIE_TERMINOLOGY_TOKEN` when the engine has one. Nothing else from the engine's environment reaches it.
- **A fresh working directory** for every worker process, empty, deleted when the process ends.
- **What a failure does to the record.** A record whose transform does not answer within the deadline, runs out of memory, writes a file past its limit, crashes the process, answers out of turn or has something else write a line to the worker's stdout is **rejected** with kind `transform` and the reason (with the end of the worker's stderr), and the next record gets a new worker process. It is never sent again by the engine, because a transform that timed out may already have had effects; a person reprocesses it (`POST /api/records/{id}/reprocess`) after a fix. A worker process that exits on its own between records is cleaned up at once, and the next record gets a new one. A worker that cannot start at all (the transform does not import, the interpreter is missing) leaves the records received, and the next pass tries again; the interpreter is looked up again for every new worker, so installing it or fixing `PATH` takes effect without a restart. The engine stores each record's outcome before it sends the next.
- **Stopping.** A source that is replaced or deleted stops after its current record, whose outcome is stored as usual; what it has not delivered yet waits for the next runner of that source. Two runners of a source never work on the same record at the same time: the replacement (a new definition, the same id created again after a delete, or a restart after its transform was saved) starts only after the old runner has stopped. If the old runner is still busy `max(request_seconds, 120 s) + 30 s` after the replacement began waiting for it, the old runner is aborted and the replacement waits for it to end. The API call itself returns at once. Records an aborted runner had not stored an outcome for stay received and are done by the next runner, if there is one, and a delivery it had already sent may be sent again; that second write is an update of the same resource, by id or identifier, not a new one. A replacement that waits for the old runner to stop does not itself repeat a delivery; a forced abort, like an engine crash or a retry after an unclear answer from the FHIR server, can. A runner blocked inside a synchronous database call cannot be aborted until that call returns. A pause takes effect when the running pass ends. When the engine stops (Ctrl-C or `SIGTERM`), the call in flight is cancelled: its record has no outcome and stays received, and every worker's process group is killed and its run directory removed before the engine exits.

This protects the engine and the other sources against accidents (an endless loop, runaway memory, a huge file, leftover child processes), not against hostile code: the network is open, the transform can read and write anything the engine's user can, limits apply to each process rather than to a transform and the processes it starts, and a process that leaves its group survives the kill. On Linux every limit is enforced, or the worker does not start; on macOS and Windows, which are for development, the engine logs the limits the launcher could not apply. The engine no longer sends transforms to a remote runtime service, and does not ignore the old configuration: a top-level `runtime` key in `hie.yaml` (whatever its value) or a non-empty `HIE_RUNTIME_URL` stops it at start, `hie check` included.

### Terminology

With `terminology_url` in `hie.yaml` (or `HIE_TERMINOLOGY_SERVER`; `HIE_TERMINOLOGY_TOKEN` adds a bearer token) naming an HTS, a transform can ask the terminology server:

```python
from hie import DataError, lookup, translate

known = lookup("http://loinc.org", code)                     # CodeSystem/$lookup: {"display": ...} or None
mapped = translate(code, "urn:lab:north:codes", "http://loinc.org")  # ConceptMap/$translate: {"code", "display", "system"} or None
```

Answers are cached for the life of the worker. A code the server does not know is `None`, and the transform decides (usually a `DataError`); a server that cannot be reached raises `TerminologyError`, which the engine treats as **transient**: the record waits and is tried again, nothing is rejected. Without a server both helpers answer `None`, so a transform keeps a fallback or rejects. `transforms/lab_results.py` does both: a LOINC code must be known to the server when there is one, and takes its display from it; a local code goes through the server's ConceptMap first and the script's own table second.
