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

By default each source runs its transform in a Python process of its own, started by the engine from `hie_runtime/worker.py`, fed one record at a time over stdin/stdout as JSON lines, and restarted if it dies. With a **shared runtime** configured, transforms run at [python-runtime](https://github.com/HeliosSoftware/python-runtime) instead, in a sandbox, through its HIE facade:

```yaml
runtime:
  url: http://127.0.0.1:8790        # HIE_RUNTIME_URL overrides it
  token_env: HIE_RUNTIME_TOKEN      # the consumer's bearer token, read from the environment
  batch_size: 200                   # records per call (at most 1000; the runtime partitions them further)
```

Each version's code is stored at the runtime under a name of its own, `{transform}-v{version}-{12 hex of its sha256}` (`PUT /transforms/{name}`, naming the version's environment; the sha256 the runtime answers must match), each chunk is one call (`POST /transforms/{name}/run`, naming the environment too) and one reply per record comes back, in the worker's own line format, so the outcomes (`resources`, `data` error, `transform` error with its trace) are the same as locally; per-record `diagnostics` (warnings, events, location) are passed on with them. A runtime that forgot the code (`404 transform_unknown`, after a restart without its store) gets it stored again before the batch runs. A runtime that is busy, not ready or unreachable, or a transform that fails to load there, leaves the records received and the batch is tried again on the next pass; the reason is in the log. Names no pin, load or retry still needs are deleted (`DELETE /transforms/{name}`), at most once a minute. The runtime's sandbox has no network, so the terminology helpers answer `None` there, and `hie.py` is the runtime's own pinned copy.

### Versions

A transform has a stable id and immutable numbered versions, kept in the engine's store ([API](api.md#transforms-and-versions), decision 35). Each version records the environment it runs in: `local` for the engine's worker, or at the runtime the consumer's default environment (`GET /v1/capabilities`) when it was saved. A version runs only there: when the runtime reports another environment, nothing it returned is kept, and the load or retry waits (`waiting: environment_unavailable`) until the environment is back.

- Every load fixes its version when it starts and keeps it until it ends; a save never restarts a source or changes a running load. Sources pin a version or follow the current one (`PUT /api/sources/{id}/pin`).
- A file under `transforms/` found at start is a transform; a file a 0.1 definition names (`transform: transforms/x.py`) follows that file's transform. A file is identified by its path, which one transform owns. Its id is its stem when the stem is a valid id (`[a-z0-9][a-z0-9_-]{0,63}`) and free; otherwise (another file holds the stem, or the stem has upper case or other characters) the stem lower-cased with 8 hex of the file's path (`Lab.py` is `lab-1f0c…`). When a file's content changes from what the engine last wrote or read there, it becomes a new version with `author: "file"`; the file under `transforms/` is rewritten with the current version after every save, so 0.1 tools keep reading the code there.
- `PUT /api/transforms/{name}` takes the 0.1 names (`[A-Za-z0-9_-]{1,64}`): a name that is a valid id is the transform's id; any other gets an id as a file's stem does, and keeps the name as given in 0.1 answers and as its file name.
- A load is admitted only with its version fixed. While no version can be fixed (the runtime's environment is not known yet after a start, the transform or version is gone), the object stays at its source and the next pass takes it.
- Locally, each source has a worker of its own per version, over that version's code written from the store (never the file a person may be editing), so module-level state never crosses sources; it stops when the source is replaced or deleted. What a transform prints is discarded, never logged. The engine warns at start that it runs transforms with its own rights when no `runtime:` is configured; with one configured nothing ever runs on the local worker (a runtime that is down holds the rows, and Try answers `503 executor_unavailable`).

### Try

`POST /api/transforms/try` (code inline) and `POST /api/transforms/{id}/try` (a saved version) run up to 100 records once and store nothing ([API](api.md#try), decision 39). At the runtime that is `POST /transforms/try`, with the version's environment named for a saved version (no redirect followed, no automatic retry); locally a fresh worker of its own, which returns each record's own output (at most 4 KiB per stream) in its diagnostics. A run is dispatched at most once: an answer that was lost, redirected, or that the facade did not sign, is `indeterminate` and is never sent again, and its run id cannot be used again for 24 hours. A saved version's results are shown only when the answer's environment headers prove it ran in its own environment; otherwise the route answers `409 environment_unavailable` and the run keeps its id. The deadline covers the whole run, `$validate` included. Cancel (`POST /api/transforms/try-runs/{run}/cancel`) and the deadline stop it (the request to the runtime is dropped, which stops the partition; a local worker is killed). Each item's diagnostics come back exactly as the executor returned them, `stdout` and `stderr` included; they are never stored or logged.

### Terminology

With `terminology_url` in `hie.yaml` (or `HIE_TERMINOLOGY_SERVER`; `HIE_TERMINOLOGY_TOKEN` adds a bearer token) naming an HTS, a transform can ask the terminology server:

```python
from hie import DataError, lookup, translate

known = lookup("http://loinc.org", code)                     # CodeSystem/$lookup: {"display": ...} or None
mapped = translate(code, "urn:lab:north:codes", "http://loinc.org")  # ConceptMap/$translate: {"code", "display", "system"} or None
```

Answers are cached for the life of the worker. A code the server does not know is `None`, and the transform decides (usually a `DataError`); a server that cannot be reached raises `TerminologyError`, which the engine treats as **transient**: the record waits and is tried again, nothing is rejected. Without a server both helpers answer `None`, so a transform keeps a fallback or rejects. `transforms/lab_results.py` does both: a LOINC code must be known to the server when there is one, and takes its display from it; a local code goes through the server's ConceptMap first and the script's own table second.
