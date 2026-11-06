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

Each source runs its transform in a Python process of its own, started by the engine from `hie_runtime/worker.py`, fed one record at a time over stdin/stdout as JSON lines, and restarted if it dies. Try (`POST /api/transforms/{name}/try`) runs the code the same way, in a worker of its own. The engine no longer sends transforms to a remote runtime service, and does not ignore the old configuration: a top-level `runtime` key in `hie.yaml` (whatever its value) or a non-empty `HIE_RUNTIME_URL` stops it at start, `hie check` included.

### Terminology

With `terminology_url` in `hie.yaml` (or `HIE_TERMINOLOGY_SERVER`; `HIE_TERMINOLOGY_TOKEN` adds a bearer token) naming an HTS, a transform can ask the terminology server:

```python
from hie import DataError, lookup, translate

known = lookup("http://loinc.org", code)                     # CodeSystem/$lookup: {"display": ...} or None
mapped = translate(code, "urn:lab:north:codes", "http://loinc.org")  # ConceptMap/$translate: {"code", "display", "system"} or None
```

Answers are cached for the life of the worker. A code the server does not know is `None`, and the transform decides (usually a `DataError`); a server that cannot be reached raises `TerminologyError`, which the engine treats as **transient**: the record waits and is tried again, nothing is rejected. Without a server both helpers answer `None`, so a transform keeps a fallback or rejects. `transforms/lab_results.py` does both: a LOINC code must be known to the server when there is one, and takes its display from it; a local code goes through the server's ConceptMap first and the script's own table second.
