# API and authentication

The HTTP API clients and UIs use, and how it and the engine's own calls to HFS are authenticated. Back to the [README](../README.md).

- [API](#api): every route, with a note on the ones 0.2.0 changes.
- [The 0.2.0 contract](#the-020-contract): conventions, secrets, admission, database sources, transforms and their versions, Try, pins, loads, failures and retries, delivery, destination facts, monitoring. It is the contract the conformance corpus (`tests/conformance/`) checks. Parts of it are not built yet; `tests/conformance/known-red.txt` lists which.
- [Authentication](#authentication).

## API

| Method and path | Does | 0.2.0 |
|---|---|---|
| `GET /api/sources` | Every source: definition, state (`waiting`, `receiving`, `late`, `paused`), freshness, record counts, last file, last alert | Adds `pin`, `credential`, `first_load` ([Sources](#reading-a-source)) |
| `POST /api/sources` | Creates a source from its definition (the YAML as JSON); writes `sources/{id}.yaml` and starts it. `409` if it exists, `400` with the reason if invalid | Adds `Idempotency-Key`, `evidence`, transform pins ([Start](#start-creating-a-database-source)) |
| `POST /api/sources/test` | Tests a connection: `{"source": {...}}` or a full definition. Answers `reachable`, `elapsed_ms`, `objects`, `newest`, `newest_at` | Adds `evidence` for database sources ([Test](#test-a-connection)) |
| `GET /api/sources/{id}` | One source, as above | As `GET /api/sources` |
| `PUT /api/sources/{id}` | Replaces the definition and restarts the source | Same rules as Start for database sources |
| `DELETE /api/sources/{id}` | Stops the source and removes its file; its records and alerts stay | Ends its pin period; purges its failed content |
| `POST /api/sources/{id}/pause` | Stops polling and delivery until resumed; body `{"by": "..."}` is kept with the time | Unchanged |
| `POST /api/sources/{id}/resume` | Resumes and passes at once | Unchanged |
| `POST /api/sources/{id}/check-now` | Passes now instead of at the next poll | Adds an optional `pass` ([Check now](#check-now-and-passes)) |
| `GET /api/sources/{id}/loads` (also `/files`) | Every load: the file, its method, records read, sent, rejected, in progress, how long it took, where it is archived | Adds paging, `rows`, `progress`, the transform version ([Loads](#loads)) |
| `GET /api/loads/{id}` | One load with its **failures grouped by reason**: kind, reason, count, first line, and up to three sample rows each | Groups gain a key, version, location, content state, and their history: every group the load ever had stays listed, with `outstanding`, `resolved`, `first_failed_at`, `resolved_at` and `retries` ([Failures](#failures)) |
| `POST /api/loads/{id}/retry` | Puts the load's rejected rows back at the start, all of them or those with one reason (`{"reason": "..."}`); the source passes at once | Kept; now a [retry](#retries) with `version: null` that keeps the history |
| `GET /api/sources/{id}/stats?days=14` | What a source's page draws: today (files, records, sent, rejected, pending) and one row per day | Adds `tz` and historical counts ([Period counts](#period-counts)) |
| `GET /api/transforms` | Every transform file: name, path, size, modified, and the sources that use it | Adds `id`, `display_name`, `current_version` ([Transforms](#transforms-and-versions)) |
| `GET /api/transforms/{name}` | The transform's code | Adds `?version=` and the version's metadata |
| `PUT /api/transforms/{name}` | Saves the code (`{"code": "..."}`); the sources using it restart their Python worker. `201` when new | Now saves a new version; a running load keeps its version ([compatibility](#compatibility)) |
| `POST /api/transforms/{name}/try` | Runs the transform on `{"records": [...]}` (up to 100) without loading: per record, the resources it became or the rejection reason | Adds the [Try answer](#try) beside `results` |
| `GET /api/monitoring/status` | What the monitoring page opens on: `needs_you` (late, paused, or rejecting sources with the reason), `last_24h` (files, sent, rejected, alerts), every source | Adds `last_24h.failed` |
| `GET /api/records?source=&status=&file_id=` | Records; `status` is `received`, `pending`, `sent` or `rejected` | Adds `outcome` per record |
| `GET /api/records/{id}` | One record: file, line, original and corrected row, resources sent, HFS response | `raw` is `null` once its failed content is purged; `no-store` |
| `POST /api/records/{id}/reprocess` | Runs a record again; body `{"raw": {...}}` replaces the row and keeps the original | Kept; appends to the history; `409 record_in_flight` while the record is in an unsettled delivery, an unknown outcome or an active retry |
| `GET /api/alerts?source=&open=true` | Alerts, newest first, each with its rule, recipients, channels, and when and why it cleared; `open=true` lists only those not cleared | Unchanged |
| `GET /api/alert-rules?source=` | Every rule; `POST` creates one (`400` names what is wrong with it) | Unchanged |
| `GET` / `PUT` / `DELETE /api/alert-rules/{id}` | One rule: read, replace, remove | Unchanged |
| `GET /metrics` | Prometheus text: records by status, last received time, paused flag, alerts by kind | Unchanged |
| `GET /health` | `{"status": "ok", "service": "hie", "version", "uptime_seconds", "started_at", "timestamp"}` | Unchanged |

New in 0.2.0:

| Method and path | Does |
|---|---|
| `POST /api/secrets`, `GET /api/secrets`, `GET` / `PUT` / `DELETE /api/secrets/{id}` | Write-only secrets with revisions ([Secrets](#secrets)) |
| `GET /api/admission` | The admission policy in force ([Admission](#admission-policy)) |
| `GET /api/evidence/{id}` | A connection test's evidence |
| `POST /api/evidence/{id}/describe` | Tables, or one table's columns, key and row count, through a tested connection |
| `POST /api/evidence/{id}/sample` | Sample rows of a tested read |
| `PUT /api/sources/{id}/pin` | Pins a source to a transform version, or makes it follow the current one |
| `GET /api/sources/{id}/passes/{pass}` | What one pass found |
| `GET /api/sources/{id}/destination` | What a source has written to the destination |
| `POST /api/transforms` | Creates a transform and its version 1 |
| `PATCH /api/transforms/{id}` | Renames a transform; its id stays |
| `DELETE /api/transforms/{id}` | Deletes a transform no source uses |
| `GET /api/transforms/{id}/versions` | Its immutable versions |
| `POST /api/transforms/{id}/versions` | Saves a new version, with conflict detection |
| `GET /api/transforms/{id}/usage` | Which sources used each version, and when; every load and retry each version ran |
| `POST /api/transforms/try` | Try on code sent inline |
| `GET /api/transforms/try-runs/{run}` | A Try run's outcome, without its results |
| `POST /api/transforms/try-runs/{run}/cancel` | Stops a Try run |
| `GET /api/loads/{id}/failures/{group}` | One failure group with its record ids |
| `GET /api/loads/{id}/failures/{group}/samples` | Its failed rows, while kept |
| `POST /api/loads/{id}/retries`, `GET /api/loads/{id}/retries`, `GET /api/loads/{id}/retries/latest`, `GET /api/loads/{id}/retries/{retry}` | Durable retries of a failure group |
| `GET /api/records/{id}/history` | A record's immutable attempts |
| `POST /api/failed-content/purge` | Purges expired failed content now |
| `GET /api/destination` | The destination's facts |

## The 0.2.0 contract

### Conventions

**Bodies.** JSON in and out, `snake_case` names. Times are RFC 3339 in UTC with milliseconds (`2026-11-03T10:15:00.000Z`). Request members a route does not define are refused with `400 unknown_field`, except on the legacy routes, which ignore them as before.

**Errors.** Every error answers `{"error": "<a sentence>", "code": "<stable code>"}`, plus the members the route names (for example `field` or `current_version`). Clients branch on `code`; `error` is for people and may change. `error` keeps its 0.1 meaning, so a client that reads only `error` still works. Codes used across routes:

| Status | `code` | Means |
|---|---|---|
| `400` | `invalid_body`, `invalid_definition`, `unknown_field`, `invalid_cursor`, `invalid_tz`, `idempotency_key_required`, `idempotency_key_invalid`, `invalid_request` | The request is wrong; `error` says how (a path or query value that does not parse, or `limit` out of range, is `invalid_body` with `field`) |
| `401`, `403` | `unauthorized`, `forbidden` | Authentication ([Authentication](#authentication)); `403` keeps `required` |
| `403` | `admission_refused` | The admission policy refuses the connection; `field` names the first part that does not match |
| `404` | `source_not_found`, `transform_not_found`, `version_not_found`, `load_not_found`, `record_not_found`, `rule_not_found`, `group_not_found`, `retry_not_found`, `secret_not_found`, `evidence_not_found`, `table_not_found`, `run_not_found`, `pass_not_found`, `not_found` | What the code says |
| `405`, `413` | `method_not_allowed`, `body_too_large` | The method is not served on that path (`Allow` says which are); the body is too large |
| `409` | `idempotency_conflict`, `record_in_flight` | The `Idempotency-Key` was used with another body; a record is in an unsettled delivery or an active retry, so it cannot be reprocessed now |
| `410` | `content_expired` | Failed content that was purged or expired |
| `429` | `try_busy` | Too many Try runs at once; `Retry-After` says when to try again |
| `500` | `internal_error` | HIE failed; `error` is a fixed sentence |
| `503` | `secrets_unavailable`, `executor_unavailable`, `destination_unavailable`, `source_unavailable` | A dependency HIE needs is missing or down (`source_unavailable`: describe or sample could not reach the tested connection; `error` is secret-safe) |

Codes a route defines for itself (`name_in_use`, `version_conflict`, `transform_in_use`, `transform_deleted`, `environment_unavailable`, `code_too_large`, `run_id_in_use`, `run_finished`, `try_busy`, `secret_in_use`, `evidence_*`, `read_not_admitted`, `source_exists`, `retry_in_progress`, `nothing_to_retry`, `content_expired`, `record_not_in_load`, `attempt_changed`, `nothing_to_try`) are listed with the route. An unknown `sample` in a Try input is `404 not_found`.

**Secret safety.** No answer, error, log line or stored history holds a secret value. An error caused by a connection that used a secret is reduced to its kind and a fixed sentence (`authentication failed`, `connection refused`, `TLS handshake failed`, `timed out`); a driver's own message is kept only when HIE has checked that it contains no secret value it holds. A request body that HIE cannot parse is never echoed.

**Idempotency.** Routes that create something (`POST /api/secrets`, `PUT /api/secrets/{id}`, `POST /api/sources`, `POST /api/sources/test`, `POST /api/transforms`, `POST /api/transforms/{id}/versions`, `POST /api/loads/{id}/retries`) honour an `Idempotency-Key` header, up to 128 printable ASCII characters. HIE keeps a key with the SHA-256 of the request (method, route, target and canonical JSON body) for at least 24 hours, and across restarts. The same key with the same request replays the first answer without doing the work again; where the first answer was `201` or `202`, the replay is `200` with the same body (`202` stays `202` for retries). The same key with another request is `409 idempotency_conflict`. Only a successful answer is kept: an error answer releases the key, so the same request can be sent again. A request whose key is still being worked on by an earlier one waits for that answer (up to 30 seconds, then `409 idempotency_conflict`). A malformed key is `400 idempotency_key_invalid`. Routes marked **key required** answer `400 idempotency_key_required` without one.

**Ids.** Sources keep the ids their definitions give (`[a-z0-9][a-z0-9-]*`). Everything HIE creates gets an opaque id: secrets `sec_…`, evidence `ev_…`, samples `smp_…`, retries `rt_…`, passes `ps_…`, failure groups `g_…`, transforms an id of `[a-z0-9][a-z0-9_-]{0,63}` (a transform that exists as a file keeps its file stem as id when the stem is such an id; any other file stem or 0.1 name (`[A-Za-z0-9_-]{1,64}`, upper case for example) gets its lower-cased form followed by `-` and 8 hex characters, and keeps its name as given in 0.1 answers and as its file name; `try` and `try-runs` are reserved). Loads and records keep integer ids. Clients treat every id as opaque.

**Paging.** A paged list answers `{"<items>": [...], "page": {"next": <cursor or null>, "previous": <cursor or null>}}`. A cursor is opaque and URL-safe (`[A-Za-z0-9_-]`), valid for 24 hours, and bound to the list it came from; any other is `400 invalid_cursor`. `limit` is 1 to 200, 50 by default.

**No-store.** Answers that carry source rows (samples, failed content, Try results, records) send `Cache-Control: no-store`.

### Secrets

A secret is a write-only value, such as a database password, kept by HIE. Nothing reads it back. Definitions refer to it as `{"secret": "<id>", "revision": <n>}`. Secret routes need custody configured: `secrets.key_file` in `hie.yaml` (a file holding a 32-byte key, base64), or the same in `HIE_SECRETS_KEY`; without it every secret route is `503 secrets_unavailable`, and a definition that refers to a secret is `400 invalid_definition`.

`POST /api/secrets` (key required)

```json
{"name": "labs-reader", "value": "<the password>"}
```

`201`:

```json
{"id": "sec_7f3k2", "name": "labs-reader", "revision": 1, "created_at": "2026-11-03T10:00:00.000Z",
 "revisions": [{"revision": 1, "created_at": "2026-11-03T10:00:00.000Z", "used_by": {"evidence": [], "sources": []}}]}
```

- `name` is 1 to 64 characters and unique (`409 name_in_use`); `value` is 1 to 4096 bytes of UTF-8. Neither member may be missing or empty (`400 invalid_body`).
- No answer, of any route, carries `value`.

`PUT /api/secrets/{id}` (key required), `{"value": "..."}`: a new revision; `200` with the secret as above, `revision` the new number. Earlier revisions stay while evidence or a source refers to them, and are destroyed once nothing does.

`GET /api/secrets` → `{"secrets": [<secret>, ...]}`; `GET /api/secrets/{id}` → one, as above. `used_by` lists the evidence ids and source ids that refer to each revision.

`DELETE /api/secrets/{id}`: `204`, every revision destroyed. `409 secret_in_use` with `sources` while a source refers to it.

**Binding.** A Test records the revision it used. A Start (or replace) with a definition that refers to a secret uses the revision its evidence recorded: a reference that names a revision must name that one, and a reference that leaves the revision out takes it (see [Start](#start-creating-a-database-source)). A source keeps using the revision it started with until it is replaced with a new definition and evidence; a new revision never reaches a running source by itself.

### Admission policy

HIE checks every connection a definition names against an allowlist of **whole connections** in `hie.yaml`, inside HIE, before it decrypts a secret or opens any network connection. It applies to Test, describe, sample, Start, replace, and to every pass of a running source.

```yaml
admission:
  synthetic_only: true          # the installation declares it holds synthetic data only
  databases:
    - {engine: sqlserver, host: db.lab.internal, port: 1433, database: labs}
    - {engine: sqlite, database: /data/fixtures/labs.db}
```

- A connection is admitted only when one entry matches all of its parts. Parts are normalized first: `engine` lower case, with `mssql` read as `sqlserver` and `postgresql` as `postgres`; `host` lower case without a trailing dot; `port` filled in with the engine's default (1433, 5432) when left out; `database` compared exactly as written. A host name is not resolved, so `localhost` and `127.0.0.1` are different hosts.
- A refused connection is `403 admission_refused` with `field` set to the first part, in the order engine, host, port, database, that has no match among the entries that match the parts before it. Nothing is attempted: no secret is decrypted, no address is resolved, no socket is opened.
- A legacy definition (`url_env`) is checked the same way, on the parts of the URL the variable holds.
- A running source whose connection is no longer admitted (the policy changed) is not passed; its state reads `paused` with `paused.by` set to `admission`.
- Without an `admission` block nothing is enforced, as in 0.1; `GET /api/admission` then says so.

`GET /api/admission` → `200`:

```json
{"enforced": true, "synthetic_only": true,
 "databases": [{"engine": "sqlserver", "host": "db.lab.internal", "port": 1433, "database": "labs"}]}
```

### Database sources

A database source names a connection and what to read. The legacy shape (`url_env`, `query`, `cursor_column`) keeps working.

```json
{
  "id": "labs",
  "name": "Lab results",
  "source": {
    "type": "database",
    "connection": {
      "engine": "sqlserver", "host": "db.lab.internal", "port": 1433, "database": "labs",
      "user": "hie_reader", "password": {"secret": "sec_7f3k2", "revision": 1},
      "encrypt": true, "trust_cert": false
    },
    "read": {"mode": "table", "table": {"schema": "dbo", "name": "lab_results"}, "key": ["id"], "new_rows_by": "id"},
    "poll_seconds": 900
  },
  "format": "ndjson",
  "transform": {"id": "lab-results", "version": 2},
  "delivery": {"method": "rest", "initial_method": "bulk_submit", "ids": "namespaced"},
  "expect_every": "24h"
}
```

- `connection.engine` is `sqlserver`, `postgres` or `sqlite`. `sqlite` takes only `database`, a file path, and exists for tests and local work. SQL Server authentication only.
- `read.mode: table` reads one table by keyset on (`new_rows_by`, `key`), so rows that share a `new_rows_by` value are never skipped; `key` defaults to the table's primary key, and a table without one is refused at Start (`409 read_not_admitted`). The `new_rows_by` and `key` columns must be not nullable and of a type a keyset can compare and bind (integer, floating point, decimal, date, time, timestamp, text, UUID or boolean; not binary, JSON or arrays); otherwise the Test's `read` check fails with the reason, and a Start with that evidence is `409 evidence_not_passed`. `read.mode: query` is the operator's own SQL as in 0.1 (`query` with `{cursor}`, `cursor_column`).
- `transform` is a [pin](#pins): `{"id", "version"}` or `{"id", "follow": "current"}`. A string is the 0.1 file path and means follow-current of that file's transform.
- A load is one pass, as in 0.1.

#### Test a connection

`POST /api/sources/test` (key honoured), with `{"source": {...}}` or a whole definition. For a database source the answer keeps the 0.1 probe members and adds `evidence`:

```json
{
  "reachable": true, "elapsed_ms": 41, "objects": 10, "newest": null, "newest_at": null,
  "evidence": {
    "id": "ev_q81", "kind": "database", "verdict": "passed",
    "tested_at": "2026-11-03T10:01:00.000Z",
    "definition_sha256": "<64 hex>",
    "credential": {"secret": "sec_7f3k2", "revision": 1},
    "checks": [
      {"name": "admission", "result": "passed"},
      {"name": "connect", "result": "passed"},
      {"name": "authenticate", "result": "passed"},
      {"name": "read", "result": "passed"}
    ],
    "tables": [{"schema": "dbo", "name": "lab_results"}],
    "read": {"rows": 10}
  }
}
```

- `verdict` is `passed` only when every check is `passed`; `reachable` alone never means a passing Test. A check is `passed`, `failed` (with a secret-safe `message`) or `not_verified` (an earlier check failed). A failed connection still answers `200`, with `verdict: failed`.
- `definition_sha256` fingerprints what was tested, apart from the credential's revision. It is the SHA-256 of the canonical JSON (keys sorted, no whitespace, UTF-8) of `{"connection": ..., "read": ...}` taken from the definition after normalization: the engine, host and port normalized as [admission](#admission-policy) does, every defaulted member filled in (`encrypt: true`, `trust_cert: false`, a table read's `key` as the table's primary key), and the password reduced to `{"secret": "<id>"}` without its revision. `read` is `null` when the definition has none; it is checked and counted only when present, and `read.rows` is its row count at the time of the Test, or `null` when counting is not possible.
- `credential` is the revision used, `{"secret", "revision"}`, or `null` without a secret. A revision left out of the reference is resolved to the secret's current revision when the Test starts, and that revision is what the evidence records and binds.
- A refused connection is `403 admission_refused` and creates no evidence.
- Evidence is kept for 24 hours, and for as long as a source refers to it. `GET /api/evidence/{id}` answers the `evidence` object.

#### Describe and sample

Both work through passing evidence: HIE reconnects with the evidence's definition and credential revision, after checking admission again. Evidence that did not pass is `409 evidence_not_passed`.

`POST /api/evidence/{id}/describe`, `{}` → `{"tables": [{"schema": "dbo", "name": "lab_results"}, ...]}`. With `{"table": {"schema": "dbo", "name": "lab_results"}}`:

```json
{"table": {"schema": "dbo", "name": "lab_results"},
 "columns": [{"name": "id", "type": "int", "nullable": false}, {"name": "value", "type": "nvarchar", "nullable": true}],
 "primary_key": ["id"],
 "new_rows_candidates": ["id", "updated_at"],
 "row_count": 10}
```

`type` is the engine's own type name. `new_rows_candidates` are the columns of an integer, decimal, date or time type that are not nullable. An unknown table is `404 table_not_found`.

`POST /api/evidence/{id}/sample`, `{"read": {...}, "limit": 5}` (`read` defaults to the evidence's own, `limit` 1 to 50, default 5). `200`, `Cache-Control: no-store`:

```json
{"sample": {"id": "smp_3n", "rows": [{"id": 1, "value": "5.4"}], "count": 1, "expires_at": "2026-11-03T10:31:00.000Z"}}
```

Rows are read from the start of the read, in its order, as the transform will receive them. The sample is held in memory for 30 minutes so Try can name it instead of carrying the rows (`{"input": {"sample": "smp_3n"}}`); it is never written to the store or a log.

#### Start: creating a database source

`POST /api/sources` (key honoured) with the definition plus `"evidence": "<id>"`. HIE checks, in this order, that the definition is valid (`400 invalid_definition`, which includes a transform or version that does not exist), that admission allows it, and then its evidence. A database source with a `connection` needs evidence:

| Refusal | When |
|---|---|
| `400 evidence_required` | No `evidence` |
| `404 evidence_not_found` | Unknown or expired |
| `409 evidence_not_passed` | Its verdict is not `passed` |
| `409 evidence_mismatch`, `field` | The definition's fingerprint, computed as the Test computes it, differs from the evidence's (`field: definition`); otherwise, the definition names another secret revision than the evidence's (`field: credential`). A definition that leaves the revision out takes the evidence's |
| `409 read_not_admitted` | A table read without a key |
| `403 admission_refused` | The policy refuses the connection |
| `409 source_exists` | The id exists and the key is new or absent |

A source keeps the credential revision it started with; a later revision of the secret reaches it only through a replace with new evidence.

`201` answers the source as `GET /api/sources/{id}` does, with `first_load: {"id": <load id>}`. The first load exists from that moment, `status: queued`, so a client can follow it at once. `delivery.initial_method: bulk_submit` sends that first load through `$bulk-submit` (`bulk_from: transformed`) and every later one by `delivery.method`.

#### Reading a source

`GET /api/sources/{id}` adds, beside the 0.1 members:

```json
{
  "pin": {"transform": {"id": "lab-results", "display_name": "Lab results"}, "mode": "pinned", "version": 2, "resolved_version": 2},
  "credential": {"secret": "sec_7f3k2", "revision": 1},
  "evidence": "ev_q81",
  "first_load": {"id": 41, "status": "done"}
}
```

`definition` holds secret references, never values. `transform` keeps its 0.1 string for display.

### Transforms and versions

A transform has a stable `id`, a mutable `display_name`, and immutable numbered versions (1, 2, …). A version holds its code, `code_sha256`, `note`, `author`, `display_name` (the transform's name when the version was saved), `base_version`, `created_at` and `environment`. Nothing changes a version once it exists.

`environment` is the identity of the place the version runs: `{"executor": "python-runtime", "id": "cpython-3.14@2-env-<32 hex>", "digest": "<64 hex>"}` from the runtime (its `Python-Runtime-Environment-Id` and `-Digest`), or `{"executor": "local", "id": "local", "digest": null}` for the engine's own worker. It is the executor's default environment when the version is saved, unless the request names one: for python-runtime, HIE reads its own consumer's `GET /v1/capabilities`, takes `default_environment` and the `digest` of that entry in `environments`. A version only runs in its environment: HIE names it on every call to python-runtime, and when the executor refuses it (`403 environment_unavailable`), reports another one, or does not report one (python-runtime's `Python-Runtime-Environment-Id` and `-Digest` headers on a run or Try answer), the run is not applied and nothing it returned is kept or delivered. A load or retry then keeps its rows where they were and reads `waiting: environment_unavailable` until the environment is back; Try answers `409 environment_unavailable`.

`GET /api/transforms` → `{"transforms": [...]}`, each with the 0.1 members (`name` is the id, `path`, `size`, `modified_at`, `used_by`) plus `id`, `display_name`, `current_version`, `created_at`, `updated_at`.

`POST /api/transforms` (key required):

```json
{"display_name": "Lab results", "code": "def transform(record, context): ...", "note": "First version", "author": "Sam Whitaker"}
```

`201`:

```json
{"id": "lab-results", "display_name": "Lab results", "current_version": 1,
 "version": {"version": 1, "code_sha256": "<64 hex>", "note": "First version", "author": "Sam Whitaker",
             "display_name": "Lab results", "base_version": null, "created_at": "...", "environment": {...}}}
```

- The transform and its version 1 are created together or not at all.
- `display_name` is unique, compared without case (`409 name_in_use`). `code` is at most 256 KiB (`413 code_too_large`). `note` and `author` are optional text up to 1000 and 200 characters. An optional `environment` names an environment id the executor offers; one it does not is `409 environment_unavailable`. No environment can be determined (the runtime is down) is `503 executor_unavailable`.

`POST /api/transforms/{id}/versions` (key required), `{"code", "base_version", "note", "author", "environment"?}`:

- `201` with the new version object, numbered `current_version + 1`, when `base_version` is the current version.
- `409 version_conflict` with `current_version` when it is not. Two saves on the same base: one is `201`, the other `409`.
- Saving never changes what a pinned source runs, nor the version a load or retry already started with.

`GET /api/transforms/{id}` → the transform with `code` and `version` of the current version; `?version=<n>` the same for version n (`404 version_not_found`). `GET /api/transforms/{id}/versions` → `{"versions": [<version>, ...]}`, newest first, without `code`.

`PATCH /api/transforms/{id}`, `{"display_name": "..."}` → `200` with the transform; the id and the versions are unchanged.

`DELETE /api/transforms/{id}` → `204`. `409 transform_in_use` with `sources` while a source pins or follows it, or while a load or retry that runs one of its versions has not finished. The usage history keeps naming the deleted versions. A retry with `version: null` of a group whose transform is gone is `409 transform_deleted`.

#### Compatibility

- `PUT /api/transforms/{name}` saves a new version with the current version as base and no note or author, when the code differs from the current version; the same code saves nothing. `201` and version 1 when the name is new. The answer keeps `{"name", "saved": true}` and adds `version`. Sources that follow the current version take it at their next load; a load already running keeps its version, so no worker restarts in the middle of a load.
- A transform file under `transforms/` that HIE finds at start becomes a transform with its stem as id; when its content differs from the current version, it is saved as a new version with `author: "file"`.

### Pins

A source either **pins** a version (`{"id", "version"}`) or **follows the current one** (`{"id", "follow": "current"}`). Each load fixes its version when it starts (its `transform` member) and keeps it until it finishes, whatever is saved meanwhile. A follow-current load takes the current version at that moment.

`PUT /api/sources/{id}/pin`, `{"version": 3}` or `{"follow": "current"}` → `200` with the source; a version that does not exist is `404 version_not_found`. A source's `pin` reads `mode: pinned` with its `version`, or `mode: follow_current` with `version: null`; `resolved_version` is what a load starting now would run. The change applies to loads that start afterwards, and ends the previous pin period in the usage history. Nothing else moves a pin: neither a save nor a retry.

`GET /api/transforms/{id}/usage`:

```json
{"transform": "lab-results",
 "versions": [
   {"version": 2,
    "pins": [{"source": "labs", "mode": "pinned", "since": "...", "until": null}],
    "runs": [{"kind": "load", "id": "41", "source": "labs", "load": 41, "rows": 10, "written": 8, "failed": 2,
              "outcome": "partial", "started_at": "...", "finished_at": "..."},
             {"kind": "retry", "id": "rt_9", "source": "labs", "load": 41, "rows": 2, "written": 2, "failed": 0,
              "outcome": "written", "started_at": "...", "finished_at": "..."}]}]}
```

`pins` lists every period a source pinned or followed that version, ended ones with their `until`; a follow-current period is attributed to each version it resolved to, split where the current version changed. `runs` lists every load and retry that ran the version, newest first, up to `limit` (default 50) per version. `outcome` is `running`, `written` (every row written), `failed` (none written), `partial`, or `outcome_unknown` (some rows not confirmed). It is built from the immutable history, so a version's past use stays after its source moved or was deleted.

### Try

Try runs code on a few records without storing anything or writing to the destination. It calls the destination's `$validate`, which writes nothing.

`POST /api/transforms/try` (code inline) and `POST /api/transforms/{id}/try` (a saved version):

```json
{"run_id": "try-01HF", "code": "...", "version": 3,
 "input": {"records": [{"value": "5.4"}]},
 "deadline_ms": 30000, "validate": true}
```

- `run_id` (required): the caller's id for this run, `[A-Za-z0-9._-]{1,64}`. A run id already used in the last 24 hours is `409 run_id_in_use`, and HIE does not run anything: a client that lost an answer reads the run with `GET /api/transforms/try-runs/{run}` instead of sending it again.
- `code` only on the inline route; `version` only on the saved route, the current version when left out.
- `input` is one of `{"records": [...]}` (1 to 100), `{"sample": "<sample id>", "limit"?}`, or `{"failures": {"load": 41, "group": "g_…", "records": [<record id>, ...]?}}` (the failed rows of a group, all or those named, while their content is kept; else `410 content_expired`), or `{"stored": {...}}`, rows HIE stored for one load, by reference ([below](#stored-rows-as-try-input)). The legacy body `{"records": [...]}` without `run_id` still works on `POST /api/transforms/{id}/try` and gets a generated run id.
- <a id="stored-rows-as-try-input"></a>**Stored rows as input.** `{"stored": {"load": 41, "records": [7, {"id": 9, "attempt": 2}], "passing": 2}}` runs rows of load 41 as they were stored, whatever they became, so a failed row can be tried beside rows that were written. It uses only the content HIE keeps at that moment ([Failed content](#failures)) and extends no retention:
  - `records` names records of that load, failed or written, each as its id or `{"id", "attempt"}` (`attempt` optional); `passing` (0 to 100, default 0) adds up to that many rows of the same load whose outcome is `written`, not already named, in line order. The number of records named plus `passing` is 1 to 100, else `400 invalid_body`; a record named twice is `400 invalid_body`. The rows run in the order named, then the passing ones.
  - `attempt`, when given, is the record's attempt the caller saw (as `GET /api/records/{id}` and `/history` number them); a record that has had another attempt since is refused, so a Try never compares against rows that moved on.
  - Refusals, in this order: the body (`400`); `404 load_not_found`; `404 record_not_found` with `record`; `409 record_not_in_load` with `record` and the `load` it belongs to; `409 attempt_changed` with `record` and its current `attempt`; `410 content_expired` with `records: [{"id", "content"}]` (`expired`, `purged`, or `none`: the row's content was never kept or cannot be opened) when a named row's content is no longer kept. A passing row whose content is gone is passed over, not refused; `409 nothing_to_try` when no row is left.
  - Each item adds `input`: `{"record", "load", "line", "outcome", "attempt", "transform": {"id", "version"}}`, the stored row's reference and what it last became (`outcome` as on records). The run's own version is still the one the route names.
- `deadline_ms`, 1000 to 120000, default 30000. At the deadline HIE stops the run.
- `validate`, default `true`: each produced resource is checked with the destination's `$validate`.
- At most `try.max_concurrent` runs (default 1) at once per engine; more are `429 try_busy`.

`200`, `Cache-Control: no-store`:

```json
{
  "run": {"id": "try-01HF", "status": "finished", "outcome": "completed", "reason": null,
          "termination": "confirmed", "cleanup": "confirmed",
          "transform": {"id": "lab-results", "version": 3}, "code_sha256": "<64 hex>",
          "environment": {"executor": "python-runtime", "id": "...", "digest": "..."},
          "validation": "completed", "load_error": null},
  "items": [
    {"index": 0, "disposition": "reported",
     "result": {"resources": [{"resourceType": "Observation", "status": "final"}]},
     "validation": [{"resource": 0, "valid": true, "issues": []}],
     "diagnostics": {"warnings": [{"message": "unit missing, assumed mg/dL", "path": "unit"}]}}
  ],
  "coverage": {"requested": 1, "reported": 1, "complete": true},
  "results": [{"record": {"value": "5.4"}, "resources": [{"resourceType": "Observation", "status": "final"}]}]
}
```

- `run.status` is `running` until HIE has the run's answer, then `finished`; only a finished run has an `outcome`, `termination` and `cleanup` (they are `null` while running).
- `run.outcome`: `completed` (the executor answered for every item), `stopped` (cancelled, or the deadline passed), `indeterminate` (anything else after the run was dispatched, including an answer that never came), `not_started` (refused before it started, or the code did not load; `load_error` then holds `{"message", "trace"}`, and `location` when the executor reports one). `reason` is a content-free label: `null`, `cancelled`, `deadline`, `response_lost`, `busy`, `load_failed`, `worker_exited` (the engine's own worker died), or the executor's own outcome label (`timed_out`, `limited:memory`, …).
- `termination` and `cleanup` are `confirmed` or `unconfirmed`, as the executor reports them. HIE never reports `confirmed` for something the executor did not confirm; a lost answer is `unconfirmed` for both. From python-runtime's facade, only two answers confirm both: a `200` with its replies, and a `422 transform_load_failed` that carries the facade's `Python-Runtime-Error` header. Any other status, and any non-`200` without that header (a proxy's answer cannot be attributed to the facade), is `indeterminate` with both `unconfirmed`, and is not sent again. When the facade's answer carries python-runtime 0.1.3's `execution` evidence, HIE reads it instead: `termination` is `confirmed` for `stopped_confirmed`, `cleanup` is `confirmed` for `cleaned`, anything else `unconfirmed`; an item the facade reports `unknown` is `not_reported`. A redirect is not an answer from the facade: the run is `indeterminate`, `unconfirmed`, and is not followed. HIE stops a runtime Try at its deadline or on cancel by closing the request (the facade takes no deadline), so a stopped runtime run reads `unconfirmed`. A run of the engine's own worker is `confirmed` once HIE has reaped the worker process.
- `items[i].disposition`: `reported` (with `result`), `not_reported` (dispatched, no answer; also an item the executor reported but whose reply it did not return, as python-runtime does for a failed run), `not_started`. `result` is `{"resources": [...]}` or `{"error": {"kind": "data" | "transform" | "validation", "message", "trace"?}}`.
- `items[i].validation` lists one entry per resource with the `OperationOutcome`'s issues (`severity`, `code`, `diagnostics`, `expression`); `valid` is false when an issue is `error` or `fatal`. It is `null` when validation was not run. `run.validation` is `completed`, `unavailable` (the destination could not validate; results are kept, validation `null`) or `skipped`.
- `coverage.complete` is true only when every requested item is `reported`.
- **No replay.** After an uncertain dispatch (a lost answer, a failed partition) HIE answers `indeterminate` and never sends the run again by itself.
- **Diagnostics pass through.** `items[i].diagnostics` is what the executor returned for that item, unchanged, or absent:

  ```json
  {"warnings": [{"message": "string", "path": "string?"}],
   "events": [{"kind": "string", "data": {}}],
   "location": {"line": 12, "column": 5},
   "stdout": "string", "stderr": "string", "truncated": false}
  ```

  `location` is relative to the transform's own code. `stdout` and `stderr` appear only in Try answers: HIE never stores or logs them, and drops them from load and retry diagnostics.
- `results` is the 0.1 answer, kept for compatibility.

`GET /api/transforms/try-runs/{run}` → `200 {"run": {...}, "coverage": {...}}` as above, without items or results, kept 24 hours. While the run executes it reads `{"run": {"id", "status": "running", "outcome": null, "termination": null, "cleanup": null, "started_at"}, "coverage": {"requested": n, "reported": 0, "complete": false}}`; once HIE has the answer, `status: finished` and the members of the POST's answer. A client whose POST answer was lost polls this route until `finished`. `POST /api/transforms/try-runs/{run}/cancel` → `202 {"run": "<id>", "cancelling": true}`; `409 run_finished` once it has an answer; `404 run_not_found`. The run's own answer then reads `stopped`, with `termination` as the executor confirms it.

### Loads

`GET /api/sources/{id}/loads?limit=&cursor=` is paged, newest first. `around=<load id>` answers the page that holds that load. The answer keeps `files` as an alias of `loads` for 0.1 clients. Each load keeps its 0.1 members and adds:

```json
{
  "id": 41, "source_id": "labs", "first": true, "method": "rest",
  "transform": {"id": "lab-results", "version": 2},
  "status": "done",
  "rows": {"read": 10, "written": 8, "failed": 2, "failed_ever": 2, "outcome_unknown": 0, "in_progress": 0, "filtered": 0},
  "resources": {"written": 8},
  "progress": {"phase": "done", "rows_admitted": 10, "rows_processed": 10, "total_rows": 10},
  "waiting": null,
  "environment": {"executor": "local", "id": "local", "digest": null}
}
```

- `status` adds `queued` (created at Start, not begun) to the 0.1 values.
- `rows` counts the load's rows by their current outcome, except `rows.failed_ever`: the distinct rows of the load that ever failed (filtered rows aside), from the history, so a retry or reprocess that writes them later lowers `rows.failed` (and the 0.1 `rejected`) but never `rows.failed_ever`. The load list and `GET /api/loads/{id}` carry the same `rows`. `resources` counts resources, apart from rows.
- `progress` is derived from the store: `rows_admitted` rows stored for the load, `rows_processed` of them with an outcome (written, failed, filtered or outcome unknown), `phase` one of `queued`, `reading`, `transforming`, `delivering`, `settling`, `done`. `total_rows` is the evidence's `read.rows` for a source's first load, `null` for other loads and once more rows were admitted than were counted.
- **Outcome unknown** is explicit: a row whose write was sent and not confirmed counts in `rows.outcome_unknown`, and its record reads `outcome: outcome_unknown`. That is any write without a confirming answer: a transport error or timeout after the request was sent, a `429` or `5xx` for the Bundle or for the row's entry, and a bulk submission whose receipts have not settled. It is never counted as written or failed until a later attempt settles it.
- `waiting`: `null`, or why the load's rows are held without failing: `validation_unavailable` (the destination cannot validate), `environment_unavailable` (the executor runs another environment than the load's version), `destination_unavailable` (writes are not confirmed and are retried). Retries carry the same member.
- `GET /api/records` and `GET /api/records/{id}` add `outcome`: `in_progress`, `written`, `failed`, `filtered` or `outcome_unknown`; and `attempt`, the number of the record's latest attempt as its history numbers them; `status` keeps its 0.1 values.

### Failures

`GET /api/loads/{id}` keeps its 0.1 members; each of its `failures` adds:

```json
{"group": "g_5d1", "kind": "data", "reason": "value 'alto' is not a number", "count": 2, "first_line": 3,
 "transform": {"id": "lab-results", "version": 2}, "location": {"line": 14, "column": 9},
 "content": "kept", "complete": true,
 "outstanding": 2, "resolved": 0, "first_failed_at": "2026-11-03T10:31:00.000Z", "resolved_at": null, "retries": [],
 "samples": [...]}
```

- A group is the load's rows that failed with the same kind, reason and transform version. `group` is stable for the load's life. Rows a retry leaves failing with the same kind, reason and version stay in their group; a retry that runs another version and fails them again puts them in that version's group, and they stay rows of the first group's history too.
- `failures` lists **every group the load ever had**, those with rows failing now first (most first), then the others by when they were created. A group whose rows all recovered stays listed with `outstanding: 0`; nothing is dropped, so the original failure and how it was resolved can be read after a retry.
- Current state: `count` and `outstanding` (the same number) are the group's rows failing in it now. A row a retry is running is not failing, so both read `0` while it runs.
- History, read from the immutable events (never counters): `resolved` is how many of the rows that ever failed in the group were written later, by a retry or a reprocess; `first_failed_at` is when its first row failed; `resolved_at` is when its last row left it (written, or failed into another group), `null` while any row still fails in it or is in flight; `retries` are the ids of the retries that targeted the group, oldest first (a reprocess of one record is not a retry and is not listed).
- `kind` adds `validation` (the destination's `$validate` found errors), `reference` (a conditional reference matched no resource, or several) and `environment` to the 0.1 kinds.
- `location` is the diagnostics' `location` of the first row, or `null`.
- `content`: `kept`, `expired` (removed when its retention ran out), `purged` (removed earlier: its row was written by a retry or reprocess, or its source deleted), or `none` (the row was never kept). `complete` is false when some of the group's rows are no longer known (for example a source replaced in the middle of a load).
- `content` of a group with rows failing now is that of those rows; of a group with none, that of the rows that ever failed in it: `kept` while any of them keeps its content. A row written by a retry or reprocess loses its content, so a group whose rows were all written reads `purged`.
- `samples` is kept for 0.1 clients while the content is kept, and `[]` after; the answer is `no-store`. Samples are the rows that ever failed in the group (from the history) whose content is still kept, those failing in it now first, so a group with nothing failing keeps showing the rows that failed in it while their content is kept (for example rows a retry moved to its version's group).

`GET /api/loads/{id}/failures/{group}` → the group, with the same members as in `failures` (history included), whether or not rows fail in it now, and `records: [{"id", "line", "attempts"}]` and no content. `records` are the rows failing in the group now, those a retry of it or a Try of its failures acts on, so a resolved group answers `records: []`; each row's own past is at `GET /api/records/{id}/history`. `GET /api/loads/{id}/failures/{group}/samples?limit=3` (1 to 50) → `{"group", "samples": [{"record", "line", "raw"}], "content": "kept"}` with the same rows as the group's `samples`, `no-store`; `410 content_expired` once the group's `content` is expired or purged, a resolved group included.

**History.** Every attempt on a row is an immutable event: admitted, transformed, failed (kind, reason, version, location), validated, delivered, outcome unknown, settled, retried. A retry or a reprocess adds events; it never removes or rewrites one. `GET /api/records/{id}/history` → `{"record": 7, "events": [{"at", "kind", "attempt", "load", "retry", "transform": {"id", "version"}, "error": {"kind", "reason"}?}]}`, oldest first; `retry` is `null` for the load's own attempts. Events never hold row content or diagnostics output.

**Failed content.** A failed row's content (its raw input, the resources it became) is stored encrypted with authenticated encryption under a key HIE holds (`content.key_file` or `HIE_CONTENT_KEY`; a random key in the store's directory when neither is set), never in plaintext in the store, the history, logs or errors. It is kept for `retention.failed_content` (default `30d`) and purged earlier when the row is written by a retry or reprocess, or its source is deleted. A purge removes only content: counts, groups, events and period counts stay as they were. HIE purges every hour; `POST /api/failed-content/purge` runs it now and answers `{"purged": <rows>}`. Its optional body `{"before": "<time>"}` also removes the content of rows that failed before that instant, as if their retention had run out (`content: expired`); an operator uses it to shorten retention at once.

### Retries

`POST /api/loads/{id}/retries` (key required):

```json
{"group": "g_5d1", "version": null}
```

`202`:

```json
{"retry": {"id": "rt_9", "load": 41, "group": "g_5d1", "transform": {"id": "lab-results", "version": 3},
           "status": "queued", "rows": 2, "written": 0, "failed": 0, "outcome_unknown": 0,
           "previous": null, "accepted_at": "...", "finished_at": null}}
```

- `group` (required) names one failure group of the load. A retry covers exactly that group's failing rows when it is accepted, and runs one transform version.
- `version: null` is the **latest saved version** of the group's transform at the moment HIE accepts the retry. A number names that version (`404 version_not_found`). The version is recorded on the retry when it is accepted and never changes after; the source's pin never moves because of a retry.
- The retry re-runs the group's failed rows through that version, validates and delivers them like a load, and survives a restart.
- Refusals are checked in this order: the body (`400`), the load and group (`404`), the version (`404 version_not_found`), the content (`410 content_expired`), then `409`.
- `409 retry_in_progress` with `retry` while an unfinished retry covers the same rows. `409 nothing_to_retry` when the group has no failing rows. `410 content_expired` when their content is gone. A replayed key answers `202` with the same retry.
- `previous` is the latest earlier retry of the same group, or `null`, so retries of a group form a chain. Two retries never cover the same row at once, so the chain is linear.

`GET /api/loads/{id}/retries` → `{"retries": [...]}`, newest first; the retries of one group are also in its `retries` ([Failures](#failures)), oldest first. `GET /api/loads/{id}/retries/latest` and `GET /api/loads/{id}/retries/{retry}` → `{"retry": {...}}`; `status` is `queued`, `running`, `done` or `failed`, `written` and `failed` count its rows.

`POST /api/loads/{id}/retry` (0.1) stays: it creates one retry with `version: null` for each failure group that has rows with `reason` (all groups without one), skipping groups that already have an unfinished retry, and answers its 0.1 body `{"load", "retried", "reason"}` plus `retries`, the ids it created. It no longer erases the earlier failure.

### Period counts

`GET /api/sources/{id}/stats?days=14&tz=Europe/Madrid`: days end at midnight in `tz` (an IANA zone, UTC when left out; `400 invalid_tz`). Each day and `today` keep the 0.1 members and add:

```json
{"day": "2026-11-03", "files": 1, "sent": 8, "rejected": 2,
 "rows_read": 10, "written": 8, "failed": 2, "outcome_unknown": 0, "resources_written": 8}
```

`failed` counts the rows that failed in a period, by when they failed: a row counts once in each period in which it failed at least once, so a row that failed on Monday and was written by Tuesday's retry counts as failed on Monday and written on Tuesday, a retry that fails again the same day does not count it twice, and a later retry or purge never lowers Monday. `rejected` keeps its 0.1 meaning (rows rejected now). `today` adds `complete: false` while a load of the day is still running. `GET /api/monitoring/status` adds `last_24h.failed` with the same meaning.

### Delivery

**Validation before delivery.** With `validation: required` in `hie.yaml`, every resource a load or retry produces is checked with the destination's `POST /{type}/$validate` before it is delivered, by REST or `$bulk-submit`:

- An `OperationOutcome` with an `error` or `fatal` issue fails the row with kind `validation` and the first such issue as reason (`diagnostics` and `expression`); the row is kept as failed and none of its resources is delivered. Warnings do not fail it.
- When the destination cannot validate (unreachable, a timeout, a `5xx`, or an answer that is not an `OperationOutcome`), nothing of that batch is delivered: the rows wait with their outcome unchanged, the load or retry reads `waiting: validation_unavailable`, and they are tried again with the delivery backoff. An outage is never a row failure.
- `validation: off` (the default when the setting is absent, as in 0.1) skips it.
- `$bulk-submit` of the source objects themselves (`bulk_from: source_objects`, or `auto` resolving to it) cannot be validated or namespaced by HIE: a definition that combines it with `validation: required` or `delivery.ids: namespaced` is `400 invalid_definition`. A retry of such a source's rows runs its version through the transform and delivers by REST.

**Source namespacing.** With `delivery.ids: namespaced` on a source, every resource that carries an `id` is written under a derived id: the first 48 hex characters of SHA-256 over `{source id}|{resourceType}|{origin id}`. Every relative reference `Type/id` in the source's resources is rewritten to the derived id of the same source; absolute, conditional (`Type?…`), contained (`#…`) and `urn:` references are left as written. Each resource also gets `meta.tag` `{"system": "urn:helios:hie:source", "code": "<source id>"}`. The same origin record always maps to the same resource, two sources never share one, and replaying records creates nothing. `delivery.ids: as_sent`, the default, delivers ids as the transform returns them, as in 0.1. Resources with only an `identifier` keep their conditional update.

**Bundle type.** `delivery.bundle` is `batch`, `transaction` or `auto` (default). With `auto` a chunk goes as a `transaction` when one of its resources holds a conditional reference (`Type?query`), so the destination resolves it, and as a `batch` otherwise. How a transaction's answer is read:

- A definitive refusal (`400`, `404`, `409`, `412` or `422` with an `OperationOutcome`) means the destination committed nothing. HIE then sends each row of it in a transaction of its own. A row whose own transaction is refused with an issue about a conditional reference (zero or several matches) fails with kind `reference`; any other definitive refusal fails it with kind `server`.
- A `429`, a `5xx`, a timeout, a lost answer, or a `2xx` that is not a complete `transaction-response` (one entry per resource, every entry `2xx`) is not a refusal nor a confirmation: the transaction may have committed. Its rows are `outcome_unknown`, the transaction is not split, and the same transaction is sent again with the delivery backoff until an answer settles it.

### Destination facts

`GET /api/destination` → `200`:

```json
{"base_url": "http://hfs:8080", "display_name": "Northgate FHIR", "fhir_version": "4.0.1",
 "software": {"name": "Helios FHIR Server", "version": "0.4.0"}, "reachable": true,
 "checked_at": "...", "resource_count": null, "validation": "required"}
```

From the destination's `/metadata`, read at most once a minute. `display_name` is `destination.display_name` in `hie.yaml`, or `null`. `resource_count` is `null` when counting would be costly or is not supported.

`GET /api/sources/{id}/destination` → what this source has written, from HIE's history: `{"source": "labs", "resources_written": 8, "by_type": {"Observation": 8}, "last_written_at": "..."}`.

### Check now and passes

`POST /api/sources/{id}/check-now` keeps `202 {"id", "checking": true}` and adds `pass`, the id of the pass it asked for. `GET /api/sources/{id}/passes/{pass}` → `{"id": "ps_2", "status": "queued" | "running" | "done" | "skipped" | "failed", "skipped_reason": "paused" | null, "error": null, "loads": [42], "new_rows": 3, "finished_at": "..."}`. A paused source answers `skipped` with `paused`. A pass that could not read its source (the connection or the credential refused) is `failed` with a secret-safe `error`.

### Monitoring

`GET /api/monitoring/status`, `GET /api/alerts`, the alert rules and check-now are unchanged apart from `last_24h.failed` and `pass` above.

## Authentication

With `HIE_AUTH_ENABLED=true`, every request under `/api` needs a bearer token signed by the identity provider: a JWT validated against `HIE_AUTH_JWKS_URL` with the expected `HIE_AUTH_ISSUER` and, when set, `HIE_AUTH_AUDIENCE`. This is the same validation HFS performs, through its `helios-auth` crate. `/health` and `/metrics` stay open. A missing or invalid token answers `401` with `WWW-Authenticate: Bearer`. A pause without a `by` in the body records the token's subject.

| Variable | Default | Purpose |
|---|---|---|
| `HIE_AUTH_ENABLED` | `false` | Require bearer tokens under `/api` |
| `HIE_AUTH_JWKS_URL` | | The identity provider's JWKS |
| `HIE_AUTH_ISSUER` | | Expected `iss`; required when enabled |
| `HIE_AUTH_AUDIENCE` | | Expected `aud`, when the provider sets one |
| `HIE_AUTH_ALGORITHMS` | `RS256,RS384,ES256,ES384` | Accepted signing algorithms |
| `HIE_AUTH_JWKS_MIN_REFRESH_INTERVAL` | `10` | Seconds between JWKS refreshes |

What a token may do is decided by its scopes (the `scope`, `scp` or `roles` claim, as HFS reads them):

| Scope | Grants |
|---|---|
| `hie/read` | Every `GET`: sources, loads, records, transforms, alerts, rules, monitoring |
| `hie/manage` | Changing sources, rules, loads and records (`POST`, `PUT`, `DELETE`, pause, resume, retry, reprocess, try), secrets, and reading |
| `hie/transforms` | Writing transforms (`PUT /api/transforms/{name}`, `POST /api/transforms`, `POST /api/transforms/{id}/versions`, `PATCH` and `DELETE /api/transforms/{id}`): code the engine then runs; and reading |
| `hie/*` | Everything |

A valid token without the scope a request needs answers `403` with the scope in `required`. A token from the same identity provider that carries only HFS scopes (`system/*.cruds`) is authenticated but may do nothing here.

`docker/keycloak/` holds a Keycloak for local work: HFS's `fhir` realm (its `hfs-backend-client`, which the engine uses towards HFS) plus the `hie/*` client scopes and three service-account clients, `hie-admin-client` (`hie/*`), `hie-operator-client` (`hie/manage`) and `hie-monitor-client` (`hie/read`), all with audience `hie-api`. `docker compose -f docker/keycloak/docker-compose.yml up -d` serves it on 8180; `docker/keycloak/get-token.sh [client]` prints a token. The engine then runs with `HIE_AUTH_JWKS_URL=http://localhost:8180/realms/fhir/protocol/openid-connect/certs`, `HIE_AUTH_ISSUER=http://localhost:8180/realms/fhir`, `HIE_AUTH_AUDIENCE=hie-api`, and `HIE_FHIR_TOKEN_URL=http://localhost:8180/realms/fhir/protocol/openid-connect/token` with `hfs-backend-client` / `hfs-backend-secret` towards an HFS configured for the same realm.

With auth off the engine logs a warning at start: `PUT /api/transforms/{name}` writes code the engine then runs, so an open API must only be reachable from trusted clients. With auth off the admission policy is the only server-side guard on what HIE connects to.

### The engine's identity towards HFS

The engine calls HFS as itself, with an OAuth2 **client-credentials** grant at the identity provider's token endpoint (Keycloak first; the same providers HFS accepts). The token is cached and renewed a minute before it expires; a `401` or `403` from HFS discards it, the rows wait, and the next pass goes out with a fresh one. A token endpoint that cannot be reached or refuses the client is a transient delivery failure, so the data is kept and the outage alerts say why. A wrong endpoint or secret fails at startup.

| Variable | Default | Purpose |
|---|---|---|
| `HIE_FHIR_TOKEN_URL` | | The provider's token endpoint, e.g. `https://idp/realms/hfs/protocol/openid-connect/token` |
| `HIE_FHIR_CLIENT_ID` | | The engine's client |
| `HIE_FHIR_CLIENT_SECRET` | | Its secret |
| `HIE_FHIR_SCOPE` | | Scopes to ask for, when the provider needs them, e.g. `system/*.cruds` |

With none of them set the engine calls HFS anonymously and logs a warning at start, which is what the public sandbox does against the public HFS. Setting some but not all three is refused.
