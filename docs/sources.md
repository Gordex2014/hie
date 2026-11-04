# Sources

A source is one connection: where the data comes from, its format, the transform that turns it into FHIR, how it is delivered, and how often data is expected. Back to the [README](../README.md).

## Sources

A source is one connection: where the data comes from, its format, the transform that turns it into FHIR, how it is delivered, and how often data is expected. Each source is one YAML file in `sources/`, which the engine reads at start and **writes when a source is created or replaced through the API**, so definitions stay under version control either way.

```yaml
id: lab-results
name: Lab results
source:
  type: folder               # or s3 (see sources/synthea-s3.yaml.example)
  path: inbox/lab-results
  pattern: "*.csv"
  poll_seconds: 5
format: csv                  # csv (default), ndjson, or fhir_bundle
transform: transforms/lab_results.py
delivery:
  batch_size: 100            # entries per batch Bundle
  initial_backoff_ms: 2000   # doubles per failed attempt, up to 5 minutes
  alert_after_attempts: 5
expect_every: 24h            # the silence rule the source is born with
```

An S3 source names a `bucket`, an optional `prefix`, and an `archive_prefix` (default `processed`) that acknowledged objects are moved under. `endpoint` points at an S3-compatible store such as MinIO. `anonymous: true` reads a public bucket without credentials (requests are not signed, so no pre-signed URLs either), and `read_only: true` is for someone else's bucket: nothing is moved or written there, and an object the engine has already taken is recognised by its name and size, then by its content, and left alone on later passes.

With a **notification queue** (`queue:` the URL of an SQS queue the bucket's `s3:ObjectCreated:*` events go to, directly or through SNS) a pass reads the queue instead of listing the bucket: only the announced objects are fetched, and their messages are deleted once the objects are in the store, so a pass that fails leaves them to be delivered again. The bucket is still listed in full at start and every `relist_seconds` (default 3600), so an object whose event was lost is taken within the hour rather than never. The queue is read with the source's own AWS credentials (SigV4), so `anonymous` and `queue` do not go together; the region comes from the queue URL. The queue's visibility timeout should exceed a pass over the largest drop.

A **zip** that arrives on any file source is a drop of several files: every entry whose extension fits the source's format (`.csv`, `.ndjson`/`.jsonl`, `.json` for Bundles, `.hl7`) becomes a load of its own, named `drop.zip!/path/inside`; the other entries (a manifest, a README, a CSV in an NDJSON drop) are skipped. The zip is recorded as `unpacked`, archived like any file, and not read twice. Entries are unpacked in memory, so a zip should hold a few hundred megabytes at most. On the `source_objects` bulk path HFS cannot read a zip, so each unpacked entry is **staged** where HFS can fetch it: in the source's own store under `{archive_prefix}/unpacked/` when the store can be written and can sign URLs, otherwise under the engine's `bulk_dir`, served at `/bulk/{token}/files/{n}`; staged entries go once the receipts are in.

A **FHIR server** source (`type: fhir_server`, see `sources/other-fhir.yaml.example`) pulls another server into HFS: it names the server's `url`, the resource `types` to pull, and optionally `since` (a FHIR instant to start from) and `auth` (`method: client_credentials` with `token_url`, `client_id`, `client_secret_env`, or `method: bearer` with `token_env`; secrets are read from the named environment variable, never written in the file). Each pass searches every type for `_lastUpdated=gt{cursor}` sorted by `_lastUpdated`, `page_size` per page and up to `max_pages` pages, and hands the resources on as one NDJSON load per type; the cursor moves to the newest `meta.lastUpdated` once the records are stored, so a crash repeats a page rather than losing it. The format is `ndjson` whatever the file says and resources go out by their id. `bulk_submit` works with `bulk_from: transformed` (the engine writes the NDJSON); only `bulk_from: source_objects` is refused, since there is no file for HFS to fetch. The connection test reads `/metadata` and counts what waits past each cursor.

A **database** source (`type: database`, see `sources/lab-database.yaml.example` and `transforms/sql_lab_results.py`) reads a SQL database with the operator's own query: `url_env` names the environment variable holding the connection URL (`postgres://`, `mysql://`, `sqlserver://` or `sqlite:`; the URL carries the password, so it is never in the file), `query` is the SQL with `{cursor}` where the cursor value goes, an `ORDER BY` on the cursor column and a `LIMIT`, and `cursor_column` is the column the cursor follows. Prefer a **unique, increasing column** such as an id: with an updated-at column, rows that share one timestamp (a multi-row `INSERT` gets a single `now()`) and fall on both sides of a `LIMIT` are never read, because the next run asks for values strictly past the cursor; the engine logs a warning when a whole run shares one cursor value. Each pass binds the cursor as a parameter, runs the query up to `max_runs` times moving the cursor between runs, and hands the rows on as one NDJSON load; each row reaches the transform as a JSON object with the database's types (numbers, ISO timestamps, JSON). The cursor keeps its kind (integer, float, timestamp, date, text) and moves to the largest value seen once the records are stored. `cursor_start` says where to begin the first time. PostgreSQL is the target; MySQL/MariaDB and SQLite go through the same code (the tests run on SQLite), and **SQL Server** through its own driver with the same definition: `sqlserver://user:password@host:1433/database?encrypt=on&trust_cert=true` (`mssql://` too; `encrypt` is on by default, `trust_cert=true` accepts a self-signed certificate; the placeholder is bound as `@P1`, and a `TOP n` plays the part of `LIMIT`). SQL Server authentication only; Windows authentication is not supported. The connection test connects and runs the query once past the cursor.

A database source can also name its **connection by its parts** and **what to read** (0.2.0), with the password kept by HIE as a [secret](api.md#secrets) rather than in an environment variable:

```yaml
source:
  type: database
  connection:
    engine: sqlserver          # sqlserver, postgres or sqlite (mssql and postgresql are read as aliases)
    host: db.lab.internal
    port: 1433                 # the engine's default (1433, 5432) when left out
    database: labs
    user: hie_reader
    password: {secret: sec_7f3k2, revision: 1}   # a reference; the value never appears in the file
    encrypt: true              # default true
    trust_cert: false          # default false; true accepts a self-signed certificate
  read:
    mode: table
    table: {schema: dbo, name: lab_results}      # the schema defaults to dbo, public or main
    key: [id]                  # defaults to the table's primary key
    new_rows_by: id            # not nullable; describe suggests integer, decimal, date or time columns
  poll_seconds: 900
```

- `read.mode: table` reads the whole table by **keyset** on (`new_rows_by`, `key`): each run takes up to 1000 rows strictly past the last row stored on that whole tuple, ordered the same way, up to `max_runs` runs per pass, so rows that share a `new_rows_by` value are never skipped at a run's boundary, and rows added later with the same value are read next. The position moves once the rows are stored. A table without a primary key needs `key`; a Start without either is refused (`409 read_not_admitted`). Each row reaches the transform as a JSON object of all its columns.
- `read.mode: query` is the operator's own SQL as above (`query` with `{cursor}`, `cursor_column`, `cursor_start`).
- `sqlite` takes only `database`, a file path, and opens it read-only; it exists for tests and local work. SQL Server authentication only.
- A source with a `connection` is created through the API after a **Test** (`POST /api/sources/test` answers `evidence`): the Start names that evidence, must define exactly what was tested, and uses the secret revision the Test used ([Start](api.md#start-creating-a-database-source)). Describe (tables, a table's columns, key and row count) and sample (the first rows of a read, held in memory for 30 minutes) work through passing evidence.
- Every connection is checked against the **admission policy** in `hie.yaml` before a secret is opened or a socket is opened: on Test, describe, sample, Start, replace and every pass ([Admission](api.md#admission-policy)). The legacy `url_env` shape is checked on the parts of its URL. A running source the policy no longer admits reads `paused` by `admission` and is not passed.
- Each pass opens its own connection and closes it after; the password is opened only while the connection is being opened. Connection errors are reduced to a fixed sentence (`authentication failed`, `connection refused`, `TLS handshake failed`, `timed out`, ...); a driver's own message is kept only when it holds no secret value HIE keeps.

### HL7 v2

`format: hl7v2` reads a file of HL7 v2 messages (one or more, each starting at its `MSH`; segments separated by `\r`, `\n` or `\r\n`) into one record per message. The record is a JSON object the transform reads without parsing pipes:

```json
{"message_type": "ADT^A01", "control_id": "MSG0001", "sending_application": "LAB", "sending_facility": "NORTH",
 "receiving_application": "HIE", "receiving_facility": "HFS", "version": "2.5.1",
 "segments": [{"name": "MSH", "fields": ["|", "^~\\&", "LAB", ...]}, {"name": "PID", "fields": ["1", "", "12345^^^NORTH^MR", "", "Doe^Jane^M", ...]}],
 "text": "MSH|^~\\&|LAB|..."}
```

Fields keep their separators; the helpers in `hie_runtime/hie.py` split them: `hl7(record, "PID-5.1")` (segment, field, component; `PID-3[2].1` for a repetition, `OBX(3)-5` for the third OBX), `hl7_segments(record, "OBX")`, `hl7_field(segment, "5.1")`, and `hl7_datetime("20261021120000")` for an HL7 timestamp as a FHIR `dateTime`. `transforms/hl7_adt_patient.py` turns an ADT A01/A04/A08/A28/A31 into a `Patient` delivered as a conditional update on the MRN in `PID-3`.

An **MLLP** source (`type: mllp`, see `sources/adt-mllp.yaml.example`) listens on `listen` (e.g. `0.0.0.0:2575`) for MLLP-framed messages. Every message is parsed, written to `spool` as its own file (written, fsynced, renamed), and **only then acknowledged** `AA`, so an acknowledgement means HIE has it. A message that cannot be stored is answered `AE` (the sender keeps and resends it); bytes that are not HL7 are answered `AR` and dropped. The spool is picked up every `poll_seconds` like a folder: one load per message, the same SHA-256 dedupe (a resent message is a duplicate, not a second update), the transform, delivery. The listener starts with the source and is retried, with an alert, when the port cannot be bound. There is no TLS on the listener; put it behind a TLS proxy or a VPN as MLLP usually is. The connection test checks the port can be taken (when the source is not already listening) and counts what waits in the spool.

### Delivery methods

`delivery.method` picks how a source's records reach HFS:

- **`rest`** (default): the transform runs on every record and the resources go out in `batch` Bundles, one `PUT` per record, with a status per entry. For anything that needs a transform, and for feeds of hundreds to tens of thousands of records at a time.
- **`bulk_submit`**: HFS ingests through a `$bulk-submit` manifest the engine serves at `{public_url}/bulk/{token}/manifest.json`, polls the status URL, reads HFS's receipts (`{"reference": "Type/id"}` per stored resource, an `OperationOutcome` per failure) and maps them to its records by `Type/id`. What the manifest points at is `delivery.bulk_from`:
  - `source_objects`: for data that already is FHIR with ids, such as an NDJSON export. No transform runs. The engine stores the rows for the history, predicts what HFS will refuse (no `resourceType`, no `id`), and hands the **source objects themselves** over: pre-signed URLs for S3 objects, the engine's own `/bulk/{token}/files/{n}` for folder files. One submission per drop (up to `manifest_objects_max` objects), with a `submissionId` derived from the files so a repeated drop is refused by HFS instead of ingested twice. Objects stay at the source until the receipts are in, then they are archived.
  - `transformed`: for anything else, a lab CSV included. The transform runs as on the REST path; a resource that carries only an `identifier` gets an **`id` derived from it** (48 hex characters of SHA-256 over `system|value`), so `$bulk-submit`, which stores by id, updates the same resource every time the row is sent. The engine writes the resources as NDJSON per resource type, puts them in the source's own store when it can sign URLs (an S3 source: HFS fetches from the bucket, under `{archive_prefix}/submissions/{token}/`) or serves them itself from `bulk_dir`, and opens one submission per `bulk_records_max` records. The source file is archived as soon as it is read; the engine's files go once the receipts are in.
  - `auto` (the default): `source_objects` for NDJSON or Bundle files with the passthrough transform, `transformed` otherwise.

  `url_ttl_seconds` bounds how long the URLs stay valid.

`public_url` in `hie.yaml` must be the URL HFS reaches the engine at.

The same document is what `POST /api/sources` takes. Before creating one, `POST /api/sources/test` with `{"source": {...}}` lists the connection once and answers with the evidence: reachable or not, in how many milliseconds, how many objects match, and the newest one.

### Alerts and rules

Alerts are governed by **rules**: data, created and edited over the API, each naming a source (or every source), a condition with its threshold, who to notify, and the channels (`app`, `webhook`, `email`). Every source is born with one `silence` rule set to its `expect_every` (two hours when it has none), so nobody has to remember to watch it.

| Condition | Threshold | Fires when | Clears when |
|-----------|-----------|------------|-------------|
| `silence` | a duration, `2h` | nothing has arrived for longer than that | data arrives |
| `missing_by` | a time of day in UTC, `06:00` | that time passes and nothing arrived today | the file arrives |
| `rejected_share` | a percentage, `10%` | a load has more than that share of rows rejected | a later load is within the limit |
| `unreachable` | a number of attempts, `5` | delivery to HFS has failed that many times in a row | delivery succeeds |

A condition raises **one** alert per rule and source, open until it clears itself; the alert keeps its rule, who was notified, the channels, and the clearing time and reason. `app` alerts are what the UI lists; `webhook` posts them to `alerts.webhook_url` (the `text` field is the shape Slack and Teams accept); `email` is accepted and recorded, with no mail sink wired yet. A source with no `rejected_share` rule alerts on every load that rejects rows, and one with no `unreachable` rule alerts after `delivery.alert_after_attempts`.
