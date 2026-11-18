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

An S3 source names a `bucket`, an optional `prefix`, and an `archive_prefix` (default `processed`) that acknowledged objects are moved under. `endpoint` points at an S3-compatible store such as MinIO; `region` defaults to `us-east-1`. It says how it reaches the bucket with exactly one of:

- `credential: <name or id>`: a credential stored through [`/api/secrets`](api.md#secrets) (an access key id and secret access key). The source always uses its **current revision**: after a `PUT /api/secrets/{id}` it loads the new one on its next pass (at once with `check-now`), registers itself for it so the credential cannot be deleted while used, and lists the bucket in full on that pass. Until a revision has been loaded (a credential not stored yet, or deleted), a `credential` alert stays open, nothing is listed or handed over, and what is already submitted is still polled and settled. When loading a new revision fails after an earlier one was loaded, the source keeps working with the earlier one, the `credential` alert stays open, and the next pass tries again. Either way the alert clears on the first pass that loads the current revision;
- `anonymous: true`, for a public bucket.

A definition with neither, or both, is refused (`400`, and at start a file that has neither stops the engine, naming the file). The engine reads no AWS credentials from its environment, profiles or instance metadata. To move an existing source: add `credential: <name>` to its file, start the engine, then `POST /api/secrets` with that name; the source loads it on its next pass, without another restart. The connection test (`POST /api/sources/test`) signs with the credential's current revision, so it proves the credential before the source exists.

`anonymous: true` reads a public bucket without credentials: requests are not signed, so there are no pre-signed URLs either, and on the bulk path each object is handed over by its public URL (virtual-hosted on AWS, `https://{bucket}.s3.{region}.amazonaws.com/{key}`; path style for a bucket name with a dot or under `endpoint`; the key encoded once, segment by segment). `read_only: true` is for someone else's bucket: nothing is moved or written there, and an object the engine has already taken is recognised by its name and size, then by its content, and left alone on later passes. A read-only source can read a whole bucket: an empty `prefix`, and `archive_prefix: ""`, which excludes nothing. Keys are read and handed over exactly as the bucket lists them. A `prefix` or `archive_prefix` that contains `%`, `~`, non-ASCII or other characters an object-store path escapes is matched in its escaped form; keep them to plain ASCII. When the source sets no `region` or `endpoint`, the ones in effect may come from the environment (`AWS_REGION`, then `AWS_DEFAULT_REGION`; `AWS_ENDPOINT_URL`, then `AWS_ENDPOINT`; and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`), which only say where the bucket is; an empty one is refused, since it cannot be unset and would make the public URL point elsewhere. An endpoint over plain `http` needs `endpoint:` on the source.

With a **notification queue** (`queue:` the URL of an SQS queue the bucket's `s3:ObjectCreated:*` events go to, directly or through SNS) a pass reads the queue instead of listing the bucket: only the announced objects are fetched, and their messages are deleted once the objects are in the store, so a pass that fails leaves them to be delivered again. The bucket is still listed in full at start and every `relist_seconds` (default 3600), so an object whose event was lost is taken within the hour rather than never. The queue is signed (SigV4) with the source's stored credential, so `anonymous` and `queue` do not go together; its region comes from the queue URL, else the bucket's region in effect, else `us-east-1`. The queue's visibility timeout should exceed a pass over the largest drop.

A **zip** that arrives on any file source is a drop of several files: every entry whose extension fits the source's format (`.csv`, `.ndjson`/`.jsonl`, `.json` for Bundles, `.hl7`) becomes a load of its own, named `drop.zip!/path/inside`; the other entries (a manifest, a README, a CSV in an NDJSON drop) are skipped. The zip is recorded as `unpacked`, archived like any file, and not read twice. Entries are unpacked in memory, so a zip should hold a few hundred megabytes at most. On the `source_objects` bulk path HFS cannot read a zip, so each unpacked entry is **staged** where HFS can fetch it: in the source's own store under `{archive_prefix}/unpacked/` when the store can be written and can sign URLs, otherwise under the engine's `bulk_dir`, served at `/bulk/{token}/files/{n}`; staged entries go once the receipts are in.

A **FHIR server** source (`type: fhir_server`, see `sources/other-fhir.yaml.example`) pulls another server into HFS: it names the server's `url`, the resource `types` to pull, and optionally `since` (a FHIR instant to start from) and `auth` (`method: client_credentials` with `token_url`, `client_id`, `client_secret_env`, or `method: bearer` with `token_env`; secrets are read from the named environment variable, never written in the file). Each pass searches every type for `_lastUpdated=gt{cursor}` sorted by `_lastUpdated`, `page_size` per page and up to `max_pages` pages, and hands the resources on as one NDJSON load per type; the cursor moves to the newest `meta.lastUpdated` once the records are stored, so a crash repeats a page rather than losing it. The format is `ndjson` whatever the file says, resources go out by their id, and `bulk_submit` is refused. The connection test reads `/metadata` and counts what waits past each cursor.

A **database** source (`type: database`, see `sources/lab-database.yaml.example` and `transforms/sql_lab_results.py`) reads a SQL database with the operator's own query: `url_env` names the environment variable holding the connection URL (`postgres://`, `mysql://`, `sqlserver://` or `sqlite:`; the URL carries the password, so it is never in the file), `query` is the SQL with `{cursor}` where the cursor value goes, an `ORDER BY` on the cursor column and a `LIMIT`, and `cursor_column` is the column the cursor follows. Prefer a **unique, increasing column** such as an id: with an updated-at column, rows that share one timestamp (a multi-row `INSERT` gets a single `now()`) and fall on both sides of a `LIMIT` are never read, because the next run asks for values strictly past the cursor; the engine logs a warning when a whole run shares one cursor value. Each pass binds the cursor as a parameter, runs the query up to `max_runs` times moving the cursor between runs, and hands the rows on as one NDJSON load; each row reaches the transform as a JSON object with the database's types (numbers, ISO timestamps, JSON). The cursor keeps its kind (integer, float, timestamp, date, text) and moves to the largest value seen once the records are stored. `cursor_start` says where to begin the first time. PostgreSQL is the target; MySQL/MariaDB and SQLite go through the same code (the tests run on SQLite), and **SQL Server** through its own driver with the same definition: `sqlserver://user:password@host:1433/database?encrypt=on&trust_cert=true` (`mssql://` too; `encrypt` is on by default, `trust_cert=true` accepts a self-signed certificate; the placeholder is bound as `@P1`, and a `TOP n` plays the part of `LIMIT`). SQL Server authentication only; Windows authentication is not supported. The connection test connects and runs the query once past the cursor.

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
  - `source_objects`: for data that already is FHIR with ids, such as an NDJSON export. No transform runs. The engine stores the rows for the history, predicts what HFS will refuse (no `resourceType`, no `id`), and hands the **source objects themselves** over: pre-signed URLs for S3 objects, public URLs for the objects of an anonymous bucket, the engine's own `/bulk/{token}/files/{n}` for folder files and for the zip entries it staged itself. An object in a store is never served by the engine; only files the engine reads from disk or unpacked itself are. A URL that cannot be signed yet leaves the file's records pending for the next pass; an object no configuration can hand over (no signer and not a public bucket, or a file taken from a store whose source has since been replaced by one that reads from disk) has its records rejected with the reason. A bulk delivery failure is logged, alerted and stored with every URL in its reason cut to its origin (`https://host/…`): no path, query or user information. One submission per drop (up to `manifest_objects_max` objects), with a `submissionId` derived from the files so a repeated drop is refused by HFS instead of ingested twice. Objects stay at the source until the receipts are in, then they are archived.
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
