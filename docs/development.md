# Development, tests, release

Layout of the repository, how the tests run, and how a release reaches the sandbox. Back to the [README](../README.md).

## Layout

```
src/            engine: config, connectors (folder, S3, FHIR server, database, MLLP), formats
                (CSV, NDJSON, Bundle, HL7 v2, zip), transform worker, FHIR delivery, pipeline,
                the source manager, API, alerts, storage; see the module map below
hie_runtime/    Python side of the transform protocol (worker.py, hie.DataError)
transforms/     Python transforms: lab_results.py (CSV to Observation), fhir_passthrough.py
sources/        one YAML file per source; the API writes here too
examples/data/  sample files used by the tests and the demo
tests/          end-to-end Rust tests with a stand-in FHIR server; Python tests for the transform
docs/           design decisions
```
## Tests

```sh
cargo test                                 # unit tests and the end-to-end tests
python -m unittest tests/test_lab_results.py tests/test_hl7.py tests/test_terminology.py tests/test_worker.py
cargo clippy --all-targets -- -D warnings
```

The end-to-end tests run the real Python transforms against a stand-in FHIR server. `tests/pipeline.rs` covers a folder source: the outage and recovery, the duplicate file and the re-sent rows, each kind of bad row, reprocessing with a correction, and the silence alert. `tests/alerts.rs` covers the rules: the one a source is born with, the CRUD, and the `rejected_share` and `silence` rules firing once and clearing. `tests/bulk_submit.rs` drives both bulk paths against a stand-in consumer: an NDJSON export handed over as source objects, and a lab CSV transformed, given derived ids, written by the engine and settled by receipts. `tests/object_source.rs` covers the object-store path the S3 source uses, with an in-memory store; `tests/sqs_source.rs` adds a notification queue to it, against a stand-in SQS that checks the SigV4 headers: the bucket listed once at start, then only announced objects taken, with their messages deleted after the pass and test events, foreign keys and gone objects consumed without effect; `tests/runtime.rs` runs a feed against a stand-in of python-runtime's HIE facade: the code stored once per version and again after the runtime lost it, batches of `batch_size`, one reply per record applied, records waiting while the runtime is busy, and Try inline; `tests/overlap.rs` checks that HFS receives the first Bundle well before a slow transform finishes the file; the same source was run by hand against a MinIO (the `ghcr.io/coollabsio/minio` mirror HFS's CI pins) holding the Synthea export, on both delivery paths against an HFS that requires tokens: 632 resources by REST and the same 632 by `$bulk-submit` through pre-signed URLs, each resource stored once, objects archived under the processed prefix, and a re-uploaded file recognised as a duplicate. `tests/fhir_server_source.rs` covers pulling another FHIR server: pages followed, a cursor per type that moves once the records are stored, nothing pulled twice, the token on every call, and the connection test. `tests/database_source.rs` covers the database source on SQLite: runs of the query past a cursor, the cursor moving once the rows are stored, nothing read twice, `cursor_start`, and the connection test; `tests/mssql_source.rs` runs the same source against a SQL Server when `HIE_TEST_MSSQL_URL` is set, and is skipped otherwise. `tests/leases.rs` puts two instances on one store: one runs the source, the other stands by, and takes over when the lease lapses. `tests/test_terminology.py` covers the terminology helpers against a stand-in HTS and the lab transform using them. `tests/zip_drop.rs` covers zips: a drop in a read-only bucket unpacked once and left in place, and one in a folder unpacked and archived, with a broken zip reported. `tests/hl7v2.rs` covers HL7 v2: a file of messages through a folder source, and messages over MLLP spooled, acknowledged, delivered, and resent as duplicates; `tests/test_hl7.py` covers the Python helpers. `tests/api.rs` drives a source through the API: test, create, check now, pause, resume, replace, delete.

They are skipped if no Python 3.8+ is found; set `HIE_TEST_PYTHON` to point at one.

## Module map

Each module owns one capability behind a small interface; the rest of the engine reaches it only through that interface. Work that adds a capability goes in the files listed for it, so several changes can proceed side by side. The **registration points** at the end are the only shared lines: a change there is one line, appended.

### Connectors: `src/connectors/`

Where a source's data comes from. Interface (`connectors/mod.rs`):

- `Connector`: `start` (listen; a no-op once started), `list(&Cursors) -> Vec<Listed>` (key, name, size), `fetch(key) -> Incoming`, `acknowledge(key, archived_name, &Cursors)` (archive the object or move the cursor; returns where it is now), `settle` (delete the SQS notifications of what the pass took), `probe(&Cursors) -> Probe` (the connection test; takes nothing), `read_only`, `staging() -> Option<&dyn Staging>`, and the optional `describe(&Cursors)` and `sample(&Cursors, limit)`, which answer `CapabilityError::Unsupported` until a connector implements them.
- `Staging` (bulk object staging): `share` (pre-signed URL), `put_submission_file`, `put_unpacked`, `remove_submission_files`. Every `ObjectSource` has it and keeps each operation's own condition: sharing needs a signer (and works on a read-only bucket), writing needs a signer and a writable bucket, cleaning up needs a writable bucket; otherwise the operation answers `None` or does nothing and the engine serves the file itself. Other connectors have no staging.
- `Cursors`: how far a pulling source has read, per key, in the store; without a store (`Source::from_config` before `with_db`, the connection test) nothing is kept and every read starts from the configured start.
- `Source` (`connectors/source.rs`): the handle the pipeline and the manager hold, one connector bound to its cursors; capabilities a connector lacks answer as before (nothing to start or settle, no URL, nowhere to stage). `Source::from_config` gets its connector from `connectors/build.rs`, the one place a connector comes into being: where an admission policy goes, ahead of the registry's `build`.
- Registry: `kind(&SourceConfig) -> &Kind`, one `Kind` per `type:`: `check` (what the type cannot check), `resolve` (paths against `hie.yaml`), `describe` (for logs and the API), `format` (the record format a server, query or MLLP sender imposes), `build` (the connector; checks only what building needs, such as environment variables and URLs, and does not connect). `SourceConfig::{check, resolve, describe}` and `FeedConfig::resolve` delegate to it; `SourceConfig` stays in `config.rs` as the YAML schema.
- `is_gone(&error)`: whether a fetch failed because the object is no longer there.

| Kind | Files | Inside |
| --- | --- | --- |
| `folder`, `s3` | `object/mod.rs` (kinds, S3 builder), `object/folder.rs` (`FolderSource`), `object/store.rs` (`ObjectSource`, staging), `object/sqs.rs` (`SqsQueue`) | listing, age guard, archive rename, SQS-announced listing with periodic relist, signing |
| `fhir_server` | `fhir_server/mod.rs` (pull per type, cursors), `fhir_server/client.rs` (remote auth, GET) | `_lastUpdated` paging, one cursor per resource type |
| `database` | `database/mod.rs` (runs past the cursor), `database/connection.rs` (the URL `url_env` names), `database/driver.rs` (pools, binding per engine), `database/rows.rs` (row decoding), `database/cursor.rs` (`Cursor`) | PostgreSQL, MySQL, SQLite (sqlx), SQL Server (tiberius) |
| `mllp` | `mllp/mod.rs` (connector over a spool `FolderSource`), `mllp/listener.rs` (MLLP framing, ACK, listener) | spool before ACK |

`hie::source`, `hie::sqs` and `hie::hl7` re-export what was reached there before these modules existed. Log lines moved out of the old modules keep their old `tracing` targets (`hie::source`, `hie::database`, `hie::hl7`, `hie::pipeline`), so an `HIE_LOG` filter written for them still applies.

### Formats: `src/formats/`

How a fetched object becomes records: `read(format, bytes) -> Vec<SourceRecord>` (`mod.rs`, with `decode_text` and `sha256_hex`), one file per format (`csv.rs`, `ndjson.rs`, `fhir_bundle.rs`, `hl7v2.rs`), and `zip.rs` for the container (`is_container`, `entries`, `takes(format, path)`). Connectors never parse; the pipeline calls `formats` on what a connector fetched.

### Pipeline: `src/pipeline/`

`FeedRunner` and its pass (`mod.rs`: lease, start, tick, poll loop), with each stage an `impl FeedRunner` block of its own: `ingest.rs` (list, fetch, duplicates, zips, admit, acknowledge), `transforming.rs` (run the transform over received records), `outcomes.rs` (what each record became: rejected, filtered, failing validation, transformed, passed through by the bulk preflight, or left for the next pass), `deliver.rs` (REST batches, retries, backoff), `submit.rs` (`$bulk-submit` submissions, receipts, archive), `alerting.rs` (rejected share, outage and recovery, silence, missing by). It uses connectors only through `Source`.

### Manager: `src/manager/`

`SourceManager`: `mod.rs` holds the source lifecycle (start, create, replace, delete, pause, resume, check now); `probe.rs` the connection test (`test`: resolve without check, no store attached, a probe error is `reachable: false`, a build error is a 400); `transforms.rs` holds the transform files (list, read, save and restart the sources using one, try).

### API: `src/api/`

`mod.rs` holds `AppState`, `ApiError` and `router`, which merges each feature's router under the bearer layer for `/api` and leaves the open routes open. One file per feature, each with `router()` (and `open_router()` for unauthenticated routes): `sources.rs` (sources, test, stats), `loads.rs` (loads and their failures, retry), `records.rs` (records, reprocess), `transforms.rs`, `monitoring.rs` (status; open `/health`, `/metrics`), `alerts.rs` (alerts and rules), `bulk.rs` (open `/bulk/{token}/...` for HFS).

### Store: `src/db/`

`mod.rs` opens the store and creates or upgrades it; `engine.rs` holds the two drivers (SQLite, PostgreSQL), `Param`/`params!` and `Row`. One file per area owns its tables' DDL, the columns added to them since, its row types and its queries (`impl Db`): `loads.rs` (files, records), `submissions.rs`, `sources.rs` (pause state, cursors, leases), `alerts.rs` (alerts, rules). `mod.rs` runs every table's DDL as one batch in `TABLES` order (the order stores have always been created in), then every area's `ADDED_COLUMNS` in order, each added when missing; that is how an existing store upgrades, on SQLite and on PostgreSQL alike.

### Elsewhere

`config.rs` (definitions and their YAML), `transform.rs` and `runtime.rs` (the local worker and the shared runtime), `fhir.rs` and `bulk.rs` (calls to HFS), `credentials.rs` (the engine's identity towards HFS), `auth.rs` (bearer tokens on the API), `alerts.rs` (raising and clearing), `secrets.rs` (`named(name)`: the value of a secret a source names, today an environment variable).

### Who owns what next

| Change | Owns | Adds |
| --- | --- | --- |
| H-versions: transform versions and their store and API | `src/transform.rs`, `src/runtime.rs`, `src/manager/transforms.rs`, `src/api/transforms.rs`, `src/pipeline/transforming.rs` (which version a load runs) | `src/db/versions.rs` |
| H-connectors: describe, sample, test evidence, first-load progress | `src/connectors/**` except the three H-secrets files below; `src/manager/probe.rs`; `src/api/sources.rs` | describe/sample files per kind (for example `connectors/database/sample.rs`); `src/db/connectors.rs` if evidence is kept |
| H-history: failures, attempts, retry, usage, validation, delivery | `src/db/loads.rs`, `src/api/loads.rs`, `src/api/records.rs`, `src/pipeline/outcomes.rs`, `src/pipeline/deliver.rs`, `src/pipeline/submit.rs`, `src/pipeline/ingest.rs`, `src/pipeline/alerting.rs` | `src/db/history.rs` |
| H-secrets: secrets and admission | `src/secrets.rs` (may become `src/secrets/`), `src/connectors/build.rs` (admission, before any connector is built), `src/connectors/database/connection.rs`, `src/connectors/fhir_server/client.rs`, `src/manager/mod.rs` (`create`, `replace`) | `src/db/secrets.rs`, `src/api/secrets.rs` |

Registration points, each a one-line append: `src/lib.rs` (`pub mod`), `src/api/mod.rs` (`mod` and `.merge(…::router())`), `src/db/mod.rs` (`mod`, `TABLES`, `ADDED_COLUMNS`), `src/connectors/mod.rs` (`kind()`, only for a new kind of source). A new column on an existing table is appended to that area's `ADDED_COLUMNS` and, so a new store has it too, to its DDL.

## Release and deploy

A version tag (`v0.1.0`) runs `.github/workflows/release.yml`, modelled on the HFS release chain (see [RELEASING.md](../RELEASING.md) for how to cut one):

1. **Build**: the Linux x86_64 release binary, packaged with `hie_runtime/`, `transforms/`, `sources/` and `examples/` as `hie-{version}-x86_64-unknown-linux-gnu.tar.gz`.
2. **Release**: a GitHub Release with the archive and its SHA-256.
3. **Image**: `ghcr.io/heliossoftware/hie:{version}`, `:{major.minor}`, `:sha-{sha}`, and `:latest` for a non-prerelease.
4. **Deploy**: from the `Deploy` runner over SSH to the host behind `hie.heliossoftware.com`: the binary and runtime go to `/opt/hie`, a systemd unit `hie` runs it on `0.0.0.0:8090`, and the job fails unless `/health` answers. `data/` and anything created through the API in `sources/` and `transforms/` survive a deploy.

Secrets the deploy needs: `HIE_DEPLOY_HOST`, `HIE_DEPLOY_USER`, `HIE_DEPLOY_PORT` (optional) and `HIE_FHIR_BASE_URL` (defaults to the public HFS). The public sandbox at `hie.heliossoftware.com` is deployed open, like the public HFS: a place to show the work and run demos, with nothing sensitive on it. A secure environment adds `HIE_AUTH_JWKS_URL`, `HIE_AUTH_ISSUER` and `HIE_AUTH_AUDIENCE` for the API, and `HIE_FHIR_TOKEN_URL`, `HIE_FHIR_CLIENT_ID`, `HIE_FHIR_CLIENT_SECRET` (and `HIE_FHIR_SCOPE`) for the engine's calls to HFS; the deploy writes them to the root-only `/etc/hie/hie.env` on the host.
