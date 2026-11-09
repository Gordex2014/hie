# Development, tests, release

Layout of the repository, how the tests run, and how a release reaches the sandbox. Back to the [README](../README.md).

## Layout

```
src/            engine: config, sources (folder, S3), readers (CSV, NDJSON, Bundle), transform worker,
                FHIR delivery, pipeline, the source manager, API, alerts, storage
hie_runtime/    Python side of the transform protocol (worker.py, hie.DataError) and the pinned
                launcher it runs under (limits.py, limits.pin)
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

The end-to-end tests run the real Python transforms against a stand-in FHIR server. `tests/pipeline.rs` covers a folder source: the outage and recovery, the duplicate file and the re-sent rows, each kind of bad row, reprocessing with a correction, and the silence alert. `tests/alerts.rs` covers the rules: the one a source is born with, the CRUD, and the `rejected_share` and `silence` rules firing once and clearing. `tests/bulk_submit.rs` drives both bulk paths against a stand-in consumer: an NDJSON export handed over as source objects, and a lab CSV transformed, given derived ids, written by the engine and settled by receipts. `tests/object_source.rs` covers the object-store path the S3 source uses, with an in-memory store; `tests/sqs_source.rs` adds a notification queue to it, against a stand-in SQS that checks the SigV4 headers: the bucket listed once at start, then only announced objects taken, with their messages deleted after the pass and test events, foreign keys and gone objects consumed without effect; `tests/overlap.rs` checks that HFS receives the first Bundle well before a slow transform finishes the file; the same source was run by hand against a MinIO (the `ghcr.io/coollabsio/minio` mirror HFS's CI pins) holding the Synthea export, on both delivery paths against an HFS that requires tokens: 632 resources by REST and the same 632 by `$bulk-submit` through pre-signed URLs, each resource stored once, objects archived under the processed prefix, and a re-uploaded file recognised as a duplicate. `tests/fhir_server_source.rs` covers pulling another FHIR server: pages followed, a cursor per type that moves once the records are stored, nothing pulled twice, the token on every call, and the connection test. `tests/database_source.rs` covers the database source on SQLite: runs of the query past a cursor, the cursor moving once the rows are stored, nothing read twice, `cursor_start`, and the connection test; `tests/mssql_source.rs` runs the same source against a SQL Server when `HIE_TEST_MSSQL_URL` is set, and is skipped otherwise. `tests/leases.rs` puts two instances on one store: one runs the source, the other stands by, and takes over when the lease lapses. `tests/test_terminology.py` covers the terminology helpers against a stand-in HTS and the lab transform using them. `tests/zip_drop.rs` covers zips: a drop in a read-only bucket unpacked once and left in place, and one in a folder unpacked and archived, with a broken zip reported. `tests/hl7v2.rs` covers HL7 v2: a file of messages through a folder source, and messages over MLLP spooled, acknowledged, delivered, and resent as duplicates; `tests/test_hl7.py` covers the Python helpers. `tests/api.rs` drives a source through the API: test, create, check now, pause, resume, replace, delete. `tests/check.rs` runs `hie check`: it refuses to start while `hie.yaml` has a `runtime` key or `HIE_RUNTIME_URL` is set, and passes when the variable is unset or blank. `tests/worker.rs` runs the real launcher over a small test program for the long-lived worker cases of the `limited-process@1` contract (answers in order from one process, a reply written in pieces, a deadline and a late reply, a worker that exits or stays silent before it is ready, one that stops reading) and the engine's own duties: the pin check, setup errors, a stray line on stdout, the stderr tail, the run directories, the limits, the process group, the environment and cancelled calls; `tests/pipeline.rs` adds rows that hang, crash or run out of memory, each rejected once with the rows after it delivered, and `tests/api.rs` the bound on concurrent tries. `tests/worker.rs` also runs the real `hie_runtime/` with transforms that write to file descriptor 1 directly and through a child process, every record still getting its own reply. `tests/test_worker.py` checks the worker over its pipes (a write to file descriptor 1 included) and, on Linux, under the launcher's memory limit.

They are skipped if no Python 3.9+ is found; set `HIE_TEST_PYTHON` to point at one.

## Release and deploy

A version tag (`v0.1.0`) runs `.github/workflows/release.yml`, modelled on the HFS release chain (see [RELEASING.md](../RELEASING.md) for how to cut one):

1. **Build**: the Linux x86_64 release binary, packaged with `hie_runtime/`, `transforms/`, `sources/` and `examples/` as `hie-{version}-x86_64-unknown-linux-gnu.tar.gz`.
2. **Release**: a GitHub Release with the archive and its SHA-256.
3. **Image**: `ghcr.io/heliossoftware/hie:{version}`, `:{major.minor}`, `:sha-{sha}`, and `:latest` for a non-prerelease.
4. **Deploy**: from the `Deploy` runner over SSH to the host behind `hie.heliossoftware.com`: the binary and runtime go to `/opt/hie`, a systemd unit `hie` runs it on `0.0.0.0:8090`, and the job fails unless `/health` answers. `data/` and anything created through the API in `sources/` and `transforms/` survive a deploy.

Secrets the deploy needs: `HIE_DEPLOY_HOST`, `HIE_DEPLOY_USER`, `HIE_DEPLOY_PORT` (optional) and `HIE_FHIR_BASE_URL` (defaults to the public HFS). The public sandbox at `hie.heliossoftware.com` is deployed open, like the public HFS: a place to show the work and run demos, with nothing sensitive on it. A secure environment adds `HIE_AUTH_JWKS_URL`, `HIE_AUTH_ISSUER` and `HIE_AUTH_AUDIENCE` for the API, and `HIE_FHIR_TOKEN_URL`, `HIE_FHIR_CLIENT_ID`, `HIE_FHIR_CLIENT_SECRET` (and `HIE_FHIR_SCOPE`) for the engine's calls to HFS; the deploy writes them to the root-only `/etc/hie/hie.env` on the host.
