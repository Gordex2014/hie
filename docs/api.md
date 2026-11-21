# API and authentication

The HTTP API clients and UIs use, and how it and the engine's own calls to HFS are authenticated. Back to the [README](../README.md).

## API

| Method and path | Does |
|---|---|
| `GET /api/sources` | Every source: definition, state (`waiting`, `receiving`, `late`, `paused`), freshness, record counts, last file, last alert |
| `POST /api/sources` | Creates a source from its definition (the YAML as JSON); writes `sources/{id}.yaml` and starts it. `409` if it exists, `400` with the reason if invalid (`code: invalid_definition`, also when the credential it names does not exist, and in a build without a secret backend, where no retry can help). When it names a credential and the store has no usable key: `503 secrets_unavailable`, and the same request works once the key is fixed and the engine restarted |
| `POST /api/sources/test` | Tests a connection: `{"source": {...}}` or a full definition. Answers `reachable`, `elapsed_ms`, `objects`, `newest`, `newest_at`. An S3 source with a `credential` is tested with the credential's current revision; with no secret store, whether the key is missing or unusable or the build has no secret backend, it answers `503 secrets_unavailable` |
| `GET /api/sources/{id}` | One source, as above |
| `PUT /api/sources/{id}` | Replaces the definition and restarts the source. A definition is refused as `POST` refuses it: `400 invalid_definition` (in a build without a secret backend too, when it names a credential), or `503 secrets_unavailable` when it names a credential and the store has no usable key |
| `DELETE /api/sources/{id}` | Stops the source and removes its file; its records and alerts stay |
| `POST /api/sources/{id}/pause` | Stops polling and delivery until resumed; body `{"by": "..."}` is kept with the time |
| `POST /api/sources/{id}/resume` | Resumes and passes at once |
| `POST /api/sources/{id}/check-now` | Passes now instead of at the next poll |
| `GET /api/sources/{id}/loads` (also `/files`) | Every load: the file, its method, records read, sent, rejected, in progress, how long it took, where it is archived |
| `GET /api/loads/{id}` | One load with its **failures grouped by reason**: kind, reason, count, first line, and up to three sample rows each |
| `POST /api/loads/{id}/retry` | Puts the load's rejected rows back at the start, all of them or those with one reason (`{"reason": "..."}`); the source passes at once |
| `GET /api/sources/{id}/stats?days=14` | What a source's page draws: today (files, records, sent, rejected, pending) and one row per day |
| `GET /api/sources/{id}/failures?days=14` | Distinct records the source rejected over the last 24 hours and each of the last `days` UTC days (1 to 90), from the event log, with how far the log covers each (see [Rejected records over a period](#rejected-records-over-a-period)) |
| `GET /api/transforms` | Every transform file: name, path, size, modified, and the sources that use it |
| `GET /api/transforms/{name}` | The transform's code |
| `PUT /api/transforms/{name}` | Saves the code (`{"code": "..."}`); the sources using it restart their Python worker. `201` when new |
| `POST /api/transforms/{name}/try` | Runs the transform on `{"records": [...]}` (up to 100) without loading: per record, the resources it became or the rejection reason. At most four tries run at once; `503` when one cannot start within 10 s |
| `GET /api/monitoring/status` | What the monitoring page opens on: `needs_you` (late, paused, or rejecting sources with the reason), `last_24h` (files, sent, rejected, alerts), every source |
| `GET /api/records?source=&status=&file_id=` | Records; `status` is `received`, `pending`, `sent` or `rejected` |
| `GET /api/records/{id}` | One record: file, line, original and corrected row, resources sent, HFS response |
| `POST /api/records/{id}/reprocess` | Runs a record again; body `{"raw": {...}}` replaces the row and keeps the original |
| `GET /api/alerts?source=&open=true` | Alerts, newest first, each with its rule, recipients, channels, and when and why it cleared; `open=true` lists only those not cleared. Kept for `retention.history` (see [Alerts and retention](#alerts-and-retention)) |
| `GET /api/events?source=&kind=&since=&cursor=&limit=` | The event log, oldest first: each record's attempts and outcomes, each load's steps, each submission's steps (see [Events](#events)) |
| `GET /api/alert-rules?source=` | Every rule, each with its `created_at`; `POST` creates one (`400` names what is wrong with it). `created_at` is set by the engine, never by the body. A rule id is never given again, like alert and event ids |
| `GET` / `PUT` / `DELETE /api/alert-rules/{id}` | One rule: read, replace, remove |
| `GET /api/secrets` | Every stored credential, by name, without its value |
| `POST /api/secrets` | Stores a credential: `{"name", "kind": "aws", "value": {"access_key_id", "secret_access_key"}}`. `201` |
| `GET /api/secrets/{id}` | One credential, without its value |
| `PUT /api/secrets/{id}` | Replaces its value with a new revision: `{"value": {...}}`. Sources use it from their next pass |
| `DELETE /api/secrets/{id}` | Removes it. `409` with `sources` while a source is registered for it |
| `GET /metrics` | Prometheus text: records by status, last received time, paused flag, alerts by kind (`hie_alerts_total`, which counts purged alerts too and never goes down) |
| `GET /health` | `{"status": "ok", "service": "hie", "version", "uptime_seconds", "started_at", "timestamp", "secrets"}`; `secrets` is `{"backend": "native"\|"none", "available", "key_id"}`, the key id being a one-way 16-hex fingerprint of the key in use, or `null` |

Errors answer `{"error": "<sentence>"}`. Where a client needs to tell cases apart they also carry a stable `code`, and sometimes more members: `invalid_definition` (a source definition that cannot run), `source_not_started` (`500`: the definition was saved, but its source could not be started; it starts with that definition at the next start of the engine), `store_busy` (`503` with `Retry-After: 1`: the store's lock was not free; try again), `invalid_query` (`400` with `field`: a query parameter of `/api/events` that cannot be read), and the codes of the credential routes below.

### Events

The engine appends an event for each attempt and outcome of a record, each step of a load and each step of a `$bulk-submit` submission. An event is written in the same transaction as the change it reports: when a record's outcome is stored, so is its event, before the next record is sent or transformed. If the event cannot be stored, neither is the outcome. Events are never changed; the retention purge removes those older than `retention.history` (see [Retention](running.md#retention)).

```json
{"id": 41, "at": "2026-11-20T08:00:03.120Z", "source_id": "lab-results", "kind": "record.rejected",
 "load_id": 7, "record_id": 912, "attempt": null, "error_kind": "data",
 "reason": "value 'alto' is not a number", "detail": null}
```

- `id` orders the log: ids are given in commit order and never given twice, even after a purge or a restart.
- `at` is when the writing instance stored it, by its own clock. Across instances, or after a clock step, it may be out of order with `id`.
- `load_id` is the load (`/api/loads/{id}`), `record_id` the record (`/api/records/{id}`). `detail` is an object or `null`.

Record events carry `load_id` and `record_id`:

| `kind` | When | Also |
|---|---|---|
| `record.delivered` | The FHIR server took the record (its batch entry answered 2xx, or a bulk receipt named it) with no failed attempt since it was received or put back | `attempt` (REST); `detail.submission`, `detail.run` (bulk) |
| `record.recovered` | The same, after failed attempts: on REST the record's own; on the bulk path its submission's | `attempt` (REST); `detail.failures` (bulk) |
| `record.attempt_failed` | An attempt failed for a reason that may pass; the record is kept and tried again. A delivery (`detail.phase: "delivery"`, `attempt`, `detail.next_attempt_at`), or a transform that could not finish now (`detail.phase: "transform"`, no `attempt`; one for a record and reason while the source's runner runs, and again when the reason changes, the record is put back or the runner starts again) | `reason` |
| `record.rejected` | The record is rejected: `error_kind` is `parse`, `data`, `validation`, `filtered`, `transform` or `server`, including a file that can no longer be handed over and a refused submission | `error_kind`, `reason`; `attempt` when the FHIR server refused it on REST |
| `record.retried` | A person put the record back: a load's Retry or a record's reprocess | `detail.from_status`, `detail.corrected` |

Load events carry `load_id`:

| `kind` | When | `detail` |
|---|---|---|
| `load.received` | A file, object or zip entry was recorded and its records stored, or a zip was recorded | `name`, `size`, `records`; `container: true` for a zip |
| `load.duplicate` | The same content was already received | `name`, `duplicate_of` |
| `load.unreadable` | The file could not be read (`reason` says why) | `name`, `size` |
| `load.finished` | Every record of the load is sent or rejected | `records`, `sent`, `rejected`; `container: true` for a zip |
| `load.retried` | A person retried the load's rejected rows and at least one was put back (`reason` is the reason it was limited to, if any) | `retried` |

Submission events (the bulk path) carry `detail.submission`, the id the FHIR server sees (the same for a repeated drop), and `detail.run`, a 16-hex key of this run that grants nothing:

| `kind` | When | Also |
|---|---|---|
| `submission.opened` | The engine opened a submission | `detail.records`, `detail.files` |
| `submission.attempt_failed` | A kick-off or status poll failed for a reason that may pass | `attempt`, `reason`, `detail.next_attempt_at` |
| `submission.refused` | The FHIR server refused the submission; each of its records gets `record.rejected` | `reason`, `detail.records` |
| `submission.settled` | The receipts were applied and the submission closed, once | `detail.sent`, `detail.rejected`, `detail.failures` |

Nothing else is an event: a Try, a worker that cannot start (no record is involved; the log says why), changes to sources, the transform step itself, archiving a file, and alerts (which have their own list).

- Deliveries in a period are `kind=record.delivered,record.recovered` (the two never describe the same delivery). The `record` family also holds rejections, put-backs and failed attempts.
- An attempt is a delivery attempt that ended, for this record, in a delivery, a rejection or a failure that may pass. A batch the server refused as a whole is sent again record by record, and that whole refusal is not counted as an attempt.
- A load finishes again after a Retry or a reprocess put some of its records back: there is one `load.finished` each time.
- An outcome worked out for a record that a person put back meanwhile (a Retry or a reprocess while it was being transformed or sent) changes nothing and writes no event; the record goes through again.
- On a store upgraded from an earlier version, a submission that was partly settled before the upgrade counts in `submission.settled` only the records settled after it.

**Text.** A `reason` is the engine's own text for the fact (the stored error), with every URL cut to its origin followed by `/…` (a signed URL's path and query never appear), and at most 500 characters. A request target the engine names in a failure (a status, manifest or receipt URL, absolute or relative) is only ever its origin, or `<URL>`. Any other word (up to whitespace, a quote, `<` or `>`) that has a `?` followed later in the word by `=` keeps what comes before the `?` and loses the rest, so a relative target a server echoes, such as `/receipts/1?X-Amz-Signature=…`, becomes `/receipts/1?…`. On the bulk path the submission's token is replaced by `<token>` wherever it would appear, in the records' stored errors and answers too. No event carries a submission's token, its status URL or a server's full answer.

**Reading.** `GET /api/events` lists events oldest first, by `id`:

- `source`: one source id.
- `kind`: one or more kinds or families (`record`, `load`, `submission`), separated by commas.
- `since`: an RFC 3339 instant (`2026-11-20T08:00:00Z`). Only events stored at or after it (`at >= since`) are listed, with or without a cursor. Events are stored to the millisecond, so a finer instant counts from the next millisecond. Without a cursor it also sets where the page starts: at the first event, by id, stored at or after it.
- `cursor`: the `next_cursor` of an earlier answer, to continue after it. Its text is opaque (today `e1.` and a number). Give the same filters again with it. A cursor stays valid after its event is purged.
- `limit`: 1 to 1000, 100 by default.

```json
{"events": [ ... ], "next_cursor": "e1.41", "has_more": false}
```

`has_more` says another page is ready now. `next_cursor` is where the next request continues: after the last event listed when `has_more`, otherwise the newest position of the log when the request was read, or the cursor given if that is later. A poller whose filters matched nothing still moves forward, and a cursor never moves back, even after a purge. It is `null` only when the log is empty and no cursor was given. Ids are given in commit order (on PostgreSQL every instance writes events under one lock, in turn), and a page never lists past the newest id committed when it was read, so polling from `next_cursor` never skips an event.

Every parameter that cannot be read is refused with `400`, `code: "invalid_query"` and `field` naming it: a value that does not parse (`limit` is digits only), a parameter given twice, or one that is not `source`, `kind`, `since`, `cursor` or `limit`.

A poller pages with `cursor`: the first request may give `since`, and each next one the `next_cursor` it got (with the same filters). A `since` without a cursor reads a range of the log by time on every request, which costs more on a large log.

### Rejected records over a period

`GET /api/sources/{id}/failures?days=14` counts, from the event log, the distinct records a source rejected in each window: a record rejected more than once in a window counts once.

```json
{"source_id": "lab-results", "tz": "UTC", "covered_from": "2026-11-12T09:30:00.000Z",
 "last_24h": {"from": "...", "to": "...", "records": 3, "filtered": 1, "complete": true},
 "days": [{"day": "2026-11-07", "from": "2026-11-07T00:00:00.000Z", "to": "2026-11-08T00:00:00.000Z",
           "records": 0, "filtered": 0, "complete": false}, ...]}
```

- `last_24h` is the 24 hours before the request; `days` are UTC calendar days, oldest first, the last one today (still running). A window holds what was stored from its `from`, included, to its `to`, left out: an event at midnight belongs to the day that starts then.
- `records` counts records rejected as `parse`, `data`, `validation`, `transform` or `server`. Records a transform filtered out on purpose (`filtered`) are not failures and are counted apart, in `filtered`.
- `covered_from` is where the log's history starts: when this store began recording events (its first start with a version that keeps them), or the retention's limit, whichever is later. A window is `complete` when `covered_from` is at or before its `from`. A window that is not complete is still counted over what is kept, and says `complete: false`.
- `days` is a whole number from 1 to 90 (14 by default). A value that cannot be read, a second `days` or any other parameter answers `400 invalid_query` with `field`. An unknown source answers `404` with `{"error": "source <id> not found"}`, as `/stats` does.

### Alerts and retention

Stored alerts follow `retention.history` too. An alert is purged that long after it cleared, or after it was raised if it is still open, except:
- open alerts of a rule that exists, is enabled and was created before the alert (on SQLite a deleted rule's id may be given to a new rule);
- open `credential` and `definition` alerts, which the engine clears itself;
- the alert with the highest id.

Open alerts of a deleted or disabled rule, and other open alerts without a rule, are purged by age. An alert id is never given again after a purge: the newest alert is always kept, so a SQLite store never reuses one, and PostgreSQL ids come from a sequence. Rule ids are never given again either: SQLite rules take their ids from `AUTOINCREMENT` (a store made before is rebuilt to it once, at its first start with this version), PostgreSQL's from a sequence. `hie_alerts_total` in `/metrics` adds the purged alerts to the stored ones, so it never goes down.

### Secrets

Connector credentials, which S3 sources name with `credential:` (see [sources](sources.md)). A credential has an id (`sec_` and 16 hex characters), a name, a kind and a current revision. Only `aws` exists: an access key id and a secret access key, held together in each revision, with no session token.

```json
{"id": "sec_3f9c2a7d1e4b6a80", "name": "lab-bucket", "kind": "aws", "hint": "MPLE",
 "revision": 1, "created_at": "2026-11-12T10:00:00.000Z", "updated_at": "2026-11-12T10:00:00.000Z",
 "used_by": ["lab-s3"]}
```

- **No answer carries a value.** `hint` is the last four characters of the access key id, to tell credentials apart.
- **Names** are 1 to 64 characters, unique, with no whitespace around them, no control characters, and not starting with `sec_`. A `credential:` starting with `sec_` is an id; anything else is a name.
- **A `PUT`** makes a new revision; `name` and `kind` stay. A running source uses it from its next pass (or at once with `check-now`). The previous value is no longer returned by the store, but may remain in database files and backups until they are overwritten.
- **`used_by`** and the `409`'s `sources` list the sources registered for the credential by any engine instance on the store. A source is registered when it is created or replaced with the credential, and released once its runner for another credential (or for none) has ended. `POST /api/sources/test` uses a credential without registering anything, so a credential may be deleted while a Test runs, and the Test then reports that it could not use it.
- **Bodies** are a JSON object with exactly the members shown; anything else, at any level, is refused with a fixed sentence that never repeats what was sent.

| Status | `code` | When |
|---|---|---|
| `400` | `invalid_body` (with `field` when one is at fault) | The body does not parse, has unknown members, misses a field, or breaks a rule (name, kind, value shape) |
| `404` | `secret_not_found` | No credential with that id |
| `409` | `name_in_use` | The name is taken |
| `409` | `secret_in_use` (with `sources`) | A source is registered for the credential |
| `503` | `secrets_unavailable` | No store: this build has no secret backend, the store has no key, the key is malformed, cannot be read or is not safe to use, or the key file could not be created. `error` is a fixed sentence for the cause, with no path or system error; the engine log says why. When the key is at fault, the same request works once it is fixed and the engine restarted |
| `503` | `store_busy` (`Retry-After: 1`) | The store's lock was not free |
| `500` | `internal_error` | The engine failed; its log has the reason |

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
| `hie/read` | Every `GET` except `/api/secrets`: sources, loads, records, transforms, alerts, events, rules, monitoring |
| `hie/manage` | Changing sources, rules, loads and records (`POST`, `PUT`, `DELETE`, pause, resume, retry, reprocess, try), and reading; and credentials: every method on `/api/secrets`, reads included |
| `hie/transforms` | `PUT /api/transforms/{name}`: code the engine then runs; and reading |
| `hie/*` | Everything |

A valid token without the scope a request needs answers `403` with the scope in `required`. A token from the same identity provider that carries only HFS scopes (`system/*.cruds`) is authenticated but may do nothing here.

`docker/keycloak/` holds a Keycloak for local work: HFS's `fhir` realm (its `hfs-backend-client`, which the engine uses towards HFS) plus the `hie/*` client scopes and three service-account clients, `hie-admin-client` (`hie/*`), `hie-operator-client` (`hie/manage`) and `hie-monitor-client` (`hie/read`), all with audience `hie-api`. `docker compose -f docker/keycloak/docker-compose.yml up -d` serves it on 8180; `docker/keycloak/get-token.sh [client]` prints a token. The engine then runs with `HIE_AUTH_JWKS_URL=http://localhost:8180/realms/fhir/protocol/openid-connect/certs`, `HIE_AUTH_ISSUER=http://localhost:8180/realms/fhir`, `HIE_AUTH_AUDIENCE=hie-api`, and `HIE_FHIR_TOKEN_URL=http://localhost:8180/realms/fhir/protocol/openid-connect/token` with `hfs-backend-client` / `hfs-backend-secret` towards an HFS configured for the same realm.

With auth off the engine logs a warning at start: `PUT /api/transforms/{name}` writes code the engine then runs, so an open API must only be reachable from trusted clients.

### The engine's identity towards HFS

The engine calls HFS as itself, with an OAuth2 **client-credentials** grant at the identity provider's token endpoint (Keycloak first; the same providers HFS accepts). The token is cached and renewed a minute before it expires; a `401` or `403` from HFS discards it, the rows wait, and the next pass goes out with a fresh one. A token endpoint that cannot be reached or refuses the client is a transient delivery failure, so the data is kept and the outage alerts say why. A wrong endpoint or secret fails at startup.

| Variable | Default | Purpose |
|---|---|---|
| `HIE_FHIR_TOKEN_URL` | | The provider's token endpoint, e.g. `https://idp/realms/hfs/protocol/openid-connect/token` |
| `HIE_FHIR_CLIENT_ID` | | The engine's client |
| `HIE_FHIR_CLIENT_SECRET` | | Its secret |
| `HIE_FHIR_SCOPE` | | Scopes to ask for, when the provider needs them, e.g. `system/*.cruds` |

With none of them set the engine calls HFS anonymously and logs a warning at start, which is what the public sandbox does against the public HFS. Setting some but not all three is refused.
