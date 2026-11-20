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
| `DELETE /api/sources/{id}` | Stops the source and removes its file `sources/{id}.yaml`; its loads, records and alerts stay (see [Loads](#loads) for a source created again with the same id) |
| `POST /api/sources/{id}/pause` | Stops polling and delivery until resumed; body `{"by": "..."}` is kept with the time |
| `POST /api/sources/{id}/resume` | Resumes and passes at once |
| `POST /api/sources/{id}/check-now` | Passes now instead of at the next poll |
| `GET /api/sources/{id}/loads` (also `/files`) | Every load: the file, its method, records read, sent, rejected, in progress, how long it took, where it is archived, whether it is `failed` and the `source_name` it was recorded under ([below](#loads)) |
| `GET /api/loads?status=&source=&limit=50&cursor=` | The loads of every source, newest first, a page at a time, with the same fields; `status=failed` keeps the loads failing now ([below](#loads)) |
| `GET /api/loads/{id}` | One load, with the same fields, and its **failures grouped by reason**: kind, reason, count, first line, and up to three sample rows each |
| `POST /api/loads/{id}/retry` | Puts the load's rejected rows back at the start, all of them or those with one reason (`{"reason": "..."}`); the source passes at once |
| `GET /api/sources/{id}/stats?days=14` | What a source's page draws: today (files, records, sent, rejected, pending) and one row per day |
| `GET /api/transforms` | Every transform file: name, path, size, modified, and the sources that use it |
| `GET /api/transforms/{name}` | The transform's code |
| `PUT /api/transforms/{name}` | Saves the code (`{"code": "..."}`); the sources using it restart their Python worker. `201` when new |
| `POST /api/transforms/{name}/try` | Runs the transform on `{"records": [...]}` (up to 100) without loading: per record, the resources it became or the rejection reason. At most four tries run at once; `503` when one cannot start within 10 s |
| `GET /api/monitoring/status` | What the monitoring page opens on: `needs_you` (late, paused, or rejecting sources with the reason), `last_24h` (files, sent, rejected, alerts), every source |
| `GET /api/records?source=&status=&file_id=` | Records; `status` is `received`, `pending`, `sent` or `rejected` |
| `GET /api/records/{id}` | One record: file, line, original and corrected row, resources sent, HFS response |
| `POST /api/records/{id}/reprocess` | Runs a record again; body `{"raw": {...}}` replaces the row and keeps the original |
| `GET /api/alerts?source=&open=true` | Alerts, newest first, each with its rule, recipients, channels, and when and why it cleared; `open=true` lists only those not cleared |
| `GET /api/alert-rules?source=` | Every rule; `POST` creates one (`400` names what is wrong with it) |
| `GET` / `PUT` / `DELETE /api/alert-rules/{id}` | One rule: read, replace, remove |
| `GET /api/secrets` | Every stored credential, by name, without its value |
| `POST /api/secrets` | Stores a credential: `{"name", "kind": "aws", "value": {"access_key_id", "secret_access_key"}}`. `201` |
| `GET /api/secrets/{id}` | One credential, without its value |
| `PUT /api/secrets/{id}` | Replaces its value with a new revision: `{"value": {...}}`. Sources use it from their next pass |
| `DELETE /api/secrets/{id}` | Removes it. `409` with `sources` while a source is registered for it |
| `GET /metrics` | Prometheus text: records by status, last received time, paused flag, alerts by kind |
| `GET /health` | `{"status": "ok", "service": "hie", "version", "uptime_seconds", "started_at", "timestamp", "secrets"}`; `secrets` is `{"backend": "native"\|"none", "available", "key_id"}`, the key id being a one-way 16-hex fingerprint of the key in use, or `null` |

Errors answer `{"error": "<sentence>"}`. Where a client needs to tell cases apart they also carry a stable `code`, and sometimes more members: `invalid_definition` (a source definition that cannot run), `source_not_started` (`500`: the definition was saved, but its source could not be started; it starts with that definition at the next start of the engine), `store_busy` (`503` with `Retry-After: 1`: the store's lock was not free; try again), `invalid_query` (`400`: a query string that cannot be read, with `field` when one parameter's value is at fault; so far only `/api/loads` checks its query this way), and the codes of the credential routes below.

### Loads

A load is one file, object or zip entry a source took, and what became of its rows. `GET /api/loads` lists the loads of every source; `GET /api/sources/{id}/loads` and `GET /api/loads/{id}` answer the same fields:

```json
{"id": 812, "source_id": "lab-results", "source_name": "Lab results", "name": "labs-2026-11-20.csv",
 "status": "done", "failed": true, "records": 120, "sent": 117, "rejected": 3, "in_progress": 0,
 "received_at": "2026-11-20T08:00:01.120Z", "finished_at": "2026-11-20T08:00:04.870Z", "took_ms": 3750,
 "duplicate_of": null, "size": 8812, "error": null, "source_key": null, "archived": 1, "method": "rest"}
```

- **`status`** is how the engine stores the load: `processing` (rows still to transform or deliver), `done` (every row sent or rejected), `unreadable` (the file could not be read; `error` says why), `duplicate` (the same content was already taken; `duplicate_of` names that load; no rows), or `unpacked` (a zip, while its entries are taken; each entry is a load of its own named `drop.zip!/path`, and the zip becomes `done` with no rows at a later pass).
- **`failed`** is `true` while the load fails now: it is `unreadable`, or at least one of its rows is rejected (`rejected > 0`, any kind of rejection), whatever its `status`, so a load still `processing` fails as soon as one row is rejected. Rows put back by a Retry or a reprocess are not rejected any more: the load stops failing until a row is rejected again; a Retry limited to one reason leaves it failing while rows with another reason remain. Rows waiting for another delivery attempt (the FHIR server could not be reached) are not rejected. A duplicate does not fail because the load it repeats does, and a zip fails only when it could not be read: the failures of its entries are on the entries. A source that cannot be listed or read records no load: it is logged, and only a credential that cannot be used or, later, a silence rule raises an alert.
- **`source_name`** is the name the source had when the load was recorded, kept with the load: it stays after the source is deleted and does not follow a later rename. `null` when it was not recorded: loads recorded before this field existed, or by an instance of an earlier version sharing the store, and a name with a NUL character, which a PostgreSQL store cannot hold.
- **`method`** is the delivery method of the source now registered under `source_id` on the instance that answers, `null` when there is none.

`GET /api/loads` takes:

| Parameter | |
|---|---|
| `status` | One of `processing`, `unpacked`, `done`, `unreadable`, `duplicate`, or `failed` (the loads whose `failed` is `true`) |
| `source` | One source id, whether or not a source is registered under it now |
| `limit` | The loads per page, from 1 to 200; 50 by default |
| `cursor` | The previous page's `next_cursor` |

```
GET /api/loads?status=failed&limit=50
→ {"loads": [...], "next_cursor": "a1.812"}
GET /api/loads?status=failed&limit=50&cursor=a1.812
→ {"loads": [...], "next_cursor": null}
```

- **Order:** newest first by `id`, the order the engine recorded the loads. `received_at` normally agrees; between instances whose clocks differ, the id decides.
- **`next_cursor`** is set when more loads matched as the page was read, and `null` on the last page. It is opaque: pass it back as `cursor` with the same `status`, `source` and `limit`; it does not carry them. An empty `status=`, `source=` or `cursor=` is the same as leaving it out (an empty `limit=` is refused); other parameters are ignored, so there is no `since` here.
- **While a reader pages**, each page holds the matching loads with an id below the cursor, read in one statement, so its fields, counts and `next_cursor` agree with each other. Following the cursors never returns a load twice, and returns every load that existed when the walk began and still matched when its page was read. Loads recorded after the first page have higher ids and stay outside the walk; a new walk finds them (on PostgreSQL an id is taken before its row commits, so a load can also commit below the cursor and be found by a later page). With `status=failed`, or a status a load leaves (`processing` to `done`), a load can start or stop matching between two pages: one that starts matching above the cursor needs a new walk. A page after a cursor can be empty when the loads it would have held stopped matching: stop on `null`, not on an empty page.
- **Refused** with `400 invalid_query`: a `limit` outside 1 to 200 (`field: limit`), a `source` with a control character (`field: source`), another `status` (`field: status`), a malformed `cursor` (`field: cursor`), and values of the wrong type or given twice (`limit=abc`, `limit=1&limit=2`, without `field`). The sentence never repeats what was sent.

**Deleted sources and reused ids.** A load stays in the store when its source is deleted, and `GET /api/loads` keeps listing it under the `source_id` and `source_name` it was recorded with. A source id is the only key the engine has for a source's history, and everything kept under it stays when the source is deleted. A source created later with the same id takes it over:

- its loads: they appear in the new source's list, under `source=`, and in its stats and monitoring figures;
- its pause state, its rules and its open alerts: a silence rule counts from the earlier source's last load and keeps its threshold, so the new source may be reported silent at once, and its `expect_every` does not replace that rule;
- its read cursors, when the new source reads the same column or resource types: a database or FHIR-server source then continues after the earlier source's cursor rather than from its own start;
- its unfinished rows: rows still received, and rows put back by a Retry of an earlier load, are transformed by the new source's transform and delivered to its destination; rows already transformed and waiting for delivery keep the resources the earlier transform made and are delivered as they are; submissions still open on the `$bulk-submit` path are followed only if the new source also delivers that way;
- its files, for deduplication: a file or object whose content the earlier source took is recorded as a `duplicate` of the earlier load by a source that moves what it reads, skipped without a load by a `read_only` source (which knows a file it took by its name and size, so a different file with the same name and size is skipped too), and skipped while the earlier object still waits to be fetched on the `$bulk-submit` path; an entry of a new zip whose content was taken before is recorded as a `duplicate` by either kind.

To keep two histories apart, give the new source a new id. `source_name` and `received_at` tell the earlier loads apart when the name changed. `GET /api/sources` says whether a source is registered under an id now, on the instance that answers; it cannot say whether that is the definition that recorded a given load. `DELETE` removes `sources/{id}.yaml` only: a definition kept under another file name in the sources directory starts again with the engine.

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
| `hie/read` | Every `GET` except `/api/secrets`: sources, loads, records, transforms, alerts, rules, monitoring |
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
