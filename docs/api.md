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
| `GET /api/transforms` | Every transform file: name, path, size, modified, and the sources that use it |
| `GET /api/transforms/{name}` | The transform's code |
| `PUT /api/transforms/{name}` | Saves the code (`{"code": "..."}`); the sources using it restart their Python worker. `201` when new |
| `POST /api/transforms/{name}/try` | Runs the transform on `{"records": [...]}` (up to 100) without loading: per record, the resources it became or the rejection reason. At most four tries run at once; `503` when one cannot start within 10 s |
| `GET /api/monitoring/status` | What the monitoring page opens on: `needs_you` (late, paused, or rejecting sources with the reason), `last_24h` (files, sent, rejected, alerts), `destination` (whether the FHIR server answers, see [The destination](#the-destination)), every source with its `last_alert` (`kind`, `message`, `created_at`, `cleared_at`) |
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

Errors answer `{"error": "<sentence>"}`. Where a client needs to tell cases apart they also carry a stable `code`, and sometimes more members: `invalid_definition` (a source definition that cannot run), `source_not_started` (`500`: the definition was saved, but its source could not be started; it starts with that definition at the next start of the engine), `store_busy` (`503` with `Retry-After: 1`: the store's lock was not free; try again), and the codes of the credential routes below.

### The destination

`destination` in `GET /api/monitoring/status` says whether the FHIR server at `fhir_base_url` answers, as this engine process saw it:

```json
"destination": {
  "reachable": false,
  "since": "2026-11-20T10:00:00.123+00:00",
  "last_error": "could not connect to the FHIR server",
  "consecutive_failures": 7,
  "observed_at": "2026-11-20T10:06:00.456+00:00"
}
```

| Member | Meaning |
|---|---|
| `reachable` | `true`, `false`, or `null` while nothing has been observed since the engine started |
| `since` | When `reachable` took its current value: the first failure of the current outage, or the first answer after one |
| `last_error` | While unreachable, why, as one of these fixed sentences; `null` otherwise: `could not connect to the FHIR server`, `the FHIR server did not answer in time`, `the connection to the FHIR server was lost`, `the FHIR server answered HTTP 502` (or `503`, `504`). Never a URL, a host or a library's error text; the engine log has the detail |
| `consecutive_failures` | Failed observations in a row, every source's deliveries and the probe together, so several failing sources add up quickly; `0` while reachable. It is not a source's attempt count and is not compared with `alert_after_attempts` |
| `observed_at` | The latest observation that counted: "last checked" |

Timestamps are RFC 3339 in UTC, as everywhere in the API.

**What is observed.** Each request a delivery sends under the base (a batch Bundle, a `$bulk-submit` kick-off or status request), and the **idle probe**: once nothing has reached the server for `destination.probe_seconds` (60 by default; see [running](running.md#the-destination-probe)), a `GET {base}/metadata`, anonymous (no token, and no user information from the base URL), without following redirects. Requests that deliveries send push the probe back, so an engine that keeps sending does not probe; one that is only waiting on a long bulk submission still does. Bulk status pages and receipt files, whose URLs HFS chooses, are not observed; neither are the token endpoint, the terminology server, sources that read another FHIR server, or the alert webhook. A request the server redirects counts as the server's.

**What counts.** No connection, no answer in time, a connection lost before the answer, or an answer of `502`, `503` or `504` is a failure. Any other answer, `4xx` (`401`, `403`, `429` included) and `500` included, means the server answers. So `reachable` says the server's HTTP route answers, not that delivery works: an HFS answering `500` to every batch, a batch whose every entry is to be retried, a token the server refuses, or a token endpoint that is down (which records nothing) all leave it `true`. Each source's records, loads and `delivery` alerts say what failed.

**Order.** A request that started before the latest observation does not overwrite it, and a probe gives way to a delivery that started after it. While deliveries see `502` to `504`, a probe that gets an answer does not end the outage (the server may serve `/metadata` and still be unable to store); a delivery's answer does, or the probe's once no delivery has seen `502` to `504` for ten minutes. A source retrying less often than that (a `max_backoff_ms` above ten minutes) can see the state turn `true` between its retries. In that state `observed_at` advances with the probe's failures and the deliveries' retries, which back off up to `max_backoff_ms`.

**Per process.** The state is kept in memory: it starts unknown at every start of the engine (`since` is not kept across a restart), and with several instances on one store each answers with what it observed itself. An instance that holds no source answers from its probe only.

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
