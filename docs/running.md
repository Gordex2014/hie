# Running HIE

How to run the engine: the binary, the Docker image, the environment variables, and the store shared by several instances. Back to the [README](../README.md).

## Run it

You need:
- Rust 1.90 or later;
- Python 3.9 or later (`py` on Windows, `python3` elsewhere; set `python:` in `hie.yaml` to change it, to a virtual environment's interpreter for instance). The floor is the `limited-process@1` launcher's, not HIE's: an older interpreter leaves every source's records received, with "the launcher could not set up the worker: python_version: Python 3.9 or later is required" in the log, until `python:` points at a newer one. On Linux and macOS it must be the interpreter itself, not a script that starts Python as a child: the engine refuses a worker it did not start directly. On Windows the engine asks the configured interpreter which `python.exe` it runs and starts that one, so the `py` launcher works;
- HFS running at the URL in `hie.yaml` (default `http://localhost:8080`), with its search parameters loaded (`HFS_DATA_DIR`): a row delivered by business identifier is a conditional `PUT Type?identifier=...`, which HFS refuses with `400` when it does not know `identifier` for that type, and the engine then rejects the row as a data error.

```sh
cargo run -- check          # validate hie.yaml and sources/
cargo run                   # run every source, serve the API on 127.0.0.1:8090
cp examples/data/labs.csv inbox/lab-results/
curl localhost:8090/api/sources
```

### In Docker

Each release publishes a public Linux x86_64 image to `ghcr.io/heliossoftware/hie` (tags `:{version}`, `:{major.minor}`, `:latest`; see [Release and deploy](development.md#release-and-deploy)). The image holds the engine, Python 3, `hie_runtime/`, and the example `transforms/` and `sources/` under `/hie`.

The `hie.yaml` baked into the image listens on `0.0.0.0:8090`, so `docker run -p 8090:8090 ghcr.io/heliossoftware/hie:latest` serves the API, but it expects HFS at `localhost:8080`, which inside a container is the container itself. To deliver anywhere, mount your own config. To run HIE and HFS side by side on one Docker network:

```sh
mkdir -p inbox/lab-results data
cat > hie.docker.yaml <<'EOF'
fhir_base_url: http://hfs:8080   # HFS as this container sees it
public_url: http://hie:8090      # HIE as HFS sees it (for $bulk-submit)
database: data/hie.db
listen: 0.0.0.0:8090             # all interfaces, so -p can reach it
sources_dir: sources
transforms_dir: transforms
EOF

docker network create hie-net
docker run -d --name hfs --network hie-net -p 8080:8080 ghcr.io/heliossoftware/hfs:latest
docker run -d --name hie --network hie-net -p 8090:8090 \
  -v $PWD/hie.docker.yaml:/hie/hie.yaml:ro \
  -v $PWD/inbox:/hie/inbox \
  -v $PWD/data:/hie/data \
  ghcr.io/heliossoftware/hie:latest

curl localhost:8090/health                        # {"status":"ok","service":"hie","version":...}
cp examples/data/labs.csv inbox/lab-results/      # picked up within 5 s
curl localhost:8090/api/sources/lab-results       # records.sent: 3
curl 'localhost:8080/Observation?_summary=count'  # total: 3
```

If HFS runs on the host instead, drop `--network`, use `fhir_base_url: http://host.docker.internal:8080`, and add `--add-host=host.docker.internal:host-gateway` on Linux.

| Mount | Holds |
|---|---|
| `/hie/hie.yaml` | the engine configuration (required, see above) |
| `/hie/inbox` | the folders the example folder sources watch; processed files move to `processed/` |
| `/hie/data` | the SQLite store: records, delivery state, alerts, stored credentials, and the secret key that seals them (`hie-secrets.key`) unless one is given (see [Credentials and the secret key](#credentials-and-the-secret-key)). Without it, history is lost with the container |
| `/hie/sources`, `/hie/transforms` | optional: your own sources and transforms, and anything created through the API, kept across upgrades |

Other commands: `docker run --rm ghcr.io/heliossoftware/hie:latest --version`, and `... check` with the same mounts to validate the config and sources. The container runs as root, so files it writes to the mounts are owned by root. Pass the `HIE_AUTH_*` variables (see [Authentication](api.md#authentication)) with `-e`. S3 credentials are not passed to the container: they are stored through `/api/secrets` and named by each source. To keep the secret key off the data volume, mount a file holding it read-only and name it: `-v $PWD/hie_secret_key:/run/secrets/hie_secret_key:ro -e HIE_SECRET_KEY_FILE=/run/secrets/hie_secret_key`. With Docker Compose, a `secrets:` entry does the same:

```yaml
services:
  hie:
    image: ghcr.io/heliossoftware/hie:latest
    environment:
      HIE_SECRET_KEY_FILE: /run/secrets/hie_secret_key
    secrets: [hie_secret_key]
secrets:
  hie_secret_key:
    file: ./hie_secret_key
```

To build the image yourself: `docker build -t hie .`

| Variable | Default | Purpose |
|---|---|---|
| `HIE_CONFIG` | `hie.yaml` | Engine configuration file |
| `HIE_RUNTIME_DIR` | `hie_runtime` next to the config | Location of the Python worker |
| `HIE_LOG` | `info` | Log filter (`debug`, `hie=debug`, ...). Whatever it says, `tokio_postgres` never logs below `info`: at `debug` and `trace` it would log the values bound to every statement (record data, status URLs, tokens) |
| `HIE_DATABASE_URL` | | The store, overriding `database:` in `hie.yaml`: a `postgres://user:password@host/db` URL. Set it here so the password is not in the file |
| `HIE_INSTANCE_ID` | host, pid and a random tail | This instance's name in the leases, when several share one store; also the owner of its credential registrations and of the alerts of the source files it skips (otherwise an id kept in `sources/.hie-instance-id`) |
| `HIE_ALERT_WEBHOOK_URL` | `alerts.webhook_url` in `hie.yaml` | Where [alert events](sources.md#the-alert-webhook) are posted, `http` or `https`. A chat webhook's URL is itself a credential: set it here rather than in the file |
| `HIE_ALERT_WEBHOOK_SECRET` | `alerts.webhook_secret` in `hie.yaml` | The secret that signs alert events (`X-HIE-Signature`, over `X-HIE-Timestamp` and the body); unsigned without one. Leading and trailing whitespace is ignored |
| `HIE_TERMINOLOGY_SERVER` | `terminology_url` in `hie.yaml` | The HTS transforms may ask through `lookup` and `translate` |
| `HIE_TERMINOLOGY_TOKEN` | | A bearer token for that HTS, when it requires one |
| `HIE_SECRET_KEY` | | The secret key that seals stored credentials: 32 random bytes in base64, e.g. from `openssl rand -base64 32`. See [Credentials and the secret key](#credentials-and-the-secret-key) |
| `HIE_SECRET_KEY_FILE` | | A file holding that key, absolute or relative to the working directory; `secrets.key_file` in `hie.yaml` names one relative to `hie.yaml` |
| `AWS_REGION`, `AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL` (or `AWS_ENDPOINT`), `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` | | Where an S3 source's bucket is, when the source sets no `region` or `endpoint`. AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, profiles, web identity, container or instance credentials) are **not read**: S3 sources and their queues sign with the stored credential their definition names, and a start with any of them set says so in its log |

### The transform worker

Each source's transform runs in a Python worker under operating-system limits (see [Where transforms run](transforms.md#where-transforms-run)). The `worker:` block in `hie.yaml` sets them for every worker; all of it is optional:

```yaml
worker:
  memory_mib: 2048        # address space per process (RLIMIT_AS on Linux)
  file_mib: 256           # largest file a transform may write
  open_files: 256         # open file descriptors per process
  request_seconds: 60     # per record, from before it is written until its reply
  startup_seconds: 10     # starting the worker until it is ready
```

Every value must be at least 1. A transform that imports a heavy library may need a larger `memory_mib`. A reply line is at most 32 MiB. Each worker process gets a fresh run directory under the system temporary directory, deleted when it ends.

### The destination probe

The engine reports whether the FHIR server answers (`destination` in `GET /api/monitoring/status`, see [The destination](api.md#the-destination)) from its deliveries, and probes `{fhir_base_url}/metadata` once nothing has reached the server for a while, so the report does not go stale when nothing arrives:

```yaml
destination:
  probe_seconds: 60   # seconds of quiet before a probe; 0 turns the probe off
```

`probe_seconds` is at most 86400. The probe is a plain `GET` with a 10-second limit and no token: a running engine keeps probing while its token endpoint is down, and an answer such as `401` still means the server answers. With the probe off, the members of `destination` stay `null` (and `consecutive_failures` `0`) until the first delivery.

### Retention

The engine keeps an event log (see [Events](api.md#events)), its alerts and the checks `check-now` records (see [Checks](api.md#checks)) for a time set by the `retention:` block in `hie.yaml`:

```yaml
retention:
  history: 30d   # events, stored alerts and checks; from 1d to 3650d
```

Older events, alerts and checks are purged at start and every hour after, in chunks of 5,000 rows; a check counts from when it was asked for, answered or not. The alerts the engine still tracks stay whatever their age: open alerts of an enabled rule, open `credential` and `definition` alerts, and open outage (`delivery`) alerts without a rule (see [Alerts and retention](api.md#alerts-and-retention)). An open alert of a rule whose source was deleted while it was open stays until the rule is turned off, deleted or changed in what it watches, which clears it. Records, loads and submissions are not purged: `history` covers what the engine logs about them, not the data. Instances sharing one store each purge with their own setting, so the shortest one applies. A value outside 1d to 3650d, or a key other than `history`, is refused at start and by `hie check`.

The first start after the upgrade that brings the event log builds new indexes on `records` (one on SQLite, two on PostgreSQL), and the one that brings checks an index on `files`; each blocks writes to its table while it runs: on a large store, plan for it. Instances starting together on one PostgreSQL schema build it in turn.

### Tagging delivered resources with their source

A consumer that reads HFS can learn which engine source delivered a resource when the engine tags it. Off by default; for every source, in `hie.yaml`:

```yaml
delivery:
  stamp_source: true
```

The block takes only `stamp_source`; any other key in it stops the engine at start (a source's own delivery settings live in its definition, and `stamp_source` there is refused). Every instance sharing a store should use the same value. A change needs a restart.

With it on, every resource the engine sends carries `meta.tag` `{"system": "urn:helios:hie:source", "code": "<source id>"}`, added as the resource leaves, never stored with the record:

- **Covered:** the REST path (each resource of the batch Bundle) and `$bulk-submit` with `bulk_from: transformed` (each line of the NDJSON the engine writes, served from `bulk_dir` or put in the source's store).
- **Not covered:** `$bulk-submit` with `bulk_from: source_objects` (or `auto` resolving to it). HFS fetches those objects untouched, so the engine cannot tag them; tagging them needs an option on the server side. Such a source reports `reason: "direct_bulk"`, and each start of it logs a warning naming it, since `auto` picks this path for NDJSON and Bundle files with the passthrough transform (`format: ndjson` or `fhir_bundle`; `format: fhir` takes `transformed`).
- The resource's other tags keep their order, and `meta.source`, `profile`, `security` and the rest of `meta` are kept; the engine's tag comes last, once. A tag of the same system that the transform set is replaced, whatever its code, so a resource carries one source tag. Sending a record again sends the same JSON. A resource whose `meta` is not an object, or whose `meta.tag` is not an array, cannot carry the tag: its record is rejected (`validation`) and the rest go on.
- Try never writes, and shows the resources as the transform returned them, without the engine's tag.
- With it off, the engine leaves tags as the transform wrote them: a tag of that system then proves nothing.
- Turning it on changes, once, every resource delivered before without the engine's tag as its last tag: the next delivery of an unchanged row sends a new `meta.tag`, so HFS writes a new version (HFS's bulk ingestion, which skips unchanged resources, compares `meta` tags included). Turning it off, the engine no longer adds or replaces the tag, and sends resources as the transform returned them; whether the server keeps an earlier tag on update is the server's.

**What was delivered without the tag.** Every source read (`GET /api/sources`, `GET /api/sources/{id}`, each source of `GET /api/monitoring/status`) carries `stamping`:

```json
"stamping": {"active": true, "reason": null, "untagged": "until", "untagged_until": "2026-11-21T09:14:02.120+00:00"}
```

- `active`: what this instance prepares for this source from now on carries the tag. A submission already written keeps what it was written with: one written untagged before the setting was turned on goes out untagged, and keeps `untagged` at `open` until it ends. `reason` says why not: `"off"` (the setting is off on this instance) or `"direct_bulk"`.
- `untagged`: `"none"` when the store records no delivery of this source that could have reached HFS without the tag; `"until"` when deliveries up to `untagged_until` may lack it and every delivery recorded after it carried it; `"open"` when one without it is still in progress or how it ended cannot be known, so there is no date yet (`untagged_until` then holds the last known bound, which may lie a few minutes ahead, or `null`).
- A consumer can say, for `active` with `none`, that nothing of the source is untagged; for `active` with `until`, that deliveries through that date may lack the tag; otherwise, that the source's data may lack it.

How it is kept: before each untagged REST request leaves, once the engine holds its token for HFS, it stores a bound of that instant plus 250 seconds: ten for the store write, counted from when the engine holds its store connection (a wait for that connection, while another source writes a large file, does not count; a wait for the database's own locks does), until the request leaves (a slower write is taken again from a fresh instant, and a second slow one sends nothing and fails the pass) and twice the request timeout (the request, and a server still writing after the engine stopped waiting). The bound only moves later. A `$bulk-submit` submission without the tag (every direct one, and transformed ones written with the setting off) keeps the source `open` from when it is written until it ends, and bounds it when HFS reports it done. One that failed keeps the source `open`: a refusal may answer the repeat of a kick-off HFS took, or cross another instance's kick-off during a handover, so HFS may still be importing it. So does a submission left unpolled because its source was deleted or replaced by a REST definition, until a bulk source with the same id polls it to its end. The facts are kept by source id and survive a restart, a change of the setting and the deletion of the source.

On a store from an earlier version, every delivery was untagged: the first start of this version records that instant (the `stamping_tracked` row of the `store_meta` table) and gives each source that has a load the bound of that instant plus the margin. Failed submissions last touched before it are inside that bound; one still to send or running is not, since this version may send it, and keeps its source `open` until it ends. That holds when every older instance on the store was stopped before the first instance of this version started, and HFS runs no import they started. After a rolling upgrade, or a downgrade and a new upgrade, or to close a source left `open` by a failed submission once HFS is known to run nothing of it: delete that row and restart. The reset is store-wide: it moves every source's bound to the new instant, and every failed submission before it stops counting as open, so take it only once HFS is known to run nothing from a failed submission of **any** source; there is no reset for one source alone. The bounds are taken again, later, and the failed submissions before the new instant are inside them. An unpolled submission (status `kickoff` or `running`) is closed the same way once its `status` is set to `failed` by hand.

Out of the engine's knowledge: a server still writing a REST request more than four minutes after it was sent; another tool writing the same resources; a store that was wiped or replaced; earlier versions of a resource in HFS's history, which keep what they were sent with. A resource two sources both write carries what the last write sent. Instants come from each instance's clock, so instances on one store need synchronized clocks, and comparing with HFS's `meta.lastUpdated` (HFS's clock) needs a margin. While a source is replaced, its previous runner may finish its current step after the read already shows the new definition.

### Credentials and the secret key

S3 sources sign with credentials stored in the engine (`/api/secrets`, see [Secrets](api.md#secrets)), sealed in its database with XChaCha20-Poly1305 under a key derived from one **secret key**. Only metadata is ever answered; values never leave the engine except to sign requests.

The secret key comes from exactly one place:
- `HIE_SECRET_KEY`: the key itself, base64 of 32 bytes (43 or 44 characters, either alphabet, whitespace around it ignored);
- `HIE_SECRET_KEY_FILE`, or `secrets.key_file` in `hie.yaml`: a file holding it. Both may name the same file. The file may be a symbolic link with any owner and mode, as Kubernetes and Docker secret mounts are; protect it yourself. It must be a regular file;
- with none of them and a SQLite file store, `hie-secrets.key` next to the database (`data/hie-secrets.key`), created at the first start with mode `0600`, owner-only. It must stay a regular file owned by the engine's user that no one else can read, and is never replaced.

The key next to the database sits on the same volume: a copy of that volume, or a backup of it, holds both the credentials and their key. The engine says so in its log. Keep a copy of the key elsewhere, and prefer `HIE_SECRET_KEY` or `HIE_SECRET_KEY_FILE` to keep it off that volume. A PostgreSQL or in-memory store has no place for a key file and needs one of the variables; every instance on a shared store uses the same key.

The first start with a key **seals** the store: it writes a key check. Every later start proves its key against it, and is refused, before serving, when the store is sealed and the key is missing, wrong or malformed. The refusal ends with both key ids (one-way fingerprints; `/health` shows the one in use):

```
... (store key 3f9c2a7d1e4b6a80, this instance's key 91b0c3d4e5f60718)
```

The second id may also be `none`, `malformed`, `unavailable`, `ambiguous` or `unsafe`. The engine never makes a new key for a sealed store and never falls back to anything else. Two cases are refused even before the first credential: a key set in more than one place (`HIE_SECRET_KEY` with a key file, or two different key files), and an automatic key file that is a symbolic link, not a regular file, someone else's, or readable by others.

A store that is not sealed yet and has no usable key (PostgreSQL without a key, a malformed `HIE_SECRET_KEY`, or an automatic key file the engine could not create, on a volume without hard links or a directory it cannot write for instance) still starts and logs why; when the automatic key file could not be created, that error names the file and the system error. `/api/secrets`, and creating, replacing or testing a source that names a credential, answer `503 secrets_unavailable` with a fixed sentence for the cause and no path; sources that name a credential raise a `credential` alert with the same sentence. The engine reads its key only at start, and creates the automatic key file only at start: once the key is fixed, restart it.

There is no key rotation. A lost key loses the credentials: delete the rows of `secrets`, `secret_keys` and `secret_refs`, start with a new key, and store the credentials again. A replaced value is no longer returned, but may remain in the SQLite WAL and free pages, PostgreSQL's dead rows until vacuum, and backups, all sealed under the same key.

A source's **registration** for its credential is what makes `DELETE /api/secrets/{id}` answer `409`. Each instance owns its registrations: its `HIE_INSTANCE_ID`, or an id kept in `sources/.hie-instance-id`, made at the first start. Instances that share one sources directory must each set `HIE_INSTANCE_ID`. Changing an instance's id leaves its old registrations behind; restore the id, or remove them with `DELETE FROM secret_refs WHERE instance_id = '<old id>'`. At start, each instance makes its registrations match the definitions it loaded.

If two instances make their first start at the same time and one seals the store with its key while the other creates the automatic key file, the second is refused, and its refusal names the key file it created: that file unlocks nothing; remove it, then start with the store's key. The engine never removes a key file itself.

If the engine stops while it creates the automatic key file, a `data/.hie-secrets.key.*.tmp` copy of the key may remain; delete it while no engine is starting.

A build without the `secrets-native` feature has no secret store: it skips source files that name a credential, with an error and a `definition` alert naming each file, and starts with the rest (`hie check` prints what it skipped). The alert is the instance's that skipped the file (its `HIE_INSTANCE_ID`, or the id in `sources/.hie-instance-id`, which such a build makes too when it skips a file or finds open an alert some instance raised), one per source id and instance: the first start of that instance that skips no file of the source (the file fixed, removed, or loaded by a build with a store) clears it with the reason `<id>: its definition is no longer skipped`. Another instance on the store leaves it open, and raises its own while it skips a file of that source too. An alert an earlier version raised names no instance, so no start clears it (the instance it describes may still skip the file): an instance that skips a file of that source raises its own, and retention purges the earlier one by age, as other open alerts without a rule. Without a stable id (no `HIE_INSTANCE_ID` and a sources directory it cannot write, which the engine warns about), each start is a new instance: it raises an alert naming no instance, only for a source with none open, and no start clears it, as before this version. `secrets-aws`, a store on AWS Secrets Manager, is reserved but not built.

### The store, and more than one instance

`database:` in `hie.yaml` is where the engine keeps files, records, events, alerts, rules, checks, cursors and leases: a SQLite file (the default, `data/hie.db`, no setup) or a `postgres://` URL. The SQL is the same on both; the test suite runs on either (`HIE_TEST_DATABASE_URL=postgres://... cargo test` runs every test on PostgreSQL, each in a schema of its own).

With PostgreSQL, several instances may share one store, with the same `alerts:` settings: any of them sends any waiting alert event. A source is run by the instance that holds its **lease**: each pass takes or renews it for three poll intervals (at least 30 s); another instance that finds it held stands by; when the holder dies, its lease lapses and the next pass elsewhere takes the source over, with the cursors, pause state and rules it left in the store. Whether a source was created, which decides its default rule, is recorded in the store too, so it is the same on every instance. A clean stop releases the lease at once. `GET /api/sources/{id}` shows `lease` (owner, until, whether it is this instance's) and `instance`, and reports a source held elsewhere as `standby`. `POST .../check-now` makes a pass only on the instance it was sent to, so send it to the holder; the check it records is in the store, and is answered by the holder's next scan wherever it was sent.

A PostgreSQL store must use the `UTF8` encoding: names, files and rows are stored as text. The engine creates its tables and indexes, and adds new columns, when it starts. An upgrade can add an index over a large table: the first start of a version that lists loads across sources builds two partial indexes over `records`, each read in full, and two over `files`, each in a transaction of its own, during which SQLite holds its write lock and PostgreSQL blocks writes to the table being indexed. On a large store, plan for that pause. Instances that start together on one PostgreSQL store build the schema in turn, so the others wait for the first to finish. On PostgreSQL every index, old or new, is checked or built in a transaction of its own after the tables: `CREATE INDEX IF NOT EXISTS` locks its table even when the index exists, so a start that waits for another instance's write to one table holds no other table meanwhile, and a rolling start cannot deadlock with a running instance. Each start also checks every added column with `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, which takes an exclusive lock on its table even when the column exists: a start waits behind a running instance's longest transaction on that table, and that instance's next statements on it wait for the start.
