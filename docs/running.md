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
| `HIE_INSTANCE_ID` | host, pid and a random tail | This instance's name in the leases, when several share one store; also the owner of its credential registrations (otherwise an id kept in `sources/.hie-instance-id`) |
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

A store that is not sealed yet and has no usable key (PostgreSQL without a key, or a malformed `HIE_SECRET_KEY`) still starts: `/api/secrets` answers `503` with the reason, and sources that name a credential raise a `credential` alert. The engine reads its key only at start: once the key is fixed, restart it.

There is no key rotation. A lost key loses the credentials: delete the rows of `secrets`, `secret_keys` and `secret_refs`, start with a new key, and store the credentials again. A replaced value is no longer returned, but may remain in the SQLite WAL and free pages, PostgreSQL's dead rows until vacuum, and backups, all sealed under the same key.

A source's **registration** for its credential is what makes `DELETE /api/secrets/{id}` answer `409`. Each instance owns its registrations: its `HIE_INSTANCE_ID`, or an id kept in `sources/.hie-instance-id`, made at the first start. Instances that share one sources directory must each set `HIE_INSTANCE_ID`. Changing an instance's id leaves its old registrations behind; restore the id, or remove them with `DELETE FROM secret_refs WHERE instance_id = '<old id>'`. At start, each instance makes its registrations match the definitions it loaded.

If two instances make their first start at the same time and one seals the store with its key while the other creates the automatic key file, the second is refused, and its refusal names the key file it created: that file unlocks nothing; remove it, then start with the store's key. The engine never removes a key file itself.

If the engine stops while it creates the automatic key file, a `data/.hie-secrets.key.*.tmp` copy of the key may remain; delete it while no engine is starting.

A build without the `secrets-native` feature has no secret store: it skips source files that name a credential, with an error and a `definition` alert naming each file, and starts with the rest (`hie check` prints what it skipped). `secrets-aws`, a store on AWS Secrets Manager, is reserved but not built.

### The store, and more than one instance

`database:` in `hie.yaml` is where the engine keeps files, records, alerts, rules, cursors and leases: a SQLite file (the default, `data/hie.db`, no setup) or a `postgres://` URL. The SQL is the same on both; the test suite runs on either (`HIE_TEST_DATABASE_URL=postgres://... cargo test` runs every test on PostgreSQL, each in a schema of its own).

With PostgreSQL, several instances may share one store. A source is run by the instance that holds its **lease**: each pass takes or renews it for three poll intervals (at least 30 s); another instance that finds it held stands by; when the holder dies, its lease lapses and the next pass elsewhere takes the source over, with the cursors, pause state and rules it left in the store. A clean stop releases the lease at once. `GET /api/sources/{id}` shows `lease` (owner, until, whether it is this instance's) and `instance`, and reports a source held elsewhere as `standby`. `POST .../check-now` reaches the instance it was sent to, so send it to the holder.
