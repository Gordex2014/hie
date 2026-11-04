# Running HIE

How to run the engine: the binary, the Docker image, the environment variables, and the store shared by several instances. Back to the [README](../README.md).

## Run it

You need:
- Rust 1.90 or later;
- Python 3.8 or later (`py` on Windows, `python3` elsewhere; set `python:` in `hie.yaml` to change it);
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
| `/hie/data` | the SQLite store: records, delivery state, alerts. Without it, history is lost with the container |
| `/hie/sources`, `/hie/transforms` | optional: your own sources and transforms, and anything created through the API, kept across upgrades |

Other commands: `docker run --rm ghcr.io/heliossoftware/hie:latest --version`, and `... check` with the same mounts to validate the config and sources. The container runs as root, so files it writes to the mounts are owned by root. Pass the `HIE_AUTH_*` variables (see [Authentication](api.md#authentication)) and AWS credentials with `-e`.

To build the image yourself: `docker build -t hie .`

| Variable | Default | Purpose |
|---|---|---|
| `HIE_CONFIG` | `hie.yaml` | Engine configuration file |
| `HIE_RUNTIME_DIR` | `hie_runtime` next to the config | Location of the Python worker |
| `HIE_LOG` | `info` | Log filter (`debug`, `hie=debug`, ...). The database drivers' own events (`tiberius`, `sqlx`) are always off: they can carry what a server echoed or a statement's values; HIE logs its own sanitized account instead |
| `HIE_DATABASE_URL` | | The store, overriding `database:` in `hie.yaml`: a `postgres://user:password@host/db` URL. Set it here so the password is not in the file |
| `HIE_INSTANCE_ID` | host, pid and a random tail | This instance's name in the leases, when several share one store |
| `HIE_TERMINOLOGY_SERVER` | `terminology_url` in `hie.yaml` | The HTS transforms may ask through `lookup` and `translate` |
| `HIE_TERMINOLOGY_TOKEN` | | A bearer token for that HTS, when it requires one |
| `HIE_RUNTIME_URL` | `runtime.url` in `hie.yaml` | The shared Python runtime transforms run at, instead of a local process (see [Transforms](transforms.md#transforms)) |
| `HIE_RUNTIME_TOKEN` | | The consumer's bearer token for that runtime (`runtime.token_env` names another variable) |
| `HIE_FHIR_TENANT` | `destination.tenant` in `hie.yaml` | The destination partition HIE delivers into (see [The destination's partition](#the-destinations-partition)); blank leaves the file's setting |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` | | Credentials for S3 sources, as for any AWS tool (profiles work too) |

### The destination's partition

A destination that keeps tenants apart by a request header (HFS reads `X-Tenant-ID`) stores what arrives without one in its default partition. To deliver into a tenant's partition, name it in `hie.yaml`:

```yaml
destination:
  tenant: my-organization       # or HIE_FHIR_TENANT; unset or blank: no header
  tenant_header: X-Tenant-ID    # the default
```

With a tenant set, every request HIE makes to the destination carries `<tenant_header>: <tenant>`: REST batches and transactions (with their conditional updates), `$validate`, the `$bulk-submit` kick-off and status requests, status polls and receipt files, and `/metadata` for `GET /api/destination`. A status or receipt URL on another origin than `fhir_base_url` (a pre-signed object store, say) does not get it; nor do source FHIR servers, the Python runtime, the terminology server or the token endpoint. A `tenant_header` that is not a valid HTTP header name, or a tenant that is not a valid header value, stops the engine at start (and `hie check`). `GET /api/destination` reports both.

### The store, and more than one instance

`database:` in `hie.yaml` is where the engine keeps files, records, alerts, rules, cursors and leases: a SQLite file (the default, `data/hie.db`, no setup) or a `postgres://` URL. The SQL is the same on both; the test suite runs on either (`HIE_TEST_DATABASE_URL=postgres://... cargo test` runs every test on PostgreSQL, each in a schema of its own).

With PostgreSQL, several instances may share one store. A source is run by the instance that holds its **lease**: each pass takes or renews it for three poll intervals (at least 30 s); another instance that finds it held stands by; when the holder dies, its lease lapses and the next pass elsewhere takes the source over, with the cursors, pause state and rules it left in the store. A clean stop releases the lease at once. `GET /api/sources/{id}` shows `lease` (owner, until, whether it is this instance's) and `instance`, and reports a source held elsewhere as `standby`. `POST .../check-now` reaches the instance it was sent to, so send it to the holder.
