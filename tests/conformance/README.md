# Conformance corpus

Request/response pairs and behavioural sequences for HIE's HTTP contract ([`docs/api.md`](../../docs/api.md)). They are written generically, with ids and times normalized by patterns, so a client can replay the HTTP pairs against its own adapter and HIE runs every case against itself.

```sh
HIE_TEST_PYTHON=python3 cargo test --test conformance
HIE_CONFORMANCE_ONLY=sequences/ HIE_CONFORMANCE_VERBOSE=1 cargo test --test conformance   # a subset, with the reasons known-red cases fail
```

## Files

| Path | What |
|---|---|
| `cases/<area>/<name>.json` | One case. Its id is the path without `cases/` and `.json`, such as `sequences/lost-try-not-replayed` |
| `known-red.txt` | Cases expected to fail until the front named beside each implements them |
| `endpoints.txt` | Every route of the contract; some case must send a request to each |
| `SHA256SUMS` | SHA-256 of every other file here; consumers that vendor the corpus check it |

The runner (`tests/conformance.rs`) fails on any failure of an unlisted case, on a listed case that passes (take it off the list), on a route no case requests (a request counts for the most specific route its method and path match), and on a checksum that does not match. Each case runs on a runtime of its own, which is shut down when the case ends, so nothing it started outlives it. A case whose requirement is missing is skipped and counted as such. After editing any file here:

```sh
cd tests/conformance && find . -type f ! -name SHA256SUMS | sed 's|^\./||' | LC_ALL=C sort | xargs sha256sum > SHA256SUMS
```

## A case

```json
{
  "id": "area/name",
  "title": "One sentence: what the case proves",
  "fixtures": {...},
  "forbid": ["values that must never appear"],
  "steps": [...]
}
```

### Fixtures

Each case gets an engine of its own in an empty directory, its store in `data/hie.db`, calling a stand-in destination.

| Member | Sets up |
|---|---|
| `requires` | `["mssql"]`: the case runs only when `HIE_TEST_MSSQL_URL` names a SQL Server holding the `lab_results` table of `sources/lab-database.yaml.example` with at least three rows, and is skipped otherwise. It provides `{{mssql:host}}`, `{{mssql:port}}`, `{{mssql:database}}`, `{{mssql:user}}`, `{{mssql:password}}`, `{{mssql:encrypt}}` and `{{mssql:trust_cert}}`. A variable that is set but unusable fails the case |
| `hie` | Merged into `hie.yaml` (for example `admission`, `validation`, `retention`, `runtime`, `destination`) |
| `secrets_key` | `true` writes a 32-byte key file and sets `secrets.key_file` |
| `files` | Files written under the engine's directory before it starts: transforms, `sources/*.yaml`, inbox files |
| `sqlite` | `{"<name>": ["<SQL>", ...]}`: a SQLite database at `<name>.db`, built by those statements |
| `tcp` | `["<name>", ...]`: TCP listeners on free ports that accept and close every connection, and count them |
| `destination` | The stand-in FHIR server: `validate` (`valid` or `unavailable`), `invalid` (rules `{resourceType, field, equals, expression, message}` that make `$validate` answer an error issue), `writes` (`ok`; `unavailable`: every write answers `503`; `commit_then_fail`: writes are stored and the answer is a `503`; `hold`: every write waits until the mode changes, a barrier a case releases with `set_destination`), `seed` (resources it holds from the start) |
| `runtime` | A stand-in of python-runtime's HIE facade. `environment` holds the `Python-Runtime-Environment-*` headers it answers with, and `GET /v1/capabilities` answers as python-runtime does for a consumer, with that environment as the default; without `environment` a fixed one is used. `try` is the list of answers to `POST /transforms/try`, used in order: `{"status", "headers", "body", "delay_ms"}`, `{"run": true}` to answer `200` as `run` below does for the records HIE sent (the fixed lab program, with the run's marker and diagnostics), `{"drop": true}` to close the connection without answering, or `{"hold": true}` to never answer and wait for the client to go away (a cancel); a `200` without `headers` carries the environment. `run`, `{"marker", "diagnostics"?}`, makes `POST /transforms/{name}/run` answer as a fixed lab program would (`{id, value, status?}` to an Observation `obs-<id>` whose note is the marker, or a `data` error when `value` is not a number), with those diagnostics on every reply; without it a run answers `503 busy`. Replies get the request ids by position. `PUT /transforms/{name}` answers its sha256 |

The stand-in destination serves `GET /metadata` (FHIR `4.0.1`), `POST /` for `batch` and `transaction` Bundles (a transaction resolves `Type?identifier=` references against what the server held before it, as HFS does, commits all or nothing, and fails whole when a reference matches nothing or several), `PUT /{type}/{id}`, conditional `PUT /{type}?identifier=`, and `POST /{type}/$validate` (the resource, or a `Parameters` with a `resource` parameter).

### Placeholders in requests and fixtures

`{{destination}}` and `{{runtime}}` (base URLs), `{{root}}` (the engine's directory), `{{port:<name>}}` (a listener's port, a number when it is the whole string), `{{sqlite:<name>}}` (a database's path), and `{{$<name>}}`, a value captured earlier in the case.

### Steps

| Step | Does |
|---|---|
| `{"request": {"method", "path", "headers"?, "body"? or "raw_body"?}, "expect": {"status", "headers"?, "body"?}}` | One HTTP exchange. `until_ms` repeats it until the expectation holds or the time is up; `background: "<label>"` sends it without waiting |
| `{"await": "<label>", "expect": {...}}` | Waits for a background request and checks its answer |
| `{"destination": {"resources"?, "stored"?, "not_stored"?, "writes"?, "validations"?, "bundle_types"?}}` | Checks the stand-in destination: counts per resource type, patterns some stored resource must match (or none may), the number of writes and `$validate` calls, the Bundle types received, and `bundles`, the entry urls of every Bundle received in order, answered or not. `until_ms` repeats it |
| `{"runtime": {"calls": {"try": 1}}}` | Checks how often each facade route was called; `until_ms` repeats it |
| `{"tcp": {"<name>": 0}}` | Checks how many connections a listener accepted |
| `{"sql": {"db", "statements"}}` | Runs SQL on a fixture database |
| `{"set_destination": {...}}` | Changes the stand-in destination's `validate` or `writes` |
| `{"set_runtime": {"environment": {...}}}` | Changes the environment the stand-in runtime reports |
| `{"write_files": {"<path>": "<content>"}}` | Writes files under the engine's directory |
| `{"sleep_ms": 500}` | Waits |

Every step may carry a `name`. An HTTP step is a request/response pair any client can replay; the other steps need the provider's stand-ins and are for HIE's own runner.

### Patterns

An expected body matches when every member the pattern names matches; other members are ignored, unless the object holds `"{{exact}}": true`. An array matches element by element with the same length, unless its first element is `"{{contains}}"` (each later pattern matches some element) or `"{{each}}"` (every element matches the one pattern after it). A string that is one of these tokens matches by kind:

| Token | Matches |
|---|---|
| `{{any}}` | Any value that is present |
| `{{absent}}` | The member is missing |
| `{{string}}`, `{{int}}`, `{{number}}`, `{{bool}}`, `{{object}}`, `{{array}}` | A value of that kind |
| `{{nonempty}}` | A non-empty string or array |
| `{{time}}` | An RFC 3339 time |
| `{{hex:64}}` | That many hex digits |
| `{{gte:1}}` | A number at least that |
| `{{contains:text}}` | A string holding that text |
| `{{$name}}` | Captures the value the first time, and must equal it after |
| `{{destination}}`, `{{port:x}}`, ... | Equals that placeholder's value |

Any other string matches itself after placeholders are filled in (`"Patient/{{$pid}}"`). Numbers compare by value.

### Forbidden values

Every value in `forbid` must never appear in any answer's body or headers, nor in any file under the engine's directory when the case ends (the store, the source files and anything else the engine wrote), nor in the engine's log, which the runner captures at `debug`. Custody is checked after the case's runtime is shut down, whatever the outcome (passed, failed, timed out or failed to start), and a custody violation fails the run even for a case listed in `known-red.txt`, unless its line there carries the flag `custody` (custody not built yet); a flagged case that keeps custody fails the run too, so the flag goes when custody is built.

`forbid_at_rest`, `{"values": [...], "under": ["data"]}`, names values an answer may carry (failed rows' content, Try's stdout) that must not be stored in plaintext in any file under those directories of the engine's (all of it when `under` is left out) when the case ends, nor logged. Placeholders are filled in both lists.
