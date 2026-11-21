# Design decisions

Each decision states what was chosen, the evidence behind it, and what it costs. "Exercise" means the hands-on comparison of Apache Camel 4.22 and Open Integration Engine 4.6 run in October 2026: a lab CSV sent to HFS as `Observation`s, with bad rows, an HFS outage and re-sent files. "Demo" means the run in the [README](../README.md#what-the-prototype-does).

## 1. A separate service that talks to HFS only through the FHIR API

**Chosen:** HIE is its own repository and process. It writes to HFS with standard FHIR REST calls (`POST /` with a `batch` Bundle) and reads nothing from HFS internals.

**Why:**
- The analytics product that consumes the data is separate from HFS, with HIE between it and HFS/HTS.
- Talking FHIR means HIE works with any FHIR server, and HFS stays a plain FHIR server with nothing integration-specific inside it.

**Cost:** HIE cannot use HFS shortcuts (for example a direct database load). For very large feeds, `$bulk-submit` is the path (see 7).

## 2. Rust for the engine

**Chosen:** The engine (polling, storage, delivery, API, alerts) is a single Rust binary, built and run on Linux (see the `Dockerfile` and the CI workflow).

**Why:**
- HIE is a separate product from the applications built on HFS: the integration work is messy and error-prone, and it is kept in its own service that those applications call through an API. Because it is independent, it does not inherit the stack of any one client application. [Discussion #2](https://github.com/HeliosSoftware/hie/discussions/2) has the comparison with .NET and the record of the decision, which was taken for .NET on 2026-10-19 and returned to Rust on 2026-10-20 on that reasoning.
- It is the same toolchain, CI and dependency set as HFS and HTS: the crate uses the versions already in the HFS lockfile, and it can later reuse `helios-fhir` types and the shared auth and observability crates.
- One static binary, a small memory footprint, no runtime to install.
- Both engines tried are Java. OIE needed a server, an embedded database and a desktop client, and the client failed three ways on Windows before it worked: launcher JVM options, missing JavaFX, classpath order.

**Cost:**
- Transform authors should not need Rust (see 3).
- The client application's services run under Aspire; the engine joins that as a container, and emits OpenTelemetry so the same dashboard covers it (to be wired, as in HFS's `helios-observability`).

A complete .NET 10 port of the engine exists in the history (commit `c3272d4`), test for test, and was green on Linux; it is the fallback if the decision changes again.

## 3. Transforms in Python, in a separate process

**Chosen:** A feed's transform is a Python file with a `transform(record, context)` function that returns FHIR resources or raises `DataError`. HIE runs it in a long-lived Python process and exchanges one JSON line per record (`hie_runtime/worker.py`).

**Why:**
- The plan is Python transforms first and FHIR Mapping Language later.
- Python is readable by the people who know the source data.
- Out of process, a transform that crashes or hangs cannot take the engine down: the worker is restarted, and the record stays queued.
- The line protocol is language-neutral, so a FHIR Mapping Language worker can be added later behind the same interface.
- The example transform uses only the standard library.

**Cost:** One inter-process round trip per record. Fine for lab volumes; a batched protocol is the next step if it shows up in measurements.

## 4. Feeds and transforms are files under version control

**Chosen:** One YAML file per feed (`feeds/*.yaml`) plus its Python transform, reviewed like code.

**Why:**
- In OIE the channel lives in the UI and its database; reviewing a change means exporting XML.
- Camel's YAML routes were easy to read and diff.
- Mapping rules are agreements with a data source and should have history and review.

**Cost:** No editing from a UI yet. A client UI could write feed definitions through an API later.

## 5. Every row is stored before delivery

**Chosen:** When a file arrives, each row is written to the database with its file name, file hash and line number. Then the file moves out of the inbox, and transformation and delivery run from the database.

**Why:**
- In the Camel exercise, with HFS down, the whole file went to the error folder, and nothing delivered it when HFS came back.
- Here the rows wait as `pending` and are delivered on the next retry. Demo §1–2: HFS down, 3 rows pending, HFS started, 3 delivered, nothing lost.

**Cost:** HIE keeps a copy of source data, which needs a retention policy.

## 6. Idempotent delivery: conditional update on a business identifier, plus file hashes

**Chosen:**
- Every resource carries an `identifier` (feed system + business key).
- Delivery uses `PUT {type}?identifier=system|value`, so a repeated record updates instead of creating.
- A file whose SHA-256 was already received is recorded as a duplicate and not reprocessed.

**Why:**
- In the exercise, re-sending a file doubled the data.
- A file failing on one row was re-read every half second, writing its good rows again each time: 20 copies in 10 seconds.
- HFS supports conditional update by identifier, including inside a Bundle: checked against HFS, 201 the first time and 200 after.
- Demo §3: the identical file is skipped, and the same rows in a new file leave HFS at 3 resources, the re-sent one at `versionId` 2.

**Cost:** Each feed needs a real business key from the source, such as an accession or order number plus the test code. The example uses `patient|code|date`, which would merge two same-day results; real feeds must provide a proper key.

## 7. `batch` Bundles, not `transaction`

**Chosen:**
- Records go to HFS in `batch` Bundles of up to `batch_size` entries.
- If HFS refuses a whole Bundle, the records are re-sent one by one to isolate the bad one.

**Why:**
- One bad row must not block the rest of a file. A batch gives a status per entry.
- Lab results do not need all-or-nothing per file.

**Cost:** No atomicity across a file. Feeds that need it (a resource and its dependents) would use `transaction`; very large feeds would use `$bulk-submit`.

## 8. Two kinds of failure, handled differently

**Chosen:**

| Kind | Examples | Handling |
|---|---|---|
| Data | Wrong column count, non-numeric value, unknown code or unit, missing identifier, a 4xx from HFS for that entry | Rejected at once. Kept with file, line, original row and reason. Reprocessable after a fix. |
| Transient | HFS unreachable, timeout, 429, 5xx | Retried with exponential backoff (2 s doubling to 5 min), never dropped. One alert after `alert_after_attempts`, one when delivery recovers. |

**Why:**
- In the exercise, retrying a bad row only multiplied writes, and treating an outage as a bad file stopped good data.
- Demo §4: 5 bad rows rejected, each with its reason, while the good row in the same file went through.

## 9. Validate in the transform; the engine checks only what it relies on

**Chosen:**
- The transform validates the source data: types, required columns, known codes and units, date format.
- The engine checks only `resourceType` and the identifier needed for idempotency.

**Why:**
- HFS accepts what it is given unless validation is enabled: an `Observation` with `status: "bogus"` was stored.
- Building FHIR by text templating (the Camel exercise) let `alto` reach the server, and could let crafted values inject fields.
- Building objects in code and failing early gives the precise reason ("value 'alto' is not a number").

**Later:** Optional profile validation before sending, through HFS `$validate` or with HFS validation enabled.

## 10. Every record is traceable, and corrections keep the original

**Chosen:** For each record HIE keeps:
- the file name, hash and line;
- the row as received;
- the corrected row, if it was fixed;
- the resources sent;
- HFS's response, with location and version.

A reprocess with corrected data keeps the original (`original_raw`).

**Why:** OIE's per-message history (status, error, source file, row) was the most useful thing either tool offered. It answers "where did this value come from, and what happened to it". Demo §5 shows a corrected row with both values and HFS's `201`.

## 11. Monitoring: an HTTP API for client UIs and Prometheus metrics

**Chosen:**
- `/api/...` serves feed status (freshness, counts, last alert), files, records and alerts for a client UI.
- `/metrics` exposes Prometheus gauges and counters.

**Why:**
- A data-sources page in a client UI needs per-feed freshness and completeness.
- Camel's counters were in memory and reset on restart. Here they come from the stored history.
- Prometheus is what HFS already exposes, so the same monitoring stack (Grafana or a hosted product) can watch both. Integrating a monitoring product stays an option, not a rewrite.

## 12. Alerts for data events in the engine; routing left to a product

**Chosen:**
- HIE raises alerts for rejected records, unreadable files, delivery outages and recovery, and **silence**: no data within the feed's `expect_every`.
- Alerts are stored, logged, and posted to a webhook (`text` field, the shape Slack and Teams accept). (Superseded in part by decision 40: the webhook gets a signed, retried and recorded event per change, and keeps `text`.)
- A repeated condition alerts once. (Superseded in part by decision 22: conditions are rules, and an alert clears itself.)

**Why:**
- OIE's alerts needed an SMTP server or a second channel, and they cannot detect a feed going quiet.
- Silence is the case the lab cares most about ("nothing has arrived since Tuesday").

**Not built:** Who gets paged, escalation and on-call schedules. Those belong to an alerting product fed by the webhook or the metrics. The recipients depend on the identity provider.

## 13. SQLite for the prototype, PostgreSQL next

**Chosen:** A single SQLite file (WAL mode).

**Why:**
- No setup, runs anywhere, enough for one engine instance.
- The schema is plain SQL with nothing SQLite-specific beyond types, so moving to PostgreSQL (the planned stack) is a driver change and a migration.

**Cost:** One instance at a time. Running several, or high availability, needs PostgreSQL. (Decision 28 adds PostgreSQL behind the same SQL, and leases for several instances.)

## 14. Source robustness: encodings and malformed rows

**Chosen:**
- UTF-8, UTF-8 with a byte-order mark, and UTF-16 with a byte-order mark are decoded.
- Anything else marks the file `unreadable` and raises an alert.
- A row with the wrong number of columns is rejected alone, with the expected header in the message.

**Why:**
- In the exercise, a file written by Windows PowerShell (UTF-16) made Camel fail and retry it forever.
- A seventh column appearing in a feed was the example given for messy sources.

## 15. Two delivery keys: the resource's `id`, or a business identifier

**Chosen:** A resource that carries an `id` is written to `PUT {type}/{id}`. One without needs an `identifier` and is written as a conditional update on it. A resource with neither is rejected.

**Why:**
- Data that already is FHIR (an NDJSON export, a Synthea Bundle) comes with ids that other resources reference. Writing it under those ids keeps the references valid, and re-sending the same export updates instead of duplicating: the same three patients delivered as Bundles and again as NDJSON left HFS with each resource once.
- Data mapped from a non-FHIR source has no id to offer; the business identifier is what makes it idempotent (decision 6).

**Cost:** A transform that wants a conditional update must not set `id`.

## 16. A Bundle is split into records, with references made absolute

**Chosen:** A `fhir_bundle` file is read as one record per entry. Before that, references to another entry's `fullUrl` (`urn:uuid:...`, which Synthea writes) are rewritten to that entry's `Type/id`.

**Why:**
- Per-record traceability (decision 10) needs one record per resource, not one per file.
- Delivered independently, the entries would otherwise keep `urn:uuid` references that only make sense inside the transaction.
- Posting the Bundle as a transaction would keep the references intact but lose the per-resource history and the per-entry disposition; it stays an option for feeds that need atomicity (decision 7).

**Cost:** References to entries that carry no `id` cannot be rewritten and are passed through unchanged.

## 17. S3 through `object_store`

**Chosen:** The S3 source uses the `object_store` crate (the AWS implementation), listing a prefix, fetching objects and renaming them under an archive prefix.

**Why:**
- `object_store` is already in the HFS dependency set (SQL-on-FHIR uses it), and one trait covers S3, S3-compatible stores (MinIO, through `endpoint`), the local filesystem and an in-memory store, so the same source code is tested in memory and runs against a bucket.
- Credentials come from the standard AWS environment, like every other AWS tool an operator runs.

**Not yet verified:** The source has not been run against a real bucket; the object-store path is covered by the in-memory tests. That is the first thing to do once a bucket exists.

## 18. Sources are API resources, persisted as the same files

**Chosen:** `POST /api/sources` creates a source from its definition (the YAML document as JSON), the engine writes `sources/{id}.yaml` and starts the source without a restart; `PUT` replaces and restarts it; `DELETE` stops it and removes the file. Pause and resume are operational state kept in the database with who and when, not in the definition. `check-now` makes a source pass at once (and, since decision 41, records a check that says what the pass found). `POST /api/sources/test` lists a connection once and returns the evidence (reachable, elapsed, objects, newest).

**Why:**
- The client UI's design configures a source in the application, in five steps, with a connection test whose result stays on the page, and pauses, resumes and re-checks a source from its page. All of that needs the engine to own the definition.
- Decision 4 still holds: the engine writes the same files it reads, so definitions stay under version control, diffable, and usable without the API.
- Pause is not part of the definition because it is a fact about operations ("paused by X since Y"), it must survive a restart, and it must not appear as a change to the source's file.

**Cost:** Two sources of truth for a definition while a process runs (the file and memory); the engine resolves it by re-reading the file when it reports a definition, and by rewriting the file before restarting a source. Concurrent edits of the same file by a person and the API are not arbitrated.

## 19. Transforms and monitoring over the API, preliminary

**Chosen:** Transforms are listed, read, saved and tried through `/api/transforms`; a save restarts the sources that use the transform. Monitoring is one aggregate (`/api/monitoring/status`) plus per-source day series (`/api/sources/{id}/stats`), both computed from the engine's store.

**Why:** The client UI's Transforms and Monitoring modules need these to render anything, and the assessment in discussion #1 lists them as the next additions after sources. "Try it" reuses the same Python worker as a real load, so the test is the real thing.

**Not yet:** transform versions and "used by since"; failures grouped by reason with the transform line; alert rules with recipients.

## 20. Bearer tokens on the API, validated as HFS validates them

**Chosen:** With `HIE_AUTH_ENABLED=true`, everything under `/api` requires a JWT validated against the identity provider's JWKS with the expected issuer and audience, through HFS's `helios-auth` crate; `/health` and `/metrics` stay open. Keycloak first, and the same providers HFS supports after it. The public sandbox follows the public HFS: open, for showing the work and for demos, with nothing sensitive on it; a secure environment sets the `HIE_AUTH_*` secrets and the deploy turns validation on.

**Why:**
- Saving a transform is running code on the engine; an open API on a public host is remote code execution for anyone who finds it.
- The same crate and the same claims as HFS mean one identity provider, one token, and the same `HFS_AUTH_*`-shaped settings an operator already knows.
- The subject of the token is what the UI's "paused by" needs, so it is recorded.

**Not yet:** per-user audit of changes. Scopes are decision 24. The engine's own credentials for calling HFS are decision 23.

## 21. `$bulk-submit` points HFS at the source objects, and receipts map by `Type/id`

**Chosen:** On `delivery.method: bulk_submit` the engine does not rewrite files. It stores the rows, runs a pre-flight in Rust instead of the transform (`resourceType` and `id` present), and serves a manifest whose entries are pre-signed URLs of the S3 objects, or the engine's own file URLs for a folder. The kick-off closes the submission (`submissionStatus: completed`); the engine polls `$bulk-submit-status`, reads the receipt files, marks a record `sent` when a `{"reference": "Type/id"}` for it appears and `rejected` otherwise, then archives the objects. One submission per drop, a `submissionId` derived from the files, `409` from HFS treated as "already held". A public bucket (`anonymous: true`) is handed over by its objects' public URLs: virtual-hosted on AWS, but path style for a bucket name with a dot, which AWS's wildcard certificate does not cover. Each kept file records where it lives (`files.origin`: a store, a local source, or an entry the engine staged), and only what the engine holds is served from its file route; rows from earlier versions get their origin once at start-up.

**Why:**
- It is the picture agreed in discussion #1 for an NDJSON export in a bucket: HFS does the ingestion, the engine keeps the history and the dispositions.
- HFS's receipts carry `Type/id` for every stored resource but **no input line number or file** on the error side (the line number exists inside HFS and is not published). Mapping by `Type/id` is what the published receipts allow; the engine's pre-flight is what names the line for the failures it can predict. HFS publishing the line in its error `OperationOutcome` would let the rest be named too; that is requested on the HFS side.
- Objects stay at the source until the receipts are in, so a retry or a replacement manifest can point at them again, and a pre-signed URL never points at an archived key.

**Cost:**
- A resource without an `id` cannot go this way; the pre-flight rejects it and says so. (Decision 29 adds the `transformed` path, where the engine derives one.)
- A line HFS refuses that the pre-flight did not predict is rejected with "no receipt", plus HFS's first error text, without a certain attribution.
- The engine's `public_url` must be reachable from HFS, and the folder-file route must stay open (unguessable token in the path).
- A public URL for a dotted bucket name uses a different host form from the engine's own reads; both name the same bucket and key. The stored origin is decided when a file is taken (or once, at the upgrade) and does not follow later edits of the source's definition.

## 22. Alert rules are data, and an alert clears itself

**Chosen:** A rule names a source (or all of them), one of four conditions with its threshold — `silence` (duration), `missing_by` (time of day, UTC), `rejected_share` (percentage of a load), `unreachable` (failed delivery attempts) — who to notify, and the channels (`app`, `webhook`, `email`). Rules are rows in the engine's database with a CRUD under `/api/alert-rules`. A condition raises **one** alert per rule and source, and the engine closes it when the condition goes away: data arrives, the file arrives, a later load is within the limit, delivery succeeds. The alert keeps its rule, the recipients, the channels, and when and why it cleared. Every source gets a `silence` rule at birth, set to its `expect_every` or two hours. Without a `rejected_share` or `unreachable` rule the fixed behaviour of decision 12 stays.

**Why:**
- The four conditions are the ones the monitoring brief asks the UI to offer, with the thresholds in the units a person would type.
- An alert that clears itself is what lets a list of *open* alerts mean something; an alert that never closes has to be reasoned about by its timestamp.
- Rules as data rather than fields of the source definition: a rule may cover every source, an operator may add one without touching the YAML a developer owns, and the UI edits them on their own page.
- The default rule keeps the promise of decision 12 that a quiet source is noticed without anyone configuring it.

**Cost / not yet:** `email` is recorded but not sent; the webhook is still the one outward channel. Recipients are free text; nothing resolves them against the identity provider. Rules are evaluated once per pass of the source, so a `silence` or `missing_by` alert is late by up to one poll interval.

## 23. The engine calls HFS with its own client-credentials token

**Chosen:** `HIE_FHIR_TOKEN_URL`, `HIE_FHIR_CLIENT_ID`, `HIE_FHIR_CLIENT_SECRET` and an optional `HIE_FHIR_SCOPE` name an OAuth2 client at the identity provider. One token source serves every request the engine makes to HFS: batch Bundles, `$bulk-submit` kick-offs, status polls and receipt files. The token is cached and renewed a minute before expiry; a `401`/`403` from HFS discards it and is treated as a transient failure, like an outage. Absent settings mean anonymous calls, with a warning at start; partial settings are refused.

**Why:**
- Any environment beyond the public sandbox has an HFS that requires a token; without this the engine could only deliver to the open one.
- Client credentials is what Keycloak, Entra ID and Okta all offer for a service identity, and what HFS validates today; the same token source will take a private-key JWT (SMART backend services) when HFS asks for one.
- A refused token must not reject the data. It is the same situation as HFS being down: wait, retry, and tell someone.

**Not yet:** private-key JWT instead of a shared secret; a token per tenant when one engine serves several; scopes narrowed per source.

## 24. Three scopes on the API: read, manage, transforms

**Chosen:** With auth on, a request needs a scope decided by its method and path: `hie/read` for every `GET`; `hie/transforms` for `PUT /api/transforms/{name}`; `hie/manage` for every other write. The write scopes also read; `hie/*` grants everything. Scopes are read from the `scope`, `scp` or `roles` claim through `helios-auth`, so Keycloak client scopes, Okta scopes and Entra ID app roles all work. A token without the needed scope answers `403` naming it.

**Why:**
- The monitoring brief has people who only watch; they should not hold a token that can rewrite a source.
- Saving a transform is running code on the engine. It is the one action worth its own scope, so an operator who manages sources and alert rules cannot ship code, and a developer who writes transforms cannot pause a feed.
- Three names rather than a SMART `hie/Source.cruds` grammar: the API is not FHIR resources, and the brief's roles map onto three verbs.
- A token that carries only HFS scopes is refused here on purpose: the engine and HFS are different universes, even when one identity provider serves both.

**Not yet:** per-source grants (a token for one source only); a `user/` context tied to who created the source; audit of who changed what.

## 25. A FHIR server as a source: pulled by `_lastUpdated`, one cursor per type

**Chosen:** `source.type: fhir_server` names a server, the resource types to pull, and how to authenticate (a client-credentials grant or a fixed bearer token, the secret always read from an environment variable the file names). Each pass searches every type for `_lastUpdated=gt{cursor}`, sorted by `_lastUpdated`, page by page up to `max_pages`, and hands the resources on as one NDJSON load per type. The cursor is the newest `meta.lastUpdated` seen and moves only after the engine has stored the records. The load's format is forced to `ndjson`, resources are delivered by their id, and `$bulk-submit` is refused for this source. The connection test reads `/metadata` and counts what waits past each cursor.

**Why:**
- Migrating from or mirroring another FHIR server is a connector customers ask for first, and it needs nothing new on the wire: search and paging that every server has.
- `_lastUpdated` with a cursor is the one incremental mechanism that works on any server; `_history` and Subscriptions are not everywhere, and `$export` is a different pipeline (decision 21).
- Moving the cursor after storing, not after fetching, means a crash repeats a page instead of losing it; idempotent `PUT`s make the repeat harmless.
- One cursor per type rather than one for the server, because servers page each type independently and a slow type should not hold back the others.

**Cost / not yet:** resources with equal `lastUpdated` at a page boundary rely on `_sort` being honoured; a server that ignores `_sort` must be drained in one pass (`max_pages` high enough). Deleted resources are not seen (`_history` would be needed). `since` is the only way to skip old data.

## 26. A SQL database as a source: the operator's query, a bound cursor

**Chosen:** `source.type: database` names the environment variable that holds the connection URL (PostgreSQL first; MySQL/MariaDB and SQLite through the same code; SQL Server through its own driver), the query, and the cursor column. The query is the operator's own SQL with `{cursor}` where the cursor value goes, an `ORDER BY` on the cursor column and a `LIMIT`; the engine binds the cursor as a parameter (never spliced), runs the query up to `max_runs` times per pass moving the cursor between runs, and hands the rows on as one NDJSON load. Rows are decoded by column type (numbers as numbers, timestamps as ISO text, JSON as JSON); the cursor keeps the kind it binds as (integer, float, timestamp, date, text). The cursor moves to the largest value seen once the records are stored. The transform receives one row as a JSON object, as it receives a CSV row.

**Why:**
- The query is what the person who knows the schema would write anyway; a table/column mapping in YAML would be a worse query language. Keeping it opaque to the engine means any join or view works.
- The cursor is bound, so the one value that comes from data cannot change the query; the SQL itself comes from the source definition, which is already code the operator owns (like a transform).
- One typed cursor per source covers the two columns people have: a sequence and an updated-at. Moving it after storing, not after reading, means a crash repeats a run instead of losing it; idempotent `PUT`s make the repeat harmless.
- `sqlx` gives PostgreSQL, MySQL and SQLite with one API, so the test suite runs on SQLite with no server and the same code is what reaches PostgreSQL.

**Cost / not yet:** SQL Server comes through a second driver (`tiberius`) behind the same definition; its placeholder is `@P1` and `TOP n` stands in for `LIMIT`, and only SQL Server authentication is supported. Rows that share the cursor's largest value across a `LIMIT` boundary are skipped (use a unique column, or an updated-at with enough precision). Deleted rows are not seen. MySQL decoding is written to the same pattern as PostgreSQL but not yet exercised against a server.

## 27. HL7 v2: messages parsed to JSON for the transform, and an MLLP listener that spools before it acknowledges

**Chosen:** `format: hl7v2` reads a file of messages (one or more, each from its `MSH`; `\r`, `\n` or `\r\n` between segments) into one record per message: a JSON object with the message type, control id, sender and receiver, and every segment as a list of fields, components and repetitions left as text. The Python helpers `hl7(record, "PID-5.1")`, `hl7_segments`, `hl7_field` and `hl7_datetime` in `hie_runtime/hie.py` do the splitting. `source.type: mllp` listens on a TCP address: each framed message is parsed, written to the `spool` folder as its own file (write, fsync, rename), and only then answered `AA`; a message that cannot be stored is answered `AE` and one that is not HL7 at all `AR`. The spool is then taken like a folder source: one load per message, SHA-256 dedupe, the transform, delivery. Format is forced to `hl7v2` and `bulk_submit` refused.

**Why:**
- An integration engine's first wire is HL7 v2 over MLLP; the lab exercise with Camel and OIE was about exactly this.
- Acknowledging only after the message is on disk is the one promise a sender relies on: an `AA` means HIE has it, whatever happens next. An `AE` on a full disk makes the sender keep and resend.
- Spooling to files reuses the folder path unchanged: traceability per message (the file is the message), duplicates recognised by hash, pause and reprocess as for any load. A listener that wrote straight to the records table would need its own copies of those.
- Fields as text with their separators, plus helpers, rather than a deep nested structure: a transform reads `PID-5.1` the way the HL7 world writes it, and a segment the parser does not know needs no schema.
- The parser is small and in the engine (segments, fields, the `MSH` separators). An HL7 library would bring a schema for every version and still need the same JSON for Python.

**Cost / not yet:** no TLS on the listener (front it with a TLS proxy or a VPN, as is usual for MLLP); no batch (`BHS`/`BTS`) or `FHS` files; no outbound MLLP (sending ACK-dependent results back); `MSH-9.3` structure codes are not validated; one ACK per message, so a sender that pipelines many frames gets them in order but waits for each disk write.

## 28. One store written once, on SQLite or PostgreSQL; one instance per source, by lease

**Chosen:** The store's SQL is written once, with `$N` placeholders and only what both engines share (`RETURNING id`, `ON CONFLICT DO UPDATE`, `IS NOT DISTINCT FROM`, `CASE` instead of boolean sums, integer epoch milliseconds instead of date types). `database:` in `hie.yaml` is a SQLite file path or a `postgres://` URL; `HIE_DATABASE_URL` overrides it so the password stays out of the file. PostgreSQL goes through the synchronous `postgres` client on a thread of its own, called over a channel, so the async runtime is never asked to block on another runtime. Every store the tests open can be pointed at a PostgreSQL with `HIE_TEST_DATABASE_URL`; each test gets a schema of its own and drops it.

Several engine instances may share one PostgreSQL. A source is run by the instance that holds its **lease** (`source_leases`): a pass takes or renews the lease for three poll intervals (at least 30 s) and runs; an instance that finds the lease held stands by; a lease that lapses, because its holder died, is taken by the next pass of another instance; a clean stop releases it. The API shows the holder (`lease`, `instance`) and reports a source held elsewhere as `standby`. Pause, resume, rules and cursors live in the store, so they apply whichever instance runs the source.

**Why:**
- A second machine, a restart without losing in-flight work, or a database the operators already back up: all three want PostgreSQL, and all three are the first things a production asks for.
- One SQL text for both engines keeps SQLite as the no-setup default (a laptop, the public sandbox, the test suite) without a second store to maintain; the test suite on PostgreSQL is the proof the two behave the same.
- A lease per source rather than a lock per record: the sources are the unit of ownership people reason about ("who is running the lab feed?"), the folder and MLLP sources cannot be shared across machines anyway, and a pulling source with a cursor must not be pulled by two instances at once. Record-level claims, for throughput on one feed, can be added under the lease later.
- The synchronous client on its own thread rather than an async store: the pipeline calls the store from many places; making them all async would have touched every call site for no gain, since a SQLite call blocks the same way.

**Cost / not yet:** no TLS to PostgreSQL yet (same host or a private network); one connection per instance, no pool; a `check-now` makes a pass only on the instance it was sent to (the check it records is answered by the holder's next scan, decision 41); no per-record parallelism within a source.

## 29. `$bulk-submit` from any source: the engine writes the NDJSON, and derives ids

**Chosen:** `delivery.bulk_from` says what a manifest points at. `source_objects` is decision 21 unchanged. `transformed` runs the transform as the REST path does, writes the resources as NDJSON per resource type, and hands those files to HFS: in the source's own store by pre-signed URL when the source is S3, else served by the engine from `bulk_dir`. A resource with only an `identifier` gets an `id` derived from it (SHA-256 of `system|value`, 48 hex characters), kept alongside the identifier. `auto`, the default, picks `source_objects` only for NDJSON or Bundle files with the passthrough transform. The source file is archived once read; the engine's files are removed once the receipts are in.

**Why:**
- A customer sends a lab CSV, not a manifest. The bulk path must work from whatever arrives, with the same transform and the same rejection of bad rows as the REST path, or it is a path only for exports.
- `$bulk-submit` stores by id, and a lab row has an identifier, not an id. Deriving the id from the identifier keeps the one promise that matters, that re-sending the row updates rather than duplicates, without asking every transform to invent ids.
- Writing the NDJSON into the source's bucket for an S3 source means HFS fetches from S3 by pre-signed URL, as it does for exports, and the engine's `public_url` need not be reachable from HFS: an engine on a laptop can bulk-submit to a public HFS.
- Bad rows never reach the file: the transform rejects them with line and reason before the manifest exists; what HFS refuses comes back as a receipt, mapped by the derived id.

**Cost / not yet:** the derived id is opaque (a hash), so a resource created this way is found by its identifier, not by a readable id; a record whose transform returns several resources is settled on its first; the `auto` rule looks at the transform's file name, so a custom passthrough should set `bulk_from` explicitly.

## 30. Terminology in transforms: HTS through two helpers, with a fallback

**Chosen:** `terminology_url` in `hie.yaml` (`HIE_TERMINOLOGY_SERVER` overrides it, `HIE_TERMINOLOGY_TOKEN` adds a bearer) names an HTS. The engine hands it to the worker, and a transform calls `lookup(system, code)` (`CodeSystem/$lookup`: the display, or `None` when the code is unknown) and `translate(code, source_system, target_system)` (`ConceptMap/$translate`: the first equivalent target concept, or `None`). Answers are cached for the life of the worker. A server that cannot be reached raises `TerminologyError`, which the worker reports as `transient`: the engine keeps the record and tries again later, with the outage alerts saying why. Without a server both helpers answer `None`, so a transform keeps a fallback (a local table) or rejects the row. `lab_results.py` validates LOINC codes against the server and takes their display from it, and translates local codes through the server's ConceptMap before its own table.

**Why:**
- A lab's codes are local and must become LOINC; the mapping belongs in the terminology server, where it is maintained by the people who own terminology, not in a dictionary inside a script.
- A code HTS does not know is a data error worth stopping on; a server that is down is not, and must not turn rows into rejections.
- Two helpers rather than a FHIR client in every transform: the transform author writes `lookup` and `translate`, and the worker decides how to reach the server.

**Cost / not yet:** `$validate-code` against a ValueSet, `$expand`, and `$subsumes` are not wrapped; the cache is per worker process and forgets on restart; a token for HTS is a single static one, not the engine's client credentials.

## 31. Zips are drops of several files; a bucket may be read without being touched

**Chosen:** A zip on a file source is unpacked by the engine: every entry whose extension fits the source's format becomes a load of its own (`drop.zip!/path`), with its own hash, records and dispositions; the other entries are skipped; the zip is recorded as `unpacked` and archived. An S3 source may be `anonymous` (unsigned requests to a public bucket) and `read_only` (nothing moved or written; an object already taken is recognised by its content on later passes and skipped without a duplicate row).

**Why:**
- The first real drop the engine was given was a public bucket holding zips: a customer's export tool writes zips, and a data provider's bucket is not ours to move things in. Asking people to unzip and copy first is the kind of step an integration engine exists to remove.
- One load per entry keeps the traceability the engine promises: a bad line is named by file and line inside the zip, and a resent zip whose entries are unchanged is all duplicates.
- Read-only is a property of the source, not of the engine: `acknowledge` becomes a no-op and content hashes do the remembering, so every other part of the pipeline is unchanged.

**Cost / not yet:** entries are unpacked in memory, one at a time, so a zip of several gigabytes needs streaming, not yet written; nested zips and tar/gzip are not handled; on a read-only bucket the engine re-downloads an unchanged zip every pass to hash it (object ETags would avoid that).

## 32. Transform and delivery overlap on the REST path

**Chosen:** A pass transforms and delivers concurrently: the transform loop signals after each chunk of 200 records, and a delivery loop sends what is due in batch Bundles as soon as it is signalled, finishing with one last sweep when the transform ends. Both run on the pass's own task (`tokio::join!`), not in threads.

**Why:**
- With a large CSV the two slow parts are Python and HFS, and each was idle while the other worked: the file took the sum of the two. Interleaving brings it to about the longer of the two, with no change to what is stored or how a failure is handled.
- The store already orders the work (`received` → `pending` → `sent`), so delivery needs no queue of its own: it simply asks for what is due, as it did after the transform.
- Delivery during an HFS outage behaves as before: deferred records carry a backoff and are not due, so the delivery loop does not hammer a server that is down.

**Cost / not yet:** the tail of a file goes out in smaller Bundles than `batch_size`; the bulk path still transforms the whole load before the submission opens, because a manifest names complete files.

## 33. A bucket may announce objects through an SQS queue

**Chosen:** An S3 source may name a `queue`: the SQS queue the bucket's `s3:ObjectCreated:*` notifications go to, directly or through SNS. With one, a pass receives the queue's messages, takes the announced objects that are under the prefix and match the pattern, and deletes the messages once the objects are in the store. The bucket is still listed in full at start and every `relist_seconds` (an hour by default). The queue is read with the source's own credential (since decision 37, the stored credential its definition names), signed with SigV4 like the S3 client; no AWS SDK is added.

**Why:**
- Listing a bucket of many objects every few seconds is slow and costs a request per thousand keys; a queue delivers only what is new, at once.
- Messages are deleted after the pass, not when received, so a crash or a failed fetch leaves them to be delivered again after the visibility timeout; a duplicate delivery is harmless because the store recognises the object by its hash.
- The periodic listing is the safety net for a lost or malformed event, which S3's at-least-once delivery does not rule out.
- Signing the SQS calls with the same credential provider as the bucket keeps one way of holding AWS credentials and avoids the AWS SDK's weight for two API calls (`ReceiveMessage`, `DeleteMessageBatch`).

**Cost / not yet:** an anonymous source cannot have a queue; FIFO queues and message attributes are not handled; the queue must be the bucket's own notification target (the engine does not subscribe or configure anything); MinIO cannot feed a real queue, so the lab runs this only against the stand-in.

## 34. Transforms may run at a remote runtime service (superseded by 35)

**Chosen:** `runtime:` in `hie.yaml` sent each chunk of records to a remote runtime service over HTTP instead of the local worker.

## 35. Transforms run only in the local worker

**Chosen:** The `runtime:` backend and its configuration are removed. Every transform runs in the source's local worker, and Try runs in a worker of its own. A leftover `runtime:` in `hie.yaml` (whatever its value) or a non-empty `HIE_RUNTIME_URL` stops the engine at start, `hie check` included.

**Why:**
- The service the backend called no longer exists, and no deployment configures it.
- One backend keeps one outcome path, and one place to constrain customer code.
- Failing at start rather than ignoring the block means no operator silently loses the isolation they configured: their transforms would otherwise run locally, with the engine's rights, without a word.

**Cost / not yet:** the local worker still runs a transform with the engine's own rights until it runs under operating-system limits (next).

## 36. The local worker runs under `limited-process@1`

**Chosen:** The transform worker is started through the reference launcher of the `limited-process@1` contract, vendored unchanged as `hie_runtime/limits.py` and pinned by contract id and SHA-256 in `hie_runtime/limits.pin`, which the engine compiles in. The contract's text lives in HIE as [`docs/limited-process.md`](limited-process.md) (the launcher's docstring calls it `docs/contract.md`; the file cannot be edited without changing its hash). The engine hashes `limits.py` on disk before every start and refuses one that differs from the pin without starting a process; the hash the launcher reports in its handshake is checked as well. To re-vendor: replace the file byte for byte, update both lines of `limits.pin`, run the tests (`hie_runtime/README.md`). The engine is the contract's host in its long-lived worker pattern: a fresh run directory and an environment built from nothing for each process, the control handshake and the pin check before the transform runs, a startup budget that includes the worker's own ready line, a deadline per record taken before the write, bounded reply lines and a continuously drained stderr tail, the process group killed whenever a process ends, and one report per process in the log. Memory, file size and open files are limited per process from the `worker:` block. A record that times out, exceeds a limit, crashes the process or breaks the protocol is rejected as a `transform` error and never retried; its process is replaced for the next record. The pipeline stores each outcome before it sends the next record.

**Why:**
- Customer code ran with the engine's rights and no bounds: one endless loop held its source forever, one runaway allocation could take the host down, and a child it started outlived it.
- A contract with a reference launcher gives one tested way to apply the limits, a handshake that separates "could not start" from "the transform failed", and evidence of what was applied; pinning the hash means the engine runs the launcher it was built and tested with.
- Rejecting rather than retrying: a record that timed out may already have had effects, and one that always hangs would otherwise block its source on every pass. A person sees it with its reason and reprocesses it.
- Storing each outcome before the next record is sent means a record that was decided is never sent again, whatever stops the pass afterwards.
- Python 3.9 is the floor. The contract sets 3.9 as the launcher's own floor and leaves the interpreter to the host; nothing in the launcher needs more, and a higher floor would stop transforms on machines that run HIE today (Ubuntu 22.04 ships 3.10, others 3.9). The Docker image's Debian bookworm ships 3.11.

**Cost / not yet:** limits are per process, not per transform with its children, and nothing bounds the number of processes; the network stays open and the filesystem readable and writable with the engine's rights; CPU time is deliberately unlimited, because it accumulates over a long-lived worker and would end a healthy one, so the per-record deadline is what stops a spinning transform; a process that leaves its group survives the kill; on macOS and Windows only the deadline, the group kill and the bounded output are guaranteed. A transform's in-memory state (caches) is lost whenever its process is replaced. Machines with only Python 3.8 or older need a newer interpreter for `python:` before any transform runs.

## 37. Connector credentials in an encrypted store inside the engine

**Chosen:** S3 sources (and their queues) sign with credentials stored in the engine, never with AWS credentials from its environment. A credential is stored through `/api/secrets` (kind `aws`: an access key id and a secret access key), has a current revision, and is named by a source's `credential:`; a source with neither `credential` nor `anonymous: true` is refused. The engine answers metadata only, never a value. The native backend (`secrets-native`, on by default) keeps each credential's current revision in the engine's own database, sealed with XChaCha20-Poly1305 under a key derived with HKDF-SHA256 from one master key, with a fresh random nonce per seal and associated data that binds the store, the secret's id, name, kind and revision, the algorithm and the key id. The master key comes from `HIE_SECRET_KEY`, a key file, or, for a SQLite store, a key file the engine creates next to the database. The first start with a key writes a key check; a later start with a sealed store and a missing, wrong or malformed key is refused before serving, naming both key ids. A running source loads a new revision on its next pass. Each engine instance registers the sources it runs for their credentials, so a credential used by any instance on the store cannot be deleted.

**Why:**
- Credentials in the process environment are one set for every source, cannot be changed without a restart, and are readable by anything that can read the environment. Stored per credential, each source reaches only its bucket, and a rotation needs no restart.
- In the engine's own store, they need no external secret manager for a single-host install, and work the same on SQLite and PostgreSQL. The trait keeps room for an external backend (`secrets-aws` is reserved).
- An AEAD with associated data detects a value moved to another row, renamed, or rolled to another revision; XChaCha20's 24-byte nonces can be random without a counter.
- Refusing to start on a sealed store without its key, rather than starting with credentials unusable, makes a lost or wrong key visible at once; the engine never makes a new key for a sealed store.
- Registering before use, under the secret's row lock, makes "deleted while used" impossible across instances, and a manager-owned cleanup after the old runner has ended releases nothing still in use.

**Cost / not yet:**
- Two settings are refused even before the store is sealed: a key set in more than one place, and an automatic key file that is a link, not a regular file, someone else's or readable by others. Explicit key files are taken with any owner and mode, so secret mounts work; protecting them is the operator's.
- By default the SQLite key sits on the same volume as the database, so a copy of that volume holds both; the engine warns. There is no key rotation and no envelope encryption: a lost key loses the credentials.
- A replaced value remains in database files (SQLite WAL and free pages, PostgreSQL dead rows) and backups until overwritten, under the same key. A whole row rolled back to an older revision still opens: nothing outside the row counts revisions.
- Plaintext copies outside the engine's wiped types: the S3 client's own credential strings for the client's lifetime, HTTP and JSON parser buffers. HMAC pad blocks and HKDF temporaries made once at start are not wiped.
- Registrations belong to an instance (`HIE_INSTANCE_ID`, or an id kept in the sources directory). After a failure they err on keeping: a start that fails after a definition was published keeps both credentials registered until the next start or replace, a cleanup that keeps failing leaves a row until the next start, and a changed instance id leaves its old rows behind. Each only makes a delete answer `409`. A crash while the key file is created can leave a temporary copy of the key next to it.

## 38. An append-only event log, and one retention for history

**Chosen:** The engine appends an event for each attempt and outcome of a record (`record.delivered`, `record.recovered`, `record.attempt_failed`, `record.rejected`, `record.retried`), each step of a load and each step of a `$bulk-submit` submission, to an `events` table, and serves it oldest first by id at `GET /api/events` with filters and an opaque cursor. An event is written in the same transaction as the change it reports, and only for rows that change: every outcome is a guarded update (the status it leaves and, for a record, the revision it had when the work was selected, which a person putting a record back bumps, and on REST its attempt count) whose returned rows give the events. A submission opens only if it claims exactly the records its manifest was built for, and every write that follows one of its requests (progress, failure, refusal, receipts and settling) holds only at the attempt count read before that request, so an answer to an older poll, from a runner that held the source before its lease lapsed, changes nothing. Admission is one transaction (the file row, its records or its unreadable state, and their events), so a failure leaves nothing and the next scan takes the object again; a source's admissions take turns (on PostgreSQL, a lock per source) and look the content up again, so an instance that took a source over cannot record the same object twice. A load is finished, and a Retry or reprocess put back, under its file's row lock, so the two never miss each other. On PostgreSQL every event insert takes one advisory lock, keyed by the store's schema and held to commit, after every other change of its transaction. One setting, `retention.history` (30 days by default, from 1 to 3650 days), purges events and stored alerts, at start and hourly, in chunks; open alerts the engine still tracks are kept, and so is the newest alert, so an alert id is never given again on SQLite or PostgreSQL (event ids never are either: `AUTOINCREMENT` on SQLite, a sequence on PostgreSQL); purged alerts are added to a tally `/metrics` counts. Rule ids are never given again either (a SQLite `alert_rules` made before is rebuilt once with `AUTOINCREMENT`), and each rule answers when it was made. `GET /api/sources/{id}/failures` counts distinct rejected records per UTC day from the log, saying for each window whether the log covers it: from when this store began recording events, within the retention. Records, loads and submissions are not purged.

**Why:**
- The engine kept each record's current status only. A retry cleared the earlier failure, so nobody could count failures over a period or say what happened to a record; an external monitoring tool needs that history through the API, not the database.
- In the same transaction as the change, the log can neither miss an outcome nor report one that was not stored; an outcome stored before the next record is sent (decision 36) has its event stored too.
- Guarding each outcome by status and revision means a slow transform or delivery that finishes after a person corrected the record changes nothing and logs nothing, instead of overwriting the correction.
- Ids in commit order let a reader poll with a cursor and never skip an event: a SQLite store has one writer at a time (`BEGIN IMMEDIATE`), and on PostgreSQL the lock makes event ids commit in order across instances. Taking it last means a holder never waits for a row lock, so it cannot deadlock with a finish or a Retry elsewhere.
- One retention for every kind of history keeps one setting to explain; the module is where later kinds (check results, alert delivery state) add their purge.
- Never purging the newest alert keeps SQLite from giving a purged alert's id again, without rebuilding the table; the purged tally keeps `hie_alerts_total` a counter.

**Cost / not yet:**
- One more write per decided record, batched per REST batch and per settled page; on the bulk path a million-record submission writes a million events.
- On PostgreSQL, writers of events across instances take turns on one lock, held through each insert to commit; a file with many unparsable rows, or a large refusal, holds it while its events go in.
- The purge runs hourly, so history can outlive the setting by up to an hour; instances sharing a store purge to the shortest setting among them.
- Records, loads and submissions are still not purged (decision 5 keeps a records retention open).
- The newest alert is always kept, whatever its age. An open alert of a rule whose source was deleted while it was open stays until the rule is deleted.
- An outcome computed for a superseded revision of a record is discarded, and the record is transformed and sent again; so is a submission whose records changed while it was built.
- A zip entry whose content turns out to be taken already, after it was staged in the source's store for `$bulk-submit`, leaves that copy under the store's `unpacked/` prefix: the source has no way to remove one object, and the name is not used again.
- A transform that cannot finish a record now is logged once per record and reason while its runner runs, not on every pass, so a long wait on a terminology server is one event per record, not one per poll. A record reprocessed while held by a submission keeps that submission, as before, and is not picked up again by the bulk path.
- `at` is the writer's clock and may be out of order with `id`; the log is ordered by `id`. A submission partly settled before the upgrade counts only the records settled after it.

## 39. Alerts are read by period and in pages, with a cursor on their id

**Chosen:** `GET /api/alerts` takes `since`, an RFC 3339 instant that keeps the alerts created at or after it, and `cursor`, the previous page's `next_cursor`. Alerts stay newest first by id; a page is the next `limit` alerts (100 by default) with an id below the last one returned, read with one row more than the page so that `next_cursor` is `null` exactly when nothing more matches (or `limit` is `0`, an empty page). The cursor is opaque: it carries the last id behind a mark of its form's version (`a1.`, as the event log's is `e1.`), and a bare id is refused. `source` and `open` are unchanged and combine with both. A query string that cannot be read answers `400 invalid_query`, with `field` for `since` and `cursor`, in a fixed sentence. A negative `limit` now means every matching alert on PostgreSQL too, as it always did on SQLite.

**Why:**
- A monitoring tool reads every alert of a period (the last seven days, say). With only the newest `limit` it could not tell whether it had them all.
- A cursor on the id is stable without a snapshot: new alerts take higher ids, so they never shift a walk in progress, and an alert cleared or purged between pages moves no other. An offset would skip or repeat rows whenever alerts arrive.
- `since` filters on the creation time but does not order: ids and clocks can disagree by a few milliseconds between instances, and paging stays correct either way. Rounding a finer instant up keeps "at or after" true against stored milliseconds.
- The bound on the id lets SQLite seek on the primary key; the `IS NULL OR` form other filters use made it scan.
- An opaque cursor leaves room to change it: the mark tells a later form apart, and a client cannot take an alert id for a cursor.

**Cost / not yet:**
- The rule is per page, not a snapshot: a walk returns every alert that existed when it began and still matches, once each; an alert created meanwhile may or may not appear. On PostgreSQL an id is taken before its row commits, so an alert can commit behind a page already read; a new walk finds it. A page after a cursor can be empty when the alerts it would have held left in the meantime.
- There is no index on the creation time: `since` stops nothing early, and a period far in the past reads back through newer alerts. Alerts are few, and retention (38) bounds them.
- `/api/alerts` keeps its typed parameters, so a value of the wrong type or given twice is refused without `field`, and an unknown parameter is ignored, unlike `/api/events`, which names the field of every refusal. `/api/records` and the loads lists keep the extractor's plain-text `400`.
## 40. The alert webhook is a signed, retried and recorded contract

**Chosen:** Every change of an alert, `fired` and `cleared`, is an event with a random `event_id`, written with its exact body in the same transaction as the alert, in the store's `alert_webhooks`. The body is `{event_id, event, alert: {id, rule_id, kind, message, notify, created_at, cleared_at, cleared_reason}, source: {id, name}, text}`. With `alerts.webhook_secret` (or `HIE_ALERT_WEBHOOK_SECRET`) set, `X-HIE-Signature: sha256=<hex>` carries the HMAC-SHA256 of the raw body. A sender task per instance posts waiting events oldest first: a 2xx delivers; anything else, a 3xx included (redirects are not followed), is retried up to five times after `alerts.webhook_backoff_ms` (10 s), doubling, then the event is `failed`. Each attempt is claimed with one conditional update, for a lease of 60 s, and its outcome is stored only while the claim is still that attempt's; a last attempt whose outcome was never stored ends `failed`. A `cleared` event waits for its `fired` event for the same URL to be delivered or given up on. Each event belongs to the URL configured when it was written and waits for that URL. `GET /api/alerts` shows, per alert, the delivery of its latest event (`event`, `state`, `attempts`, `last_error`, `last_status`, `delivered_at`), and `webhook: {configured, signed}` for the instance. Events not requested by the rule's channels, or written with no URL configured, are recorded as `not_requested` and `not_configured` and never sent.

**Why:**
- External monitoring and paging tools are meant to receive the engine's alerts through its webhook, so it needs a stable shape, a way to prove the sender, and delivery that survives a receiver's outage and the engine's restarts. A host application shows whether an alert reached them, and needs to tell "no webhook configured" from "not delivered yet".
- Writing the event with the alert, in one transaction, means no alert change is lost between the two; storing the exact body means every retry is the same bytes, so a receiver de-duplicates by `event_id` and a signature never depends on when it was computed.
- A body-only HMAC (the shape GitHub's webhooks use) is the simplest scheme receivers verify. A replayed request carries an `event_id` the receiver has seen; an old event names an old alert.
- Claiming by `event_id` with a conditional update, and fencing the outcome by the attempt number, lets several instances on one store send without sending one attempt twice, on SQLite and PostgreSQL alike, and keeps a stalled instance from overwriting a newer outcome.
- Binding an event to its URL keeps alert text and recipients from going to a receiver configured later for something else; waiting for that URL keeps a brief misconfiguration from losing them.
- Redirects are refused because a redirected POST becomes a GET without the body, and would carry the signature to another host.

**Cost / not yet:**
- The body changed: the old `feed`, `kind`, `alert_id`, `notify` and `cleared` keys are gone (`text` stays), and a clear is now posted only when the channels recorded on the alert when it fired include the webhook (a later edit of the rule does not change that; a rule turned off or deleted leaves its open alert open, with no clear). `alerts:` refuses unknown keys, so a misspelt `webhook_secret` cannot send unsigned; a file with stray keys there no longer starts. While instances of an earlier version share the store, those still post the old body, unsigned. In the library, an `Alerter` now only records events: they are sent by the task `Alerter::start` returns, which `build` keeps with the source manager.
- A receiver can still get an event twice, and a late `fired` after its `cleared`; it must de-duplicate. `event_id` covers retries of one event, not a second alert raised for one condition. Receivers match the two events of an alert by its `id`, which is never given again (decision 38).
- No timestamp is signed: a receiver that forgets the `event_id`s it has seen accepts a replay of an old event. Use HTTPS.
- Sends are sequential, one per instance at a time: with a receiver that times out, waiting events queue behind each other. Claims and due times use each instance's clock, as source leases do: instances on one store need synchronised clocks. All instances on a store need the same `alerts:` settings.
- The API shows the latest event only: a `fired` that failed is shown until its `cleared` is written. A monitor tracking delivery re-reads the alerts it still follows; delivery changes do not move a cursor. Events are kept with their alert and removed with it when `retention.history` purges it (decision 38), waiting ones included: by then only an event parked for an earlier URL, or never sent, can still be waiting.
- No mail sink, per-rule URLs, redelivery endpoint or delivery metrics.

## 41. Check now has an id and an outcome

**Chosen:** `POST /api/sources/{id}/check-now` records a check in the store and answers its `check_id`; `GET /api/checks/{id}` answers `pending`, `found`, `nothing_new` or `failed`, with when it was asked for and answered. A check is recorded with a baseline, the source's newest load at that moment, and is answered by the first scan of the source that starts after it was recorded, on the instance holding the source: each pass reads the source's pending checks just before its scan and answers exactly those when the scan ends, before delivering. `found` lists the source's loads after the baseline up to the end of that scan, read with their current status when asked; `nothing_new` when there are none; `failed` when the source is paused, its credential is not loaded, the scan failed, or the source was deleted (unless another instance held it then), with one of a fixed set of reasons and the loads taken before the failure. The first answer stays. Checks are purged with events and alerts after `retention.history` (decision 38), counted from when they were asked for, answered or not. The route keeps its name; nothing new is needed to ask for a check.

**Why:**
- A monitoring tool that asks for a check needs to know whether it ran and what came of it, not only that it was accepted: "did my file get picked up" has an answer per request.
- The baseline makes the answer what the source took from the request on, not what one scan did: a file taken by a scan already running when the check came, or by a scan that crashed, was aborted while its source was replaced, or could not store its answer, is still in the answer the next scan gives. The pass reads the pending checks as an explicit set rather than up to an id, so a check another instance commits late is never answered by a scan that started before it.
- Fixed reasons: a scan error can hold a signed URL, a query, a cursor or a server's answer; an endpoint any reading token can call must not show them. The log keeps the error.
- Ids from `AUTOINCREMENT` on SQLite (a sequence on PostgreSQL): a purged check's id is never handed to another, so an old `Location` cannot show someone else's check.

**Cost / not yet:**
- A check sent to an instance that stands by waits for the holder's next poll: the pass it starts is local. A check whose source is gone from every instance without being deleted through the API (removed from disk while the engine was down), or held by an older engine that does not know checks, stays pending until purged.
- A pass that outlives its lease (longer than three poll intervals, at least 30 s) can overlap another instance's pass, as before: a check is then answered by whichever finishes first, and both list real loads of the source, but on PostgreSQL a load the other instance commits late may be left out of the answer, or appear in an answered check's `files` after `file_count` was counted.
- A delete fails the source's waiting checks unless another instance held the source at that moment; on PostgreSQL, an instance that takes the source over while the delete runs may find its checks already failed as deleted.
- The `files(feed_id, id)` index is built at the first start of this version: on a large PostgreSQL store, other instances' new loads wait for it once.
- A pass fails the waiting checks when the source is paused both before and after it reads them; a resume in between leaves them to the pass the resume starts. A pause that lands while the pass loads its credential fails the checks it reads and ends the pass; one that lands after the checks were read does not stop that pass's scan, which answers them.
- No list of a source's checks; a caller keeps the ids it was given.

## 42. Loads are listed across sources, newest first in pages, with `failed` derived from their rows

**Chosen:** `GET /api/loads` lists the loads of every source, newest first by id, with the fields of the per-source list (and `GET /api/loads/{id}`), which all gain `failed`, `failing` (the rejected rows that are not `filtered`, the count `failed` is decided on) and `source_name`. `source` keeps one source id; `status` keeps one stored status (`processing`, `unpacked`, `done`, `unreadable`, `duplicate`) or `failed`: a load that is `unreadable` or has at least one row rejected, for any reason but `filtered`, when the page is read, whatever its status. A page is at most 200 loads (50 by default) below the previous page's `next_cursor`, an opaque cursor on the id in the form `/api/alerts` uses; `next_cursor` is `null` exactly when nothing more matched. The page, whether more follow, and the counts come from one statement. Each way of choosing a page reads an index: `files(feed_id, id)`, `files(status, id)`, `files(feed_id, status, id)`, and two partial indexes on records holding only the failing rows (rejected and not `filtered`; `(file_id)` and `(feed_id, file_id)`), from which a recursive query takes the failed loads one seek each. The runner keeps the source's name with each load it records (`files.source_name`). A deleted source's loads stay listed under their id and name; a source created again with that id takes over everything kept under it, loads included. A query string that cannot be read answers `400 invalid_query`, with `field` for `limit`, `status`, `source` and `cursor`.

**Why:**
- A monitoring tool needs every source's loads in one list, and to page through them; per source and only the newest 50, it could neither see them together nor tell whether it had them all.
- There is no failed load status: a load is `done` once every row is sent or rejected, whatever the share rejected, and rows are rejected while the load is still in progress. "Failing now" is what a person acts on, so it is derived from the rows when the page is read: a load leaves `failed` as soon as a Retry or reprocess leaves it no rejected row, and a row rejected later puts it back. Storing a flag would need every path that rejects or puts back a row to keep it.
- The cursor on the id is stable without a snapshot, as for alerts (39): new loads take higher ids and never shift a walk, and a load that leaves a filter between pages moves no other.
- One statement per page, so a page never holds a load that stopped matching between two reads, and its counts and `next_cursor` agree.
- A `filtered` row is a transform turning a row into nothing on purpose (a row of another kind, a test record): it is counted as rejected, as before, but a person has nothing to fix, so it does not make a load fail.
- Failing rows are few and the loads with any are fewer, so a partial index holds only them and changes only when a row starts or stops failing. Seeking from one failed load to the next stops a page from reading every rejected row of an older load, or walking every load, when failures are sparse.
- A source id is the only identity a source has, in the store and across instances that may each hold their own definitions. A marker of deletion kept in the store could not tell which definition recorded a load once two instances, a renamed definition file, or a delete that fails halfway disagree; the name kept with each load, and the documented take-over, say what the engine knows.

**Cost / not yet:**
- The rule is per page, not a snapshot: a load created during a walk may or may not appear (on PostgreSQL an id is taken before its row commits), and with `failed` or a changing status a load can start or stop matching between pages.
- The counts of a page read every row of each of its loads, in one call that holds the store; 200 bounds the number of loads, not their rows. The per-source list has the same cost with no bound.
- A reused id shares history: the new source's lists, stats, pause state, rules, alerts, cursors, unfinished rows and deduplication include the earlier source's. A new source should get a new id. Loads recorded before this version have no `source_name`.
- The first start of this version builds the new indexes; each partial one reads `records` once. On a large store that is a pause at start (see [running](running.md#the-store-and-more-than-one-instance)).
- `limit` here is 1 to 200; `/api/alerts` keeps its older meaning of `0` and negative values. There is no `since` on loads; other parameters are ignored.

## Not in the prototype

- Connectors besides a folder, S3, another FHIR server, a SQL database and MLLP: MongoDB, Oracle.
- HL7 v2 batches (`BHS`/`BTS`), outbound MLLP, TLS on the listener.
- Code translation through HTS (`ConceptMap/$translate`) inside transforms.
- Patient matching (source identifier to `Patient` reference).
- Authentication to HFS with a private-key JWT (SMART backend services); client credentials are in.
- A UI, FHIR Mapping Language, record-level parallelism within one source.
