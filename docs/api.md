# API and authentication

The HTTP API clients and UIs use, and how it and the engine's own calls to HFS are authenticated. Back to the [README](../README.md).

## API

| Method and path | Does |
|---|---|
| `GET /api/sources` | Every source: definition, `contact` (or `null`), state (`waiting`, `receiving`, `late`, `paused`), freshness, record counts, last file, last alert, and `stamping`: whether its deliveries carry the source tag and since when what it delivered did (see [Tagging delivered resources with their source](running.md#tagging-delivered-resources-with-their-source)) |
| `POST /api/sources` | Creates a source from its definition (the YAML as JSON); writes `sources/{id}.yaml` and starts it. It is created with the default `silence` rule, unless its id still has one. `409` if it exists, `400` with the reason if invalid (`code: invalid_definition`, also when the credential it names does not exist, and in a build without a secret backend, where no retry can help). When it names a credential and the store has no usable key: `503 secrets_unavailable`, and the same request works once the key is fixed and the engine restarted. `503 store_busy` when the store's lock was not free, with nothing written |
| `POST /api/sources/test` | Tests a connection: `{"source": {...}}` or a full definition. Answers `reachable`, `elapsed_ms`, `objects`, `newest`, `newest_at`. An S3 source with a `credential` is tested with the credential's current revision; with no secret store, whether the key is missing or unusable or the build has no secret backend, it answers `503 secrets_unavailable` |
| `GET /api/sources/{id}` | One source, as above |
| `PUT /api/sources/{id}` | Replaces the whole definition and restarts the source, without adding a rule: an optional member left out takes its default or none, and a body without `contact` leaves the source with none, so a client changing one member sends back the `definition` it read with that change. A definition is refused as `POST` refuses it: `400 invalid_definition` (in a build without a secret backend too, when it names a credential), or `503 secrets_unavailable` when it names a credential and the store has no usable key |
| `DELETE /api/sources/{id}` | Stops the source and removes its file `sources/{id}.yaml`; its loads, records, alerts and rules stay (see [Loads](#loads) for a source created again with the same id), except its open `delivery` and `credential` alerts, which only its runner would clear: once its runner has ended they are cleared with the reason `<id>: the source was deleted` (not while another instance holds the source, which still runs its own definition), and the stopped runner raises no more of them, even for a request that fails after the delete. A source created again with the id raises its own outage alert even if one from before is still open; its first successful delivery clears both. Its checks still pending fail (unless another instance holds the source); `503 store_busy` when the store's lock was not free, and the source is left as it was |
| `POST /api/sources/{id}/pause` | Stops polling and delivery until resumed; body `{"by": "..."}` is kept with the time |
| `POST /api/sources/{id}/resume` | Resumes and passes at once |
| `POST /api/sources/{id}/check-now` | Passes now instead of at the next poll, and records a check of the source: `202 {"id", "checking": true, "check_id"}` with `Location: /api/checks/{check_id}`. `503 store_busy` when the store's lock was not free. See [checks](#checks) |
| `GET /api/checks/{id}?cursor=&limit=` | What a check found: `pending`, `found` (with the loads), `nothing_new` or `failed` (with the reason), and when it was asked for and answered. `404` for an unknown or purged check; an id that is not an integer answers `400`, a parameter that cannot be read `400 invalid_query` |
| `GET /api/sources/{id}/loads?limit=50` (also `/files`) | Its newest loads (`limit` 1 to 1000, 50 by default; another value answers `400 invalid_query`, `field: limit`. **Breaking since 0.2.0**, which took any `limit`, and listed every load for a negative one on SQLite): the file, its method, records read, sent, rejected (and `failing`, those that fail it), in progress, how long it took, where it is archived, whether it is `failed` and the `source_name` it was recorded under ([below](#loads)) |
| `GET /api/loads?status=&source=&limit=50&cursor=` | The loads of every source, newest first, a page at a time, with the same fields; `status=failed` keeps the loads failing now ([below](#loads)) |
| `GET /api/loads/{id}` | One load, with the same fields, and its rejected rows **grouped by reason** in `failures`: kind, reason, count, first line, and up to three sample rows each. The rows its transform filtered out on purpose are a group too, with kind `filtered`, so they can be read and retried by reason; whether the load fails is `failed` and `failing` |
| `POST /api/loads/{id}/retry` | Puts the load's rejected rows back at the start, all of them or those with one reason (`{"reason": "..."}`); the source passes at once |
| `GET /api/sources/{id}/stats?days=14&tz=Europe/Madrid` | What a source's page draws: today (its `day`, files, records, sent, rejected, failing, pending) and one row per `day` (files, sent, rejected, failing), oldest first, today last. `rejected` counts every rejected row; `failing` leaves out the rows a transform filtered out on purpose (rejected as `filtered`, by returning no resources). Days are local days in `tz` (UTC without it), and the answer's `tz` names the zone they were counted in: see [Days and time zones](#days-and-time-zones) |
| `GET /api/sources/{id}/failures?days=14&tz=Europe/Madrid` | Distinct records the source rejected over the last 24 hours and each of the last `days` local days in `tz` (1 to 90; UTC without it), from the event log, with how far the log covers each; each day names its `day`, and the answer's `tz` the zone (see [Rejected records over a period](#rejected-records-over-a-period) and [Days and time zones](#days-and-time-zones)) |
| `GET /api/transforms` | Every transform file: name, path, size, modified, and the sources that use it |
| `GET /api/transforms/{name}` | The transform's code |
| `PUT /api/transforms/{name}` | Saves the code (`{"code": "..."}`); the sources using it restart their Python worker. `201` when new |
| `POST /api/transforms/{name}/try` | Runs the transform on `{"records": [...]}` (up to 100) without loading: per record, the resources it became or the rejection reason. At most four tries run at once; `503` when one cannot start within 10 s |
| `GET /api/monitoring/status` | What the monitoring page opens on: `needs_you` (late, paused, or failing sources with the reason and their `contact`), `last_24h` (files, sent, rejected, failing, alerts), `destination` (whether the FHIR server answers, see [The destination](#the-destination)), every source with its `rejected_last_24h`, `failing_last_24h` and `last_alert` (`kind`, `message`, `created_at`, `cleared_at`). A source is failing when it has failing rows (see `/stats`) in the last 24 hours; the reason still reads "N records rejected in the last 24 hours", N counting failing rows. Its figures cover the last 24 hours, not calendar days, so it takes no `tz` |
| `GET /api/records?source=&status=&file_id=` | Records; `status` is `received`, `pending`, `sent` or `rejected` |
| `GET /api/records/{id}` | One record: file, line, original and corrected row, resources sent, HFS response |
| `POST /api/records/{id}/reprocess` | Runs a record again; body `{"raw": {...}}` replaces the row and keeps the original |
| `GET /api/alerts?source=&open=true&since=&limit=100&cursor=` | Alerts, newest first, each with its rule, recipients, channels, when and why it cleared, and `webhook`: the delivery of its latest webhook event (`event`, `event_id`, `state`, `attempts`, `last_error`, `last_status`, `delivered_at`), and in `webhook.events` every event of the alert with the same fields, oldest first (a `fired` still parked or failed stays visible after its `cleared` is delivered); null for alerts from before events were recorded; `open=true` lists only those not cleared. `since` and `cursor` read a period page by page (see [Reading alerts by period](#reading-alerts-by-period)). Kept for `retention.history` (see [Alerts and retention](#alerts-and-retention)). Also `webhook: {configured, signed}`, this instance's webhook settings (see [the alert webhook](sources.md#the-alert-webhook)) |
| `GET /api/events?source=&kind=&since=&cursor=&limit=` | The event log, oldest first: each record's attempts and outcomes, each load's steps, each submission's steps (see [Events](#events)) |
| `GET /api/alert-rules?source=` | Every rule, each with its `created_at`; `POST` creates one (`400` names what is wrong with it). `created_at` is set by the engine, never by the body. A rule id is never given again, like alert and event ids. A `missing_by` rule may name a `tz` (see [sources](sources.md#alerts-and-rules)) |
| `GET` / `PUT` / `DELETE /api/alert-rules/{id}` | One rule: read, replace, remove. A `PUT` that turns the rule off (`enabled: false`) or changes its `condition`, `threshold`, `tz` or `source_id`, and a `DELETE`, clear the rule's open alerts in the same transaction, each with its webhook `cleared` event; a change of `notify` or `channels` alone does not |
| `GET /api/secrets` | Every stored credential, by name, without its value |
| `POST /api/secrets` | Stores a credential: `{"name", "kind": "aws", "value": {"access_key_id", "secret_access_key"}}`. `201` |
| `GET /api/secrets/{id}` | One credential, without its value |
| `PUT /api/secrets/{id}` | Replaces its value with a new revision: `{"value": {...}}`. Sources use it from their next pass |
| `DELETE /api/secrets/{id}` | Removes it. `409` with `sources` while a source is registered for it |
| `GET /metrics` | Prometheus text: records by status, last received time, paused flag, alerts by kind (`hie_alerts_total`, which counts purged alerts too and never goes down) |
| `GET /health` | `{"status": "ok", "service": "hie", "version", "uptime_seconds", "started_at", "timestamp", "secrets"}`; `secrets` is `{"backend": "native"\|"none", "available", "key_id"}`, the key id being a one-way 16-hex fingerprint of the key in use, or `null` |

A source's top-level `contact` and `name` are those of the definition it runs, normalised (a blank contact is `null`); `definition` is its file as saved, as written. The two views can differ: after `500 source_not_started` until the saved definition starts, after a hand edit of the file until the next start, and for a blank contact written by hand.

Errors answer `{"error": "<sentence>"}`. Where a client needs to tell cases apart they also carry a stable `code`, and sometimes more members: `invalid_definition` (a source definition that cannot run), `source_not_started` (`500`: the definition was saved, but its source could not be started; it starts with that definition at the next start of the engine), `store_busy` (`503` with `Retry-After: 1`: the store's lock was not free; try again), `invalid_query` (`400`: a query parameter of `/api/events`, `/api/alerts`, `/api/checks/{id}`, `/api/loads`, `/stats` or `/failures` that cannot be read or used, such as an unknown `tz`, with `field` as their sections say; the sentence never repeats what was sent), and the codes of the credential routes below.

### Days and time zones

Daily figures (`/stats` and `/failures`) are counted in UTC unless the request names a zone with `tz`, an IANA name such as `Europe/Madrid` or `America/New_York` (matched without regard to case; a `+` is sent as `%2B`, as in `Etc/GMT%2B5`). The answer's `tz` names the zone used, as the database spells it, and is `UTC` when none was asked for; a client that wants its own zone's days checks it before labelling them. A name that is not a zone answers `400` `{"error": "tz must be an IANA time zone name, such as Europe/Madrid or UTC", "code": "invalid_query", "field": "tz"}`; on `/stats`, a `tz` given twice is refused before that, with a plain-text `400`; on `/failures`, with `invalid_query`.

- A day runs from local midnight to the next local midnight. When a change of offset (such as the start of daylight saving time) skips midnight, the day starts at the change, the first valid moment after it; when one repeats midnight, the day starts at the first one.
- So a day with a change lasts as long as it did on the clock (23 or 25 hours in most zones, 23½ on Lord Howe Island), and a date the zone skipped altogether is a day with nothing in it.
- Zone rules come from the IANA database built into the engine, the same on every instance and never the host's; a release brings newer rules.

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
| `load.finished` | Every record of the load is sent or rejected | `records`, `sent`, `rejected` (filtered rows included), `failing` (the rejected rows that are not `filtered`; absent from events written before this field, or by an earlier version sharing the store); `container: true` for a zip |
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
- `kind`: one or more kinds or families (`record`, `load`, `submission`), separated by commas. A family stands for the kinds of it this version knows, those in the tables above.
- `since`: an RFC 3339 instant (`2026-11-20T08:00:00Z`). Only events stored at or after it (`at >= since`) are listed, with or without a cursor. Events are stored to the millisecond, so a finer instant counts from the next millisecond. Without a cursor, the reading starts at the lowest id among the events from `since` to five minutes after the first of them, found through the index on `at`: an event stored after another may carry an `at` up to five minutes behind it (the clocks of two instances) and is still listed. With a cursor, the reading starts at the cursor: `since` then only filters, so a poller following `next_cursor` lists every event stored after it at or after `since`. An event whose instance's clock was further ahead than that, stored before the first event at or after `since`, is left out of that request and of the pages that follow from it. Instances sharing a store already need clocks that agree to well within that: a source's lease lasts 30 seconds at least.
- `cursor`: the `next_cursor` of an earlier answer, to continue after it. Its text is opaque (today `e1.` and a number). Give the same filters again with it. A cursor stays valid after its event is purged.
- `limit`: 1 to 1000, 100 by default.

```json
{"events": [ ... ], "next_cursor": "e1.41", "has_more": false}
```

A request whose filters no index serves in id order (`since`, a `source` with a `kind`, or several kinds, a family included) reads at most 50 000 events of the log past where it starts, whatever its filters keep, so a request on a large log never holds the store for long. No filter, a `source` alone and one exact kind alone read through an index and stop at `limit`. `has_more` says to ask again now: the page is full, or the request stopped at the end of what it reads before the newest event, **possibly with no event listed**; keep reading while `has_more` is `true`, and stop on `false`, not on an empty page. `next_cursor` is where the next request continues: after the last event listed when the page is full, after the last event read when the request stopped there, otherwise the newest position of the log when the request was read, or the cursor given if that is later. A poller whose filters matched nothing still moves forward, and a cursor never moves back, even after a purge. It is `null` only when the log is empty and no cursor was given. Ids are given in commit order (on PostgreSQL every instance writes events under one lock, in turn), and a page never lists past the newest id committed when it was read, so polling from `next_cursor` never skips an event.

Every parameter that cannot be read is refused with `400`, `code: "invalid_query"` and `field` naming it: a value that does not parse (`limit` is digits only), a parameter given twice, or one that is not `source`, `kind`, `since`, `cursor` or `limit`.

A poller pages with `cursor`: the first request may give `since`, and each next one the `next_cursor` it got (with the same filters). A `since` without a cursor starts near its period; with a filter that reads a span at a time, its first requests may still come back empty with `has_more`, so follow `next_cursor` rather than asking with `since` again. `/api/events` is in no release yet: these rules break no released client.

### Rejected records over a period

`GET /api/sources/{id}/failures?days=14&tz=Europe/Madrid` counts, from the event log, the distinct records a source rejected in each window: a record rejected more than once in a window counts once.

```json
{"source_id": "lab-results", "tz": "Europe/Madrid", "covered_from": "2026-11-12T09:30:00.000Z",
 "last_24h": {"from": "...", "to": "...", "records": 3, "filtered": 1, "complete": true},
 "days": [{"day": "2026-11-07", "from": "2026-11-06T23:00:00.000Z", "to": "2026-11-07T23:00:00.000Z",
           "records": 0, "filtered": 0, "complete": false}, ...]}
```

- `last_24h` is the 24 hours before the request, whatever the zone; `days` are local days in `tz` (UTC without it; see [Days and time zones](#days-and-time-zones)), oldest first, the last one today (still running), each named by its `day`, and the answer's `tz` names the zone they were counted in. A window holds what was stored from its `from`, included, to its `to`, left out: an event at midnight belongs to the day that starts then.
- `records` counts records rejected as `parse`, `data`, `validation`, `transform` or `server`. Records a transform filtered out on purpose (`filtered`) are not failures and are counted apart, in `filtered`.
- `covered_from` is where the log's history starts: when this store began recording events (its first start with a version that keeps them), this instance's retention limit, or the latest cutoff any instance of this version on the store purged events under (one with a shorter retention may share the store), whichever is later. It is read after the counts, so a purge while they ran makes it later rather than leaving a window marked complete. A purge by an earlier version is not recorded. A window is `complete` when `covered_from` is at or before its `from`. A window that is not complete is still counted over what is kept, and says `complete: false`.
- `days` is a whole number from 1 to 90 (14 by default). A value that cannot be read, an unknown `tz`, a second `days` or `tz`, or any other parameter answers `400 invalid_query` with `field`. An unknown source answers `404` with `{"error": "source <id> not found"}`, as `/stats` does, once the query has been read.

### Alerts and retention

Stored alerts follow `retention.history` too. An alert is purged that long after it cleared, or after it was raised if it is still open, except:
- open alerts of a rule that exists, is enabled and was created before the alert (on SQLite a deleted rule's id may be given to a new rule);
- open `credential` and `definition` alerts, which the engine clears itself;
- open `delivery` alerts without a rule: an outage still going on, which its recovery clears;
- the alert with the highest id.

Turning a rule off, deleting it or changing what it watches clears its open alerts (see the rules above); other open alerts without a rule are purged by age. An alert id is never given again after a purge: the newest alert is always kept, so a SQLite store never reuses one, and PostgreSQL ids come from a sequence. Rule ids are never given again either: SQLite rules take their ids from `AUTOINCREMENT` (a store made before is rebuilt to it once, at its first start with this version), PostgreSQL's from a sequence. `hie_alerts_total` in `/metrics` adds the purged alerts to the stored ones, so it never goes down. A purged alert's [webhook events](sources.md#the-alert-webhook) go with it, any still waiting included.

### Reading alerts by period

`GET /api/alerts` reads every alert of a period by following a cursor:

```
GET /api/alerts?since=2026-11-13T00:00:00Z&limit=100
→ {"alerts": [...], "next_cursor": "a1.8421", "webhook": {...}}
GET /api/alerts?since=2026-11-13T00:00:00Z&limit=100&cursor=a1.8421
→ {"alerts": [...], "next_cursor": null, "webhook": {...}}
```

- **`since`** keeps the alerts created at or after an RFC 3339 instant (`Z` or an offset, with `+` sent as `%2B`). Alerts are stored to the millisecond, so a finer instant counts from the next millisecond; more than nine fraction digits and leap seconds (`:60`) are refused. Alerts purged by `retention.history` are no longer listed.
- **`next_cursor`** is set when more alerts match, and `null` on the last page, so a reader stops there without asking again. It is opaque: pass it back as `cursor` with the same `source`, `open`, `since` and `limit`; it does not carry them. Empty `since=` and `cursor=` are the same as leaving them out.
- **`webhook`** (`configured`, `signed`) is on every page, the empty one included: this instance's [webhook](sources.md#the-alert-webhook) settings, not part of the paging.
- **`limit`** is 0 to 1000, 100 by default. `0` answers an empty page with `next_cursor: null`, whatever matches. A negative value or one above 1000 is refused (`field: limit`); read every alert by following `next_cursor`. (Until this version a negative value listed every alert on SQLite, and answered `500` on PostgreSQL.)
- **While a reader pages**, the order holds: a walk returns, once each and newest first, every alert that existed when it began and still matches when its page is read. An alert created meanwhile may or may not appear; a new walk finds it. An alert that clears, or is removed, between two pages leaves the others where they were: with `open=true` it simply no longer appears, and the alert a cursor points after need not exist any more. So a page after a cursor can be empty, when the alerts it would have held left in the meantime: stop on `null`, not on an empty page.
- **Refused** with `400 invalid_query`: a `since` that is not such an instant (`field: since`), a malformed `cursor` (`field: cursor`), a `limit` out of range (`field: limit`), and values of the wrong type or given twice (`limit=abc`, `open=yes`, `limit=1&limit=2`, without `field`). The sentence never repeats what was sent. **Breaking since 0.2.0**: `limit` is 0 to 1000; 0.2.0 took any value, listed every alert for a negative one on SQLite and answered `500` for it on PostgreSQL.

### Checks

`POST /api/sources/{id}/check-now` records a check and answers its `check_id`; `GET /api/checks/{id}` says what it found (sent with `Cache-Control: no-store`):

```json
{"check_id": 7, "source": "lab-results", "status": "found",
 "requested_at": "2026-11-20T10:00:00.000Z", "finished_at": "2026-11-20T10:00:00.412Z",
 "file_count": 1, "files": [{"id": 31, "name": "labs.csv", "status": "processing", "duplicate_of": null}],
 "next_cursor": null, "has_more": false, "reason": null}
```

- **Who answers it.** The first scan of the source that starts after the check was recorded, on whichever instance holds the source. A pass reads the waiting checks just before its scan; a check asked for after that is answered by the next pass, which the request starts at once on that instance once the current pass, delivery included, has finished. Sent to an instance that stands by, the check waits for the holder's next poll.
- **`found`** lists the loads the source recorded from the moment the check was asked for until the end of the scan that answered it: duplicates (with `duplicate_of`), a zip and its entries, and files that could not be read are loads too. Each comes with its current `status`; `GET /api/loads/{id}` has the rest. A check covers taking files in, not delivering them: delivery is on the loads.
- **`nothing_new`**: the source recorded no load from the moment the check was asked for until the end of the scan that answered it. Files younger than the source's `min_file_age_ms` are not new, nor are objects a read-only source took before, copies of content a read-only source already took, or a copy whose original still waits for the FHIR server on the bulk path.
- **`failed`**, with a fixed `reason`: the source is paused; its credential is not loaded; listing it, reading an object, taking one in or acknowledging its notifications failed; or it was deleted. The reason never carries the error itself (it can hold a URL, a query or a server's answer); the engine log does. Loads taken before the failure are still listed.
- **`pending`**: not answered yet; `finished_at` is `null` and `files` empty. A check whose source was removed from disk while the engine was down, or sent to an instance whose holder never scans it, stays pending until the retention purge.
- **More than 1000 loads**: `files` holds the first `limit` (1 to 1000, by default 1000), `has_more` says whether there are more, and `?cursor=<next_cursor>` lists the following ones; the cursor is opaque. `file_count` counts them all. An unknown parameter, one given twice, or a value that cannot be read answers `400 invalid_query` with `field`.
- **Timestamps**: `requested_at` is taken by the instance that recorded the check, `finished_at` by the one that answered it (never before `requested_at`); with several instances their clocks may differ.
- **Retention**: checks are kept for `retention.history` (see [Retention](running.md#retention)), purged with events and alerts, from when they were asked for, answered or not, then purged; a purged check answers `404`, and its id is never given to another check. Only loads still stored are listed.

### Loads

A load is one file, object or zip entry a source took, and what became of its rows. `GET /api/loads` lists the loads of every source; `GET /api/sources/{id}/loads` and `GET /api/loads/{id}` answer the same fields:

```json
{"id": 812, "source_id": "lab-results", "source_name": "Lab results", "name": "labs-2026-11-20.csv",
 "status": "done", "failed": true, "records": 120, "sent": 117, "rejected": 3, "failing": 2, "in_progress": 0,
 "received_at": "2026-11-20T08:00:01.120Z", "finished_at": "2026-11-20T08:00:04.870Z", "took_ms": 3750,
 "duplicate_of": null, "size": 8812, "error": null, "source_key": null, "archived": 1, "method": "rest"}
```

- **`status`** is how the engine stores the load: `processing` (rows still to transform or deliver), `done` (every row sent or rejected), `unreadable` (the file could not be read; `error` says why), `duplicate` (the same content was already taken; `duplicate_of` names that load; no rows), or `unpacked` (a zip, while its entries are taken; each entry is a load of its own named `drop.zip!/path`, and the zip becomes `done` with no rows at a later pass).
- **`failed`** is `true` while the load fails now: it is `unreadable`, or at least one of its rows is rejected for a reason other than `filtered`, whatever its `status`, so a load still `processing` fails as soon as one row fails. A `filtered` row is one its transform turned into no resources, on purpose: it counts in `rejected` but does not make the load fail, so a load whose only rejected rows are filtered has `rejected > 0` and `failed: false`. Every other kind fails it (`parse`, `data`, `validation`, `transform`, `server`, or any other). Rows put back by a Retry or a reprocess are not rejected any more: the load stops failing until a row fails again; a Retry limited to one reason leaves it failing while failing rows with another reason remain. Rows waiting for another delivery attempt (the FHIR server could not be reached) are not rejected. A duplicate does not fail because the load it repeats does, and a zip fails only when it could not be read: the failures of its entries are on the entries. A source that cannot be listed or read records no load: it is logged, and the alerts that may follow are those of a listener that cannot start, a credential that cannot be used, and, later, a `silence` or `missing_by` rule.
- **`records`**, **`sent`**, **`rejected`** and **`in_progress`** count its rows by what became of them; **`failing`** counts the rejected rows that fail it, every rejected row but the `filtered` ones, so `failed` is `true` exactly when `failing > 0` or the load is `unreadable`. A load whose only rejected rows are filtered shows `rejected > 0` and `failing: 0`.
- **`source_name`** is the name the source had when the load was recorded, kept with the load: it stays after the source is deleted and does not follow a later rename. `null` when it was not recorded: loads recorded before this field existed, or by an instance of an earlier version sharing the store, and a name with a NUL character, which a PostgreSQL store cannot hold.
- **`method`** is the delivery method of the source now registered under `source_id` on the instance that answers, `null` when there is none.
- **Kept:** loads and their rows are never purged; `retention.history` (see [Retention](running.md#retention)) applies to events, alerts and checks only, so a load stays listed after the events that tell its story are gone.

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
- **`next_cursor`** is set when more loads matched as the page was read, and `null` on the last page. It is opaque: pass it back as `cursor` with the same `status`, `source` and `limit`; it does not carry them. An empty `status=`, `source=` or `cursor=` is the same as leaving it out (an empty `limit=` is refused). There is no `since` here; any parameter but `status`, `source`, `limit` and `cursor` is refused (`?state=failed` would otherwise list every load), a cache-buster such as `?_=1732457000` included: send `Cache-Control: no-cache` instead. `GET /api/loads` is in no release; builds of it before this rule ignored such a parameter.
- **While a reader pages**, each page holds the matching loads with an id below the cursor; which loads, and `next_cursor`, are chosen in one statement, then each load's fields and counts are read in one statement of its own, so they agree with each other but may be newer than the choice (a load chosen under `status=failed` may show `failed: false` if a Retry fixed it in between). The store is let go between two loads: a page of large loads keeps other sources and API calls waiting for about one load's count at a time, not for the page. Following the cursors never returns a load twice, and returns every load that existed when the walk began and still matched when its page was read. Loads recorded after the first page have higher ids and stay outside the walk; a new walk finds them (on PostgreSQL an id is taken before its row commits, so a load can also commit below the cursor and be found by a later page). With `status=failed`, or a status a load leaves (`processing` to `done`), a load can start or stop matching between two pages: one that starts matching above the cursor needs a new walk. A page after a cursor can be empty when the loads it would have held stopped matching: stop on `null`, not on an empty page.
- **Refused** with `400 invalid_query`: a `limit` outside 1 to 200 (`field: limit`), a `source` with a control character (`field: source`), another `status` (`field: status`), a malformed `cursor` (`field: cursor`), a parameter the list does not read (`field` names it when it is letters, digits and `_`, up to 32), and values of the wrong type or given twice (`limit=abc`, `limit=1&limit=2`, without `field`). The sentence never repeats what was sent.

**Deleted sources and reused ids.** A load stays in the store when its source is deleted, and `GET /api/loads` keeps listing it under the `source_id` and `source_name` it was recorded with. A source id is the only key the engine has for a source's history, and everything kept under it stays when the source is deleted. A source created later with the same id takes it over:

- its loads: they appear in the new source's list, under `source=`, and in its stats and monitoring figures;
- its events and failure figures: `/api/events?source=` and `/api/sources/{id}/failures` include the earlier source's;
- checks still pending for the id: the new source's next scan answers them with its own loads;
- its pause state, its rules and its open alerts: a silence rule counts from the earlier source's last load and keeps its threshold, so the new source may be reported silent at once, and its `expect_every` does not replace that rule;
- its read cursors, when the new source reads the same column or resource types: a database or FHIR-server source then continues after the earlier source's cursor rather than from its own start;
- its unfinished rows: rows still received, and rows put back by a Retry of an earlier load, are handled as the new source handles its own: transformed by its transform and delivered to its destination, or, when it hands over source objects, checked and handed over without a transform; rows already transformed and waiting for delivery keep the resources the earlier transform made, and a new source that delivers by REST or by `$bulk-submit` of transformed NDJSON sends them as they are, while one that hands over source objects hands over the load's file, which fails for a file the engine did not keep; submissions still open on the `$bulk-submit` path are followed only if the new source also delivers that way;
- its files, for deduplication: a file or object whose content the earlier source took is recorded as a `duplicate` of the earlier load by a source that moves what it reads, skipped without a load by a `read_only` source (which knows a non-empty file it took by its name and size, so a different file with the same name and size is skipped too), and skipped while the earlier object still waits to be fetched on the `$bulk-submit` path; an entry of a new zip whose content was taken before is recorded as a `duplicate` by either kind.

To keep two histories apart, give the new source a new id. `source_name` and `received_at` tell the earlier loads apart when the name changed. `GET /api/sources` says whether a source is registered under an id now, on the instance that answers; it cannot say whether that is the definition that recorded a given load. `DELETE` removes `sources/{id}.yaml` only: a definition kept under another file name in the sources directory starts again with the engine.

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

**What counts.** No connection, no answer in time, a connection lost before the answer was read whole (on a REST delivery, while its body arrives too), or an answer of `502`, `503` or `504` is a failure. Any other answer, `4xx` (`401`, `403`, `429` included) and `500` included, means the server answers. So `reachable` says the server's HTTP route answers, not that delivery works: an HFS answering `500` to every batch, a batch whose every entry is to be retried, or a token the server refuses all leave it `true`. A token that cannot be fetched records nothing: the state stays what it was (`null` if nothing was observed yet), and the probe, which needs no token, keeps it current. Each source's records, loads and `delivery` alerts say what failed.

**Order.** A request that started before the latest observation does not overwrite it, and a probe gives way to a delivery that started after it. While deliveries see `502` to `504`, time out or lose their connection, a probe that gets an answer does not end the outage (the server may serve `/metadata` and still be unable to store, or take too long to); a delivery's answer does, or the probe's once no delivery has seen any of those for ten minutes. A connection that cannot be made has no such weight: the probe cannot make it either. A source retrying less often than that (a `max_backoff_ms` above ten minutes) can see the state turn `true` between its retries. In that state `observed_at` advances with the probe's failures and the deliveries' retries, which back off up to `max_backoff_ms`.

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
| `hie/read` | Every `GET` except `/api/secrets`: sources, loads, checks, records, transforms, alerts, events, rules, monitoring |
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
