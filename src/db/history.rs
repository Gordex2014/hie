//! The immutable history: every attempt on a row as an event, failure groups,
//! retries as records of their own, and what the usage, the period counts and
//! the destination facts are built from (decision 42).
//!
//! Events are appended, never updated or deleted. They hold who ran an attempt
//! (load, retry, transform version), what happened, and for a failure its kind,
//! reason and location; never row content or a program's output.

use std::collections::BTreeMap;

use anyhow::Result;
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::engine::Conn;
use super::{Db, Param, now_ms, params};
use crate::ids::{self, Kind};
use crate::versions::VersionRef;

/// The DDL of this area's tables.
pub(super) const TABLES: &str = "CREATE TABLE IF NOT EXISTS record_events (
  id INTEGER PRIMARY KEY,
  record_id INTEGER NOT NULL,
  load_id INTEGER NOT NULL,
  feed_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  retry_id TEXT,
  transform_id TEXT,
  transform_version INTEGER,
  error_kind TEXT,
  reason TEXT,
  location TEXT,
  resources INTEGER,
  types TEXT,
  at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS record_events_record ON record_events(record_id, id);
CREATE INDEX IF NOT EXISTS record_events_feed_kind_at ON record_events(feed_id, kind, at);
CREATE INDEX IF NOT EXISTS record_events_kind_at ON record_events(kind, at);
CREATE INDEX IF NOT EXISTS record_events_load ON record_events(load_id, kind);
CREATE TABLE IF NOT EXISTS failure_groups (
  id TEXT PRIMARY KEY,
  load_id INTEGER NOT NULL,
  kind TEXT NOT NULL,
  reason TEXT NOT NULL,
  transform_id TEXT NOT NULL DEFAULT '',
  transform_version INTEGER NOT NULL DEFAULT 0,
  location TEXT,
  created_at INTEGER NOT NULL,
  UNIQUE (load_id, kind, reason, transform_id, transform_version)
);
CREATE TABLE IF NOT EXISTS retries (
  id TEXT PRIMARY KEY,
  load_id INTEGER NOT NULL,
  feed_id TEXT NOT NULL,
  group_id TEXT NOT NULL,
  transform_id TEXT NOT NULL,
  transform_version INTEGER NOT NULL,
  status TEXT NOT NULL,
  rows INTEGER NOT NULL,
  written INTEGER,
  failed INTEGER,
  outcome_unknown INTEGER,
  previous TEXT,
  accepted_at INTEGER NOT NULL,
  started_at INTEGER,
  finished_at INTEGER
);
CREATE INDEX IF NOT EXISTS retries_load ON retries(load_id, accepted_at);
CREATE INDEX IF NOT EXISTS retries_feed_status ON retries(feed_id, status);
CREATE INDEX IF NOT EXISTS retries_version ON retries(transform_id, transform_version);
CREATE TABLE IF NOT EXISTS retry_rows (
  retry_id TEXT NOT NULL,
  record_id INTEGER NOT NULL,
  PRIMARY KEY (retry_id, record_id)
);
";

/// Columns added to this area's tables after a store was first created.
pub(super) const ADDED_COLUMNS: &[(&str, &str, &str)] = &[];

/// A record's outcome now, from its status: `written`, `failed`, `filtered`,
/// `outcome_unknown` or `in_progress`. Needs the record as `r`.
pub(super) const OUTCOME_SQL: &str = "CASE WHEN r.status = 'sent' THEN 'written'
     WHEN r.status = 'rejected' AND r.error_kind = 'filtered' THEN 'filtered'
     WHEN r.status = 'rejected' THEN 'failed'
     WHEN r.uncertain = 1 THEN 'outcome_unknown'
     ELSE 'in_progress' END";

/// One load or retry that ran a transform version, as `GET
/// /api/transforms/{id}/usage` lists it under `runs`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VersionRun {
    /// `load` or `retry`.
    pub kind: String,
    /// The load's id as text, or the retry's `rt_` id.
    pub id: String,
    pub source: String,
    pub load: i64,
    pub rows: i64,
    pub written: i64,
    pub failed: i64,
    /// `running`, `written`, `failed`, `partial` or `outcome_unknown`.
    pub outcome: String,
    /// RFC 3339 with milliseconds (`crate::ids::rfc3339_ms`).
    pub started_at: String,
    pub finished_at: Option<String>,
}

/// What a failure event records besides who ran the attempt.
#[derive(Debug, Default, Clone)]
pub(super) struct Failure<'a> {
    pub kind: &'a str,
    pub reason: &'a str,
    /// The diagnostics' `location`, as JSON text.
    pub location: Option<String>,
}

/// The counts of one period (a day, today, the last 24 hours).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PeriodCounts {
    pub rows_read: i64,
    pub written: i64,
    pub failed: i64,
    pub outcome_unknown: i64,
    pub resources_written: i64,
}

/// What a retry asked for is refused for, in the order the contract checks.
#[derive(Debug)]
pub enum RetryRefusal {
    /// The group's failing rows lost their content (`410 content_expired`).
    ContentExpired,
    /// An unfinished retry covers the group's rows (`409 retry_in_progress`).
    InProgress(String),
    /// The group has no failing rows (`409 nothing_to_retry`).
    NothingToRetry,
    Store(anyhow::Error),
}

impl From<anyhow::Error> for RetryRefusal {
    fn from(e: anyhow::Error) -> Self {
        RetryRefusal::Store(e)
    }
}

/// A failure group as its routes read it.
#[derive(Debug, Clone)]
pub struct Group {
    pub id: String,
    pub load_id: i64,
    pub kind: String,
    pub reason: String,
    /// `None` for rows whose load recorded no version.
    pub transform: Option<(String, i64)>,
    pub location: Option<Value>,
    /// Rows failing in it now.
    pub count: i64,
    pub first_line: Option<i64>,
    /// `kept`, `expired`, `purged` or `none`.
    pub content: String,
}

impl Group {
    pub fn to_json(&self) -> Value {
        json!({
            "group": self.id,
            "kind": self.kind,
            "reason": self.reason,
            "count": self.count,
            "first_line": self.first_line,
            "transform": self.transform.as_ref().map(|(id, v)| json!({"id": id, "version": v})),
            "location": self.location,
            "content": self.content,
            "complete": true,
        })
    }
}

/// Unfinished retry statuses.
const UNFINISHED: &str = "('queued', 'running')";

/// Rows per multi-row statement.
const IDS_PER_STATEMENT: usize = 200;

/// Appends one event of `kind` per record in `ids`, each with who runs the
/// record's current attempt (its retry and version, else its load's).
/// `delivered` events also copy the record's resource count and types.
pub(super) fn append_events(
    c: &mut Conn,
    ids: &[i64],
    kind: &str,
    failure: Option<&Failure>,
) -> Result<()> {
    let now = now_ms();
    for chunk in ids.chunks(IDS_PER_STATEMENT) {
        let marks: Vec<String> = (0..chunk.len()).map(|i| format!("${}", i + 6)).collect();
        let mut values: Vec<Param> = Vec::with_capacity(chunk.len() + 5);
        values.extend(params![
            kind,
            failure.map(|f| f.kind),
            failure.map(|f| f.reason),
            failure.and_then(|f| f.location.clone()),
            now
        ]);
        values.extend(chunk.iter().map(|id| Param::Int(*id)));
        c.execute(
            &format!(
                "INSERT INTO record_events (record_id, load_id, feed_id, kind, attempt, retry_id, transform_id,
                        transform_version, error_kind, reason, location, resources, types, at)
                 SELECT r.id, r.file_id, r.feed_id, CAST($1 AS TEXT), r.run, r.exec_retry,
                        COALESCE(r.exec_transform_id, f.transform_id),
                        CASE WHEN r.exec_transform_id IS NULL THEN f.transform_version ELSE r.exec_version END,
                        CAST($2 AS TEXT), CAST($3 AS TEXT), CAST($4 AS TEXT),
                        CASE WHEN CAST($1 AS TEXT) = 'delivered' THEN r.resource_count ELSE NULL END,
                        CASE WHEN CAST($1 AS TEXT) = 'delivered' THEN r.resource_types ELSE NULL END,
                        $5
                 FROM records r JOIN files f ON f.id = r.file_id WHERE r.id IN ({}) ORDER BY r.id",
                marks.join(", ")
            ),
            &values,
        )?;
    }
    Ok(())
}

/// Appends `kind` events for every record a submission holds (the bulk path:
/// handed to the destination without a confirming answer yet).
pub(super) fn append_submission_events(c: &mut Conn, token: &str, kind: &str) -> Result<()> {
    c.execute(
        "INSERT INTO record_events (record_id, load_id, feed_id, kind, attempt, retry_id, transform_id, transform_version, at)
         SELECT r.id, r.file_id, r.feed_id, CAST($2 AS TEXT), r.run, r.exec_retry,
                COALESCE(r.exec_transform_id, f.transform_id),
                CASE WHEN r.exec_transform_id IS NULL THEN f.transform_version ELSE r.exec_version END, $3
         FROM records r JOIN files f ON f.id = r.file_id WHERE r.submission = $1 ORDER BY r.id",
        &params![token, kind, now_ms()],
    )
    .map(drop)
}

/// The failure group of a load for this kind, reason and version, created on
/// first use with a fresh `g_` id; stable for the load's life.
pub(super) fn group_for(
    c: &mut Conn,
    load_id: i64,
    failure: &Failure,
    transform: Option<(&str, i64)>,
) -> Result<String> {
    let (tid, tver) = transform.unwrap_or(("", 0));
    let find = |c: &mut Conn| -> Result<Option<String>> {
        Ok(c.query(
            "SELECT id FROM failure_groups WHERE load_id = $1 AND kind = $2 AND reason = $3
               AND transform_id = $4 AND transform_version = $5",
            &params![load_id, failure.kind, failure.reason, tid, tver],
        )?
        .first()
        .map(|r| r.string(0)))
    };
    if let Some(id) = find(c)? {
        return Ok(id);
    }
    c.execute(
        "INSERT INTO failure_groups (id, load_id, kind, reason, transform_id, transform_version, location, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
        &params![
            ids::new(Kind::Group),
            load_id,
            failure.kind,
            failure.reason,
            tid,
            tver,
            failure.location.clone(),
            now_ms()
        ],
    )?;
    find(c)?.ok_or_else(|| anyhow::anyhow!("failure group not created"))
}

/// The JSON of an RFC 3339 instant from epoch milliseconds, or null.
fn at(ms: Option<i64>) -> Value {
    ms.and_then(ids::rfc3339_ms_at)
        .map_or(Value::Null, Value::String)
}

fn transform_json(id: Option<String>, version: Option<i64>) -> Value {
    match (id, version) {
        (Some(id), Some(version)) if !id.is_empty() => json!({"id": id, "version": version}),
        _ => Value::Null,
    }
}

impl Db {
    // ---- events ----------------------------------------------------------------

    /// A record's events, oldest first; `None` when there is no such record.
    pub fn record_history(&self, record_id: i64) -> Result<Option<Value>> {
        if self
            .query_one("SELECT 1 FROM records WHERE id = $1", &params![record_id])?
            .is_none()
        {
            return Ok(None);
        }
        let events: Vec<Value> = self
            .query(
                "SELECT at, kind, attempt, load_id, retry_id, transform_id, transform_version, error_kind, reason
                 FROM record_events WHERE record_id = $1 ORDER BY id",
                &params![record_id],
            )?
            .iter()
            .map(|r| {
                let mut event = json!({
                    "at": at(r.opt_i64(0)),
                    "kind": r.string(1),
                    "attempt": r.i64(2),
                    "load": r.i64(3),
                    "retry": r.opt_string(4),
                    "transform": transform_json(r.opt_string(5), r.opt_i64(6)),
                });
                if let Some(kind) = r.opt_string(7) {
                    event["error"] = json!({"kind": kind, "reason": r.opt_string(8)});
                }
                event
            })
            .collect();
        Ok(Some(json!({"record": record_id, "events": events})))
    }

    // ---- failure groups --------------------------------------------------------

    fn groups(&self, load_id: i64, only: Option<&str>) -> Result<Vec<Group>> {
        let rows = self.query(
            "SELECT g.id, g.kind, g.reason, g.transform_id, g.transform_version, g.location,
                    CAST(COALESCE(SUM(CASE WHEN r.status = 'rejected' THEN 1 ELSE 0 END), 0) AS BIGINT),
                    MIN(CASE WHEN r.status = 'rejected' THEN r.line END),
                    MIN(r.line),
                    CAST(COALESCE(SUM(CASE WHEN r.status = 'rejected' AND r.content = 'expired' THEN 1 ELSE 0 END), 0) AS BIGINT),
                    CAST(COALESCE(SUM(CASE WHEN r.status = 'rejected' AND r.content = 'purged' THEN 1 ELSE 0 END), 0) AS BIGINT),
                    CAST(COALESCE(SUM(CASE WHEN r.content = 'expired' THEN 1 ELSE 0 END), 0) AS BIGINT),
                    CAST(COALESCE(SUM(CASE WHEN r.content = 'purged' THEN 1 ELSE 0 END), 0) AS BIGINT),
                    MIN(g.created_at)
             FROM failure_groups g LEFT JOIN records r ON r.group_id = g.id AND r.file_id = g.load_id
             WHERE g.load_id = $1 AND (CAST($2 AS TEXT) IS NULL OR g.id = CAST($2 AS TEXT))
             GROUP BY g.id, g.kind, g.reason, g.transform_id, g.transform_version, g.location
             ORDER BY 7 DESC, 8, 14",
            &params![load_id, only],
        )?;
        Ok(rows
            .iter()
            .map(|r| {
                let count = r.i64(6);
                let (expired, purged) = if count > 0 {
                    (r.i64(9), r.i64(10))
                } else {
                    (r.i64(11), r.i64(12))
                };
                let content = if expired > 0 {
                    "expired"
                } else if purged > 0 {
                    "purged"
                } else {
                    "kept"
                };
                let tid = r.string(3);
                Group {
                    id: r.string(0),
                    load_id,
                    kind: r.string(1),
                    reason: r.string(2),
                    transform: (!tid.is_empty()).then(|| (tid, r.i64(4))),
                    location: r.json(5),
                    count,
                    first_line: r.opt_i64(7).or_else(|| r.opt_i64(8)),
                    content: content.to_string(),
                }
            })
            .collect())
    }

    /// One failure group of a load, failing rows or not.
    pub fn failure_group(&self, load_id: i64, group: &str) -> Result<Option<Group>> {
        Ok(self.groups(load_id, Some(group))?.into_iter().next())
    }

    /// The load's groups with failing rows, most rows first, each as `GET
    /// /api/loads/{id}` lists it, with up to three sample rows while kept.
    pub fn load_failures(&self, load_id: i64) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        for group in self.groups(load_id, None)? {
            if group.count == 0 {
                continue;
            }
            let samples: Vec<Value> = if group.content == "kept" {
                self.group_rows(&group, 3)?
                    .into_iter()
                    .map(|(id, line, raw)| json!({"id": id, "line": line, "raw": raw}))
                    .collect()
            } else {
                Vec::new()
            };
            let mut g = group.to_json();
            g["samples"] = Value::Array(samples);
            out.push(g);
        }
        Ok(out)
    }

    /// The group's failing rows whose content is kept: (record, line, raw).
    pub fn group_rows(&self, group: &Group, limit: usize) -> Result<Vec<(i64, i64, Value)>> {
        Ok(self
            .query(
                "SELECT id, line, raw FROM records
                 WHERE file_id = $1 AND group_id = $2 AND status = 'rejected' AND content IS NULL
                 ORDER BY line, id LIMIT $3",
                &params![group.load_id, group.id, limit],
            )?
            .iter()
            .map(|r| {
                (
                    r.i64(0),
                    r.i64(1),
                    self.unseal(r.get(2)).unwrap_or(Value::Null),
                )
            })
            .collect())
    }

    /// The ids, lines and attempts of the group's failing rows, in line order.
    pub fn group_records(&self, group: &Group, limit: usize) -> Result<Vec<Value>> {
        self.query_json(
            "SELECT id, line, run AS attempts FROM records
             WHERE file_id = $1 AND group_id = $2 AND status = 'rejected' ORDER BY line, id LIMIT $3",
            params![group.load_id, group.id, limit],
        )
    }

    // ---- retries ---------------------------------------------------------------

    /// Accepts a retry of a group's failing rows with `version`, claiming them
    /// in the same transaction so two retries never cover one row. The rows go
    /// back to the start, run by the retry's version.
    pub fn create_retry(&self, group: &Group, version: &VersionRef) -> Result<Value, RetryRefusal> {
        let id = ids::new(Kind::Retry);
        let created = self.atomically(|c| {
            let unfinished = c.query(
                &format!(
                    "SELECT DISTINCT t.id FROM retries t JOIN retry_rows rr ON rr.retry_id = t.id
                     JOIN records r ON r.id = rr.record_id
                     WHERE t.load_id = $1 AND t.status IN {UNFINISHED}
                       AND (t.group_id = $2 OR r.group_id = $2)"
                ),
                &params![group.load_id, group.id],
            )?;
            let rows = c.query(
                "SELECT id, content FROM records WHERE file_id = $1 AND group_id = $2 AND status = 'rejected' ORDER BY id",
                &params![group.load_id, group.id],
            )?;
            if !rows.is_empty() && rows.iter().any(|r| r.opt_string(1).is_some()) {
                return Ok(Err(RetryRefusal::ContentExpired));
            }
            if let Some(other) = unfinished.first() {
                return Ok(Err(RetryRefusal::InProgress(other.string(0))));
            }
            if rows.is_empty() {
                return Ok(Err(RetryRefusal::NothingToRetry));
            }
            let feed = c
                .query("SELECT feed_id FROM files WHERE id = $1", &params![group.load_id])?
                .first()
                .map(|r| r.string(0))
                .unwrap_or_default();
            let previous = c
                .query(
                    "SELECT id FROM retries WHERE load_id = $1 AND group_id = $2 ORDER BY accepted_at DESC, id DESC LIMIT 1",
                    &params![group.load_id, group.id],
                )?
                .first()
                .map(|r| r.string(0));
            let now = now_ms();
            c.execute(
                "INSERT INTO retries (id, load_id, feed_id, group_id, transform_id, transform_version, status, rows, previous, accepted_at)
                 VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7, $8, $9)",
                &params![
                    id,
                    group.load_id,
                    feed,
                    group.id,
                    version.transform_id,
                    version.version,
                    rows.len(),
                    previous,
                    now
                ],
            )?;
            let ids: Vec<i64> = rows.iter().map(|r| r.i64(0)).collect();
            for chunk in ids.chunks(IDS_PER_STATEMENT) {
                let marks: Vec<String> = (0..chunk.len()).map(|i| format!("${}", i + 5)).collect();
                let mut values: Vec<Param> = params![id, version.transform_id, version.version, now].to_vec();
                values.extend(chunk.iter().map(|i| Param::Int(*i)));
                c.execute(
                    &format!(
                        "UPDATE records SET status = 'received', exec_retry = $1, exec_transform_id = $2, exec_version = $3,
                                run = run + 1, resources = NULL, resource_count = NULL, resource_types = NULL, response = NULL,
                                error_kind = NULL, error = NULL, attempts = 0, next_attempt_at = 0, submission = NULL,
                                uncertain = 0, waiting = NULL, dispatch = NULL, validated = 0, updated_at = $4
                         WHERE id IN ({})",
                        marks.join(", ")
                    ),
                    &values,
                )?;
                let rows_sql: Vec<String> = (0..chunk.len()).map(|i| format!("($1, ${})", i + 2)).collect();
                let mut values: Vec<Param> = vec![Param::Text(id.clone())];
                values.extend(chunk.iter().map(|i| Param::Int(*i)));
                c.execute(
                    &format!("INSERT INTO retry_rows (retry_id, record_id) VALUES {}", rows_sql.join(", ")),
                    &values,
                )?;
            }
            append_events(c, &ids, "retried", None)?;
            c.execute(
                "UPDATE files SET status = 'processing', finished_at = NULL WHERE id = $1",
                &params![group.load_id],
            )?;
            Ok(Ok(()))
        })?;
        created?;
        Ok(self
            .retry(group.load_id, &id)?
            .ok_or_else(|| anyhow::anyhow!("retry {id} not stored"))?)
    }

    fn retries_where(&self, condition: &str, p: &[Param]) -> Result<Vec<Value>> {
        let rows = self.query(
            &format!(
                "SELECT t.id, t.load_id, t.group_id, t.transform_id, t.transform_version, t.status, t.rows,
                        t.written, t.failed, t.outcome_unknown, t.previous, t.accepted_at, t.finished_at,
                        CAST(COALESCE(SUM(CASE WHEN r.status = 'sent' THEN 1 ELSE 0 END), 0) AS BIGINT),
                        CAST(COALESCE(SUM(CASE WHEN r.status = 'rejected' AND r.error_kind <> 'filtered' THEN 1 ELSE 0 END), 0) AS BIGINT),
                        CAST(COALESCE(SUM(CASE WHEN r.status IN ('pending', 'submitted') AND r.uncertain = 1 THEN 1 ELSE 0 END), 0) AS BIGINT),
                        MAX(CASE WHEN r.status NOT IN ('sent', 'rejected') AND r.waiting = 'validation_unavailable' THEN 1 ELSE 0 END),
                        MAX(CASE WHEN r.status NOT IN ('sent', 'rejected') AND r.waiting = 'environment_unavailable' THEN 1 ELSE 0 END),
                        MAX(CASE WHEN r.status NOT IN ('sent', 'rejected') AND (r.uncertain = 1 OR r.waiting = 'destination_unavailable') THEN 1 ELSE 0 END)
                 FROM retries t
                 LEFT JOIN records r ON r.exec_retry = t.id AND t.status IN {UNFINISHED}
                 WHERE {condition}
                 GROUP BY t.id, t.load_id, t.group_id, t.transform_id, t.transform_version, t.status, t.rows,
                          t.written, t.failed, t.outcome_unknown, t.previous, t.accepted_at, t.finished_at
                 ORDER BY t.accepted_at DESC, t.id DESC"
            ),
            p,
        )?;
        Ok(rows
            .iter()
            .map(|r| {
                let finished = r.opt_i64(7).is_some();
                let (written, failed, unknown) = if finished {
                    (r.i64(7), r.i64(8), r.i64(9))
                } else {
                    (r.i64(13), r.i64(14), r.i64(15))
                };
                let held = !finished
                    && self
                        .version_waiting(r.i64(1), Some(&r.string(0)))
                        .ok()
                        .flatten()
                        .is_some();
                let waiting = if finished {
                    Value::Null
                } else if r.i64(16) == 1 {
                    json!("validation_unavailable")
                } else if r.i64(17) == 1 || held {
                    json!("environment_unavailable")
                } else if r.i64(18) == 1 {
                    json!("destination_unavailable")
                } else {
                    Value::Null
                };
                json!({
                    "id": r.string(0),
                    "load": r.i64(1),
                    "group": r.string(2),
                    "transform": {"id": r.string(3), "version": r.i64(4)},
                    "status": r.string(5),
                    "rows": r.i64(6),
                    "written": written,
                    "failed": failed,
                    "outcome_unknown": unknown,
                    "waiting": waiting,
                    "previous": r.opt_string(10),
                    "accepted_at": at(r.opt_i64(11)),
                    "finished_at": at(r.opt_i64(12)),
                })
            })
            .collect())
    }

    /// A load's retries, newest first.
    pub fn retries(&self, load_id: i64) -> Result<Vec<Value>> {
        self.retries_where("t.load_id = $1", &params![load_id])
    }

    /// One retry of a load.
    pub fn retry(&self, load_id: i64, retry_id: &str) -> Result<Option<Value>> {
        Ok(self
            .retries_where("t.load_id = $1 AND t.id = $2", &params![load_id, retry_id])?
            .into_iter()
            .next())
    }

    /// The groups of a load the 0.1 retry covers: those with failing rows (with
    /// `reason`, when given) that no unfinished retry covers.
    pub fn groups_to_retry(&self, load_id: i64, reason: Option<&str>) -> Result<Vec<Group>> {
        let busy: Vec<String> = self
            .query(
                &format!(
                    "SELECT DISTINCT r.group_id FROM retries t JOIN retry_rows rr ON rr.retry_id = t.id
                     JOIN records r ON r.id = rr.record_id
                     WHERE t.load_id = $1 AND t.status IN {UNFINISHED} AND r.group_id IS NOT NULL
                     UNION SELECT group_id FROM retries WHERE load_id = $1 AND status IN {UNFINISHED}"
                ),
                &params![load_id],
            )?
            .iter()
            .map(|r| r.string(0))
            .collect();
        Ok(self
            .groups(load_id, None)?
            .into_iter()
            .filter(|g| g.count > 0 && !busy.contains(&g.id))
            .filter(|g| reason.is_none_or(|reason| g.reason == reason))
            .collect())
    }

    /// Marks a retry running once one of its rows has been worked on.
    pub fn retry_started(&self, retry_id: &str) -> Result<()> {
        self.execute(
            "UPDATE retries SET status = 'running', started_at = $2 WHERE id = $1 AND status = 'queued'",
            &params![retry_id, now_ms()],
        )
        .map(drop)
    }

    /// Ends the feed's retries whose rows all reached an outcome: `done`, with
    /// their counts kept as they ended.
    pub fn finish_retries(&self, feed: &str) -> Result<Vec<String>> {
        let rows = self.query(
            &format!(
                "SELECT t.id,
                        CAST(COALESCE(SUM(CASE WHEN r.status = 'sent' THEN 1 ELSE 0 END), 0) AS BIGINT),
                        CAST(COALESCE(SUM(CASE WHEN r.status = 'rejected' AND r.error_kind <> 'filtered' THEN 1 ELSE 0 END), 0) AS BIGINT),
                        CAST(COALESCE(SUM(CASE WHEN r.status IN ('received', 'pending', 'submitted') THEN 1 ELSE 0 END), 0) AS BIGINT)
                 FROM retries t LEFT JOIN records r ON r.exec_retry = t.id
                 WHERE t.feed_id = $1 AND t.status IN {UNFINISHED}
                 GROUP BY t.id"
            ),
            &params![feed],
        )?;
        let mut done = Vec::new();
        for r in rows.iter().filter(|r| r.i64(3) == 0) {
            let id = r.string(0);
            self.execute(
                &format!(
                    "UPDATE retries SET status = 'done', written = $2, failed = $3, outcome_unknown = 0,
                            started_at = COALESCE(started_at, $4), finished_at = $4
                     WHERE id = $1 AND status IN {UNFINISHED}"
                ),
                &params![id, r.i64(1), r.i64(2), now_ms()],
            )?;
            done.push(id);
        }
        Ok(done)
    }

    // ---- usage -----------------------------------------------------------------

    /// Every load and retry that ran `version` of `transform_id`, newest first,
    /// at most `limit`; built from the store's loads, retries and history, so it
    /// outlives the source.
    pub fn version_runs(
        &self,
        transform_id: &str,
        version: i64,
        limit: usize,
    ) -> Result<Vec<VersionRun>> {
        let mut runs: Vec<(i64, VersionRun)> = Vec::new();
        for r in self.query(
            "SELECT f.id, f.feed_id, f.status, f.received_at, f.finished_at,
                    (SELECT COUNT(*) FROM records r WHERE r.file_id = f.id),
                    (SELECT COUNT(DISTINCT e.record_id) FROM record_events e
                      WHERE e.load_id = f.id AND e.retry_id IS NULL AND e.kind = 'delivered'),
                    (SELECT COUNT(DISTINCT e.record_id) FROM record_events e
                      WHERE e.load_id = f.id AND e.retry_id IS NULL AND e.kind = 'failed' AND e.error_kind <> 'filtered'),
                    (SELECT COUNT(*) FROM records r WHERE r.file_id = f.id AND r.exec_retry IS NULL
                      AND r.uncertain = 1 AND r.status IN ('pending', 'submitted'))
             FROM files f WHERE f.transform_id = $1 AND f.transform_version = $2
             ORDER BY f.received_at DESC, f.id DESC LIMIT $3",
            &params![transform_id, version, limit],
        )? {
            let running = matches!(r.string(2).as_str(), "processing" | "queued");
            runs.push((
                r.i64(3),
                VersionRun {
                    kind: "load".into(),
                    id: r.i64(0).to_string(),
                    source: r.string(1),
                    load: r.i64(0),
                    rows: r.i64(5),
                    written: r.i64(6),
                    failed: r.i64(7),
                    outcome: run_outcome(running, r.i64(6), r.i64(7), r.i64(8)),
                    started_at: at(r.opt_i64(3)).as_str().unwrap_or_default().to_string(),
                    finished_at: at(r.opt_i64(4)).as_str().map(str::to_string),
                },
            ));
        }
        for r in self.query(
            &format!(
                "SELECT t.id, t.feed_id, t.load_id, t.status, t.rows, t.written, t.failed, t.outcome_unknown,
                        COALESCE(t.started_at, t.accepted_at), t.finished_at,
                        CAST(COALESCE(SUM(CASE WHEN r.status = 'sent' THEN 1 ELSE 0 END), 0) AS BIGINT),
                        CAST(COALESCE(SUM(CASE WHEN r.status = 'rejected' AND r.error_kind <> 'filtered' THEN 1 ELSE 0 END), 0) AS BIGINT),
                        CAST(COALESCE(SUM(CASE WHEN r.status IN ('pending', 'submitted') AND r.uncertain = 1 THEN 1 ELSE 0 END), 0) AS BIGINT)
                 FROM retries t LEFT JOIN records r ON r.exec_retry = t.id AND t.status IN {UNFINISHED}
                 WHERE t.transform_id = $1 AND t.transform_version = $2
                 GROUP BY t.id, t.feed_id, t.load_id, t.status, t.rows, t.written, t.failed, t.outcome_unknown,
                          t.started_at, t.accepted_at, t.finished_at
                 ORDER BY 9 DESC LIMIT $3"
            ),
            &params![transform_id, version, limit],
        )? {
            let running = matches!(r.string(3).as_str(), "queued" | "running");
            let (written, failed, unknown) = if running {
                (r.i64(10), r.i64(11), r.i64(12))
            } else {
                (r.i64(5), r.i64(6), r.i64(7))
            };
            runs.push((
                r.i64(8),
                VersionRun {
                    kind: "retry".into(),
                    id: r.string(0),
                    source: r.string(1),
                    load: r.i64(2),
                    rows: r.i64(4),
                    written,
                    failed,
                    outcome: run_outcome(running, written, failed, unknown),
                    started_at: at(r.opt_i64(8)).as_str().unwrap_or_default().to_string(),
                    finished_at: at(r.opt_i64(9)).as_str().map(str::to_string),
                },
            ));
        }
        runs.sort_by_key(|run| std::cmp::Reverse(run.0));
        Ok(runs.into_iter().take(limit).map(|(_, run)| run).collect())
    }

    // ---- period counts -----------------------------------------------------------

    /// What happened to a source's rows (all sources' when `feed` is `None`)
    /// between two instants, from the history: a row counts once per measure.
    /// `failed` counts rows by when they failed, so a later retry or purge
    /// never lowers it.
    pub fn period_counts(
        &self,
        feed: Option<&str>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<PeriodCounts> {
        let mut counts = PeriodCounts::default();
        for r in self.query(
            "SELECT kind, COUNT(DISTINCT record_id), CAST(COALESCE(SUM(resources), 0) AS BIGINT)
             FROM record_events
             WHERE (CAST($1 AS TEXT) IS NULL OR feed_id = CAST($1 AS TEXT)) AND at >= $2 AND at < $3
               AND kind IN ('admitted', 'delivered', 'failed')
               AND (kind <> 'failed' OR error_kind <> 'filtered')
             GROUP BY kind",
            &params![feed, from_ms, to_ms],
        )? {
            match r.string(0).as_str() {
                "admitted" => counts.rows_read = r.i64(1),
                "delivered" => {
                    counts.written = r.i64(1);
                    counts.resources_written = r.i64(2);
                }
                "failed" => counts.failed = r.i64(1),
                _ => {}
            }
        }
        counts.outcome_unknown = self
            .query_one(
                "SELECT COUNT(DISTINCT e.record_id) FROM record_events e JOIN records r ON r.id = e.record_id
                 WHERE (CAST($1 AS TEXT) IS NULL OR e.feed_id = CAST($1 AS TEXT)) AND e.at >= $2 AND e.at < $3
                   AND e.kind = 'outcome_unknown' AND r.uncertain = 1 AND r.status IN ('pending', 'submitted')",
                &params![feed, from_ms, to_ms],
            )?
            .map_or(0, |r| r.i64(0));
        Ok(counts)
    }

    /// Whether a load of the source received since `since_ms` is still running.
    pub fn loads_running_since(&self, feed: &str, since_ms: i64) -> Result<bool> {
        Ok(self
            .query_one(
                "SELECT 1 FROM files WHERE feed_id = $1 AND received_at >= $2 AND status IN ('queued', 'processing') LIMIT 1",
                &params![feed, since_ms],
            )?
            .is_some())
    }

    /// What a source has written to the destination, from its history:
    /// resources in all, per type, and when the last was written.
    pub fn destination_written(&self, feed: &str) -> Result<Value> {
        let mut by_type: BTreeMap<String, i64> = BTreeMap::new();
        let mut total = 0;
        let mut last = None;
        for r in self.query(
            "SELECT types, CAST(COUNT(*) AS BIGINT), CAST(COALESCE(SUM(resources), 0) AS BIGINT), MAX(at)
             FROM record_events WHERE feed_id = $1 AND kind = 'delivered' GROUP BY types",
            &params![feed],
        )? {
            total += r.i64(2);
            last = last.max(r.opt_i64(3));
            let times = r.i64(1);
            if let Some(Value::Object(types)) = r.json(0) {
                for (kind, n) in types {
                    *by_type.entry(kind).or_default() += n.as_i64().unwrap_or(0) * times;
                }
            }
        }
        Ok(json!({
            "source": feed,
            "resources_written": total,
            "by_type": Value::Object(by_type.into_iter().map(|(k, v)| (k, json!(v))).collect::<Map<_, _>>()),
            "last_written_at": at(last),
        }))
    }
}

fn run_outcome(running: bool, written: i64, failed: i64, unknown: i64) -> String {
    if running {
        "running"
    } else if unknown > 0 {
        "outcome_unknown"
    } else if failed == 0 {
        "written"
    } else if written == 0 {
        "failed"
    } else {
        "partial"
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::versions::Environment;

    fn version(n: i64) -> VersionRef {
        VersionRef {
            transform_id: "lab".into(),
            version: n,
            code_sha256: "a".repeat(64),
            environment: Environment::local(),
        }
    }

    /// Retries asked for at once never cover a row twice: one claims the
    /// group's rows, the others see it in progress.
    #[test]
    fn concurrent_retries_of_a_group_claim_its_rows_once() {
        let db = Arc::new(Db::in_memory().unwrap());
        let load = db
            .open_load("src", "a.csv", "sha", 3, None, Some(&version(1)))
            .unwrap();
        db.insert_records(
            load,
            "src",
            &[(1, json!({"v": "x"}), None), (2, json!({"v": "y"}), None)],
        )
        .unwrap();
        for item in db.received_records("src", 10).unwrap() {
            db.mark_failed(
                item.id,
                "data",
                "not a number",
                None,
                Some(&json!({"line": 3})),
            )
            .unwrap();
        }
        let failures = db.load_failures(load).unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0]["count"], 2);
        assert_eq!(failures[0]["location"], json!({"line": 3}));
        let group = db
            .failure_group(load, failures[0]["group"].as_str().unwrap())
            .unwrap()
            .unwrap();
        let outcomes: Vec<Result<Value, RetryRefusal>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let (db, group) = (db.clone(), group.clone());
                    s.spawn(move || db.create_retry(&group, &version(2)))
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let accepted: Vec<&Value> = outcomes.iter().filter_map(|o| o.as_ref().ok()).collect();
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0]["rows"], 2);
        assert_eq!(accepted[0]["transform"], json!({"id": "lab", "version": 2}));
        assert!(outcomes.iter().all(|o| match o {
            Ok(_) => true,
            Err(RetryRefusal::InProgress(id)) => *id == accepted[0]["id"],
            Err(_) => false,
        }));
        // The claimed rows run the retry's version.
        let items = db.received_records("src", 10).unwrap();
        assert_eq!(items.len(), 2);
        assert!(items.iter().all(|i| i.retry.as_ref()
            == accepted[0]["id"].as_str().map(str::to_string).as_ref()
            && i.version.as_ref().map(|v| v.version) == Some(2)));
        let history = db.record_history(items[0].id).unwrap().unwrap();
        let kinds: Vec<&str> = history["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, ["admitted", "failed", "retried"]);
        assert_eq!(history["events"][2]["transform"]["version"], 2);
        assert_eq!(history["events"][1]["transform"]["version"], 1);
    }

    /// Content is sealed in the store's files and opened when read.
    #[test]
    fn row_content_is_sealed_at_rest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hie.db");
        let db = Db::open(&path).unwrap();
        let load = db.open_load("src", "a.csv", "sha", 3, None, None).unwrap();
        db.insert_records(
            load,
            "src",
            &[(1, json!({"patient": "zq-secret-marker"}), None)],
        )
        .unwrap();
        let stored = db.query("SELECT raw FROM records", &[]).unwrap();
        assert!(stored[0].string(0).starts_with("v1."));
        assert_eq!(
            db.query_json("SELECT raw FROM records", []).unwrap()[0]["raw"]["patient"],
            "zq-secret-marker"
        );
        drop(db);
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let bytes = std::fs::read(entry.unwrap().path()).unwrap();
            assert!(!bytes.windows(16).any(|w| w == b"zq-secret-marker"));
        }
    }
}
