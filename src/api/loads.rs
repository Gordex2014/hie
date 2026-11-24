//! `GET /api/loads`: the loads of every source, newest first, a page at a time,
//! narrowed to one source and to one status or to the loads failing now. See
//! `docs/api.md` (Loads).

use axum::Json;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, RawQuery, State};
use serde::Deserialize;
use serde_json::json;

use super::{ApiError, ApiResult, AppState, load_rows, paging};
use crate::db::{IntoParam, Param};

/// How many loads a page holds unless asked: the per-source list's default.
const DEFAULT_LIMIT: i64 = 50;
/// The most a page holds. Each load's counts read every row of it, one load at
/// a time.
const MAX_LIMIT: i64 = 200;

/// The statuses a load is stored with.
const STATUSES: [&str; 5] = ["processing", "unpacked", "done", "unreadable", "duplicate"];

#[derive(Deserialize)]
pub(super) struct LoadQuery {
    source: Option<String>,
    status: Option<String>,
    limit: Option<i64>,
    /// The previous page's `next_cursor`.
    cursor: Option<String>,
}

/// The parameters `GET /api/loads` reads.
const PARAMETERS: [&str; 4] = ["source", "status", "limit", "cursor"];

/// A parameter the list does not read is refused, rather than ignored: a
/// misspelt filter (`?state=failed`) would otherwise list every load. The
/// sentence is fixed; `field` names the parameter only when it reads as one.
fn refuse_unknown(raw: Option<&str>) -> Result<(), ApiError> {
    for (key, _) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
        if PARAMETERS.contains(&key.as_ref()) {
            continue;
        }
        let error =
            paging::invalid_query("unknown parameter (use source, status, limit or cursor)");
        let named = !key.is_empty()
            && key.len() <= 32
            && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        return Err(if named {
            error.with("field", key.as_ref())
        } else {
            error
        });
    }
    Ok(())
}

/// What a page keeps, besides the cursor.
#[derive(Debug, PartialEq)]
enum Keep {
    All,
    Status(String),
    /// Unreadable, or with at least one row rejected now for a reason other
    /// than its transform filtering it out.
    Failed,
}

/// A page asked for, once its query string has been read.
#[derive(Debug, PartialEq)]
struct Ask {
    source: Option<String>,
    keep: Keep,
    /// The id the previous page ended on.
    after: Option<i64>,
    limit: i64,
}

impl Ask {
    fn read(q: LoadQuery) -> Result<Self, ApiError> {
        let after = paging::cursor_id(q.cursor.as_deref())?;
        let limit = q.limit.unwrap_or(DEFAULT_LIMIT);
        if !(1..=MAX_LIMIT).contains(&limit) {
            return Err(
                paging::invalid_query("limit must be a whole number from 1 to 200")
                    .with("field", "limit"),
            );
        }
        let keep = match q.status.as_deref().filter(|t| !t.is_empty()) {
            None => Keep::All,
            Some("failed") => Keep::Failed,
            Some(status) if STATUSES.contains(&status) => Keep::Status(status.to_string()),
            Some(_) => {
                return Err(paging::invalid_query(
                    "status must be processing, unpacked, done, unreadable, duplicate or failed",
                )
                .with("field", "status"));
            }
        };
        let source = q.source.filter(|t| !t.is_empty());
        // Any id a load may carry is accepted. Source ids have no control
        // characters, and PostgreSQL text cannot hold a NUL: refuse them rather
        // than fail the statement.
        if source
            .as_deref()
            .is_some_and(|id| id.chars().any(char::is_control))
        {
            return Err(paging::invalid_query("source must be a source id").with("field", "source"));
        }
        Ok(Ask {
            source,
            keep,
            after,
            limit,
        })
    }

    /// The `WITH RECURSIVE` clause naming `candidates`: the ids of the page and
    /// of one load more, newest first. Only the conditions asked for are in the
    /// SQL, each one an index can serve.
    fn candidates(&self) -> (String, Vec<Param>) {
        let mut p: Vec<Param> = Vec::new();
        let mut bind = |v: &dyn IntoParam| {
            p.push(v.to_param());
            format!("${}", p.len())
        };
        // Ids below the cursor's: a bound SQLite seeks on, where `IS NULL OR` would scan.
        let below = bind(&self.after.map_or(i64::MAX, |id| id - 1));
        let take = bind(&(self.limit + 1));
        let of_source = self
            .source
            .as_ref()
            .map(|id| format!(" AND feed_id = CAST({} AS TEXT)", bind(id)))
            .unwrap_or_default();
        let sql = match &self.keep {
            Keep::All => format!(
                "WITH RECURSIVE candidates(id) AS (
                   SELECT id FROM files WHERE id <= CAST({below} AS BIGINT){of_source}
                   ORDER BY id DESC LIMIT CAST({take} AS BIGINT))"
            ),
            Keep::Status(status) => {
                let status = bind(status);
                format!(
                    "WITH RECURSIVE candidates(id) AS (
                       SELECT id FROM files WHERE id <= CAST({below} AS BIGINT){of_source} AND status = CAST({status} AS TEXT)
                       ORDER BY id DESC LIMIT CAST({take} AS BIGINT))"
                )
            }
            // The loads with a failing row (rejected, and not filtered out on
            // purpose), newest first, take one seek each on the index of those
            // rows, so a load with many of them is not read row by row on the way
            // to the next one. The condition is written as the index's is, which
            // is what lets both engines use that partial index. A record's
            // `feed_id` is its load's, as it is stored with it.
            Keep::Failed => format!(
                "WITH RECURSIVE rejected(id, n) AS (
                   SELECT (SELECT MAX(file_id) FROM records WHERE status = 'rejected' AND COALESCE(error_kind, '') <> 'filtered'{of_source} AND file_id <= CAST({below} AS BIGINT)), 1
                   UNION ALL
                   SELECT (SELECT MAX(file_id) FROM records WHERE status = 'rejected' AND COALESCE(error_kind, '') <> 'filtered'{of_source} AND file_id < rejected.id), rejected.n + 1
                   FROM rejected WHERE rejected.id IS NOT NULL AND rejected.n < CAST({take} AS BIGINT)
                 ), candidates(id) AS (
                   SELECT id FROM (
                     SELECT id FROM rejected WHERE id IS NOT NULL
                     UNION
                     SELECT id FROM (SELECT id FROM files WHERE status = 'unreadable'{of_source} AND id <= CAST({below} AS BIGINT)
                                     ORDER BY id DESC LIMIT CAST({take} AS BIGINT)) unreadable
                   ) failed ORDER BY id DESC LIMIT CAST({take} AS BIGINT))"
            ),
        };
        (sql, p)
    }
}

/// Newest first, a page at a time: `next_cursor` is set only when more loads
/// match, and the next page holds those older (by id) than the last one here.
/// The page and whether more match come from one statement; each load's fields
/// are then read on their own (`load_rows`), so they may be newer than the
/// choice of the page: a load chosen as failed may show `failed: false` if a
/// Retry fixed it in between.
pub(super) async fn list_loads(
    State(s): State<AppState>,
    RawQuery(raw): RawQuery,
    q: Result<Query<LoadQuery>, QueryRejection>,
) -> ApiResult {
    refuse_unknown(raw.as_deref())?;
    let Query(q) = q.map_err(paging::rejected)?;
    let ask = Ask::read(q)?;
    // The page is read one load at a time, with pauses between them: on a
    // blocking thread, so the async workers serve other requests meanwhile.
    tokio::task::spawn_blocking(move || page_of_loads(&s, &ask))
        .await
        .map_err(anyhow::Error::new)?
}

fn page_of_loads(s: &AppState, ask: &Ask) -> ApiResult {
    let (with, mut p) = ask.candidates();
    p.push(ask.limit.to_param());
    let page = p.len();
    // The page and the load after it: more than a page means more follow.
    let chosen = s.db.query_json(
        &format!(
            "{with} SELECT id, (SELECT COUNT(*) FROM candidates) AS matched
             FROM candidates ORDER BY id DESC LIMIT CAST(${page} AS BIGINT)"
        ),
        p,
    )?;
    let more = chosen
        .first()
        .and_then(|row| row["matched"].as_i64())
        .is_some_and(|n| n > ask.limit);
    let ids: Vec<i64> = chosen.iter().filter_map(|row| row["id"].as_i64()).collect();
    let rows = load_rows(s, &ids)?;
    let next_cursor = if more {
        let last = ids.last().copied();
        Some(paging::next_cursor(last.ok_or_else(|| {
            anyhow::anyhow!("a load row without an id")
        })?))
    } else {
        None
    };
    Ok(Json(json!({"loads": rows, "next_cursor": next_cursor})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    fn ask(query: &[(&str, &str)]) -> Result<Ask, ApiError> {
        let text = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let Query(q) = Query::<LoadQuery>::try_from_uri(
            &format!("http://x/api/loads?{text}").parse().unwrap(),
        )
        .map_err(paging::rejected)?;
        Ask::read(q)
    }

    #[test]
    fn a_query_is_read_with_its_defaults_and_refusals() {
        let read = ask(&[]).ok().unwrap();
        assert_eq!(
            read,
            Ask {
                source: None,
                keep: Keep::All,
                after: None,
                limit: 50
            }
        );
        let read = ask(&[
            ("source", "lab"),
            ("status", "failed"),
            ("cursor", "a1.9"),
            ("limit", "200"),
        ])
        .ok()
        .unwrap();
        assert_eq!(
            read,
            Ask {
                source: Some("lab".into()),
                keep: Keep::Failed,
                after: Some(9),
                limit: 200
            }
        );
        let empty = ask(&[("source", ""), ("status", ""), ("cursor", "")])
            .ok()
            .unwrap();
        assert_eq!(
            (empty.source, empty.keep, empty.after),
            (None, Keep::All, None)
        );
        for status in STATUSES {
            assert_eq!(
                ask(&[("status", status)]).ok().unwrap().keep,
                Keep::Status(status.into())
            );
        }
        let field = |query: &[(&str, &str)]| {
            let e = ask(query).expect_err("refused");
            assert_eq!(e.code, Some("invalid_query"));
            e.extra
                .get("field")
                .and_then(|f| f.as_str())
                .map(str::to_string)
        };
        for limit in ["0", "201", "-1"] {
            assert_eq!(field(&[("limit", limit)]).as_deref(), Some("limit"));
        }
        assert_eq!(field(&[("status", "rejected")]).as_deref(), Some("status"));
        assert_eq!(field(&[("source", "lab%00")]).as_deref(), Some("source"));
        assert_eq!(field(&[("source", "lab%0A")]).as_deref(), Some("source"));
        for cursor in ["42", "a1.0", "a1.07", "l1.5"] {
            assert_eq!(field(&[("cursor", cursor)]).as_deref(), Some("cursor"));
        }
        assert_eq!(field(&[("limit", "abc")]), None);
        assert_eq!(field(&[("limit", "1"), ("limit", "2")]), None);
    }

    /// Every way of choosing a page reads an index, and a page of failed loads
    /// seeks from one load to the next.
    #[test]
    fn each_page_is_chosen_through_an_index() {
        let db = Db::in_memory().unwrap();
        if db.is_postgres() {
            return;
        }
        let plan = |query: &[(&str, &str)]| {
            let (with, p) = ask(query).ok().unwrap().candidates();
            db.query_json(
                &format!("EXPLAIN QUERY PLAN {with} SELECT id FROM candidates"),
                p,
            )
            .unwrap()
            .iter()
            .map(|row| row["detail"].as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>()
        };
        let uses = |query: &[(&str, &str)], wanted: &[&str]| {
            let plan = plan(query);
            for index in wanted {
                assert!(
                    plan.iter().any(|line| line.contains(index)),
                    "{query:?} does not use {index}: {plan:#?}"
                );
            }
            assert!(
                !plan
                    .iter()
                    .any(|line| line.starts_with("SCAN files") || line.starts_with("SCAN records")),
                "{query:?} scans a table: {plan:#?}"
            );
        };
        uses(&[], &["INTEGER PRIMARY KEY"]);
        uses(&[("source", "lab")], &["files_feed_id"]);
        uses(&[("status", "done")], &["files_status_id"]);
        uses(
            &[("source", "lab"), ("status", "done")],
            &["files_feed_status_id"],
        );
        uses(
            &[("status", "failed")],
            &["records_failing_file", "files_status_id"],
        );
        uses(
            &[("source", "lab"), ("status", "failed")],
            &["records_failing_feed_file", "files_feed_status_id"],
        );
    }
}
