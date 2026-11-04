//! Period counts: the numbers a source's page draws, today and per day, with
//! days ending at midnight in a time zone (`GET /api/sources/{id}/stats`); and
//! what a source has written to the destination (`GET
//! /api/sources/{id}/destination`). Both come from the history, so a later
//! retry or purge never lowers a past period's failures.

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{Days, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::{ApiError, ApiResult, AppState, count, not_found};
use crate::db::{PeriodCounts, params};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/sources/{id}/stats", get(source_stats))
        .route("/api/sources/{id}/destination", get(source_destination))
}

#[derive(Deserialize)]
struct StatsQuery {
    days: Option<i64>,
    tz: Option<String>,
}

/// The instant a calendar day starts in `tz` (the first instant of it, so a
/// day that starts inside a DST gap still has one).
fn day_start(tz: Tz, day: NaiveDate) -> i64 {
    let midnight = day.and_hms_opt(0, 0, 0).expect("midnight");
    match tz.from_local_datetime(&midnight) {
        chrono::LocalResult::Single(t) => t.timestamp_millis(),
        chrono::LocalResult::Ambiguous(a, _) => a.timestamp_millis(),
        chrono::LocalResult::None => (1..=180)
            .find_map(|m| {
                tz.from_local_datetime(&(midnight + chrono::Duration::minutes(m)))
                    .earliest()
            })
            .map_or_else(
                || midnight.and_utc().timestamp_millis(),
                |t| t.timestamp_millis(),
            ),
    }
}

fn period_json(c: &PeriodCounts) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("rows_read".into(), json!(c.rows_read));
    m.insert("written".into(), json!(c.written));
    m.insert("failed".into(), json!(c.failed));
    m.insert("outcome_unknown".into(), json!(c.outcome_unknown));
    m.insert("resources_written".into(), json!(c.resources_written));
    m
}

/// What a source's page draws: today's numbers and one row per day for the
/// last N days, days ending at midnight in `tz` (UTC by default).
async fn source_stats(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<StatsQuery>,
) -> ApiResult {
    s.manager
        .source(&id)
        .ok_or_else(|| not_found("source_not_found", &format!("source {id}")))?;
    let tz: Tz = match q.tz.as_deref() {
        None | Some("") => Tz::UTC,
        Some(name) => name.parse().map_err(|_| {
            ApiError::bad_request(
                "invalid_tz",
                "tz must be an IANA time zone such as Europe/Madrid",
            )
        })?,
    };
    let days = q.days.unwrap_or(14).clamp(1, 366);
    let now = Utc::now();
    let today = now.with_timezone(&tz).date_naive();
    let first = today - Days::new(days as u64 - 1);

    let mut rows = Vec::with_capacity(days as usize);
    for i in 0..days {
        let day = first + Days::new(i as u64);
        let (from, to) = (day_start(tz, day), day_start(tz, day + Days::new(1)));
        let files = count(
            &s.db,
            "SELECT COUNT(*) AS n FROM files WHERE feed_id = $1 AND status <> 'duplicate' AND received_at >= $2 AND received_at < $3",
            params![id, from, to],
        )?;
        let sent = count(
            &s.db,
            "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status = 'sent' AND updated_at >= $2 AND updated_at < $3",
            params![id, from, to],
        )?;
        let rejected = count(
            &s.db,
            "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status = 'rejected' AND updated_at >= $2 AND updated_at < $3",
            params![id, from, to],
        )?;
        let mut row = Map::new();
        row.insert("day".into(), json!(day.to_string()));
        row.insert("files".into(), json!(files));
        row.insert("sent".into(), json!(sent));
        row.insert("rejected".into(), json!(rejected));
        row.extend(period_json(&s.db.period_counts(Some(&id), from, to)?));
        rows.push(Value::Object(row));
    }

    let since = day_start(tz, today);
    let until = now.timestamp_millis() + 1;
    let mut today_json = Map::new();
    today_json.insert("files".into(), json!(count(&s.db, "SELECT COUNT(*) AS n FROM files WHERE feed_id = $1 AND status <> 'duplicate' AND received_at >= $2", params![id, since])?));
    today_json.insert("records".into(), json!(count(&s.db, "SELECT COUNT(r.id) AS n FROM records r JOIN files f ON f.id = r.file_id WHERE r.feed_id = $1 AND f.received_at >= $2", params![id, since])?));
    today_json.insert("sent".into(), json!(count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status = 'sent' AND updated_at >= $2", params![id, since])?));
    today_json.insert("rejected".into(), json!(count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status = 'rejected' AND updated_at >= $2", params![id, since])?));
    today_json.insert("pending".into(), json!(count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status IN ('received', 'pending')", params![id])?));
    today_json.extend(period_json(&s.db.period_counts(Some(&id), since, until)?));
    today_json.insert(
        "complete".into(),
        json!(!s.db.loads_running_since(&id, since)?),
    );
    Ok(Json(json!({
        "source": id,
        "days": days,
        "tz": tz.name(),
        "today": today_json,
        "by_day": rows,
    })))
}

/// What a source has written to the destination, from its history.
async fn source_destination(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult {
    if s.manager.source(&id).is_none() && s.db.first_load_id(&id)?.is_none() {
        return Err(not_found("source_not_found", &format!("source {id}")));
    }
    Ok(Json(s.db.destination_written(&id)?))
}
