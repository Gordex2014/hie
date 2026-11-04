//! Period counts: the numbers a source's page draws, today and per day
//! (`GET /api/sources/{id}/stats`).

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::{ApiResult, AppState, count, not_found};
use crate::db::params;

pub(super) fn router() -> Router<AppState> {
    Router::new().route("/api/sources/{id}/stats", get(source_stats))
}

#[derive(Deserialize)]
struct StatsQuery {
    days: Option<i64>,
}

/// What a source's page draws: today's numbers and one row per day for the last N days.
async fn source_stats(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<StatsQuery>,
) -> ApiResult {
    s.manager
        .source(&id)
        .ok_or_else(|| not_found("source_not_found", &format!("source {id}")))?;
    let days = q.days.unwrap_or(14).clamp(1, 366);
    let today = chrono::Utc::now().date_naive();
    let first = today - chrono::Days::new(days as u64 - 1);
    let since = first
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();

    let mut by_day: Map<String, Value> = (0..days)
        .map(|i| {
            let day = (first + chrono::Days::new(i as u64)).to_string();
            (day, json!({"files": 0, "sent": 0, "rejected": 0}))
        })
        .collect();
    for row in s.db.query_json(
        &format!(
            "SELECT {} AS day, COUNT(*) AS n FROM files
             WHERE feed_id = $1 AND status <> 'duplicate' AND received_at >= $2 GROUP BY 1",
            s.db.day("received_at")
        ),
        params![id, since],
    )? {
        if let Some(entry) = row["day"].as_str().and_then(|d| by_day.get_mut(d)) {
            entry["files"] = row["n"].clone();
        }
    }
    for row in s.db.query_json(
        &format!(
            "SELECT {} AS day, status, COUNT(*) AS n FROM records
             WHERE feed_id = $1 AND status IN ('sent', 'rejected') AND updated_at >= $2 GROUP BY 1, status",
            s.db.day("updated_at")
        ),
        params![id, since],
    )? {
        if let (Some(entry), Some(status)) = (
            row["day"].as_str().and_then(|d| by_day.get_mut(d)),
            row["status"].as_str(),
        ) {
            entry[status] = row["n"].clone();
        }
    }
    let rows: Vec<Value> = by_day
        .into_iter()
        .map(|(day, v)| json!({"day": day, "files": v["files"], "sent": v["sent"], "rejected": v["rejected"]}))
        .collect();

    let today_since = today
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    let today_json = json!({
        "files": count(&s.db, "SELECT COUNT(*) AS n FROM files WHERE feed_id = $1 AND status <> 'duplicate' AND received_at >= $2", params![id, today_since])?,
        "records": count(&s.db, "SELECT COUNT(r.id) AS n FROM records r JOIN files f ON f.id = r.file_id WHERE r.feed_id = $1 AND f.received_at >= $2", params![id, today_since])?,
        "sent": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status = 'sent' AND updated_at >= $2", params![id, today_since])?,
        "rejected": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status = 'rejected' AND updated_at >= $2", params![id, today_since])?,
        "pending": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status IN ('received', 'pending')", params![id])?,
    });
    Ok(Json(
        json!({"source": id, "days": days, "today": today_json, "by_day": rows}),
    ))
}
