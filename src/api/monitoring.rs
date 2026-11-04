//! Monitoring: the status page behind authentication, and the open `/health`
//! and `/metrics` for probes and the metrics scraper.

use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};

use super::sources::source_status;
use super::{ApiError, ApiResult, AppState, DAY_MS, count};
use crate::db::{now_ms, params};

pub(super) fn router() -> Router<AppState> {
    Router::new().route("/api/monitoring/status", get(monitoring_status))
}

/// Routes that stay open when authentication is on.
pub(super) fn open_router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
}

async fn health(State(s): State<AppState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "service": "hie",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": s.started.instant.elapsed().as_secs_f64(),
        "started_at": s.started.at.to_rfc3339(),
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
}

/// What the monitoring page opens on: what needs a person, the last 24 hours, every source.
async fn monitoring_status(State(s): State<AppState>) -> ApiResult {
    let mut sources = Vec::new();
    let mut needs_you = Vec::new();
    for feed in s.manager.sources() {
        let status = source_status(&s, &feed)?;
        let rejected = status["rejected_last_24h"].as_i64().unwrap_or(0);
        let reason = match status["state"].as_str() {
            Some("late") => Some(format!(
                "nothing received since {}, expected at least every {}",
                status["last_received_at"].as_str().unwrap_or("the start"),
                status["expect_every"].as_str().unwrap_or("?")
            )),
            Some("paused") => Some(format!(
                "paused{} since {}",
                status["paused"]["by"]
                    .as_str()
                    .map(|b| format!(" by {b}"))
                    .unwrap_or_default(),
                status["paused"]["since"].as_str().unwrap_or("?")
            )),
            _ if rejected > 0 => Some(format!("{rejected} records rejected in the last 24 hours")),
            _ => None,
        };
        if let Some(reason) = reason {
            needs_you.push(json!({"id": feed.id, "name": feed.name, "state": status["state"], "reason": reason}));
        }
        sources.push(status);
    }
    let since = now_ms() - DAY_MS;
    let last_24h = json!({
        "files": count(&s.db, "SELECT COUNT(*) AS n FROM files WHERE status <> 'duplicate' AND received_at >= $1", params![since])?,
        "sent": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE status = 'sent' AND updated_at >= $1", params![since])?,
        "rejected": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE status = 'rejected' AND updated_at >= $1", params![since])?,
        "alerts": count(&s.db, "SELECT COUNT(*) AS n FROM alerts WHERE created_at >= $1", params![since])?,
    });
    Ok(Json(json!({
        "state": if needs_you.is_empty() { "all_receiving" } else { "needs_you" },
        "needs_you": needs_you,
        "last_24h": last_24h,
        "sources": sources,
    })))
}

/// Prometheus text format, so the numbers can live in the same monitoring stack as HFS.
async fn metrics(State(s): State<AppState>) -> Result<Response, ApiError> {
    let sources = s.manager.sources();
    let mut out = String::new();
    out.push_str("# TYPE hie_records gauge\n");
    for f in &sources {
        if let Value::Object(counts) = s.db.feed_counts(&f.id)? {
            for status in ["received", "pending", "submitted", "sent", "rejected"] {
                let n = counts.get(status).and_then(Value::as_i64).unwrap_or(0);
                out.push_str(&format!(
                    "hie_records{{source=\"{}\",status=\"{status}\"}} {n}\n",
                    f.id
                ));
            }
        }
    }
    out.push_str("# TYPE hie_source_last_received_timestamp_seconds gauge\n");
    for f in &sources {
        if let Some(t) = s.db.last_received_at(&f.id)? {
            out.push_str(&format!(
                "hie_source_last_received_timestamp_seconds{{source=\"{}\"}} {}\n",
                f.id,
                t / 1000
            ));
        }
    }
    out.push_str("# TYPE hie_source_paused gauge\n");
    for f in &sources {
        out.push_str(&format!(
            "hie_source_paused{{source=\"{}\"}} {}\n",
            f.id,
            u8::from(s.db.is_paused(&f.id)?)
        ));
    }
    out.push_str("# TYPE hie_alerts_total counter\n");
    for row in s.db.query_json("SELECT COALESCE(feed_id, '') AS source, kind, COUNT(*) AS n FROM alerts GROUP BY feed_id, kind", [])? {
        out.push_str(&format!(
            "hie_alerts_total{{source=\"{}\",kind=\"{}\"}} {}\n",
            row["source"].as_str().unwrap_or_default(),
            row["kind"].as_str().unwrap_or_default(),
            row["n"]
        ));
    }
    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response())
}
