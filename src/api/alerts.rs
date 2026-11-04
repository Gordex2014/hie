//! Alerts raised, and the rules that raise them.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{ApiError, ApiResult, AppState};
use crate::db::{Rule, params};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/alerts", get(list_alerts))
        .route("/api/alert-rules", get(list_rules).post(create_rule))
        .route(
            "/api/alert-rules/{id}",
            get(get_rule).put(replace_rule).delete(delete_rule),
        )
}

#[derive(Deserialize)]
struct AlertQuery {
    source: Option<String>,
    limit: Option<i64>,
    /// `true` lists only alerts that have not cleared.
    open: Option<bool>,
}

async fn list_alerts(State(s): State<AppState>, Query(q): Query<AlertQuery>) -> ApiResult {
    let rows = s.db.query_json(
        &format!(
            "SELECT id, feed_id AS source_id, kind, message, rule_id, notify, channels,
                    {} AS created_at, {} AS cleared_at, cleared_reason
             FROM alerts
             WHERE (CAST($1 AS TEXT) IS NULL OR feed_id = CAST($1 AS TEXT)) AND (CAST($3 AS BIGINT) = 0 OR cleared_at IS NULL)
             ORDER BY id DESC LIMIT $2",
            s.db.iso("created_at"),
            s.db.iso("cleared_at")
        ),
        params![q.source, q.limit.unwrap_or(100), q.open.unwrap_or(false)],
    )?;
    let rows: Vec<Value> = rows
        .into_iter()
        .map(|mut row| {
            for key in ["notify", "channels"] {
                if let Some(Value::String(text)) = row.get(key) {
                    let parsed =
                        serde_json::from_str::<Value>(text).unwrap_or(Value::Array(vec![]));
                    row[key] = parsed;
                }
            }
            row
        })
        .collect();
    Ok(Json(json!({"alerts": rows})))
}

// ---- Alert rules --------------------------------------------------------------

async fn list_rules(State(s): State<AppState>, Query(q): Query<AlertQuery>) -> ApiResult {
    let rules: Vec<Rule> =
        s.db.rules()?
            .into_iter()
            .filter(|r| {
                q.source
                    .as_ref()
                    .is_none_or(|id| r.source_id.as_ref() == Some(id))
            })
            .collect();
    Ok(Json(json!({"rules": rules})))
}

async fn get_rule(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    let rule =
        s.db.rule(id)?
            .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("rule {id} not found")))?;
    Ok(Json(json!(rule)))
}

/// Parses a rule body, checks its shape and that the source it names exists.
fn parse_rule(s: &AppState, body: Value) -> Result<Rule, ApiError> {
    let rule: Rule = serde_json::from_value(body)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    crate::alerts::validate_rule(&rule).map_err(|m| ApiError(StatusCode::BAD_REQUEST, m))?;
    if let Some(source) = &rule.source_id
        && s.manager.source(source).is_none()
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("source {source} not found"),
        ));
    }
    Ok(rule)
}

async fn create_rule(
    State(s): State<AppState>,
    principal: Option<axum::Extension<Arc<helios_auth::Principal>>>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let mut rule = parse_rule(&s, body)?;
    if rule.created_by.is_none() {
        rule.created_by = principal.map(|axum::Extension(p)| p.subject().to_string());
    }
    rule.id = s.db.insert_rule(&rule)?;
    Ok((StatusCode::CREATED, Json(json!(rule))))
}

async fn replace_rule(
    State(s): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<Value>,
) -> ApiResult {
    let mut rule = parse_rule(&s, body)?;
    if !s.db.update_rule(id, &rule)? {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("rule {id} not found"),
        ));
    }
    rule.id = id;
    Ok(Json(json!(rule)))
}

async fn delete_rule(
    State(s): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    if !s.db.delete_rule(id)? {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("rule {id} not found"),
        ));
    }
    Ok(StatusCode::NO_CONTENT)
}
