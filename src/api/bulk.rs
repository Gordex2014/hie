//! What HFS fetches during a `$bulk-submit`: the manifest, and the objects of
//! sources that cannot sign URLs. The token in the path is unguessable, and the
//! routes stay open when authentication is on.

use axum::extract::{Path, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use super::{ApiError, ApiResult, AppState, not_found};

pub(super) fn open_router() -> Router<AppState> {
    Router::new()
        .route("/bulk/{token}/manifest.json", get(bulk_manifest))
        .route("/bulk/{token}/files/{file_id}", get(bulk_file))
}

async fn bulk_manifest(State(s): State<AppState>, Path(token): Path<String>) -> ApiResult {
    let submission =
        s.db.submission(&token)?
            .ok_or_else(|| not_found("not_found", "submission"))?;
    Ok(Json(submission.manifest))
}

async fn bulk_file(
    State(s): State<AppState>,
    Path((token, file_id)): Path<(String, i64)>,
) -> Result<Response, ApiError> {
    // A file the engine wrote for a transformed submission, or else the source
    // file as it arrived (the source_objects path on a folder).
    let engine_file = s.bulk_dir.join(&token).join(format!("{file_id}.ndjson"));
    let path = if engine_file.is_file() && s.db.submission(&token)?.is_some() {
        engine_file.to_string_lossy().into_owned()
    } else {
        s.db.submission_file(&token, file_id)?
            .ok_or_else(|| not_found("not_found", "file"))?
    };
    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|_| not_found("not_found", "file"))?;
    Ok(([(header::CONTENT_TYPE, "application/fhir+ndjson")], bytes).into_response())
}
