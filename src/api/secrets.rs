//! Secrets: write-only values kept encrypted (`/api/secrets`).
//!
//! Empty until track S fills it; registered in `api/mod.rs` under the
//! bearer layer, so adding routes here needs no change elsewhere.

use axum::Router;

use super::AppState;

pub(super) fn router() -> Router<AppState> {
    Router::new()
}
