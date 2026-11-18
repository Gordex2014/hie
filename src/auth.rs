//! Bearer-token validation for the API, with the same crate HFS uses: a JWT
//! signed by the identity provider's JWKS, with the expected issuer and audience.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use axum::Json;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use helios_auth::{AuthConfig, AuthProvider, JwksBearerAuthProvider, JwksCache};
use serde_json::json;

/// `None` when authentication is off: every request passes.
pub type Auth = Option<Arc<dyn AuthProvider>>;

#[derive(Debug, Clone)]
pub struct AuthSettings {
    pub enabled: bool,
    pub jwks_url: Option<String>,
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub algorithms: Vec<String>,
    pub jwks_min_refresh_interval: u64,
}

impl AuthSettings {
    /// `HIE_AUTH_ENABLED`, `HIE_AUTH_JWKS_URL`, `HIE_AUTH_ISSUER`, `HIE_AUTH_AUDIENCE`,
    /// `HIE_AUTH_ALGORITHMS` and `HIE_AUTH_JWKS_MIN_REFRESH_INTERVAL`, named as HFS names
    /// its own.
    pub fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        Self {
            enabled: var("HIE_AUTH_ENABLED")
                .is_some_and(|v| v.eq_ignore_ascii_case("true") || v == "1"),
            jwks_url: var("HIE_AUTH_JWKS_URL"),
            issuer: var("HIE_AUTH_ISSUER"),
            audience: var("HIE_AUTH_AUDIENCE"),
            algorithms: var("HIE_AUTH_ALGORITHMS")
                .unwrap_or_else(|| "RS256,RS384,ES256,ES384".into())
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            jwks_min_refresh_interval: var("HIE_AUTH_JWKS_MIN_REFRESH_INTERVAL")
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
        }
    }
}

/// Builds the validator, fetching the JWKS once so a wrong URL fails at startup.
pub async fn build(settings: &AuthSettings) -> Result<Auth> {
    if !settings.enabled {
        return Ok(None);
    }
    let config = AuthConfig {
        enabled: true,
        jwks_url: settings.jwks_url.clone(),
        expected_issuer: settings.issuer.clone(),
        expected_audience: settings.audience.clone(),
        allowed_algorithms: settings.algorithms.clone(),
        jwks_min_refresh_interval: settings.jwks_min_refresh_interval,
        ..AuthConfig::default()
    };
    if let Err(problems) = config.validate() {
        bail!("HIE_AUTH_* configuration: {}", problems.join("; "));
    }
    let jwks_url = config
        .jwks_url
        .as_deref()
        .context("HIE_AUTH_JWKS_URL is required when HIE_AUTH_ENABLED=true")?;
    let cache = Arc::new(JwksCache::new(jwks_url, config.jwks_min_refresh_interval));
    cache
        .initial_fetch()
        .await
        .map_err(|e| anyhow::anyhow!("fetching the JWKS at {jwks_url}: {e}"))?;
    Ok(Some(Arc::new(JwksBearerAuthProvider::new(cache, &config))))
}

/// Requires a valid bearer token on every request it wraps, when auth is on. The
/// authenticated principal is left in the request's extensions for the handlers.
pub async fn require_bearer(
    State(auth): State<Auth>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(provider) = auth else {
        return next.run(request).await;
    };
    let authorization = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let Some(authorization) = authorization else {
        return unauthorized("missing bearer token");
    };
    match provider.authenticate(&authorization).await {
        Ok(principal) => {
            let required = required_scope(request.method().as_str(), request.uri().path());
            if !granted(principal.scopes.raw(), required) {
                return forbidden(required, principal.scopes.raw());
            }
            request.extensions_mut().insert(Arc::new(principal));
            next.run(request).await
        }
        Err(e) => unauthorized(&e.to_string()),
    }
}

/// Reading anything but connector credentials.
pub const SCOPE_READ: &str = "hie/read";
/// Changing sources, rules, loads and records: everything that is not code.
/// Also every request on connector credentials, reads included.
pub const SCOPE_MANAGE: &str = "hie/manage";
/// Saving a transform: code the engine then runs.
pub const SCOPE_TRANSFORMS: &str = "hie/transforms";
/// Everything.
pub const SCOPE_ALL: &str = "hie/*";

/// The scope a request needs: its method and path decide, so a token that may
/// read cannot change anything, and one that may change sources cannot write code.
pub fn required_scope(method: &str, path: &str) -> &'static str {
    // Credentials are managed, not read: listing them names what the engine
    // can reach.
    if path == "/api/secrets" || path.starts_with("/api/secrets/") {
        return SCOPE_MANAGE;
    }
    if matches!(method, "GET" | "HEAD" | "OPTIONS") {
        return SCOPE_READ;
    }
    if method == "PUT" && path.starts_with("/api/transforms/") {
        return SCOPE_TRANSFORMS;
    }
    SCOPE_MANAGE
}

/// Whether a token's scopes cover the required one. `hie/*` covers everything,
/// and either write scope also reads.
pub fn granted(scopes: &[String], required: &str) -> bool {
    scopes.iter().any(|s| {
        s == SCOPE_ALL
            || s == required
            || (required == SCOPE_READ && (s == SCOPE_MANAGE || s == SCOPE_TRANSFORMS))
    })
}

fn unauthorized(reason: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(json!({"error": reason})),
    )
        .into_response()
}

fn forbidden(required: &str, held: &[String]) -> Response {
    let hie: Vec<&String> = held.iter().filter(|s| s.starts_with("hie/")).collect();
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": format!("this needs the {required} scope; the token has {}",
                if hie.is_empty() { "no hie/ scope".to_string() } else { format!("{hie:?}") }),
            "required": required,
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(list: &[&str]) -> Vec<String> {
        list.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn the_method_and_path_pick_the_scope() {
        assert_eq!(required_scope("GET", "/api/sources"), SCOPE_READ);
        assert_eq!(required_scope("POST", "/api/sources"), SCOPE_MANAGE);
        assert_eq!(required_scope("DELETE", "/api/alert-rules/3"), SCOPE_MANAGE);
        assert_eq!(
            required_scope("POST", "/api/transforms/x/try"),
            SCOPE_MANAGE
        );
        assert_eq!(required_scope("GET", "/api/transforms/x"), SCOPE_READ);
        assert_eq!(required_scope("PUT", "/api/transforms/x"), SCOPE_TRANSFORMS);
    }

    #[test]
    fn every_request_on_credentials_needs_the_manage_scope() {
        for method in ["GET", "HEAD", "OPTIONS", "POST", "PUT", "DELETE"] {
            assert_eq!(required_scope(method, "/api/secrets"), SCOPE_MANAGE);
            assert_eq!(
                required_scope(method, "/api/secrets/sec_0123456789abcdef"),
                SCOPE_MANAGE
            );
        }
        assert_eq!(required_scope("GET", "/api/secretsx"), SCOPE_READ);
    }

    #[test]
    fn write_scopes_read_and_the_wildcard_does_everything() {
        assert!(granted(&s(&["openid", "hie/read"]), SCOPE_READ));
        assert!(!granted(&s(&["hie/read"]), SCOPE_MANAGE));
        assert!(granted(&s(&["hie/manage"]), SCOPE_READ));
        assert!(!granted(&s(&["hie/manage"]), SCOPE_TRANSFORMS));
        assert!(granted(&s(&["hie/transforms"]), SCOPE_READ));
        assert!(granted(&s(&["hie/*"]), SCOPE_TRANSFORMS));
        assert!(!granted(&s(&["system/*.cruds"]), SCOPE_READ));
        assert!(!granted(&[], SCOPE_READ));
    }
}
