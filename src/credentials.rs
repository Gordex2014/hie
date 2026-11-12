//! The engine's own identity towards HFS: an OAuth2 client-credentials grant at
//! the identity provider's token endpoint, cached until shortly before it expires.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::fhir::SendError;

/// `None` when the engine calls HFS anonymously, as the public sandbox does.
pub type Credentials = Option<Arc<TokenSource>>;

#[derive(Clone)]
pub struct CredentialSettings {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
    pub scope: Option<String>,
}

/// Everything but the client secret, which a debug print or a log line must
/// never carry.
impl std::fmt::Debug for CredentialSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialSettings")
            .field("token_url", &self.token_url)
            .field("client_id", &self.client_id)
            .field("client_secret", &"<redacted>")
            .field("scope", &self.scope)
            .finish()
    }
}

impl CredentialSettings {
    /// `HIE_FHIR_TOKEN_URL`, `HIE_FHIR_CLIENT_ID`, `HIE_FHIR_CLIENT_SECRET` and
    /// `HIE_FHIR_SCOPE`. All three of the first are needed; none set means anonymous.
    pub fn from_env() -> Result<Option<Self>> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let (token_url, client_id, client_secret) = (
            var("HIE_FHIR_TOKEN_URL"),
            var("HIE_FHIR_CLIENT_ID"),
            var("HIE_FHIR_CLIENT_SECRET"),
        );
        if token_url.is_none() && client_id.is_none() && client_secret.is_none() {
            return Ok(None);
        }
        let missing: Vec<&str> = [
            ("HIE_FHIR_TOKEN_URL", token_url.is_none()),
            ("HIE_FHIR_CLIENT_ID", client_id.is_none()),
            ("HIE_FHIR_CLIENT_SECRET", client_secret.is_none()),
        ]
        .into_iter()
        .filter_map(|(name, absent)| absent.then_some(name))
        .collect();
        if !missing.is_empty() {
            bail!(
                "HIE_FHIR_* credentials are incomplete: {} not set",
                missing.join(", ")
            );
        }
        Ok(Some(Self {
            token_url: token_url.expect("checked"),
            client_id: client_id.expect("checked"),
            client_secret: client_secret.expect("checked"),
            scope: var("HIE_FHIR_SCOPE"),
        }))
    }
}

struct Cached {
    token: String,
    expires_at: Instant,
}

/// Hands out a bearer token for HFS, fetching a new one when the cached one is
/// within a minute of expiring or was refused.
pub struct TokenSource {
    settings: CredentialSettings,
    http: reqwest::Client,
    cached: Mutex<Option<Cached>>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default = "default_expires_in")]
    expires_in: u64,
}

fn default_expires_in() -> u64 {
    300
}

impl TokenSource {
    pub fn new(settings: CredentialSettings) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("HTTP client");
        Self {
            settings,
            http,
            cached: Mutex::new(None),
        }
    }

    pub fn client_id(&self) -> &str {
        &self.settings.client_id
    }

    pub fn settings(&self) -> &CredentialSettings {
        &self.settings
    }

    /// The token to send, fetching one if needed. A token endpoint that cannot be
    /// reached or refuses the client is a transient delivery failure: the data
    /// waits and is retried, and the outage alerts say why.
    pub async fn bearer(&self) -> Result<String, SendError> {
        let mut cached = self.cached.lock().await;
        if let Some(c) = cached.as_ref()
            && c.expires_at > Instant::now() + Duration::from_secs(60)
        {
            return Ok(c.token.clone());
        }
        let fetched = self.fetch().await.map_err(|e| {
            SendError::Transient(format!("token from {}: {e:#}", self.settings.token_url))
        })?;
        let token = fetched.token.clone();
        *cached = Some(fetched);
        Ok(token)
    }

    /// Forgets the cached token, after HFS answered 401 with it.
    pub async fn invalidate(&self) {
        *self.cached.lock().await = None;
    }

    /// Fetches a token once, so a wrong endpoint or secret fails at startup.
    pub async fn check(&self) -> Result<()> {
        let fetched = self.fetch().await?;
        *self.cached.lock().await = Some(fetched);
        Ok(())
    }

    async fn fetch(&self) -> Result<Cached> {
        let mut form = vec![
            ("grant_type", "client_credentials"),
            ("client_id", self.settings.client_id.as_str()),
            ("client_secret", self.settings.client_secret.as_str()),
        ];
        if let Some(scope) = &self.settings.scope {
            form.push(("scope", scope));
        }
        let response = self
            .http
            .post(&self.settings.token_url)
            .form(&form)
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("cannot reach the token endpoint")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!(
                "token endpoint answered {status} for client {}: {}",
                self.settings.client_id,
                body.chars().take(200).collect::<String>()
            );
        }
        let body: TokenResponse = response
            .json()
            .await
            .map_err(reqwest::Error::without_url)
            .context("token endpoint answered without an access_token")?;
        tracing::debug!(
            client = %self.settings.client_id,
            expires_in = body.expires_in,
            "fetched an HFS token"
        );
        Ok(Cached {
            token: body.access_token,
            expires_at: Instant::now() + Duration::from_secs(body.expires_in),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_leaves_the_client_secret_out() {
        let settings = CredentialSettings {
            token_url: "https://idp/token".into(),
            client_id: "hie".into(),
            client_secret: "CANARY-0b6c2f4e-client-secret".into(),
            scope: Some("system/*.cruds".into()),
        };
        let printed = format!("{settings:?} {settings:#?}");
        assert!(!printed.contains("CANARY"), "{printed}");
        assert!(printed.contains("https://idp/token") && printed.contains("hie"));
    }
}
