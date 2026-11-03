//! How the engine calls the remote FHIR server: anonymously, with a fixed
//! bearer token, or with tokens from an OAuth2 client-credentials grant.
//! Secrets are read from the environment variables the source names.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::config::RemoteAuth;
use crate::credentials::{CredentialSettings, Credentials, TokenSource};

pub(super) struct Client {
    http: reqwest::Client,
    credentials: Credentials,
    bearer: Option<String>,
}

impl Client {
    pub(super) fn new(auth: Option<&RemoteAuth>) -> Result<Self> {
        let env = |name: &str| {
            crate::secrets::named(name).with_context(|| {
                format!("environment variable {name} (named in the source's auth) is not set")
            })
        };
        let (credentials, bearer) = match auth {
            None => (None, None),
            Some(RemoteAuth::Bearer { token_env }) => (None, Some(env(token_env)?)),
            Some(RemoteAuth::ClientCredentials {
                token_url,
                client_id,
                client_secret_env,
                scope,
            }) => (
                Some(Arc::new(TokenSource::new(CredentialSettings {
                    token_url: token_url.clone(),
                    client_id: client_id.clone(),
                    client_secret: env(client_secret_env)?,
                    scope: scope.clone(),
                }))),
                None,
            ),
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .expect("HTTP client");
        Ok(Self {
            http,
            credentials,
            bearer,
        })
    }

    pub(super) async fn get(&self, url: &str) -> Result<serde_json::Value> {
        let mut request = self.http.get(url).header("Accept", "application/fhir+json");
        if let Some(token) = &self.bearer {
            request = request.bearer_auth(token);
        }
        if let Some(source) = &self.credentials {
            let token = source
                .bearer()
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("cannot reach {url}"))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!(
                "{url} answered {status}: {}",
                body.chars().take(300).collect::<String>()
            );
        }
        response
            .json()
            .await
            .with_context(|| format!("{url} did not answer with JSON"))
    }
}
