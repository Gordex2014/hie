//! The connection URL of a database source: the secret its `url_env` names,
//! checked for a driver the engine has.

use anyhow::{Context, Result, bail};

use super::driver;

/// The URL `url_env` names, if it is set and one the drivers take.
pub(super) fn url(url_env: &str) -> Result<String> {
    let url = crate::secrets::named(url_env).with_context(|| {
        format!("environment variable {url_env} (the database URL named by the source) is not set")
    })?;
    if !(url.starts_with("postgres://")
        || url.starts_with("postgresql://")
        || url.starts_with("mysql://")
        || url.starts_with("sqlite:")
        || url.starts_with("sqlserver://")
        || url.starts_with("mssql://"))
    {
        bail!("{url_env} must be a postgres://, mysql://, sqlserver:// or sqlite: URL");
    }
    if url.starts_with("sqlserver://") || url.starts_with("mssql://") {
        driver::mssql_config(&url)?;
    }
    Ok(url)
}
