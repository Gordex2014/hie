use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "hie",
    version,
    about = "Helios Integration Engine: feeds into HFS"
)]
struct Cli {
    /// Engine configuration file.
    #[arg(long, short, default_value = "hie.yaml", env = "HIE_CONFIG")]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Watch every feed and serve the API (default).
    Run,
    /// Load the configuration and feeds, report problems, and exit.
    Check,
}

#[tokio::main]
async fn main() -> Result<()> {
    // The database drivers' own events are never logged, whatever HIE_LOG
    // says (no directive, however specific, re-enables them): tiberius logs a
    // server's error and info messages (a login failure may echo what was
    // sent) and sqlx its statements and connect parameters. HIE logs its own
    // sanitized account of a failure instead.
    {
        use tracing_subscriber::Layer;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        let env = tracing_subscriber::EnvFilter::try_from_env("HIE_LOG")
            .unwrap_or_else(|_| "info".into());
        let drivers = tracing_subscriber::filter::filter_fn(|metadata| {
            !hie::connectors::database::is_driver_event(metadata.target())
        });
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_filter(env)
                    .with_filter(drivers),
            )
            .init();
    }
    let cli = Cli::parse();
    let (mut config, feeds) = hie::config::load(&cli.config)?;
    // The store may be a PostgreSQL URL with a password: it comes from the
    // environment, never from the file.
    if let Some(url) = std::env::var("HIE_DATABASE_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        config.database = PathBuf::from(url);
    }
    if let Some(url) = std::env::var("HIE_TERMINOLOGY_SERVER")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        config.terminology_url = Some(url);
    }
    if let Some(url) = std::env::var("HIE_RUNTIME_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        config.runtime.get_or_insert_with(Default::default).url = url;
    }
    let runtime_dir = std::env::var_os("HIE_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            cli.config
                .parent()
                .map(|p| p.join("hie_runtime"))
                .unwrap_or_else(|| "hie_runtime".into())
        });

    if matches!(cli.command, Some(Command::Check)) {
        println!(
            "config ok: {} source(s), HFS at {}",
            feeds.len(),
            config.fhir_base_url
        );
        for f in &feeds {
            println!(
                "  {} ({}) <- {} via {}",
                f.id,
                f.name,
                f.source.describe(),
                f.transform.display()
            );
        }
        return Ok(());
    }

    let auth = hie::auth::build(&hie::auth::AuthSettings::from_env()).await?;
    match &auth {
        Some(_) => tracing::info!("API authentication on: bearer tokens required under /api"),
        None => tracing::warn!(
            "API authentication is off (HIE_AUTH_ENABLED is not true): anyone who can reach the API can change sources and transforms"
        ),
    }
    let credentials = match hie::credentials::CredentialSettings::from_env()? {
        Some(settings) => {
            let source = hie::credentials::TokenSource::new(settings);
            source.check().await?;
            tracing::info!(
                client = source.client_id(),
                "calling HFS as the engine: token from {}",
                source.settings().token_url
            );
            Some(std::sync::Arc::new(source))
        }
        None => {
            tracing::warn!(
                "calling HFS anonymously (HIE_FHIR_TOKEN_URL, HIE_FHIR_CLIENT_ID and HIE_FHIR_CLIENT_SECRET are not set)"
            );
            None
        }
    };
    let (_manager, state) = hie::build(&config, feeds, &runtime_dir, auth, credentials)?;
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    tracing::info!("API on http://{}", config.listen);
    axum::serve(listener, hie::api::router(state)).await?;
    Ok(())
}
