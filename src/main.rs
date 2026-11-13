use std::path::PathBuf;

use anyhow::{Context, Result, bail};
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
    {
        use tracing_subscriber::layer::{Layer, SubscriberExt};
        use tracing_subscriber::util::SubscriberInitExt;
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_filter(hie::log_filter(std::env::var("HIE_LOG").ok().as_deref())),
            )
            .init();
    }
    let cli = Cli::parse();
    // The `runtime:` backend is gone: an operator who still points transforms at
    // the remote runtime service must not have them run here unnoticed.
    // `var_os`, so a value that is not UTF-8 is refused too rather than ignored.
    if std::env::var_os("HIE_RUNTIME_URL").is_some_and(|v| !v.to_string_lossy().trim().is_empty()) {
        bail!(
            "HIE_RUNTIME_URL is no longer supported: transforms run in the local worker; unset it"
        );
    }
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

    // Registered before any source starts, so a signal from the first moment
    // stops the engine instead of killing it with its workers left behind.
    let mut stop = StopSignals::new()?;
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
    // The transform workers run in process groups of their own, so a signal to
    // the engine does not reach them: the engine stops on it instead of dying,
    // and returning from here drops every runner and call, which kills each
    // worker's group and removes its run directory.
    tokio::select! {
        served = axum::serve(listener, hie::api::router(state)) => served?,
        signal = stop.recv() => tracing::info!("{signal}: stopping"),
    }
    Ok(())
}

/// Ctrl-C, and SIGTERM where there is one. Each stream registers its handler
/// when it is created.
struct StopSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(windows)]
    ctrl_c: tokio::signal::windows::CtrlC,
}

impl StopSignals {
    fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt()).context("listening for SIGINT")?,
                terminate: signal(SignalKind::terminate()).context("listening for SIGTERM")?,
            })
        }
        #[cfg(windows)]
        {
            Ok(Self {
                ctrl_c: tokio::signal::windows::ctrl_c().context("listening for Ctrl-C")?,
            })
        }
    }

    async fn recv(&mut self) -> &'static str {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.interrupt.recv() => "SIGINT",
                _ = self.terminate.recv() => "SIGTERM",
            }
        }
        #[cfg(windows)]
        {
            self.ctrl_c.recv().await;
            "Ctrl-C"
        }
    }
}
