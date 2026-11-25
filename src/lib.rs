//! Helios Integration Engine: gets data from sources into HFS,
//! with per-record traceability, idempotent delivery, retries and alerts.

/// A named place where a test may hold the thread or task that reaches it
/// (`pause_point!(db, "name")`, or `pause_point!(await manager, "name")` in async
/// code). Nothing outside tests.
#[allow(unused_macros)]
macro_rules! pause_point {
    (await $owner:expr, $name:literal) => {
        #[cfg(all(test, feature = "secrets-native"))]
        $owner.pause_at($name).await;
    };
    ($owner:expr, $name:literal) => {
        #[cfg(all(test, feature = "secrets-native"))]
        $owner.pause_at($name);
    };
}

/// What a test runs at an async `pause_point!`, to hold the task there.
#[cfg(all(test, feature = "secrets-native"))]
pub(crate) type AsyncPause =
    std::sync::Arc<dyn Fn(&'static str) -> futures::future::BoxFuture<'static, ()> + Send + Sync>;

pub mod alerts;
pub mod api;
pub mod auth;
pub mod bulk;
pub mod config;
pub mod credentials;
#[cfg(feature = "secrets-native")]
pub mod crypto;
pub mod database;
pub mod db;
pub mod destination;
pub mod events;
pub mod fhir;
pub mod hl7;
pub mod manager;
pub mod manifest;
pub mod pipeline;
pub mod reader;
pub mod retention;
pub mod secrets;
pub mod source;
pub mod sqs;
pub mod transform;
pub mod worker;
pub mod zone;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

/// Libraries that log the values bound to SQL statements (record data, status
/// URLs, tokens) at DEBUG and TRACE. They never log below INFO.
pub const PARAMETER_LOGGERS: &[&str] = &["tokio_postgres"];

/// The process's log filter: `spec` as `HIE_LOG` gives it (`info` when it is unset
/// or invalid), and, whatever it says, nothing below INFO from the libraries in
/// `PARAMETER_LOGGERS`.
pub fn log_filter<S>(spec: Option<&str>) -> impl tracing_subscriber::layer::Filter<S> + use<S>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use tracing_subscriber::filter::{EnvFilter, FilterExt, filter_fn};
    let wanted = spec
        .and_then(|s| EnvFilter::try_new(s).ok())
        .unwrap_or_else(|| EnvFilter::new("info"));
    wanted.and(filter_fn(|meta| {
        *meta.level() <= tracing::Level::INFO
            || !PARAMETER_LOGGERS.iter().any(|library| {
                meta.target() == *library
                    || meta
                        .target()
                        .strip_prefix(library)
                        .is_some_and(|rest| rest.starts_with("::"))
            })
    }))
}

/// Opens the store and the HFS client, starts every configured source, and returns
/// the manager (which owns the runners) and the API state. No secret key is
/// given: a SQLite file store uses or makes the key file next to it.
pub fn build(
    config: &config::Config,
    feeds: Vec<config::FeedConfig>,
    runtime_dir: &Path,
    auth: auth::Auth,
    credentials: credentials::Credentials,
) -> Result<(Arc<manager::SourceManager>, api::AppState)> {
    build_with(
        config,
        feeds,
        runtime_dir,
        auth,
        credentials,
        secrets::KeySettings::none(),
    )
}

/// `build`, with the secret key inputs of the process (`KeySettings::load()`
/// in production). Refuses to start when the store is sealed and the key is
/// missing, wrong or malformed, or when the key settings are ambiguous.
pub fn build_with(
    config: &config::Config,
    feeds: Vec<config::FeedConfig>,
    runtime_dir: &Path,
    auth: auth::Auth,
    credentials: credentials::Credentials,
    keys: secrets::KeySettings,
) -> Result<(Arc<manager::SourceManager>, api::AppState)> {
    config.alerts.validate()?;
    let db = Arc::new(db::Db::connect(&config.database.to_string_lossy())?);
    let secrets = secrets::open(config, &db, keys)?;
    let fhir = Arc::new(fhir::FhirClient::with_credentials(
        &config.fhir_base_url,
        credentials,
    ));
    let alerts = Arc::new(alerts::Alerter::from_config(db.clone(), &config.alerts));
    // Source files this build could not load: each stays visible as an alert.
    for skipped in &config.skipped {
        alerts.know_source(&skipped.source_id, &skipped.source_name);
        if db
            .open_alert(&skipped.source_id, "definition", None)?
            .is_none()
        {
            alerts.raise(
                Some(&skipped.source_id),
                "definition",
                &format!(
                    "{}: {}; the file is skipped",
                    skipped.file.display(),
                    skipped.reason
                ),
            )?;
        }
    }
    // Alerts older versions left open after a recovery: closed before any
    // source runs, so none of them stands in for a new outage.
    let closed = db.close_legacy_recoveries()?;
    if closed > 0 {
        tracing::info!("closed {closed} delivery alert(s) left open by an earlier version");
    }
    // Alerts of rules turned off, deleted or changed by an earlier version,
    // which left them open: cleared, each with its `cleared` event.
    let unwatched = alerts.clear_unwatched()?;
    if unwatched > 0 {
        tracing::info!("cleared {unwatched} alert(s) whose rule no longer watches them");
    }
    destination::spawn_probe(
        Arc::downgrade(fhir.destination()),
        config.destination.probe_every(),
    );
    let manager = manager::SourceManager::new(
        config,
        runtime_dir,
        db.clone(),
        fhir,
        alerts.clone(),
        secrets.clone(),
    );
    // The manager owns the webhook sender: it stops when the engine goes.
    manager.keep_webhook_sender(alerts.start(alerts::SenderTuning::default()));
    manager.start_all(feeds)?;
    // Events and alerts older than the retention go, at start and every hour.
    retention::spawn(
        Arc::downgrade(&db),
        retention::Retention::from_config(&config.retention),
    );
    Ok((
        manager.clone(),
        api::AppState {
            db,
            manager,
            auth,
            started: api::Started::now(),
            bulk_dir: config.bulk_dir(),
            secrets,
            alerts,
            retention: retention::Retention::from_config(&config.retention),
        },
    ))
}
