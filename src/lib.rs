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

/// Source files this build could not load: each stays visible as an alert of
/// this instance, one per source id, until a start of this instance no longer
/// skips a file of that source (fixed, removed, or loaded by a build with a
/// secret backend), which clears it. An alert another instance raised is left
/// to it. One an earlier version raised names no instance: an instance that
/// skips a file of that source takes it as its own, and any other leaves it.
fn skipped_file_alerts(
    config: &config::Config,
    db: &db::Db,
    alerts: &alerts::Alerter,
) -> Result<()> {
    let open = db.open_rule_less_alerts("definition")?;
    if config.skipped.is_empty() && open.iter().all(|(_, _, by)| by.is_none()) {
        // Nothing to raise, and nothing raised by an instance to clear.
        return Ok(());
    }
    let owner =
        secrets::owner::owner_id(std::env::var("HIE_INSTANCE_ID").ok(), &config.sources_dir);
    for skipped in &config.skipped {
        alerts.know_source(&skipped.source_id, &skipped.source_name);
        match db.open_alert(&skipped.source_id, "definition", None)? {
            Some((id, _)) => {
                db.adopt_alert(id, &owner)?;
            }
            None => alerts.raise_as(
                &skipped.source_id,
                "definition",
                &format!(
                    "{}: {}; the file is skipped",
                    skipped.file.display(),
                    skipped.reason
                ),
                &owner,
            )?,
        }
    }
    let skipped: std::collections::HashSet<&str> = config
        .skipped
        .iter()
        .map(|s| s.source_id.as_str())
        .collect();
    for (id, source, raised_by) in open {
        if raised_by.as_deref() == Some(owner.as_str()) && !skipped.contains(source.as_str()) {
            alerts.clear(
                id,
                &source,
                "definition",
                &format!("{source}: its definition is no longer skipped"),
            )?;
        }
    }
    Ok(())
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
    skipped_file_alerts(config, &db, &alerts)?;
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
