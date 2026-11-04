//! Helios Integration Engine: gets data from sources into HFS,
//! with per-record traceability, idempotent delivery, retries and alerts.

pub mod admission;
pub mod alerts;
pub mod api;
pub mod auth;
pub mod bulk;
pub mod config;
pub mod connectors;
pub mod credentials;
pub mod crypto;
pub mod db;
pub mod diagnostics;
pub mod evidence;
pub mod fhir;
pub mod formats;
pub mod idempotency;
pub mod ids;
pub mod manager;
pub mod pipeline;
pub mod purge;
pub mod runtime;
pub mod secrets;
pub mod transform;
pub mod tryrun;
pub mod versions;

/// The source handle and the object connector, as they were reached before
/// connectors had modules of their own.
pub mod source {
    pub use crate::connectors::object::ObjectSource;
    pub use crate::connectors::{Incoming, Probe, Source};
}

/// The SQS queue of the object connector, as it was reached before.
pub use connectors::object::sqs;

/// HL7 v2, as it was reached before the formats and the MLLP connector had
/// modules of their own: the message format, and MLLP framing.
pub mod hl7 {
    pub use crate::connectors::mllp::{END, START, ack, frame, serve};
    pub use crate::formats::hl7v2::*;
}

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

/// Opens the store and the HFS client, starts every configured source, and returns
/// the manager (which owns the runners) and the API state.
pub fn build(
    config: &config::Config,
    feeds: Vec<config::FeedConfig>,
    runtime_dir: &Path,
    auth: auth::Auth,
    credentials: credentials::Credentials,
) -> Result<(Arc<manager::SourceManager>, api::AppState)> {
    let db = Arc::new(db::Db::connect(&config.database.to_string_lossy())?);
    let fhir = Arc::new(fhir::FhirClient::with_credentials(
        &config.fhir_base_url,
        credentials,
    ));
    let alerts = Arc::new(alerts::Alerter::new(
        db.clone(),
        config.alerts.webhook_url.clone(),
    ));
    let manager = manager::SourceManager::new(config, runtime_dir, db.clone(), fhir, alerts)?;
    manager.start_all(feeds)?;
    Ok((
        manager.clone(),
        api::AppState {
            db,
            auth,
            started: api::Started::now(),
            bulk_dir: config.bulk_dir(),
            config: manager.config().clone(),
            versions: manager.versions().clone(),
            samples: Arc::new(evidence::SampleRegistry::default()),
            manager,
        },
    ))
}
