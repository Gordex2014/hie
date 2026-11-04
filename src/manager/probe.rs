//! The connection test: a definition (or just its `source`) is built and
//! probed once, before any source exists. Nothing is taken from the source and
//! no cursor is kept; a probe that fails is an answer (`reachable: false`), a
//! definition that cannot be built is invalid, a connection the admission
//! policy refuses is `403` before anything is attempted. A database named by
//! its parts answers `evidence` too (`crate::evidence`).
//!
//! Also the destination's facts, read from its `/metadata` at most once a minute.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::{ManagerError, SourceManager, read_definition};
use crate::config::{FeedConfig, SourceConfig, Validation};
use crate::connectors::Source;

/// How long the destination's facts are reused.
const FACTS_TTL: Duration = Duration::from_secs(60);

impl SourceManager {
    /// Tests a connection before a source exists: the body is a full definition or
    /// just `{"source": {...}}`. Nothing is taken from the source.
    pub async fn test(&self, body: &Value) -> Result<Value, ManagerError> {
        let source_value = body.get("source").unwrap_or(body);
        let mut source: SourceConfig = read_definition(source_value)?;
        source.resolve(&self.base_dir);
        source
            .check()
            .map_err(|e| ManagerError::invalid("invalid_definition", format!("{e:#}")))?;
        if crate::evidence::needs_evidence(&source) {
            return self.evidence.test(&source).await;
        }
        self.evidence.admit(&source)?;
        let feed = FeedConfig {
            id: "test".into(),
            name: "test".into(),
            source,
            format: Default::default(),
            transform: PathBuf::new().into(),
            delivery: Default::default(),
            expect_every_text: None,
            expect_every: None,
            file: None,
        };
        let source = Source::from_config_in(&feed, &self.connect)
            .map_err(|e| ManagerError::invalid("invalid_definition", format!("{e:#}")))?;
        let probe = match source.probe().await {
            Ok(probe) => probe,
            Err(e) => crate::connectors::Probe {
                reachable: false,
                elapsed_ms: 0,
                objects: 0,
                newest: None,
                newest_at: Some(format!("{e:#}")),
            },
        };
        Ok(serde_json::to_value(probe).map_err(anyhow::Error::from)?)
    }

    /// `GET /api/destination`: what the destination's `/metadata` says,
    /// read at most once a minute.
    pub async fn destination(&self) -> Value {
        let mut cached = self.destination.lock().await;
        if let Some((at, facts)) = cached.as_ref()
            && at.elapsed() < FACTS_TTL
        {
            return facts.clone();
        }
        let metadata = self.services.fhir.metadata().await;
        if let Err(e) = &metadata {
            tracing::warn!("reading the destination's metadata: {e:#}");
        }
        let metadata = metadata.ok();
        let facts = json!({
            "base_url": self.config.fhir_base_url,
            "display_name": self.config.destination.display_name,
            "tenant": self.config.destination.tenant_name(),
            "tenant_header": self.config.destination.tenant_header,
            "fhir_version": metadata.as_ref().and_then(|m| m.fhir_version.clone()),
            "software": metadata.as_ref().and_then(|m| m.software.as_ref()).map(|s| json!({"name": s.name, "version": s.version})),
            "reachable": metadata.is_some(),
            "checked_at": crate::ids::now_rfc3339_ms(),
            "resource_count": null,
            "validation": match self.config.validation {
                Validation::Off => "off",
                Validation::Required => "required",
            },
        });
        *cached = Some((Instant::now(), facts.clone()));
        facts
    }
}
