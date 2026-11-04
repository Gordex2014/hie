//! The connection test: a definition (or just its `source`) is built and
//! probed once, before any source exists. Nothing is taken from the source and
//! no cursor is kept; a probe that fails is an answer (`reachable: false`), a
//! definition that cannot be built is invalid.

use std::path::PathBuf;

use serde_json::Value;

use super::{ManagerError, SourceManager};
use crate::config::{FeedConfig, SourceConfig};
use crate::connectors::{Probe, Source};

impl SourceManager {
    /// Tests a connection before a source exists: the body is a full definition or
    /// just `{"source": {...}}`. Nothing is taken from the source.
    pub async fn test(&self, body: &Value) -> Result<Probe, ManagerError> {
        let source_value = body.get("source").unwrap_or(body);
        let mut source: SourceConfig = serde_json::from_value(source_value.clone())
            .map_err(|e| ManagerError::invalid("invalid_definition", e.to_string()))?;
        source.resolve(&self.base_dir);
        let feed = FeedConfig {
            id: "test".into(),
            name: "test".into(),
            source,
            format: Default::default(),
            transform: PathBuf::new().into(),
            delivery: Default::default(),
            expect_every_text: None,
            expect_every: None,
        };
        let source = Source::from_config(&feed)
            .map_err(|e| ManagerError::invalid("invalid_definition", format!("{e:#}")))?;
        match source.probe().await {
            Ok(probe) => Ok(probe),
            Err(e) => Ok(Probe {
                reachable: false,
                elapsed_ms: 0,
                objects: 0,
                newest: None,
                newest_at: Some(format!("{e:#}")),
            }),
        }
    }
}
