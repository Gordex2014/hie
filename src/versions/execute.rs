//! Running records for one version: the [`Execute`] interface and the phase-0
//! executor over the engine's [`Transformer`].

use std::path::Path;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use super::{Environment, ExecutorSettings};
use crate::transform::{Outcome, Transformer};

/// What a transform made of one record, with the diagnostics the executor
/// returned for it (`warnings`, `events`, `location`, and in Try only `stdout`
/// and `stderr`), unchanged. Persist diagnostics only through
/// `crate::diagnostics::for_storage`.
#[derive(Debug, Clone, PartialEq)]
pub struct ItemOutcome {
    pub outcome: Outcome,
    pub diagnostics: Option<Value>,
}

/// One run: where it ran, and one outcome per record, in order.
#[derive(Debug, Clone, PartialEq)]
pub struct Execution {
    pub environment: Environment,
    pub items: Vec<ItemOutcome>,
}

/// Runs records for exactly one version.
#[async_trait]
pub trait Execute: Send + Sync {
    /// Transforms `(record, context)` pairs. An error means the executor failed,
    /// not the data: nothing in the batch is decided.
    async fn run(&self, items: &[(Value, Value)]) -> Result<Execution>;
}

/// Phase 0: the engine's transformer (local worker or python-runtime) over the
/// transform's file.
pub(super) struct TransformerExecutor {
    transformer: Transformer,
    environment: Environment,
}

impl TransformerExecutor {
    pub(super) fn new(
        settings: &ExecutorSettings,
        script: &Path,
        environment: Environment,
    ) -> Self {
        let mut transformer = Transformer::new(&settings.python, &settings.runtime_dir, script);
        transformer.set_terminology(settings.terminology.as_deref());
        transformer.set_runtime(settings.runtime.as_ref());
        Self {
            transformer,
            environment,
        }
    }
}

#[async_trait]
impl Execute for TransformerExecutor {
    async fn run(&self, items: &[(Value, Value)]) -> Result<Execution> {
        let outcomes = self.transformer.transform_many(items).await?;
        Ok(Execution {
            environment: self.environment.clone(),
            items: outcomes
                .into_iter()
                .map(|outcome| ItemOutcome {
                    outcome,
                    diagnostics: None,
                })
                .collect(),
        })
    }
}
