//! Running records for one version: the [`Execute`] interface, the engine's
//! own worker over the version's code written from the store, and
//! python-runtime's facade over the code stored under the version's name.
//! Either reports the environment it ran in; a run in another environment is
//! not applied by the caller.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use reqwest::StatusCode;
use serde_json::Value;

use super::{Environment, ExecutorKey, VersionRef, Versions, sha256_hex};
use crate::db::Db;
use crate::runtime::{Facade, MAX_REQUESTS_PER_CALL, describe, program_name};
use crate::transform::{Outcome, Transformer, parse_reply};

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

/// The executor cannot run the version's environment (it is not offered, or
/// the version was saved for another executor): its records wait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentUnavailable(pub String);

impl std::fmt::Display for EnvironmentUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "environment unavailable: {}", self.0)
    }
}

impl std::error::Error for EnvironmentUnavailable {}

/// Runs records for exactly one version.
#[async_trait]
pub trait Execute: Send + Sync {
    /// Transforms `(record, context)` pairs. An error means the executor failed,
    /// not the data: nothing in the batch is decided.
    async fn run(&self, items: &[(Value, Value)]) -> Result<Execution>;
}

/// The engine's own worker, over the version's code written to a file of its
/// own. One per source and version, so module state never crosses sources.
struct LocalExecutor {
    transformer: Transformer,
}

#[async_trait]
impl Execute for LocalExecutor {
    async fn run(&self, items: &[(Value, Value)]) -> Result<Execution> {
        let outcomes = self.transformer.transform_many(items).await?;
        Ok(Execution {
            environment: Environment::local(),
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

/// The version at python-runtime: its code stored under its own name, runs
/// naming its environment. A `404 transform_unknown` stores it again and runs
/// once more; a `403 environment_unavailable` holds the records.
struct RuntimeExecutor {
    facade: Facade,
    db: Arc<Db>,
    version: VersionRef,
    name: String,
    code: String,
    stored: tokio::sync::Mutex<bool>,
}

impl RuntimeExecutor {
    async fn ensure_stored(&self, again: bool) -> Result<()> {
        let mut stored = self.stored.lock().await;
        if *stored && !again {
            return Ok(());
        }
        let answer = self
            .facade
            .put(&self.name, &self.code, Some(&self.version.environment.id))
            .await?;
        if answer.status == StatusCode::FORBIDDEN
            && answer.error_code() == "environment_unavailable"
        {
            return Err(EnvironmentUnavailable(self.version.environment.id.clone()).into());
        }
        if !(answer.status == StatusCode::OK || answer.status == StatusCode::CREATED) {
            bail!(
                "the runtime did not store transform {}: {}",
                self.name,
                describe(&answer)
            );
        }
        let theirs = answer
            .body
            .get("sha256")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if theirs != self.version.code_sha256 {
            bail!(
                "the runtime stored transform {} with sha256 {theirs}, expected {}",
                self.name,
                self.version.code_sha256
            );
        }
        self.db
            .program_stored(&self.name, &self.version.transform_id, self.version.version)?;
        *stored = true;
        Ok(())
    }
}

#[async_trait]
impl Execute for RuntimeExecutor {
    async fn run(&self, items: &[(Value, Value)]) -> Result<Execution> {
        self.ensure_stored(false).await?;
        let mut outcomes = Vec::with_capacity(items.len());
        let mut ran_in: Option<Environment> = None;
        let per_call = self
            .facade
            .settings()
            .batch_size
            .clamp(1, MAX_REQUESTS_PER_CALL);
        let env = Some(self.version.environment.id.as_str());
        for chunk in items.chunks(per_call) {
            let mut answer = self.facade.run(&self.name, env, chunk).await?;
            if answer.status == StatusCode::NOT_FOUND && answer.error_code() == "transform_unknown"
            {
                // The runtime lost or never had it (a restart without its
                // store, or a name reclaimed meanwhile): stored again, run again.
                self.ensure_stored(true).await?;
                answer = self.facade.run(&self.name, env, chunk).await?;
            }
            if answer.status == StatusCode::FORBIDDEN
                && answer.error_code() == "environment_unavailable"
            {
                return Err(EnvironmentUnavailable(self.version.environment.id.clone()).into());
            }
            if answer.status != StatusCode::OK {
                bail!("the runtime did not run the batch: {}", describe(&answer));
            }
            // Nothing is applied without complete evidence of where it ran.
            let Some(environment) = answer.environment.clone() else {
                return Err(EnvironmentUnavailable(format!(
                    "the runtime's answer for version {} of {} names no complete environment",
                    self.version.version, self.version.transform_id
                ))
                .into());
            };
            if environment != self.version.environment {
                // Nothing of this run is kept: the caller sees where it ran.
                return Ok(Execution {
                    environment,
                    items: Vec::new(),
                });
            }
            ran_in = Some(environment);
            let replies = answer
                .body
                .get("replies")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("the runtime answered without replies"))?;
            if replies.len() != chunk.len() {
                bail!(
                    "the runtime answered {} replies for {} requests",
                    replies.len(),
                    chunk.len()
                );
            }
            outcomes.extend(replies.iter().map(|reply| ItemOutcome {
                outcome: parse_reply(reply),
                diagnostics: reply.get("diagnostics").cloned(),
            }));
        }
        Ok(Execution {
            environment: ran_in.unwrap_or_else(|| self.version.environment.clone()),
            items: outcomes,
        })
    }
}

impl Versions {
    /// Runs records of `source` for exactly `version` and reports the
    /// environment it ran in. Its code is read from the store and checked
    /// against the version's `code_sha256`. An executor that cannot run the
    /// version's environment is an [`EnvironmentUnavailable`] error.
    pub fn executor(&self, source: &str, version: &VersionRef) -> Result<Arc<dyn Execute>> {
        let local = version.environment.executor == "local";
        let key: ExecutorKey = (
            local.then(|| source.to_string()),
            version.transform_id.clone(),
            version.version,
            version.code_sha256.clone(),
        );
        if let Some(executor) = self.executors.lock().expect("executors").get(&key) {
            return Ok(executor.clone());
        }
        match (local, &self.facade) {
            (true, None) => {}
            (false, Some(_)) if version.environment.executor == crate::runtime::EXECUTOR => {}
            _ => {
                return Err(EnvironmentUnavailable(format!(
                    "version {} of {} runs in {} {}, which this engine's executor does not run",
                    version.version,
                    version.transform_id,
                    version.environment.executor,
                    version.environment.id
                ))
                .into());
            }
        }
        let code = self
            .db
            .version_code(&version.transform_id, version.version)?
            .with_context(|| {
                format!(
                    "the code of version {} of {} is gone",
                    version.version, version.transform_id
                )
            })?;
        if sha256_hex(&code) != version.code_sha256 {
            bail!(
                "the stored code of version {} of {} does not match its sha256",
                version.version,
                version.transform_id
            );
        }
        let executor: Arc<dyn Execute> = match &self.facade {
            None => {
                std::fs::create_dir_all(&self.materialized)?;
                let script = self.materialized.join(format!(
                    "{}.py",
                    program_name(&version.transform_id, version.version, &version.code_sha256)
                ));
                if !script.is_file() {
                    std::fs::write(&script, &code)
                        .with_context(|| format!("writing {}", script.display()))?;
                }
                let mut transformer =
                    Transformer::new(&self.settings.python, &self.settings.runtime_dir, &script);
                transformer.set_terminology(self.settings.terminology.as_deref());
                Arc::new(LocalExecutor { transformer })
            }
            Some(facade) => Arc::new(RuntimeExecutor {
                facade: facade.clone(),
                db: self.db.clone(),
                name: program_name(&version.transform_id, version.version, &version.code_sha256),
                version: version.clone(),
                code,
                stored: tokio::sync::Mutex::new(false),
            }),
        };
        let mut executors = self.executors.lock().expect("executors");
        Ok(executors.entry(key).or_insert(executor).clone())
    }

    /// A source's local workers stop (it was replaced or deleted).
    pub(super) fn drop_workers(&self, source: &str) {
        self.executors
            .lock()
            .expect("executors")
            .retain(|(owner, ..), _| owner.as_deref() != Some(source));
    }

    /// Executors of versions nothing needs any more stop.
    pub(super) fn drop_executors_except(&self, needed: &HashSet<(String, i64)>) {
        self.executors
            .lock()
            .expect("executors")
            .retain(|(_, id, version, _), _| needed.contains(&(id.clone(), *version)));
    }
}
