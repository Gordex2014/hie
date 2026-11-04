//! Transforms as resources: one Python file per transform in `transforms_dir`,
//! listed with the sources that use it, saved (restarting those sources so
//! they load the new code) and tried on supplied records.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::{ManagerError, SourceManager};
use crate::transform::{Outcome, Transformer, check_resource};

impl SourceManager {
    fn transform_path(&self, name: &str) -> Result<PathBuf, ManagerError> {
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(ManagerError::invalid(
                "invalid_body",
                format!("transform name {name:?} must be letters, digits, underscores and dashes"),
            ));
        }
        Ok(self.transforms_dir.join(format!("{name}.py")))
    }

    fn transform_name(path: &Path) -> Option<String> {
        (path.extension().and_then(|e| e.to_str()) == Some("py"))
            .then(|| path.file_stem()?.to_str().map(str::to_string))
            .flatten()
    }

    /// Every transform file, with the sources that use it.
    pub fn transforms(&self) -> Result<Vec<Value>> {
        std::fs::create_dir_all(&self.transforms_dir)?;
        let sources = self.sources();
        let mut entries: Vec<PathBuf> = std::fs::read_dir(&self.transforms_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file() && Self::transform_name(p).is_some())
            .collect();
        entries.sort();
        let mut out = Vec::new();
        for path in entries {
            let name = Self::transform_name(&path).unwrap_or_default();
            let meta = std::fs::metadata(&path)?;
            let modified = meta
                .modified()
                .ok()
                .map(|m| DateTime::<Utc>::from(m).to_rfc3339());
            let canonical = path.canonicalize().unwrap_or(path.clone());
            let used_by: Vec<&str> = sources
                .iter()
                .filter(|f| {
                    f.transform
                        .file()
                        .and_then(|p| p.canonicalize().ok())
                        .as_ref()
                        == Some(&canonical)
                })
                .map(|f| f.id.as_str())
                .collect();
            let relative = path
                .strip_prefix(&self.base_dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            out.push(json!({
                "name": name,
                "path": relative,
                "size": meta.len(),
                "modified_at": modified,
                "used_by": used_by,
            }));
        }
        Ok(out)
    }

    pub fn transform_code(&self, name: &str) -> Result<Option<String>, ManagerError> {
        let path = self.transform_path(name)?;
        if !path.is_file() {
            return Ok(None);
        }
        Ok(Some(
            std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?,
        ))
    }

    /// Writes the transform's code and restarts the sources that use it, so their
    /// Python workers load the new version.
    pub fn save_transform(&self, name: &str, code: &str) -> Result<bool, ManagerError> {
        let path = self.transform_path(name)?;
        std::fs::create_dir_all(&self.transforms_dir).map_err(anyhow::Error::from)?;
        let existed = path.exists();
        std::fs::write(&path, code).with_context(|| format!("writing {}", path.display()))?;
        let canonical = path.canonicalize().unwrap_or(path);
        for feed in self.sources() {
            if feed
                .transform
                .file()
                .and_then(|p| p.canonicalize().ok())
                .as_ref()
                == Some(&canonical)
            {
                self.start(feed)?;
            }
        }
        Ok(existed)
    }

    /// Runs a transform on supplied records without loading anything: what each
    /// record became, or why it was rejected.
    pub async fn try_transform(
        &self,
        name: &str,
        records: &[Value],
    ) -> Result<Vec<Value>, ManagerError> {
        let path = self.transform_path(name)?;
        if !path.is_file() {
            return Err(ManagerError::not_found(
                "transform_not_found",
                format!("transform {name} not found"),
            ));
        }
        let mut transformer = Transformer::new(&self.python, &self.runtime_dir, &path);
        transformer.set_terminology(self.terminology_url.as_deref());
        transformer.set_runtime(self.runtime.as_ref());
        let items: Vec<(Value, Value)> = records
            .iter()
            .enumerate()
            .map(|(i, record)| {
                (
                    record.clone(),
                    json!({"feed": "try", "file": "try", "line": i + 1}),
                )
            })
            .collect();
        let outcomes = transformer.try_many(&items).await.map_err(|e| {
            ManagerError::invalid(
                "transform_failed",
                format!("transform failed to run: {e:#}"),
            )
        })?;
        let mut out = Vec::with_capacity(records.len());
        for (record, outcome) in records.iter().zip(outcomes) {
            out.push(match outcome {
                Outcome::Rejected { kind, message } => {
                    json!({"record": record, "error": {"kind": kind, "message": message}})
                }
                Outcome::Transient(message) => {
                    json!({"record": record, "error": {"kind": "transient", "message": message}})
                }
                Outcome::Resources(resources) => {
                    let problem = resources.iter().find_map(|r| check_resource(r).err());
                    match problem {
                        Some(message) => json!({"record": record, "resources": resources,
                            "error": {"kind": "validation", "message": message}}),
                        None => json!({"record": record, "resources": resources}),
                    }
                }
            });
        }
        Ok(out)
    }
}
