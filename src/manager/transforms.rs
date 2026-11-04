//! The manager's side of transforms and pins: what only the manager knows
//! (every source, its definition file) joined with the version store: the
//! transform list with the sources that use each, deleting a transform no
//! source uses, moving a source's pin (and its definition with it), and
//! checking a definition's transform reference.

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde_json::Value;

use super::{ManagerError, SourceManager};
use crate::config::{FeedConfig, TransformRef};
use crate::versions::{PinChange, TransformInfo};

impl SourceManager {
    /// How this engine's executors are made (the version store holds its own copy).
    pub fn executor_settings(&self) -> crate::versions::ExecutorSettings {
        crate::versions::ExecutorSettings {
            python: self.python.clone(),
            runtime_dir: self.runtime_dir.clone(),
            terminology: self.terminology_url.clone(),
            runtime: self.runtime.clone(),
        }
    }

    /// Where transform files are found and written.
    pub fn transforms_dir(&self) -> &std::path::Path {
        &self.transforms_dir
    }

    /// The sources whose pin names the transform (pinned or following).
    pub fn transform_users(&self, transform_id: &str) -> Vec<String> {
        self.sources()
            .iter()
            .filter(|feed| {
                self.versions()
                    .source_transform(feed)
                    .is_ok_and(|id| id == transform_id)
            })
            .map(|feed| feed.id.clone())
            .collect()
    }

    /// `GET /api/transforms`: every transform with its 0.1 members (`name`,
    /// `path`, `size`, `modified_at`, `used_by`) and its 0.2.0 ones.
    pub fn transforms(&self) -> anyhow::Result<Vec<Value>> {
        let versions = self.versions();
        let sources = self.sources();
        let users: Vec<(String, String)> = sources
            .iter()
            .filter_map(|feed| Some((versions.source_transform(feed).ok()?, feed.id.clone())))
            .collect();
        let mut out = Vec::new();
        for t in versions.transforms()? {
            let mut view = versions.transform_json(&t);
            let (path, size, modified) = self.file_facts(&t)?;
            view["path"] = path.into();
            view["size"] = size.into();
            view["modified_at"] = modified.into();
            let used_by: Vec<&str> = users
                .iter()
                .filter(|(id, _)| *id == t.id)
                .map(|(_, source)| source.as_str())
                .collect();
            view["used_by"] = used_by.into();
            out.push(view);
        }
        Ok(out)
    }

    /// A transform's file as 0.1 listed it: its path relative to the base
    /// directory, its size and when it changed; without a file, the current
    /// version's size and save time.
    fn file_facts(
        &self,
        t: &TransformInfo,
    ) -> anyhow::Result<(Option<String>, i64, Option<String>)> {
        let base = self
            .base_dir
            .canonicalize()
            .unwrap_or(self.base_dir.clone());
        if let Some(path) = &t.file_path {
            let path = std::path::Path::new(path);
            if let Ok(meta) = std::fs::metadata(path) {
                let relative = path
                    .strip_prefix(&base)
                    .unwrap_or(path)
                    .to_string_lossy()
                    .replace('\\', "/");
                let modified = meta
                    .modified()
                    .ok()
                    .map(|m| DateTime::<Utc>::from(m).to_rfc3339());
                return Ok((Some(relative), meta.len() as i64, modified));
            }
        }
        let size = self
            .versions()
            .code(&t.id, t.current_version)?
            .map_or(0, |c| c.len() as i64);
        Ok((None, size, crate::ids::rfc3339_ms_at(t.updated_at)))
    }

    /// `DELETE /api/transforms/{id}`: refused while a source pins or follows it.
    pub fn delete_transform(&self, id: &str) -> Result<(), ManagerError> {
        let users = self.transform_users(id);
        self.versions().delete(id, &users)
    }

    /// Checks a definition's transform reference against the version store
    /// (`400 invalid_definition` when the transform or the version does not exist).
    pub(super) fn check_transform(&self, feed: &FeedConfig) -> Result<(), ManagerError> {
        self.versions().check_reference(&feed.transform)
    }

    /// `PUT /api/sources/{id}/pin`: the source's pin moves, its definition
    /// file with it, and loads that start afterwards run it; a running load
    /// keeps its version (the runner is not restarted).
    pub fn set_pin(&self, id: &str, change: PinChange) -> Result<FeedConfig, ManagerError> {
        let feed = self.source(id).ok_or_else(|| {
            ManagerError::not_found("source_not_found", format!("source {id} not found"))
        })?;
        let pin = self.versions().set_pin(&feed, change)?;
        self.write_pin(id, &pin)?;
        let mut sources = self.sources.lock().expect("manager mutex");
        let managed = sources.get_mut(id).ok_or_else(|| {
            ManagerError::not_found("source_not_found", format!("source {id} not found"))
        })?;
        managed.feed.transform = pin;
        Ok(managed.feed.clone())
    }

    /// Writes a pin into the source's definition file, the rest as it is.
    fn write_pin(&self, id: &str, pin: &TransformRef) -> anyhow::Result<()> {
        let path = self.file_for(id);
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let mut yaml: serde_yaml::Value = serde_yaml::from_str(&text)?;
        if let serde_yaml::Value::Mapping(map) = &mut yaml {
            map.insert("transform".into(), serde_yaml::to_value(pin)?);
        }
        std::fs::write(&path, serde_yaml::to_string(&yaml)?)
            .with_context(|| format!("writing {}", path.display()))
    }
}
