//! A local folder, polled: the folder source, and the spool an MLLP listener
//! writes to.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::config::glob_match;
use crate::connectors::{Connector, Cursors, Incoming, Listed, Probe};

/// A local folder, polled. A file is taken once it has not changed for `min_age`,
/// so a file still being written is left alone.
pub struct FolderSource {
    dir: PathBuf,
    pattern: String,
    min_age: Duration,
    archive: PathBuf,
}

impl FolderSource {
    pub(crate) fn new(dir: PathBuf, pattern: String, min_age: Duration, archive: PathBuf) -> Self {
        Self {
            dir,
            pattern,
            min_age,
            archive,
        }
    }

    /// The folder it reads.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    pub(crate) fn list(&self) -> Result<Vec<String>> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("creating {}", self.dir.display()))?;
        let mut keys = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !glob_match(&self.pattern, name) {
                continue;
            }
            let age = std::fs::metadata(&path)?
                .modified()
                .ok()
                .and_then(|m| SystemTime::now().duration_since(m).ok())
                .unwrap_or_default();
            if age < self.min_age {
                continue;
            }
            keys.push(path.to_string_lossy().into_owned());
        }
        keys.sort();
        Ok(keys)
    }

    pub(crate) fn fetch(&self, key: &str) -> Result<Incoming> {
        let path = Path::new(key);
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string();
        let bytes = std::fs::read(path).with_context(|| format!("reading {key}"))?;
        Ok(Incoming {
            key: key.to_string(),
            name,
            bytes,
        })
    }

    pub(crate) fn acknowledge(&self, key: &str, archived_name: &str) -> Result<String> {
        std::fs::create_dir_all(&self.archive)?;
        let target = self.archive.join(archived_name);
        if std::fs::rename(key, &target).is_err() {
            std::fs::copy(key, &target)?;
            std::fs::remove_file(key)?;
        }
        Ok(target.to_string_lossy().into_owned())
    }

    /// Every file matching the pattern, whatever its age, with when it changed.
    pub(crate) fn found(&self) -> Result<Vec<(String, DateTime<Utc>)>> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(&self.dir)
            .with_context(|| format!("reading {}", self.dir.display()))?
        {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !path.is_file() || !glob_match(&self.pattern, name) {
                continue;
            }
            let modified = std::fs::metadata(&path)?
                .modified()
                .map(DateTime::<Utc>::from)
                .unwrap_or_default();
            found.push((name.to_string(), modified));
        }
        Ok(found)
    }
}

#[async_trait]
impl Connector for FolderSource {
    async fn list(&self, _cursors: &Cursors) -> Result<Vec<Listed>> {
        Ok(FolderSource::list(self)?
            .into_iter()
            .map(Listed::from_key)
            .collect())
    }

    async fn fetch(&self, key: &str) -> Result<Incoming> {
        FolderSource::fetch(self, key)
    }

    async fn acknowledge(
        &self,
        key: &str,
        archived_name: &str,
        _cursors: &Cursors,
    ) -> Result<String> {
        FolderSource::acknowledge(self, key, archived_name)
    }

    async fn probe(&self, _cursors: &Cursors) -> Result<Probe> {
        let started = Instant::now();
        Ok(Probe::found(started, &self.found()?))
    }
}
