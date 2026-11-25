//! This instance's id among the instances sharing a store, kept across
//! restarts: who its credential registrations and the alerts of the source
//! files it skipped belong to. Built with or without a secret backend.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};

/// Where an instance keeps its id among the instances sharing a store, when
/// `HIE_INSTANCE_ID` is not set.
pub const INSTANCE_ID_FILE: &str = ".hie-instance-id";

/// Removes the file it names when dropped: a creator's own temporary file.
pub(crate) struct OwnTemp(pub(crate) PathBuf);

impl Drop for OwnTemp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Who this instance's registrations of sources for secrets, and the alerts
/// of the source files it skipped, belong to: `HIE_INSTANCE_ID`, else the id
/// kept in the sources directory, made once. Unlike the per-process lease
/// name, it stays the same across restarts, so a restart finds and cleans what
/// an earlier run of this instance left.
pub(crate) fn owner_id(env: Option<String>, sources_dir: &Path) -> String {
    if let Some(id) = env.map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
        return id;
    }
    match stable_id(sources_dir) {
        Ok(id) => id,
        Err(e) => {
            tracing::warn!(
                "this instance's id: set HIE_INSTANCE_ID, or make {} writable; without a stable id, what an earlier run left (credential registrations, alerts of skipped source files) is not cleaned ({e:#})",
                sources_dir.display()
            );
            crate::pipeline::instance_id().to_string()
        }
    }
}

fn stable_id(dir: &Path) -> Result<String> {
    let path = dir.join(INSTANCE_ID_FILE);
    let read = |path: &Path| -> Result<Option<String>> {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let id = text.trim().to_string();
                if id.is_empty() || id.len() > 64 || id.chars().any(char::is_whitespace) {
                    bail!("{} does not hold an instance id", path.display());
                }
                Ok(Some(id))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    };
    if let Some(id) = read(&path)? {
        return Ok(id);
    }
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).map_err(|e| anyhow!("no random bytes for an id: {e}"))?;
    let id = hex::encode(bytes);
    let temp = dir.join(format!("{INSTANCE_ID_FILE}.{id}.tmp"));
    {
        let _own = OwnTemp(temp.clone());
        std::fs::write(&temp, format!("{id}\n"))
            .with_context(|| format!("writing {}", temp.display()))?;
        match std::fs::hard_link(&temp, &path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e).with_context(|| format!("creating {}", path.display())),
        }
    }
    read(&path)?.with_context(|| format!("{} is gone right after it was made", path.display()))
}
