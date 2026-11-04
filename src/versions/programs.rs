//! What no longer needs to run is let go: the names stored at the runtime
//! for versions that no pin names (pinned, or current and followed) and no
//! unfinished load or retry still has rows waiting to run, and the executors
//! (local workers) of those versions. Best effort, at most once a minute; a
//! name reclaimed too early is stored again on its next run (`404
//! transform_unknown`).

use std::collections::HashSet;
use std::time::{Duration, Instant};

use anyhow::Result;

use super::Versions;
use crate::runtime::is_program_name;

const EVERY: Duration = Duration::from_secs(60);

impl Versions {
    /// Frees what no version that may still run needs. The first call of a
    /// process also reconciles with the runtime's own list of names.
    pub async fn reclaim(&self) {
        let first = {
            let mut last = self.reclaimed.lock().expect("reclaimed");
            if last.is_some_and(|at| at.elapsed() < EVERY) {
                return;
            }
            let first = last.is_none();
            *last = Some(Instant::now());
            first
        };
        if let Err(e) = self.reclaim_now(first).await {
            tracing::warn!(target: "hie::versions", "reclaiming unused transform versions: {e:#}");
        }
    }

    /// The versions a load may still run: pinned, current and followed, or
    /// with rows waiting to run.
    fn needed(&self) -> Result<HashSet<(String, i64)>> {
        let mut needed: HashSet<(String, i64)> =
            self.db.versions_waiting_to_run()?.into_iter().collect();
        for pin in self.db.pin_rows()? {
            match pin.version {
                Some(v) => {
                    needed.insert((pin.transform_id, v));
                }
                None => {
                    if let Some(t) = self.transform(&pin.transform_id)? {
                        needed.insert((t.id, t.current_version));
                    }
                }
            }
        }
        Ok(needed)
    }

    async fn reclaim_now(&self, reconcile: bool) -> Result<()> {
        let needed = self.needed()?;
        self.drop_executors_except(&needed);
        let Some(facade) = &self.facade else {
            return Ok(());
        };
        let mut unused: Vec<String> = self
            .db
            .programs()?
            .into_iter()
            .filter(|(_, id, version)| !needed.contains(&(id.clone(), *version)))
            .map(|(name, ..)| name)
            .collect();
        if reconcile {
            // Names this engine stored before it knew of them (or before a
            // store was replaced): the shape of ours, and not needed.
            let needed_names: HashSet<String> = self
                .db
                .programs()?
                .into_iter()
                .filter(|(_, id, version)| needed.contains(&(id.clone(), *version)))
                .map(|(name, ..)| name)
                .collect();
            if let Ok(held) = facade.list().await {
                for (name, _) in held {
                    if is_program_name(&name)
                        && !needed_names.contains(&name)
                        && !unused.contains(&name)
                        && !self.held_for_needed(&name, &needed)
                    {
                        unused.push(name);
                    }
                }
            }
        }
        for name in unused {
            match facade.delete(&name).await {
                Ok(true) => self.db.program_forgotten(&name)?,
                Ok(false) => {}
                Err(e) => {
                    tracing::debug!(target: "hie::versions", name, "the runtime did not free a name: {e:#}");
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Whether a name at the runtime is the name of a needed version (one
    /// stored by an earlier process, unknown to this store's list).
    fn held_for_needed(&self, name: &str, needed: &HashSet<(String, i64)>) -> bool {
        needed.iter().any(|(id, version)| {
            self.db
                .version_info(id, *version)
                .ok()
                .flatten()
                .is_some_and(|v| crate::runtime::program_name(id, *version, &v.code_sha256) == name)
        })
    }
}
