//! Pins: which version a source runs. The store holds each source's pin,
//! set from its definition (when it is created or replaced, or first seen by
//! this process with a definition that changed meanwhile) and by `PUT
//! /api/sources/{id}/pin`; every load reads it afresh when it starts
//! ([`Versions::resolve_for_source`]), so a pin change applies to loads that
//! start afterwards without restarting a running one. Each change ends a pin
//! period and starts another; the periods are the pin half of the usage.

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};

use super::{PinPeriod, PinRow, VersionRef, Versions};
use crate::config::{FeedConfig, PinMode, TransformRef};
use crate::ids::rfc3339_ms_at;
use crate::manager::ManagerError;

/// What `PUT /api/sources/{id}/pin` asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinChange {
    Version(i64),
    FollowCurrent,
}

/// How many runs per version the usage lists by default.
pub const USAGE_RUNS: usize = 50;

fn declared(id: &str, version: Option<i64>) -> String {
    match version {
        Some(v) => format!("{id}@{v}"),
        None => format!("{id}@current"),
    }
}

impl Versions {
    /// The transform a definition's reference names: a pin's id, or the
    /// transform a 0.1 path's file is (imported when new or changed).
    fn reference_id(&self, reference: &TransformRef) -> Result<String> {
        match reference {
            TransformRef::Pin(pin) => Ok(pin.id.clone()),
            TransformRef::File(path) => self.import_file(path),
        }
    }

    /// The pin a definition declares.
    fn declared_pin(&self, feed: &FeedConfig) -> Result<PinRow> {
        let id = self.reference_id(&feed.transform)?;
        let version = match feed.transform.mode() {
            PinMode::Version(v) => Some(v),
            PinMode::FollowCurrent => None,
        };
        Ok(PinRow {
            declared: declared(&id, version),
            transform_id: id,
            version,
        })
    }

    /// The source's pin: the store's, taken from its definition the first time
    /// this process reconciles the source when the definition changed
    /// meanwhile. A source counts as reconciled only once that succeeded (a
    /// definition whose transform cannot be read yet is tried again at the
    /// next call), and its first reconciliation runs under a lock of its own.
    fn effective_pin(&self, feed: &FeedConfig) -> Result<PinRow> {
        if let TransformRef::File(path) = &feed.transform {
            // A 0.1 path follows its file: a changed file is a new version.
            self.import_file(path)?;
        }
        if self.is_reconciled(&feed.id)
            && let Some(row) = self.db.pin_row(&feed.id)?
        {
            return Ok(row);
        }
        let lock = self.source_lock(&feed.id);
        let _reconciling = lock.lock().expect("source reconciliation");
        if self.is_reconciled(&feed.id)
            && let Some(row) = self.db.pin_row(&feed.id)?
        {
            return Ok(row);
        }
        let stored = self.db.pin_row(&feed.id)?;
        let declared = self.declared_pin(feed)?;
        let row = match stored {
            Some(row) if row.declared == declared.declared => row,
            _ => {
                if !self.db.set_pin_row(&feed.id, &declared)? {
                    anyhow::bail!("transform {} not found", declared.transform_id);
                }
                declared
            }
        };
        self.seen
            .lock()
            .expect("seen sources")
            .insert(feed.id.clone());
        Ok(row)
    }

    fn is_reconciled(&self, source: &str) -> bool {
        self.seen.lock().expect("seen sources").contains(source)
    }

    fn source_lock(&self, source: &str) -> Arc<Mutex<()>> {
        self.reconciling
            .lock()
            .expect("source locks")
            .entry(source.to_string())
            .or_default()
            .clone()
    }

    /// The transform a source's pin names.
    pub fn source_transform(&self, feed: &FeedConfig) -> Result<String> {
        Ok(self.effective_pin(feed)?.transform_id)
    }

    /// The version a load of `feed` runs if it starts now: the version its
    /// pin names, or the current one it follows. Read afresh at every start.
    /// An error means no version can be fixed now (the runtime's environment
    /// is not known yet, so the transform files are not imported; the
    /// transform or version does not exist): the caller does not admit the
    /// load, and the next pass tries again.
    pub fn resolve_for_source(&self, feed: &FeedConfig) -> Result<VersionRef> {
        if !self.imported.load(std::sync::atomic::Ordering::Acquire) {
            self.import_dir();
            if !self.imported.load(std::sync::atomic::Ordering::Acquire) {
                anyhow::bail!(
                    "the runtime's default environment is not known yet; no version can be fixed for a load"
                );
            }
        }
        let pin = self.effective_pin(feed)?;
        let t = self
            .transform(&pin.transform_id)?
            .ok_or_else(|| anyhow!("transform {} not found", pin.transform_id))?;
        let number = pin.version.unwrap_or(t.current_version);
        self.db
            .version_info(&t.id, number)?
            .map(|v| v.reference())
            .with_context(|| format!("transform {} has no version {number}", t.id))
    }

    /// A source's `pin` member: `{transform: {id, display_name}, mode,
    /// version, resolved_version}`.
    pub fn pin_view(&self, feed: &FeedConfig) -> Result<Value> {
        let pin = self.effective_pin(feed)?;
        let t = self.transform(&pin.transform_id)?;
        let resolved = match (&t, pin.version) {
            (Some(t), Some(v)) => self.db.version_info(&t.id, v)?.map(|v| v.version),
            (Some(t), None) => Some(t.current_version),
            (None, _) => None,
        };
        Ok(json!({
            "transform": {"id": pin.transform_id, "display_name": t.map(|t| t.display_name)},
            "mode": if pin.version.is_some() { "pinned" } else { "follow_current" },
            "version": pin.version,
            "resolved_version": resolved,
        }))
    }

    /// Checks a definition's transform reference before the source is
    /// written: the transform and the pinned version exist.
    pub fn check_reference(&self, reference: &TransformRef) -> Result<(), ManagerError> {
        let invalid = |m: String| ManagerError::invalid("invalid_definition", m);
        let id = self
            .reference_id(reference)
            .map_err(|e| invalid(format!("{e:#}")))?;
        let t = self
            .transform(&id)?
            .ok_or_else(|| invalid(format!("transform {id} not found")))?;
        if let PinMode::Version(v) = reference.mode()
            && self.db.version_info(&t.id, v)?.is_none()
        {
            return Err(invalid(format!("transform {id} has no version {v}")));
        }
        Ok(())
    }

    /// Moves a source's pin (`PUT /api/sources/{id}/pin`); the new pin, as a
    /// definition writes it. Loads that start afterwards run it.
    pub fn set_pin(
        &self,
        feed: &FeedConfig,
        change: PinChange,
    ) -> Result<TransformRef, ManagerError> {
        let current = self.effective_pin(feed)?;
        let id = current.transform_id;
        if self.transform(&id)?.is_none() {
            return Err(ManagerError::not_found(
                "transform_not_found",
                format!("transform {id} not found"),
            ));
        }
        let version = match change {
            PinChange::Version(v) => {
                if self.db.version_info(&id, v)?.is_none() {
                    return Err(ManagerError::not_found(
                        "version_not_found",
                        format!("transform {id} has no version {v}"),
                    ));
                }
                Some(v)
            }
            PinChange::FollowCurrent => None,
        };
        let row = PinRow {
            declared: declared(&id, version),
            transform_id: id.clone(),
            version,
        };
        if !self.db.set_pin_row(&feed.id, &row)? {
            return Err(ManagerError::not_found(
                "transform_not_found",
                format!("transform {id} not found"),
            ));
        }
        Ok(TransformRef::Pin(crate::config::TransformPin {
            id,
            version,
            follow: version.is_none().then_some(crate::config::Follow::Current),
        }))
    }

    /// A source was created: its pin is its definition's, and a pin period starts.
    pub fn source_created(&self, feed: &FeedConfig) -> Result<()> {
        self.adopt_definition(feed)
    }

    /// A source's definition was replaced: its pin is the new definition's
    /// (a period ends when that changes what it runs); its local workers go.
    pub fn source_replaced(&self, before: &FeedConfig, after: &FeedConfig) -> Result<()> {
        self.drop_workers(&before.id);
        self.adopt_definition(after)
    }

    /// A source was deleted: its pin period ends and its local workers go.
    pub fn source_deleted(&self, source_id: &str) -> Result<()> {
        self.drop_workers(source_id);
        self.seen.lock().expect("seen sources").remove(source_id);
        self.db.remove_pin_row(source_id)
    }

    fn adopt_definition(&self, feed: &FeedConfig) -> Result<()> {
        let lock = self.source_lock(&feed.id);
        let _reconciling = lock.lock().expect("source reconciliation");
        // Until the new definition's pin is in the store, the next load
        // start reconciles it again rather than trusting the old one.
        self.seen.lock().expect("seen sources").remove(&feed.id);
        let declared = self.declared_pin(feed)?;
        if !self.db.set_pin_row(&feed.id, &declared)? {
            anyhow::bail!("transform {} not found", declared.transform_id);
        }
        self.seen
            .lock()
            .expect("seen sources")
            .insert(feed.id.clone());
        Ok(())
    }

    /// `GET /api/transforms/{id}/usage`: per version, every period a source
    /// pinned or followed it (a follow-current period split where the current
    /// version changed), and every load and retry that ran it. Deleted
    /// transforms keep theirs.
    pub fn usage(&self, id: &str, limit: usize) -> Result<Option<Value>> {
        if self.db.transform_info(id)?.is_none() {
            return Ok(None);
        }
        let versions = self.db.version_infos(id)?; // newest first
        let periods = self.db.pin_periods(id)?;
        let mut out = Vec::with_capacity(versions.len());
        for (i, v) in versions.iter().enumerate() {
            // When this version was current: from its save until the next one.
            let current_from = v.created_at;
            let current_until = i.checked_sub(1).map(|newer| versions[newer].created_at);
            let mut pins = Vec::new();
            for p in &periods {
                match p.version {
                    Some(n) if n == v.version => {
                        pins.push(period_json(p, "pinned", p.since, p.until))
                    }
                    Some(_) => {}
                    None => {
                        let since = p.since.max(current_from);
                        let until = match (p.until, current_until) {
                            (Some(a), Some(b)) => Some(a.min(b)),
                            (a, b) => a.or(b),
                        };
                        if until.is_none_or(|u| u > since) {
                            pins.push(period_json(p, "follow_current", since, until));
                        }
                    }
                }
            }
            let runs = self.db.version_runs(id, v.version, limit)?;
            out.push(json!({"version": v.version, "pins": pins, "runs": runs}));
        }
        Ok(Some(json!({"transform": id, "versions": out})))
    }
}

fn period_json(p: &PinPeriod, mode: &str, since: i64, until: Option<i64>) -> Value {
    json!({
        "source": p.source,
        "mode": mode,
        "since": rfc3339_ms_at(since),
        "until": until.and_then(rfc3339_ms_at),
    })
}
