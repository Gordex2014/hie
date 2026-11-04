//! `$bulk-submit` delivery: submissions opened over the source objects or the
//! engine's transformed NDJSON, polled, settled from receipts, and archived.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::json;

use crate::bulk::{self, BulkSubmitter, Poll};
use crate::config::DeliveryMethod;
use crate::db::now_ms;
use crate::fhir::{self, SendError};
use crate::transform;

use super::FeedRunner;
use super::deliver::Checked;

/// Records settled per page when a submission's receipts come back.
const SETTLE_PAGE: usize = 5_000;

/// How long after a poll still in progress the receipts are polled again
/// (the first poll after a kick-off waits the delivery's initial backoff).
const RECEIPT_POLL_MS: i64 = 2_000;

impl FeedRunner {
    /// Whether `$bulk-submit` is only for the source's first load
    /// (`delivery.initial_method: bulk_submit` with another `method`).
    pub(super) fn initial_bulk_only(&self) -> bool {
        self.feed.delivery.initial_method == Some(DeliveryMethod::BulkSubmit)
            && self.feed.delivery.method != DeliveryMethod::BulkSubmit
    }

    /// The load that goes by `$bulk-submit` while the others go by `method`:
    /// the source's first, when [`FeedRunner::initial_bulk_only`].
    pub(super) fn initial_bulk_load(&self) -> Result<Option<i64>> {
        if !self.initial_bulk_only() {
            return Ok(None);
        }
        self.db.first_load_id(&self.feed.id)
    }

    /// Opens one submission per drop: the files with pending records, up to the
    /// manifest cap, handed to HFS through a manifest the engine serves.
    pub(super) async fn submit_pending(&self) -> Result<()> {
        let Some(bulk) = &self.bulk else {
            return Ok(());
        };
        if self.bulk_transformed {
            return self.submit_transformed(bulk).await;
        }
        // HFS reads the source objects themselves: nothing the engine must see
        // first (validation, namespacing) can happen on this path.
        if let Err(e) = bulk::check_delivery(&self.feed, self.validation) {
            tracing::error!(target: "hie::pipeline", feed = %self.feed.id, "nothing submitted: {e:#}");
            return Ok(());
        }
        // Rows a retry runs were transformed by its version: they go by REST.
        self.deliver_due_where(true).await?;
        loop {
            let files = self
                .db
                .submittable_files(&self.feed.id, self.feed.delivery.manifest_objects_max)?;
            if files.is_empty() {
                return Ok(());
            }
            let token = bulk::token();
            let ttl = Duration::from_secs(self.feed.delivery.url_ttl_seconds);
            let mut entries = Vec::new();
            let mut file_ids = Vec::new();
            let mut records = 0usize;
            let mut hashes = Vec::new();
            let mut generation = 0;
            for file in &files {
                let url = match &file.source_key {
                    // An entry the engine keeps itself is served, never signed.
                    Some(key) if !self.is_staged_locally(key) => {
                        match self.source.share(key, ttl).await? {
                            Some(signed) => signed,
                            None => bulk.file_url(&token, file.file_id),
                        }
                    }
                    _ => bulk.file_url(&token, file.file_id),
                };
                entries.push((file.resource_type.clone(), url, file.count));
                file_ids.push(file.file_id);
                records += file.count;
                hashes.push(file.name.clone());
                generation = generation.max(file.attempts);
            }
            // Rows sent again after unconfirmed receipts are a new submission
            // for the destination, not a repeat it would refuse as known.
            if generation > 0 {
                hashes.push(format!("attempt {generation}"));
            }
            let hash_refs: Vec<&str> = hashes.iter().map(String::as_str).collect();
            let submission_id = bulk::submission_id(&self.feed.id, &hash_refs);
            let manifest = bulk.manifest(&entries);
            self.db.create_submission_for_files(
                &token,
                &self.feed.id,
                &submission_id,
                &manifest,
                &file_ids,
            )?;
            tracing::info!(target: "hie::pipeline", feed = %self.feed.id, submission = %submission_id, files = files.len(), records, "submission opened");
        }
    }

    /// Moves every open submission one step: kick-off, then status polls until the
    /// receipts arrive, then the records' dispositions and the objects' archive.
    pub(super) async fn poll_submissions(&self) -> Result<()> {
        let Some(bulk) = &self.bulk else {
            return Ok(());
        };
        for sub in self.db.submissions_due(&self.feed.id)? {
            let mut unresolved: Option<(usize, u32)> = None;
            let step = match sub.status.as_str() {
                "kickoff" => {
                    match bulk
                        .kickoff(
                            &self.feed.id,
                            &sub.submission_id,
                            &bulk.manifest_url(&sub.token),
                        )
                        .await
                    {
                        Ok(()) => match bulk.status_url(&self.feed.id, &sub.submission_id).await {
                            Ok(url) => Ok(Some(("running", Some(url)))),
                            Err(e) => Err(e),
                        },
                        // HFS already holds this drop: a repeat of the same files.
                        Err(SendError::Refused(409, _)) => {
                            match bulk.status_url(&self.feed.id, &sub.submission_id).await {
                                Ok(url) => Ok(Some(("running", Some(url)))),
                                Err(e) => Err(e),
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => match sub.status_url.as_deref() {
                    None => Ok(Some(("kickoff", None))),
                    Some(url) => match bulk.poll(url).await {
                        Ok(Poll::InProgress) => Ok(None),
                        Ok(Poll::Done {
                            references,
                            errors,
                            malformed,
                        }) => {
                            if malformed > 0 {
                                tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, submission = %sub.submission_id, malformed, "receipt lines that could not be read; their rows stay unknown");
                            }
                            unresolved = self.settle(&sub.token, &references, &errors).await?;
                            Ok(Some(("done", None)))
                        }
                        Err(e) => Err(e),
                    },
                },
            };
            match step {
                Ok(None) => {
                    self.db.update_submission(
                        &sub.token,
                        &sub.status,
                        None,
                        sub.attempts,
                        now_ms() + RECEIPT_POLL_MS,
                        None,
                    )?;
                }
                Ok(Some((status, url))) => {
                    // A running submission is polled for its receipts when due.
                    let next = if status == "running" {
                        now_ms() + self.backoff_ms(1)
                    } else {
                        0
                    };
                    self.db
                        .update_submission(&sub.token, status, url.as_deref(), 0, next, None)?;
                    if status == "done" {
                        match unresolved.take() {
                            // Rows the receipts left unconfirmed go again: their
                            // attempts carry on, and so does the outage count.
                            Some((rows, attempts)) => self.outage(
                                attempts,
                                &format!("the receipts confirmed neither the writing nor the refusal of {rows} row(s)"),
                            )?,
                            None => self.recovered()?,
                        }
                    }
                }
                Err(SendError::Refused(status, body)) => {
                    let reason = fhir::refusal_reason(status, &body);
                    tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, submission = %sub.submission_id, "refused: {reason}");
                    for (record_id, run, _, _) in self.db.submitted_records(&sub.token)? {
                        self.db.mark_failed(
                            record_id,
                            run,
                            "server",
                            &reason,
                            Some(&body),
                            None,
                        )?;
                    }
                    self.db.update_submission(
                        &sub.token,
                        "failed",
                        None,
                        sub.attempts,
                        0,
                        Some(&reason),
                    )?;
                    self.archive_submission(&sub.token).await?;
                    self.remove_submission_files(&sub.token).await;
                }
                Err(SendError::Transient(reason)) => {
                    let attempts = sub.attempts + 1;
                    let d = &self.feed.delivery;
                    let backoff = d
                        .initial_backoff_ms
                        .saturating_mul(1u64 << (attempts - 1).min(20))
                        .min(d.max_backoff_ms);
                    self.db.update_submission(
                        &sub.token,
                        &sub.status,
                        None,
                        attempts,
                        now_ms() + backoff as i64,
                        Some(&reason),
                    )?;
                    self.outage(attempts, &reason)?;
                }
            }
        }
        Ok(())
    }

    /// Gives every submitted record its disposition from the receipts, then
    /// archives the objects HFS has read. A row is written when every one of
    /// its resources has a receipt; failed when HFS reports an error naming
    /// one of them; otherwise its outcome stays unknown and it is submitted
    /// again (missing or unreadable receipts are no verdict). Answers how many
    /// rows went back that way and the most attempts one of them has had.
    pub(super) async fn settle(
        &self,
        token: &str,
        references: &std::collections::HashSet<String>,
        errors: &[serde_json::Value],
    ) -> Result<Option<(usize, u32)>> {
        // Page through the submission: a large one holds a million records, and
        // each page's receipts are applied in sets.
        let mut after_id = 0;
        let again = now_ms() + self.backoff_ms(2);
        let mut unresolved: Option<(usize, u32)> = None;
        loop {
            let page = self
                .db
                .submitted_records_after(token, after_id, SETTLE_PAGE)?;
            if page.is_empty() {
                break;
            }
            let mut sent = Vec::with_capacity(page.len());
            let mut unknown = Vec::new();
            for (record_id, run, _, resources) in &page {
                after_id = *record_id;
                let wanted: Option<Vec<String>> = resources
                    .as_array()
                    .filter(|r| !r.is_empty())
                    .and_then(|r| {
                        r.iter()
                            .map(|r| {
                                transform::check_resource(r)
                                    .ok()
                                    .map(|key| fhir::request_url(&key))
                            })
                            .collect()
                    });
                let Some(wanted) = wanted else {
                    unknown.push((*record_id, *run));
                    continue;
                };
                if wanted.iter().all(|r| references.contains(r)) {
                    let receipts: Vec<serde_json::Value> = wanted
                        .iter()
                        .map(|r| json!({"reference": r, "via": "$bulk-submit"}))
                        .collect();
                    sent.push((*record_id, *run, serde_json::Value::Array(receipts)));
                    continue;
                }
                // An outcome that names one of its missing resources refuses it.
                let refused = wanted
                    .iter()
                    .filter(|r| !references.contains(*r))
                    .find_map(|r| errors.iter().find(|e| e.to_string().contains(r.as_str())));
                match refused {
                    Some(outcome) => self.db.mark_failed(
                        *record_id,
                        *run,
                        "server",
                        &match fhir::first_issue_text(outcome) {
                            Some(text) => format!("HFS refused it: {text}"),
                            None => "HFS refused it".to_string(),
                        },
                        Some(outcome),
                        None,
                    )?,
                    None => unknown.push((*record_id, *run)),
                }
            }
            self.db.mark_sent_many(&sent)?;
            if !unknown.is_empty() {
                let worst = self.db.resubmit(
                    &unknown,
                    again,
                    "no receipt from HFS confirms this row yet; it is submitted again",
                )?;
                let (rows, attempts) = unresolved.unwrap_or((0, 0));
                unresolved = Some((rows + unknown.len(), attempts.max(worst)));
            }
        }
        self.archive_submission(token).await?;
        self.remove_submission_files(token).await;
        Ok(unresolved)
    }

    /// Drops what the engine wrote for a transformed submission, now that HFS has
    /// read it and answered.
    pub(super) async fn remove_submission_files(&self, token: &str) {
        let dir = self.bulk_dir.join(token);
        if dir.is_dir()
            && let Err(e) = std::fs::remove_dir_all(&dir)
        {
            tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, "cannot remove {}: {e}", dir.display());
        }
        if let Err(e) = self.source.remove_submission_files(token).await {
            tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, "cannot remove submission files at the source: {e:#}");
        }
    }

    /// Writes the pending records' resources as NDJSON per resource type, in the
    /// source's store when it can sign URLs (HFS fetches from there) or under
    /// `bulk_dir` served by the engine, and opens one submission over them.
    pub(super) async fn submit_transformed(&self, bulk: &BulkSubmitter) -> Result<()> {
        loop {
            let only = match self.initial_bulk_load()? {
                Some(load) => Some(load),
                None if self.initial_bulk_only() => return Ok(()),
                None => None,
            };
            let items = self.db.submittable_items(
                &self.feed.id,
                self.feed.delivery.bulk_records_max,
                only,
            )?;
            if items.is_empty() {
                return Ok(());
            }
            // With validation required, nothing of the batch goes while the
            // destination cannot validate, and invalid rows never go.
            let generation = items.iter().map(|i| i.attempts).max().unwrap_or(0);
            let records: Vec<(i64, serde_json::Value)> = match self.validate(&items).await? {
                Checked::Held => return Ok(()),
                Checked::Deliver(items) => items
                    .into_iter()
                    .map(|i| (i.id, i.resources.unwrap_or(serde_json::Value::Null)))
                    .collect(),
            };
            if records.is_empty() {
                continue;
            }
            let token = bulk::token();
            let ttl = Duration::from_secs(self.feed.delivery.url_ttl_seconds);
            let mut by_type: std::collections::BTreeMap<String, Vec<u8>> = Default::default();
            let mut counts: std::collections::BTreeMap<String, usize> = Default::default();
            for (_, resources) in &records {
                for resource in resources.as_array().into_iter().flatten() {
                    let kind = resource["resourceType"]
                        .as_str()
                        .unwrap_or("Resource")
                        .to_string();
                    let lines = by_type.entry(kind.clone()).or_default();
                    lines.extend_from_slice(resource.to_string().as_bytes());
                    lines.push(b'\n');
                    *counts.entry(kind).or_default() += 1;
                }
            }
            let mut entries = Vec::new();
            for (n, (kind, bytes)) in by_type.into_iter().enumerate() {
                let n = n as i64 + 1;
                let name = format!("{n}.ndjson");
                let shared = match self
                    .source
                    .put_submission_file(&token, &name, bytes.clone())
                    .await?
                {
                    Some(key) => self.source.share(&key, ttl).await?,
                    None => None,
                };
                let url = match shared {
                    Some(url) => url,
                    None => {
                        let dir = self.bulk_dir.join(&token);
                        std::fs::create_dir_all(&dir)
                            .with_context(|| format!("creating {}", dir.display()))?;
                        bulk::write_served(&self.db, &dir.join(&name), &bytes)?;
                        bulk.file_url(&token, n)
                    }
                };
                entries.push((kind.clone(), url, counts[&kind]));
            }
            let record_ids: Vec<i64> = records.iter().map(|(id, _)| *id).collect();
            let span = format!(
                "records {}-{}",
                record_ids[0],
                record_ids[record_ids.len() - 1]
            );
            // Rows sent again after unconfirmed receipts are a new submission
            // for the destination, not a repeat it would refuse as known.
            let generation = format!("attempt {generation}");
            let parts: Vec<&str> = if generation == "attempt 0" {
                vec![span.as_str()]
            } else {
                vec![span.as_str(), generation.as_str()]
            };
            let submission_id = bulk::submission_id(&self.feed.id, &parts);
            let manifest = bulk.manifest(&entries);
            self.db.create_submission(
                &token,
                &self.feed.id,
                &submission_id,
                &manifest,
                &record_ids,
            )?;
            tracing::info!(target: "hie::pipeline", feed = %self.feed.id, submission = %submission_id, files = entries.len(), records = record_ids.len(), "submission opened (transformed)");
        }
    }

    /// Archives (or, staged by the engine, removes) the input objects of a
    /// submission whose rows have all settled; one with rows still to be
    /// submitted again stays where HFS can read it.
    pub(super) async fn archive_submission(&self, token: &str) -> Result<()> {
        let file_ids: Vec<i64> = {
            let rows = self.db.query_json(
                "SELECT DISTINCT r.file_id FROM records r WHERE r.submission = $1
                   AND NOT EXISTS (SELECT 1 FROM records o WHERE o.file_id = r.file_id
                                     AND o.status NOT IN ('sent', 'rejected'))",
                crate::db::params![token],
            )?;
            rows.iter().filter_map(|r| r["file_id"].as_i64()).collect()
        };
        for file_id in file_ids {
            if let Some((key, name)) = self.db.file_source(file_id)? {
                let archived = if self.is_staged_locally(&key) {
                    // An unpacked entry the engine served: nothing to move, it goes.
                    if let Err(e) = std::fs::remove_file(&key) {
                        tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, "cannot remove {key}: {e}");
                    } else if let Some(dir) = Path::new(&key).parent() {
                        // The container's directory goes with its last entry.
                        let _ = std::fs::remove_dir(dir);
                    }
                    format!("removed {key}")
                } else {
                    self.source
                        .acknowledge(&key, &format!("{file_id}-{name}"))
                        .await?
                };
                self.db.mark_file_archived(file_id, &archived)?;
            }
        }
        Ok(())
    }

    /// Whether a kept file lives in the engine's own `bulk_dir` rather than at the source.
    pub(super) fn is_staged_locally(&self, key: &str) -> bool {
        let prefix = self.bulk_dir.to_string_lossy();
        key.starts_with(prefix.as_ref())
    }
}
