//! A `manifest` source's pass: nothing is listed or read; each open manifest
//! load is moved on, from the status request through the status pages to its
//! receipts. See `db::manifest` for the states and `crate::manifest` for the
//! requests.

use anyhow::Result;
use serde_json::Value;

use super::FeedRunner;
use crate::db::manifest::Due;
use crate::db::{Claim, now_ms};
use crate::manifest::{Page, StepError, Tally};

/// What one step of a submission ends with.
enum Step {
    /// Moved on, or nothing left to do this pass.
    Done,
    /// Asked again from then.
    Later(i64),
    /// Failed for a reason that may pass.
    Transient(String),
    /// The load is unreadable, for this reason.
    Unreadable(String),
    /// The load is unreadable: the FHIR server never received the manifest.
    NotReceived,
    /// The runner no longer holds the submission (stopped, paused, or its
    /// lease or claim lost): it writes nothing.
    Abandoned,
}

impl FeedRunner {
    /// The pass of a `manifest` source: its open loads move on, then the
    /// checks waiting since before the pass are answered with the loads
    /// recorded up to now, then its timed rules run.
    pub(super) async fn manifest_pass(&self, snapshot: &[i64]) -> Result<()> {
        if let Err(e) = self.poll_manifests().await {
            tracing::error!(feed = %self.feed.id, "polling the manifest loads failed, will retry next pass: {}", crate::bulk::safe_reason(&format!("{e:#}")));
        }
        self.answer_checks(snapshot, None);
        self.check_silence()?;
        Ok(())
    }

    async fn poll_manifests(&self) -> Result<()> {
        let Some(client) = &self.manifest else {
            return Ok(());
        };
        // Kick-offs the FHIR server refused whose request could not remove
        // them: never listed, removed here.
        for token in self.db.manifest_refused_left(&self.feed.id)? {
            self.db.manifest_remove_refused(&token)?;
        }
        let leased = self.leased.load(std::sync::atomic::Ordering::Acquire);
        for due in self.db.manifest_due(&self.feed.id)? {
            if self.halted() {
                tracing::info!(feed = %self.feed.id, "stopped during the pass; its manifest loads are left to its successor");
                return Ok(());
            }
            // Paused meanwhile: what is open waits, unpolled.
            if self.db.is_paused(&self.feed.id)? {
                return Ok(());
            }
            if leased
                && !self
                    .db
                    .acquire_lease(&self.feed.id, &self.instance, self.lease_ttl_ms())?
            {
                tracing::info!(feed = %self.feed.id, "another instance took the source during the pass; its manifest loads are left to it");
                return Ok(());
            }
            let Some(claim) = self.db.claim_manifest(&due, &self.instance, leased)? else {
                tracing::debug!(feed = %self.feed.id, submission = %due.submission_id, "not claimed: another runner moved it");
                continue;
            };
            if self.halted() {
                return Ok(());
            }
            let step = match due.status.as_str() {
                "m_sending" => {
                    // Its request stopped before it listed the load.
                    self.db.manifest_list_stale(&due.token, claim)?;
                    Step::Done
                }
                "m_status" => self.ask_status(client, &due, claim, leased).await?,
                "m_polling" => self.read_receipts(client, &due, claim, leased).await?,
                _ => Step::Done,
            };
            match step {
                Step::Done => {}
                Step::Abandoned => {
                    tracing::info!(feed = %self.feed.id, load = due.load_id, "the source was paused, stopped or taken over during a step; the load is left as it was");
                    return Ok(());
                }
                Step::Later(at) => {
                    self.db.manifest_later(&due.token, claim, at)?;
                }
                Step::Unreadable(reason) => {
                    let reason = crate::bulk::scrub(&reason, &due.token);
                    if self
                        .db
                        .manifest_unreadable(&due.token, claim, &reason, false)?
                    {
                        tracing::warn!(feed = %self.feed.id, load = due.load_id, "manifest load unreadable: {reason}");
                    }
                }
                Step::NotReceived => {
                    let reason = crate::manifest::NOT_RECEIVED;
                    if self
                        .db
                        .manifest_unreadable(&due.token, claim, reason, true)?
                    {
                        tracing::warn!(feed = %self.feed.id, load = due.load_id, "manifest load unreadable: {reason}");
                    }
                }
                Step::Transient(reason) => {
                    let reason = crate::bulk::scrub(&reason, &due.token);
                    let attempts = claim.attempts + 1;
                    let d = &self.feed.delivery;
                    let backoff = d
                        .initial_backoff_ms
                        .saturating_mul(1u64 << (attempts - 1).min(20))
                        .min(d.max_backoff_ms);
                    if self.db.manifest_attempt_failed(
                        &due.token,
                        claim,
                        now_ms() + backoff as i64,
                        &reason,
                    )? {
                        self.outage(attempts, &reason)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Whether this runner may send the next request of a step: it is not
    /// stopped, it still holds the source's lease (renewed here, between
    /// requests, so a long step keeps it), and the submission is still under
    /// its claim, its source not paused.
    fn keeps(&self, due: &Due, claim: Claim, leased: bool) -> Result<bool> {
        if self.halted() {
            return Ok(false);
        }
        if leased
            && !self
                .db
                .acquire_lease(&self.feed.id, &self.instance, self.lease_ttl_ms())?
        {
            return Ok(false);
        }
        self.db.manifest_claim_holds(&due.token, claim)
    }

    /// The next poll of an open load, at the source's interval or later when
    /// the server asked for more.
    fn next_poll(&self, at_least: Option<std::time::Duration>) -> i64 {
        let interval = self.feed.source.poll_interval();
        now_ms() + at_least.unwrap_or_default().max(interval).as_millis() as i64
    }

    /// Asks the FHIR server where the submission's status is polled. A server
    /// that does not know a submission whose kick-off went unanswered is
    /// believed only once it has had time to finish that kick-off.
    async fn ask_status(
        &self,
        client: &crate::manifest::Client,
        due: &Due,
        claim: Claim,
        leased: bool,
    ) -> Result<Step> {
        use crate::manifest::Status;
        if !self.keeps(due, claim, leased)? {
            return Ok(Step::Abandoned);
        }
        Ok(
            match client.status_url(&self.feed.id, &due.submission_id).await {
                Ok(Status::Found(url)) => {
                    if self.db.manifest_status_found(&due.token, claim, &url)? {
                        self.recovered()?;
                    }
                    Step::Done
                }
                Ok(Status::Unknown) if due.answered => {
                    Step::Unreadable("the FHIR server no longer knows this submission".into())
                }
                Ok(Status::Unknown) => {
                    let believed = due.created_at + crate::manifest::unknown_after_ms();
                    if now_ms() >= believed {
                        Step::NotReceived
                    } else {
                        Step::Later(believed.max(now_ms() + 1000))
                    }
                }
                Ok(Status::Refused(reason)) => Step::Unreadable(reason),
                Err(StepError::Transient(reason)) => Step::Transient(reason),
                Err(StepError::Unreadable(reason)) => Step::Unreadable(reason),
            },
        )
    }

    /// Reads the status pages from where the last poll stopped, keeping what
    /// each page said, then settles the load once the last page is read.
    async fn read_receipts(
        &self,
        client: &crate::manifest::Client,
        due: &Due,
        claim: Claim,
        leased: bool,
    ) -> Result<Step> {
        let Some(status_url) = due.status_url.as_deref() else {
            return Ok(Step::Unreadable(
                "no status URL is known for this submission".into(),
            ));
        };
        let mut tally = due.progress.clone();
        // Set once a status-only kick-off said the submission was completed.
        let mut completed = false;
        loop {
            if !self.keeps(due, claim, leased)? {
                return Ok(Step::Abandoned);
            }
            let page_url = tally.next.clone().unwrap_or_else(|| status_url.to_string());
            let manifest = match client.page(&page_url, status_url, &due.submission_id).await {
                Ok(Page::InProgress(wait)) => return Ok(Step::Later(self.next_poll(wait))),
                Ok(Page::Unknown) => {
                    return Ok(Step::Unreadable(
                        "the FHIR server no longer knows this submission".into(),
                    ));
                }
                Ok(Page::Done(manifest)) => manifest,
                Err(StepError::Transient(reason)) => return Ok(Step::Transient(reason)),
                Err(StepError::Unreadable(reason)) => return Ok(Step::Unreadable(reason)),
            };
            // The page counts whole or not at all: a receipt that cannot be
            // read leaves the tally as the previous page left it, and the
            // page is read again, for fresh URLs, at the next attempt. Five
            // failed reads in a row of one receipt file give it up.
            let mut page = tally.clone();
            let mut failures = std::mem::take(&mut tally.receipt_failures);
            match self
                .tally_page(
                    client,
                    (due, claim, leased),
                    status_url,
                    &manifest,
                    &mut page,
                    &mut failures,
                )
                .await
            {
                Ok(true) => {}
                Ok(false) => return Ok(Step::Abandoned),
                Err(StepError::Transient(reason)) => {
                    tally.receipt_failures = failures;
                    if !self.db.manifest_checkpoint(&due.token, claim, &tally)? {
                        return Ok(Step::Abandoned);
                    }
                    return Ok(Step::Transient(reason));
                }
                Err(StepError::Unreadable(reason)) => return Ok(Step::Unreadable(reason)),
            }
            page.receipt_failures.clear();
            let next = manifest["link"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|l| l["relation"] == "next")
                .and_then(|l| l["url"].as_str())
                .map(str::to_string);
            if let Some(next) = next {
                page.next = Some(next);
                page.page += 1;
                if !self.db.manifest_checkpoint(&due.token, claim, &page)? {
                    return Ok(Step::Abandoned);
                }
                tally = page;
                continue;
            }
            page.next = None;
            // A load whose kick-off went unanswered and whose status lists no
            // file: a manifest with no files, or a kick-off cut after the
            // submission was registered and before its manifest. Only a
            // completed submission tells; then the status is read once more,
            // since the kick-off may have added the manifest meanwhile.
            if !due.answered && !page.files && !completed {
                use crate::manifest::Probe;
                if !self.keeps(due, claim, leased)? {
                    return Ok(Step::Abandoned);
                }
                match client.probe(&self.feed.id, &due.submission_id).await {
                    Ok(Probe::Completed) => {
                        completed = true;
                        tally = Tally::default();
                        continue;
                    }
                    Ok(Probe::Open) => {
                        let believed = due.created_at + crate::manifest::unknown_after_ms();
                        return Ok(if now_ms() >= believed {
                            Step::NotReceived
                        } else {
                            Step::Later(believed.max(self.next_poll(None)))
                        });
                    }
                    Err(StepError::Transient(reason)) => return Ok(Step::Transient(reason)),
                    Err(StepError::Unreadable(reason)) => return Ok(Step::Unreadable(reason)),
                }
            }
            if self.db.manifest_settle(&due.token, claim, &page)? {
                self.recovered()?;
            }
            return Ok(Step::Done);
        }
    }

    /// Adds one status page to `tally`: the resources written (`output`, by
    /// its counts), the deletions, and the failing outcomes, read line by
    /// line. `false` when the runner stopped holding the submission between
    /// two receipt files. `failures` counts each receipt file's failed reads
    /// in a row: one read whole is taken out, and one that fails for the
    /// `RECEIPT_ATTEMPTS`th time makes the load unreadable.
    async fn tally_page(
        &self,
        client: &crate::manifest::Client,
        (due, claim, leased): (&Due, Claim, bool),
        status_url: &str,
        manifest: &Value,
        tally: &mut Tally,
        failures: &mut std::collections::BTreeMap<String, u32>,
    ) -> Result<bool, StepError> {
        let page = tally.page;
        let counted = |failures: &mut std::collections::BTreeMap<String, u32>,
                       key: String,
                       read: Result<(), StepError>|
         -> Result<(), StepError> {
            match read {
                Ok(()) => {
                    failures.remove(&key);
                    Ok(())
                }
                Err(StepError::Transient(reason)) => {
                    let n = failures.entry(key).or_default();
                    *n += 1;
                    if *n >= crate::manifest::RECEIPT_ATTEMPTS {
                        Err(StepError::Unreadable(format!(
                            "the FHIR server's receipts could not be read: {reason}"
                        )))
                    } else {
                        Err(StepError::Transient(reason))
                    }
                }
                Err(other) => Err(other),
            }
        };
        let keeps = || {
            self.keeps(due, claim, leased)
                .map_err(|e| StepError::Transient(crate::bulk::safe_reason(&format!("{e:#}"))))
        };
        for (i, entry) in manifest["output"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
        {
            tally.files = true;
            let kind = entry["type"].as_str().unwrap_or("unknown").to_string();
            let count = match entry["count"].as_i64() {
                Some(n) => n,
                None => match entry["url"].as_str() {
                    Some(url) => {
                        if !keeps()? {
                            return Ok(false);
                        }
                        let mut n = 0;
                        let read = client
                            .lines(url, status_url, |line| {
                                if serde_json::from_slice::<Value>(line)
                                    .is_ok_and(|v| v["reference"].is_string())
                                {
                                    n += 1;
                                }
                                Ok(())
                            })
                            .await;
                        counted(failures, format!("{page}:output:{i}"), read)?;
                        n
                    }
                    None => 0,
                },
            };
            tally.sent += count;
            *tally.sent_by_type.entry(kind).or_default() += count;
        }
        for entry in manifest["deleted"].as_array().into_iter().flatten() {
            tally.files = true;
            tally.deleted += entry["count"].as_i64().unwrap_or(0);
        }
        for (i, entry) in manifest["outcome"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
        {
            tally.files = true;
            if let Some(url) = entry["url"].as_str() {
                if !keeps()? {
                    return Ok(false);
                }
                let read = client
                    .lines(url, status_url, |line| tally.outcome(line, url))
                    .await;
                counted(failures, format!("{page}:outcome:{i}"), read)?;
            }
        }
        Ok(true)
    }
}
