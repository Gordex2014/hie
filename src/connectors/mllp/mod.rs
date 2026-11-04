//! An MLLP listener for HL7 v2 as a source: every message received is written
//! to the spool before it is acknowledged (`listener`), then picked up from
//! there like a file by the folder source.

mod listener;

use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;

use super::object::FolderSource;
use super::{Connector, Cursors, Incoming, Kind, Listed, Probe};
use crate::config::{FeedConfig, Format, SourceConfig};
pub use listener::{END, START, ack, frame, serve};

/// The MLLP kind in the registry: its records are HL7 v2 messages.
pub(super) const KIND: Kind = Kind {
    name: "mllp",
    format: Some(Format::Hl7v2),
    check,
    resolve,
    describe,
    build,
};

fn check(source: &SourceConfig) -> Result<()> {
    if let SourceConfig::Mllp { listen, .. } = source
        && listen.parse::<std::net::SocketAddr>().is_err()
    {
        bail!("mllp: listen must be an address such as 0.0.0.0:2575, not {listen:?}");
    }
    Ok(())
}

/// The spool is relative to the directory holding `hie.yaml`.
fn resolve(source: &mut SourceConfig, base: &Path) {
    if let SourceConfig::Mllp { spool, .. } = source {
        *spool = base.join(&*spool);
    }
}

fn describe(source: &SourceConfig) -> String {
    match source {
        SourceConfig::Mllp { listen, spool, .. } => {
            format!("mllp://{listen} (spool {})", spool.display())
        }
        _ => String::new(),
    }
}

fn build(feed: &FeedConfig) -> Result<Box<dyn Connector>> {
    let SourceConfig::Mllp { listen, spool, .. } = &feed.source else {
        bail!("source {} is not mllp", feed.id);
    };
    Ok(Box::new(MllpSource {
        feed_id: feed.id.clone(),
        listen: listen.clone(),
        folder: FolderSource::new(
            spool.clone(),
            "*.hl7".into(),
            Duration::ZERO,
            spool.join("processed"),
        ),
        started: Mutex::new(false),
    }))
}

/// An MLLP listener whose messages wait in a spool folder: the listener writes
/// them, the folder source takes them.
pub struct MllpSource {
    feed_id: String,
    listen: String,
    folder: FolderSource,
    started: Mutex<bool>,
}

#[async_trait]
impl Connector for MllpSource {
    /// Binds the port once; a port that cannot be bound is reported and tried
    /// again on the next call.
    async fn start(&self) -> Result<()> {
        if *self.started.lock().expect("mllp mutex") {
            return Ok(());
        }
        let listener = tokio::net::TcpListener::bind(&self.listen)
            .await
            .with_context(|| format!("cannot listen for MLLP on {}", self.listen))?;
        tracing::info!(target: "hie::source", feed = %self.feed_id, listen = %self.listen, spool = %self.folder.dir().display(), "listening for MLLP");
        tokio::spawn(serve(
            self.feed_id.clone(),
            listener,
            self.folder.dir().to_path_buf(),
        ));
        *self.started.lock().expect("mllp mutex") = true;
        Ok(())
    }

    async fn list(&self, _cursors: &Cursors) -> Result<Vec<Listed>> {
        Ok(self
            .folder
            .list()?
            .into_iter()
            .map(Listed::from_key)
            .collect())
    }

    async fn fetch(&self, key: &str) -> Result<Incoming> {
        self.folder.fetch(key)
    }

    async fn acknowledge(
        &self,
        key: &str,
        archived_name: &str,
        _cursors: &Cursors,
    ) -> Result<String> {
        self.folder.acknowledge(key, archived_name)
    }

    /// Can the port be taken? Then what waits in the spool.
    async fn probe(&self, _cursors: &Cursors) -> Result<Probe> {
        let started = Instant::now();
        if !*self.started.lock().expect("mllp mutex") {
            drop(
                tokio::net::TcpListener::bind(&self.listen)
                    .await
                    .with_context(|| format!("cannot listen on {}", self.listen))?,
            );
        }
        Ok(Probe::found(started, &self.folder.found()?))
    }
}
