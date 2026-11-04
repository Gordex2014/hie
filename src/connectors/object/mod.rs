//! Object sources: a local folder, or an object store (S3) polled by listing
//! a prefix and optionally told of new objects by the bucket's SQS queue.
//!
//! - `folder`: `FolderSource`, also the spool of the MLLP connector.
//! - `store`: `ObjectSource` over any `object_store` (S3 in production, in
//!   memory in tests), with the bulk path's staging.
//! - `sqs`: the notification queue, read with the bucket's own credentials.

pub mod folder;
pub mod sqs;
pub mod store;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use object_store::aws::AmazonS3Builder;

use super::{Connector, Kind};
use crate::config::{FeedConfig, SourceConfig};
pub use folder::FolderSource;
use sqs::SqsQueue;
pub use store::ObjectSource;

/// The folder kind in the registry.
pub(super) const FOLDER: Kind = Kind {
    name: "folder",
    format: None,
    check: |_| Ok(()),
    resolve: resolve_folder,
    describe,
    build: build_folder,
};

/// The S3 kind in the registry.
pub(super) const S3: Kind = Kind {
    name: "s3",
    format: None,
    check: check_s3,
    resolve: |_, _| {},
    describe,
    build: build_s3,
};

fn describe(source: &SourceConfig) -> String {
    match source {
        SourceConfig::Folder { path, .. } => path.display().to_string(),
        SourceConfig::S3 { bucket, prefix, .. } => format!("s3://{bucket}/{prefix}"),
        _ => String::new(),
    }
}

/// Makes a folder's paths absolute against the directory holding `hie.yaml`.
fn resolve_folder(source: &mut SourceConfig, base: &Path) {
    if let SourceConfig::Folder { path, archive, .. } = source {
        *path = base.join(&*path);
        if let Some(a) = archive {
            *a = base.join(&*a);
        }
    }
}

/// A queue needs its URL, credentials and a relist interval.
fn check_s3(source: &SourceConfig) -> Result<()> {
    if let SourceConfig::S3 {
        queue: Some(queue),
        anonymous,
        relist_seconds,
        ..
    } = source
    {
        if !(queue.starts_with("http://") || queue.starts_with("https://")) {
            bail!("s3: queue must be the SQS queue URL, not {queue:?}");
        }
        if *anonymous {
            bail!("s3: a queue is read with credentials; anonymous and queue do not go together");
        }
        if *relist_seconds == 0 {
            bail!("s3: relist_seconds must be at least 1");
        }
    }
    Ok(())
}

fn build_folder(
    feed: &FeedConfig,
    _ctx: &crate::connectors::Context,
) -> Result<Box<dyn Connector>> {
    let SourceConfig::Folder {
        path,
        pattern,
        min_file_age_ms,
        archive,
        ..
    } = &feed.source
    else {
        bail!("source {} is not a folder", feed.id);
    };
    Ok(Box::new(FolderSource::new(
        path.clone(),
        pattern.clone(),
        Duration::from_millis(*min_file_age_ms),
        archive.clone().unwrap_or_else(|| path.join("processed")),
    )))
}

/// An S3 bucket. Credentials come from the usual AWS environment
/// (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, profiles).
fn build_s3(feed: &FeedConfig, _ctx: &crate::connectors::Context) -> Result<Box<dyn Connector>> {
    let SourceConfig::S3 {
        bucket,
        prefix,
        region,
        endpoint,
        pattern,
        min_file_age_ms,
        archive_prefix,
        anonymous,
        read_only,
        queue,
        relist_seconds,
        ..
    } = &feed.source
    else {
        bail!("source {} is not s3", feed.id);
    };
    // Objects of hundreds of megabytes take minutes to fetch.
    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_client_options(
            object_store::ClientOptions::new().with_timeout(Duration::from_secs(1800)),
        );
    if *anonymous {
        builder = builder.with_skip_signature(true);
    }
    if let Some(region) = region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = endpoint {
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false);
    }
    let store = Arc::new(
        builder
            .build()
            .with_context(|| format!("feed {}: S3 source", feed.id))?,
    );
    let mut source = ObjectSource::new(
        store.clone(),
        prefix,
        archive_prefix,
        pattern,
        Duration::from_millis(*min_file_age_ms),
    )
    .read_only(*read_only);
    if let Some(queue) = queue {
        let region = SqsQueue::region_of(queue)
            .or_else(|| region.clone())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
            .or_else(|| std::env::var("AWS_REGION").ok())
            .unwrap_or_else(|| "us-east-1".into());
        source = source
            .with_queue(SqsQueue::new(queue, &region, store.credentials().clone()))
            .relist_every(Duration::from_secs(*relist_seconds));
    }
    Ok(Box::new(if *anonymous {
        source
    } else {
        source.with_signer(store)
    }))
}
