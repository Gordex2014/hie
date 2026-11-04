//! Where a connector comes into being for a source definition: the registry's
//! `build` for its kind. Anything that must hold before a source may connect
//! anywhere (an admission policy) belongs here, ahead of the build.

use anyhow::Result;

use super::{Connector, kind};
use crate::config::FeedConfig;

/// The connector for a resolved definition.
pub(super) fn connector(feed: &FeedConfig) -> Result<Box<dyn Connector>> {
    (kind(&feed.source).build)(feed)
}
