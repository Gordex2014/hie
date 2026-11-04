//! Where a connector comes into being for a source definition: the registry's
//! `build` for its kind, with the engine's [`Context`]. Building never
//! connects; the admission policy travels in the context so that a connector
//! checks it before every connection it opens (a pass, a probe), and so that
//! nothing reaches a secret or a socket for a connection the policy refuses.

use anyhow::Result;

use super::{Connector, Context, kind};
use crate::config::FeedConfig;

/// The connector for a resolved definition.
pub(super) fn connector(feed: &FeedConfig, ctx: &Context) -> Result<Box<dyn Connector>> {
    (kind(&feed.source).build)(feed, ctx)
}
