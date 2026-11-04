//! Another FHIR server as a source, pulled by `_lastUpdated`, one cursor per
//! resource type. `client` is how the engine authenticates to it.

mod client;

use std::collections::HashMap;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::{Connector, Cursors, Incoming, Kind, Listed, Probe};
use crate::config::{FeedConfig, Format, RemoteAuth, SourceConfig};
use client::Client;

/// The FHIR server kind in the registry: what it pulls is NDJSON.
pub(super) const KIND: Kind = Kind {
    name: "fhir_server",
    format: Some(Format::Ndjson),
    check,
    resolve: |_, _| {},
    describe,
    build,
};

/// What the type itself cannot check: at least one resource type, sane paging,
/// and a URL that looks like one.
fn check(source: &SourceConfig) -> Result<()> {
    let SourceConfig::FhirServer {
        url,
        types,
        page_size,
        max_pages,
        ..
    } = source
    else {
        return Ok(());
    };
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        bail!("fhir_server url must start with http:// or https://");
    }
    if types.is_empty() {
        bail!("fhir_server needs at least one resource type in types");
    }
    if let Some(bad) = types
        .iter()
        .find(|t| t.is_empty() || !t.chars().all(|c| c.is_ascii_alphanumeric()))
    {
        bail!("fhir_server: {bad:?} is not a resource type");
    }
    if *page_size == 0 || *max_pages == 0 {
        bail!("fhir_server: page_size and max_pages must be at least 1");
    }
    Ok(())
}

fn describe(source: &SourceConfig) -> String {
    match source {
        SourceConfig::FhirServer { url, types, .. } => format!("{url} ({})", types.join(", ")),
        _ => String::new(),
    }
}

fn build(feed: &FeedConfig, _ctx: &crate::connectors::Context) -> Result<Box<dyn Connector>> {
    let SourceConfig::FhirServer {
        url,
        types,
        page_size,
        max_pages,
        since,
        auth,
        ..
    } = &feed.source
    else {
        bail!("source {} is not a fhir_server", feed.id);
    };
    Ok(Box::new(FhirServerSource::new(
        &feed.id,
        url,
        types,
        *page_size,
        *max_pages,
        since.clone(),
        auth.as_ref(),
    )?))
}

/// Another FHIR server, pulled by `_lastUpdated`. Each resource type is a key: a
/// pass searches it for what changed after the type's cursor, in pages sorted by
/// `_lastUpdated`, and hands the resources on as one NDJSON "file". The cursor
/// moves to the newest `meta.lastUpdated` seen once the engine has stored the
/// records, so a crash in between pulls the same resources again (idempotent
/// `PUT`s) rather than skipping any.
pub struct FhirServerSource {
    feed_id: String,
    base: String,
    types: Vec<String>,
    page_size: usize,
    max_pages: usize,
    since: Option<String>,
    client: Client,
    /// What `list` pulled, waiting for `fetch` and `acknowledge`.
    pulled: std::sync::Mutex<HashMap<String, Pulled>>,
}

struct Pulled {
    bytes: Vec<u8>,
    count: usize,
    newest: String,
}

impl FhirServerSource {
    pub fn new(
        feed_id: &str,
        url: &str,
        types: &[String],
        page_size: usize,
        max_pages: usize,
        since: Option<String>,
        auth: Option<&RemoteAuth>,
    ) -> Result<Self> {
        let client = Client::new(auth)?;
        Ok(Self {
            feed_id: feed_id.to_string(),
            base: url.trim_end_matches('/').to_string(),
            types: types.to_vec(),
            page_size,
            max_pages,
            since,
            client,
            pulled: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Where a type's search starts: its stored cursor, else `since`.
    fn cursor(&self, resource_type: &str, cursors: &Cursors) -> Result<Option<String>> {
        Ok(cursors.get(resource_type)?.or_else(|| self.since.clone()))
    }

    /// Searches one type for what changed after its cursor, page by page.
    async fn pull(&self, resource_type: &str, cursors: &Cursors) -> Result<Pulled> {
        let cursor = self.cursor(resource_type, cursors)?;
        let mut url = format!(
            "{}/{resource_type}?_sort=_lastUpdated&_count={}",
            self.base, self.page_size
        );
        if let Some(c) = &cursor {
            url.push_str(&format!("&_lastUpdated=gt{c}"));
        }
        let mut bytes = Vec::new();
        let mut count = 0;
        let mut newest = cursor.clone().unwrap_or_default();
        let mut next = Some(url);
        let mut pages = 0;
        while let Some(url) = next.take() {
            if pages >= self.max_pages {
                break;
            }
            pages += 1;
            let bundle = self.client.get(&url).await?;
            for entry in bundle["entry"].as_array().into_iter().flatten() {
                let resource = &entry["resource"];
                if resource.get("resourceType").and_then(|t| t.as_str()) != Some(resource_type) {
                    continue;
                }
                if let Some(at) = resource["meta"]["lastUpdated"].as_str()
                    && later(at, &newest)
                {
                    newest = at.to_string();
                }
                bytes.extend_from_slice(resource.to_string().as_bytes());
                bytes.push(b'\n');
                count += 1;
            }
            next = bundle["link"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|l| l["relation"] == "next")
                .and_then(|l| l["url"].as_str())
                .map(str::to_string);
        }
        Ok(Pulled {
            bytes,
            count,
            newest,
        })
    }
}

#[async_trait]
impl Connector for FhirServerSource {
    async fn list(&self, cursors: &Cursors) -> Result<Vec<Listed>> {
        let mut keys = Vec::new();
        for resource_type in &self.types {
            let pulled = self.pull(resource_type, cursors).await?;
            if pulled.count == 0 {
                continue;
            }
            tracing::debug!(target: "hie::source", feed = %self.feed_id, resource_type, resources = pulled.count, newest = %pulled.newest, "pulled");
            self.pulled
                .lock()
                .expect("pulled mutex")
                .insert(resource_type.clone(), pulled);
            keys.push(Listed::from_key(resource_type.clone()));
        }
        Ok(keys)
    }

    async fn fetch(&self, key: &str) -> Result<Incoming> {
        let pulled = self.pulled.lock().expect("pulled mutex");
        let p = pulled
            .get(key)
            .with_context(|| format!("{key} was not pulled in this pass"))?;
        Ok(Incoming {
            key: key.to_string(),
            name: format!("{key}-{}.ndjson", Utc::now().format("%Y%m%dT%H%M%SZ")),
            bytes: p.bytes.clone(),
        })
    }

    /// The records are stored: the cursor may move past them.
    async fn acknowledge(
        &self,
        key: &str,
        _archived_name: &str,
        cursors: &Cursors,
    ) -> Result<String> {
        let newest = self
            .pulled
            .lock()
            .expect("pulled mutex")
            .remove(key)
            .map(|p| p.newest)
            .with_context(|| format!("{key} was not pulled in this pass"))?;
        if !newest.is_empty() {
            cursors.set(key, &newest)?;
        }
        Ok(format!("cursor {newest}"))
    }

    /// The server's capability statement, then what each type holds past its cursor.
    async fn probe(&self, cursors: &Cursors) -> Result<Probe> {
        let started = Instant::now();
        self.client.get(&format!("{}/metadata", self.base)).await?;
        let mut objects = 0;
        let mut newest: Option<(String, String)> = None;
        for resource_type in &self.types {
            let mut url = format!("{}/{resource_type}?_sort=-_lastUpdated&_count=1", self.base);
            if let Some(c) = self.cursor(resource_type, cursors)? {
                url.push_str(&format!("&_lastUpdated=gt{c}"));
            }
            let bundle = self.client.get(&url).await?;
            // Not every server puts a total on a page (HFS does not); ask for it.
            let total = match bundle["total"].as_u64() {
                Some(t) => t,
                None => self
                    .client
                    .get(&url.replace("&_count=1", "&_summary=count"))
                    .await?["total"]
                    .as_u64()
                    .unwrap_or(bundle["entry"].as_array().map_or(0, Vec::len) as u64),
            };
            objects += total as usize;
            if let Some(at) = bundle["entry"][0]["resource"]["meta"]["lastUpdated"].as_str()
                && newest.as_ref().is_none_or(|(_, n)| later(at, n))
            {
                newest = Some((resource_type.clone(), at.to_string()));
            }
        }
        Ok(Probe {
            reachable: true,
            elapsed_ms: started.elapsed().as_millis() as u64,
            objects,
            newest: newest.as_ref().map(|(t, _)| t.clone()),
            newest_at: newest.map(|(_, at)| at),
        })
    }
}

/// Whether instant `a` is after `b`, comparing as instants when both parse and as
/// text otherwise (an empty `b` is before everything).
fn later(a: &str, b: &str) -> bool {
    if b.is_empty() {
        return true;
    }
    match (
        DateTime::parse_from_rfc3339(a),
        DateTime::parse_from_rfc3339(b),
    ) {
        (Ok(a), Ok(b)) => a > b,
        _ => a > b,
    }
}
