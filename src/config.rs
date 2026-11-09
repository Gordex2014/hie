use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub fhir_base_url: String,
    #[serde(default = "default_database")]
    pub database: PathBuf,
    #[serde(default = "default_listen")]
    pub listen: String,
    /// The URL HFS reaches this engine at: manifests and folder-sourced files for
    /// `$bulk-submit` are served from it.
    #[serde(default = "default_public_url")]
    pub public_url: String,
    #[serde(default = "default_python")]
    pub python: String,
    /// The terminology server (HTS) transforms may ask through `lookup` and
    /// `translate`; `HIE_TERMINOLOGY_SERVER` overrides it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminology_url: Option<String>,
    /// One YAML file per source. The engine writes here when a source is created
    /// through the API, so the definitions stay files under version control.
    #[serde(default = "default_sources_dir", alias = "feeds_dir")]
    pub sources_dir: PathBuf,
    /// One Python file per transform; the API lists, edits and tries them here.
    #[serde(default = "default_transforms_dir")]
    pub transforms_dir: PathBuf,
    #[serde(default)]
    pub alerts: AlertConfig,
    /// What each transform's worker process may use and how long it may take.
    #[serde(default)]
    pub worker: WorkerConfig,
    /// The directory holding `hie.yaml`; relative paths in source files resolve against it.
    #[serde(skip)]
    pub base_dir: PathBuf,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AlertConfig {
    pub webhook_url: Option<String>,
}

/// The `worker:` block: the limits every transform worker process runs under.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct WorkerConfig {
    /// Address space per process, in MiB (`RLIMIT_AS` on Linux).
    pub memory_mib: u64,
    /// The largest file a transform may write, in MiB.
    pub file_mib: u64,
    /// Open file descriptors per process.
    pub open_files: u64,
    /// Per record, from before it is written to the worker until the reply line.
    pub request_seconds: u64,
    /// From starting the worker until it is ready.
    pub startup_seconds: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let limits = crate::worker::WorkerLimits::default();
        Self {
            memory_mib: limits.memory_mib,
            file_mib: limits.file_mib,
            open_files: limits.open_files,
            request_seconds: limits.request.as_secs(),
            startup_seconds: limits.startup.as_secs(),
        }
    }
}

impl WorkerConfig {
    pub fn check(&self) -> Result<()> {
        for (name, value) in [
            ("memory_mib", self.memory_mib),
            ("file_mib", self.file_mib),
            ("open_files", self.open_files),
            ("request_seconds", self.request_seconds),
            ("startup_seconds", self.startup_seconds),
        ] {
            if value < 1 {
                bail!("worker.{name} must be at least 1");
            }
        }
        Ok(())
    }

    pub fn limits(&self) -> crate::worker::WorkerLimits {
        crate::worker::WorkerLimits {
            memory_mib: self.memory_mib,
            file_mib: self.file_mib,
            open_files: self.open_files,
            request: Duration::from_secs(self.request_seconds),
            startup: Duration::from_secs(self.startup_seconds),
        }
    }
}

/// A source: one connection, its format, its transform, how it is delivered and
/// how often data is expected. Serialized as the source's YAML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedConfig {
    pub id: String,
    pub name: String,
    pub source: SourceConfig,
    #[serde(default)]
    pub format: Format,
    pub transform: PathBuf,
    #[serde(default)]
    pub delivery: DeliveryConfig,
    #[serde(
        default,
        rename = "expect_every",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) expect_every_text: Option<String>,
    #[serde(skip)]
    pub expect_every: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    Folder {
        path: PathBuf,
        #[serde(default = "default_pattern")]
        pattern: String,
        #[serde(default = "default_poll_seconds")]
        poll_seconds: u64,
        #[serde(default = "default_min_file_age_ms")]
        min_file_age_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        archive: Option<PathBuf>,
    },
    /// An S3 bucket. Credentials come from the usual AWS environment
    /// (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, profiles).
    S3 {
        bucket: String,
        #[serde(default)]
        prefix: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        region: Option<String>,
        /// For S3-compatible stores such as MinIO.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        #[serde(default = "default_pattern")]
        pattern: String,
        #[serde(default = "default_poll_seconds")]
        poll_seconds: u64,
        #[serde(default = "default_min_file_age_ms")]
        min_file_age_ms: u64,
        #[serde(default = "default_archive_prefix")]
        archive_prefix: String,
        /// A public bucket: no credentials, requests are not signed.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        anonymous: bool,
        /// Someone else's bucket: nothing is moved or written there. Objects the
        /// engine has already taken are recognised by content and left alone.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        read_only: bool,
        /// An SQS queue the bucket's `s3:ObjectCreated:*` notifications go to.
        /// With one, a pass reads the queue instead of listing the bucket; the
        /// bucket is still listed at start and every `relist_seconds`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        queue: Option<String>,
        #[serde(default = "default_relist_seconds")]
        relist_seconds: u64,
    },
    /// Another FHIR server, pulled by `_lastUpdated`: each pass takes what changed
    /// since the last one, per resource type, and hands it on as NDJSON.
    FhirServer {
        url: String,
        /// Resource types to pull, each with its own cursor.
        types: Vec<String>,
        #[serde(default = "default_poll_seconds")]
        poll_seconds: u64,
        /// `_count` per page.
        #[serde(default = "default_page_size")]
        page_size: usize,
        /// Pages per type per pass; the rest waits for the next pass.
        #[serde(default = "default_max_pages")]
        max_pages: usize,
        /// Where to start the first time, as a FHIR instant; everything when unset.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        since: Option<String>,
        /// How to authenticate to that server; anonymous when unset.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth: Option<RemoteAuth>,
    },
    /// A SQL database, read with a query that follows a cursor column: each pass
    /// binds the cursor where the query says `{cursor}` and takes the rows past it.
    Database {
        /// Environment variable holding the connection URL
        /// (`postgres://user:pass@host/db`, `mysql://...`, `sqlite://path`).
        url_env: String,
        /// The query, with `{cursor}` where the cursor value goes and an
        /// `ORDER BY` on the cursor column; a `LIMIT` bounds each run.
        query: String,
        /// The column the cursor follows: its largest value in a run becomes the
        /// next cursor. A monotonic column (an id, an updated-at with a sequence).
        cursor_column: String,
        /// The first cursor value, when the table should not be read from the start.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor_start: Option<String>,
        #[serde(default = "default_poll_seconds")]
        poll_seconds: u64,
        /// Runs of the query per pass; the rest waits for the next pass.
        #[serde(default = "default_max_pages")]
        max_runs: usize,
    },
    /// An MLLP listener for HL7 v2: every message received is written to `spool`
    /// before it is acknowledged, then picked up from there like a file.
    Mllp {
        /// Address to listen on, e.g. `0.0.0.0:2575`.
        listen: String,
        /// Where received messages wait as files; relative to `hie.yaml`.
        #[serde(default = "default_spool")]
        spool: PathBuf,
        /// How often the spool is picked up.
        #[serde(default = "default_mllp_poll_seconds")]
        poll_seconds: u64,
    },
}

fn default_spool() -> PathBuf {
    PathBuf::from("spool")
}

fn default_mllp_poll_seconds() -> u64 {
    2
}

/// Credentials for a remote FHIR server. Secrets are named by environment
/// variable, never written in the source file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum RemoteAuth {
    /// An OAuth2 client-credentials grant at the server's identity provider.
    ClientCredentials {
        token_url: String,
        client_id: String,
        client_secret_env: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<String>,
    },
    /// A fixed bearer token.
    Bearer { token_env: String },
}

fn default_page_size() -> usize {
    200
}

fn default_max_pages() -> usize {
    20
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    #[default]
    Csv,
    Ndjson,
    FhirBundle,
    /// HL7 v2 messages, one or more per file, each starting with `MSH`.
    Hl7v2,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryConfig {
    /// `rest`: batch Bundles, one PUT per record. `bulk_submit`: the source objects
    /// are handed to HFS through a `$bulk-submit` manifest, without a transform.
    #[serde(default)]
    pub method: DeliveryMethod,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    #[serde(default = "default_alert_after_attempts")]
    pub alert_after_attempts: u32,
    /// How long the URLs in a manifest stay valid for HFS to fetch the objects.
    #[serde(default = "default_url_ttl_seconds")]
    pub url_ttl_seconds: u64,
    /// Objects per manifest; a larger drop is split into several submissions.
    #[serde(default = "default_manifest_objects_max")]
    pub manifest_objects_max: usize,
    /// What a `$bulk-submit` manifest points at. `source_objects`: the files as
    /// they arrived, read by HFS straight from the source (FHIR NDJSON with ids,
    /// no transform). `transformed`: the engine runs the transform, writes the
    /// resources as NDJSON per resource type and hands those over; a resource
    /// without an `id` gets one derived from its identifier. `auto` (default) is
    /// `source_objects` for NDJSON or Bundle files with the passthrough transform
    /// and `transformed` for everything else.
    #[serde(default)]
    pub bulk_from: BulkFrom,
    /// Records per submission on the `transformed` path.
    #[serde(default = "default_bulk_records_max")]
    pub bulk_records_max: usize,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BulkFrom {
    #[default]
    Auto,
    SourceObjects,
    Transformed,
}

fn default_bulk_records_max() -> usize {
    10_000
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMethod {
    #[default]
    Rest,
    BulkSubmit,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            method: DeliveryMethod::Rest,
            batch_size: default_batch_size(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            alert_after_attempts: default_alert_after_attempts(),
            url_ttl_seconds: default_url_ttl_seconds(),
            manifest_objects_max: default_manifest_objects_max(),
            bulk_from: BulkFrom::Auto,
            bulk_records_max: default_bulk_records_max(),
        }
    }
}

fn default_database() -> PathBuf {
    PathBuf::from("data/hie.db")
}
fn default_listen() -> String {
    "127.0.0.1:8090".into()
}
fn default_python() -> String {
    if cfg!(windows) {
        "py".into()
    } else {
        "python3".into()
    }
}
fn default_sources_dir() -> PathBuf {
    PathBuf::from("sources")
}
fn default_transforms_dir() -> PathBuf {
    PathBuf::from("transforms")
}
fn default_pattern() -> String {
    "*".into()
}
fn default_relist_seconds() -> u64 {
    3600
}
fn default_poll_seconds() -> u64 {
    5
}
fn default_min_file_age_ms() -> u64 {
    1000
}
fn default_archive_prefix() -> String {
    "processed".into()
}
fn default_batch_size() -> usize {
    100
}
fn default_initial_backoff_ms() -> u64 {
    2000
}
fn default_max_backoff_ms() -> u64 {
    300_000
}
fn default_alert_after_attempts() -> u32 {
    5
}
fn default_url_ttl_seconds() -> u64 {
    86_400
}
fn default_manifest_objects_max() -> usize {
    50
}
fn default_public_url() -> String {
    "http://127.0.0.1:8090".into()
}

impl SourceConfig {
    /// Where the source reads from, for logs and the API.
    pub fn describe(&self) -> String {
        match self {
            SourceConfig::Folder { path, .. } => path.display().to_string(),
            SourceConfig::S3 { bucket, prefix, .. } => format!("s3://{bucket}/{prefix}"),
            SourceConfig::FhirServer { url, types, .. } => {
                format!("{url} ({})", types.join(", "))
            }
            SourceConfig::Database {
                url_env,
                cursor_column,
                ..
            } => format!("database ${url_env} by {cursor_column}"),
            SourceConfig::Mllp { listen, spool, .. } => {
                format!("mllp://{listen} (spool {})", spool.display())
            }
        }
    }

    pub fn poll_interval(&self) -> Duration {
        match self {
            SourceConfig::Folder { poll_seconds, .. }
            | SourceConfig::S3 { poll_seconds, .. }
            | SourceConfig::FhirServer { poll_seconds, .. }
            | SourceConfig::Database { poll_seconds, .. }
            | SourceConfig::Mllp { poll_seconds, .. } => Duration::from_secs(*poll_seconds),
        }
    }

    /// Checks what the type itself cannot: a FHIR server source needs at least one
    /// resource type, sane paging, and a URL that looks like one.
    pub fn check(&self) -> Result<()> {
        if let SourceConfig::FhirServer {
            url,
            types,
            page_size,
            max_pages,
            ..
        } = self
        {
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
        }
        if let SourceConfig::Database {
            url_env,
            query,
            cursor_column,
            max_runs,
            ..
        } = self
        {
            if url_env.trim().is_empty() {
                bail!(
                    "database: url_env names the environment variable holding the connection URL"
                );
            }
            if !query.contains("{cursor}") {
                bail!(
                    "database: the query must use {{cursor}} where the cursor value goes, e.g. WHERE updated_at > {{cursor}}"
                );
            }
            if cursor_column.trim().is_empty()
                || !cursor_column
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                bail!("database: cursor_column must be a column name");
            }
            if *max_runs == 0 {
                bail!("database: max_runs must be at least 1");
            }
        }
        if let SourceConfig::Mllp { listen, .. } = self
            && listen.parse::<std::net::SocketAddr>().is_err()
        {
            bail!("mllp: listen must be an address such as 0.0.0.0:2575, not {listen:?}");
        }
        if let SourceConfig::S3 {
            queue: Some(queue),
            anonymous,
            relist_seconds,
            ..
        } = self
        {
            if !(queue.starts_with("http://") || queue.starts_with("https://")) {
                bail!("s3: queue must be the SQS queue URL, not {queue:?}");
            }
            if *anonymous {
                bail!(
                    "s3: a queue is read with credentials; anonymous and queue do not go together"
                );
            }
            if *relist_seconds == 0 {
                bail!("s3: relist_seconds must be at least 1");
            }
        }
        Ok(())
    }

    /// Makes a folder's paths absolute against the directory holding `hie.yaml`.
    pub fn resolve(&mut self, base: &Path) {
        if let SourceConfig::Folder { path, archive, .. } = self {
            *path = base.join(&*path);
            if let Some(a) = archive {
                *a = base.join(&*a);
            }
        }
        if let SourceConfig::Mllp { spool, .. } = self {
            *spool = base.join(&*spool);
        }
    }
}

impl FeedConfig {
    /// Makes the paths absolute, checks the transform exists and parses `expect_every`.
    pub fn resolve(&mut self, base: &Path) -> Result<()> {
        if self.id.is_empty()
            || !self
                .id
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            bail!(
                "source id {:?} must be lowercase letters, digits and dashes",
                self.id
            );
        }
        if self.name.trim().is_empty() {
            bail!("source {}: name is required", self.id);
        }
        self.source.resolve(base);
        self.source
            .check()
            .with_context(|| format!("source {}", self.id))?;
        // What a server, a query or an MLLP sender hands over has its own shape,
        // whatever the file says; and there is no object for $bulk-submit to fetch.
        let fixed = match self.source {
            SourceConfig::FhirServer { .. } => Some(("fhir_server", Format::Ndjson)),
            SourceConfig::Database { .. } => Some(("database", Format::Ndjson)),
            SourceConfig::Mllp { .. } => Some(("mllp", Format::Hl7v2)),
            _ => None,
        };
        if let Some((kind, format)) = fixed {
            self.format = format;
            if self.delivery.method == DeliveryMethod::BulkSubmit
                && self.delivery.bulk_from == BulkFrom::SourceObjects
            {
                bail!(
                    "source {}: a {kind} source has no file for HFS to fetch; use bulk_from: transformed (the default) or rest",
                    self.id
                );
            }
        }
        self.transform = base.join(&self.transform);
        if !self.transform.is_file() {
            bail!(
                "source {}: transform {} not found",
                self.id,
                self.transform.display()
            );
        }
        self.expect_every = self
            .expect_every_text
            .as_deref()
            .map(parse_duration)
            .transpose()
            .with_context(|| format!("source {}: expect_every", self.id))?;
        Ok(())
    }
}

impl Config {
    /// Where the engine writes the NDJSON it serves for transformed submissions:
    /// next to the SQLite file, or under `data/bulk` when the store is PostgreSQL.
    pub fn bulk_dir(&self) -> PathBuf {
        let spec = self.database.to_string_lossy();
        if spec.starts_with("postgres://") || spec.starts_with("postgresql://") {
            self.base_dir.join("data").join("bulk")
        } else {
            self.database
                .parent()
                .map(|p| p.join("bulk"))
                .unwrap_or_else(|| PathBuf::from("bulk"))
        }
    }
}

impl FeedConfig {
    /// Whether `$bulk-submit` hands HFS the engine's transformed NDJSON rather
    /// than the source objects.
    pub fn bulk_transformed(&self) -> bool {
        if self.delivery.method != DeliveryMethod::BulkSubmit {
            return false;
        }
        match self.delivery.bulk_from {
            BulkFrom::Transformed => true,
            BulkFrom::SourceObjects => false,
            BulkFrom::Auto => {
                let passthrough = self
                    .transform
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n == "fhir_passthrough.py");
                !(matches!(self.format, Format::Ndjson | Format::FhirBundle) && passthrough)
            }
        }
    }

    /// The source's YAML file, as submitted (paths as given, not resolved).
    pub fn to_yaml(&self) -> Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }
}

/// Loads `hie.yaml` and every source definition in its `sources_dir`.
/// Relative paths resolve against the directory holding `hie.yaml`.
pub fn load(path: &Path) -> Result<(Config, Vec<FeedConfig>)> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    // The `runtime:` backend is gone. A leftover key would otherwise be ignored
    // and its transforms run in the local worker instead: refuse to start, whatever
    // its value (`runtime: null` and a bare `runtime:` included) and however the
    // key is tagged (`as_str` sees through YAML tags).
    let raw: serde_yaml::Value =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    if raw
        .as_mapping()
        .is_some_and(|m| m.keys().any(|k| k.as_str() == Some("runtime")))
    {
        bail!(
            "`runtime:` in {} is no longer supported: transforms run in the local worker; remove it",
            path.display()
        );
    }
    let mut config: Config =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    config
        .worker
        .check()
        .with_context(|| format!("checking {}", path.display()))?;
    let base = path.parent().map(Path::to_path_buf).unwrap_or_default();
    config.base_dir = base.clone();
    config.database = base.join(&config.database);
    config.sources_dir = base.join(&config.sources_dir);
    config.transforms_dir = base.join(&config.transforms_dir);
    std::fs::create_dir_all(&config.sources_dir)
        .with_context(|| format!("creating {}", config.sources_dir.display()))?;

    let mut feeds = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(&config.sources_dir)
        .with_context(|| format!("reading {}", config.sources_dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| matches!(p.extension().and_then(|e| e.to_str()), Some("yaml" | "yml")))
        .collect();
    entries.sort();
    let mut ids = HashSet::new();
    for feed_path in entries {
        let text = std::fs::read_to_string(&feed_path)?;
        let mut feed: FeedConfig = serde_yaml::from_str(&text)
            .with_context(|| format!("parsing {}", feed_path.display()))?;
        feed.resolve(&base)?;
        if !ids.insert(feed.id.clone()) {
            bail!("duplicate source id {}", feed.id);
        }
        feeds.push(feed);
    }
    Ok((config, feeds))
}

/// Parses `90s`, `30m`, `24h` or `7d`.
pub fn parse_duration(text: &str) -> Result<Duration> {
    let text = text.trim();
    let (number, unit) = text.split_at(
        text.find(|c: char| !c.is_ascii_digit())
            .unwrap_or(text.len()),
    );
    let n: u64 = number
        .parse()
        .with_context(|| format!("invalid duration {text:?}"))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86_400,
        _ => bail!("invalid duration unit in {text:?} (use s, m, h or d)"),
    };
    Ok(Duration::from_secs(secs))
}

/// Matches a file name against a pattern with at most one `*`, ignoring case.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern.eq_ignore_ascii_case(name),
        Some((prefix, suffix)) => {
            let (n, p, s) = (
                name.to_ascii_lowercase(),
                prefix.to_ascii_lowercase(),
                suffix.to_ascii_lowercase(),
            );
            n.len() >= p.len() + s.len() && n.starts_with(&p) && n.ends_with(&s)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse_with_units() {
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86_400));
        assert!(parse_duration("5 weeks").is_err());
    }

    #[test]
    fn patterns_match_by_prefix_and_suffix() {
        assert!(glob_match("*.csv", "labs.CSV"));
        assert!(glob_match("lab_*.csv", "lab_2026-10-18.csv"));
        assert!(!glob_match("*.csv", "labs.csv.tmp"));
        assert!(glob_match("*", "anything"));
    }

    #[test]
    fn an_s3_source_parses_with_defaults_and_round_trips() {
        let feed: FeedConfig = serde_yaml::from_str(
            "id: x\nname: X\nsource:\n  type: s3\n  bucket: lab\n  prefix: results/\nformat: ndjson\ntransform: t.py\nexpect_every: 24h\n",
        )
        .unwrap();
        assert_eq!(feed.format, Format::Ndjson);
        assert_eq!(feed.source.describe(), "s3://lab/results/");
        let yaml = feed.to_yaml().unwrap();
        let again: FeedConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(again.source.describe(), "s3://lab/results/");
        assert!(yaml.contains("expect_every: 24h"));
        assert!(!yaml.contains("region"));
        match feed.source {
            SourceConfig::S3 {
                pattern,
                archive_prefix,
                ..
            } => assert_eq!(
                (pattern.as_str(), archive_prefix.as_str()),
                ("*", "processed")
            ),
            _ => panic!("expected s3"),
        }
    }

    #[test]
    fn source_ids_are_checked_on_resolve() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("t.py"), "def transform(r, c): return r\n").unwrap();
        let mut feed: FeedConfig = serde_yaml::from_str(
            "id: Bad Id\nname: X\nsource:\n  type: folder\n  path: inbox\ntransform: t.py\n",
        )
        .unwrap();
        assert!(
            feed.resolve(dir.path())
                .unwrap_err()
                .to_string()
                .contains("lowercase")
        );
        feed.id = "good-id".into();
        feed.resolve(dir.path()).unwrap();
        assert!(feed.transform.is_absolute());
    }

    fn load_engine_yaml(yaml: &str) -> Result<(Config, Vec<FeedConfig>)> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prod-engine.yaml");
        std::fs::write(&path, yaml).unwrap();
        load(&path)
    }

    #[test]
    fn a_config_without_runtime_loads() {
        let (config, feeds) = load_engine_yaml("fhir_base_url: http://hfs\n").unwrap();
        assert_eq!(config.fhir_base_url, "http://hfs");
        assert!(feeds.is_empty());
    }

    #[test]
    fn the_worker_block_has_defaults_and_is_checked() {
        let (config, _) = load_engine_yaml("fhir_base_url: http://hfs\n").unwrap();
        assert_eq!(
            config.worker.limits(),
            crate::worker::WorkerLimits::default()
        );
        let (config, _) = load_engine_yaml(
            "fhir_base_url: http://hfs\nworker:\n  memory_mib: 4096\n  request_seconds: 5\n",
        )
        .unwrap();
        assert_eq!(config.worker.memory_mib, 4096);
        assert_eq!(config.worker.request_seconds, 5);
        assert_eq!(config.worker.file_mib, 256);
        let err =
            load_engine_yaml("fhir_base_url: http://hfs\nworker:\n  open_files: 0\n").unwrap_err();
        assert!(
            format!("{err:#}").contains("worker.open_files must be at least 1"),
            "{err:#}"
        );
        // A misspelt key is refused, not silently left at its default.
        let err =
            load_engine_yaml("fhir_base_url: http://hfs\nworker:\n  memroy_mib: 1\n").unwrap_err();
        assert!(format!("{err:#}").contains("memroy_mib"), "{err:#}");
    }

    #[test]
    fn a_leftover_runtime_key_stops_the_load() {
        for yaml in [
            "fhir_base_url: http://hfs\nruntime:\n  url: http://127.0.0.1:8790\n",
            "fhir_base_url: http://hfs\nruntime: null\n",
            "fhir_base_url: http://hfs\nruntime:\n",
            "fhir_base_url: http://hfs\nruntime: http://127.0.0.1:8790\n",
            "fhir_base_url: http://hfs\n!legacy runtime:\n  url: http://127.0.0.1:8790\n",
        ] {
            let err = load_engine_yaml(yaml).unwrap_err().to_string();
            assert!(
                err.contains("is no longer supported") && err.contains("prod-engine.yaml"),
                "{yaml}: {err}"
            );
        }
    }
}
