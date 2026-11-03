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
    /// The shared Python runtime transforms run at, instead of a local process;
    /// `HIE_RUNTIME_URL` overrides its URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeConfig>,
    /// One YAML file per source. The engine writes here when a source is created
    /// through the API, so the definitions stay files under version control.
    #[serde(default = "default_sources_dir", alias = "feeds_dir")]
    pub sources_dir: PathBuf,
    /// One Python file per transform; the API lists, edits and tries them here.
    #[serde(default = "default_transforms_dir")]
    pub transforms_dir: PathBuf,
    #[serde(default)]
    pub alerts: AlertConfig,
    /// The directory holding `hie.yaml`; relative paths in source files resolve against it.
    #[serde(skip)]
    pub base_dir: PathBuf,
}

/// The shared Python runtime (python-runtime's HIE facade).
#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    pub url: String,
    /// The environment variable holding the consumer's bearer token.
    #[serde(default = "default_runtime_token_env")]
    pub token_env: String,
    /// Requests per call; the runtime partitions them further itself.
    #[serde(default = "default_runtime_batch_size")]
    pub batch_size: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            token_env: default_runtime_token_env(),
            batch_size: default_runtime_batch_size(),
        }
    }
}

impl RuntimeConfig {
    /// What a transformer needs: the URL and the token read from the environment now.
    pub fn settings(&self) -> crate::runtime::RuntimeSettings {
        crate::runtime::RuntimeSettings {
            url: self.url.clone(),
            token: std::env::var(&self.token_env)
                .ok()
                .filter(|t| !t.trim().is_empty()),
            batch_size: self.batch_size,
        }
    }
}

fn default_runtime_token_env() -> String {
    "HIE_RUNTIME_TOKEN".into()
}
fn default_runtime_batch_size() -> usize {
    200
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AlertConfig {
    pub webhook_url: Option<String>,
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
        (crate::connectors::kind(self).describe)(self)
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

    /// Checks what the type itself cannot, as the source's connector says: a
    /// FHIR server source needs at least one resource type, a database query a
    /// `{cursor}`, an MLLP listener an address, an SQS queue its URL.
    pub fn check(&self) -> Result<()> {
        (crate::connectors::kind(self).check)(self)
    }

    /// Makes a folder's (or a spool's) paths absolute against the directory
    /// holding `hie.yaml`.
    pub fn resolve(&mut self, base: &Path) {
        (crate::connectors::kind(self).resolve)(self, base)
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
        let kind = crate::connectors::kind(&self.source);
        if let Some(format) = kind.format {
            let kind = kind.name;
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
    let mut config: Config =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
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
}
