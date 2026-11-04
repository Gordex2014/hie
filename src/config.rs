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
    /// The connections the engine may open; nothing is enforced without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<AdmissionConfig>,
    /// Whether resources are checked with the destination's `$validate` before
    /// they are delivered.
    #[serde(default)]
    pub validation: Validation,
    #[serde(default)]
    pub retention: RetentionConfig,
    #[serde(default)]
    pub destination: DestinationConfig,
    /// Custody of secret values: `key_file`, or `HIE_SECRETS_KEY`.
    #[serde(default)]
    pub secrets: KeyFileConfig,
    /// Custody of failed rows' content: `key_file`, or `HIE_CONTENT_KEY`, or a
    /// random key file in the store's directory.
    #[serde(default)]
    pub content: KeyFileConfig,
    #[serde(default, rename = "try")]
    pub try_runs: TryConfig,
    /// The directory holding `hie.yaml`; relative paths in source files resolve against it.
    #[serde(skip)]
    pub base_dir: PathBuf,
}

/// `admission` in `hie.yaml`: the whole connections the engine may open.
/// Entries are normalized when `hie.yaml` is loaded (see [`ConnectionParts`]).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct AdmissionConfig {
    /// The installation declares it holds synthetic data only.
    #[serde(default)]
    pub synthetic_only: bool,
    #[serde(default)]
    pub databases: Vec<ConnectionParts>,
}

/// The parts of a database connection admission compares, normalized:
/// `engine` lower case with `mssql` read as `sqlserver` and `postgresql` as
/// `postgres`; `host` lower case without a trailing dot; `port` the engine's
/// default (1433, 5432) when left out; `database` exactly as written.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ConnectionParts {
    pub engine: String,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default, deserialize_with = "flex::opt_port")]
    pub port: Option<u16>,
    #[serde(default)]
    pub database: Option<String>,
}

impl ConnectionParts {
    /// The same parts, normalized.
    pub fn normalized(&self) -> Self {
        let engine = normalize_engine(&self.engine);
        Self {
            port: self.port.or_else(|| default_port(&engine)),
            host: self
                .host
                .as_deref()
                .map(|h| h.trim().trim_end_matches('.').to_ascii_lowercase()),
            database: self.database.clone(),
            engine,
        }
    }
}

/// `sqlserver`, `postgres`, `sqlite`, ...: lower case, with the aliases read.
pub fn normalize_engine(engine: &str) -> String {
    match engine.trim().to_ascii_lowercase().as_str() {
        "mssql" => "sqlserver".into(),
        "postgresql" => "postgres".into(),
        other => other.into(),
    }
}

/// The port an engine listens on when a definition leaves it out.
pub fn default_port(engine: &str) -> Option<u16> {
    match engine {
        "sqlserver" => Some(1433),
        "postgres" => Some(5432),
        "mysql" => Some(3306),
        _ => None,
    }
}

/// `validation` in `hie.yaml`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Validation {
    /// As in 0.1: resources are not checked before delivery.
    #[default]
    Off,
    /// Every resource is checked with the destination's `$validate` first.
    Required,
}

/// `retention` in `hie.yaml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetentionConfig {
    /// How long failed rows' content is kept (`30d` by default).
    #[serde(
        default = "default_failed_content",
        deserialize_with = "flex::duration",
        serialize_with = "flex::serialize_duration"
    )]
    pub failed_content: Duration,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            failed_content: default_failed_content(),
        }
    }
}

fn default_failed_content() -> Duration {
    Duration::from_secs(30 * 86_400)
}

/// `destination` in `hie.yaml`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct DestinationConfig {
    /// What people call the destination; `null` in the API when unset.
    #[serde(default)]
    pub display_name: Option<String>,
}

/// `secrets` and `content` in `hie.yaml`: where a key lives.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct KeyFileConfig {
    /// A file holding the key (32 bytes, base64); relative to `hie.yaml`.
    #[serde(default)]
    pub key_file: Option<PathBuf>,
}

/// `try` in `hie.yaml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TryConfig {
    /// Try runs at once per engine; more are `429 try_busy`.
    #[serde(default = "default_try_max_concurrent")]
    pub max_concurrent: usize,
}

impl Default for TryConfig {
    fn default() -> Self {
        Self {
            max_concurrent: default_try_max_concurrent(),
        }
    }
}

fn default_try_max_concurrent() -> usize {
    1
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

/// What a source's `transform` names: a pin (`{"id", "version"}` or
/// `{"id", "follow": "current"}`), or the 0.1 file path, which follows the
/// current version of that file's transform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TransformRef {
    Pin(TransformPin),
    File(PathBuf),
}

/// A transform pin as a definition writes it; exactly one of `version` and
/// `follow` (checked by [`FeedConfig::resolve`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformPin {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub follow: Option<Follow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Follow {
    Current,
}

/// How a source chooses its version, once the reference is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinMode {
    Version(i64),
    FollowCurrent,
}

impl TransformRef {
    /// The transform's id: the pin's, or a file's stem.
    pub fn id(&self) -> String {
        match self {
            TransformRef::Pin(p) => p.id.clone(),
            TransformRef::File(path) => path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string(),
        }
    }

    /// Pinned to a version, or following the current one (a file path follows).
    pub fn mode(&self) -> PinMode {
        match self {
            TransformRef::Pin(TransformPin {
                version: Some(v), ..
            }) => PinMode::Version(*v),
            _ => PinMode::FollowCurrent,
        }
    }

    /// The file a 0.1 reference names; `None` for a pin.
    pub fn file(&self) -> Option<&Path> {
        match self {
            TransformRef::File(path) => Some(path),
            TransformRef::Pin(_) => None,
        }
    }

    /// For logs: the path, or `id@version` / `id@current`.
    pub fn display(&self) -> String {
        match (self, self.mode()) {
            (TransformRef::File(path), _) => path.display().to_string(),
            (_, PinMode::Version(v)) => format!("{}@{v}", self.id()),
            (_, PinMode::FollowCurrent) => format!("{}@current", self.id()),
        }
    }
}

impl From<PathBuf> for TransformRef {
    fn from(path: PathBuf) -> Self {
        TransformRef::File(path)
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
    /// The transform a load runs: a pin, or (0.1) a file path. A path is made
    /// absolute by [`FeedConfig::resolve`].
    pub transform: TransformRef,
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
    /// The file the definition was read from or written to; `None` for one
    /// that is not a file (yet). Never part of the definition itself.
    #[serde(skip)]
    pub file: Option<PathBuf>,
}

/// The extensions a source definition file may have.
const SOURCE_EXTENSIONS: [&str; 2] = ["yaml", "yml"];

/// The file named for source `id` in `dir`: `<id>.yaml` or `<id>.yml`,
/// whichever exists and is not `taken` (the file of another loaded source),
/// or else `<id>.yaml`, where a new one is written. A loaded source knows
/// its file ([`FeedConfig::file`]), whatever its name; this is for one that
/// is not loaded.
pub fn source_file(dir: &Path, id: &str, taken: &[PathBuf]) -> PathBuf {
    SOURCE_EXTENSIONS
        .iter()
        .map(|ext| dir.join(format!("{id}.{ext}")))
        .find(|path| path.exists() && !taken.contains(path))
        .unwrap_or_else(|| dir.join(format!("{id}.{}", SOURCE_EXTENSIONS[0])))
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
        /// 0.1: the environment variable holding the connection URL
        /// (`postgres://user:pass@host/db`, `mysql://...`, `sqlite://path`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url_env: Option<String>,
        /// 0.2.0: the connection by its parts, with the password a secret reference.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        connection: Option<DatabaseConnection>,
        /// 0.2.0: what to read (a table by keyset, or the operator's query).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        read: Option<DatabaseRead>,
        /// 0.1: the query, with `{cursor}` where the cursor value goes and an
        /// `ORDER BY` on the cursor column; a `LIMIT` bounds each run.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        query: Option<String>,
        /// 0.1: the column the cursor follows: its largest value in a run becomes
        /// the next cursor. A monotonic column (an id, an updated-at with a sequence).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor_column: Option<String>,
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

/// A database connection by its parts (0.2.0). `sqlite` takes only `database`,
/// a file path. Ports and flags may be written as strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseConnection {
    /// `sqlserver`, `postgres` or `sqlite` (aliases `mssql`, `postgresql`).
    pub engine: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "flex::opt_port"
    )]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<SecretRef>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "flex::opt_bool"
    )]
    pub encrypt: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "flex::opt_bool"
    )]
    pub trust_cert: Option<bool>,
}

impl DatabaseConnection {
    /// The parts admission compares, normalized.
    pub fn parts(&self) -> ConnectionParts {
        ConnectionParts {
            engine: self.engine.clone(),
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
        }
        .normalized()
    }
}

/// A reference to a kept secret: `{"secret": "sec_...", "revision": n}`; a
/// reference without `revision` takes the one the source's evidence recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretRef {
    pub secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u32>,
}

/// What a database source reads (0.2.0).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum DatabaseRead {
    /// One table by keyset on (`new_rows_by`, `key`); `key` defaults to the
    /// table's primary key.
    Table {
        table: TableName,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        key: Vec<String>,
        new_rows_by: String,
    },
    /// The operator's own SQL, as in 0.1.
    Query {
        query: String,
        cursor_column: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor_start: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableName {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub name: String,
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
    /// `as_sent` (default) or `namespaced`: ids derived from the source.
    #[serde(default, skip_serializing_if = "is_default")]
    pub ids: DeliveryIds,
    /// `auto` (default), `batch` or `transaction`.
    #[serde(default, skip_serializing_if = "is_default")]
    pub bundle: BundleType,
    /// The method of a source's first load, when it differs from `method`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_method: Option<DeliveryMethod>,
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryIds {
    /// Ids as the transform returns them, as in 0.1.
    #[default]
    AsSent,
    /// Every id derived from the source, the type and the origin id.
    Namespaced,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BundleType {
    /// A `transaction` when a chunk holds a conditional reference, else a `batch`.
    #[default]
    Auto,
    Batch,
    Transaction,
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
            ids: DeliveryIds::default(),
            bundle: BundleType::default(),
            initial_method: None,
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
        match &mut self.transform {
            TransformRef::File(path) => {
                *path = base.join(&*path);
                if !path.is_file() {
                    bail!("source {}: transform {} not found", self.id, path.display());
                }
            }
            // Whether the transform and its version exist is the version
            // store's to say, when the source starts.
            TransformRef::Pin(pin) => {
                if !is_transform_id(&pin.id) {
                    bail!(
                        "source {}: transform id {:?} must be 1 to 64 lowercase letters, digits, dashes and underscores",
                        self.id,
                        pin.id
                    );
                }
                match (pin.version, pin.follow) {
                    (Some(v), None) if v >= 1 => {}
                    (None, Some(Follow::Current)) => {}
                    _ => bail!(
                        "source {}: transform takes either {{\"id\", \"version\": n}} or {{\"id\", \"follow\": \"current\"}}",
                        self.id
                    ),
                }
            }
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

/// Whether `id` can name a transform: `[a-z0-9][a-z0-9_-]{0,63}`.
pub fn is_transform_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_' || *b == b'-')
}

impl Config {
    /// The keys secret values are sealed with: `HIE_SECRETS_KEY`, else
    /// `secrets.key_file`; `None` when neither is set (the secret routes then
    /// answer `503 secrets_unavailable`). An unreadable key is an error.
    pub fn secrets_keys(&self) -> Result<Option<crate::crypto::Keyring>> {
        if let Some(keys) = crate::crypto::Keyring::from_env("HIE_SECRETS_KEY")? {
            return Ok(Some(keys));
        }
        self.secrets
            .key_file
            .as_deref()
            .map(crate::crypto::Keyring::from_file)
            .transpose()
    }

    /// The keys failed rows' content is sealed with: `HIE_CONTENT_KEY`, else
    /// `content.key_file`, else `content.key` in the store's directory,
    /// created with a random key the first time.
    pub fn content_keys(&self) -> Result<crate::crypto::Keyring> {
        if let Some(keys) = crate::crypto::Keyring::from_env("HIE_CONTENT_KEY")? {
            return Ok(keys);
        }
        match &self.content.key_file {
            Some(path) => crate::crypto::Keyring::from_file(path),
            None => crate::crypto::Keyring::load_or_create(&self.store_dir().join("content.key")),
        }
    }

    /// The directory of the store: the SQLite file's, or `data/` under
    /// `hie.yaml`'s directory when the store is PostgreSQL.
    pub fn store_dir(&self) -> PathBuf {
        let spec = self.database.to_string_lossy();
        if spec.starts_with("postgres://") || spec.starts_with("postgresql://") {
            self.base_dir.join("data")
        } else {
            self.database
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("."))
        }
    }

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
                let passthrough = self.transform.id() == "fhir_passthrough";
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
    for path in [&mut config.secrets.key_file, &mut config.content.key_file]
        .into_iter()
        .flatten()
    {
        *path = base.join(&*path);
    }
    if let Some(admission) = &mut config.admission {
        for entry in &mut admission.databases {
            *entry = entry.normalized();
        }
    }
    if config.try_runs.max_concurrent == 0 {
        bail!("try.max_concurrent must be at least 1");
    }
    std::fs::create_dir_all(&config.sources_dir)
        .with_context(|| format!("creating {}", config.sources_dir.display()))?;

    let mut feeds = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(&config.sources_dir)
        .with_context(|| format!("reading {}", config.sources_dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| SOURCE_EXTENSIONS.contains(&e))
        })
        .collect();
    entries.sort();
    let mut ids = HashSet::new();
    for feed_path in entries {
        let text = std::fs::read_to_string(&feed_path)?;
        let mut feed: FeedConfig = serde_yaml::from_str(&text)
            .with_context(|| format!("parsing {}", feed_path.display()))?;
        feed.resolve(&base)?;
        feed.file = Some(feed_path);
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

/// Lenient readers for values a definition may write as numbers or strings.
mod flex {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumberOrText {
        Number(u64),
        Text(String),
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrText {
        Bool(bool),
        Text(String),
    }

    /// A port as a number or a numeric string.
    pub fn opt_port<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u16>, D::Error> {
        use serde::de::Error;
        let port = match Option::<NumberOrText>::deserialize(d)? {
            None => return Ok(None),
            Some(NumberOrText::Number(n)) => n,
            Some(NumberOrText::Text(t)) => t
                .trim()
                .parse::<u64>()
                .map_err(|_| D::Error::custom("port must be a number"))?,
        };
        u16::try_from(port)
            .ok()
            .filter(|p| *p > 0)
            .map(Some)
            .ok_or_else(|| D::Error::custom("port must be 1 to 65535"))
    }

    /// A flag as a boolean or `"true"` / `"false"`.
    pub fn opt_bool<'de, D: Deserializer<'de>>(d: D) -> Result<Option<bool>, D::Error> {
        use serde::de::Error;
        match Option::<BoolOrText>::deserialize(d)? {
            None => Ok(None),
            Some(BoolOrText::Bool(b)) => Ok(Some(b)),
            Some(BoolOrText::Text(t)) => match t.trim().to_ascii_lowercase().as_str() {
                "true" | "yes" | "1" => Ok(Some(true)),
                "false" | "no" | "0" => Ok(Some(false)),
                _ => Err(D::Error::custom("expected true or false")),
            },
        }
    }

    /// A duration written as `30d`, `24h`, `30m` or `90s`.
    pub fn duration<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        use serde::de::Error;
        let text = String::deserialize(d)?;
        super::parse_duration(&text).map_err(|e| D::Error::custom(format!("{e:#}")))
    }

    pub fn serialize_duration<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{}s", d.as_secs()))
    }
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
        assert!(feed.transform.file().is_some_and(Path::is_absolute));
    }
}
