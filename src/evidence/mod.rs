//! Connection evidence: what a Test of a database source established (its
//! checks, the credential revision it used, the rows it counted), describe and
//! sample through it, and the binding of a Start to its evidence (decisions 36
//! to 38).
//!
//! - [`Evidence::test`] checks admission, resolves the password's revision,
//!   runs the connector's Test and keeps what it found for 24 hours (and for
//!   as long as a source holds it), with the revision it used held for it.
//! - [`Evidence::describe`] and [`Evidence::sample`] reconnect with the
//!   evidence's definition and revision, after checking admission again.
//! - [`Evidence::bind`] is what a Start (or replace) checks: the evidence
//!   exists, passed, fingerprints the same definition and names the revision
//!   the definition names (or gives it one). [`Evidence::source_started`],
//!   [`Evidence::source_deleted`] keep what a source holds.
//!
//! The fingerprint (`definition_sha256`) is the SHA-256 of the canonical JSON
//! of `{"connection", "read"}` after normalization: engine, host and port as
//! admission normalizes them, defaults filled in (`encrypt: true`,
//! `trust_cert: false`, the table's schema, a table read's key as its primary
//! key), the password reduced to `{"secret"}`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::config::{DatabaseConnection, DatabaseRead, SourceConfig, TableName};
use crate::connectors::Context;
use crate::connectors::database::{
    Check, Described, Endpoint, Engine, TestReport, describe_connection, qualified,
    sample_connection, test_connection,
};
use crate::db::{Db, now_ms};
use crate::ids::{Kind, rfc3339_ms_at};
use crate::manager::{ErrorStatus, ManagerError};
use crate::secrets::{SecretError, SecretUse};

/// How long evidence no source holds is kept.
const KEEP_MS: i64 = 24 * 3_600_000;
/// How long a sample is held in memory.
const SAMPLE_TTL: Duration = Duration::from_secs(30 * 60);

/// Evidence as the store keeps it.
#[derive(Debug, Clone, PartialEq)]
pub struct EvidenceRow {
    pub id: String,
    pub verdict: String,
    /// The `evidence` object as the Test answered it.
    pub body: Value,
    /// `{"connection", "read"}` as tested: references, never values.
    pub definition: Value,
    pub fingerprint: String,
    pub credential: Option<(String, u32)>,
    /// A table read's table's primary key, as the Test found it.
    pub primary_key: Option<Vec<String>>,
    pub tested_at: i64,
    pub expires_at: i64,
    pub source_id: Option<String>,
}

/// What a source is bound to.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Binding {
    pub source: String,
    pub evidence: Option<String>,
    pub credential: Option<(String, u32)>,
    pub first_load: Option<i64>,
}

/// What a Start takes from its evidence.
#[derive(Debug, Clone, PartialEq)]
pub struct StartBinding {
    pub evidence: String,
    pub credential: Option<(String, u32)>,
    /// The read's key, filled with the table's primary key when left out.
    pub key: Option<Vec<String>>,
}

/// Samples taken from a source (`smp_` ids), kept in memory for 30 minutes
/// so Try can run on them (`input: {"sample": ...}`); never stored or logged.
#[derive(Default)]
pub struct SampleRegistry {
    samples: Mutex<HashMap<String, (Instant, Vec<Value>)>>,
}

impl SampleRegistry {
    /// The rows of a sample, while it is kept.
    pub fn get(&self, sample_id: &str) -> Option<Vec<Value>> {
        let mut samples = self.samples.lock().expect("sample registry");
        samples.retain(|_, (until, _)| *until > Instant::now());
        samples.get(sample_id).map(|(_, rows)| rows.clone())
    }

    /// Keeps rows as a new sample; its id and when it expires (epoch ms).
    pub fn put(&self, rows: Vec<Value>) -> (String, i64) {
        let id = crate::ids::new(Kind::Sample);
        let mut samples = self.samples.lock().expect("sample registry");
        samples.retain(|_, (until, _)| *until > Instant::now());
        samples.insert(id.clone(), (Instant::now() + SAMPLE_TTL, rows));
        (id, now_ms() + SAMPLE_TTL.as_millis() as i64)
    }
}

/// A database definition by parts, as a Test, describe or sample takes it.
fn parts_of(source: &SourceConfig) -> Option<(&DatabaseConnection, Option<&DatabaseRead>)> {
    match source {
        SourceConfig::Database {
            connection: Some(connection),
            read,
            ..
        } => Some((connection, read.as_ref())),
        _ => None,
    }
}

/// Whether a definition is one evidence speaks for: a database by parts.
pub fn needs_evidence(source: &SourceConfig) -> bool {
    parts_of(source).is_some()
}

fn invalid(message: impl Into<String>) -> ManagerError {
    ManagerError::invalid("invalid_definition", message)
}

fn refused(r: crate::admission::Refused) -> ManagerError {
    ManagerError::new(ErrorStatus::Forbidden, "admission_refused", r.to_string())
        .with("field", r.field)
}

fn not_found() -> ManagerError {
    ManagerError::not_found(
        "evidence_not_found",
        "evidence not found (unknown, or expired)",
    )
}

fn not_passed() -> ManagerError {
    ManagerError::conflict(
        "evidence_not_passed",
        "the evidence's verdict is not passed",
    )
}

/// A connection that failed while describing or sampling: the stage's
/// secret-safe sentence.
fn unreachable(e: crate::connectors::database::DbError) -> ManagerError {
    ManagerError::new(
        ErrorStatus::Unavailable,
        "source_unavailable",
        e.to_string(),
    )
}

/// The normalized `{"connection", "read"}` of a definition.
pub fn normalized(
    connection: &DatabaseConnection,
    read: Option<&DatabaseRead>,
    primary_key: Option<&[String]>,
) -> Value {
    let parts = connection.parts();
    let engine = Engine::parse(&parts.engine);
    let connection = json!({
        "engine": parts.engine,
        "host": parts.host,
        "port": parts.port,
        "database": parts.database,
        "user": connection.user,
        "password": connection.password.as_ref().map(|p| json!({"secret": p.secret})),
        "encrypt": connection.encrypt.unwrap_or(true),
        "trust_cert": connection.trust_cert.unwrap_or(false),
    });
    let read = read.map(|read| match read {
        DatabaseRead::Table {
            table,
            key,
            new_rows_by,
        } => {
            let table = match engine {
                Some(engine) => qualified(engine, table),
                None => table.clone(),
            };
            let key: Vec<String> = if key.is_empty() {
                primary_key.map(<[String]>::to_vec).unwrap_or_default()
            } else {
                key.clone()
            };
            json!({"mode": "table", "table": {"schema": table.schema, "name": table.name},
                   "key": key, "new_rows_by": new_rows_by})
        }
        DatabaseRead::Query {
            query,
            cursor_column,
            cursor_start,
        } => json!({"mode": "query", "query": query, "cursor_column": cursor_column,
                    "cursor_start": cursor_start}),
    });
    json!({"connection": connection, "read": read})
}

/// The SHA-256 of the canonical JSON of `value` (keys sorted, no whitespace).
pub fn fingerprint(value: &Value) -> String {
    fn write(value: &Value, out: &mut String) {
        match value {
            Value::Object(members) => {
                let mut names: Vec<&String> = members.keys().collect();
                names.sort();
                out.push('{');
                for (i, name) in names.into_iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&Value::String(name.clone()).to_string());
                    out.push(':');
                    write(&members[name], out);
                }
                out.push('}');
            }
            Value::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write(item, out);
                }
                out.push(']');
            }
            other => out.push_str(&other.to_string()),
        }
    }
    let mut canonical = String::new();
    write(value, &mut canonical);
    hex::encode(Sha256::digest(canonical.as_bytes()))
}

/// Evidence and its operations, over the engine's store, policy and secrets.
pub struct Evidence {
    db: Arc<Db>,
    ctx: Context,
}

impl Evidence {
    pub fn new(db: Arc<Db>, ctx: Context) -> Self {
        Self { db, ctx }
    }

    /// The revision a definition's password takes for a Test: the one it
    /// names (which must exist), else the current one.
    pub fn resolve_credential(
        &self,
        connection: &DatabaseConnection,
    ) -> Result<Option<(String, u32)>, ManagerError> {
        let Some(password) = &connection.password else {
            return Ok(None);
        };
        let secrets = self
            .ctx
            .secrets
            .as_ref()
            .filter(|s| s.available())
            .ok_or_else(|| {
                invalid("the password is a kept secret, and secrets need custody configured (secrets.key_file or HIE_SECRETS_KEY)")
            })?;
        let missing = |e: SecretError| match e {
            SecretError::NotFound => invalid(format!("secret {} not found", password.secret)),
            SecretError::RevisionNotFound => invalid(format!(
                "secret {} has no revision {}",
                password.secret,
                password.revision.unwrap_or_default()
            )),
            SecretError::Store(e) => ManagerError::from(e),
            _ => invalid("the password's secret cannot be used"),
        };
        let revision = match password.revision {
            Some(revision) => {
                secrets
                    .has_revision(&password.secret, revision)
                    .map_err(missing)?;
                revision
            }
            None => secrets
                .current_revision(&password.secret)
                .map_err(missing)?,
        };
        Ok(Some((password.secret.clone(), revision)))
    }

    /// Checks a definition's connection against the policy.
    pub fn admit(&self, source: &SourceConfig) -> Result<(), ManagerError> {
        self.ctx.admission.check_source(source).map_err(refused)
    }

    fn purge(&self) -> Result<()> {
        for id in self.db.expire_evidence()? {
            if let Some(secrets) = &self.ctx.secrets {
                secrets.release(SecretUse::Evidence, &id, None)?;
            }
        }
        Ok(())
    }

    /// Tests a database connection by parts: the 0.1 probe members and
    /// `evidence`. A refused connection creates nothing.
    pub async fn test(&self, source: &SourceConfig) -> Result<Value, ManagerError> {
        let (connection, read) =
            parts_of(source).ok_or_else(|| invalid("not a database connection by parts"))?;
        let credential = self.resolve_credential(connection)?;
        self.admit(source)?;
        let endpoint =
            Endpoint::new(connection, credential.as_ref().map(|c| c.1)).map_err(invalid)?;
        let report = test_connection(&endpoint, read, &self.ctx).await;
        let primary_key = report.primary_key.clone();
        let definition = normalized(connection, read, primary_key.as_deref());
        let row = self.keep(&report, definition, credential, primary_key)?;
        Ok(json!({
            "reachable": report.connect == Check::Passed,
            "elapsed_ms": report.elapsed_ms,
            "objects": report.rows.unwrap_or(report.tables.len() as i64),
            "newest": null,
            "newest_at": null,
            "evidence": row.body,
        }))
    }

    fn keep(
        &self,
        report: &TestReport,
        definition: Value,
        credential: Option<(String, u32)>,
        primary_key: Option<Vec<String>>,
    ) -> Result<EvidenceRow, ManagerError> {
        self.purge()?;
        let id = crate::ids::new(Kind::Evidence);
        let now = now_ms();
        let check = |name: &str, check: &Check| match check {
            Check::Passed => json!({"name": name, "result": "passed"}),
            Check::Failed(message) => json!({"name": name, "result": "failed", "message": message}),
            Check::NotVerified => json!({"name": name, "result": "not_verified"}),
        };
        let mut checks = vec![
            json!({"name": "admission", "result": "passed"}),
            check("connect", &report.connect),
            check("authenticate", &report.authenticate),
        ];
        if let Some(read) = &report.read {
            checks.push(check("read", read));
        }
        let verdict = if report.passed() { "passed" } else { "failed" };
        let fingerprint = fingerprint(&definition);
        let body = json!({
            "id": id,
            "kind": "database",
            "verdict": verdict,
            "tested_at": rfc3339_ms_at(now),
            "definition_sha256": fingerprint,
            "credential": credential.as_ref().map(|(s, r)| json!({"secret": s, "revision": r})),
            "checks": checks,
            "tables": report.tables.iter().map(|t| json!({"schema": t.schema, "name": t.name})).collect::<Vec<_>>(),
            "read": report.read.as_ref().map(|_| json!({"rows": report.rows})),
        });
        let row = EvidenceRow {
            id: id.clone(),
            verdict: verdict.into(),
            body,
            definition,
            fingerprint,
            credential: credential.clone(),
            primary_key,
            tested_at: now,
            expires_at: now + KEEP_MS,
            source_id: None,
        };
        // The revision is held before the evidence exists, so it can never
        // be destroyed under it.
        if let (Some((secret, revision)), Some(secrets)) = (&credential, &self.ctx.secrets) {
            secrets.hold(secret, *revision, SecretUse::Evidence, &id)?;
        }
        self.db.insert_evidence(&row)?;
        Ok(row)
    }

    /// Kept evidence.
    pub fn get(&self, id: &str) -> Result<EvidenceRow, ManagerError> {
        self.purge()?;
        self.db.evidence(id)?.ok_or_else(not_found)
    }

    /// The endpoint passing evidence connects to, after checking admission again.
    fn reconnect(&self, id: &str) -> Result<(EvidenceRow, Endpoint), ManagerError> {
        let row = self.get(id)?;
        if row.verdict != "passed" {
            return Err(not_passed());
        }
        let connection: DatabaseConnection =
            serde_json::from_value(row.definition["connection"].clone()).map_err(|_| {
                ManagerError::from(anyhow::anyhow!(
                    "evidence {id} holds an unreadable definition"
                ))
            })?;
        let connection = DatabaseConnection {
            password: connection.password.map(|mut p| {
                p.revision = row.credential.as_ref().map(|c| c.1);
                p
            }),
            ..connection
        };
        self.ctx
            .admission
            .check(&connection.parts())
            .map_err(refused)?;
        let endpoint =
            Endpoint::new(&connection, row.credential.as_ref().map(|c| c.1)).map_err(invalid)?;
        Ok((row, endpoint))
    }

    /// The tables, or one table's columns, key and row count.
    pub async fn describe(
        &self,
        id: &str,
        table: Option<&TableName>,
    ) -> Result<Value, ManagerError> {
        let (_, endpoint) = self.reconnect(id)?;
        match describe_connection(&endpoint, table, &self.ctx)
            .await
            .map_err(unreachable)?
        {
            Described::Tables(tables) => Ok(json!({
                "tables": tables.iter().map(|t| json!({"schema": t.schema, "name": t.name})).collect::<Vec<_>>(),
            })),
            Described::Table(None) => Err(ManagerError::not_found(
                "table_not_found",
                "table not found",
            )),
            Described::Table(Some((info, rows))) => Ok(json!({
                "table": {"schema": info.table.schema, "name": info.table.name},
                "columns": info.columns.iter().map(|c| json!({
                    "name": c.name, "type": c.type_name, "nullable": c.nullable,
                })).collect::<Vec<_>>(),
                "primary_key": info.primary_key,
                "new_rows_candidates": info.new_rows_candidates(),
                "row_count": rows,
            })),
        }
    }

    /// The first `limit` rows of a read (the evidence's own by default).
    pub async fn sample(
        &self,
        id: &str,
        read: Option<&DatabaseRead>,
        limit: usize,
    ) -> Result<Vec<Value>, ManagerError> {
        let (row, endpoint) = self.reconnect(id)?;
        let own: Option<DatabaseRead> = serde_json::from_value(row.definition["read"].clone()).ok();
        let read = read.cloned().or(own).ok_or_else(|| {
            ManagerError::invalid("invalid_body", "the evidence has no read; name one")
        })?;
        sample_connection(&endpoint, &read, limit, &self.ctx)
            .await
            .map_err(unreachable)
    }

    /// What a Start (or replace) of `source` takes from evidence `id`, in
    /// the contract's order; `None` when the definition needs no evidence.
    pub fn bind(
        &self,
        source: &SourceConfig,
        id: Option<&str>,
    ) -> Result<Option<StartBinding>, ManagerError> {
        let Some((connection, read)) = parts_of(source) else {
            return Ok(None);
        };
        let id = id.ok_or_else(|| {
            ManagerError::invalid(
                "evidence_required",
                "a database source needs the evidence of a passing Test",
            )
        })?;
        let row = self.get(id)?;
        if row.verdict != "passed" {
            return Err(not_passed());
        }
        let definition = normalized(connection, read, row.primary_key.as_deref());
        if fingerprint(&definition) != row.fingerprint {
            return Err(ManagerError::conflict(
                "evidence_mismatch",
                "the definition differs from the one the evidence tested",
            )
            .with("field", "definition"));
        }
        let named = connection.password.as_ref().and_then(|p| p.revision);
        let evidence_revision = row.credential.as_ref().map(|c| c.1);
        if named.is_some() && named != evidence_revision {
            return Err(ManagerError::conflict(
                "evidence_mismatch",
                "the definition names another secret revision than the evidence's",
            )
            .with("field", "credential"));
        }
        let key = match read {
            Some(DatabaseRead::Table { key, .. }) => {
                let key: Vec<String> = definition["read"]["key"]
                    .as_array()
                    .map(|k| {
                        k.iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_else(|| key.clone());
                if key.is_empty() {
                    return Err(ManagerError::conflict(
                        "read_not_admitted",
                        "the table has no primary key; name the read's key",
                    ));
                }
                Some(key)
            }
            _ => None,
        };
        Ok(Some(StartBinding {
            evidence: row.id,
            credential: row.credential,
            key,
        }))
    }

    /// A source started (or was replaced) with this binding: it holds its
    /// evidence and credential revision; what it held before is released.
    pub fn source_started(&self, binding: &Binding) -> Result<()> {
        if let Some(secrets) = &self.ctx.secrets {
            let keep = binding.credential.as_ref().map(|(s, r)| (s.as_str(), *r));
            if let Some((secret, revision)) = keep {
                secrets.hold(secret, revision, SecretUse::Source, &binding.source)?;
            }
            // What the previous definition held, other than this revision.
            secrets.release(SecretUse::Source, &binding.source, keep)?;
        }
        match &binding.evidence {
            Some(id) => self.db.hold_evidence(&binding.source, id)?,
            None => self.db.release_evidence(&binding.source)?,
        }
        self.db.set_binding(binding)?;
        self.purge()
    }

    /// A source was deleted: its evidence and revisions are released.
    pub fn source_deleted(&self, source: &str) -> Result<()> {
        if let Some(secrets) = &self.ctx.secrets {
            secrets.release(SecretUse::Source, source, None)?;
        }
        self.db.release_evidence(source)?;
        self.db.delete_binding(source)?;
        self.purge()
    }

    /// What a source is bound to.
    pub fn binding(&self, source: &str) -> Result<Option<Binding>> {
        self.db.binding(source)
    }
}
