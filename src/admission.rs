//! Admission: every connection a definition names is checked against the
//! allowlist of whole connections in `hie.yaml` (`Config::admission`, entries
//! already normalized), inside HIE, before a secret is decrypted, a name
//! resolved or a socket opened (decision 37). Without a policy nothing is
//! enforced.
//!
//! A connection is admitted when one entry matches all of its parts. A refused
//! one names the first part, in the order engine, host, port, database, that
//! no entry matching the parts before it matches.

use serde_json::{Value, json};

use crate::config::{AdmissionConfig, ConnectionParts, SourceConfig};

/// The policy in force.
#[derive(Debug, Clone, Default)]
pub struct Admission {
    policy: Option<AdmissionConfig>,
}

/// A connection the policy refuses, and the first part that does not match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    pub field: &'static str,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the admission policy does not allow this connection: no allowed connection matches its {}",
            self.field
        )
    }
}

impl std::error::Error for Refused {}

impl Admission {
    pub fn new(policy: Option<AdmissionConfig>) -> Self {
        let policy = policy.map(|mut p| {
            for entry in &mut p.databases {
                *entry = entry.normalized();
            }
            p
        });
        Self { policy }
    }

    pub fn enforced(&self) -> bool {
        self.policy.is_some()
    }

    /// Checks one connection's parts (normalized here).
    pub fn check(&self, parts: &ConnectionParts) -> Result<(), Refused> {
        let Some(policy) = &self.policy else {
            return Ok(());
        };
        let parts = parts.normalized();
        let mut candidates: Vec<&ConnectionParts> = policy.databases.iter().collect();
        type Part = fn(&ConnectionParts, &ConnectionParts) -> bool;
        let steps: [(&'static str, Part); 4] = [
            ("engine", |a, b| a.engine == b.engine),
            ("host", |a, b| a.host == b.host),
            ("port", |a, b| a.port == b.port),
            ("database", |a, b| a.database == b.database),
        ];
        for (field, same) in steps {
            candidates.retain(|entry| same(entry, &parts));
            if candidates.is_empty() {
                return Err(Refused { field });
            }
        }
        Ok(())
    }

    /// Checks every connection a source names: a database's connection by its
    /// parts, or (0.1) the parts of the URL its variable holds. Other kinds
    /// name no connection the policy lists.
    pub fn check_source(&self, source: &SourceConfig) -> Result<(), Refused> {
        if !self.enforced() {
            return Ok(());
        }
        match source {
            SourceConfig::Database {
                connection: Some(connection),
                ..
            } => self.check(&connection.parts()),
            SourceConfig::Database {
                url_env: Some(url_env),
                ..
            } => match crate::secrets::named(url_env)
                .as_deref()
                .and_then(url_parts)
            {
                Some(parts) => self.check(&parts),
                None => Err(Refused { field: "engine" }),
            },
            _ => Ok(()),
        }
    }

    /// `GET /api/admission`.
    pub fn view(&self) -> Value {
        match &self.policy {
            None => json!({"enforced": false, "synthetic_only": false, "databases": []}),
            Some(p) => json!({
                "enforced": true,
                "synthetic_only": p.synthetic_only,
                "databases": p.databases.iter().map(|d| {
                    let mut entry = serde_json::Map::new();
                    entry.insert("engine".into(), json!(d.engine));
                    if let Some(host) = &d.host { entry.insert("host".into(), json!(host)); }
                    if let Some(port) = d.port { entry.insert("port".into(), json!(port)); }
                    if let Some(database) = &d.database { entry.insert("database".into(), json!(database)); }
                    Value::Object(entry)
                }).collect::<Vec<_>>(),
            }),
        }
    }
}

/// The parts of a 0.1 connection URL (`postgres://`, `mysql://`,
/// `sqlserver://`, `sqlite:`), without its user or password.
pub fn url_parts(url: &str) -> Option<ConnectionParts> {
    if let Some(path) = url.strip_prefix("sqlite:") {
        let path = path.strip_prefix("//").unwrap_or(path);
        let path = path.split('?').next().unwrap_or(path);
        return Some(ConnectionParts {
            engine: "sqlite".into(),
            host: None,
            port: None,
            database: Some(path.to_string()),
        });
    }
    let parsed = url::Url::parse(url).ok()?;
    let database = parsed.path().trim_start_matches('/');
    Some(ConnectionParts {
        engine: parsed.scheme().to_string(),
        host: parsed.host_str().map(str::to_string),
        port: parsed.port(),
        database: (!database.is_empty()).then(|| database.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(engine: &str, host: Option<&str>, port: Option<u16>, db: &str) -> ConnectionParts {
        ConnectionParts {
            engine: engine.into(),
            host: host.map(str::to_string),
            port,
            database: Some(db.into()),
        }
    }

    #[test]
    fn the_first_part_without_a_match_is_named() {
        let admission = Admission::new(Some(AdmissionConfig {
            synthetic_only: true,
            databases: vec![
                parts("mssql", Some("DB.Lab."), None, "labs"),
                parts("postgres", Some("pg"), None, "labs"),
            ],
        }));
        assert_eq!(
            admission.check(&parts("sqlserver", Some("db.lab"), Some(1433), "labs")),
            Ok(())
        );
        let field = |p| admission.check(&p).unwrap_err().field;
        assert_eq!(
            field(parts("mysql", Some("db.lab"), None, "labs")),
            "engine"
        );
        // The postgres entry's host would match, but not with this engine.
        assert_eq!(field(parts("sqlserver", Some("pg"), None, "labs")), "host");
        assert_eq!(
            field(parts("sqlserver", Some("db.lab"), Some(1434), "labs")),
            "port"
        );
        assert_eq!(
            field(parts("sqlserver", Some("db.lab"), None, "Labs")),
            "database"
        );
        assert!(
            Admission::new(None)
                .check(&parts("x", None, None, "y"))
                .is_ok()
        );
    }

    #[test]
    fn legacy_urls_give_their_parts() {
        assert_eq!(
            url_parts("postgres://u:p@Host:5433/labs")
                .unwrap()
                .normalized(),
            parts("postgres", Some("host"), Some(5433), "labs")
        );
        assert_eq!(
            url_parts("sqlite:///tmp/a.db?mode=ro")
                .unwrap()
                .database
                .as_deref(),
            Some("/tmp/a.db")
        );
    }
}
