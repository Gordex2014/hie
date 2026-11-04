//! A FHIR Bundle, one record per entry resource, or a single resource.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use super::SourceRecord;

/// Reads a FHIR Bundle as one record per entry resource. A file holding a single
/// resource is one record.
///
/// Entries are delivered independently, so references between them that use the
/// entry's `fullUrl` (Synthea writes `urn:uuid:...`) are rewritten to `Type/id`
/// from the referenced entry's resource.
pub fn read_fhir_bundle(text: &str) -> Result<Vec<SourceRecord>> {
    let doc: Value = serde_json::from_str(text).context("invalid JSON")?;
    let rt = doc.get("resourceType").and_then(Value::as_str);
    match rt {
        None => bail!("JSON has no resourceType"),
        Some("Bundle") => {}
        Some(_) => {
            return Ok(vec![SourceRecord {
                line: 1,
                raw: doc,
                parse_error: None,
            }]);
        }
    }
    let entries = doc
        .get("entry")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut urls: HashMap<String, String> = HashMap::new();
    for e in &entries {
        let full = e.get("fullUrl").and_then(Value::as_str);
        let res = e.get("resource");
        if let (Some(full), Some(res)) = (full, res) {
            let rt = res.get("resourceType").and_then(Value::as_str);
            let id = res.get("id").and_then(Value::as_str);
            if let (Some(rt), Some(id)) = (rt, id) {
                urls.insert(full.to_string(), format!("{rt}/{id}"));
            }
        }
    }
    let mut out = Vec::with_capacity(entries.len());
    for (i, e) in entries.iter().enumerate() {
        let line = i as i64 + 1;
        match e.get("resource") {
            Some(res @ Value::Object(_)) => {
                let mut raw = res.clone();
                if !urls.is_empty() {
                    rewrite_references(&mut raw, &urls);
                }
                out.push(SourceRecord {
                    line,
                    raw,
                    parse_error: None,
                });
            }
            _ => out.push(SourceRecord {
                line,
                raw: e.clone(),
                parse_error: Some("entry has no resource".into()),
            }),
        }
    }
    Ok(out)
}

fn rewrite_references(v: &mut Value, urls: &HashMap<String, String>) {
    match v {
        Value::Object(m) => {
            for (k, val) in m.iter_mut() {
                if k == "reference"
                    && let Value::String(s) = val
                    && let Some(target) = urls.get(s.as_str())
                {
                    *s = target.clone();
                    continue;
                }
                rewrite_references(val, urls);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(|x| rewrite_references(x, urls)),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn bundle_entries_become_records_with_absolute_references() {
        let bundle = json!({
            "resourceType": "Bundle", "type": "transaction",
            "entry": [
                {"fullUrl": "urn:uuid:p1", "resource": {"resourceType": "Patient", "id": "p1"}},
                {"fullUrl": "urn:uuid:o1", "resource": {"resourceType": "Observation", "id": "o1",
                    "subject": {"reference": "urn:uuid:p1"},
                    "performer": [{"reference": "urn:uuid:unknown"}]}},
                {"fullUrl": "urn:uuid:x"}
            ]
        });
        let recs = read_fhir_bundle(&bundle.to_string()).unwrap();
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[1].line, 2);
        assert_eq!(recs[1].raw["subject"]["reference"], "Patient/p1");
        assert_eq!(recs[1].raw["performer"][0]["reference"], "urn:uuid:unknown");
        assert_eq!(
            recs[2].parse_error.as_deref(),
            Some("entry has no resource")
        );
    }

    #[test]
    fn a_single_resource_file_is_one_record() {
        let recs = read_fhir_bundle(r#"{"resourceType":"Patient","id":"x"}"#).unwrap();
        assert_eq!(recs.len(), 1);
        assert!(read_fhir_bundle(r#"{"a":1}"#).is_err());
    }
}
