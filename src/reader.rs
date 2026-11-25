use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::config::Format;

/// One record cut from a source object. `line` is the 1-based line in the file
/// for CSV and NDJSON, and the 1-based entry index for a FHIR Bundle, so a
/// rejection can be reported as "line 3 of labs.csv" or "entry 12 of patient.json".
#[derive(Debug, Clone, PartialEq)]
pub struct SourceRecord {
    pub line: i64,
    pub raw: Value,
    pub parse_error: Option<String>,
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Decodes UTF-8 (with or without a byte-order mark) and UTF-16 with a byte-order mark.
pub fn decode_text(bytes: &[u8]) -> Result<String> {
    if let Some(rest) = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]) {
        return Ok(String::from_utf8(rest.to_vec())?);
    }
    let utf16 = |rest: &[u8], le: bool| -> Result<String> {
        if !rest.len().is_multiple_of(2) {
            bail!("odd byte length for UTF-16 text");
        }
        let units: Vec<u16> = rest
            .as_chunks::<2>()
            .0
            .iter()
            .map(|c| {
                if le {
                    u16::from_le_bytes(*c)
                } else {
                    u16::from_be_bytes(*c)
                }
            })
            .collect();
        Ok(String::from_utf16(&units)?)
    };
    if let Some(rest) = bytes.strip_prefix(&[0xFF, 0xFE]) {
        return utf16(rest, true);
    }
    if let Some(rest) = bytes.strip_prefix(&[0xFE, 0xFF]) {
        return utf16(rest, false);
    }
    match String::from_utf8(bytes.to_vec()) {
        Ok(s) => Ok(s),
        Err(_) => bail!("file is not UTF-8 or UTF-16 with a byte-order mark"),
    }
}

/// Cuts a source object into records according to the feed's format.
pub fn read(format: Format, bytes: &[u8]) -> Result<Vec<SourceRecord>> {
    let text = decode_text(bytes)?;
    match format {
        Format::Csv => read_csv(&text),
        Format::Ndjson => Ok(read_ndjson(&text)),
        Format::FhirBundle => read_fhir_bundle(&text),
        Format::Hl7v2 => Ok(read_hl7v2(&text)),
        Format::Fhir => {
            bail!("format fhir has no reader of its own: each file is read by its extension")
        }
    }
}

/// Reads a `.json` file of a `fhir` source as a Bundle, or as nothing (`None`)
/// when it is a bulk export manifest: an object with no string `resourceType`,
/// a string `transactionTime` and an array `output`.
pub fn read_fhir_json(bytes: &[u8]) -> Result<Option<Vec<SourceRecord>>> {
    let doc: Value = serde_json::from_str(&decode_text(bytes)?).context("invalid JSON")?;
    let manifest = doc.get("resourceType").and_then(Value::as_str).is_none()
        && doc.get("transactionTime").is_some_and(Value::is_string)
        && doc.get("output").is_some_and(Value::is_array);
    if manifest {
        return Ok(None);
    }
    bundle_records(doc).map(Some)
}

/// Reads HL7 v2 messages, one record each, with the line their `MSH` is on. A
/// message that does not parse is kept as a record with the error, so the file's
/// other messages still go through.
pub fn read_hl7v2(text: &str) -> Vec<SourceRecord> {
    let mut out = Vec::new();
    for (line, message) in crate::hl7::split_messages(text) {
        out.push(match crate::hl7::parse(&message) {
            Ok(m) => SourceRecord {
                line,
                raw: m.to_json(),
                parse_error: None,
            },
            Err(e) => SourceRecord {
                line,
                raw: json!({ "_text": message }),
                parse_error: Some(format!("{e:#}")),
            },
        });
    }
    if out.is_empty() && !text.trim().is_empty() {
        out.push(SourceRecord {
            line: 1,
            raw: json!({ "_text": text }),
            parse_error: Some("no MSH segment: not an HL7 v2 message".into()),
        });
    }
    out
}

/// Reads a CSV with a header row into one JSON object per data row, keyed by column name.
/// A row whose column count differs from the header is kept, marked with a parse error,
/// instead of failing the whole file.
pub fn read_csv(text: &str) -> Result<Vec<SourceRecord>> {
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_reader(text.as_bytes());
    let headers: Vec<String> = reader.headers()?.iter().map(str::to_string).collect();
    if headers.iter().all(String::is_empty) {
        bail!("CSV has no header row");
    }
    let mut out = Vec::new();
    let (mut counted_to, mut line) = (0usize, 1i64);
    for row in reader.records() {
        let row = row?;
        let mut start = row
            .position()
            .map(|p| p.byte() as usize)
            .unwrap_or(counted_to)
            .min(text.len());
        while matches!(text.as_bytes().get(start), Some(b'\n' | b'\r')) {
            start += 1;
        }
        line += text.as_bytes()[counted_to..start]
            .iter()
            .filter(|b| **b == b'\n')
            .count() as i64;
        counted_to = start;
        if row.iter().all(str::is_empty) {
            continue;
        }
        if row.len() != headers.len() {
            let fields: Vec<Value> = row.iter().map(|s| Value::String(s.into())).collect();
            out.push(SourceRecord {
                line,
                raw: Value::Object(Map::from_iter([(
                    "_fields".to_string(),
                    Value::Array(fields),
                )])),
                parse_error: Some(format!(
                    "expected {} columns ({}), found {}",
                    headers.len(),
                    headers.join(","),
                    row.len()
                )),
            });
            continue;
        }
        let obj = headers
            .iter()
            .cloned()
            .zip(row.iter().map(|s| Value::String(s.into())))
            .collect();
        out.push(SourceRecord {
            line,
            raw: Value::Object(obj),
            parse_error: None,
        });
    }
    Ok(out)
}

/// Reads NDJSON: one JSON object per line. A line that is not a JSON object is
/// kept as a record with a parse error, and the rest of the file proceeds.
pub fn read_ndjson(text: &str) -> Vec<SourceRecord> {
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let line_no = i as i64 + 1;
        let rec = match serde_json::from_str::<Value>(line) {
            Ok(v @ Value::Object(_)) => SourceRecord {
                line: line_no,
                raw: v,
                parse_error: None,
            },
            Ok(_) => SourceRecord {
                line: line_no,
                raw: json!({ "_text": line }),
                parse_error: Some("line is not a JSON object".into()),
            },
            Err(e) => SourceRecord {
                line: line_no,
                raw: json!({ "_text": line }),
                parse_error: Some(format!("invalid JSON: {e}")),
            },
        };
        out.push(rec);
    }
    out
}

/// Reads a FHIR Bundle as one record per entry resource. A file holding a single
/// resource is one record.
///
/// Entries are delivered independently, so references between them that use the
/// entry's `fullUrl` (Synthea writes `urn:uuid:...`) are rewritten to `Type/id`
/// from the referenced entry's resource.
pub fn read_fhir_bundle(text: &str) -> Result<Vec<SourceRecord>> {
    bundle_records(serde_json::from_str(text).context("invalid JSON")?)
}

fn bundle_records(doc: Value) -> Result<Vec<SourceRecord>> {
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

    #[test]
    fn rows_are_keyed_by_header_with_file_line_numbers() {
        let recs = read_csv("patient,value\nPatient/1,95\n\nPatient/2,180\n").unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].line, 2);
        assert_eq!(recs[0].raw, json!({"patient": "Patient/1", "value": "95"}));
        assert_eq!(recs[1].line, 4);
    }

    #[test]
    fn a_row_with_an_extra_column_is_rejected_alone() {
        let recs = read_csv("a,b\n1,2\n1,2,3\n4,5\n").unwrap();
        assert_eq!(recs.len(), 3);
        assert!(recs[0].parse_error.is_none());
        assert_eq!(
            recs[1].parse_error.as_deref(),
            Some("expected 2 columns (a,b), found 3")
        );
        assert!(recs[2].parse_error.is_none());
    }

    #[test]
    fn byte_order_marks_are_understood() {
        assert_eq!(decode_text(b"\xEF\xBB\xBFa,b").unwrap(), "a,b");
        let utf16: Vec<u8> = [0xFF, 0xFE]
            .into_iter()
            .chain("a,b".encode_utf16().flat_map(u16::to_le_bytes))
            .collect();
        assert_eq!(decode_text(&utf16).unwrap(), "a,b");
        assert!(decode_text(&[0xC3, 0x28]).is_err());
    }

    #[test]
    fn ndjson_keeps_bad_lines_as_rejected_records() {
        let recs = read_ndjson("{\"resourceType\":\"Patient\",\"id\":\"a\"}\n\nnot json\n[1,2]\n");
        assert_eq!(recs.len(), 3);
        assert_eq!((recs[0].line, recs[0].parse_error.is_none()), (1, true));
        assert_eq!(recs[1].line, 3);
        assert!(
            recs[1]
                .parse_error
                .as_deref()
                .unwrap()
                .starts_with("invalid JSON")
        );
        assert_eq!(
            (recs[2].line, recs[2].parse_error.as_deref()),
            (4, Some("line is not a JSON object"))
        );
    }

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
