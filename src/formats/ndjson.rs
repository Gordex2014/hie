//! NDJSON: one JSON object per line.

use serde_json::{Value, json};

use super::SourceRecord;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
