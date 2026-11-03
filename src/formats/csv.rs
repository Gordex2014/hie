//! CSV with a header row: one JSON object per data row, keyed by column name.

use anyhow::{Result, bail};
use serde_json::{Map, Value};

use super::SourceRecord;

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

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

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
}
