//! Formats: how a source object is cut into records. CSV with a header row,
//! NDJSON, a FHIR Bundle (or a single resource) and HL7 v2 messages; a zip is a
//! container of such files. Which format a source reads is `config::Format`.

pub mod csv;
pub mod fhir_bundle;
pub mod hl7v2;
pub mod ndjson;
pub mod zip;

use anyhow::{Result, bail};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::Format;
pub use csv::read_csv;
pub use fhir_bundle::read_fhir_bundle;
pub use hl7v2::read_hl7v2;
pub use ndjson::read_ndjson;

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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
