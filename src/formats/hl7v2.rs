//! HL7 v2 messages as records.
//!
//! A message becomes a JSON object the transform can read without parsing pipes:
//! its type, control id, sender, and every segment as a list of fields (`PID-5`
//! is `fields[4]` of the `PID` segment; `MSH` counts the separator as its first
//! field, as the standard does). Components and repetitions stay as text with
//! their separators; the Python helpers in `hie_runtime/hie.py` split them.

use anyhow::{Result, bail};
use serde_json::{Value, json};

use super::SourceRecord;

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub message_type: String,
    pub control_id: String,
    pub sending_application: String,
    pub sending_facility: String,
    pub receiving_application: String,
    pub receiving_facility: String,
    pub version: String,
    pub segments: Vec<Segment>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Segment {
    pub name: String,
    pub fields: Vec<String>,
}

/// Segment separators as they arrive: `\r` by the standard, `\n` or `\r\n` from
/// files edited by hand.
fn normalize(text: &str) -> String {
    text.replace("\r\n", "\r").replace('\n', "\r")
}

/// Parses one message. The first segment must be `MSH` with at least the
/// separator fields; a message is not refused for anything after that.
pub fn parse(text: &str) -> Result<Message> {
    let normalized = normalize(text);
    let lines: Vec<&str> = normalized
        .split('\r')
        .map(|l| l.trim_end_matches('\n'))
        .filter(|l| !l.trim().is_empty())
        .collect();
    let Some(first) = lines.first() else {
        bail!("empty message");
    };
    if !first.starts_with("MSH") || first.len() < 8 {
        bail!(
            "a message starts with an MSH segment; this one starts with {:?}",
            first.chars().take(20).collect::<String>()
        );
    }
    let field_sep = first[3..4].to_string();
    let mut segments = Vec::new();
    for line in &lines {
        let mut parts: Vec<String> = line.split(field_sep.as_str()).map(str::to_string).collect();
        let name = parts.remove(0);
        if name.len() != 3 {
            bail!("segment name {name:?} is not three characters");
        }
        let fields = if name == "MSH" {
            let mut f = vec![field_sep.clone()];
            f.extend(parts);
            f
        } else {
            parts
        };
        segments.push(Segment { name, fields });
    }
    let msh = |n: usize| segments[0].fields.get(n - 1).cloned().unwrap_or_default();
    Ok(Message {
        message_type: msh(9),
        control_id: msh(10),
        sending_application: msh(3),
        sending_facility: msh(4),
        receiving_application: msh(5),
        receiving_facility: msh(6),
        version: msh(12),
        segments,
        text: normalized,
    })
}

/// Splits a file holding several messages at every `MSH` segment. Returns each
/// message's text with the line it starts on.
pub fn split_messages(text: &str) -> Vec<(i64, String)> {
    let normalized = normalize(text);
    let mut out: Vec<(i64, String)> = Vec::new();
    for (i, line) in normalized.split('\r').enumerate() {
        if line.starts_with("MSH") {
            out.push((i as i64 + 1, String::new()));
        }
        if let Some((_, current)) = out.last_mut()
            && !line.trim().is_empty()
        {
            current.push_str(line);
            current.push('\r');
        }
    }
    out
}

impl Message {
    pub fn to_json(&self) -> Value {
        json!({
            "message_type": self.message_type,
            "control_id": self.control_id,
            "sending_application": self.sending_application,
            "sending_facility": self.sending_facility,
            "receiving_application": self.receiving_application,
            "receiving_facility": self.receiving_facility,
            "version": self.version,
            "segments": self.segments.iter().map(|s| json!({"name": s.name, "fields": s.fields})).collect::<Vec<_>>(),
            "text": self.text,
        })
    }

    /// The first segment of a name.
    pub fn segment(&self, name: &str) -> Option<&Segment> {
        self.segments.iter().find(|s| s.name == name)
    }
}

/// Reads HL7 v2 messages, one record each, with the line their `MSH` is on. A
/// message that does not parse is kept as a record with the error, so the file's
/// other messages still go through.
pub fn read_hl7v2(text: &str) -> Vec<SourceRecord> {
    let mut out = Vec::new();
    for (line, message) in split_messages(text) {
        out.push(match parse(&message) {
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

#[cfg(test)]
mod tests {
    use super::*;

    const ADT: &str = "MSH|^~\\&|LAB|NORTH|HIE|HFS|20261021120000||ADT^A01|MSG0001|P|2.5.1\rEVN|A01|20261021120000\rPID|1||12345^^^NORTH^MR||Doe^Jane^M||19800101|F\r";

    #[test]
    fn a_message_parses_into_segments_and_fields() {
        let m = parse(ADT).unwrap();
        assert_eq!(m.message_type, "ADT^A01");
        assert_eq!(m.control_id, "MSG0001");
        assert_eq!(m.sending_application, "LAB");
        assert_eq!(m.version, "2.5.1");
        assert_eq!(m.segments.len(), 3);
        let msh = m.segment("MSH").unwrap();
        assert_eq!(msh.fields[0], "|");
        assert_eq!(msh.fields[1], "^~\\&");
        assert_eq!(msh.fields[8], "ADT^A01");
        let pid = m.segment("PID").unwrap();
        assert_eq!(pid.fields[2], "12345^^^NORTH^MR");
        assert_eq!(pid.fields[4], "Doe^Jane^M");
        assert_eq!(m.to_json()["segments"][2]["fields"][4], "Doe^Jane^M");
    }

    #[test]
    fn newlines_are_accepted_and_bad_messages_refused() {
        let m = parse(&ADT.replace('\r', "\n")).unwrap();
        assert_eq!(m.segments.len(), 3);
        assert!(parse("PID|1|x").is_err());
        assert!(parse("").is_err());
    }

    #[test]
    fn files_split_at_each_msh() {
        let two = format!("{ADT}\n{}", ADT.replace("MSG0001", "MSG0002"));
        let parts = split_messages(&two);
        assert_eq!(parts.len(), 2);
        assert_eq!((parts[0].0, parts[1].0), (1, 4));
        assert_eq!(parse(&parts[1].1).unwrap().control_id, "MSG0002");
    }
}
