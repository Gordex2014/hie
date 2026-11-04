//! An executor's per-item diagnostics (`warnings`, `events`, `location`,
//! `stdout`, `stderr`, `truncated`) pass through unchanged in Try answers; what
//! HIE keeps of them for loads and retries never holds the program's output.

use serde_json::Value;

/// The diagnostics as they may be stored or logged: without `stdout` and
/// `stderr` (nor `truncated`, which only describes them).
pub fn for_storage(diagnostics: &Value) -> Value {
    match diagnostics {
        Value::Object(members) => Value::Object(
            members
                .iter()
                .filter(|(name, _)| !matches!(name.as_str(), "stdout" | "stderr" | "truncated"))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        ),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn output_is_never_kept() {
        let kept = for_storage(
            &json!({"warnings": [{"message": "w"}], "location": {"line": 3},
            "stdout": "row 5.4", "stderr": "boom", "truncated": false}),
        );
        assert_eq!(
            kept,
            json!({"warnings": [{"message": "w"}], "location": {"line": 3}})
        );
        assert_eq!(for_storage(&json!("stdout")), Value::Null);
    }
}
