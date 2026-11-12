//! What each kind of secret holds, and its rules. Only this module knows that an
//! `aws` secret is an access key id and a secret access key.

use secrecy::ExposeSecret;

use super::{SecretError, SecretKind, SecretValue};

impl SecretKind {
    /// The value's fields, in the order they are sealed.
    pub fn fields(self) -> &'static [&'static str] {
        match self {
            SecretKind::Aws => &["access_key_id", "secret_access_key"],
        }
    }

    /// Every field present, each 1 to 256 printable ASCII characters without
    /// whitespace. A refusal names the field, never its content.
    pub fn check(self, value: &SecretValue) -> Result<(), SecretError> {
        for field in self.fields() {
            let Some(text) = value.field(field) else {
                return Err(SecretError::Invalid {
                    field,
                    message: "is required".into(),
                });
            };
            let text = text.expose_secret();
            if text.is_empty() || text.len() > 256 {
                return Err(SecretError::Invalid {
                    field,
                    message: "must be 1 to 256 characters".into(),
                });
            }
            if !text.bytes().all(|b| b.is_ascii_graphic()) {
                return Err(SecretError::Invalid {
                    field,
                    message: "must be printable ASCII without whitespace".into(),
                });
            }
        }
        if value.fields().len() != self.fields().len() {
            return Err(SecretError::Invalid {
                field: "value",
                message: format!("a value of kind {} has other fields", self.as_str()),
            });
        }
        Ok(())
    }

    /// What tells two credentials apart without revealing them: the last four
    /// characters of the access key id.
    pub fn hint(self, value: &SecretValue) -> Option<String> {
        match self {
            SecretKind::Aws => value.field("access_key_id").map(|id| {
                let id = id.expose_secret();
                id[id.len().saturating_sub(4)..].to_string()
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use secrecy::SecretString;

    use super::*;

    fn value(pairs: &[(&str, &str)]) -> Result<SecretValue, SecretError> {
        SecretValue::new(
            SecretKind::Aws,
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), SecretString::from(v.to_string()))),
        )
    }

    #[test]
    fn an_aws_value_needs_both_keys_and_nothing_else() {
        let ok = value(&[
            ("secret_access_key", "s/k+1"),
            ("access_key_id", "AKIDEXAMPLE"),
        ]);
        let ok = ok.unwrap();
        assert_eq!(SecretKind::Aws.hint(&ok).as_deref(), Some("MPLE"));
        assert_eq!(ok.fields()[0].0, "access_key_id");
        let short = value(&[("access_key_id", "AK"), ("secret_access_key", "s")]).unwrap();
        assert_eq!(SecretKind::Aws.hint(&short).as_deref(), Some("AK"));

        let refused = [
            value(&[("access_key_id", "AK")]),
            value(&[("access_key_id", "AK"), ("secret_access_key", "")]),
            value(&[("access_key_id", "A K"), ("secret_access_key", "s")]),
            value(&[("access_key_id", "AK"), ("secret_access_key", "é")]),
            value(&[
                ("access_key_id", "AK"),
                ("secret_access_key", &"s".repeat(257)),
            ]),
            value(&[
                ("access_key_id", "AK"),
                ("secret_access_key", "s"),
                ("session_token", "t"),
            ]),
            value(&[("access_key_id", "AK"), ("access_key_id", "AK2")]),
        ];
        for r in refused {
            let Err(SecretError::Invalid { message, .. }) = r else {
                panic!("not refused");
            };
            assert!(!message.contains("AK") && !message.contains("session"));
        }
    }
}
