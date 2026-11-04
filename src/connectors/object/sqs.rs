//! S3 event notifications read from an SQS queue, so a source learns of new
//! objects without listing the bucket on every pass. Messages are the bucket's
//! own `s3:ObjectCreated:*` events, directly or wrapped by SNS; test events and
//! anything else are consumed and ignored. Requests are signed with the
//! source's own AWS credentials (SigV4, service `sqs`), through the signer the
//! S3 client already carries; no SDK is involved.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use object_store::CredentialProvider;
use object_store::aws::{AwsAuthorizer, AwsCredential};
use serde_json::{Value, json};

/// Messages taken from the queue in one receive.
const RECEIVE_BATCH: usize = 10;

pub type Credentials = Arc<dyn CredentialProvider<Credential = AwsCredential>>;

/// One queue, read as the source's AWS identity.
pub struct SqsQueue {
    url: String,
    region: String,
    credentials: Credentials,
    client: reqwest::Client,
}

/// An object the queue announced, with the message to delete once it is taken.
#[derive(Debug, Clone)]
pub struct Notice {
    pub key: String,
    pub size: Option<u64>,
    pub receipt: String,
}

impl SqsQueue {
    pub fn new(url: &str, region: &str, credentials: Credentials) -> Self {
        Self {
            url: url.to_string(),
            region: region.to_string(),
            credentials,
            client: reqwest::Client::builder()
                .user_agent(concat!("hie/", env!("CARGO_PKG_VERSION")))
                .build()
                .expect("reqwest client"),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// The region a standard queue URL names (`sqs.<region>.amazonaws.com`).
    pub fn region_of(url: &str) -> Option<String> {
        let host = url.split("://").nth(1)?.split('/').next()?;
        let mut parts = host.split('.');
        if parts.next()? != "sqs" {
            return None;
        }
        parts.next().map(str::to_string)
    }

    /// Takes what the queue holds, up to `max` notices, keeping only object
    /// creations. Messages that announce nothing (test events, other shapes)
    /// are deleted here.
    pub async fn receive(&self, max: usize) -> Result<Vec<Notice>> {
        let mut notices = Vec::new();
        let mut noise = Vec::new();
        while notices.len() < max {
            let answer = self
                .call(
                    "AmazonSQS.ReceiveMessage",
                    json!({
                        "QueueUrl": self.url,
                        "MaxNumberOfMessages": RECEIVE_BATCH,
                        "WaitTimeSeconds": 0,
                    }),
                )
                .await?;
            let messages = answer
                .get("Messages")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if messages.is_empty() {
                break;
            }
            for message in messages {
                let Some(receipt) = message.get("ReceiptHandle").and_then(Value::as_str) else {
                    continue;
                };
                let body = message.get("Body").and_then(Value::as_str).unwrap_or("");
                let keys = created_objects(body);
                if keys.is_empty() {
                    noise.push(receipt.to_string());
                    continue;
                }
                for (key, size) in keys {
                    notices.push(Notice {
                        key,
                        size,
                        receipt: receipt.to_string(),
                    });
                }
            }
        }
        if !noise.is_empty() {
            self.delete(&noise).await?;
        }
        Ok(notices)
    }

    /// Deletes messages by receipt, ten per call.
    pub async fn delete(&self, receipts: &[String]) -> Result<()> {
        let mut unique: Vec<&String> = receipts.iter().collect();
        unique.sort();
        unique.dedup();
        for chunk in unique.chunks(RECEIVE_BATCH) {
            let entries: Vec<Value> = chunk
                .iter()
                .enumerate()
                .map(|(i, r)| json!({"Id": i.to_string(), "ReceiptHandle": r}))
                .collect();
            let answer = self
                .call(
                    "AmazonSQS.DeleteMessageBatch",
                    json!({"QueueUrl": self.url, "Entries": entries}),
                )
                .await?;
            if let Some(failed) = answer.get("Failed").and_then(Value::as_array)
                && !failed.is_empty()
            {
                bail!("{} notifications could not be deleted", failed.len());
            }
        }
        Ok(())
    }

    async fn call(&self, target: &str, body: Value) -> Result<Value> {
        let credential = self
            .credentials
            .get_credential()
            .await
            .context("AWS credentials for the queue")?;
        let mut request = self
            .client
            .post(&self.url)
            .header("content-type", "application/x-amz-json-1.0")
            .header("x-amz-target", target)
            .body(body.to_string())
            .build()?;
        AwsAuthorizer::new(&credential, "sqs", &self.region)
            .with_sign_payload(true)
            .authorize(&mut request, None);
        let response = self
            .client
            .execute(request)
            .await
            .map_err(|e| anyhow!("queue {}: {e}", self.url))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let answer: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        if !status.is_success() {
            let kind = answer
                .get("__type")
                .and_then(Value::as_str)
                .unwrap_or("error");
            let message = answer
                .get("message")
                .or_else(|| answer.get("Message"))
                .and_then(Value::as_str)
                .unwrap_or(&text);
            bail!(
                "queue {}: {target} answered HTTP {status} {kind}: {message}",
                self.url
            );
        }
        Ok(answer)
    }
}

/// The objects an S3 notification announces as created: `(key, size)` per
/// record, from the event itself or from the SNS envelope around it.
pub fn created_objects(body: &str) -> Vec<(String, Option<u64>)> {
    let Ok(mut event) = serde_json::from_str::<Value>(body) else {
        return Vec::new();
    };
    // SNS wraps the event as the string `Message`.
    if let Some(inner) = event.get("Message").and_then(Value::as_str)
        && let Ok(parsed) = serde_json::from_str::<Value>(inner)
    {
        event = parsed;
    }
    let mut keys = Vec::new();
    for record in event
        .get("Records")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let name = record
            .get("eventName")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !name.starts_with("ObjectCreated") && !name.starts_with("s3:ObjectCreated") {
            continue;
        }
        let Some(object) = record.get("s3").and_then(|s| s.get("object")) else {
            continue;
        };
        let Some(key) = object.get("key").and_then(Value::as_str) else {
            continue;
        };
        keys.push((
            percent_decode(key),
            object.get("size").and_then(Value::as_u64),
        ));
    }
    keys
}

/// S3 writes keys URL-encoded in events, with `+` for a space.
fn percent_decode(key: &str) -> String {
    let bytes = key.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => match u8::from_str_radix(&key[i + 1..i + 3], 16) {
                Ok(b) => {
                    out.push(b);
                    i += 2;
                }
                Err(_) => out.push(b'%'),
            },
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_decoded_from_the_event_or_its_sns_envelope() {
        let event = json!({"Records": [
            {"eventName": "ObjectCreated:Put", "s3": {"object": {"key": "in/lab+results%2Bv2.csv", "size": 12}}},
            {"eventName": "ObjectRemoved:Delete", "s3": {"object": {"key": "in/gone.csv"}}},
        ]});
        assert_eq!(
            created_objects(&event.to_string()),
            vec![("in/lab results+v2.csv".to_string(), Some(12))]
        );
        let sns = json!({"Type": "Notification", "Message": event.to_string()});
        assert_eq!(created_objects(&sns.to_string()).len(), 1);
        assert!(created_objects(r#"{"Service":"Amazon S3","Event":"s3:TestEvent"}"#).is_empty());
        assert!(created_objects("not json").is_empty());
    }

    #[test]
    fn the_region_comes_from_a_standard_queue_url() {
        assert_eq!(
            SqsQueue::region_of("https://sqs.eu-west-1.amazonaws.com/123456789012/hie-drops"),
            Some("eu-west-1".into())
        );
        assert_eq!(
            SqsQueue::region_of("http://localhost:9324/queue/drops"),
            None
        );
    }
}
