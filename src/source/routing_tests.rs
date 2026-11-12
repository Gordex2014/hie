//! The routing environment and the credential an S3 source signs with: what
//! reaches the client, and what never does.

use std::time::Duration;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};

use super::{Source, Unshareable, object_source_from, routing_env, s3_credentials};
use crate::config::{FeedConfig, SourceConfig};
use crate::secrets::Revealed;

fn s3_feed(extra: &str) -> FeedConfig {
    serde_yaml::from_str(&format!(
        "id: x\nname: X\nsource:\n  type: s3\n  bucket: b\n  anonymous: true\n{extra}format: ndjson\ntransform: t.py\n"
    ))
    .unwrap()
}

async fn shared(feed: &FeedConfig, seed: AmazonS3Builder) -> anyhow::Result<String> {
    let source = Source::Object(object_source_from(feed, seed, None)?);
    Ok(source
        .share("a.ndjson", Duration::from_secs(60))
        .await?
        .expect("a store object has a URL"))
}

fn fake_env(pairs: &[(&str, &str)]) -> AmazonS3Builder {
    let pairs: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    routing_env(move |name| {
        pairs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    })
}

#[test]
fn only_the_routing_variables_reach_the_builder() {
    use AmazonS3ConfigKey::*;
    let env = fake_env(&[
        ("AWS_ACCESS_KEY_ID", "AKIDFROMENV"),
        ("AWS_SECRET_ACCESS_KEY", "CANARY-env-secret"),
        ("AWS_SESSION_TOKEN", "token"),
        ("AWS_PROFILE", "default"),
        ("AWS_WEB_IDENTITY_TOKEN_FILE", "/token"),
        ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
        ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/creds"),
        ("AWS_ALLOW_HTTP", "true"),
        ("AWS_DEFAULT_REGION", "eu-west-2"),
        ("AWS_REGION", "eu-central-1"),
        ("AWS_ENDPOINT", "https://second"),
        ("AWS_ENDPOINT_URL", "https://first"),
        ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true"),
    ]);
    for key in [
        AccessKeyId,
        SecretAccessKey,
        Token,
        ContainerCredentialsRelativeUri,
    ] {
        assert_eq!(env.get_config_value(&key), None, "{key:?}");
    }
    assert_ne!(
        env.get_config_value(&Client(object_store::ClientConfigKey::AllowHttp))
            .as_deref(),
        Some("true")
    );
    assert_eq!(
        env.get_config_value(&Region).as_deref(),
        Some("eu-central-1")
    );
    assert_eq!(
        env.get_config_value(&Endpoint).as_deref(),
        Some("https://first")
    );
    assert_eq!(
        env.get_config_value(&VirtualHostedStyleRequest).as_deref(),
        Some("true")
    );
    // The default region fills an unset region; empty values are unset.
    let env = fake_env(&[
        ("AWS_REGION", ""),
        ("AWS_DEFAULT_REGION", "eu-west-2"),
        ("AWS_ENDPOINT_URL", " "),
        ("AWS_ENDPOINT", "https://second"),
    ]);
    assert_eq!(env.get_config_value(&Region).as_deref(), Some("eu-west-2"));
    assert_eq!(
        env.get_config_value(&Endpoint).as_deref(),
        Some("https://second")
    );
}

#[tokio::test]
async fn the_source_beats_the_routing_environment_which_keeps_its_host_style() {
    let env = || {
        fake_env(&[
            ("AWS_REGION", "eu-central-1"),
            ("AWS_ENDPOINT_URL", "https://b.store.example"),
            ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true"),
        ])
    };
    // An endpoint from the environment alone keeps the inherited host style.
    assert_eq!(
        shared(&s3_feed(""), env()).await.unwrap(),
        "https://b.store.example/a.ndjson"
    );
    // The source's endpoint wins, in path style.
    assert_eq!(
        shared(&s3_feed("  endpoint: https://store:9000\n"), env())
            .await
            .unwrap(),
        "https://store:9000/b/a.ndjson"
    );
    // The source's region wins over the environment's.
    let env = fake_env(&[("AWS_REGION", "eu-central-1")]);
    assert_eq!(
        shared(&s3_feed("  region: us-west-2\n"), env)
            .await
            .unwrap(),
        "https://b.s3.us-west-2.amazonaws.com/a.ndjson"
    );
}

fn credential_feed(extra: &str) -> FeedConfig {
    serde_yaml::from_str(&format!(
        "id: x\nname: X\nsource:\n  type: s3\n  bucket: b\n  region: us-east-1\n  credential: lab\n{extra}format: ndjson\ntransform: t.py\n"
    ))
    .unwrap()
}

fn revealed(key_id: &str, secret: &str, revision: u32) -> Revealed {
    use crate::secrets::{SecretKind, SecretMeta, SecretValue};
    Revealed {
        meta: SecretMeta {
            id: "sec_0123456789abcdef".into(),
            name: "lab".into(),
            kind: SecretKind::Aws,
            hint: None,
            revision,
            created_at: 0,
            updated_at: 0,
            used_by: vec![],
        },
        value: SecretValue::new(
            SecretKind::Aws,
            [
                ("access_key_id".to_string(), key_id.to_string().into()),
                ("secret_access_key".to_string(), secret.to_string().into()),
            ],
        )
        .unwrap(),
    }
}

#[tokio::test]
async fn a_credential_source_signs_with_its_credential_and_nothing_else() {
    let env = || {
        fake_env(&[
            ("AWS_ACCESS_KEY_ID", "AKIDFROMENV"),
            ("AWS_SECRET_ACCESS_KEY", "CANARY-env-secret"),
        ])
    };
    // Not loaded yet: signing fails, and may succeed later (not Unshareable).
    let source = Source::Object(object_source_from(&credential_feed(""), env(), None).unwrap());
    let err = source
        .share("a.ndjson", Duration::from_secs(60))
        .await
        .unwrap_err();
    assert!(err.downcast_ref::<Unshareable>().is_none());
    assert!(
        format!("{err:#}").contains("the credential is not loaded yet"),
        "{err:#}"
    );

    // Loaded: signed with the stored key id, never the environment's.
    let secret = "CANARY-6d0f9c5e-stored-secret";
    let r = revealed("AKIDSTORED", secret, 2);
    let source = Source::Object(object_source_from(&credential_feed(""), env(), Some(&r)).unwrap());
    let url = source
        .share("a.ndjson", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert!(url.contains("X-Amz-Credential=AKIDSTORED%2F"), "{url}");
    assert!(url.contains("X-Amz-Signature="), "{url}");
    assert!(
        !url.contains("AKIDFROMENV") && !url.contains("CANARY"),
        "{url}"
    );

    // The provider prints its credential's name and revision, nothing more.
    let provider = s3_credentials(&credential_feed(""), Some(&r)).unwrap();
    let printed = format!("{provider:?}");
    assert_eq!(printed, "RevisionCredentials(lab rev 2, ..)");

    // Neither anonymous nor a credential: refused even here.
    let mut neither = credential_feed("");
    if let SourceConfig::S3 { credential, .. } = &mut neither.source {
        *credential = None;
    }
    let err = object_source_from(&neither, env(), None).err().unwrap();
    assert!(format!("{err:#}").contains("say how to reach the bucket"));
}
