//! The API behind bearer tokens: a JWKS served by the test, tokens signed with a
//! key that exists only for this test, and the requests the UI will make.

use std::path::PathBuf;

use axum::routing::get;
use axum::{Json, Router};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{Value, json};

/// Test-only RSA key. It signs nothing outside this file.
const TEST_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDeCiyvdpj8EBeT
PjW8AWRMNPL9gEAIV3FfrOFrfm4yVBkODCusZEKHmioWurLbaaGiRt0gXtbHRRPa
9IsPM5mhOu/8A5eK4VZZvVLo7by0twEsV8dUe9fK9A4pvATo30+h5uw6b304BGhI
BxKFNIQIs/09JWTkhGrKuoFaPqbQexqfZBZ4pfu4Ovax4TW1QIdhwbrtRW6d0gow
MMZMn0DHXLJPajzhuNgSjc/vQOFgqCNWxIgMQGsLT0JqDJzv8tREnF2Q7EalrZ1E
tifDwVRkdfhjGeZbyc/f/wN13aiv7Uj5J2HP6hWEC9pDNhy4zhIhmSwtsXVL8xae
vC0e58xVAgMBAAECggEALExVW/BKV0eSnIK1z7trWjTaIa87ly5y5Qaltn9z1sCm
ewH8mJksGMeuC/jVhCbP44NFzJZ/PSRP8MgHElJWHS+X/2xsCH4qbx7xcp0zn84/
AqzEjNtkUw5b4QGs7RTYW1Gp4YKqtclTUMNwc3jluqLGLDPQrDKq56VL90g2mbMG
jwJr9f0wgimuufoLg63TSx+CBJJLVpqALIpWQCycoJgFKovjSZ7ZXTbbHsB5g+O5
0exkbiZtDIO+EREzD1rJ+4VseebvCEdsLq7HnS9FUmUpNDGz+7zJKf1Y+s1MfR86
TCQ1mtKesR9KPBWZHGz1gn+EXBozhYb1FhkQGRBUBQKBgQD2S7VX94ZgnzMCtV0g
wEiNenqkGJRKbHsVcT6zsitjIM+lGXtnq9SeDkghu5BI13YdXb0gdPXWcqkwcpRN
hGT/4L7fTxaxaqeTgLoJfKBcux6ilNKfmQUkgj+4psJr9JFuReid37+pYfV05Qph
plyOqF/0JTvHmszLmwamnpuj+wKBgQDmyc4hZEKzrrslPDFFa9QHbwt3oSwuBgiG
iG07Gl3/5A5+0YxuJ4mEC4MiLZbI99VmLEiTz2OUUsYrP1Iqy8AVTyRt4eg0RPsG
iGzZsSw8NyAXEQu7bO/YAvKhP7irScjYJctFkohpq9Qc5bjQ0wFENCWkbokRxHII
lKbvWJoP7wKBgGs6ArVNeh9Or/KZ6vV7QRr3G7WdTdIo8F0hdF7fTqxVeJE0StmT
oXk+VoD5G5Pa2f1m9hPPV8gJG0eQWqh031PXEMI/uG19NX+uJ1BCBb7ZaDj3+cqp
3+dJHVzSnQpK65XKsb8pzlN75lUB8kcZGmqxOzUbkK8R4d8ymu6nVrNJAoGAfExa
Ya+fISlNkBtgKhSOKMbM9UxwAAWt189ojTmBQQ2yJOl1aeqPbPfKU8EcAfdBDvtw
x3MeHceLBcXf2j7MoG10oF3YczZnwLf/cCu1OGU0Olgq6nIcp0nHJcOtEUv3CTsp
xLnMEKXD4yByrr3GY9Z2I56I4nHcPPTbB/O/Xi8CgYEA8Yc00EONBbVIMYuzT1Kv
E0ONsIlODKk9nLzZwV5swRZ+8Zdpv1IIqcyPJXJhx4TKiD34jZYXccDuWXdcfDf8
8RzUGatejCY///ufHWYXedmuvYELaSi8f6bTxQZu+OSOktG7822GMeUk1bmBy2rm
tqPS/UNjZ0GnBFL4lSRvF+0=
-----END PRIVATE KEY-----
";

const TEST_JWK: &str = r#"{"kty":"RSA","n":"3gosr3aY_BAXkz41vAFkTDTy_YBACFdxX6zha35uMlQZDgwrrGRCh5oqFrqy22mhokbdIF7Wx0UT2vSLDzOZoTrv_AOXiuFWWb1S6O28tLcBLFfHVHvXyvQOKbwE6N9PoebsOm99OARoSAcShTSECLP9PSVk5IRqyrqBWj6m0Hsan2QWeKX7uDr2seE1tUCHYcG67UVundIKMDDGTJ9Ax1yyT2o84bjYEo3P70DhYKgjVsSIDEBrC09Cagyc7_LURJxdkOxGpa2dRLYnw8FUZHX4YxnmW8nP3_8Ddd2or-1I-Sdhz-oVhAvaQzYcuM4SIZksLbF1S_MWnrwtHufMVQ","e":"AQAB","kid":"test-1","alg":"RS256","use":"sig"}"#;

const ISSUER: &str = "https://issuer.test";

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

async fn jwks_server() -> String {
    let keys = json!({"keys": [serde_json::from_str::<Value>(TEST_JWK).unwrap()]});
    let base =
        serve(Router::new().route("/jwks", get(move || async move { Json(keys.clone()) }))).await;
    format!("{base}/jwks")
}

fn token(claims: Value) -> String {
    let header = Header {
        alg: Algorithm::RS256,
        kid: Some("test-1".into()),
        ..Default::default()
    };
    let key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM.as_bytes()).unwrap();
    jsonwebtoken::encode(&header, &claims, &key).unwrap()
}

fn claims(subject: &str, issuer: &str, expires_in: i64) -> Value {
    scoped(subject, issuer, expires_in, "openid hie/*")
}

fn scoped(subject: &str, issuer: &str, expires_in: i64, scope: &str) -> Value {
    let now = chrono::Utc::now().timestamp();
    json!({
        "iss": issuer, "aud": "hie", "sub": subject,
        "iat": now, "exp": now + expires_in,
        "scope": scope,
    })
}

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// An engine with no sources, so no Python is needed; `auth` decides who may call it.
async fn engine(auth: hie::auth::Auth) -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/fhir_passthrough.py"),
        root.join("transforms/fhir_passthrough.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        "fhir_base_url: http://unused\ndatabase: data/hie.db\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), auth, None).unwrap();
    (serve(hie::api::router(state)).await, dir)
}

#[tokio::test]
async fn the_api_requires_a_valid_bearer_token_when_auth_is_on() {
    let jwks_url = jwks_server().await;
    let settings = hie::auth::AuthSettings {
        enabled: true,
        jwks_url: Some(jwks_url),
        issuer: Some(ISSUER.into()),
        audience: Some("hie".into()),
        algorithms: vec!["RS256".into()],
        jwks_min_refresh_interval: 0,
    };
    let auth = hie::auth::build(&settings).await.unwrap();
    assert!(auth.is_some());
    let (api, _dir) = engine(auth).await;
    let http = reqwest::Client::new();

    // Probes and the metrics scraper need no token.
    assert_eq!(
        http.get(format!("{api}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        http.get(format!("{api}/metrics"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    // The API does.
    let anonymous = http.get(format!("{api}/api/sources")).send().await.unwrap();
    assert_eq!(anonymous.status(), 401);
    assert_eq!(
        anonymous.headers().get("www-authenticate").unwrap(),
        "Bearer"
    );
    assert_eq!(
        anonymous.json::<Value>().await.unwrap()["error"],
        "missing bearer token"
    );

    for (name, bad) in [
        ("garbage", "not-a-jwt".to_string()),
        (
            "wrong issuer",
            token(claims("sam", "https://someone-else.test", 600)),
        ),
        ("expired", token(claims("sam", ISSUER, -600))), // past the validator's 60 s leeway
    ] {
        let status = http
            .get(format!("{api}/api/sources"))
            .bearer_auth(&bad)
            .send()
            .await
            .unwrap()
            .status();
        assert_eq!(status, 401, "{name} should be refused");
    }

    let good = token(claims("sam@northgate.test", ISSUER, 600));
    let listed = http
        .get(format!("{api}/api/sources"))
        .bearer_auth(&good)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.status(), 200);

    // A write with a token, and the pause records who did it from the token's subject.
    let created = http
        .post(format!("{api}/api/sources"))
        .bearer_auth(&good)
        .json(&json!({"id": "exports", "name": "Exports",
            "source": {"type": "folder", "path": "inbox", "poll_seconds": 3600},
            "format": "ndjson", "transform": "transforms/fhir_passthrough.py"}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let paused: Value = http
        .post(format!("{api}/api/sources/exports/pause"))
        .bearer_auth(&good)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(paused["paused"]["by"], "sam@northgate.test");
    assert_eq!(
        http.post(format!("{api}/api/sources/exports/resume"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
}

#[tokio::test]
async fn scopes_decide_what_a_token_may_do() {
    let jwks_url = jwks_server().await;
    let settings = hie::auth::AuthSettings {
        enabled: true,
        jwks_url: Some(jwks_url),
        issuer: Some(ISSUER.into()),
        audience: Some("hie".into()),
        algorithms: vec!["RS256".into()],
        jwks_min_refresh_interval: 0,
    };
    let auth = hie::auth::build(&settings).await.unwrap();
    let (api, _dir) = engine(auth).await;
    let http = reqwest::Client::new();
    let mint = |scope: &str| token(scoped("sam", ISSUER, 600, scope));
    let source = json!({"id": "exports", "name": "Exports",
        "source": {"type": "folder", "path": "inbox", "poll_seconds": 3600},
        "format": "ndjson", "transform": "transforms/fhir_passthrough.py"});

    // A valid token from the issuer with no hie/ scope is authenticated but may do nothing.
    let hfs_only = mint("system/*.cruds");
    let r = http
        .get(format!("{api}/api/sources"))
        .bearer_auth(&hfs_only)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["required"], "hie/read");
    assert!(
        body["error"].as_str().unwrap().contains("no hie/ scope"),
        "{body}"
    );

    // Read: lists, but cannot change.
    let reader = mint("openid hie/read");
    assert_eq!(
        http.get(format!("{api}/api/sources"))
            .bearer_auth(&reader)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let r = http
        .post(format!("{api}/api/sources"))
        .bearer_auth(&reader)
        .json(&source)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    assert_eq!(r.json::<Value>().await.unwrap()["required"], "hie/manage");

    // Credentials are not for readers, not even to list them.
    for path in ["/api/secrets", "/api/secrets/sec_0123456789abcdef"] {
        let r = http
            .get(format!("{api}{path}"))
            .bearer_auth(&reader)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 403, "{path}");
        assert_eq!(r.json::<Value>().await.unwrap()["required"], "hie/manage");
    }

    // Manage: sources and rules, but no code.
    let manager = mint("hie/manage");
    assert_eq!(
        http.get(format!("{api}/api/secrets"))
            .bearer_auth(&manager)
            .send()
            .await
            .unwrap()
            .status(),
        if cfg!(feature = "secrets-native") {
            200
        } else {
            503
        }
    );
    assert_eq!(
        http.post(format!("{api}/api/sources"))
            .bearer_auth(&manager)
            .json(&source)
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(
        http.get(format!("{api}/api/transforms"))
            .bearer_auth(&manager)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let r = http
        .put(format!("{api}/api/transforms/new_one"))
        .bearer_auth(&manager)
        .json(&json!({"code": "def transform(record):\n    return []\n"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    assert_eq!(
        r.json::<Value>().await.unwrap()["required"],
        "hie/transforms"
    );

    // Transforms: code, and reading, but not sources.
    let developer = mint("hie/transforms");
    assert_eq!(
        http.put(format!("{api}/api/transforms/new_one"))
            .bearer_auth(&developer)
            .json(&json!({"code": "def transform(record):\n    return []\n"}))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(
        http.post(format!("{api}/api/sources/exports/pause"))
            .bearer_auth(&developer)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
}

#[tokio::test]
async fn auth_off_leaves_the_api_open() {
    let (api, _dir) = engine(None).await;
    let status = reqwest::get(format!("{api}/api/sources"))
        .await
        .unwrap()
        .status();
    assert_eq!(status, 200);
}

#[tokio::test]
async fn an_enabled_configuration_without_an_issuer_is_refused() {
    let settings = hie::auth::AuthSettings {
        enabled: true,
        jwks_url: Some("http://127.0.0.1:9/jwks".into()),
        issuer: None,
        audience: None,
        algorithms: vec!["RS256".into()],
        jwks_min_refresh_interval: 0,
    };
    let err = match hie::auth::build(&settings).await {
        Err(e) => e.to_string(),
        Ok(_) => panic!("an enabled configuration without an issuer was accepted"),
    };
    assert!(err.contains("HIE_AUTH_"), "{err}");
}
