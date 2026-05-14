//! Middleware-example client: demonstrates `ClientConfig::with_default_header`
//! for the per-call auth header and `CallOptions::with_timeout` for a
//! per-call deadline.

use std::time::Duration;

use connectrpc::client::{CallOptions, ClientConfig, HttpClient};

pub mod proto {
    connectrpc::include_generated!();
}

use proto::anthropic::connectrpc::middleware_demo::v1::*;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::var("MIDDLEWARE_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let token = std::env::var("MIDDLEWARE_TOKEN").unwrap_or_else(|_| "demo-token-alice".into());

    // Auth header lives on `ClientConfig` so every call picks it up
    // automatically. Set per-call defaults here for anything you want
    // applied to all RPCs (auth, tracing IDs, request budget).
    let config = ClientConfig::new(url.parse()?)
        .with_default_header("authorization", format!("Bearer {token}"))
        .with_default_timeout(Duration::from_secs(10));

    let http = HttpClient::plaintext();
    let client = SecretServiceClient::new(http, config);

    // No-options call: picks up the auth header + 10s timeout from config.
    // Edition 2023 default presence is EXPLICIT, so string fields are
    // Option<String> on the wire and Option<&str> on views.
    let resp = client
        .get_secret(GetSecretRequest {
            name: Some("shared".into()),
            ..Default::default()
        })
        .await?;
    println!("{:<11} -> {}", "shared", resp.view().value.unwrap_or(""));
    if let Some(server) = resp.trailers().get("x-served-by") {
        println!("  trailer x-served-by: {}", server.to_str().unwrap_or("?"));
    }

    // Per-call override: tighter 2s deadline for this single RPC.
    // Per-call options replace config defaults for fields they set
    // (timeout here); other defaults (the auth header) still apply.
    let resp = client
        .get_secret_with_options(
            GetSecretRequest {
                name: Some("alice-only".into()),
                ..Default::default()
            },
            CallOptions::default().with_timeout(Duration::from_secs(2)),
        )
        .await;
    match resp {
        Ok(r) => println!("{:<11} -> {}", "alice-only", r.view().value.unwrap_or("")),
        Err(e) => println!(
            "{:<11} -> error: {}",
            "alice-only",
            e.message.as_deref().unwrap_or("(no message)")
        ),
    }

    Ok(())
}
