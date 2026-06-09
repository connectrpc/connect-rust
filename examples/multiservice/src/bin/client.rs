//! Multi-service example ConnectRPC client.
//!
//! Demonstrates using generated clients with shared connection pooling.
//!
//! Run with: `cargo run --bin multiservice-client`
//!
//! Make sure the multiservice-server is running first:
//!   `cargo run --bin multiservice-server`

use std::collections::HashMap;

// `value` (lowercase) is the oneof submodule for `Value`'s `kind`
// oneof, re-exported at the natural path by buffa 0.5+.
use buffa_types::google::protobuf::{Duration, Struct, Timestamp, Value, value};
use connectrpc::client::ClientConfig;
use connectrpc::client::HttpClient;
use multiservice_example::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // Create a shared HTTP client with connection pooling
    let http = HttpClient::plaintext();
    let base_uri: http::Uri = "http://127.0.0.1:8080".parse()?;
    let config = ClientConfig::new(base_uri);

    // Create typed clients sharing the same connection pool
    let greet_client = GreetServiceClient::new(http.clone(), config.clone());
    let math_client = MathServiceClient::new(http.clone(), config.clone());
    let wkt_client = WellKnownTypesServiceClient::new(http.clone(), config.clone());

    tracing::info!("Created clients with shared HTTP connection pool");

    // --- GreetService ---
    tracing::info!("Testing Greet RPC...");
    let response = greet_client
        .greet(GreetRequest {
            name: "World".to_string(),
            ..Default::default()
        })
        .await?;
    tracing::info!("Greet response: {}", response.view().message);

    let response = greet_client
        .greet(GreetRequest {
            name: "ConnectRPC".to_string(),
            ..Default::default()
        })
        .await?;
    tracing::info!("Greet response: {}", response.view().message);

    // --- MathService ---
    tracing::info!("Testing Add RPC...");
    let response = math_client
        .add(AddRequest {
            a: 40,
            b: 2,
            ..Default::default()
        })
        .await?;
    tracing::info!("Add: 40 + 2 = {}", response.view().result);

    let response = math_client
        .add(AddRequest {
            a: -10,
            b: 25,
            ..Default::default()
        })
        .await?;
    tracing::info!("Add: -10 + 25 = {}", response.view().result);

    // --- Error handling ---
    tracing::info!("Testing error handling (empty name)...");
    match greet_client
        .greet(GreetRequest {
            name: String::new(),
            ..Default::default()
        })
        .await
    {
        Ok(_) => tracing::warn!("Expected error but got success"),
        Err(e) => tracing::info!("Got expected error: {e}"),
    }

    // --- WellKnownTypesService ---
    tracing::info!("Testing CreateEvent RPC...");
    let response = wkt_client
        .create_event(CreateEventRequest {
            name: "Conference Talk".to_string(),
            occurred_at: Timestamp {
                seconds: 1704067200, // 2024-01-01 00:00:00 UTC
                nanos: 0,
                ..Default::default()
            }
            .into(),
            duration: Duration {
                seconds: 3600, // 1 hour
                nanos: 0,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .await?;
    let event = response.view().event.as_option().unwrap();
    tracing::info!(
        "Created event: id={}, name={}, occurred_at={:?}",
        event.id,
        event.name,
        event.occurred_at.as_option()
    );

    tracing::info!("Testing CalculateDuration RPC...");
    let response = wkt_client
        .calculate_duration(CalculateDurationRequest {
            start: Timestamp {
                seconds: 1704067200, // 2024-01-01 00:00:00 UTC
                nanos: 0,
                ..Default::default()
            }
            .into(),
            end: Timestamp {
                seconds: 1704153600, // 2024-01-02 00:00:00 UTC
                nanos: 500_000_000,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .await?;
    let duration = response.view().duration.as_option().unwrap();
    tracing::info!("Duration: {}s {}ns", duration.seconds, duration.nanos);

    tracing::info!("Testing ProcessMetadata RPC...");
    let mut fields = HashMap::new();
    fields.insert(
        "name".to_string(),
        Value {
            kind: Some(value::Kind::StringValue("hello".to_string())),
            ..Default::default()
        },
    );
    fields.insert(
        "count".to_string(),
        Value {
            kind: Some(value::Kind::NumberValue(42.0)),
            ..Default::default()
        },
    );

    let response = wkt_client
        .process_metadata(ProcessMetadataRequest {
            metadata: Struct {
                fields,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .await?;
    tracing::info!(
        "ProcessMetadata: input had {} fields, output has {} fields",
        response.view().field_count,
        response
            .view()
            .metadata
            .as_option()
            .map(|m| m.fields.len())
            .unwrap_or(0)
    );

    tracing::info!("Testing Heartbeat RPC...");
    let response = wkt_client
        .heartbeat(buffa_types::google::protobuf::Empty::default())
        .await?;
    let now = response.view();
    tracing::info!("Heartbeat: server time {}s {}ns", now.seconds, now.nanos);

    tracing::info!("All tests completed successfully!");
    Ok(())
}
