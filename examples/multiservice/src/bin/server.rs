//! Multi-service example ConnectRPC server using axum.
//!
//! Demonstrates registering multiple ConnectRPC services from different
//! protobuf packages into a single axum web server.
//!
//! Run with: `cargo run --bin multiservice-server`
//!
//! Test with:
//!   - `curl http://localhost:8080/health`
//!   - `cargo run --bin multiservice-client`
//!
//! The server also mounts gRPC server reflection (`grpc.reflection.v1` +
//! `v1alpha`), so schema-aware tools can discover and call the services
//! with no local proto files — see `./reflection-demo.sh` for a `buf curl`
//! walkthrough.

use std::sync::Arc;
use std::time::SystemTime;

use axum::Router;
use axum::routing::get;
// `value` (lowercase) is the oneof submodule for `Value`'s `kind`
// oneof, re-exported at the natural path by buffa 0.5+.
use buffa_types::google::protobuf::{Duration, Struct, Timestamp, Value, value};
use connectrpc::ConnectError;
use connectrpc::Router as ConnectRouter;
use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult};
use connectrpc_reflection::Reflector;
use multiservice_example::*;

/// The `FileDescriptorSet` for every proto this server compiles, with the
/// full import closure — the input to gRPC server reflection.
///
/// This example uses checked-in generated code, so the set is produced by
/// `buf build` during `task example:multiservice:generate` and checked in
/// alongside it. A `build.rs`-based project gets the same bytes from
/// `connectrpc_build::Config::emit_descriptor_set("services.fds.bin")` and
/// would point this `include_bytes!` at `concat!(env!("OUT_DIR"), ...)`.
const DESCRIPTOR_SET: &[u8] = include_bytes!("../../descriptor/services.fds.bin");

/// Build the reflection index from one of the two supported descriptor
/// sources, selected by `REFLECTION_SOURCE`:
///
/// - `fds` (default) — the checked-in `FileDescriptorSet` bytes above.
///   Responses carry those exact per-file bytes.
/// - `pool` — the `descriptor_pool()` that buffa codegen emits with
///   `reflect_mode=bridge` (see `buf.gen.yaml`). The pool covers the whole
///   codegen run, so any one package's accessor works; no descriptor file
///   is needed at all.
fn build_reflector() -> Reflector {
    match std::env::var("REFLECTION_SOURCE").as_deref() {
        Ok("pool") => {
            tracing::info!("reflection source: generated descriptor_pool()");
            let pool = proto::anthropic::connectrpc::greet::v1::descriptor_pool();
            Reflector::from_descriptor_pool(Arc::clone(pool))
                .expect("generated descriptor pool is valid")
        }
        _ => {
            tracing::info!("reflection source: checked-in FileDescriptorSet");
            Reflector::from_descriptor_set_bytes(DESCRIPTOR_SET)
                .expect("checked-in descriptor set is valid")
        }
    }
}

/// Implementation of the GreetService trait.
struct MyGreetService;

impl GreetService for MyGreetService {
    async fn greet(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GreetRequest>,
    ) -> ServiceResult<GreetResponse> {
        // Zero-copy reads: `request.name` is a &str borrowed from the
        // decoded request buffer - no owned conversion needed.
        tracing::info!("Received greet request for: {}", request.name);

        if request.name.is_empty() {
            return Err(ConnectError::invalid_argument("name cannot be empty"));
        }

        let response = GreetResponse {
            message: format!("Hello, {}!", request.name),
            ..Default::default()
        };
        Response::ok(response)
    }
}

/// Implementation of the MathService trait.
struct MyMathService;

impl MathService for MyMathService {
    async fn add(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, AddRequest>,
    ) -> ServiceResult<AddResponse> {
        tracing::info!("Received add request: {} + {}", request.a, request.b);

        let result = request
            .a
            .checked_add(request.b)
            .ok_or_else(|| ConnectError::invalid_argument("arithmetic overflow"))?;

        let response = AddResponse {
            result,
            ..Default::default()
        };
        Response::ok(response)
    }
}

/// Implementation of the WellKnownTypesService trait.
/// Demonstrates usage of Timestamp, Duration, and Struct types.
struct MyWellKnownTypesService;

impl WellKnownTypesService for MyWellKnownTypesService {
    async fn create_event(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, CreateEventRequest>,
    ) -> ServiceResult<CreateEventResponse> {
        let request = request.to_owned_message()?;
        tracing::info!("Received create_event request: {:?}", request.name);

        let now = SystemTime::now();
        let now_duration = now.duration_since(std::time::UNIX_EPOCH).unwrap();
        let now_timestamp = Timestamp {
            seconds: now_duration.as_secs() as i64,
            nanos: now_duration.subsec_nanos() as i32,
            ..Default::default()
        };
        let occurred_at = if request.occurred_at.is_set() {
            (*request.occurred_at).clone()
        } else {
            now_timestamp.clone()
        };
        let created_at = now_timestamp;

        let duration = if request.duration.is_set() {
            (*request.duration).clone()
        } else {
            Duration {
                seconds: 3600,
                nanos: 0,
                ..Default::default()
            }
        };

        let id = format!("evt_{}", now_duration.as_millis());

        let event = Event {
            id,
            name: request.name,
            occurred_at: occurred_at.into(),
            duration: duration.into(),
            created_at: created_at.into(),
            ..Default::default()
        };

        let response = CreateEventResponse {
            event: event.into(),
            ..Default::default()
        };
        Response::ok(response)
    }

    async fn calculate_duration(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, CalculateDurationRequest>,
    ) -> ServiceResult<CalculateDurationResponse> {
        let request = request.to_owned_message()?;
        tracing::info!("Received calculate_duration request");

        let start = request
            .start
            .as_option()
            .ok_or_else(|| ConnectError::invalid_argument("start timestamp is required"))?;
        let end = request
            .end
            .as_option()
            .ok_or_else(|| ConnectError::invalid_argument("end timestamp is required"))?;

        let start_nanos = start.seconds * 1_000_000_000 + start.nanos as i64;
        let end_nanos = end.seconds * 1_000_000_000 + end.nanos as i64;
        let diff_nanos = end_nanos - start_nanos;

        let duration = Duration {
            seconds: diff_nanos / 1_000_000_000,
            nanos: (diff_nanos % 1_000_000_000) as i32,
            ..Default::default()
        };

        let response = CalculateDurationResponse {
            duration: duration.into(),
            ..Default::default()
        };
        Response::ok(response)
    }

    async fn process_metadata(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ProcessMetadataRequest>,
    ) -> ServiceResult<ProcessMetadataResponse> {
        let request = request.to_owned_message()?;
        tracing::info!("Received process_metadata request");

        let input_metadata = if request.metadata.is_set() {
            (*request.metadata).clone()
        } else {
            Struct::default()
        };
        let field_count = input_metadata.fields.len() as i32;

        let mut output_fields = input_metadata.fields.clone();
        output_fields.insert(
            "processed".to_string(),
            Value {
                kind: Some(value::Kind::BoolValue(true)),
                ..Default::default()
            },
        );
        output_fields.insert(
            "original_field_count".to_string(),
            Value {
                kind: Some(value::Kind::NumberValue(field_count as f64)),
                ..Default::default()
            },
        );

        let response = ProcessMetadataResponse {
            metadata: Struct {
                fields: output_fields,
                ..Default::default()
            }
            .into(),
            field_count,
            ..Default::default()
        };
        Response::ok(response)
    }

    async fn heartbeat(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, buffa_types::google::protobuf::Empty>,
    ) -> ServiceResult<Timestamp> {
        // Well-known types as the direct RPC input and output: the request
        // parameter and response type come from buffa-types via extern_path,
        // wrapped in the same ServiceRequest surface as local types.
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        Response::ok(Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
            ..Default::default()
        })
    }
}

async fn health() -> &'static str {
    "OK"
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let greet_service = Arc::new(MyGreetService);
    let math_service = Arc::new(MyMathService);
    let well_known_types_service = Arc::new(MyWellKnownTypesService);

    let connect_router = ConnectRouter::new()
        .add_service(greet_service)
        .add_service(math_service)
        .add_service(well_known_types_service);

    // Mount gRPC server reflection (v1 + v1alpha) so `grpcurl`, `buf curl`,
    // Postman, and `grpcui` can discover and call the services above with
    // no local proto files. The reflector is self-describing, so this is
    // all the setup there is.
    let connect_router = connectrpc_reflection::install(connect_router, build_reflector());

    tracing::info!("Registered RPC methods:");
    for method in connect_router.methods() {
        tracing::info!("  POST /{method}");
    }

    let app = Router::new()
        .route("/health", get(health))
        .fallback_service(connect_router.into_axum_service());

    let addr = std::env::var("ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    tracing::info!("Starting server on http://{addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
