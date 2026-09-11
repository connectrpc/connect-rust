//! gRPC server reflection for `connectrpc`.
//!
//! Wire-compatible with [`grpc.reflection.v1.ServerReflection`] and its
//! `v1alpha` predecessor, so `grpcurl`, `buf curl`, Postman, `grpcui`,
//! and every other reflection-aware client just works — over gRPC,
//! gRPC-Web, and the Connect protocol alike.
//!
//! # Quick start
//!
//! Emit a descriptor set from your build script alongside code
//! generation:
//!
//! ```ignore
//! // build.rs
//! connectrpc_build::Config::new()
//!     .files(&["proto/app.proto"])
//!     .includes(&["proto/"])
//!     .emit_descriptor_set("app.fds.bin")
//!     .compile()
//!     .unwrap();
//! ```
//!
//! then embed it and mount the service:
//!
//! ```no_run
//! use connectrpc::Router;
//! use connectrpc_reflection::{Reflector, install};
//!
//! // In real code: include_bytes!(concat!(env!("OUT_DIR"), "/app.fds.bin"))
//! # fn descriptor_set_bytes() -> &'static [u8] { &[] }
//! let reflector = Reflector::from_descriptor_set_bytes(descriptor_set_bytes()).unwrap();
//! let router = install(Router::new(), reflector);
//! ```
//!
//! [`install`] registers both protocol versions; use the generated
//! extension traits directly if you want only one (and see
//! [Request limits](#request-limits) below for the extra call that needs).
//!
//! Alternatively, when your buffa codegen has reflection enabled, skip
//! the build-script step and serve straight from the generated package's
//! descriptor pool:
//!
//! ```ignore
//! let reflector =
//!     Reflector::from_descriptor_pool(myapp::proto::descriptor_pool().clone()).unwrap();
//! ```
//!
//! The bytes path needs only `emit_descriptor_set` — reflection codegen
//! is **not** required — and answers with the compiler's original
//! per-file descriptor bytes; the pool path re-encodes (semantically
//! faithful, unknown fields preserved). See [`Reflector`] for the
//! trade-off.
//!
//! # Request limits
//!
//! A `ServerReflectionRequest` is a host plus one symbol, file name or type
//! name, so the reflection routes do not need the multi-megabyte request
//! ceiling a `connectrpc` service allows by default. This crate sizes them
//! to [`MAX_REQUEST_BYTES`] (16 KiB) per request *message* through per-route
//! [`Limits`](connectrpc::Limits) — see [`request_limits`] for the exact
//! profile; `ServerReflectionInfo` is a bidirectional stream, so the bound
//! is per message rather than per call. A larger message ends the stream
//! with `resource_exhausted`. The profile *replaces* the service-wide limits
//! on these routes, whether those are looser or tighter; its decode budget
//! is likewise charged per message. Within that ceiling, a lookup miss
//! echoes at most 128 bytes of the queried name in
//! its `ErrorResponse`, so the error text is bounded by a constant rather
//! than by the size of the request. What remains is the protocol's own
//! echo: every response carries `original_request` whole and repeats its
//! `host` as `valid_host`, as grpc-go's does, so a response is at most about
//! twice the request that produced it.
//!
//! * [`install`] applies [`request_limits`] for you, to both versions.
//! * Registering a [`ReflectionService`] any other way — one version through
//!   the generated [`ServerReflectionExt::register`](ServerReflectionExt), or
//!   [`Router::add_service`](connectrpc::Router::add_service) — does not, so
//!   follow it with [`apply_request_limits`]`(router, `[`request_limits`]`())`;
//!   it covers whichever versions are mounted.
//! * To tune the reflection routes specifically, call
//!   [`apply_request_limits`] with your own `Limits` after either path; the
//!   later call wins.
//!
//! ```no_run
//! use connectrpc::Router;
//! use connectrpc_reflection::{Reflector, apply_request_limits, install, request_limits};
//!
//! # fn descriptor_set_bytes() -> &'static [u8] { &[] }
//! let reflector = Reflector::from_descriptor_set_bytes(descriptor_set_bytes()).unwrap();
//! let router = install(Router::new(), reflector);
//! // Optional: hold reflection messages to 1 KiB instead of the bundled 16 KiB.
//! // Start from `request_limits()` so the rest of the profile carries over.
//! let router = apply_request_limits(router, request_limits().with_max_message_size(1024));
//! # drop(router);
//! ```
//!
//! # What gets exposed
//!
//! Everything in the descriptor set: all files, their transitive
//! imports, and every service compiled into it — whether or not the
//! corresponding handlers are mounted on the router. Use
//! [`Reflector::with_services`] to curate the advertised service list,
//! and [`Reflector::service_names`] to inspect it. Build the set from
//! the same protos you serve, and remember that reflection
//! intentionally publishes your schema: gate or omit the service on
//! deployments where that is not wanted.
//!
//! The reflection service is **self-describing**: queries about
//! `grpc.reflection.*` fall back to the crate's own descriptors, and
//! `ListServices` advertises the reflection services alongside yours.
//! This matches grpc-go (where the reflection proto is always
//! registered) and is what schema-free callers like `buf curl` need to
//! invoke `ServerReflectionInfo` directly. Use
//! [`Reflector::with_services`] to advertise a different list — the
//! override is verbatim, so omitting the reflection names de-lists them
//! (they stay resolvable as symbols).
//!
//! # Cargo features
//!
//! * **`client`** (on by default) — re-exports the generated
//!   `ServerReflectionClient` for querying a reflection server
//!   (integration tests, CLI tooling). Pulls in `connectrpc`'s `client`
//!   feature; server-only deployments opt out with
//!   `default-features = false`.
//!
//! [`grpc.reflection.v1.ServerReflection`]: https://github.com/grpc/grpc-proto/blob/master/grpc/reflection/v1/reflection.proto
#![cfg_attr(docsrs, feature(doc_cfg))]

mod reflector;
mod service;

#[path = "generated/connect/mod.rs"]
mod connect;
// `message_response`'s variants all end in `Response` (proto field names);
// buffa 0.7's generated allow-list does not yet cover this lint firing on
// oneofs.
#[allow(clippy::enum_variant_names)]
#[path = "generated/buffa/mod.rs"]
mod proto;

pub use reflector::{ReflectionError, Reflector};
pub use service::{
    MAX_REQUEST_BYTES, ReflectionService, apply_request_limits, install, request_limits,
};

/// The wire-format `FileDescriptorSet` for this crate's protos
/// (`grpc.reflection.v1` and `v1alpha`, from the public Buf Schema
/// Registry's `buf.build/grpc/grpc` module).
///
/// Every [`Reflector`] already consults these descriptors as a built-in
/// fallback, so the reflection service describes and lists itself with
/// no setup. The constant is exposed for other uses — e.g. registering
/// the reflection schema with a different protobuf runtime, the way
/// tonic-reflection's constant of the same name is consumed.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../descriptor/reflection.fds.bin");

/// Fully-qualified name of the v1 reflection service.
pub use connect::grpc::reflection::v1::SERVER_REFLECTION_SERVICE_NAME;
/// Generated v1 service trait and registration extension, for callers
/// that mount a single protocol version by hand:
///
/// ```no_run
/// use std::sync::Arc;
/// use connectrpc::Router;
/// use connectrpc_reflection::{Reflector, ReflectionService, ServerReflectionExt};
///
/// # fn descriptor_set_bytes() -> &'static [u8] { &[] }
/// let reflector = Reflector::from_descriptor_set_bytes(descriptor_set_bytes()).unwrap();
/// let service = Arc::new(ReflectionService::new(reflector));
/// // v1 only; `apply_request_limits` bounds whichever versions are mounted.
/// let router = service.register(Router::new());
/// let router = connectrpc_reflection::apply_request_limits(
///     router,
///     connectrpc_reflection::request_limits(),
/// );
/// ```
pub use connect::grpc::reflection::v1::{ServerReflection, ServerReflectionExt};

/// Fully-qualified name of the v1alpha reflection service.
pub use connect::grpc::reflection::v1alpha::SERVER_REFLECTION_SERVICE_NAME as SERVER_REFLECTION_V1ALPHA_SERVICE_NAME;
/// Generated v1alpha service trait and registration extension, renamed to
/// avoid colliding with the v1 items, for callers that mount the legacy
/// protocol version by hand.
pub use connect::grpc::reflection::v1alpha::{
    ServerReflection as ServerReflectionV1alpha, ServerReflectionExt as ServerReflectionV1alphaExt,
};

/// Generated client for querying a `grpc.reflection.v1.ServerReflection`
/// server.
#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub use connect::grpc::reflection::v1::ServerReflectionClient;

/// Re-exports of the generated `grpc.reflection.*` wire types — request
/// and response messages, their oneof modules, and the method `Spec`
/// constants. Everything a downstream crate needs to drive
/// `ServerReflectionClient` (gated on the `client` feature) or inspect
/// responses without regenerating the protos.
pub mod wire {
    /// `grpc.reflection.v1` wire types.
    pub mod v1 {
        pub use crate::connect::grpc::reflection::v1::SERVER_REFLECTION_SERVER_REFLECTION_INFO_SPEC;
        pub use crate::proto::grpc::reflection::v1::{
            ErrorResponse, ExtensionNumberResponse, ExtensionRequest, FileDescriptorResponse,
            ListServiceResponse, ServerReflectionRequest, ServerReflectionResponse,
            ServiceResponse, server_reflection_request, server_reflection_response,
        };
    }
    /// `grpc.reflection.v1alpha` wire types.
    pub mod v1alpha {
        pub use crate::connect::grpc::reflection::v1alpha::SERVER_REFLECTION_SERVER_REFLECTION_INFO_SPEC;
        pub use crate::proto::grpc::reflection::v1alpha::{
            ErrorResponse, ExtensionNumberResponse, ExtensionRequest, FileDescriptorResponse,
            ListServiceResponse, ServerReflectionRequest, ServerReflectionResponse,
            ServiceResponse, server_reflection_request, server_reflection_response,
        };
    }
}
