//! ConnectRPC implementation for Rust
//!
//! This crate provides a tower-based ConnectRPC runtime that can be integrated
//! with any HTTP framework that supports tower services (axum, hyper, tonic, etc.).
//!
//! # Architecture
//!
//! The core abstraction is [`ConnectRpcService`], a [`tower::Service`] that handles
//! ConnectRPC requests. This allows seamless integration with existing web servers:
//!
//! ```rust,ignore
//! use connectrpc::{Router, ConnectRpcService};
//! use std::sync::Arc;
//!
//! // Build your router with RPC handlers
//! let greet_impl = Arc::new(MyGreetService);
//! let router = Router::new().add_service(greet_impl);
//!
//! // Get a tower::Service - use with ANY compatible framework
//! let service = ConnectRpcService::new(router);
//! ```
//!
//! # Framework Integration
//!
//! ## With Axum (recommended)
//!
//! Enable the `axum` feature for convenient integration:
//!
//! ```rust,ignore
//! use axum::{Router, routing::get};
//! use connectrpc::Router as ConnectRouter;
//! use std::sync::Arc;
//!
//! let greet_impl = Arc::new(MyGreetService);
//! let connect = ConnectRouter::new().add_service(greet_impl);
//!
//! let app = Router::new()
//!     .route("/health", get(health))
//!     .fallback_service(connect.into_axum_service());
//!
//! axum::serve(listener, app).await?;
//! ```
//!
//! ## With Raw Hyper
//!
//! Use `ConnectRpcService` directly with hyper's service machinery.
//!
//! ## Standalone Server
//!
//! For simple cases, enable the `server` feature for a built-in hyper server:
//!
//! ```rust,ignore
//! use connectrpc::{Router, Server};
//!
//! let router = Router::new();
//! // ... register handlers ...
//!
//! Server::new(router).serve(addr).await?;
//! ```
//!
//! # Modules
//!
//! - [`codec`] - Message encoding/decoding (protobuf and JSON)
//! - [`compression`] - Pluggable compression (gzip, zstd) with streaming support
//! - [`envelope`] - Streaming message framing (5-byte header + payload)
//! - [`error`] - ConnectRPC error types and HTTP status mapping
//! - [`handler`] - Async handler traits for implementing RPC methods
//! - [`request`] - Borrowed single-message request views ([`ServiceRequest`])
//! - [`stream_message`] - Owned per-item streaming message wrapper ([`StreamMessage`])
//! - [`response`] - Handler response types and [`RequestContext`]
//! - [`router`] - Request routing and service registration
//! - [`service`] - Tower service implementation (primary integration point)
//! - [`dispatcher`] - Method dispatch glue between router and generated code
//! - [`spec`] - Static per-method metadata ([`Spec`], [`StreamType`])
//! - [`payload`] - Lazily-decoded, type-erased message bodies ([`Payload`])
//! - [`interceptor`] - RPC-level interceptors ([`Interceptor`], [`Next`])
//! - [`deadline`] - Server-side deadline moderation ([`DeadlinePolicy`])
//! - [`protocol`] - Protocol detection ([`Protocol`]: Connect, gRPC, gRPC-Web)
//! - [`client`] - Tower-based HTTP client utilities (transports require the `client` feature)
//! - [`server`] - Standalone hyper-based server (requires `server` feature)
//!
//! # Protocol Support
//!
//! Servers speak the [Connect protocol](https://connectrpc.com/docs/protocol),
//! gRPC, and gRPC-Web from a single registration; clients can be configured
//! for any of the three:
//! - All four RPC shapes: unary, server-streaming, client-streaming, bidi
//!   (full-duplex bidi requires HTTP/2; browsers additionally cannot
//!   stream request bodies, regardless of protocol)
//! - Proto and JSON message encoding
//! - Compression negotiation (gzip, zstd) with streaming support
//! - Error handling with proper HTTP status mapping
//! - Trailers via `trailer-` prefixed headers
//! - Envelope framing for streaming messages
//! - Deadline propagation and server-side deadline moderation
//!
//! # Client
//!
//! Enable the `client` feature and use generated clients with a transport.
//!
//! **For gRPC** (HTTP/2), use [`Http2Connection`](client::Http2Connection):
//!
//! ```rust,ignore
//! use connectrpc::client::{Http2Connection, ClientConfig};
//! use connectrpc::Protocol;
//!
//! let uri: http::Uri = "http://localhost:8080".parse()?;
//! let conn = Http2Connection::connect_plaintext(uri.clone()).await?.shared(1024);
//! let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
//!
//! let client = GreetServiceClient::new(conn, config);
//! let response = client.greet(request).await?;
//! ```
//!
//! **For Connect over HTTP/1.1** (or unknown protocol), use
//! [`HttpClient`](client::HttpClient):
//!
//! ```rust,ignore
//! use connectrpc::client::{HttpClient, ClientConfig};
//!
//! let http = HttpClient::plaintext();  // cleartext http:// only
//! let config = ClientConfig::new("http://localhost:8080".parse()?);
//!
//! let client = GreetServiceClient::new(http, config);
//! ```
//!
//! ## Per-call options and defaults
//!
//! Generated clients expose both `foo(req)` and `foo_with_options(req, opts)`
//! for each RPC. Use [`CallOptions`](client::CallOptions) for per-call timeout,
//! headers, message-size limits, and compression overrides.
//!
//! For settings you want on every call, configure [`ClientConfig`](client::ClientConfig)
//! defaults — they're applied automatically by the no-options method:
//!
//! ```rust,ignore
//! let config = ClientConfig::new(uri)
//!     .with_default_timeout(Duration::from_secs(30))
//!     .with_default_header("authorization", "Bearer ...");
//!
//! let client = GreetServiceClient::new(http, config);
//! client.greet(req).await?;  // uses 30s timeout + auth header
//! ```
//!
//! Per-call `CallOptions` override config defaults.
//!
//! See the [`client`] module docs for connection balancing and the
//! transport selection rationale.
//!
//! # Feature Flags
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `json` | ✓ | JSON codec for protobuf messages; disable for proto-only builds |
//! | `gzip` | ✓ | Gzip compression |
//! | `zstd` | ✓ | Zstandard compression |
//! | `streaming` | ✓ | Streaming compression support |
//! | `client` | ✗ | HTTP client transports (plaintext) |
//! | `client-tls` | ✗ | TLS for client transports |
//! | `server` | ✗ | Standalone hyper-based server |
//! | `server-tls` | ✗ | TLS for the built-in server |
//! | `tls` | ✗ | Convenience: `server-tls` + `client-tls` |
//! | `axum` | ✗ | Axum framework integration |

#![deny(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

/// Spawn a detached background future on the ambient executor.
///
/// On native targets this dispatches via [`tokio::spawn`] and returns the join
/// handle. On `wasm32` there is no tokio runtime, so the future is dispatched
/// via [`wasm_bindgen_futures::spawn_local`] and `None` is returned (no
/// joinable handle available).
///
/// The `Send` bound is required on native (`tokio::spawn`) but relaxed on
/// wasm32 (`spawn_local` is single-threaded).
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn spawn_detached<F>(future: F) -> Option<tokio::task::JoinHandle<()>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    Some(tokio::spawn(future))
}

/// wasm32 variant — see non-wasm docs above.
#[cfg(target_arch = "wasm32")]
pub(crate) fn spawn_detached<F>(future: F) -> Option<tokio::task::JoinHandle<()>>
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
    None
}

// Core modules (always available)
pub mod codec;
pub mod compression;
pub mod deadline;
pub mod dispatcher;
pub mod envelope;
pub mod error;
pub(crate) mod grpc_status;
pub mod handler;
pub mod interceptor;
pub mod payload;
pub mod protocol;
pub mod request;
pub mod response;
pub mod router;
pub mod service;
pub mod spec;
pub mod stream_message;

// Optional: HTTP client
pub mod client;

// Optional: Standalone hyper-based server
#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub mod server;

// Optional: TLS-aware `axum::serve` counterpart with peer-identity passthrough.
//
// Note: this module shadows the extern-prelude `axum` crate within the crate
// root scope only. Don't add `use axum::...` here in `lib.rs`; use
// `::axum::...` if a root-level reference to the external crate is ever needed.
#[cfg(all(feature = "axum", feature = "server-tls"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "axum", feature = "server-tls"))))]
pub mod axum;

// ============================================================================
// Primary exports - Tower-first API
// ============================================================================

// The main entry point - a tower::Service for ConnectRPC
pub use service::ConnectRpcBody;
pub use service::ConnectRpcService;
pub use service::Limits;
pub use service::StreamingResponseBody;

// Router for registering RPC handlers
pub use router::MethodKind;
pub use router::Router;
pub use router::RouterMergeError;
pub use router::ServiceRegister;
pub use router::merge_routers;

// Dispatcher trait for monomorphic dispatch (codegen-backed alternative to Router)
pub use dispatcher::Chain;
pub use dispatcher::Dispatcher;
pub use dispatcher::MethodDescriptor;

// Handler traits and request/response types
pub use handler::BidiStreamingHandler;
pub use handler::ClientStreamingHandler;
pub use handler::Handler;
pub use handler::StreamingHandler;
pub use handler::ViewBidiStreamingHandler;
pub use handler::ViewClientStreamingHandler;
pub use handler::ViewHandler;
pub use handler::ViewStreamingHandler;
pub use handler::bidi_streaming_handler_fn;
pub use handler::client_streaming_handler_fn;
pub use handler::handler_fn;
pub use handler::streaming_handler_fn;
pub use handler::view_bidi_streaming_handler_fn;
pub use handler::view_client_streaming_handler_fn;
pub use handler::view_handler_fn;
pub use handler::view_streaming_handler_fn;
pub use request::HasMessageView;
pub use request::ServiceRequest;
pub use response::Encodable;
pub use response::EncodedResponse;
pub use response::InboundStream;
pub use response::MaybeBorrowed;
pub use response::PreEncoded;
pub use response::RequestContext;
pub use response::Response;
pub use response::ServiceResult;
pub use response::ServiceStream;
pub use stream_message::StreamMessage;

/// Re-exports for generated code. Not part of the public API; subject
/// to change without a semver bump.
#[doc(hidden)]
pub mod __codegen {
    pub use crate::response::encode_view_body;
}

// Error types
pub use error::ConnectError;
pub use error::ErrorCode;
pub use error::ErrorDetail;

/// Re-export of the `http-body` crate whose [`Body`](http_body::Body) trait
/// appears in generated client bounds — so consumers don't need their own
/// `http-body` dependency to use generated code.
pub use http_body;

// Protocol detection
pub use protocol::Protocol;
pub use protocol::RequestProtocol;

// Static method metadata
pub use spec::IdempotencyLevel;
pub use spec::Spec;
pub use spec::SpecOrigin;
pub use spec::StreamType;

// Type-erased message bodies for interceptors
pub use interceptor::async_trait;
pub use payload::AnyMessage;
pub use payload::Payload;

// RPC interceptors (unary and streaming). The wire-level request/response
// aliases (interceptor::UnaryRequest and friends) stay module-scoped: at the
// crate root those names belong to the far more common client-facing types
// below.
pub use interceptor::Interceptor;
pub use interceptor::Next;
pub use interceptor::NextStream;
pub use interceptor::PayloadStream;
pub use interceptor::streaming_interceptor;
pub use interceptor::unary_interceptor;

// Client response and stream handles (what generated client methods return)
pub use client::BidiStream;
pub use client::ServerStream;
pub use client::UnaryResponse;

// Request-side adapter for client-streaming calls. Re-exported at the root
// because adapting a ready collection is the common call site.
pub use client::stream_iter;

// ============================================================================
// Codec exports
// ============================================================================

pub use codec::CodecFormat;
#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
pub use codec::JsonCodec;
pub use codec::JsonDeserialize;
pub use codec::JsonSerialize;
pub use codec::ProtoCodec;

// ============================================================================
// Compression exports
// ============================================================================

pub use compression::CompressionPolicy;
pub use compression::CompressionProvider;
pub use compression::CompressionRegistry;
pub use compression::DEFAULT_COMPRESSION_MIN_SIZE;

// ============================================================================
// Deadline exports
// ============================================================================

pub use deadline::DeadlinePolicy;

#[cfg(feature = "gzip")]
#[cfg_attr(docsrs, doc(cfg(feature = "gzip")))]
pub use compression::GzipProvider;

#[cfg(feature = "zstd")]
#[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
pub use compression::ZstdProvider;

#[cfg(feature = "streaming")]
#[cfg_attr(docsrs, doc(cfg(feature = "streaming")))]
pub use compression::BoxedAsyncBufRead;

#[cfg(feature = "streaming")]
#[cfg_attr(docsrs, doc(cfg(feature = "streaming")))]
pub use compression::BoxedAsyncRead;

#[cfg(feature = "streaming")]
#[cfg_attr(docsrs, doc(cfg(feature = "streaming")))]
pub use compression::StreamingCompressionProvider;

// ============================================================================
// Optional: Standalone server
// ============================================================================

#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub use server::BoundServer;

#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub use server::Server;

#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub use server::PeerAddr;
#[cfg(feature = "server-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
pub use server::PeerCerts;

/// Re-export of `rustls` for TLS configuration.
///
/// Use this to construct a [`rustls::ServerConfig`] for [`Server::with_tls`]
/// or a [`rustls::ClientConfig`] for [`HttpClient::with_tls`](client::HttpClient::with_tls)
/// / [`Http2Connection::connect_tls`](client::Http2Connection::connect_tls).
#[cfg(any(feature = "server-tls", feature = "client-tls"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "server-tls", feature = "client-tls"))))]
pub use rustls;

/// Include the generated ConnectRPC file from `$OUT_DIR`.
///
/// Shorthand for `include!(concat!(env!("OUT_DIR"), "/_connectrpc.rs"))`.
///
/// Requires `Config::include_file` in `build.rs` (the no-arg form assumes
/// the filename `"_connectrpc.rs"`):
///
/// ```rust,ignore
/// // build.rs
/// connectrpc_build::Config::new()
///     .files(&["proto/my_service.proto"])
///     .includes(&["proto/"])
///     .include_file("_connectrpc.rs")
///     .compile()
///     .unwrap();
/// ```
///
/// ```rust,ignore
/// // src/lib.rs
/// pub mod proto {
///     connectrpc::include_generated!();
/// }
/// ```
///
/// `OUT_DIR` is resolved in the **calling crate's** compilation context.
///
/// If you customised the output filename via `Config::include_file`, pass the
/// **filename** (including the `.rs` extension) as a string literal. Unlike
/// `tonic::include_proto!`, this argument is a filename, not a proto package
/// name:
///
/// ```rust,ignore
/// pub mod proto {
///     connectrpc::include_generated!("my_output.rs");
/// }
/// ```
///
/// # Notes
///
/// - This macro is only for the `build.rs`/`OUT_DIR` workflow. If you use
///   `buf generate` to write files into `src/generated/`, use `#[path]`:
///
///   ```rust,ignore
///   #[path = "generated/proto/mod.rs"]
///   pub mod proto;
///   ```
///
/// - If `Config::out_dir` was used to redirect output away from `$OUT_DIR`,
///   this macro does not apply; use `#[path]` or raw `include!` instead.
///
/// - If your proto package hierarchy contains a module named `connectrpc`,
///   the crate name may be shadowed in scope. Use the absolute path to avoid
///   the ambiguity:
///
///   ```rust,ignore
///   mod proto {
///       ::connectrpc::include_generated!();
///   }
///   ```
///
/// # Compile errors
///
/// This macro produces a compile error (not a runtime panic) if:
///
/// - `OUT_DIR` is not set — the crate is not being built by Cargo.
/// - The generated file does not exist — `Config::include_file` was not
///   called in `build.rs`, or the filename passed to the one-arg form does
///   not match what was passed to `Config::include_file`.
#[macro_export]
macro_rules! include_generated {
    () => {
        include!(concat!(env!("OUT_DIR"), "/_connectrpc.rs"));
    };
    ($file:literal) => {
        include!(concat!(env!("OUT_DIR"), "/", $file));
    };
}
