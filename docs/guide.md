# connectrpc User Guide

This guide is the long-form companion to the
[crate README](../README.md). It covers installation, code generation,
server and client usage, streaming, tower middleware, TLS, error
handling, and compression. If you just want to try the library, start
with the [README quick start](../README.md#quick-start) and the
[`examples/`](../examples) directory.

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Code generation](#code-generation)
- [Implementing servers](#implementing-servers)
- [Streaming RPCs](#streaming-rpcs)
- [Tower middleware](#tower-middleware)
- [Hosting](#hosting)
- [Clients](#clients)
- [Errors and status codes](#errors-and-status-codes)
- [Compression](#compression)
- [Examples directory tour](#examples-directory-tour)

## Installation

`connectrpc` ships as three crates:

| Crate | Purpose |
|---|---|
| `connectrpc` | Tower-based runtime: server dispatcher, client transports, codec, compression |
| `protoc-gen-connect-rust` (binary, in `connectrpc-codegen`) | `protoc` plugin that generates service stubs |
| `connectrpc-build` | `build.rs` integration that runs the codegen at build time |

Add the runtime to your `Cargo.toml`:

```toml
[dependencies]
connectrpc = "0.4"
```

The runtime depends on [`buffa`](https://github.com/anthropics/buffa)
for protobuf message types. Generated code requires a small set of
direct dependencies; see
[Generated Code Dependencies](../README.md#generated-code-dependencies)
in the README for the exact list.

### MSRV

The MSRV is **Rust 1.88**, declared on the workspace and verified in
CI. The crate uses Rust 2024 edition.

### Feature flags

The runtime is feature-gated so you only pay for what you use:

| Feature | Default | What it adds |
|---|---|---|
| `gzip` | yes | Gzip compression via `flate2` |
| `zstd` | yes | Zstandard compression via `zstd` |
| `streaming` | yes | Streaming compression via `async-compression` |
| `client` | no | HTTP client transports (cleartext) |
| `client-tls` | no | TLS for client transports |
| `server` | no | Built-in hyper server (`Server`) |
| `server-tls` | no | TLS for the built-in server |
| `tls` | no | Convenience alias for both `server-tls` + `client-tls` |
| `axum` | no | Axum integration (`Router::into_axum_service`, `Router::into_axum_router`) |

Common combinations:

```toml
# Just the server, behind axum
connectrpc = { version = "0.4", features = ["axum"] }

# Server + client, both with TLS
connectrpc = { version = "0.4", features = ["axum", "client", "tls"] }

# Built-in server (no axum)
connectrpc = { version = "0.4", features = ["server"] }

# Minimal (wasm-friendly: no networking, no native compression)
connectrpc = { version = "0.4", default-features = false }
```

## Quick start

Define a service:

```protobuf
// proto/greet.proto
syntax = "proto3";
package greet.v1;

service GreetService {
  rpc Greet(GreetRequest) returns (GreetResponse);
}

message GreetRequest { string name = 1; }
message GreetResponse { string greeting = 1; }
```

Generate code with `connectrpc-build` in `build.rs`:

```toml
[build-dependencies]
connectrpc-build = "0.4"
```

```rust
// build.rs
fn main() {
    connectrpc_build::Config::new()
        .files(&["proto/greet.proto"])
        .includes(&["proto/"])
        .include_file("_connectrpc.rs")
        .compile()
        .unwrap();
}
```

Implement the service:

```rust
// src/main.rs
use std::sync::Arc;
use buffa::view::OwnedView;
use connectrpc::{RequestContext, Response, Router, ServiceResult};

pub mod proto {
    connectrpc::include_generated!();
}
use proto::greet::v1::*;

struct MyGreet;

impl GreetService for MyGreet {
    async fn greet(
        &self,
        _ctx: RequestContext,
        req: OwnedView<GreetRequestView<'static>>,
    ) -> ServiceResult<GreetResponse> {
        Response::ok(GreetResponse {
            greeting: format!("Hello, {}!", req.name),
            ..Default::default()
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let service = Arc::new(MyGreet);
    let router = service.register(Router::new());
    let app = router.into_axum_router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
```

That's the full server. Make a request with curl to confirm it works:

```sh
curl -X POST http://localhost:8080/greet.v1.GreetService/Greet \
  -H 'content-type: application/json' \
  -d '{"name": "World"}'
```

For runnable end-to-end examples, see the
[`examples/`](../examples) directory.

## Code generation

Two workflows are supported. Both produce the same runtime API.

### `connectrpc-build` (build-time, simplest)

Used in `build.rs`. Compiles `.proto` files at build time, regenerates
on change, no extra binaries needed.

```rust
// build.rs
fn main() {
    connectrpc_build::Config::new()
        .files(&["proto/greet.proto", "proto/billing.proto"])
        .includes(&["proto/"])
        .include_file("_connectrpc.rs")
        .compile()
        .unwrap();
}
```

Output is unified: message types and service stubs in one file per
proto, included into your crate with `connectrpc::include_generated!()`.
Best for simple projects.

### `buf generate` (checked-in code, production-grade)

Recommended when you want generated code committed to the repo,
multi-output structure (e.g. separate proto modules from service
modules), or when generating across language boundaries from one
schema. Requires three plugins: `protoc-gen-buffa` for message types,
`protoc-gen-connect-rust` for service stubs, and
`protoc-gen-buffa-packaging` for assembling `mod.rs` trees.

`protoc-gen-buffa` owns `<stem>.rs` and its ancillary companion files
(`<stem>.__view.rs`, `<stem>.__oneof.rs`, …); `protoc-gen-connect-rust`
adds `<stem>.__connect.rs` containing the service trait + client. Each
package gets a `<pkg>.mod.rs` stitcher that `include!`s all of them.

If you'd rather have one file per proto package — the convention that
[Buf Schema Registry] cargo SDK generation and [`tonic`]-style build
integrations expect — pass `opt: file_per_package` to **both**
`protoc-gen-buffa` and `protoc-gen-connect-rust`. That collapses each
plugin's output to one `<dotted.pkg>.rs` per package with everything
inlined and no per-file companion files or `<pkg>.mod.rs` stitcher.
Drop the `protoc-gen-buffa-packaging` invocations under this layout —
there is nothing for them to wire — and either let your downstream tool
synthesise the module tree from `<dotted.package>.rs` filenames (BSR
cargo SDKs do this automatically) or hand-write the `mod.rs`. Keep
routing each plugin to its own `out:` directory; the filename is shared
between them and would silently overwrite in a shared one.
`connectrpc-build` users get the same option as
`Config::file_per_package(true)`, which inlines the service stubs into
buffa's `<dotted.pkg>.rs` and is otherwise transparent — the include
file picks up the new filename automatically.

See the README's
[Code generation section](../README.md#generate-rust-code) for plugin
installation, `buf.gen.yaml` configuration, and the `buffa_module`
shorthand for cross-tree references.

[Buf Schema Registry]: https://buf.build/docs/bsr/generated-sdks/cargo/
[`tonic`]: https://docs.rs/tonic-build/latest/tonic_build/

### Inclusion patterns side-by-side

Both workflows produce the same runtime API. The only difference is how
you include the generated code into your crate:

```rust
// connectrpc-build (build.rs) users:
pub mod proto { connectrpc::include_generated!(); }

// buf generate users:
#[path = "generated/proto/mod.rs"]
pub mod proto;
```

The underlying difference (`OUT_DIR` vs a known source path) is honest
and visible, but the call-site shape is parallel.

## Implementing servers

A service is a Rust trait generated from your `.proto` file. The
trait name matches the proto service name (`GreetService` becomes
`trait GreetService`), and each RPC becomes an async method.

### Handler signatures

Unary handlers take a read-only `RequestContext` plus an
`OwnedView<RequestView<'static>>`, and return
`ServiceResult<ResponseType>`:

```rust
impl GreetService for MyGreet {
    async fn greet(
        &self,
        _ctx: RequestContext,
        req: OwnedView<GreetRequestView<'static>>,
    ) -> ServiceResult<GreetResponse> {
        // req derefs to the view: zero-copy field access.
        // String fields are &str borrowed from the request buffer.
        Response::ok(GreetResponse {
            greeting: format!("Hello, {}!", req.name),
            ..Default::default()
        })
    }
}
```

The `OwnedView` shape lets handlers read string fields without
allocating - `req.name` is a `&str` directly into the request bytes.
Call `.to_owned_message()` to get the prost-style owned struct when
you need it.

### `RequestContext` and `Response`

Request-side metadata lives on `RequestContext` (passed in);
response-side metadata lives on `Response<B>` (returned):

| `RequestContext` field | Purpose |
|---|---|
| `headers` | Caller-supplied headers (read with `ctx.header(name)` or `ctx.headers.get(...)`) |
| `deadline` | Absolute `Instant` if the caller set a timeout |
| `extensions` | `http::Extensions` carried from the underlying `http::Request` |

| `Response<B>` field | Purpose |
|---|---|
| `body` | The response message (or `ServiceStream<M>` for streaming) |
| `headers` | Headers to send before the body |
| `trailers` | Trailers to send after the body |
| `compress` | Override the server's compression policy for this RPC |

`ServiceResult<B>` is `Result<Response<B>, ConnectError>`. The happy
path is `Response::ok(body)`; to attach response metadata, use the
builder:

```rust
async fn greet(
    &self,
    _ctx: RequestContext,
    req: OwnedView<GreetRequestView<'static>>,
) -> ServiceResult<GreetResponse> {
    Ok(Response::new(GreetResponse { /* ... */ })
        .with_header("x-greet-version", "v2")
        .with_trailer("x-server-id", "node-7"))
}
```

`RequestContext::extensions` is the passthrough channel for tower-layer state:
a custom auth layer can stamp a `UserId` into the request's
`http::Extensions`, and the dispatcher forwards that map verbatim into
`RequestContext::extensions` for the handler to read with
`ctx.extensions.get::<UserId>()`. See [Tower middleware](#tower-middleware)
for the full pattern.

### What you see vs. what you write

The generated trait declares unary methods with the full RPITIT bounds:

    fn say(&self, ctx: RequestContext, req: ...)
        -> impl Future<Output = ServiceResult<impl Encodable<SayResponse> + Send + 'static + use<Self>>> + Send;

That is what `cargo doc` and rust-analyzer hover show. You never write
that form in an impl - `async fn` desugars the outer `impl Future`, and
returning `ServiceResult<SayResponse>` (the concrete owned type) refines
the `impl Encodable<...>` bound. The short form in the examples above is
all you need.

### The `refining_impl_trait` lint

The generated trait declares unary/client-stream returns as
`ServiceResult<impl Encodable<M>>` so handlers can return either the
owned `M` or a borrowed view that encodes as `M` (see below). Writing
your impl as `-> ServiceResult<FooResponse>` *refines* that opaque
bound to a concrete type, which triggers
`refining_impl_trait_internal` / `refining_impl_trait_reachable`. This
is intentional - the refinement is the point. Add at your crate root:

```rust
#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)]
```

or `#[allow(refining_impl_trait)]` on the impl block.

### Returning a view body

For handlers that often return the request unchanged (proxies, filters,
validators), the `Encodable<M>` bound lets you skip the owned-message
allocation by returning the request view directly. Codegen emits
`OwnedFooView` aliases and `impl Encodable<Foo> for OwnedFooView` per
RPC type. (When two RPC types in the same package would alias to the
same `OwnedFooView` name — e.g. a local `MyMessage` plus an imported
`api.v1.foo.bar.MyMessage` — the alias is suppressed for both and the
trait signature uses the inlined `OwnedView<…View<'static>>` form
instead.) `connectrpc::MaybeBorrowed` covers the conditional case:

```rust
use connectrpc::{MaybeBorrowed, RequestContext, Response, ServiceResult};

async fn redact(
    &self,
    _ctx: RequestContext,
    req: OwnedRecordView,
) -> ServiceResult<MaybeBorrowed<Record, OwnedRecordView>> {
    if req.email.is_empty() && req.ssn.is_empty() {
        // pass-through: re-encode straight from the request bytes
        return Response::ok(MaybeBorrowed::Borrowed(req));
    }
    let mut owned = req.to_owned_message();
    owned.email.clear();
    owned.ssn.clear();
    Response::ok(MaybeBorrowed::Owned(owned))
}
```

The `'a` on the trait method also lets the body borrow from `&self`
(e.g. cached server state). View bodies only encode for the proto
codec - JSON clients receive `unimplemented`; see
[`MaybeBorrowed`'s codec note](https://docs.rs/connectrpc/latest/connectrpc/enum.MaybeBorrowed.html#codec-compatibility).
View-body impls are not emitted for output types mapped via
`extern_path` (the impl would be an orphan); return owned for WKT or
extern outputs.

### Returning errors

Handlers return `ConnectError` for failures. Each error carries an
`ErrorCode` (the canonical Connect/gRPC status), a message, optional
structured details, and optional metadata (headers + trailers):

```rust
use connectrpc::{ConnectError, ErrorCode};

return Err(ConnectError::new(
    ErrorCode::NotFound,
    format!("user {name:?} not found"),
));
```

The dispatcher maps `ErrorCode` to the appropriate HTTP status and
serializes the error in the protocol the caller is using (Connect
JSON, Connect binary, gRPC trailers, or gRPC-Web). Handlers don't
need to know which protocol the caller chose.

### Registering services on a Router

Generated services have a `register` method (via the `register`
extension trait) that wires every RPC into a `connectrpc::Router`:

```rust
let service = Arc::new(MyGreet);
let router = service.register(Router::new());
```

To compose multiple services on one server, chain `register` calls:

```rust
let router = Router::new();
let router = Arc::new(MyGreet).register(router);
let router = Arc::new(MyBilling).register(router);
```

The router is what you mount on axum (`router.into_axum_router()`)
or pass to the built-in `Server`.

## Streaming RPCs

ConnectRPC supports all four RPC types. Define them in your `.proto`
file with the standard `stream` keyword:

```protobuf
service NumberService {
  rpc Square(SquareRequest) returns (SquareResponse);                 // unary
  rpc Range(RangeRequest) returns (stream RangeResponse);             // server stream
  rpc Sum(stream SumRequest) returns (SumResponse);                   // client stream
  rpc RunningSum(stream RunningSumRequest) returns (stream RunningSumResponse);  // bidi
}
```

The runnable demo for each type lives in
[`examples/streaming-tour/`](../examples/streaming-tour). The handler
signatures are summarized below.

The streaming-handler trait signatures use `Pin<Box<dyn Stream<...> +
Send>>` for both inbound and outbound streams. That's verbose, so the
snippets here use `connectrpc::ServiceStream<T>` (a boxed `Send`
stream of `Result<T, ConnectError>`).

### Server streaming

The handler returns a stream of responses. Use any `futures::Stream`
you like, then wrap it with `Response::stream_ok` (or
`Ok(Response::stream(s).with_header(...))` if you need response
metadata):

```rust
async fn range(
    &self,
    _ctx: RequestContext,
    req: OwnedView<RangeRequestView<'static>>,
) -> ServiceResult<ServiceStream<RangeResponse>> {
    let stream = futures::stream::iter(/* ... */);
    Response::stream_ok(stream)
}
```

### Client streaming

The handler receives a stream of request views and returns a single
response:

```rust
async fn sum(
    &self,
    _ctx: RequestContext,
    mut requests: ServiceStream<OwnedView<SumRequestView<'static>>>,
) -> ServiceResult<SumResponse> {
    let mut total: i64 = 0;
    while let Some(req) = requests.next().await {
        total += req?.value.unwrap_or(0) as i64;
    }
    Response::ok(SumResponse { total: Some(total), ..Default::default() })
}
```

### Bidirectional streaming

Takes a request stream and returns a response stream. Both sides can
emit messages independently:

```rust
async fn running_sum(
    &self,
    _ctx: RequestContext,
    requests: ServiceStream<OwnedView<RunningSumRequestView<'static>>>,
) -> ServiceResult<ServiceStream<RunningSumResponse>> {
    // Map the request stream to a response stream however you like.
    let response_stream = futures::stream::unfold(/* ... */);
    Response::stream_ok(response_stream)
}
```

For bidirectional streams that need true full-duplex behavior (server
emits messages independently of client send rate), use a
`tokio::sync::mpsc` channel: spawn a task that reads from `requests`
and writes to the channel sender, return a
`ReceiverStream` as the response. See `tests/streaming/src/lib.rs`
for an example.

### Calling streaming RPCs from a client

Generated clients expose a method for each RPC. Server streaming
returns a stream you call `.message().await?` on; bidi returns a
handle with `.send(req).await?` and `.message().await?` plus
`.close_send()`:

```rust
// Server streaming
let mut stream = client.range(req).await?;
while let Some(msg) = stream.message().await? {
    // ...
}

// Client streaming - takes a Vec
let resp = client.sum(vec![req1, req2, req3]).await?;

// Bidi
let mut bidi = client.running_sum().await?;
bidi.send(req).await?;
let reply = bidi.message().await?;
bidi.close_send();
```

Both `streaming-tour/src/client.rs` and the eliza example show these
patterns end-to-end.

## Tower middleware

The connect router is a `tower::Service`, so any tower layer composes
on top. The full reference is in
[`examples/middleware/`](../examples/middleware), which uses an
`axum::middleware::from_fn` for bearer-token auth and chains it with
`tower-http`'s `TraceLayer` and `TimeoutLayer`.

### Composing layers

Use `tower::ServiceBuilder` for clear top-to-bottom ordering, mounted
on `axum::Router::layer()` so axum handles the body conversion from
`ConnectRpcBody` to `axum::body::Body`:

```rust
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceBuilder;
use tower_http::{trace::TraceLayer, timeout::TimeoutLayer};

let connect_router = service.register(Router::new());
let tokens = Arc::new(token_table());
let app = axum::Router::new()
    .fallback_service(connect_router.into_axum_service())
    .layer(
        ServiceBuilder::new()
            .layer(TraceLayer::new_for_http())                  // outermost
            .layer(axum::middleware::from_fn_with_state(tokens, auth_middleware))
            .layer(TimeoutLayer::with_status_code(              // innermost
                http::StatusCode::REQUEST_TIMEOUT,
                Duration::from_secs(5),
            )),
    );
```

ServiceBuilder applies layers top-to-bottom: the first `.layer()`
sees requests first (and responses last). A request flows
trace -> auth -> timeout -> dispatcher -> handler.

For auth and similar interceptors, `axum::middleware::from_fn` (or
`from_fn_with_state` for stateful cases) is usually the lightest path
because it lets you write the middleware as a plain async function.
A hand-rolled `tower::Layer` + `tower::Service` pair is also fine
when you need finer control - both produce a `Layer` that
ServiceBuilder accepts.

### Passing data from a layer to a handler

The dispatch path moves the request's `http::Extensions` into
`RequestContext::extensions` verbatim. So a middleware that inserts a value
via `req.extensions_mut().insert(value)` makes that value available to
the handler via `ctx.extensions.get::<T>()`. This is the canonical way
to pass per-request state from middleware (auth identity, trace IDs,
remote addr, TLS peer info) into the handler.

The middleware example does exactly this with a `UserId`:

```rust
// In the auth middleware:
req.extensions_mut().insert(UserId(user.into()));
next.run(req).await

// In the handler:
let user = ctx.extensions.get::<UserId>().unwrap();
```

### Short-circuit responses

A layer can short-circuit by returning a response without invoking
the inner service. The middleware example does this for unauthorized
requests, returning a 401 with a Connect-protocol JSON error body so
clients see the failure on the same code path they use for handler
errors.

## Hosting

### With axum (recommended)

`Router::into_axum_service()` returns a tower service you mount via
`axum::Router::fallback_service`, and `into_axum_router()` returns a
ready-to-merge axum router. This is the common path because it lets
you compose connect RPC routes with regular HTTP routes (health
checks, static files, OAuth callbacks):

```rust
let app = axum::Router::new()
    .route("/health", axum::routing::get(|| async { "OK" }))
    .fallback_service(connect_router.into_axum_service())
    .layer(/* tower layers */);

let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
axum::serve(listener, app).await?;
```

### Standalone server

Enable the `server` feature for a built-in hyper-based server. This
is the no-frills path when you don't need axum's routing or per-route
configuration:

```rust
use connectrpc::Server;

let connect_router = service.register(Router::new());
Server::new(connect_router)
    .serve("127.0.0.1:8080".parse()?)
    .await?;
```

The standalone `Server` handles HTTP/1.1, HTTP/2 with prior knowledge,
and graceful shutdown. It's a single dispatcher with no per-route
configuration, so add things like health endpoints either as RPC
methods or by switching to the axum path.

### TLS

Enable the `server-tls` feature (or the `tls` umbrella feature for
both server and client TLS).

For the standalone `Server`:

```rust
use std::sync::Arc;

let server_config: Arc<rustls::ServerConfig> = /* load PEMs, build config */;

Server::new(connect_router)
    .with_tls(server_config)
    .serve("0.0.0.0:8443".parse()?)
    .await?;
```

For the axum path, `connectrpc::axum::serve_tls` (requires both the
`axum` and `server-tls` features) is a drop-in replacement for
`axum::serve` that owns the rustls accept loop and stamps `PeerAddr` /
`PeerCerts` into request extensions exactly as the standalone `Server`
does, so handler code that reads `ctx.extensions.get::<PeerCerts>()`
is portable across both hosting paths:

```rust
let app = axum::Router::new()
    .route("/health", axum::routing::get(|| async { "OK" }))
    .fallback_service(connect_router.into_axum_service());

let listener = tokio::net::TcpListener::bind("0.0.0.0:8443").await?;
connectrpc::axum::serve_tls(listener, app, server_config)
    .with_graceful_shutdown(shutdown_signal)
    .await?;
```

The eliza example
([`examples/eliza/README.md`](../examples/eliza/README.md)) walks
through generating self-signed certificates with openssl, configuring
mTLS via `--client-ca`, and the rustls strict-PKI requirement that
your CA cert must be distinct from the server leaf cert. The
mtls-identity example
([`examples/mtls-identity/README.md`](../examples/mtls-identity/README.md))
demonstrates `serve_tls` end-to-end with cert-SAN identity extraction
and an ACL keyed on it.

## Clients

Enable the `client` feature for HTTP client support with connection
pooling.

### HttpClient

`HttpClient` is the standard transport built on hyper. Construct one
of two variants: cleartext (`http://` only) or TLS-enabled
(`https://` only):

```rust
use connectrpc::client::HttpClient;

// Cleartext
let http = HttpClient::plaintext();

// TLS - requires client-tls or tls feature
let tls_config: Arc<rustls::ClientConfig> = /* trust store + ALPN */;
let http = HttpClient::with_tls(tls_config);
```

A `plaintext()` client refuses `https://` URIs and a `with_tls()`
client refuses `http://` URIs - this catches misconfiguration loudly
rather than silently downgrading.

### ClientConfig

`ClientConfig` carries the base URI and per-call defaults that apply
to every RPC made with the client:

```rust
use std::time::Duration;
use connectrpc::client::ClientConfig;

let config = ClientConfig::new("http://localhost:8080".parse()?)
    .default_timeout(Duration::from_secs(30))
    .default_header("authorization", "Bearer demo-token")
    .default_header("x-trace-id", "trace-12345");
```

These defaults automatically apply to every call from that client.
Use them for cross-cutting concerns like auth or tracing IDs.

### CallOptions

For per-call overrides, use the `_with_options` method variants and
pass `CallOptions`:

```rust
use connectrpc::client::CallOptions;

let resp = client.greet_with_options(
    GreetRequest { name: "World".into(), ..Default::default() },
    CallOptions::default()
        .with_timeout(Duration::from_secs(5))
        .with_max_message_size(1024 * 1024),
).await?;
```

Per-call options replace config defaults for the fields they set
(timeout here); other defaults (the auth header) still apply.

### Reading the response

Unary responses give you several access patterns:

```rust
let resp = client.greet(req).await?;

// Pattern 1: borrow the view via .view(). Zero-copy. Use this when
// you also need headers/trailers - OwnedView derefs to the view, so
// field access (.greeting -> &str) works directly.
println!("{}", resp.view().greeting);
let _ = resp.headers();
let _ = resp.trailers();

// Pattern 2: consume via .into_view() to get the OwnedView. Still
// zero-copy via Deref, but discards headers/trailers.
let msg = client.greet(req).await?.into_view();
let greeting: &str = msg.greeting;

// Pattern 3: .into_owned() for the prost-style owned struct.
// Allocates and copies all string/bytes fields.
let owned: GreetResponse = client.greet(req).await?.into_owned();
```

### Custom transports

Generated clients are generic over `ClientTransport`, which is auto-
implemented for any `tower::Service` that handles
`http::Request<ClientBody>` and returns `http::Response<B>`. So you
can plug in any tower stack as the transport:

```rust
use tower::ServiceBuilder;
use tower_http::timeout::TimeoutLayer;
use connectrpc::client::{Http2Connection, ServiceTransport};

let conn = Http2Connection::connect_plaintext(uri).await?.shared(1024);
let stacked = ServiceBuilder::new()
    .layer(TimeoutLayer::new(Duration::from_secs(30)))
    .service(conn);

let client = GreetServiceClient::new(
    ServiceTransport::new(stacked),
    config,
);
```

This is also how the wasm example
([`examples/wasm-client/`](../examples/wasm-client)) plugs in a
browser `fetch`-based transport.

## Errors and status codes

`ConnectError` is the error type for both server-returned and
client-observed errors:

```rust
pub struct ConnectError {
    pub code: ErrorCode,
    pub message: Option<String>,
    pub details: Vec<ErrorDetail>,
    pub headers: http::HeaderMap,
    pub trailers: http::HeaderMap,
    // ...
}
```

`ErrorCode` is the canonical Connect/gRPC status set:
`Canceled`, `Unknown`, `InvalidArgument`, `DeadlineExceeded`,
`NotFound`, `AlreadyExists`, `PermissionDenied`, `ResourceExhausted`,
`FailedPrecondition`, `Aborted`, `OutOfRange`, `Unimplemented`,
`Internal`, `Unavailable`, `DataLoss`, `Unauthenticated`.

Construct one with the message:

```rust
return Err(ConnectError::new(
    ErrorCode::PermissionDenied,
    format!("user {user} cannot read {name}"),
));
```

The dispatcher maps each code to the appropriate HTTP status (e.g.
`NotFound` -> 404, `Unauthenticated` -> 401, `PermissionDenied` ->
403) and the appropriate protocol-specific representation. Clients
parse it back into the same `ConnectError` shape regardless of which
protocol they're speaking.

For more structured errors, attach `ErrorDetail` entries (which carry
typed protobuf messages) before returning. These flow through to
clients in the standard Connect error-detail wire format.

## Compression

The runtime ships with gzip, zstd, and identity by default. Servers
advertise supported algorithms in the `accept-encoding` response and
honor the client's `connect-content-encoding` request header (or
`grpc-encoding` for the gRPC protocols).

### Per-RPC compression control

A handler can override the server's compression policy for a single
response via `Response::compress`:

```rust
async fn greet(
    &self,
    _ctx: RequestContext,
    req: OwnedView<GreetRequestView<'static>>,
) -> ServiceResult<GreetResponse> {
    let mut resp = Response::new(/* ... */);
    if response_is_huge() {
        resp = resp.compress(true);  // force compress this response
    }
    Ok(resp)
}
```

### Custom compression algorithms

`CompressionRegistry` is pluggable. Implement `CompressionProvider`
for your algorithm and register it on the dispatcher:

```rust
use connectrpc::{CompressionProvider, CompressionRegistry, ConnectError};
use bytes::Bytes;

struct MyCompression;

impl CompressionProvider for MyCompression {
    fn name(&self) -> &'static str { "my-algo" }

    fn compress(&self, data: &[u8]) -> Result<Bytes, ConnectError> {
        // ...
    }

    fn decompressor<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<Box<dyn std::io::Read + 'a>, ConnectError> {
        // Return a reader that yields decompressed bytes. The framework
        // controls how much is read, so decompression is bounded by
        // ConnectRpcService::max_message_size.
        // ...
    }
}

let registry = CompressionRegistry::default().register(MyCompression);
let service = ConnectRpcService::new(router).with_compression(registry);
```

## Examples directory tour

| Example | What it covers |
|---|---|
| [`streaming-tour/`](../examples/streaming-tour) | All four RPC types (unary, server stream, client stream, bidi) on a trivial NumberService. Smallest demo of handler signatures and client invocation patterns. |
| [`middleware/`](../examples/middleware) | Server-side tower middleware composition: an `axum::middleware::from_fn` bearer-token auth, identity passthrough via `RequestContext::extensions`, response trailers via `Response::with_trailer`. Client demos `ClientConfig::default_header` and `CallOptions::with_timeout`. |
| [`mtls-identity/`](../examples/mtls-identity) | mTLS twin of `middleware/`: axum hosted behind `connectrpc::axum::serve_tls`, identity from the client cert's DNS SAN via `PeerCerts` instead of a bearer token, ACL keyed on the cert-derived identity. In-memory `rcgen` PKI; no PEM files. |
| [`eliza/`](../examples/eliza) | Production-shaped streaming app: a port of the `connectrpc/examples-go` ELIZA demo. Server-streaming Introduce + bidi-streaming Converse, TLS, mTLS, CORS, IPv6, both server and client binaries, interoperates with the hosted Go reference at `demo.connectrpc.com`. |
| [`multiservice/`](../examples/multiservice) | Multiple proto packages compiled together with `buf generate`, multiple services on one server, well-known type usage. |
| [`wasm-client/`](../examples/wasm-client) | Browser fetch transport: same generated client used from `wasm32-unknown-unknown` with a custom `ClientTransport` backed by `web-sys::fetch`. |
| [`bazel/`](../examples/bazel) | Bazel build integration via custom rules. |

Each example has its own README with run instructions.
