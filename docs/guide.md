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
- [Interceptors](#interceptors)
- [Hosting](#hosting)
- [Health checking](#health-checking)
- [Server reflection](#server-reflection)
- [Production hardening](#production-hardening)
- [Clients](#clients)
- [Errors and status codes](#errors-and-status-codes)
- [Compression](#compression)
- [Examples directory tour](#examples-directory-tour)

## Installation

`connectrpc` ships as five crates:

| Crate | Purpose |
|---|---|
| `connectrpc` | Tower-based runtime: server dispatcher, client transports, codec, compression |
| `protoc-gen-connect-rust` (binary, in `connectrpc-codegen`) | `protoc` plugin that generates service stubs |
| `connectrpc-build` | `build.rs` integration that runs the codegen at build time |
| `connectrpc-health` | The standard `grpc.health.v1.Health` service for liveness / readiness probes ([Health checking](#health-checking)) |
| `connectrpc-reflection` | The standard gRPC server reflection service (`grpc.reflection.v1` + `v1alpha`) for `grpcurl` / `buf curl` / Postman / `grpcui` ([Server reflection](#server-reflection)) |

Generated code references a small set of crates from your namespace, so
a working `Cargo.toml` needs more than the runtime itself — this is the
complete dependency block for a typical (JSON-capable) service:

```toml
[dependencies]
connectrpc = "0.8"
buffa = { version = "0.9", features = ["json"] }
buffa-types = { version = "0.9", features = ["json"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"

[build-dependencies]
connectrpc-build = "0.8"
```

The `buffa`/`serde` entries come from buffa's generated message types.
For proto-only builds (no JSON), drop the `json` features and
`serde`/`serde_json` — see
[Generated Code Dependencies](../README.md#generated-code-dependencies)
in the README.

### MSRV

The MSRV is **Rust 1.88**, declared on the workspace and verified in
CI. The crate uses Rust 2024 edition.

### Feature flags

The runtime is feature-gated so you only pay for what you use:

| Feature | Default | What it adds |
|---|---|---|
| `json` | yes | JSON codec for protobuf messages (the proto3-JSON wire format). Disabling it drops the `serde` requirement on message types — see [Proto-only builds](#proto-only-no-json-builds) |
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
connectrpc = { version = "0.8", features = ["axum"] }

# Server + client, both with TLS
connectrpc = { version = "0.8", features = ["axum", "client", "tls"] }

# Built-in server (no axum)
connectrpc = { version = "0.8", features = ["server"] }

# Minimal (wasm-friendly: no networking, no native compression)
connectrpc = { version = "0.8", default-features = false }
```

### Proto-only (no-JSON) builds

The Connect protocol supports two message codecs: binary proto and proto3
JSON. The JSON codec needs every message type to be `serde::Serialize` /
`Deserialize`, which is why the code generator derives those impls by default.
A deployment that only ever speaks binary proto can turn JSON off and shed
those derives — smaller generated code, no `serde_derive` in the message-type
build.

It takes two coordinated settings:

1. **Generate without serde derives.** Pass the `no_json` plugin option (or
   `connectrpc-build`'s [`.generate_json(false)`](#connectrpc-build-build-time-simplest)),
   so message structs are emitted without `#[derive(serde::Serialize,
   serde::Deserialize)]`.
2. **Disable the runtime `json` feature**, which relaxes the message-type
   bounds from `Message + Serialize`/`DeserializeOwned` to just `Message`:

   ```toml
   # Proto-only server: no JSON codec, no serde on message types.
   # `default-features = false` is the only way to drop `json`, so it also drops
   # the default compression features (`gzip`/`zstd`/`streaming`) — re-list the
   # ones you still want.
   connectrpc = { version = "0.8", default-features = false, features = ["server", "gzip", "zstd", "streaming"] }
   ```

With `json` off, the `Message + serde` requirement is replaced by the
`JsonSerialize` / `JsonDeserialize` marker traits, which become empty bounds —
so a serde-free generated type still satisfies every handler, router, and
client signature.

A proto-only server **rejects JSON at content negotiation**, before it touches
the request body: `application/json` and `application/connect+json` (and the
Connect GET `encoding=json` parameter) are unsupported media types, so the
server responds with a bodyless **HTTP 415 Unsupported Media Type** (the client
maps the status to an error code); `application/grpc+json` and
`application/grpc-web+json` get a gRPC error status. Message-level encode/decode
also returns `Unimplemented` as a defense-in-depth backstop. Handler-level
errors (and the streaming end-of-stream frame) remain JSON, as the Connect spec
requires regardless of the request codec. On the client side, the
`ClientConfig::json` shorthand is removed from the API in a proto-only build, so
JSON cannot be selected by mistake.

`connectrpc` itself still depends on `serde` and `serde_json` even in a
proto-only build — the always-JSON error wire format needs them — so they stay
in `cargo tree`. What proto-only mode removes is the serde *derive* on your
generated message types and the per-message JSON (de)serialization paths.

Because `json` is an additive, default-on Cargo feature, it is only truly off
when *every* crate in your dependency graph that depends on `connectrpc`
disables it. If any other crate pulls in `connectrpc` with `json` enabled,
feature unification turns it back on for the whole build, the markers revert to
`Serialize`/`DeserializeOwned`, and your serde-free generated types stop
compiling (`Serialize is not satisfied` — note the error names the trait, not
the feature). Proto-only mode therefore fits a leaf binary or a fully
proto-only graph, not one library inside a mixed workspace.

> View-body responses are already proto-only and return `Unimplemented` for the
> JSON codec — see [Returning a view body](#returning-a-view-body) — so a
> proto-only build changes nothing for them.

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
connectrpc-build = "0.8"
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
use connectrpc::{RequestContext, Response, Router, ServiceRequest, ServiceResult};

pub mod proto {
    connectrpc::include_generated!();
}
use proto::greet::v1::*;

struct MyGreet;

impl GreetService for MyGreet {
    async fn greet(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, GreetRequest>,
    ) -> ServiceResult<GreetResponse> {
        Response::ok(GreetResponse {
            greeting: format!("Hello, {}!", req.name),
            ..Default::default()
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let router = Router::new().add_service(Arc::new(MyGreet));
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

If you need the compiled `FileDescriptorSet` at runtime — most commonly
to feed the [`connectrpc-reflection`](#server-reflection) crate — chain
`.emit_descriptor_set("svc_descriptor.bin")` before `.compile()`. The
name must be a bare file name (no path separators). The set (including
the full transitive import closure) is written to `OUT_DIR` and can be
embedded with
`include_bytes!(concat!(env!("OUT_DIR"), "/svc_descriptor.bin"))`. See
the `Config::emit_descriptor_set` rustdoc for details.

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

Unary handlers take a read-only `RequestContext` plus a borrowed
`ServiceRequest<'_, RequestType>`, and return
`ServiceResult<ResponseType>`:

```rust
impl GreetService for MyGreet {
    async fn greet(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, GreetRequest>,
    ) -> ServiceResult<GreetResponse> {
        // req derefs to the request view: zero-copy field access.
        // String fields are &str borrowed from the request buffer.
        Response::ok(GreetResponse {
            greeting: format!("Hello, {}!", req.name),
            ..Default::default()
        })
    }
}
```

The `ServiceRequest` shape lets handlers read string fields without
allocating - `req.name` is a `&str` directly into the request bytes,
and the borrow may be held across `.await` points. The request is
borrowed from the dispatcher-owned body, so the response (and anything
moved into `tokio::spawn`) cannot borrow from it - call
`.to_owned_message()` to get the owned struct when you need one. The
conversion is infallible: buffa charges every unknown-field record
against the decode-time allowance, so a request that
decoded successfully always re-materializes.

### `RequestContext` and `Response`

Request-side metadata lives on `RequestContext` (passed in);
response-side metadata lives on `Response<B>` (returned):

`RequestContext` is `#[non_exhaustive]`; read it through the accessor
methods (new request-scoped metadata can then be added in minor releases):

| `RequestContext` accessor | Purpose |
|---|---|
| `ctx.header(name)` / `ctx.headers()` | Caller-supplied headers (after protocol-prefix stripping) |
| `ctx.deadline()` | Absolute `Instant` if the caller set a timeout |
| `ctx.time_remaining()` | Saturating `Option<Duration>` until the deadline (`None` when no deadline is set) — budget downstream calls with this |
| `ctx.extensions()` | `http::Extensions` carried from the underlying `http::Request` |
| `ctx.path()` | Requested procedure path (`/package.Service/Method`) from the request URI |
| `ctx.spec()` | Static metadata for the dispatched RPC method ([`Spec`](#static-method-metadata-spec)); `None` only for low-level manual registrations that do not attach one |
| `ctx.protocol()` | The negotiated wire protocol for this request (`Connect` / `Grpc` / `GrpcWeb`) |
| `ctx.peer_addr()` | Remote socket address (requires the `server` feature; `None` when the transport didn't insert it) |
| `ctx.peer_certs()` | TLS client cert chain (requires the `server-tls` feature; `None` for plaintext or no client cert) |

For example, propagating the caller's deadline to a downstream RPC and
reading the peer cert chain:

```rust,ignore
// Budget downstream calls from the remaining time, leaving a margin
// for response encoding and network round-trips.
if let Some(remaining) = ctx.time_remaining() {
    let budget = remaining.saturating_sub(Duration::from_millis(50));
    options = options.with_timeout(budget);
}

// Typed peer lookup — returns None instead of panicking when the
// request didn't arrive over mTLS.
if let Some(certs) = ctx.peer_certs() {
    authorize(certs)?;
}
```

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
    req: ServiceRequest<'_, GreetRequest>,
) -> ServiceResult<GreetResponse> {
    Ok(Response::new(GreetResponse { /* ... */ })
        .with_header("x-greet-version", "v2")
        .with_trailer("x-server-id", "node-7"))
}
```

`RequestContext::extensions()` is the passthrough channel for tower-layer
state: a custom auth layer can stamp a `UserId` into the request's
`http::Extensions`, and the dispatcher forwards that map verbatim into the
request context for the handler to read with
`ctx.extensions().get::<UserId>()`. For the well-known peer types, prefer
the typed `ctx.peer_addr()` / `ctx.peer_certs()` accessors — they return
`None` rather than panicking when the transport didn't insert them. See
[Tower middleware](#tower-middleware) for the full pattern.

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
allocation by returning an `OwnedView` rebuilt zero-copy from the
retained request bytes (the request itself is borrowed and cannot
outlive the call, so it is re-decoded - a Bytes refcount bump plus a
decode walk, with no per-field copy). Codegen emits
`OwnedFooView` aliases and `impl Encodable<Foo> for OwnedFooView` per
RPC type (or, with `encodable_impls=all_messages`, the impls for every
message in the generated crate - the aliases stay RPC-scoped). (When two RPC types in the same package would alias to the
same `OwnedFooView` name — e.g. a local `MyMessage` plus an imported
`api.v1.foo.bar.MyMessage` — the alias is suppressed for both; spell
the inlined `OwnedView<…View<'static>>` form for those types.) `connectrpc::MaybeBorrowed` covers the conditional case:

```rust
use connectrpc::{MaybeBorrowed, RequestContext, Response, ServiceRequest, ServiceResult};
// `Record` and `OwnedRecordView` come from your generated module.

async fn redact(
    &self,
    _ctx: RequestContext,
    req: ServiceRequest<'_, Record>,
) -> ServiceResult<MaybeBorrowed<Record, OwnedRecordView>> {
    if req.email.is_empty() && req.ssn.is_empty() {
        // Pass-through. The response must be 'static, so rebuild an
        // OwnedView from the retained body bytes - zero-copy (Bytes
        // refcount + decode walk), then re-encode via ViewEncode.
        return Response::ok(MaybeBorrowed::Borrowed(req.to_owned_view()));
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
`extern_path` (the impl would be an orphan in the consuming crate) -
the impls must live in the crate that owns the type. If you generate
that crate yourself, regenerate it with `encodable_impls=all_messages`
(see the `protoc-gen-connect-rust` option docs) and views of its types
become returnable from any crate. For types you don't generate (e.g.
well-known types from `buffa-types`), return the owned message or use
`PreEncoded::from_view`.

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

Register generated services from the router so multiple services read
top-to-bottom:

```rust
let router = Router::new()
    .add_service(Arc::new(MyGreet))
    .add_service(Arc::new(MyBilling));
```

The generated `register` extension method remains available when the
inside-out form is useful:

```rust
let router = Arc::new(MyGreet).register(Router::new());
```

To combine routers that were built separately, use `Router::merge` (owned,
chainable), `Router::merge_in_place` (in place), or the `merge_routers` free
function for many at once. Merging two routers that register the same method
path panics by default, so an accidental collision fails loudly at startup;
call `Router::allow_overrides()` first when last-wins replacement is intended:

```rust
let router = defaults.allow_overrides().merge(overrides);
```

When the routers come from dynamic input (a plugin list, config-driven
service set) and a collision should be handled rather than crash the process,
use `Router::try_merge` / `Router::try_merge_in_place`, which return a
`RouterMergeError` listing the conflicting paths instead of panicking.

The router is what you mount on axum (`router.into_axum_router()`)
or pass to the built-in `Server`.

### Testing handlers

Handlers are plain async methods, so unit tests call them directly -
no server, no sockets. Construct the inputs the same way the dispatcher
does:

```rust
use buffa::Message;               // encode_to_vec / decode_from_slice
use buffa::view::HasMessageView;  // GreetRequest::decode_view

#[tokio::test]
async fn greet_uses_the_name() {
    let svc = GreetServiceImpl::default();

    // Unary: encode the request, decode a view over it, wrap the pair.
    let body = Bytes::from(GreetRequest {
        name: "ada".into(),
        ..Default::default()
    }.encode_to_vec());
    let view = GreetRequest::decode_view(&body).unwrap();
    let req = ServiceRequest::<GreetRequest>::from_parts(&view, &body);

    let resp = svc.greet(RequestContext::new(HeaderMap::new()), req)
        .await
        .unwrap();

    // The trait's response body is an opaque `impl Encodable<GreetResponse>`;
    // encode it (exactly what the dispatcher does) and decode to assert on
    // fields. Headers and trailers are directly accessible on `resp`. The
    // UFCS call avoids ambiguity with `buffa::Message::encode`, which is
    // also in scope.
    use connectrpc::Encodable;
    let bytes = Encodable::encode(&resp.body, CodecFormat::Proto).unwrap();
    let reply = GreetResponse::decode_from_slice(&bytes).unwrap();
    assert_eq!(reply.greeting, "Hello, ada!");
}
```

Streaming inputs are one call each: `StreamMessage::from_message(&msg)`
builds an item, and `futures::stream::iter([...])` boxed into an
`InboundStream` builds the request stream:

```rust
let items = [Ok(StreamMessage::from_message(&SumRequest {
    value: Some(3), ..Default::default()
}))];
let requests: InboundStream<SumRequest> =
    Box::pin(futures::stream::iter(items));
let resp = svc.sum(RequestContext::new(HeaderMap::new()), requests).await?;
```

`RequestContext::new` takes the request headers; its `with_*` builders
cover peer identity and other per-call inputs.

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
    req: ServiceRequest<'_, RangeRequest>,
) -> ServiceResult<ServiceStream<RangeResponse>> {
    let stream = futures::stream::iter(/* ... */);
    Response::stream_ok(stream)
}
```

### Client streaming

The handler receives an `InboundStream<Req>` — a `ServiceStream` of
`StreamMessage<Req>` items — and returns a single response. Each item
owns its decoded buffer, is `Send + 'static` (so it can be buffered or
moved into spawned tasks), and exposes zero-copy accessor methods per
field:

```rust
async fn sum(
    &self,
    _ctx: RequestContext,
    mut requests: InboundStream<SumRequest>,
) -> ServiceResult<SumResponse> {
    let mut total: i64 = 0;
    while let Some(req) = requests.next().await {
        total += req?.value().unwrap_or(0) as i64;
    }
    Response::ok(SumResponse { total: Some(total), ..Default::default() })
}
```

The request stream yields `Err(ConnectError)` if the upload fails partway
— a truncated body or broken transport — so a partial stream is not
mistaken for a complete one. The `req?` in the loop above propagates that
error as the RPC's failure, which is the right default for handlers that
aggregate inbound messages. Only a clean `None` means the client finished
the stream.

### Bidirectional streaming

Takes a request stream and returns a response stream. Both sides can
emit messages independently:

```rust
async fn running_sum(
    &self,
    _ctx: RequestContext,
    requests: InboundStream<RunningSumRequest>,
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
// Server streaming. Each item is a `StreamMessage` - the same wrapper
// server handlers receive for inbound streams. Read fields zero-copy
// via `.view()` (or the generated accessor methods), and convert with
// `.to_owned_message()` when you need the owned struct.
let mut stream = client.range(req).await?;
while let Some(msg) = stream.message().await? {
    println!("{}", msg.view().value.unwrap_or_default());
}

// Client streaming - takes an async `Stream` of requests, so messages
// can be produced as they become available without buffering the whole
// upload. A ready collection is adapted with `stream_iter` (a re-export
// of `futures::stream::iter`, so no direct `futures` dependency needed):
let resp = client
    .sum(connectrpc::stream_iter(vec![req1, req2, req3]))
    .await?;

// ...or feed the call from a live producer through a channel-backed
// stream (add the `tokio-stream` crate for the wrapper). The generated
// bound, `ClientRequestStream<T>`, is `Stream<Item = T> + Send + 'static`:
// the stream backs the request body, so yield owned messages rather than
// borrows of local data.
let (tx, rx) = tokio::sync::mpsc::channel(16);
tokio::spawn(async move {
    while let Some(chunk) = source.recv().await {
        if tx.send(request_for(chunk)).await.is_err() {
            break; // call ended — stop producing
        }
    }
});
let resp = client
    .sum(tokio_stream::wrappers::ReceiverStream::new(rx))
    .await?;

// Bidi - received items are `StreamMessage`s too
let mut bidi = client.running_sum().await?;
bidi.send(req).await?;
if let Some(reply) = bidi.message().await? {
    println!("{}", reply.view().total.unwrap_or_default());
}
bidi.close_send();

// For true full duplex, split the bidi stream into independently owned
// halves and drive them from separate tasks. Response-dependent sends
// require an HTTP/2 transport (on HTTP/1.1 no response arrives until the
// upload completes). Dropping the send half ends the upload cleanly;
// dropping the receive half cancels the RPC.
let (mut send, mut recv) = client.running_sum().await?.into_split();
let reader = tokio::spawn(async move {
    while let Some(reply) = recv.message().await? {
        println!("{}", reply.view().total.unwrap_or_default());
    }
    Ok::<_, connectrpc::ConnectError>(())
});
for req in requests {
    send.send(req).await?;
}
send.close_send();
reader.await.expect("reader task")?;
```

`?` on `message()` is the complete error handling: `Ok(None)` means the
server finished cleanly, and a terminal RPC error — including a
gRPC/gRPC-Web stream that ends without a usable `grpc-status` — comes
back as `Err`, sticky across calls. The `error()` and `trailers()`
accessors remain available afterwards for post-hoc inspection.

Dropping a client-streaming call cancels it: the request body is dropped
with the future, so messages the stream had not yet yielded never reach
the server. Wrapping such a call in a `timeout` therefore abandons the
upload rather than truncating it cleanly — drive the call to completion
whenever the request must be delivered.

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

let connect_router = Router::new().add_service(service);
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

The dispatch path moves the request's `http::Extensions` into the
request context verbatim. So a middleware that inserts a value via
`req.extensions_mut().insert(value)` makes that value available to the
handler via `ctx.extensions().get::<T>()`. This is the canonical way to
pass per-request state from middleware (auth identity, trace IDs, remote
addr, TLS peer info) into the handler.

The middleware example does exactly this with a `UserId`:

```rust
// In the auth middleware:
req.extensions_mut().insert(UserId(user.into()));
next.run(req).await

// In the handler:
let user = ctx.extensions().get::<UserId>().unwrap();
```

### Static method metadata (`Spec`)

Handlers and middleware can read which RPC method is being invoked
without re-parsing the request URL. `ctx.spec()` returns an
`Option<Spec>` describing the dispatched method: its fully-qualified
procedure path, message-flow shape, the proto-declared idempotency
contract, and whether the spec came from a server-side dispatcher or a
generated client.

```rust
async fn greet(
    &self,
    ctx: RequestContext,
    req: ServiceRequest<'_, GreetRequest>,
) -> ServiceResult<GreetResponse> {
    if let Some(spec) = ctx.spec() {
        tracing::info_span!(
            "rpc",
            "rpc.system" = "connect_rpc",
            "rpc.service" = spec.service(),
            "rpc.method" = spec.method(),
        );
    }
    // ...
}
```

`ctx.protocol()` is the per-request companion: it returns the negotiated
wire protocol (`Connect`, `Grpc`, or `GrpcWeb`) so an observability
layer can label spans with `rpc.system` correctly. `Spec` carries only
**registration-time** facts that are the same for every request to that
method; per-request state lives on `RequestContext`. This mirrors
`connect-go`'s `Spec` / `Peer` split.

`Spec` is `Copy`, contains only `'static` data, and is `#[non_exhaustive]`
— destructure with a trailing `..`:

```rust
use connectrpc::{Spec, SpecOrigin, StreamType, IdempotencyLevel};

let Spec { procedure, stream_type, origin, idempotency_level, .. } = spec;
```

Code generation also emits a `pub const <SERVICE>_<METHOD>_SPEC: Spec`
per method that you can reference directly without a request in flight —
useful for building static lookup tables, validating routing, or testing:

```rust,ignore
use crate::connect::greet::v1::GREET_SERVICE_GREET_SPEC;

assert_eq!(GREET_SERVICE_GREET_SPEC.procedure, "/greet.v1.GreetService/Greet");
assert_eq!(GREET_SERVICE_GREET_SPEC.stream_type, StreamType::Unary);
assert_eq!(GREET_SERVICE_GREET_SPEC.origin, SpecOrigin::Server);
```

> **Both dispatch paths populate `ctx.spec()`.** A code-generated
> `FooServiceServer<T>` always supplies a `Spec`. The dynamic `Router`
> (used by `FooServiceExt::register(Router)`) does too — the generated
> `register()` chains `.with_spec(SPEC_CONST)` after each route. The
> only handlers that see `ctx.spec() == None` are those registered
> through low-level manual registration without attaching a `Spec`.
> `ctx.path()` is populated unconditionally regardless of dispatch path
> — use it when you only need the procedure name and want to be robust
> to a missing `Spec`.

### Short-circuit responses

A layer can short-circuit by returning a response without invoking
the inner service. The middleware example does this for unauthorized
requests, returning a 401 with a Connect-protocol JSON error body so
clients see the failure on the same code path they use for handler
errors.

## Interceptors

Tower middleware (above) operates on `http::Request` / `http::Response`
— it's the right level for cross-cutting concerns that don't need to
know they're wrapping an RPC: connection-scoped tracing, gzip, raw
header manipulation. **Interceptors** are the typed RPC layer on top: a
single async hook per call that runs after envelope decoding,
decompression, and protocol header parsing, and before the handler.
Interceptors see the resolved [`Spec`](#static-method-metadata-spec),
the parsed headers, the deadline, the negotiated protocol, the request
extensions, and a lazily decoded message body — everything an auth
boundary, span builder, validator, or rate limiter actually wants.

```rust,ignore
use connectrpc::interceptor::{UnaryRequest, UnaryResponse};
use connectrpc::{ConnectError, Interceptor, Next};

struct Logging;

#[connectrpc::async_trait]
impl Interceptor for Logging {
    async fn intercept_unary(
        &self,
        req: UnaryRequest,
        next: Next<'_>,
    ) -> Result<UnaryResponse, ConnectError> {
        let path = req.ctx.path().unwrap_or("<unknown>").to_owned();
        let started = std::time::Instant::now();
        let resp = next.run(req).await;
        tracing::info!(rpc = %path, elapsed = ?started.elapsed(), ok = resp.is_ok());
        resp
    }
}

let server = GreetServiceServer::new(GreetServiceImpl);
let service = ConnectRpcService::new(server).with_interceptor(Logging);
```

Annotate impls with the re-exported `#[connectrpc::async_trait]` — there
is no separate `async-trait` dependency for downstream crates. The
default impls are passthroughs, so you only override the hook you need.
For one-off interceptors, the `unary_interceptor` and
`streaming_interceptor` closure helpers skip the struct boilerplate.

### Ordering and registration

`with_interceptor` registers in **outermost-first** order, matching
`connect-go`'s `WithInterceptors`: the first interceptor registered
sees the request first and the response last.

```text
.with_interceptor(A).with_interceptor(B)

request:   A → B → handler
response:  A ← B ← handler
```

A service with no interceptors registered pays one `is_empty()` branch
on the dispatch path — no per-request allocation, no `Payload`
construction, no `Box`ing.

To share one interceptor instance across several `ConnectRpcService`s
(an auth interceptor whose token cache or rate-limit counter is
process-wide), use `with_interceptor_arc(Arc<dyn Interceptor>)`.
`with_interceptor` allocates a fresh `Arc` per registration;
`with_interceptor_arc` accepts the one you already hold.

### Reading and rewriting the request

`UnaryRequest` is `{ ctx: RequestContext, payload: Payload }`. Mutating
`ctx` (headers, extensions) before `next.run` propagates to the handler.
The `payload` is the request body — wire bytes plus a lazy decode
cache. Most interceptors never read it; ones that do call
`payload.message::<M>()` to decode once and cache, so the handler's
decode is free:

```rust,ignore
async fn intercept_unary(
    &self,
    mut req: UnaryRequest,
    next: Next<'_>,
) -> Result<UnaryResponse, ConnectError> {
    // Decode once; the handler reuses this decode via the Payload cache.
    let body = req.payload.message::<GreetRequest>()?;
    if body.name.is_empty() {
        return Err(ConnectError::invalid_argument("name is required"));
    }
    // Replace the body — the handler sees the replacement.
    let mut rewritten = body.clone();
    rewritten.name = rewritten.name.trim().to_owned();
    req.payload.set_message(rewritten);
    next.run(req).await
}
```

### Short-circuiting

Returning without calling `next.run()` short-circuits the chain —
neither inner interceptors nor the handler run. Returning `Err`
surfaces the error on the protocol's normal error path, including
any `response_headers` the error carries:

```rust,ignore
async fn intercept_unary(
    &self,
    req: UnaryRequest,
    next: Next<'_>,
) -> Result<UnaryResponse, ConnectError> {
    let Some(token) = req.ctx.header("authorization") else {
        let mut err = ConnectError::unauthenticated("missing bearer token");
        err.response_headers_mut().insert(
            http::header::WWW_AUTHENTICATE,
            http::HeaderValue::from_static("Bearer"),
        );
        return Err(err);
    };
    self.tokens.verify(token)?;
    next.run(req).await
}
```

### Streaming RPCs

`intercept_streaming` covers server-streaming, client-streaming, and
bidi with one `Stream`-shaped hook. It runs once at stream
establishment — before any messages flow — and receives an inbound
`PayloadStream` plus a `NextStream<'_>` continuation. The returned
`StreamResponse` carries the outbound `PayloadStream` and response
metadata.

```rust,ignore
use connectrpc::interceptor::{StreamRequest, StreamResponse};
use connectrpc::{Interceptor, NextStream, PayloadStream};

#[connectrpc::async_trait]
impl Interceptor for AuthInterceptor {
    async fn intercept_streaming(
        &self,
        req: StreamRequest,
        inbound: PayloadStream,
        next: NextStream<'_>,
    ) -> Result<StreamResponse, ConnectError> {
        // Auth runs once at establishment, not per message.
        self.check(&req.ctx)?;
        let resp = next.run(req, inbound).await?;
        Ok(resp.with_header("x-served-by", &self.node_id))
    }
}
```

To observe or transform individual messages, wrap `inbound` (or the
returned `resp.body`) with a `futures::Stream` adapter — `.map()`,
`.then()`, `.filter()`. There is no per-message `send()` call site to
hook because Rust handlers *return* a `Stream`, they don't *push* into
a connection. This is the same shape `tower`, `tonic`, and `axum` use
for body interception. Cross-stream coordination (deciding on an
outbound item based on what was observed inbound) needs shared state
captured by both adapter closures (`Arc<Mutex<..>>`); this is rare —
most interceptors observe one direction or none.

For server-streaming the inbound stream yields exactly one item; for
client-streaming the outbound stream yields exactly one item. Read
`req.ctx.spec().map(|s| s.stream_type)` to branch on cardinality.

### Interceptors vs. Tower middleware

| | Tower middleware | Interceptor |
|---|---|---|
| Operates on | `http::Request` / `http::Response` | Decoded RPC: `Spec`, headers, deadline, `Payload` |
| Runs | Before envelope decode + protocol parse | After envelope decode + protocol parse |
| Sees the RPC method | No (must re-parse the URI) | Yes (`ctx.path()`, `ctx.spec()`) |
| Sees the message body | Compressed/enveloped wire bytes | Lazily decoded, codec-aware `Payload` |
| Short-circuits | By returning an `http::Response` | By returning `Err` or a `UnaryResponse` |
| Best for | gzip, raw header rewriting, generic HTTP concerns | Auth, RPC-aware tracing, validation, rate limiting |

Both compose: a Tower layer wraps the whole `ConnectRpcService`
(including its interceptor chain). An interceptor that needs an
HTTP-level fact (e.g. the remote socket address) reads it from
`ctx.extensions()` after a Tower layer inserts it.

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

let connect_router = Router::new().add_service(service);
Server::new(connect_router)
    .serve("127.0.0.1:8080".parse()?)
    .await?;
```

The standalone `Server` handles HTTP/1.1, HTTP/2 with prior knowledge,
and graceful shutdown. It's a single dispatcher with no per-route
configuration, so add things like health endpoints either as RPC
methods or by mounting the Connect service in axum.

For connection and HTTP/2 settings that `Server` does not expose, drop
down to raw hyper instead.

### Advanced transport configuration

The built-in `Server` exposes the common connection knobs, but it does
not try to mirror every hyper option. For long-tail transport tuning —
flow-control windows, HPACK table size, frame size, or exact keepalive
behavior — drive the Connect service from your own hyper accept loop.

Add `hyper-util` as a direct dependency with the `server-auto`,
`service`, and `tokio` features enabled. Then wrap `ConnectRpcService`
with `TowerToHyperService` before handing each connection to hyper's
auto builder:

```rust,ignore
use connectrpc::{ConnectRpcService, Router};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as AutoBuilder,
    service::TowerToHyperService,
};

let connect_router = Router::new().add_service(greeter_service);
let connect_service = ConnectRpcService::new(connect_router);

let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
let mut builder = AutoBuilder::new(TokioExecutor::new());
builder
    .http2()
    .max_concurrent_streams(1_000)
    .max_frame_size(1 << 20)
    .adaptive_window(true);

loop {
    let (stream, _peer_addr) = listener.accept().await?;
    let conn = builder
        .serve_connection(
            TokioIo::new(stream),
            TowerToHyperService::new(connect_service.clone()),
        )
        .into_owned();

    tokio::spawn(async move {
        if let Err(err) = conn.await {
            eprintln!("connection ended with error: {err}");
        }
    });
}
```

This is the escape hatch for connection- and protocol-level settings.
Axum remains the better fit for routing, health checks, ordinary HTTP
endpoints, and request-level Tower middleware such as auth, timeouts,
or rate limiting.

Unlike the built-in `Server` and `connectrpc::axum::serve_tls`, a raw
hyper loop does not automatically insert `PeerAddr` or `PeerCerts` into
request extensions. If handlers call `ctx.peer_addr()` or
`ctx.peer_certs()`, insert those extensions in your own Tower layer or
service wrapper before the request reaches `ConnectRpcService`.

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
does, so handler code that reads `ctx.peer_certs()` is portable across
both hosting paths:

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

## Health checking

The `connectrpc-health` crate implements the standard
`grpc.health.v1.Health` service. Mount it on your Connect router and
clients like `grpc_health_probe`, kubelet's `grpc:` probe, and gRPC-aware
service meshes (Linkerd, Istio) just work.

This is the gRPC protocol — different from the plain HTTP `GET /health`
route shown earlier in the [Hosting](#hosting) section. Keep the HTTP
route for `httpGet:` probes; add the gRPC service for `grpc:` probes.

```toml
[dependencies]
connectrpc = { version = "0.8", features = ["server"] }
connectrpc-health = "0.8"
```

```rust,no_run
use connectrpc::Router;
use connectrpc_health::{install_static, Status};

// `install_static` registers every name with `Status::Serving`; use the
// generated `*_SERVICE_NAME` constants from your service stubs so the
// registered name matches exactly what clients ask for. The
// whole-process `""` entry is seeded for you, so probes that don't
// pass a service name also work.
let (router, health) = install_static(Router::new(), [
    proto::greet::v1::GREET_SERVICE_SERVICE_NAME,
]);

// Flip status when something goes wrong. `set_status` errors on an
// unknown name, so typos surface immediately instead of silently
// shadowing the real entry.
health
    .set_status(proto::greet::v1::GREET_SERVICE_SERVICE_NAME, Status::NotServing)
    .expect("registered above");

// At shutdown, drain. `shutdown()` flips every registered service,
// including the empty whole-process entry:
health.shutdown();
```

For custom logic (e.g. report `NotServing` while a database connection
is down), implement the `Checker` trait directly and wrap it in
`HealthService::new(...)` or `HealthService::from_arc(...)`. The default
`Checker::watch` body returns `Unimplemented`, which is fine for
Check-only probes; override it if your probes call Watch.

The `HealthClient` (for in-process probes, integration tests, sidecar
tooling) is gated on a `client` Cargo feature that is **on by default**.
Server-only deployments turn it off:

```toml
[dependencies]
connectrpc = { version = "0.8", features = ["server"] }
connectrpc-health = { version = "0.8", default-features = false }
```

That drops `connectrpc/client` (the HTTP/2 transport stack) from the
dependency graph entirely. `use connectrpc_health::HealthClient` then
becomes an unresolved import, but the binary stays lean.

**Unknown services on `Watch`.** Non-empty unregistered services return
`Err(ConnectError::not_found(_))` from both `Check` and `Watch`; the
empty service auto-subscribes on `Watch` and returns `Serving` on
`Check` by default. The gRPC Health spec additionally describes a
`SERVICE_UNKNOWN` keep-stream-open flow for `Watch` that this crate
does not implement, matching the Go `connectrpc.com/grpchealth`
reference. Every probe that treats any error as a failure — kubelet's
`grpc:` probe, `grpc_health_probe`, Linkerd, Istio — works unchanged.
See `HealthService`'s `# Unknown services` section in the crate docs
for the full context.

## Server reflection

The `connectrpc-reflection` crate implements the standard gRPC server
reflection service (`grpc.reflection.v1` and its `v1alpha` predecessor),
so schema-aware clients — `grpcurl`, `buf curl`, Postman, `grpcui` —
can discover and call your services without local proto files, over
gRPC, gRPC-Web, and the Connect protocol alike.

```toml
[dependencies]
connectrpc = { version = "0.8", features = ["server"] }
connectrpc-reflection = "0.8"
```

Emit a descriptor set from your build script (see
[Code generation](#code-generation)), embed it, and mount the service:

```rust,ignore
// build.rs: .emit_descriptor_set("app.fds.bin") before .compile()
use connectrpc::Router;
use connectrpc_reflection::{Reflector, install};

let bytes: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/app.fds.bin"));
let reflector = Reflector::from_descriptor_set_bytes(bytes)?;
// `router` is your service router from `register()`.
let router = install(router, reflector); // mounts v1 + v1alpha
```

Alternatively, when your buffa codegen has reflection enabled, serve
straight from the generated package's descriptor pool with
`Reflector::from_descriptor_pool(proto::descriptor_pool().clone())` —
no build-script step. The bytes path answers with the compiler's
original per-file descriptor bytes; the pool path re-encodes
(semantically faithful, unknown fields preserved). See the `Reflector`
crate docs for the trade-off.

> **Reflection intentionally publishes your schema.** Everything in
> the descriptor set is exposed — all files, their transitive imports,
> and every compiled service, whether or not its handlers are mounted.
> Gate or omit the service on deployments where that is not wanted.

Two more behaviors worth knowing before deploying:

- **`Reflector::with_services` curates the advertised list** (the
  override is verbatim, like Go `grpcreflect`'s `Namer`) — use it when
  the descriptor set compiles in more services than you want to
  advertise; `Reflector::service_names` inspects the current list.
- **The service is self-describing**: queries about `grpc.reflection.*`
  fall back to the crate's own descriptors and `ListServices` includes
  the reflection services, matching grpc-go. Schema-free callers like
  `buf curl` need this to invoke `ServerReflectionInfo` at all.

The [multiservice example](../examples/multiservice) mounts reflection
with both descriptor sources (selectable via `REFLECTION_SOURCE=fds|pool`),
and [`reflection-demo.sh`](../examples/multiservice/reflection-demo.sh)
walks through discovery and schema-free calls with `buf curl`.

## Production hardening

### Deadline policy

Connect and gRPC clients send a per-request timeout header
(`Connect-Timeout-Ms` or `grpc-timeout`). With no policy, the server
trusts that value verbatim: a `Connect-Timeout-Ms: 1` request cancels
the handler mid-write, while a `Connect-Timeout-Ms: 86400000` request
holds a worker for 24 hours, and a request with no timeout header runs
unbounded. `DeadlinePolicy` gives the server the say.

```rust,ignore
use connectrpc::{ConnectRpcService, DeadlinePolicy};
use std::time::Duration;

let policy = DeadlinePolicy::new()
    .with_min(Duration::from_millis(5))           // floor: reject "cancel me instantly"
    .with_max(Duration::from_secs(30))            // cap: bound worker lifetime
    .with_default_timeout(Duration::from_secs(10)) // applied when client asserts nothing
    .with_enforce_on_streams(true);               // also cut off streaming bodies

let service = ConnectRpcService::new(router)
    .with_deadline_policy(policy);
// or: Server::new(router).with_deadline_policy(policy)
```

Why each knob:

- **`with_max`** is the most important one for any service that accepts
  untrusted callers — without it a client controls how long a worker
  stays busy. Set it to your longest acceptable request — for unary and
  server-streaming RPCs the capped budget covers receiving the request
  body as well as handler execution, so size it for uploads, not just
  handler runtime.
- **`with_default_timeout`** matters because the timeout header is
  optional. A request that omits it has no bound at all unless you set
  one. Set it to your SLA. For unary and server-streaming RPCs, the
  budget includes receiving the request body as well as handler execution.
- **`with_min`** protects against a misbehaving or adversarial client
  cancelling the handler before it can do anything (e.g. mid-write on a
  streaming response). A few milliseconds is usually enough.
- **`with_enforce_on_streams(true)`** closes the streaming-body gap.
  By default the deadline only bounds the time-to-first-response —
  once a server- or bidi-streaming handler returns its stream, the
  items flow unbounded. Enabling this wraps the response body so the
  next item after the deadline is a `deadline_exceeded` error and the
  stream ends. Cancellation drops the inner stream at the next yield
  point with no grace period; spawn commit-critical work off the
  request future if it must outlive the caller.
- **`with_inter_message_timeout(d)`** detects stalled streams (a
  handler waiting on a slow upstream). Independent of
  `with_enforce_on_streams` — takes effect whenever set, with or
  without the absolute deadline. Arms when the response stream is
  first polled (stream-setup latency before that is not counted) and
  resets on each yielded item.

`DeadlinePolicy::new()` with no `with_*` calls is a no-op that
preserves the prior default behavior. Existing services see no change
without opting in.

When a client value is clamped, a `tracing::debug!` event fires on
target `connectrpc::deadline` with the path and before/after
durations. Enable `RUST_LOG=connectrpc::deadline=debug` to spot
misbehaving clients.

Inside a handler, `ctx.deadline()` reflects the *moderated* value
(after clamping), so the handler can budget downstream calls —
propagate the remaining time minus a margin as the timeout for
outbound RPCs. `ctx.time_remaining()` does the subtraction for you
(`None` when the request has no deadline):

```rust,ignore
if let Some(remaining) = ctx.time_remaining() {
    options = options.with_timeout(remaining.saturating_sub(margin));
}
```

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

### Connection-establishment bounds and keep-alive

Both `HttpClient` and `Http2Connection` bound connection establishment by default: a 20-second wall-clock budget on the whole DNS + TCP + TLS chain (`DEFAULT_ESTABLISHMENT_TIMEOUT`) with an additional 5-second per-address TCP bound (`DEFAULT_TCP_CONNECT_TIMEOUT`). Exceeding either surfaces as `ErrorCode::Unavailable`, so a server that accepts the TCP connection but stalls the TLS handshake cannot park `poll_ready` indefinitely. To adjust or opt out, use the `builder()` entry point on either transport:

```rust
use std::time::Duration;
use connectrpc::client::Http2Connection;

let conn = Http2Connection::builder()
    .establishment_timeout(Duration::from_secs(10))
    .keep_alive_interval(Duration::from_secs(30))
    .keep_alive_while_idle(true)
    .connect_tls(uri, tls_config)
    .await?;
```

`Http2ConnectionBuilder` also proxies hyper's HTTP/2 keep-alive and flow-control knobs (`keep_alive_interval`, `keep_alive_timeout`, `keep_alive_while_idle`, `initial_stream_window_size`, `initial_connection_window_size`, `adaptive_window`), with a `TokioTimer` pre-wired so the keep-alive setters work without further plumbing. The `h2_settings(|b| ...)` escape hatch exposes the underlying hyper builder for knobs not surfaced directly. To restore the unbounded pre-0.8.0 behaviour, chain `.no_establishment_timeout().no_tcp_connect_timeout()`.

### ClientConfig

`ClientConfig` carries the base URI and per-call defaults that apply
to every RPC made with the client:

```rust
use std::time::Duration;
use connectrpc::client::ClientConfig;

let config = ClientConfig::new("http://localhost:8080".parse()?)
    .with_default_timeout(Duration::from_secs(30))
    .with_default_header("authorization", "Bearer demo-token")
    .with_default_header("x-trace-id", "trace-12345");
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

// Pattern 1: borrow the view via .view(). Zero-copy. Field access
// (.greeting -> &str) works directly on the returned view, and the
// response handle keeps headers/trailers available alongside it.
println!("{}", resp.view().greeting);
let _ = resp.headers();
let _ = resp.trailers();

// Pattern 2: consume via .into_view() to get the OwnedView. Still
// zero-copy - read fields through .reborrow() - but discards
// headers/trailers.
let msg = client.greet(req).await?.into_view();
let greeting: &str = msg.reborrow().greeting;

// Pattern 3: .into_owned() for the prost-style owned struct.
// Allocates and copies all string/bytes fields.
let owned: GreetResponse = client.greet(req).await?.into_owned();

// Pattern 4: .into_owned_parts() when you need the owned struct AND
// the response metadata - the metadata-preserving form of Pattern 3.
let (headers, owned, trailers) = client.greet(req).await?.into_owned_parts();
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
    // response headers and trailers: private, exposed via the
    // response_headers()/trailers() accessors and their _mut variants
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
    req: ServiceRequest<'_, GreetRequest>,
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
        // Limits::max_message_size.
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
| [`middleware/`](../examples/middleware) | Server-side tower middleware composition: an `axum::middleware::from_fn` bearer-token auth, identity passthrough via `RequestContext::extensions()`, response trailers via `Response::with_trailer`. Client demos `ClientConfig::with_default_header` and `CallOptions::with_timeout`. |
| [`mtls-identity/`](../examples/mtls-identity) | mTLS twin of `middleware/`: axum hosted behind `connectrpc::axum::serve_tls`, identity from the client cert's DNS SAN via `PeerCerts` instead of a bearer token, ACL keyed on the cert-derived identity. In-memory `rcgen` PKI; no PEM files. |
| [`eliza/`](../examples/eliza) | Production-shaped streaming app: a port of the `connectrpc/examples-go` ELIZA demo. Server-streaming Introduce + bidi-streaming Converse, TLS, mTLS, CORS, IPv6, both server and client binaries, interoperates with the hosted Go reference at `demo.connectrpc.com`. |
| [`multiservice/`](../examples/multiservice) | Multiple proto packages compiled together with `buf generate`, multiple services on one server, well-known type usage, and server reflection mounted from both descriptor sources (`REFLECTION_SOURCE=fds\|pool`; see `reflection-demo.sh`). |
| [`wasm-client/`](../examples/wasm-client) | Browser fetch transport: same generated client used from `wasm32-unknown-unknown` with a custom `ClientTransport` backed by `web-sys::fetch`. |
| [`bazel/`](../examples/bazel) | Bazel build integration via custom rules. |

Most examples have their own README with run instructions; the rest
document themselves through their `test.sh` / demo scripts.
