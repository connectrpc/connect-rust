# connectrpc

[![crates.io](https://img.shields.io/crates/v/connectrpc.svg)](https://crates.io/crates/connectrpc)
[![docs.rs](https://img.shields.io/docsrs/connectrpc)](https://docs.rs/connectrpc)
[![CI](https://github.com/anthropics/connect-rust/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/anthropics/connect-rust/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/crates/msrv/connectrpc)](Cargo.toml)
[![deps.rs](https://deps.rs/repo/github/anthropics/connect-rust/status.svg)](https://deps.rs/repo/github/anthropics/connect-rust)
[![License](https://img.shields.io/crates/l/connectrpc)](LICENSE)

A [Tower](https://docs.rs/tower/latest/tower/)-based Rust implementation of [ConnectRPC](https://connectrpc.com/), serving Connect, gRPC, and gRPC-Web clients over HTTP with binary or JSON protobuf messages.

**Status:** pre-1.0. The API surface is settling but may shift in 0.x. Production-quality runtime: passes the full ConnectRPC conformance suite — 3,600 server and 6,872 client tests across the three protocols.

**MSRV:** Rust 1.88 (declared on the workspace, verified in CI).

**Documentation:**

- [User guide](docs/guide.md) - long-form coverage of installation, code generation, server/client usage, streaming, tower middleware, TLS, and errors.
- [`examples/`](examples/) - runnable end-to-end examples (streaming, tower middleware, TLS, multi-service, browser/wasm, Bazel).
- [docs.rs](https://docs.rs/connectrpc) - API reference.

## Overview

connectrpc provides:

- **`connectrpc`** — A Tower-based runtime library implementing the Connect protocol
- **`protoc-gen-connect-rust`** — A `protoc` plugin that generates service traits, clients, and message types
- **`connectrpc-build`** — `build.rs` integration for generating code at build time
- **`connectrpc-health`** — The standard `grpc.health.v1.Health` service, for `grpc_health_probe` / kubelet gRPC probes / service-mesh health checks
- **`connectrpc-reflection`** — The standard gRPC server reflection service (`grpc.reflection.v1` + `v1alpha`), so `grpcurl`, `buf curl`, Postman, and `grpcui` can discover and call your services

The runtime is built on [`tower::Service`](https://docs.rs/tower/latest/tower/trait.Service.html), making it framework-agnostic. It integrates with any tower-compatible HTTP framework including [Axum](https://docs.rs/axum), [Hyper](https://docs.rs/hyper), and others.

## Quick Start

### Define your service

```protobuf
// greet.proto
syntax = "proto3";
package greet.v1;

service GreetService {
  rpc Greet(GreetRequest) returns (GreetResponse);
}

message GreetRequest {
  string name = 1;
}

message GreetResponse {
  string greeting = 1;
}
```

### Generate Rust code

Two workflows are supported. Both produce the same runtime API; pick the one
that fits your build pipeline.

#### Option A - `buf generate` (recommended for checked-in code)

Runs two codegen plugins (`protoc-gen-buffa` for message types,
`protoc-gen-connect-rust` for service stubs) and `protoc-gen-buffa-packaging`
twice to assemble the `mod.rs` module tree for each output directory. The
codegen plugins are invoked per-file; only the packaging plugin needs
`strategy: all`.

##### Installing the plugins

`protoc-gen-buffa` and `protoc-gen-buffa-packaging` ship from the
[`buffa`](https://github.com/anthropics/buffa) repo - see its release
page for binaries or `cargo install`.

For `protoc-gen-connect-rust`, three options:

**1. Download a pre-built binary from the GitHub release.** Releases
ship Linux (x86_64, aarch64), macOS (x86_64, aarch64), and Windows
(x86_64) binaries, each with a SHA-256 checksum, a Sigstore signature
(`.sig` + `.pem`), and a GitHub-native build provenance attestation.

```sh
VERSION=v0.7.0
PLATFORM=linux-x86_64        # or darwin-aarch64, etc.
BASE=https://github.com/anthropics/connect-rust/releases/download/${VERSION}
BIN=protoc-gen-connect-rust-${VERSION}-${PLATFORM}

curl -fSL -o "${BIN}"        "${BASE}/${BIN}"
curl -fSL -o "${BIN}.sig"    "${BASE}/${BIN}.sig"
curl -fSL -o "${BIN}.pem"    "${BASE}/${BIN}.pem"
curl -fSL -o checksums-sha256.txt "${BASE}/checksums-sha256.txt"

# Verify the checksum.
grep " ${BIN}\$" checksums-sha256.txt | sha256sum -c -

# Verify the GitHub-native attestation (no .sig/.pem download needed).
gh attestation verify "${BIN}" --repo anthropics/connect-rust

# Or verify the cosign signature directly.
cosign verify-blob \
  --certificate "${BIN}.pem" \
  --signature "${BIN}.sig" \
  --certificate-identity "https://github.com/anthropics/connect-rust/.github/workflows/release.yml@refs/tags/${VERSION}" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  "${BIN}"

install -m 0755 "${BIN}" /usr/local/bin/protoc-gen-connect-rust
```

**2. Build from source via cargo.** Pulls the latest published
`connectrpc-codegen` crate from crates.io and installs the binary into
`$CARGO_HOME/bin`:

```sh
cargo install --locked connectrpc-codegen
```

**3. Buf Schema Registry remote plugin (planned).** Once accepted upstream
the plugin will be runnable as `remote: buf.build/anthropics/connect-rust`
in `buf.gen.yaml`, with no local install step.

```yaml
# buf.gen.yaml
version: v2
plugins:
  - local: protoc-gen-buffa
    out: src/generated/buffa
    opt: [views=true, json=true]
  - local: protoc-gen-buffa-packaging
    out: src/generated/buffa
    strategy: all
  - local: protoc-gen-connect-rust
    out: src/generated/connect
    opt: [buffa_module=crate::proto]
  - local: protoc-gen-buffa-packaging
    out: src/generated/connect
    strategy: all
    opt: [filter=services]
```

```rust
// src/lib.rs
#[path = "generated/buffa/mod.rs"]
pub mod proto;
#[path = "generated/connect/mod.rs"]
pub mod connect;
```

`buffa_module=crate::proto` tells the service-stub generator where you
mounted the buffa output. For a method input type `greet.v1.GreetRequest`
it emits `crate::proto::greet::v1::GreetRequest` - the `crate::proto` root
you named, then the proto package as nested modules, then the type. The
second packaging invocation uses `filter=services` so the connect tree's
`mod.rs` only `include!`s files that actually have service stubs in them.
Changing the mount point requires regenerating.

> The underlying option is `extern_path=.=crate::proto` - same format the
> Buf Schema Registry uses when generating Cargo SDKs. `buffa_module=X`
> is shorthand for the `.` catch-all case. Any module an `extern_path`
> points at must be buffa-generated code from buffa 0.7.0 or newer with
> views enabled (buffa-types 0.7+ for the well-known types): the service
> stubs rely on the `HasMessageView` impls and owned-view wrappers that
> buffa generates alongside each message, just as they rely on the JSON
> serialization impls.

#### Option B - `build.rs` (generated at build time)

Unified output: message types and service stubs in one file per proto,
assembled via a single `include!`. No plugin binaries required at build time.

```toml
[build-dependencies]
connectrpc-build = "0.7"
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

```rust
// lib.rs
pub mod proto {
    connectrpc::include_generated!();
}
```

### Implement the server

```rust
use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult};

struct MyGreetService;

impl GreetService for MyGreetService {
    async fn greet(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GreetRequest>,
    ) -> ServiceResult<GreetResponse> {
        // `request` derefs to the view — string fields are borrowed `&str`
        // directly from the request buffer (zero-copy). The borrow lives for
        // the duration of the call; use `request.to_owned_message()` for
        // anything that must outlive it (e.g. `tokio::spawn`).
        Response::ok(GreetResponse {
            greeting: format!("Hello, {}!", request.name),
            ..Default::default()
        })
    }
}
```

### With Axum (recommended)

```rust
use axum::{Router, routing::get};
use connectrpc::Router as ConnectRouter;
use std::sync::Arc;

let service = Arc::new(MyGreetService);
let connect = service.register(ConnectRouter::new());

// Plain HTTP liveness probe for `kubectl`'s httpGet style. For the
// standard gRPC Health protocol (grpc_health_probe, kubelet `grpc:`
// probes), mount `connectrpc_health::HealthService` on the Connect
// router instead — see docs/guide.md#health-checking.
let app = Router::new()
    .route("/health", get(|| async { "OK" }))
    .fallback_service(connect.into_axum_service());

let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
axum::serve(listener, app).await?;
```

### Standalone server

For simple cases, enable the `server` feature for a built-in hyper server:

```rust
use connectrpc::{Router, Server};
use std::sync::Arc;

let service = Arc::new(MyGreetService);
let router = service.register(Router::new());

Server::new(router).serve("127.0.0.1:8080".parse()?).await?;
```

### Client

Enable the `client` feature for HTTP client support with connection pooling:

```rust
use connectrpc::client::{HttpClient, ClientConfig};

let http = HttpClient::plaintext();  // cleartext http:// only; use with_tls() for https://
let config = ClientConfig::new("http://localhost:8080".parse()?);
let client = GreetServiceClient::new(http, config);

let response = client.greet(GreetRequest {
    name: "World".into(),
}).await?;
```

### Per-call options and client-wide defaults

Generated clients expose both a no-options convenience method and a
`_with_options` variant for per-call control (timeout, headers, max
message size, compression override):

```rust
use connectrpc::client::CallOptions;
use std::time::Duration;

// Per-call timeout
let response = client.greet_with_options(
    GreetRequest { name: "World".into() },
    CallOptions::default().with_timeout(Duration::from_secs(5)),
).await?;
```

For options you want on *every* call (e.g. auth headers, a default
timeout), set them on `ClientConfig` instead — the no-options method
picks them up automatically:

```rust
let config = ClientConfig::new("http://localhost:8080".parse()?)
    .with_default_timeout(Duration::from_secs(30))
    .with_default_header("authorization", "Bearer ...");

let client = GreetServiceClient::new(http, config);

// Uses the 30s timeout and auth header without repeating them:
let response = client.greet(request).await?;
```

Per-call `CallOptions` override config defaults (options win).

### Streaming, interceptors, middleware, TLS

The Quick Start above shows the unary path. For everything else, see the user guide and the focused examples:

- **Streaming RPCs** (server, client, bidi) - see [docs/guide.md#streaming-rpcs](docs/guide.md#streaming-rpcs) and [`examples/streaming-tour/`](examples/streaming-tour) for all four RPC types side-by-side.
- **Interceptors** (typed, async per-RPC middleware for unary and streaming calls) - see [docs/guide.md#interceptors](docs/guide.md#interceptors). Interceptors see the resolved `Spec`, headers, deadline, and a lazily decoded message body, and can rewrite or short-circuit the call - the equivalent of `connect-go`'s `WithInterceptors`.
- **Tower middleware on the server** (gzip, raw header rewriting, generic HTTP concerns below the RPC layer) - see [docs/guide.md#tower-middleware](docs/guide.md#tower-middleware) and [`examples/middleware/`](examples/middleware) for a custom auth layer that stamps caller identity into request extensions.
- **TLS / mTLS** - see [docs/guide.md#tls](docs/guide.md#tls) and [`examples/eliza/README.md`](examples/eliza/README.md) for cert generation and `Server::with_tls` / `HttpClient::with_tls` patterns.
- **gRPC health checking** (`grpc.health.v1.Health`, used by `grpc_health_probe`, kubelet `grpc:` probes, and service meshes) - see [docs/guide.md#health-checking](docs/guide.md#health-checking) and the [`connectrpc-health`](connectrpc-health/) crate.
- **gRPC server reflection** (`grpc.reflection.v1` + `v1alpha`, used by `grpcurl`, `buf curl`, Postman, and `grpcui`) - see the [`connectrpc-reflection`](connectrpc-reflection/) crate, and run [`examples/multiservice/reflection-demo.sh`](examples/multiservice/reflection-demo.sh) for a `buf curl` walkthrough against a live server.

## Feature Flags

| Feature      | Default | Description                                      |
| ------------ | ------- | ------------------------------------------------ |
| `json`       | Yes     | JSON codec for protobuf messages. Disable (with codegen `no_json`) for proto-only builds — see [Proto-only builds](#proto-only-no-json-builds) |
| `gzip`       | Yes     | Gzip compression via flate2                      |
| `zstd`       | Yes     | Zstandard compression via zstd                   |
| `streaming`  | Yes     | Streaming compression via async-compression      |
| `client`     | No      | HTTP client transports (plaintext)               |
| `client-tls` | No      | TLS for client transports (`HttpClient::with_tls`, `Http2Connection::connect_tls`) |
| `server`     | No      | Standalone hyper-based server                    |
| `server-tls` | No      | TLS for the built-in server (`Server::with_tls`) |
| `tls`        | No      | Convenience: enables both `server-tls` + `client-tls` |
| `axum`       | No      | Axum framework integration                       |

### wasm32

The core crate compiles for `wasm32-unknown-unknown`. Generated clients are generic over `ClientTransport`, so they work on wasm with a custom transport (e.g. `web-sys::fetch`). The `client`/`server`/`tls` features require platform networking and `zstd` requires native C compilation. See [`examples/wasm-client`](examples/wasm-client) for a complete Fetch-based transport.

```toml
[dependencies]
connectrpc = { version = "0.7", default-features = false, features = ["gzip"] }
```

### Minimal build (no compression)

```toml
[dependencies]
connectrpc = { version = "0.7", default-features = false }
```

### Proto-only (no-JSON) builds

A deployment that only speaks binary proto can drop the JSON codec and the
`serde` derives it requires on message types. Generate code with the `no_json`
plugin option (or `connectrpc-build`'s `.generate_json(false)`) so message
structs are emitted without serde derives, and disable the runtime `json`
feature:

```toml
[dependencies]
connectrpc = { version = "0.7", default-features = false, features = ["server"] }
```

With `json` off, message-type bounds relax from `Message + Serialize` to just
`Message`, so serde-free generated code compiles. A JSON request to such a
server gets a Connect `Unimplemented` error (the error body stays JSON, as the
protocol requires). See the [user guide](docs/guide.md#proto-only-no-json-builds)
for details.

### With Axum integration

```toml
[dependencies]
connectrpc = { version = "0.7", features = ["axum"] }
```

## Generated Code Dependencies

Code generated by `protoc-gen-connect-rust` requires these dependencies:

```toml
[dependencies]
connectrpc = { version = "0.7", features = ["client"] }
buffa = { version = "0.7", features = ["json"] }
buffa-types = { version = "0.7", features = ["json"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
http-body = "1"
```

For **proto-only** code (generated with `no_json`, and `connectrpc` built with
`default-features = false`), drop the `json` feature on `buffa`/`buffa-types`
and omit `serde`/`serde_json` — the generated message types no longer derive
them. See [Proto-only builds](#proto-only-no-json-builds).

### Optional: gate the client behind a Cargo feature

If you want a server-only build of your crate to drop the
`connectrpc/client` transport stack, opt in to the cfg gate. With
`buf generate`:

```yaml
# buf.gen.yaml
plugins:
  - local: protoc-gen-connect-rust
    out: src/gen/connect
    opt: [buffa_module=crate::proto, gate_client_feature]
```

Or with `connectrpc-build` in `build.rs`:

```rust
// build.rs
connectrpc_build::Config::new()
    .files(&["proto/greet.proto"])
    .includes(&["proto/"])
    .gate_client_feature(true)
    .compile()?;
```

The codegen then prefixes every emitted `FooClient<T>` struct and its
`impl` block with `#[cfg(feature = "client")]`. Declare the feature in
your `Cargo.toml` to forward it through to the runtime dep:

```toml
[features]
default = ["client"]
client = ["connectrpc/client"]

[dependencies]
connectrpc = { version = "0.7", features = ["server"] }  # no "client"
```

`cargo build --no-default-features` now leaves out the `FooClient` items
*and* drops `connectrpc/client` (the HTTP/2 transport stack) from the
dependency graph. See `connectrpc-health` for the minimal example. The
option is opt-in; the default emission is unconditional.

## Protocol Support

| Protocol | Status |
|---|---|
| Connect (unary + streaming) | ✓ |
| gRPC over HTTP/2 | ✓ |
| gRPC-Web | ✓ |

All 3,600 ConnectRPC server conformance tests and 6,872 client conformance
tests pass across all three protocols (2,580 Connect, 1,454 gRPC,
2,838 gRPC-Web). Run the server suite with `task conformance:test` and the
client suites with `task conformance:test-client-*`.

| RPC type | Status |
|---|---|
| Unary | ✓ (POST + GET for idempotent methods) |
| Server streaming | ✓ |
| Client streaming | ✓ |
| Bidirectional streaming | ✓ |

The gRPC server reflection service (`grpc.reflection.v1` and `v1alpha`)
is provided by the [`connectrpc-reflection`](connectrpc-reflection/)
crate, fed by `connectrpc_build::Config::emit_descriptor_set` (which
writes the `FileDescriptorSet` with its full import closure to `OUT_DIR`
for `include_bytes!`) or by an existing `buffa_descriptor::DescriptorPool`.

## Performance

Comparison against [tonic](https://docs.rs/tonic/) 0.14 (the standard Rust gRPC
implementation, built on the same hyper/h2 stack). Measured on Intel Xeon
Platinum 8488C with [buffa](https://github.com/anthropics/buffa) as the proto
library. Higher is better unless noted.

### Single-request latency

Criterion benchmarks at concurrency=1 (no h2 contention), measuring per-request
framework + proto work in isolation. Lower is better.

![Single-request latency](benches/charts/latency.svg)

<details><summary>Raw data (μs, lower is better)</summary>

| Benchmark | connectrpc-rs | tonic | ratio |
|---|---:|---:|---:|
| unary_small (1 int32 + nested msg) | 87.6 | 170.8 | **1.95×** |
| unary_logs_50 (50 log records, ~15 KB) | 195.0 | 338.5 | **1.74×** |
| client_stream (10 messages) | 166.1 | 223.8 | **1.35×** |
| server_stream (10 messages) | 109.8 | 110.1 | 1.00× |

Run with `task bench:cross:quick`.

</details>

### Echo throughput

64-byte string echo, 8 h2 connections (to avoid single-connection mutex
contention — see [h2 #531](https://github.com/hyperium/h2/issues/531)).
Measures framework dispatch + envelope framing + proto encode/decode with
minimal handler work.

![Echo throughput](benches/charts/echo.svg)

<details><summary>Raw data (req/s)</summary>

| Concurrency | connectrpc-rs | tonic |
|---|---:|---:|
| c=16 | 170,292 | 168,811 (−1%) |
| c=64 | 238,498 | 234,304 (−2%) |
| c=256 | 252,000 | 247,167 (−2%) |

Run with `task bench:echo -- --multi-conn=8`.

</details>

### Log ingest (decode-heavy)

50 structured log records per request (~22 KB batch): varints, string fields,
nested message, map entries. Handler iterates every field to force full decode.
This is where the proto library matters — buffa's zero-copy views avoid the
per-string allocations that prost's owned types require.

![Log ingest throughput](benches/charts/log-ingest.svg)

<details><summary>Raw data (req/s)</summary>

| Concurrency | connectrpc-rs | tonic |
|---|---:|---:|
| c=16 | 32,257 | 28,110 (−13%) |
| c=64 | 73,313 | 68,690 (−6%) |
| c=256 | 112,027 | 84,171 (−25%) |

At c=256, connectrpc-rs decodes **5.6M records/sec** vs tonic's **4.2M**.

**Raw mode (`strict_utf8_mapping`):** For trusted-source log ingestion where
UTF-8 validation is unnecessary, buffa can emit `&[u8]` instead of `&str` for
string fields (editions `utf8_validation = NONE` + the `strict_utf8_mapping`
codegen option). CPU profile shows this eliminates 11.8% of server CPU
(`str::from_utf8` drops to zero). End-to-end throughput gain in this benchmark
is smaller (~1%) because client encode becomes the bottleneck when both run on
one machine — in production with separate client/server, the server sees ~15%
more capacity.

Run with `task bench:log`.

</details>

### Fortunes (realistic workload + backing store)

Handler performs a network round-trip to a [valkey](https://valkey.io/)
container (`HGETALL` of 12 fortune messages, ~800 bytes), adds an ephemeral
record, sorts, and encodes a 13-message response. This is the shape of a
typical read-mostly service: RPC framing + async I/O wait + moderate-size
response. All three servers use an 8-connection valkey pool; client uses
8 h2 connections so protocol framing is the only variable.

<details><summary>Raw data (req/s, c=256)</summary>

**Cross-implementation (gRPC protocol):**

| Implementation | req/s | vs connectrpc-rs |
|---|---:|---:|
| connectrpc-rs | 199,574 | — |
| tonic | 192,127 | −4% |
| connect-go | 88,054 | −56% |

**Protocol framing (connectrpc-rs server):**

| Protocol | c=16 | c=64 | c=256 | Connect ÷ gRPC |
|---|---:|---:|---:|---:|
| Connect | 73,511 | 177,700 | 245,173 | **1.23×** |
| gRPC | 69,706 | 157,481 | 199,574 | — |
| gRPC-Web | 69,067 | 153,727 | 191,811 | — |

Connect's ~20% unary throughput advantage over gRPC at c=256 comes from
simpler framing: no envelope header, no trailing HEADERS frame. At 200k+
req/s, gRPC's trailer frame is ~200k extra h2 HEADERS encodes per second.
The gap grows with throughput (5% @ c=16 → 23% @ c=256).

Run with `task bench:fortunes:protocols:h2`. Requires `docker` for the
valkey sibling container (image pulled automatically on first run).

</details>

### Where the advantage comes from

CPU profile breakdown (log-ingest, c=64, 30s, `task profile:log`):

| Cost center | connectrpc-rs | tonic |
|---|---:|---:|
| Proto decode (views/owned) | 14.7% | 2.1% |
| UTF-8 validation | 11.2% | 4.0% |
| Varint decode | 2.2% | 3.5% |
| String alloc + copy | ~0 | **6.2%** |
| HashMap ops (map fields) | ~0 | **8.5%** |
| **Total proto** | **27.1%** | **~24%** (+allocator) |
| Allocator (malloc/free/realloc) | **3.6%** | **9.6%** |

connectrpc-rs spends a *larger fraction* of CPU in proto decode — because it
spends so much less everywhere else. buffa's view types borrow string data
directly from the request buffer (zero allocs per string field); `MapView` is
a flat `Vec<(K,V)>` scan with no hashing. tonic/prost must fully materialize
`String` + `HashMap<String,String>` for every record before the handler runs.

The framework itself contributes: codegen-emitted `FooServiceServer<T>` with
compile-time `match` dispatch (no `Arc<dyn Handler>` vtable), a two-frame
`GrpcUnaryBody` for the common unary case, and stream-message batching into
fewer h2 DATA frames.

## Custom Compression

The compression system is pluggable:

```rust
use connectrpc::{CompressionProvider, CompressionRegistry, ConnectError};
use bytes::Bytes;

struct MyCompression;

impl CompressionProvider for MyCompression {
    fn name(&self) -> &'static str { "my-algo" }
    fn compress(&self, data: &[u8]) -> Result<Bytes, ConnectError> { /* ... */ }
    fn decompressor<'a>(&self, data: &'a [u8]) -> Result<Box<dyn std::io::Read + 'a>, ConnectError> {
        // Return a reader that yields decompressed bytes. The framework
        // controls how much is read, so decompress_with_limit is safe by default.
        /* ... */
    }
}

let registry = CompressionRegistry::default().register(MyCompression);
```

## Protocol Specifications

This implementation tracks the following upstream specifications:

- [Connect Protocol](https://connectrpc.com/docs/protocol/)
- [gRPC over HTTP/2](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
- [gRPC-Web](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md)

Local copies can be fetched with `task specs:fetch` (see [`docs/specs/`](docs/specs/)).

## Contributing

By submitting a pull request, you agree to the terms of our [Contributor License Agreement](CLA.md).

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
