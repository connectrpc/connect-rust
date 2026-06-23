# Changelog

All notable changes to connectrpc will be documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
with the [Rust 0.x convention](https://doc.rust-lang.org/cargo/reference/semver.html):
breaking changes increment the minor version (0.2 → 0.3), additive changes
increment the patch version.

## [Unreleased]

### Added

- **Optional `json` cargo feature for proto-only builds** ([#172]). The
  Connect JSON codec requires `serde::Serialize`/`Deserialize` on every
  message type, so the code generator derives them by default — pure cost for
  crates that only speak binary proto. The new default-on `json` feature, when
  disabled (`connectrpc = { default-features = false }`), relaxes the runtime's
  message-type bounds to just `buffa::Message` via the new
  `JsonSerialize`/`JsonDeserialize` marker traits, so message types
  generated with the codegen `no_json` option (no serde derives) compile
  against the runtime. A proto-only server declines JSON at content
  negotiation — `application/json` / `application/connect+json` (and the
  Connect GET `encoding=json` parameter) return HTTP 415, and
  `application/grpc+json` / `application/grpc-web+json` return a gRPC error
  status — with message-level encode/decode returning `Unimplemented` as a
  backstop; the client's `ClientConfig::json` selector is removed from the API
  in that build. The Connect error/end-stream wire format
  is always JSON per spec, so `serde`/`serde_json` remain required
  dependencies. See the
  [proto-only build guide](docs/guide.md#proto-only-no-json-builds).
- **Top-down service registration on `Router`** ([#164]). `Router::add_service`
  registers a generated service from the router outward
  (`Router::new().add_service(Arc::new(svc))`), the discoverable counterpart to
  the existing `FooServiceExt::register` extension method, which remains
  available. New `Router::merge` / `Router::merge_in_place` combine routers, and
  a new public `ServiceRegister` trait (implemented by codegen) backs
  `add_service`. Registering or merging a method path that already exists now
  fails by default so an accidental collision — such as adding the same service
  twice — surfaces loudly instead of silently shadowing a route: `add_service`,
  `register`, `merge`, `merge_in_place`, and `merge_routers` panic. Call
  `Router::allow_overrides` to opt into last-wins replacement across all of
  them. For assembling routers from dynamic input, `Router::try_merge` /
  `Router::try_merge_in_place` return a `RouterMergeError` listing the
  conflicting paths instead of panicking.
- **Maximum connection age for the built-in server** ([#151]).
  `with_max_connection_age` (on both `Server` and `BoundServer`) retires
  long-lived HTTP/2 connections by sending a GOAWAY once a connection reaches
  the configured age, then force-closing after a grace period
  (`with_max_connection_age_grace`, default 5s); HTTP/1.1 connections have
  keep-alive disabled instead. A symmetric ±10% jitter is applied per
  connection to avoid reconnect bursts. Disabled by default; whole-server
  graceful shutdown still drains in-flight requests indefinitely.

### Changed

- **Connect streaming EOF without END_STREAM now returns `internal`**
  ([#168]). The `ServerStream` Connect EOF path that 0.7.0's [#140]
  introduced as `unavailable` now returns `internal` — the code
  connect-go reports for this path, and the primary expected code in the
  upstream conformance suite addition ([connectrpc/conformance#1104]).
  The HTTP body completes cleanly in this case; it is the Connect
  envelope sequence that is missing its terminus, so connect-go and other
  gRPC stacks classify it as a wire-level error in the same family as a
  failed decompression or an unparseable response, rather than the
  transport flakiness [#140]'s entry described. **Clients on 0.7.0 that
  match `Unavailable` for a truncated Connect stream must match `Internal`
  after this release**, and generic retry middleware that retries on
  `unavailable` will now treat this case as terminal — a server that omits
  END_STREAM is not expected to start sending it on retry. The
  client-streaming check from [#163] ships with the same `internal` code.
  (The 0.7.0 [#140] entry's "matching connect-go" parenthetical was also
  inaccurate as to the code, but correct that connect-go is the reference:
  it returns `internal` for this path.)
- **Connect Unary-Get query parameters are emitted in the spec-recommended
  order** ([#167]): `connect`, `base64`, `compression`, `encoding`, `message`.
  Servers must accept any order, so this is not a wire-compatibility change;
  the recommended order keeps the variable-length `message` last so the URL
  prefix is stable for shared caches. Aligns with the order check added to
  the upstream conformance suite.
- **Unsupported gRPC/gRPC-Web message codecs return `unimplemented`**
  ([#180]). A request with a valid `application/grpc` / `application/grpc-web`
  prefix but a codec the server does not speak (for example
  `application/grpc+thrift`, or `application/grpc+json` in a proto-only build)
  now returns grpc-status `unimplemented` (12) instead of `internal` (13).
  This matches the compression axis, which already returns `unimplemented` for
  an unsupported `grpc-encoding`. Clients that branch on grpc-status will
  observe 13 → 12 for this case on upgrade.

### Fixed

- **Connect client-streaming responses require the END_STREAM envelope**
  ([#163]). A response that ended after its single data message but before
  the END_STREAM envelope was accepted as a success with empty trailers, so
  a truncated response was indistinguishable from a complete one. It now
  returns `Err(internal)` (see [#168]); complete responses are unchanged.

[#151]: https://github.com/anthropics/connect-rust/issues/151
[#163]: https://github.com/anthropics/connect-rust/pull/163
[#164]: https://github.com/anthropics/connect-rust/pull/164
[#167]: https://github.com/anthropics/connect-rust/pull/167
[#168]: https://github.com/anthropics/connect-rust/pull/168
[#172]: https://github.com/anthropics/connect-rust/pull/172
[#180]: https://github.com/anthropics/connect-rust/issues/180
[connectrpc/conformance#1104]: https://github.com/connectrpc/conformance/pull/1104

## [0.7.0] - 2026-06-10

A breaking release that reworks both ends of the message surface:
handlers take borrowed request views built on buffa 0.7.0, and client
streams surface terminal RPC errors from `message()` instead of hiding
them behind `Ok(None)`. It is also the first release of two new crates
that join the lockstep versioning scheme at 0.7.0: `connectrpc-health`
(the standard gRPC health-checking service) and `connectrpc-reflection`
(gRPC server reflection).

On the server side, buffa 0.7.0 removed `OwnedView`'s `Deref` impl (the
impl let safe code hold view fields past the backing buffer's lifetime),
and handlers move from owned `OwnedView<FooView<'static>>` parameters to
borrowed requests and owned stream items with per-field accessors.
**Consumers with checked-in `protoc-gen-connect-rust` output must
regenerate with this release's toolchain and buffa ≥ 0.7.0.**

### Breaking

- **Unary and server-streaming handlers take `ServiceRequest<'_, Req>`**
  ([#143]). The request is borrowed from the dispatcher-owned body for the
  duration of the call: it Derefs to the request view for zero-copy field
  access, and offers `to_owned_message()` (zero-copy from the retained body
  bytes), `to_owned_view()` (a `'static` view for pass-through responses),
  `bytes()`, and `view()`. The borrow may be held across `.await`
  points; the response — and anything moved into `tokio::spawn` — cannot
  borrow from it (enforced by the compiler). Existing handler impls must
  update their signatures; bodies that already started with
  `.to_owned_message()` work unchanged:

  ```rust
  // before (0.6)
  async fn greet(&self, ctx: RequestContext, request: OwnedView<GreetRequestView<'static>>)
      -> ServiceResult<GreetResponse>
  // after (0.7)
  async fn greet(&self, ctx: RequestContext, request: ServiceRequest<'_, GreetRequest>)
      -> ServiceResult<GreetResponse>
  ```
- **Client-streaming and bidi inbound items are `StreamMessage<Req>`**
  ([#143]). Each item owns its decoded buffer, is `Send + 'static`, Derefs
  to the buffa-generated `FooOwnedView` wrapper for per-field accessor
  methods (`item.name()`), and re-encodes from the retained wire bytes when
  yielded back (`StreamMessage<M>: Encodable<M>`).
- **`UnaryResponse::view()` returns the reborrowed view** rather than a
  reference to the `OwnedView`, so `resp.view().field` works directly on
  the client ([#143]).
- **`extern_path` targets must be buffa ≥ 0.7.0 generated code with views
  enabled** ([#143]). Request types resolved through `extern_path`
  (e.g. well-known types from `buffa-types`) use the same
  `ServiceRequest`/`StreamMessage` wrappers as local types, backed by
  `buffa::HasMessageView` impls that buffa emits with each message.
  `buffa-types` 0.7+ qualifies; a crate generated with older buffa or with
  views disabled fails to compile with a missing `HasMessageView` impl
  (on buffa 0.7.1+ the compile error itself explains the fix; 0.7.0 only
  names the missing impl). The output side is
  unchanged: view-body `Encodable` impls are still not emitted for extern
  output types — return the owned message for those.
- **The workspace requires `buffa`/`buffa-types`/`buffa-codegen` 0.7**
  ([#143]), which is also the regen baseline for checked-in generated code.
- **Client streams return terminal server errors from `message()`**
  ([#159]). Previously, an RPC error carried in the stream's termination
  metadata (gRPC HTTP/2 trailers, gRPC-Web trailer frame, or Connect
  END_STREAM envelope) made `ServerStream::message()` /
  `BidiStream::message()` return `Ok(None)` — indistinguishable from a
  clean close — with the error retrievable only via the easy-to-miss
  `error()` accessor. A caller that treated `Ok(None)` as success would
  silently swallow every failed streaming RPC. `message()` now returns
  `Err(...)` for an errored end and reserves `Ok(None)` for a clean end
  (gRPC status OK / error-free END_STREAM), matching `tonic`. Every
  `Err` is terminal and sticky — re-polling returns the same error,
  never a clean-looking `Ok(None)`; `error()` and `trailers()` remain
  populated for post-hoc inspection. Callers that only match `Err` need
  no changes; callers doing the `Ok(None)`-then-`error()` dance can
  delete the dance:

  ```rust
  // Before: errors hid behind Ok(None)        // After: `?` is complete
  while let Some(m) = s.message().await? {     while let Some(m) = s.message().await? {
      handle(m);                                   handle(m);
  }                                            }
  if let Some(err) = s.error() {
      return Err(err.clone().into());
  }
  ```

- **A gRPC/gRPC-Web stream that never delivers a usable `grpc-status`
  is now an error** instead of a clean `Ok(None)` ([#159]): `internal`
  when no trailers arrived at all, `unknown` when trailers arrived
  without a `grpc-status`, and `unknown` for a present-but-malformed
  `grpc-status` value — each matching grpc-go (and the conformance
  suite's primary expectations). A Trailers-Only response carrying
  `grpc-status: 0` in the response headers (grpc-go's shape for an OK
  end with zero messages) remains a clean end. The Connect-protocol
  equivalent — a missing END_STREAM envelope reported as `unavailable` —
  ships in this release as [#140] (see Fixed below); this entry covers
  only the gRPC and gRPC-Web protocols.

### Added

- **New `connectrpc-health` crate** ([#128]) — the standard
  `grpc.health.v1.Health` service for connectrpc routers, wire-compatible
  with `grpc_health_probe`, kubelet `grpc:` probes, and gRPC-aware service
  meshes (Linkerd, Istio). `install_static(router, [names])` mounts a
  `StaticChecker`-backed service in one call; implement the `Checker`
  trait for custom logic (e.g. reporting `NotServing` while a dependency
  is down). The generated `HealthClient` re-export is gated on the
  default-on `client` feature so server-only deployments can drop the
  client transport stack. See the
  [health checking](docs/guide.md#health-checking) section of the guide.
- **`ServiceRequest<'a, Req>` and `StreamMessage<M>`** ([#143]) — the
  request wrappers described above, exported from the crate root.
- The multiservice example gains a `Heartbeat(google.protobuf.Empty) →
  google.protobuf.Timestamp` RPC exercising well-known types as direct RPC
  input and output ([#143]).
- **`connectrpc_build::Config::emit_descriptor_set(name)`** ([#141]) —
  also write the input `FileDescriptorSet` (full transitive import
  closure, regardless of descriptor source) to `<out_dir>/<name>` as
  wire-format bytes, ready to `include_bytes!` and serve via
  `grpc.reflection.v1.ServerReflection` (e.g. for `grpcurl`). The inverse
  of `Config::descriptor_set`, which reads a precompiled set; build
  scripts no longer need a second `protoc --descriptor_set_out` pass.
- **New `connectrpc-reflection` crate** ([#157]) — gRPC server reflection,
  wire-compatible with `grpc.reflection.v1.ServerReflection` and its
  `v1alpha` predecessor, so `grpcurl`, `buf curl`, Postman, and `grpcui`
  work against connectrpc servers over gRPC, gRPC-Web, and Connect alike.
  Build a `Reflector` from `emit_descriptor_set` output (responses carry
  the compiler's original per-file descriptor bytes) or adopt an existing
  `buffa_descriptor::DescriptorPool` (e.g. the `descriptor_pool()`
  accessor emitted by reflection-enabled buffa codegen); `install(router,
  reflector)` mounts both protocol versions. `Reflector::with_services`
  curates the advertised service list, mirroring Go `grpcreflect`'s
  `Namer`. The service is self-describing: queries about
  `grpc.reflection.*` fall back to the crate's own descriptors and
  `ListServices` advertises the reflection services, matching grpc-go —
  no setup needed for schema-free clients like `buf curl`. The
  multiservice example mounts it with both sources selectable via
  `REFLECTION_SOURCE=fds|pool`;
  `examples/multiservice/reflection-demo.sh` walks through discovery
  and schema-free calls with `buf curl`.

### Fixed

- **The gRPC and gRPC-Web unary response parsers enforce the
  single-message rule before decompressing, and gRPC-Web parsing stops at
  the trailers frame** ([#147]). A second data envelope is rejected before
  its payload is touched (matching the Connect client-streaming parser
  from [#133]), and a gRPC-Web response now completes as soon as a
  complete trailers frame is buffered instead of reading the body to EOF,
  so well-formed responses finish even if the server keeps writing.
- **Connect streaming clients report an error when a response body ends
  without the required END_STREAM envelope** ([#140]). `message()` now
  returns `Err` with code `unavailable` (matching connect-go) instead of
  a clean `Ok(None)`, so a stream cut off mid-response is no longer
  indistinguishable from a complete one. Streams that previously appeared
  to drain cleanly against a known-good server may now error — if you see
  this, suspect an intermediary (proxy or load balancer) stripping the
  trailing envelope.
- **The inter-message timeout no longer starts at response-stream
  construction** ([#127]). `DeadlinePolicy::with_inter_message_timeout`
  armed its timer when the response stream was built, so the first
  measurement covered stream-setup latency (encoding, header writing,
  framework overhead) rather than the gap between messages, and a short
  timeout could fail the stream with `deadline_exceeded` before the
  consumer ever polled it. The timer now arms when the stream is first
  polled and re-arms after each yielded item, so setup latency before
  the first poll is excluded while a handler that stalls before its
  first item still times out.

### Changed

- Malformed gzip and zstd compressed payloads now return `invalid_argument`
  instead of `internal` ([#139]). For servers this attributes the failure
  to the sender and moves it out of 5xx metrics (the Connect HTTP status
  changes from 500 to 400) — update any alerting that keys on 5xx for
  these events. On the client, where the corrupt payload is a *response*,
  the error is remapped to `data_loss` so callers are not told their
  request was invalid. The client-side remap deliberately diverges from
  connect-go, which reports `invalid_argument` in both directions;
  `data_loss` is more descriptive of what actually happened. The default
  `CompressionProvider::decompress_with_limit` implementation (used by
  custom providers that only implement `decompressor`) follows the same
  convention: read failures now map to `invalid_argument` instead of
  `internal`.
- **Client-streaming and bidi handlers see request body failures as
  stream errors** ([#150]). A request body that fails mid-upload
  (truncated or broken transport) now yields `Err(internal)` from the
  handler's inbound stream instead of ending it cleanly, so partial input
  is no longer mistaken for a complete client stream. This refines the
  [#130] behavior, which logged transport errors at debug level in all
  cases; errors after END_STREAM (or after the handler stopped reading)
  remain diagnostic-only. Handlers that `?`-propagate stream items now
  fail the RPC on truncated uploads — the right default for aggregation;
  handlers that want to tolerate truncation must match on the error.
- **Configured deadlines now bound request-body receipt for unary and
  server-streaming RPCs** ([#136]). Previously the body was collected
  before the timeout was applied, so `with_default_timeout` / `with_max`
  bounded only handler execution and a slow-sending client could hold the
  request open indefinitely. One absolute deadline now covers body
  receipt, handler execution, and (with `enforce_on_streams`) the response
  stream; the deadline visible through `RequestContext` matches what is
  enforced. Services with timeouts sized only for handler CPU time may
  need to raise them to accommodate large uploads from slow clients.
  Client- and bidi-streaming RPCs are unchanged — their bodies are
  consumed inside the handler, already within the handler deadline.

### Internal

- The client streaming receive path is restructured so the terminal
  contract above is enforced by construction rather than convention
  ([#160]): the terminal outcome is recorded exactly once, and ending
  the stream without recording why is unrepresentable. No public-API or
  intended behavior change beyond [#159]; the streaming contract tests'
  assertions are unchanged.

[#127]: https://github.com/anthropics/connect-rust/pull/127
[#128]: https://github.com/anthropics/connect-rust/pull/128
[#136]: https://github.com/anthropics/connect-rust/issues/136
[#139]: https://github.com/anthropics/connect-rust/issues/139
[#140]: https://github.com/anthropics/connect-rust/issues/140
[#141]: https://github.com/anthropics/connect-rust/pull/141
[#143]: https://github.com/anthropics/connect-rust/pull/143
[#147]: https://github.com/anthropics/connect-rust/pull/147
[#150]: https://github.com/anthropics/connect-rust/pull/150
[#157]: https://github.com/anthropics/connect-rust/pull/157
[#159]: https://github.com/anthropics/connect-rust/pull/159
[#160]: https://github.com/anthropics/connect-rust/pull/160

## [0.6.1] - 2026-05-27

A patch release focused on the robustness of the streaming request and
response paths and of decompression. There are no API changes; the only
dependency-facing change is that the minimum supported `bytes` version is
now 1.6.

### Fixed

- **The streaming request body reader treats the Connect END_STREAM
  envelope as terminal** ([#130]). Request body bytes that arrive after
  END_STREAM are now drained (bounded) and ignored with a warning instead
  of being buffered, the decode buffer is released as soon as the decoder
  finishes, and transport-level body errors are logged at debug level
  instead of being silently discarded.
- **Gzip decompression returns an error for truncated deflate streams**
  ([#131]). A gzip payload whose deflate stream ends without an
  end-of-stream marker is rejected with
  `"gzip decompression stalled: truncated or invalid deflate stream"`
  instead of never completing.
- **Decompression output buffers are sized from the compressed input, not
  from `max_message_size`** ([#132]). Previously every decompressed
  message was backed by a limit-sized allocation (4 MiB by default) for as
  long as the message was held; the backing allocation now tracks the
  actual message size, with the same limits enforced as the buffer grows.
- **The Connect client-streaming response parser stops at END_STREAM and
  enforces the single-message rule before decompressing** ([#133]). A
  second data envelope is rejected before its payload is touched, and
  bytes after the END_STREAM envelope are ignored.

### Changed

- The minimum supported `bytes` version is now 1.6 ([#132]).

[#130]: https://github.com/anthropics/connect-rust/pull/130
[#131]: https://github.com/anthropics/connect-rust/pull/131
[#132]: https://github.com/anthropics/connect-rust/pull/132
[#133]: https://github.com/anthropics/connect-rust/pull/133

## [0.6.0] - 2026-05-20

The headline feature is **server-side interceptors** ([#114], [#121]) —
typed, async middleware that wraps a single RPC after envelope decoding,
decompression, and header parsing, and before the handler. Interceptors
see the resolved [`Spec`], headers, deadline, extensions, and a lazily
decoded [`Payload`], can rewrite the request and response, and can
short-circuit. Both unary (`Interceptor::intercept_unary`) and streaming
(`Interceptor::intercept_streaming`, covering server-, client-, and
bidi-streaming with one `Stream`-shaped hook) are supported. See the
[interceptors](docs/guide.md#interceptors) section of the user guide.

The supporting types — [`Spec`] static method metadata ([#112]),
[`Payload`] / [`AnyMessage`] type-erased message bodies ([#113]),
`RequestContext::path()` / `spec()` / `protocol()` ([#112], [#116],
[#120]) — are useful on their own for tracing, auth, and routing layers
that need to know which RPC is in flight.

**Consumers with checked-in `protoc-gen-connect-rust` output must
regenerate** with the 0.6.0 toolchain: the generated `Dispatcher::lookup`
emits per-method `Spec` constants ([#112]), `register()` chains
`.with_spec(...)` ([#120]), and `call_unary` takes [`Payload`] ([#119]).
`connectrpc-build` users (build.rs integration) are unaffected — Cargo
rebuilds `OUT_DIR` automatically.

### Breaking

- **`Dispatcher::call_unary` takes `Payload`, not `Bytes`** ([#119]).
  The [`Payload`] carries the wire bytes plus an interceptor's lazy
  decode cache; an owned-message handler calls `Payload::take_message()`
  to reuse a decode an interceptor already paid for, instead of decoding
  the same bytes twice. Generated dispatchers and `Router` impls follow.
  Any hand-rolled `impl Dispatcher` must update the `call_unary`
  signature; the streaming `call_*` methods are unchanged.

- **`MethodDescriptor` is now `#[non_exhaustive]`** ([#112]). It gains a
  `spec: Option<Spec>` field and `from_kind` / `with_idempotent` /
  `with_spec` const builders. Hand-rolled `impl Dispatcher` blocks that
  constructed `MethodDescriptor` via struct literal must switch to the
  builders (`MethodDescriptor::unary(idempotent)`,
  `MethodDescriptor::from_kind(kind)`, …); destructuring patterns need a
  trailing `..`. Reads of the existing `kind` / `idempotent` `pub` fields
  are unaffected.

- **`AnyMessage` gained a required `into_any` method** ([#119]). The
  blanket `impl<T: Message + Serialize> AnyMessage for T` covers every
  generated message type, so this only affects manual `AnyMessage`
  impls — which the trait docs already discourage.

### Added

- **`Interceptor` trait, `Next` continuation, and server registration**
  ([#114]). `Interceptor` is an `async_trait` with default-passthrough
  `intercept_unary(req, next)`. `Next<'_>` holds the rest of the chain;
  `next.run(req).await` invokes it (consume-once, enforced by the type
  system); not calling it short-circuits. Register with
  `ConnectRpcService::with_interceptor(...)`. The first-registered
  interceptor runs outermost, matching `connect-go::WithInterceptors`.
  A service with no interceptors pays one `is_empty()` branch and no
  per-request allocation. The `unary_interceptor` helper turns a closure
  returning a boxed future into an `Interceptor` for one-off use, and
  `#[connectrpc::async_trait]` is re-exported so downstream crates don't
  need an `async-trait` dep.

- **`Interceptor::intercept_streaming`** ([#121]). One `Stream`-shaped
  hook covers server-streaming, client-streaming, and bidi: interceptors
  receive an inbound `PayloadStream` and a `NextStream<'_>` continuation
  and return a `StreamResponse` carrying the outbound `PayloadStream`,
  response headers, trailers, and a compression hint. Cross-stream
  coordination is shared state captured by the inbound and outbound
  adapter closures. The `streaming_interceptor` closure helper mirrors
  the unary one. The empty-chain fast path is preserved.

- **`with_interceptor_arc`** ([#118]). Register an already-`Arc`'d
  `Arc<dyn Interceptor>` so several `ConnectRpcService` instances can
  share one interceptor (a process-wide auth token cache, rate-limit
  counter, connection pool). `with_interceptor` is now a thin wrapper
  over it.

- **`Payload` and `AnyMessage`** ([#113]). [`Payload`] holds the wire
  `Bytes` + `CodecFormat` plus a lazy decode cache and an optional
  replacement (`set_message`). Typed access via `message::<M>()` (decode
  once, proto and JSON), `view::<V>()` (zero-copy proto view), and
  `take_message::<M>()` ([#119], consume the cache or decode fresh).
  Most interceptors only read `Spec` and headers, never the body — so
  the body is never decoded unless someone asks. `AnyMessage` is the
  object-safe surface for type-erased messages, with a blanket impl over
  every `T: Message + Serialize` (no codegen required).

- **`Spec` static method metadata** ([#112], refs [#87]). New
  `connectrpc::spec` module with `Spec`, `StreamType`, `IdempotencyLevel`,
  and `SpecOrigin` types describing a single RPC method: its
  fully-qualified `procedure` path (`"/package.Service/Method"`), message
  flow shape, proto-declared idempotency contract, and which generated
  artifact (server or client) produced it. `Spec` is `Copy` and `'static`,
  with `const fn` constructors (`Spec::server(...)`, `Spec::client(...)`)
  so generated `Spec` constants live in `.rodata`. Code generation emits a
  `pub const <SERVICE>_<METHOD>_SPEC: Spec` per method that user code can
  reference directly — this also closes [#110], which asked for
  connect-go-style procedure-path constants.

- **`RequestContext::spec()`, `protocol()`, and `path()`** ([#112],
  [#116]). `spec()` returns the resolved [`Spec`] for the dispatched
  method; `protocol()` returns the negotiated wire protocol (`Connect` /
  `Grpc` / `GrpcWeb`); `path()` returns the requested procedure path
  taken directly from the request URI. `path()` is the wire truth and is
  populated unconditionally; `spec()` carries the richer static metadata
  but requires the route to have a `Spec` attached.

- **`Router::with_spec`** ([#120]). The dynamic `Router` can now carry a
  [`Spec`] per route, attached after registration via
  `.with_spec(SPEC_CONST)`. The generated `register()` chains it after
  every `route_*` call, so handlers and interceptors see the same
  `RequestContext::spec()` whether the host wired up the codegen
  dispatcher or the dynamic `Router`. Routes registered without
  `with_spec` behave exactly as before (`spec() == None`).

- **`HttpClientBuilder` with `connect_timeout`** ([#117]).
  `HttpClient::builder().connect_timeout(dur).plaintext()` (or
  `plaintext_http2_only()`, `with_tls(...)`) bounds the TCP `connect(2)`
  call so a silently dropped SYN fails in milliseconds instead of the
  kernel's `tcp_syn_retries` default (~130s). The existing constructors
  delegate to the builder with no timeout, so behaviour is unchanged for
  current callers. `connect_timeout` covers TCP connect only, not DNS
  resolution or the TLS handshake — use `CallOptions::with_timeout` for
  an end-to-end bound.

- **`Server::with_interceptor` and `Server::with_interceptor_arc`**
  ([#123]). Proxies to the same methods on `ConnectRpcService`, completing
  the proxy set started in [#105]. Standalone `Server` users can now
  register interceptors without dropping down to
  `Server::from_service(ConnectRpcService::new(...).with_interceptor(...))`.

[#87]: https://github.com/anthropics/connect-rust/issues/87
[#105]: https://github.com/anthropics/connect-rust/pull/105
[#110]: https://github.com/anthropics/connect-rust/issues/110
[#112]: https://github.com/anthropics/connect-rust/pull/112
[#113]: https://github.com/anthropics/connect-rust/pull/113
[#114]: https://github.com/anthropics/connect-rust/pull/114
[#116]: https://github.com/anthropics/connect-rust/pull/116
[#117]: https://github.com/anthropics/connect-rust/pull/117
[#118]: https://github.com/anthropics/connect-rust/pull/118
[#119]: https://github.com/anthropics/connect-rust/pull/119
[#120]: https://github.com/anthropics/connect-rust/pull/120
[#121]: https://github.com/anthropics/connect-rust/pull/121
[#123]: https://github.com/anthropics/connect-rust/pull/123
[`Spec`]: https://docs.rs/connectrpc/latest/connectrpc/spec/struct.Spec.html
[`Payload`]: https://docs.rs/connectrpc/latest/connectrpc/payload/struct.Payload.html
[`AnyMessage`]: https://docs.rs/connectrpc/latest/connectrpc/payload/trait.AnyMessage.html

## [0.5.0] - 2026-05-18

This release tracks **buffa 0.6.0** ([#108]) and lands the contract-locking
breaking changes for the runtime types (`RequestContext`, `ClientConfig`,
`CallOptions`, the streaming handler traits) so future request- and
client-scoped metadata can ship as non-breaking additions.

**Consumers with checked-in `protoc-gen-connect-rust` output must
regenerate** with the 0.5.0 toolchain and buffa 0.6.0 plugins. The regen
picks up the buffa 0.6.0 codegen output changes — `with_<field>()` builder
setters on explicit-presence fields, `MessageName` impls on owned and view
message types, `serde::Serialize` impls on view types, and the removal of
empty `__oneof.rs` / `__ext.rs` / `__view_oneof.rs` ancillary content
files. After regenerating, **delete any orphaned empty ancillary files**;
the package stitcher (`*.mod.rs`) no longer `include!`s them. It also
picks up the streaming-trait `ServiceStream<impl Encodable<Out>>` change
described under **Breaking** below. `connectrpc-build` users (build.rs
integration) are unaffected — Cargo rebuilds `OUT_DIR` automatically.

### Breaking

- **buffa dependency floor bumped to 0.6** ([#108]). buffa 0.6.0 has no
  API-breaking changes for connect-rust's call sites — the bump is for
  the codegen output baseline (see the regen note above) and to pick up
  the `with_*` setters and `MessageName` impls in generated message
  types. Consumers must align their direct `buffa` / `buffa-types`
  dependencies to `0.6` to avoid duplicate crate versions.

The next three entries change the streaming-handler trait shape. They
are breaking under semver but the practical blast radius is narrow: the
common consumer surfaces — `impl <GeneratedServiceTrait>` blocks and
`*_handler_fn` registrations — compile unchanged. Only hand-rolled
`impl StreamingHandler`/`impl BidiStreamingHandler` blocks and direct
callers of `dispatcher::codegen::encode_response_stream` need a one-line
edit.

- **Streaming handler traits gain `type Item: Encodable<Res>`** ([#98])
  and return `ServiceStream<Self::Item>` instead of `ServiceStream<Res>` —
  brings `StreamingHandler`, `BidiStreamingHandler`,
  `ViewStreamingHandler`, and `ViewBidiStreamingHandler` to parity with
  unary `Handler::Body` (added in 0.4.0). Stream items can now be
  `PreEncoded`, `MaybeBorrowed`, or any `Encodable<Res>`; previously
  they had to be the owned `Res` itself.

  These are the lower-level escape-hatch traits behind `Router`, not
  the primary handler surface. Most consumers use the codegen-generated
  service trait or the `*_handler_fn` closure helpers, neither of which
  is affected — codegen handles the new shape and the helpers infer
  `type Item` from the closure return. Hand-rolled `impl
  StreamingHandler` blocks must add `type Item = Res;`. We surveyed the
  in-tree consumers and found two; if you have hand-rolled impls,
  expect a single one-line addition per impl block.

- **Generated server-streaming and bidi-streaming trait methods now
  declare `ServiceStream<impl Encodable<Out> + Send + use<Self>>`**
  ([#98]) instead of `ServiceStream<Out>`. **Existing `impl <Service>` blocks
  that return `ServiceStream<Out>` compile unchanged via RPITIT
  refinement** (the same mechanism the unary path used since 0.4.0; the
  `refining_impl_trait` lint suppression documented in 0.4.1 covers the
  streaming case too). Handlers that want to yield `PreEncoded` items
  must do so from `'static` data — the `use<Self>` precise-capturing
  clause excludes `&self`'s lifetime, so views built inside the stream
  body must encode to bytes before the borrow ends.

  **Consumers with checked-in `protoc-gen-connect-rust` output must
  regenerate** (the same regeneration footgun documented in 0.4.0).
  `connectrpc-build` users (build.rs) are unaffected.

- **`dispatcher::codegen::encode_response_stream` gains a `B` type
  parameter** ([#98]) for the stream item type. The generated dispatcher and
  route-registration code passes `Res` explicitly because the trait
  method's stream item is the *opaque* `impl Encodable<Out>` (RPITIT),
  which can't be unified against the `Encodable<Res>` impls. Only
  consumers that call `dispatcher::codegen::encode_response_stream`
  directly need to turbofish `encode_response_stream::<Res, _, _>(s,
  format)`. We are not aware of any.

The next three entries lock the runtime config-type contracts behind
accessors and `with_*` builders so future request- and client-scoped
metadata can land as non-breaking additions. The migration is
mechanical: direct field reads become accessor calls (`ctx.headers` →
`ctx.headers()`) and bare-name setters gain a `with_` prefix
(`.protocol(p)` → `.with_protocol(p)`).

- **`RequestContext` is now `#[non_exhaustive]` with `pub(crate)` fields
  and accessor methods** ([#101]). Direct field reads — `ctx.headers`,
  `ctx.deadline`, `ctx.extensions` — must move to `ctx.headers()`,
  `ctx.deadline()`, `ctx.extensions()`. Construction via
  `RequestContext::new(headers)` and the `with_*` builders is unchanged.

  New (additive) accessors landed alongside the change:
  - `ctx.time_remaining()` — saturating `Duration` until the deadline,
    for budgeting downstream calls.
  - `ctx.extensions_mut()` — mutable extensions, for tower middleware
    that builds a `RequestContext` directly.
  - `ctx.peer_addr()` (`server` feature) and `ctx.peer_certs()`
    (`server-tls` feature) — typed extension lookups for the well-known
    peer types. They return `None` when the transport didn't insert the
    value, replacing the panic-prone
    `ctx.extensions.get::<PeerAddr>().unwrap()` pattern.

- **`ClientConfig` and `CallOptions` fields are now `pub(crate)` and the
  `ClientConfig` setter methods are renamed to `with_*`** ([#100]). Both
  structs are `#[non_exhaustive]`, so struct-literal and
  functional-update construction was already rejected by the compiler
  outside the crate (E0639); the `pub` fields just made the rustdoc
  suggest a path that didn't compile. Each field now has a same-named
  read accessor (`config.protocol()`, `config.base_uri()`,
  `options.timeout()`, `options.headers()`, …). To free the bare names
  for accessors, the `ClientConfig` setter methods were renamed to the
  `with_*` form already used by `CallOptions`, and one `CallOptions`
  setter was renamed to match its field:

  | Type | Before | After |
  |---|---|---|
  | `ClientConfig` | `.protocol(p)` | `.with_protocol(p)` |
  | `ClientConfig` | `.codec_format(f)` | `.with_codec_format(f)` |
  | `ClientConfig` | `.compression(r)` | `.with_compression(r)` |
  | `ClientConfig` | `.compression_policy(p)` | `.with_compression_policy(p)` |
  | `ClientConfig` | `.default_timeout(t)` | `.with_default_timeout(t)` |
  | `ClientConfig` | `.default_max_message_size(s)` | `.with_default_max_message_size(s)` |
  | `ClientConfig` | `.default_header(n, v)` | `.with_default_header(n, v)` |
  | `ClientConfig` | `.default_headers(h)` | `.with_default_headers(h)` |
  | `CallOptions` | `.with_compression(b)` | `.with_compress(b)` |

  `ClientConfig::new(uri)`, `.json()`, `.proto()`, and
  `.compress_requests(e)` are unchanged. The remaining `CallOptions`
  builders (`with_timeout`, `with_header`, …) are unchanged. Migrating
  reads: `config.protocol` → `config.protocol()`, `options.timeout` →
  `options.timeout()`.

  If you see `error[E0061]: this method takes 0 arguments but 1 argument
  was supplied` on a `ClientConfig` builder call, you've hit the rename:
  the same-named read accessor now occupies the old name. Prefix the
  call with `with_` per the table above.

- **`Server` / `BoundServer` / `ServeTls` builders renamed to `with_*`**
  ([#105]) — `tls_handshake_timeout(...)` is now `with_tls_handshake_timeout(...)`
  (on `Server`, `BoundServer`, and `axum::ServeTls`) and
  `BoundServer::http1_keep_alive(...)` is now
  `with_http1_keep_alive(...)`. This matches the `with_tls(...)` /
  `with_graceful_shutdown(...)` siblings that were already `with_*` and
  the `ConnectRpcService`/`ClientConfig` convention. Migration: prefix
  the call with `with_`. `with_tls(...)` is unchanged.

### Added

- **`PreEncoded<M>` response body** ([#98]) — wraps already-encoded protobuf
  bytes and satisfies `Encodable<M>`. Use when the handler builds and
  encodes a borrowing view internally — e.g. a `*View<'a>` borrowing
  from a local snapshot held in `Arc` — rather than returning the view
  itself. The `'static` bound on handler bodies and stream items means a
  view with a non-`'static` lifetime can't cross the handler boundary;
  `PreEncoded` carries the bytes across instead.

  The `M` type parameter is a compile-time witness. Three construction
  paths, in decreasing order of guarantee:
  `PreEncoded::from_message(&m)` (also `From<&M>`/`.into()`) encodes an
  owned `M` and the receiver type is the witness;
  `PreEncoded::from_view(&view)` enforces `M = MView::Owned`;
  `PreEncoded::from_bytes_unchecked(bytes)` wraps already-encoded bytes
  from elsewhere — a cache, a blob store, a sidecar — and takes `M` on
  trust (in debug builds it also decodes once as a `debug_assert!`).

  Optimized for the proto codec, where the bytes pass through verbatim.
  JSON requests fall back to decoding the proto bytes as `M` and
  re-serializing — slow, but a working response rather than a runtime
  `unimplemented` error. Services with significant JSON traffic should
  build and return the owned message (or `MaybeBorrowed::Owned`) so the
  codec layer can skip the proto round-trip.

- **`DeadlinePolicy`** ([#103]) — server-side moderation of client-asserted RPC
  deadlines. Clients control the per-request timeout via
  `Connect-Timeout-Ms` / `grpc-timeout`; without a policy the server
  trusts that value verbatim. `DeadlinePolicy` clamps the asserted
  timeout to a server-controlled `[min, max]` range, applies a default
  when the client asserts nothing (or sends an unparseable header), and
  optionally extends enforcement to streaming bodies.

  Configure via `ConnectRpcService::with_deadline_policy(...)` (axum /
  tower) or `Server::with_deadline_policy(...)`. `DeadlinePolicy::new()`
  is a no-op policy that preserves the prior default behavior — no
  clamping, no default, streaming bodies unbounded once the handler
  returns. Existing services see no change without an explicit opt-in.
  Recommended production starting point: set at least `with_max(...)` to
  bound worker lifetime and `with_default_timeout(...)` to your SLA.

  Two independent opt-in extensions, both off by default:
  - `with_enforce_on_streams(true)` wraps server- and bidi-streaming
    response bodies so the next item after the deadline is a
    `deadline_exceeded` error and the stream ends. Unary and
    client-streaming responses were already bounded by the handler
    future timeout; this closes the streaming-body gap.
  - `with_inter_message_timeout(d)` cuts off a stream that goes longer
    than `d` between yielded items (a stalled handler waiting on a slow
    upstream). Takes effect whenever set, with or without
    `with_enforce_on_streams`.

  When clamping changes a client value, a `tracing::debug!` event fires
  on target `connectrpc::deadline` with the path and before/after
  durations. Per-route deadline policy is a planned follow-up coupled
  to the typed routing surface (#91).

- **`Server` proxies for `ConnectRpcService` dispatch config** ([#105]) —
  `Server::with_limits`, `Server::with_compression`, and
  `Server::with_compression_policy` delegate to the same-named
  `ConnectRpcService` builders, so a `Server::new(router)` user no longer
  has to drop down to `Server::from_service(...)` to set request limits
  or compression. (`Server` already held the inner `ConnectRpcService`;
  the proxies just expose the existing surface.)

- **`Server::with_http1_keep_alive`** ([#105]) — `Server` already had an
  `http1_keep_alive` field (used by `serve`/`serve_with_graceful_shutdown`)
  but no builder to set it; only `BoundServer` did. Adds the missing
  builder so a one-step `Server::new(router).serve(addr)` user can disable
  HTTP/1.1 keep-alive without switching to the `bind`/`from_listener`
  two-step path.

- **`ConnectError::into_http_response(headers)`** ([#111]) — renders a
  `ConnectError` as a protocol-correct `http::Response<ConnectRpcBody>`
  for the protocol detected from the inbound request headers. Tower
  layers wrapping `ConnectRpcService` (auth, rate limiting, validation)
  use this to short-circuit a request *before* dispatch and still
  produce an error the client's protocol can decode — gRPC / gRPC-Web
  clients get an HTTP 200 with `grpc-status` trailers, Connect-streaming
  clients get an `EndStreamResponse` envelope, Connect-unary clients
  get a JSON error body. Without it, layers had to hand-roll a
  Connect-unary JSON body that gRPC and streaming clients see as a
  transport-level failure. Unrecognized or absent `Content-Type`
  (Connect GET, non-RPC traffic) falls back to Connect-unary JSON.

- **`#[doc(cfg(...))]` annotations across the feature-gated public
  surface** ([#109], thanks [@Yong-yuan-X]). docs.rs now renders
  "Available on crate feature `…` only" badges on every public item that
  requires a non-default feature — `client` / `client-tls` transports,
  `server` / `server-tls` types, `gzip` / `zstd` / `streaming`
  compression, and `axum` integration. Annotations show the minimal cfg
  expression after Cargo feature implications collapse (`client-tls`
  rather than `all(client, client-tls)`); `all(...)` is used only where
  neither feature implies the other. No runtime change.

### Fixed

- **`connectrpc-build`: `.files()` no longer emits `cargo:rerun-if-changed`
  in Buf mode** ([#59], thanks [@hobostay]). When `Config::files(...)` is
  used with `Config::use_buf()`, the listed entries are proto-relative
  names as they appear in the buf module (e.g. `"my/service.proto"`), not
  filesystem paths. Emitting `cargo:rerun-if-changed` for them pointed
  Cargo at non-existent files and forced a rebuild on every invocation.
  The directives are now suppressed in Buf mode, completing the fix #56
  applied to `Precompiled` mode. Manual `protoc` mode is unchanged.

[#59]: https://github.com/anthropics/connect-rust/pull/59
[#98]: https://github.com/anthropics/connect-rust/pull/98
[#100]: https://github.com/anthropics/connect-rust/pull/100
[#101]: https://github.com/anthropics/connect-rust/pull/101
[#103]: https://github.com/anthropics/connect-rust/pull/103
[#105]: https://github.com/anthropics/connect-rust/pull/105
[#108]: https://github.com/anthropics/connect-rust/pull/108
[#109]: https://github.com/anthropics/connect-rust/pull/109
[#111]: https://github.com/anthropics/connect-rust/pull/111
[@Yong-yuan-X]: https://github.com/Yong-yuan-X
[@hobostay]: https://github.com/hobostay

## [0.4.2] - 2026-05-07

### Added

- **`connectrpc::axum::serve_tls`** ([#80]). Companion to `serve` that
  hands off to the standalone `Server` for the TLS path — wrapping axum
  with `tokio_rustls::TlsAcceptor` directly hangs on h2 ALPN
  negotiation. Comes with an `examples/mtls-identity` example showing
  client-cert extraction in a handler.

### Fixed

- **`connectrpc`: `server` feature now enables `tokio/macros`** ([#80]).
  The accept loop in `Server::serve` and the new `axum::serve_tls` both
  use `tokio::select!`, but the `server` feature only enabled
  `tokio/net`. Crates depending on `connectrpc = { features = ["server"]
  }` only compiled when something else in the dependency closure enabled
  `tokio/macros` for them. Our conformance suite and examples both have
  `tokio = { features = ["macros", …] }` in dev-deps, which kept the gap
  hidden in CI.

- **`connectrpc-build`: generated `mod.rs` `#[allow(...)]` is now sourced
  from `buffa_codegen::ALLOW_LINTS`.** The hardcoded list had drifted
  behind buffa's: it was missing `clippy::uninlined_format_args` (which
  buffa enum JSON deserialize errors trip), `clippy::doc_lazy_continuation`,
  and `clippy::module_inception`. The `pub mod <pkg>` tree wraps buffa's
  per-proto split output (Owned/View/Oneof/Ext/PackageMod) plus our own
  `__connect.rs` companions, and the per-proto Owned content has no
  `#[allow(...)]` of its own — buffa scopes `package_mod_allow_attr()` to
  `__buffa` and `protoc-gen-buffa-packaging` covers the rest with an
  inner `#![allow(...)]` that has no analogue in `connectrpc-build`'s
  outer-mod layout. Sourcing from `ALLOW_LINTS` (chained with the
  `connectrpc-build`-specific `impl_trait_redundant_captures`) keeps the
  two from drifting again. Bumps the `buffa-codegen` dependency floor to
  `0.5.1`, where `unused_qualifications` landed in `ALLOW_LINTS`. The
  checked-in conformance/example/bench output was regenerated against the
  buffa 0.5.2 toolchain (`Self::` in oneof serde, inlined format args in
  enum serde) — those are codegen *output* changes, not API changes, and
  don't affect the floor.

## [0.4.1] - 2026-05-07

### Fixed

- **`connectrpc-build`: generated `mod.rs` `#[allow(...)]` now suppresses
  `unused_qualifications` and `impl_trait_redundant_captures`.** The 0.4.0
  trait method RPIT carries a `use<'a, Self>` precise-capturing clause that
  is required for edition-2021 consumers but redundant under edition 2024,
  and buffa 0.5 codegen references sibling types through the canonical
  `__buffa::view::*` path even when a shorter natural-path re-export exists
  (the re-export can be shadowed by a same-named proto type). Both lints
  fired against the generated code in workspaces that opt them in (or build
  with `-D warnings`). The two extra entries in the `#[allow(...)]`
  block scope the suppression to the `pub mod <pkg>` tree and don't touch
  hand-written code.

  Also documenting one related lint with no codegen-side workaround:
  `refining_impl_trait_internal` (`warn` by default since rust 1.86,
  rust-lang/rust#121718) fires on every handler `impl`, because the
  generated trait declares `ServiceResult<impl Encodable<Out> + ...>`
  while the handler returns `ServiceResult<Out>`. The refinement is
  intentional — it is what lets handlers return either an owned
  message, a borrowed view, or a `MaybeBorrowed<M, V>` — and is benign
  for handler impls, which are not part of the service's public API.
  There is no place in the generated module tree where `#[allow(...)]`
  could reach the handler impl. Consumers who deny warnings should set
  `refining_impl_trait_internal = "allow"` in `[lints.rust]` (or
  workspace lints) or `#[allow(refining_impl_trait)]` on each handler
  `impl` block.

## [0.4.0] - 2026-05-06

This release tracks buffa 0.5.0. **Consumers with checked-in
`protoc-gen-connect-rust` output must regenerate** with the 0.4.0
toolchain (and buffa 0.5.0 plugins): service stubs are now emitted as
`<stem>.__connect.rs` (was `<stem>.rs`), and the new package stitcher
only `include!`s the new name. After regenerating, **delete the stale
`<stem>.rs` files** in the connect output directory — protoc plugins
do not delete or overwrite the old name. Regenerate before bumping
the runtime crate, not after: regenerated buffa output references
runtime symbols (`ViewReborrow`, `decode_bytes_to_bytes`,
`__private::arbitrary_bytes`) that don't exist in `buffa` 0.4.
`connectrpc-build` users (build.rs integration) are unaffected — Cargo
rebuilds `OUT_DIR` automatically.

### Breaking

- **buffa dependency bumped to 0.5** ([buffa#97]). The only
  codegen-facing change is that `buffa_codegen::GeneratedFileKind` is
  now `#[non_exhaustive]`. This has no effect on `connectrpc` runtime
  consumers; build integrations that consume `connectrpc-codegen` and
  match `GeneratedFileKind` exhaustively need a wildcard arm
  (connect-rust itself matches only by `==`). See the
  [buffa 0.5.0 release](https://github.com/anthropics/buffa/releases/tag/v0.5.0)
  for the new natural-path re-exports — buffa 0.5.0 re-exports views
  at `pkg::FooView`, oneof enums at `pkg::msg_name::Kind`, and oneof
  view enums at `pkg::msg_name::KindView` — so the canonical
  `pkg::__buffa::view::FooView` / `pkg::__buffa::oneof::msg_name::Kind`
  paths from the buffa-0.4 layout are no longer needed in hand-written
  consumer code (they remain available for disambiguation if a proto
  type ever shadows a re-export). connect-rust's examples and tests now
  use the natural paths throughout.
- **Generated service code is now emitted as a `<stem>.__connect.rs`
  companion file** rather than appended to buffa's `<stem>.rs`
  (unified path) or written as a bare `<stem>.rs` (split / plugin
  path). connectrpc-codegen tags these files
  `GeneratedFileKind::Companion` and wires them into the per-package
  stitcher with `buffa_codegen::apply_companions` ([buffa#91],
  designed in [buffa#81]). The
  module structure exposed to consumers is unchanged; the visible
  effect is the on-disk filename change for projects with checked-in
  generated code (see the regeneration note above). Build integrations
  that inspect `GeneratedFileKind` should now match `Companion` for
  connect-rust's service files.
- **Carried over from the buffa 0.4 sync** ([buffa#62], [buffa#55]):
  generated code uses buffa's per-package stitcher layout, with view
  and oneof types canonically located under `<pkg>::__buffa::view::`
  and `<pkg>::__buffa::oneof::` (buffa 0.5.0's natural-path re-exports
  above hide this from consumers). `buffa_types::Any.value` is now
  `bytes::Bytes` (was `Vec<u8>`). buffa's size cache is externalized
  ([buffa#22]): generated structs no longer carry `__buffa_cached_size`,
  and `Message::compute_size`/`write_to` take `&mut SizeCache`. The
  provided `encode_to_bytes()` / `encoded_len()` are unchanged;
  connectrpc itself only uses those, but direct callers of
  `compute_size()` should switch to `encoded_len()`.
- **`connectrpc-codegen`**: `Options` now embeds the buffa
  `CodeGenConfig` directly as `Options::buffa` instead of mirroring
  individual fields ([#34]). The previous per-field shims
  (`strict_utf8_mapping`, `generate_json`, `extern_paths`,
  `emit_register_fn`) are gone; set `options.buffa.<field>` instead.
  `CodeGenConfig` is re-exported from `connectrpc_codegen::codegen` and
  `connectrpc_build`. `connectrpc_build::Config` keeps its existing
  builder methods as thin shims and gains `.buffa_config(cfg)` for
  wholesale replacement. `generate_views = true` is still enforced.
- **`ConnectError` shrunk from 248 to 72 bytes** ([#61]). The
  `response_headers` and `trailers` fields are now crate-private
  `Option<Box<http::HeaderMap>>` (was `pub http::HeaderMap`), so
  `Result<_, ConnectError>` no longer trips
  `clippy::result_large_err`. New accessors replace direct field
  access: `response_headers()` / `trailers()` (borrow, empty map if
  unset), `response_headers_mut()` / `trailers_mut()`, and
  `set_response_headers()` / `set_trailers()`. The `with_headers()` /
  `with_trailers()` builders keep their signatures. Behaviour notes:
  `with_headers` / `with_trailers` / `set_*` now normalize an empty
  `HeaderMap` to "unset" (observationally identical via the
  accessors), and the `Debug` output for an unset map now shows
  `None` instead of `{}`.
- **Handler signatures redesigned** ([#7]): the generated service
  trait no longer threads a single `Context` in and out. Handlers
  now receive a read-only `RequestContext` (headers, deadline,
  extensions) and return `ServiceResult<B>` =
  `Result<Response<B>, ConnectError>`, where `Response<B>` carries
  the body plus optional response headers/trailers/compression hint.
  Unary and client-stream methods return
  `ServiceResult<impl Encodable<Out>>`; server-stream and bidi
  return `ServiceResult<ServiceStream<Out>>`. `Response::ok(body)` is
  the bare-body happy-path shorthand; for streaming bodies use
  `Response::stream_ok(s)`. `Encodable<M>` is the new "encodes as
  M" bound on response bodies. The old `Context` type is removed.

  ```rust
  // before
  async fn say(&self, ctx: Context, req: ...) -> Result<(SayResponse, Context), ConnectError> {
      Ok((SayResponse { ... }, ctx))
  }
  // after
  async fn say(&self, _ctx: RequestContext, req: ...) -> ServiceResult<SayResponse> {
      Response::ok(SayResponse { ... })
  }
  ```
- **View response bodies** ([#7]): unary and client-stream trait
  methods are now `<'a>(&'a self, ...) -> ServiceResult<impl
  Encodable<Out> + use<'a, Self>>`, so a handler can return a body
  that borrows from `&self`. Codegen emits `impl Encodable<Out> for
  OutView<'_>` and for `OwnedView<OutView<'static>>` per RPC output
  type (proto via `ViewEncode`; JSON returns an `unimplemented`
  error since view types lack `Serialize`). The new
  `MaybeBorrowed<M, V>` enum lets a handler return either: see
  `benches/rpc/benches/filter_handler.rs` for a redaction example
  (~1.65x at the codec layer when no modification is needed).
  `ViewHandler`/`ViewClientStreamingHandler` now take `CodecFormat`
  and return the response already encoded, dropping the `Res` type
  param.

### Changed

- **`GzipProvider` defaults tuned for throughput**: the default
  compression level is now **1** (was 6), and `flate2` is built with
  the `zlib-rs` backend (pure-Rust port of zlib-ng) instead of
  `miniz_oxide`. Together this is ~2.7× throughput on the
  `unary/large_gzip` bench. Gzip wire format is unchanged; payloads
  compressed at level 1 are larger than at level 6. Restore the old
  ratio with `GzipProvider::with_level(6)`. Note that Cargo feature
  unification means the `zlib-rs` backend also applies to any other
  `flate2` use in the same dependency graph.
- `GzipProvider::DEFAULT_LEVEL` and `ZstdProvider::DEFAULT_LEVEL` are
  now public constants.

### Fixed

- `StreamingCompressionProvider::compress_stream` (gzip and zstd) now
  honors the provider's configured level; previously it ignored
  `self.level` and used `async-compression`'s default.
- **`connectrpc` no longer pulls `axum`'s default features** ([#55]),
  which transitively required `tokio/net` → `mio` and made the crate
  impossible to use on WASM hosts that integrate with `axum` (e.g.
  Cloudflare Workers). The `axum` dependency now declares
  `default-features = false`.
- **wasm32 client-stream and bidi RPCs no longer hang** ([#63]). The
  body-reader future is now spawned via `wasm_bindgen_futures::spawn_local`
  on `wasm32-unknown-unknown` (it was being polled inline, deadlocking
  on the first `.await`). Native targets keep `tokio::spawn`.

### Added

- **`file_per_package` output layout** for `protoc-gen-connect-rust` and
  `connectrpc-build`. When enabled (`opt: file_per_package` in
  `buf.gen.yaml`, `--connect-rust_opt=file_per_package` with `protoc`, or
  `Config::file_per_package(true)` from `build.rs`), the per-proto split
  is collapsed to one `<dotted.pkg>.rs` per proto package with all
  service stubs inlined and no `<pkg>.mod.rs` stitcher — matching the
  `<dotted.package>.rs` filename convention `protoc-gen-buffa` produces
  under its own `file_per_package` option ([buffa#73]) and that BSR cargo
  SDK generation and `tonic`-style build integrations expect (module tree
  synthesised from filenames). The two plugins generate disjoint content
  (buffa: message types, connect-rust: service stubs); set
  `file_per_package` on both. In the `connectrpc-build` path service
  stubs are inlined into buffa's per-package `PackageMod` rather than
  written as `<stem>.__connect.rs` siblings; the include file picks up
  the new filename automatically and consumer code is unaffected. When
  using the protoc plugin from `buf generate`, **drop the
  `protoc-gen-buffa-packaging` invocations** under this layout — there
  are no per-file content files or stitchers for it to wire — and keep
  routing `file_per_package` output to its own directory: the filename
  matches `protoc-gen-buffa`'s and would silently overwrite in a shared
  one. See [`CodeGenConfig::file_per_package`] for the
  `strategy: directory` constraint.
- **`connectrpc::include_generated!()`**: shorthand macro for
  `include!(concat!(env!("OUT_DIR"), "/_connectrpc.rs"))`. An optional
  filename argument (note: a filename including `.rs`, **not** a proto
  package name as in `tonic::include_proto!`) supports projects that
  customise the output via `Config::include_file` ([#50]).
- **`connectrpc-build`**: `Config::emit_rerun_directives(bool)` to suppress
  the `cargo:rerun-if-changed=` lines when running outside a Cargo
  `build.rs` context (e.g. from a Bazel genrule or standalone host tool).
  Default remains `true`.

[#80]: https://github.com/anthropics/connect-rust/pull/80
[#7]: https://github.com/anthropics/connect-rust/issues/7
[#34]: https://github.com/anthropics/connect-rust/issues/34
[#50]: https://github.com/anthropics/connect-rust/issues/50
[#55]: https://github.com/anthropics/connect-rust/pull/55
[#61]: https://github.com/anthropics/connect-rust/issues/61
[#63]: https://github.com/anthropics/connect-rust/pull/63
[buffa#22]: https://github.com/anthropics/buffa/pull/22
[buffa#55]: https://github.com/anthropics/buffa/pull/55
[buffa#62]: https://github.com/anthropics/buffa/pull/62
[buffa#73]: https://github.com/anthropics/buffa/pull/73
[buffa#81]: https://github.com/anthropics/buffa/issues/81
[buffa#91]: https://github.com/anthropics/buffa/pull/91
[buffa#97]: https://github.com/anthropics/buffa/pull/97
[`CodeGenConfig::file_per_package`]: https://docs.rs/buffa-codegen/latest/buffa_codegen/struct.CodeGenConfig.html#structfield.file_per_package

## [0.3.3] - 2026-04-17

### Fixed

- **`connectrpc-build` no longer emits invalid
  `cargo:rerun-if-changed` directives in `Precompiled` input mode**
  ([#56]). When a precompiled `FileDescriptorSet` was supplied instead
  of `.proto` source files, `.files()` paths were still being passed
  through to cargo, causing spurious rebuild triggers on paths that
  don't exist in that mode.

### Changed

- **MSRV is now declared as Rust 1.88** on the workspace and verified
  in CI ([#44]). The code has required 1.88 since v0.3.2 (let-chains);
  this commit documents the requirement in `Cargo.toml` and adds an
  explicit CI check.

### Added

- New `examples/streaming-tour` and `examples/middleware` crates,
  plus a user guide under `docs/guide.md` ([#46], [#48]).

[#44]: https://github.com/anthropics/connect-rust/pull/44
[#46]: https://github.com/anthropics/connect-rust/pull/46
[#48]: https://github.com/anthropics/connect-rust/pull/48
[#56]: https://github.com/anthropics/connect-rust/pull/56

## [0.3.2] - 2026-04-03

### Fixed

- **Generated service code now compiles when multiple services are
  `include!`d into the same Rust module** ([#32]). The codegen previously
  emitted top-level `use` statements that collided with E0252 when
  buffa-packaging's flat-output strategy concatenated several service
  files into one module. Bindings now use fully-qualified paths
  throughout (`::connectrpc::Context`, `::buffa::view::OwnedView`,
  `::http_body::Body`, etc.), so multiple service files can coexist in
  the same `mod` block.

### Changed

- **Generated client methods reference the per-service `*_SERVICE_NAME`
  const** ([#16]) instead of repeating the fully-qualified service name
  as a string literal at every call site. Matches the server-side
  router.
- **Workspace `tokio` feature footprint narrowed** ([#19]). The published
  `connectrpc` crate previously inherited the full workspace tokio
  feature set (`macros`, `net`, `signal`, `rt-multi-thread`, ...) when
  `workspace = true` was inlined at publish time. It now requests only
  `rt`, `io-util`, `sync`, `time`, plus `net` when the `client` or
  `server` feature is enabled. Downstream crates that use `tokio`
  directly should declare their own features rather than relying on
  transitive activation.
- **Workspace dependency updates** ([#37]).

### Added

- **`wasm32-unknown-unknown` target compatibility** ([#19]) for the
  `connectrpc` crate with default features off. A new
  `examples/wasm-client` demonstrates a Fetch-based `ClientTransport`
  implementation with browser-based integration tests via `wasm-pack`.
  Currently exercises unary calls without deadlines; timeouts and
  streaming require additional setup beyond the example.

[#16]: https://github.com/anthropics/connect-rust/pull/16
[#19]: https://github.com/anthropics/connect-rust/pull/19
[#32]: https://github.com/anthropics/connect-rust/pull/32
[#37]: https://github.com/anthropics/connect-rust/pull/37

## [0.3.1] - 2026-04-02

### Added

- **`emit_register_fn` option** ([#35]) on `connectrpc_codegen::codegen::Options`
  and `connectrpc_build::Config`, plumbing through to
  `buffa_codegen::CodeGenConfig::emit_register_fn`. Set to `false` to suppress
  the per-file `register_types(&mut TypeRegistry)` aggregator when multiple
  generated files are `include!`d into the same module (the identically-named
  functions would otherwise collide). The protoc plugin accepts a matching
  `no_register_fn` parameter for path-compat with the unified `connectrpc-build`
  flow.

[#35]: https://github.com/anthropics/connect-rust/pull/35

## [0.3.0] - 2026-04-02

### Changed

- **Upgraded `buffa` to 0.3.0** ([#24]). buffa 0.3 renames `AnyRegistry` to
  `TypeRegistry` (with `JsonAnyEntry` and `register_json_any()` replacing the
  old `AnyTypeEntry` / `register()`). Generated code and the runtime crate
  now use the new types; users who construct a registry manually for
  `google.protobuf.Any` JSON encoding will need to migrate.
- **`connectrpc-build` only rewrites output files when content changes**
  ([#22]). Preserves mtimes so touching one `.proto` no longer triggers a
  full downstream recompile of every generated `.rs` file. Mirrors
  prost-build's `write_file_if_changed`.

### Added

- **mTLS peer credentials and remote address are now available to handlers**
  ([#31]) via `Context::extensions`. The built-in server inserts `PeerAddr`
  (always) and `PeerCerts` (when `server-tls` is enabled and the client
  presented a certificate chain) into every request's extensions; handlers
  read them with `ctx.extensions.get::<PeerAddr>()` /
  `ctx.extensions.get::<PeerCerts>()`. Custom HTTP stacks (axum, raw hyper)
  can insert the same types from a tower layer so handler code stays
  transport-agnostic.
- **`Server::from_listener(TcpListener)`** ([#31]) wraps a pre-bound
  listener, allowing socket options (`IPV6_V6ONLY=false` for dual-stack,
  `SO_REUSEPORT`, inherited file descriptors) to be configured before
  handing the listener to connectrpc.
- **`Http2Connection::lazy_with_connector` / `connect_with_connector`** ([#15])
  as the generic transport escape hatch — supply any `tower::Service<Uri>`
  yielding a `hyper::rt::Read + Write` stream and the library runs the h2
  handshake over it. `lazy_unix` / `connect_unix` are thin wrappers for
  Unix domain sockets.
- **Codegen now rejects RPC method names that collide after `to_snake_case`**
  ([#28]). `rpc GetFoo(...)` and `rpc get_foo(...)` in the same service
  previously emitted duplicate `fn get_foo` and failed with a rustc error
  pointing at generated code; the build script now fails with a clear error
  naming both proto methods. Also catches a method whose name collides with
  another's `_with_options` client variant.

### Fixed

- **RPC methods whose snake_case names are Rust keywords now generate valid
  code** ([#23], [#26]). `rpc Move(...)` previously emitted `fn move(...)`
  and failed at build-script time. Method idents are now routed through
  buffa's keyword escaper, producing `r#move` (or a `_` suffix for the four
  keywords that cannot be raw identifiers).
- **`service Self {}` no longer generates `trait Self`** ([#27]). The handler
  trait is suffixed to `Self_`; the `SelfExt` / `SelfClient` / `SelfServer`
  derivatives are unaffected since the suffix already de-keywords them.

[#15]: https://github.com/anthropics/connect-rust/pull/15
[#22]: https://github.com/anthropics/connect-rust/pull/22
[#23]: https://github.com/anthropics/connect-rust/issues/23
[#24]: https://github.com/anthropics/connect-rust/pull/24
[#26]: https://github.com/anthropics/connect-rust/pull/26
[#27]: https://github.com/anthropics/connect-rust/pull/27
[#28]: https://github.com/anthropics/connect-rust/pull/28
[#31]: https://github.com/anthropics/connect-rust/pull/31

## [0.2.1] - 2026-03-18

### Fixed

- **`BidiStream` half-duplex deadlock on `SharedHttp2Connection`** ([#2], [#4]).
  `call_bidi_stream` stored the transport's `send()` future unpolled, so for
  transports where that future contains the connect/handshake/stream work
  (i.e. not hyper's pooled client), the HTTP request never initiated until
  the first `message()` call. The half-duplex pattern (send all, close,
  then read) would buffer into the 32-deep `ChannelBody` mpsc with nobody
  draining it and deadlock on the 33rd send. The send future is now
  spawned so the request streams immediately.
- **TLS connections to IPv6 literal URIs failed** ([#1], [#3]). `Uri::host()`
  returns `[::1]` with brackets, which `rustls_pki_types::ServerName`
  rejected as an invalid DNS name. Brackets are now stripped so the
  address parses as `ServerName::IpAddress`.
- **README required-dependencies example showed `buffa = "0.1"`** instead
  of `"0.2"`. The `connectrpc` crate bakes the workspace README via
  `readme = "../README.md"`, so the crates.io page for 0.2.0 shows the
  stale version; this release updates it.

[#1]: https://github.com/anthropics/connect-rust/issues/1
[#2]: https://github.com/anthropics/connect-rust/issues/2
[#3]: https://github.com/anthropics/connect-rust/pull/3
[#4]: https://github.com/anthropics/connect-rust/pull/4

## [0.2.0] - 2026-03-17

First release from the [anthropics/connect-rust](https://github.com/anthropics/connect-rust)
repository. This is a complete from-scratch implementation — not a continuation
of the 0.1.x releases previously published under the `connectrpc` crate name,
which have been superseded.

### Protocol support

| Protocol | Server | Client |
|---|---|---|
| Connect (unary + streaming) | ✅ | ✅ |
| Connect GET (idempotent unary via query string) | ✅ | ✅ |
| gRPC over HTTP/2 | ✅ | ✅ |
| gRPC-Web | ✅ | ✅ |

| RPC type | Server | Client |
|---|---|---|
| Unary | ✅ | ✅ |
| Server streaming | ✅ | ✅ |
| Client streaming | ✅ | ✅ |
| Bidirectional streaming (full-duplex on h2, half-duplex on h1/h2) | ✅ | ✅ |

### Conformance

All applicable ConnectRPC conformance features are enabled. Test counts:

| Suite | Tests |
|---|---|
| Server (default) | 3600 |
| Server Connect+TLS (incl. mTLS) | 2396 |
| Client Connect (incl. GET, bidi, zstd, mTLS, h1 half-duplex) | 2580 |
| Client gRPC | 1454 |
| Client gRPC-Web | 2838 |

### Key features

**Runtime**
- Tower-based `ConnectRpcService<D>` — framework-agnostic, works with Axum, Hyper, etc.
- Monomorphic `FooServiceServer<T>` dispatcher (compile-time method dispatch, no `dyn Handler` vtable)
- Dynamic `Router` with runtime registration for multi-service or reflection use cases
- Pluggable compression via `CompressionProvider` trait; gzip + zstd built-in
- `#![deny(unsafe_code)]`, `#![warn(missing_docs)]`

**Client transports** (feature = `client`)
- `HttpClient::plaintext()` / `::with_tls()` — pooled hyper client, HTTP/1.1 + HTTP/2 via ALPN
- `Http2Connection::connect_plaintext()` / `::connect_tls()` — single raw h2 connection with
  honest `poll_ready`, composes with `tower::balance` for N-connection load spreading
- Security-first naming: no bare `::new()` — plaintext vs TLS is an explicit choice
- TLS accepts `Arc<rustls::ClientConfig>`, preserving dynamic cert rotation through
  `Arc<dyn ResolvesClientCert>`
- Whole-call deadline enforcement via `tokio::time::timeout_at` (gRPC semantics: deadline
  applies to the entire call, not per-message)

**Server** (feature = `server`)
- `Server::with_tls(Arc<rustls::ServerConfig>)` — mTLS via `with_client_cert_verifier()`
- Graceful shutdown with connection draining

**Generated clients**
- Dual methods per RPC: `foo(req)` (uses config defaults) + `foo_with_options(req, opts)`
- `ClientConfig` carries defaults for timeout, max message size, and headers — applied
  automatically by the no-options method

### Security

- **Message size limits enforced on both sides.** Request body collection,
  response body collection, envelope decoding, and decompression all apply
  configurable size limits, preventing either a malicious client or server
  from forcing unbounded memory allocation via oversized payloads or
  compression bombs.
- Both client and server default to 4 MiB per message
  (`DEFAULT_MAX_MESSAGE_SIZE`) when no explicit limit is configured — matching
  connect-go. Server: raise via `Limits::max_message_size`. Client: raise via
  `ClientConfig::default_max_message_size` or `CallOptions::max_message_size`.
- **TLS handshake timeout.** The server disconnects clients that open a TCP
  connection but stall the TLS handshake, preventing slowloris-style connection
  exhaustion. Defaults to 10 seconds (`DEFAULT_TLS_HANDSHAKE_TIMEOUT`);
  configure via `Server::tls_handshake_timeout`.
- **Timeout header digit-limit enforcement.** Per spec, `connect-timeout-ms`
  is capped at 10 digits and `grpc-timeout` at 8 digits (matching connect-go).
  Over-spec values are treated as no-timeout. Prevents a malicious client from
  triggering a per-request panic via `Instant + Duration` overflow. Deadline
  computation also uses `checked_add` as defense in depth.

### Code generation

- `connectrpc-codegen` — descriptor → Rust source library
- `connectrpc-build` — `build.rs` integration (protoc/buf → codegen → `OUT_DIR`)
- `protoc-gen-connect-rust` — protoc plugin binary

Generated code emits service traits, `FooServiceServer<T>` monomorphic dispatchers,
`FooServiceClient<T>` clients, and buffa message types via `buffa-codegen`.

### Not yet implemented

- gRPC server reflection
- HTTP/3 (blocked on hyper support)

### Performance

vs tonic 0.14 (same hyper/h2 stack), Intel Xeon 8488C:
- **1.95×** faster on small unary (single-request latency, no contention)
- **1.74×** faster on decode-heavy log ingest (50 records, ~15 KB)
- **~4%** ahead on realistic fortune+valkey workload (c=256)

The advantage comes from buffa's zero-copy view types (borrowed string fields
directly from the request buffer, no per-string alloc; `MapView` as flat
`Vec<(K,V)>` with no hashing) and compile-time dispatch via the generated
`FooServiceServer<T>`. See README for the full CPU breakdown.
