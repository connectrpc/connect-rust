# Middleware example

Demonstrates how data flows from a middleware layer into a connectrpc
handler via `Context::extensions`. The server stack composes a
bearer-token auth check (written as an `axum::middleware::from_fn`),
tower-http's `TraceLayer` for request logging, and `TimeoutLayer` for
a per-request deadline.

The handler reads the caller identity from `Context::extensions()` and
writes a `x-served-by` response trailer via `Context::set_trailer()`.

## Run it

```bash
# Terminal 1: server with INFO-level tracing
RUST_LOG=info,tower_http=debug \
    cargo run -p middleware-example --bin middleware-server

# Terminal 2: client (sends auth header via ClientConfig::with_default_header)
cargo run -p middleware-example --bin middleware-client

# Or with curl:
curl -X POST http://127.0.0.1:8080/anthropic.connectrpc.middleware_demo.v1.SecretService/GetSecret \
  -H 'authorization: Bearer demo-token-alice' \
  -H 'content-type: application/json' \
  -d '{"name": "shared"}'
```

Expected client output:

```
shared      -> the value of teamwork
  trailer x-served-by: alice
alice-only  -> alice's diary entry
```

Set `MIDDLEWARE_TOKEN` to `demo-token-bob` to see the permission-denied
path on `alice-only`. Set `MIDDLEWARE_URL` to point at a different
address.

## What to look at

### Server side (`src/server.rs`)

- **`auth_middleware`** - an async function written in axum's
  `from_fn` style. Validates a `Bearer <token>` header against a
  static map. On success, stamps a `UserId` into the request
  extensions and calls `next.run(req)`. On failure, returns a 401
  directly with a Connect-protocol JSON error body. Mounted via
  `axum::middleware::from_fn_with_state(tokens, auth_middleware)`,
  which is the idiomatic axum pattern for stateful auth.

  A hand-rolled `tower::Layer` + `tower::Service` pair would reach the
  same `Context::extensions` endpoint but requires more boilerplate.
  The connectrpc dispatcher only cares that something earlier in the
  stack inserted the value; how it got there is up to you.

- **Tower stack composition** - `ServiceBuilder` applies layers
  top-to-bottom (outermost first). Wrapped in axum's `Router::layer()`
  so axum handles the body conversion from `ConnectRpcBody` to
  `axum::body::Body` automatically:

  ```rust
  let tokens = Arc::new(token_table());
  axum::Router::new()
      .fallback_service(connect_router.into_axum_service())
      .layer(
          ServiceBuilder::new()
              .layer(TraceLayer::new_for_http())   // outermost
              .layer(axum::middleware::from_fn_with_state(tokens, auth_middleware))
              .layer(TimeoutLayer::with_status_code(...)),  // innermost
      );
  ```

- **Handler reading from `Context`** - the dispatch path moves the
  request's `http::Extensions` into `Context::extensions`. The handler
  reads `UserId` via `ctx.extensions().get::<UserId>()`, performs its own
  permission check against the secret store, and writes the
  `x-served-by` response trailer via `ctx.set_trailer(...)`.

### Client side (`src/client.rs`)

- **`ClientConfig::with_default_header`** - sets the auth header once on
  the client config; every RPC call picks it up automatically.
- **`ClientConfig::with_default_timeout`** - default deadline for every
  call.
- **`CallOptions::with_timeout`** - per-call deadline override.
  `_with_options` variants let any RPC method take per-call options.
- **`resp.trailers()`** - unary responses surface trailers alongside
  the body (and headers via `resp.headers()`).

## Integration test

`tests/e2e.rs` spins up the server with the same `from_fn` middleware
stack and exercises four paths: authorized success (verifies the
trailer arrives), missing auth header (expects `Unauthenticated`),
invalid token (expects `Unauthenticated`), and permission denied at
the handler level (expects `PermissionDenied`).

```bash
cargo test -p middleware-example
```

## Where to go next

- See [`examples/streaming-tour`](../streaming-tour) for handler
  signatures across all four RPC types.
- See [`examples/eliza`](../eliza) for tower-http CORS layered onto a
  streaming service plus TLS/mTLS support.
