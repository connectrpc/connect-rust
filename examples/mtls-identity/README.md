# mTLS identity example

Demonstrates cert-SAN-based identity for an axum-hosted ConnectRPC
service. The server is hosted on axum behind
`connectrpc::axum::serve_tls`, which terminates TLS, captures the
verified client certificate chain and remote address, and stamps them
into request extensions as `PeerCerts` / `PeerAddr` — the same
convention the standalone `connectrpc::Server::with_tls` uses. The
handler parses the leaf cert's DNS SAN to derive a workload identity
and enforces an ACL against it.

This is the mTLS twin of [`examples/middleware/`](../middleware): same
secret-store-with-ACL shape, but the credential is a client certificate
instead of a `Bearer` token. The handler-side code that reads
`ctx.extensions.get::<T>()` is unchanged; only what the layer/accept
loop puts into extensions differs.

## Run it

The demo is a single self-contained binary: it generates an in-memory
PKI (CA, server cert, two workload client certs) with `rcgen`, starts
the server, makes a few calls with each identity, and shuts down. No
PEM files touch disk.

```bash
cargo run -p mtls-identity-example
```

Expected output (port and source ports vary):

```
IdentityService listening on https://127.0.0.1:PORT (mTLS required)

[alice] WhoAmI -> identity="alice" san="alice.workloads.example.com" from="127.0.0.1:..."
[bob] WhoAmI -> identity="bob" san="bob.workloads.example.com" from="127.0.0.1:..."

[alice] GetSecret(    shared) -> "the value of teamwork" (x-served-by: alice)
[alice] GetSecret(alice-only) -> "alice's diary entry" (x-served-by: alice)
[bob] GetSecret(    shared) -> "the value of teamwork" (x-served-by: bob)
[bob] GetSecret(alice-only) -> permission_denied: workload "bob" (bob.workloads.example.com) cannot read "alice-only"
```

## What to look at

### `serve_tls` instead of `axum::serve` (`src/lib.rs::serve`)

`axum::serve` accepts a plain `TcpListener` with no hook for
terminating TLS, so an axum + mTLS deployment normally has to write a
rustls accept loop by hand. `connectrpc::axum::serve_tls` is a drop-in
replacement that owns that loop and stamps `PeerAddr` / `PeerCerts`
into request extensions:

```rust
let app = axum::Router::new().fallback_service(connect_router.into_axum_service());
connectrpc::axum::serve_tls(listener, app, server_config)
    .with_graceful_shutdown(shutdown)
    .await?;
```

Handler code that reads `ctx.extensions.get::<PeerCerts>()` is then
portable between the standalone `Server::with_tls` and an axum app.

### Cert-SAN identity (`src/lib.rs::extract_identity`)

The handler reads the leaf cert from `PeerCerts`, parses its DNS SAN
with `x509-parser`, and derives a short workload name from a SAN under
`workloads.example.com`. A real deployment would typically match a
SPIFFE ID (a URI SAN) instead, or hand the whole step to an
authorization framework — the shape is the same: read `PeerCerts`,
parse the leaf, derive an identity.

Two failure modes both surface as `Unauthenticated`:

- No client cert presented: only reachable if the server's
  `ClientCertVerifier` made client auth optional. This example uses
  `WebPkiClientVerifier`, which *requires* a verified chain, so this
  path is dead in practice — kept as defense in depth.
- A cert is presented but no SAN matches the workload domain.

### In-memory PKI (`src/lib.rs::pki`)

`pki::generate(&["alice", "bob"])` builds a CA, a server leaf
(`SAN = localhost`), and one client leaf per workload
(`SAN = <name>.workloads.example.com`), all in memory via `rcgen`. A
deployment would load these from a secret store; the rustls types are
identical.

The server config requires *and verifies* client certs against the
demo CA, so the chain that reaches the handler is always verified —
the SAN parsing only has to decide *which* trusted client this is, not
whether to trust it.

## Integration test

`tests/e2e.rs` exercises four paths: identity reflection (`WhoAmI`),
authorized read with response trailer, permission denied (bob reading
alice's secret), and a TLS client without a cert being rejected at the
handshake before the request reaches HTTP.

```bash
cargo test -p mtls-identity-example
```

## Where to go next

- See [`examples/middleware`](../middleware) for the bearer-token
  equivalent of this example.
- See [`examples/eliza`](../eliza) for loading certs from PEM files
  with `--cert`/`--key`/`--client-ca` CLI flags.
