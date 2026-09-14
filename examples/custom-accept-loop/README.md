# Custom accept loop example

Demonstrates replacing `Server::serve`'s accept loop with your own while
keeping everything else the built-in server does for a connection.

The loop in [`src/lib.rs`](src/lib.rs) (`serve`) accepts
TLS connections on one listener, parses the workload identity out of each
client certificate once, **refuses** connections whose certificate names no
known workload, and **places** the rest on one of two tokio runtimes:
`batch-*` workloads on a small `bulk` runtime, everyone else on the runtime
running the loop. Handlers read the parsed identity from the request
extensions and report which thread they ran on.

It is written against two public layers of `connectrpc::server`:

- `Acceptor` — `accept()` yields an `Accepted` connection; `handshake()`
  (run on the connection's own task) terminates TLS and returns the stream
  plus a `ConnectionInfo` with the peer address and verified certificate
  chain.
- `Server::serve_connection(io, info, shutdown)` — a future that serves that
  one connection with the server's service and `ConnectionConfig`; whatever
  the loop put in `info.extensions_mut()` (here, the parsed identity) reaches
  every request on it. Whatever runtime polls it runs the connection.

## What the loop inherits

Everything `serve_connection` does, which is everything `Server::serve` does
per connection: HTTP/1.1 and HTTP/2 (auto-detected), the header-read
timeout, HTTP/2 keepalive and flow-control settings, max connection age /
idle / request-count retirement with their grace period, GOAWAY and drain
when the shutdown future resolves, a Connect `internal` response instead of a
dead connection when a handler panics, and `PeerAddr` / `PeerCerts` /
connection extensions on every request. None of that is re-implemented here.

## What the loop owns

Placement, admission, and two lifetime rules the library cannot enforce for
you:

1. **The loop outlives what it spawned.** It keeps a `JoinSet` of connection
   tasks, and on shutdown stops accepting, signals every connection to drain,
   and waits for the set to empty before returning.
2. **The accepting runtime outlives its connections.** A socket's IO stays
   registered with the runtime that accepted it (`interactive` here) even
   when another runtime (`bulk`) serves the connection. `main.rs` therefore
   drains the loop (rule 1) before dropping either runtime, and the accepting
   runtime is the one everything else runs inside.

## Run it

```bash
cargo run -p custom-accept-loop-example
```

```
PlacementService listening on https://127.0.0.1:PORT (mTLS required)

[frontend] served as "frontend" on a "interactive" thread
[batch-indexer] served as "batch-indexer" on a "bulk" thread
```

`tests/e2e.rs` asserts the placement and that a certificate without a
workload identity is turned away before any HTTP is spoken.

## Where to go next

- [`examples/mtls-identity`](../mtls-identity) reads the same certificate
  identity per request behind the built-in loop (`connectrpc::axum::serve_tls`)
  when you do not need placement or admission.
- The crate guide's "Custom accept loops" section lists other things this seam
  is for: per-tenant connection caps, shedding by source address, serving a
  Unix socket.
