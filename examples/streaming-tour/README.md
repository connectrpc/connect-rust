# Streaming tour example

A minimal `NumberService` that demonstrates all four ConnectRPC RPC types
in one place. The methods are deliberately trivial - the example exists
to show the wire-protocol shapes (handler signatures, client invocation
patterns), not to be useful.

| RPC | Type | Semantics |
|---|---|---|
| `Square` | Unary | n -> n*n |
| `Range` | Server streaming | emit `count` consecutive integers from `start` |
| `Sum` | Client streaming | total all values, return when client closes its send side |
| `RunningSum` | Bidirectional streaming | after each request, emit the running total |

## Run it

```bash
# Terminal 1: server (listens on 127.0.0.1:8080)
cargo run -p streaming-tour-example --bin streaming-tour-server

# Terminal 2: client (calls each RPC, prints results)
cargo run -p streaming-tour-example --bin streaming-tour-client
```

Expected client output:

```
Square(7) -> 49
Range(start=10, count=5) -> [10, 11, 12, 13, 14]
Sum([3, 5, 7, 9]) -> 24
RunningSum([2, 4, 6, 8]) -> [2, 6, 12, 20]
```

The client connects to `http://127.0.0.1:8080` by default; set
`TOUR_URL` to point at a different address.

## Integration test

`tests/e2e.rs` reuses the same handler implementation, spins up the
server in-process on a random port, and exercises every RPC type:

```bash
cargo test -p streaming-tour-example
```

## What to look at

- **`proto/anthropic/connectrpc/tour/v1/number.proto`** - service definition with
  one example of each RPC type.
- **`src/server.rs`** - handler signatures for each kind. Note how the
  request type changes: unary takes `OwnedView<RequestView<'static>>`,
  client/bidi take a `Pin<Box<dyn Stream<Item = Result<OwnedView<...>>>>>`.
- **`src/client.rs`** - generated-client invocation patterns. The
  client-streaming `Sum` takes an async `Stream<Item = SumRequest>`
  (a ready collection is adapted with `connectrpc::stream_iter`);
  the bidi `RunningSum` returns a stream you `.send()` to and
  `.message()` from interleaved.

## Where to go next

- See [`examples/eliza`](../eliza) for a real-feeling streaming app
  (a port of the `connectrpc/examples-go` ELIZA demo, with TLS, mTLS,
  CORS, and IPv6 support).
- See [`examples/middleware`](../middleware) for tower middleware
  composition (custom auth layer, request tracing, timeouts) on the
  server side.
