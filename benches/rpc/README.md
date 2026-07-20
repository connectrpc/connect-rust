# rpc-bench

Benchmark crate for `connectrpc`. `publish = false`; nothing here ships.

## Benches

| Bench | What it measures | Run |
|---|---|---|
| `rpc_bench` | Full-stack unary/stream RPC over loopback HTTP, Connect/gRPC/gRPC-Web × proto/JSON. Needs a running server (`cargo run --bin echo_server` etc.). | `cargo bench --bench rpc_bench` |
| `cross_impl_bench` | Cross-implementation comparison vs tonic. | `cargo bench --bench cross_impl_bench` |
| `echo_bloat` | Codec-layer (no HTTP) `{owned,view}×{decode,encode}` sweep across five payload shapes + a 1→N fanout sweep. Motivates the future view-response handler API. | `cargo bench --bench echo_bloat` |
| `view_rope_encode` | Encode cost of a response view, contiguous vs a rope backed by the view's own buffer, swept either side of the segment threshold. Shows what the segmented response path buys and what it costs below the threshold. | `cargo bench --bench view_rope_encode` |

Filter by criterion regex: `cargo bench --bench echo_bloat -- fanout` or `-- map_dominated`.

## Load-gen binaries

`{echo,log,fortune}_server` / `_load` / `_bench` are standalone server +
client load generators for ad-hoc throughput testing. The `_noutf8`
variants exercise buffa's `respect_utf8_validation_feature(true)` mode.

## echo_bloat: adding a payload shape

See the "Adding a new shape" comment in `benches/echo_bloat.rs`. Short
version: add a message to `proto/echo_bloat.proto`, regenerate, add a
`mod <name>` with the five `&[u8] -> Vec<u8>` path fns, then a
`bench_shape!` invocation. The per-field struct construction in each path
fn is intentionally explicit — it makes the alloc/copy/borrow cost of
each variant readable in source.
