# rpc-bench-grpc-rust

Benchmark servers and a client-stack benchmark built on [grpc-rust](https://github.com/grpc/grpc-rust), Google's gRPC implementation for Rust, which builds on tonic. `publish = false`; nothing here ships.

grpc-rust's `grpc` crate (0.9.0 preview on crates.io) is a client-side channel only, and its own benchmarks pair that client with a tonic server. The part of grpc-rust that changes what a server does is `tonic-protobuf`: a tonic codec over Google's official `protobuf` v4 Rust runtime, which wraps the upb C kernel and parses each message eagerly into a per-message arena. The server binaries here are the same echo, log-ingest, fortunes and `BenchService` handlers as `benches/rpc-tonic`, with prost swapped for that codec, so the bench drivers in `benches/rpc` report them as `tonic-protobuf` next to `tonic` (prost) and `connectrpc-rs` (buffa). One caveat when reading those two tonic rows against each other: `benches/rpc-tonic` builds tonic 0.14 from crates.io, while this crate builds tonic from the pinned grpc-rust revision, so the rows differ in tonic source as well as in codec.

`client_bench` is the inverse experiment. It holds the server constant (the connectrpc-rs echo server unless `--server-bin=PATH` says otherwise) and drives it with each client stack in turn — connectrpc-rs over hyper-util's pooled client, connectrpc-rs over its own `Http2Connection`, tonic's `Channel` with prost, and grpc-rust's `grpc::client::Channel` with grpc-protobuf stubs — because a client benchmark is the only way to compare against the `grpc` crate at all.

| Binary | Driven by |
|---|---|
| `bench-server-tonic-protobuf` | `cargo bench -p rpc-bench --bench cross_impl_bench` |
| `echo-server-tonic-protobuf` | `task bench:echo` |
| `log-server-tonic-protobuf` | `task bench:log` |
| `fortune-server-tonic-protobuf` | `task bench:fortunes` (needs docker for valkey) |
| `client_bench` | `task bench:clients -- [--quick] [--conns=1,8] [--repeat=3] [--server-bin=PATH]` |

## Why this crate is outside the workspace

The root `Cargo.toml` excludes this directory, and the bench drivers build it on demand with `cargo build --release --manifest-path benches/rpc-grpc-rust/Cargo.toml`, so it has its own `target/`. Code generation needs `protoc` 35.1 exactly (the `protobuf-codegen` crate refuses any other version) plus the C++ `protoc-gen-rust-grpc` plugin, and `tonic-protobuf` is unpublished, so every grpc-rust crate is a git dependency pinned to one revision. Keeping that toolchain out of the workspace means CI and ordinary contributors never need it.

## Toolchain

By default the first build compiles `protoc` and the plugin from C++ source through grpc-rust's `build-plugin` feature. That needs `cmake` ≥ 3.14, a C++17 compiler, and network access, because the cmake project downloads the protobuf and abseil sources; it takes about a minute on a 20-core machine and is cached in `target/` afterwards. The same `protoc` is handed to `tonic-prost-build` for the tonic client stubs, so no system `protoc` is needed. The `protobuf` runtime crate also compiles upb with `cc`, so a C compiler is always required.

To use prebuilt binaries instead, put `protoc` (35.1) and `protoc-gen-rust-grpc` in one directory and export `GRPC_RUST_PROTOC_DIR` pointing at it. The bench drivers and `benches/profile_server.sh` then build this crate with `--no-default-features`, which skips the cmake build; a manual build needs the flag spelled out:

```bash
GRPC_RUST_PROTOC_DIR=/path/to/dir cargo build --release --no-default-features \
  --manifest-path benches/rpc-grpc-rust/Cargo.toml
```

## Bumping grpc-rust

Every grpc-rust git dependency in `Cargo.toml` must move to the same revision together, and the `protobuf = "=…"` pin must match the `protobuf-codegen` version that revision's `grpc-protobuf-build` uses, because the generated message code names `::protobuf` directly. When grpc-rust publishes `tonic-protobuf`, switch all of them to crates.io versions.
