//! Benchmark servers built on grpc-rust's `tonic-protobuf` codec (Google's
//! `protobuf` v4 runtime on the upb kernel) for comparison against
//! connectrpc-rs + buffa and tonic + prost.

use std::net::{Ipv4Addr, SocketAddr};

use tonic::transport::server::TcpIncoming;

/// Google-`protobuf` message types, grpc-rust client stubs and
/// tonic-protobuf server stubs for `bench.v1` and `fortune.v1`.
/// grpc-protobuf-build flattens every input proto's messages into one
/// `generated.rs`, so both packages share this namespace: `include_proto!`
/// pulls in that file plus `bench_grpc.pb.rs`, and only the per-proto service
/// file is left to include for `fortune`.
#[allow(
    unreachable_pub,
    missing_debug_implementations,
    clippy::all,
    clippy::pedantic,
    rustdoc::all
)]
pub mod pb {
    grpc::include_proto!("bench");
    include!(concat!(env!("OUT_DIR"), "/fortune_grpc.pb.rs"));
}

/// Binds an ephemeral loopback port with `TCP_NODELAY` and prints the
/// address on stdout, which is how the bench drivers in `benches/rpc`
/// discover every server they spawn.
///
/// # Errors
///
/// Returns the bind or `local_addr` failure.
pub fn listen() -> std::io::Result<TcpIncoming> {
    let incoming =
        TcpIncoming::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?.with_nodelay(Some(true));
    println!("{}", incoming.local_addr()?);
    Ok(incoming)
}

/// Resolves when the process receives Ctrl-C, for
/// `serve_with_incoming_shutdown`.
pub async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
