//! Standalone load generator for profiling echo servers.
//!
//! Usage: `echo_load <addr> [duration_secs] [concurrency] [n_conns]`
//!
//! Uses gRPC (HTTP/2) always. `n_conns` spreads load across N
//! SharedHttp2Connection instances to reduce h2 mutex contention,
//! making framework overhead visible in profiles.

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, Http2Connection, SharedHttp2Connection};
use rpc_bench::connect::bench::v1::*;
use rpc_bench::proto::bench::v1::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const PAYLOAD: &str = "lorem ipsum dolor sit amet, consectetur adipiscing elit sed do e";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args
        .get(1)
        .expect("usage: echo_load <addr> [duration] [concurrency] [n_conns]");
    let duration = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(30u64);
    let concurrency: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);
    let n_conns: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(8);

    let uri: http::Uri = format!("http://{addr}").parse().unwrap();
    let config = ClientConfig::new(uri.clone()).with_protocol(Protocol::Grpc);

    eprintln!(
        "Generating load: {concurrency} tasks × {n_conns} connections for {duration}s against {addr}"
    );

    // Establish N connections eagerly.
    let mut conns: Vec<SharedHttp2Connection> = Vec::with_capacity(n_conns);
    for _ in 0..n_conns {
        let c = Http2Connection::connect_plaintext(uri.clone())
            .await
            .expect("connect");
        conns.push(c.shared(1024));
    }

    let request = EchoRequest {
        message: PAYLOAD.to_string(),
        ..Default::default()
    };

    let running = Arc::new(AtomicBool::new(true));
    let count = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for i in 0..concurrency {
        let conn = conns[i % n_conns].clone();
        let client = EchoServiceClient::new(conn, config.clone());
        let running = Arc::clone(&running);
        let count = Arc::clone(&count);
        let request = request.clone();
        handles.push(tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if client.echo(request.clone()).await.is_ok() {
                    count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    tokio::time::sleep(std::time::Duration::from_secs(duration)).await;
    running.store(false, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }

    let total = count.load(Ordering::Relaxed);
    eprintln!(
        "Completed {total} requests ({:.0} req/s)",
        total as f64 / duration as f64
    );
}
