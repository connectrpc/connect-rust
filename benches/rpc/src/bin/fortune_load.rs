//! Standalone load generator for profiling fortune servers.
//!
//! Usage: `fortune_load <addr> [duration_secs] [concurrency] [protocol]`
//!   protocol: "connect" (default) or "grpc"

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, HttpClient};
use rpc_bench::connect::fortune::v1::*;
use rpc_bench::proto::fortune::v1::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args
        .get(1)
        .expect("usage: fortune_load <addr> [duration] [concurrency] [connect|grpc]");
    let duration = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(15u64);
    let concurrency: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);
    let protocol = match args.get(4).map(|s| s.as_str()) {
        Some("grpc") => Protocol::Grpc,
        _ => Protocol::Connect,
    };

    let config =
        ClientConfig::new(format!("http://{addr}").parse().unwrap()).with_protocol(protocol);
    let http = if protocol.requires_http2() {
        HttpClient::plaintext_http2_only()
    } else {
        HttpClient::plaintext()
    };

    eprintln!("Generating load: {concurrency} tasks for {duration}s against {addr}");

    let running = Arc::new(AtomicBool::new(true));
    let count = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let client = FortuneServiceClient::new(http.clone(), config.clone());
        let running = Arc::clone(&running);
        let count = Arc::clone(&count);
        handles.push(tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if client
                    .get_fortunes(GetFortunesRequest::default())
                    .await
                    .is_ok()
                {
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
