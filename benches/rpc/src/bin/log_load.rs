//! Standalone load generator for log-ingest profiling.
//!
//! Usage: `log_load <addr> [duration_secs] [concurrency] [n_conns] [records_per_batch]`
//!
//! Sends batches of `LogRecord`s against the ingest endpoint over gRPC.
//! Uses N `SharedHttp2Connection` instances round-robin to reduce h2
//! mutex contention so proto-library overhead is visible in profiles.
//!
//! Each record is ~200–300 bytes encoded (9 string fields, 1 map with
//! 6 entries, 2 varints, 1 enum) — a 50-record batch is ~10–15 KB.
//! That's ~450 varints and ~400 string fields to decode per request.

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, Http2Connection, SharedHttp2Connection};
use rpc_bench::connect::bench::v1::*;
use rpc_bench::log_request;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args
        .get(1)
        .expect("usage: log_load <addr> [duration] [concurrency] [n_conns] [records_per_batch]");
    let duration = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(30u64);
    let concurrency: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);
    let n_conns: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(8);
    let records_per_batch: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(50);

    let uri: http::Uri = format!("http://{addr}").parse().unwrap();
    let config = ClientConfig::new(uri.clone()).with_protocol(Protocol::Grpc);

    // Pre-build the request once and clone per RPC. The cloning cost is
    // client-side only; the server's decode work is what we're profiling.
    let request = log_request(records_per_batch);
    let approx_size = {
        use buffa::Message;
        request.encode_to_bytes().len()
    };

    eprintln!(
        "Generating load: {concurrency} tasks × {n_conns} connections for {duration}s against {addr}"
    );
    eprintln!("  Batch: {records_per_batch} records (~{approx_size} bytes encoded)");

    // Establish N connections eagerly.
    let mut conns: Vec<SharedHttp2Connection> = Vec::with_capacity(n_conns);
    for _ in 0..n_conns {
        let c = Http2Connection::connect_plaintext(uri.clone())
            .await
            .expect("connect");
        conns.push(c.shared(1024));
    }

    let running = Arc::new(AtomicBool::new(true));
    let count = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for i in 0..concurrency {
        let conn = conns[i % n_conns].clone();
        let client = LogIngestServiceClient::new(conn, config.clone());
        let running = Arc::clone(&running);
        let count = Arc::clone(&count);
        let request = request.clone();
        handles.push(tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if client.ingest(request.clone()).await.is_ok() {
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
        "Completed {total} requests ({:.0} req/s, {:.0} records/s)",
        total as f64 / duration as f64,
        (total * records_per_batch as u64) as f64 / duration as f64,
    );
}
