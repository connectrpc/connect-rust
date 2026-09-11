//! Standalone closed-loop load generator for `BenchService.Unary` with the
//! `small_request()` payload — the same call `cross_impl_bench`'s
//! `unary_small_grpc` measures — for profiling a server (or this client) under
//! `perf`/`strace` without criterion in the picture.
//!
//! Usage: `unary_load <addr> [duration_secs] [concurrency] [hyper|h2]`
//!
//! `hyper` (default) uses `HttpClient::plaintext_http2_only()`, exactly as the
//! criterion bench does; `h2` uses one `Http2Connection` shared by all tasks.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, Http2Connection, HttpClient};
use rpc_bench::{BenchServiceClient, small_request};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args
        .get(1)
        .expect("usage: unary_load <addr> [duration] [concurrency] [hyper|h2]");
    let duration = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(30u64);
    let concurrency: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
    let transport = args.get(4).map_or("hyper", String::as_str);

    let uri: http::Uri = format!("http://{addr}").parse().expect("valid addr");
    let config = ClientConfig::new(uri.clone()).with_protocol(Protocol::Grpc);
    let request = small_request();

    eprintln!(
        "unary_load: {concurrency} task(s), {transport} transport, {duration}s against {addr}"
    );

    let running = Arc::new(AtomicBool::new(true));
    let count = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    // One call site per transport keeps the client types monomorphic.
    macro_rules! spawn_workers {
        ($client:expr) => {
            for _ in 0..concurrency {
                let client = $client.clone();
                let running = Arc::clone(&running);
                let count = Arc::clone(&count);
                let request = request.clone();
                handles.push(tokio::spawn(async move {
                    while running.load(Ordering::Relaxed) {
                        if client.unary(request.clone()).await.is_ok() {
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }));
            }
        };
    }

    match transport {
        "h2" => {
            let conn = Http2Connection::connect_plaintext(uri)
                .await
                .expect("connect")
                .shared(1024);
            let client = BenchServiceClient::new(conn, config.clone());
            spawn_workers!(client);
        }
        _ => {
            let client = BenchServiceClient::new(HttpClient::plaintext_http2_only(), config);
            spawn_workers!(client);
        }
    }

    let start = Instant::now();
    tokio::time::sleep(Duration::from_secs(duration)).await;
    running.store(false, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let total = count.load(Ordering::Relaxed);
    eprintln!(
        "{total} requests in {elapsed:.1}s = {:.0} req/s, {:.1} us/req at c={concurrency}",
        total as f64 / elapsed,
        elapsed * 1e6 / total.max(1) as f64 * concurrency as f64
    );
}
