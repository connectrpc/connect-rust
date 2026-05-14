//! Echo benchmark: connectrpc-rs vs tonic, framework overhead only.
//!
//! Unlike fortune_bench, this eliminates the database, spawn_blocking, and
//! complex message encoding from the critical path. The handler just copies
//! a short string from request to response. What remains is:
//!
//!   - HTTP/2 framing (h2 crate — shared between both frameworks)
//!   - Protocol detection + envelope framing
//!   - Dispatch (monomorphic `FooServiceServer<T>` vs tonic's match)
//!   - Proto encode/decode of one string field
//!   - Response building
//!
//! This makes framework-specific overhead a much larger fraction of
//! per-request CPU, so small improvements become visible.

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, Http2Connection, HttpClient, SharedHttp2Connection};
use rpc_bench::connect::bench::v1::*;
use rpc_bench::proto::bench::v1::*;

// ── Configuration ────────────────────────────────────────────────────

const CONCURRENCY_LEVELS: &[usize] = &[16, 64, 256];
const DEFAULT_WARMUP: Duration = Duration::from_secs(3);
const DEFAULT_MEASUREMENT: Duration = Duration::from_secs(10);
const QUICK_WARMUP: Duration = Duration::from_secs(1);
const QUICK_MEASUREMENT: Duration = Duration::from_secs(3);
const MAX_LATENCY_SAMPLES: usize = 500_000;

/// A 64-byte payload — long enough to hit a realistic proto-encoding code
/// path (length-delimited string, not the tiny-inline optimization) but
/// short enough that memcpy doesn't dominate.
const PAYLOAD: &str = "lorem ipsum dolor sit amet, consectetur adipiscing elit sed do e";

// ── Server process management ────────────────────────────────────────

struct ServerProcess {
    child: Child,
    addr: SocketAddr,
}

impl ServerProcess {
    fn start(cmd: &str, args: &[&str]) -> Self {
        let mut child = Command::new(cmd)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to start {cmd}: {e}"));

        let stdout = child.stdout.take().expect("no stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let bytes_read = reader
            .read_line(&mut line)
            .expect("failed to read server address");
        if bytes_read == 0 {
            panic!("server {cmd} exited before printing its address");
        }
        let addr: SocketAddr = line.trim().parse().unwrap_or_else(|e| {
            panic!("failed to parse server address from {cmd} ({line:?}): {e}")
        });

        std::thread::sleep(Duration::from_millis(50));

        Self { child, addr }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ── Build helpers ────────────────────────────────────────────────────

fn build_connectrpc_server() -> String {
    eprintln!("  Building connectrpc-rs echo server...");
    let output = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "rpc-bench",
            "--bin",
            "echo_server",
            "--message-format=short",
        ])
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to build echo_server");
    assert!(output.status.success(), "failed to build echo_server");

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/../../target/release/echo_server")
}

fn build_tonic_server() -> String {
    eprintln!("  Building tonic echo server...");
    let output = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "rpc-bench-tonic",
            "--bin",
            "echo-server-tonic",
            "--message-format=short",
        ])
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to build echo-server-tonic");
    assert!(output.status.success(), "failed to build echo-server-tonic");

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/../../target/release/echo-server-tonic")
}

// ── Benchmark result ─────────────────────────────────────────────────

struct BenchResult {
    impl_name: String,
    concurrency: usize,
    rps: f64,
    p50_us: u64,
    p99_us: u64,
}

// ── Benchmark runner ─────────────────────────────────────────────────

async fn bench_server(
    impl_name: &str,
    addr: SocketAddr,
    protocol: Protocol,
    concurrency: usize,
    warmup: Duration,
    measurement: Duration,
) -> BenchResult {
    let config = ClientConfig::new(format!("http://{addr}").parse().expect("valid server URL"))
        .with_protocol(protocol);
    let http = if protocol.requires_http2() {
        HttpClient::plaintext_http2_only()
    } else {
        HttpClient::plaintext()
    };

    let running = Arc::new(AtomicBool::new(true));
    let count = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
        MAX_LATENCY_SAMPLES.min(measurement.as_secs() as usize * 50_000),
    )));

    let request = EchoRequest {
        message: PAYLOAD.to_string(),
        ..Default::default()
    };

    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let client = EchoServiceClient::new(http.clone(), config.clone());
        let running = Arc::clone(&running);
        let count = Arc::clone(&count);
        let latencies = Arc::clone(&latencies);
        let request = request.clone();

        handles.push(tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                let start = Instant::now();
                if client.echo(request.clone()).await.is_ok() {
                    let elapsed = start.elapsed();
                    let n = count.fetch_add(1, Ordering::Relaxed);
                    // Sample every 10th request to reduce lock contention,
                    // capped to avoid unbounded growth.
                    if n.is_multiple_of(10) {
                        let mut lats = latencies.lock().await;
                        if lats.len() < MAX_LATENCY_SAMPLES {
                            lats.push(elapsed.as_micros() as u64);
                        }
                    }
                }
            }
        }));
    }

    // Warmup phase.
    tokio::time::sleep(warmup).await;
    count.store(0, Ordering::Relaxed);
    latencies.lock().await.clear();
    let measure_start = Instant::now();

    // Measurement phase.
    tokio::time::sleep(measurement).await;
    running.store(false, Ordering::Relaxed);
    let elapsed = measure_start.elapsed();

    for h in handles {
        let _ = h.await;
    }

    let total = count.load(Ordering::Relaxed);
    let rps = total as f64 / elapsed.as_secs_f64();

    let mut lats = latencies.lock().await;
    lats.sort_unstable();
    let p50 = if lats.is_empty() {
        0
    } else {
        lats[lats.len() / 2]
    };
    let p99 = if lats.is_empty() {
        0
    } else {
        lats[lats.len() * 99 / 100]
    };

    BenchResult {
        impl_name: impl_name.to_string(),
        concurrency,
        rps,
        p50_us: p50,
        p99_us: p99,
    }
}

/// Benchmark using N `SharedHttp2Connection` instances distributed across
/// worker tasks round-robin. This spreads requests across N separate h2
/// connections, reducing contention on each connection's `Mutex<Inner>`
/// to ~1/N.
///
/// Uses gRPC protocol (HTTP/2) unconditionally — `Http2Connection` is
/// h2-only by design.
#[allow(clippy::too_many_arguments)]
async fn bench_server_multiconn(
    impl_name: &str,
    addr: SocketAddr,
    n_conns: usize,
    concurrency: usize,
    warmup: Duration,
    measurement: Duration,
) -> BenchResult {
    let uri: http::Uri = format!("http://{addr}").parse().expect("valid server URL");
    let config = ClientConfig::new(uri.clone()).with_protocol(Protocol::Grpc);

    // Establish N connections eagerly so warmup doesn't include handshakes.
    let mut conns: Vec<SharedHttp2Connection> = Vec::with_capacity(n_conns);
    for _ in 0..n_conns {
        let c = Http2Connection::connect_plaintext(uri.clone())
            .await
            .expect("connect");
        conns.push(c.shared(1024));
    }

    let running = Arc::new(AtomicBool::new(true));
    let count = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
        MAX_LATENCY_SAMPLES.min(measurement.as_secs() as usize * 100_000),
    )));

    let request = EchoRequest {
        message: PAYLOAD.to_string(),
        ..Default::default()
    };

    let mut handles = Vec::new();
    for worker_idx in 0..concurrency {
        // Round-robin: worker i uses connection i % N. This gives an even
        // static distribution without per-request coordination overhead.
        let conn = conns[worker_idx % n_conns].clone();
        let client = EchoServiceClient::new(conn, config.clone());
        let running = Arc::clone(&running);
        let count = Arc::clone(&count);
        let latencies = Arc::clone(&latencies);
        let request = request.clone();

        handles.push(tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                let start = Instant::now();
                if client.echo(request.clone()).await.is_ok() {
                    let elapsed = start.elapsed();
                    let n = count.fetch_add(1, Ordering::Relaxed);
                    if n.is_multiple_of(10) {
                        let mut lats = latencies.lock().await;
                        if lats.len() < MAX_LATENCY_SAMPLES {
                            lats.push(elapsed.as_micros() as u64);
                        }
                    }
                }
            }
        }));
    }

    tokio::time::sleep(warmup).await;
    count.store(0, Ordering::Relaxed);
    latencies.lock().await.clear();
    let measure_start = Instant::now();

    tokio::time::sleep(measurement).await;
    running.store(false, Ordering::Relaxed);
    let elapsed = measure_start.elapsed();

    for h in handles {
        let _ = h.await;
    }

    let total = count.load(Ordering::Relaxed);
    let rps = total as f64 / elapsed.as_secs_f64();

    let mut lats = latencies.lock().await;
    lats.sort_unstable();
    let p50 = if lats.is_empty() {
        0
    } else {
        lats[lats.len() / 2]
    };
    let p99 = if lats.is_empty() {
        0
    } else {
        lats[lats.len() * 99 / 100]
    };

    BenchResult {
        impl_name: impl_name.to_string(),
        concurrency,
        rps,
        p50_us: p50,
        p99_us: p99,
    }
}

// ── Main ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let quick = args.iter().any(|a| a == "--quick");
    // --multi-conn=N runs an additional pass with N h2 connections
    // per server. Pass --multi-conn=0 (or omit) to skip.
    let multi_conn: usize = args
        .iter()
        .find_map(|a| a.strip_prefix("--multi-conn="))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let (warmup, measurement) = if quick {
        eprintln!("Running in quick mode (1s warmup, 3s measurement)");
        (QUICK_WARMUP, QUICK_MEASUREMENT)
    } else {
        eprintln!("Running full benchmark (3s warmup, 10s measurement)");
        (DEFAULT_WARMUP, DEFAULT_MEASUREMENT)
    };

    eprintln!("\nBuilding servers...");
    let connectrpc_bin = build_connectrpc_server();
    let tonic_bin = build_tonic_server();

    let servers = [
        ("connectrpc-rs", &connectrpc_bin, Protocol::Grpc),
        ("tonic", &tonic_bin, Protocol::Grpc),
    ];

    let mut results = Vec::new();

    for &(impl_name, bin, ref protocol) in &servers {
        for &concurrency in CONCURRENCY_LEVELS {
            let server = ServerProcess::start(bin, &[]);
            eprintln!(
                "  Benchmarking {impl_name} @ concurrency={concurrency} (warmup {warmup:?}, measure {measurement:?})..."
            );
            let result = bench_server(
                impl_name,
                server.addr,
                *protocol,
                concurrency,
                warmup,
                measurement,
            )
            .await;
            eprintln!(
                "    => {:.0} req/s, p50={:.1}ms, p99={:.1}ms",
                result.rps,
                result.p50_us as f64 / 1000.0,
                result.p99_us as f64 / 1000.0,
            );
            results.push(result);
            drop(server);
        }
    }

    // Optional multi-connection pass.
    if multi_conn > 0 {
        eprintln!("\nMulti-connection pass: {multi_conn} h2 connections per server...");
        for &(impl_name, bin, _) in &servers {
            for &concurrency in CONCURRENCY_LEVELS {
                let server = ServerProcess::start(bin, &[]);
                let label = format!("{impl_name} ({multi_conn}-conn)");
                eprintln!("  Benchmarking {label} @ concurrency={concurrency}...");
                let result = bench_server_multiconn(
                    &label,
                    server.addr,
                    multi_conn,
                    concurrency,
                    warmup,
                    measurement,
                )
                .await;
                eprintln!(
                    "    => {:.0} req/s, p50={:.1}ms, p99={:.1}ms",
                    result.rps,
                    result.p50_us as f64 / 1000.0,
                    result.p99_us as f64 / 1000.0,
                );
                results.push(result);
                drop(server);
            }
        }
    }

    // Print results table.
    println!();
    println!(
        "{:<24} {:>12} {:>14} {:>12} {:>12}",
        "Implementation", "Concurrency", "Requests/sec", "p50 (ms)", "p99 (ms)"
    );
    println!("{}", "-".repeat(78));
    for r in &results {
        println!(
            "{:<24} {:>12} {:>14.0} {:>12.2} {:>12.2}",
            r.impl_name,
            r.concurrency,
            r.rps,
            r.p50_us as f64 / 1000.0,
            r.p99_us as f64 / 1000.0,
        );
    }
}
