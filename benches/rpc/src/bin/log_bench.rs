//! Log-ingest benchmark: connectrpc-rs (buffa views) vs tonic (prost owned).
//!
//! Decode-heavy workload: each request carries a batch of structured log
//! records (50 by default, ~15 KB encoded). The handler iterates every
//! field on every record — varints, strings, map entries, nested message.
//!
//! This is where the proto library cost becomes visible:
//!   - buffa/connectrpc-rs: zero-copy view decode, no string allocs
//!   - prost/tonic: fully-materialized owned types, ~10 string allocs/record
//!
//! Per 50-record batch that's ~450 varints + ~400 string fields decoded.

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, Http2Connection, SharedHttp2Connection};
use rpc_bench::connect::bench::v1::*;
use rpc_bench::log_request;

const CONCURRENCY_LEVELS: &[usize] = &[16, 64, 256];
const DEFAULT_WARMUP: Duration = Duration::from_secs(3);
const DEFAULT_MEASUREMENT: Duration = Duration::from_secs(10);
const QUICK_WARMUP: Duration = Duration::from_secs(1);
const QUICK_MEASUREMENT: Duration = Duration::from_secs(3);
const DEFAULT_RECORDS: usize = 50;
const MAX_LATENCY_SAMPLES: usize = 500_000;

// ── Server process management ────────────────────────────────────────

struct ServerProcess {
    child: Child,
    addr: SocketAddr,
}

impl ServerProcess {
    fn start(cmd: &str) -> Self {
        let mut child = Command::new(cmd)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to start {cmd}: {e}"));
        let stdout = child.stdout.take().expect("no stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).expect("read addr");
        let addr: SocketAddr = line.trim().parse().expect("parse addr");
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
    eprintln!("  Building connectrpc-rs log server...");
    let output = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "rpc-bench",
            "--bin",
            "log_server",
            "--message-format=short",
        ])
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to build log_server");
    assert!(output.status.success(), "failed to build log_server");
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/../../target/release/log_server")
}

fn build_tonic_server() -> String {
    eprintln!("  Building tonic log server...");
    let output = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "rpc-bench-tonic",
            "--bin",
            "log-server-tonic",
            "--message-format=short",
        ])
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to build log-server-tonic");
    assert!(output.status.success(), "failed to build log-server-tonic");
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/../../target/release/log-server-tonic")
}

fn build_noutf8_server() -> String {
    eprintln!("  Building connectrpc-rs log server (no-utf8)...");
    let output = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "rpc-bench",
            "--bin",
            "log_server_noutf8",
            "--message-format=short",
        ])
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to build log_server_noutf8");
    assert!(output.status.success(), "failed to build log_server_noutf8");
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/../../target/release/log_server_noutf8")
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
    n_conns: usize,
    concurrency: usize,
    records_per_batch: usize,
    warmup: Duration,
    measurement: Duration,
) -> BenchResult {
    let uri: http::Uri = format!("http://{addr}").parse().expect("valid server URL");
    let config = ClientConfig::new(uri.clone()).with_protocol(Protocol::Grpc);

    let mut conns: Vec<SharedHttp2Connection> = Vec::with_capacity(n_conns);
    for _ in 0..n_conns {
        let c = Http2Connection::connect_plaintext(uri.clone())
            .await
            .expect("connect");
        conns.push(c.shared(1024));
    }

    let request = log_request(records_per_batch);

    let running = Arc::new(AtomicBool::new(true));
    let count = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
        MAX_LATENCY_SAMPLES.min(measurement.as_secs() as usize * 50_000),
    )));

    let mut handles = Vec::new();
    for i in 0..concurrency {
        let conn = conns[i % n_conns].clone();
        let client = LogIngestServiceClient::new(conn, config.clone());
        let running = Arc::clone(&running);
        let count = Arc::clone(&count);
        let latencies = Arc::clone(&latencies);
        let request = request.clone();

        handles.push(tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                let start = Instant::now();
                if client.ingest(request.clone()).await.is_ok() {
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

/// Benchmark against the noutf8 server variant.
///
/// Separate from `bench_server` because the client type
/// (`bench::noutf8::v1::LogIngestServiceClient`) and request type differ.
/// The service path is `bench.noutf8.v1.LogIngestService`.
async fn bench_server_noutf8(
    impl_name: &str,
    addr: SocketAddr,
    n_conns: usize,
    concurrency: usize,
    records_per_batch: usize,
    warmup: Duration,
    measurement: Duration,
) -> BenchResult {
    use rpc_bench::connect::bench::noutf8::v1::LogIngestServiceClient;
    use rpc_bench::proto::bench::noutf8;

    let uri: http::Uri = format!("http://{addr}").parse().expect("valid server URL");
    let config = ClientConfig::new(uri.clone()).with_protocol(Protocol::Grpc);

    let mut conns: Vec<SharedHttp2Connection> = Vec::with_capacity(n_conns);
    for _ in 0..n_conns {
        let c = Http2Connection::connect_plaintext(uri.clone())
            .await
            .expect("connect");
        conns.push(c.shared(1024));
    }

    // Build the noutf8 request — same field contents as log_request(),
    // but Vec<u8> for strings. Matches the utf8 request field-for-field
    // so wire payloads are byte-identical.
    let request: noutf8::v1::LogRequest = {
        use std::collections::HashMap;
        let records = (0..records_per_batch)
            .map(|i| {
                let mut labels = HashMap::new();
                for j in 0..6 {
                    labels.insert(
                        format!("label-key-{j}").into_bytes(),
                        format!("label-value-{i}-{j}").into_bytes(),
                    );
                }
                noutf8::v1::LogRecord {
                    timestamp_nanos: Some(1_700_000_000_000_000_000 + i as i64),
                    service_name: Some(b"api-gateway".to_vec()),
                    instance_id: Some(format!("instance-{i:04x}").into_bytes()),
                    severity: Some(noutf8::v1::log_record::Severity::SEVERITY_INFO.into()),
                    message: Some(
                        format!(
                            "Processing request from client {i}: \
                         GET /api/v1/users?page={}&limit=50 completed in {}ms with status 200 OK",
                            i % 100,
                            42 + i % 200
                        )
                        .into_bytes(),
                    ),
                    trace_id: Some(
                        format!("{:032x}", 0xDEAD_BEEF_0000_0000u64 + i as u64).into_bytes(),
                    ),
                    span_id: Some(format!("{:016x}", 0xCAFE_0000u64 + i as u64).into_bytes()),
                    labels,
                    source: noutf8::v1::LogSource {
                        file: Some(format!("src/handlers/user_{}.rs", i % 10).into_bytes()),
                        line: Some(42 + (i as i32 % 500)),
                        function: Some(format!("handle_get_users_{}", i % 5).into_bytes()),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                }
            })
            .collect();
        noutf8::v1::LogRequest {
            records,
            ..Default::default()
        }
    };

    let running = Arc::new(AtomicBool::new(true));
    let count = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
        MAX_LATENCY_SAMPLES.min(measurement.as_secs() as usize * 50_000),
    )));

    let mut handles = Vec::new();
    for i in 0..concurrency {
        let conn = conns[i % n_conns].clone();
        let client = LogIngestServiceClient::new(conn, config.clone());
        let running = Arc::clone(&running);
        let count = Arc::clone(&count);
        let latencies = Arc::clone(&latencies);
        let request = request.clone();

        handles.push(tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                let start = Instant::now();
                if client.ingest(request.clone()).await.is_ok() {
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
    let n_conns: usize = args
        .iter()
        .find_map(|a| a.strip_prefix("--conns="))
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let records: usize = args
        .iter()
        .find_map(|a| a.strip_prefix("--records="))
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_RECORDS);

    let (warmup, measurement) = if quick {
        eprintln!("Running in quick mode (1s warmup, 3s measurement)");
        (QUICK_WARMUP, QUICK_MEASUREMENT)
    } else {
        eprintln!("Running full benchmark (3s warmup, 10s measurement)");
        (DEFAULT_WARMUP, DEFAULT_MEASUREMENT)
    };
    eprintln!("  {n_conns} connections, {records} records per batch");

    // Report encoded batch size for reference.
    let approx_bytes = {
        use buffa::Message;
        log_request(records).encode_to_bytes().len()
    };
    eprintln!("  Batch size: ~{approx_bytes} bytes encoded\n");

    eprintln!("Building servers...");
    let connectrpc_bin = build_connectrpc_server();
    let noutf8_bin = build_noutf8_server();
    let tonic_bin = build_tonic_server();

    let mut results = Vec::new();

    // Benchmark utf8 servers (connectrpc-rs + tonic).
    for (impl_name, bin) in [("connectrpc-rs", &connectrpc_bin), ("tonic", &tonic_bin)] {
        for &concurrency in CONCURRENCY_LEVELS {
            let server = ServerProcess::start(bin);
            eprintln!(
                "  Benchmarking {impl_name} @ concurrency={concurrency} ({n_conns} conns, {records} records)..."
            );
            let result = bench_server(
                impl_name,
                server.addr,
                n_conns,
                concurrency,
                records,
                warmup,
                measurement,
            )
            .await;
            eprintln!(
                "    => {:.0} req/s ({:.0} records/s), p50={:.1}ms, p99={:.1}ms",
                result.rps,
                result.rps * records as f64,
                result.p50_us as f64 / 1000.0,
                result.p99_us as f64 / 1000.0,
            );
            results.push(result);
            drop(server);
        }
    }

    // Benchmark noutf8 connectrpc-rs variant (separate bench fn, different types).
    for &concurrency in CONCURRENCY_LEVELS {
        let server = ServerProcess::start(&noutf8_bin);
        let impl_name = "connectrpc-noutf8";
        eprintln!(
            "  Benchmarking {impl_name} @ concurrency={concurrency} ({n_conns} conns, {records} records)..."
        );
        let result = bench_server_noutf8(
            impl_name,
            server.addr,
            n_conns,
            concurrency,
            records,
            warmup,
            measurement,
        )
        .await;
        eprintln!(
            "    => {:.0} req/s ({:.0} records/s), p50={:.1}ms, p99={:.1}ms",
            result.rps,
            result.rps * records as f64,
            result.p50_us as f64 / 1000.0,
            result.p99_us as f64 / 1000.0,
        );
        results.push(result);
        drop(server);
    }

    println!();
    println!(
        "{:<20} {:>12} {:>14} {:>14} {:>12} {:>12}",
        "Implementation", "Concurrency", "Requests/sec", "Records/sec", "p50 (ms)", "p99 (ms)"
    );
    println!("{}", "-".repeat(90));
    for r in &results {
        println!(
            "{:<20} {:>12} {:>14.0} {:>14.0} {:>12.2} {:>12.2}",
            r.impl_name,
            r.concurrency,
            r.rps,
            r.rps * records as f64,
            r.p50_us as f64 / 1000.0,
            r.p99_us as f64 / 1000.0,
        );
    }
}
