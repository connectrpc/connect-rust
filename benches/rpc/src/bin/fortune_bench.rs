//! Fortunes benchmark: connectrpc-rs vs tonic vs connect-go.
//!
//! Adapted from the TechEmpower Web Framework Benchmarks. Measures a realistic
//! workload: network round-trip to a valkey backing store, string processing,
//! sorting, and response encoding via a single unary RPC. A valkey container
//! is spawned as a sibling process and shared across all server runs.

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, Http2Connection, HttpClient, SharedHttp2Connection};
use rpc_bench::connect::fortune::v1::*;
use rpc_bench::fortune;
use rpc_bench::proto::fortune::v1::*;

// ── Configuration ────────────────────────────────────────────────────

// c=256 is the sweet spot: enough concurrency to reveal framing differences,
// not so much that the h2 mutex ceiling (≈ multi_conn × ~33k) compresses
// everyone together. --high-c extends to 512 for ceiling-probing.
const CONCURRENCY_LEVELS: &[usize] = &[16, 64, 256];
const HIGH_CONCURRENCY_LEVELS: &[usize] = &[16, 64, 256, 512];
const DEFAULT_WARMUP: Duration = Duration::from_secs(3);
const DEFAULT_MEASUREMENT: Duration = Duration::from_secs(10);
const QUICK_WARMUP: Duration = Duration::from_secs(1);
const QUICK_MEASUREMENT: Duration = Duration::from_secs(3);
const MAX_LATENCY_SAMPLES: usize = 500_000;

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

// ── Valkey container ────────────────────────────────────────────────

/// Managed valkey container: spawns via `docker run` on an ephemeral host
/// port, killed via `docker rm -f` on Drop. Shared by all server processes
/// in a single benchmark run.
struct ValkeyContainer {
    name: String,
    addr: String,
}

impl ValkeyContainer {
    fn start() -> Self {
        let name = format!("valkey-bench-{}", std::process::id());
        let run = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "-p",
                "127.0.0.1::6379",
                "--name",
                &name,
                "valkey/valkey:8-alpine",
            ])
            .output()
            .expect("failed to docker run valkey");
        assert!(
            run.status.success(),
            "docker run valkey failed: {}",
            String::from_utf8_lossy(&run.stderr)
        );

        let port = Command::new("docker")
            .args(["port", &name, "6379"])
            .output()
            .expect("failed to docker port");
        assert!(port.status.success(), "docker port failed");
        // `docker port` may emit both IPv4 and IPv6 mappings on
        // dual-stack daemons; take the first line.
        let addr = String::from_utf8_lossy(&port.stdout)
            .lines()
            .next()
            .expect("docker port returned no output")
            .trim()
            .to_string();

        Self { name, addr }
    }

    /// Poll until valkey responds, then load the fortunes hash. Retries
    /// for a few seconds to accommodate container startup.
    async fn seed(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut conn = loop {
            match fortune::connect(&self.addr).await {
                Ok(c) => break c,
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => panic!("valkey at {} not ready after 5s: {e}", self.addr),
            }
        };
        fortune::seed(&mut conn)
            .await
            .expect("failed to seed valkey");
    }
}

impl Drop for ValkeyContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

// ── Build helpers ────────────────────────────────────────────────────

fn build_connectrpc_server() -> String {
    eprintln!("  Building connectrpc-rs fortune server...");
    let output = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "rpc-bench",
            "--bin",
            "fortune_server",
            "--message-format=short",
        ])
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to build fortune_server");
    assert!(output.status.success(), "failed to build fortune_server");

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/../../target/release/fortune_server")
}

fn build_tonic_server() -> String {
    eprintln!("  Building tonic fortune server...");
    let output = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "rpc-bench-tonic",
            "--bin",
            "fortune-server-tonic",
            "--message-format=short",
        ])
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to build fortune-server-tonic");
    assert!(
        output.status.success(),
        "failed to build fortune-server-tonic"
    );

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/../../target/release/fortune-server-tonic")
}

fn build_connect_go_server() -> String {
    eprintln!("  Building connect-go fortune server...");
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let go_dir = format!("{manifest_dir}/../rpc-go");
    let bin_path = format!("{go_dir}/fortune-connect-go");

    let output = Command::new("go")
        .args(["build", "-o", &bin_path, "./cmd/fortune"])
        .current_dir(&go_dir)
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to build connect-go fortune server");
    assert!(
        output.status.success(),
        "failed to build connect-go fortune server"
    );

    bin_path
}

// ── Benchmark result ─────────────────────────────────────────────────

struct BenchResult {
    impl_name: String,
    protocol: Protocol,
    concurrency: usize,
    rps: f64,
    p50_us: u64,
    p99_us: u64,
}

fn protocol_label(p: Protocol) -> &'static str {
    match p {
        Protocol::Connect => "connect",
        Protocol::Grpc => "grpc",
        Protocol::GrpcWeb => "grpc-web",
        // Protocol is #[non_exhaustive] — future variants render as their
        // Display form is opaque, but we only bench the three we know.
        _ => "?",
    }
}

// ── Benchmark runner ─────────────────────────────────────────────────

/// Run a benchmark pass against `addr` using the given wire protocol.
///
/// The connectrpc-rs server handles all three protocols transparently
/// (protocol is detected from Content-Type). tonic only speaks gRPC;
/// connect-go speaks Connect and gRPC (not gRPC-Web without a proxy).
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
        MAX_LATENCY_SAMPLES.min(measurement.as_secs() as usize * 20_000),
    )));

    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let client = FortuneServiceClient::new(http.clone(), config.clone());
        let running = Arc::clone(&running);
        let count = Arc::clone(&count);
        let latencies = Arc::clone(&latencies);

        handles.push(tokio::spawn(async move {
            // Relaxed is sufficient: this is a stop signal only; the join below
            // provides the necessary synchronization.
            while running.load(Ordering::Relaxed) {
                let start = Instant::now();
                if client
                    .get_fortunes(GetFortunesRequest::default())
                    .await
                    .is_ok()
                {
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
        protocol,
        concurrency,
        rps,
        p50_us: p50,
        p99_us: p99,
    }
}

/// Run the same benchmark using N `SharedHttp2Connection` instances
/// round-robin distributed across workers, forcing **all** protocols onto
/// identical multi-connection HTTP/2 transport.
///
/// This is the "fair" protocol comparison: with the single-client
/// `bench_server` path, gRPC uses a single h2 connection (mutex ceiling)
/// while Connect/gRPC-Web use HTTP/1.1 with hyper's pool (N connections,
/// no contention) — the 3× gap that shows up there is a transport artifact,
/// not a framing one. Pinning all three to the same N-h2-connection transport
/// isolates the actual wire-protocol overhead.
#[allow(clippy::too_many_arguments)]
async fn bench_server_multiconn(
    impl_name: &str,
    addr: SocketAddr,
    protocol: Protocol,
    n_conns: usize,
    concurrency: usize,
    warmup: Duration,
    measurement: Duration,
) -> BenchResult {
    let uri: http::Uri = format!("http://{addr}").parse().expect("valid server URL");
    let config = ClientConfig::new(uri.clone()).with_protocol(protocol);

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
        MAX_LATENCY_SAMPLES.min(measurement.as_secs() as usize * 50_000),
    )));

    let mut handles = Vec::new();
    for worker_idx in 0..concurrency {
        // Round-robin: worker i uses connection i % N.
        let conn = conns[worker_idx % n_conns].clone();
        let client = FortuneServiceClient::new(conn, config.clone());
        let running = Arc::clone(&running);
        let count = Arc::clone(&count);
        let latencies = Arc::clone(&latencies);

        handles.push(tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                let start = Instant::now();
                if client
                    .get_fortunes(GetFortunesRequest::default())
                    .await
                    .is_ok()
                {
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
        protocol,
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
    // --protocols runs all three wire protocols against connectrpc-rs
    // (and Connect+gRPC against connect-go). tonic only speaks gRPC.
    // Default (omitted): gRPC only, for the classic cross-impl comparison.
    let all_protocols = args.iter().any(|a| a == "--protocols");
    // --multi-conn=N forces all protocols onto N HTTP/2 connections via
    // Http2Connection + round-robin. This gives an apples-to-apples
    // protocol framing comparison by removing the single-h2-connection
    // mutex ceiling that otherwise penalizes gRPC.
    let multi_conn: usize = args
        .iter()
        .find_map(|a| a.strip_prefix("--multi-conn="))
        .map(|s| s.parse().expect("--multi-conn requires a positive integer"))
        .unwrap_or(0);
    // --high-c adds a c=512 level to probe the ceiling (h2 mutex re-emerges
    // at multi_conn × ~33k/conn). Framework differences compress in that
    // region, so the default stops at c=256.
    let levels: &[usize] = if args.iter().any(|a| a == "--high-c") {
        HIGH_CONCURRENCY_LEVELS
    } else {
        CONCURRENCY_LEVELS
    };

    let (warmup, measurement) = if quick {
        eprintln!("Running in quick mode (1s warmup, 3s measurement)");
        (QUICK_WARMUP, QUICK_MEASUREMENT)
    } else {
        eprintln!("Running full benchmark (3s warmup, 10s measurement)");
        (DEFAULT_WARMUP, DEFAULT_MEASUREMENT)
    };
    if all_protocols {
        eprintln!("  Testing all three wire protocols (Connect, gRPC, gRPC-Web)");
    }
    if multi_conn > 0 {
        eprintln!("  Using {multi_conn} HTTP/2 connections per server for all protocols");
    }

    eprintln!("\nStarting valkey container...");
    let valkey = ValkeyContainer::start();
    valkey.seed().await;
    eprintln!("  valkey ready at {}", valkey.addr);

    eprintln!("\nBuilding servers...");
    let connectrpc_bin = build_connectrpc_server();
    let tonic_bin = build_tonic_server();
    let connect_go_bin = build_connect_go_server();

    // Each server + the set of protocols to hit it with. The connectrpc-rs
    // server detects protocol from Content-Type so it needs no flag to speak
    // all three. tonic is gRPC-only. connect-go handles Connect and gRPC.
    //
    // Protocol slices are &'static (promoted literals); the bin path is an
    // owned String so we take &str to it in the tuple (no lifetime alias).
    let grpc_only: &[Protocol] = &[Protocol::Grpc];
    let connect_and_grpc: &[Protocol] = &[Protocol::Connect, Protocol::Grpc];
    let all_three: &[Protocol] = &[Protocol::Connect, Protocol::Grpc, Protocol::GrpcWeb];

    let servers: Vec<(&str, &str, &[Protocol])> = if all_protocols {
        vec![
            ("connectrpc-rs", &connectrpc_bin, all_three),
            ("tonic", &tonic_bin, grpc_only),
            ("connect-go", &connect_go_bin, connect_and_grpc),
        ]
    } else {
        vec![
            ("connectrpc-rs", &connectrpc_bin, grpc_only),
            ("tonic", &tonic_bin, grpc_only),
            ("connect-go", &connect_go_bin, grpc_only),
        ]
    };

    let mut results = Vec::new();

    for &(impl_name, bin, protocols) in &servers {
        for &protocol in protocols {
            for &concurrency in levels {
                let server = ServerProcess::start(bin, &[&valkey.addr]);
                let proto_label = protocol_label(protocol);
                eprintln!(
                    "  Benchmarking {impl_name} [{proto_label}] @ concurrency={concurrency} (warmup {warmup:?}, measure {measurement:?})..."
                );
                let result = if multi_conn > 0 {
                    bench_server_multiconn(
                        impl_name,
                        server.addr,
                        protocol,
                        multi_conn,
                        concurrency,
                        warmup,
                        measurement,
                    )
                    .await
                } else {
                    bench_server(
                        impl_name,
                        server.addr,
                        protocol,
                        concurrency,
                        warmup,
                        measurement,
                    )
                    .await
                };
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
        "{:<16} {:<10} {:>12} {:>14} {:>12} {:>12}",
        "Implementation", "Protocol", "Concurrency", "Requests/sec", "p50 (ms)", "p99 (ms)"
    );
    println!("{}", "-".repeat(82));
    for r in &results {
        println!(
            "{:<16} {:<10} {:>12} {:>14.0} {:>12.2} {:>12.2}",
            r.impl_name,
            protocol_label(r.protocol),
            r.concurrency,
            r.rps,
            r.p50_us as f64 / 1000.0,
            r.p99_us as f64 / 1000.0,
        );
    }
}
