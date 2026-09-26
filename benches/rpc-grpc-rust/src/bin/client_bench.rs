//! Client-stack benchmark: connectrpc-rs vs tonic vs grpc-rust's `grpc`
//! channel, all driving the same server.
//!
//! The server-side benches in `benches/rpc` hold the client constant
//! (connectrpc-rs) and vary the server. This one holds the server constant
//! (the connectrpc-rs echo server by default) and varies the client, which is
//! the only way to compare against the `grpc` crate: it has no server. Every
//! arm sends the same 64-byte `EchoRequest` over gRPC on h2 with
//! `TCP_NODELAY`, closed-loop, one in-flight request per worker.
//!
//! Arms:
//!   - `connectrpc-hyper`: `HttpClient::plaintext_http2_only()` (hyper-util
//!     pooled client), one client per connection
//!   - `connectrpc-h2`: `Http2Connection` shared with a 1024-deep buffer,
//!     the transport the connectrpc-rs docs recommend for gRPC
//!   - `tonic`: `tonic::transport::Channel` + prost stubs
//!   - `grpc`: `grpc::client::Channel` (pick_first over a `dns:///` target,
//!     `LocalChannelCredentials`) + grpc-protobuf stubs on upb messages
//!
//! `--conns=N[,N...]` (default `1,8`) runs the whole sweep once per value,
//! giving each arm N independent channels/connections with workers assigned
//! round-robin: N=1 exposes each stack's single-connection ceiling and N=8
//! takes h2 connection-mutex contention out of the picture. `connectrpc-h2`
//! and `tonic` dial all N up front; `connectrpc-hyper` and `grpc` connect
//! lazily, so at concurrency below N they hold only as many connections as
//! there are workers. Concurrency 1 is the per-request latency floor.
//! `--repeat=R` runs the full sweep R times, interleaving the arms, and
//! reports the median-throughput run of each cell so that drift over the run
//! does not land on one arm.
//!
//! ```text
//! cargo run --release --manifest-path benches/rpc-grpc-rust/Cargo.toml \
//!   --bin client_bench -- [--quick] [--conns=1,8] [--repeat=3] [--server-bin=PATH]
//! ```

use std::future::Future;
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, Http2Connection, HttpClient, SharedHttp2Connection};
use grpc::credentials::LocalChannelCredentials;
use rpc_bench::ServerProcess;
use rpc_bench_grpc_rust::{pb, tonic_pb};

// ── Configuration ────────────────────────────────────────────────────

const CONCURRENCY_LEVELS: &[usize] = &[1, 16, 64];
const DEFAULT_WARMUP: Duration = Duration::from_secs(3);
const DEFAULT_MEASUREMENT: Duration = Duration::from_secs(10);
const QUICK_WARMUP: Duration = Duration::from_secs(1);
const QUICK_MEASUREMENT: Duration = Duration::from_secs(3);
/// Per-worker latency sample cap; every 10th request is sampled.
const MAX_LATENCY_SAMPLES_PER_WORKER: usize = 100_000;

/// Same 64-byte payload as `benches/rpc/src/bin/echo_bench.rs`.
const PAYLOAD: &str = "lorem ipsum dolor sit amet, consectetur adipiscing elit sed do e";

/// Builds the connectrpc-rs echo server from the enclosing workspace, or
/// takes it from `RPC_BENCH_BIN_DIR` like the other drivers.
fn build_default_server() -> String {
    if let Some(path) = rpc_bench::prebuilt_bin("echo_server") {
        return path;
    }
    eprintln!("  Building connectrpc-rs echo server...");
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    // --target-dir pins the output so the returned path holds under
    // CARGO_TARGET_DIR or a `[build] target-dir` config.
    let status = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "rpc-bench",
            "--bin",
            "echo_server",
        ])
        .args(["--message-format=short", "--manifest-path"])
        .arg(format!("{root}/Cargo.toml"))
        .arg("--target-dir")
        .arg(format!("{root}/target"))
        .stderr(Stdio::inherit())
        .status()
        .expect("failed to run cargo build for echo_server");
    assert!(status.success(), "failed to build echo_server");
    format!("{root}/target/release/echo_server")
}

// ── Client arms ──────────────────────────────────────────────────────

/// Outcome of one RPC; the error text is only built on failure.
type CallResult = Result<(), String>;

/// One client stack under test. `connect` opens `n_conns` independent
/// channels up front (dialling eagerly where the stack allows it, so warmup
/// excludes handshakes); `worker` hands worker `idx` whatever per-task state
/// it needs, bound to channel `idx % n_conns`; `call` issues one echo RPC.
trait Arm {
    const NAME: &'static str;
    type Worker: Send + 'static;

    fn connect(addr: SocketAddr, n_conns: usize) -> impl Future<Output = Self> + Send;
    fn worker(&self, idx: usize) -> Self::Worker;
    fn call(worker: &mut Self::Worker) -> impl Future<Output = CallResult> + Send;
}

fn buffa_echo_request() -> rpc_bench::EchoRequest {
    rpc_bench::EchoRequest {
        message: PAYLOAD.to_string(),
        ..Default::default()
    }
}

fn server_uri(addr: SocketAddr) -> http::Uri {
    format!("http://{addr}").parse().expect("valid server URL")
}

fn grpc_config(uri: http::Uri) -> ClientConfig {
    ClientConfig::new(uri).with_protocol(Protocol::Grpc)
}

/// connectrpc-rs over hyper-util's pooled HTTP/2 client.
struct ConnectHyper {
    clients: Vec<rpc_bench::EchoServiceClient<HttpClient>>,
}

impl Arm for ConnectHyper {
    const NAME: &'static str = "connectrpc-hyper";
    type Worker = (
        rpc_bench::EchoServiceClient<HttpClient>,
        rpc_bench::EchoRequest,
    );

    async fn connect(addr: SocketAddr, n_conns: usize) -> Self {
        // Each HttpClient has its own pool and so its own h2 connection; the
        // pool connects lazily, during warmup.
        let config = grpc_config(server_uri(addr));
        let clients = (0..n_conns)
            .map(|_| {
                rpc_bench::EchoServiceClient::new(
                    HttpClient::plaintext_http2_only(),
                    config.clone(),
                )
            })
            .collect();
        Self { clients }
    }

    fn worker(&self, idx: usize) -> Self::Worker {
        (
            self.clients[idx % self.clients.len()].clone(),
            buffa_echo_request(),
        )
    }

    async fn call((client, req): &mut Self::Worker) -> CallResult {
        // The generated stub takes the request by value, so a clone per call
        // is inherent to the API (tonic is the same; grpc-rust takes a view).
        client
            .echo(req.clone())
            .await
            .map(drop)
            .map_err(|e| e.to_string())
    }
}

/// connectrpc-rs over its own `Http2Connection` transport.
struct ConnectH2 {
    clients: Vec<rpc_bench::EchoServiceClient<SharedHttp2Connection>>,
}

impl Arm for ConnectH2 {
    const NAME: &'static str = "connectrpc-h2";
    type Worker = (
        rpc_bench::EchoServiceClient<SharedHttp2Connection>,
        rpc_bench::EchoRequest,
    );

    async fn connect(addr: SocketAddr, n_conns: usize) -> Self {
        let uri = server_uri(addr);
        let config = grpc_config(uri.clone());
        let mut clients = Vec::with_capacity(n_conns);
        for _ in 0..n_conns {
            let conn = Http2Connection::connect_plaintext(uri.clone())
                .await
                .expect("connectrpc-h2 connect");
            // 1024 matches the buffer depth tonic's Channel uses.
            clients.push(rpc_bench::EchoServiceClient::new(
                conn.shared(1024),
                config.clone(),
            ));
        }
        Self { clients }
    }

    fn worker(&self, idx: usize) -> Self::Worker {
        (
            self.clients[idx % self.clients.len()].clone(),
            buffa_echo_request(),
        )
    }

    async fn call((client, req): &mut Self::Worker) -> CallResult {
        client
            .echo(req.clone())
            .await
            .map(drop)
            .map_err(|e| e.to_string())
    }
}

/// tonic's `Channel` with prost messages.
struct Tonic {
    clients: Vec<tonic_pb::echo_service_client::EchoServiceClient<tonic::transport::Channel>>,
}

impl Arm for Tonic {
    const NAME: &'static str = "tonic";
    type Worker = (
        tonic_pb::echo_service_client::EchoServiceClient<tonic::transport::Channel>,
        tonic_pb::EchoRequest,
    );

    async fn connect(addr: SocketAddr, n_conns: usize) -> Self {
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .expect("valid server URL");
        let mut clients = Vec::with_capacity(n_conns);
        for _ in 0..n_conns {
            let channel = endpoint.connect().await.expect("tonic connect");
            clients.push(tonic_pb::echo_service_client::EchoServiceClient::new(
                channel,
            ));
        }
        Self { clients }
    }

    fn worker(&self, idx: usize) -> Self::Worker {
        let req = tonic_pb::EchoRequest {
            message: PAYLOAD.to_string(),
        };
        (self.clients[idx % self.clients.len()].clone(), req)
    }

    async fn call((client, req): &mut Self::Worker) -> CallResult {
        client
            .echo(req.clone())
            .await
            .map(drop)
            .map_err(|e| e.to_string())
    }
}

/// grpc-rust's `grpc::client::Channel` with grpc-protobuf stubs.
struct Grpc {
    clients: Vec<pb::echo_service_client::EchoServiceClient<grpc::client::Channel>>,
}

impl Arm for Grpc {
    const NAME: &'static str = "grpc";
    type Worker = (
        pb::echo_service_client::EchoServiceClient<grpc::client::Channel>,
        pb::EchoRequest,
    );

    async fn connect(addr: SocketAddr, n_conns: usize) -> Self {
        // A Channel is one pick_first subchannel, i.e. one h2 connection, and
        // it connects lazily on the first RPC (during warmup). Local
        // credentials are grpc-rust's plaintext option; they refuse
        // non-loopback peers, which is all this bench uses.
        let clients = (0..n_conns)
            .map(|_| {
                let channel = grpc::client::Channel::builder(
                    format!("dns:///{addr}"),
                    LocalChannelCredentials::new_arc(),
                )
                .build();
                pb::echo_service_client::EchoServiceClient::new(channel)
            })
            .collect();
        Self { clients }
    }

    fn worker(&self, idx: usize) -> Self::Worker {
        let mut req = pb::EchoRequest::new();
        req.set_message(PAYLOAD);
        (self.clients[idx % self.clients.len()].clone(), req)
    }

    async fn call((client, req): &mut Self::Worker) -> CallResult {
        // The generated stub takes a message view, so unlike the other arms
        // there is no per-call request clone to pay for.
        // grpc-protobuf's StatusError has no Display impl; spell it out.
        client
            .echo(req.as_view())
            .await
            .map(drop)
            .map_err(|e| format!("{:?}: {}", e.code(), e.message()))
    }
}

// ── Benchmark runner ─────────────────────────────────────────────────

#[derive(Clone)]
struct BenchResult {
    arm: &'static str,
    conns: usize,
    concurrency: usize,
    rps: f64,
    p50_us: u64,
    p99_us: u64,
}

/// What each worker hands back when the run stops.
struct WorkerTally {
    measured: u64,
    failures: u64,
    first_error: Option<String>,
    latencies_us: Vec<u64>,
}

async fn run_arm<A: Arm>(
    addr: SocketAddr,
    n_conns: usize,
    concurrency: usize,
    warmup: Duration,
    measurement: Duration,
) -> BenchResult {
    let arm = A::connect(addr, n_conns).await;

    let running = Arc::new(AtomicBool::new(true));
    let measuring = Arc::new(AtomicBool::new(false));
    // Shared only during warmup, to prove the arm works at all; the measured
    // window counts per worker so no arm pays for a contended cache line in
    // proportion to its own throughput.
    let warmup_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..concurrency)
        .map(|idx| {
            let mut worker = arm.worker(idx);
            let running = Arc::clone(&running);
            let measuring = Arc::clone(&measuring);
            let warmup_count = Arc::clone(&warmup_count);
            tokio::spawn(async move {
                let mut tally = WorkerTally {
                    measured: 0,
                    failures: 0,
                    first_error: None,
                    latencies_us: Vec::with_capacity(MAX_LATENCY_SAMPLES_PER_WORKER),
                };
                while running.load(Ordering::Relaxed) {
                    let start = Instant::now();
                    match A::call(&mut worker).await {
                        Ok(()) if measuring.load(Ordering::Relaxed) => {
                            tally.measured += 1;
                            if tally.measured.is_multiple_of(10)
                                && tally.latencies_us.len() < MAX_LATENCY_SAMPLES_PER_WORKER
                            {
                                tally.latencies_us.push(start.elapsed().as_micros() as u64);
                            }
                        }
                        Ok(()) => {
                            warmup_count.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            tally.failures += 1;
                            tally.first_error.get_or_insert(e);
                        }
                    }
                }
                tally
            })
        })
        .collect();

    tokio::time::sleep(warmup).await;
    // A server that rejects every call (e.g. an unimplemented method after a
    // stub regen) would otherwise report 0 req/s instead of failing.
    assert!(
        warmup_count.load(Ordering::Relaxed) > 0,
        "{}: no request succeeded during warmup",
        A::NAME
    );
    measuring.store(true, Ordering::Relaxed);
    let measure_start = Instant::now();

    tokio::time::sleep(measurement).await;
    running.store(false, Ordering::Relaxed);
    let elapsed = measure_start.elapsed();

    let mut total = 0u64;
    let mut failures = 0u64;
    let mut first_error = None;
    let mut latencies = Vec::new();
    for h in handles {
        let tally = h.await.expect("worker panicked");
        total += tally.measured;
        failures += tally.failures;
        first_error = first_error.or(tally.first_error);
        latencies.extend(tally.latencies_us);
    }
    // A cell with failed calls is not a throughput measurement; refuse to
    // report it rather than let a degraded arm read as a slow one.
    assert!(
        failures == 0,
        "{}: {failures} calls failed (first: {})",
        A::NAME,
        first_error.unwrap_or_default()
    );
    latencies.sort_unstable();
    // Same percentile convention as echo_bench, so the two drivers compare.
    let pct = |p: usize| {
        latencies
            .get(latencies.len() * p / 100)
            .copied()
            .unwrap_or(0)
    };

    BenchResult {
        arm: A::NAME,
        conns: n_conns,
        concurrency,
        rps: total as f64 / elapsed.as_secs_f64(),
        p50_us: pct(50),
        p99_us: pct(99),
    }
}

async fn sweep_arm<A: Arm>(
    server_bin: &str,
    n_conns: usize,
    warmup: Duration,
    measurement: Duration,
    results: &mut Vec<BenchResult>,
) {
    for &concurrency in CONCURRENCY_LEVELS {
        // Fresh server per run, as in echo_bench, so no arm inherits another
        // arm's connections or allocator state.
        let server = ServerProcess::start(server_bin, &[]);
        eprintln!(
            "  Benchmarking {} @ conns={n_conns} concurrency={concurrency}...",
            A::NAME
        );
        let r = run_arm::<A>(server.addr, n_conns, concurrency, warmup, measurement).await;
        eprintln!(
            "    => {:.0} req/s, p50={:.3}ms, p99={:.3}ms",
            r.rps,
            r.p50_us as f64 / 1000.0,
            r.p99_us as f64 / 1000.0
        );
        results.push(r);
        drop(server);
    }
}

/// Collapses repeated runs of the same (arm, conns, concurrency) cell to the
/// run with the median throughput, keeping first-seen cell order.
fn median_runs(results: &[BenchResult]) -> Vec<BenchResult> {
    let mut cells: Vec<(&str, usize, usize)> = Vec::new();
    for r in results {
        let key = (r.arm, r.conns, r.concurrency);
        if !cells.contains(&key) {
            cells.push(key);
        }
    }
    cells
        .into_iter()
        .map(|(arm, conns, concurrency)| {
            let mut runs: Vec<&BenchResult> = results
                .iter()
                .filter(|r| r.arm == arm && r.conns == conns && r.concurrency == concurrency)
                .collect();
            runs.sort_by(|a, b| a.rps.total_cmp(&b.rps));
            runs[runs.len() / 2].clone()
        })
        .collect()
}

fn parse_list(args: &[String], flag: &str, default: &[usize]) -> Vec<usize> {
    let values: Vec<usize> = args
        .iter()
        .find_map(|a| a.strip_prefix(flag))
        .map(|s| {
            s.split(',')
                .map(|n| {
                    n.parse()
                        .unwrap_or_else(|_| panic!("{flag} takes comma-separated integers"))
                })
                .collect()
        })
        .unwrap_or_else(|| default.to_vec());
    assert!(
        values.iter().all(|&n| n > 0),
        "{flag} values must be positive"
    );
    values
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let quick = args.iter().any(|a| a == "--quick");
    let conns = parse_list(&args, "--conns=", &[1, 8]);
    let repeat = parse_list(&args, "--repeat=", &[1])[0];
    let server_bin = args
        .iter()
        .find_map(|a| a.strip_prefix("--server-bin="))
        .map_or_else(build_default_server, str::to_owned);

    let (warmup, measurement) = if quick {
        eprintln!("Running in quick mode (1s warmup, 3s measurement)");
        (QUICK_WARMUP, QUICK_MEASUREMENT)
    } else {
        eprintln!("Running full benchmark (3s warmup, 10s measurement)");
        (DEFAULT_WARMUP, DEFAULT_MEASUREMENT)
    };
    eprintln!("Server: {server_bin}, repeat={repeat}\n");

    let mut results = Vec::new();
    for rep in 0..repeat {
        if repeat > 1 {
            eprintln!("Repetition {} of {repeat}", rep + 1);
        }
        for &n_conns in &conns {
            sweep_arm::<ConnectHyper>(&server_bin, n_conns, warmup, measurement, &mut results)
                .await;
            sweep_arm::<ConnectH2>(&server_bin, n_conns, warmup, measurement, &mut results).await;
            sweep_arm::<Tonic>(&server_bin, n_conns, warmup, measurement, &mut results).await;
            sweep_arm::<Grpc>(&server_bin, n_conns, warmup, measurement, &mut results).await;
        }
    }

    println!();
    if repeat > 1 {
        println!("(median-throughput run of {repeat} per cell)");
    }
    println!(
        "{:<18} {:>6} {:>12} {:>14} {:>10} {:>10}",
        "Client", "Conns", "Concurrency", "Requests/sec", "p50 (ms)", "p99 (ms)"
    );
    println!("{}", "-".repeat(74));
    for r in median_runs(&results) {
        println!(
            "{:<18} {:>6} {:>12} {:>14.0} {:>10.3} {:>10.3}",
            r.arm,
            r.conns,
            r.concurrency,
            r.rps,
            r.p50_us as f64 / 1000.0,
            r.p99_us as f64 / 1000.0,
        );
    }
}
