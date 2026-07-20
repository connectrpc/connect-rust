use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, HttpClient};
use rpc_bench::*;

const STREAM_MSG_COUNT: i32 = 10;

// ── Server process management ─────────────────────────────────────────

struct ServerProcess {
    child: Child,
    addr: SocketAddr,
}

impl ServerProcess {
    /// Start a server process and read the address from its first stdout line.
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
        reader
            .read_line(&mut line)
            .expect("failed to read server address");
        let addr: SocketAddr = line.trim().parse().unwrap_or_else(|e| {
            panic!("failed to parse server address from {cmd} ({line:?}): {e}")
        });

        // Give the server a moment to be fully ready for connections.
        std::thread::sleep(Duration::from_millis(50));

        Self { child, addr }
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ── Helpers ───────────────────────────────────────────────────────────

fn make_grpc_client(addr: SocketAddr) -> BenchServiceClient<HttpClient> {
    let config =
        ClientConfig::new(format!("http://{addr}").parse().unwrap()).with_protocol(Protocol::Grpc);
    BenchServiceClient::new(HttpClient::plaintext_http2_only(), config)
}

fn make_connect_client(addr: SocketAddr) -> BenchServiceClient<HttpClient> {
    let config = ClientConfig::new(format!("http://{addr}").parse().unwrap())
        .with_protocol(Protocol::Connect);
    BenchServiceClient::new(HttpClient::plaintext(), config)
}

// ── Server paths ──────────────────────────────────────────────────────

fn connectrpc_server_path() -> String {
    // Build the server binary if needed and return its path.
    let output = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "rpc-bench",
            "--bin",
            "bench_server",
            "--message-format=short",
        ])
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to build bench_server");
    assert!(output.status.success(), "failed to build bench_server");

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    // The workspace target dir is two levels up from benches/rpc/
    format!("{manifest_dir}/../../target/release/bench_server")
}

fn tonic_server_path() -> String {
    let output = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "rpc-bench-tonic",
            "--message-format=short",
        ])
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to build rpc-bench-tonic");
    assert!(output.status.success(), "failed to build rpc-bench-tonic");

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/../../target/release/rpc-bench-tonic")
}

fn connect_go_server_path() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let go_dir = format!("{manifest_dir}/../rpc-go");
    let bin_path = format!("{go_dir}/bench-connect-go");

    let output = Command::new("go")
        .args(["build", "-o", &bin_path, "."])
        .current_dir(&go_dir)
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to build connect-go server");
    assert!(output.status.success(), "failed to build connect-go server");

    bin_path
}

// ── Benchmarks ────────────────────────────────────────────────────────

/// Labels for each server implementation.
const IMPLS: [&str; 3] = ["connectrpc-rs", "tonic", "connect-go"];

fn bench_unary_small_grpc(c: &mut Criterion) {
    let connectrpc_bin = connectrpc_server_path();
    let tonic_bin = tonic_server_path();
    let connect_go_bin = connect_go_server_path();

    let servers = [
        ServerProcess::start(&connectrpc_bin, &[]),
        ServerProcess::start(&tonic_bin, &[]),
        ServerProcess::start(&connect_go_bin, &[]),
    ];

    let req = small_request();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/unary_small_grpc");

    for (impl_name, server) in IMPLS.iter().zip(servers.iter()) {
        let client = make_grpc_client(server.addr());
        group.bench_function(BenchmarkId::from_parameter(impl_name), |b| {
            b.to_async(&rt)
                .iter(|| async { client.unary(req.clone()).await.expect("unary RPC failed") });
        });
    }

    group.finish();
    drop(servers);
}

fn bench_unary_small_connect(c: &mut Criterion) {
    let connectrpc_bin = connectrpc_server_path();
    let connect_go_bin = connect_go_server_path();

    // tonic doesn't support Connect protocol, only gRPC
    let servers = [
        ("connectrpc-rs", ServerProcess::start(&connectrpc_bin, &[])),
        ("connect-go", ServerProcess::start(&connect_go_bin, &[])),
    ];

    let req = small_request();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/unary_small_connect");

    for (impl_name, server) in &servers {
        let client = make_connect_client(server.addr());
        group.bench_function(BenchmarkId::from_parameter(impl_name), |b| {
            b.to_async(&rt)
                .iter(|| async { client.unary(req.clone()).await.expect("unary RPC failed") });
        });
    }

    group.finish();
}

fn bench_unary_large_grpc(c: &mut Criterion) {
    let connectrpc_bin = connectrpc_server_path();
    let tonic_bin = tonic_server_path();
    let connect_go_bin = connect_go_server_path();

    let servers = [
        ServerProcess::start(&connectrpc_bin, &[]),
        ServerProcess::start(&tonic_bin, &[]),
        ServerProcess::start(&connect_go_bin, &[]),
    ];

    let req = large_request();
    let payload_size = {
        use buffa::Message;
        req.encoded_len() as u64
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/unary_large_grpc");
    group.throughput(Throughput::Bytes(payload_size));

    for (impl_name, server) in IMPLS.iter().zip(servers.iter()) {
        let config = ClientConfig::new(format!("http://{}", server.addr()).parse().unwrap())
            .with_protocol(Protocol::Grpc)
            .compress_requests("gzip");
        let client = BenchServiceClient::new(HttpClient::plaintext_http2_only(), config);
        group.bench_function(BenchmarkId::from_parameter(impl_name), |b| {
            b.to_async(&rt)
                .iter(|| async { client.unary(req.clone()).await.expect("unary RPC failed") });
        });
    }

    group.finish();
    drop(servers);
}

fn bench_server_stream_grpc(c: &mut Criterion) {
    let connectrpc_bin = connectrpc_server_path();
    let tonic_bin = tonic_server_path();
    let connect_go_bin = connect_go_server_path();

    let servers = [
        ServerProcess::start(&connectrpc_bin, &[]),
        ServerProcess::start(&tonic_bin, &[]),
        ServerProcess::start(&connect_go_bin, &[]),
    ];

    let base_req = BenchRequest {
        response_count: STREAM_MSG_COUNT,
        payload: small_payload().into(),
        ..Default::default()
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/server_stream_grpc");
    group.throughput(Throughput::Elements(STREAM_MSG_COUNT as u64));

    for (impl_name, server) in IMPLS.iter().zip(servers.iter()) {
        let client = make_grpc_client(server.addr());
        group.bench_function(BenchmarkId::from_parameter(impl_name), |b| {
            b.to_async(&rt).iter(|| async {
                let mut stream = client
                    .server_stream(base_req.clone())
                    .await
                    .expect("server_stream failed");
                let mut count = 0;
                while stream
                    .message()
                    .await
                    .expect("stream message failed")
                    .is_some()
                {
                    count += 1;
                }
                assert_eq!(count, STREAM_MSG_COUNT);
            });
        });
    }

    group.finish();
    drop(servers);
}

fn bench_client_stream_grpc(c: &mut Criterion) {
    let connectrpc_bin = connectrpc_server_path();
    let tonic_bin = tonic_server_path();
    let connect_go_bin = connect_go_server_path();

    let servers = [
        ServerProcess::start(&connectrpc_bin, &[]),
        ServerProcess::start(&tonic_bin, &[]),
        ServerProcess::start(&connect_go_bin, &[]),
    ];

    let messages: Vec<BenchRequest> = (0..STREAM_MSG_COUNT).map(|_| small_request()).collect();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/client_stream_grpc");
    group.throughput(Throughput::Elements(STREAM_MSG_COUNT as u64));

    for (impl_name, server) in IMPLS.iter().zip(servers.iter()) {
        let client = make_grpc_client(server.addr());
        group.bench_function(BenchmarkId::from_parameter(impl_name), |b| {
            b.to_async(&rt).iter(|| async {
                client
                    .client_stream(futures::stream::iter(messages.clone()))
                    .await
                    .expect("client_stream failed")
            });
        });
    }

    group.finish();
    drop(servers);
}

fn bench_unary_logs_grpc(c: &mut Criterion) {
    let connectrpc_bin = connectrpc_server_path();
    let tonic_bin = tonic_server_path();
    let connect_go_bin = connect_go_server_path();

    let servers = [
        ServerProcess::start(&connectrpc_bin, &[]),
        ServerProcess::start(&tonic_bin, &[]),
        ServerProcess::start(&connect_go_bin, &[]),
    ];

    let req = log_request(50);
    let payload_size = {
        use buffa::Message;
        req.encoded_len() as u64
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/unary_logs_50_grpc");
    group.throughput(Throughput::Bytes(payload_size));

    for (impl_name, server) in IMPLS.iter().zip(servers.iter()) {
        let client = make_grpc_client(server.addr());
        group.bench_function(BenchmarkId::from_parameter(impl_name), |b| {
            b.to_async(&rt)
                .iter(|| async { client.log_unary(req.clone()).await.expect("log RPC failed") });
        });
    }

    group.finish();
    drop(servers);
}

fn bench_unary_logs_connect(c: &mut Criterion) {
    let connectrpc_bin = connectrpc_server_path();
    let connect_go_bin = connect_go_server_path();

    let servers = [
        ("connectrpc-rs", ServerProcess::start(&connectrpc_bin, &[])),
        ("connect-go", ServerProcess::start(&connect_go_bin, &[])),
    ];

    let req = log_request(50);
    let payload_size = {
        use buffa::Message;
        req.encoded_len() as u64
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/unary_logs_50_connect");
    group.throughput(Throughput::Bytes(payload_size));

    for (impl_name, server) in &servers {
        let client = make_connect_client(server.addr());
        group.bench_function(BenchmarkId::from_parameter(impl_name), |b| {
            b.to_async(&rt)
                .iter(|| async { client.log_unary(req.clone()).await.expect("log RPC failed") });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_unary_small_grpc,
    bench_unary_small_connect,
    bench_unary_logs_grpc,
    bench_unary_logs_connect,
    bench_unary_large_grpc,
    bench_server_stream_grpc,
    bench_client_stream_grpc,
);
criterion_main!(benches);
