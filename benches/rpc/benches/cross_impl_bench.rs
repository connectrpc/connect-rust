use std::net::SocketAddr;
use std::process::{Command, Stdio};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, HttpClient};
use rpc_bench::*;

const STREAM_MSG_COUNT: i32 = 10;

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
    if let Some(path) = prebuilt_bin("bench_server") {
        return path;
    }
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
    if let Some(path) = prebuilt_bin("rpc-bench-tonic") {
        return path;
    }
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

fn tonic_protobuf_server_path() -> String {
    build_grpc_rust_bin("bench-server-tonic-protobuf")
}

fn connect_go_server_path() -> String {
    if let Some(path) = prebuilt_bin("bench-connect-go") {
        return path;
    }
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

// ── Server sets ───────────────────────────────────────────────────────

/// One freshly started server per implementation that speaks gRPC, labelled
/// for the criterion report. `tonic` is tonic + prost; `tonic-protobuf` is
/// tonic + grpc-rust's codec on Google's upb-kernel `protobuf` runtime.
fn grpc_servers() -> Vec<(&'static str, ServerProcess)> {
    vec![
        (
            "connectrpc-rs",
            ServerProcess::start(&connectrpc_server_path(), &[]),
        ),
        ("tonic", ServerProcess::start(&tonic_server_path(), &[])),
        (
            "tonic-protobuf",
            ServerProcess::start(&tonic_protobuf_server_path(), &[]),
        ),
        (
            "connect-go",
            ServerProcess::start(&connect_go_server_path(), &[]),
        ),
    ]
}

/// One freshly started server per implementation that speaks the Connect
/// protocol. tonic serves gRPC only.
fn connect_servers() -> Vec<(&'static str, ServerProcess)> {
    vec![
        (
            "connectrpc-rs",
            ServerProcess::start(&connectrpc_server_path(), &[]),
        ),
        (
            "connect-go",
            ServerProcess::start(&connect_go_server_path(), &[]),
        ),
    ]
}

// ── Benchmarks ────────────────────────────────────────────────────────

fn bench_unary_small_grpc(c: &mut Criterion) {
    let servers = grpc_servers();
    let req = small_request();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/unary_small_grpc");

    for (impl_name, server) in &servers {
        let client = make_grpc_client(server.addr);
        group.bench_function(BenchmarkId::from_parameter(impl_name), |b| {
            b.to_async(&rt)
                .iter(|| async { client.unary(req.clone()).await.expect("unary RPC failed") });
        });
    }

    group.finish();
}

fn bench_unary_small_connect(c: &mut Criterion) {
    let servers = connect_servers();
    let req = small_request();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/unary_small_connect");

    for (impl_name, server) in &servers {
        let client = make_connect_client(server.addr);
        group.bench_function(BenchmarkId::from_parameter(impl_name), |b| {
            b.to_async(&rt)
                .iter(|| async { client.unary(req.clone()).await.expect("unary RPC failed") });
        });
    }

    group.finish();
}

fn bench_unary_large_grpc(c: &mut Criterion) {
    let servers = grpc_servers();
    let req = large_request();
    let payload_size = {
        use buffa::Message;
        req.encoded_len() as u64
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/unary_large_grpc");
    group.throughput(Throughput::Bytes(payload_size));

    for (impl_name, server) in &servers {
        let config = ClientConfig::new(format!("http://{}", server.addr).parse().unwrap())
            .with_protocol(Protocol::Grpc)
            .compress_requests("gzip");
        let client = BenchServiceClient::new(HttpClient::plaintext_http2_only(), config);
        group.bench_function(BenchmarkId::from_parameter(impl_name), |b| {
            b.to_async(&rt)
                .iter(|| async { client.unary(req.clone()).await.expect("unary RPC failed") });
        });
    }

    group.finish();
}

fn bench_server_stream_grpc(c: &mut Criterion) {
    let servers = grpc_servers();
    let base_req = BenchRequest {
        response_count: STREAM_MSG_COUNT,
        payload: small_payload().into(),
        ..Default::default()
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/server_stream_grpc");
    group.throughput(Throughput::Elements(STREAM_MSG_COUNT as u64));

    for (impl_name, server) in &servers {
        let client = make_grpc_client(server.addr);
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
}

fn bench_client_stream_grpc(c: &mut Criterion) {
    let servers = grpc_servers();
    let messages: Vec<BenchRequest> = (0..STREAM_MSG_COUNT).map(|_| small_request()).collect();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/client_stream_grpc");
    group.throughput(Throughput::Elements(STREAM_MSG_COUNT as u64));

    for (impl_name, server) in &servers {
        let client = make_grpc_client(server.addr);
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
}

fn bench_unary_logs_grpc(c: &mut Criterion) {
    let servers = grpc_servers();
    let req = log_request(50);
    let payload_size = {
        use buffa::Message;
        req.encoded_len() as u64
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/unary_logs_50_grpc");
    group.throughput(Throughput::Bytes(payload_size));

    for (impl_name, server) in &servers {
        let client = make_grpc_client(server.addr);
        group.bench_function(BenchmarkId::from_parameter(impl_name), |b| {
            b.to_async(&rt)
                .iter(|| async { client.log_unary(req.clone()).await.expect("log RPC failed") });
        });
    }

    group.finish();
}

fn bench_unary_logs_connect(c: &mut Criterion) {
    let servers = connect_servers();
    let req = log_request(50);
    let payload_size = {
        use buffa::Message;
        req.encoded_len() as u64
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cross/unary_logs_50_connect");
    group.throughput(Throughput::Bytes(payload_size));

    for (impl_name, server) in &servers {
        let client = make_connect_client(server.addr);
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
