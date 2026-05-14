use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};

use connectrpc::client::{HttpClient, full_body};
use connectrpc::{CodecFormat, Protocol};
use rpc_bench::*;

const PROTOCOLS: [Protocol; 3] = [Protocol::Connect, Protocol::Grpc, Protocol::GrpcWeb];
const STREAM_MSG_COUNT: i32 = 10;

fn bench_unary_empty(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _handle) = rt.block_on(start_server());

    let mut group = c.benchmark_group("unary/empty");
    for protocol in PROTOCOLS {
        let client = make_client(addr, protocol, CodecFormat::Proto);
        group.bench_function(BenchmarkId::from_parameter(protocol), |b| {
            b.to_async(&rt).iter(|| async {
                client
                    .unary(empty_request())
                    .await
                    .expect("unary RPC failed")
            });
        });
    }
    group.finish();
}

fn bench_unary_small(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _handle) = rt.block_on(start_server());

    let mut group = c.benchmark_group("unary/small");
    let req = small_request();
    for protocol in PROTOCOLS {
        let client = make_client(addr, protocol, CodecFormat::Proto);
        group.bench_function(BenchmarkId::from_parameter(protocol), |b| {
            b.to_async(&rt)
                .iter(|| async { client.unary(req.clone()).await.expect("unary RPC failed") });
        });
    }
    group.finish();
}

fn bench_unary_small_json(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _handle) = rt.block_on(start_server());

    let mut group = c.benchmark_group("unary/small_json");
    let req = small_request();
    for protocol in PROTOCOLS {
        let client = make_client(addr, protocol, CodecFormat::Json);
        group.bench_function(BenchmarkId::from_parameter(protocol), |b| {
            b.to_async(&rt)
                .iter(|| async { client.unary(req.clone()).await.expect("unary RPC failed") });
        });
    }
    group.finish();
}

fn bench_unary_logs(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _handle) = rt.block_on(start_server());

    // 10 log records ≈ 2-3 KB, ~140 string allocations avoided by zero-copy
    let req = log_request(10);
    let payload_size = {
        use buffa::Message;
        req.encoded_len() as u64
    };

    let mut group = c.benchmark_group("unary/logs_10");
    group.throughput(Throughput::Bytes(payload_size));
    for protocol in PROTOCOLS {
        let client = make_client(addr, protocol, CodecFormat::Proto);
        group.bench_function(BenchmarkId::from_parameter(protocol), |b| {
            b.to_async(&rt)
                .iter(|| async { client.log_unary(req.clone()).await.expect("log RPC failed") });
        });
    }
    group.finish();

    // 50 log records ≈ 12-15 KB, ~700 string allocations avoided
    let req = log_request(50);
    let payload_size = {
        use buffa::Message;
        req.encoded_len() as u64
    };

    let mut group = c.benchmark_group("unary/logs_50");
    group.throughput(Throughput::Bytes(payload_size));
    for protocol in PROTOCOLS {
        let client = make_client(addr, protocol, CodecFormat::Proto);
        group.bench_function(BenchmarkId::from_parameter(protocol), |b| {
            b.to_async(&rt)
                .iter(|| async { client.log_unary(req.clone()).await.expect("log RPC failed") });
        });
    }
    group.finish();
}

fn bench_unary_logs_owned_vs_view(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _handle) = rt.block_on(start_server());

    for record_count in [10, 50] {
        let req = log_request(record_count);
        let payload_size = {
            use buffa::Message;
            req.encoded_len() as u64
        };

        let mut group = c.benchmark_group(format!("unary/logs_{record_count}_owned_vs_view"));
        group.throughput(Throughput::Bytes(payload_size));

        // Connect protocol only — isolate decode overhead from protocol differences
        let client = make_client(addr, Protocol::Connect, CodecFormat::Proto);

        group.bench_function("owned", |b| {
            b.to_async(&rt).iter(|| async {
                client
                    .log_unary_owned(req.clone())
                    .await
                    .expect("log RPC failed")
            });
        });

        group.bench_function("view", |b| {
            b.to_async(&rt)
                .iter(|| async { client.log_unary(req.clone()).await.expect("log RPC failed") });
        });

        group.finish();
    }
}

fn bench_unary_large(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _handle) = rt.block_on(start_server());

    let req = large_request();
    let payload_size = {
        use buffa::Message;
        req.encoded_len() as u64
    };

    for compression in ["gzip", "zstd"] {
        let mut group = c.benchmark_group(format!("unary/large_{compression}"));
        group.throughput(Throughput::Bytes(payload_size));

        for protocol in PROTOCOLS {
            let config =
                connectrpc::client::ClientConfig::new(format!("http://{addr}").parse().unwrap())
                    .with_protocol(protocol)
                    .compress_requests(compression);
            let http = if protocol.requires_http2() {
                HttpClient::plaintext_http2_only()
            } else {
                HttpClient::plaintext()
            };
            let client = BenchServiceClient::new(http, config);
            group.bench_function(BenchmarkId::from_parameter(protocol), |b| {
                b.to_async(&rt)
                    .iter(|| async { client.unary(req.clone()).await.expect("unary RPC failed") });
            });
        }
        group.finish();
    }
}

fn bench_server_stream(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _handle) = rt.block_on(start_server());

    let mut group = c.benchmark_group("server_stream");
    group.throughput(Throughput::Elements(STREAM_MSG_COUNT as u64));
    let base_req = BenchRequest {
        response_count: STREAM_MSG_COUNT,
        payload: small_payload().into(),
        ..Default::default()
    };
    for protocol in PROTOCOLS {
        let client = make_client(addr, protocol, CodecFormat::Proto);
        group.bench_function(BenchmarkId::from_parameter(protocol), |b| {
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

fn bench_client_stream(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _handle) = rt.block_on(start_server());

    let mut group = c.benchmark_group("client_stream");
    group.throughput(Throughput::Elements(STREAM_MSG_COUNT as u64));
    let messages: Vec<BenchRequest> = (0..STREAM_MSG_COUNT).map(|_| small_request()).collect();
    for protocol in PROTOCOLS {
        let client = make_client(addr, protocol, CodecFormat::Proto);
        group.bench_function(BenchmarkId::from_parameter(protocol), |b| {
            b.to_async(&rt).iter(|| async {
                client
                    .client_stream(messages.clone())
                    .await
                    .expect("client_stream failed")
            });
        });
    }
    group.finish();
}

fn bench_bidi_stream(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _handle) = rt.block_on(start_server());

    // Bidi streaming is only supported over Connect protocol currently
    // (generated client stub returns unimplemented for gRPC/gRPC-Web bidi).
    // Benchmark using raw envelope-framed HTTP to test the server-side path.
    let mut group = c.benchmark_group("bidi_stream");
    group.throughput(Throughput::Elements(STREAM_MSG_COUNT as u64));

    let messages: Vec<BenchRequest> = (0..STREAM_MSG_COUNT).map(|_| small_request()).collect();

    // Pre-encode the envelope-framed request body.
    use buffa::Message;
    use bytes::{BufMut, BytesMut};
    let mut body_bytes = BytesMut::new();
    for msg in &messages {
        let data = msg.encode_to_vec();
        body_bytes.put_u8(0); // flags: no compression
        body_bytes.put_u32(data.len() as u32);
        body_bytes.extend_from_slice(&data);
    }
    let body_bytes = body_bytes.freeze();

    let http = HttpClient::plaintext();
    let uri: http::Uri = format!("http://{addr}/{BENCH_SERVICE_SERVICE_NAME}/BidiStream")
        .parse()
        .unwrap();

    group.bench_function("connect", |b| {
        b.to_async(&rt).iter(|| async {
            use connectrpc::client::ClientTransport;
            use http_body_util::BodyExt;

            let request = http::Request::builder()
                .method(http::Method::POST)
                .uri(uri.clone())
                .header("content-type", "application/connect+proto")
                .header("connect-protocol-version", "1")
                .body(full_body(body_bytes.clone()))
                .expect("failed to build request");

            let response = http.send(request).await.expect("bidi request failed");
            assert_eq!(response.status(), 200);
            // Consume the body to include response processing in the measurement.
            response
                .into_body()
                .collect()
                .await
                .expect("failed to read response body");
        });
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(997, Output::Protobuf));
    targets =
        bench_unary_empty,
        bench_unary_small,
        bench_unary_small_json,
        bench_unary_logs,
        bench_unary_logs_owned_vs_view,
        bench_unary_large,
        bench_server_stream,
        bench_client_stream,
        bench_bidi_stream,
}
criterion_main!(benches);
