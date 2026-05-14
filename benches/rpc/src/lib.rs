pub mod filter;
pub mod fortune;

#[path = "generated/connect/mod.rs"]
pub mod connect;
#[path = "generated/buffa/mod.rs"]
pub mod proto;

pub use connect::bench::v1::*;
// View types are re-exported at package root via buffa's natural-path
// `pub use`s (since buffa 0.5.0), so `bench::v1::*` covers `FooView` types
// directly. If a proto type ever shadows a re-export (e.g. a literal
// `message FooView`), buffa silently skips emitting that re-export and the
// natural path resolves to the proto type instead — switch to the canonical
// `__buffa::view::FooView` path for that type if it happens.
pub use proto::bench::v1::*;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use buffa::view::OwnedView;
use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::{
    CodecFormat, ConnectError, Protocol, RequestContext, Response, Router, ServiceResult,
    ServiceStream,
};
use futures::StreamExt;

/// Echo-style bench service that reflects payloads back.
pub struct BenchServiceImpl;

impl BenchService for BenchServiceImpl {
    async fn unary(
        &self,
        _ctx: RequestContext,
        req: OwnedView<BenchRequestView<'static>>,
    ) -> ServiceResult<BenchResponse> {
        let req = req.to_owned_message();
        Response::ok(BenchResponse {
            payload: req.payload,
            ..Default::default()
        })
    }

    async fn server_stream(
        &self,
        _ctx: RequestContext,
        req: OwnedView<BenchRequestView<'static>>,
    ) -> ServiceResult<ServiceStream<BenchResponse>> {
        let req = req.to_owned_message();
        let count = req.response_count;
        let payload = req.payload;
        let stream = futures::stream::unfold(0, move |i| {
            let payload = payload.clone();
            async move {
                if i >= count {
                    return None;
                }
                Some((
                    Ok(BenchResponse {
                        payload,
                        ..Default::default()
                    }),
                    i + 1,
                ))
            }
        });
        Response::stream_ok(stream)
    }

    async fn client_stream(
        &self,
        _ctx: RequestContext,
        mut requests: ServiceStream<OwnedView<BenchRequestView<'static>>>,
    ) -> ServiceResult<BenchResponse> {
        let mut last_payload = Default::default();
        while let Some(req) = requests.next().await {
            let req = req?.to_owned_message();
            last_payload = req.payload;
        }
        Response::ok(BenchResponse {
            payload: last_payload,
            ..Default::default()
        })
    }

    async fn log_unary(
        &self,
        _ctx: RequestContext,
        req: OwnedView<LogRequestView<'static>>,
    ) -> ServiceResult<LogResponse> {
        // Realistic handler: iterate records, read string fields, compute aggregate.
        // All field access is zero-copy via &str borrows from the request buffer.
        let count = process_log_records_view(&req.records);
        Response::ok(LogResponse {
            count,
            ..Default::default()
        })
    }

    async fn log_unary_owned(
        &self,
        _ctx: RequestContext,
        req: OwnedView<LogRequestView<'static>>,
    ) -> ServiceResult<LogResponse> {
        // Same handler logic but using owned types (pre-OwnedView path).
        let req = req.to_owned_message();
        let count = process_log_records_owned(&req.records);
        Response::ok(LogResponse {
            count,
            ..Default::default()
        })
    }

    async fn bidi_stream(
        &self,
        _ctx: RequestContext,
        requests: ServiceStream<OwnedView<BenchRequestView<'static>>>,
    ) -> ServiceResult<ServiceStream<BenchResponse>> {
        // Map stream to owned types before spawning to satisfy Send bounds
        let mut requests = Box::pin(requests.map(|r| r.map(|v| v.to_owned_message())));
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<BenchResponse, ConnectError>>(1);
        tokio::spawn(async move {
            while let Some(req) = requests.next().await {
                match req {
                    Ok(req) => {
                        let resp = BenchResponse {
                            payload: req.payload,
                            ..Default::default()
                        };
                        if tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Response::stream_ok(stream)
    }
}

/// Start the bench server (with H2C support for gRPC), returning the bound
/// address and join handle.
pub async fn start_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let router = Router::new();
    let router = Arc::new(BenchServiceImpl).register(router);
    let bound = connectrpc::server::Server::bind("127.0.0.1:0")
        .await
        .expect("failed to bind bench server");
    let addr = bound.local_addr().expect("failed to get local addr");
    let handle = tokio::spawn(async move {
        bound.serve(router).await.expect("bench server error");
    });
    (addr, handle)
}

/// Create a client for the given address, protocol, and codec format.
///
/// gRPC requires HTTP/2 (uses `HttpClient::plaintext_http2_only()`), while Connect and
/// gRPC-Web work over HTTP/1.1 or HTTP/2.
pub fn make_client(
    addr: SocketAddr,
    protocol: Protocol,
    codec_format: CodecFormat,
) -> BenchServiceClient<HttpClient> {
    let config = ClientConfig::new(format!("http://{addr}").parse().unwrap())
        .with_protocol(protocol)
        .with_codec_format(codec_format);
    let http = if protocol.requires_http2() {
        HttpClient::plaintext_http2_only()
    } else {
        HttpClient::plaintext()
    };
    BenchServiceClient::new(http, config)
}

// ── Log record processing (realistic handler work) ──────────────────

/// Process log records using zero-copy view fields.
///
/// Iterates all records, reads string fields, checks label counts,
/// and computes an aggregate. All string access is via `&str` borrows
/// from the request buffer — zero allocations.
fn process_log_records_view(records: &buffa::RepeatedView<'_, LogRecordView<'_>>) -> i32 {
    let mut count = 0i32;
    let mut total_msg_len = 0usize;
    for record in records.iter() {
        // Read scalar + string fields (zero-copy)
        if record.severity.as_known() == Some(log_record::Severity::SEVERITY_ERROR) {
            count += 1;
        }
        total_msg_len += record.message.len();
        total_msg_len += record.service_name.len();
        total_msg_len += record.trace_id.len();

        // Iterate map labels (zero-copy &str keys and values)
        for (k, v) in record.labels.iter() {
            total_msg_len += k.len() + v.len();
        }

        // Access nested sub-message
        if let Some(src) = record.source.as_option() {
            total_msg_len += src.file.len() + src.function.len();
        }
    }
    // Include total_msg_len in result to prevent dead-code elimination
    count + (total_msg_len % 1000) as i32
}

/// Process log records using owned fields (equivalent logic).
///
/// Same computation as `process_log_records_view`, but operating on
/// owned `String` fields after `to_owned_message()`. The string data
/// was already allocated during the owned decode.
fn process_log_records_owned(records: &[LogRecord]) -> i32 {
    let mut count = 0i32;
    let mut total_msg_len = 0usize;
    for record in records {
        if record.severity.as_known() == Some(log_record::Severity::SEVERITY_ERROR) {
            count += 1;
        }
        total_msg_len += record.message.len();
        total_msg_len += record.service_name.len();
        total_msg_len += record.trace_id.len();

        for (k, v) in &record.labels {
            total_msg_len += k.len() + v.len();
        }

        if record.source.is_set() {
            total_msg_len += record.source.file.len() + record.source.function.len();
        }
    }
    count + (total_msg_len % 1000) as i32
}

// ── Payload builders ──────────────────────────────────────────────────

pub fn empty_request() -> BenchRequest {
    BenchRequest::default()
}

pub fn small_request() -> BenchRequest {
    BenchRequest {
        payload: small_payload().into(),
        ..Default::default()
    }
}

pub fn large_request() -> BenchRequest {
    BenchRequest {
        payload: large_payload().into(),
        ..Default::default()
    }
}

pub fn small_payload() -> Payload {
    let mut attributes = HashMap::new();
    for i in 0..5 {
        attributes.insert(format!("key-{i}"), format!("value-{i}"));
    }

    Payload {
        id: 42,
        timestamp_nanos: 1_700_000_000_000_000_000,
        latitude: 37.7749,
        longitude: -122.4194,
        active: true,
        trace_id: 0xDEAD_BEEF_CAFE_BABE,
        name: "benchmark-item".into(),
        description: "A realistic benchmark payload for testing".into(),
        region: "us-west-2".into(),
        data: Vec::new(),
        status: Status::STATUS_ACTIVE.into(),
        metadata: Metadata {
            request_id: "req-abc-123-def-456".into(),
            user_agent: "connectrpc-rs/bench/1.0".into(),
            created_at: 1_700_000_000,
            headers: HashMap::from([
                ("x-request-id".into(), "abc123".into()),
                ("authorization".into(), "Bearer tok".into()),
            ]),
            ..Default::default()
        }
        .into(),
        scores: vec![95, 87, 73, 91, 88],
        tags: vec!["benchmark".into(), "rust".into(), "connect".into()],
        attributes,
        ..Default::default()
    }
}

/// Build a string-heavy log request with `n` records.
///
/// Each record has ~8 string fields plus a map of labels, so N records
/// means ~(8 + labels_per_record) allocations in owned decode vs zero
/// allocations in view decode. With 10 records × 6 labels each,
/// that's ~140 string allocations eliminated by zero-copy.
pub fn log_request(n: usize) -> LogRequest {
    let records = (0..n)
        .map(|i| {
            let mut labels = HashMap::new();
            for j in 0..6 {
                labels.insert(format!("label-key-{j}"), format!("label-value-{i}-{j}"));
            }

            LogRecord {
                timestamp_nanos: 1_700_000_000_000_000_000 + i as i64,
                service_name: "api-gateway".into(),
                instance_id: format!("instance-{i:04x}"),
                severity: log_record::Severity::SEVERITY_INFO.into(),
                message: format!(
                    "Processing request from client {i}: \
                     GET /api/v1/users?page={}&limit=50 completed in {}ms with status 200 OK",
                    i % 100,
                    42 + i % 200
                ),
                labels,
                trace_id: format!("{:032x}", 0xDEAD_BEEF_0000_0000u64 + i as u64),
                span_id: format!("{:016x}", 0xCAFE_0000u64 + i as u64),
                source: LogSource {
                    file: format!("src/handlers/user_{}.rs", i % 10),
                    line: 42 + (i as i32 % 500),
                    function: format!("handle_get_users_{}", i % 5),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }
        })
        .collect();

    LogRequest {
        records,
        ..Default::default()
    }
}

pub fn large_payload() -> Payload {
    let mut attributes = HashMap::new();
    for i in 0..50 {
        attributes.insert(
            format!("attribute-key-{i:03}"),
            format!("attribute-value-{i:03}-with-some-extra-content"),
        );
    }

    let mut headers = HashMap::new();
    for i in 0..20 {
        headers.insert(format!("x-header-{i:03}"), format!("header-value-{i:03}"));
    }

    Payload {
        id: 999_999,
        timestamp_nanos: 1_700_000_000_000_000_000,
        latitude: 37.774929,
        longitude: -122.419416,
        active: true,
        trace_id: 0xFFFF_FFFF_FFFF_FFFF,
        name: "large-benchmark-payload-with-a-longer-name-for-realistic-sizing-test-scenario"
            .into(),
        description: "A".repeat(200),
        region: "us-west-2-extra-long-region-identifier".into(),
        data: vec![0xAB; 1024 * 1024], // 1 MiB
        status: Status::STATUS_PENDING.into(),
        metadata: Metadata {
            request_id: "req-large-payload-benchmark-test-identifier-12345".into(),
            user_agent: "connectrpc-rs/bench/1.0 (large payload test suite)".into(),
            created_at: 1_700_000_000,
            headers,
            ..Default::default()
        }
        .into(),
        scores: (0..100).collect(),
        tags: (0..20).map(|i| format!("tag-{i:03}-benchmark")).collect(),
        attributes,
        ..Default::default()
    }
}
