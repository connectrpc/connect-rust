//! Request-body reader cost for client-streaming calls, without a network.
//!
//! Envelope-framed messages are fed to `ConnectRpcService` through an
//! in-memory body, so what is timed is the handler's request stream decoding
//! the body, plus the call and response around it. A client and a server
//! sharing one machine would time each other instead.
//!
//! - `light` handlers count 5-byte messages, so the per-message decoding cost
//!   dominates; `real` runs the `BenchService` handler on ~100-byte messages.
//! - `body_ready` hands over one message per body frame that is always ready;
//!   `body_chunked` hands over 16 KiB frames; `body_yielding` returns pending
//!   once before every frame, as a network body does between DATA frames.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::StreamExt as _;
use http_body::{Body, Frame};
use http_body_util::BodyExt as _;
use tower::Service as _;

use buffa::Message as _;
use buffa_types::Empty;
use connectrpc::{ConnectRpcService, Router, ServiceStream, client_streaming_handler_fn};
use rpc_bench::*;

const MESSAGES: usize = 1000;
const CHUNK: usize = 16 * 1024;

/// A request body that yields its frames, optionally returning pending once
/// (after waking itself) before each.
struct FramedBody {
    frames: VecDeque<Bytes>,
    yield_before_frame: bool,
    yielded: bool,
}

impl Body for FramedBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if self.yield_before_frame && !self.yielded && !self.frames.is_empty() {
            self.yielded = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.yielded = false;
        Poll::Ready(self.frames.pop_front().map(|frame| Ok(Frame::data(frame))))
    }
}

fn envelope(payload: &[u8]) -> Vec<u8> {
    let mut wire = vec![0_u8];
    wire.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    wire.extend_from_slice(payload);
    wire
}

/// `MESSAGES` messages of `payload`, one per frame.
fn one_per_frame(payload: &[u8]) -> Vec<Bytes> {
    (0..MESSAGES)
        .map(|_| Bytes::from(envelope(payload)))
        .collect()
}

/// The same messages in frames of at most `CHUNK` bytes.
fn chunked(payload: &[u8]) -> Vec<Bytes> {
    let wire: Vec<u8> = (0..MESSAGES).flat_map(|_| envelope(payload)).collect();
    wire.chunks(CHUNK).map(Bytes::copy_from_slice).collect()
}

fn light_service() -> ConnectRpcService<Router> {
    let count =
        client_streaming_handler_fn(|_ctx, mut requests: ServiceStream<Empty>| async move {
            let mut count = 0;
            while let Some(request) = requests.next().await {
                request?;
                count += 1;
            }
            if count != MESSAGES {
                return Err(connectrpc::ConnectError::internal(format!(
                    "handler saw {count} of {MESSAGES} messages"
                )));
            }
            connectrpc::Response::ok(Empty::default())
        });
    ConnectRpcService::new(Router::new().route_client_stream("bench.v1.Reader", "Count", count))
}

fn real_service() -> ConnectRpcService<Router> {
    ConnectRpcService::new(Arc::new(BenchServiceImpl).register(Router::new()))
}

/// One client-streaming call; returns the response body.
async fn call(
    mut service: ConnectRpcService<Router>,
    path: &str,
    frames: VecDeque<Bytes>,
    yield_before_frame: bool,
) -> Bytes {
    let request = http::Request::post(path)
        .header("content-type", "application/connect+proto")
        .header("connect-protocol-version", "1")
        .body(FramedBody {
            frames,
            yield_before_frame,
            yielded: false,
        })
        .unwrap();
    let response = service.call(request).await.unwrap();
    response.into_body().collect().await.unwrap().to_bytes()
}

/// One benchmarked call shape.
struct Case {
    name: &'static str,
    service: ConnectRpcService<Router>,
    path: &'static str,
    frames: Vec<Bytes>,
    yield_before_frame: bool,
}

impl Case {
    async fn call(&self, frames: VecDeque<Bytes>) -> Bytes {
        call(
            self.service.clone(),
            self.path,
            frames,
            self.yield_before_frame,
        )
        .await
    }
}

fn bench_reader(c: &mut Criterion) {
    const LIGHT: &str = "/bench.v1.Reader/Count";

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let real_payload = small_request().encode_to_vec();
    let case = |name, path, frames, yield_before_frame| Case {
        name,
        service: if path == LIGHT {
            light_service()
        } else {
            real_service()
        },
        path,
        frames,
        yield_before_frame,
    };
    let cases = [
        case("light/body_ready", LIGHT, one_per_frame(&[]), false),
        case("light/body_chunked", LIGHT, chunked(&[]), false),
        case("light/body_yielding", LIGHT, one_per_frame(&[]), true),
        case(
            "real/body_ready",
            "/bench.v1.BenchService/ClientStream",
            one_per_frame(&real_payload),
            false,
        ),
    ];

    // One untimed call per case, so a case that fails cannot be mistaken for
    // a fast one: an error reaches the client in the end-of-stream envelope.
    for case in &cases {
        let end = rt.block_on(case.call(case.frames.iter().cloned().collect()));
        assert!(
            !end.windows(7).any(|w| w == b"\"error\""),
            "{}: {}",
            case.name,
            String::from_utf8_lossy(&end)
        );
    }

    let mut group = c.benchmark_group("client_stream_reader");
    group.throughput(Throughput::Elements(MESSAGES as u64));
    for case in &cases {
        group.bench_function(BenchmarkId::from_parameter(case.name), |b| {
            b.to_async(&rt).iter_batched(
                || case.frames.iter().cloned().collect::<VecDeque<_>>(),
                |frames| case.call(frames),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_reader);
criterion_main!(benches);
