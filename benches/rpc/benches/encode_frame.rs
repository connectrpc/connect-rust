//! Encode and de-frame cost, isolated from HTTP.
//!
//! - `view_encode/*`: a decoded view (`ViewEncode`) -> `Bytes`, three ways:
//!   buffa's `encode_to_bytes`, `Bytes::from(encode_to_vec())`, and
//!   `connectrpc::__codegen::encode_view_body` — the path every generated
//!   `impl Encodable<M> for MView` takes, which sizes its own buffer.
//! - `deframe/*`: one gRPC request envelope (as `collect().to_bytes()` hands
//!   it to a handler) -> payload `Bytes`, via `Envelope::decode_with_limit`
//!   on a `BytesMut` copy of the body (what the gRPC unary / server-streaming
//!   paths used to do) vs on the `Bytes` itself.
//!
//! Shapes: ~350 B mixed message, ~4 KB and ~270 KB of string/varint-heavy
//! log records, and ~256 KB in four large strings (memcpy-bound control).
//!
//! ```text
//! cargo bench -p rpc-bench --bench encode_frame
//! ```

use std::hint::black_box;

use buffa::view::MessageView;
use buffa::{Message, ViewEncode};
use bytes::{BufMut, Bytes, BytesMut};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use connectrpc::CodecFormat;
use connectrpc::envelope::{Envelope, HEADER_SIZE, flags};
use rpc_bench::proto::bench::v1::__buffa::view::FewLargeStringsView;
use rpc_bench::proto::bench::v1::FewLargeStrings;
use rpc_bench::{
    BenchResponse, BenchResponseView, LogRequest, LogRequestView, log_request, small_payload,
};

/// `LogRequest` with enough records to reach roughly `target` encoded bytes.
fn log_request_of_size(target: usize) -> LogRequest {
    let per = log_request(1).encoded_len() as usize;
    log_request(target.div_ceil(per).max(1))
}

fn few_large_strings(each: usize) -> FewLargeStrings {
    let body = "x".repeat(each);
    FewLargeStrings {
        body_a: body.clone(),
        body_b: body.clone(),
        body_c: body.clone(),
        body_d: body,
        ts: 1,
        seq: 2,
        ..Default::default()
    }
}

/// One gRPC data envelope: 5-byte header + payload.
fn framed(payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len());
    buf.put_u8(flags::DATA);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

macro_rules! bench_shape {
    ($c:expr, $name:expr, $msg:expr, $view_ty:ty) => {{
        let wire = Bytes::from($msg.encode_to_vec());
        let view = <$view_ty>::decode_view(&wire).expect("decode view");
        let len = wire.len() as u64;

        let mut g = $c.benchmark_group(format!("view_encode/{}", $name));
        g.throughput(Throughput::Bytes(len));
        g.bench_function("buffa_view_encode_to_bytes", |b| {
            b.iter(|| black_box(ViewEncode::encode_to_bytes(&view)))
        });
        g.bench_function("buffa_view_vec_into_bytes", |b| {
            b.iter(|| black_box(Bytes::from(ViewEncode::encode_to_vec(&view))))
        });
        g.bench_function("connectrpc_encode_view_body", |b| {
            b.iter(|| {
                black_box(
                    connectrpc::__codegen::encode_view_body(&view, CodecFormat::Proto).unwrap(),
                )
            })
        });
        g.finish();

        let body = framed(&wire);
        let mut g = $c.benchmark_group(format!("deframe/{}", $name));
        g.throughput(Throughput::Bytes(len));
        g.bench_function("bytesmut_copy", |b| {
            b.iter(|| {
                let mut buf = BytesMut::from(&body[..]);
                black_box(Envelope::decode_with_limit(&mut buf, usize::MAX).unwrap())
            })
        });
        g.bench_function("bytes_in_place", |b| {
            b.iter(|| {
                let mut buf = body.clone();
                black_box(Envelope::decode_with_limit(&mut buf, usize::MAX).unwrap())
            })
        });
        g.finish();
    }};
}

fn bench_encode_frame(c: &mut Criterion) {
    bench_shape!(
        c,
        "small_300B",
        BenchResponse {
            payload: small_payload().into(),
            ..Default::default()
        },
        BenchResponseView
    );
    bench_shape!(c, "logs_4KB", log_request_of_size(4 * 1024), LogRequestView);
    bench_shape!(
        c,
        "logs_256KB",
        log_request_of_size(256 * 1024),
        LogRequestView
    );
    bench_shape!(
        c,
        "blob_256KB",
        few_large_strings(64 * 1024),
        FewLargeStringsView
    );
}

criterion_group!(benches, bench_encode_frame);
criterion_main!(benches);
