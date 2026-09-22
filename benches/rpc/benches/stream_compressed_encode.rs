//! What the segmented encode costs per streamed view item once the response
//! is compressed, and what it saves when it is not.
//!
//! A server-streaming handler that yields views has each item encoded
//! through [`Encodable::encode_segments`], so a large borrowed field is
//! captured by reference count instead of copied. That choice is made before
//! the framing layer applies the negotiated compression, and a compressor
//! needs one contiguous input, so a compressed item is flattened right back.
//! With the default policy and a client that advertises gzip — connect-go's
//! default — every item past the segment threshold pays the rope for
//! nothing. The flatten itself is the one payload copy a contiguous encode
//! makes anyway; what the rope adds around it — its tail buffer, the segment
//! list, the fragments re-copied between captures — is the waste, and the
//! gzip arms show how it compares to the compressor's own cost.
//!
//! Each arm drives a real [`ConnectRpcService`] in process: a streaming
//! request whose handler yields `ITEMS` pre-decoded views, framed through the
//! same batching body the server uses, and drained frame by frame. The arms
//! differ only in whether the item encodes through `encode_segments`
//! (`segmented`, the generated `OwnedView` impl) or `encode` (`contiguous`),
//! and in whether the request negotiated gzip. The identity arms are the
//! control that shows what segmenting buys when the framing layer can use it.
//! Every arm carries the same fixed per-request cost — dispatch, the request
//! decode, the response headers — so the arms compare directly but the
//! per-byte figure reads a little low at the small sizes.
//!
//! Two field shapes: one large string field, which the rope captures whole,
//! and 1 KiB strings summing to the same size, none of which it can capture.
//! The sizes straddle the segment threshold, so the smallest is a control
//! where both arms run the same code.

use std::sync::{Arc, Mutex};

use buffa::view::OwnedView;
use bytes::Bytes;
use connectrpc::handler::streaming_handler_fn;
use connectrpc::{
    CodecFormat, ConnectError, ConnectRpcService, Encodable, RequestContext, Response, Router,
};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use http::{Request, header};
use http_body_util::{BodyExt, Full};
use rpc_bench::proto::bench::v1::__buffa::view::BloatEchoView;
use rpc_bench::proto::bench::v1::BloatEcho;
use tower::Service as _;

/// Items per streamed response. Enough to amortise dispatch and the request
/// parse, few enough that an uncompressed iteration stays in the tens of
/// microseconds at the small sizes.
const ITEMS: usize = 16;

type Item = OwnedView<BloatEchoView<'static>>;

/// Where the setup leaves the items for the handler to take. The bench
/// decodes the views outside the timed region so the measurement is the
/// encode and framing of an item, not its decode.
type Slot = Arc<Mutex<Vec<Item>>>;

/// A view that encodes through `Encodable::encode` only, so the default
/// `encode_segments` hands the framing layer one contiguous buffer — the
/// pre-0.9 path, and what the segmented arm is measured against.
struct Contiguous(Item);

impl Encodable<BloatEcho> for Contiguous {
    fn encode(&self, codec: CodecFormat) -> Result<Bytes, ConnectError> {
        self.0.encode(codec)
    }
}

/// Text that compresses like a log line rather than like a run of one byte.
/// Deterministic, so every item and every arm sees the same bytes; `seed`
/// keeps the 1 KiB fields of one item from being copies of each other, which
/// gzip would otherwise match whole.
fn filler(len: usize, seed: u64) -> String {
    const WORDS: [&str; 16] = [
        "request", "tenant", "latency", "span", "error", "retry", "region", "cache", "upstream",
        "timeout", "header", "route", "shard", "replica", "queue", "trace",
    ];
    let mut out = String::with_capacity(len + 16);
    // Knuth's MMIX LCG; the seed is mixed with the xorshift* multiplier so
    // seed 0 is not a degenerate start.
    let mut state = 0x2545_F491_4F6C_DD1D_u64 ^ seed;
    while out.len() < len {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let word = WORDS[(state >> 60) as usize];
        out.push_str(word);
        out.push('=');
        out.push_str(&((state >> 32) as u32 % 10_000).to_string());
        out.push(' ');
    }
    out.truncate(len);
    out
}

/// How an item's payload is divided between fields.
#[derive(Clone, Copy)]
enum Shape {
    /// All of it in one `string` field, which the rope captures whole.
    OneField,
    /// 1 KiB `string` fields, none of which reaches the segment threshold.
    ManyFields,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Self::OneField => "one_field",
            Self::ManyFields => "many_fields",
        }
    }

    /// The wire bytes of one item carrying `size` bytes of payload.
    fn encoded_item(self, size: usize) -> Bytes {
        let msg = match self {
            Self::OneField => BloatEcho {
                user_agent: filler(size, 0),
                ..Default::default()
            },
            Self::ManyFields => BloatEcho {
                tags: (0..size / 1024).map(|i| filler(1024, i as u64)).collect(),
                ..Default::default()
            },
        };
        Bytes::from(buffa::Message::encode_to_vec(&msg))
    }
}

/// Decode `ITEMS` views, each from its own copy of `wire` so the backing
/// buffer an item captures from is that item's alone.
fn decode_items(wire: &Bytes) -> Vec<Item> {
    (0..ITEMS)
        .map(|_| OwnedView::decode(Bytes::copy_from_slice(wire)).expect("decode view"))
        .collect()
}

/// Two server-streaming routes over the same item slot; the handler takes
/// whatever the setup left there and yields it either as-is (the generated
/// `OwnedView` impl, which segments) or wrapped in [`Contiguous`].
fn service(slot: &Slot) -> ConnectRpcService<Router> {
    let segmented = Arc::clone(slot);
    let contiguous = Arc::clone(slot);
    let router = Router::new()
        .route_server_stream(
            "bench.v1.BloatEchoService",
            "Segmented",
            streaming_handler_fn(move |_ctx: RequestContext, _req: BloatEcho| {
                let items = std::mem::take(&mut *segmented.lock().expect("slot"));
                async move { Response::stream_ok(futures::stream::iter(items.into_iter().map(Ok))) }
            }),
        )
        .route_server_stream(
            "bench.v1.BloatEchoService",
            "Contiguous",
            streaming_handler_fn(move |_ctx: RequestContext, _req: BloatEcho| {
                let items = std::mem::take(&mut *contiguous.lock().expect("slot"));
                async move {
                    Response::stream_ok(futures::stream::iter(
                        items.into_iter().map(|v| Ok(Contiguous(v))),
                    ))
                }
            }),
        );
    ConnectRpcService::new(router)
}

/// A Connect server-streaming request for `method`, advertising gzip when
/// `gzip` so the response negotiates it. The request message is empty: the
/// handler ignores it.
fn request(method: &str, gzip: bool) -> Request<Full<Bytes>> {
    let mut req = Request::builder()
        .method(http::Method::POST)
        .uri(format!("/bench.v1.BloatEchoService/{method}"))
        .header(header::CONTENT_TYPE, "application/connect+proto");
    if gzip {
        req = req.header("connect-accept-encoding", "gzip");
    }
    // One empty data envelope: flags 0, length 0.
    req.body(Full::new(Bytes::from_static(&[0, 0, 0, 0, 0])))
        .expect("request")
}

/// Drive one request through the service and drain the response body frame
/// by frame, the way a connection would. Returns the data frames emitted.
async fn run(svc: &mut ConnectRpcService<Router>, method: &str, gzip: bool) -> Vec<Bytes> {
    let resp = svc.call(request(method, gzip)).await.expect("infallible");
    assert_eq!(resp.status(), http::StatusCode::OK);
    let negotiated = resp.headers().contains_key("connect-content-encoding");
    assert_eq!(
        negotiated, gzip,
        "gzip negotiation did not match the request"
    );
    let mut body = resp.into_body();
    let mut frames = Vec::new();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame.expect("frame").into_data() {
            frames.push(data);
        }
    }
    // Every item costs at least its 5-byte envelope header, so a response
    // that framed nothing — an empty slot — cannot pass as a fast one.
    let emitted: usize = frames.iter().map(Bytes::len).sum();
    assert!(emitted > ITEMS * 5, "{method}: the handler framed no items");
    frames
}

/// Whether any of `frames` lies inside one of the `backings` — a payload
/// that reached the body by reference count rather than by copy.
fn aliases_backing(frames: &[Bytes], backings: &[(usize, usize)]) -> bool {
    frames.iter().any(|frame| {
        let start = frame.as_ptr() as usize;
        backings
            .iter()
            .any(|&(base, len)| start >= base && start + frame.len() <= base + len)
    })
}

/// Put `items` where the handler will find them, after checking that the
/// previous request took its own.
fn fill(slot: &Slot, items: Vec<Item>) {
    let stale = std::mem::replace(&mut *slot.lock().expect("slot"), items);
    assert!(stale.is_empty(), "a request did not take its items");
}

fn bench_stream_compressed_encode(c: &mut Criterion) {
    // Single-threaded: the whole path runs inline on the polling thread, and
    // a pinned-core run must not have worker threads to contend with.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let slot: Slot = Arc::new(Mutex::new(Vec::new()));
    let mut svc = service(&slot);

    for shape in [Shape::OneField, Shape::ManyFields] {
        let mut group = c.benchmark_group(format!("stream_compressed_encode/{}", shape.name()));

        // 4 KiB sits under the 16 KiB segment threshold, so both arms encode
        // contiguously there and the pair should match; 32 KiB and 256 KiB
        // clear it.
        for size in [4 * 1024usize, 32 * 1024, 256 * 1024] {
            let wire = shape.encoded_item(size);
            group.throughput(Throughput::Bytes((wire.len() * ITEMS) as u64));

            for (encoding, gzip) in [("gzip", true), ("identity", false)] {
                for method in ["Segmented", "Contiguous"] {
                    // Every arm must produce a well-formed response before it
                    // is timed; the identity body carries every payload byte.
                    let items = decode_items(&wire);
                    // Held for the duration so a freed backing cannot be
                    // reused for a frame and alias by coincidence.
                    let held: Vec<Bytes> = items.iter().map(|item| item.bytes().clone()).collect();
                    let backings: Vec<(usize, usize)> = held
                        .iter()
                        .map(|bytes| (bytes.as_ptr() as usize, bytes.len()))
                        .collect();
                    fill(&slot, items);
                    let frames = rt.block_on(run(&mut svc, method, gzip));
                    let emitted: usize = frames.iter().map(Bytes::len).sum();
                    if gzip {
                        assert!(
                            emitted < wire.len() * ITEMS,
                            "{method}: gzip did not shrink the body"
                        );
                    } else {
                        assert!(
                            emitted > wire.len() * ITEMS,
                            "{method}: identity body is short"
                        );
                    }
                    // The segmented arm is only a different arm if its
                    // captures reach the body untouched, which happens for
                    // exactly one combination: uncompressed, past the
                    // threshold, through the generated impl, with a field
                    // large enough to capture. Anywhere else a frame aliasing
                    // an item's buffer would mean a copy was skipped that the
                    // arm claims to make.
                    let captured = !gzip
                        && method == "Segmented"
                        && size >= 16 * 1024
                        && matches!(shape, Shape::OneField);
                    assert_eq!(
                        aliases_backing(&frames, &backings),
                        captured,
                        "{encoding}/{method}/{size}: capture did not match the arm"
                    );
                    drop(held);

                    let id = BenchmarkId::new(
                        format!("{encoding}/{}", method.to_ascii_lowercase()),
                        size,
                    );
                    group.bench_with_input(id, &wire, |b, wire| {
                        // The service clone is a handful of `Arc` bumps; a
                        // `Fn` setup cannot lend the one `&mut svc` out.
                        // `LargeInput` keeps criterion from materialising
                        // hundreds of decoded batches ahead of the timer.
                        b.to_async(&rt).iter_batched(
                            || (decode_items(wire), svc.clone()),
                            |(items, mut svc)| {
                                let slot = Arc::clone(&slot);
                                async move {
                                    fill(&slot, items);
                                    std::hint::black_box(run(&mut svc, method, gzip).await)
                                }
                            },
                            BatchSize::LargeInput,
                        );
                    });
                }
            }
        }

        group.finish();
    }
}

criterion_group!(benches, bench_stream_compressed_encode);
criterion_main!(benches);
