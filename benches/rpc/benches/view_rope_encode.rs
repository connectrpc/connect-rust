//! What encoding a response view through a rope costs, across payload sizes.
//!
//! Encoding a message into one contiguous buffer copies every field into it.
//! A view's fields are slices into the buffer the view was decoded from, so a
//! rope backed by that buffer can take a large field by reference instead,
//! which is what `encode_view_body_segments` does on the response path.
//!
//! The arms isolate where the cost goes:
//!
//! - `contiguous` — the copy-everything baseline.
//! - `rope_backed` — the rope with the view's own buffer, so captures engage.
//! - `encode_view_segments` — the production entry point, including its size
//!   gate, which is what makes the small sizes match the baseline.
//! - `owned_contiguous` — an owned message, whose `String` fields cannot be
//!   captured; this is why owned bodies are left on the contiguous path.
//! - `rope_unbacked` — a rope with no buffer to capture from. Separates the
//!   rope's own cost from the saving the capture produces.
//!
//! Above the segment threshold the encode is payload-independent: only the
//! framing is still being written.

use buffa::view::MessageView;
use buffa::{Rope, ViewEncode};
use bytes::Bytes;
use connectrpc::{CodecFormat, Encodable};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rpc_bench::proto::bench::v1::__buffa::view::FewLargeStringsView;
use rpc_bench::proto::bench::v1::FewLargeStrings;

/// Build a `FewLargeStrings` whose four string fields are `each` bytes, then
/// return its encoded wire bytes. The view decoded from these borrows each
/// field directly out of the buffer, which is exactly what a rope backed by
/// that buffer can capture without copying.
fn encoded_message(each: usize) -> Bytes {
    let body = "x".repeat(each);
    let msg = FewLargeStrings {
        body_a: body.clone(),
        body_b: body.clone(),
        body_c: body.clone(),
        body_d: body,
        ts: 1,
        seq: 2,
        ..Default::default()
    };
    Bytes::from(buffa::Message::encode_to_vec(&msg))
}

fn bench_view_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("view_encode");

    // Straddle the 16 KiB segment threshold in both directions: at 256 B and
    // 1 KiB neither the fields nor the whole message reach it, from 16 KiB up
    // every field clears it.
    for each in [256usize, 1024, 16 * 1024, 256 * 1024, 1024 * 1024] {
        let buffer = encoded_message(each);
        let view = FewLargeStringsView::decode_view(&buffer).expect("decode view");

        group.throughput(Throughput::Bytes(buffer.len() as u64));

        // Today's path: one contiguous buffer, every field memcpy'd in.
        group.bench_with_input(BenchmarkId::new("contiguous", each), &view, |b, view| {
            b.iter(|| std::hint::black_box(view.encode_to_bytes()))
        });

        // Rope with the decode buffer attached, so a large borrowed field is
        // captured by reference count instead of copied.
        group.bench_with_input(BenchmarkId::new("rope_backed", each), &view, |b, view| {
            b.iter(|| {
                let mut rope = Rope::new().with_backing(buffer.clone());
                ViewEncode::encode(view, &mut rope);
                std::hint::black_box(rope.into_segments())
            });
        });

        // The production entry point, so the size gate is included. Below one
        // segment it falls back to contiguous rather than pay for a rope that
        // cannot capture anything.
        group.bench_with_input(
            BenchmarkId::new("encode_view_segments", each),
            &view,
            |b, view| {
                b.iter(|| {
                    std::hint::black_box(
                        connectrpc::__codegen::encode_view_body_with_min_segment(
                            view,
                            &buffer,
                            CodecFormat::Proto,
                            16 * 1024,
                        )
                        .expect("encode"),
                    )
                });
            },
        );

        // Owned messages are deliberately NOT routed through a rope: their
        // `String` / `Vec<u8>` fields cannot be handed over by reference, so
        // the rope captures nothing and only adds cost. Kept as a measured
        // control for that decision.
        let owned: FewLargeStrings = view.to_owned_message().expect("to owned");
        group.bench_with_input(
            BenchmarkId::new("owned_contiguous", each),
            &owned,
            |b, owned| {
                b.iter(|| {
                    std::hint::black_box(
                        Encodable::<FewLargeStrings>::encode(owned, CodecFormat::Proto)
                            .expect("encode"),
                    )
                });
            },
        );

        // Control: a rope with no backing buffer has nothing to capture, so
        // this arm shows the rope's own cost with the saving removed.
        group.bench_with_input(BenchmarkId::new("rope_unbacked", each), &view, |b, view| {
            b.iter(|| {
                let mut rope = Rope::new();
                ViewEncode::encode(view, &mut rope);
                std::hint::black_box(rope.into_segments())
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_view_encode);
criterion_main!(benches);
