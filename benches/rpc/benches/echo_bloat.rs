//! Codec-layer echo benchmark for ViewEncode — payload-shape sweep.
//!
//! NOTE: This benches raw buffa decode→encode (the codec layer), not
//! connect-rust's handler API — handlers can't return `*View<'a>` yet.
//! It lands ahead of the view-response API it motivates so the numbers
//! are reproducible from this repo; once that API exists this becomes
//! its regression sentinel.
//!
//! Two suites:
//!
//! - **Shape sweep**: five payload shapes × four `{owned,view}×{decode,encode}`
//!   paths, plus an `identity` re-encode reference (decode → encode with no
//!   rebuild — the worst case for views, where a real handler would just
//!   re-serialize the request struct).
//! - **Fanout**: decode once, then N×(build+encode) for N∈{1,4,16,64},
//!   modeling a 1-source → N-reader streaming service where the owned
//!   baseline is forced to clone.
//!
//! The four non-identity paths rebuild the response field-by-field from
//! request data, modeling a handler that **transforms** request fields (the
//! common case), not pure echo. For the pure-echo case see the `identity`
//! row. For "build response from already-resident state" (no decode at all),
//! see buffa's `build_encode` benchmarks.

use buffa::Map;

use buffa::{Message, MessageView, ViewEncode};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use rpc_bench::proto::bench::v1::{
    BloatEchoView, BloatHeaderView, DeepNestedView, FewLargeStringsView, MapDominatedView,
    NestL1View, NestL2View, NestL3View, NestL4View, NestL5View, ScalarHeavyView,
};
use rpc_bench::{
    BloatEcho, BloatHeader, DeepNested, FewLargeStrings, MapDominated, NestL1, NestL2, NestL3,
    NestL4, NestL5, ScalarHeavy,
};

// ──────────────────────────────────────────────────────────────────────
// Adding a new shape:
//   1. Define the message in `proto/echo_bloat.proto`, regenerate
//      (`buf generate` from `benches/rpc/`).
//   2. Add a `mod <name> { payload, owned_owned, view_owned, owned_view,
//      view_view }` block below — each fn is `&[u8] -> Vec<u8>` and the
//      explicit per-field construction is intentional (it makes the
//      alloc/copy/borrow cost of each path visible in the source).
//   3. Add a `bench_shape!` invocation and register it in
//      `criterion_group!`.
//   To make the shape fanout-able, also provide `clone_from(&Owned)`,
//   `borrow_from(&Owned)`, `reborrow_from(&View<'a>)` (each `-> Vec<u8>`)
//   and add a `bench_fanout!` invocation.
// ──────────────────────────────────────────────────────────────────────

/// Run the four-way comparison for one shape. `$paths` is a module providing
/// `owned_owned`, `view_owned`, `owned_view`, `view_view` (each `&[u8] -> Vec<u8>`)
/// and a `payload() -> Owned` builder. Asserts wire-equivalence at startup.
macro_rules! bench_shape {
    ($fn_name:ident, $group:literal, $owned:ty, $paths:path) => {
        fn $fn_name(c: &mut Criterion) {
            use $paths as p;
            let input = p::payload().encode_to_vec();
            eprintln!("{}: {} bytes", $group, input.len());
            let baseline =
                <$owned>::decode_from_slice(&p::owned_owned(&input)).expect("baseline decode");
            for (name, out) in [
                ("view/owned", p::view_owned(&input)),
                ("owned/view", p::owned_view(&input)),
                ("view/view", p::view_view(&input)),
            ] {
                let got = <$owned>::decode_from_slice(&out).expect("roundtrip decode");
                assert_eq!(got, baseline, "{} {name} diverges", $group);
            }
            let mut g = c.benchmark_group($group);
            g.throughput(Throughput::Bytes(input.len() as u64));
            // Reference floor: decode → re-encode the SAME struct (no
            // rebuild). Against this, view→view shows the win when a
            // handler does zero per-field work; owned/owned should land
            // within ~1% of it (move-based rebuild is nearly free).
            g.bench_function("identity", |b| {
                b.iter(|| {
                    black_box(
                        <$owned>::decode_from_slice(black_box(&input))
                            .unwrap()
                            .encode_to_vec(),
                    )
                })
            });
            g.bench_function("owned/owned", |b| {
                b.iter(|| black_box(p::owned_owned(black_box(&input))))
            });
            g.bench_function("view/owned", |b| {
                b.iter(|| black_box(p::view_owned(black_box(&input))))
            });
            g.bench_function("owned/view", |b| {
                b.iter(|| black_box(p::owned_view(black_box(&input))))
            });
            g.bench_function("view/view", |b| {
                b.iter(|| black_box(p::view_view(black_box(&input))))
            });
            g.finish();
        }
    };
}

// ── scalar_heavy: 16 ints + 2 short strings, ~2 string allocs ────────

mod scalar_heavy {
    use super::*;

    pub fn payload() -> ScalarHeavy {
        ScalarHeavy {
            a: 1_111_111_111_111,
            b: 2_222_222_222_222,
            c: 3_333_333_333_333,
            d: 4_444_444_444_444,
            e: 5_555_555_555_555,
            f: 6_666_666_666_666,
            g: 7_777_777_777_777,
            h: 8_888_888_888_888,
            i: 9_999_999_999_999,
            j: 1_010_101_010_101,
            k: 1_212_121_212_121,
            l: 1_313_131_313_131,
            m: 1_000_001,
            n: 2_000_002,
            o: 3_000_003,
            p: 4_000_004,
            note_a: "scalar-heavy-shape-a".into(),
            note_b: "scalar-heavy-shape-b".into(),
            ..Default::default()
        }
    }

    pub fn owned_owned(input: &[u8]) -> Vec<u8> {
        let r = ScalarHeavy::decode_from_slice(input).unwrap();
        ScalarHeavy {
            a: r.a,
            b: r.b,
            c: r.c,
            d: r.d,
            e: r.e,
            f: r.f,
            g: r.g,
            h: r.h,
            i: r.i,
            j: r.j,
            k: r.k,
            l: r.l,
            m: r.m,
            n: r.n,
            o: r.o,
            p: r.p,
            note_a: r.note_a,
            note_b: r.note_b,
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn view_owned(input: &[u8]) -> Vec<u8> {
        let r = ScalarHeavyView::decode_view(input).unwrap();
        ScalarHeavy {
            a: r.a,
            b: r.b,
            c: r.c,
            d: r.d,
            e: r.e,
            f: r.f,
            g: r.g,
            h: r.h,
            i: r.i,
            j: r.j,
            k: r.k,
            l: r.l,
            m: r.m,
            n: r.n,
            o: r.o,
            p: r.p,
            note_a: r.note_a.into(),
            note_b: r.note_b.into(),
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn owned_view(input: &[u8]) -> Vec<u8> {
        let r = ScalarHeavy::decode_from_slice(input).unwrap();
        ScalarHeavyView {
            a: r.a,
            b: r.b,
            c: r.c,
            d: r.d,
            e: r.e,
            f: r.f,
            g: r.g,
            h: r.h,
            i: r.i,
            j: r.j,
            k: r.k,
            l: r.l,
            m: r.m,
            n: r.n,
            o: r.o,
            p: r.p,
            note_a: &r.note_a,
            note_b: &r.note_b,
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn view_view(input: &[u8]) -> Vec<u8> {
        let r = ScalarHeavyView::decode_view(input).unwrap();
        ScalarHeavyView {
            a: r.a,
            b: r.b,
            c: r.c,
            d: r.d,
            e: r.e,
            f: r.f,
            g: r.g,
            h: r.h,
            i: r.i,
            j: r.j,
            k: r.k,
            l: r.l,
            m: r.m,
            n: r.n,
            o: r.o,
            p: r.p,
            note_a: r.note_a,
            note_b: r.note_b,
            ..Default::default()
        }
        .encode_to_vec()
    }
}

// ── few_large_strings: 4×~1200B + 2 ints, 4 allocs / ~5KB ────────────

mod few_large_strings {
    use super::*;

    pub fn payload() -> FewLargeStrings {
        let chunk = "the-quick-brown-fox-jumps-over-the-lazy-dog-0123456789-".repeat(22);
        FewLargeStrings {
            body_a: chunk.clone(),
            body_b: chunk.clone(),
            body_c: chunk.clone(),
            body_d: chunk,
            ts: 1_700_000_000_000_000_000,
            seq: 424_242,
            ..Default::default()
        }
    }

    pub fn owned_owned(input: &[u8]) -> Vec<u8> {
        let r = FewLargeStrings::decode_from_slice(input).unwrap();
        FewLargeStrings {
            body_a: r.body_a,
            body_b: r.body_b,
            body_c: r.body_c,
            body_d: r.body_d,
            ts: r.ts,
            seq: r.seq,
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn view_owned(input: &[u8]) -> Vec<u8> {
        let r = FewLargeStringsView::decode_view(input).unwrap();
        FewLargeStrings {
            body_a: r.body_a.into(),
            body_b: r.body_b.into(),
            body_c: r.body_c.into(),
            body_d: r.body_d.into(),
            ts: r.ts,
            seq: r.seq,
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn owned_view(input: &[u8]) -> Vec<u8> {
        let r = FewLargeStrings::decode_from_slice(input).unwrap();
        FewLargeStringsView {
            body_a: &r.body_a,
            body_b: &r.body_b,
            body_c: &r.body_c,
            body_d: &r.body_d,
            ts: r.ts,
            seq: r.seq,
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn view_view(input: &[u8]) -> Vec<u8> {
        let r = FewLargeStringsView::decode_view(input).unwrap();
        FewLargeStringsView {
            body_a: r.body_a,
            body_b: r.body_b,
            body_c: r.body_c,
            body_d: r.body_d,
            ts: r.ts,
            seq: r.seq,
            ..Default::default()
        }
        .encode_to_vec()
    }
}

// ── many_small_strings (= original BloatEcho) ────────────────────────

mod many_small_strings {
    use super::*;

    pub fn payload() -> BloatEcho {
        let header = |name: &str, value: &str| BloatHeader {
            name: name.into(),
            value: value.into(),
            source: "client-supplied".into(),
            note: "validated against allowlist; forwarded as-is".into(),
            ..Default::default()
        };
        let labels: Map<String, String> = (0..11)
            .map(|i| {
                (
                    format!("k8s.label.app.example.com/tier-{i:02}"),
                    format!("workload-partition-{i:02}-us-west-2a-r5.2xlarge"),
                )
            })
            .collect();
        BloatEcho {
            tenant_id: "tenant-0193fae1-7d4c-77a2-b8e0-0e9c6ab2d041".into(),
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7".into(),
            span_id: "00f067aa0ba902b7".into(),
            service: "api-gateway.ingress.svc.cluster.local".into(),
            region: "us-west-2".into(),
            instance_id: "i-0a1b2c3d4e5f67890-spot-r5.2xlarge".into(),
            request_path: "/api/v2/orders/0193fae1-7d4c/line-items?expand=product,inventory".into(),
            user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15".into(),
            timestamp_nanos: 1_714_000_000_000_000_000,
            status_code: 200,
            tags: (0..9)
                .map(|i| format!("tag-{i:02}-canary-rollout-cohort"))
                .collect(),
            labels,
            auth: header(
                "authorization",
                "Bearer benchmark-placeholder-not-a-real-token",
            )
            .into(),
            origin: header("x-forwarded-for", "203.0.113.42, 198.51.100.7, 10.0.0.1").into(),
            extra_headers: vec![
                header("x-request-id", "req-0193fae1-7d4c-77a2-b8e0-0e9c6ab2d041"),
                header(
                    "x-correlation-id",
                    "corr-77a2b8e0-0e9c-6ab2-d041-4bf92f3577b3",
                ),
                header("x-client-version", "mobile-ios/4.21.0 (build 8412; arm64)"),
                header("accept-language", "en-US,en;q=0.9,fr-CA;q=0.5"),
            ],
            ..Default::default()
        }
    }

    fn borrow_header(h: &BloatHeader) -> BloatHeaderView<'_> {
        BloatHeaderView {
            name: &h.name,
            value: &h.value,
            source: &h.source,
            note: &h.note,
            ..Default::default()
        }
    }

    fn echo_header_view<'a>(h: &BloatHeaderView<'a>) -> BloatHeaderView<'a> {
        BloatHeaderView {
            name: h.name,
            value: h.value,
            source: h.source,
            note: h.note,
            ..Default::default()
        }
    }

    fn header_to_owned(h: &BloatHeaderView<'_>) -> BloatHeader {
        BloatHeader {
            name: h.name.into(),
            value: h.value.into(),
            source: h.source.into(),
            note: h.note.into(),
            ..Default::default()
        }
    }

    pub fn owned_owned(input: &[u8]) -> Vec<u8> {
        let r = BloatEcho::decode_from_slice(input).unwrap();
        BloatEcho {
            tenant_id: r.tenant_id,
            trace_id: r.trace_id,
            span_id: r.span_id,
            service: r.service,
            region: r.region,
            instance_id: r.instance_id,
            request_path: r.request_path,
            user_agent: r.user_agent,
            timestamp_nanos: r.timestamp_nanos,
            status_code: r.status_code,
            tags: r.tags,
            labels: r.labels,
            auth: r.auth,
            origin: r.origin,
            extra_headers: r.extra_headers,
            ..Default::default()
        }
        .encode_to_vec()
    }

    /// Build+encode one response by **cloning** every field of an
    /// already-decoded owned source. Forced-clone path: the source can be
    /// fanned out to N readers without consuming it.
    pub fn clone_from(r: &BloatEcho) -> Vec<u8> {
        BloatEcho {
            tenant_id: r.tenant_id.clone(),
            trace_id: r.trace_id.clone(),
            span_id: r.span_id.clone(),
            service: r.service.clone(),
            region: r.region.clone(),
            instance_id: r.instance_id.clone(),
            request_path: r.request_path.clone(),
            user_agent: r.user_agent.clone(),
            timestamp_nanos: r.timestamp_nanos,
            status_code: r.status_code,
            tags: r.tags.clone(),
            labels: r.labels.clone(),
            auth: r.auth.clone(),
            origin: r.origin.clone(),
            extra_headers: r.extra_headers.clone(),
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn view_owned(input: &[u8]) -> Vec<u8> {
        let r = BloatEchoView::decode_view(input).unwrap();
        BloatEcho {
            tenant_id: r.tenant_id.into(),
            trace_id: r.trace_id.into(),
            span_id: r.span_id.into(),
            service: r.service.into(),
            region: r.region.into(),
            instance_id: r.instance_id.into(),
            request_path: r.request_path.into(),
            user_agent: r.user_agent.into(),
            timestamp_nanos: r.timestamp_nanos,
            status_code: r.status_code,
            tags: r.tags.iter().map(|s| (*s).into()).collect(),
            labels: r
                .labels
                .iter()
                .map(|(k, v)| ((*k).into(), (*v).into()))
                .collect(),
            auth: r.auth.as_option().map(header_to_owned).into(),
            origin: r.origin.as_option().map(header_to_owned).into(),
            extra_headers: r.extra_headers.iter().map(header_to_owned).collect(),
            ..Default::default()
        }
        .encode_to_vec()
    }

    /// Build+encode one response as a `BloatEchoView` **borrowing** from an
    /// already-decoded owned source. ViewEncode-only fanout: owned app
    /// state, N borrowed responses.
    pub fn borrow_from(r: &BloatEcho) -> Vec<u8> {
        BloatEchoView {
            tenant_id: &r.tenant_id,
            trace_id: &r.trace_id,
            span_id: &r.span_id,
            service: &r.service,
            region: &r.region,
            instance_id: &r.instance_id,
            request_path: &r.request_path,
            user_agent: &r.user_agent,
            timestamp_nanos: r.timestamp_nanos,
            status_code: r.status_code,
            tags: r.tags.iter().map(String::as_str).collect(),
            labels: r
                .labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
            auth: r
                .auth
                .as_option()
                .map(borrow_header)
                .map(From::from)
                .unwrap_or_default(),
            origin: r
                .origin
                .as_option()
                .map(borrow_header)
                .map(From::from)
                .unwrap_or_default(),
            extra_headers: r.extra_headers.iter().map(borrow_header).collect(),
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn owned_view(input: &[u8]) -> Vec<u8> {
        borrow_from(&BloatEcho::decode_from_slice(input).unwrap())
    }

    /// Build+encode one response as a `BloatEchoView` whose `&'a str` fields
    /// flow straight through from an already-decoded source view. Full
    /// view→view fanout: zero string allocs per response.
    pub fn reborrow_from<'a>(r: &BloatEchoView<'a>) -> Vec<u8> {
        BloatEchoView {
            tenant_id: r.tenant_id,
            trace_id: r.trace_id,
            span_id: r.span_id,
            service: r.service,
            region: r.region,
            instance_id: r.instance_id,
            request_path: r.request_path,
            user_agent: r.user_agent,
            timestamp_nanos: r.timestamp_nanos,
            status_code: r.status_code,
            tags: r.tags.iter().copied().collect(),
            labels: r.labels.iter().map(|(k, v)| (*k, *v)).collect(),
            auth: r
                .auth
                .as_option()
                .map(echo_header_view)
                .map(From::from)
                .unwrap_or_default(),
            origin: r
                .origin
                .as_option()
                .map(echo_header_view)
                .map(From::from)
                .unwrap_or_default(),
            extra_headers: r.extra_headers.iter().map(echo_header_view).collect(),
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn view_view(input: &[u8]) -> Vec<u8> {
        reborrow_from(&BloatEchoView::decode_view(input).unwrap())
    }
}

// ── deep_nested: 5 levels of singular sub-message ────────────────────

mod deep_nested {
    use super::*;

    fn s(tag: &str, lvl: u8) -> String {
        format!("level-{lvl}-{tag}-payload-string-abcdefghijklmnop-0123456789")
    }

    pub fn payload() -> DeepNested {
        DeepNested {
            root_a: s("root-a", 0),
            root_b: s("root-b", 0),
            child: NestL1 {
                a: s("a", 1),
                b: s("b", 1),
                child: NestL2 {
                    a: s("a", 2),
                    b: s("b", 2),
                    child: NestL3 {
                        a: s("a", 3),
                        b: s("b", 3),
                        child: NestL4 {
                            a: s("a", 4),
                            b: s("b", 4),
                            child: NestL5 {
                                a: s("a", 5),
                                b: s("b", 5),
                                ..Default::default()
                            }
                            .into(),
                            ..Default::default()
                        }
                        .into(),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
    }

    pub fn owned_owned(input: &[u8]) -> Vec<u8> {
        let r = DeepNested::decode_from_slice(input).unwrap();
        DeepNested {
            root_a: r.root_a,
            root_b: r.root_b,
            child: r.child,
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn view_owned(input: &[u8]) -> Vec<u8> {
        let r = DeepNestedView::decode_view(input).unwrap();
        r.to_owned_message().unwrap().encode_to_vec()
    }

    pub fn owned_view(input: &[u8]) -> Vec<u8> {
        let r = DeepNested::decode_from_slice(input).unwrap();
        fn b5(x: &NestL5) -> NestL5View<'_> {
            NestL5View {
                a: &x.a,
                b: &x.b,
                ..Default::default()
            }
        }
        fn b4(x: &NestL4) -> NestL4View<'_> {
            NestL4View {
                a: &x.a,
                b: &x.b,
                child: x
                    .child
                    .as_option()
                    .map(b5)
                    .map(From::from)
                    .unwrap_or_default(),
                ..Default::default()
            }
        }
        fn b3(x: &NestL3) -> NestL3View<'_> {
            NestL3View {
                a: &x.a,
                b: &x.b,
                child: x
                    .child
                    .as_option()
                    .map(b4)
                    .map(From::from)
                    .unwrap_or_default(),
                ..Default::default()
            }
        }
        fn b2(x: &NestL2) -> NestL2View<'_> {
            NestL2View {
                a: &x.a,
                b: &x.b,
                child: x
                    .child
                    .as_option()
                    .map(b3)
                    .map(From::from)
                    .unwrap_or_default(),
                ..Default::default()
            }
        }
        fn b1(x: &NestL1) -> NestL1View<'_> {
            NestL1View {
                a: &x.a,
                b: &x.b,
                child: x
                    .child
                    .as_option()
                    .map(b2)
                    .map(From::from)
                    .unwrap_or_default(),
                ..Default::default()
            }
        }
        DeepNestedView {
            root_a: &r.root_a,
            root_b: &r.root_b,
            child: r
                .child
                .as_option()
                .map(b1)
                .map(From::from)
                .unwrap_or_default(),
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn view_view(input: &[u8]) -> Vec<u8> {
        let r = DeepNestedView::decode_view(input).unwrap();
        fn e5<'a>(x: &NestL5View<'a>) -> NestL5View<'a> {
            NestL5View {
                a: x.a,
                b: x.b,
                ..Default::default()
            }
        }
        fn e4<'a>(x: &NestL4View<'a>) -> NestL4View<'a> {
            NestL4View {
                a: x.a,
                b: x.b,
                child: x
                    .child
                    .as_option()
                    .map(e5)
                    .map(From::from)
                    .unwrap_or_default(),
                ..Default::default()
            }
        }
        fn e3<'a>(x: &NestL3View<'a>) -> NestL3View<'a> {
            NestL3View {
                a: x.a,
                b: x.b,
                child: x
                    .child
                    .as_option()
                    .map(e4)
                    .map(From::from)
                    .unwrap_or_default(),
                ..Default::default()
            }
        }
        fn e2<'a>(x: &NestL2View<'a>) -> NestL2View<'a> {
            NestL2View {
                a: x.a,
                b: x.b,
                child: x
                    .child
                    .as_option()
                    .map(e3)
                    .map(From::from)
                    .unwrap_or_default(),
                ..Default::default()
            }
        }
        fn e1<'a>(x: &NestL1View<'a>) -> NestL1View<'a> {
            NestL1View {
                a: x.a,
                b: x.b,
                child: x
                    .child
                    .as_option()
                    .map(e2)
                    .map(From::from)
                    .unwrap_or_default(),
                ..Default::default()
            }
        }
        DeepNestedView {
            root_a: r.root_a,
            root_b: r.root_b,
            child: r
                .child
                .as_option()
                .map(e1)
                .map(From::from)
                .unwrap_or_default(),
            ..Default::default()
        }
        .encode_to_vec()
    }
}

// ── map_dominated: 30-entry string map + 2 strings ───────────────────

mod map_dominated {
    use super::*;

    pub fn payload() -> MapDominated {
        let labels: Map<String, String> = (0..30)
            .map(|i| {
                (
                    format!("k{i:02}"),
                    format!("workload-partition-{i:02}-us-west-2a-r5.2xlarge-spot-replacement-candidate-v3"),
                )
            })
            .collect();
        MapDominated {
            id: "deployment-0193fae1-7d4c".into(),
            kind: "ReplicaSet".into(),
            labels,
            ..Default::default()
        }
    }

    pub fn owned_owned(input: &[u8]) -> Vec<u8> {
        let r = MapDominated::decode_from_slice(input).unwrap();
        MapDominated {
            id: r.id,
            kind: r.kind,
            labels: r.labels,
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn view_owned(input: &[u8]) -> Vec<u8> {
        let r = MapDominatedView::decode_view(input).unwrap();
        MapDominated {
            id: r.id.into(),
            kind: r.kind.into(),
            labels: r
                .labels
                .iter()
                .map(|(k, v)| ((*k).into(), (*v).into()))
                .collect(),
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn owned_view(input: &[u8]) -> Vec<u8> {
        let r = MapDominated::decode_from_slice(input).unwrap();
        MapDominatedView {
            id: &r.id,
            kind: &r.kind,
            labels: r
                .labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn view_view(input: &[u8]) -> Vec<u8> {
        let r = MapDominatedView::decode_view(input).unwrap();
        MapDominatedView {
            id: r.id,
            kind: r.kind,
            labels: r.labels.iter().map(|(k, v)| (*k, *v)).collect(),
            ..Default::default()
        }
        .encode_to_vec()
    }
}

bench_shape!(
    bench_scalar_heavy,
    "scalar_heavy",
    ScalarHeavy,
    scalar_heavy
);
bench_shape!(
    bench_few_large_strings,
    "few_large_strings",
    FewLargeStrings,
    few_large_strings
);
bench_shape!(
    bench_many_small_strings,
    "many_small_strings",
    BloatEcho,
    many_small_strings
);
bench_shape!(bench_deep_nested, "deep_nested", DeepNested, deep_nested);
bench_shape!(
    bench_map_dominated,
    "map_dominated",
    MapDominated,
    map_dominated
);

/// Fanout: decode the source ONCE, then build+encode N responses borrowing
/// from it. Models a switchboard-style 1-source → N-reader stream where the
/// owned baseline is forced to clone (can't move into N places). Throughput
/// is N×payload bytes so per-iter time scales with N but per-byte rate is
/// comparable across N.
///
/// `$paths` must provide `payload`, `owned_owned`, `clone_from(&Owned)`,
/// `borrow_from(&Owned)`, `reborrow_from(&View<'a>)`.
macro_rules! bench_fanout {
    ($fn_name:ident, $group:literal, $owned:ty, $view:ty, $paths:path) => {
        fn $fn_name(c: &mut Criterion) {
            use $paths as p;
            let input = p::payload().encode_to_vec();
            // Wire-equivalence guard: each per-response builder round-trips.
            let baseline = <$owned>::decode_from_slice(&p::owned_owned(&input)).unwrap();
            for out in [
                p::clone_from(&<$owned>::decode_from_slice(&input).unwrap()),
                p::borrow_from(&<$owned>::decode_from_slice(&input).unwrap()),
                p::reborrow_from(&<$view>::decode_view(&input).unwrap()),
            ] {
                assert_eq!(<$owned>::decode_from_slice(&out).unwrap(), baseline);
            }
            let mut g = c.benchmark_group($group);
            for &n in &[1usize, 4, 16, 64] {
                g.throughput(Throughput::Bytes(input.len() as u64 * n as u64));
                g.bench_with_input(BenchmarkId::new("owned/owned", n), &n, |b, &n| {
                    b.iter(|| {
                        let src = <$owned>::decode_from_slice(black_box(&input)).unwrap();
                        for _ in 0..n {
                            black_box(p::clone_from(black_box(&src)));
                        }
                    })
                });
                g.bench_with_input(BenchmarkId::new("owned/view", n), &n, |b, &n| {
                    b.iter(|| {
                        let src = <$owned>::decode_from_slice(black_box(&input)).unwrap();
                        for _ in 0..n {
                            black_box(p::borrow_from(black_box(&src)));
                        }
                    })
                });
                g.bench_with_input(BenchmarkId::new("view/view", n), &n, |b, &n| {
                    b.iter(|| {
                        let src = <$view>::decode_view(black_box(&input)).unwrap();
                        for _ in 0..n {
                            black_box(p::reborrow_from(black_box(&src)));
                        }
                    })
                });
            }
            g.finish();
        }
    };
}

bench_fanout!(
    bench_fanout,
    "fanout",
    BloatEcho,
    BloatEchoView,
    many_small_strings
);

criterion_group!(
    benches,
    bench_scalar_heavy,
    bench_few_large_strings,
    bench_many_small_strings,
    bench_deep_nested,
    bench_map_dominated,
    bench_fanout,
);
criterion_main!(benches);
