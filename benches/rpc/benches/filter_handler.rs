//! Filter (redaction) handler benchmark.
//!
//! Measures owned-response vs view-response (`MaybeBorrowed`) handlers
//! across a sweep of modification ratios (0/10/50/100% of records have a
//! sensitive field set).
//!
//! Two layers:
//! - `codec/*`: decode→handler-logic→encode only, no RPC. Isolates the
//!   per-field-allocation cost the view path avoids.
//! - `rpc/*`: end-to-end unary RPC against `filter_server_{owned,view}`
//!   subprocesses. Shows the win net of HTTP/dispatch overhead.

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use buffa::Message as _;
use buffa::view::ViewEncode as _;
use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, HttpClient};
use rpc_bench::filter::*;

const RATIOS: [u32; 4] = [0, 10, 50, 100];
const BATCH: usize = 100;

// ── Codec layer (no RPC) ──────────────────────────────────────────────

/// Decode → always to_owned + scrub-if-needed → encode owned.
fn codec_owned(req_bytes: &bytes::Bytes) -> bytes::Bytes {
    let req = OwnedRecordView::decode(req_bytes.clone()).unwrap();
    let mut owned = req.to_owned_message();
    if has_sensitive(&req) {
        scrub(&mut owned);
    }
    owned.encode_to_bytes()
}

/// Decode → if clean, ViewEncode the request view directly; else
/// to_owned + scrub → encode owned.
fn codec_view(req_bytes: &bytes::Bytes) -> bytes::Bytes {
    let req = OwnedRecordView::decode(req_bytes.clone()).unwrap();
    if !has_sensitive(&req) {
        return (*req).encode_to_bytes();
    }
    let mut owned = req.to_owned_message();
    scrub(&mut owned);
    owned.encode_to_bytes()
}

fn bench_codec(c: &mut Criterion) {
    // Semantic-equivalence guard: both paths must decode to the same
    // Record. (Byte-for-byte equality doesn't hold because owned encode
    // iterates HashMap fields in hash order while view encode preserves
    // wire order; both are valid proto map encodings.)
    for pct in RATIOS {
        for bytes in &sample_batch(BATCH, pct) {
            let o = Record::decode_from_slice(&codec_owned(bytes)).unwrap();
            let v = Record::decode_from_slice(&codec_view(bytes)).unwrap();
            assert_eq!(o, v, "owned/view diverged at {pct}%");
        }
    }

    for pct in RATIOS {
        let batch = sample_batch(BATCH, pct);
        let payload_len: u64 = batch.iter().map(|b| b.len() as u64).sum();
        let mut group = c.benchmark_group(format!("filter/codec/{pct}pct"));
        group.throughput(Throughput::Bytes(payload_len));
        group.bench_function("owned", |b| {
            b.iter(|| {
                for bytes in &batch {
                    std::hint::black_box(codec_owned(bytes));
                }
            })
        });
        group.bench_function("view", |b| {
            b.iter(|| {
                for bytes in &batch {
                    std::hint::black_box(codec_view(bytes));
                }
            })
        });
        group.finish();
    }
}

// ── RPC layer (end-to-end) ────────────────────────────────────────────

struct ServerProcess {
    child: Child,
    addr: SocketAddr,
}

impl ServerProcess {
    fn start(bin: &str) -> Self {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let cmd = format!("{manifest_dir}/../../target/release/{bin}");
        let mut child = Command::new(&cmd)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to start {cmd}: {e}"));
        let stdout = child.stdout.take().expect("no stdout");
        let mut line = String::new();
        BufReader::new(stdout)
            .read_line(&mut line)
            .expect("failed to read server address");
        let addr: SocketAddr = line.trim().parse().unwrap_or_else(|e| {
            panic!("failed to parse server address from {cmd} ({line:?}): {e}")
        });
        std::thread::sleep(Duration::from_millis(50));
        Self { child, addr }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn build_servers() {
    let status = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "rpc-bench",
            "--bin",
            "filter_server_owned",
            "--bin",
            "filter_server_view",
        ])
        .stderr(Stdio::inherit())
        .status()
        .expect("cargo build failed");
    assert!(status.success(), "failed to build filter servers");
}

fn make_client(addr: SocketAddr) -> FilterServiceClient<HttpClient> {
    let config = ClientConfig::new(format!("http://{addr}").parse().unwrap())
        .with_protocol(Protocol::Connect);
    FilterServiceClient::new(HttpClient::plaintext(), config)
}

fn bench_rpc(c: &mut Criterion) {
    build_servers();
    let owned = ServerProcess::start("filter_server_owned");
    let view = ServerProcess::start("filter_server_view");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let owned_client = make_client(owned.addr);
    let view_client = make_client(view.addr);

    for pct in RATIOS {
        let reqs = sample_records(BATCH, pct);
        let payload_len: u64 = reqs.iter().map(|r| r.encoded_len() as u64).sum();
        let mut group = c.benchmark_group(format!("filter/rpc/{pct}pct"));
        group.throughput(Throughput::Bytes(payload_len));
        group.bench_with_input(BenchmarkId::from_parameter("owned"), &reqs, |b, reqs| {
            b.to_async(&rt).iter(|| async {
                for r in reqs {
                    owned_client.redact(r.clone()).await.expect("redact failed");
                }
            });
        });
        group.bench_with_input(BenchmarkId::from_parameter("view"), &reqs, |b, reqs| {
            b.to_async(&rt).iter(|| async {
                for r in reqs {
                    view_client.redact(r.clone()).await.expect("redact failed");
                }
            });
        });
        group.finish();
    }
}

criterion_group!(benches, bench_codec, bench_rpc);
criterion_main!(benches);
