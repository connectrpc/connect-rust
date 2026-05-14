//! Load generator for the noutf8 log-ingest variant.
//!
//! Usage: `log_load_noutf8 <addr> [duration_secs] [concurrency] [n_conns] [records_per_batch]`
//!
//! Sends batches against `bench.noutf8.v1.LogIngestService`. The wire
//! format is identical to `log_load` (same field types on the wire —
//! string == bytes in proto encoding), but the service path differs and
//! the Rust request type uses `Vec<u8>` for strings.

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, Http2Connection, SharedHttp2Connection};
use rpc_bench::connect::bench::noutf8::v1::*;
use rpc_bench::proto::bench::noutf8::v1::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Build a batch of `n` log records with realistic payload shapes.
/// Matches `log_request()` in lib.rs but uses `Vec<u8>` for strings
/// and populates the labels map with 6 entries per record.
fn build_request(n: usize) -> LogRequest {
    let records = (0..n)
        .map(|i| {
            let mut labels = HashMap::new();
            for j in 0..6 {
                labels.insert(
                    format!("label-key-{j}").into_bytes(),
                    format!("label-value-{i}-{j}").into_bytes(),
                );
            }
            LogRecord {
                timestamp_nanos: Some(1_700_000_000_000_000_000 + i as i64),
                service_name: Some(b"api-gateway".to_vec()),
                instance_id: Some(format!("instance-{i:04x}").into_bytes()),
                severity: Some(log_record::Severity::SEVERITY_INFO.into()),
                message: Some(
                    format!(
                        "Processing request from client {i}: \
                         GET /api/v1/users?page={}&limit=50 completed in {}ms with status 200 OK",
                        i % 100,
                        42 + i % 200
                    )
                    .into_bytes(),
                ),
                trace_id: Some(
                    format!("{:032x}", 0xDEAD_BEEF_0000_0000u64 + i as u64).into_bytes(),
                ),
                span_id: Some(format!("{:016x}", 0xCAFE_0000u64 + i as u64).into_bytes()),
                labels,
                source: LogSource {
                    file: Some(format!("src/handlers/user_{}.rs", i % 10).into_bytes()),
                    line: Some(42 + (i as i32 % 500)),
                    function: Some(format!("handle_get_users_{}", i % 5).into_bytes()),
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

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).expect(
        "usage: log_load_noutf8 <addr> [duration] [concurrency] [n_conns] [records_per_batch]",
    );
    let duration = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(30u64);
    let concurrency: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);
    let n_conns: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(8);
    let records_per_batch: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(50);

    let uri: http::Uri = format!("http://{addr}").parse().unwrap();
    let config = ClientConfig::new(uri.clone()).with_protocol(Protocol::Grpc);

    let request = build_request(records_per_batch);
    let approx_size = {
        use buffa::Message;
        request.encode_to_bytes().len()
    };

    eprintln!(
        "Generating load: {concurrency} tasks × {n_conns} connections for {duration}s against {addr}"
    );
    eprintln!("  Batch: {records_per_batch} records (~{approx_size} bytes encoded)");

    let mut conns: Vec<SharedHttp2Connection> = Vec::with_capacity(n_conns);
    for _ in 0..n_conns {
        let c = Http2Connection::connect_plaintext(uri.clone())
            .await
            .expect("connect");
        conns.push(c.shared(1024));
    }

    let running = Arc::new(AtomicBool::new(true));
    let count = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for i in 0..concurrency {
        let conn = conns[i % n_conns].clone();
        let client = LogIngestServiceClient::new(conn, config.clone());
        let running = Arc::clone(&running);
        let count = Arc::clone(&count);
        let request = request.clone();
        handles.push(tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if client.ingest(request.clone()).await.is_ok() {
                    count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    tokio::time::sleep(std::time::Duration::from_secs(duration)).await;
    running.store(false, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }

    let total = count.load(Ordering::Relaxed);
    eprintln!(
        "Completed {total} requests ({:.0} req/s, {:.0} records/s)",
        total as f64 / duration as f64,
        (total * records_per_batch as u64) as f64 / duration as f64,
    );
}
