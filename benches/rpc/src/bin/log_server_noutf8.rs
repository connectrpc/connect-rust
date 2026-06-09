//! Log-ingest server with UTF-8 validation DISABLED.
//!
//! Uses the `bench_noutf8.proto` types where all string fields are
//! `&[u8]` / `Vec<u8>` (strict_utf8_mapping + editions utf8_validation=NONE).
//! Decode calls `borrow_bytes` instead of `borrow_str` — no `from_utf8`.
//!
//! Compare against `log_server` (UTF-8 ON, baseline). The baseline profile
//! showed `str::from_utf8` at 11.23% of server CPU. This variant measures
//! the actual recovery.

use connectrpc::{ConnectRpcService, RequestContext, Response, ServiceRequest, ServiceResult};
use rpc_bench::connect::bench::noutf8::v1::*;
use rpc_bench::proto::bench::noutf8::v1::*;

struct LogIngestImpl;

impl LogIngestService for LogIngestImpl {
    async fn ingest(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, LogRequest>,
    ) -> ServiceResult<LogIngestResponse> {
        let mut count = 0i32;
        let mut total_message_bytes = 0i64;
        let mut max_severity = 0i32;
        let mut total_label_bytes = 0i64;

        // Same iteration shape as log_server, but fields are
        // `Option<&[u8]>` (edition 2023 explicit presence) instead of
        // `&str`. .len() is identical — just summing byte lengths.
        // The key difference: decode called borrow_bytes, no from_utf8.
        for rec in req.records.iter() {
            count += 1;

            if let Some(sev) = rec.severity {
                let s = sev.to_i32();
                if s > max_severity {
                    max_severity = s;
                }
            }

            // unwrap_or_default() for Option<&[u8]> is a null-check + branch,
            // cheaper than the from_utf8 it replaced.
            total_message_bytes += rec.message.unwrap_or_default().len() as i64;
            total_message_bytes += rec.service_name.unwrap_or_default().len() as i64;
            total_message_bytes += rec.instance_id.unwrap_or_default().len() as i64;
            total_message_bytes += rec.trace_id.unwrap_or_default().len() as i64;
            total_message_bytes += rec.span_id.unwrap_or_default().len() as i64;

            total_message_bytes += rec.source.file.unwrap_or_default().len() as i64;
            total_message_bytes += rec.source.function.unwrap_or_default().len() as i64;
            let _ = rec.source.line;

            // MapView<&[u8], &[u8]> — iterate entries, sum byte lengths.
            // Same work as the utf8 variant's String-key map iteration
            // but entries are borrowed byte slices (no alloc, no hashing).
            for (k, v) in rec.labels.iter() {
                total_label_bytes += (k.len() + v.len()) as i64;
            }
        }

        Response::ok(LogIngestResponse {
            count: Some(count),
            total_message_bytes: Some(total_message_bytes),
            total_label_bytes: Some(total_label_bytes),
            max_severity: Some(max_severity),
            ..Default::default()
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = LogIngestServiceServer::new(LogIngestImpl);
    let service = ConnectRpcService::new(server);

    let bound = connectrpc::server::Server::bind("127.0.0.1:0").await?;
    let addr = bound.local_addr()?;
    println!("{addr}");

    tokio::select! {
        result = bound.serve_with_service(service) => result?,
        _ = tokio::signal::ctrl_c() => {}
    }
    Ok(())
}
