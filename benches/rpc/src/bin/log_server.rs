//! Log-ingest server for decode-heavy profiling.
//!
//! Receives batches of `LogRecord`s and aggregates field-length totals.
//! The handler touches every field on every record via the zero-copy
//! buffa view — varints (timestamp, line, severity), string pointers
//! (message, service_name, trace_id, span_id), nested message (source),
//! map entries (labels). No I/O, no allocation beyond the response
//! struct: proto decode/encode cost dominates.

use connectrpc::{ConnectRpcService, RequestContext, Response, ServiceRequest, ServiceResult};
use rpc_bench::connect::bench::v1::*;
use rpc_bench::proto::bench::v1::*;

struct LogIngestImpl;

impl LogIngestService for LogIngestImpl {
    async fn ingest(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, LogRequest>,
    ) -> ServiceResult<LogIngestResponse> {
        let mut count = 0i32;
        let mut total_message_bytes = 0i64;
        let mut total_label_bytes = 0i64;
        let mut max_severity = 0i32;

        // RepeatedView derefs to &[LogRecordView]. Each field access
        // on a view is either direct (already decoded during view parse)
        // or a zero-copy pointer/slice reference — NO string allocs.
        //
        // Compare: prost fully decodes into Vec<LogRecord> with owned
        // String fields (N allocs for N string fields per record) before
        // the handler even runs.
        for rec in req.records.iter() {
            count += 1;

            // Varint fields (decoded eagerly in the view):
            // timestamp_nanos is i64, severity is enum-varint.
            let sev = rec.severity.to_i32();
            if sev > max_severity {
                max_severity = sev;
            }

            // String fields are &str slices into the request buffer —
            // .len() is just reading a usize that was resolved when
            // the view parsed the length-delimited header.
            total_message_bytes += rec.message.len() as i64;

            // Touch all other string fields so they're not optimized away.
            // Summing lengths proves we resolved the varint length + pointer.
            total_message_bytes += rec.service_name.len() as i64;
            total_message_bytes += rec.instance_id.len() as i64;
            total_message_bytes += rec.trace_id.len() as i64;
            total_message_bytes += rec.span_id.len() as i64;

            // Nested message field — MessageFieldView derefs if present.
            total_message_bytes += rec.source.file.len() as i64;
            total_message_bytes += rec.source.function.len() as i64;
            // rec.source.line is i32 varint — touching it forces decode:
            let _ = rec.source.line;

            // Map iteration — each entry was decoded as a nested message
            // (key-varint-tag + key-len + key-bytes + val-varint-tag + ...)
            for (k, v) in rec.labels.iter() {
                total_label_bytes += (k.len() + v.len()) as i64;
            }
        }

        Response::ok(LogIngestResponse {
            count,
            total_message_bytes,
            total_label_bytes,
            max_severity,
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
