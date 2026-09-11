//! Log-ingest server for decode-heavy profiling (tonic + tonic-protobuf on
//! the upb kernel).
//!
//! Matches the connectrpc-rs `log_server` and tonic/prost `log-server-tonic`
//! handlers field-for-field so the measured difference is the proto library.
//! upb parses the whole `LogRequest` eagerly into an arena before the
//! handler runs (string data is copied into the arena, not borrowed from
//! the request buffer); the accessors below then read arena-backed views.

use rpc_bench_grpc_rust::pb::log_ingest_service_server::{
    LogIngestService, LogIngestServiceServer,
};
use rpc_bench_grpc_rust::pb::{LogIngestResponse, LogRequest};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

struct LogIngestImpl;

#[tonic::async_trait]
impl LogIngestService for LogIngestImpl {
    async fn ingest(
        &self,
        request: Request<LogRequest>,
    ) -> Result<Response<LogIngestResponse>, Status> {
        let req = request.into_inner();

        let mut count = 0i32;
        let mut total_message_bytes = 0i64;
        let mut total_label_bytes = 0i64;
        let mut max_severity = 0i32;

        for rec in req.records() {
            count += 1;

            let sev = i32::from(rec.severity());
            if sev > max_severity {
                max_severity = sev;
            }

            total_message_bytes += rec.message().len() as i64;
            total_message_bytes += rec.service_name().len() as i64;
            total_message_bytes += rec.instance_id().len() as i64;
            total_message_bytes += rec.trace_id().len() as i64;
            total_message_bytes += rec.span_id().len() as i64;

            if let Some(src) = rec.source_opt() {
                total_message_bytes += src.file().len() as i64;
                total_message_bytes += src.function().len() as i64;
                let _ = src.line();
            }

            for (k, v) in rec.labels() {
                total_label_bytes += (k.len() + v.len()) as i64;
            }
        }

        let mut resp = LogIngestResponse::new();
        resp.set_count(count);
        resp.set_total_message_bytes(total_message_bytes);
        resp.set_total_label_bytes(total_label_bytes);
        resp.set_max_severity(max_severity);
        Ok(Response::new(resp))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Server::builder()
        .add_service(LogIngestServiceServer::new(LogIngestImpl))
        .serve_with_incoming_shutdown(
            rpc_bench_grpc_rust::listen()?,
            rpc_bench_grpc_rust::shutdown_signal(),
        )
        .await?;
    Ok(())
}
