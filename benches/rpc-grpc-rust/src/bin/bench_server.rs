//! `bench.v1.BenchService` server for the cross-implementation criterion
//! bench (tonic + tonic-protobuf on the upb kernel). Mirrors
//! `benches/rpc-tonic/src/main.rs` handler-for-handler.

use rpc_bench_grpc_rust::pb::bench_service_server::{BenchService, BenchServiceServer};
use rpc_bench_grpc_rust::pb::{BenchRequest, BenchResponse, LogRequest, LogResponse};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::codegen::BoxStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

struct BenchServiceImpl;

/// Builds a response carrying a copy of the request's payload. upb messages
/// each own an arena, so the payload cannot be moved across as prost does;
/// setting from a view deep-copies it into the response arena.
fn echo_payload(req: &BenchRequest) -> BenchResponse {
    let mut resp = BenchResponse::new();
    if let Some(payload) = req.payload_opt() {
        resp.set_payload(payload);
    }
    resp
}

fn log_count(req: &LogRequest) -> LogResponse {
    let mut resp = LogResponse::new();
    resp.set_count(i32::try_from(req.records().len()).unwrap_or(i32::MAX));
    resp
}

#[tonic::async_trait]
impl BenchService for BenchServiceImpl {
    async fn unary(
        &self,
        request: Request<BenchRequest>,
    ) -> Result<Response<BenchResponse>, Status> {
        Ok(Response::new(echo_payload(&request.into_inner())))
    }

    async fn server_stream(
        &self,
        request: Request<BenchRequest>,
    ) -> Result<Response<BoxStream<BenchResponse>>, Status> {
        let req = request.into_inner();
        let count = req.response_count();
        let stream = futures::stream::unfold((req, 0), move |(req, i)| async move {
            if i >= count {
                return None;
            }
            Some((Ok(echo_payload(&req)), (req, i + 1)))
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn client_stream(
        &self,
        request: Request<Streaming<BenchRequest>>,
    ) -> Result<Response<BenchResponse>, Status> {
        let mut stream = request.into_inner();
        let mut last = None;
        while let Some(req) = stream.next().await {
            last = Some(req?);
        }
        Ok(Response::new(
            last.as_ref().map_or_else(BenchResponse::new, echo_payload),
        ))
    }

    async fn bidi_stream(
        &self,
        request: Request<Streaming<BenchRequest>>,
    ) -> Result<Response<BoxStream<BenchResponse>>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            while let Some(req) = stream.next().await {
                let item = req.map(|req| echo_payload(&req));
                let failed = item.is_err();
                if tx.send(item).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn log_unary(
        &self,
        request: Request<LogRequest>,
    ) -> Result<Response<LogResponse>, Status> {
        Ok(Response::new(log_count(&request.into_inner())))
    }

    async fn log_unary_owned(
        &self,
        request: Request<LogRequest>,
    ) -> Result<Response<LogResponse>, Status> {
        Ok(Response::new(log_count(&request.into_inner())))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // As with the tonic/prost server: accept compressed requests, but do not
    // send_compressed — tonic has no minimum-size threshold and would gzip
    // every tiny streaming message, which connectrpc-rs does not.
    let svc = BenchServiceServer::new(BenchServiceImpl)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd);

    Server::builder()
        .add_service(svc)
        .serve_with_incoming_shutdown(
            rpc_bench_grpc_rust::listen()?,
            rpc_bench_grpc_rust::shutdown_signal(),
        )
        .await?;
    Ok(())
}
