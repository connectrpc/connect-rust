use std::pin::Pin;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod bench_proto {
    tonic::include_proto!("bench.v1");
}

use bench_proto::bench_service_server::{BenchService, BenchServiceServer};
use bench_proto::{BenchRequest, BenchResponse};

/// Echo-style bench service that reflects payloads back.
#[derive(Default)]
pub struct BenchServiceImpl;

#[tonic::async_trait]
impl BenchService for BenchServiceImpl {
    async fn unary(
        &self,
        request: Request<BenchRequest>,
    ) -> Result<Response<BenchResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(BenchResponse {
            payload: req.payload,
        }))
    }

    type ServerStreamStream =
        Pin<Box<dyn Stream<Item = Result<BenchResponse, Status>> + Send + 'static>>;

    async fn server_stream(
        &self,
        request: Request<BenchRequest>,
    ) -> Result<Response<Self::ServerStreamStream>, Status> {
        let req = request.into_inner();
        let count = req.response_count;
        let payload = req.payload;
        let stream = futures::stream::unfold((payload, 0), move |(payload, i)| async move {
            if i >= count {
                return None;
            }
            // Each item owns its payload, as a handler that builds a response
            // per message would; the copy is part of the measured work.
            let item = BenchResponse {
                payload: payload.clone(),
            };
            Some((Ok(item), (payload, i + 1)))
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn client_stream(
        &self,
        request: Request<tonic::Streaming<BenchRequest>>,
    ) -> Result<Response<BenchResponse>, Status> {
        let mut stream = request.into_inner();
        let mut last_payload = None;
        while let Some(req) = stream.next().await {
            let req = req?;
            last_payload = req.payload;
        }
        Ok(Response::new(BenchResponse {
            payload: last_payload,
        }))
    }

    async fn log_unary(
        &self,
        request: Request<bench_proto::LogRequest>,
    ) -> Result<Response<bench_proto::LogResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(bench_proto::LogResponse {
            count: req.records.len() as i32,
        }))
    }

    async fn log_unary_owned(
        &self,
        request: Request<bench_proto::LogRequest>,
    ) -> Result<Response<bench_proto::LogResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(bench_proto::LogResponse {
            count: req.records.len() as i32,
        }))
    }

    type BidiStreamStream =
        Pin<Box<dyn Stream<Item = Result<BenchResponse, Status>> + Send + 'static>>;

    async fn bidi_stream(
        &self,
        request: Request<tonic::Streaming<BenchRequest>>,
    ) -> Result<Response<Self::BidiStreamStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            while let Some(req) = stream.next().await {
                match req {
                    Ok(req) => {
                        let resp = BenchResponse {
                            payload: req.payload,
                        };
                        if tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Don't enable send_compressed: tonic has no minimum-size threshold
    // and creates a fresh GzEncoder per message (no pooling), which dominates
    // per-message cost for small streaming payloads. connectrpc-rs skips
    // compression below 1 KiB, so for a fair comparison tonic must not
    // compress either. accept_compressed stays so the server can decode
    // any compressed requests the client chooses to send.
    let svc = BenchServiceServer::new(BenchServiceImpl)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd);

    // Bind to a random port with TCP_NODELAY enabled.
    let addr = "127.0.0.1:0".parse().unwrap();
    let incoming = tonic::transport::server::TcpIncoming::bind(addr)?.with_nodelay(Some(true));
    let addr = incoming.local_addr()?;
    println!("{addr}");

    Server::builder()
        .add_service(svc)
        .serve_with_incoming(incoming)
        .await?;

    Ok(())
}
