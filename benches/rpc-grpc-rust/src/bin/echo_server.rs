//! Minimal echo server for framework-overhead benchmarking (tonic +
//! tonic-protobuf on the upb kernel).

use rpc_bench_grpc_rust::pb::echo_service_server::{EchoService, EchoServiceServer};
use rpc_bench_grpc_rust::pb::{EchoRequest, EchoResponse};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

struct EchoImpl;

#[tonic::async_trait]
impl EchoService for EchoImpl {
    async fn echo(&self, req: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        // upb messages own an arena each, so unlike prost the string cannot
        // be moved from request to response; `set_message` copies it into
        // the response's arena.
        let req = req.into_inner();
        let mut resp = EchoResponse::new();
        resp.set_message(req.message());
        Ok(Response::new(resp))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Server::builder()
        .add_service(EchoServiceServer::new(EchoImpl))
        .serve_with_incoming_shutdown(
            rpc_bench_grpc_rust::listen()?,
            rpc_bench_grpc_rust::shutdown_signal(),
        )
        .await?;
    Ok(())
}
