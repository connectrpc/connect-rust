//! Minimal echo server for framework-overhead benchmarking.
//!
//! No database, no spawn_blocking, no complex message types — just
//! dispatch + proto decode/encode of a single string. This isolates
//! the per-request cost of the connectrpc-rs request pipeline.

use connectrpc::{ConnectRpcService, RequestContext, Response, ServiceRequest, ServiceResult};
use rpc_bench::connect::bench::v1::*;
use rpc_bench::proto::bench::v1::*;

struct EchoImpl;

impl EchoService for EchoImpl {
    async fn echo(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, EchoRequest>,
    ) -> ServiceResult<EchoResponse> {
        // One allocation to copy the borrowed &str into the owned response.
        // This is the minimal work a real echo server would do.
        Response::ok(EchoResponse {
            message: req.message.to_string(),
            ..Default::default()
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = EchoServiceServer::new(EchoImpl);
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
