//! `bench.v1.BenchService` server for the cross-implementation criterion
//! bench, on the codegen `BenchServiceServer<T>` dispatcher — the same
//! monomorphic path `echo_server` and `log_server` use. Pass `--router` to
//! serve through the dynamic `Router` instead, for comparing the two.

use std::sync::Arc;

use connectrpc::{ConnectRpcService, Router};
use rpc_bench::{BenchServiceExt, BenchServiceImpl, BenchServiceServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let use_router = std::env::args().any(|a| a == "--router");

    let bound = connectrpc::server::Server::bind("127.0.0.1:0").await?;
    let addr = bound.local_addr()?;
    // Print the address to stdout for the benchmark harness.
    println!("{addr}");

    if use_router {
        let router = Arc::new(BenchServiceImpl).register(Router::new());
        bound.serve(router).await?;
    } else {
        let service = ConnectRpcService::new(BenchServiceServer::new(BenchServiceImpl));
        bound.serve_with_service(service).await?;
    }
    Ok(())
}
