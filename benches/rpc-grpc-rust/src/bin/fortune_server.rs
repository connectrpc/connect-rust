//! Fortunes server backed by valkey (tonic + tonic-protobuf on the upb
//! kernel). Mirrors `benches/rpc-tonic/src/bin/fortune_server.rs`, but takes
//! the valkey pool and query from `rpc_bench::fortune` rather than copying
//! them, since this crate already depends on `rpc-bench`.

use rpc_bench::fortune::{ValkeyPool, query_fortunes};
use rpc_bench_grpc_rust::pb::fortune_service_server::{FortuneService, FortuneServiceServer};
use rpc_bench_grpc_rust::pb::{GetFortunesRequest, GetFortunesResponse};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// Same pool size as the connectrpc-rs and tonic/prost fortune servers.
const VALKEY_POOL_SIZE: usize = 8;

struct FortuneServiceImpl {
    pool: ValkeyPool,
}

#[tonic::async_trait]
impl FortuneService for FortuneServiceImpl {
    async fn get_fortunes(
        &self,
        _req: Request<GetFortunesRequest>,
    ) -> Result<Response<GetFortunesResponse>, Status> {
        let mut conn = self.pool.get();
        let fortunes = query_fortunes(&mut conn)
            .await
            .map_err(|e| Status::internal(format!("valkey: {e}")))?;

        // Each message string is copied into the response arena; prost moves
        // the `String`s into its `Fortune`s instead.
        let mut resp = GetFortunesResponse::new();
        let mut out = resp.fortunes_mut();
        for (id, message) in fortunes {
            let mut f = out.push_default();
            f.set_id(id);
            f.set_message(message);
        }
        Ok(Response::new(resp))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let valkey_addr = std::env::args()
        .nth(1)
        .ok_or("usage: fortune-server-tonic-protobuf <valkey_addr>")?;
    let pool = ValkeyPool::connect(&valkey_addr, VALKEY_POOL_SIZE).await?;

    Server::builder()
        .add_service(FortuneServiceServer::new(FortuneServiceImpl { pool }))
        .serve_with_incoming_shutdown(
            rpc_bench_grpc_rust::listen()?,
            rpc_bench_grpc_rust::shutdown_signal(),
        )
        .await?;
    Ok(())
}
