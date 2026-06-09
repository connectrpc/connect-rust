use std::sync::Arc;

use connectrpc::{
    ConnectError, ConnectRpcService, RequestContext, Response, ServiceRequest, ServiceResult,
};

use rpc_bench::connect::fortune::v1::*;
use rpc_bench::fortune;
use rpc_bench::proto::fortune::v1::*;

const VALKEY_POOL_SIZE: usize = 8;

struct FortuneServiceImpl {
    pool: Arc<fortune::ValkeyPool>,
}

impl FortuneService for FortuneServiceImpl {
    async fn get_fortunes(
        &self,
        _ctx: RequestContext,
        _req: ServiceRequest<'_, GetFortunesRequest>,
    ) -> ServiceResult<GetFortunesResponse> {
        let mut conn = self.pool.get();
        let fortunes = fortune::query_fortunes(&mut conn)
            .await
            .map_err(|e| ConnectError::internal(format!("valkey: {e}")))?;

        let response = GetFortunesResponse {
            fortunes: fortunes
                .into_iter()
                .map(|(id, message)| Fortune {
                    id,
                    message,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        Response::ok(response)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let valkey_addr = std::env::args()
        .nth(1)
        .expect("usage: fortune_server <valkey_addr>");
    let pool = Arc::new(fortune::ValkeyPool::connect(&valkey_addr, VALKEY_POOL_SIZE).await?);

    let server = FortuneServiceServer::new(FortuneServiceImpl { pool });
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
