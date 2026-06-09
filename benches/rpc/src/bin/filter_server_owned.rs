//! Baseline filter server: always converts the request to the owned
//! `Record`, scrubs sensitive fields if any are set, and returns owned.

use connectrpc::{ConnectRpcService, RequestContext, Response, ServiceRequest, ServiceResult};

use rpc_bench::filter::*;

struct Impl;

impl FilterService for Impl {
    async fn redact(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, Record>,
    ) -> ServiceResult<Record> {
        let mut owned = request.to_owned_message();
        if has_sensitive(request.view()) {
            scrub(&mut owned);
        }
        Response::ok(owned)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let service = ConnectRpcService::new(FilterServiceServer::new(Impl));
    let bound = connectrpc::server::Server::bind("127.0.0.1:0").await?;
    println!("{}", bound.local_addr()?);
    tokio::select! {
        result = bound.serve_with_service(service) => result?,
        _ = tokio::signal::ctrl_c() => {}
    }
    Ok(())
}
