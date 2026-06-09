//! View-response filter server: if no sensitive field is set, return
//! the request as an `OwnedView` rebuilt zero-copy from the retained
//! request bytes (re-encodes via `ViewEncode`, no per-field allocation).
//! Otherwise convert to owned, scrub, and return owned.

use connectrpc::{
    ConnectRpcService, MaybeBorrowed, RequestContext, Response, ServiceRequest, ServiceResult,
};

use rpc_bench::filter::*;

struct Impl;

impl FilterService for Impl {
    async fn redact(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, Record>,
    ) -> ServiceResult<MaybeBorrowed<Record, OwnedRecordView>> {
        if !has_sensitive(request.view()) {
            // Pass-through: rebuild a 'static view from the retained body
            // (zero-copy) and keep the ViewEncode response path under test.
            return Response::ok(MaybeBorrowed::Borrowed(request.to_owned_view()));
        }
        let mut owned = request.to_owned_message();
        scrub(&mut owned);
        Response::ok(MaybeBorrowed::Owned(owned))
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
