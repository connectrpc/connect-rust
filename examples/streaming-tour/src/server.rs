//! Streaming-tour server: NumberService implementation covering all four
//! ConnectRPC RPC types (unary, server stream, client stream, bidi stream).
//!
//! Run with:
//!
//! ```sh
//! cargo run -p streaming-tour-example --bin streaming-tour-server
//! ```
//!
//! Then in another terminal:
//!
//! ```sh
//! cargo run -p streaming-tour-example --bin streaming-tour-client
//! ```

use std::sync::Arc;

use connectrpc::{
    RequestContext, Response, Router, ServiceRequest, ServiceResult, ServiceStream, StreamMessage,
};
use futures::StreamExt;

pub mod proto {
    connectrpc::include_generated!();
}

use proto::anthropic::connectrpc::tour::v1::*;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// Local type alias that flattens the streaming-handler signatures.
// The verbose `Pin<Box<dyn Stream<...> + Send>>` form is what the
// generated traits expect today; the alias is pure sugar at the
// call site.
type RequestStream<M> = ServiceStream<StreamMessage<M>>;

/// Trivial NumberService implementation. Each method demonstrates one
/// of the four RPC patterns.
struct NumberServiceImpl;

impl NumberService for NumberServiceImpl {
    /// Unary: square the input value.
    async fn square(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, SquareRequest>,
    ) -> ServiceResult<SquareResponse> {
        // Edition 2023 default presence is EXPLICIT, so scalar fields
        // are Option<T>. unwrap_or(0) treats unset as zero, mirroring
        // the proto3 implicit-presence semantics.
        let v = request.value.unwrap_or(0) as i64;
        Response::ok(SquareResponse {
            squared: Some(v * v),
            ..Default::default()
        })
    }

    /// Server streaming: emit `count` consecutive integers from `start`.
    async fn range(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, RangeRequest>,
    ) -> ServiceResult<ServiceStream<RangeResponse>> {
        let start = request.start.unwrap_or(0);
        let count = request.count.unwrap_or(0).max(0);
        let stream = futures::stream::iter((0..count).map(move |i| {
            Ok(RangeResponse {
                value: Some(start + i),
                ..Default::default()
            })
        }));
        Response::stream_ok(stream)
    }

    /// Client streaming: drain the request stream, return the total.
    async fn sum(
        &self,
        _ctx: RequestContext,
        mut requests: RequestStream<SumRequest>,
    ) -> ServiceResult<SumResponse> {
        let mut total: i64 = 0;
        while let Some(req) = requests.next().await {
            total += req?.value().unwrap_or(0) as i64;
        }
        Response::ok(SumResponse {
            total: Some(total),
            ..Default::default()
        })
    }

    /// Bidirectional streaming: emit a running total after each request.
    async fn running_sum(
        &self,
        _ctx: RequestContext,
        requests: RequestStream<RunningSumRequest>,
    ) -> ServiceResult<ServiceStream<RunningSumResponse>> {
        let response_stream =
            futures::stream::unfold((requests, 0i64), |(mut requests, mut total)| async move {
                match requests.next().await? {
                    Ok(req) => {
                        total += req.value().unwrap_or(0) as i64;
                        Some((
                            Ok(RunningSumResponse {
                                total: Some(total),
                                ..Default::default()
                            }),
                            (requests, total),
                        ))
                    }
                    Err(e) => Some((Err(e), (requests, total))),
                }
            });
        Response::stream_ok(response_stream)
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let addr: std::net::SocketAddr = "127.0.0.1:8080".parse()?;

    let service = Arc::new(NumberServiceImpl);
    let router = service.register(Router::new());
    let app = router.into_axum_router();

    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("NumberService listening on http://{addr}");

    axum::serve(listener, app).await?;
    Ok(())
}
