//! End-to-end test: spin up the NumberService in-process, exercise all
//! four RPC types over a real TCP socket, assert expected results.

use std::sync::Arc;

use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::{
    RequestContext, Response, Router, ServiceRequest, ServiceResult, ServiceStream, StreamMessage,
};
use futures::StreamExt;

pub mod proto {
    connectrpc::include_generated!();
}

use proto::anthropic::connectrpc::tour::v1::*;

// Local alias that flattens client/bidi-stream request parameters.
type RequestStream<M> = ServiceStream<StreamMessage<M>>;

struct NumberServiceImpl;

impl NumberService for NumberServiceImpl {
    async fn square(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, SquareRequest>,
    ) -> ServiceResult<SquareResponse> {
        let v = request.value.unwrap_or(0) as i64;
        Response::ok(SquareResponse {
            squared: Some(v * v),
            ..Default::default()
        })
    }

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

async fn start_server() -> std::net::SocketAddr {
    let service = Arc::new(NumberServiceImpl);
    let router = service.register(Router::new());
    let app = router.into_axum_router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

fn make_client(addr: std::net::SocketAddr) -> NumberServiceClient<HttpClient> {
    let config = ClientConfig::new(format!("http://{addr}").parse().unwrap());
    NumberServiceClient::new(HttpClient::plaintext(), config)
}

#[tokio::test]
async fn unary_square() {
    let addr = start_server().await;
    let client = make_client(addr);
    let resp = client
        .square(SquareRequest {
            value: Some(7),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(resp.view().squared, Some(49));
}

#[tokio::test]
async fn server_stream_range() {
    let addr = start_server().await;
    let client = make_client(addr);
    let mut stream = client
        .range(RangeRequest {
            start: Some(10),
            count: Some(5),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut got = Vec::new();
    while let Some(msg) = stream.message().await.unwrap() {
        got.push(msg.view().value.unwrap());
    }
    assert_eq!(got, vec![10, 11, 12, 13, 14]);
}

#[tokio::test]
async fn client_stream_sum() {
    let addr = start_server().await;
    let client = make_client(addr);
    let messages = connectrpc::client::stream_iter([3, 5, 7, 9].map(|v| SumRequest {
        value: Some(v),
        ..Default::default()
    }));
    let resp = client.sum(messages).await.unwrap();
    assert_eq!(resp.view().total, Some(24));
}

#[tokio::test]
async fn bidi_stream_running_sum() {
    let addr = start_server().await;
    let client = make_client(addr);
    let mut bidi = client.running_sum().await.unwrap();
    let mut got = Vec::new();
    for v in [2, 4, 6, 8] {
        bidi.send(RunningSumRequest {
            value: Some(v),
            ..Default::default()
        })
        .await
        .unwrap();
        let msg = bidi.message().await.unwrap().unwrap();
        got.push(msg.view().total.unwrap());
    }
    bidi.close_send();
    assert_eq!(got, vec![2, 6, 12, 20]);
}
