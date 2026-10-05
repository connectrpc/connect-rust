//! RPCs named after methods that an `Arc` already has (issue #309).
//!
//! That the generated code for `NamesService` compiles is the main assertion.
//! Each RPC is also called through both server paths, which shows that the
//! call reaches the handler and not the `Arc`'s method of the same name.

use std::sync::Arc;

use buffa::Message;
use buffa::view::HasMessageView;
use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::{
    CodecFormat, ConnectError, ConnectRpcService, Encodable, RequestContext, Response, Router,
    ServiceRequest, ServiceResult, ServiceStream, StreamMessage,
};
use futures::StreamExt;
use tokio::net::TcpListener;

use crate::proto::test::names::v1::{
    NameRequest, NameResponse, NamesService, NamesServiceClient, NamesServiceExt,
    NamesServiceServer,
};

fn named(name: String) -> NameResponse {
    NameResponse {
        name,
        ..Default::default()
    }
}

fn one(name: String) -> ServiceResult<ServiceStream<NameResponse>> {
    Response::stream_ok(futures::stream::iter([Ok::<_, ConnectError>(named(name))]))
}

async fn count(
    mut requests: ServiceStream<StreamMessage<NameRequest>>,
) -> Result<usize, ConnectError> {
    let mut count = 0;
    while let Some(request) = requests.next().await {
        request?;
        count += 1;
    }
    Ok(count)
}

struct Names;

impl NamesService for Names {
    async fn register(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, NameRequest>,
    ) -> ServiceResult<NameResponse> {
        Response::ok(named(format!("register {}", request.name)))
    }

    async fn clone(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, NameRequest>,
    ) -> ServiceResult<ServiceStream<NameResponse>> {
        one(format!("clone {}", request.name))
    }

    async fn into(
        &self,
        _ctx: RequestContext,
        requests: ServiceStream<StreamMessage<NameRequest>>,
    ) -> ServiceResult<NameResponse> {
        Response::ok(named(format!("into {}", count(requests).await?)))
    }

    async fn drop(
        &self,
        _ctx: RequestContext,
        requests: ServiceStream<StreamMessage<NameRequest>>,
    ) -> ServiceResult<ServiceStream<NameResponse>> {
        one(format!("drop {}", count(requests).await?))
    }
}

async fn serve(app: axum::Router) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

fn ada() -> NameRequest {
    NameRequest {
        name: "ada".into(),
        ..Default::default()
    }
}

/// Calls each RPC once and returns the four responses.
async fn call_each(addr: std::net::SocketAddr) -> [String; 4] {
    let config = ClientConfig::new(format!("http://{addr}").parse().unwrap());
    let client = NamesServiceClient::new(HttpClient::plaintext(), config);
    // `client.clone()` would be the RPC, which takes a request.
    let client = Clone::clone(&client);

    let register = client.register(ada()).await.unwrap().into_owned().name;

    let mut stream = client.clone(ada()).await.unwrap();
    let clone = stream
        .message()
        .await
        .unwrap()
        .unwrap()
        .view()
        .name
        .to_owned();

    let requests = futures::stream::iter([ada(), ada()]);
    // `client.into(requests)` would resolve to `Into::into`, which takes the
    // client by value and so matches before the generated `&self` method.
    let into = NamesServiceClient::into(&client, requests)
        .await
        .unwrap()
        .into_owned()
        .name;

    let mut stream = client.drop().await.unwrap();
    stream.send(ada()).await.unwrap();
    stream.close_send();
    let drop = stream
        .message()
        .await
        .unwrap()
        .unwrap()
        .view()
        .name
        .to_owned();

    [register, clone, into, drop]
}

const EXPECTED: [&str; 4] = ["register ada", "clone ada", "into 2", "drop 1"];

#[tokio::test]
async fn rpcs_named_like_arc_methods_through_the_router() {
    // On an `Arc`, `register` is the extension method that takes a `Router`.
    let router = Arc::new(Names).register(Router::new());
    let addr = serve(router.into_axum_router()).await;
    assert_eq!(call_each(addr).await, EXPECTED);
}

#[tokio::test]
async fn rpcs_named_like_arc_methods_through_the_dispatcher() {
    let service = ConnectRpcService::new(NamesServiceServer::new(Names));
    let addr = serve(axum::Router::new().fallback_service(service)).await;
    assert_eq!(call_each(addr).await, EXPECTED);
}

#[tokio::test]
async fn rpcs_named_like_arc_methods_through_add_service() {
    let router = Router::new().add_service(Arc::new(Names));
    let addr = serve(router.into_axum_router()).await;
    assert_eq!(call_each(addr).await, EXPECTED);
}

/// Encodes the response of a unary handler and returns its name.
fn name_of(response: &Response<impl Encodable<NameResponse>>) -> String {
    let bytes = Encodable::encode(&response.body, CodecFormat::Proto).unwrap();
    NameResponse::decode_from_slice(&bytes).unwrap().name
}

#[tokio::test]
async fn handler_called_directly() {
    let body = bytes::Bytes::from(ada().encode_to_vec());
    let view = NameRequest::decode_view(&body).unwrap();
    let request = || ServiceRequest::<NameRequest>::from_parts(&view, &body);
    let ctx = || RequestContext::new(http::HeaderMap::new());

    let names = Names;
    let response = NamesService::register(&names, ctx(), request()).await;
    assert_eq!(name_of(&response.unwrap()), "register ada");

    // `names.register(ctx, request)` would be the extension method.
    let names = Arc::new(names);
    let response = NamesService::register(&*names, ctx(), request()).await;
    assert_eq!(name_of(&response.unwrap()), "register ada");
}
