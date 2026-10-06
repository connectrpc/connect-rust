//! Proto types and services named after what generated service code refers
//! to: prelude items such as `Result` and `Send`, and the generic parameters
//! `S` and `T`.
//!
//! That the generated code for the `shadow_*.proto` files compiles is the main
//! assertion; for `services::Result` and `ChildService` it is the only one.
//! The other service traits are implemented, and the calls with `S` and `T`
//! run each type of RPC through the Router and the dispatcher.

use std::sync::Arc;

use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::{
    ConnectError, ConnectRpcService, InboundStream, RequestContext, Response, Router,
    ServiceRequest, ServiceResult, ServiceStream,
};
use futures::StreamExt;
use tokio::net::TcpListener;

use crate::proto::test::shadow::{services::v1 as services, types::v1 as types};

fn s(value: String) -> types::S {
    types::S {
        value,
        ..Default::default()
    }
}

fn t(value: String) -> types::T {
    types::T {
        value,
        ..Default::default()
    }
}

struct Impl;

impl types::GenericParamService for Impl {
    async fn unary(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, types::S>,
    ) -> ServiceResult<types::T> {
        Response::ok(t(format!("unary {}", request.value)))
    }

    async fn server_stream(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, types::T>,
    ) -> ServiceResult<ServiceStream<types::S>> {
        let item = s(format!("server_stream {}", request.value));
        Response::stream_ok(futures::stream::iter([Ok::<_, ConnectError>(item)]))
    }

    async fn client_stream(
        &self,
        _ctx: RequestContext,
        mut requests: InboundStream<types::S>,
    ) -> ServiceResult<types::T> {
        let mut values = vec!["client_stream".to_owned()];
        while let Some(request) = requests.next().await {
            values.push(request?.view().value.to_owned());
        }
        Response::ok(t(values.join(" ")))
    }

    async fn bidi(
        &self,
        _ctx: RequestContext,
        requests: InboundStream<types::T>,
    ) -> ServiceResult<ServiceStream<types::S>> {
        Response::stream_ok(
            requests.map(|request| Ok(s(format!("bidi {}", request?.view().value)))),
        )
    }
}

impl types::PreludeService for Impl {
    async fn unary(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, types::Option>,
    ) -> ServiceResult<types::Result> {
        Response::ok(types::Result {
            value: format!("unary {}", request.value),
            ..Default::default()
        })
    }

    async fn server_stream(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, types::Box>,
    ) -> ServiceResult<ServiceStream<types::Send>> {
        Response::stream_ok(futures::stream::empty())
    }

    async fn client_stream(
        &self,
        _ctx: RequestContext,
        _requests: InboundStream<types::Sync>,
    ) -> ServiceResult<types::Clone> {
        Response::ok(types::Clone::default())
    }

    async fn bidi(
        &self,
        _ctx: RequestContext,
        _requests: InboundStream<types::Into>,
    ) -> ServiceResult<ServiceStream<types::From>> {
        Response::stream_ok(futures::stream::empty())
    }
}

/// Implements a single-RPC service from `shadow_services.proto`. Only `S` and
/// `T` are served.
macro_rules! impl_call {
    ($($service:ident),*) => {$(
        impl services::$service for Impl {
            async fn call(
                &self,
                _ctx: RequestContext,
                _request: ServiceRequest<'_, services::Request>,
            ) -> ServiceResult<services::Response> {
                Response::ok(services::Response::default())
            }
        }
    )*};
}

impl_call!(Box, Clone, Into, Option, S, Send, Sync, T);

async fn serve(app: axum::Router) -> ClientConfig {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    ClientConfig::new(format!("http://{addr}").parse().unwrap())
}

/// Calls each `GenericParamService` RPC once and returns the four responses.
async fn call_each(config: ClientConfig) -> [String; 4] {
    let client = types::GenericParamServiceClient::new(HttpClient::plaintext(), config);

    let unary = client.unary(s("a".into())).await.unwrap().into_owned();

    let mut stream = client.server_stream(t("b".into())).await.unwrap();
    let server_stream = stream.message().await.unwrap().unwrap();

    let requests = futures::stream::iter([s("c".into()), s("d".into())]);
    let client_stream = client.client_stream(requests).await.unwrap().into_owned();

    let mut stream = client.bidi().await.unwrap();
    stream.send(t("e".into())).await.unwrap();
    stream.close_send();
    let bidi = stream.message().await.unwrap().unwrap();

    [
        unary.value,
        server_stream.view().value.to_owned(),
        client_stream.value,
        bidi.view().value.to_owned(),
    ]
}

const EXPECTED: [&str; 4] = ["unary a", "server_stream b", "client_stream c d", "bidi e"];

#[tokio::test]
async fn types_named_like_generic_params_through_the_router() {
    let router =
        Router::new().add_service::<_, types::GenericParamServiceRegisterMarker>(Arc::new(Impl));
    let config = serve(router.into_axum_router()).await;
    assert_eq!(call_each(config).await, EXPECTED);
}

#[tokio::test]
async fn types_named_like_generic_params_through_the_dispatcher() {
    let service = ConnectRpcService::new(types::GenericParamServiceServer::new(Impl));
    let config = serve(axum::Router::new().fallback_service(service)).await;
    assert_eq!(call_each(config).await, EXPECTED);
}

#[tokio::test]
async fn types_named_like_prelude_items() {
    let service = ConnectRpcService::new(types::PreludeServiceServer::new(Impl));
    let config = serve(axum::Router::new().fallback_service(service)).await;
    let client = types::PreludeServiceClient::new(HttpClient::plaintext(), config);

    let request = types::Option {
        value: "a".into(),
        ..Default::default()
    };
    let response: Result<_, ConnectError> = client.unary(request).await;
    assert_eq!(response.unwrap().view().value, "unary a");
}

#[tokio::test]
async fn services_named_like_generic_params() {
    use services::{SExt, TExt};

    let service = Arc::new(Impl);
    let router = SExt::register(Arc::clone(&service), Router::new());
    let router = TExt::register(service, router);
    let config = serve(router.into_axum_router()).await;

    let request = services::Request::default;
    let client = services::SClient::new(HttpClient::plaintext(), config.clone());
    client.call(request()).await.unwrap();
    let client = services::TClient::new(HttpClient::plaintext(), config.clone());
    client.call(request()).await.unwrap();

    let service = ConnectRpcService::new(services::TServer::new(Impl));
    let config = serve(axum::Router::new().fallback_service(service)).await;
    let client = services::TClient::new(HttpClient::plaintext(), config);
    client.call(request()).await.unwrap();
}
