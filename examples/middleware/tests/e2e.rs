//! End-to-end test: spin up SecretService with the full middleware
//! stack, exercise the auth layer + handler permission check, and
//! verify response trailers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use buffa::view::OwnedView;
use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::{ConnectError, ErrorCode, RequestContext, Router, ServiceResult};
use http_body_util::BodyExt;
use tower::ServiceBuilder;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

pub mod proto {
    connectrpc::include_generated!();
}

use proto::anthropic::connectrpc::middleware_demo::v1::*;

// ============================================================================
// Domain types (mirror src/server.rs)
// ============================================================================

#[derive(Debug, Clone)]
struct UserId(String);

fn token_table() -> HashMap<String, UserId> {
    HashMap::from([
        ("demo-token-alice".into(), UserId("alice".into())),
        ("demo-token-bob".into(), UserId("bob".into())),
    ])
}

fn secret_store() -> HashMap<String, (String, Vec<&'static str>)> {
    HashMap::from([
        (
            "shared".into(),
            ("the value of teamwork".into(), vec!["alice", "bob"]),
        ),
        (
            "alice-only".into(),
            ("alice's diary entry".into(), vec!["alice"]),
        ),
    ])
}

// ============================================================================
// auth_middleware (mirror src/server.rs)
// ============================================================================

async fn auth_middleware(
    State(tokens): State<Arc<HashMap<String, UserId>>>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(token) = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
    else {
        return unauthorized("missing Bearer token");
    };
    let Some(user) = tokens.get(token).cloned() else {
        return unauthorized("invalid Bearer token");
    };
    req.extensions_mut().insert(user);
    next.run(req).await
}

fn unauthorized(message: &'static str) -> Response {
    let err = ConnectError::new(ErrorCode::Unauthenticated, message);
    let body = http_body_util::Full::new(err.to_json())
        .map_err(|never| match never {})
        .boxed_unsync();
    http::Response::builder()
        .status(http::StatusCode::UNAUTHORIZED)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::new(body))
        .unwrap()
}

// ============================================================================
// SecretService impl (mirror src/server.rs)
// ============================================================================

struct SecretServiceImpl {
    store: HashMap<String, (String, Vec<&'static str>)>,
}

impl SecretService for SecretServiceImpl {
    async fn get_secret(
        &self,
        ctx: RequestContext,
        request: OwnedView<GetSecretRequestView<'static>>,
    ) -> ServiceResult<GetSecretResponse> {
        let user = ctx
            .extensions()
            .get::<UserId>()
            .ok_or_else(|| ConnectError::new(ErrorCode::Internal, "auth layer misconfigured"))?
            .clone();
        let name = request.name.unwrap_or("").to_owned();
        let (value, allowed) = self
            .store
            .get(&name)
            .ok_or_else(|| ConnectError::new(ErrorCode::NotFound, format!("no {name:?}")))?;
        if !allowed.iter().any(|u| *u == user.0) {
            return Err(ConnectError::new(
                ErrorCode::PermissionDenied,
                format!("user {:?} cannot read {name:?}", user.0),
            ));
        }
        Ok(connectrpc::Response::new(GetSecretResponse {
            value: Some(value.clone()),
            ..Default::default()
        })
        .with_trailer("x-served-by", user.0))
    }
}

// ============================================================================
// Test scaffolding
// ============================================================================

async fn start_server() -> std::net::SocketAddr {
    let service = Arc::new(SecretServiceImpl {
        store: secret_store(),
    });
    let connect_router = service.register(Router::new());
    let tokens = Arc::new(token_table());
    let app = axum::Router::new()
        .fallback_service(connect_router.into_axum_service())
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(axum::middleware::from_fn_with_state(
                    tokens,
                    auth_middleware,
                ))
                .layer(TimeoutLayer::with_status_code(
                    http::StatusCode::REQUEST_TIMEOUT,
                    Duration::from_secs(5),
                )),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

fn make_client(addr: std::net::SocketAddr, token: &str) -> SecretServiceClient<HttpClient> {
    let config = ClientConfig::new(format!("http://{addr}").parse().unwrap())
        .with_default_header("authorization", format!("Bearer {token}"));
    SecretServiceClient::new(HttpClient::plaintext(), config)
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn authorized_call_returns_value_and_trailer() {
    let addr = start_server().await;
    let client = make_client(addr, "demo-token-alice");
    let resp = client
        .get_secret(GetSecretRequest {
            name: Some("shared".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(resp.view().value, Some("the value of teamwork"));
    let served_by = resp
        .trailers()
        .get("x-served-by")
        .expect("trailer should be present")
        .to_str()
        .unwrap();
    assert_eq!(served_by, "alice");
}

#[tokio::test]
async fn missing_auth_returns_unauthenticated() {
    let addr = start_server().await;
    // Build a client with no auth header at all.
    let config = ClientConfig::new(format!("http://{addr}").parse().unwrap());
    let client = SecretServiceClient::new(HttpClient::plaintext(), config);
    let err = client
        .get_secret(GetSecretRequest {
            name: Some("shared".into()),
            ..Default::default()
        })
        .await
        .expect_err("must reject unauthenticated request");
    assert_eq!(err.code, ErrorCode::Unauthenticated);
}

#[tokio::test]
async fn invalid_token_returns_unauthenticated() {
    let addr = start_server().await;
    let client = make_client(addr, "not-a-real-token");
    let err = client
        .get_secret(GetSecretRequest {
            name: Some("shared".into()),
            ..Default::default()
        })
        .await
        .expect_err("must reject invalid token");
    assert_eq!(err.code, ErrorCode::Unauthenticated);
}

#[tokio::test]
async fn permission_denied_for_other_users_secret() {
    let addr = start_server().await;
    let client = make_client(addr, "demo-token-bob");
    let err = client
        .get_secret(GetSecretRequest {
            name: Some("alice-only".into()),
            ..Default::default()
        })
        .await
        .expect_err("bob cannot read alice's secret");
    assert_eq!(err.code, ErrorCode::PermissionDenied);
}
