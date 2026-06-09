//! Middleware-example server: demonstrates how data flows from a
//! middleware layer into a connectrpc handler via `Context::extensions`.
//! Three layers wrap the dispatcher:
//!
//! 1. `TraceLayer` - request/response logging via the `tracing` crate.
//! 2. `auth_middleware` - validates a `Bearer <token>` header, stamps
//!    the caller's identity into request extensions, and short-circuits
//!    unauthorized requests with 401. Written as an axum `from_fn`
//!    middleware (the idiomatic axum pattern for stateful auth).
//! 3. `TimeoutLayer` - hard ceiling on per-request handler latency.
//!
//! The handler reads the identity from `Context::extensions()` and writes
//! a `x-served-by` response trailer via `Context::set_trailer()`.
//!
//! Run with:
//!
//! ```sh
//! RUST_LOG=info,tower_http=debug \
//!     cargo run -p middleware-example --bin middleware-server
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use connectrpc::{ConnectError, ErrorCode, RequestContext, Router, ServiceRequest, ServiceResult};
use http_body_util::BodyExt;
use tower::ServiceBuilder;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

pub mod proto {
    connectrpc::include_generated!();
}

use proto::anthropic::connectrpc::middleware_demo::v1::*;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ============================================================================
// Domain types
// ============================================================================

/// Caller identity, attached to the request by `auth_middleware`.
#[derive(Debug, Clone)]
struct UserId(String);

/// Static token → identity table. Real services would consult a
/// database, JWT verifier, or upstream auth service here.
fn token_table() -> HashMap<String, UserId> {
    HashMap::from([
        ("demo-token-alice".into(), UserId("alice".into())),
        ("demo-token-bob".into(), UserId("bob".into())),
    ])
}

/// Static secret store. Each secret declares which users may read it.
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
// Auth middleware (axum from_fn pattern)
// ============================================================================

/// Validates a `Bearer <token>` header against the static token table
/// and stamps the caller's identity into request extensions before
/// forwarding. Unauthorized requests get a 401 directly - the inner
/// service (the connect dispatcher) is never invoked.
///
/// `axum::middleware::from_fn_with_state` is the idiomatic axum pattern
/// for stateful interceptors: write a plain async fn, mount it via
/// `from_fn_with_state(state, fn)`, and the resulting Layer composes
/// with any tower stack. Equivalent to a hand-rolled `tower::Layer` +
/// `tower::Service` pair, but without the trait-boilerplate ceremony.
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

    // The connect dispatcher forwards req.extensions() into the request
    // context verbatim, so the handler reads the UserId via
    // ctx.extensions().get::<UserId>().
    req.extensions_mut().insert(user);
    next.run(req).await
}

/// Build a 401 response in the Connect-protocol JSON error shape.
/// Returning a structured Connect error keeps clients on the same
/// error-handling path they use for handler-side `ConnectError`s.
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
// Service handler
// ============================================================================

struct SecretServiceImpl {
    store: HashMap<String, (String, Vec<&'static str>)>,
}

impl SecretService for SecretServiceImpl {
    async fn get_secret(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetSecretRequest>,
    ) -> ServiceResult<GetSecretResponse> {
        // The auth layer stamped UserId into the http::Request extensions,
        // which the connect dispatcher then forwarded into ctx.extensions().
        let user = ctx
            .extensions()
            .get::<UserId>()
            .ok_or_else(|| {
                ConnectError::new(
                    ErrorCode::Internal,
                    "auth layer did not attach UserId - middleware misconfigured",
                )
            })?
            .clone();

        // Edition 2023 default presence is EXPLICIT, so string fields
        // are Option<String> on owned messages and Option<&str> on
        // views. unwrap_or("") treats unset as empty, mirroring proto3.
        let name = request.name.unwrap_or("").to_owned();
        let (value, allowed) = self.store.get(&name).ok_or_else(|| {
            ConnectError::new(ErrorCode::NotFound, format!("no secret named {name:?}"))
        })?;

        if !allowed.iter().any(|u| *u == user.0) {
            return Err(ConnectError::new(
                ErrorCode::PermissionDenied,
                format!("user {:?} cannot read {name:?}", user.0),
            ));
        }

        // Stamp the serving user into a response trailer so the client
        // (or any tracing middleware downstream) can attribute the read.
        Ok(connectrpc::Response::new(GetSecretResponse {
            value: Some(value.clone()),
            ..Default::default()
        })
        .with_trailer("x-served-by", user.0))
    }
}

// ============================================================================
// main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let addr: std::net::SocketAddr = "127.0.0.1:8080".parse()?;

    let service = Arc::new(SecretServiceImpl {
        store: secret_store(),
    });
    let connect_router = service.register(Router::new());

    // Compose the tower stack. ServiceBuilder applies layers top-to-bottom:
    // TraceLayer is outermost (wraps everything), TimeoutLayer is innermost
    // (closest to the dispatcher). A request flows trace -> auth -> timeout
    // -> connect dispatcher -> handler.
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

    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("SecretService listening on http://{addr}");
    println!(
        "Try: curl -X POST http://{addr}/anthropic.connectrpc.middleware_demo.v1.SecretService/GetSecret \\"
    );
    println!("       -H 'authorization: Bearer demo-token-alice' \\");
    println!("       -H 'content-type: application/json' \\");
    println!("       -d '{{\"name\": \"shared\"}}'");

    axum::serve(listener, app).await?;
    Ok(())
}
