//! The bridge from a user [`Checker`] to the generated `grpc.health.v1.Health`
//! service trait.

use std::sync::Arc;

use connectrpc::{
    ConnectError, RequestContext, Response, Router, ServiceRequest, ServiceResult, ServiceStream,
};
use futures::StreamExt;

use crate::connect::grpc::health::v1::{HEALTH_CHECK_SPEC, HEALTH_WATCH_SPEC, Health, HealthExt};
use crate::proto::grpc::health::v1::{
    HealthCheckRequest, HealthCheckResponse, health_check_response::ServingStatus,
};
use crate::{Checker, StaticChecker};

/// gRPC-compatible health service backed by a user-supplied [`Checker`].
///
/// Wraps any `Checker` and exposes it as the wire-format
/// `grpc.health.v1.Health` service. For the common case of a
/// [`StaticChecker`]-backed setup, prefer the [`install_static`]
/// free function; reach for `HealthService` directly when you implement
/// [`Checker`] yourself.
///
/// ```no_run
/// use std::sync::Arc;
/// use connectrpc::Router;
/// use connectrpc_health::{HealthExt, HealthService, StaticChecker};
///
/// let checker = Arc::new(StaticChecker::with_services([
///     "acme.user.v1.UserService",
/// ]));
/// let service = Arc::new(HealthService::from_arc(Arc::clone(&checker)));
/// // Hold the health routes to a few KiB per request; `install_static`
/// // does this itself, generic registration cannot.
/// let router = service.register(Router::new());
/// let router = connectrpc_health::apply_request_limits(router, connectrpc_health::request_limits());
/// ```
///
/// `HealthService::new(checker)` is the move-in shorthand; use
/// [`from_arc`](Self::from_arc) when you keep your own clone of the
/// `Arc<C>` to flip status from outside the service.
///
/// # Unknown services
///
/// Non-empty unregistered services surface as
/// `Err(ConnectError::not_found(_))` from both `Check` and `Watch`; the
/// empty service is pre-registered with [`Status::Serving`] and behaves
/// like any other service (see [`StaticChecker`]'s `# Empty service name`
/// section). The [gRPC Health spec] additionally specifies a
/// `SERVICE_UNKNOWN` keep-stream-open flow for `Watch` that this crate
/// does not implement (matching the Go `connectrpc.com/grpchealth`
/// reference). Probes that treat any error as failure — kubelet,
/// `grpc_health_probe`, Linkerd, Istio — work unchanged.
///
/// [`Status::Serving`]: crate::Status::Serving
/// [`StaticChecker`]: crate::StaticChecker
///
/// [gRPC Health spec]: https://github.com/grpc/grpc/blob/master/doc/health-checking.md
pub struct HealthService<C> {
    checker: Arc<C>,
}

impl<C: Checker> HealthService<C> {
    /// Wrap a checker by value; it is moved into a fresh `Arc<C>`.
    #[must_use]
    pub fn new(checker: C) -> Self {
        Self {
            checker: Arc::new(checker),
        }
    }

    /// Wrap a checker that is already inside an `Arc<C>`. Use this when
    /// you keep your own clone of the `Arc<C>` to flip status from
    /// outside the service.
    #[must_use]
    pub fn from_arc(checker: Arc<C>) -> Self {
        Self { checker }
    }

    /// Return a fresh `Arc<C>` handle to the inner checker. One atomic
    /// increment per call; safe to keep, store, or pass into another
    /// `HealthService::from_arc` to mount the same checker behind a
    /// second service.
    #[must_use]
    pub fn checker(&self) -> Arc<C> {
        Arc::clone(&self.checker)
    }
}

/// One-line installation for the static-checker happy path.
///
/// Builds a [`StaticChecker`] pre-populated with `services` (each
/// reporting [`Status::Serving`](crate::Status::Serving)), wraps it in
/// a [`HealthService`], registers that service on `router`, and hands
/// back both the updated router and a shared `Arc<StaticChecker>` for
/// status mutation. The two health routes are set to
/// [`request_limits`] (16 KiB per request) via [`apply_request_limits`],
/// which replaces the service-wide limits on those routes even when they
/// are tighter; to tune them, call `apply_request_limits(router, yours)`
/// on the returned router — the later call wins.
///
/// Pass the generated `*_SERVICE_NAME` constants from your service
/// stubs to avoid drifting from the wire name a probe will ask about.
///
/// See the crate-level [Quick start](crate#quick-start) for an
/// end-to-end example including status flips and shutdown.
///
/// # Caveat on destructuring discards
///
/// `#[must_use]` only fires when the *whole* return value is dropped at
/// statement position (`install_static(...);`). Any pattern binding —
/// `let _ = install_static(...);`,
/// `let (router, _) = install_static(...);`,
/// `let (_, _) = install_static(...);` — counts as a use by the lint
/// (the `Arc<StaticChecker>` is still dropped at end of statement, but
/// the lint stays quiet). Also note that the input `router` is moved
/// into the function; if you drop the returned tuple you've lost both
/// the mount and the only handle for status mutation. Bind both halves
/// of the tuple even if you don't immediately call the checker.
#[must_use = "install_static returns (Router, Arc<StaticChecker>) — \
              drop either and you've lost the mount or the only handle \
              for status mutation"]
pub fn install_static<I, S>(router: Router, services: I) -> (Router, Arc<StaticChecker>)
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let checker = Arc::new(StaticChecker::with_services(services));
    let service = Arc::new(HealthService::from_arc(Arc::clone(&checker)));
    let router = apply_request_limits(service.register(router), request_limits());
    (router, checker)
}

/// The largest request message, in bytes on the wire and after
/// decompression, the health routes accept under [`request_limits`]: 16 KiB.
///
/// A `HealthCheckRequest` carries one fully-qualified service name, so a
/// legitimate request is a few hundred bytes; 16 KiB leaves two orders of
/// magnitude of headroom while sizing the routes to their actual request
/// profile rather than the general-purpose service-wide default.
pub const MAX_REQUEST_BYTES: usize = 16 * 1024;

/// The per-route [`Limits`](connectrpc::Limits) this crate sets on its
/// `Check` and `Watch` routes: message size capped at [`MAX_REQUEST_BYTES`]
/// (the request body at that plus the 5-byte envelope framed protocols add),
/// and the element-memory decode budget at four times that instead of the
/// 32 MiB `connectrpc` default. A `HealthCheckRequest` has no repeated or
/// map fields, so a decode that charges the budget at all is one this
/// service was never going to answer; the bound keeps a future revision of
/// the protocol from changing that quietly.
#[must_use]
pub fn request_limits() -> connectrpc::Limits {
    connectrpc::Limits::default()
        .with_max_request_body_size(MAX_REQUEST_BYTES + connectrpc::envelope::HEADER_SIZE)
        .with_max_message_size(MAX_REQUEST_BYTES)
        .with_element_memory_limit(4 * MAX_REQUEST_BYTES)
}

/// Set the `Check` and `Watch` routes on `router` to `limits`, replacing
/// the service-wide limits for them (whether looser or tighter).
///
/// [`install_static`] already applies [`request_limits`]. Call this
/// yourself, usually with [`request_limits`], after registering a
/// [`HealthService`] built around a custom [`Checker`] — via
/// [`HealthExt::register`](crate::HealthExt) or
/// [`Router::add_service`](connectrpc::Router::add_service) — since those
/// generic registration paths cannot; or call it after either path with
/// your own [`Limits`](connectrpc::Limits) to tune the health routes
/// specifically. The later call wins. Build an override from
/// [`request_limits`] rather than `Limits::default()`: the replacement is
/// whole, so anything the profile sets and the override does not is reset
/// to the `connectrpc` default.
///
/// # Panics
///
/// Panics if the health routes are not registered on `router`.
#[must_use]
pub fn apply_request_limits(router: Router, limits: connectrpc::Limits) -> Router {
    router
        .with_route_limits(HEALTH_CHECK_SPEC.procedure, limits)
        .with_route_limits(HEALTH_WATCH_SPEC.procedure, limits)
}

impl<C> Clone for HealthService<C> {
    fn clone(&self) -> Self {
        Self {
            checker: Arc::clone(&self.checker),
        }
    }
}

impl<C> std::fmt::Debug for HealthService<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthService").finish_non_exhaustive()
    }
}

impl<C: Checker> Health for HealthService<C> {
    async fn check(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, HealthCheckRequest>,
    ) -> ServiceResult<HealthCheckResponse> {
        let status = self.checker.check(request.service).await?;
        Response::ok(HealthCheckResponse {
            status: ServingStatus::from(status).into(),
            ..Default::default()
        })
    }

    async fn watch(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, HealthCheckRequest>,
    ) -> ServiceResult<ServiceStream<HealthCheckResponse>> {
        let stream = self.checker.watch(request.service).await?;
        Response::stream_ok(stream.map(|status| {
            Ok::<_, ConnectError>(HealthCheckResponse {
                status: ServingStatus::from(status).into(),
                ..Default::default()
            })
        }))
    }
}

// Integration tests drive the server through the generated client,
// which is feature-gated. `client` is default-on; `cargo test
// --no-default-features` skips this module.
#[cfg(all(test, feature = "client"))]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use connectrpc::Router;
    use connectrpc::client::{ClientConfig, HttpClient};
    use tokio::net::TcpListener;

    use super::*;
    use crate::connect::grpc::health::v1::{HealthClient, HealthExt};
    use crate::proto::grpc::health::v1::HealthCheckRequest;
    use crate::{StaticChecker, Status};

    /// Spin up a Health server on a free port and hand back the address
    /// and a client targeting it. The server runs until the test exits.
    async fn spawn_health_server(
        checker: Arc<StaticChecker>,
    ) -> (HealthClient<HttpClient>, std::net::SocketAddr) {
        let service = Arc::new(HealthService::from_arc(checker));
        let router = service.register(Router::new());
        let app = router.into_axum_router();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let config = ClientConfig::new(format!("http://{addr}").parse().unwrap());
        let client = HealthClient::new(HttpClient::plaintext(), config);
        (client, addr)
    }

    #[tokio::test]
    async fn check_serving_service() {
        let checker = Arc::new(StaticChecker::with_services(["acme.A"]));
        let (client, _addr) = spawn_health_server(checker).await;

        let resp = client
            .check(HealthCheckRequest {
                service: "acme.A".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.view().status, ServingStatus::SERVING);
    }

    #[tokio::test]
    async fn check_empty_service_returns_serving() {
        let checker = Arc::new(StaticChecker::new());
        let (client, _addr) = spawn_health_server(checker).await;

        let resp = client.check(HealthCheckRequest::default()).await.unwrap();
        assert_eq!(resp.view().status, ServingStatus::SERVING);
    }

    #[tokio::test]
    async fn check_unknown_service_returns_not_found() {
        let checker = Arc::new(StaticChecker::new());
        let (client, _addr) = spawn_health_server(checker).await;

        let err = client
            .check(HealthCheckRequest {
                service: "acme.NoSuch".into(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn check_reflects_not_serving_after_update() {
        let checker = Arc::new(StaticChecker::with_services(["acme.A"]));
        let (client, _addr) = spawn_health_server(Arc::clone(&checker)).await;

        checker.set_status("acme.A", Status::NotServing).unwrap();

        let resp = client
            .check(HealthCheckRequest {
                service: "acme.A".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.view().status, ServingStatus::NOT_SERVING);
    }

    /// End-to-end companion to `dropping_watch_stream_releases_subscriber`
    /// in `static_checker`: when a client drops its handle to the Watch
    /// RPC mid-stream, the server's spawned response future is cancelled,
    /// the response body's stream chain (boxed `Map` → `StatusStream` →
    /// `WatchStream`) is dropped, and the underlying
    /// [`tokio::sync::watch::Receiver`] is released back to the
    /// [`watch::Sender`]'s receiver count. The unit test pins the
    /// `WatchStream`→`Receiver` rung in isolation; this test ties the
    /// chain together through axum + hyper so a regression in any
    /// intermediate layer (axum's response cancellation, connectrpc's
    /// body driver, the codegen's stream wrapper) surfaces here.
    #[tokio::test]
    async fn client_disconnect_releases_server_side_subscriber() {
        let checker = Arc::new(StaticChecker::with_services(["acme.A"]));
        let (client, _addr) = spawn_health_server(Arc::clone(&checker)).await;

        let before = checker
            .receiver_count_for("acme.A")
            .expect("registered service must have a Sender");

        // Open Watch and pull the initial frame. The pull is
        // load-bearing: `client.watch(...).await` returns once the
        // request headers are written, but the server's `subscribe()`
        // call happens inside the spawned handler future. Without a
        // round-trip-forcing read, the next `receiver_count_for` read
        // could observe `before` (handler not yet polled to subscribe)
        // and the +1 assertion would race.
        let mut stream = client
            .watch(HealthCheckRequest {
                service: "acme.A".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let first = stream
            .message()
            .await
            .unwrap()
            .expect("expected initial Watch message");
        assert_eq!(first.view().status, ServingStatus::SERVING);

        // Server-side receiver count must be elevated by exactly 1.
        assert_eq!(
            checker.receiver_count_for("acme.A"),
            Some(before + 1),
            "server-side Receiver must be live while the client holds \
             the stream open"
        );

        // Simulate client disconnect. `drop(stream)` is the load-bearing
        // step: it closes the HTTP/2 response body's receive half, which
        // h2 surfaces to the server's body-driver task as cancellation.
        // `drop(client)` is belt-and-braces — the HttpClient owns the
        // connection pool but not this RPC's lifetime; for an already
        // cancelled stream it's a no-op.
        drop(stream);
        drop(client);

        // Cancellation propagation through axum+hyper is asynchronous;
        // poll with a tight tick and a generous overall budget. Two
        // seconds is well above the observed propagation latency
        // (sub-100ms locally) and tolerates slow CI.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let count = checker
                .receiver_count_for("acme.A")
                .expect("entry still registered post-disconnect");
            if count == before {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "server-side Receiver was not released within 2s of \
                     client disconnect — expected {before}, still {count}"
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn watch_streams_initial_then_changes() {
        let checker = Arc::new(StaticChecker::with_services(["acme.A"]));
        let (client, _addr) = spawn_health_server(Arc::clone(&checker)).await;

        let mut stream = client
            .watch(HealthCheckRequest {
                service: "acme.A".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        // First message is the current state.
        let initial = stream
            .message()
            .await
            .unwrap()
            .expect("expected initial Watch message");
        assert_eq!(initial.view().status, ServingStatus::SERVING);

        // Update fires a follow-up message.
        checker.set_status("acme.A", Status::NotServing).unwrap();
        let after = tokio::time::timeout(Duration::from_secs(2), stream.message())
            .await
            .expect("watch did not deliver update within timeout")
            .unwrap()
            .expect("expected follow-up Watch message");
        assert_eq!(after.view().status, ServingStatus::NOT_SERVING);
    }

    #[tokio::test]
    async fn checker_accessor_returns_shared_arc() {
        let svc = HealthService::new(StaticChecker::with_services(["acme.A"]));
        // Mutating through the accessor must be visible to the service.
        svc.checker()
            .set_status("acme.A", Status::NotServing)
            .unwrap();
        let (client, _addr) = spawn_health_server(svc.checker()).await;
        let resp = client
            .check(HealthCheckRequest {
                service: "acme.A".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.view().status, ServingStatus::NOT_SERVING);
    }

    #[tokio::test]
    async fn install_static_mounts_and_returns_mutable_checker() {
        use crate::install_static;
        let (router, health) = install_static(Router::new(), ["acme.A"]);
        let app = router.into_axum_router();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let config = ClientConfig::new(format!("http://{addr}").parse().unwrap());
        let client = HealthClient::new(HttpClient::plaintext(), config);

        // Default: Serving.
        let resp = client
            .check(HealthCheckRequest {
                service: "acme.A".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.view().status, ServingStatus::SERVING);

        // Flip via the returned handle.
        health.set_status("acme.A", Status::NotServing).unwrap();
        let resp = client
            .check(HealthCheckRequest {
                service: "acme.A".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.view().status, ServingStatus::NOT_SERVING);

        // `install_static` holds both routes to MAX_REQUEST_BYTES: an
        // oversized request is refused before it reaches the checker. Over
        // HTTP/2, so the refused request's stream is reset on its own rather
        // than closing an HTTP/1.1 connection the next call might reuse.
        let client = HealthClient::new(
            HttpClient::plaintext_http2_only(),
            ClientConfig::new(format!("http://{addr}").parse().unwrap()),
        );
        let oversized = HealthCheckRequest {
            service: "x".repeat(2 * crate::MAX_REQUEST_BYTES),
            ..Default::default()
        };
        let err = client.check(oversized.clone()).await.unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::ResourceExhausted);
        let mut stream = client.watch(oversized).await.unwrap();
        let err = stream.message().await.unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::ResourceExhausted);
    }

    /// An integrator's own `apply_request_limits` after `install_static`
    /// replaces the bundled profile: tightened to 1 KiB, a 2 KiB request the
    /// default 16 KiB would serve is refused.
    #[tokio::test]
    async fn integrator_limits_replace_the_bundled_profile() {
        use crate::{apply_request_limits, install_static};
        let (router, _health) = install_static(Router::new(), ["acme.A"]);
        let router = apply_request_limits(
            router,
            connectrpc::Limits::default().with_max_message_size(1024),
        );
        let app = router.into_axum_router();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = HealthClient::new(
            HttpClient::plaintext_http2_only(),
            ClientConfig::new(format!("http://{addr}").parse().unwrap()),
        );
        let err = client
            .check(HealthCheckRequest {
                service: "x".repeat(2048),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::ResourceExhausted);
    }

    /// The largest name the bundled profile admits decodes under its decode
    /// budget and misses with a `not_found` whose message is a small
    /// fraction of the request, over both the header-borne (gRPC) and
    /// body-borne (Connect) error paths.
    #[tokio::test]
    async fn largest_admitted_name_misses_with_a_bounded_error() {
        use crate::install_static;
        let (router, _health) = install_static(Router::new(), ["acme.A"]);
        let app = router.into_axum_router();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        // 8 bytes of slack covers the field tag and length varint, so the
        // encoded message sits just under MAX_REQUEST_BYTES.
        let request = HealthCheckRequest {
            service: "x".repeat(crate::MAX_REQUEST_BYTES - 8),
            ..Default::default()
        };
        let config = || ClientConfig::new(format!("http://{addr}").parse().unwrap());
        for (transport, config) in [
            (
                HttpClient::plaintext_http2_only(),
                config().with_protocol(connectrpc::Protocol::Grpc),
            ),
            (HttpClient::plaintext(), config()),
        ] {
            let client = HealthClient::new(transport, config);
            let err = client.check(request.clone()).await.unwrap_err();
            assert_eq!(err.code, connectrpc::ErrorCode::NotFound, "{err}");
            let message = err.message.unwrap();
            assert!(message.len() < 512, "{}", message.len());
            assert!(
                message.ends_with(&format!(
                    "[name truncated, {} bytes]",
                    request.service.len()
                )),
                "{message}"
            );
        }
    }

    #[tokio::test]
    async fn watch_unimplemented_when_checker_does_not_support_it() {
        struct CheckOnly;
        impl Checker for CheckOnly {
            async fn check(&self, _service: &str) -> Result<Status, ConnectError> {
                Ok(Status::Serving)
            }
            // No watch override → default returns Unimplemented.
        }
        let svc = Arc::new(HealthService::new(CheckOnly));
        let router = svc.register(Router::new());
        let app = router.into_axum_router();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let config = ClientConfig::new(format!("http://{addr}").parse().unwrap());
        let client: HealthClient<HttpClient> = HealthClient::new(HttpClient::plaintext(), config);

        let mut stream = client.watch(HealthCheckRequest::default()).await.unwrap();
        // Server-streaming RPCs surface trailer-borne errors directly from
        // `message()` — `Ok(None)` is reserved for a clean end.
        let err = stream
            .message()
            .await
            .expect_err("expected Unimplemented error");
        assert_eq!(err.code, connectrpc::ErrorCode::Unimplemented);
    }
}
