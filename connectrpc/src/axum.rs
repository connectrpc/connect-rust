//! `axum::serve` counterparts that run an `axum::Router` on connectrpc's
//! connection driver.
//!
//! [`Router::into_axum_service`](crate::Router::into_axum_service) and
//! [`Router::into_axum_router`](crate::Router::into_axum_router) mount
//! ConnectRPC routes on an `axum::Router`; `axum::serve` can host the result.
//! [`serve`] and `serve_tls` host it on the same accept loop and
//! [connection driver](crate::server::serve_connection) as the standalone
//! [`Server`](crate::Server) instead, so an axum app gets everything that
//! server has and `axum::serve` does not: TLS termination with
//! [`PeerAddr`] / `PeerCerts` on every request, every [`ConnectionConfig`]
//! setting (header-read timeout, HTTP/2 keepalive and flow control, max
//! connection age / idle / requests), graceful GOAWAY on shutdown, and panic
//! isolation. Handler code that reads `ctx.peer_addr()` / `ctx.peer_certs()`
//! is then portable between the standalone `Server` and an axum app.
//!
//! ```rust,ignore
//! // Plaintext, with connectrpc's connection lifecycle.
//! connectrpc::axum::serve(listener, app)
//!     .with_connection_config(ConnectionConfig::new().with_max_connection_age(Duration::from_secs(600)))
//!     .await?;
//!
//! // TLS, with PeerAddr / PeerCerts on every request.
//! connectrpc::axum::serve_tls(listener, app, tls_config).await?;
//! ```
//!
//! # Differences from `axum::serve`
//!
//! - **Service type.** [`serve`] takes a concrete `axum::Router`, not the
//!   make-service forms `axum::serve` is generic over. There is no
//!   `into_make_service_with_connect_info::<SocketAddr>()` equivalent because
//!   [`PeerAddr`] (the same socket address) is already in the request
//!   extensions; read that instead of `ConnectInfo<SocketAddr>`. A
//!   `Router<S>` with state must have `.with_state(...)` applied first.
//! - **`PeerCerts` is conditional.** It is only inserted when the
//!   `rustls::ServerConfig` requests client authentication *and* the peer
//!   presents a chain rustls verifies. Handlers must treat `ctx.peer_certs()`
//!   as optional.
//! - **ALPN.** The TLS terminator speaks the protocol ALPN selects. To allow
//!   HTTP/2 (required for gRPC; preferred for Connect streaming), set
//!   `server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()]`
//!   before passing it in. Without ALPN, hyper falls back to HTTP/1.1.
//! - **Panics are caught.** A handler that panics before returning its
//!   response yields a `500` whose body is a Connect-JSON `internal` error
//!   (for non-RPC routes too) and the connection survives; `axum::serve`
//!   drops the connection (HTTP/1.1) or resets the stream (HTTP/2). A panic
//!   while a response body is produced is logged and resets that stream with
//!   `INTERNAL_ERROR` (HTTP/2; `axum::serve` resets with `CANCEL`) or closes
//!   the connection (HTTP/1.1). A `CatchPanicLayer` of your own still takes
//!   precedence for the routes it wraps.
//! - **Idle reaping ends HTTP/2 WebSockets.** With
//!   [`ConnectionConfig::with_max_connection_idle`](crate::server::ConnectionConfig::with_max_connection_idle)
//!   set, a connection that carries only extended-CONNECT streams counts as
//!   idle and is closed; see that method.
//! - **Connections are owned by the future.** Dropping (or timing out) the
//!   [`Serve`] future aborts the connections it accepted rather than leaving
//!   them running detached.
//!
//! Available with the `axum` and `server` features; `serve_tls` additionally
//! needs `server-tls`.
//!
//! [`PeerAddr`]: crate::PeerAddr
//! [`ConnectionConfig`]: crate::server::ConnectionConfig

use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;
#[cfg(feature = "server-tls")]
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;

use crate::server::AcceptConfig;
use crate::server::ConnectionConfig;
use crate::server::ConnectionExtensionsFn;
use crate::server::ConnectionInfo;
use crate::server::serve_with_listener;

/// Serve an `axum::Router` over plaintext TCP on connectrpc's connection
/// driver: [`PeerAddr`](crate::PeerAddr) on every request, every
/// [`ConnectionConfig`] setting, graceful shutdown, panic isolation.
///
/// See the [module docs](self) for what this adds over `axum::serve`. After
/// running out of file descriptors (`EMFILE` / `ENFILE`, or `WSAEMFILE` on
/// Windows) the loop pauses accepts for up to a second, or until the
/// shutdown signal fires.
///
/// # Errors
///
/// The future resolves to `Err` only for an accept error that is neither
/// transient nor file-descriptor exhaustion, as for
/// [`BoundServer::serve`](crate::BoundServer::serve). Per-connection
/// failures are logged and never end the loop.
pub fn serve(listener: TcpListener, router: axum::Router) -> Serve {
    Serve {
        listener,
        router,
        accept: AcceptConfig::default(),
        connection: ConnectionConfig::default(),
        connection_extensions: None,
        shutdown: None,
    }
}

/// Serve an `axum::Router` over TLS, exposing peer identity to handlers.
///
/// As [`serve`], plus TLS termination with `tls_config`: every request
/// carries [`PeerAddr`](crate::PeerAddr) and, when `tls_config` requests
/// client authentication and the peer presents a chain rustls verifies,
/// [`PeerCerts`](crate::server::PeerCerts). The handshake is bounded by
/// [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`](crate::server::DEFAULT_TLS_HANDSHAKE_TIMEOUT);
/// tune it with [`Serve::with_tls_handshake_timeout`].
///
/// ```rust,no_run
/// # use std::sync::Arc;
/// # async fn demo(connect_router: connectrpc::Router, tls_config: Arc<rustls::ServerConfig>,
/// #     shutdown_signal: tokio::sync::oneshot::Receiver<()>)
/// #     -> Result<(), Box<dyn std::error::Error>> {
/// let app = axum::Router::new()
///     .route("/health", axum::routing::get(|| async { "OK" }))
///     .fallback_service(connect_router.into_axum_service());
///
/// let listener = tokio::net::TcpListener::bind("0.0.0.0:8443").await?;
/// connectrpc::axum::serve_tls(listener, app, tls_config)
///     .with_graceful_shutdown(async { shutdown_signal.await.ok(); })
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// As [`serve`]: only an accept error that is neither transient nor
/// file-descriptor exhaustion. TLS handshake failures and timeouts are
/// logged at `debug` / `warn` and never end the loop.
#[cfg(feature = "server-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
pub fn serve_tls(
    listener: TcpListener,
    router: axum::Router,
    tls_config: Arc<rustls::ServerConfig>,
) -> Serve {
    let mut serve = serve(listener, router);
    serve.accept.tls = Some(tls_config);
    serve
}

/// Configurable future returned by [`serve`] and `serve_tls`.
///
/// Mirrors the shape of `axum::serve::Serve`: tweak it with builder methods,
/// then `.await` it (or pass it anywhere an `IntoFuture` is accepted). It
/// resolves once the shutdown signal has fired and every connection has
/// drained, or on a fatal accept error.
#[must_use = "Serve does nothing unless `.await`ed"]
pub struct Serve {
    listener: TcpListener,
    router: axum::Router,
    accept: AcceptConfig,
    connection: ConnectionConfig,
    connection_extensions: Option<ConnectionExtensionsFn>,
    shutdown: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

/// The previous name of [`Serve`], from when only [`serve_tls`] existed.
#[cfg(feature = "server-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
#[deprecated(since = "0.10.0", note = "renamed to `Serve`; `serve_tls` returns it")]
pub type ServeTls = Serve;

impl Serve {
    /// Override the TLS handshake timeout (default
    /// [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`](crate::server::DEFAULT_TLS_HANDSHAKE_TIMEOUT)).
    /// Set generously; clients on high-latency links need a few round trips
    /// to complete the handshake. No effect on plaintext [`serve`].
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use = "Serve does nothing unless `.await`ed"]
    pub fn with_tls_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.accept.tls_handshake_timeout = Some(timeout);
        self
    }

    /// Override the header read timeout (default
    /// [`DEFAULT_HEADER_READ_TIMEOUT`](crate::server::DEFAULT_HEADER_READ_TIMEOUT);
    /// `None` or zero disables). Shorthand for
    /// [`ConnectionConfig::with_header_read_timeout`].
    #[must_use = "Serve does nothing unless `.await`ed"]
    pub fn with_header_read_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.connection = self.connection.with_header_read_timeout(timeout);
        self
    }

    /// Replace the per-connection settings wholesale — the same
    /// [`ConnectionConfig`] value the standalone `Server` accepts, so an axum
    /// app gets max connection age / idle / requests, HTTP/2 keepalive and
    /// flow-control tuning with no second implementation to drift from.
    #[must_use = "Serve does nothing unless `.await`ed"]
    pub fn with_connection_config(mut self, config: ConnectionConfig) -> Self {
        self.connection = config;
        self
    }

    /// Add request extensions computed once per accepted connection from its
    /// [`ConnectionInfo`], before its first request; same semantics as
    /// [`BoundServer::with_connection_extensions`](crate::BoundServer::with_connection_extensions).
    /// Calling this again replaces the function.
    #[must_use = "Serve does nothing unless `.await`ed"]
    pub fn with_connection_extensions<F>(mut self, f: F) -> Self
    where
        F: Fn(&ConnectionInfo, &mut http::Extensions) + Send + Sync + 'static,
    {
        self.connection_extensions = Some(ConnectionExtensionsFn::new(f));
        self
    }

    /// Stop accepting new connections when `signal` resolves and drain
    /// in-flight connections before the future resolves. Mirrors
    /// `axum::serve::Serve::with_graceful_shutdown`.
    #[must_use = "Serve does nothing unless `.await`ed"]
    pub fn with_graceful_shutdown<F>(mut self, signal: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.shutdown = Some(Box::pin(signal));
        self
    }
}

impl std::fmt::Debug for Serve {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Serve");
        s.field("listener", &self.listener);
        #[cfg(feature = "server-tls")]
        s.field("tls", &self.accept.tls.is_some()).field(
            "tls_handshake_timeout",
            &self.accept.tls_handshake_timeout(),
        );
        s.field("connection", &self.connection)
            .field(
                "connection_extensions",
                &self.connection_extensions.is_some(),
            )
            .field("shutdown", &self.shutdown.is_some())
            .finish_non_exhaustive()
    }
}

impl IntoFuture for Serve {
    type Output = std::io::Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        let Serve {
            listener,
            router,
            accept,
            connection,
            connection_extensions,
            shutdown,
        } = self;
        Box::pin(serve_with_listener(
            listener,
            router,
            accept,
            connection,
            connection_extensions,
            shutdown,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use std::sync::Arc;

    use crate::{Response as ConnectResponse, Router as ConnectRouter, handler_fn};
    #[cfg(feature = "server-tls")]
    use rcgen::{CertificateParams, CertifiedIssuer, IsCa, KeyPair, SanType};
    #[cfg(feature = "server-tls")]
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[cfg(feature = "server-tls")]
    type Pki = (
        Arc<rustls::ServerConfig>,
        Arc<rustls::ClientConfig>,
        CertificateDer<'static>,
    );

    /// Minimal in-memory mTLS PKI: one CA, one server leaf, one client leaf.
    /// Returns `(server_config, client_config, client_leaf_der)`.
    #[cfg(feature = "server-tls")]
    fn pki() -> Pki {
        // Idempotent; err == already installed (tests share process state).
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();

        let issue = |sans: &[SanType]| {
            let k = KeyPair::generate().unwrap();
            let mut p = CertificateParams::default();
            p.subject_alt_names = sans.to_vec();
            let c = p.signed_by(&k, &ca).unwrap();
            (
                CertificateDer::from(c.der().to_vec()),
                PrivatePkcs8KeyDer::from(k.serialized_der().to_vec()).into(),
            )
        };
        let (srv_cert, srv_key) = issue(&[SanType::DnsName("localhost".try_into().unwrap())]);
        let (cli_cert, cli_key) = issue(&[]);

        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from(ca.der().to_vec())).unwrap();
        let roots = Arc::new(roots);

        let cv = rustls::server::WebPkiClientVerifier::builder(Arc::clone(&roots))
            .build()
            .unwrap();
        let server = rustls::ServerConfig::builder()
            .with_client_cert_verifier(cv)
            .with_single_cert(vec![srv_cert], srv_key)
            .unwrap();
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(vec![cli_cert.clone()], cli_key)
            .unwrap();
        (Arc::new(server), Arc::new(client), cli_cert)
    }

    /// HTTP/1.1 Connect unary request matching `server.rs`'s fixture.
    const ECHO_REQ: &[u8] = b"POST /svc/Echo HTTP/1.1\r\n\
        Host: localhost\r\n\
        Content-Type: application/proto\r\n\
        Content-Length: 0\r\n\
        Connection: close\r\n\
        \r\n";

    /// Plaintext `serve` stamps `PeerAddr` too — something `axum::serve`
    /// needs `into_make_service_with_connect_info` for.
    #[tokio::test]
    async fn serve_plaintext_injects_peer_addr() {
        let captured: Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));
        let handler_captured = Arc::clone(&captured);
        let connect = ConnectRouter::new().route(
            "svc",
            "Echo",
            handler_fn(
                move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let cap = Arc::clone(&handler_captured);
                    async move {
                        *cap.lock().unwrap() = ctx.peer_addr();
                        ConnectResponse::ok(buffa_types::Empty::default())
                    }
                },
            ),
        );
        let app = axum::Router::new().fallback_service(connect.into_axum_service());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            serve(listener, app)
                .with_graceful_shutdown(async {
                    rx.await.ok();
                })
                .into_future(),
        );

        let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let client = tcp.local_addr().unwrap();
        tcp.write_all(ECHO_REQ).await.unwrap();
        let mut resp = Vec::new();
        tcp.read_to_end(&mut resp).await.unwrap();
        assert!(resp.starts_with(b"HTTP/1.1 2"));

        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(captured.lock().unwrap().take(), Some(client));
    }

    /// `ConnectionConfig` settings apply to axum apps: max connection age
    /// retires an h2 connection.
    #[tokio::test(start_paused = true)]
    async fn serve_honours_connection_config_max_age() {
        let app = axum::Router::new().route("/ok", axum::routing::get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            serve(listener, app)
                .with_connection_config(
                    ConnectionConfig::new()
                        .with_max_connection_age(Duration::from_secs(10))
                        .with_max_connection_age_grace(Duration::from_secs(1)),
                )
                .with_graceful_shutdown(async {
                    rx.await.ok();
                })
                .into_future(),
        );

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);
        let req = http::Request::get(format!("http://{addr}/ok"))
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        assert_eq!(resp.await.unwrap().status(), http::StatusCode::OK);

        tokio::time::advance(Duration::from_secs(12)).await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        assert!(h2_task.is_finished(), "aged connection was not retired");
        if let Err(err) = h2_task.await.unwrap() {
            assert!(err.is_go_away(), "{err:?}");
        }

        drop(send_request);
        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    /// A panicking axum handler costs its request, not the connection.
    #[tokio::test]
    async fn panicking_handler_yields_500_and_connection_survives() {
        let app = axum::Router::new()
            .route(
                "/panic",
                axum::routing::get(|| async {
                    if true {
                        panic!("handler bug");
                    }
                    ""
                }),
            )
            .route("/ok", axum::routing::get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            serve(listener, app)
                .with_graceful_shutdown(async {
                    rx.await.ok();
                })
                .into_future(),
        );

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(h2_conn);
        let get = |path: &str| {
            http::Request::get(format!("http://{addr}{path}"))
                .body(())
                .unwrap()
        };
        let (resp, _) = send_request.send_request(get("/panic"), true).unwrap();
        let resp = resp
            .await
            .expect("panic must not kill the stream's connection");
        assert_eq!(resp.status(), http::StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            resp.headers().get(http::header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let (resp, _) = send_request.send_request(get("/ok"), true).unwrap();
        assert_eq!(
            resp.await.unwrap().status(),
            http::StatusCode::OK,
            "same connection keeps serving after a panic"
        );

        drop(send_request);
        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    /// The free `serve_connection` hosts an `axum::Router` a custom loop
    /// accepted for, with `PeerAddr` from the `ConnectionInfo` it was handed
    /// and a close reason.
    #[tokio::test]
    async fn serve_connection_hosts_an_axum_router() {
        use crate::server::{CloseReason, ConnectionInfo, serve_connection};

        let captured: Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));
        let handler_captured = Arc::clone(&captured);
        let connect = ConnectRouter::new().route(
            "svc",
            "Echo",
            handler_fn(
                move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let cap = Arc::clone(&handler_captured);
                    async move {
                        *cap.lock().unwrap() = ctx.peer_addr();
                        ConnectResponse::ok(buffa_types::Empty::default())
                    }
                },
            ),
        );
        let app = axum::Router::new()
            .route("/health", axum::routing::get(|| async { "up" }))
            .fallback_service(connect.into_axum_service());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let conn = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            let info = ConnectionInfo::new().with_peer_addr(peer);
            serve_connection(
                stream,
                info,
                app,
                ConnectionConfig::new(),
                std::future::pending(),
            )
            .await
        });

        let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let client = tcp.local_addr().unwrap();
        tcp.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 512];
        let n = tcp.read(&mut buf).await.unwrap();
        assert!(buf[..n].starts_with(b"HTTP/1.1 200"));
        assert!(buf[..n].ends_with(b"up"));
        tcp.write_all(ECHO_REQ).await.unwrap();
        let mut resp = Vec::new();
        tcp.read_to_end(&mut resp).await.unwrap();
        assert!(resp.starts_with(b"HTTP/1.1 2"));

        let closed = tokio::time::timeout(Duration::from_secs(5), conn)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(closed.reason(), CloseReason::Closed);
        assert_eq!(captured.lock().unwrap().take(), Some(client));
    }

    #[cfg(feature = "server-tls")]
    #[tokio::test]
    async fn serve_tls_injects_peer_identity() {
        let (server_cfg, client_cfg, expected_client_der) = pki();

        // The handler stashes whatever peer identity it sees via the typed
        // `RequestContext` accessors.
        type CapturedCerts = Vec<rustls::pki_types::CertificateDer<'static>>;
        type Captured = Arc<Mutex<Option<(std::net::SocketAddr, Option<CapturedCerts>)>>>;
        let captured: Captured = Arc::new(Mutex::new(None));
        let handler_captured = Arc::clone(&captured);
        let connect = ConnectRouter::new().route(
            "svc",
            "Echo",
            handler_fn(
                move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let cap = Arc::clone(&handler_captured);
                    async move {
                        *cap.lock().unwrap() = Some((
                            ctx.peer_addr().expect("serve_tls inserts PeerAddr"),
                            ctx.peer_certs().map(<[_]>::to_vec),
                        ));
                        ConnectResponse::ok(buffa_types::Empty::default())
                    }
                },
            ),
        );
        let app = axum::Router::new().fallback_service(connect.into_axum_service());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            serve_tls(listener, app, server_cfg)
                .with_graceful_shutdown(async {
                    rx.await.ok();
                })
                .into_future(),
        );

        let resp = echo_over_tls(addr, client_cfg).await;
        assert!(
            resp.starts_with(b"HTTP/1.1 2"),
            "expected 2xx, got: {}",
            String::from_utf8_lossy(&resp[..resp.len().min(120)])
        );

        // Graceful shutdown should drain and resolve the serve task.
        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("serve should shut down within timeout")
            .unwrap()
            .unwrap();

        let (peer_addr, peer_certs) = captured.lock().unwrap().take().expect("handler ran");
        assert_eq!(peer_addr.ip(), addr.ip());
        let certs = peer_certs.expect("mTLS client should present a cert chain");
        assert_eq!(certs.len(), 1);
        assert_eq!(certs[0].as_ref(), expected_client_der.as_ref());
    }

    /// The `with_connection_extensions` function runs once per connection
    /// through `serve_tls`, sees the verified client chain, and what it
    /// inserts reaches the handler next to (not instead of) `PeerAddr` /
    /// `PeerCerts`.
    #[cfg(feature = "server-tls")]
    #[tokio::test]
    async fn serve_tls_connection_extensions_reach_handler() {
        let (server_cfg, client_cfg, expected_client_der) = pki();

        #[derive(Clone, Debug, PartialEq)]
        struct LeafLen(usize);

        type Captured = Arc<Mutex<Option<(Option<LeafLen>, std::net::SocketAddr, Option<usize>)>>>;
        let captured: Captured = Arc::new(Mutex::new(None));
        let handler_captured = Arc::clone(&captured);
        let connect = ConnectRouter::new().route(
            "svc",
            "Echo",
            handler_fn(
                move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let cap = Arc::clone(&handler_captured);
                    async move {
                        *cap.lock().unwrap() = Some((
                            ctx.extensions().get::<LeafLen>().cloned(),
                            ctx.peer_addr().expect("serve_tls inserts PeerAddr"),
                            ctx.peer_certs().map(|c| c[0].as_ref().len()),
                        ));
                        ConnectResponse::ok(buffa_types::Empty::default())
                    }
                },
            ),
        );
        let app = axum::Router::new().fallback_service(connect.into_axum_service());

        let calls_seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = Arc::clone(&calls_seen);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            serve_tls(listener, app, server_cfg)
                .with_connection_extensions(move |conn, ext| {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let leaf_len = conn
                        .peer_certs()
                        .and_then(<[_]>::first)
                        .map(|l| l.as_ref().len());
                    if let Some(len) = leaf_len {
                        ext.insert(LeafLen(len));
                    }
                    // The transport's `PeerAddr` must win over this one.
                    ext.insert(crate::PeerAddr("10.0.0.1:1".parse().unwrap()));
                })
                .with_graceful_shutdown(async {
                    rx.await.ok();
                })
                .into_future(),
        );

        let resp = echo_over_tls(addr, client_cfg).await;
        assert!(
            resp.starts_with(b"HTTP/1.1 2"),
            "expected 2xx, got: {}",
            String::from_utf8_lossy(&resp[..resp.len().min(120)])
        );

        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("serve should shut down within timeout")
            .unwrap()
            .unwrap();

        assert_eq!(calls_seen.load(std::sync::atomic::Ordering::SeqCst), 1);
        let (leaf_len, peer_addr, handler_len) =
            captured.lock().unwrap().take().expect("handler ran");
        let expected = expected_client_der.as_ref().len();
        assert_eq!(leaf_len, Some(LeafLen(expected)));
        assert_eq!(handler_len, Some(expected));
        assert_eq!(peer_addr.ip(), addr.ip());
    }

    /// Open a TLS+HTTP/1.1 connection, send `ECHO_REQ`, and return the raw
    /// HTTP response bytes.
    #[cfg(feature = "server-tls")]
    async fn echo_over_tls(
        addr: std::net::SocketAddr,
        client_cfg: Arc<rustls::ClientConfig>,
    ) -> Vec<u8> {
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let connector = tokio_rustls::TlsConnector::from(client_cfg);
        let sni = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(sni, tcp).await.unwrap();
        tls.write_all(ECHO_REQ).await.unwrap();
        let mut resp = Vec::new();
        tls.read_to_end(&mut resp).await.unwrap();
        resp
    }

    #[cfg(feature = "server-tls")]
    #[tokio::test]
    async fn handshake_timeout_drops_stalled_connection() {
        let (server_cfg, _, _) = pki();
        let app = axum::Router::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            serve_tls(listener, app, server_cfg)
                .with_tls_handshake_timeout(Duration::from_millis(100))
                .with_graceful_shutdown(async {
                    rx.await.ok();
                })
                .into_future(),
        );

        // Open TCP but never speak TLS, and keep it open through shutdown.
        // If the handshake timeout doesn't release this connection's watcher,
        // the graceful drain blocks until the outer timeout fails the test.
        let _stalled = tokio::net::TcpStream::connect(addr).await.unwrap();
        // Generous margin so the accept loop spawns the per-connection task
        // (and its watcher) before we signal shutdown — otherwise the test
        // passes vacuously without exercising the timeout path.
        tokio::time::sleep(Duration::from_millis(250)).await;

        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("handshake timeout must release the watcher so drain completes")
            .unwrap()
            .unwrap();
    }

    #[cfg(feature = "server-tls")]
    #[tokio::test]
    async fn header_read_timeout_closes_stalled_connection() {
        let (server_cfg, client_cfg, _) = pki();
        let app = axum::Router::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            serve_tls(listener, app, server_cfg)
                .with_header_read_timeout(Some(Duration::from_millis(150)))
                .with_graceful_shutdown(async {
                    rx.await.ok();
                })
                .into_future(),
        );

        // Complete the TLS handshake, then send a partial HTTP/1.1 request whose
        // header block never terminates, so hyper stays in "reading headers".
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let connector = tokio_rustls::TlsConnector::from(client_cfg);
        let sni = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(sni, tcp).await.unwrap();
        tls.write_all(b"POST /svc/Echo HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .unwrap();

        // The header read timeout must close the connection. `read_to_end`
        // resolves on close; if the timeout were not wired, it would hang until
        // the outer guard fails the test. The server tears the connection down
        // abruptly when the timeout fires, so rustls reports an `UnexpectedEof`
        // rather than a clean close — either outcome confirms closure.
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), tls.read_to_end(&mut buf))
            .await
            .expect("server did not close the stalled connection");
        assert!(
            !buf.starts_with(b"HTTP/1.1 2"),
            "stalled request should not have been served: {}",
            String::from_utf8_lossy(&buf[..buf.len().min(80)])
        );

        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("serve should shut down")
            .unwrap()
            .unwrap();
    }

    #[cfg(feature = "server-tls")]
    #[tokio::test]
    async fn handshake_error_does_not_kill_accept_loop() {
        let (server_cfg, client_cfg, _) = pki();
        let calls = Arc::new(Mutex::new(0u32));
        let handler_calls = Arc::clone(&calls);
        let connect = ConnectRouter::new().route(
            "svc",
            "Echo",
            handler_fn(
                move |_ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let calls = Arc::clone(&handler_calls);
                    async move {
                        *calls.lock().unwrap() += 1;
                        ConnectResponse::ok(buffa_types::Empty::default())
                    }
                },
            ),
        );
        let app = axum::Router::new().fallback_service(connect.into_axum_service());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            serve_tls(listener, app, server_cfg)
                .with_graceful_shutdown(async {
                    rx.await.ok();
                })
                .into_future(),
        );

        // Speak garbage instead of a ClientHello: the rustls handshake fails
        // immediately. The accept loop must log-and-continue, not propagate.
        let mut bad = tokio::net::TcpStream::connect(addr).await.unwrap();
        bad.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        let mut buf = [0u8; 64];
        let _ = bad.read(&mut buf).await; // server closes / sends a TLS alert
        drop(bad);

        // A valid client must still get through.
        let resp = echo_over_tls(addr, client_cfg).await;
        assert!(
            resp.starts_with(b"HTTP/1.1 2"),
            "valid client must succeed after a handshake error: {}",
            String::from_utf8_lossy(&resp[..resp.len().min(120)])
        );

        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            *calls.lock().unwrap(),
            1,
            "only the valid client reaches the handler"
        );
    }

    #[cfg(feature = "server-tls")]
    #[tokio::test]
    async fn graceful_shutdown_drains_in_flight_request() {
        let (server_cfg, client_cfg, _) = pki();

        // The handler blocks until the test releases it; this lets us pin a
        // request as "in-flight" across the shutdown signal.
        let (in_flight_tx, in_flight_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let in_flight_tx = Arc::new(Mutex::new(Some(in_flight_tx)));
        let release_rx = Arc::new(Mutex::new(Some(release_rx)));
        let connect = ConnectRouter::new().route(
            "svc",
            "Echo",
            handler_fn(
                move |_ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let in_flight = in_flight_tx.lock().unwrap().take();
                    let release = release_rx.lock().unwrap().take();
                    async move {
                        if let Some(tx) = in_flight {
                            tx.send(()).ok();
                        }
                        if let Some(rx) = release {
                            rx.await.ok();
                        }
                        ConnectResponse::ok(buffa_types::Empty::default())
                    }
                },
            ),
        );
        let app = axum::Router::new().fallback_service(connect.into_axum_service());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            serve_tls(listener, app, server_cfg)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.ok();
                })
                .into_future(),
        );

        let client = tokio::spawn(echo_over_tls(addr, client_cfg));

        // Once the request is in-flight, signal shutdown. The watcher held by
        // the per-connection task must anchor it until the handler returns.
        in_flight_rx.await.unwrap();
        shutdown_tx.send(()).unwrap();

        // Release the handler: the in-flight request must complete cleanly
        // (proving the connection wasn't torn down by the shutdown), and only
        // then should the serve future drain.
        release_tx.send(()).unwrap();
        let resp = tokio::time::timeout(Duration::from_secs(5), client)
            .await
            .expect("in-flight request should complete during drain")
            .unwrap();
        assert!(
            resp.starts_with(b"HTTP/1.1 2"),
            "in-flight request must complete: {}",
            String::from_utf8_lossy(&resp[..resp.len().min(120)])
        );
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("serve should drain after the in-flight request completes")
            .unwrap()
            .unwrap();
    }
}
