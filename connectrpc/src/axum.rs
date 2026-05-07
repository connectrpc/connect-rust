//! TLS-aware `axum::serve` counterpart that exposes peer identity to handlers.
//!
//! [`Router::into_axum_service`](crate::Router::into_axum_service) and
//! [`Router::into_axum_router`](crate::Router::into_axum_router) cover the
//! plaintext path: mount your ConnectRPC routes on an `axum::Router` and
//! hand the result to `axum::serve`. This module fills the TLS gap.
//!
//! `axum::serve` accepts a plain [`TcpListener`] and has no hook for
//! terminating TLS. The standalone [`Server`](crate::Server), by contrast,
//! owns the rustls accept loop and so can capture [`PeerAddr`]/[`PeerCerts`]
//! once per connection and inject them into every request's extensions for
//! handlers to read via `ctx.extensions.get::<T>()`. Without help, an axum +
//! mTLS deployment has to reimplement that accept loop and per-connection
//! plumbing by hand for handlers to get the same view.
//!
//! [`serve_tls`] is that help: it serves an `axum::Router`, terminates TLS,
//! captures peer identity, and stamps it into request extensions. Handler
//! code that reads `ctx.extensions.get::<PeerCerts>()` is then portable
//! between the standalone `Server` and an axum app — the hosting choice no
//! longer leaks into your authorization logic.
//!
//! ```rust,ignore
//! // Plaintext: axum's built-in serve.
//! axum::serve(listener, app).await?;
//!
//! // TLS with PeerAddr/PeerCerts passthrough.
//! connectrpc::axum::serve_tls(listener, app, tls_config).await?;
//! ```
//!
//! # Differences from `axum::serve`
//!
//! `serve_tls` is the TLS counterpart to `axum::serve(listener, router)` for
//! the common `axum::Router` case. It is intentionally less generic:
//!
//! - **Service type.** `serve_tls` accepts a concrete `axum::Router`, not
//!   the make-service forms `axum::serve` is generic over. There is no
//!   `into_make_service_with_connect_info::<SocketAddr>()` equivalent because
//!   `serve_tls` already injects [`PeerAddr`] (the same socket address) into
//!   request extensions; read that instead of `ConnectInfo<SocketAddr>`.
//!   A `Router<S>` with state must have `.with_state(...)` applied first.
//! - **`PeerCerts` is conditional.** It is only inserted when the
//!   [`rustls::ServerConfig`] requests client authentication *and* the peer
//!   presents a chain rustls verifies. With `with_no_client_auth()` (or a
//!   permissive verifier and a client that sends no cert), only [`PeerAddr`]
//!   is present. Handlers must treat `ctx.extensions.get::<PeerCerts>()` as
//!   optional.
//! - **ALPN.** The TLS terminator speaks the protocol ALPN selects. To allow
//!   HTTP/2 (required for gRPC; preferred for Connect streaming), set
//!   `server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()]`
//!   before passing it in. Without ALPN, hyper falls back to HTTP/1.1.
//! - **No automatic panic catching.** Unlike the standalone
//!   [`Server`](crate::Server), `serve_tls` does not wrap your `axum::Router`
//!   in `tower_http::catch_panic::CatchPanicLayer` (`axum::serve` doesn't
//!   either). If you want a panicking handler to surface as a Connect error
//!   rather than a dropped connection, add the layer yourself.
//!
//! Available only with both the `axum` and `server-tls` features enabled.

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tower::ServiceExt;

use crate::server::{
    DEFAULT_TLS_HANDSHAKE_TIMEOUT, PeerAddr, PeerCerts, is_transient_accept_error,
};

/// Serve an `axum::Router` over TLS, exposing peer identity to handlers.
///
/// The TLS counterpart to `axum::serve(listener, router)` for when handlers
/// need [`PeerAddr`] and [`PeerCerts`] in request extensions — the same
/// convention the standalone [`Server::with_tls`](crate::Server::with_tls)
/// uses. The accept loop terminates TLS with `tokio-rustls`, captures the
/// remote address and any verified client certificate chain, then injects
/// both into every request before handing off to the axum service.
/// [`PeerCerts`] is only present when `tls_config` requests client
/// authentication and the peer presented a chain rustls verified.
///
/// Like the standalone server, the TLS handshake is bounded by a
/// [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`] to prevent slowloris-style connection
/// exhaustion; tune it with [`ServeTls::tls_handshake_timeout`].
///
/// The returned [`ServeTls`] resolves once the listener stops accepting and
/// in-flight connections drain (after [`ServeTls::with_graceful_shutdown`]'s
/// signal fires) or when a non-transient accept error occurs.
///
/// See the [module docs](self) for the differences from `axum::serve`,
/// including ALPN setup and panic-handling expectations.
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
/// The future resolves to `Err` only for non-transient I/O errors from the
/// underlying `accept(2)` (for example, file-descriptor exhaustion that
/// persists past `EMFILE`/`ENFILE` retries, or a closed listener). Per-peer
/// failures — TLS handshake errors, handshake timeouts, and HTTP-layer errors
/// on a single connection — are logged at `debug`/`warn`/`trace` and never
/// abort the accept loop.
pub fn serve_tls(
    listener: TcpListener,
    router: axum::Router,
    tls_config: Arc<rustls::ServerConfig>,
) -> ServeTls {
    ServeTls {
        listener,
        router,
        acceptor: tokio_rustls::TlsAcceptor::from(tls_config),
        tls_handshake_timeout: DEFAULT_TLS_HANDSHAKE_TIMEOUT,
        shutdown: None,
    }
}

/// Configurable future returned by [`serve_tls`].
///
/// Mirrors the shape of `axum::serve::Serve`: tweak it with builder
/// methods, then `.await` it (or pass it anywhere an `IntoFuture` is
/// accepted).
#[must_use = "ServeTls does nothing unless `.await`ed"]
pub struct ServeTls {
    listener: TcpListener,
    router: axum::Router,
    acceptor: tokio_rustls::TlsAcceptor,
    tls_handshake_timeout: Duration,
    shutdown: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl ServeTls {
    /// Override the TLS handshake timeout (default
    /// [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`]). Set generously; clients on
    /// high-latency links need a few round trips to complete the handshake.
    #[must_use = "ServeTls does nothing unless `.await`ed"]
    pub fn tls_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.tls_handshake_timeout = timeout;
        self
    }

    /// Stop accepting new connections when `signal` resolves and drain
    /// in-flight connections before the future returned by [`serve_tls`]
    /// resolves. Mirrors `axum::serve::Serve::with_graceful_shutdown`.
    #[must_use = "ServeTls does nothing unless `.await`ed"]
    pub fn with_graceful_shutdown<F>(mut self, signal: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.shutdown = Some(Box::pin(signal));
        self
    }
}

impl std::fmt::Debug for ServeTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServeTls")
            .field("listener", &self.listener)
            .field("tls_handshake_timeout", &self.tls_handshake_timeout)
            .field("shutdown", &self.shutdown.is_some())
            .finish_non_exhaustive()
    }
}

impl IntoFuture for ServeTls {
    type Output = std::io::Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
}

impl ServeTls {
    async fn run(self) -> std::io::Result<()> {
        let ServeTls {
            listener,
            router,
            acceptor,
            tls_handshake_timeout,
            shutdown,
        } = self;

        // `select!` needs a polled-in-place future for both arms; default to
        // a never-resolving signal when no graceful shutdown is configured.
        let mut shutdown = shutdown.unwrap_or_else(|| Box::pin(std::future::pending()));
        let graceful = GracefulShutdown::new();

        loop {
            let (stream, remote_addr) = tokio::select! {
                biased; // honor shutdown before another accept

                _ = &mut shutdown => {
                    tracing::info!("Shutdown signal received; draining connections");
                    break;
                }
                accepted = listener.accept() => match accepted {
                    Ok(conn) => conn,
                    Err(err) if is_transient_accept_error(&err) => {
                        tracing::warn!("Transient accept error (continuing): {err}");
                        continue;
                    }
                    Err(err) => return Err(err),
                },
            };

            // Same TCP_NODELAY rationale as the standalone Server: avoid
            // Nagle/delayed-ACK interaction on small HTTP/2 control frames.
            if let Err(e) = stream.set_nodelay(true) {
                tracing::warn!("failed to set TCP_NODELAY: {e}");
            }

            let acceptor = acceptor.clone();
            let router = router.clone();
            let watcher = graceful.watcher();

            tokio::spawn(async move {
                let tls_stream = match tokio::time::timeout(
                    tls_handshake_timeout,
                    acceptor.accept(stream),
                )
                .await
                {
                    Ok(Ok(s)) => s,
                    Ok(Err(err)) => {
                        tracing::debug!(remote_addr = %remote_addr, error = ?err, "TLS handshake failed");
                        return;
                    }
                    Err(_) => {
                        tracing::warn!(
                            remote_addr = %remote_addr,
                            "TLS handshake timed out after {tls_handshake_timeout:?}",
                        );
                        return;
                    }
                };

                // Capture peer info now — once hyper owns the stream we can't
                // borrow it again. `into_owned()` detaches the cert bytes from
                // the session lifetime so the Arc can outlive the TlsStream.
                let (_, conn) = tls_stream.get_ref();
                let peer_addr = PeerAddr(remote_addr);
                let peer_certs = conn
                    .peer_certificates()
                    .map(|chain| PeerCerts(chain.iter().map(|c| c.clone().into_owned()).collect()));

                // Per-request: stamp peer info into extensions and forward to
                // the axum service. `Router::clone()` is an Arc bump.
                let svc = hyper::service::service_fn(
                    move |mut req: hyper::Request<hyper::body::Incoming>| {
                        req.extensions_mut().insert(peer_addr.clone());
                        if let Some(c) = &peer_certs {
                            req.extensions_mut().insert(c.clone());
                        }
                        router.clone().oneshot(req.map(axum::body::Body::new))
                    },
                );

                // `serve_connection_with_upgrades` (vs `serve_connection` on the
                // standalone `Server`) so axum routes that need HTTP `Upgrade:`
                // (WebSockets) work out of the box. ConnectRPC routes don't
                // upgrade, so this is a no-op for them. Keep this divergence —
                // it matches what `axum::serve` does internally.
                let conn = AutoBuilder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(TokioIo::new(tls_stream), svc)
                    .into_owned();
                if let Err(err) = watcher.watch(conn).await {
                    tracing::trace!(remote_addr = %remote_addr, error = %err, "Connection ended with error");
                }
            });
        }

        // Stop accepting before signalling the drain so no new connection
        // sneaks in between the watcher snapshot and the listener close.
        drop(listener);
        graceful.shutdown().await;
        tracing::info!("All connections drained; shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use crate::{Response as ConnectResponse, Router as ConnectRouter, handler_fn};
    use rcgen::{CertificateParams, CertifiedIssuer, IsCa, KeyPair, SanType};
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    type Pki = (
        Arc<rustls::ServerConfig>,
        Arc<rustls::ClientConfig>,
        CertificateDer<'static>,
    );

    /// Minimal in-memory mTLS PKI: one CA, one server leaf, one client leaf.
    /// Returns `(server_config, client_config, client_leaf_der)`.
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

    #[tokio::test]
    async fn serve_tls_injects_peer_identity() {
        let (server_cfg, client_cfg, expected_client_der) = pki();

        // The handler stashes whatever PeerAddr/PeerCerts it sees.
        type Captured = Arc<Mutex<Option<(PeerAddr, Option<PeerCerts>)>>>;
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
                            ctx.extensions.get::<PeerAddr>().cloned().unwrap(),
                            ctx.extensions.get::<PeerCerts>().cloned(),
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
        assert_eq!(peer_addr.0.ip(), addr.ip());
        let certs = peer_certs.expect("mTLS client should present a cert chain");
        assert_eq!(certs.0.len(), 1);
        assert_eq!(certs.0[0].as_ref(), expected_client_der.as_ref());
    }

    /// Open a TLS+HTTP/1.1 connection, send `ECHO_REQ`, and return the raw
    /// HTTP response bytes.
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

    #[tokio::test]
    async fn handshake_timeout_drops_stalled_connection() {
        let (server_cfg, _, _) = pki();
        let app = axum::Router::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(
            serve_tls(listener, app, server_cfg)
                .tls_handshake_timeout(Duration::from_millis(100))
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
