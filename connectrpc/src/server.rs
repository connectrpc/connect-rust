//! Hyper-based HTTP server for ConnectRPC.
//!
//! This module provides the HTTP server implementation that handles incoming
//! ConnectRPC requests and routes them to the appropriate handlers.
//!
//! # TLS Support
//!
//! When the `tls` feature is enabled, the server can be configured with a
//! [`rustls::ServerConfig`] to serve requests over TLS:
//!
//! ```rust,ignore
//! let tls_config = Arc::new(rustls::ServerConfig::builder()
//!     .with_no_client_auth()
//!     .with_single_cert(certs, key)?);
//!
//! Server::new(router)
//!     .with_tls(tls_config)
//!     .serve(addr).await?;
//! ```
//!
//! # Graceful Shutdown
//!
//! Use [`BoundServer::serve_with_graceful_shutdown`] to stop accepting new
//! connections when a signal future resolves, then drain in-flight connections
//! before returning:
//!
//! ```rust,ignore
//! let bound = Server::bind("127.0.0.1:8080").await?;
//! bound
//!     .serve_with_graceful_shutdown(router, async {
//!         tokio::signal::ctrl_c().await.ok();
//!     })
//!     .await?;
//! ```

use std::any::Any;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use http::Response;
use http::StatusCode;
use http::header;
use http_body_util::Full;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::server::graceful::Watcher;
use tokio::net::TcpListener;
use tower::Service;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;

use crate::codec::content_type;
use crate::dispatcher::Dispatcher;
use crate::error::ConnectError;
use crate::error::ErrorCode;
use crate::router::Router;
use crate::service::ConnectRpcService;

/// Remote socket address of the connected peer.
///
/// Inserted into every request's extensions by the built-in [`Server`]'s
/// accept loop and by `connectrpc::axum::serve_tls`. Handlers read it via
/// `ctx.extensions.get::<PeerAddr>()`.
///
/// Callers using a different HTTP stack (axum, raw hyper) in front of
/// [`ConnectRpcService`] can insert this same type
/// from a tower layer so handlers stay agnostic to the transport.
#[derive(Clone, Debug)]
pub struct PeerAddr(pub SocketAddr);

/// TLS client certificate chain presented by the peer (leaf first).
///
/// Inserted by the built-in [`Server`]'s TLS accept loop and by
/// `connectrpc::axum::serve_tls` when the [`rustls::ServerConfig`] requests
/// client authentication and the peer presents a valid chain. Absent on
/// plaintext connections or when the client presents no certificate.
/// Handlers read it via `ctx.extensions.get::<PeerCerts>()`.
///
/// The `Arc` makes per-request insertion cheap: all requests on a
/// connection share one chain, so this is a refcount bump, not a copy.
#[cfg(feature = "server-tls")]
#[derive(Clone, Debug)]
pub struct PeerCerts(pub Arc<[rustls::pki_types::CertificateDer<'static>]>);

/// Connection-scoped peer info captured once per accepted stream and
/// inserted into every request's extensions by [`PeerInfo::insert_into`].
#[derive(Clone, Debug)]
struct PeerInfo {
    addr: SocketAddr,
    #[cfg(feature = "server-tls")]
    certs: Option<Arc<[rustls::pki_types::CertificateDer<'static>]>>,
}

impl PeerInfo {
    /// Insert this connection's peer info as public extension types
    /// ([`PeerAddr`], [`PeerCerts`]) so handlers can read them via
    /// `ctx.extensions.get::<T>()`.
    fn insert_into(&self, ext: &mut http::Extensions) {
        ext.insert(PeerAddr(self.addr));
        #[cfg(feature = "server-tls")]
        if let Some(certs) = &self.certs {
            ext.insert(PeerCerts(Arc::clone(certs)));
        }
    }
}

/// Default TLS handshake timeout.
///
/// Bounds how long the server waits after TCP accept for a client to complete
/// the TLS handshake. Prevents slowloris-style connection-exhaustion attacks
/// where a client opens a TCP connection and stalls the handshake indefinitely,
/// holding a task and file descriptor per connection.
///
/// Override via [`Server::tls_handshake_timeout`] or
/// [`BoundServer::tls_handshake_timeout`].
#[cfg(feature = "server-tls")]
pub const DEFAULT_TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// ConnectRPC server built on hyper.
pub struct Server {
    service: ConnectRpcService,
    http1_keep_alive: bool,
    #[cfg(feature = "server-tls")]
    tls_config: Option<Arc<rustls::ServerConfig>>,
    #[cfg(feature = "server-tls")]
    tls_handshake_timeout: std::time::Duration,
}

impl Server {
    /// Create a new server with the given router.
    pub fn new(router: Router) -> Self {
        Self {
            service: ConnectRpcService::new(router),
            http1_keep_alive: true,
            #[cfg(feature = "server-tls")]
            tls_config: None,
            #[cfg(feature = "server-tls")]
            tls_handshake_timeout: DEFAULT_TLS_HANDSHAKE_TIMEOUT,
        }
    }

    /// Create a new server from an existing [`ConnectRpcService`].
    pub fn from_service(service: ConnectRpcService) -> Self {
        Self {
            service,
            http1_keep_alive: true,
            #[cfg(feature = "server-tls")]
            tls_config: None,
            #[cfg(feature = "server-tls")]
            tls_handshake_timeout: DEFAULT_TLS_HANDSHAKE_TIMEOUT,
        }
    }

    /// Enable TLS with the given rustls server configuration.
    ///
    /// The configuration controls all TLS behavior including certificate
    /// selection, client authentication, and protocol versions. For dynamic
    /// certificate rotation, use a [`rustls::server::ResolvesServerCert`]
    /// implementation in the config.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    ///
    /// let tls_config = Arc::new(rustls::ServerConfig::builder()
    ///     .with_no_client_auth()
    ///     .with_single_cert(certs, key)?);
    ///
    /// Server::new(router)
    ///     .with_tls(tls_config)
    ///     .serve(addr).await?;
    /// ```
    #[cfg(feature = "server-tls")]
    #[must_use]
    pub fn with_tls(mut self, config: Arc<rustls::ServerConfig>) -> Self {
        self.tls_config = Some(config);
        self
    }

    /// Set the TLS handshake timeout.
    ///
    /// Defaults to [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`] (10 seconds). A client
    /// that connects via TCP but does not complete the TLS handshake within
    /// this duration is disconnected.
    #[cfg(feature = "server-tls")]
    #[must_use]
    pub fn tls_handshake_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.tls_handshake_timeout = timeout;
        self
    }

    /// Get a reference to the underlying router.
    pub fn router(&self) -> &Router {
        self.service.dispatcher()
    }

    /// Bind and serve on the given address.
    ///
    /// This runs forever until the process is killed. For graceful shutdown,
    /// use [`Server::bind`] + [`BoundServer::serve_with_graceful_shutdown`].
    pub async fn serve(
        self,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(addr).await?;
        #[cfg(feature = "server-tls")]
        let tls_acceptor = self.tls_config.map(tokio_rustls::TlsAcceptor::from);
        #[cfg(not(feature = "server-tls"))]
        let tls_acceptor: Option<()> = None;

        let scheme = if tls_acceptor.is_some() {
            "https"
        } else {
            "http"
        };
        tracing::info!("ConnectRPC server listening on {scheme}://{addr}");

        serve_with_listener(
            listener,
            self.service,
            tls_acceptor,
            self.http1_keep_alive,
            #[cfg(feature = "server-tls")]
            self.tls_handshake_timeout,
            None,
        )
        .await
    }

    /// Bind to the given address and return a [`BoundServer`].
    ///
    /// Accepts anything implementing [`tokio::net::ToSocketAddrs`]:
    /// - `"127.0.0.1:8080"` — IPv4 loopback (safest default for dev)
    /// - `"[::1]:8080"` — IPv6 loopback
    /// - `"0.0.0.0:8080"` — all IPv4 interfaces (only for trusted networks)
    /// - `"[::]:8080"` — all IPv6 interfaces (on Linux, also accepts IPv4
    ///   via IPv4-mapped addresses by default)
    /// - `"localhost:8080"` — resolves via DNS/hosts (may yield v4, v6, or both)
    ///
    /// Wrap a pre-bound [`TcpListener`].
    ///
    /// Use this instead of [`Server::bind`] when you need to configure
    /// socket options before binding — e.g. `IPV6_V6ONLY=false` for
    /// dual-stack listening, `SO_REUSEPORT` for multi-process accept,
    /// or binding to a listener inherited from a parent process.
    #[must_use]
    pub fn from_listener(listener: TcpListener) -> BoundServer {
        BoundServer {
            listener,
            http1_keep_alive: true,
            #[cfg(feature = "server-tls")]
            tls_config: None,
            #[cfg(feature = "server-tls")]
            tls_handshake_timeout: DEFAULT_TLS_HANDSHAKE_TIMEOUT,
        }
    }

    /// When multiple addresses are returned (e.g. `localhost` resolving to
    /// both `::1` and `127.0.0.1`), the first that successfully binds is used.
    pub async fn bind(
        addr: impl tokio::net::ToSocketAddrs,
    ) -> Result<BoundServer, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(addr).await?;
        Ok(BoundServer {
            listener,
            http1_keep_alive: true,
            #[cfg(feature = "server-tls")]
            tls_config: None,
            #[cfg(feature = "server-tls")]
            tls_handshake_timeout: DEFAULT_TLS_HANDSHAKE_TIMEOUT,
        })
    }
}

/// A server that has been bound to an address but not yet started.
pub struct BoundServer {
    listener: TcpListener,
    http1_keep_alive: bool,
    #[cfg(feature = "server-tls")]
    tls_config: Option<Arc<rustls::ServerConfig>>,
    #[cfg(feature = "server-tls")]
    tls_handshake_timeout: std::time::Duration,
}

impl BoundServer {
    /// Get the local address the server is bound to.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Enable TLS with the given rustls server configuration.
    #[cfg(feature = "server-tls")]
    #[must_use]
    pub fn with_tls(mut self, config: Arc<rustls::ServerConfig>) -> Self {
        self.tls_config = Some(config);
        self
    }

    /// Set the TLS handshake timeout.
    ///
    /// Defaults to [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`] (10 seconds).
    #[cfg(feature = "server-tls")]
    #[must_use]
    pub fn tls_handshake_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.tls_handshake_timeout = timeout;
        self
    }

    /// Disable HTTP/1.1 keep-alive.
    ///
    /// When disabled, the server sends `Connection: close` and handles
    /// only one request per TCP connection. This avoids stale-connection
    /// races where the server closes an idle connection at the same time
    /// the client sends a new request on it.
    ///
    /// HTTP/2 multiplexing is unaffected.
    #[must_use]
    pub fn http1_keep_alive(mut self, enabled: bool) -> Self {
        self.http1_keep_alive = enabled;
        self
    }

    /// Start serving requests with the given router.
    ///
    /// Runs until the process is killed. For graceful shutdown use
    /// [`serve_with_graceful_shutdown`](Self::serve_with_graceful_shutdown).
    pub async fn serve(
        self,
        router: Router,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.serve_with_service(ConnectRpcService::new(router))
            .await
    }

    /// Start serving requests, shutting down gracefully when `signal` resolves.
    ///
    /// When the shutdown signal fires, the server:
    ///  1. drops the listener (new connection attempts are refused with RST),
    ///  2. signals every open connection to wind down — HTTP/2 connections
    ///     receive a GOAWAY (using the standard two-phase
    ///     GOAWAY/PING/GOAWAY sequence so racing client streams are handled
    ///     correctly); HTTP/1.1 connections have keep-alive disabled so they
    ///     close after the in-flight request,
    ///  3. waits for all in-flight requests to complete before returning
    ///     `Ok(())`.
    ///
    /// In-flight requests are not cancelled; this method waits indefinitely
    /// for them. For bounded shutdown (e.g. Kubernetes preStop hooks with a
    /// deadline), wrap this call in `tokio::time::timeout`:
    ///
    /// ```rust,ignore
    /// tokio::time::timeout(
    ///     Duration::from_secs(30),
    ///     bound.serve_with_graceful_shutdown(router, signal),
    /// )
    /// .await??;
    /// ```
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let bound = Server::bind("127.0.0.1:0").await?;
    /// bound
    ///     .serve_with_graceful_shutdown(router, async {
    ///         tokio::signal::ctrl_c().await.ok();
    ///     })
    ///     .await?;
    /// ```
    pub async fn serve_with_graceful_shutdown<F>(
        self,
        router: Router,
        signal: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.serve_with_service_and_shutdown(ConnectRpcService::new(router), signal)
            .await
    }

    /// Start serving requests with the given [`ConnectRpcService`].
    ///
    /// This is useful when you want to share a service between multiple servers,
    /// or when you've wrapped the service with additional tower layers.
    pub async fn serve_with_service<D: Dispatcher>(
        self,
        service: ConnectRpcService<D>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        #[cfg(feature = "server-tls")]
        let tls_acceptor = self.tls_config.map(tokio_rustls::TlsAcceptor::from);
        #[cfg(not(feature = "server-tls"))]
        let tls_acceptor: Option<()> = None;

        serve_with_listener(
            self.listener,
            service,
            tls_acceptor,
            self.http1_keep_alive,
            #[cfg(feature = "server-tls")]
            self.tls_handshake_timeout,
            None,
        )
        .await
    }

    /// Start serving requests with the given service, with graceful shutdown.
    ///
    /// See [`serve_with_graceful_shutdown`](Self::serve_with_graceful_shutdown)
    /// for behaviour and limitations.
    pub async fn serve_with_service_and_shutdown<D, F>(
        self,
        service: ConnectRpcService<D>,
        signal: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        D: Dispatcher,
        F: Future<Output = ()> + Send + 'static,
    {
        #[cfg(feature = "server-tls")]
        let tls_acceptor = self.tls_config.map(tokio_rustls::TlsAcceptor::from);
        #[cfg(not(feature = "server-tls"))]
        let tls_acceptor: Option<()> = None;

        serve_with_listener(
            self.listener,
            service,
            tls_acceptor,
            self.http1_keep_alive,
            #[cfg(feature = "server-tls")]
            self.tls_handshake_timeout,
            Some(Box::pin(signal)),
        )
        .await
    }
}

/// Type alias for the panic-catching wrapper around ConnectRpcService, used
/// by the per-connection task. Writing this out inline below would be verbose.
type WrappedService<D> = tower_http::catch_panic::CatchPanic<
    ConnectRpcService<D>,
    fn(Box<dyn Any + Send>) -> Response<Full<Bytes>>,
>;

/// Serve HTTP requests on an already-accepted stream.
///
/// Generic over the IO type so it works for both plain TCP and TLS streams.
/// Logs connection outcome at trace level.
///
/// `peer` is inserted into every request's extensions so handlers can read
/// the remote address (and TLS client cert chain, if any) via
/// `ctx.extensions.get::<PeerAddr>()` / `get::<PeerCerts>()`.
async fn serve_accepted_stream<D, S>(
    io: S,
    peer: PeerInfo,
    service: Arc<WrappedService<D>>,
    http1_keep_alive: bool,
    watcher: Watcher,
) where
    D: Dispatcher,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tracing::trace!(remote_addr = %peer.addr, "Accepted new connection");

    let peer_for_requests = peer.clone();
    let svc = hyper::service::service_fn(move |mut req| {
        peer_for_requests.insert_into(req.extensions_mut());
        let mut service = (*service).clone();
        async move { service.call(req).await }
    });

    let mut builder = AutoBuilder::new(TokioExecutor::new());
    builder.http1().keep_alive(http1_keep_alive);

    let conn = builder.serve_connection(TokioIo::new(io), svc).into_owned();
    match watcher.watch(conn).await {
        Ok(()) => {
            tracing::trace!(remote_addr = %peer.addr, "Connection completed normally");
        }
        Err(err) => {
            tracing::trace!(
                remote_addr = %peer.addr,
                error = %err,
                "Connection ended with error",
            );
        }
    }
}

/// Internal function to serve connections using the given listener and service.
///
/// A single implementation shared between TLS and non-TLS builds. The only
/// conditional code is the optional TLS handshake in the per-connection task;
/// the accept loop, nodelay handling, panic wrapping, and error logging are
/// identical.
#[cfg(feature = "server-tls")]
type MaybeTlsAcceptor = Option<tokio_rustls::TlsAcceptor>;
#[cfg(not(feature = "server-tls"))]
type MaybeTlsAcceptor = Option<()>;

/// Optional boxed shutdown-signal future.
type ShutdownSignal = Option<Pin<Box<dyn Future<Output = ()> + Send>>>;

async fn serve_with_listener<D: Dispatcher>(
    listener: TcpListener,
    service: ConnectRpcService<D>,
    tls_acceptor: MaybeTlsAcceptor,
    http1_keep_alive: bool,
    #[cfg(feature = "server-tls")] tls_handshake_timeout: std::time::Duration,
    shutdown: ShutdownSignal,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Wrap the service with panic handling to convert panics to 500 responses
    let service: WrappedService<D> = ServiceBuilder::new()
        .layer(CatchPanicLayer::custom(panic_handler as fn(_) -> _))
        .service(service);
    let service = Arc::new(service);

    #[cfg(feature = "server-tls")]
    let tls_acceptor = tls_acceptor.map(Arc::new);
    #[cfg(not(feature = "server-tls"))]
    let _ = tls_acceptor; // always None; silence unused warning

    // Per-connection graceful-shutdown coordinator. Each accepted connection
    // gets a `Watcher` (created in the accept loop, moved into the task) and
    // is wrapped via `watcher.watch(conn)`. On shutdown, `graceful.shutdown()`
    // signals every wrapped connection to send GOAWAY (HTTP/2) or disable
    // keep-alive (HTTP/1), then waits for every `Watcher` to drop — which
    // covers tasks still in TLS handshake as well as those serving traffic.
    let graceful = GracefulShutdown::new();

    // Pin the shutdown future so we can poll it in select!. If no shutdown
    // signal was provided, use a never-resolving pending() future.
    let mut shutdown = shutdown.unwrap_or_else(|| Box::pin(std::future::pending()));

    loop {
        let (stream, remote_addr) = tokio::select! {
            biased; // check shutdown first so we don't accept one more after signal

            _ = &mut shutdown => {
                tracing::info!("Shutdown signal received; draining connections");
                break;
            }
            accept_result = listener.accept() => match accept_result {
                Ok(conn) => conn,
                Err(err) => {
                    if is_transient_accept_error(&err) {
                        tracing::warn!("Transient accept error (continuing): {}", err);
                        continue;
                    }
                    return Err(err.into());
                }
            },
        };

        // Disable Nagle's algorithm to avoid latency from the interaction
        // between Nagle buffering and delayed ACKs, which is especially
        // problematic for HTTP/2's small control frames.
        if let Err(e) = stream.set_nodelay(true) {
            tracing::warn!("failed to set TCP_NODELAY: {e}");
        }

        let service = Arc::clone(&service);
        let watcher = graceful.watcher();

        #[cfg(feature = "server-tls")]
        let tls_acceptor = tls_acceptor.clone();

        tokio::spawn(async move {
            #[cfg(feature = "server-tls")]
            if let Some(acceptor) = tls_acceptor {
                // Apply a timeout to the TLS handshake to prevent connection
                // exhaustion attacks where clients stall the handshake
                // indefinitely, holding a task and file descriptor per connection.
                match tokio::time::timeout(tls_handshake_timeout, acceptor.accept(stream)).await {
                    Ok(Ok(tls_stream)) => {
                        // Extract the client cert chain now — once hyper owns
                        // the stream for I/O we can't borrow it again.
                        // `into_owned()` detaches from the session's lifetime
                        // so the Arc can outlive the TlsStream (which it must,
                        // since we move the stream into hyper but need the certs
                        // for every request on this connection).
                        let (_, conn) = tls_stream.get_ref();
                        let certs = conn.peer_certificates().map(|chain| -> Arc<[_]> {
                            chain.iter().map(|c| c.clone().into_owned()).collect()
                        });
                        let peer = PeerInfo {
                            addr: remote_addr,
                            certs,
                        };
                        serve_accepted_stream(tls_stream, peer, service, http1_keep_alive, watcher)
                            .await;
                    }
                    Ok(Err(err)) => {
                        tracing::debug!(
                            remote_addr = %remote_addr,
                            error = ?err,
                            "TLS handshake failed: {err}",
                        );
                    }
                    Err(_) => {
                        tracing::warn!(
                            remote_addr = %remote_addr,
                            "TLS handshake timed out after {tls_handshake_timeout:?}",
                        );
                    }
                }
                return;
            }

            // Plain TCP (no TLS or TLS not configured)
            let peer = PeerInfo {
                addr: remote_addr,
                #[cfg(feature = "server-tls")]
                certs: None,
            };
            serve_accepted_stream(stream, peer, service, http1_keep_alive, watcher).await;
        });
    }

    // Drop the listener (refuse new conns), then signal & drain existing ones.
    drop(listener);
    graceful.shutdown().await;
    tracing::info!("All connections drained; shutdown complete");

    Ok(())
}

/// Handle panics in request handlers by converting them to ConnectRPC error responses.
fn panic_handler(err: Box<dyn Any + Send + 'static>) -> Response<Full<Bytes>> {
    // Capture the backtrace for debugging
    let backtrace = std::backtrace::Backtrace::capture();

    // Try to extract a message from the panic
    let message = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        (*s).to_string()
    } else {
        "handler panicked".to_string()
    };

    // Log the panic with backtrace if available
    match backtrace.status() {
        std::backtrace::BacktraceStatus::Captured => {
            tracing::error!(
                "Request handler panicked: {}\n\nBacktrace:\n{}",
                message,
                backtrace
            );
        }
        _ => {
            tracing::error!(
                "Request handler panicked: {} (set RUST_BACKTRACE=1 for backtrace)",
                message
            );
        }
    }

    // Create a ConnectRPC internal error response
    let error = ConnectError::new(ErrorCode::Internal, "internal server error");
    let body = error.to_json();

    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(header::CONTENT_TYPE, content_type::JSON)
        .body(Full::new(body))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::new()))
                .unwrap()
        })
}

/// Check if an accept error is transient and can be recovered from.
///
/// Transient errors include:
/// - `EMFILE` / `ENFILE`: Too many open files (file descriptor exhaustion)
/// - `ECONNABORTED`: Connection was aborted before accept completed
/// - `EINTR`: Interrupted system call
pub(crate) fn is_transient_accept_error(err: &std::io::Error) -> bool {
    use std::io::ErrorKind;

    matches!(
        err.kind(),
        // Resource temporarily unavailable
        ErrorKind::WouldBlock |
        // Interrupted system call
        ErrorKind::Interrupted |
        // Connection aborted
        ErrorKind::ConnectionAborted |
        // Connection reset by peer
        ErrorKind::ConnectionReset
    ) || {
        // Check for EMFILE/ENFILE (too many open files)
        // These are mapped to Other on some platforms
        err.raw_os_error()
            .is_some_and(|code| code == libc::EMFILE || code == libc::ENFILE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    /// Hand-crafted Connect unary request (`POST /svc/Echo`, empty proto
    /// body, `Connection: close`). Used by the peer-info tests to probe the
    /// server over raw TCP/TLS without pulling in an HTTP client dep.
    const ECHO_REQ: &[u8] = concat!(
        "POST /svc/Echo HTTP/1.1\r\n",
        "Host: localhost\r\n",
        "Content-Type: application/proto\r\n",
        "Content-Length: 0\r\n",
        "Connection: close\r\n",
        "\r\n",
    )
    .as_bytes();

    #[test]
    fn test_server_creation() {
        let router = Router::new();
        let _server = Server::new(router);
    }

    #[tokio::test]
    async fn test_graceful_shutdown_immediate() {
        // Bind to an ephemeral port, trigger shutdown immediately,
        // verify serve returns cleanly without any connections.
        let bound = Server::bind("127.0.0.1:0").await.unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();

        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    rx.await.ok();
                })
                .await
        });

        // Fire the shutdown signal
        tx.send(()).unwrap();

        // Server should complete cleanly and promptly
        let result = tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("server did not shut down in time")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test]
    async fn test_graceful_shutdown_drains_inflight_request() {
        // Spawn a server with a handler that blocks until released. Start a
        // request, fire shutdown, verify the server waits for that request to
        // complete (not just the connection to close).
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let chans = Arc::new(Mutex::new(Some((entered_tx, release_rx))));
        let router = Router::new().route(
            "svc",
            "Slow",
            crate::handler_fn(
                move |_ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let chans = Arc::clone(&chans);
                    async move {
                        let taken = chans.lock().unwrap().take();
                        if let Some((entered_tx, release_rx)) = taken {
                            entered_tx.send(()).ok();
                            release_rx.await.ok();
                        }
                        crate::Response::ok(buffa_types::Empty::default())
                    }
                },
            ),
        );

        let bound = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        // Start the slow request over h2 and leave it in flight.
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(h2_conn);
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Slow"))
            .header(header::CONTENT_TYPE, "application/proto")
            .body(())
            .unwrap();
        let (resp_fut, _) = send_request.send_request(req, true).unwrap();
        let mut resp_fut = tokio::spawn(resp_fut);
        // Wait until the handler has actually started before firing shutdown.
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .expect("handler never entered")
            .unwrap();

        // Fire shutdown — the in-flight request must still be allowed to
        // complete.
        shutdown_tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !serve.is_finished(),
            "server shut down before in-flight request completed"
        );
        assert!(!resp_fut.is_finished(), "response arrived too early");

        // Release the handler; the response should arrive and the server
        // should drain.
        release_tx.send(()).unwrap();
        let resp = tokio::time::timeout(Duration::from_secs(5), &mut resp_fut)
            .await
            .expect("response never arrived")
            .expect("join error")
            .expect("h2 request failed");
        assert!(resp.status().is_success(), "got status {}", resp.status());

        let result = tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("server did not shut down after in-flight request drained")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test]
    async fn test_graceful_shutdown_rejects_new_connections() {
        // After shutdown signal, new connection attempts should fail.
        let bound = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = bound.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();

        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    rx.await.ok();
                })
                .await
        });

        // Give the server a moment to start the accept loop
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Trigger shutdown
        tx.send(()).unwrap();

        // Wait for serve to complete
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        // Now a new connection should fail (listener was dropped)
        let connect_result = tokio::net::TcpStream::connect(addr).await;
        assert!(
            connect_result.is_err(),
            "expected connection refused after shutdown"
        );
    }

    #[tokio::test]
    async fn test_graceful_shutdown_sends_h2_goaway() {
        // Regression: on graceful shutdown the server must send HTTP/2 GOAWAY
        // to existing connections so clients learn to stop sending new streams
        // and the server can drain promptly. Prior behaviour just dropped the
        // listener and waited, leaving idle h2 connections open until the
        // client hung up.
        let bound = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = bound.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();

        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    rx.await.ok();
                })
                .await
        });

        // Establish a raw HTTP/2 connection (prior-knowledge, no TLS).
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);

        // Round-trip a request to prove the h2 connection is fully established
        // on the server side before we fire the shutdown signal. The router is
        // empty so this errors (415, no Content-Type), but any response will do.
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Unknown"))
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        resp.await.unwrap();

        // Fire shutdown.
        tx.send(()).unwrap();

        // Expectation: the server sends GOAWAY(NO_ERROR) on this connection.
        // The h2 client surfaces that on the connection task and on subsequent
        // SendRequest readiness. We assert on the connection task: it must
        // complete (server closed cleanly after GOAWAY) within the timeout,
        // without us dropping our end first.
        let conn_result = tokio::time::timeout(Duration::from_secs(2), h2_task)
            .await
            .expect("server did not close idle h2 connection (no GOAWAY?)")
            .expect("h2 connection task panicked");
        if let Err(e) = conn_result {
            assert!(
                e.is_go_away(),
                "h2 connection ended with non-GOAWAY error: {e:?}"
            );
        }

        // And the server itself should now drain promptly — the only open
        // connection has been closed via GOAWAY.
        let result = tokio::time::timeout(Duration::from_secs(2), serve)
            .await
            .expect("server did not shut down after GOAWAY drained the connection")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");

        // Keep send_request alive until here so the client doesn't initiate
        // close before the server gets a chance to GOAWAY.
        drop(send_request);
    }

    // ========================================================================
    // PeerAddr / PeerCerts extension plumbing
    // ========================================================================

    #[tokio::test]
    async fn peer_addr_reaches_handler() {
        // Handler stashes the PeerAddr it sees into a shared slot.
        let captured: Arc<Mutex<Option<PeerAddr>>> = Arc::new(Mutex::new(None));
        let handler_captured = Arc::clone(&captured);
        let router = Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let cap = Arc::clone(&handler_captured);
                    async move {
                        *cap.lock().unwrap() = ctx.extensions.get::<PeerAddr>().cloned();
                        crate::Response::ok(buffa_types::Empty::default())
                    }
                },
            ),
        );

        let bound = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = bound.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    rx.await.ok();
                })
                .await
        });

        // Hand-crafted Connect unary request over raw TCP (HTTP/1.1).
        // Body is an empty-serialized `Empty` message (zero bytes).
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let client_local = stream.local_addr().unwrap();
        stream.write_all(ECHO_REQ).await.unwrap();
        // Drain the response so the server-side connection can complete.
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.unwrap();
        // Sanity: 2xx status.
        assert!(
            resp.starts_with(b"HTTP/1.1 2"),
            "expected 2xx, got: {}",
            String::from_utf8_lossy(&resp[..resp.len().min(80)])
        );

        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let peer = captured
            .lock()
            .unwrap()
            .take()
            .expect("handler should have captured PeerAddr");
        // The server sees the client's local_addr() as the remote peer.
        assert_eq!(peer.0, client_local);
    }

    /// End-to-end mTLS: client presents a cert; handler reads it from
    /// `ctx.extensions.get::<PeerCerts>()` and the DER bytes round-trip.
    #[cfg(feature = "server-tls")]
    #[tokio::test]
    async fn peer_certs_reach_handler() {
        // Inline minimal mTLS PKI: one CA → one server leaf + one client leaf.
        // Returns (server_config, client_config, client_cert_der).
        fn pki() -> (
            Arc<rustls::ServerConfig>,
            Arc<rustls::ClientConfig>,
            rustls::pki_types::CertificateDer<'static>,
        ) {
            use rcgen::CertificateParams;
            use rcgen::KeyPair;
            use rcgen::SanType;
            use rustls::pki_types::CertificateDer;
            use rustls::pki_types::PrivatePkcs8KeyDer;

            // Idempotent; err = already installed (tests share process state).
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

            let ca_key = KeyPair::generate().unwrap();
            let mut ca_params = CertificateParams::default();
            ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let ca = rcgen::CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();

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

        let (server_cfg, client_cfg, expected_client_der) = pki();

        let captured: Arc<Mutex<Option<PeerCerts>>> = Arc::new(Mutex::new(None));
        let handler_captured = Arc::clone(&captured);
        let router = Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let cap = Arc::clone(&handler_captured);
                    async move {
                        *cap.lock().unwrap() = ctx.extensions.get::<PeerCerts>().cloned();
                        crate::Response::ok(buffa_types::Empty::default())
                    }
                },
            ),
        );

        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_tls(server_cfg);
        let addr = bound.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    rx.await.ok();
                })
                .await
        });

        // TLS-over-raw-TCP + hand-crafted HTTP/1.1 Connect unary request.
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let connector = tokio_rustls::TlsConnector::from(client_cfg);
        let sni = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(sni, tcp).await.unwrap();
        tls.write_all(ECHO_REQ).await.unwrap();
        let mut resp = Vec::new();
        tls.read_to_end(&mut resp).await.unwrap();
        assert!(
            resp.starts_with(b"HTTP/1.1 2"),
            "expected 2xx, got: {}",
            String::from_utf8_lossy(&resp[..resp.len().min(80)])
        );

        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let certs = captured
            .lock()
            .unwrap()
            .take()
            .expect("handler should have captured PeerCerts");
        // The exact DER bytes the client presented.
        assert_eq!(certs.0.len(), 1);
        assert_eq!(certs.0[0].as_ref(), expected_client_der.as_ref());
    }
}
