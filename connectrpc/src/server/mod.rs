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
//!
//! # Connection Retirement
//!
//! Retire long-lived connections proactively — recommended behind load
//! balancers so clients reconnect periodically and traffic redistributes
//! across restarts. Two independent triggers are available, and either, both,
//! or neither may be set:
//!
//! - [`Server::with_max_connection_age`] (or the [`BoundServer`] equivalent)
//!   retires by age: a connection is sent a GOAWAY once it reaches the
//!   configured age (with a ±10% jitter).
//! - [`Server::with_max_requests_per_connection`] retires by request count: a
//!   connection is sent a GOAWAY once it has dispatched the configured number
//!   of requests.
//!
//! When both are set, whichever trigger fires first retires the connection.
//! After a trigger fires the connection is force-closed once the shared grace
//! period ([`with_max_connection_age_grace`](BoundServer::with_max_connection_age_grace))
//! elapses. Retirement is independent of whole-server graceful shutdown, which
//! still drains in-flight requests indefinitely even while a connection is in
//! its grace window.
//!
//! # Maximum Concurrent Streams
//!
//! Use [`Server::with_max_concurrent_streams`] (or the [`BoundServer`]
//! equivalent) to bound the number of concurrent HTTP/2 streams (in-flight
//! requests) a single connection may have open. This maps to hyper's
//! `SETTINGS_MAX_CONCURRENT_STREAMS`; it is left at hyper's default (200)
//! when unset. Raise it for high-fan-in internal services, or lower it as a
//! cheap hardening measure against less-trusted clients.
//!
//! # HTTP/2 Keepalive
//!
//! Use [`Server::with_http2_keepalive_interval`] (or the [`BoundServer`]
//! equivalent) to make the server send HTTP/2 keepalive PING frames and
//! reclaim dead or half-open peers. Disabled by default. Once an interval is
//! set, an unacknowledged PING after
//! [`with_http2_keepalive_timeout`](BoundServer::with_http2_keepalive_timeout)
//! (20 seconds by default) closes the connection. This detects long-lived
//! server-streaming or bidirectional connections that have gone silent (NAT
//! timeout, client crash, network partition) instead of leaving them
//! half-open until the OS TCP timeout.
//!
//! # Maximum Connection Idle
//!
//! Use [`Server::with_max_connection_idle`] (or the [`BoundServer`] equivalent)
//! to reclaim connections that have gone quiet. A connection is idle when it
//! has no in-flight requests; once it stays idle for the configured duration it
//! is retired through the same GOAWAY-then-grace path as maximum age, draining
//! over the same grace period set by `with_max_connection_age_grace`. The idle
//! timer resets on activity, so a connection with steady traffic is never
//! retired. The window is evaluated lazily, so retirement happens between one
//! and two times the configured duration after the last activity. When both
//! limits are configured, whichever fires first wins.
//!
//! For transport and HTTP/2 knobs that [`Server`] does not expose, drive
//! [`ConnectRpcService`] directly from a hyper accept loop. The crate guide's
//! "Advanced transport configuration" section shows the `hyper_util` pattern.

use std::any::Any;
use std::collections::hash_map::RandomState;
use std::future::Future;
use std::hash::BuildHasher;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use http::Response;
use http::StatusCode;
use http::header;
use http_body_util::Full;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::rt::TokioTimer;
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use hyper_util::server::graceful::GracefulConnection;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tower::Service;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;

use crate::codec::content_type;
use crate::dispatcher::Dispatcher;
use crate::error::ConnectError;
use crate::error::ErrorCode;
use crate::router::Router;
use crate::service::ConnectRpcService;

mod config;

pub use config::AcceptConfig;
pub use config::ConnectionConfig;
pub use config::DEFAULT_HEADER_READ_TIMEOUT;
pub use config::DEFAULT_HTTP2_ADAPTIVE_WINDOW;
pub use config::DEFAULT_HTTP2_KEEPALIVE_TIMEOUT;
#[cfg(feature = "server-tls")]
pub use config::DEFAULT_TLS_HANDSHAKE_TIMEOUT;

/// Remote socket address of the connected peer.
///
/// Inserted into every request's extensions by the built-in [`Server`]'s
/// accept loop and by `connectrpc::axum::serve_tls`. Handlers read it via
/// [`RequestContext::peer_addr`](crate::RequestContext::peer_addr) (or
/// `ctx.extensions().get::<PeerAddr>()`).
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
/// Handlers read it via
/// [`RequestContext::peer_certs`](crate::RequestContext::peer_certs) (or
/// `ctx.extensions().get::<PeerCerts>()`).
///
/// The `Arc` makes per-request insertion cheap: all requests on a
/// connection share one chain, so this is a refcount bump, not a copy.
#[cfg(feature = "server-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
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
    /// `ctx.peer_addr()` / `ctx.peer_certs()`.
    fn insert_into(&self, ext: &mut http::Extensions) {
        ext.insert(PeerAddr(self.addr));
        #[cfg(feature = "server-tls")]
        if let Some(certs) = &self.certs {
            ext.insert(PeerCerts(Arc::clone(certs)));
        }
    }
}

const MAX_CONNECTION_AGE_JITTER_BASIS_POINTS: u128 = 10_000;
const MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS: u128 = 1_000;
const NANOS_PER_SEC: u128 = 1_000_000_000;

/// ConnectRPC server built on hyper: a [`ConnectRpcService`] plus the
/// [`ConnectionConfig`] and [`AcceptConfig`] every accepted connection is
/// served with.
pub struct Server {
    service: ConnectRpcService,
    connection: ConnectionConfig,
    accept: AcceptConfig,
}

impl Server {
    /// Create a new server with the given router.
    pub fn new(router: Router) -> Self {
        Self::from_service(ConnectRpcService::new(router))
    }

    /// Create a new server from an existing [`ConnectRpcService`].
    pub fn from_service(service: ConnectRpcService) -> Self {
        Self {
            service,
            connection: ConnectionConfig::default(),
            accept: AcceptConfig::default(),
        }
    }

    /// Replace the per-connection settings wholesale.
    ///
    /// [`ConnectionConfig`] is a plain value, so one configuration can be
    /// built once and applied to a `Server`, a [`BoundServer`], or
    /// `connectrpc::axum::Serve` alike. The individual `with_*` setters below
    /// edit the same value in place.
    #[must_use]
    pub fn with_connection_config(mut self, config: ConnectionConfig) -> Self {
        self.connection = config;
        self
    }

    /// The per-connection settings accepted connections are served with.
    #[must_use]
    pub fn connection_config(&self) -> &ConnectionConfig {
        &self.connection
    }

    /// Replace the accept-time settings (TLS, handshake timeout) wholesale.
    #[must_use]
    pub fn with_accept_config(mut self, config: AcceptConfig) -> Self {
        self.accept = config;
        self
    }

    /// The accept-time settings.
    #[must_use]
    pub fn accept_config(&self) -> &AcceptConfig {
        &self.accept
    }

    /// Enable TLS with the given rustls server configuration.
    ///
    /// Shorthand for [`AcceptConfig::with_tls`]. The configuration controls
    /// all TLS behavior including certificate selection, client
    /// authentication, and protocol versions. For dynamic certificate
    /// rotation, use a [`rustls::server::ResolvesServerCert`] implementation
    /// in the config.
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
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls(mut self, config: Arc<rustls::ServerConfig>) -> Self {
        self.accept = self.accept.with_tls(config);
        self
    }

    /// Set the TLS handshake timeout (default [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`],
    /// 10 seconds). Shorthand for [`AcceptConfig::with_tls_handshake_timeout`].
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.accept = self.accept.with_tls_handshake_timeout(timeout);
        self
    }

    /// Set the HTTP/1.1 header read timeout (default
    /// [`DEFAULT_HEADER_READ_TIMEOUT`]; `None` disables). Shorthand for
    /// [`ConnectionConfig::with_header_read_timeout`].
    #[must_use]
    pub fn with_header_read_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.connection = self.connection.with_header_read_timeout(timeout);
        self
    }

    /// Enable or disable HTTP/1.1 keep-alive (default: enabled). Shorthand for
    /// [`ConnectionConfig::with_http1_keep_alive`].
    #[must_use]
    pub fn with_http1_keep_alive(mut self, enabled: bool) -> Self {
        self.connection = self.connection.with_http1_keep_alive(enabled);
        self
    }

    /// Configure server-side moderation of client-asserted RPC deadlines.
    ///
    /// Delegates to [`ConnectRpcService::with_deadline_policy`]. The
    /// default [`DeadlinePolicy::new`](crate::DeadlinePolicy::new) is a
    /// no-op: client timeout headers are honored verbatim for request
    /// receipt and handler execution, but streaming response bodies are
    /// not bounded by them. Set a policy to clamp, supply a default, or
    /// enforce on streams. See [`DeadlinePolicy`](crate::DeadlinePolicy).
    #[must_use]
    pub fn with_deadline_policy(mut self, policy: crate::DeadlinePolicy) -> Self {
        self.service = self.service.with_deadline_policy(policy);
        self
    }

    /// Configure request limits on the underlying service.
    ///
    /// Delegates to [`ConnectRpcService::with_limits`]. See
    /// [`Limits`](crate::Limits) for available options. Replaces any
    /// previously configured limits.
    #[must_use]
    pub fn with_limits(mut self, limits: crate::Limits) -> Self {
        self.service = self.service.with_limits(limits);
        self
    }

    /// Configure the compression registry on the underlying service.
    ///
    /// Delegates to [`ConnectRpcService::with_compression`]. The
    /// [`CompressionRegistry`](crate::CompressionRegistry) determines which
    /// compression algorithms are available for request decompression and
    /// response compression. Replaces any previously configured registry.
    #[must_use]
    pub fn with_compression(mut self, registry: crate::CompressionRegistry) -> Self {
        self.service = self.service.with_compression(registry);
        self
    }

    /// Configure the compression policy on the underlying service.
    ///
    /// Delegates to [`ConnectRpcService::with_compression_policy`]. The
    /// [`CompressionPolicy`](crate::CompressionPolicy) controls when
    /// compression is applied (e.g. minimum message size). Replaces any
    /// previously configured policy.
    #[must_use]
    pub fn with_compression_policy(mut self, policy: crate::CompressionPolicy) -> Self {
        self.service = self.service.with_compression_policy(policy);
        self
    }

    /// Append an [`Interceptor`](crate::Interceptor) to the chain on the
    /// underlying service.
    ///
    /// Delegates to [`ConnectRpcService::with_interceptor`]. Interceptors
    /// run after the request body has been read and decompressed and before
    /// it is decoded — they see the parsed request head and a lazily decoded
    /// body, not the wire bytes; a credential check belongs in Tower
    /// middleware, before the body is read. The first interceptor registered runs **outermost**:
    /// first on the way in, last on the way out. To share one interceptor
    /// instance across several `Server`s, use
    /// [`with_interceptor_arc`](Self::with_interceptor_arc).
    #[must_use]
    pub fn with_interceptor(mut self, interceptor: impl crate::Interceptor) -> Self {
        self.service = self.service.with_interceptor(interceptor);
        self
    }

    /// Append an already-`Arc`'d [`Interceptor`](crate::Interceptor) to the
    /// chain on the underlying service.
    ///
    /// Delegates to [`ConnectRpcService::with_interceptor_arc`]. Same
    /// ordering and semantics as [`with_interceptor`](Self::with_interceptor);
    /// use this when one interceptor instance is shared across multiple
    /// services or `Server`s.
    #[must_use]
    pub fn with_interceptor_arc(mut self, interceptor: Arc<dyn crate::Interceptor>) -> Self {
        self.service = self.service.with_interceptor_arc(interceptor);
        self
    }

    /// Retire each connection once it reaches `max_age` (±10% jitter, then
    /// GOAWAY and a grace period). Shorthand for
    /// [`ConnectionConfig::with_max_connection_age`].
    ///
    /// # Panics
    ///
    /// Panics if `max_age` is zero.
    #[must_use]
    pub fn with_max_connection_age(mut self, max_age: Duration) -> Self {
        self.connection = self.connection.with_max_connection_age(max_age);
        self
    }

    /// Set the drain window shared by the three retirement triggers (default
    /// five seconds). Shorthand for
    /// [`ConnectionConfig::with_max_connection_age_grace`].
    #[must_use]
    pub fn with_max_connection_age_grace(mut self, grace: Duration) -> Self {
        self.connection = self.connection.with_max_connection_age_grace(grace);
        self
    }

    /// Retire a connection that has had no in-flight requests for `duration`.
    /// Shorthand for [`ConnectionConfig::with_max_connection_idle`].
    ///
    /// # Panics
    ///
    /// Panics if `duration` is zero.
    #[must_use]
    pub fn with_max_connection_idle(mut self, duration: Duration) -> Self {
        self.connection = self.connection.with_max_connection_idle(duration);
        self
    }

    /// Enable or disable HTTP/2 adaptive flow-control window sizing (default
    /// [`DEFAULT_HTTP2_ADAPTIVE_WINDOW`]). Shorthand for
    /// [`ConnectionConfig::with_http2_adaptive_window`].
    #[must_use]
    pub fn with_http2_adaptive_window(mut self, enabled: bool) -> Self {
        self.connection = self.connection.with_http2_adaptive_window(enabled);
        self
    }

    /// Set the HTTP/2 initial stream window, in bytes; supplying a size turns
    /// adaptive sizing off. Shorthand for
    /// [`ConnectionConfig::with_http2_initial_stream_window_size`].
    #[must_use]
    pub fn with_http2_initial_stream_window_size(mut self, size: impl Into<Option<u32>>) -> Self {
        self.connection = self.connection.with_http2_initial_stream_window_size(size);
        self
    }

    /// Set the HTTP/2 initial connection window, in bytes; supplying a size
    /// turns adaptive sizing off. Shorthand for
    /// [`ConnectionConfig::with_http2_initial_connection_window_size`].
    #[must_use]
    pub fn with_http2_initial_connection_window_size(
        mut self,
        size: impl Into<Option<u32>>,
    ) -> Self {
        self.connection = self
            .connection
            .with_http2_initial_connection_window_size(size);
        self
    }

    /// Set the advertised HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS` (hyper's
    /// default, 200, when unset). Shorthand for
    /// [`ConnectionConfig::with_max_concurrent_streams`].
    ///
    /// # Panics
    ///
    /// Panics if `max_streams` is zero.
    #[must_use]
    pub fn with_max_concurrent_streams(mut self, max_streams: u32) -> Self {
        self.connection = self.connection.with_max_concurrent_streams(max_streams);
        self
    }

    /// Retire each connection after it has dispatched `max` requests.
    /// Shorthand for [`ConnectionConfig::with_max_requests_per_connection`].
    #[must_use]
    pub fn with_max_requests_per_connection(mut self, max: NonZeroU64) -> Self {
        self.connection = self.connection.with_max_requests_per_connection(max);
        self
    }

    /// Send HTTP/2 keepalive PINGs every `interval` on idle connections
    /// (disabled by default). Shorthand for
    /// [`ConnectionConfig::with_http2_keepalive_interval`].
    ///
    /// # Panics
    ///
    /// Panics if `interval` is zero.
    #[must_use]
    pub fn with_http2_keepalive_interval(mut self, interval: Duration) -> Self {
        self.connection = self.connection.with_http2_keepalive_interval(interval);
        self
    }

    /// Set how long an HTTP/2 keepalive PING may go unacknowledged (default
    /// [`DEFAULT_HTTP2_KEEPALIVE_TIMEOUT`]). Shorthand for
    /// [`ConnectionConfig::with_http2_keepalive_timeout`].
    #[must_use]
    pub fn with_http2_keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.connection = self.connection.with_http2_keepalive_timeout(timeout);
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
        let scheme = if self.accept.is_tls() {
            "https"
        } else {
            "http"
        };
        tracing::info!("ConnectRPC server listening on {scheme}://{addr}");

        serve_with_listener(listener, self.service, self.accept, self.connection, None).await
    }

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
            connection: ConnectionConfig::default(),
            accept: AcceptConfig::default(),
        }
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
    /// When multiple addresses are returned (e.g. `localhost` resolving to
    /// both `::1` and `127.0.0.1`), the first that successfully binds is used.
    pub async fn bind(
        addr: impl tokio::net::ToSocketAddrs,
    ) -> Result<BoundServer, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self::from_listener(TcpListener::bind(addr).await?))
    }
}

/// A listener plus the settings its connections will be served with; the
/// service is supplied when serving starts.
pub struct BoundServer {
    listener: TcpListener,
    connection: ConnectionConfig,
    accept: AcceptConfig,
}

impl BoundServer {
    /// Get the local address the server is bound to.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Replace the per-connection settings wholesale; see
    /// [`Server::with_connection_config`].
    #[must_use]
    pub fn with_connection_config(mut self, config: ConnectionConfig) -> Self {
        self.connection = config;
        self
    }

    /// The per-connection settings accepted connections are served with.
    #[must_use]
    pub fn connection_config(&self) -> &ConnectionConfig {
        &self.connection
    }

    /// Replace the accept-time settings (TLS, handshake timeout) wholesale.
    #[must_use]
    pub fn with_accept_config(mut self, config: AcceptConfig) -> Self {
        self.accept = config;
        self
    }

    /// The accept-time settings.
    #[must_use]
    pub fn accept_config(&self) -> &AcceptConfig {
        &self.accept
    }

    /// Enable TLS with the given rustls server configuration. Shorthand for
    /// [`AcceptConfig::with_tls`].
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls(mut self, config: Arc<rustls::ServerConfig>) -> Self {
        self.accept = self.accept.with_tls(config);
        self
    }

    /// Set the TLS handshake timeout (default [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`],
    /// 10 seconds). Shorthand for [`AcceptConfig::with_tls_handshake_timeout`].
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.accept = self.accept.with_tls_handshake_timeout(timeout);
        self
    }

    /// Set the HTTP/1.1 header read timeout (default
    /// [`DEFAULT_HEADER_READ_TIMEOUT`]; `None` disables). Shorthand for
    /// [`ConnectionConfig::with_header_read_timeout`].
    #[must_use]
    pub fn with_header_read_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.connection = self.connection.with_header_read_timeout(timeout);
        self
    }

    /// Enable or disable HTTP/1.1 keep-alive (default: enabled). Shorthand for
    /// [`ConnectionConfig::with_http1_keep_alive`].
    #[must_use]
    pub fn with_http1_keep_alive(mut self, enabled: bool) -> Self {
        self.connection = self.connection.with_http1_keep_alive(enabled);
        self
    }

    /// Retire each connection once it reaches `max_age` (±10% jitter, then
    /// GOAWAY and a grace period). Shorthand for
    /// [`ConnectionConfig::with_max_connection_age`].
    ///
    /// # Panics
    ///
    /// Panics if `max_age` is zero.
    #[must_use]
    pub fn with_max_connection_age(mut self, max_age: Duration) -> Self {
        self.connection = self.connection.with_max_connection_age(max_age);
        self
    }

    /// Set the drain window shared by the three retirement triggers (default
    /// five seconds). Shorthand for
    /// [`ConnectionConfig::with_max_connection_age_grace`].
    #[must_use]
    pub fn with_max_connection_age_grace(mut self, grace: Duration) -> Self {
        self.connection = self.connection.with_max_connection_age_grace(grace);
        self
    }

    /// Retire a connection that has had no in-flight requests for `duration`.
    /// Shorthand for [`ConnectionConfig::with_max_connection_idle`].
    ///
    /// # Panics
    ///
    /// Panics if `duration` is zero.
    #[must_use]
    pub fn with_max_connection_idle(mut self, duration: Duration) -> Self {
        self.connection = self.connection.with_max_connection_idle(duration);
        self
    }

    /// Enable or disable HTTP/2 adaptive flow-control window sizing (default
    /// [`DEFAULT_HTTP2_ADAPTIVE_WINDOW`]). Shorthand for
    /// [`ConnectionConfig::with_http2_adaptive_window`].
    #[must_use]
    pub fn with_http2_adaptive_window(mut self, enabled: bool) -> Self {
        self.connection = self.connection.with_http2_adaptive_window(enabled);
        self
    }

    /// Set the HTTP/2 initial stream window, in bytes; supplying a size turns
    /// adaptive sizing off. Shorthand for
    /// [`ConnectionConfig::with_http2_initial_stream_window_size`].
    #[must_use]
    pub fn with_http2_initial_stream_window_size(mut self, size: impl Into<Option<u32>>) -> Self {
        self.connection = self.connection.with_http2_initial_stream_window_size(size);
        self
    }

    /// Set the HTTP/2 initial connection window, in bytes; supplying a size
    /// turns adaptive sizing off. Shorthand for
    /// [`ConnectionConfig::with_http2_initial_connection_window_size`].
    #[must_use]
    pub fn with_http2_initial_connection_window_size(
        mut self,
        size: impl Into<Option<u32>>,
    ) -> Self {
        self.connection = self
            .connection
            .with_http2_initial_connection_window_size(size);
        self
    }

    /// Set the advertised HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS` (hyper's
    /// default, 200, when unset). Shorthand for
    /// [`ConnectionConfig::with_max_concurrent_streams`].
    ///
    /// # Panics
    ///
    /// Panics if `max_streams` is zero.
    #[must_use]
    pub fn with_max_concurrent_streams(mut self, max_streams: u32) -> Self {
        self.connection = self.connection.with_max_concurrent_streams(max_streams);
        self
    }

    /// Retire each connection after it has dispatched `max` requests.
    /// Shorthand for [`ConnectionConfig::with_max_requests_per_connection`].
    #[must_use]
    pub fn with_max_requests_per_connection(mut self, max: NonZeroU64) -> Self {
        self.connection = self.connection.with_max_requests_per_connection(max);
        self
    }

    /// Send HTTP/2 keepalive PINGs every `interval` on idle connections
    /// (disabled by default). Shorthand for
    /// [`ConnectionConfig::with_http2_keepalive_interval`].
    ///
    /// # Panics
    ///
    /// Panics if `interval` is zero.
    #[must_use]
    pub fn with_http2_keepalive_interval(mut self, interval: Duration) -> Self {
        self.connection = self.connection.with_http2_keepalive_interval(interval);
        self
    }

    /// Set how long an HTTP/2 keepalive PING may go unacknowledged (default
    /// [`DEFAULT_HTTP2_KEEPALIVE_TIMEOUT`]). Shorthand for
    /// [`ConnectionConfig::with_http2_keepalive_timeout`].
    #[must_use]
    pub fn with_http2_keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.connection = self.connection.with_http2_keepalive_timeout(timeout);
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
        serve_with_listener(self.listener, service, self.accept, self.connection, None).await
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
        serve_with_listener(
            self.listener,
            service,
            self.accept,
            self.connection,
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

#[derive(Clone, Copy, Debug)]
struct ConnectionAgeConfig {
    max_age: Duration,
    grace: Duration,
}

impl ConnectionAgeConfig {
    fn with_jitter(self, sample: u64) -> Self {
        Self {
            max_age: jitter_connection_age(self.max_age, sample),
            grace: self.grace,
        }
    }
}

/// Per-connection idle-reaping configuration.
#[derive(Clone, Copy, Debug)]
struct IdleConfig {
    /// How long a connection may have zero in-flight requests before it is
    /// retired.
    idle: Duration,
    /// Drain window after GOAWAY before the connection is force-closed. Shared
    /// with [`ConnectionAgeConfig::grace`].
    grace: Duration,
}

/// Per-connection retirement policy: the optional max-age, max-idle, and
/// max-request-count limits that the connection lifecycle enforces. Bundled so
/// the accept loop and the per-connection task pass a single value rather than
/// three parallel options.
#[derive(Clone, Copy, Debug)]
struct RetirementConfig {
    age: Option<ConnectionAgeConfig>,
    idle: Option<IdleConfig>,
    requests: Option<RequestRetirementConfig>,
}

impl RetirementConfig {
    /// The retirement triggers configured in `config`, before per-connection
    /// jitter. All three share one grace period.
    fn new(config: &ConnectionConfig) -> Self {
        let grace = config.max_connection_age_grace();
        Self {
            age: config
                .max_connection_age()
                .map(|max_age| ConnectionAgeConfig { max_age, grace }),
            idle: config
                .max_connection_idle()
                .map(|idle| IdleConfig { idle, grace }),
            requests: config
                .max_requests_per_connection()
                .map(|max| RequestRetirementConfig { max, grace }),
        }
    }
}

/// Shared in-flight request accounting for one connection.
///
/// The per-request `service_fn` wrapper bumps these counters at the dispatch
/// boundary (hyper does not surface per-connection stream counts directly), and
/// the connection lifecycle reads them to decide whether the connection has
/// been idle. `epoch` increments on every request start *and* completion, so a
/// short request that begins and ends entirely within an idle window is still
/// observed as activity and resets the idle timer.
///
/// The two counters use `SeqCst` for clarity; correctness only needs each
/// counter to be individually monotonic, so `Relaxed` would also be sound. The
/// ordering is not load-bearing — in particular [`snapshot`](Self::snapshot)
/// does not read the pair atomically (see its note).
#[derive(Debug, Default)]
struct ConnectionActivity {
    in_flight: AtomicUsize,
    epoch: AtomicU64,
}

impl ConnectionActivity {
    fn request_started(&self) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    fn request_finished(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    /// Current `(in_flight, epoch)` pair.
    ///
    /// The two fields are read as independent loads, so a request arriving in
    /// the instant between them (or between the writer's two increments in
    /// [`request_started`](Self::request_started)) can be observed as
    /// `(0, armed_epoch)` and trigger a reap while a request is actually
    /// landing. This is benign: `graceful_shutdown` sends GOAWAY with the
    /// standard last-stream-id handling and the grace window drains anything
    /// genuinely in flight, so the racing request either completes or the
    /// client retries on a fresh connection.
    fn snapshot(&self) -> (usize, u64) {
        (
            self.in_flight.load(Ordering::SeqCst),
            self.epoch.load(Ordering::SeqCst),
        )
    }
}

/// RAII guard that records a request as in-flight for the lifetime of its
/// response future. Decrementing on drop (rather than on a success path) keeps
/// the in-flight count correct even when a request future is cancelled or its
/// handler panics.
struct ActiveRequestGuard(Arc<ConnectionActivity>);

impl ActiveRequestGuard {
    fn new(activity: Arc<ConnectionActivity>) -> Self {
        activity.request_started();
        Self(activity)
    }
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.0.request_finished();
    }
}

/// Per-connection request-count retirement settings.
///
/// `max` is the number of requests a connection may serve before it is retired
/// via graceful shutdown; `grace` is how long in-flight requests are allowed to
/// finish afterwards (shared with the max-age grace period).
#[derive(Clone, Copy, Debug)]
struct RequestRetirementConfig {
    max: NonZeroU64,
    grace: Duration,
}

/// Serve HTTP requests on an already-accepted stream.
///
/// Generic over the IO type so it works for both plain TCP and TLS streams.
/// Logs connection outcome at trace level.
///
/// `peer` describes the connection; its address (and TLS client cert
/// chain, if any) is inserted into every request's extensions so handlers
/// can read them via `ctx.peer_addr()` / `ctx.peer_certs()`.
async fn serve_accepted_stream<D, S>(
    io: S,
    peer: PeerInfo,
    service: Arc<WrappedService<D>>,
    config: ConnectionConfig,
    global_shutdown: watch::Receiver<bool>,
    retirement: RetirementConfig,
) where
    D: Dispatcher,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tracing::trace!(remote_addr = %peer.addr, "Accepted new connection");

    // In-flight accounting is only needed when idle reaping is enabled; when it
    // is off there is no per-request bookkeeping overhead.
    let activity = retirement
        .idle
        .map(|_| Arc::new(ConnectionActivity::default()));

    // When request-count retirement is enabled, the service counts every
    // dispatched request and flips this watch channel once the limit is
    // reached; the connection lifecycle observes it and starts draining. The
    // counter lives only as long as this connection task.
    let (request_counter, request_retire) = match retirement.requests {
        Some(config) => {
            let (tx, rx) = watch::channel(false);
            (
                Some(RequestCounter {
                    served: AtomicU64::new(0),
                    max: config.max,
                    retire: tx,
                }),
                Some((rx, config.grace)),
            )
        }
        None => (None, None),
    };

    let peer_for_requests = peer.clone();
    let activity_for_requests = activity.clone();
    let svc = hyper::service::service_fn(move |mut req| {
        peer_for_requests.insert_into(req.extensions_mut());
        if let Some(counter) = &request_counter {
            counter.record_request();
        }
        let mut service = (*service).clone();
        // Mark the request in-flight before its future is polled; the guard
        // decrements on completion or drop.
        let guard = activity_for_requests
            .as_ref()
            .map(|activity| ActiveRequestGuard::new(Arc::clone(activity)));
        async move {
            let _guard = guard;
            service.call(req).await
        }
    });

    let mut builder = AutoBuilder::new(TokioExecutor::new());
    // A timer is required for hyper's header read timeout (and any other
    // time-based connection behaviour) to take effect; without it the
    // configured `header_read_timeout` is silently ignored.
    builder
        .http1()
        .timer(TokioTimer::new())
        .keep_alive(config.http1_keep_alive())
        .header_read_timeout(config.header_read_timeout());
    configure_http2(&mut builder, &config);

    let conn = builder.serve_connection(TokioIo::new(io), svc).into_owned();
    serve_connection_with_lifecycle(
        conn,
        peer.addr,
        global_shutdown,
        retirement.age,
        retirement.idle.zip(activity),
        request_retire,
    )
    .await;
}

/// Per-connection request counter that triggers retirement once the configured
/// limit is reached.
struct RequestCounter {
    served: AtomicU64,
    max: NonZeroU64,
    retire: watch::Sender<bool>,
}

impl RequestCounter {
    /// Count one dispatched request. Once the count reaches the limit, flip the
    /// retirement signal so the connection lifecycle begins graceful shutdown.
    fn record_request(&self) {
        // The atomic itself wraps on overflow (atomic ops never panic), but the
        // limit is reached long before 2^64 requests and the watch value is
        // sticky-true thereafter. `saturating_add` only clamps the local
        // comparison value so it can't wrap below the limit in that extreme.
        let served = self
            .served
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if served >= self.max.get() {
            // `send` only errs if the receiver was dropped (the connection is
            // already gone), in which case there is nothing left to retire.
            let _ = self.retire.send(true);
        }
    }
}

/// Apply the HTTP/2 configuration to a connection builder.
///
/// `adaptive_window` is always set explicitly so the default tracks
/// [`DEFAULT_HTTP2_ADAPTIVE_WINDOW`] regardless of hyper's own default. Explicit
/// window sizes are applied only when adaptive sizing is off (see
/// [`ConnectionConfig::effective_http2_windows`]), so the two never reach hyper
/// at once and the precedence does not depend on hyper's internal call
/// ordering.
fn configure_http2(builder: &mut AutoBuilder<TokioExecutor>, config: &ConnectionConfig) {
    let mut http2 = builder.http2();
    http2.adaptive_window(config.http2_adaptive_window());
    let (stream_window, connection_window) = config.effective_http2_windows();
    if let Some(size) = stream_window {
        http2.initial_stream_window_size(size);
    }
    if let Some(size) = connection_window {
        http2.initial_connection_window_size(size);
    }
    if let Some(max) = config.max_concurrent_streams() {
        http2.max_concurrent_streams(max);
    }
    // Keepalive is opt-in: when no interval is set, leave hyper's default
    // (disabled) untouched. When enabled, a timer must be installed — hyper's
    // HTTP/2 keepalive requires one and panics the connection task without it.
    if let Some(interval) = config.http2_keepalive_interval() {
        http2
            .timer(TokioTimer::new())
            .keep_alive_interval(interval)
            .keep_alive_timeout(config.http2_keepalive_timeout());
    }
}

fn serve_connection_with_lifecycle<C>(
    conn: C,
    remote_addr: SocketAddr,
    global_shutdown: watch::Receiver<bool>,
    connection_age: Option<ConnectionAgeConfig>,
    connection_idle: Option<(IdleConfig, Arc<ConnectionActivity>)>,
    request_retire: Option<(watch::Receiver<bool>, Duration)>,
) -> ConnectionLifecycle<C>
where
    C: GracefulConnection,
    C::Error: std::fmt::Display,
{
    ConnectionLifecycle {
        conn: Box::pin(conn),
        remote_addr,
        global_shutdown: global_shutdown_future(global_shutdown),
        age: connection_age.map(|config| (Box::pin(tokio::time::sleep(config.max_age)), config)),
        idle: connection_idle.map(|(config, activity)| {
            let armed_epoch = activity.snapshot().1;
            IdleTracker {
                config,
                activity,
                timer: Box::pin(tokio::time::sleep(config.idle)),
                armed_epoch,
            }
        }),
        // The retirement receiver flips to `true` when the connection's request
        // count reaches its limit; `global_shutdown_future` resolves on that
        // same watch-channel edge, so it is reused here as the awaiter.
        requests: request_retire.map(|(rx, grace)| (global_shutdown_future(rx), grace)),
        state: ConnectionLifecycleState::Serving,
    }
}

/// The future a connection awaits to learn the server is shutting down.
///
/// Resolves when the accept loop sets the watch value to `true`, or drops the
/// sender (for example on a fatal accept error). Both are treated as "begin
/// graceful shutdown" so a connection always drains rather than hanging when
/// the accept loop goes away.
fn global_shutdown_future(
    mut global_shutdown: watch::Receiver<bool>,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        let _ = global_shutdown.wait_for(|fired| *fired).await;
    })
}

/// A boxed future that resolves when a per-connection retirement trigger (such
/// as the request-count limit) fires.
type RetirementSignal = Pin<Box<dyn Future<Output = ()> + Send>>;

struct ConnectionLifecycle<C: GracefulConnection> {
    conn: Pin<Box<C>>,
    remote_addr: SocketAddr,
    global_shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
    age: Option<(Pin<Box<tokio::time::Sleep>>, ConnectionAgeConfig)>,
    idle: Option<IdleTracker>,
    /// Resolves when the per-connection request count reaches its limit; the
    /// `Duration` is the grace period to drain with once it fires.
    requests: Option<(RetirementSignal, Duration)>,
    state: ConnectionLifecycleState,
}

/// Per-connection idle-timer state held by [`ConnectionLifecycle`].
struct IdleTracker {
    config: IdleConfig,
    activity: Arc<ConnectionActivity>,
    /// Fires when the current idle window elapses.
    timer: Pin<Box<tokio::time::Sleep>>,
    /// Activity epoch observed when the current window was armed. If it is
    /// unchanged and there are no in-flight requests when the timer fires, the
    /// connection has been idle for the whole window.
    armed_epoch: u64,
}

enum ConnectionLifecycleState {
    Serving,
    GlobalDraining,
    /// Draining after a per-connection retirement trigger (max age, max idle,
    /// or max requests): graceful shutdown has been issued and the connection
    /// is given a grace window to finish in-flight work before being
    /// force-closed.
    Draining {
        grace: Pin<Box<tokio::time::Sleep>>,
        duration: Duration,
    },
}

impl<C> Future for ConnectionLifecycle<C>
where
    C: GracefulConnection,
    C::Error: std::fmt::Display,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                ConnectionLifecycleState::Serving => {
                    if let Poll::Ready(result) = this.conn.as_mut().poll(cx) {
                        log_connection_result(this.remote_addr, result);
                        return Poll::Ready(());
                    }

                    if let Poll::Ready(()) = this.global_shutdown.as_mut().poll(cx) {
                        this.conn.as_mut().graceful_shutdown();
                        this.state = ConnectionLifecycleState::GlobalDraining;
                        continue;
                    }

                    if let Some((age, config)) = &mut this.age
                        && age.as_mut().poll(cx).is_ready()
                    {
                        tracing::trace!(
                            remote_addr = %this.remote_addr,
                            max_age = ?config.max_age,
                            grace = ?config.grace,
                            "Connection reached maximum age; starting graceful shutdown",
                        );
                        this.conn.as_mut().graceful_shutdown();
                        this.state = ConnectionLifecycleState::Draining {
                            grace: Box::pin(tokio::time::sleep(config.grace)),
                            duration: config.grace,
                        };
                        continue;
                    }

                    if let Some(idle) = &mut this.idle
                        && idle.timer.as_mut().poll(cx).is_ready()
                    {
                        let (in_flight, epoch) = idle.activity.snapshot();
                        if in_flight == 0 && epoch == idle.armed_epoch {
                            tracing::trace!(
                                remote_addr = %this.remote_addr,
                                idle = ?idle.config.idle,
                                grace = ?idle.config.grace,
                                "Connection idle; starting graceful shutdown",
                            );
                            this.conn.as_mut().graceful_shutdown();
                            this.state = ConnectionLifecycleState::Draining {
                                grace: Box::pin(tokio::time::sleep(idle.config.grace)),
                                duration: idle.config.grace,
                            };
                            continue;
                        }
                        // A request is in flight, or activity occurred during
                        // the window: reset the idle timer and re-arm. Reuse the
                        // existing `Sleep` allocation rather than boxing a new
                        // one each window.
                        idle.armed_epoch = epoch;
                        let next = tokio::time::Instant::now() + idle.config.idle;
                        idle.timer.as_mut().reset(next);
                        continue;
                    }

                    if let Some((requests, grace)) = &mut this.requests
                        && requests.as_mut().poll(cx).is_ready()
                    {
                        let grace = *grace;
                        tracing::trace!(
                            remote_addr = %this.remote_addr,
                            grace = ?grace,
                            "Connection reached maximum requests; starting graceful shutdown",
                        );
                        this.conn.as_mut().graceful_shutdown();
                        this.state = ConnectionLifecycleState::Draining {
                            grace: Box::pin(tokio::time::sleep(grace)),
                            duration: grace,
                        };
                        continue;
                    }

                    return Poll::Pending;
                }
                ConnectionLifecycleState::GlobalDraining => {
                    if let Poll::Ready(result) = this.conn.as_mut().poll(cx) {
                        log_connection_result(this.remote_addr, result);
                        return Poll::Ready(());
                    }
                    return Poll::Pending;
                }
                ConnectionLifecycleState::Draining { grace, duration } => {
                    if let Poll::Ready(result) = this.conn.as_mut().poll(cx) {
                        log_connection_result(this.remote_addr, result);
                        return Poll::Ready(());
                    }

                    if let Poll::Ready(()) = this.global_shutdown.as_mut().poll(cx) {
                        this.state = ConnectionLifecycleState::GlobalDraining;
                        continue;
                    }

                    if grace.as_mut().poll(cx).is_ready() {
                        tracing::trace!(
                            remote_addr = %this.remote_addr,
                            grace = ?duration,
                            "Connection retirement grace expired; closing connection",
                        );
                        return Poll::Ready(());
                    }

                    return Poll::Pending;
                }
            }
        }
    }
}

fn log_connection_result<E: std::fmt::Display>(remote_addr: SocketAddr, result: Result<(), E>) {
    match result {
        Ok(()) => {
            tracing::trace!(remote_addr = %remote_addr, "Connection completed normally");
        }
        Err(err) => {
            tracing::trace!(
                remote_addr = %remote_addr,
                error = %err,
                "Connection ended with error",
            );
        }
    }
}

fn jitter_connection_age(age: Duration, sample: u64) -> Duration {
    if age.is_zero() {
        return age;
    }

    let spread = MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS * 2;
    let offset = (u128::from(sample) * spread) / u128::from(u64::MAX);
    let basis_points = MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
        - MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS
        + offset;
    let scaled = age.as_nanos().saturating_mul(basis_points);
    let nanos = if basis_points < MAX_CONNECTION_AGE_JITTER_BASIS_POINTS {
        scaled.saturating_add(MAX_CONNECTION_AGE_JITTER_BASIS_POINTS - 1)
            / MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
    } else {
        scaled / MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
    };

    duration_from_nanos(nanos.min(Duration::MAX.as_nanos()))
}

fn duration_from_nanos(nanos: u128) -> Duration {
    Duration::new(
        (nanos / NANOS_PER_SEC) as u64,
        (nanos % NANOS_PER_SEC) as u32,
    )
}

/// Optional boxed shutdown-signal future.
type ShutdownSignal = Option<Pin<Box<dyn Future<Output = ()> + Send>>>;

/// Serve connections from `listener` with `service` until `shutdown` resolves,
/// then drain them.
///
/// A single implementation shared between TLS and non-TLS builds: the only
/// conditional code is the optional TLS handshake in the per-connection task.
async fn serve_with_listener<D: Dispatcher>(
    listener: TcpListener,
    service: ConnectRpcService<D>,
    accept: AcceptConfig,
    connection: ConnectionConfig,
    shutdown: ShutdownSignal,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    connection.lint();
    let retirement = RetirementConfig::new(&connection);

    // Wrap the service with panic handling to convert panics to 500 responses
    let service: WrappedService<D> = ServiceBuilder::new()
        .layer(CatchPanicLayer::custom(panic_handler as fn(_) -> _))
        .service(service);
    let service = Arc::new(service);

    #[cfg(feature = "server-tls")]
    let tls_handshake_timeout = accept.tls_handshake_timeout();
    #[cfg(feature = "server-tls")]
    let tls_acceptor = accept
        .tls()
        .cloned()
        .map(|config| Arc::new(tokio_rustls::TlsAcceptor::from(config)));
    #[cfg(not(feature = "server-tls"))]
    let _ = accept; // carries no settings without TLS

    // Pin the shutdown future so we can poll it in select!. If no shutdown
    // signal was provided, use a never-resolving pending() future.
    let mut shutdown = shutdown.unwrap_or_else(|| Box::pin(std::future::pending()));
    // Broadcasts "begin graceful shutdown" to every live connection. `watch`
    // gives a cloneable receiver per connection and a sticky value, so a
    // connection that registers after the signal still observes it.
    let (global_shutdown_tx, global_shutdown_rx) = watch::channel(false);
    let mut connections = JoinSet::new();
    let jitter_state = RandomState::new();
    let mut connection_sequence = 0u64;

    loop {
        let (stream, remote_addr) = tokio::select! {
            biased; // check shutdown first so we don't accept one more after signal

            _ = &mut shutdown => {
                tracing::info!("Shutdown signal received; draining connections");
                break;
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                log_connection_task_result(result);
                continue;
            }
            accept_result = listener.accept() => match accept_result {
                Ok(conn) => conn,
                Err(err) => {
                    if is_transient_accept_error(&err) {
                        tracing::warn!("Transient accept error (continuing): {}", err);
                        continue;
                    }
                    connections.detach_all();
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
        let global_shutdown = global_shutdown_rx.clone();
        connection_sequence = connection_sequence.wrapping_add(1);
        // Max age gets per-connection jitter; idle reaping and request-count
        // retirement are reactive and need none.
        let retirement = RetirementConfig {
            age: retirement.age.map(|config| {
                config.with_jitter(jitter_state.hash_one((remote_addr, connection_sequence)))
            }),
            ..retirement
        };

        #[cfg(feature = "server-tls")]
        let tls_acceptor = tls_acceptor.clone();
        let config = connection.clone();

        connections.spawn(async move {
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
                        serve_accepted_stream(
                            tls_stream,
                            peer,
                            service,
                            config,
                            global_shutdown,
                            retirement,
                        )
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
            serve_accepted_stream(stream, peer, service, config, global_shutdown, retirement).await;
        });
    }

    // Drop the listener (refuse new conns), then signal & drain existing ones.
    drop(listener);
    // Errors only if every connection already finished (no receivers left),
    // in which case there is nothing to drain.
    let _ = global_shutdown_tx.send(true);
    while let Some(result) = connections.join_next().await {
        log_connection_task_result(result);
    }
    tracing::info!("All connections drained; shutdown complete");

    Ok(())
}

fn log_connection_task_result(result: Result<(), tokio::task::JoinError>) {
    if let Err(err) = result {
        tracing::warn!(error = %err, "Connection task ended unexpectedly");
    }
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
mod tests;
