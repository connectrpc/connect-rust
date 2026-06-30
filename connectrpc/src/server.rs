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

/// Default TLS handshake timeout.
///
/// Bounds how long the server waits after TCP accept for a client to complete
/// the TLS handshake. Prevents slowloris-style connection-exhaustion attacks
/// where a client opens a TCP connection and stalls the handshake indefinitely,
/// holding a task and file descriptor per connection.
///
/// Override via [`Server::with_tls_handshake_timeout`] or
/// [`BoundServer::with_tls_handshake_timeout`].
#[cfg(feature = "server-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
pub const DEFAULT_TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Default HTTP/1.1 header read timeout.
///
/// Bounds how long the server waits to receive a complete set of request
/// headers, measured from the point hyper begins reading a new request on the
/// connection. On a keep-alive connection this also bounds the idle wait
/// between requests, so a peer that opens a connection (or finishes one
/// request) and then stalls without sending the next request's headers is
/// disconnected rather than holding a task and file descriptor open
/// indefinitely. This mitigates slowloris-style connection-exhaustion attacks.
///
/// Applies to HTTP/1.1 only; it does not bound idle or stalled HTTP/2
/// connections — use `with_max_connection_age` to retire those by age.
///
/// This default is applied to every accepted connection. Earlier releases
/// installed no connection timer, so the header read timeout never took
/// effect; it is active by default as of the release that introduced
/// [`Server::with_header_read_timeout`].
///
/// Override via [`Server::with_header_read_timeout`] or
/// [`BoundServer::with_header_read_timeout`]; pass `None` to disable.
pub const DEFAULT_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

const DEFAULT_MAX_CONNECTION_AGE_GRACE: Duration = Duration::from_secs(5);
const MAX_CONNECTION_AGE_JITTER_BASIS_POINTS: u128 = 10_000;
const MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS: u128 = 1_000;
const NANOS_PER_SEC: u128 = 1_000_000_000;

/// Default timeout for an HTTP/2 keepalive PING acknowledgement.
///
/// Once an HTTP/2 keepalive interval is set via
/// [`Server::with_http2_keepalive_interval`] (or the [`BoundServer`]
/// equivalent), the server waits this long for the peer to acknowledge a PING
/// before treating the connection as dead and closing it. Matches the
/// 20-second default used by grpc-go, grpc-java, and tonic. Override with
/// [`Server::with_http2_keepalive_timeout`].
pub const DEFAULT_HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// Default for HTTP/2 adaptive (BDP-based) flow-control window sizing.
///
/// Enabled by default so connections over high bandwidth-delay-product links
/// (cross-region, high-throughput streaming) are not throttled by hyper's
/// fixed 64 KiB stream/connection windows. hyper grows the window based on the
/// measured bandwidth-delay product, matching grpc-go and grpc-java, which both
/// autotune by default. The trade-off is slightly higher per-connection memory
/// under load.
///
/// Disable with [`Server::with_http2_adaptive_window`] /
/// [`BoundServer::with_http2_adaptive_window`], or override the windows
/// explicitly with the `with_http2_initial_*_window_size` setters (which turn
/// adaptive sizing off).
pub const DEFAULT_HTTP2_ADAPTIVE_WINDOW: bool = true;

/// HTTP/2 protocol configuration applied to every accepted connection's
/// hyper builder via [`configure_http2`].
///
/// `adaptive_window` and the explicit window sizes are mutually exclusive in
/// hyper: enabling adaptive sizing overrides any explicit window size. The
/// public setters keep them consistent by clearing the adaptive flag whenever
/// an explicit size is supplied.
///
/// Keepalive PING is disabled unless `keepalive_interval` is set;
/// `keepalive_timeout` is only consulted by hyper once an interval is active.
#[derive(Clone, Copy, Debug)]
struct Http2Config {
    adaptive_window: bool,
    initial_stream_window_size: Option<u32>,
    initial_connection_window_size: Option<u32>,
    max_concurrent_streams: Option<u32>,
    keepalive_interval: Option<Duration>,
    keepalive_timeout: Duration,
}

impl Default for Http2Config {
    fn default() -> Self {
        Self {
            adaptive_window: DEFAULT_HTTP2_ADAPTIVE_WINDOW,
            initial_stream_window_size: None,
            initial_connection_window_size: None,
            max_concurrent_streams: None,
            keepalive_interval: None,
            keepalive_timeout: DEFAULT_HTTP2_KEEPALIVE_TIMEOUT,
        }
    }
}

impl Http2Config {
    /// The `(stream, connection)` explicit window sizes that should actually be
    /// applied to hyper's builder.
    ///
    /// Adaptive sizing takes precedence: when it is on, no explicit window is
    /// applied, so the two never reach hyper at once regardless of the order
    /// the builder methods were called in. The public setters already clear the
    /// adaptive flag when a size is supplied, but a later
    /// `with_http2_adaptive_window(true)` can leave both set; this resolves that
    /// case deterministically in favour of adaptive sizing.
    fn effective_windows(self) -> (Option<u32>, Option<u32>) {
        if self.adaptive_window {
            (None, None)
        } else {
            (
                self.initial_stream_window_size,
                self.initial_connection_window_size,
            )
        }
    }
}

/// ConnectRPC server built on hyper.
pub struct Server {
    service: ConnectRpcService,
    http1_keep_alive: bool,
    #[cfg(feature = "server-tls")]
    tls_config: Option<Arc<rustls::ServerConfig>>,
    #[cfg(feature = "server-tls")]
    tls_handshake_timeout: std::time::Duration,
    header_read_timeout: Option<Duration>,
    max_connection_age: Option<Duration>,
    max_connection_age_grace: Duration,
    http2: Http2Config,
    max_requests_per_connection: Option<NonZeroU64>,
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
            header_read_timeout: Some(DEFAULT_HEADER_READ_TIMEOUT),
            max_connection_age: None,
            max_connection_age_grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            http2: Http2Config::default(),
            max_requests_per_connection: None,
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
            header_read_timeout: Some(DEFAULT_HEADER_READ_TIMEOUT),
            max_connection_age: None,
            max_connection_age_grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            http2: Http2Config::default(),
            max_requests_per_connection: None,
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
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
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
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls_handshake_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.tls_handshake_timeout = timeout;
        self
    }

    /// Set the HTTP/1.1 header read timeout.
    ///
    /// Defaults to [`DEFAULT_HEADER_READ_TIMEOUT`] (30 seconds). Bounds how
    /// long the server waits to read a complete set of request headers,
    /// measured from when hyper begins reading a new request; on a keep-alive
    /// connection this also bounds the idle wait between requests. A peer that
    /// connects (or finishes a request) and then stalls without sending the
    /// next request's headers is disconnected, which mitigates slowloris-style
    /// connection-exhaustion attacks. Pass `None` to disable.
    ///
    /// Applies to HTTP/1.1 only; it does not bound idle or stalled HTTP/2
    /// connections — use `with_max_connection_age` to retire those by age.
    #[must_use]
    pub fn with_header_read_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.header_read_timeout = timeout.into();
        self
    }

    /// Enable or disable HTTP/1.1 keep-alive (default: enabled).
    ///
    /// When disabled, the server sends `Connection: close` and handles
    /// only one request per TCP connection. This avoids stale-connection
    /// races where the server closes an idle connection at the same time
    /// the client sends a new request on it.
    ///
    /// HTTP/2 multiplexing is unaffected.
    #[must_use]
    pub fn with_http1_keep_alive(mut self, enabled: bool) -> Self {
        self.http1_keep_alive = enabled;
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
    /// run after envelope decoding, decompression, and protocol header
    /// parsing, and before the handler — they see the parsed request, not
    /// the wire bytes. The first interceptor registered runs **outermost**:
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

    /// Set a maximum age for each accepted HTTP connection.
    ///
    /// The one-step counterpart of
    /// [`BoundServer::with_max_connection_age`]; see it for full behaviour
    /// (±10% jitter, GOAWAY, grace period). Disabled by default.
    ///
    /// # Panics
    ///
    /// Panics if `max_age` is zero.
    #[must_use]
    pub fn with_max_connection_age(mut self, max_age: Duration) -> Self {
        assert!(
            !max_age.is_zero(),
            "with_max_connection_age requires a non-zero duration",
        );
        self.max_connection_age = Some(max_age);
        self
    }

    /// Set the grace period used after a retired connection begins shutdown.
    ///
    /// The one-step counterpart of
    /// [`BoundServer::with_max_connection_age_grace`]. Defaults to five
    /// seconds. The grace period is shared by both retirement triggers
    /// ([`with_max_connection_age`](Self::with_max_connection_age) and
    /// [`with_max_requests_per_connection`](Self::with_max_requests_per_connection));
    /// it has no effect unless at least one of them is set.
    #[must_use]
    pub fn with_max_connection_age_grace(mut self, grace: Duration) -> Self {
        self.max_connection_age_grace = grace;
        self
    }

    /// Enable or disable HTTP/2 adaptive flow-control window sizing.
    ///
    /// The one-step counterpart of
    /// [`BoundServer::with_http2_adaptive_window`]; see it for full behaviour.
    /// Enabled by default ([`DEFAULT_HTTP2_ADAPTIVE_WINDOW`]).
    #[must_use]
    pub fn with_http2_adaptive_window(mut self, enabled: bool) -> Self {
        self.http2.adaptive_window = enabled;
        self
    }

    /// Set the HTTP/2 initial stream-level flow-control window size, in bytes.
    ///
    /// The one-step counterpart of
    /// [`BoundServer::with_http2_initial_stream_window_size`]; see it for full
    /// behaviour. Supplying a size turns adaptive sizing off.
    #[must_use]
    pub fn with_http2_initial_stream_window_size(mut self, size: impl Into<Option<u32>>) -> Self {
        self.http2.initial_stream_window_size = size.into();
        if self.http2.initial_stream_window_size.is_some() {
            self.http2.adaptive_window = false;
        }
        self
    }

    /// Set the HTTP/2 initial connection-level flow-control window size, in bytes.
    ///
    /// The one-step counterpart of
    /// [`BoundServer::with_http2_initial_connection_window_size`]; see it for
    /// full behaviour. Supplying a size turns adaptive sizing off.
    #[must_use]
    pub fn with_http2_initial_connection_window_size(
        mut self,
        size: impl Into<Option<u32>>,
    ) -> Self {
        self.http2.initial_connection_window_size = size.into();
        if self.http2.initial_connection_window_size.is_some() {
            self.http2.adaptive_window = false;
        }
        self
    }

    /// Set the maximum number of concurrent HTTP/2 streams per connection.
    ///
    /// The one-step counterpart of
    /// [`BoundServer::with_max_concurrent_streams`]; see it for full
    /// behaviour. Left at hyper's default (200) when unset.
    ///
    /// # Panics
    ///
    /// Panics if `max_streams` is zero; see
    /// [`BoundServer::with_max_concurrent_streams`].
    #[must_use]
    pub fn with_max_concurrent_streams(mut self, max_streams: u32) -> Self {
        assert!(
            max_streams != 0,
            "with_max_concurrent_streams requires a non-zero value",
        );
        self.http2.max_concurrent_streams = Some(max_streams);
        self
    }

    /// Retire each accepted connection after it has dispatched `max` requests.
    ///
    /// The one-step counterpart of
    /// [`BoundServer::with_max_requests_per_connection`]; see it for full
    /// behaviour (GOAWAY, shared grace period, and why `max` is a
    /// [`NonZeroU64`]). Disabled by default.
    #[must_use]
    pub fn with_max_requests_per_connection(mut self, max: NonZeroU64) -> Self {
        self.max_requests_per_connection = Some(max);
        self
    }

    /// Set the interval between HTTP/2 keepalive PING frames.
    ///
    /// The one-step counterpart of
    /// [`BoundServer::with_http2_keepalive_interval`]; see it for full
    /// behaviour. Disabled by default.
    ///
    /// # Panics
    ///
    /// Panics if `interval` is zero.
    #[must_use]
    pub fn with_http2_keepalive_interval(mut self, interval: Duration) -> Self {
        assert!(
            !interval.is_zero(),
            "with_http2_keepalive_interval requires a non-zero duration",
        );
        self.http2.keepalive_interval = Some(interval);
        self
    }

    /// Set how long to wait for an HTTP/2 keepalive PING acknowledgement.
    ///
    /// The one-step counterpart of
    /// [`BoundServer::with_http2_keepalive_timeout`]. Defaults to
    /// [`DEFAULT_HTTP2_KEEPALIVE_TIMEOUT`] (20 seconds) and has no effect
    /// unless [`with_http2_keepalive_interval`](Self::with_http2_keepalive_interval)
    /// is also set.
    #[must_use]
    pub fn with_http2_keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.http2.keepalive_timeout = timeout;
        self
    }

    fn connection_age_config(&self) -> Option<ConnectionAgeConfig> {
        build_connection_age_config(
            self.max_connection_age,
            self.max_connection_age_grace,
            self.max_requests_per_connection.is_some(),
        )
    }

    fn request_retirement_config(&self) -> Option<RequestRetirementConfig> {
        build_request_retirement_config(
            self.max_requests_per_connection,
            self.max_connection_age_grace,
        )
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
        let connection_age = self.connection_age_config();
        let request_retirement = self.request_retirement_config();
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
            self.header_read_timeout,
            #[cfg(feature = "server-tls")]
            self.tls_handshake_timeout,
            None,
            connection_age,
            self.http2,
            request_retirement,
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
            header_read_timeout: Some(DEFAULT_HEADER_READ_TIMEOUT),
            max_connection_age: None,
            max_connection_age_grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            http2: Http2Config::default(),
            max_requests_per_connection: None,
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
            header_read_timeout: Some(DEFAULT_HEADER_READ_TIMEOUT),
            max_connection_age: None,
            max_connection_age_grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            http2: Http2Config::default(),
            max_requests_per_connection: None,
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
    header_read_timeout: Option<Duration>,
    max_connection_age: Option<Duration>,
    max_connection_age_grace: Duration,
    http2: Http2Config,
    max_requests_per_connection: Option<NonZeroU64>,
}

impl BoundServer {
    /// Get the local address the server is bound to.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Enable TLS with the given rustls server configuration.
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls(mut self, config: Arc<rustls::ServerConfig>) -> Self {
        self.tls_config = Some(config);
        self
    }

    /// Set the TLS handshake timeout.
    ///
    /// Defaults to [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`] (10 seconds).
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls_handshake_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.tls_handshake_timeout = timeout;
        self
    }

    /// Set the HTTP/1.1 header read timeout.
    ///
    /// Defaults to [`DEFAULT_HEADER_READ_TIMEOUT`] (30 seconds). Bounds how
    /// long the server waits to read a complete set of request headers,
    /// measured from when hyper begins reading a new request; on a keep-alive
    /// connection this also bounds the idle wait between requests. A peer that
    /// connects (or finishes a request) and then stalls without sending the
    /// next request's headers is disconnected, which mitigates slowloris-style
    /// connection-exhaustion attacks. Pass `None` to disable.
    ///
    /// Applies to HTTP/1.1 only; it does not bound idle or stalled HTTP/2
    /// connections — use `with_max_connection_age` to retire those by age.
    #[must_use]
    pub fn with_header_read_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.header_read_timeout = timeout.into();
        self
    }

    /// Enable or disable HTTP/1.1 keep-alive.
    ///
    /// When disabled, the server sends `Connection: close` and handles
    /// only one request per TCP connection. This avoids stale-connection
    /// races where the server closes an idle connection at the same time
    /// the client sends a new request on it.
    ///
    /// HTTP/2 multiplexing is unaffected.
    #[must_use]
    pub fn with_http1_keep_alive(mut self, enabled: bool) -> Self {
        self.http1_keep_alive = enabled;
        self
    }

    /// Set a maximum age for each accepted HTTP connection.
    ///
    /// Disabled by default. When enabled, the age is measured from the start
    /// of HTTP serving (after any TLS handshake) and each connection gets a
    /// symmetric ±10% jitter to avoid reconnect bursts. Once the age expires,
    /// the server begins graceful shutdown for that connection — HTTP/2
    /// connections receive a GOAWAY, HTTP/1.1 connections have keep-alive
    /// disabled — then waits up to
    /// [`with_max_connection_age_grace`](Self::with_max_connection_age_grace)
    /// for in-flight requests before force-closing it.
    ///
    /// # Panics
    ///
    /// Panics if `max_age` is zero — a zero age is rejected rather than
    /// silently retiring every connection the instant it starts serving.
    #[must_use]
    pub fn with_max_connection_age(mut self, max_age: Duration) -> Self {
        assert!(
            !max_age.is_zero(),
            "with_max_connection_age requires a non-zero duration",
        );
        self.max_connection_age = Some(max_age);
        self
    }

    /// Set the grace period used after a retired connection begins shutdown.
    ///
    /// Defaults to five seconds. The grace period is shared by both retirement
    /// triggers — [`with_max_connection_age`](Self::with_max_connection_age) and
    /// [`with_max_requests_per_connection`](Self::with_max_requests_per_connection)
    /// — and applies to whichever one fires. Setting it without enabling either
    /// trigger has no effect. Whole-server graceful shutdown still waits
    /// indefinitely for in-flight requests.
    #[must_use]
    pub fn with_max_connection_age_grace(mut self, grace: Duration) -> Self {
        self.max_connection_age_grace = grace;
        self
    }

    /// Enable or disable HTTP/2 adaptive flow-control window sizing.
    ///
    /// Enabled by default ([`DEFAULT_HTTP2_ADAPTIVE_WINDOW`]). When enabled,
    /// hyper grows the stream and connection flow-control windows based on the
    /// measured bandwidth-delay product, which improves throughput on
    /// high-latency, high-bandwidth links at the cost of slightly higher
    /// per-connection memory under load.
    ///
    /// Adaptive sizing and an explicit window size are mutually exclusive:
    /// enabling adaptive sizing overrides any window set via
    /// [`with_http2_initial_stream_window_size`](Self::with_http2_initial_stream_window_size)
    /// or
    /// [`with_http2_initial_connection_window_size`](Self::with_http2_initial_connection_window_size).
    /// Whichever is set last wins.
    #[must_use]
    pub fn with_http2_adaptive_window(mut self, enabled: bool) -> Self {
        self.http2.adaptive_window = enabled;
        self
    }

    /// Set the HTTP/2 initial stream-level flow-control window size, in bytes.
    ///
    /// Controls the per-stream `SETTINGS_INITIAL_WINDOW_SIZE` advertised to
    /// clients. Supplying a size turns
    /// [adaptive sizing](Self::with_http2_adaptive_window) off, mirroring
    /// grpc-go semantics; passing `None` leaves hyper's default in place and
    /// does not change the adaptive flag. The window can be raised above
    /// hyper's 64 KiB default to improve throughput when adaptive sizing is
    /// not wanted.
    ///
    /// The adaptive toggle and the explicit window are last-write-wins: a later
    /// [`with_http2_adaptive_window(true)`](Self::with_http2_adaptive_window)
    /// re-enables autotuning and the explicit window is ignored. Per HTTP/2,
    /// the window must not exceed `2^31 - 1`; larger values are a protocol error.
    #[must_use]
    pub fn with_http2_initial_stream_window_size(mut self, size: impl Into<Option<u32>>) -> Self {
        self.http2.initial_stream_window_size = size.into();
        if self.http2.initial_stream_window_size.is_some() {
            self.http2.adaptive_window = false;
        }
        self
    }

    /// Set the HTTP/2 initial connection-level flow-control window size, in bytes.
    ///
    /// Controls the whole-connection flow-control window, which bounds the
    /// total unacknowledged data across all streams on the connection.
    /// Supplying a size turns
    /// [adaptive sizing](Self::with_http2_adaptive_window) off, mirroring
    /// grpc-go semantics; passing `None` leaves hyper's default in place and
    /// does not change the adaptive flag.
    ///
    /// The adaptive toggle and the explicit window are last-write-wins: a later
    /// [`with_http2_adaptive_window(true)`](Self::with_http2_adaptive_window)
    /// re-enables autotuning and the explicit window is ignored. Per HTTP/2,
    /// the window must not exceed `2^31 - 1`; larger values are a protocol error.
    #[must_use]
    pub fn with_http2_initial_connection_window_size(
        mut self,
        size: impl Into<Option<u32>>,
    ) -> Self {
        self.http2.initial_connection_window_size = size.into();
        if self.http2.initial_connection_window_size.is_some() {
            self.http2.adaptive_window = false;
        }
        self
    }

    /// Set the maximum number of concurrent HTTP/2 streams per connection.
    ///
    /// This maps to hyper's HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS`, which
    /// the server advertises to each peer. A client may have at most this
    /// many in-flight requests (streams) open at once on a single connection;
    /// attempts to exceed it are refused with a `REFUSED_STREAM` error and
    /// can be safely retried. The setting has no effect on HTTP/1.1
    /// connections, which are not multiplexed.
    ///
    /// Left at hyper's default (200) when unset. Raise it for high-fan-in
    /// internal services that multiplex many concurrent RPCs over one
    /// connection, or lower it as an additional hardening measure when
    /// serving less-trusted clients.
    ///
    /// # Panics
    ///
    /// Panics if `max_streams` is zero — advertising a limit of zero refuses
    /// every stream, leaving a server that accepts connections but rejects
    /// all requests. It is rejected at configuration time rather than
    /// silently producing a dead server.
    #[must_use]
    pub fn with_max_concurrent_streams(mut self, max_streams: u32) -> Self {
        assert!(
            max_streams != 0,
            "with_max_concurrent_streams requires a non-zero value",
        );
        self.http2.max_concurrent_streams = Some(max_streams);
        self
    }

    /// Retire each accepted connection after it has dispatched `max` requests.
    ///
    /// Disabled by default. The request count is per-connection: every
    /// inbound request (each HTTP/2 stream, or each HTTP/1.1 request) is
    /// counted, and once the `max`th request has been dispatched the server
    /// begins graceful shutdown for that connection — HTTP/2 connections
    /// receive a GOAWAY, HTTP/1.1 connections have keep-alive disabled — then
    /// waits up to
    /// [`with_max_connection_age_grace`](Self::with_max_connection_age_grace)
    /// for in-flight requests before force-closing it. The `max`th request
    /// itself still completes; subsequent requests are turned away.
    ///
    /// `max` is a soft floor rather than an exact cap: under HTTP/2 a client
    /// may open several streams concurrently before the GOAWAY takes effect, so
    /// the connection is retired at or after the `max`th request, not strictly
    /// at it.
    ///
    /// This is the count-based complement of
    /// [`with_max_connection_age`](Self::with_max_connection_age); both may be
    /// set at once, in which case whichever trigger fires first retires the
    /// connection. Whole-server graceful shutdown still drains in-flight
    /// requests indefinitely.
    ///
    /// `max` is a [`NonZeroU64`] so that "retire after zero requests" — which
    /// would refuse every connection before it served anything — is
    /// unrepresentable. (This differs from
    /// [`with_max_connection_age`](Self::with_max_connection_age), which takes a
    /// plain [`Duration`] and panics on a zero value.)
    #[must_use]
    pub fn with_max_requests_per_connection(mut self, max: NonZeroU64) -> Self {
        self.max_requests_per_connection = Some(max);
        self
    }

    /// Set the interval between HTTP/2 keepalive PING frames sent on an
    /// otherwise idle connection.
    ///
    /// Disabled by default. When set, the server sends a PING after the
    /// connection has been idle for `interval` and, if the peer fails to
    /// acknowledge it within
    /// [`with_http2_keepalive_timeout`](Self::with_http2_keepalive_timeout),
    /// closes the connection. This detects dead or half-open peers (NAT
    /// timeout, client crash, network partition) on long-lived
    /// server-streaming or bidirectional connections that would otherwise sit
    /// half-open until the OS TCP timeout, holding a task and file descriptor.
    ///
    /// Affects HTTP/2 connections only; HTTP/1.1 is unaffected. Note the
    /// spelling difference from the HTTP/1.1 toggle
    /// [`with_http1_keep_alive`](Self::with_http1_keep_alive) (`keep_alive`):
    /// these HTTP/2 knobs use `keepalive` as a single word.
    ///
    /// # Panics
    ///
    /// Panics if `interval` is zero — a zero interval would request an
    /// unbounded PING flood rather than periodic keepalives.
    #[must_use]
    pub fn with_http2_keepalive_interval(mut self, interval: Duration) -> Self {
        assert!(
            !interval.is_zero(),
            "with_http2_keepalive_interval requires a non-zero duration",
        );
        self.http2.keepalive_interval = Some(interval);
        self
    }

    /// Set how long to wait for an HTTP/2 keepalive PING acknowledgement
    /// before closing the connection.
    ///
    /// Defaults to [`DEFAULT_HTTP2_KEEPALIVE_TIMEOUT`] (20 seconds). This only
    /// takes effect once
    /// [`with_http2_keepalive_interval`](Self::with_http2_keepalive_interval)
    /// is set — setting it without an interval has no effect.
    #[must_use]
    pub fn with_http2_keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.http2.keepalive_timeout = timeout;
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
        let connection_age = self.connection_age_config();
        let request_retirement = self.request_retirement_config();

        #[cfg(feature = "server-tls")]
        let tls_acceptor = self.tls_config.map(tokio_rustls::TlsAcceptor::from);
        #[cfg(not(feature = "server-tls"))]
        let tls_acceptor: Option<()> = None;

        serve_with_listener(
            self.listener,
            service,
            tls_acceptor,
            self.http1_keep_alive,
            self.header_read_timeout,
            #[cfg(feature = "server-tls")]
            self.tls_handshake_timeout,
            None,
            connection_age,
            self.http2,
            request_retirement,
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
        let connection_age = self.connection_age_config();
        let request_retirement = self.request_retirement_config();

        #[cfg(feature = "server-tls")]
        let tls_acceptor = self.tls_config.map(tokio_rustls::TlsAcceptor::from);
        #[cfg(not(feature = "server-tls"))]
        let tls_acceptor: Option<()> = None;

        serve_with_listener(
            self.listener,
            service,
            tls_acceptor,
            self.http1_keep_alive,
            self.header_read_timeout,
            #[cfg(feature = "server-tls")]
            self.tls_handshake_timeout,
            Some(Box::pin(signal)),
            connection_age,
            self.http2,
            request_retirement,
        )
        .await
    }

    fn connection_age_config(&self) -> Option<ConnectionAgeConfig> {
        build_connection_age_config(
            self.max_connection_age,
            self.max_connection_age_grace,
            self.max_requests_per_connection.is_some(),
        )
    }

    fn request_retirement_config(&self) -> Option<RequestRetirementConfig> {
        build_request_retirement_config(
            self.max_requests_per_connection,
            self.max_connection_age_grace,
        )
    }
}

/// Build the per-connection age config, warning if a grace was configured
/// without a max age (in which case the grace has no effect).
///
/// `request_retirement_active` suppresses the warning when
/// [`with_max_requests_per_connection`](BoundServer::with_max_requests_per_connection)
/// is also set, since that knob shares the same grace period and so the grace
/// does have an effect even without a max age.
fn build_connection_age_config(
    max_age: Option<Duration>,
    grace: Duration,
    request_retirement_active: bool,
) -> Option<ConnectionAgeConfig> {
    let Some(max_age) = max_age else {
        if grace != DEFAULT_MAX_CONNECTION_AGE_GRACE && !request_retirement_active {
            tracing::debug!(
                "max_connection_age_grace is set but max_connection_age is not; \
                 the grace period has no effect",
            );
        }
        return None;
    };
    Some(ConnectionAgeConfig { max_age, grace })
}

/// Build the per-connection request-count retirement config. The grace period
/// is shared with [`with_max_connection_age_grace`](BoundServer::with_max_connection_age_grace).
fn build_request_retirement_config(
    max_requests: Option<NonZeroU64>,
    grace: Duration,
) -> Option<RequestRetirementConfig> {
    max_requests.map(|max| RequestRetirementConfig { max, grace })
}

/// Type alias for the panic-catching wrapper around ConnectRpcService, used
/// by the per-connection task. Writing this out inline below would be verbose.
type WrappedService<D> = tower_http::catch_panic::CatchPanic<
    ConnectRpcService<D>,
    fn(Box<dyn Any + Send>) -> Response<Full<Bytes>>,
>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

/// Per-connection request-count retirement settings.
///
/// `max` is the number of requests a connection may serve before it is retired
/// via graceful shutdown; `grace` is how long in-flight requests are allowed to
/// finish afterwards (shared with the max-age grace period).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RequestRetirementConfig {
    max: NonZeroU64,
    grace: Duration,
}

/// Serve HTTP requests on an already-accepted stream.
///
/// Generic over the IO type so it works for both plain TCP and TLS streams.
/// Logs connection outcome at trace level.
///
/// `peer` is inserted into every request's extensions so handlers can read
/// the remote address (and TLS client cert chain, if any) via
/// `ctx.peer_addr()` / `ctx.peer_certs()`.
// Each accepted-connection knob is forwarded verbatim from the accept loop;
// see the matching allow on `serve_with_listener`.
#[allow(clippy::too_many_arguments)]
async fn serve_accepted_stream<D, S>(
    io: S,
    peer: PeerInfo,
    service: Arc<WrappedService<D>>,
    http1_keep_alive: bool,
    header_read_timeout: Option<Duration>,
    global_shutdown: watch::Receiver<bool>,
    connection_age: Option<ConnectionAgeConfig>,
    http2: Http2Config,
    request_retirement: Option<RequestRetirementConfig>,
) where
    D: Dispatcher,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tracing::trace!(remote_addr = %peer.addr, "Accepted new connection");

    // When request-count retirement is enabled, the service counts every
    // dispatched request and flips this watch channel once the limit is
    // reached; the connection lifecycle observes it and starts draining. The
    // counter lives only as long as this connection task.
    let (request_counter, request_retire) = match request_retirement {
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
    let svc = hyper::service::service_fn(move |mut req| {
        peer_for_requests.insert_into(req.extensions_mut());
        if let Some(counter) = &request_counter {
            counter.record_request();
        }
        let mut service = (*service).clone();
        async move { service.call(req).await }
    });

    let mut builder = AutoBuilder::new(TokioExecutor::new());
    // A timer is required for hyper's header read timeout (and any other
    // time-based connection behaviour) to take effect; without it the
    // configured `header_read_timeout` is silently ignored.
    builder
        .http1()
        .timer(TokioTimer::new())
        .keep_alive(http1_keep_alive)
        .header_read_timeout(header_read_timeout);
    configure_http2(&mut builder, http2);

    let conn = builder.serve_connection(TokioIo::new(io), svc).into_owned();
    serve_connection_with_lifecycle(
        conn,
        peer.addr,
        global_shutdown,
        connection_age,
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
/// [`Http2Config::effective_windows`]), so the two never reach hyper at once and
/// the precedence does not depend on hyper's internal call ordering.
fn configure_http2(builder: &mut AutoBuilder<TokioExecutor>, config: Http2Config) {
    let mut http2 = builder.http2();
    http2.adaptive_window(config.adaptive_window);
    let (stream_window, connection_window) = config.effective_windows();
    if let Some(size) = stream_window {
        http2.initial_stream_window_size(size);
    }
    if let Some(size) = connection_window {
        http2.initial_connection_window_size(size);
    }
    if let Some(max) = config.max_concurrent_streams {
        http2.max_concurrent_streams(max);
    }
    // Keepalive is opt-in: when no interval is set, leave hyper's default
    // (disabled) untouched. When enabled, a timer must be installed — hyper's
    // HTTP/2 keepalive requires one and panics the connection task without it.
    if let Some(interval) = config.keepalive_interval {
        http2
            .timer(TokioTimer::new())
            .keep_alive_interval(interval)
            .keep_alive_timeout(config.keepalive_timeout);
    }
}

fn serve_connection_with_lifecycle<C>(
    conn: C,
    remote_addr: SocketAddr,
    global_shutdown: watch::Receiver<bool>,
    connection_age: Option<ConnectionAgeConfig>,
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
    /// Resolves when the per-connection request count reaches its limit; the
    /// `Duration` is the grace period to drain with once it fires.
    requests: Option<(RetirementSignal, Duration)>,
    state: ConnectionLifecycleState,
}

enum ConnectionLifecycleState {
    Serving,
    GlobalDraining,
    /// Draining after a per-connection retirement trigger (max age or max
    /// requests). In-flight requests get `grace` to finish before the
    /// connection is force-closed.
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

// This internal accept loop carries one parameter per connection-level config
// knob (TLS, keep-alive, connection age, HTTP/2 flow control, ...), so it
// exceeds clippy's default argument count. The parameters are all plumbing for
// the same call; grouping them into a struct would not improve clarity here.
#[allow(clippy::too_many_arguments)]
async fn serve_with_listener<D: Dispatcher>(
    listener: TcpListener,
    service: ConnectRpcService<D>,
    tls_acceptor: MaybeTlsAcceptor,
    http1_keep_alive: bool,
    header_read_timeout: Option<Duration>,
    #[cfg(feature = "server-tls")] tls_handshake_timeout: std::time::Duration,
    shutdown: ShutdownSignal,
    connection_age: Option<ConnectionAgeConfig>,
    http2: Http2Config,
    request_retirement: Option<RequestRetirementConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Mirror the connection-age diagnostic: a timeout without an interval is a
    // configuration mistake (keepalive stays disabled), so surface it.
    if http2.keepalive_interval.is_none()
        && http2.keepalive_timeout != DEFAULT_HTTP2_KEEPALIVE_TIMEOUT
    {
        tracing::debug!(
            "http2_keepalive_timeout is set but http2_keepalive_interval is not; \
             HTTP/2 keepalive stays disabled and the timeout has no effect",
        );
    }

    // Wrap the service with panic handling to convert panics to 500 responses
    let service: WrappedService<D> = ServiceBuilder::new()
        .layer(CatchPanicLayer::custom(panic_handler as fn(_) -> _))
        .service(service);
    let service = Arc::new(service);

    #[cfg(feature = "server-tls")]
    let tls_acceptor = tls_acceptor.map(Arc::new);
    #[cfg(not(feature = "server-tls"))]
    let _ = tls_acceptor; // always None; silence unused warning

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
        let connection_age = connection_age.map(|config| {
            config.with_jitter(jitter_state.hash_one((remote_addr, connection_sequence)))
        });

        #[cfg(feature = "server-tls")]
        let tls_acceptor = tls_acceptor.clone();

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
                            http1_keep_alive,
                            header_read_timeout,
                            global_shutdown,
                            connection_age,
                            http2,
                            request_retirement,
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
            serve_accepted_stream(
                stream,
                peer,
                service,
                http1_keep_alive,
                header_read_timeout,
                global_shutdown,
                connection_age,
                http2,
                request_retirement,
            )
            .await;
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

    const KEEPALIVE_ECHO_REQ: &[u8] = concat!(
        "POST /svc/Echo HTTP/1.1\r\n",
        "Host: localhost\r\n",
        "Content-Type: application/proto\r\n",
        "Content-Length: 0\r\n",
        "Connection: keep-alive\r\n",
        "\r\n",
    )
    .as_bytes();

    #[test]
    fn test_server_creation() {
        let router = Router::new();
        let _server = Server::new(router);
    }

    /// `Server` proxies the dispatch-config builders so users don't have to
    /// drop down to `Server::from_service(ConnectRpcService::new(...).with_*())`.
    /// Exercises the chain and verifies the readable knobs (`limits()`, the
    /// `http1_keep_alive` field) round-trip; the compression knobs have no
    /// public read path so the test only confirms the builders compile and
    /// chain.
    #[test]
    fn test_server_dispatch_config_proxies() {
        use crate::service::Limits;
        use crate::{CompressionPolicy, CompressionRegistry};

        let limits = Limits {
            max_request_body_size: 1024,
            max_message_size: 512,
        };
        let server = Server::new(Router::new())
            .with_limits(limits.clone())
            .with_compression(CompressionRegistry::default())
            .with_compression_policy(CompressionPolicy::default().min_size(8192))
            .with_http1_keep_alive(false);

        assert_eq!(server.service.limits().max_request_body_size, 1024);
        assert_eq!(server.service.limits().max_message_size, 512);
        assert!(!server.http1_keep_alive);
    }

    /// `Server::with_interceptor` / `with_interceptor_arc` must reach the
    /// underlying `ConnectRpcService` chain. The interceptor list has no
    /// public read path, so the test pins delegation through `Arc` strong
    /// counts: registering a shared `Arc<dyn Interceptor>` on the `Server`
    /// must bump the count exactly as registering it on the service
    /// directly would, and dropping the `Server` must release it.
    #[test]
    fn test_server_interceptor_proxies() {
        struct Noop;
        #[async_trait::async_trait]
        impl crate::Interceptor for Noop {}

        let shared: Arc<dyn crate::Interceptor> = Arc::new(Noop);
        assert_eq!(Arc::strong_count(&shared), 1);

        let server = Server::new(Router::new())
            // `with_interceptor` Arc::new()s internally; only proves the
            // proxy compiles and chains.
            .with_interceptor(Noop)
            // `with_interceptor_arc` must store a clone of `shared`.
            .with_interceptor_arc(Arc::clone(&shared));
        assert_eq!(
            Arc::strong_count(&shared),
            2,
            "Server::with_interceptor_arc must reach the underlying service"
        );

        drop(server);
        assert_eq!(Arc::strong_count(&shared), 1);
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

    #[test]
    fn max_connection_age_jitter_stays_within_bounds() {
        let samples = [0, 1, u64::MAX / 2, u64::MAX - 1, u64::MAX];
        let ages = [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_secs(10),
            Duration::MAX,
        ];

        assert_eq!(
            jitter_connection_age(Duration::from_secs(10), 0),
            Duration::from_secs(9)
        );
        assert_eq!(
            jitter_connection_age(Duration::from_secs(10), u64::MAX),
            Duration::from_secs(11)
        );

        for age in ages {
            for sample in samples {
                let jittered = jitter_connection_age(age, sample);
                if age.is_zero() {
                    assert_eq!(jittered, Duration::ZERO);
                    continue;
                }

                assert!(
                    jittered
                        .as_nanos()
                        .saturating_mul(MAX_CONNECTION_AGE_JITTER_BASIS_POINTS)
                        >= age.as_nanos().saturating_mul(
                            MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
                                - MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS
                        ),
                    "{jittered:?} was below the 90% jitter bound for {age:?}"
                );
                assert!(
                    jittered
                        .as_nanos()
                        .saturating_mul(MAX_CONNECTION_AGE_JITTER_BASIS_POINTS)
                        <= age.as_nanos().saturating_mul(
                            MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
                                + MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS
                        ),
                    "{jittered:?} was above the 110% jitter bound for {age:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn max_connection_age_builder_defaults_and_overrides() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Server::from_listener(listener);
        assert_eq!(bound.max_connection_age, None);
        assert_eq!(
            bound.max_connection_age_grace,
            DEFAULT_MAX_CONNECTION_AGE_GRACE
        );

        let bound = bound
            .with_max_connection_age(Duration::from_secs(30))
            .with_max_connection_age_grace(Duration::ZERO);
        assert_eq!(bound.max_connection_age, Some(Duration::from_secs(30)));
        assert_eq!(bound.max_connection_age_grace, Duration::ZERO);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound =
            Server::from_listener(listener).with_max_connection_age_grace(Duration::from_secs(2));
        assert_eq!(bound.max_connection_age, None);
        assert_eq!(bound.max_connection_age_grace, Duration::from_secs(2));
    }

    #[test]
    fn server_max_connection_age_builder_threads_through() {
        let server = Server::new(Router::new());
        assert_eq!(server.max_connection_age, None);
        assert_eq!(server.connection_age_config(), None);

        let server = Server::new(Router::new())
            .with_max_connection_age(Duration::from_secs(30))
            .with_max_connection_age_grace(Duration::from_secs(2));
        assert_eq!(
            server.connection_age_config(),
            Some(ConnectionAgeConfig {
                max_age: Duration::from_secs(30),
                grace: Duration::from_secs(2),
            })
        );
    }

    #[test]
    #[should_panic(expected = "non-zero duration")]
    fn with_max_connection_age_rejects_zero() {
        let _ = Server::new(Router::new()).with_max_connection_age(Duration::ZERO);
    }

    #[test]
    fn header_read_timeout_builder_defaults_and_overrides() {
        // Default is on at DEFAULT_HEADER_READ_TIMEOUT.
        let server = Server::new(Router::new());
        assert_eq!(
            server.header_read_timeout,
            Some(DEFAULT_HEADER_READ_TIMEOUT)
        );

        // An explicit value overrides the default.
        let server =
            Server::new(Router::new()).with_header_read_timeout(Some(Duration::from_secs(5)));
        assert_eq!(server.header_read_timeout, Some(Duration::from_secs(5)));

        // `None` disables it.
        let server = Server::new(Router::new()).with_header_read_timeout(None::<Duration>);
        assert_eq!(server.header_read_timeout, None);
    }

    #[tokio::test]
    async fn bound_server_header_read_timeout_builder_defaults_and_overrides() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Server::from_listener(listener);
        assert_eq!(bound.header_read_timeout, Some(DEFAULT_HEADER_READ_TIMEOUT));

        let bound = bound.with_header_read_timeout(Some(Duration::from_secs(2)));
        assert_eq!(bound.header_read_timeout, Some(Duration::from_secs(2)));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Server::from_listener(listener).with_header_read_timeout(None::<Duration>);
        assert_eq!(bound.header_read_timeout, None);
    }

    /// A peer that opens a connection and sends an incomplete header block must
    /// be disconnected once the header read timeout elapses, rather than
    /// holding the connection (and its task and file descriptor) open forever.
    #[tokio::test(start_paused = true)]
    async fn header_read_timeout_closes_stalled_connection() {
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_header_read_timeout(Some(Duration::from_secs(10)));
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        // Send a partial request that never terminates the header block, so
        // hyper stays in "reading request headers" and arms the timeout.
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"POST /svc/Echo HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .unwrap();

        // Relies on tokio's paused-clock auto-advance: once the connection
        // task has armed the header-read timer and parked, the runtime
        // advances to fire it. The explicit advance keeps the intent obvious.
        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;

        let mut buf = [0; 1];
        let read = stream.read(&mut buf).await.unwrap();
        assert_eq!(
            read, 0,
            "stalled connection stayed open past the header read timeout"
        );

        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    /// A complete request well within the header read timeout is served
    /// normally — the timer must not interfere with healthy traffic.
    #[tokio::test]
    async fn header_read_timeout_allows_prompt_requests() {
        let router = Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                |_ctx: crate::RequestContext, _req: buffa_types::Empty| async move {
                    crate::Response::ok(buffa_types::Empty::default())
                },
            ),
        );
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_header_read_timeout(Some(Duration::from_secs(30)));
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(ECHO_REQ).await.unwrap();
        let resp = read_http1_response(&mut stream).await;
        assert!(
            resp.starts_with(b"HTTP/1.1 2"),
            "expected 2xx, got: {}",
            String::from_utf8_lossy(&resp[..resp.len().min(80)])
        );

        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[test]
    fn http2_config_default_enables_adaptive_window() {
        let config = Http2Config::default();
        assert!(config.adaptive_window);
        assert_eq!(config.adaptive_window, DEFAULT_HTTP2_ADAPTIVE_WINDOW);
        assert_eq!(config.initial_stream_window_size, None);
        assert_eq!(config.initial_connection_window_size, None);
    }

    #[test]
    fn server_http2_builder_defaults_match_adaptive_on() {
        let server = Server::new(Router::new());
        assert!(server.http2.adaptive_window);
        assert_eq!(server.http2.initial_stream_window_size, None);
        assert_eq!(server.http2.initial_connection_window_size, None);

        // `from_service` must seed the same defaults as `new`.
        let from_service = Server::from_service(ConnectRpcService::new(Router::new()));
        assert!(from_service.http2.adaptive_window);
    }

    #[tokio::test]
    async fn bound_server_http2_builder_defaults_match_adaptive_on() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Server::from_listener(listener);
        assert!(bound.http2.adaptive_window);
        assert_eq!(bound.http2.initial_stream_window_size, None);
        assert_eq!(bound.http2.initial_connection_window_size, None);

        let bound = Server::bind("127.0.0.1:0").await.unwrap();
        assert!(bound.http2.adaptive_window);
    }

    #[test]
    fn with_http2_adaptive_window_toggles_flag() {
        let server = Server::new(Router::new()).with_http2_adaptive_window(false);
        assert!(!server.http2.adaptive_window);

        let server = server.with_http2_adaptive_window(true);
        assert!(server.http2.adaptive_window);
    }

    #[test]
    fn explicit_stream_window_disables_adaptive() {
        let server = Server::new(Router::new()).with_http2_initial_stream_window_size(1 << 20);
        assert_eq!(server.http2.initial_stream_window_size, Some(1 << 20));
        assert!(
            !server.http2.adaptive_window,
            "an explicit stream window must turn adaptive sizing off"
        );
    }

    #[test]
    fn explicit_connection_window_disables_adaptive() {
        let server = Server::new(Router::new()).with_http2_initial_connection_window_size(2 << 20);
        assert_eq!(server.http2.initial_connection_window_size, Some(2 << 20));
        assert!(
            !server.http2.adaptive_window,
            "an explicit connection window must turn adaptive sizing off"
        );
    }

    #[test]
    fn clearing_window_with_none_keeps_adaptive_flag() {
        // Passing `None` must not flip the adaptive flag in either direction.
        let server = Server::new(Router::new())
            .with_http2_initial_stream_window_size(None)
            .with_http2_initial_connection_window_size(None);
        assert!(server.http2.adaptive_window);
        assert_eq!(server.http2.initial_stream_window_size, None);
        assert_eq!(server.http2.initial_connection_window_size, None);
    }

    #[test]
    fn re_enabling_adaptive_after_explicit_window_wins() {
        // The setters are last-write-wins: re-enabling adaptive after setting a
        // window leaves the window stored but turns adaptive back on, matching
        // the documented precedence (and hyper, where adaptive overrides the
        // explicit window).
        let server = Server::new(Router::new())
            .with_http2_initial_stream_window_size(1 << 20)
            .with_http2_adaptive_window(true);
        assert!(server.http2.adaptive_window);
        assert_eq!(server.http2.initial_stream_window_size, Some(1 << 20));
        // ...and the stored window must not reach hyper while adaptive is on.
        assert_eq!(server.http2.effective_windows(), (None, None));
    }

    #[test]
    fn effective_windows_resolves_adaptive_precedence() {
        // Default (adaptive on): no explicit window reaches hyper.
        assert_eq!(Http2Config::default().effective_windows(), (None, None));

        // Adaptive explicitly off but no sizes set: still nothing to apply.
        let off = Server::new(Router::new()).with_http2_adaptive_window(false);
        assert_eq!(off.http2.effective_windows(), (None, None));

        // Adaptive off with explicit sizes: both windows are applied.
        let fixed = Server::new(Router::new())
            .with_http2_initial_stream_window_size(1 << 20)
            .with_http2_initial_connection_window_size(2 << 20);
        assert!(!fixed.http2.adaptive_window);
        assert_eq!(
            fixed.http2.effective_windows(),
            (Some(1 << 20), Some(2 << 20))
        );
    }

    #[tokio::test]
    async fn bound_server_http2_window_setters_thread_through() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Server::from_listener(listener)
            .with_http2_initial_stream_window_size(512 * 1024)
            .with_http2_initial_connection_window_size(1024 * 1024);
        assert_eq!(bound.http2.initial_stream_window_size, Some(512 * 1024));
        assert_eq!(
            bound.http2.initial_connection_window_size,
            Some(1024 * 1024)
        );
        assert!(!bound.http2.adaptive_window);
    }

    /// End-to-end check that explicit window knobs reach hyper's builder
    /// (`configure_http2`) without breaking the connection: a server with
    /// custom stream/connection windows still completes an HTTP/2 request.
    #[tokio::test]
    async fn http2_explicit_windows_serve_request() {
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_http2_initial_stream_window_size(256 * 1024)
            .with_http2_initial_connection_window_size(512 * 1024);
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);

        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Unknown"))
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        // The route is unknown; we only need the response to resolve, which
        // proves the connection negotiated and served under the configured
        // flow-control windows.
        let _resp = resp.await.expect("h2 request failed");

        drop(send_request);
        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
        h2_task.await.expect("h2 connection task panicked").ok();
    }

    /// Same as above but with adaptive window left on (the default), proving the
    /// default `configure_http2` path also serves requests cleanly.
    #[tokio::test]
    async fn http2_adaptive_window_default_serves_request() {
        let bound = Server::bind("127.0.0.1:0").await.unwrap();
        assert!(bound.http2.adaptive_window);
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);

        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Unknown"))
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        // The route is unknown; we only need the response to resolve, which
        // proves the connection negotiated and served under the configured
        // flow-control windows.
        let _resp = resp.await.expect("h2 request failed");

        drop(send_request);
        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
        h2_task.await.expect("h2 connection task panicked").ok();
    }

    #[tokio::test]
    async fn http2_keepalive_builder_defaults_and_overrides() {
        // BoundServer: disabled by default, default timeout.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Server::from_listener(listener);
        assert_eq!(bound.http2.keepalive_interval, None);
        assert_eq!(
            bound.http2.keepalive_timeout,
            DEFAULT_HTTP2_KEEPALIVE_TIMEOUT
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Server::from_listener(listener)
            .with_http2_keepalive_interval(Duration::from_secs(30))
            .with_http2_keepalive_timeout(Duration::from_secs(5));
        assert_eq!(
            bound.http2.keepalive_interval,
            Some(Duration::from_secs(30))
        );
        assert_eq!(bound.http2.keepalive_timeout, Duration::from_secs(5));

        // Setting only the timeout leaves keepalive disabled (no interval).
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound =
            Server::from_listener(listener).with_http2_keepalive_timeout(Duration::from_secs(1));
        assert_eq!(bound.http2.keepalive_interval, None);
        assert_eq!(bound.http2.keepalive_timeout, Duration::from_secs(1));
    }

    #[test]
    fn server_http2_keepalive_builder_threads_through() {
        let server = Server::new(Router::new());
        assert_eq!(server.http2.keepalive_interval, None);
        assert_eq!(
            server.http2.keepalive_timeout,
            DEFAULT_HTTP2_KEEPALIVE_TIMEOUT
        );

        let server = Server::new(Router::new())
            .with_http2_keepalive_interval(Duration::from_millis(500))
            .with_http2_keepalive_timeout(Duration::from_millis(250));
        assert_eq!(
            server.http2.keepalive_interval,
            Some(Duration::from_millis(500))
        );
        assert_eq!(server.http2.keepalive_timeout, Duration::from_millis(250));
    }

    #[test]
    #[should_panic(expected = "non-zero duration")]
    fn with_http2_keepalive_interval_rejects_zero() {
        let _ = Server::new(Router::new()).with_http2_keepalive_interval(Duration::ZERO);
    }

    /// `configure_http2` leaves keepalive untouched when no interval is set, so
    /// hyper's default (keepalive disabled) is preserved unless the user opts
    /// in. There is no public getter on the builder, so this guards the opt-in
    /// contract at the call boundary by exercising the default path without
    /// panicking.
    #[test]
    fn configure_http2_default_leaves_keepalive_disabled() {
        assert!(Http2Config::default().keepalive_interval.is_none());
        let mut builder = AutoBuilder::new(TokioExecutor::new());
        configure_http2(&mut builder, Http2Config::default());
    }

    /// A configured keepalive interval must reach hyper's HTTP/2 builder: once
    /// a peer with an active stream stops acknowledging PING frames, the server
    /// closes the connection after the keepalive timeout rather than leaving it
    /// half-open indefinitely.
    #[tokio::test]
    async fn http2_keepalive_closes_unresponsive_peer() {
        // The blocked handler keeps a stream active on the server; holding
        // `_release_tx` keeps it blocked for the whole test.
        let (router, entered_rx, _release_tx) = slow_router();
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_http2_keepalive_interval(Duration::from_millis(100))
            .with_http2_keepalive_timeout(Duration::from_millis(100));
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, mut h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Slow"))
            .header(header::CONTENT_TYPE, "application/proto")
            .body(())
            .unwrap();
        // Keep the response future alive so the stream stays open server-side.
        let (_resp, _) = send_request.send_request(req, true).unwrap();

        // Drive the connection only until the handler starts — this flushes the
        // request and opens an active server-side stream. After this point the
        // client never polls the connection again, so it cannot acknowledge the
        // server's keepalive PINGs, simulating a dead or half-open peer.
        tokio::select! {
            result = &mut h2_conn => panic!("connection closed before handler ran: {result:?}"),
            entered = entered_rx => entered.expect("handler never entered"),
        }

        // Stay frozen for longer than interval + timeout. The server PINGs,
        // gets no ack, and abruptly closes the connection.
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Resuming the driver, the connection future must resolve: the server
        // has closed the connection. Without the keepalive being plumbed
        // through, the blocked handler and frozen client would leave it open
        // forever and this timeout would elapse.
        let closed = tokio::time::timeout(Duration::from_secs(5), &mut h2_conn).await;
        assert!(
            closed.is_ok(),
            "server did not close the unresponsive connection; keepalive PINGs were not plumbed through",
        );

        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test]
    async fn max_concurrent_streams_builder_defaults_and_overrides() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Server::from_listener(listener);
        assert_eq!(bound.http2.max_concurrent_streams, None);
        let bound = bound.with_max_concurrent_streams(64);
        assert_eq!(bound.http2.max_concurrent_streams, Some(64));

        let server = Server::new(Router::new());
        assert_eq!(server.http2.max_concurrent_streams, None);
        let server = server.with_max_concurrent_streams(64);
        assert_eq!(server.http2.max_concurrent_streams, Some(64));
    }

    #[test]
    #[should_panic(expected = "non-zero value")]
    fn with_max_concurrent_streams_rejects_zero() {
        let _ = Server::new(Router::new()).with_max_concurrent_streams(0);
    }

    #[tokio::test]
    async fn max_concurrent_streams_is_advertised_in_settings() {
        // The server must advertise the configured limit to peers via the
        // HTTP/2 SETTINGS_MAX_CONCURRENT_STREAMS parameter. Read the server's
        // initial SETTINGS frame off the raw connection and assert its value.
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_concurrent_streams(7);
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let advertised = read_advertised_max_concurrent_streams(addr).await;
        assert_eq!(
            advertised,
            Some(7),
            "server did not advertise the configured max_concurrent_streams",
        );

        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test]
    async fn max_concurrent_streams_unset_uses_hyper_default() {
        // When unset, the value is left to hyper. hyper's HTTP/2 server
        // default is 200, so the advertised value must remain that default.
        // This deliberately tracks hyper's internal default: if a hyper bump
        // changes it (or stops advertising it), this canary fails so the doc
        // comments that quote "200" can be updated in lockstep.
        let bound = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let advertised = read_advertised_max_concurrent_streams(addr).await;
        assert_eq!(
            advertised,
            Some(200),
            "unset max_concurrent_streams should keep hyper's default of 200",
        );

        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    /// HTTP/2 SETTINGS_MAX_CONCURRENT_STREAMS identifier (RFC 7540 §6.5.2).
    const SETTINGS_MAX_CONCURRENT_STREAMS_ID: u16 = 0x3;

    /// Open a raw HTTP/2 connection, send the client preface plus an empty
    /// SETTINGS frame, then read the server's initial SETTINGS frame and
    /// return the advertised `MAX_CONCURRENT_STREAMS` value, if present.
    async fn read_advertised_max_concurrent_streams(addr: SocketAddr) -> Option<u32> {
        let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();

        // Client connection preface, then an empty SETTINGS frame (length 0,
        // type 0x4, flags 0, stream 0) so the server proceeds with the
        // connection.
        tcp.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        tcp.write_all(&[0, 0, 0, 0x4, 0, 0, 0, 0, 0]).await.unwrap();
        tcp.flush().await.unwrap();

        // Scan frames until the first non-ACK SETTINGS frame from the server.
        loop {
            let mut header = [0u8; 9];
            tcp.read_exact(&mut header).await.unwrap();
            let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
            let frame_type = header[3];
            let flags = header[4];

            let mut payload = vec![0u8; length];
            tcp.read_exact(&mut payload).await.unwrap();

            // SETTINGS = 0x4; skip the ACK (flag 0x1) the server sends for our
            // empty SETTINGS frame.
            if frame_type == 0x4 && flags & 0x1 == 0 {
                return parse_max_concurrent_streams(&payload);
            }
        }
    }

    /// Parse a SETTINGS frame payload (6-byte id/value entries) for the
    /// `MAX_CONCURRENT_STREAMS` value.
    fn parse_max_concurrent_streams(payload: &[u8]) -> Option<u32> {
        payload.chunks_exact(6).find_map(|entry| {
            let id = u16::from_be_bytes([entry[0], entry[1]]);
            (id == SETTINGS_MAX_CONCURRENT_STREAMS_ID)
                .then(|| u32::from_be_bytes([entry[2], entry[3], entry[4], entry[5]]))
        })
    }

    #[tokio::test]
    async fn global_shutdown_future_resolves_on_signal() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let mut fut = global_shutdown_future(rx);
        // Stays pending until the accept loop signals shutdown.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut fut)
                .await
                .is_err(),
            "shutdown future resolved before any signal",
        );
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), fut)
            .await
            .expect("shutdown future must resolve after send(true)");
    }

    #[tokio::test]
    async fn global_shutdown_future_resolves_when_sender_dropped() {
        // On a fatal accept error the accept loop drops the sender without
        // sending; connections must still observe shutdown and drain rather
        // than hang. `wait_for` returns `Err` on a closed channel, which the
        // helper treats as shutdown.
        let (tx, rx) = tokio::sync::watch::channel(false);
        let fut = global_shutdown_future(rx);
        drop(tx);
        tokio::time::timeout(Duration::from_secs(1), fut)
            .await
            .expect("shutdown future must resolve when the sender is dropped");
    }

    #[tokio::test(start_paused = true)]
    async fn max_connection_age_sends_h2_goaway_without_global_shutdown() {
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_age(Duration::from_secs(10));
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);

        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Unknown"))
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        resp.await.unwrap();

        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;

        assert!(
            h2_task.is_finished(),
            "server did not close idle h2 connection after max age"
        );
        let conn_result = h2_task.await.expect("h2 connection task panicked");
        if let Err(err) = conn_result {
            assert!(
                err.is_go_away(),
                "h2 connection ended with non-GOAWAY error: {err:?}"
            );
        }

        drop(send_request);
        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn max_connection_age_retiring_one_connection_keeps_listener_running() {
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_age(Duration::from_secs(10));
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Unknown"))
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        resp.await.unwrap();

        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        assert!(
            h2_task.is_finished(),
            "aged connection should retire without stopping listener"
        );
        h2_task.await.expect("h2 connection task panicked").ok();
        drop(send_request);

        let second = tokio::net::TcpStream::connect(addr).await;
        assert!(
            second.is_ok(),
            "listener should still accept new connections after one ages out"
        );
        drop(second);

        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn max_connection_age_inflight_stream_completes_during_grace() {
        let (router, entered_rx, release_tx) = slow_router();
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_age(Duration::from_secs(10))
            .with_max_connection_age_grace(Duration::from_secs(5));
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Slow"))
            .header(header::CONTENT_TYPE, "application/proto")
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        let resp_task = tokio::spawn(resp);
        entered_rx.await.unwrap();

        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        assert!(
            !resp_task.is_finished(),
            "response should remain in-flight during max-age grace"
        );

        release_tx.send(()).unwrap();
        yield_to_tasks().await;
        assert!(
            resp_task.is_finished(),
            "in-flight response did not complete during grace"
        );
        let resp = resp_task
            .await
            .expect("response task panicked")
            .expect("h2 request failed");
        assert!(resp.status().is_success(), "got status {}", resp.status());
        drain_h2_body(resp).await;

        drop(send_request);
        yield_to_tasks().await;
        assert!(
            h2_task.is_finished(),
            "h2 connection should close after graceful max-age drain"
        );
        h2_task.await.expect("h2 connection task panicked").ok();

        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn max_connection_age_unfinished_stream_closes_after_grace() {
        let (router, entered_rx, _release_tx) = slow_router();
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_age(Duration::from_secs(10))
            .with_max_connection_age_grace(Duration::from_secs(5));
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Slow"))
            .header(header::CONTENT_TYPE, "application/proto")
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        let resp_task = tokio::spawn(resp);
        entered_rx.await.unwrap();

        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        assert!(
            !resp_task.is_finished(),
            "unfinished stream should remain open until age grace expires"
        );

        tokio::time::advance(Duration::from_secs(6)).await;
        yield_to_tasks().await;
        assert!(
            resp_task.is_finished(),
            "unfinished in-flight stream should close after age grace"
        );
        let resp_result = resp_task.await.expect("response task panicked");
        assert!(
            resp_result.is_err(),
            "unfinished stream unexpectedly completed after max-age grace"
        );

        drop(send_request);
        yield_to_tasks().await;
        assert!(
            h2_task.is_finished(),
            "h2 connection should close after max-age grace expires"
        );
        h2_task.await.expect("h2 connection task panicked").ok();

        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn max_connection_age_http1_keep_alive_connections_retire() {
        let router = Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                |_ctx: crate::RequestContext, _req: buffa_types::Empty| async move {
                    crate::Response::ok(buffa_types::Empty::default())
                },
            ),
        );
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_age(Duration::from_secs(10));
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(KEEPALIVE_ECHO_REQ).await.unwrap();
        let resp = read_http1_response(&mut stream).await;
        assert!(
            resp.starts_with(b"HTTP/1.1 2"),
            "expected 2xx, got: {}",
            String::from_utf8_lossy(&resp[..resp.len().min(80)])
        );

        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;

        let mut buf = [0; 1];
        let read = stream.read(&mut buf).await.unwrap();
        assert_eq!(read, 0, "HTTP/1.1 keep-alive connection stayed open");

        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn max_connection_age_grace_does_not_cap_global_shutdown() {
        let (router, entered_rx, release_tx) = slow_router();
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_age(Duration::from_secs(10))
            .with_max_connection_age_grace(Duration::from_secs(1));
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Slow"))
            .header(header::CONTENT_TYPE, "application/proto")
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        let resp_task = tokio::spawn(resp);
        entered_rx.await.unwrap();

        shutdown_tx.send(()).unwrap();
        tokio::time::advance(Duration::from_secs(30)).await;
        yield_to_tasks().await;
        assert!(
            !serve.is_finished(),
            "global shutdown should not be capped by max-age grace"
        );
        assert!(
            !resp_task.is_finished(),
            "global shutdown should keep in-flight request alive"
        );

        release_tx.send(()).unwrap();
        yield_to_tasks().await;
        assert!(
            resp_task.is_finished(),
            "in-flight response did not complete after release"
        );
        let resp = resp_task
            .await
            .expect("response task panicked")
            .expect("h2 request failed");
        assert!(resp.status().is_success(), "got status {}", resp.status());
        drain_h2_body(resp).await;

        drop(send_request);
        yield_to_tasks().await;
        h2_task.await.expect("h2 connection task panicked").ok();

        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn max_connection_age_global_shutdown_during_age_grace_drains_indefinitely() {
        let (router, entered_rx, release_tx) = slow_router();
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_age(Duration::from_secs(10))
            .with_max_connection_age_grace(Duration::from_secs(1));
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Slow"))
            .header(header::CONTENT_TYPE, "application/proto")
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        let resp_task = tokio::spawn(resp);
        entered_rx.await.unwrap();

        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        assert!(
            !resp_task.is_finished(),
            "request should still be in-flight during age grace"
        );

        shutdown_tx.send(()).unwrap();
        tokio::time::advance(Duration::from_secs(30)).await;
        yield_to_tasks().await;
        assert!(
            !serve.is_finished(),
            "global shutdown during age grace should drain indefinitely"
        );
        assert!(
            !resp_task.is_finished(),
            "global shutdown during age grace should not force-close the request"
        );

        release_tx.send(()).unwrap();
        yield_to_tasks().await;
        assert!(
            resp_task.is_finished(),
            "in-flight response did not complete after release"
        );
        let resp = resp_task
            .await
            .expect("response task panicked")
            .expect("h2 request failed");
        assert!(resp.status().is_success(), "got status {}", resp.status());
        drain_h2_body(resp).await;

        drop(send_request);
        yield_to_tasks().await;
        h2_task.await.expect("h2 connection task panicked").ok();

        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test]
    async fn max_requests_per_connection_builder_defaults_and_threads_through() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Server::from_listener(listener);
        assert_eq!(bound.max_requests_per_connection, None);
        assert_eq!(bound.request_retirement_config(), None);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Server::from_listener(listener)
            .with_max_requests_per_connection(NonZeroU64::new(100).unwrap())
            .with_max_connection_age_grace(Duration::from_secs(3));
        assert_eq!(bound.max_requests_per_connection, NonZeroU64::new(100));
        assert_eq!(
            bound.request_retirement_config(),
            Some(RequestRetirementConfig {
                max: NonZeroU64::new(100).unwrap(),
                grace: Duration::from_secs(3),
            })
        );

        // `Server` mirrors the `BoundServer` knob and uses the default grace.
        let server = Server::new(Router::new());
        assert_eq!(server.max_requests_per_connection, None);
        assert_eq!(server.request_retirement_config(), None);
        let server = Server::new(Router::new())
            .with_max_requests_per_connection(NonZeroU64::new(5).unwrap());
        assert_eq!(
            server.request_retirement_config(),
            Some(RequestRetirementConfig {
                max: NonZeroU64::new(5).unwrap(),
                grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn max_requests_per_connection_retires_h2_after_limit() {
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_requests_per_connection(NonZeroU64::new(2).unwrap());
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);

        // First request stays under the limit: the connection must remain open.
        send_unary(&mut send_request, addr).await;
        yield_to_tasks().await;
        assert!(
            !h2_task.is_finished(),
            "connection retired before reaching the request limit"
        );

        // Second request reaches the limit and triggers a GOAWAY.
        send_unary(&mut send_request, addr).await;
        yield_to_tasks().await;
        assert!(
            h2_task.is_finished(),
            "connection did not retire after reaching the request limit"
        );
        let conn_result = h2_task.await.expect("h2 connection task panicked");
        if let Err(err) = conn_result {
            assert!(
                err.is_go_away(),
                "h2 connection ended with non-GOAWAY error: {err:?}"
            );
        }

        drop(send_request);
        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn max_requests_per_connection_unlimited_when_unset() {
        // No request limit configured: the connection serves many requests
        // without being retired.
        let bound = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);

        for _ in 0..5 {
            send_unary(&mut send_request, addr).await;
        }
        yield_to_tasks().await;
        assert!(
            !h2_task.is_finished(),
            "connection retired despite no request limit being configured"
        );

        drop(send_request);
        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn max_requests_per_connection_first_trigger_wins_over_age() {
        // A far-off max age combined with a request limit of one: the request
        // count must retire the connection first.
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_age(Duration::from_secs(3600))
            .with_max_requests_per_connection(NonZeroU64::new(1).unwrap());
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(Router::new(), async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        let h2_task = tokio::spawn(h2_conn);

        send_unary(&mut send_request, addr).await;
        yield_to_tasks().await;
        assert!(
            h2_task.is_finished(),
            "request limit should retire the connection before the max age"
        );
        h2_task.await.expect("h2 connection task panicked").ok();

        drop(send_request);
        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn max_requests_per_connection_retires_http1_after_limit() {
        let router = Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                |_ctx: crate::RequestContext, _req: buffa_types::Empty| async move {
                    crate::Response::ok(buffa_types::Empty::default())
                },
            ),
        );
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_requests_per_connection(NonZeroU64::new(1).unwrap());
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(KEEPALIVE_ECHO_REQ).await.unwrap();
        let resp = read_http1_response(&mut stream).await;
        assert!(
            resp.starts_with(b"HTTP/1.1 2"),
            "expected 2xx, got: {}",
            String::from_utf8_lossy(&resp[..resp.len().min(80)])
        );

        yield_to_tasks().await;
        // The single request hit the limit, so the keep-alive connection must
        // close even though the client requested keep-alive.
        let mut buf = [0; 1];
        let read = stream.read(&mut buf).await.unwrap();
        assert_eq!(
            read, 0,
            "HTTP/1.1 keep-alive connection stayed open past the request limit"
        );

        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), serve)
            .await
            .expect("server did not shut down")
            .expect("join error");
        assert!(result.is_ok(), "serve returned error: {result:?}");
    }

    /// Send one unary request over an h2 connection and await its response.
    async fn send_unary(send_request: &mut h2::client::SendRequest<Bytes>, addr: SocketAddr) {
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Unknown"))
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        resp.await.unwrap();
    }

    fn slow_router() -> (
        Router,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
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
        (router, entered_rx, release_tx)
    }

    async fn yield_to_tasks() {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
    }

    async fn drain_h2_body(mut resp: http::Response<h2::RecvStream>) {
        while let Some(chunk) = resp.body_mut().data().await {
            chunk.expect("h2 response body failed");
        }
    }

    async fn read_http1_response(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut resp = Vec::new();
        let mut buf = [0; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            assert!(read > 0, "connection closed before full response arrived");
            resp.extend_from_slice(&buf[..read]);

            let Some(header_end) = find_header_end(&resp) else {
                continue;
            };
            let body_start = header_end + 4;
            let content_length = content_length(&resp[..header_end]).unwrap_or(0);
            if resp.len() >= body_start + content_length {
                return resp;
            }
        }
    }

    fn find_header_end(bytes: &[u8]) -> Option<usize> {
        bytes.windows(4).position(|window| window == b"\r\n\r\n")
    }

    fn content_length(headers: &[u8]) -> Option<usize> {
        std::str::from_utf8(headers).ok()?.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())
                .flatten()
        })
    }

    // ========================================================================
    // PeerAddr / PeerCerts extension plumbing
    // ========================================================================

    #[tokio::test]
    async fn peer_addr_reaches_handler() {
        // Handler stashes the PeerAddr it sees into a shared slot.
        let captured: Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));
        let handler_captured = Arc::clone(&captured);
        let router = Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let cap = Arc::clone(&handler_captured);
                    async move {
                        *cap.lock().unwrap() = ctx.peer_addr();
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
        assert_eq!(peer, client_local);
    }

    /// End-to-end mTLS: client presents a cert; handler reads it from
    /// `ctx.peer_certs()` and the DER bytes round-trip.
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

        type CapturedCerts = Vec<rustls::pki_types::CertificateDer<'static>>;
        let captured: Arc<Mutex<Option<CapturedCerts>>> = Arc::new(Mutex::new(None));
        let handler_captured = Arc::clone(&captured);
        let router = Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let cap = Arc::clone(&handler_captured);
                    async move {
                        *cap.lock().unwrap() = ctx.peer_certs().map(<[_]>::to_vec);
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
        assert_eq!(certs.len(), 1);
        assert_eq!(certs[0].as_ref(), expected_client_der.as_ref());
    }
}
