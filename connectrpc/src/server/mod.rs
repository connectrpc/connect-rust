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
    max_connection_idle: Option<Duration>,
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
            max_connection_idle: None,
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
            max_connection_idle: None,
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
    /// seconds. This single grace period is shared by all three retirement
    /// triggers ([`with_max_connection_age`](Self::with_max_connection_age),
    /// [`with_max_connection_idle`](Self::with_max_connection_idle), and
    /// [`with_max_requests_per_connection`](Self::with_max_requests_per_connection));
    /// it has no effect unless at least one of them is set.
    #[must_use]
    pub fn with_max_connection_age_grace(mut self, grace: Duration) -> Self {
        self.max_connection_age_grace = grace;
        self
    }

    /// Retire a connection that has had no in-flight requests for `duration`.
    ///
    /// The one-step counterpart of
    /// [`BoundServer::with_max_connection_idle`]; see it for full behaviour
    /// (GOAWAY then grace-period drain). Disabled by default.
    ///
    /// # Panics
    ///
    /// Panics if `duration` is zero.
    #[must_use]
    pub fn with_max_connection_idle(mut self, duration: Duration) -> Self {
        assert!(
            !duration.is_zero(),
            "with_max_connection_idle requires a non-zero duration",
        );
        self.max_connection_idle = Some(duration);
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
            self.max_connection_idle,
            self.max_connection_age_grace,
            self.max_requests_per_connection.is_some(),
        )
    }

    fn connection_idle_config(&self) -> Option<IdleConfig> {
        build_connection_idle_config(self.max_connection_idle, self.max_connection_age_grace)
    }

    fn request_retirement_config(&self) -> Option<RequestRetirementConfig> {
        build_request_retirement_config(
            self.max_requests_per_connection,
            self.max_connection_age_grace,
        )
    }

    fn retirement_config(&self) -> RetirementConfig {
        RetirementConfig {
            age: self.connection_age_config(),
            idle: self.connection_idle_config(),
            requests: self.request_retirement_config(),
        }
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
        let retirement = self.retirement_config();
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
            retirement,
            self.http2,
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
            max_connection_idle: None,
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
            max_connection_idle: None,
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
    max_connection_idle: Option<Duration>,
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
    /// Defaults to five seconds. This single grace period is shared by all
    /// three retirement triggers —
    /// [`with_max_connection_age`](Self::with_max_connection_age),
    /// [`with_max_connection_idle`](Self::with_max_connection_idle), and
    /// [`with_max_requests_per_connection`](Self::with_max_requests_per_connection)
    /// — and applies to whichever one fires. Setting it without enabling any
    /// trigger has no effect, and the three cannot be tuned independently.
    /// Whole-server graceful shutdown still waits indefinitely for in-flight
    /// requests.
    #[must_use]
    pub fn with_max_connection_age_grace(mut self, grace: Duration) -> Self {
        self.max_connection_age_grace = grace;
        self
    }

    /// Retire a connection that has had no in-flight requests for `duration`.
    ///
    /// Disabled by default. This complements
    /// [`with_max_connection_age`](Self::with_max_connection_age): age caps a
    /// connection's total lifetime regardless of use, while idle reclaims
    /// connections that have gone quiet (clients behind NAT, bursty workloads,
    /// pooled clients holding connections they no longer need).
    ///
    /// A connection is idle when it has zero in-flight requests. The idle
    /// timer resets on activity: any request that starts, or completes, during
    /// an idle window keeps the connection alive. Once a connection stays idle
    /// for the full `duration`, the server begins graceful shutdown for it —
    /// HTTP/2 connections receive a GOAWAY, HTTP/1.1 connections have
    /// keep-alive disabled — then waits up to
    /// [`with_max_connection_age_grace`](Self::with_max_connection_age_grace)
    /// (a grace period shared with maximum age) for any straggling request
    /// before force-closing it.
    ///
    /// The idle window is evaluated lazily — it is re-checked when the timer
    /// expires rather than re-armed at the instant of each request — so a
    /// connection is retired between one and two times `duration` after its
    /// last activity. Size `duration` against that upper bound. Unlike maximum
    /// age, idle reaping applies no jitter.
    ///
    /// When both an idle timeout and a
    /// [`max age`](Self::with_max_connection_age) are configured, whichever
    /// fires first retires the connection. Whole-server graceful shutdown still
    /// waits indefinitely for in-flight requests, and is never capped by the
    /// idle grace period.
    ///
    /// # Panics
    ///
    /// Panics if `duration` is zero — a zero idle timeout is rejected rather
    /// than silently retiring every connection the instant it falls idle.
    #[must_use]
    pub fn with_max_connection_idle(mut self, duration: Duration) -> Self {
        assert!(
            !duration.is_zero(),
            "with_max_connection_idle requires a non-zero duration",
        );
        self.max_connection_idle = Some(duration);
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
        let retirement = self.retirement_config();

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
            retirement,
            self.http2,
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
        let retirement = self.retirement_config();

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
            retirement,
            self.http2,
        )
        .await
    }

    fn connection_age_config(&self) -> Option<ConnectionAgeConfig> {
        build_connection_age_config(
            self.max_connection_age,
            self.max_connection_idle,
            self.max_connection_age_grace,
            self.max_requests_per_connection.is_some(),
        )
    }

    fn connection_idle_config(&self) -> Option<IdleConfig> {
        build_connection_idle_config(self.max_connection_idle, self.max_connection_age_grace)
    }

    fn request_retirement_config(&self) -> Option<RequestRetirementConfig> {
        build_request_retirement_config(
            self.max_requests_per_connection,
            self.max_connection_age_grace,
        )
    }

    fn retirement_config(&self) -> RetirementConfig {
        RetirementConfig {
            age: self.connection_age_config(),
            idle: self.connection_idle_config(),
            requests: self.request_retirement_config(),
        }
    }
}

/// Build the per-connection age config, warning if a grace was configured
/// without anything that uses it (in which case the grace has no effect). The
/// grace period is shared with idle reaping and request-count retirement, so it
/// is only inert when none of max age, max idle, or max requests is set.
///
/// `request_retirement_active` suppresses the warning when
/// [`with_max_requests_per_connection`](BoundServer::with_max_requests_per_connection)
/// is also set, since that knob shares the same grace period and so the grace
/// does have an effect even without a max age.
fn build_connection_age_config(
    max_age: Option<Duration>,
    max_idle: Option<Duration>,
    grace: Duration,
    request_retirement_active: bool,
) -> Option<ConnectionAgeConfig> {
    let Some(max_age) = max_age else {
        if max_idle.is_none()
            && !request_retirement_active
            && grace != DEFAULT_MAX_CONNECTION_AGE_GRACE
        {
            tracing::debug!(
                "max_connection_age_grace is set but none of max_connection_age, \
                 max_connection_idle, or max_requests_per_connection are; the \
                 grace period has no effect",
            );
        }
        return None;
    };
    Some(ConnectionAgeConfig { max_age, grace })
}

/// Build the per-connection idle config. Idle reaping reuses the
/// max-connection-age grace period for its post-GOAWAY drain.
fn build_connection_idle_config(max_idle: Option<Duration>, grace: Duration) -> Option<IdleConfig> {
    max_idle.map(|idle| IdleConfig { idle, grace })
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

/// Per-connection idle-reaping configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
#[derive(Clone, Copy, Debug, Default)]
struct RetirementConfig {
    age: Option<ConnectionAgeConfig>,
    idle: Option<IdleConfig>,
    requests: Option<RequestRetirementConfig>,
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
    retirement: RetirementConfig,
    http2: Http2Config,
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
        .keep_alive(http1_keep_alive)
        .header_read_timeout(header_read_timeout);
    configure_http2(&mut builder, http2);

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
    retirement: RetirementConfig,
    http2: Http2Config,
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
                            retirement,
                            http2,
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
                retirement,
                http2,
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
mod tests;
