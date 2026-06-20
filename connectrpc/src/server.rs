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
//! # Maximum Connection Age
//!
//! Use [`Server::with_max_connection_age`] (or the [`BoundServer`] equivalent)
//! to retire long-lived connections proactively — recommended behind load
//! balancers so clients reconnect periodically and traffic redistributes
//! across restarts. Each connection is sent a GOAWAY once it reaches the
//! configured age (with a ±10% jitter), then force-closed after a grace
//! period. This is independent of whole-server graceful shutdown, which still
//! drains in-flight requests indefinitely even while a connection is in its
//! age-grace window.
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

use std::any::Any;
use std::collections::hash_map::RandomState;
use std::future::Future;
use std::hash::BuildHasher;
use std::net::SocketAddr;
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

const DEFAULT_MAX_CONNECTION_AGE_GRACE: Duration = Duration::from_secs(5);
const MAX_CONNECTION_AGE_JITTER_BASIS_POINTS: u128 = 10_000;
const MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS: u128 = 1_000;
const NANOS_PER_SEC: u128 = 1_000_000_000;

/// ConnectRPC server built on hyper.
pub struct Server {
    service: ConnectRpcService,
    http1_keep_alive: bool,
    #[cfg(feature = "server-tls")]
    tls_config: Option<Arc<rustls::ServerConfig>>,
    #[cfg(feature = "server-tls")]
    tls_handshake_timeout: std::time::Duration,
    max_connection_age: Option<Duration>,
    max_connection_age_grace: Duration,
    max_connection_idle: Option<Duration>,
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
            max_connection_age: None,
            max_connection_age_grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            max_connection_idle: None,
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
            max_connection_age: None,
            max_connection_age_grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            max_connection_idle: None,
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
    /// seconds. This single grace period is shared by both
    /// [`with_max_connection_age`](Self::with_max_connection_age) and
    /// [`with_max_connection_idle`](Self::with_max_connection_idle); it has no
    /// effect unless at least one of them is set.
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

    fn connection_age_config(&self) -> Option<ConnectionAgeConfig> {
        build_connection_age_config(
            self.max_connection_age,
            self.max_connection_idle,
            self.max_connection_age_grace,
        )
    }

    fn connection_idle_config(&self) -> Option<IdleConfig> {
        build_connection_idle_config(self.max_connection_idle, self.max_connection_age_grace)
    }

    fn retirement_config(&self) -> RetirementConfig {
        RetirementConfig {
            age: self.connection_age_config(),
            idle: self.connection_idle_config(),
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
            #[cfg(feature = "server-tls")]
            self.tls_handshake_timeout,
            None,
            retirement,
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
            max_connection_age: None,
            max_connection_age_grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            max_connection_idle: None,
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
            max_connection_age: None,
            max_connection_age_grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            max_connection_idle: None,
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
    max_connection_age: Option<Duration>,
    max_connection_age_grace: Duration,
    max_connection_idle: Option<Duration>,
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
    /// Defaults to five seconds. This single grace period is shared by both
    /// [`with_max_connection_age`](Self::with_max_connection_age) and
    /// [`with_max_connection_idle`](Self::with_max_connection_idle) — setting it
    /// without either knob has no effect, and the two cannot be tuned
    /// independently. Whole-server graceful shutdown still waits indefinitely
    /// for in-flight requests.
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
            #[cfg(feature = "server-tls")]
            self.tls_handshake_timeout,
            None,
            retirement,
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
            #[cfg(feature = "server-tls")]
            self.tls_handshake_timeout,
            Some(Box::pin(signal)),
            retirement,
        )
        .await
    }

    fn connection_age_config(&self) -> Option<ConnectionAgeConfig> {
        build_connection_age_config(
            self.max_connection_age,
            self.max_connection_idle,
            self.max_connection_age_grace,
        )
    }

    fn connection_idle_config(&self) -> Option<IdleConfig> {
        build_connection_idle_config(self.max_connection_idle, self.max_connection_age_grace)
    }

    fn retirement_config(&self) -> RetirementConfig {
        RetirementConfig {
            age: self.connection_age_config(),
            idle: self.connection_idle_config(),
        }
    }
}

/// Build the per-connection age config, warning if a grace was configured
/// without anything that uses it (in which case the grace has no effect). The
/// grace period is shared with idle reaping, so it is only inert when neither a
/// max age nor a max idle is set.
fn build_connection_age_config(
    max_age: Option<Duration>,
    max_idle: Option<Duration>,
    grace: Duration,
) -> Option<ConnectionAgeConfig> {
    let Some(max_age) = max_age else {
        if max_idle.is_none() && grace != DEFAULT_MAX_CONNECTION_AGE_GRACE {
            tracing::debug!(
                "max_connection_age_grace is set but neither max_connection_age \
                 nor max_connection_idle is; the grace period has no effect",
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

/// Per-connection retirement policy: the optional max-age and max-idle limits
/// that the connection lifecycle enforces. Bundled so the accept loop and the
/// per-connection task pass a single value rather than two parallel options.
#[derive(Clone, Copy, Debug, Default)]
struct RetirementConfig {
    age: Option<ConnectionAgeConfig>,
    idle: Option<IdleConfig>,
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

/// Serve HTTP requests on an already-accepted stream.
///
/// Generic over the IO type so it works for both plain TCP and TLS streams.
/// Logs connection outcome at trace level.
///
/// `peer` is inserted into every request's extensions so handlers can read
/// the remote address (and TLS client cert chain, if any) via
/// `ctx.peer_addr()` / `ctx.peer_certs()`.
async fn serve_accepted_stream<D, S>(
    io: S,
    peer: PeerInfo,
    service: Arc<WrappedService<D>>,
    http1_keep_alive: bool,
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

    let peer_for_requests = peer.clone();
    let activity_for_requests = activity.clone();
    let svc = hyper::service::service_fn(move |mut req| {
        peer_for_requests.insert_into(req.extensions_mut());
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
    builder.http1().keep_alive(http1_keep_alive);

    let conn = builder.serve_connection(TokioIo::new(io), svc).into_owned();
    serve_connection_with_lifecycle(
        conn,
        peer.addr,
        global_shutdown,
        retirement.age,
        retirement.idle.zip(activity),
    )
    .await;
}

fn serve_connection_with_lifecycle<C>(
    conn: C,
    remote_addr: SocketAddr,
    global_shutdown: watch::Receiver<bool>,
    connection_age: Option<ConnectionAgeConfig>,
    connection_idle: Option<(IdleConfig, Arc<ConnectionActivity>)>,
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

struct ConnectionLifecycle<C: GracefulConnection> {
    conn: Pin<Box<C>>,
    remote_addr: SocketAddr,
    global_shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
    age: Option<(Pin<Box<tokio::time::Sleep>>, ConnectionAgeConfig)>,
    idle: Option<IdleTracker>,
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
    /// Draining after a per-connection retirement trigger (max age or max
    /// idle): graceful shutdown has been issued and the connection is given a
    /// grace window to finish in-flight work before being force-closed.
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

async fn serve_with_listener<D: Dispatcher>(
    listener: TcpListener,
    service: ConnectRpcService<D>,
    tls_acceptor: MaybeTlsAcceptor,
    http1_keep_alive: bool,
    #[cfg(feature = "server-tls")] tls_handshake_timeout: std::time::Duration,
    shutdown: ShutdownSignal,
    retirement: RetirementConfig,
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
        // Max age gets per-connection jitter; idle reaping is reactive and needs
        // none.
        let retirement = RetirementConfig {
            age: retirement.age.map(|config| {
                config.with_jitter(jitter_state.hash_one((remote_addr, connection_sequence)))
            }),
            idle: retirement.idle,
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
            serve_accepted_stream(
                stream,
                peer,
                service,
                http1_keep_alive,
                global_shutdown,
                retirement,
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

    // ========================================================================
    // Maximum Connection Idle
    // ========================================================================

    #[test]
    fn connection_activity_tracks_in_flight_and_epoch() {
        let activity = ConnectionActivity::default();
        assert_eq!(activity.snapshot(), (0, 0));

        let guard = ActiveRequestGuard::new(Arc::new(ConnectionActivity::default()));
        // The guard owns its own activity; exercise the shared-Arc path too.
        drop(guard);

        let shared = Arc::new(ConnectionActivity::default());
        let g1 = ActiveRequestGuard::new(Arc::clone(&shared));
        let g2 = ActiveRequestGuard::new(Arc::clone(&shared));
        // Two starts: in_flight == 2, epoch bumped twice.
        assert_eq!(shared.snapshot(), (2, 2));
        drop(g1);
        // One completion: in_flight back to 1, epoch bumped again.
        assert_eq!(shared.snapshot(), (1, 3));
        drop(g2);
        assert_eq!(shared.snapshot(), (0, 4));
    }

    #[tokio::test]
    async fn max_connection_idle_builder_defaults_and_overrides() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Server::from_listener(listener);
        assert_eq!(bound.max_connection_idle, None);
        assert_eq!(bound.connection_idle_config(), None);

        let bound = bound.with_max_connection_idle(Duration::from_secs(30));
        assert_eq!(bound.max_connection_idle, Some(Duration::from_secs(30)));
        assert_eq!(
            bound.connection_idle_config(),
            Some(IdleConfig {
                idle: Duration::from_secs(30),
                grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            })
        );
    }

    #[test]
    fn server_max_connection_idle_builder_threads_through() {
        let server = Server::new(Router::new());
        assert_eq!(server.max_connection_idle, None);
        assert_eq!(server.connection_idle_config(), None);

        // Idle reaping reuses the max-age grace period for its drain window.
        let server = Server::new(Router::new())
            .with_max_connection_idle(Duration::from_secs(30))
            .with_max_connection_age_grace(Duration::from_secs(2));
        assert_eq!(
            server.connection_idle_config(),
            Some(IdleConfig {
                idle: Duration::from_secs(30),
                grace: Duration::from_secs(2),
            })
        );
    }

    #[test]
    #[should_panic(expected = "non-zero duration")]
    fn with_max_connection_idle_rejects_zero() {
        let _ = Server::new(Router::new()).with_max_connection_idle(Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn max_connection_idle_reaps_quiet_connection() {
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_idle(Duration::from_secs(10));
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

        // One request, then the connection goes quiet. The empty router replies
        // 415 (no Content-Type), but any response establishes activity.
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/svc/Unknown"))
            .body(())
            .unwrap();
        let (resp, _) = send_request.send_request(req, true).unwrap();
        resp.await.unwrap();

        // The request fell inside the first idle window, so that window resets
        // rather than reaping: the connection must survive it.
        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        assert!(
            !h2_task.is_finished(),
            "connection reaped despite activity within the idle window"
        );

        // A second, fully quiet window elapses: now the connection is reaped.
        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        assert!(
            h2_task.is_finished(),
            "idle connection was not reaped after a quiet window"
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
    async fn max_connection_idle_inflight_request_prevents_reaping() {
        let (router, entered_rx, release_tx) = slow_router();
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_idle(Duration::from_secs(10));
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

        // A request is in flight the whole time, so the connection is never
        // idle even though the idle timeout elapses several times over.
        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        assert!(
            !h2_task.is_finished(),
            "connection with an in-flight request was retired by the idle timer"
        );
        assert!(
            !resp_task.is_finished(),
            "in-flight request unexpectedly ended"
        );

        // Let the handler finish; the connection then goes quiet and is reaped.
        release_tx.send(()).unwrap();
        yield_to_tasks().await;
        let resp = resp_task
            .await
            .expect("response task panicked")
            .expect("h2 request failed");
        assert!(resp.status().is_success(), "got status {}", resp.status());
        drain_h2_body(resp).await;

        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        assert!(
            h2_task.is_finished(),
            "connection was not reaped after the in-flight request completed"
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
    async fn max_connection_idle_fires_before_a_longer_max_age() {
        // Idle (10s) is shorter than age (60s): a quiet connection is retired by
        // the idle timer well before it would reach max age.
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_age(Duration::from_secs(60))
            .with_max_connection_idle(Duration::from_secs(10));
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

        // Two quiet idle windows (22s total) is far short of the 60s max age
        // (even with +10% jitter), so any retirement here is the idle timer.
        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        yield_to_tasks().await;
        assert!(
            h2_task.is_finished(),
            "idle timer did not retire the connection before max age"
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
