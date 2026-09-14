//! Layer 4's public face: [`Server`] and [`BoundServer`], the batteries-included
//! way to run a [`ConnectRpcService`] on a TCP listener.
//!
//! Both are a [`ConnectionConfig`] + [`AcceptConfig`] around the shared accept
//! loop; `Server` also owns the service, `BoundServer` also owns the listener.
//! Every `with_*` setter forwards to the config value of the same name.

use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;

use super::AcceptConfig;
use super::Acceptor;
use super::ConnectionConfig;
use super::ConnectionInfo;
use super::accept_loop;
use super::serve_connection;
use crate::dispatcher::Dispatcher;
use crate::router::Router;
use crate::service::ConnectRpcService;

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

    /// Set the TLS handshake timeout (default [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`](super::DEFAULT_TLS_HANDSHAKE_TIMEOUT),
    /// 10 seconds). Shorthand for [`AcceptConfig::with_tls_handshake_timeout`].
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.accept = self.accept.with_tls_handshake_timeout(timeout);
        self
    }

    /// Set the HTTP/1.1 header read timeout (default
    /// [`DEFAULT_HEADER_READ_TIMEOUT`](super::DEFAULT_HEADER_READ_TIMEOUT); `None` disables). Shorthand for
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
    /// [`DEFAULT_HTTP2_ADAPTIVE_WINDOW`](super::DEFAULT_HTTP2_ADAPTIVE_WINDOW)). Shorthand for
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
    /// [`DEFAULT_HTTP2_KEEPALIVE_TIMEOUT`](super::DEFAULT_HTTP2_KEEPALIVE_TIMEOUT)). Shorthand for
    /// [`ConnectionConfig::with_http2_keepalive_timeout`].
    #[must_use]
    pub fn with_http2_keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.connection = self.connection.with_http2_keepalive_timeout(timeout);
        self
    }

    /// The method form of [`serve_connection`] for custom accept loops:
    /// serve one connection the caller accepted with this server's service
    /// and [`ConnectionConfig`].
    ///
    /// Pair it with [`Acceptor`] to keep every lifecycle guarantee of
    /// [`Server::serve`] while deciding yourself which connections to admit
    /// and where to run them: the returned future serves the connection on
    /// whichever runtime polls it, and `shutdown` resolving drains this one
    /// connection gracefully.
    pub fn serve_connection<I, F>(
        &self,
        io: I,
        info: ConnectionInfo,
        shutdown: F,
    ) -> impl Future<Output = ()> + Send + 'static + use<I, F>
    where
        I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        serve_connection(
            io,
            info,
            self.service.clone(),
            self.connection.clone(),
            shutdown,
        )
    }

    /// The service every connection is served with, including the limits,
    /// compression, deadline policy and interceptors configured on it. A
    /// custom loop that wants this service under a different
    /// [`ConnectionConfig`] or behind its own tower layers passes a clone to
    /// [`serve_connection`].
    #[must_use]
    pub fn service(&self) -> &ConnectRpcService {
        &self.service
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
        let scheme = if self.accept.is_tls() {
            "https"
        } else {
            "http"
        };
        let acceptor = Acceptor::bind(addr, self.accept).await?;
        tracing::info!("ConnectRPC server listening on {scheme}://{addr}");

        accept_loop::run(
            acceptor,
            self.service,
            self.connection,
            std::future::pending(),
        )
        .await?;
        Ok(())
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

    /// Set the TLS handshake timeout (default [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`](super::DEFAULT_TLS_HANDSHAKE_TIMEOUT),
    /// 10 seconds). Shorthand for [`AcceptConfig::with_tls_handshake_timeout`].
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.accept = self.accept.with_tls_handshake_timeout(timeout);
        self
    }

    /// Set the HTTP/1.1 header read timeout (default
    /// [`DEFAULT_HEADER_READ_TIMEOUT`](super::DEFAULT_HEADER_READ_TIMEOUT); `None` disables). Shorthand for
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
    /// [`DEFAULT_HTTP2_ADAPTIVE_WINDOW`](super::DEFAULT_HTTP2_ADAPTIVE_WINDOW)). Shorthand for
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
    /// [`DEFAULT_HTTP2_KEEPALIVE_TIMEOUT`](super::DEFAULT_HTTP2_KEEPALIVE_TIMEOUT)). Shorthand for
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
        self.serve_with_service_and_shutdown(service, std::future::pending())
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
        accept_loop::run(
            Acceptor::new(self.listener, self.accept),
            service,
            self.connection,
            signal,
        )
        .await?;
        Ok(())
    }
}
