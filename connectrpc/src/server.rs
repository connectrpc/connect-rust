//! Hyper-based HTTP server for ConnectRPC.
//!
//! This module provides the HTTP server implementation that handles incoming
//! ConnectRPC requests and routes them to the appropriate handlers.
//!
//! Serving needs a tokio runtime with I/O and time enabled
//! ([`enable_all`](tokio::runtime::Builder::enable_all), as `#[tokio::main]`
//! does). These features use tokio's timer: the header-read timeout and, with
//! `server-tls`, the TLS handshake timeout (both on by default), the
//! retirement timers, HTTP/2 keepalive, and the accept loop's pause after
//! running out of file descriptors. Without time enabled, tokio panics when
//! the first of them starts.
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
//! Every per-connection setting above lives on [`ConnectionConfig`], a plain
//! value that [`Server::with_connection_config`] /
//! [`BoundServer::with_connection_config`] accept whole; the `with_*` methods
//! on the two builders are shorthand for editing it.
//!
//! # Custom Accept Loops
//!
//! [`Server::serve_connection`] serves one stream you accepted yourself with
//! everything above, described by a [`ConnectionInfo`] you build (peer
//! address, TLS client certificates, connection-scoped extensions), and
//! reports why it ended as a [`ConnectionClosed`]; the free
//! [`serve_connection`] does the same for any tower HTTP service (an
//! `axum::Router`, say) under a [`ConnectionConfig`]. Use them when the accept
//! step needs a policy the built-in loop does not have: admitting connections
//! by client identity, capping them per tenant, listening on another
//! transport, or placing connections on different runtimes.
//!
//! For hyper connection-builder knobs that [`ConnectionConfig`] does not
//! expose, drive [`ConnectRpcService`] directly from a hyper accept loop (and
//! own the whole lifecycle yourself). The crate guide's "Raw hyper" section
//! shows the `hyper_util` pattern.

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
use tower::ServiceExt;
use tower_http::catch_panic::CatchPanic;

use crate::codec::content_type;
use crate::dispatcher::Dispatcher;
use crate::error::ConnectError;
use crate::error::ErrorCode;
use crate::error::SharedSource;
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

/// What the server knows about one accepted connection before any request is
/// served on it: the remote address, over TLS the verified client certificate
/// chain, and connection-scoped [`http::Extensions`] that every request on
/// the connection will carry.
///
/// The built-in accept loop builds one per connection after the (optional)
/// TLS handshake. A custom accept loop (see [`Server::serve_connection`])
/// builds its own with [`new`](Self::new) / [`with_peer_addr`](Self::with_peer_addr)
/// and adds per-connection state — a parsed client identity, a tenant —
/// through [`extensions_mut`](Self::extensions_mut) before serving. Every
/// request then carries those extensions plus [`PeerAddr`] / `PeerCerts`,
/// mirroring [`RequestContext`](crate::RequestContext) at connection scope.
/// `PeerAddr` / `PeerCerts` always come from the typed fields here, never
/// from `extensions`: the built-in loop sets those fields from the
/// transport, and a custom loop answers for what it records in them.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ConnectionInfo {
    peer_addr: Option<SocketAddr>,
    #[cfg(feature = "server-tls")]
    peer_certs: Option<Arc<[rustls::pki_types::CertificateDer<'static>]>>,
    extensions: http::Extensions,
}

impl ConnectionInfo {
    /// Describe a connection about which nothing is known yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the peer's remote socket address.
    #[must_use]
    pub fn with_peer_addr(mut self, peer_addr: SocketAddr) -> Self {
        self.peer_addr = Some(peer_addr);
        self
    }

    /// Attach the TLS client certificate chain (leaf first) the peer
    /// presented.
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_peer_certs(
        mut self,
        certs: Arc<[rustls::pki_types::CertificateDer<'static>]>,
    ) -> Self {
        self.peer_certs = Some(certs);
        self
    }

    /// Remote socket address of the peer, or `None` when the transport has no
    /// meaningful address (in-memory streams, Unix sockets). Reaches handlers
    /// as [`PeerAddr`].
    #[must_use]
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    /// TLS client certificate chain presented by the peer (leaf first), or
    /// `None` for plaintext connections and TLS connections without client
    /// authentication. Reaches handlers as [`PeerCerts`].
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn peer_certs(&self) -> Option<&[rustls::pki_types::CertificateDer<'static>]> {
        self.peer_certs.as_deref()
    }

    /// Connection-scoped extensions: cloned into every request served on this
    /// connection, where handlers read them with `ctx.extensions().get::<T>()`.
    #[must_use]
    pub fn extensions(&self) -> &http::Extensions {
        &self.extensions
    }

    /// Mutable access to the connection-scoped extensions, for code that owns
    /// the accept step and computes per-connection state before serving.
    pub fn extensions_mut(&mut self) -> &mut http::Extensions {
        &mut self.extensions
    }

    /// The extensions stamped into every request on this connection: the
    /// connection's own extensions, with [`PeerAddr`] / [`PeerCerts`] set from
    /// the peer fields. Whatever was put under those two types is discarded
    /// first.
    fn request_extensions(&self) -> http::Extensions {
        let mut ext = self.extensions.clone();
        ext.remove::<PeerAddr>();
        if let Some(peer_addr) = self.peer_addr {
            ext.insert(PeerAddr(peer_addr));
        }
        #[cfg(feature = "server-tls")]
        {
            ext.remove::<PeerCerts>();
            if let Some(certs) = &self.peer_certs {
                ext.insert(PeerCerts(Arc::clone(certs)));
            }
        }
        ext
    }
}

/// The per-connection function registered with
/// [`Server::with_connection_extensions`] and its equivalents.
#[derive(Clone)]
pub(crate) struct ConnectionExtensionsFn(Arc<DynConnectionExtensionsFn>);

type DynConnectionExtensionsFn = dyn Fn(&ConnectionInfo, &mut http::Extensions) + Send + Sync;

impl ConnectionExtensionsFn {
    pub(crate) fn new<F>(f: F) -> Self
    where
        F: Fn(&ConnectionInfo, &mut http::Extensions) + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    /// Run the function once, before the connection is served: it reads
    /// `info`, and what it inserts joins `info`'s extensions, replacing
    /// entries of the same type. If the function panicked, logs the panic and
    /// returns it as an error; the caller then drops the connection.
    fn apply(&self, info: &mut ConnectionInfo) -> Result<(), Box<ConnectionError>> {
        let mut added = http::Extensions::new();
        let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (self.0)(info, &mut added);
        }));
        match ran {
            Ok(()) => {
                info.extensions.extend(added);
                Ok(())
            }
            Err(panic) => {
                let message = panic_message(&*panic).unwrap_or("non-string payload");
                let err = format!("with_connection_extensions function panicked: {message}");
                tracing::error!(
                    remote_addr = info.peer_addr().map(tracing::field::display),
                    "{err}"
                );
                Err(err.into())
            }
        }
    }
}

// A `dyn Fn` is neither `UnwindSafe` nor `RefUnwindSafe`, which would take
// both from `BoundServer` (it has them without `server-tls`). The wrapper only
// calls the function and has no state of its own for a panic to leave
// half-updated; state the function captures is the function's to keep
// consistent.
impl std::panic::UnwindSafe for ConnectionExtensionsFn {}
impl std::panic::RefUnwindSafe for ConnectionExtensionsFn {}

/// How a connection served by [`Server::serve_connection`] or
/// [`serve_connection`] ended.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ConnectionClosed {
    reason: CloseReason,
    error: Option<SharedSource>,
}

type ConnectionError = dyn std::error::Error + Send + Sync;

// `dyn Error` is not `RefUnwindSafe`, which would take both traits from
// `ConnectionClosed`. The error is `Sync`, so any interior mutability in it
// already goes through locks or atomics that stay consistent across a panic;
// as with the unwind-safety traits generally, these impls are advisory.
impl std::panic::UnwindSafe for ConnectionClosed {}
impl std::panic::RefUnwindSafe for ConnectionClosed {}

impl ConnectionClosed {
    fn new(reason: CloseReason, error: Option<Box<ConnectionError>>) -> Self {
        debug_assert!(
            !matches!(reason, CloseReason::Error | CloseReason::HeaderReadTimeout)
                || error.is_some(),
            "{reason:?} without an error"
        );
        Self {
            reason,
            error: error.map(Arc::from),
        }
    }

    /// Why the connection ended.
    #[must_use]
    pub fn reason(&self) -> CloseReason {
        self.reason
    }

    /// The error that ended the connection, if one did.
    ///
    /// Always `Some` when [`reason`](Self::reason) is [`CloseReason::Error`]
    /// or [`CloseReason::HeaderReadTimeout`]. The latter is routine for an
    /// idle HTTP/1.1 keep-alive connection, so log by reason rather than by
    /// the presence of an error. Also `Some` when the connection failed while
    /// winding down; the reason then stays the one that started the
    /// wind-down, so this identifies a failed drain:
    ///
    /// ```
    /// # use connectrpc::{CloseReason, ConnectionClosed};
    /// fn failed_drain(closed: &ConnectionClosed) -> bool {
    ///     let draining = matches!(
    ///         closed.reason(),
    ///         CloseReason::Shutdown
    ///             | CloseReason::MaxAge
    ///             | CloseReason::Idle
    ///             | CloseReason::MaxRequests
    ///     );
    ///     draining && closed.error().is_some()
    /// }
    /// ```
    ///
    /// A connection closed because its grace period expired does not count
    /// there: it reports no error.
    ///
    /// `None` when the connection ended cleanly, when it was closed because
    /// its retirement grace period expired, and when it was told to wind down
    /// before enough bytes arrived to pick HTTP/1.1 or HTTP/2.
    ///
    /// After the protocol is known, a failure is a [`hyper::Error`]; a
    /// failure before that is a [`std::io::Error`]. A panic in the
    /// [`with_connection_extensions`](Server::with_connection_extensions)
    /// function is an error whose message contains the panic message when that
    /// is a string; the panic is also logged at `error` level.
    #[must_use]
    pub fn error(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)> {
        self.error.as_deref()
    }

    /// The same error as [`error`](Self::error), as an owned handle to keep
    /// or to pass on as another error's source.
    #[must_use]
    pub fn error_arc(&self) -> Option<SharedSource> {
        self.error.clone()
    }
}

/// Why a connection ended; see [`ConnectionClosed::reason`].
///
/// When a shutdown signal or retirement trigger tells a connection to wind
/// down, that reason is final. A retirement reason ([`MaxAge`](Self::MaxAge),
/// [`Idle`](Self::Idle), [`MaxRequests`](Self::MaxRequests)) is reported
/// whether the connection drained within the grace period or was closed when
/// the grace period expired, and even if the shutdown signal arrived while it
/// drained. A connection that fails while winding down, including when its
/// header-read timeout expires, also reports that reason, not
/// [`Error`](Self::Error) or [`HeaderReadTimeout`](Self::HeaderReadTimeout);
/// [`ConnectionClosed::error`] returns the failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CloseReason {
    /// The connection ended on its own before anything told it to wind down:
    /// the peer closed it, or an HTTP/1.1 connection without keep-alive
    /// finished its request.
    Closed,
    /// The shutdown signal told the connection to wind down before any
    /// retirement trigger did.
    Shutdown,
    /// Retired on reaching its maximum age.
    MaxAge,
    /// Retired after staying idle for the duration set with
    /// [`with_max_connection_idle`](ConnectionConfig::with_max_connection_idle).
    /// An idle HTTP/1.1 keep-alive connection can instead reach its
    /// header-read timeout first, which reports
    /// [`HeaderReadTimeout`](Self::HeaderReadTimeout).
    Idle,
    /// Retired after serving the configured number of requests.
    MaxRequests,
    /// The [header-read timeout](ConnectionConfig::with_header_read_timeout)
    /// expired before anything told the connection to wind down, while:
    ///
    /// - the peer had not sent enough to pick HTTP/1.1 or HTTP/2;
    /// - an HTTP/1.1 request head was incomplete;
    /// - an HTTP/1.1 keep-alive connection waited idle for its next request.
    ///
    /// [`ConnectionClosed::error`] returns the timeout error: a
    /// [`std::io::Error`] of kind [`TimedOut`](std::io::ErrorKind::TimedOut)
    /// in the first case, otherwise a [`hyper::Error`] whose
    /// [`is_timeout`](hyper::Error::is_timeout) is true.
    HeaderReadTimeout,
    /// Something failed before anything told the connection to wind down;
    /// [`ConnectionClosed::error`] returns the error. The causes are:
    ///
    /// - an I/O or protocol error;
    /// - an HTTP/2 keepalive ping that got no reply within
    ///   [`with_http2_keepalive_timeout`](ConnectionConfig::with_http2_keepalive_timeout);
    /// - a panic while an HTTP/1.1 response body was produced;
    /// - a panic in the
    ///   [`with_connection_extensions`](Server::with_connection_extensions)
    ///   function (reported by [`Server::serve_connection`]).
    Error,
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

/// Default header read timeout, applied to every accepted connection.
///
/// A peer that connects and then stalls before completing an HTTP/1.1
/// request head is disconnected;
/// [`ConnectionConfig::with_header_read_timeout`] describes what the timeout
/// bounds and when each bound starts.
///
/// Override via [`Server::with_header_read_timeout`] or
/// [`BoundServer::with_header_read_timeout`]; pass `None` or zero to disable.
pub const DEFAULT_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Default drain window after a per-connection retirement trigger fires.
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

/// Everything applied to one accepted connection: HTTP/1.1 and HTTP/2
/// protocol settings, keepalive, the header-read timeout, and the three
/// retirement triggers (age, idle, request count) with their shared grace
/// period.
///
/// A plain value with a [`Default`]; build one and hand it to
/// [`Server::with_connection_config`] or [`BoundServer::with_connection_config`].
/// The `with_*` setters of the same names on `Server` / `BoundServer` are
/// shorthand for editing this value in place.
///
/// HTTP/2 adaptive window sizing and the explicit window sizes are mutually
/// exclusive in hyper; the setters keep them consistent (supplying a size
/// turns adaptive sizing off, re-enabling adaptive sizing wins over any stored
/// size).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectionConfig {
    http1_keep_alive: bool,
    header_read_timeout: Option<Duration>,
    http2_adaptive_window: bool,
    http2_initial_stream_window_size: Option<u32>,
    http2_initial_connection_window_size: Option<u32>,
    max_concurrent_streams: Option<u32>,
    http2_keepalive_interval: Option<Duration>,
    http2_keepalive_timeout: Duration,
    http2_max_header_list_size: Option<u32>,
    max_connection_age: Option<Duration>,
    max_connection_age_grace: Duration,
    max_connection_idle: Option<Duration>,
    max_requests_per_connection: Option<NonZeroU64>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            http1_keep_alive: true,
            header_read_timeout: Some(DEFAULT_HEADER_READ_TIMEOUT),
            http2_adaptive_window: DEFAULT_HTTP2_ADAPTIVE_WINDOW,
            http2_initial_stream_window_size: None,
            http2_initial_connection_window_size: None,
            max_concurrent_streams: None,
            http2_keepalive_interval: None,
            http2_keepalive_timeout: DEFAULT_HTTP2_KEEPALIVE_TIMEOUT,
            http2_max_header_list_size: None,
            max_connection_age: None,
            max_connection_age_grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            max_connection_idle: None,
            max_requests_per_connection: None,
        }
    }
}

impl ConnectionConfig {
    /// The defaults: HTTP/1.1 keep-alive on, a 30-second header read timeout,
    /// adaptive HTTP/2 windows, no keepalive PINGs, and no retirement trigger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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

    /// Set how long a peer may take to start a connection and to send each
    /// HTTP/1.1 request head.
    ///
    /// Defaults to [`DEFAULT_HEADER_READ_TIMEOUT`] (30 seconds). The timeout
    /// bounds two waits in turn, each for the full duration:
    ///
    /// - from the connection's first read (after any TLS handshake) until the
    ///   peer has sent either the 24-byte HTTP/2 connection preface or a byte
    ///   that differs from it, which picks HTTP/1.1;
    /// - for HTTP/1.1, from when hyper begins reading each request head until
    ///   the head is complete, including the idle wait between requests on a
    ///   keep-alive connection.
    ///
    /// So the first HTTP/1.1 request head can arrive up to twice the timeout
    /// after the first read. A peer that connects, or finishes a request, and
    /// then stalls before completing the next request head is disconnected,
    /// which mitigates slowloris-style connection-exhaustion attacks. Pass
    /// `None` or [`Duration::ZERO`] to disable both bounds; a timeout too long
    /// to add to the current time, such as [`Duration::MAX`], also disables
    /// them. Unlike `with_max_connection_age` and `with_max_connection_idle`,
    /// which panic on zero, this setter treats zero as `None`.
    ///
    /// After a peer has sent the HTTP/2 connection preface, this timeout no
    /// longer applies to it. Both of the following are off by default: use
    /// [`with_max_connection_idle`](Self::with_max_connection_idle) to
    /// retire HTTP/2 connections with no request in flight, or
    /// [`with_max_connection_age`](Self::with_max_connection_age) to retire
    /// any connection by age.
    #[must_use]
    pub fn with_header_read_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.header_read_timeout = timeout.into().filter(|timeout| !timeout.is_zero());
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
        self.http2_adaptive_window = enabled;
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
        self.http2_initial_stream_window_size = size.into();
        if self.http2_initial_stream_window_size.is_some() {
            self.http2_adaptive_window = false;
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
        self.http2_initial_connection_window_size = size.into();
        if self.http2_initial_connection_window_size.is_some() {
            self.http2_adaptive_window = false;
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
        self.max_concurrent_streams = Some(max_streams);
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
        self.http2_keepalive_interval = Some(interval);
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
        self.http2_keepalive_timeout = timeout;
        self
    }

    /// Set the maximum size of a received HTTP/2 header list, in bytes as
    /// HTTP/2 counts it (each field's name and value plus 32, pseudo-headers
    /// included): the `SETTINGS_MAX_HEADER_LIST_SIZE` advertised to peers.
    /// Left at hyper's default (16 KiB) when unset; increase it for callers
    /// that carry large tokens or metadata. HTTP/1.1 connections are
    /// unaffected.
    ///
    /// A request whose header list reaches `max` is answered
    /// `431 Request Header Fields Too Large` before reaching any handler. Some
    /// requests close the whole connection with a GOAWAY instead, failing
    /// every request on it. With h2 0.4.15 and later, that happens past four
    /// times `max`, and for a header block spread over more CONTINUATION
    /// frames than h2 allows (about 1.25 × `max` / 16 KiB, at least five),
    /// whatever its size. For a limit of a few hundred KiB, the frame cap is
    /// reached before twice `max`. Set `max` well above what legitimate peers
    /// send.
    ///
    /// # Panics
    ///
    /// Panics if `max` is zero: no request fits in an empty header list. A
    /// limit of a few hundred bytes refuses nearly every request too, because
    /// each field counts at least 32.
    #[must_use]
    pub fn with_http2_max_header_list_size(mut self, max: u32) -> Self {
        assert!(
            max != 0,
            "with_http2_max_header_list_size requires a non-zero value",
        );
        self.http2_max_header_list_size = Some(max);
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
    /// A request is in flight until its response body has been sent, so a
    /// long streaming response keeps its connection busy, even while it waits
    /// for the client's flow-control window. Idle reaping therefore does not
    /// bound a client that stops reading;
    /// [`with_max_connection_age`](Self::with_max_connection_age) does.
    ///
    /// An HTTP/2 stream upgraded with extended CONNECT (such as a WebSocket)
    /// stops counting once its response head is sent. A connection that
    /// carries only such streams is reaped, and closed when the grace period
    /// ends, even while they carry traffic. Do not enable idle reaping on a
    /// server that hosts HTTP/2 WebSockets.
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

    /// Whether HTTP/1.1 keep-alive is enabled.
    #[must_use]
    pub fn http1_keep_alive(&self) -> bool {
        self.http1_keep_alive
    }

    /// The header read timeout, or `None` if disabled. A timeout too long to
    /// add to the current time is returned as set, although it also disables
    /// the timeout.
    #[must_use]
    pub fn header_read_timeout(&self) -> Option<Duration> {
        self.header_read_timeout
    }

    /// Whether HTTP/2 adaptive flow-control window sizing is enabled.
    #[must_use]
    pub fn http2_adaptive_window(&self) -> bool {
        self.http2_adaptive_window
    }

    /// The explicit HTTP/2 initial stream window, if one was set. Ignored while
    /// [adaptive sizing](Self::http2_adaptive_window) is on.
    #[must_use]
    pub fn http2_initial_stream_window_size(&self) -> Option<u32> {
        self.http2_initial_stream_window_size
    }

    /// The explicit HTTP/2 initial connection window, if one was set. Ignored
    /// while [adaptive sizing](Self::http2_adaptive_window) is on.
    #[must_use]
    pub fn http2_initial_connection_window_size(&self) -> Option<u32> {
        self.http2_initial_connection_window_size
    }

    /// The advertised `SETTINGS_MAX_CONCURRENT_STREAMS`, or `None` for hyper's
    /// default.
    #[must_use]
    pub fn max_concurrent_streams(&self) -> Option<u32> {
        self.max_concurrent_streams
    }

    /// The HTTP/2 keepalive PING interval, or `None` if keepalive is off.
    #[must_use]
    pub fn http2_keepalive_interval(&self) -> Option<Duration> {
        self.http2_keepalive_interval
    }

    /// How long an HTTP/2 keepalive PING may go unacknowledged.
    #[must_use]
    pub fn http2_keepalive_timeout(&self) -> Duration {
        self.http2_keepalive_timeout
    }

    /// The advertised `SETTINGS_MAX_HEADER_LIST_SIZE`, or `None` for hyper's
    /// default.
    #[must_use]
    pub fn http2_max_header_list_size(&self) -> Option<u32> {
        self.http2_max_header_list_size
    }

    /// The maximum connection age (before jitter), or `None` if disabled.
    #[must_use]
    pub fn max_connection_age(&self) -> Option<Duration> {
        self.max_connection_age
    }

    /// The drain window shared by the three retirement triggers.
    #[must_use]
    pub fn max_connection_age_grace(&self) -> Duration {
        self.max_connection_age_grace
    }

    /// The idle duration after which a connection is retired, or `None` if
    /// disabled.
    #[must_use]
    pub fn max_connection_idle(&self) -> Option<Duration> {
        self.max_connection_idle
    }

    /// The request count after which a connection is retired, or `None` if
    /// disabled.
    #[must_use]
    pub fn max_requests_per_connection(&self) -> Option<NonZeroU64> {
        self.max_requests_per_connection
    }

    /// The `(stream, connection)` explicit window sizes that should actually
    /// reach hyper's builder.
    ///
    /// Adaptive sizing takes precedence: when it is on, no explicit window is
    /// applied, so the two never reach hyper at once regardless of the order
    /// the setters were called in. The setters already clear the adaptive flag
    /// when a size is supplied, but a later `with_http2_adaptive_window(true)`
    /// can leave both set; this resolves that case deterministically in favour
    /// of adaptive sizing.
    fn effective_http2_windows(&self) -> (Option<u32>, Option<u32>) {
        if self.http2_adaptive_window {
            (None, None)
        } else {
            (
                self.http2_initial_stream_window_size,
                self.http2_initial_connection_window_size,
            )
        }
    }

    /// Log (at debug) settings that are present but inert because the setting
    /// they qualify is absent. Called once per accept loop, not per
    /// connection.
    fn lint(&self) {
        if self.max_connection_age.is_none()
            && self.max_connection_idle.is_none()
            && self.max_requests_per_connection.is_none()
            && self.max_connection_age_grace != DEFAULT_MAX_CONNECTION_AGE_GRACE
        {
            tracing::debug!(
                "max_connection_age_grace is set but none of max_connection_age, \
                 max_connection_idle, or max_requests_per_connection are; the \
                 grace period has no effect",
            );
        }
        if self.http2_keepalive_interval.is_none()
            && self.http2_keepalive_timeout != DEFAULT_HTTP2_KEEPALIVE_TIMEOUT
        {
            tracing::debug!(
                "http2_keepalive_timeout is set but http2_keepalive_interval is not; \
                 HTTP/2 keepalive stays disabled and the timeout has no effect",
            );
        }
    }
}

/// Accept-time settings: whether to terminate TLS and how long a handshake
/// may take. Empty without the `server-tls` feature, so holders and the
/// accept loop carry one field in every feature set.
#[derive(Clone, Default)]
pub(crate) struct AcceptConfig {
    #[cfg(feature = "server-tls")]
    pub(crate) tls: Option<Arc<rustls::ServerConfig>>,
    /// `None` means [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`].
    #[cfg(feature = "server-tls")]
    pub(crate) tls_handshake_timeout: Option<Duration>,
}

impl AcceptConfig {
    #[cfg(feature = "server-tls")]
    pub(crate) fn tls_handshake_timeout(&self) -> Duration {
        self.tls_handshake_timeout
            .unwrap_or(DEFAULT_TLS_HANDSHAKE_TIMEOUT)
    }

    /// Whether connections are TLS-terminated.
    fn is_tls(&self) -> bool {
        #[cfg(feature = "server-tls")]
        {
            self.tls.is_some()
        }
        #[cfg(not(feature = "server-tls"))]
        {
            false
        }
    }
}

/// ConnectRPC server built on hyper: a [`ConnectRpcService`] plus the
/// [`ConnectionConfig`] (and, with `server-tls`, the TLS settings) every
/// accepted connection is served with.
pub struct Server {
    service: ConnectRpcService,
    connection: ConnectionConfig,
    accept: AcceptConfig,
    connection_extensions: Option<ConnectionExtensionsFn>,
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
            connection_extensions: None,
        }
    }

    /// Replace the per-connection settings wholesale.
    ///
    /// [`ConnectionConfig`] is a plain value, so one configuration can be
    /// built once and applied to a `Server` or a [`BoundServer`] alike. The
    /// individual `with_*` setters below edit the same value in place.
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
        self.accept.tls = Some(config);
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
    pub fn with_tls_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.accept.tls_handshake_timeout = Some(timeout);
        self
    }

    /// Set the header read timeout (default
    /// [`DEFAULT_HEADER_READ_TIMEOUT`]; `None` or zero disables). Shorthand for
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

    /// Set a maximum age for each accepted HTTP connection (±10% jitter, then
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

    /// Set the advertised HTTP/2 `SETTINGS_MAX_HEADER_LIST_SIZE` (hyper's
    /// default, 16 KiB, when unset). Shorthand for
    /// [`ConnectionConfig::with_http2_max_header_list_size`], which says when
    /// an oversized request gets `431` and when it closes the connection.
    ///
    /// # Panics
    ///
    /// Panics if `max` is zero.
    #[must_use]
    pub fn with_http2_max_header_list_size(mut self, max: u32) -> Self {
        self.connection = self.connection.with_http2_max_header_list_size(max);
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

    /// Add request extensions computed once per accepted connection from its
    /// [`ConnectionInfo`]; see [`BoundServer::with_connection_extensions`].
    /// Also applied by [`Server::serve_connection`]. Calling this again
    /// replaces the function.
    #[must_use]
    pub fn with_connection_extensions<F>(mut self, f: F) -> Self
    where
        F: Fn(&ConnectionInfo, &mut http::Extensions) + Send + Sync + 'static,
    {
        self.connection_extensions = Some(ConnectionExtensionsFn::new(f));
        self
    }

    /// The method form of [`serve_connection`]: serve one already-accepted
    /// (and, over TLS, already-handshaken) stream with this server's service
    /// and [`ConnectionConfig`], on whichever runtime polls the future, and
    /// report why it ended. See the free function for the runtime rules.
    ///
    /// The [`with_connection_extensions`](Self::with_connection_extensions)
    /// function runs when the future is first polled. If it panics, the panic
    /// is logged and the future resolves to [`CloseReason::Error`] without
    /// serving `io`.
    ///
    /// This is the building block for a custom accept loop: accept and
    /// authenticate the stream yourself (admit or refuse it, cap per tenant,
    /// pick a runtime), then spawn this future on that runtime, with the socket
    /// moved there too. TLS is the caller's job here — [`Server::with_tls`]
    /// only affects [`serve`](Self::serve).
    pub fn serve_connection<S, F>(
        &self,
        io: S,
        mut info: ConnectionInfo,
        shutdown: F,
    ) -> impl Future<Output = ConnectionClosed> + Send + 'static + use<S, F>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        let connection_extensions = self.connection_extensions.clone();
        let service = self.service.clone();
        let config = self.connection.clone();
        // The function runs inside the future, on the serving task; a panic in
        // it drops only this connection, as on the built-in loop.
        async move {
            if let Some(f) = &connection_extensions
                && let Err(err) = f.apply(&mut info)
            {
                return ConnectionClosed::new(CloseReason::Error, Some(err));
            }
            serve_connection(io, info, service, config, shutdown).await
        }
    }

    /// Get a reference to the underlying router.
    pub fn router(&self) -> &Router {
        self.service.dispatcher()
    }

    /// Bind and serve on the given address.
    ///
    /// Runs until a non-transient accept error. Dropping the future aborts
    /// every live connection; to drain them instead, use [`Server::bind`] +
    /// [`BoundServer::serve_with_graceful_shutdown`]. Accept errors are
    /// handled as by [`BoundServer::serve`].
    ///
    /// # Errors
    ///
    /// Returns `Err` if binding `addr` fails, and for an accept error that is neither
    /// transient nor file-descriptor exhaustion, after detaching the live
    /// connections, which then drain with no deadline.
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

        serve_with_listener(
            listener,
            self.service,
            self.accept,
            self.connection,
            self.connection_extensions,
            None,
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
            connection_extensions: None,
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

/// A server that has been bound to an address but not yet started: a listener
/// plus the [`ConnectionConfig`] (and TLS settings) its connections will be
/// served with; the service is supplied when serving starts.
pub struct BoundServer {
    listener: TcpListener,
    connection: ConnectionConfig,
    accept: AcceptConfig,
    connection_extensions: Option<ConnectionExtensionsFn>,
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

    /// Enable TLS with the given rustls server configuration.
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls(mut self, config: Arc<rustls::ServerConfig>) -> Self {
        self.accept.tls = Some(config);
        self
    }

    /// Set the TLS handshake timeout.
    ///
    /// Defaults to [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`] (10 seconds).
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.accept.tls_handshake_timeout = Some(timeout);
        self
    }

    /// Set the header read timeout (default
    /// [`DEFAULT_HEADER_READ_TIMEOUT`]; `None` or zero disables). Shorthand for
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

    /// Set a maximum age for each accepted HTTP connection (±10% jitter, then
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

    /// Set the advertised HTTP/2 `SETTINGS_MAX_HEADER_LIST_SIZE` (hyper's
    /// default, 16 KiB, when unset). Shorthand for
    /// [`ConnectionConfig::with_http2_max_header_list_size`], which says when
    /// an oversized request gets `431` and when it closes the connection.
    ///
    /// # Panics
    ///
    /// Panics if `max` is zero.
    #[must_use]
    pub fn with_http2_max_header_list_size(mut self, max: u32) -> Self {
        self.connection = self.connection.with_http2_max_header_list_size(max);
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

    /// Add request extensions computed once per accepted connection.
    ///
    /// `f` runs exactly once for every connection that completes the
    /// (optional) TLS handshake, on that connection's task and before its
    /// first request. It reads the connection's [`ConnectionInfo`], whose
    /// extensions are empty on this built-in loop and hold what a custom loop
    /// inserted under [`Server::serve_connection`]. It inserts into the
    /// [`http::Extensions`] it is handed, which starts empty; those values join
    /// the connection's extensions, replacing entries of the same type, and
    /// are cloned into every request on the connection, where handlers read
    /// them with `ctx.extensions().get::<T>()`. Use it for work that would
    /// otherwise repeat per request — parsing an identity out of
    /// `peer_certs()`, say.
    ///
    /// `f` cannot remove an entry or change the peer: [`PeerAddr`] /
    /// `PeerCerts` on requests come only from the [`ConnectionInfo`] peer
    /// fields, which the built-in loop sets from the transport, whatever `f`
    /// inserts under those types. `f` is synchronous and cannot reject the
    /// connection. To refuse requests, fail them in a handler or layer; to
    /// refuse connections, write a custom loop around
    /// [`Server::serve_connection`]. A panic in `f` is logged and drops that
    /// one connection (under [`Server::serve_connection`], the future
    /// resolves to [`CloseReason::Error`]); under `panic = "abort"` it aborts
    /// the process. Calling this again replaces the function.
    ///
    /// ```rust,ignore
    /// Server::bind("0.0.0.0:8443").await?
    ///     .with_tls(tls_config)
    ///     .with_connection_extensions(|conn, ext| {
    ///         ext.insert(PeerIdentity::parse(conn.peer_certs()));
    ///     })
    ///     .serve(router).await?;
    /// ```
    ///
    /// `f` sees the [`ConnectionInfo`] read-only: it can read the connection's
    /// extensions,
    ///
    /// ```no_run
    /// # async fn bind() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let _bound = connectrpc::server::Server::bind("127.0.0.1:0")
    ///     .await?
    ///     .with_connection_extensions(|conn, _ext| {
    ///         let _ = conn.extensions().len();
    ///     });
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// but not change them, so this does not compile:
    ///
    /// ```compile_fail,E0596
    /// # async fn bind() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let _bound = connectrpc::server::Server::bind("127.0.0.1:0")
    ///     .await?
    ///     .with_connection_extensions(|conn, _ext| {
    ///         conn.extensions_mut().clear();
    ///     });
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_connection_extensions<F>(mut self, f: F) -> Self
    where
        F: Fn(&ConnectionInfo, &mut http::Extensions) + Send + Sync + 'static,
    {
        self.connection_extensions = Some(ConnectionExtensionsFn::new(f));
        self
    }

    /// Start serving requests with the given router.
    ///
    /// Runs until a non-transient accept error. Dropping the future aborts
    /// every live connection; to drain them instead, use
    /// [`serve_with_graceful_shutdown`](Self::serve_with_graceful_shutdown).
    /// Transient accept errors ([`ErrorKind`](std::io::ErrorKind)
    /// `WouldBlock`, `Interrupted`, `ConnectionAborted`, `ConnectionReset`)
    /// are logged and skipped. After
    /// running out of file descriptors (`EMFILE` / `ENFILE`, or `WSAEMFILE`
    /// on Windows) the loop pauses accepts for a second.
    ///
    /// # Errors
    ///
    /// Returns `Err` for an accept error that is neither
    /// transient nor file-descriptor exhaustion, after detaching the live
    /// connections, which then drain with no deadline.
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
    /// Transient accept errors are logged and skipped, as for
    /// [`serve`](Self::serve). After running out of file descriptors
    /// (`EMFILE` / `ENFILE`, or `WSAEMFILE` on Windows) the loop pauses
    /// accepts for up to a second, or until `signal` resolves.
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
    /// # Errors
    ///
    /// Returns `Err` for an accept error that is neither transient nor
    /// file-descriptor exhaustion, without waiting for a drain; the live
    /// connections are detached and drain with no deadline.
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
    /// Use this for a service configured with
    /// [`with_limits`](ConnectRpcService::with_limits),
    /// [`with_compression`](ConnectRpcService::with_compression) or
    /// [`with_interceptor`](ConnectRpcService::with_interceptor), or one shared
    /// between servers. To serve a tower-layered stack, pass it to
    /// [`serve_connection`] from your own accept loop, or, with the `axum`
    /// feature, mount it with `axum::Router::fallback_service` and use
    /// `connectrpc::axum::serve`. Otherwise behaves as [`serve`](Self::serve).
    ///
    /// # Errors
    ///
    /// As for [`serve`](Self::serve).
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
    ///
    /// # Errors
    ///
    /// As for
    /// [`serve_with_graceful_shutdown`](Self::serve_with_graceful_shutdown).
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
            self.connection_extensions,
            Some(Box::pin(signal)),
        )
        .await?;
        Ok(())
    }
}

/// Per-connection max-age settings, after jitter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConnectionAgeConfig {
    max_age: Duration,
    grace: Duration,
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

/// Shared in-flight request accounting for one connection.
///
/// The per-request `service_fn` wrapper bumps these counters when a request is
/// dispatched and when it completes: when hyper drops its response body, or its
/// response future if no response is produced (hyper does not surface
/// per-connection stream counts directly). The connection lifecycle reads them
/// to decide whether the connection has been idle. `epoch` increments on every
/// request start *and* completion, so a short request that begins and ends
/// entirely within an idle window is still observed as activity and resets the
/// idle timer.
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

/// RAII guard that records a request as in flight until hyper drops its
/// response body, or its response future if no response is produced.
/// Decrementing on drop (rather than on a success path) keeps the in-flight
/// count correct even when a request future is cancelled or its handler
/// panics.
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

/// Serve HTTP/1.1 or HTTP/2 (auto-detected) on one already-accepted,
/// already-authenticated stream with the RPC connection lifecycle: the
/// service-generic form of [`Server::serve_connection`], for loops that host
/// something other than a [`Server`]'s own service.
///
/// Applies everything in `config`: HTTP/1.1 keep-alive and the header-read
/// timeout, HTTP/2 windows / keepalive / stream and header limits, and max-age
/// (with ±10% jitter) / idle / request-count retirement followed by the shared
/// grace period. When `shutdown` resolves the connection is told to wind down
/// (HTTP/2 GOAWAY, HTTP/1.1 keep-alive off) and the future completes once
/// in-flight requests have finished — a retirement grace period never caps
/// that drain. Every request carries `info`'s
/// [extensions](ConnectionInfo::extensions) plus [`PeerAddr`] / `PeerCerts`
/// for the peer `info` describes. The output says why the connection ended;
/// the future never fails, and dropping it closes the socket abruptly.
///
/// A handler that panics before returning its response becomes a `500` with
/// a Connect `internal` error body instead of tearing the connection down.
/// Clients that read the Connect error body (Connect unary clients, and this
/// crate's Connect streaming client) report `internal`; clients that go by
/// the HTTP status (gRPC, gRPC-Web and most Connect streaming clients) report
/// `unknown`. A panic while any response body is polled is logged, and resets
/// that stream with `INTERNAL_ERROR` (HTTP/2) or ends the connection
/// (HTTP/1.1, where the response status is already committed). Under
/// `panic = "abort"`, any of these panics aborts the process instead.
///
/// `service` is any tower HTTP service — a [`ConnectRpcService`], an
/// `axum::Router`, or your own stack around either. HTTP upgrades
/// (`hyper::upgrade::on`) are supported, including HTTP/2 extended CONNECT
/// (RFC 8441), which the connection advertises. A completed HTTP/1.1 upgrade
/// ends the future, with [`CloseReason::Closed`] unless shutdown or retirement
/// had already begun; shutdown, retirement and dropping the future then no
/// longer reach the upgraded socket. An extended CONNECT stream stays on its
/// HTTP/2 connection, so a shutdown drain waits for it to end. The upgraded
/// connection wraps `io` in a private type, so use it through
/// `hyper::upgrade::Upgraded`'s `Read` and `Write`;
/// `hyper_util::server::conn::auto::upgrade::downcast` cannot recover `io`.
///
/// Must be polled inside a tokio runtime with I/O and time enabled. Nothing is
/// bound to a runtime until first poll: timers, the HTTP/2 stream tasks hyper
/// spawns, and every handler run on the runtime that polls this future. So an
/// accept loop chooses where a connection is served by choosing where to spawn
/// it. `io` is the exception: a tokio socket stays registered with the I/O
/// driver of the runtime that created it, so that runtime delivers the
/// socket's readiness and must outlive the connection. To move the I/O too,
/// convert the accepted socket with
/// [`into_std`](tokio::net::TcpStream::into_std) before wrapping it (a TLS
/// stream cannot be rebuilt around a moved socket), and back with
/// [`from_std`](tokio::net::TcpStream::from_std) inside the task on the
/// target runtime, as the guide's
/// [custom accept loops](https://github.com/connectrpc/connect-rust/blob/main/docs/guide.md#custom-accept-loops)
/// section shows.
#[allow(clippy::manual_async_fn, reason = "`Send` belongs in the signature")]
pub fn serve_connection<I, S, B, F>(
    io: I,
    info: ConnectionInfo,
    service: S,
    config: ConnectionConfig,
    shutdown: F,
) -> impl Future<Output = ConnectionClosed> + Send + 'static
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    S: Service<http::Request<hyper::body::Incoming>, Response = Response<B>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    F: Future<Output = ()> + Send + 'static,
{
    // Everything runtime-touching (timers, hyper's executor, the trace) is
    // created on first poll, so the future can be built anywhere and binds to
    // whichever runtime polls it.
    async move {
        let service = CatchPanic::custom(service, InternalErrorForPanic);
        let global_shutdown: RetirementSignal = Box::pin(shutdown);
        let remote_addr = info.peer_addr();
        tracing::trace!(
            remote_addr = remote_addr.map(tracing::field::display),
            "Accepted new connection"
        );
        let grace = config.max_connection_age_grace;

        // In-flight accounting is only needed when idle reaping is enabled; when it
        // is off there is no per-request bookkeeping overhead.
        let activity = config
            .max_connection_idle
            .map(|_| Arc::new(ConnectionActivity::default()));

        // When request-count retirement is enabled, the service counts every
        // dispatched request and flips this watch channel once the limit is
        // reached; the connection lifecycle observes it and starts draining. The
        // counter lives only as long as this connection task.
        let (request_counter, request_retire) = match config.max_requests_per_connection {
            Some(max) => {
                let (tx, rx) = watch::channel(false);
                (
                    Some(RequestCounter {
                        served: AtomicU64::new(0),
                        max,
                        retire: tx,
                    }),
                    Some((rx, grace)),
                )
            }
            None => (None, None),
        };

        // Computed once, before hyper reads the first request; cloned into each
        // request below.
        let request_extensions = info.request_extensions();
        let activity_for_requests = activity.clone();
        let svc =
            hyper::service::service_fn(move |mut req: http::Request<hyper::body::Incoming>| {
                req.extensions_mut().extend(request_extensions.clone());
                if let Some(counter) = &request_counter {
                    counter.record_request();
                }
                let service = service.clone();
                // Mark the request in flight before its future is polled; the guard
                // moves into the response body (see `ActiveRequestGuard`).
                let guard = activity_for_requests
                    .as_ref()
                    .map(|activity| ActiveRequestGuard::new(Arc::clone(activity)));
                async move {
                    let response = service.oneshot(req).await;
                    response.map(|response| {
                        response.map(|body| ServedBody::new(body, remote_addr, guard))
                    })
                }
            });

        // A timeout too long to add to the clock would panic hyper and
        // `DetectionTimeout`, so it disables both bounds. The setter has
        // already turned zero into `None`.
        let detection_deadline = config
            .header_read_timeout
            .and_then(|timeout| tokio::time::Instant::now().checked_add(timeout));
        let header_read_timeout = detection_deadline.and(config.header_read_timeout);
        let mut builder = AutoBuilder::new(TokioExecutor::new());
        // A timer is required for hyper's header read timeout (and any other
        // time-based connection behaviour) to take effect; without it the
        // configured `header_read_timeout` is silently ignored.
        builder
            .http1()
            .timer(TokioTimer::new())
            .keep_alive(config.http1_keep_alive)
            .header_read_timeout(header_read_timeout);
        configure_http2(&mut builder, &config);

        // Max age gets per-connection jitter so connections opened together do not
        // retire together; idle and request-count retirement are reactive and
        // need none. Each `RandomState` carries fresh keys, so this is a uniform
        // sample.
        let age = config.max_connection_age.map(|age| ConnectionAgeConfig {
            max_age: jitter_connection_age(age, RandomState::new().hash_one(remote_addr)),
            grace,
        });
        let idle = config
            .max_connection_idle
            .map(|idle| IdleConfig { idle, grace });
        let io = DetectionTimeout::new(io, detection_deadline);
        let conn = builder
            .serve_connection_with_upgrades(TokioIo::new(io), svc)
            .into_owned();
        serve_connection_with_lifecycle(
            conn,
            remote_addr,
            global_shutdown,
            age,
            idle.zip(activity),
            request_retire,
        )
        .await
    }
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

/// Apply the HTTP/2 half of `config` to a connection builder.
///
/// `adaptive_window` is always set explicitly so the default tracks
/// [`DEFAULT_HTTP2_ADAPTIVE_WINDOW`] regardless of hyper's own default. Explicit
/// window sizes are applied only when adaptive sizing is off (see
/// [`ConnectionConfig::effective_http2_windows`]), so the two never reach hyper
/// at once and the precedence does not depend on hyper's internal call
/// ordering.
fn configure_http2(builder: &mut AutoBuilder<TokioExecutor>, config: &ConnectionConfig) {
    let mut http2 = builder.http2();
    http2.adaptive_window(config.http2_adaptive_window);
    let (stream_window, connection_window) = config.effective_http2_windows();
    if let Some(size) = stream_window {
        http2.initial_stream_window_size(size);
    }
    if let Some(size) = connection_window {
        http2.initial_connection_window_size(size);
    }
    if let Some(max) = config.max_concurrent_streams {
        http2.max_concurrent_streams(max);
    }
    if let Some(max) = config.http2_max_header_list_size {
        http2.max_header_list_size(max);
    }
    // Extended CONNECT (RFC 8441) carries HTTP/2 WebSockets; without it, an
    // axum app moved onto `connectrpc::axum::serve` loses its HTTP/2 WebSocket
    // routes. A service that does not handle it answers such a request like
    // any other.
    http2.enable_connect_protocol();
    // Keepalive is opt-in: when no interval is set, leave hyper's default
    // (disabled) untouched. When enabled, a timer must be installed — hyper's
    // HTTP/2 keepalive requires one and panics the connection task without it.
    if let Some(interval) = config.http2_keepalive_interval {
        http2
            .timer(TokioTimer::new())
            .keep_alive_interval(interval)
            .keep_alive_timeout(config.http2_keepalive_timeout);
    }
}

fn serve_connection_with_lifecycle<C>(
    conn: C,
    remote_addr: Option<SocketAddr>,
    global_shutdown: RetirementSignal,
    connection_age: Option<ConnectionAgeConfig>,
    connection_idle: Option<(IdleConfig, Arc<ConnectionActivity>)>,
    request_retire: Option<(watch::Receiver<bool>, Duration)>,
) -> ConnectionLifecycle<C>
where
    C: GracefulConnection,
    C::Error: Into<Box<ConnectionError>>,
{
    ConnectionLifecycle {
        conn: Box::pin(conn),
        remote_addr,
        global_shutdown,
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

/// A boxed future that resolves when the connection should begin graceful
/// shutdown: the shutdown signal, or a per-connection retirement trigger such
/// as the request-count limit.
type RetirementSignal = Pin<Box<dyn Future<Output = ()> + Send>>;

struct ConnectionLifecycle<C: GracefulConnection> {
    conn: Pin<Box<C>>,
    remote_addr: Option<SocketAddr>,
    global_shutdown: RetirementSignal,
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
    /// Draining with no deadline after the shutdown signal; `reason` is
    /// `Shutdown` unless a retirement trigger had already fired.
    GlobalDraining {
        reason: CloseReason,
    },
    /// Draining after a per-connection retirement trigger (max age, max idle,
    /// or max requests): graceful shutdown has been issued and the connection
    /// is given a grace window to finish in-flight work before being
    /// force-closed.
    Draining {
        grace: Pin<Box<tokio::time::Sleep>>,
        duration: Duration,
        reason: CloseReason,
    },
}

impl<C> Future for ConnectionLifecycle<C>
where
    C: GracefulConnection,
    C::Error: Into<Box<ConnectionError>>,
{
    type Output = ConnectionClosed;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ConnectionClosed> {
        let this = self.get_mut();
        let remote_addr = this.remote_addr.map(tracing::field::display);

        loop {
            match &mut this.state {
                ConnectionLifecycleState::Serving => {
                    if let Poll::Ready(result) = this.conn.as_mut().poll(cx) {
                        let reason = if result.is_ok() {
                            CloseReason::Closed
                        } else {
                            CloseReason::Error
                        };
                        return Poll::Ready(log_connection_result(
                            this.remote_addr,
                            result,
                            reason,
                        ));
                    }

                    if let Poll::Ready(()) = this.global_shutdown.as_mut().poll(cx) {
                        this.conn.as_mut().graceful_shutdown();
                        this.state = ConnectionLifecycleState::GlobalDraining {
                            reason: CloseReason::Shutdown,
                        };
                        continue;
                    }

                    if let Some((age, config)) = &mut this.age
                        && age.as_mut().poll(cx).is_ready()
                    {
                        tracing::trace!(
                            remote_addr,
                            max_age = ?config.max_age,
                            grace = ?config.grace,
                            "Connection reached maximum age; starting graceful shutdown",
                        );
                        this.conn.as_mut().graceful_shutdown();
                        this.state = ConnectionLifecycleState::Draining {
                            grace: Box::pin(tokio::time::sleep(config.grace)),
                            duration: config.grace,
                            reason: CloseReason::MaxAge,
                        };
                        continue;
                    }

                    if let Some(idle) = &mut this.idle
                        && idle.timer.as_mut().poll(cx).is_ready()
                    {
                        let (in_flight, epoch) = idle.activity.snapshot();
                        if in_flight == 0 && epoch == idle.armed_epoch {
                            tracing::trace!(
                                remote_addr,
                                idle = ?idle.config.idle,
                                grace = ?idle.config.grace,
                                "Connection idle; starting graceful shutdown",
                            );
                            this.conn.as_mut().graceful_shutdown();
                            this.state = ConnectionLifecycleState::Draining {
                                grace: Box::pin(tokio::time::sleep(idle.config.grace)),
                                duration: idle.config.grace,
                                reason: CloseReason::Idle,
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
                            remote_addr,
                            grace = ?grace,
                            "Connection reached maximum requests; starting graceful shutdown",
                        );
                        this.conn.as_mut().graceful_shutdown();
                        this.state = ConnectionLifecycleState::Draining {
                            grace: Box::pin(tokio::time::sleep(grace)),
                            duration: grace,
                            reason: CloseReason::MaxRequests,
                        };
                        continue;
                    }

                    return Poll::Pending;
                }
                ConnectionLifecycleState::GlobalDraining { reason } => {
                    if let Poll::Ready(result) = this.conn.as_mut().poll(cx) {
                        return Poll::Ready(log_connection_result(
                            this.remote_addr,
                            result,
                            *reason,
                        ));
                    }
                    return Poll::Pending;
                }
                ConnectionLifecycleState::Draining {
                    grace,
                    duration,
                    reason,
                } => {
                    let reason = *reason;
                    if let Poll::Ready(result) = this.conn.as_mut().poll(cx) {
                        return Poll::Ready(log_connection_result(
                            this.remote_addr,
                            result,
                            reason,
                        ));
                    }

                    if let Poll::Ready(()) = this.global_shutdown.as_mut().poll(cx) {
                        this.state = ConnectionLifecycleState::GlobalDraining { reason };
                        continue;
                    }

                    if grace.as_mut().poll(cx).is_ready() {
                        tracing::trace!(
                            remote_addr,
                            grace = ?duration,
                            "Connection retirement grace expired; closing connection",
                        );
                        return Poll::Ready(ConnectionClosed::new(reason, None));
                    }

                    return Poll::Pending;
                }
            }
        }
    }
}

/// Log how hyper's connection future ended and report `reason` with its
/// error, if any.
///
/// A connection that was serving passes `CloseReason::Error` for any
/// failure; an expired header-read timeout is reported as
/// `CloseReason::HeaderReadTimeout` instead. A connection told to wind down
/// passes that reason even when it failed mid-drain. The exception is
/// hyper-util's error for a connection told to wind down before it picked a
/// protocol: that error is dropped, so the result has the wind-down reason
/// and no error.
fn log_connection_result<E: Into<Box<ConnectionError>>>(
    remote_addr: Option<SocketAddr>,
    result: Result<(), E>,
    reason: CloseReason,
) -> ConnectionClosed {
    let remote_addr = remote_addr.map(tracing::field::display);
    let error = result
        .err()
        .map(Into::into)
        .filter(|err| reason == CloseReason::Error || !is_cancelled_before_detection(err.as_ref()));
    let reason = match &error {
        Some(err) if reason == CloseReason::Error && is_header_read_timeout(err.as_ref()) => {
            CloseReason::HeaderReadTimeout
        }
        _ => reason,
    };
    match &error {
        None => tracing::trace!(remote_addr, "Connection completed normally"),
        Some(err) => tracing::trace!(remote_addr, error = %err, "Connection ended with error"),
    }
    ConnectionClosed::new(reason, error)
}

/// Whether `err` is an expired header-read timeout: hyper's, once it reads
/// HTTP/1.1 request heads, or [`DetectionTimeout`]'s before that.
///
/// hyper reports its header-read timeout as `is_timeout` from 1.6.0;
/// hyper-util 0.1.20 needs 1.8.0. hyper's HTTP/2 keepalive timeout is also
/// `is_timeout`, but carries its timeout as a source; the header-read
/// timeout has none.
/// `header_read_timeout_closes_idle_keep_alive_connection` and
/// `http2_keepalive_timeout_is_not_a_header_read_timeout` fail if that
/// changes.
fn is_header_read_timeout(err: &ConnectionError) -> bool {
    use std::error::Error as _;

    if let Some(err) = err.downcast_ref::<hyper::Error>() {
        return err.is_timeout() && err.source().is_none();
    }
    err.downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::get_ref)
        .is_some_and(|inner| inner.is::<DetectionTimedOut>())
}

/// Whether `err` is the one hyper-util's auto connection returns when a
/// graceful shutdown arrives before it has read enough to pick HTTP/1.1 or
/// HTTP/2.
///
/// It comes from `ReadVersion::poll` in hyper-util's `server::conn::auto`,
/// which identifies it only as an `Interrupted` error reading "Cancelled";
/// `serve_connection_close_reason_before_first_request` fails if that
/// changes.
fn is_cancelled_before_detection(err: &ConnectionError) -> bool {
    err.downcast_ref::<std::io::Error>().is_some_and(|err| {
        err.kind() == std::io::ErrorKind::Interrupted && err.to_string() == "Cancelled"
    })
}

/// The error [`DetectionTimeout`] fails a read with, inside an
/// [`std::io::Error`] of kind `TimedOut`.
#[derive(Debug)]
struct DetectionTimedOut;

impl std::fmt::Display for DetectionTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("header read timeout expired before the peer chose HTTP/1.1 or HTTP/2")
    }
}

impl std::error::Error for DetectionTimedOut {}

/// The bytes an HTTP/2 client sends first (RFC 9113 §3.4).
const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// An accepted stream whose reads fail once a deadline passes before the
/// peer has sent enough to pick HTTP/1.1 or HTTP/2.
///
/// hyper-util's auto connection reads until the bytes stop matching
/// [`H2_PREFACE`], the whole preface has arrived, or the peer closes, and has
/// no timer while it does; hyper's header-read timeout starts only after
/// that. This wrapper applies the same test to what the auto connection
/// reads and, once the protocol is picked, passes reads through untouched.
/// The test follows `ReadVersion::poll` in hyper-util 0.1.20's
/// `server::conn::auto`. If hyper-util picks a protocol where this wrapper
/// does not, `header_read_timeout_hands_over_to_hyper_after_detection` and
/// `header_read_timeout_leaves_idle_http2_connections_open` fail.
struct DetectionTimeout<I> {
    io: I,
    detection: Option<Detection>,
}

struct Detection {
    /// How many bytes of [`H2_PREFACE`] have arrived so far.
    matched: usize,
    deadline: tokio::time::Instant,
    /// Created on the first read that has to wait.
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<I> DetectionTimeout<I> {
    fn new(io: I, deadline: Option<tokio::time::Instant>) -> Self {
        Self {
            io,
            detection: deadline.map(|deadline| Detection {
                matched: 0,
                deadline,
                sleep: None,
            }),
        }
    }
}

impl Detection {
    /// Record bytes the auto connection read; returns whether it has now
    /// picked a protocol. An empty read is the peer closing, which the auto
    /// connection treats as HTTP/1.1.
    fn observe(&mut self, read: &[u8]) -> bool {
        let rest = &H2_PREFACE[self.matched..];
        if read.is_empty() || read.len() >= rest.len() || !rest.starts_with(read) {
            return true;
        }
        self.matched += read.len();
        false
    }
}

impl<I: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for DetectionTimeout<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        let Some(detection) = &mut this.detection else {
            return Pin::new(&mut this.io).poll_read(cx, buf);
        };
        let start = buf.filled().len();
        match Pin::new(&mut this.io).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if detection.observe(&buf.filled()[start..]) {
                    this.detection = None;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => {
                let deadline = detection.deadline;
                let sleep = detection
                    .sleep
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
                std::task::ready!(sleep.as_mut().poll(cx));
                this.detection = None;
                Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    DetectionTimedOut,
                )))
            }
        }
    }
}

impl<I: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for DetectionTimeout<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
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
pub(crate) type ShutdownSignal = Option<Pin<Box<dyn Future<Output = ()> + Send>>>;

/// The built-in accept loop: serve connections from `listener` with `service`
/// until `shutdown` resolves, then stop accepting, tell every live connection
/// to drain, and wait for all of them. `connection_extensions`, if set, runs
/// on each connection's task between the (optional) TLS handshake and the
/// first request. A fatal accept error detaches the live connections (they
/// observe the dropped drain signal and wind down on their own) and returns
/// the error; dropping the future aborts every connection task.
pub(crate) async fn serve_with_listener<S, B>(
    listener: TcpListener,
    service: S,
    accept: AcceptConfig,
    config: ConnectionConfig,
    connection_extensions: Option<ConnectionExtensionsFn>,
    shutdown: ShutdownSignal,
) -> std::io::Result<()>
where
    S: Service<http::Request<hyper::body::Incoming>, Response = Response<B>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    config.lint();

    #[cfg(feature = "server-tls")]
    let tls_handshake_timeout = accept.tls_handshake_timeout();
    #[cfg(feature = "server-tls")]
    let tls_acceptor = accept.tls.map(tokio_rustls::TlsAcceptor::from);
    #[cfg(not(feature = "server-tls"))]
    let AcceptConfig {} = accept;

    // Pin the shutdown future so we can poll it in select!. If no shutdown
    // signal was provided, use a never-resolving pending() future.
    let mut shutdown = shutdown.unwrap_or_else(|| Box::pin(std::future::pending()));
    // Broadcasts "begin graceful shutdown" to every live connection. `watch`
    // gives a cloneable receiver per connection and a sticky value, so a
    // connection that registers after the signal still observes it.
    let (global_shutdown_tx, global_shutdown_rx) = watch::channel(false);
    let mut connections = JoinSet::new();

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
                    if is_fd_exhaustion(&err) {
                        tracing::warn!(
                            error = %err,
                            "Out of file descriptors accepting a connection; \
                             pausing accepts for {FD_EXHAUSTION_BACKOFF:?}"
                        );
                        tokio::select! {
                            biased;
                            _ = &mut shutdown => {
                                tracing::info!("Shutdown signal received; draining connections");
                                break;
                            }
                            () = tokio::time::sleep(FD_EXHAUSTION_BACKOFF) => {}
                        }
                        continue;
                    }
                    if is_transient_accept_error(&err) {
                        tracing::warn!(error = %err, "Transient accept error (continuing)");
                        continue;
                    }
                    connections.detach_all();
                    return Err(err);
                }
            },
        };

        // Disable Nagle's algorithm to avoid latency from the interaction
        // between Nagle buffering and delayed ACKs, which is especially
        // problematic for HTTP/2's small control frames.
        if let Err(e) = stream.set_nodelay(true) {
            tracing::warn!("failed to set TCP_NODELAY: {e}");
        }

        let service = service.clone();
        let config = config.clone();
        let connection_extensions = connection_extensions.clone();
        let global_shutdown = global_shutdown_future(global_shutdown_rx.clone());

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
                        let mut peer = ConnectionInfo::new().with_peer_addr(remote_addr);
                        peer.peer_certs = conn
                            .peer_certificates()
                            .map(|chain| chain.iter().map(|c| c.clone().into_owned()).collect());
                        if let Some(f) = &connection_extensions
                            && f.apply(&mut peer).is_err()
                        {
                            return;
                        }
                        serve_connection(tls_stream, peer, service, config, global_shutdown).await;
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
            let mut peer = ConnectionInfo::new().with_peer_addr(remote_addr);
            if let Some(f) = &connection_extensions
                && f.apply(&mut peer).is_err()
            {
                return;
            }
            serve_connection(stream, peer, service, config, global_shutdown).await;
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

/// Converts a handler panic into a `500` with a Connect `internal` error body,
/// logging the message and (if enabled) the backtrace, so one request's panic
/// costs that request and not the connection.
#[derive(Clone, Copy)]
struct InternalErrorForPanic;

impl tower_http::catch_panic::ResponseForPanic for InternalErrorForPanic {
    type ResponseBody = Full<Bytes>;

    fn response_for_panic(&mut self, err: Box<dyn Any + Send + 'static>) -> Response<Full<Bytes>> {
        // Capture the backtrace for debugging
        let backtrace = std::backtrace::Backtrace::capture();

        let message = panic_message(&*err).unwrap_or("handler panicked");

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
}

/// The message a panic payload carries, when it is a string.
fn panic_message(payload: &(dyn Any + Send)) -> Option<&str> {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
}

/// The response body of the panic-catching service layer.
type CaughtBody = tower_http::body::UnsyncBoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;

/// The body hyper sends for every response. It holds the request's in-flight
/// guard until hyper drops it, and turns a panic in `poll_frame` (such as a
/// streaming handler's stream panicking) into a body error. Then hyper resets
/// that stream (HTTP/2) or closes the connection (HTTP/1.1), instead of the
/// panic unwinding through the connection task (HTTP/1.1) or hyper's
/// per-stream task (HTTP/2). It wraps the concrete [`CaughtBody`]: a generic
/// `B::Error: Into<BoxError>` bound trips a higher-ranked lifetime error in
/// the connection future.
struct ServedBody {
    /// `None` once a poll has panicked.
    inner: Option<CaughtBody>,
    remote_addr: Option<SocketAddr>,
    /// Keeps the request in flight for idle reaping.
    _in_flight: Option<ActiveRequestGuard>,
}

impl ServedBody {
    fn new(
        inner: CaughtBody,
        remote_addr: Option<SocketAddr>,
        in_flight: Option<ActiveRequestGuard>,
    ) -> Self {
        Self {
            inner: Some(inner),
            remote_addr,
            _in_flight: in_flight,
        }
    }
}

impl http_body::Body for ServedBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        let this = &mut *self;
        // hyper stops polling a body once it has returned an error, so only a
        // caller that polls past that error sees this end.
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Pin::new(inner).poll_frame(cx)
        }));
        polled.unwrap_or_else(|panic| {
            let message = panic_message(&*panic).unwrap_or("non-string payload");
            tracing::error!(
                remote_addr = this.remote_addr.map(tracing::field::display),
                "Request handler panicked in its response body: {message}",
            );
            // A body the panic left inconsistent might panic again as it drops.
            let poisoned = this.inner.take();
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || drop(poisoned)));
            Poll::Ready(Some(Err("response body panicked".into())))
        })
    }

    fn is_end_stream(&self) -> bool {
        self.inner
            .as_ref()
            .is_none_or(http_body::Body::is_end_stream)
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner
            .as_ref()
            .map_or_else(http_body::SizeHint::default, http_body::Body::size_hint)
    }
}

/// How long the accept loop waits after running out of file descriptors
/// before accepting again, unless shutdown fires first. Accepting sooner
/// fails the same way, spinning a core until a descriptor is freed.
const FD_EXHAUSTION_BACKOFF: Duration = Duration::from_secs(1);

/// Whether an accept error means the process or the system ran out of file
/// descriptors (`EMFILE` / `ENFILE`, or `WSAEMFILE` on Windows). Accepting
/// again fails the same way until one is freed.
fn is_fd_exhaustion(err: &std::io::Error) -> bool {
    err.raw_os_error()
        .is_some_and(|code| FD_EXHAUSTION_ERRORS.contains(&code))
}

/// The raw OS errors [`is_fd_exhaustion`] matches.
#[cfg(not(windows))]
const FD_EXHAUSTION_ERRORS: &[i32] = &[libc::EMFILE, libc::ENFILE];
#[cfg(windows)]
const FD_EXHAUSTION_ERRORS: &[i32] = &[WSAEMFILE];

/// Winsock's "too many open sockets", which it reports in place of the C
/// runtime's `EMFILE`.
#[cfg(any(windows, test))]
const WSAEMFILE: i32 = 10024;

/// Whether the accept loop should log `err` and keep accepting.
pub(crate) fn is_transient_accept_error(err: &std::io::Error) -> bool {
    use std::io::ErrorKind;

    matches!(
        err.kind(),
        ErrorKind::WouldBlock
            | ErrorKind::Interrupted
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionReset
    ) || is_fd_exhaustion(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;
    use std::time::Instant;
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

        let limits = Limits::default()
            .with_max_request_body_size(1024)
            .with_max_message_size(512);
        let server = Server::new(Router::new())
            .with_limits(limits)
            .with_compression(CompressionRegistry::default())
            .with_compression_policy(CompressionPolicy::default().with_min_size(8192))
            .with_http1_keep_alive(false);

        assert_eq!(server.service.limits().max_request_body_size(), 1024);
        assert_eq!(server.service.limits().max_message_size(), 512);
        assert!(!server.connection_config().http1_keep_alive());
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

    // ========================================================================
    // ConnectionConfig and the Server / BoundServer forwards
    // ========================================================================

    #[test]
    fn connection_config_defaults() {
        let config = ConnectionConfig::default();
        assert!(config.http1_keep_alive());
        assert_eq!(
            config.header_read_timeout(),
            Some(DEFAULT_HEADER_READ_TIMEOUT)
        );
        assert_eq!(
            config.http2_adaptive_window(),
            DEFAULT_HTTP2_ADAPTIVE_WINDOW
        );
        assert_eq!(config.http2_initial_stream_window_size(), None);
        assert_eq!(config.http2_initial_connection_window_size(), None);
        assert_eq!(config.max_concurrent_streams(), None);
        assert_eq!(config.http2_keepalive_interval(), None);
        assert_eq!(
            config.http2_keepalive_timeout(),
            DEFAULT_HTTP2_KEEPALIVE_TIMEOUT
        );
        assert_eq!(config.http2_max_header_list_size(), None);
        assert_eq!(config.max_connection_age(), None);
        assert_eq!(
            config.max_connection_age_grace(),
            DEFAULT_MAX_CONNECTION_AGE_GRACE
        );
        assert_eq!(config.max_connection_idle(), None);
        assert_eq!(config.max_requests_per_connection(), None);
        assert_eq!(config, ConnectionConfig::new());
    }

    #[test]
    fn connection_config_overrides() {
        let config = ConnectionConfig::new().with_header_read_timeout(Some(Duration::from_secs(5)));
        assert_eq!(config.header_read_timeout(), Some(Duration::from_secs(5)));
        let config = ConnectionConfig::new().with_header_read_timeout(None::<Duration>);
        assert_eq!(config.header_read_timeout(), None);
        let config = ConnectionConfig::new().with_header_read_timeout(Duration::ZERO);
        assert_eq!(config.header_read_timeout(), None);

        let config = ConnectionConfig::new()
            .with_http2_keepalive_interval(Duration::from_secs(30))
            .with_http2_keepalive_timeout(Duration::from_secs(5))
            .with_http2_max_header_list_size(64 << 10);
        assert_eq!(
            config.http2_keepalive_interval(),
            Some(Duration::from_secs(30))
        );
        assert_eq!(config.http2_keepalive_timeout(), Duration::from_secs(5));
        assert_eq!(config.http2_max_header_list_size(), Some(64 << 10));
        // Setting only the timeout leaves keepalive disabled (no interval).
        let config = ConnectionConfig::new().with_http2_keepalive_timeout(Duration::from_secs(1));
        assert_eq!(config.http2_keepalive_interval(), None);

        let config = ConnectionConfig::new()
            .with_max_connection_age(Duration::from_secs(30))
            .with_max_connection_idle(Duration::from_secs(10))
            .with_max_requests_per_connection(NonZeroU64::new(100).unwrap())
            .with_max_connection_age_grace(Duration::from_secs(2));
        assert_eq!(config.max_connection_age(), Some(Duration::from_secs(30)));
        assert_eq!(config.max_connection_idle(), Some(Duration::from_secs(10)));
        assert_eq!(config.max_requests_per_connection(), NonZeroU64::new(100));
        assert_eq!(config.max_connection_age_grace(), Duration::from_secs(2));
    }

    /// Supplying an explicit window turns adaptive sizing off; `None` leaves
    /// the flag alone; re-enabling adaptive afterwards wins and keeps the stored
    /// window away from hyper.
    #[test]
    fn http2_windows_and_adaptive_precedence() {
        let config = ConnectionConfig::new().with_http2_adaptive_window(false);
        assert!(!config.http2_adaptive_window());
        assert!(
            config
                .with_http2_adaptive_window(true)
                .http2_adaptive_window()
        );

        let config = ConnectionConfig::new().with_http2_initial_stream_window_size(1 << 20);
        assert_eq!(config.http2_initial_stream_window_size(), Some(1 << 20));
        assert!(!config.http2_adaptive_window());
        let config = ConnectionConfig::new().with_http2_initial_connection_window_size(2 << 20);
        assert_eq!(config.http2_initial_connection_window_size(), Some(2 << 20));
        assert!(!config.http2_adaptive_window());

        let config = ConnectionConfig::new()
            .with_http2_initial_stream_window_size(None)
            .with_http2_initial_connection_window_size(None);
        assert!(config.http2_adaptive_window());
        assert_eq!(config.http2_initial_stream_window_size(), None);
        assert_eq!(config.http2_initial_connection_window_size(), None);

        let config = ConnectionConfig::new()
            .with_http2_initial_stream_window_size(1 << 20)
            .with_http2_adaptive_window(true);
        assert!(config.http2_adaptive_window());
        assert_eq!(config.http2_initial_stream_window_size(), Some(1 << 20));
        assert_eq!(config.effective_http2_windows(), (None, None));

        assert_eq!(
            ConnectionConfig::default().effective_http2_windows(),
            (None, None)
        );
        let off = ConnectionConfig::new().with_http2_adaptive_window(false);
        assert_eq!(off.effective_http2_windows(), (None, None));
        let fixed = ConnectionConfig::new()
            .with_http2_initial_stream_window_size(1 << 20)
            .with_http2_initial_connection_window_size(2 << 20);
        assert_eq!(
            fixed.effective_http2_windows(),
            (Some(1 << 20), Some(2 << 20))
        );
    }

    #[test]
    #[should_panic(expected = "non-zero duration")]
    fn with_max_connection_age_rejects_zero() {
        let _ = ConnectionConfig::new().with_max_connection_age(Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "non-zero duration")]
    fn with_max_connection_idle_rejects_zero() {
        let _ = ConnectionConfig::new().with_max_connection_idle(Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "non-zero duration")]
    fn with_http2_keepalive_interval_rejects_zero() {
        let _ = Server::new(Router::new()).with_http2_keepalive_interval(Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "non-zero value")]
    fn with_max_concurrent_streams_rejects_zero() {
        let _ = ConnectionConfig::new().with_max_concurrent_streams(0);
    }

    #[test]
    #[should_panic(expected = "non-zero value")]
    fn with_http2_max_header_list_size_rejects_zero() {
        let _ = ConnectionConfig::new().with_http2_max_header_list_size(0);
    }

    /// `configure_http2` leaves keepalive untouched when no interval is set, so
    /// hyper's default (keepalive disabled) is preserved unless the user opts
    /// in. There is no public getter on the builder, so this guards the opt-in
    /// contract at the call boundary by exercising the default path without
    /// panicking.
    #[test]
    fn configure_http2_default_leaves_keepalive_disabled() {
        let mut builder = AutoBuilder::new(TokioExecutor::new());
        configure_http2(&mut builder, &ConnectionConfig::default());
    }

    /// Every constructor starts from the default config, every `with_*`
    /// shorthand on `Server` and `BoundServer` edits the field of the same
    /// name, and one `ConnectionConfig` value crosses between the two.
    #[tokio::test]
    async fn setters_forward_to_connection_config_and_it_crosses_between_server_and_bound_server() {
        async fn listener() -> TcpListener {
            TcpListener::bind("127.0.0.1:0").await.unwrap()
        }
        let default = ConnectionConfig::default();
        assert_eq!(Server::new(Router::new()).connection_config(), &default);
        assert_eq!(
            Server::from_service(ConnectRpcService::new(Router::new())).connection_config(),
            &default
        );
        assert_eq!(
            Server::from_listener(listener().await).connection_config(),
            &default
        );
        assert_eq!(
            Server::bind("127.0.0.1:0")
                .await
                .unwrap()
                .connection_config(),
            &default
        );

        let expected = ConnectionConfig::new()
            .with_http1_keep_alive(false)
            .with_header_read_timeout(Duration::from_secs(2))
            .with_http2_initial_stream_window_size(512 * 1024)
            .with_http2_initial_connection_window_size(1024 * 1024)
            .with_max_concurrent_streams(64)
            .with_http2_max_header_list_size(64 << 10)
            .with_http2_keepalive_interval(Duration::from_secs(30))
            .with_http2_keepalive_timeout(Duration::from_secs(5))
            .with_max_connection_age(Duration::from_secs(600))
            .with_max_connection_age_grace(Duration::from_secs(3))
            .with_max_connection_idle(Duration::from_secs(60))
            .with_max_requests_per_connection(NonZeroU64::new(100).unwrap());
        assert!(!expected.http2_adaptive_window());

        let server = Server::new(Router::new())
            .with_http1_keep_alive(false)
            .with_header_read_timeout(Duration::from_secs(2))
            .with_http2_initial_stream_window_size(512 * 1024)
            .with_http2_initial_connection_window_size(1024 * 1024)
            .with_max_concurrent_streams(64)
            .with_http2_max_header_list_size(64 << 10)
            .with_http2_keepalive_interval(Duration::from_secs(30))
            .with_http2_keepalive_timeout(Duration::from_secs(5))
            .with_max_connection_age(Duration::from_secs(600))
            .with_max_connection_age_grace(Duration::from_secs(3))
            .with_max_connection_idle(Duration::from_secs(60))
            .with_max_requests_per_connection(NonZeroU64::new(100).unwrap());
        assert_eq!(server.connection_config(), &expected);
        assert!(
            server
                .with_http2_adaptive_window(true)
                .connection_config()
                .http2_adaptive_window()
        );

        let bound = Server::from_listener(listener().await)
            .with_http1_keep_alive(false)
            .with_header_read_timeout(Duration::from_secs(2))
            .with_http2_initial_stream_window_size(512 * 1024)
            .with_http2_initial_connection_window_size(1024 * 1024)
            .with_max_concurrent_streams(64)
            .with_http2_max_header_list_size(64 << 10)
            .with_http2_keepalive_interval(Duration::from_secs(30))
            .with_http2_keepalive_timeout(Duration::from_secs(5))
            .with_max_connection_age(Duration::from_secs(600))
            .with_max_connection_age_grace(Duration::from_secs(3))
            .with_max_connection_idle(Duration::from_secs(60))
            .with_max_requests_per_connection(NonZeroU64::new(100).unwrap());
        assert_eq!(bound.connection_config(), &expected);
        assert!(
            bound
                .with_http2_adaptive_window(true)
                .connection_config()
                .http2_adaptive_window()
        );

        let bound =
            Server::from_listener(listener().await).with_connection_config(expected.clone());
        assert_eq!(bound.connection_config(), &expected);
        let server =
            Server::new(Router::new()).with_connection_config(bound.connection_config().clone());
        assert_eq!(server.connection_config(), &expected);

        #[cfg(feature = "server-tls")]
        {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            let tls = Arc::new(
                rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_cert_resolver(
                        Arc::new(rustls::server::ResolvesServerCertUsingSni::new()),
                    ),
            );
            let server = Server::new(Router::new());
            assert!(!server.accept.is_tls());
            assert_eq!(
                server.accept.tls_handshake_timeout(),
                DEFAULT_TLS_HANDSHAKE_TIMEOUT
            );
            let server = server
                .with_tls(Arc::clone(&tls))
                .with_tls_handshake_timeout(Duration::from_secs(3));
            assert!(server.accept.is_tls());
            assert_eq!(
                server.accept.tls_handshake_timeout(),
                Duration::from_secs(3)
            );
            let bound = Server::from_listener(listener().await)
                .with_tls(tls)
                .with_tls_handshake_timeout(Duration::from_secs(7));
            assert!(bound.accept.is_tls());
            assert_eq!(bound.accept.tls_handshake_timeout(), Duration::from_secs(7));
        }
    }

    /// `SETTINGS_MAX_HEADER_LIST_SIZE` reaches hyper: a request whose header
    /// list reaches it is refused with 431 before any handler runs, one under
    /// it is served, and one over four times the limit (h2 0.4.15 and later)
    /// gets a connection-closing GOAWAY. The 431 pad (8 KiB) sits above the
    /// 4 KiB limit, below hyper's 16 KiB default (so it only fails if the
    /// setting arrived) and below h2's 4 × 4 KiB connection-close threshold.
    #[tokio::test]
    async fn http2_max_header_list_size_rejects_oversized_headers() {
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_connection_config(ConnectionConfig::new().with_http2_max_header_list_size(4096));
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
        let (send_request, connection) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(connection);
        let mut send_request = send_request.ready().await.unwrap();
        let mut status = |pad: usize| {
            let req = http::Request::builder()
                .method(http::Method::POST)
                .uri(format!("http://{addr}/svc/Unknown"))
                .header("x-pad", "a".repeat(pad))
                .body(())
                .unwrap();
            let (resp, _) = send_request.send_request(req, true).unwrap();
            async move { resp.await.map(|r| r.status()) }
        };
        let too_large = http::StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE;
        assert_ne!(status(16).await.unwrap(), too_large);
        assert_eq!(status(8192).await.unwrap(), too_large);
        let err = status(20_000).await.unwrap_err();
        assert!(
            err.is_go_away() && err.reason() == Some(h2::Reason::ENHANCE_YOUR_CALM),
            "connection not closed: {err}",
        );

        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("server did not shut down")
            .expect("join error")
            .expect("serve error");
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
        assert!(bound.connection_config().http2_adaptive_window());
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

        let advertised = read_advertised_setting(addr, SETTINGS_MAX_CONCURRENT_STREAMS_ID).await;
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

        let advertised = read_advertised_setting(addr, SETTINGS_MAX_CONCURRENT_STREAMS_ID).await;
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
    /// HTTP/2 SETTINGS_ENABLE_CONNECT_PROTOCOL identifier (RFC 8441 §3).
    const SETTINGS_ENABLE_CONNECT_PROTOCOL_ID: u16 = 0x8;

    /// The server advertises extended CONNECT, so HTTP/2 WebSocket requests
    /// reach the service.
    #[tokio::test]
    async fn extended_connect_is_advertised_in_settings() {
        let bound = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = bound.local_addr().unwrap();
        let serve = tokio::spawn(bound.serve(Router::new()));
        let advertised = read_advertised_setting(addr, SETTINGS_ENABLE_CONNECT_PROTOCOL_ID).await;
        assert_eq!(advertised, Some(1));
        serve.abort();
    }

    /// Open a raw HTTP/2 connection, send the client preface plus an empty
    /// SETTINGS frame, then read the server's initial SETTINGS frame and
    /// return the value it advertises for setting `id`, if present.
    async fn read_advertised_setting(addr: SocketAddr, id: u16) -> Option<u32> {
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
                return parse_setting(&payload, id);
            }
        }
    }

    /// Parse a SETTINGS frame payload (6-byte id/value entries) for the value
    /// of setting `id`.
    fn parse_setting(payload: &[u8], id: u16) -> Option<u32> {
        payload.chunks_exact(6).find_map(|entry| {
            (u16::from_be_bytes([entry[0], entry[1]]) == id)
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

    /// A request stays in flight until its response body is sent, so idle
    /// reaping never closes a connection in the middle of a streaming
    /// response.
    #[tokio::test(start_paused = true)]
    async fn max_connection_idle_waits_for_the_response_body() {
        use futures::StreamExt;
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let release = Arc::new(Mutex::new(Some(release_rx)));
        let router = Router::new().route_server_stream(
            "svc",
            "Tick",
            crate::handler::streaming_handler_fn(
                move |_ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let release = release.lock().unwrap().take();
                    async move {
                        let first = futures::stream::iter([Ok::<_, ConnectError>(
                            buffa_types::Empty::default(),
                        )]);
                        let second = futures::stream::once(async move {
                            if let Some(release) = release {
                                release.await.ok();
                            }
                            Ok(buffa_types::Empty::default())
                        });
                        crate::Response::stream_ok(first.chain(second))
                    }
                },
            ),
        );
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_max_connection_idle(Duration::from_secs(10))
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
            .uri(format!("http://{addr}/svc/Tick"))
            .header(header::CONTENT_TYPE, "application/connect+proto")
            .body(())
            .unwrap();
        let (resp, mut req_body) = send_request.send_request(req, false).unwrap();
        req_body
            .send_data(Bytes::from_static(&[0, 0, 0, 0, 0]), true)
            .unwrap();
        // tokio's paused clock can jump to the idle deadline during these
        // awaits. That is harmless: the connection lifecycle polls hyper, which
        // dispatches the request, before it checks for idleness.
        let mut resp = resp.await.expect("h2 request failed");
        assert!(resp.status().is_success(), "got status {}", resp.status());
        resp.body_mut()
            .data()
            .await
            .expect("stream ended before its first message")
            .expect("h2 response body failed");

        // The body is still open, so the connection is never idle, although the
        // idle timeout and the grace after it elapse several times over.
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(11)).await;
            yield_to_tasks().await;
        }
        assert!(
            !h2_task.is_finished(),
            "connection with an open response body was retired by the idle timer"
        );

        release_tx.send(()).unwrap();
        drain_h2_body(resp).await;
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("server did not shut down")
            .expect("join error")
            .expect("serve error");
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
    // Streaming calls that end before their request body does
    // ========================================================================

    /// A client-streaming route that refuses the call without reading its
    /// request stream.
    fn refusing_upload_router() -> Router {
        Router::new().route_client_stream(
            "svc",
            "Upload",
            crate::handler::client_streaming_handler_fn(
                |_ctx: crate::RequestContext,
                 _requests: crate::ServiceStream<buffa_types::Empty>| async move {
                    Err::<crate::Response<buffa_types::Empty>, _>(ConnectError::permission_denied(
                        "refused before reading",
                    ))
                },
            ),
        )
    }

    /// Send `data` on an h2 client stream, waiting for flow-control capacity.
    /// Stops quietly if the server has already ended the stream.
    async fn send_h2_data(send: &mut h2::SendStream<Bytes>, mut data: Bytes) {
        while !data.is_empty() {
            send.reserve_capacity(data.len());
            let capacity = tokio::time::timeout(
                Duration::from_secs(5),
                std::future::poll_fn(|cx| send.poll_capacity(cx)),
            )
            .await
            .expect("no send window: the server is not releasing flow control");
            let Some(Ok(capacity)) = capacity else {
                return; // reset by the server
            };
            let chunk = data.split_to(capacity.min(data.len()));
            if send.send_data(chunk, false).is_err() {
                return;
            }
        }
    }

    /// A server for [`refusing_upload_router`], shut down with [`Self::stop`].
    struct RefusingServer {
        addr: std::net::SocketAddr,
        shutdown_tx: tokio::sync::oneshot::Sender<()>,
        serve: tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    }

    impl RefusingServer {
        async fn start(bound: BoundServer) -> Self {
            let addr = bound.local_addr().unwrap();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let serve = tokio::spawn(async move {
                bound
                    .serve_with_graceful_shutdown(refusing_upload_router(), async {
                        shutdown_rx.await.ok();
                    })
                    .await
            });
            Self {
                addr,
                shutdown_tx,
                serve,
            }
        }

        /// Shut down once every client connection is closed.
        async fn stop(self) {
            self.shutdown_tx.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), self.serve)
                .await
                .expect("server did not shut down")
                .expect("join error")
                .expect("serve error");
        }

        /// An h2 client connection to the server, and its driving task.
        async fn connect_h2(
            &self,
        ) -> (
            h2::client::SendRequest<Bytes>,
            tokio::task::JoinHandle<Result<(), h2::Error>>,
        ) {
            let tcp = tokio::net::TcpStream::connect(self.addr).await.unwrap();
            let (send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
            (send_request, tokio::spawn(h2_conn))
        }

        /// The request head of a call to the refusing route.
        fn upload_request(&self) -> http::Request<()> {
            http::Request::post(format!("http://{}/svc/Upload", self.addr))
                .header(header::CONTENT_TYPE, "application/grpc")
                .header("te", "trailers")
                .body(())
                .unwrap()
        }

        /// Open one call on an h2 connection: request head, then the response
        /// (which arrives at once, whatever the client sends).
        async fn start_upload(
            &self,
            send_request: &mut h2::client::SendRequest<Bytes>,
        ) -> (h2::SendStream<Bytes>, http::Response<h2::RecvStream>) {
            let (resp, send) = send_request
                .send_request(self.upload_request(), false)
                .unwrap();
            let resp = resp.await.expect("response headers");
            assert_eq!(resp.headers()["grpc-status"], "7");
            (send, resp)
        }
    }

    /// Over HTTP/2 a call that ends before its request stream does releases
    /// the stream after the drain: the client receives the whole response
    /// and, once the server gives up on the rest of the request,
    /// `RST_STREAM(NO_ERROR)` (RFC 9113 §8.1), although it stalled part-way
    /// through a message. The single-stream limit shows the slot is freed.
    #[tokio::test]
    async fn http2_early_return_releases_stalled_stream() {
        use crate::service::DRAIN_TIMEOUT;

        let server = RefusingServer::start(
            Server::bind("127.0.0.1:0")
                .await
                .unwrap()
                .with_max_concurrent_streams(1),
        )
        .await;
        let (send_request, h2_task) = server.connect_h2().await;
        let mut send_request = send_request.ready().await.unwrap();

        // The server's drain starts after this, so it cannot end sooner.
        let started = Instant::now();
        let (mut send, resp) = server.start_upload(&mut send_request).await;
        drain_h2_body(resp).await;
        // A header declaring 1 MiB, then 32 KiB of it.
        let mut partial = vec![0_u8];
        partial.extend_from_slice(&(1024_u32 * 1024).to_be_bytes());
        partial.resize(5 + 32 * 1024, 0);
        send_h2_data(&mut send, Bytes::from(partial)).await;

        let reason = tokio::time::timeout(
            DRAIN_TIMEOUT + Duration::from_secs(3),
            std::future::poll_fn(|cx| send.poll_reset(cx)),
        )
        .await
        .expect("the stalled stream was not reset")
        .expect("reset");
        assert_eq!(reason, h2::Reason::NO_ERROR);
        assert!(
            started.elapsed() >= DRAIN_TIMEOUT,
            "reset before the drain was over: {:?}",
            started.elapsed()
        );

        // The slot is free again.
        send_request = tokio::time::timeout(Duration::from_secs(2), send_request.clone().ready())
            .await
            .expect("the stream slot was not freed")
            .unwrap();
        let (send, resp) = server.start_upload(&mut send_request).await;
        drain_h2_body(resp).await;

        drop(send);
        drop(send_request);
        server.stop().await;
        h2_task.await.expect("h2 connection task panicked").ok();
    }

    /// Small DATA frames that are still in flight when a call ends early
    /// must not cost the connection. h2 charges small frames it receives
    /// for a stream nobody reads to connection-wide budgets, and closes the
    /// connection with `GOAWAY(ENHANCE_YOUR_CALM)` when they run out, so a
    /// reader that gave up on the body as soon as the handler returned would
    /// take every other call on the connection down with it.
    ///
    /// A 105-byte frame costs 151 bytes of a budget of at least 25,600, so
    /// about 170 ignored frames are enough to trip it; the drop-at-once
    /// design fails this test after roughly 1700 calls.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http2_early_return_keeps_connection_alive_under_small_frames() {
        const CALLS: usize = 3000;
        const CONCURRENCY: usize = 20;
        const FRAMES_PER_CALL: usize = 10;

        let server = RefusingServer::start(Server::bind("127.0.0.1:0").await.unwrap()).await;
        let (send_request, h2_task) = server.connect_h2().await;
        let request = server.upload_request();
        let next = Arc::new(AtomicUsize::new(0));
        let answered = Arc::new(AtomicUsize::new(0));
        let callers: Vec<_> = (0..CONCURRENCY)
            .map(|_| {
                let (send_request, request, next, answered) = (
                    send_request.clone(),
                    request.clone(),
                    Arc::clone(&next),
                    Arc::clone(&answered),
                );
                tokio::spawn(async move {
                    let frame = Bytes::from(vec![0_u8; 105]);
                    while next.fetch_add(1, Ordering::Relaxed) < CALLS {
                        let Ok(mut send_request) = send_request.clone().ready().await else {
                            return;
                        };
                        let Ok((resp, mut send)) =
                            send_request.send_request(request.clone(), false)
                        else {
                            return;
                        };
                        for _ in 0..FRAMES_PER_CALL {
                            send_h2_data(&mut send, frame.clone()).await;
                            tokio::time::sleep(Duration::from_micros(100)).await;
                        }
                        let _ = send.send_data(Bytes::new(), true);
                        let Ok(Ok(resp)) = tokio::time::timeout(Duration::from_secs(5), resp).await
                        else {
                            return;
                        };
                        drain_h2_body(resp).await;
                        answered.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for caller in callers {
            caller.await.unwrap();
        }
        assert_eq!(answered.load(Ordering::Relaxed), CALLS, "calls were lost");
        assert!(!h2_task.is_finished(), "the connection was closed");

        drop(send_request);
        server.stop().await;
        h2_task.await.expect("h2 connection task panicked").ok();
    }

    /// Length of the message the upload request's envelope declares.
    const UPLOAD_MESSAGE_LEN: usize = 100;

    /// Head of an HTTP/1.1 request to the refusing route; its body is one
    /// Connect envelope (see [`upload_body`]).
    fn upload_head() -> Vec<u8> {
        format!(
            "POST /svc/Upload HTTP/1.1\r\n\
             Host: localhost\r\n\
             Content-Type: application/connect+proto\r\n\
             Content-Length: {}\r\n\
             \r\n",
            upload_body().len(),
        )
        .into_bytes()
    }

    /// One envelope: a 5-byte header, then the message it declares.
    fn upload_body() -> Vec<u8> {
        let mut body = vec![0_u8];
        body.extend_from_slice(&u32::try_from(UPLOAD_MESSAGE_LEN).unwrap().to_be_bytes());
        body.resize(5 + UPLOAD_MESSAGE_LEN, 0);
        body
    }

    /// Read a chunked HTTP/1.1 response through its final chunk.
    async fn read_chunked_response(stream: &mut tokio::net::TcpStream) -> String {
        let mut resp = Vec::new();
        let mut buf = [0; 1024];
        while !resp.ends_with(b"0\r\n\r\n") {
            let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
                .await
                .expect("response did not arrive")
                .unwrap();
            assert!(read > 0, "connection closed before full response arrived");
            resp.extend_from_slice(&buf[..read]);
        }
        String::from_utf8_lossy(&resp).into_owned()
    }

    /// Serve [`refusing_upload_router`] and connect to it over raw TCP.
    async fn refusing_upload_server() -> (tokio::net::TcpStream, RefusingServer) {
        let server = RefusingServer::start(Server::bind("127.0.0.1:0").await.unwrap()).await;
        (
            tokio::net::TcpStream::connect(server.addr).await.unwrap(),
            server,
        )
    }

    /// Over HTTP/1.1 the request body is the connection: a call that ends
    /// early while the client is still sending must leave the connection
    /// reusable, so the reader drains what the client sends after the
    /// response.
    #[tokio::test]
    async fn http1_early_return_keeps_connection_reusable() {
        let (mut stream, server) = refusing_upload_server().await;

        let body = upload_body();
        stream.write_all(&upload_head()).await.unwrap();
        stream.write_all(&body[..10]).await.unwrap();
        let first = read_chunked_response(&mut stream).await;
        assert!(first.starts_with("HTTP/1.1 200"), "{first}");
        assert!(first.contains("permission_denied"), "{first}");

        // The rest of the body arrives after the response, then the same
        // connection carries a second request.
        stream.write_all(&body[10..]).await.unwrap();
        stream.write_all(&upload_head()).await.unwrap();
        stream.write_all(&body).await.unwrap();
        let second = read_chunked_response(&mut stream).await;
        assert!(second.starts_with("HTTP/1.1 200"), "{second}");

        drop(stream);
        server.stop().await;
    }

    /// A client that stalls part-way through the body of a call that has
    /// already ended keeps the connection only for the bounded drain, after
    /// which the server closes it.
    #[tokio::test]
    async fn http1_early_return_closes_connection_of_stalled_client() {
        use crate::service::DRAIN_TIMEOUT;

        let (mut stream, server) = refusing_upload_server().await;

        // The server's drain starts after this, so it cannot end sooner.
        let started = Instant::now();
        stream.write_all(&upload_head()).await.unwrap();
        stream.write_all(&upload_body()[..10]).await.unwrap();
        let first = read_chunked_response(&mut stream).await;
        assert!(first.starts_with("HTTP/1.1 200"), "{first}");

        let mut buf = [0; 16];
        let closed = tokio::time::timeout(
            DRAIN_TIMEOUT + Duration::from_secs(2),
            stream.read(&mut buf),
        )
        .await
        .expect("the server still holds the connection of a stalled client");
        assert!(
            matches!(closed, Ok(0) | Err(_)),
            "expected the connection to close, read {closed:?}"
        );
        assert!(
            started.elapsed() >= DRAIN_TIMEOUT,
            "closed before the drain was over: {:?}",
            started.elapsed()
        );

        server.stop().await;
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

    #[derive(Clone, Debug, PartialEq)]
    struct ConnTag(usize);

    #[test]
    fn connection_types_are_thread_and_unwind_safe() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_unwind_safe<T: std::panic::UnwindSafe + std::panic::RefUnwindSafe>() {}
        assert_send_sync::<ConnectionInfo>();
        assert_send_sync::<ConnectionClosed>();
        assert_unwind_safe::<ConnectionClosed>();
        // The connection-extensions function must not take these from
        // `BoundServer`; with TLS, `rustls::ServerConfig` already does.
        #[cfg(not(feature = "server-tls"))]
        assert_unwind_safe::<BoundServer>();
    }

    /// `request_extensions` sets the built-ins from the transport over the
    /// connection's own extensions: a loop can neither replace `PeerAddr` /
    /// `PeerCerts` nor supply them when the transport saw none, but its own
    /// values survive.
    #[test]
    fn builtins_are_authoritative_over_inserted_extensions() {
        let real: SocketAddr = "127.0.0.1:4242".parse().unwrap();
        let spoofed: SocketAddr = "10.0.0.1:1".parse().unwrap();
        let mut info = ConnectionInfo::new().with_peer_addr(real);
        info.extensions_mut().insert(PeerAddr(spoofed));
        info.extensions_mut().insert(ConnTag(7));
        let ext = info.request_extensions();
        assert_eq!(ext.get::<PeerAddr>().unwrap().0, real);
        assert_eq!(ext.get::<ConnTag>(), Some(&ConnTag(7)));

        let ext = ConnectionInfo::new()
            .with_peer_addr(real)
            .request_extensions();
        assert_eq!(ext.get::<PeerAddr>().unwrap().0, real);
        assert!(ext.get::<ConnTag>().is_none());
        #[cfg(feature = "server-tls")]
        assert!(ext.get::<PeerCerts>().is_none());

        // No address, no `PeerAddr` — even if something inserted one.
        let mut info = ConnectionInfo::new();
        info.extensions_mut().insert(PeerAddr(spoofed));
        assert!(info.request_extensions().get::<PeerAddr>().is_none());
    }

    /// A `PeerCerts` inserted into the extensions of a connection without a
    /// verified client chain does not reach requests; a real chain does.
    #[cfg(feature = "server-tls")]
    #[test]
    fn extensions_cannot_forge_peer_certs() {
        let forged = PeerCerts(vec![rustls::pki_types::CertificateDer::from(vec![9u8])].into());
        let mut info = ConnectionInfo::new().with_peer_addr("127.0.0.1:1".parse().unwrap());
        info.extensions_mut().insert(forged);
        assert!(info.request_extensions().get::<PeerCerts>().is_none());

        let der = rustls::pki_types::CertificateDer::from(vec![1u8, 2, 3]);
        let info = ConnectionInfo::new().with_peer_certs(vec![der.clone()].into());
        assert_eq!(info.peer_certs().unwrap(), &[der.clone()][..]);
        let ext = info.request_extensions();
        assert_eq!(&ext.get::<PeerCerts>().unwrap().0[..], &[der][..]);
    }

    /// A hand-written accept loop over `Server::serve_connection`: the
    /// connection-scoped extension and the transport's `PeerAddr` reach the
    /// handler (a spoofed `PeerAddr` does not), the server's max connection
    /// age retires the connection, and the output names that reason; a
    /// connection the peer closes reports `Closed`.
    #[tokio::test(start_paused = true)]
    async fn serve_connection_custom_loop_applies_max_age_and_reports_reason() {
        type Seen = Arc<Mutex<Vec<(Option<SocketAddr>, Option<ConnTag>)>>>;
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let handler_seen = Arc::clone(&seen);
        let router = Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let seen = Arc::clone(&handler_seen);
                    async move {
                        seen.lock()
                            .unwrap()
                            .push((ctx.peer_addr(), ctx.extensions().get::<ConnTag>().cloned()));
                        crate::Response::ok(buffa_types::Empty::default())
                    }
                },
            ),
        );
        let server = Arc::new(Server::new(router).with_max_connection_age(Duration::from_secs(10)));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let loop_server = Arc::clone(&server);
        let serve = tokio::spawn(async move {
            let mut reasons = Vec::new();
            for n in 1..=2 {
                let (stream, peer) = listener.accept().await.unwrap();
                let mut info = ConnectionInfo::new().with_peer_addr(peer);
                info.extensions_mut().insert(ConnTag(n));
                info.extensions_mut()
                    .insert(PeerAddr("10.9.8.7:6".parse().unwrap()));
                let closed = loop_server
                    .serve_connection(stream, info, std::future::pending())
                    .await;
                reasons.push(closed.reason());
            }
            reasons
        });

        // Connection 1: keep-alive; retired by max age.
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let client1 = stream.local_addr().unwrap();
        stream.write_all(KEEPALIVE_ECHO_REQ).await.unwrap();
        let resp = read_http1_response(&mut stream).await;
        assert!(resp.starts_with(b"HTTP/1.1 2"));
        tokio::time::advance(Duration::from_secs(12)).await;
        yield_to_tasks().await;
        let mut buf = [0; 1];
        assert_eq!(stream.read(&mut buf).await.unwrap(), 0, "not retired");
        drop(stream);

        // Connection 2: `Connection: close`; ends on its own.
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let client2 = stream.local_addr().unwrap();
        stream.write_all(ECHO_REQ).await.unwrap();
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.unwrap();
        assert!(resp.starts_with(b"HTTP/1.1 2"));

        let reasons = tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("loop did not finish")
            .unwrap();
        assert_eq!(reasons, vec![CloseReason::MaxAge, CloseReason::Closed]);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                (Some(client1), Some(ConnTag(1))),
                (Some(client2), Some(ConnTag(2)))
            ]
        );
    }

    /// The free `serve_connection` builds nothing runtime-bound until first
    /// poll: constructed on a plain thread with age and idle timers configured,
    /// then served to completion by a runtime created afterwards.
    #[test]
    fn serve_connection_future_is_built_outside_a_runtime() {
        let router = Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                |_ctx: crate::RequestContext, _req: buffa_types::Empty| async move {
                    crate::Response::ok(buffa_types::Empty::default())
                },
            ),
        );
        let (mut client_io, server_io) = tokio::io::duplex(64 << 10);
        let config = ConnectionConfig::new()
            .with_max_connection_age(Duration::from_secs(60))
            .with_max_connection_idle(Duration::from_secs(30));
        // No runtime exists yet.
        let conn = serve_connection(
            server_io,
            ConnectionInfo::new(),
            ConnectRpcService::new(router),
            config,
            std::future::pending(),
        );

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let closed = runtime.block_on(async move {
            let conn = tokio::spawn(conn);
            client_io.write_all(ECHO_REQ).await.unwrap();
            let mut resp = Vec::new();
            client_io.read_to_end(&mut resp).await.unwrap();
            assert!(resp.starts_with(b"HTTP/1.1 2"));
            conn.await.unwrap()
        });
        assert_eq!(closed.reason(), CloseReason::Closed);
    }

    /// `Server::serve_connection` reports `Shutdown` when the shutdown future
    /// drains the connection and `MaxRequests` when the request count retires
    /// it.
    #[tokio::test]
    async fn serve_connection_reports_shutdown_and_max_requests() {
        let router = || {
            Router::new().route(
                "svc",
                "Echo",
                crate::handler_fn(
                    |_ctx: crate::RequestContext, _req: buffa_types::Empty| async move {
                        crate::Response::ok(buffa_types::Empty::default())
                    },
                ),
            )
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Shutdown: an idle keep-alive connection drains when the signal fires.
        let server = Server::new(router());
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (accepted, peer) = listener.accept().await.unwrap();
        let conn = tokio::spawn(server.serve_connection(
            accepted,
            ConnectionInfo::new().with_peer_addr(peer),
            async move {
                rx.await.ok();
            },
        ));
        stream.write_all(KEEPALIVE_ECHO_REQ).await.unwrap();
        assert!(
            read_http1_response(&mut stream)
                .await
                .starts_with(b"HTTP/1.1 2")
        );
        tx.send(()).unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(5), conn)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(closed.reason(), CloseReason::Shutdown);
        let mut buf = [0; 1];
        assert_eq!(stream.read(&mut buf).await.unwrap(), 0);

        // MaxRequests: one request allowed, then the connection is retired.
        let server =
            Server::new(router()).with_max_requests_per_connection(NonZeroU64::new(1).unwrap());
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (accepted, peer) = listener.accept().await.unwrap();
        let conn = tokio::spawn(server.serve_connection(
            accepted,
            ConnectionInfo::new().with_peer_addr(peer),
            std::future::pending(),
        ));
        stream.write_all(KEEPALIVE_ECHO_REQ).await.unwrap();
        assert!(
            read_http1_response(&mut stream)
                .await
                .starts_with(b"HTTP/1.1 2")
        );
        assert_eq!(stream.read(&mut buf).await.unwrap(), 0, "not retired");
        let closed = tokio::time::timeout(Duration::from_secs(5), conn)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(closed.reason(), CloseReason::MaxRequests);
    }

    /// A connection told to wind down before its peer sent a byte reports
    /// why, not `Error`, and no error; a peer that hangs up reports `Closed`,
    /// and a malformed request reports `Error` with the error.
    #[tokio::test(start_paused = true)]
    async fn serve_connection_close_reason_before_first_request() {
        fn unstarted(
            server: &Server,
            shutdown: impl Future<Output = ()> + Send + 'static,
        ) -> (
            impl Future<Output = ConnectionClosed>,
            tokio::io::DuplexStream,
        ) {
            let (io, client) = tokio::io::duplex(1024);
            let conn = server.serve_connection(io, ConnectionInfo::new(), shutdown);
            (conn, client)
        }
        let plain = Server::new(Router::new());
        let (conn, _client) = unstarted(&plain, std::future::ready(()));
        let closed = conn.await;
        assert_eq!(closed.reason(), CloseReason::Shutdown);
        assert!(closed.error().is_none(), "{:?}", closed.error());

        let idle = Server::new(Router::new()).with_max_connection_idle(Duration::from_secs(1));
        let (conn, _client) = unstarted(&idle, std::future::pending());
        let closed = conn.await;
        assert_eq!(closed.reason(), CloseReason::Idle);
        assert!(closed.error().is_none(), "{:?}", closed.error());

        let aged = Server::new(Router::new()).with_max_connection_age(Duration::from_secs(1));
        let (conn, _client) = unstarted(&aged, std::future::pending());
        let closed = conn.await;
        assert_eq!(closed.reason(), CloseReason::MaxAge);
        assert!(closed.error().is_none(), "{:?}", closed.error());

        let (conn, client) = unstarted(&plain, std::future::pending());
        drop(client);
        let closed = conn.await;
        assert_eq!(closed.reason(), CloseReason::Closed);
        assert!(closed.error().is_none(), "{:?}", closed.error());

        let (conn, mut client) = unstarted(&plain, std::future::pending());
        client.write_all(b"GARBAGE\r\n\r\n").await.unwrap();
        let closed = conn.await;
        assert_eq!(closed.reason(), CloseReason::Error);
        assert!(closed.error().is_some());
    }

    #[test]
    fn detection_ends_where_the_auto_connection_picks_a_protocol() {
        fn reads(chunks: &[&[u8]]) -> Vec<bool> {
            let mut detection = DetectionTimeout::new((), Some(tokio::time::Instant::now()))
                .detection
                .unwrap();
            chunks.iter().map(|read| detection.observe(read)).collect()
        }
        assert_eq!(reads(&[b"P", b"RI * ", b"HTTP"]), [false; 3]);
        assert_eq!(
            reads(&[b"PRI * HTTP/2.0\r\n", b"\r\nSM\r\n\r\n"]),
            [false, true]
        );
        assert_eq!(reads(&[H2_PREFACE]), [true]);
        assert_eq!(reads(&[b"GET "]), [true]);
        assert_eq!(reads(&[b"PRI", b" X"]), [false, true]);
        assert_eq!(reads(&[b"POST"]), [true]);
        assert_eq!(reads(&[b"PRI", b""]), [false, true]);
        assert_eq!(reads(&[b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\nX"]), [true]);
    }

    /// A peer that sends nothing, or only part of the HTTP/2 preface, is
    /// closed when the header-read timeout expires, with a `TimedOut` error.
    #[tokio::test(start_paused = true)]
    async fn header_read_timeout_closes_peer_before_protocol_detection() {
        const TIMEOUT: Duration = Duration::from_secs(10);
        let server = Server::new(Router::new()).with_header_read_timeout(TIMEOUT);
        for sent in [&b""[..], b"PRI * HTTP/2.0\r\n"] {
            let (io, mut client) = tokio::io::duplex(1024);
            let conn = server.serve_connection(io, ConnectionInfo::new(), std::future::pending());
            client.write_all(sent).await.unwrap();
            let started = tokio::time::Instant::now();
            let closed = tokio::time::timeout(Duration::from_secs(3600), conn)
                .await
                .unwrap_or_else(|_| panic!("peer that sent {sent:?} was never closed"));
            assert_eq!(started.elapsed(), TIMEOUT);
            assert_eq!(closed.reason(), CloseReason::HeaderReadTimeout);
            let err = closed
                .error()
                .and_then(|err| err.downcast_ref::<std::io::Error>());
            assert_eq!(
                err.map(std::io::Error::kind),
                Some(std::io::ErrorKind::TimedOut),
                "{:?}",
                closed.error()
            );
            assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
        }

        // Bytes that keep matching the preface do not extend the deadline.
        let (io, mut client) = tokio::io::duplex(1024);
        let conn = tokio::spawn(server.serve_connection(
            io,
            ConnectionInfo::new(),
            std::future::pending(),
        ));
        let started = tokio::time::Instant::now();
        for byte in &H2_PREFACE[..3] {
            client.write_all(&[*byte]).await.unwrap();
            tokio::time::sleep(TIMEOUT / 3 - Duration::from_millis(1)).await;
        }
        client.write_all(&H2_PREFACE[3..4]).await.unwrap();
        let closed = conn.await.unwrap();
        assert_eq!(started.elapsed(), TIMEOUT);
        assert_eq!(closed.reason(), CloseReason::HeaderReadTimeout);
    }

    /// A zero timeout, or one too long to add to the clock, disables both
    /// bounds; the latter would otherwise panic the connection.
    #[tokio::test(start_paused = true)]
    async fn zero_or_unrepresentable_header_read_timeout_disables_it() {
        for timeout in [Duration::ZERO, Duration::MAX] {
            let server = Server::new(Router::new()).with_header_read_timeout(timeout);
            let (io, mut client) = tokio::io::duplex(1024);
            let conn = tokio::spawn(server.serve_connection(
                io,
                ConnectionInfo::new(),
                std::future::pending(),
            ));
            let (head, rest) = ECHO_REQ.split_at(4);
            tokio::time::sleep(Duration::from_secs(3600)).await;
            assert!(!conn.is_finished(), "{timeout:?} closed a silent peer");
            client.write_all(head).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3600)).await;
            assert!(!conn.is_finished(), "{timeout:?} closed a partial head");
            client.write_all(rest).await.unwrap();
            let mut response = [0; 12];
            client.read_exact(&mut response).await.unwrap();
            assert_eq!(&response, b"HTTP/1.1 404", "{timeout:?}");
            drop(client);
            assert_eq!(conn.await.unwrap().reason(), CloseReason::Closed);
        }
    }

    /// An HTTP/2 keepalive ping that goes unanswered is an `Error`, not a
    /// header-read timeout, although both are hyper timeouts.
    #[tokio::test(start_paused = true)]
    async fn http2_keepalive_timeout_is_not_a_header_read_timeout() {
        // A SETTINGS frame: 3-byte length 0, type 4, no flags, stream 0.
        const EMPTY_SETTINGS: [u8; 9] = [0, 0, 0, 4, 0, 0, 0, 0, 0];
        let server = Server::new(Router::new())
            .with_header_read_timeout(Duration::from_secs(10))
            .with_http2_keepalive_interval(Duration::from_secs(1))
            .with_http2_keepalive_timeout(Duration::from_secs(1));
        let (io, mut client) = tokio::io::duplex(64 * 1024);
        let conn = server.serve_connection(io, ConnectionInfo::new(), std::future::pending());
        client.write_all(H2_PREFACE).await.unwrap();
        client.write_all(&EMPTY_SETTINGS).await.unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(60), conn)
            .await
            .expect("unanswered keepalive ping never closed the connection");
        assert_eq!(closed.reason(), CloseReason::Error, "{:?}", closed.error());
        let err = closed
            .error()
            .and_then(|err| err.downcast_ref::<hyper::Error>());
        assert!(
            err.is_some_and(hyper::Error::is_timeout),
            "{:?}",
            closed.error()
        );
    }

    #[test]
    fn header_read_timeout_is_recognised_by_error_type_not_kind() {
        let detection = std::io::Error::new(std::io::ErrorKind::TimedOut, DetectionTimedOut);
        assert!(is_header_read_timeout(&detection));
        let os_timeout = std::io::Error::from(std::io::ErrorKind::TimedOut);
        assert!(!is_header_read_timeout(&os_timeout));
    }

    /// A header-read timeout that expires while the connection drains keeps
    /// the drain's reason.
    #[tokio::test(start_paused = true)]
    async fn header_read_timeout_while_draining_keeps_the_reason() {
        const TIMEOUT: Duration = Duration::from_secs(10);
        let server = Server::new(Router::new()).with_header_read_timeout(TIMEOUT);
        let (io, mut client) = tokio::io::duplex(4096);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let conn = tokio::spawn(server.serve_connection(io, ConnectionInfo::new(), async {
            shutdown_rx.await.ok();
        }));
        client
            .write_all(b"POST /svc/Echo HTTP/1.1\r\n")
            .await
            .unwrap();
        // Under the paused clock this runs every task until it is idle.
        tokio::time::sleep(Duration::from_millis(1)).await;
        shutdown_tx.send(()).unwrap();
        let started = tokio::time::Instant::now();
        let closed = conn.await.unwrap();
        assert!(
            started.elapsed() >= TIMEOUT - Duration::from_millis(1),
            "closed after {:?}",
            started.elapsed()
        );
        assert_eq!(closed.reason(), CloseReason::Shutdown);
        let err = closed
            .error()
            .and_then(|err| err.downcast_ref::<hyper::Error>());
        assert!(
            err.is_some_and(hyper::Error::is_timeout),
            "{:?}",
            closed.error()
        );
    }

    /// Once an HTTP/1.1 request has started, hyper's own header-read timer
    /// takes over, starting when hyper begins reading the head.
    #[tokio::test(start_paused = true)]
    async fn header_read_timeout_hands_over_to_hyper_after_detection() {
        const TIMEOUT: Duration = Duration::from_secs(10);
        const DELAY: Duration = Duration::from_secs(9);
        let server = Server::new(Router::new()).with_header_read_timeout(TIMEOUT);
        let (io, mut client) = tokio::io::duplex(1024);
        let conn = server.serve_connection(io, ConnectionInfo::new(), std::future::pending());
        let conn = tokio::spawn(conn);
        let started = tokio::time::Instant::now();
        tokio::time::sleep(DELAY).await;
        client
            .write_all(b"POST /svc/Echo HTTP/1.1\r\n")
            .await
            .unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(3600), conn)
            .await
            .expect("stalled request head was never timed out")
            .unwrap();
        assert_eq!(started.elapsed(), DELAY + TIMEOUT);
        assert_eq!(closed.reason(), CloseReason::HeaderReadTimeout);
        let err = closed
            .error()
            .and_then(|err| err.downcast_ref::<hyper::Error>());
        assert!(
            err.is_some_and(hyper::Error::is_timeout),
            "{:?}",
            closed.error()
        );
    }

    /// A keep-alive connection that goes idle after a request is closed when
    /// the timeout expires, with hyper's timeout error.
    #[tokio::test(start_paused = true)]
    async fn header_read_timeout_closes_idle_keep_alive_connection() {
        const TIMEOUT: Duration = Duration::from_secs(10);
        let server = Server::new(Router::new()).with_header_read_timeout(TIMEOUT);
        let (io, mut client) = tokio::io::duplex(4096);
        let conn = tokio::spawn(server.serve_connection(
            io,
            ConnectionInfo::new(),
            std::future::pending(),
        ));
        client.write_all(KEEPALIVE_ECHO_REQ).await.unwrap();
        let mut response = [0; 12];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"HTTP/1.1 404");
        let answered = tokio::time::Instant::now();
        let closed = conn.await.unwrap();
        assert_eq!(answered.elapsed(), TIMEOUT);
        assert_eq!(closed.reason(), CloseReason::HeaderReadTimeout);
        let err = closed
            .error()
            .and_then(|err| err.downcast_ref::<hyper::Error>());
        assert!(
            err.is_some_and(hyper::Error::is_timeout),
            "{:?}",
            closed.error()
        );
    }

    /// An HTTP/2 client that has sent its preface may stay idle past the
    /// header-read timeout; only the retirement triggers bound it.
    #[tokio::test(start_paused = true)]
    async fn header_read_timeout_leaves_idle_http2_connections_open() {
        let server = Server::new(Router::new()).with_header_read_timeout(Duration::from_secs(10));
        let (io, client) = tokio::io::duplex(64 * 1024);
        let conn = tokio::spawn(server.serve_connection(
            io,
            ConnectionInfo::new(),
            std::future::pending(),
        ));
        let (send_request, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(connection);
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert!(!conn.is_finished(), "idle HTTP/2 connection was closed");

        let mut send_request = send_request.ready().await.unwrap();
        let request = http::Request::post("http://localhost/svc/Unknown")
            .body(())
            .unwrap();
        let (response, _) = send_request.send_request(request, true).unwrap();
        assert_eq!(
            response.await.unwrap().status(),
            http::StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
    }

    /// `with_header_read_timeout(None)` leaves a peer that sends nothing open
    /// indefinitely.
    #[tokio::test(start_paused = true)]
    async fn disabled_header_read_timeout_leaves_silent_peer_open() {
        let server = Server::new(Router::new()).with_header_read_timeout(None);
        let (io, _client) = tokio::io::duplex(1024);
        let conn = server.serve_connection(io, ConnectionInfo::new(), std::future::pending());
        assert!(
            tokio::time::timeout(Duration::from_secs(3600), conn)
                .await
                .is_err()
        );
    }

    /// A router whose one method, `svc/Echo`, never responds.
    fn never_responding_router() -> Router {
        Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                |_ctx: crate::RequestContext, _req: buffa_types::Empty| async move {
                    std::future::pending::<()>().await;
                    crate::Response::ok(buffa_types::Empty::default())
                },
            ),
        )
    }

    /// Request-count and idle retirement close a connection that has not
    /// drained once the configured grace period expires, and report no error.
    #[tokio::test(start_paused = true)]
    async fn retirement_closes_after_the_configured_grace() {
        const GRACE: Duration = Duration::from_secs(7);
        const IDLE: Duration = Duration::from_secs(1);

        // Retired as its one request is dispatched; the request never ends.
        let server = Server::new(never_responding_router())
            .with_max_requests_per_connection(NonZeroU64::MIN)
            .with_max_connection_age_grace(GRACE);
        let (io, mut client) = tokio::io::duplex(4096);
        let conn = server.serve_connection(io, ConnectionInfo::new(), std::future::pending());
        client.write_all(KEEPALIVE_ECHO_REQ).await.unwrap();
        let started = tokio::time::Instant::now();
        let closed = tokio::time::timeout(Duration::from_secs(60), conn)
            .await
            .expect("grace period never expired");
        assert_eq!(closed.reason(), CloseReason::MaxRequests);
        assert_eq!(started.elapsed(), GRACE);
        assert!(closed.error().is_none(), "{:?}", closed.error());

        // Idle with a request head half sent, which hyper keeps reading
        // through the drain. No header-read timeout, so only the grace period
        // can end it.
        let server = Server::new(never_responding_router())
            .with_max_connection_idle(IDLE)
            .with_max_connection_age_grace(GRACE)
            .with_header_read_timeout(None);
        let (io, mut client) = tokio::io::duplex(4096);
        let conn = server.serve_connection(io, ConnectionInfo::new(), std::future::pending());
        client
            .write_all(b"POST /svc/Echo HTTP/1.1\r\nHost: x\r\n")
            .await
            .unwrap();
        let started = tokio::time::Instant::now();
        let closed = tokio::time::timeout(Duration::from_secs(60), conn)
            .await
            .expect("grace period never expired");
        assert_eq!(closed.reason(), CloseReason::Idle);
        assert_eq!(started.elapsed(), IDLE + GRACE);
        assert!(closed.error().is_none(), "{:?}", closed.error());
    }

    /// A connection that fails while draining keeps the reason that started
    /// the drain and reports the failure through `error()`.
    #[tokio::test(start_paused = true)]
    async fn error_while_draining_keeps_the_reason() {
        let server = Server::new(never_responding_router())
            .with_max_requests_per_connection(NonZeroU64::MIN);
        let (io, mut client) = tokio::io::duplex(4096);
        let conn = tokio::spawn(server.serve_connection(
            io,
            ConnectionInfo::new(),
            std::future::pending(),
        ));
        client.write_all(KEEPALIVE_ECHO_REQ).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(client);
        let closed = tokio::time::timeout(Duration::from_secs(60), conn)
            .await
            .expect("connection did not end")
            .unwrap();
        assert_eq!(closed.reason(), CloseReason::MaxRequests);
        let error = closed.error_arc().expect("no error for the hang-up");
        assert!(
            error
                .downcast_ref::<hyper::Error>()
                .is_some_and(hyper::Error::is_incomplete_message),
            "{error:?}"
        );
    }

    /// A streaming body that panics after the response started ends the
    /// HTTP/1.1 connection with `Error`, instead of the panic unwinding out
    /// of `serve_connection`.
    #[tokio::test]
    async fn serve_connection_reports_error_when_a_response_body_panics() {
        use futures::StreamExt;
        let router = Router::new().route_server_stream(
            "svc",
            "Boom",
            crate::handler::streaming_handler_fn(
                |_ctx: crate::RequestContext, _req: buffa_types::Empty| async {
                    let first = futures::stream::iter([Ok(buffa_types::Empty::default())]);
                    let boom = futures::stream::poll_fn(
                        |_| -> Poll<Option<Result<buffa_types::Empty, ConnectError>>> {
                            panic!("response body panics on purpose")
                        },
                    );
                    crate::Response::stream_ok(first.chain(boom))
                },
            ),
        );
        let (io, mut client) = tokio::io::duplex(4096);
        let conn = tokio::spawn(Server::new(router).serve_connection(
            io,
            ConnectionInfo::new(),
            std::future::pending(),
        ));
        client
            .write_all(
                concat!(
                    "POST /svc/Boom HTTP/1.1\r\nHost: x\r\n",
                    "Content-Type: application/connect+proto\r\nContent-Length: 5\r\n\r\n",
                    "\0\0\0\0\0",
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(5), conn)
            .await
            .expect("connection did not end")
            .expect("serve_connection panicked");
        assert_eq!(closed.reason(), CloseReason::Error);
        assert!(closed.error().is_some());
    }

    #[test]
    fn fd_exhaustion_is_transient_and_distinguished() {
        #[cfg(not(windows))]
        let (exhausted, other_platform) = ([libc::EMFILE, libc::ENFILE], WSAEMFILE);
        // 24 is the C runtime's `EMFILE`, which Winsock does not return.
        #[cfg(windows)]
        let (exhausted, other_platform) = ([WSAEMFILE], 24);
        for code in exhausted {
            let err = std::io::Error::from_raw_os_error(code);
            assert!(
                is_transient_accept_error(&err) && is_fd_exhaustion(&err),
                "{err}"
            );
        }
        assert!(!is_fd_exhaustion(&std::io::Error::from_raw_os_error(
            other_platform
        )));
        let aborted = std::io::Error::from(std::io::ErrorKind::ConnectionAborted);
        assert!(is_transient_accept_error(&aborted) && !is_fd_exhaustion(&aborted));
    }

    /// The guide's runtime placement: a stream accepted on one runtime and
    /// re-registered on another with `into_std` / `from_std` is served there,
    /// with its handler running there, and keeps working after the accepting
    /// runtime has shut down.
    #[test]
    fn serve_connection_runs_where_the_stream_is_reregistered() {
        let runtime = |name: &'static str| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name(name)
                .enable_all()
                .build()
                .unwrap()
        };
        let handler_threads = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&handler_threads);
        let router = Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                move |_ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let thread = std::thread::current().name().map(str::to_owned);
                    seen.lock().unwrap().push(thread);
                    async { crate::Response::ok(buffa_types::Empty::default()) }
                },
            ),
        );
        let server = Server::new(router);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let accept_rt = runtime("accept-rt");
        let accepted = accept_rt.block_on(async {
            let listener = TcpListener::from_std(listener).unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            stream.into_std().unwrap()
        });
        drop(accept_rt);

        let serve_rt = runtime("serve-rt");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let conn = serve_rt.spawn(async move {
            let stream = tokio::net::TcpStream::from_std(accepted).unwrap();
            let shutdown = async move {
                shutdown_rx.await.ok();
            };
            server
                .serve_connection(stream, ConnectionInfo::new(), shutdown)
                .await
        });

        let client_rt = runtime("client-rt");
        // Hold the request handle: dropping it lets h2 close the idle
        // connection, racing the shutdown below.
        let _send_request = client_rt.block_on(async {
            client.set_nonblocking(true).unwrap();
            let tcp = tokio::net::TcpStream::from_std(client).unwrap();
            let (send_request, connection) = h2::client::handshake(tcp).await.unwrap();
            tokio::spawn(connection);
            let mut send_request = send_request.ready().await.unwrap();
            let req = http::Request::builder()
                .method(http::Method::POST)
                .uri(format!("http://{addr}/svc/Echo"))
                .header("content-type", "application/proto")
                .body(())
                .unwrap();
            let (resp, _) = send_request.send_request(req, true).unwrap();
            assert_eq!(resp.await.unwrap().status(), http::StatusCode::OK);
            send_request
        });

        shutdown_tx.send(()).unwrap();
        let closed = serve_rt
            .block_on(async { tokio::time::timeout(Duration::from_secs(5), conn).await })
            .expect("connection did not drain")
            .unwrap();
        assert_eq!(closed.reason(), CloseReason::Shutdown);
        assert_eq!(
            *handler_threads.lock().unwrap(),
            vec![Some("serve-rt".to_owned())]
        );
    }

    // ========================================================================
    // with_connection_extensions
    // ========================================================================

    /// Connect server-streaming request body for an empty message: one
    /// envelope (flags 0, length 0) and nothing else.
    const EMPTY_ENVELOPE: &[u8] = &[0, 0, 0, 0, 0];

    /// What each handler invocation observed: the connection's tag and `PeerAddr`.
    type Seen = Arc<Mutex<Vec<(Option<ConnTag>, Option<SocketAddr>)>>>;

    /// Router with a unary `svc/Echo` and a server-streaming `svc/Stream`,
    /// both recording the `ConnTag` and `PeerAddr` they observe.
    fn tag_capturing_router() -> (Router, Seen) {
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let unary_seen = Arc::clone(&seen);
        let stream_seen = Arc::clone(&seen);
        let router = Router::new()
            .route(
                "svc",
                "Echo",
                crate::handler_fn(
                    move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                        let seen = Arc::clone(&unary_seen);
                        async move {
                            seen.lock().unwrap().push((
                                ctx.extensions().get::<ConnTag>().cloned(),
                                ctx.peer_addr(),
                            ));
                            crate::Response::ok(buffa_types::Empty::default())
                        }
                    },
                ),
            )
            .route_server_stream(
                "svc",
                "Stream",
                crate::handler::streaming_handler_fn(
                    move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                        let seen = Arc::clone(&stream_seen);
                        async move {
                            seen.lock().unwrap().push((
                                ctx.extensions().get::<ConnTag>().cloned(),
                                ctx.peer_addr(),
                            ));
                            crate::Response::stream_ok(futures::stream::iter([Ok(
                                buffa_types::Empty::default(),
                            )]))
                        }
                    },
                ),
            );
        (router, seen)
    }

    /// POST a Connect request over h2 and drain the response; `streaming`
    /// selects the server-streaming `svc/Stream` route and envelope framing.
    async fn call_over_h2(
        send_request: &mut h2::client::SendRequest<Bytes>,
        addr: SocketAddr,
        streaming: bool,
    ) {
        let (path, content_type, body) = if streaming {
            ("svc/Stream", "application/connect+proto", EMPTY_ENVELOPE)
        } else {
            ("svc/Echo", "application/proto", &b""[..])
        };
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("http://{addr}/{path}"))
            .header(http::header::CONTENT_TYPE, content_type)
            .body(())
            .unwrap();
        let (resp, mut send_body) = send_request.send_request(req, false).unwrap();
        send_body.send_data(Bytes::from_static(body), true).unwrap();
        let resp = resp.await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        drain_h2_body(resp).await;
    }

    /// The function runs once per connection (not per request), sees the peer
    /// address and no certs on plaintext, and what it inserts reaches both
    /// unary and streaming handlers on every request of that connection; a
    /// `PeerAddr` it inserts does not displace the real one.
    #[tokio::test]
    async fn connection_extensions_run_once_per_connection_and_reach_handlers() {
        let (router, seen) = tag_capturing_router();
        let calls = Arc::new(AtomicUsize::new(0));
        // What the function observed per connection: (peer_addr, has_certs).
        type Peers = Arc<Mutex<Vec<(Option<SocketAddr>, bool)>>>;
        let peers: Peers = Arc::new(Mutex::new(Vec::new()));
        let (calls_in_fn, peers_in_fn) = (Arc::clone(&calls), Arc::clone(&peers));
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_connection_extensions(move |conn, ext| {
                let n = calls_in_fn.fetch_add(1, Ordering::SeqCst) + 1;
                #[cfg(feature = "server-tls")]
                let has_certs = conn.peer_certs().is_some();
                #[cfg(not(feature = "server-tls"))]
                let has_certs = false;
                peers_in_fn
                    .lock()
                    .unwrap()
                    .push((conn.peer_addr(), has_certs));
                ext.insert(ConnTag(n));
                // The transport's `PeerAddr` must win over this one.
                ext.insert(PeerAddr("10.0.0.1:1".parse().unwrap()));
            });
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        // Connection 1: three unary + two streaming requests on one h2
        // connection.
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let client1 = tcp.local_addr().unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(h2_conn);
        for i in 0..5 {
            call_over_h2(&mut send_request, addr, i >= 3).await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "once per connection");
        drop(send_request);

        // Connection 2 gets its own invocation and tag.
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let client2 = tcp.local_addr().unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(h2_conn);
        call_over_h2(&mut send_request, addr, false).await;
        call_over_h2(&mut send_request, addr, true).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(send_request);

        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("server did not shut down")
            .expect("join error")
            .expect("serve error");

        assert_eq!(
            *peers.lock().unwrap(),
            vec![(Some(client1), false), (Some(client2), false)],
            "the function sees the real peer and no certs on plaintext"
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 7);
        for (tag, peer) in &seen[..5] {
            assert_eq!(tag, &Some(ConnTag(1)));
            assert_eq!(peer, &Some(client1), "must not displace PeerAddr");
        }
        for (tag, peer) in &seen[5..] {
            assert_eq!(tag, &Some(ConnTag(2)));
            assert_eq!(peer, &Some(client2));
        }
    }

    /// A panicking function drops only its own connection: under
    /// `Server::serve_connection` the future reports `Error` instead of
    /// panicking, and the built-in loop keeps serving later connections.
    #[tokio::test]
    async fn connection_extensions_panic_drops_only_that_connection() {
        let (io, _client) = tokio::io::duplex(1024);
        let closed = Server::new(Router::new())
            .with_connection_extensions(|_, _| panic!("connection extensions panic on purpose"))
            .serve_connection(io, ConnectionInfo::new(), std::future::pending())
            .await;
        assert_eq!(closed.reason(), CloseReason::Error);
        let error = closed.error().expect("no error for the panic").to_string();
        assert!(error.contains("panic on purpose"), "{error}");

        let (router, seen) = tag_capturing_router();
        let first = std::sync::atomic::AtomicBool::new(true);
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_connection_extensions(move |_, ext| {
                if first.swap(false, Ordering::SeqCst) {
                    panic!("connection extensions panic on purpose");
                }
                ext.insert(ConnTag(1));
            });
        let addr = bound.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    shutdown_rx.await.ok();
                })
                .await
        });

        let mut dropped = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), dropped.read_to_end(&mut response))
            .await
            .expect("first connection was not dropped")
            .ok();
        assert!(response.is_empty(), "first connection was served");

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let client = tcp.local_addr().unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(h2_conn);
        call_over_h2(&mut send_request, addr, false).await;
        drop(send_request);

        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("server did not shut down")
            .expect("join error")
            .expect("serve error");
        assert_eq!(
            *seen.lock().unwrap(),
            vec![(Some(ConnTag(1)), Some(client))]
        );
    }

    /// Without a registered function, handlers see nothing extra and
    /// `PeerAddr` is unchanged.
    #[tokio::test]
    async fn connection_extensions_absent_when_unset() {
        let (router, seen) = tag_capturing_router();
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

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let client = tcp.local_addr().unwrap();
        let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(h2_conn);
        call_over_h2(&mut send_request, addr, false).await;
        call_over_h2(&mut send_request, addr, true).await;
        drop(send_request);

        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve)
            .await
            .expect("server did not shut down")
            .expect("join error")
            .expect("serve error");
        assert_eq!(
            *seen.lock().unwrap(),
            vec![(None, Some(client)), (None, Some(client))]
        );
    }

    /// Over mTLS the function sees the verified client chain — the same DER
    /// the handler later reads from `PeerCerts` — and can hand handlers a value
    /// derived from it; a `PeerCerts` it inserts itself does not stick.
    #[cfg(feature = "server-tls")]
    #[tokio::test]
    async fn connection_extensions_see_peer_certs_over_mtls() {
        let (server_cfg, client_cfg, expected_client_der) = pki();

        /// First DER byte length of the leaf, "parsed" once per connection.
        #[derive(Clone, Debug, PartialEq)]
        struct LeafLen(usize);

        type Captured = Arc<Mutex<Option<(Option<LeafLen>, Option<usize>)>>>;
        let captured: Captured = Arc::new(Mutex::new(None));
        let handler_captured = Arc::clone(&captured);
        let router = Router::new().route(
            "svc",
            "Echo",
            crate::handler_fn(
                move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                    let cap = Arc::clone(&handler_captured);
                    async move {
                        *cap.lock().unwrap() = Some((
                            ctx.extensions().get::<LeafLen>().cloned(),
                            ctx.peer_certs().map(|c| c[0].as_ref().len()),
                        ));
                        crate::Response::ok(buffa_types::Empty::default())
                    }
                },
            ),
        );

        let seen_certs: Arc<Mutex<Option<Vec<rustls::pki_types::CertificateDer<'static>>>>> =
            Arc::new(Mutex::new(None));
        let slot = Arc::clone(&seen_certs);
        let bound = Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .with_tls(server_cfg)
            .with_connection_extensions(move |conn, ext| {
                let certs = conn.peer_certs().map(<[_]>::to_vec);
                if let Some(leaf) = certs.as_deref().and_then(<[_]>::first) {
                    ext.insert(LeafLen(leaf.as_ref().len()));
                }
                *slot.lock().unwrap() = certs;
                let forged = rustls::pki_types::CertificateDer::from(vec![9u8]);
                ext.insert(PeerCerts(vec![forged].into()));
            });
        let addr = bound.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve = tokio::spawn(async move {
            bound
                .serve_with_graceful_shutdown(router, async {
                    rx.await.ok();
                })
                .await
        });

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

        let seen_certs = seen_certs.lock().unwrap().take().expect("saw certs");
        assert_eq!(seen_certs.len(), 1);
        assert_eq!(seen_certs[0].as_ref(), expected_client_der.as_ref());
        let (leaf_len, handler_len) = captured.lock().unwrap().take().expect("handler ran");
        assert_eq!(leaf_len, Some(LeafLen(expected_client_der.as_ref().len())));
        assert_eq!(handler_len, Some(expected_client_der.as_ref().len()));
    }

    /// `Server::serve_connection` applies the registered function too, so a
    /// custom loop around it and `Server::serve` stamp requests identically.
    /// The function sees what the loop inserted, and what it inserts replaces
    /// an entry of the same type.
    #[tokio::test]
    async fn server_serve_connection_applies_connection_extensions() {
        let (router, seen) = tag_capturing_router();
        let server = Server::new(router).with_connection_extensions(|conn, ext| {
            let from_loop = conn.extensions().get::<ConnTag>().map_or(0, |t| t.0);
            ext.insert(ConnTag(from_loop + 8));
        });
        let (mut client_io, server_io) = tokio::io::duplex(64 << 10);
        let mut info = ConnectionInfo::new().with_peer_addr("127.0.0.1:4242".parse().unwrap());
        info.extensions_mut().insert(ConnTag(1));
        let conn = tokio::spawn(server.serve_connection(server_io, info, std::future::pending()));
        client_io.write_all(ECHO_REQ).await.unwrap();
        let mut resp = Vec::new();
        client_io.read_to_end(&mut resp).await.unwrap();
        assert!(resp.starts_with(b"HTTP/1.1 200"));
        assert_eq!(conn.await.unwrap().reason(), CloseReason::Closed);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![(Some(ConnTag(9)), Some("127.0.0.1:4242".parse().unwrap()))]
        );
    }

    /// Minimal mTLS PKI: one CA → one server leaf + one client leaf.
    /// Returns (server_config, client_config, client_cert_der).
    #[cfg(feature = "server-tls")]
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

    /// End-to-end mTLS: client presents a cert; handler reads it from
    /// `ctx.peer_certs()` and the DER bytes round-trip.
    #[cfg(feature = "server-tls")]
    #[tokio::test]
    async fn peer_certs_reach_handler() {
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
