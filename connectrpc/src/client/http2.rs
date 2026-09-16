//! Raw HTTP/2 connection transport with honest `poll_ready`.
//!
//! [`HttpClient`](super::HttpClient) wraps `hyper_util::client::legacy::Client`,
//! which pools connections internally and always returns `Ready(Ok)` from
//! `poll_ready`. For HTTP/2, that pool holds *one* shared connection — all
//! concurrent requests multiplex over it and contend on h2's internal
//! `Mutex<Inner>` (11–15% CPU at high req/s, see [h2 #531]).
//!
//! This module provides [`Http2Connection`] — a single raw HTTP/2 connection
//! with no internal pool. Its `poll_ready` reflects *real* connection state
//! (closed / still connecting / ready-for-streams), so it composes correctly
//! with `tower::balance::p2c::Balance` and `tower::load::PendingRequests`.
//!
//! Use a `Vec<Http2Connection>` inside a balancer to spread load across N
//! connections and reduce h2 mutex contention by ~1/N per connection.
//!
//! # Relationship to `HttpClient`
//!
//! | | `HttpClient` | `Http2Connection` |
//! |---|---|---|
//! | Protocol | HTTP/1.1 + HTTP/2 (ALPN) | HTTP/2 only |
//! | `poll_ready` | always `Ready` (internal queue) | **honest** |
//! | Connection count per host | 1 (h2) / N (h/1.1) | exactly 1 |
//! | Composes with `tower::balance` | degraded (random) | yes |
//! | Reconnect on drop | automatic (pool) | automatic ([`Reconnect`] wrapper) |
//!
//! Use [`HttpClient`](super::HttpClient) when you don't know the protocol or
//! don't care about contention. Use [`Http2Connection`] when you know it's
//! gRPC/h2 and want N-connection balancing.
//!
//! [h2 #531]: https://github.com/hyperium/h2/issues/531

use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use http::Request;
use http::Response;
use http::Uri;

use super::{BoxFuture, ClientBody, ClientTransport};
use crate::error::ConnectError;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ============================================================================
// TLS support types and helpers
// ============================================================================

use std::sync::Arc;

/// A boxed bidirectional IO stream. Used to unify the concrete types
/// `TokioIo<TcpStream>` (plaintext) and `TokioIo<TlsStream<TcpStream>>` (TLS)
/// so `handshake()` can be called once. Same pattern as tonic's `BoxedIo`.
///
/// The box allocation is once per connection (not per request) — negligible.
///
/// A combining supertrait is needed because `dyn TraitA + TraitB` only works
/// when at most one trait is non-auto (Read and Write are both non-auto).
trait H2Io: hyper::rt::Read + hyper::rt::Write + Send + Unpin {}
impl<T: hyper::rt::Read + hyper::rt::Write + Send + Unpin> H2Io for T {}
type BoxedIo = Pin<Box<dyn H2Io>>;

/// Type-erased connector stored in `MakeSendRequest.custom`. Callers of
/// [`Http2Connection::lazy_with_connector`] provide an unboxed `C`; it's
/// normalized to this shape via `ServiceExt::map_response` + `map_err` +
/// `tower::util::BoxService::new`.
type BoxedConnector = tower::util::BoxService<Uri, BoxedIo, BoxError>;

/// Normalize a caller's connector to `BoxedConnector`: box the IO, coerce
/// the error, box the future. Callers just return their concrete stream
/// type (e.g. `TokioIo<UnixStream>`) and any `Into<BoxError>` error.
fn box_connector<C>(connector: C) -> BoxedConnector
where
    C: tower::Service<Uri> + Send + 'static,
    C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin + 'static,
    C::Error: Into<BoxError>,
    C::Future: Send + 'static,
{
    use tower::ServiceExt;
    tower::util::BoxService::new(
        connector
            .map_response(|io| Box::pin(io) as BoxedIo)
            .map_err(Into::into),
    )
}

/// Build a connector that dials a Unix domain socket. The URI argument
/// is ignored — `:authority` is supplied separately to
/// [`Http2Connection::lazy_unix`].
#[cfg(unix)]
fn unix_connector(
    path: std::path::PathBuf,
) -> impl tower::Service<
    Uri,
    Response = hyper_util::rt::TokioIo<tokio::net::UnixStream>,
    Error = ConnectError,
    Future: Send + 'static,
> + Send
+ 'static {
    tower::service_fn(move |_uri: Uri| {
        let path = path.clone();
        async move {
            let stream = tokio::net::UnixStream::connect(&path).await.map_err(|e| {
                ConnectError::unavailable_from_transport(
                    format_args!("unix socket connect to {} failed", path.display()),
                    e,
                )
            })?;
            Ok(hyper_util::rt::TokioIo::new(stream))
        }
    })
}

/// Prepare a TLS config for HTTP/2: clone the caller's config and set ALPN.
///
/// The clone preserves the `Arc<dyn ResolvesClientCert>` inside — cert
/// rotation via a shared resolver is unaffected.
#[cfg(feature = "client-tls")]
fn prepare_tls_for_h2(config: &Arc<rustls::ClientConfig>) -> Arc<rustls::ClientConfig> {
    let mut cfg = (**config).clone();
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    Arc::new(cfg)
}

/// Extract the server name for TLS SNI/certificate validation from a URI's host.
#[cfg(feature = "client-tls")]
fn server_name_from_uri(uri: &Uri) -> Result<rustls_pki_types::ServerName<'static>, ConnectError> {
    let host = uri.host().ok_or_else(|| {
        ConnectError::invalid_argument("URI must have a host for TLS server name resolution")
    })?;
    // `Uri::host()` includes brackets for IPv6 literals (e.g. `[::1]`). Strip
    // them so `ServerName::try_from` parses the address as `IpAddress`
    // instead of rejecting it as an invalid DNS name.
    let stripped = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);
    rustls_pki_types::ServerName::try_from(stripped.to_owned()).map_err(|e| {
        ConnectError::invalid_argument(format!("invalid TLS server name '{host}': {e}"))
    })
}

/// Check the URI scheme is `https` (not `http`). The TLS constructors
/// reject `http://` to prevent silently skipping TLS when the user
/// explicitly asked for it.
#[cfg(feature = "client-tls")]
fn require_https_scheme(uri: &Uri) -> Result<(), ConnectError> {
    match uri.scheme_str() {
        Some("https") => Ok(()),
        Some("http") | None => Err(ConnectError::invalid_argument(
            "Http2Connection TLS constructors require https:// scheme; \
             use connect_plaintext/lazy_plaintext for http://",
        )),
        Some(other) => Err(ConnectError::invalid_argument(format!(
            "unsupported URI scheme: {other}"
        ))),
    }
}

// ============================================================================
// Http2Connection — the public transport type
// ============================================================================

/// A single raw HTTP/2 connection with honest tower-service semantics.
///
/// See the [`client` module docs](super) for the design rationale and a
/// comparison to [`HttpClient`](super::HttpClient).
///
/// # Example: single connection
///
/// ```rust,ignore
/// use connectrpc::client::Http2Connection;
///
/// let conn = Http2Connection::connect_plaintext("http://localhost:8080".parse()?).await?;
/// let client = MyServiceClient::new(conn, config);
/// ```
///
/// # Example: N-connection balance
///
/// ```rust,ignore
/// use tower::balance::p2c::Balance;
/// use tower::discover::ServiceList;
/// use tower::load::PendingRequestsDiscover;
/// use tower::load::completion::CompleteOnResponse;
///
/// let uri: http::Uri = "http://localhost:8080".parse()?;
/// let conns: Vec<_> = (0..8)
///     .map(|_| Http2Connection::lazy_plaintext(uri.clone()))
///     .collect();
///
/// let discover = ServiceList::new(conns);
/// let discover = PendingRequestsDiscover::new(discover, CompleteOnResponse::default());
/// let balance = Balance::new(discover);
///
/// // `balance` is a tower::Service — wrap it in ServiceTransport:
/// let client = MyServiceClient::new(
///     connectrpc::client::ServiceTransport::new(balance),
///     config,
/// );
/// ```
pub struct Http2Connection {
    /// Reconnect-wrapped connection: if the underlying h2 connection drops
    /// (server restart, network blip), the next `poll_ready` re-establishes it.
    inner: Reconnect<MakeSendRequest>,
}

// Manual impl: `Reconnect` holds a boxed `Future` and hyper's `SendRequest`
// which don't impl `Debug`. Surface the target URI and connection state so
// tests can `.unwrap_err()` on `Result<Http2Connection, _>`.
impl std::fmt::Debug for Http2Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self.inner.state {
            ReconnectState::Idle => "Idle",
            ReconnectState::Connecting(_) => "Connecting",
            ReconnectState::Connected(_) => "Connected",
        };
        f.debug_struct("Http2Connection")
            .field("uri", &self.inner.uri)
            .field("state", &state)
            .field("has_connected", &self.inner.has_connected)
            .finish()
    }
}

/// Check the URI scheme is `http` (not `https`). The plaintext
/// constructors reject `https://` to prevent accidental cleartext
/// connections to TLS endpoints.
fn require_http_scheme(uri: &Uri) -> Result<(), ConnectError> {
    match uri.scheme_str() {
        Some("http") | None => Ok(()),
        Some("https") => Err(ConnectError::invalid_argument(
            "Http2Connection plaintext constructors require http:// scheme; \
             use connect_tls/lazy_tls for https://",
        )),
        Some(other) => Err(ConnectError::invalid_argument(format!(
            "unsupported URI scheme: {other}"
        ))),
    }
}

impl Http2Connection {
    /// Returns a builder for configuring connection establishment (timeout
    /// bounds, HTTP/2 keep-alive, flow-control windows) before choosing a
    /// transport flavour.
    ///
    /// The constructors on `Http2Connection` are shortcuts for the builder
    /// with default settings, so `Http2Connection::lazy_plaintext(uri)` is
    /// exactly `Http2Connection::builder().lazy_plaintext(uri)`. The default
    /// establishment bounds are finite — see
    /// [`Http2ConnectionBuilder::establishment_timeout`] and
    /// [`Http2ConnectionBuilder::tcp_connect_timeout`].
    pub fn builder() -> Http2ConnectionBuilder {
        Http2ConnectionBuilder::default()
    }

    /// Create a **plaintext** h2c connection that establishes lazily on
    /// first `poll_ready`. Only for `http://` URIs.
    ///
    /// The TCP+h2 handshake happens inside `poll_ready`, so the first request
    /// sees connect latency. Use this when building a balance pool — services
    /// that aren't selected won't eagerly connect.
    ///
    /// Connection establishment is bounded by [`DEFAULT_ESTABLISHMENT_TIMEOUT`]
    /// (and [`DEFAULT_TCP_CONNECT_TIMEOUT`] per address); use
    /// [`builder()`](Self::builder) to adjust or opt out.
    ///
    /// # Errors
    ///
    /// Returns an error (surfaced from the first `poll_ready`) if the URI
    /// scheme is `https://` — use [`lazy_tls`](Self::lazy_tls) instead.
    #[must_use]
    pub fn lazy_plaintext(uri: Uri) -> Self {
        Self::builder().lazy_plaintext(uri)
    }

    /// Eagerly establish a **plaintext** h2c connection now.
    /// Only for `http://` URIs.
    ///
    /// After the initial connect succeeds, reconnect-on-failure is handled
    /// automatically by the next `poll_ready`.
    ///
    /// Connection establishment is bounded by [`DEFAULT_ESTABLISHMENT_TIMEOUT`]
    /// (and [`DEFAULT_TCP_CONNECT_TIMEOUT`] per address); use
    /// [`builder()`](Self::builder) to adjust or opt out.
    ///
    /// # Errors
    ///
    /// Returns an error if the URI scheme is `https://` (use
    /// [`connect_tls`](Self::connect_tls) instead) or the initial TCP connect
    /// or h2 handshake fails.
    pub async fn connect_plaintext(uri: Uri) -> Result<Self, ConnectError> {
        Self::builder().connect_plaintext(uri).await
    }

    /// Customize the HTTP/2 settings (window sizes, keep-alive, etc).
    ///
    /// Plaintext only; uses [`lazy_plaintext`](Self::lazy_plaintext) semantics
    /// — the connection establishes on first `poll_ready`.
    #[deprecated(
        since = "0.8.0",
        note = "use `Http2Connection::builder()` and configure via the proxied \
                keep-alive/window setters or `h2_settings(|b| ...)`"
    )]
    #[must_use]
    pub fn with_builder_plaintext(
        uri: Uri,
        mut builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
    ) -> Self {
        builder.timer(hyper_util::rt::TokioTimer::new());
        let mut b = Self::builder();
        b.h2_builder = builder;
        b.lazy_plaintext(uri)
    }

    /// Create an h2c connection using a **caller-supplied connector** that
    /// establishes lazily on first `poll_ready`.
    ///
    /// The connector may return any stream implementing `hyper::rt::Read +
    /// Write + Send + Unpin` — boxing happens internally. The h2 handshake
    /// runs over that stream after the connector resolves. This is the
    /// escape hatch for transports the built-in constructors don't cover
    /// (Unix sockets, in-memory pipes, pre-wrapped mTLS, etc.) — same
    /// pattern as tonic's `Endpoint::connect_with_connector`.
    ///
    /// `authority` becomes the HTTP/2 `:authority` pseudo-header and the
    /// base for request path construction (`{authority}/{service}/{method}`).
    /// For local IPC, `http://localhost` is typical.
    ///
    /// # Example
    ///
    /// ```ignore
    /// # use connectrpc::client::Http2Connection;
    /// # use http::Uri;
    /// let conn = Http2Connection::lazy_with_connector(
    ///     tower::service_fn(|_uri: Uri| async {
    ///         let stream = tokio::net::UnixStream::connect("/tmp/app.sock").await?;
    ///         Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
    ///     }),
    ///     "http://localhost".parse().unwrap(),
    /// );
    /// ```
    ///
    /// Connection establishment is bounded by [`DEFAULT_ESTABLISHMENT_TIMEOUT`];
    /// use [`builder()`](Self::builder) to adjust or opt out.
    #[must_use]
    pub fn lazy_with_connector<C>(connector: C, authority: Uri) -> Self
    where
        C: tower::Service<Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin + 'static,
        C::Error: Into<BoxError>,
        C::Future: Send + 'static,
    {
        Self::builder().lazy_with_connector(connector, authority)
    }

    /// Eagerly establish an h2c connection using a **caller-supplied connector**.
    ///
    /// Connection establishment is bounded by [`DEFAULT_ESTABLISHMENT_TIMEOUT`];
    /// use [`builder()`](Self::builder) to adjust or opt out.
    ///
    /// # Errors
    ///
    /// Returns an error if the connector or h2 handshake fails. See
    /// [`lazy_with_connector`](Self::lazy_with_connector) for details.
    pub async fn connect_with_connector<C>(
        connector: C,
        authority: Uri,
    ) -> Result<Self, ConnectError>
    where
        C: tower::Service<Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin + 'static,
        C::Error: Into<BoxError>,
        C::Future: Send + 'static,
    {
        Self::builder()
            .connect_with_connector(connector, authority)
            .await
    }

    /// Create an h2c connection over a **Unix domain socket** that
    /// establishes lazily on first `poll_ready`. Convenience wrapper over
    /// [`lazy_with_connector`](Self::lazy_with_connector).
    ///
    /// The server must speak h2c (cleartext HTTP/2) on the socket —
    /// `connect-go` servers do by default via `h2c.NewHandler`.
    ///
    /// `authority` sets the HTTP/2 `:authority` pseudo-header. For
    /// local IPC sockets, `http://localhost` is typical; the server
    /// generally doesn't validate it.
    ///
    /// Connection establishment is bounded by [`DEFAULT_ESTABLISHMENT_TIMEOUT`];
    /// use [`builder()`](Self::builder) to adjust or opt out.
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    #[must_use]
    pub fn lazy_unix(path: impl Into<std::path::PathBuf>, authority: Uri) -> Self {
        Self::builder().lazy_unix(path, authority)
    }

    /// Eagerly establish an h2c connection over a **Unix domain socket**.
    ///
    /// Connection establishment is bounded by [`DEFAULT_ESTABLISHMENT_TIMEOUT`];
    /// use [`builder()`](Self::builder) to adjust or opt out.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket path doesn't exist or the h2
    /// handshake fails. See [`lazy_unix`](Self::lazy_unix) for details.
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    pub async fn connect_unix(
        path: impl Into<std::path::PathBuf>,
        authority: Uri,
    ) -> Result<Self, ConnectError> {
        Self::builder().connect_unix(path, authority).await
    }

    /// Create a **TLS** h2 connection that establishes lazily on first
    /// `poll_ready`. Only for `https://` URIs.
    ///
    /// ALPN is set to `["h2"]`. After the TLS handshake, the negotiated
    /// ALPN protocol is checked — if the server didn't negotiate h2, the
    /// connection fails with a clear error (rather than a cryptic h2
    /// handshake failure).
    ///
    /// # Certificate rotation
    ///
    /// The config may contain a custom `ResolvesClientCert` for dynamic
    /// cert rotation. `rustls::ClientConfig` stores it as
    /// `Arc<dyn ResolvesClientCert>`, so the clone done here to set ALPN
    /// shares the same resolver instance — rotation keeps working.
    ///
    /// Connection establishment is bounded by [`DEFAULT_ESTABLISHMENT_TIMEOUT`]
    /// (and [`DEFAULT_TCP_CONNECT_TIMEOUT`] per address); use
    /// [`builder()`](Self::builder) to adjust or opt out.
    ///
    /// # Errors
    ///
    /// Returns an error (surfaced from the first `poll_ready`) if the URI
    /// scheme is `http://` — use [`lazy_plaintext`](Self::lazy_plaintext).
    #[cfg(feature = "client-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client-tls")))]
    #[must_use]
    pub fn lazy_tls(uri: Uri, tls_config: Arc<rustls::ClientConfig>) -> Self {
        Self::builder().lazy_tls(uri, tls_config)
    }

    /// Eagerly establish a **TLS** h2 connection now. Only for `https://` URIs.
    ///
    /// See [`lazy_tls`](Self::lazy_tls) for ALPN and cert rotation details.
    ///
    /// Connection establishment is bounded by [`DEFAULT_ESTABLISHMENT_TIMEOUT`]
    /// (and [`DEFAULT_TCP_CONNECT_TIMEOUT`] per address); use
    /// [`builder()`](Self::builder) to adjust or opt out.
    ///
    /// # Errors
    ///
    /// Returns an error if the URI scheme is `http://`, the TCP/TLS handshake
    /// fails, or the server doesn't negotiate h2 via ALPN.
    #[cfg(feature = "client-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client-tls")))]
    pub async fn connect_tls(
        uri: Uri,
        tls_config: Arc<rustls::ClientConfig>,
    ) -> Result<Self, ConnectError> {
        Self::builder().connect_tls(uri, tls_config).await
    }

    /// Customize the HTTP/2 settings (window sizes, keep-alive, etc) with TLS.
    ///
    /// TLS-only; uses lazy semantics — the connection establishes on
    /// first `poll_ready`. See [`lazy_tls`](Self::lazy_tls) for ALPN and
    /// cert rotation details.
    #[deprecated(
        since = "0.8.0",
        note = "use `Http2Connection::builder()` and configure via the proxied \
                keep-alive/window setters or `h2_settings(|b| ...)`"
    )]
    #[cfg(feature = "client-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client-tls")))]
    #[must_use]
    pub fn with_builder_tls(
        uri: Uri,
        mut builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
        tls_config: Arc<rustls::ClientConfig>,
    ) -> Self {
        builder.timer(hyper_util::rt::TokioTimer::new());
        let mut b = Self::builder();
        b.h2_builder = builder;
        b.lazy_tls(uri, tls_config)
    }
}

/// Builder for [`Http2Connection`] connection-establishment bounds.
///
/// Obtain one via [`Http2Connection::builder`]. The terminal methods mirror the
/// `Http2Connection` constructors; the bare constructors delegate here with
/// default settings.
///
/// Establishment is bounded by default ([`DEFAULT_ESTABLISHMENT_TIMEOUT`] /
/// [`DEFAULT_TCP_CONNECT_TIMEOUT`]), so a server that accepts the TCP
/// connection but stalls during the TLS handshake cannot stall `poll_ready` for
/// every caller sharing the connection. The bounds here are the *establishment*
/// budget; [`CallOptions::with_timeout`] remains the end-to-end per-request
/// bound.
///
/// # Scope
///
/// The builder covers every [`Http2Connection`] transport: plaintext, TLS,
/// caller-supplied connectors, and Unix sockets. HTTP/2 keep-alive and
/// flow-control knobs are proxied directly; for hyper settings not surfaced
/// here use [`h2_settings`](Self::h2_settings). [`tcp_connect_timeout`] (the
/// per-address TCP bound) and [`local_address`] (the source-address bind)
/// configure the built-in connector and are ignored by the custom-connector /
/// Unix-socket terminals — use [`establishment_timeout`] there as the
/// establishment bound, and bind inside your own connector.
///
/// [`tcp_connect_timeout`]: Self::tcp_connect_timeout
/// [`local_address`]: Self::local_address
/// [`establishment_timeout`]: Self::establishment_timeout
///
/// [`CallOptions::with_timeout`]: super::CallOptions::with_timeout
#[derive(Debug, Clone)]
#[must_use = "call a lazy_*/connect_* terminal to build the connection"]
pub struct Http2ConnectionBuilder {
    tcp_connect_timeout: Option<Duration>,
    establishment_timeout: Option<Duration>,
    local_address: Option<IpAddr>,
    h2_builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
}

/// Default wall-clock bound on connection establishment (DNS, TCP, TLS, h2
/// preface). Override via [`Http2ConnectionBuilder::establishment_timeout`].
/// Same default value as grpc-go's `MinConnectTimeout` (note: grpc-go's is a
/// floor that grows with exponential backoff; this is a fixed ceiling).
pub const DEFAULT_ESTABLISHMENT_TIMEOUT: Duration = Duration::from_secs(20);

/// Default TCP `connect(2)` budget on the built-in connector. hyper divides
/// this evenly across the resolved address set (e.g. 8 addresses → 625ms each).
/// Override via [`Http2ConnectionBuilder::tcp_connect_timeout`].
pub const DEFAULT_TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Normalize a setter argument: `Duration::MAX` is treated as `None` (no timer
/// at all) rather than arming a saturated tokio timer. The explicit
/// `no_*_timeout()` builder methods are the documented opt-out; this keeps the
/// `MAX` spelling working for callers who reach for it.
pub(super) fn finite(dur: Duration) -> Option<Duration> {
    (dur != Duration::MAX).then_some(dur)
}

impl Default for Http2ConnectionBuilder {
    /// A fresh builder with [`DEFAULT_ESTABLISHMENT_TIMEOUT`] /
    /// [`DEFAULT_TCP_CONNECT_TIMEOUT`] and default HTTP/2 settings. A
    /// [`TokioTimer`](hyper_util::rt::TokioTimer) is pre-wired so the
    /// keep-alive setters work without the caller having to install one (hyper
    /// otherwise panics at handshake time if a keep-alive interval is set
    /// without a timer).
    fn default() -> Self {
        let mut h2_builder =
            hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
        h2_builder.timer(hyper_util::rt::TokioTimer::new());
        Self {
            tcp_connect_timeout: Some(DEFAULT_TCP_CONNECT_TIMEOUT),
            establishment_timeout: Some(DEFAULT_ESTABLISHMENT_TIMEOUT),
            local_address: None,
            h2_builder,
        }
    }
}

impl Http2ConnectionBuilder {
    /// Set the per-address TCP connect budget.
    ///
    /// Applied to the built-in connector via hyper's
    /// [`HttpConnector::set_connect_timeout`][hyper-ct]; it covers only the TCP
    /// `connect(2)` call (per resolved address — the budget is divided across
    /// the address set), so the connector moves on to the next address rather
    /// than burning the whole [`establishment_timeout`] on one blackholed peer.
    /// It is **ignored** by the custom-connector / Unix-socket terminals (which
    /// have no built-in `HttpConnector` to apply it to).
    ///
    /// Defaults to [`DEFAULT_TCP_CONNECT_TIMEOUT`]. To disable, use
    /// [`no_tcp_connect_timeout`](Self::no_tcp_connect_timeout). Passing
    /// `Duration::ZERO` causes every per-address connect to fail immediately.
    ///
    /// [`establishment_timeout`]: Self::establishment_timeout
    /// [hyper-ct]: hyper_util::client::legacy::connect::HttpConnector::set_connect_timeout
    pub fn tcp_connect_timeout(mut self, dur: Duration) -> Self {
        self.tcp_connect_timeout = finite(dur);
        self
    }

    /// Disable the per-address TCP connect bound (the
    /// [`DEFAULT_TCP_CONNECT_TIMEOUT`] default). The whole-establishment
    /// [`establishment_timeout`](Self::establishment_timeout) still applies.
    pub fn no_tcp_connect_timeout(mut self) -> Self {
        self.tcp_connect_timeout = None;
        self
    }

    /// Set the wall-clock bound on connection establishment: DNS resolution,
    /// TCP connect, the TLS handshake (for `tls` connections), and the HTTP/2
    /// preface, as one budget. [`tcp_connect_timeout`](Self::tcp_connect_timeout)
    /// is an additional per-address TCP bound *inside* this budget.
    ///
    /// Defaults to [`DEFAULT_ESTABLISHMENT_TIMEOUT`]. To disable, use
    /// [`no_establishment_timeout`](Self::no_establishment_timeout). Passing
    /// `Duration::ZERO` causes every establishment to fail immediately.
    ///
    /// # Where this bites
    ///
    /// For **TLS** connections this is the bound that protects shared callers
    /// from a server that accepts the TCP connection but stalls the TLS
    /// handshake — the handshake genuinely blocks on the server, so the bound
    /// fires.
    ///
    /// For **plaintext** h2c, hyper's HTTP/2 handshake resolves locally (it
    /// sends the client preface without waiting for the server's `SETTINGS`), so
    /// a stalled cleartext server stalls the first *request*, not the handshake.
    /// On plaintext this bound therefore only catches a slow local h2 setup;
    /// bound a stalled cleartext server with a per-request
    /// [`CallOptions::with_timeout`](super::CallOptions::with_timeout) instead.
    ///
    /// Exceeding this bound surfaces as a [`ConnectError`] with
    /// [`ErrorCode::Unavailable`](crate::error::ErrorCode::Unavailable) (the
    /// connect is retryable); the message names the phase.
    pub fn establishment_timeout(mut self, dur: Duration) -> Self {
        self.establishment_timeout = finite(dur);
        self
    }

    /// Disable the wall-clock establishment bound (the
    /// [`DEFAULT_ESTABLISHMENT_TIMEOUT`] default). With both this and
    /// [`no_tcp_connect_timeout`](Self::no_tcp_connect_timeout), a hung server
    /// can stall `poll_ready` indefinitely — the pre-0.8.0 behaviour.
    pub fn no_establishment_timeout(mut self) -> Self {
        self.establishment_timeout = None;
        self
    }

    /// Bind the built-in connector's TCP socket to `addr` before connecting,
    /// so every connection (including reconnects) originates from that local
    /// address (IP only; the source port stays ephemeral). For multi-homed
    /// hosts where the peer derives something from the source address it
    /// observes, or where egress must leave a specific interface. Unset by
    /// default: the kernel picks the source address from the route to the
    /// peer.
    ///
    /// Applied via hyper's [`HttpConnector::set_local_address`][hyper-la].
    /// The resolved peer addresses are filtered to `addr`'s family, so a peer
    /// with no address of that family fails to connect (rather than
    /// connecting from a kernel-chosen source); there is currently no
    /// dual-stack variant. An address this host cannot bind fails every
    /// connect with the bind error. Like
    /// [`tcp_connect_timeout`](Self::tcp_connect_timeout), this is
    /// **ignored** by the custom-connector / Unix-socket terminals.
    /// [`HttpClientBuilder`](super::HttpClientBuilder) does not expose it; use
    /// `Http2Connection` when you need a pinned source address.
    ///
    /// [hyper-la]: hyper_util::client::legacy::connect::HttpConnector::set_local_address
    #[doc(alias = "bind")]
    #[doc(alias = "source_address")]
    pub fn local_address(mut self, addr: IpAddr) -> Self {
        self.local_address = Some(addr);
        self
    }

    /// Set the HTTP/2 keep-alive PING interval. Disabled by default.
    ///
    /// While the connection has at least one open stream (or always, if
    /// [`keep_alive_while_idle`](Self::keep_alive_while_idle) is set), a PING
    /// is sent every `interval`; if no acknowledgement arrives within
    /// [`keep_alive_timeout`](Self::keep_alive_timeout) the connection is
    /// closed and the next `poll_ready` reconnects. This is the post-handshake
    /// liveness bound — together with
    /// [`establishment_timeout`](Self::establishment_timeout) it bounds the transport
    /// against a peer that goes silent at any point.
    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.h2_builder.keep_alive_interval(interval);
        self
    }

    /// Set how long to wait for a keep-alive PING acknowledgement before
    /// closing the connection. Only applies when
    /// [`keep_alive_interval`](Self::keep_alive_interval) is set. hyper
    /// defaults to 20 seconds.
    pub fn keep_alive_timeout(mut self, timeout: Duration) -> Self {
        self.h2_builder.keep_alive_timeout(timeout);
        self
    }

    /// Send keep-alive PINGs even when the connection has no open streams.
    /// hyper defaults to `false`.
    ///
    /// Set this to `true` for a fully bounded transport: with it `false`, the
    /// window between handshake completion and the first request (where no
    /// stream is open yet) is not covered by keep-alive, so a peer that goes
    /// half-open exactly there is unbounded by both
    /// [`establishment_timeout`](Self::establishment_timeout) (already ended) and
    /// keep-alive (not armed).
    pub fn keep_alive_while_idle(mut self, enabled: bool) -> Self {
        self.h2_builder.keep_alive_while_idle(enabled);
        self
    }

    /// Set the initial HTTP/2 stream-level flow-control window size.
    /// hyper defaults to 65,535 bytes.
    pub fn initial_stream_window_size(mut self, size: u32) -> Self {
        self.h2_builder.initial_stream_window_size(size);
        self
    }

    /// Set the initial HTTP/2 connection-level flow-control window size.
    /// hyper defaults to 65,535 bytes.
    pub fn initial_connection_window_size(mut self, size: u32) -> Self {
        self.h2_builder.initial_connection_window_size(size);
        self
    }

    /// Enable hyper's adaptive flow-control window (BDP-based auto-tuning).
    /// When enabled, the explicit window-size setters are ignored.
    pub fn adaptive_window(mut self, enabled: bool) -> Self {
        self.h2_builder.adaptive_window(enabled);
        self
    }

    /// Configure the underlying hyper HTTP/2 builder directly.
    ///
    /// This is the escape hatch for hyper knobs not proxied above. The closure
    /// receives a mutable reference to the same builder the proxied setters
    /// write to, so it composes with them in either order:
    ///
    /// ```rust,ignore
    /// Http2Connection::builder()
    ///     .keep_alive_interval(Duration::from_secs(30))
    ///     .h2_settings(|b| { b.max_concurrent_reset_streams(32); })
    ///     .lazy_plaintext(uri)
    /// ```
    ///
    /// A [`TokioTimer`](hyper_util::rt::TokioTimer) is already wired on the
    /// builder, so keep-alive works without the caller installing one. Hyper's
    /// setters return `&mut Self`, so end the closure body with `;` (as above)
    /// when calling exactly one.
    pub fn h2_settings(
        mut self,
        f: impl FnOnce(&mut hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>),
    ) -> Self {
        f(&mut self.h2_builder);
        self
    }

    /// Finish as a lazily-established **plaintext** connection.
    /// See [`Http2Connection::lazy_plaintext`].
    #[must_use]
    pub fn lazy_plaintext(self, uri: Uri) -> Http2Connection {
        Http2Connection {
            inner: Reconnect::new(self.make_plaintext(), uri, true),
        }
    }

    /// Finish by eagerly establishing a **plaintext** connection now.
    /// See [`Http2Connection::connect_plaintext`].
    ///
    /// # Errors
    ///
    /// Returns an error if the URI scheme is `https://` or the initial TCP
    /// connect or h2 handshake fails (including exceeding a configured bound).
    pub async fn connect_plaintext(self, uri: Uri) -> Result<Http2Connection, ConnectError> {
        require_http_scheme(&uri)?;
        let mut conn = Http2Connection {
            inner: Reconnect::new(self.make_plaintext(), uri, false),
        };
        drive_connect(&mut conn, "connect failed").await?;
        Ok(conn)
    }

    /// Finish as a lazily-established **TLS** connection.
    /// See [`Http2Connection::lazy_tls`].
    #[cfg(feature = "client-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client-tls")))]
    #[must_use]
    pub fn lazy_tls(self, uri: Uri, tls_config: Arc<rustls::ClientConfig>) -> Http2Connection {
        Http2Connection {
            inner: Reconnect::new(self.make_tls(tls_config), uri, true),
        }
    }

    /// Finish by eagerly establishing a **TLS** connection now.
    /// See [`Http2Connection::connect_tls`].
    ///
    /// # Errors
    ///
    /// Returns an error if the URI scheme is `http://`, the TCP/TLS handshake
    /// fails (including exceeding a configured bound), or the server doesn't
    /// negotiate h2 via ALPN.
    #[cfg(feature = "client-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client-tls")))]
    pub async fn connect_tls(
        self,
        uri: Uri,
        tls_config: Arc<rustls::ClientConfig>,
    ) -> Result<Http2Connection, ConnectError> {
        require_https_scheme(&uri)?;
        let mut conn = Http2Connection {
            inner: Reconnect::new(self.make_tls(tls_config), uri, false),
        };
        drive_connect(&mut conn, "TLS connect failed").await?;
        Ok(conn)
    }

    /// Finish as a lazily-established connection over a **caller-supplied
    /// connector**. See [`Http2Connection::lazy_with_connector`].
    ///
    /// `establishment_timeout` bounds the connector's dial *and* the HTTP/2 preface
    /// as one wall-clock budget — the same semantics as the built-in
    /// transports. `tcp_connect_timeout` and `local_address` configure the
    /// built-in connector and are **ignored** here; to bound the dial separately
    /// from the preface, wrap the connector in `tower::timeout::Timeout`, and
    /// bind inside the connector if you need a source address.
    #[must_use]
    pub fn lazy_with_connector<C>(self, connector: C, authority: Uri) -> Http2Connection
    where
        C: tower::Service<Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin + 'static,
        C::Error: Into<BoxError>,
        C::Future: Send + 'static,
    {
        Http2Connection {
            inner: Reconnect::new(self.make_custom(box_connector(connector)), authority, true),
        }
    }

    /// Finish by eagerly establishing a connection over a **caller-supplied
    /// connector**. See [`Http2Connection::connect_with_connector`] and
    /// [`lazy_with_connector`](Self::lazy_with_connector) for the timeout
    /// semantics.
    ///
    /// # Errors
    ///
    /// Returns an error if the connector or h2 handshake fails (including
    /// exceeding a configured `establishment_timeout`).
    pub async fn connect_with_connector<C>(
        self,
        connector: C,
        authority: Uri,
    ) -> Result<Http2Connection, ConnectError>
    where
        C: tower::Service<Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin + 'static,
        C::Error: Into<BoxError>,
        C::Future: Send + 'static,
    {
        let mut conn = Http2Connection {
            inner: Reconnect::new(self.make_custom(box_connector(connector)), authority, false),
        };
        drive_connect(&mut conn, "connect failed").await?;
        Ok(conn)
    }

    /// Finish as a lazily-established connection over a **Unix domain socket**.
    /// See [`Http2Connection::lazy_unix`] and
    /// [`lazy_with_connector`](Self::lazy_with_connector) for the timeout
    /// semantics.
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    #[must_use]
    pub fn lazy_unix(self, path: impl Into<std::path::PathBuf>, authority: Uri) -> Http2Connection {
        self.lazy_with_connector(unix_connector(path.into()), authority)
    }

    /// Finish by eagerly establishing a connection over a **Unix domain
    /// socket**. See [`Http2Connection::connect_unix`].
    ///
    /// # Errors
    ///
    /// Returns an error if the socket path doesn't exist or the h2 handshake
    /// fails (including exceeding a configured `establishment_timeout`).
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    pub async fn connect_unix(
        self,
        path: impl Into<std::path::PathBuf>,
        authority: Uri,
    ) -> Result<Http2Connection, ConnectError> {
        self.connect_with_connector(unix_connector(path.into()), authority)
            .await
    }

    /// Built-in TCP connector with `nodelay` and the configured
    /// `tcp_connect_timeout` / `local_address` applied. Mirrors
    /// `HttpClientBuilder::http_connector` (which has no `local_address`).
    fn http_connector(&self) -> hyper_util::client::legacy::connect::HttpConnector {
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_connect_timeout(self.tcp_connect_timeout);
        // The family filtering documented on `local_address` is hyper's
        // happy-eyeballs path; with it disabled hyper skips a mismatched-family
        // bind silently. `HttpConnector::new()` enables it — keep it that way.
        connector.set_local_address(self.local_address);
        connector
    }

    fn make_plaintext(self) -> MakeSendRequest {
        MakeSendRequest {
            connector: self.http_connector(),
            builder: self.h2_builder,
            #[cfg(feature = "client-tls")]
            tls: None,
            custom: None,
            establishment_timeout: self.establishment_timeout,
        }
    }

    #[cfg(feature = "client-tls")]
    fn make_tls(self, tls_config: Arc<rustls::ClientConfig>) -> MakeSendRequest {
        let mut connector = self.http_connector();
        connector.enforce_http(false);
        MakeSendRequest {
            connector,
            builder: self.h2_builder,
            tls: Some(prepare_tls_for_h2(&tls_config)),
            custom: None,
            establishment_timeout: self.establishment_timeout,
        }
    }

    fn make_custom(self, conn: BoxedConnector) -> MakeSendRequest {
        MakeSendRequest {
            // Unused when `custom` is Some — `call()` branches to the custom
            // connector before touching it. `tcp_connect_timeout` is therefore
            // dropped here (it's the per-address TCP bound on this connector).
            connector: self.http_connector(),
            builder: self.h2_builder,
            #[cfg(feature = "client-tls")]
            tls: None,
            custom: Some(conn),
            establishment_timeout: self.establishment_timeout,
        }
    }
}

/// Drive a connection's `poll_ready` to completion, forcing the eager connect.
async fn drive_connect(conn: &mut Http2Connection, ctx: &str) -> Result<(), ConnectError> {
    std::future::poll_fn(|cx| conn.inner.poll_ready(cx))
        .await
        .map_err(|e| ConnectError::unavailable_from_transport(ctx, e))
}

impl tower::Service<Request<ClientBody>> for Http2Connection {
    type Response = Response<hyper::body::Incoming>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ClientBody>) -> Self::Future {
        self.inner.call(req)
    }
}

// Http2Connection is a tower `Service` taking `&mut self`, and its Reconnect
// state machine is !Clone by design (each instance tracks one connection),
// while generated clients need `T: ClientTransport`, which is `Clone` and
// sends through `&self`. `SharedHttp2Connection` bridges the two without a
// worker task: hyper's h2 `SendRequest` is already a cloneable handle that
// passes each request to the connection task over a channel, so every request
// clones the current handle and sends on it directly. A request that finds no
// live handle (never connected, or the connection dropped), or has its request
// handed back unsent, takes the reconnect state machine's lock, has it driven
// to a new connection on a task of its own, and publishes the new handle for
// everyone else. A semaphore bounds how many requests may wait for that at
// once; it is the `bound` of `Http2Connection::shared` and the only queue the
// handle keeps.

type H2SendRequest = hyper::client::conn::http2::SendRequest<ClientBody>;

/// State shared by every clone of a [`SharedHttp2Connection`].
struct SharedState {
    /// The endpoint, for `Debug`.
    uri: Uri,
    /// The live connection's request handle; cloned per request. `None`
    /// before the first connect and while a replacement is being
    /// established. A handle whose connection has gone away reports
    /// `is_closed()` and sends callers to `conn` to replace it.
    sender: std::sync::RwLock<Option<H2SendRequest>>,
    /// The reconnect state machine, locked only to (re)establish the
    /// connection. `Arc` so that an attempt can own the guard in its own
    /// task and finish even if the request that started it goes away.
    conn: Arc<tokio::sync::Mutex<Http2Connection>>,
    /// Number of completed (re)connect attempts, successful or not. A caller
    /// that queued for `conn` behind someone else's attempt shares its
    /// outcome instead of dialling again, so a burst of requests during an
    /// outage costs one dial, not one per request.
    attempts: std::sync::atomic::AtomicU64,
    /// The failure of the most recent attempt, for those callers to share.
    /// Cleared by the next successful connect.
    last_failure: std::sync::Mutex<Option<ConnectError>>,
}

impl SharedState {
    /// The current request handle if its connection is still open.
    fn live_sender(&self) -> Option<H2SendRequest> {
        self.sender
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|s| !s.is_closed())
            .cloned()
    }

    /// Whether there is a request handle onto an open connection.
    fn is_connected(&self) -> bool {
        self.sender
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|s| !s.is_closed())
    }

    fn set_sender(&self, sender: Option<H2SendRequest>) {
        *self
            .sender
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = sender;
    }

    fn last_failure(&self) -> std::sync::MutexGuard<'_, Option<ConnectError>> {
        self.last_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Get a request handle onto a live connection, establishing one if
    /// needed, and publish it.
    ///
    /// The attempt itself runs in its own task holding the state machine's
    /// lock, so it completes (and its result is published) even when the
    /// request that started it is dropped at its deadline; requests queued
    /// behind it then find either the new handle or the recorded failure
    /// rather than a half-finished handshake to resume.
    async fn reconnect(shared: &Arc<Self>) -> Result<H2SendRequest, ConnectError> {
        use std::sync::atomic::Ordering;

        let seen = shared.attempts.load(Ordering::Acquire);
        let mut conn = Arc::clone(&shared.conn).lock_owned().await;
        // Another caller may have reconnected while this one waited for the
        // lock...
        if let Some(sender) = shared.live_sender() {
            return Ok(sender);
        }
        // ...or tried to and failed, in which case this caller takes that
        // answer rather than dialling again straight away.
        if shared.attempts.load(Ordering::Acquire) != seen
            && let Some(failure) = shared.last_failure().clone()
        {
            return Err(failure);
        }

        shared.set_sender(None);
        let owner = Arc::clone(shared);
        let attempt = tokio::spawn(async move {
            let ready = std::future::poll_fn(|cx| conn.inner.poll_ready(cx)).await;
            // `Reconnect` reports a failed (re)connect as Ready plus a deferred
            // error, so that a balancer can route around it; surface it here.
            let result = match (ready, conn.inner.deferred_error.take()) {
                (Err(e), _) | (Ok(()), Some(e)) => Err(e),
                (Ok(()), None) => match &conn.inner.state {
                    ReconnectState::Connected(svc) => Ok(svc.inner.clone()),
                    ReconnectState::Idle | ReconnectState::Connecting(_) => {
                        Err("Http2Connection ready but not connected".into())
                    }
                },
            };
            owner.attempts.fetch_add(1, Ordering::Release);
            let mut last_failure = owner.last_failure();
            let result = match result {
                Ok(sender) => {
                    tracing::debug!(uri = %owner.uri, "shared h2 connection established");
                    *last_failure = None;
                    owner.set_sender(Some(sender.clone()));
                    Ok(sender)
                }
                Err(e) => {
                    let failure = h2_transport_error(e, "h2 connect failed");
                    tracing::debug!(uri = %owner.uri, error = %failure, "shared h2 connect failed");
                    *last_failure = Some(failure.clone());
                    Err(failure)
                }
            };
            // Published; only now let the next waiter in.
            drop(conn);
            result
        });
        match attempt.await {
            Ok(result) => result,
            // The runtime is shutting down; nothing was established.
            Err(e) if e.is_cancelled() => Err(ConnectError::unavailable(
                "h2 connect failed: connect task cancelled",
            )),
            Err(e) => Err(ConnectError::internal(format!(
                "h2 connect task failed: {e}"
            ))),
        }
    }

    /// Send `req` on the shared connection, connecting first if needed, and
    /// await the response head. `queued` is the caller's queue slot, given
    /// back as soon as the request has been handed to a connection.
    ///
    /// A handle can pass the `is_closed()` check and still lose the race with
    /// a connection that is going away. hyper hands the request back unsent in
    /// that case, so it is resent on a fresh handle (a bounded number of
    /// times) instead of failing a request that never reached the wire.
    async fn send(
        shared: &Arc<Self>,
        mut req: Request<ClientBody>,
        queued: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Response<hyper::body::Incoming>, BoxError> {
        /// Resends of a request hyper handed back unsent. The window is the
        /// gap between a connection starting to close and its handle
        /// reporting `is_closed()`, so one is nearly always enough.
        const MAX_RESENDS: usize = 2;
        use std::sync::atomic::Ordering;

        let mut sender = match shared.live_sender() {
            Some(sender) => sender,
            None => Self::reconnect(shared).await?,
        };
        let mut queued = Some(queued);
        let mut resends = 0;
        loop {
            let generation = shared.attempts.load(Ordering::Acquire);
            let response = sender.try_send_request(req);
            // hyper took the request (or refused it) synchronously: from here
            // it occupies an h2 stream, or the transport's queue for one, and
            // no longer a slot in the queue for the connection.
            drop(queued.take());
            match response.await {
                Ok(response) => return Ok(response),
                // hyper hands a request back only if the connection went away
                // before taking it off its queue, so its body is untouched and
                // it can be sent again as new.
                Err(mut unsent) => match unsent.take_message() {
                    Some(returned) if resends < MAX_RESENDS => {
                        resends += 1;
                        req = returned;
                        tracing::debug!(
                            uri = %shared.uri,
                            attempt = resends,
                            "shared h2 connection closed before taking the request; resending"
                        );
                        // Retire the handle that failed, unless a reconnect has
                        // already replaced it meanwhile.
                        if shared.attempts.load(Ordering::Acquire) == generation {
                            shared.set_sender(None);
                        }
                        sender = Self::reconnect(shared).await?;
                    }
                    Some(_) | None => return Err(unsent.into_error().into()),
                },
            }
        }
    }
}

/// Classify a failure from the h2 transport for the caller: a
/// [`ConnectError`] anywhere in the chain (scheme and ALPN checks, the
/// establishment timeout, a shared connect failure) is returned as is, and
/// anything else (socket, TLS, h2 protocol errors) becomes `unavailable` with
/// `context` and the original error as its source.
fn h2_transport_error(err: BoxError, context: &str) -> ConnectError {
    super::find_connect_error_in_chain(&*err)
        .unwrap_or_else(|| ConnectError::unavailable_from_transport(context, err))
}

/// A `Clone + ClientTransport` handle to a shared [`Http2Connection`].
///
/// Created via [`Http2Connection::shared`]. Clones are cheap and all use the
/// one underlying connection and its reconnect state machine; requests from
/// different clones are multiplexed as concurrent HTTP/2 streams.
///
/// As a [`tower::Service`] it keeps [`Http2Connection`]'s readiness contract:
/// [`poll_ready`](tower::Service::poll_ready) is pending while the connection
/// is being (re-)established and ready once it is up, and a failed attempt is
/// reported as ready with the connect error returned by the following
/// [`call`](tower::Service::call), after which the next `poll_ready` tries
/// again. A balancer therefore keeps the endpoint and steers by pending load
/// while it is down, rather than discarding it for good.
pub struct SharedHttp2Connection {
    shared: Arc<SharedState>,
    /// Slots for requests waiting to be handed to the connection; `bound` of
    /// them. See [`Http2Connection::shared`].
    queue: tokio_util::sync::PollSemaphore,
    /// Taken in `poll_ready`, consumed by the following `call`.
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    /// The connection attempt `poll_ready` is waiting on, having found no
    /// live connection.
    connecting: Option<sync_wrapper::SyncFuture<BoxFuture<'static, Result<(), ConnectError>>>>,
    /// The failure of that attempt, returned by the next `call` (see the
    /// readiness contract on the type).
    deferred_error: Option<ConnectError>,
}

/// A clone shares the connection but not this handle's readiness: it must
/// get its own `poll_ready` before `call`, as `tower::ServiceExt::oneshot`
/// and the [`ClientTransport`] impl do.
impl Clone for SharedHttp2Connection {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            queue: self.queue.clone(),
            permit: None,
            connecting: None,
            deferred_error: None,
        }
    }
}

impl std::fmt::Debug for SharedHttp2Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedHttp2Connection")
            .field("uri", &self.shared.uri)
            .field("connected", &self.shared.is_connected())
            .finish_non_exhaustive()
    }
}

impl Http2Connection {
    /// Turn this connection into a `Clone + ClientTransport` handle that any
    /// number of clients and tasks can share.
    ///
    /// Requests are multiplexed onto the one connection as HTTP/2 streams, so
    /// their concurrency is limited only by the peer's
    /// `max_concurrent_streams`, past which the transport queues new streams
    /// itself (in hyper's own, unbounded, queue). `bound` limits something
    /// narrower: how many requests may be waiting, at once, to be handed to the
    /// connection. A request holds one of the `bound` slots from `poll_ready`
    /// (or the start of [`send`](ClientTransport::send)) until hyper has taken
    /// it, which is immediate while the connection is up and lasts for the
    /// (re-)establishment while it is not; further callers wait for a free
    /// slot, in arrival order. A tower handle that has been polled ready and
    /// not yet called holds its slot too. For a single-connection gRPC client,
    /// 1024 is a reasonable default. Values above
    /// `tokio::sync::Semaphore::MAX_PERMITS` are clamped to it.
    ///
    /// Connecting happens on a spawned Tokio task the first time a request
    /// (or `poll_ready`) finds no live connection, so the handle must be used
    /// from within a Tokio runtime, as with every transport here.
    ///
    /// # Panics
    ///
    /// If `bound` is 0.
    #[must_use]
    pub fn shared(self, bound: usize) -> SharedHttp2Connection {
        assert!(bound > 0, "SharedHttp2Connection bound must be positive");
        let bound = bound.min(tokio::sync::Semaphore::MAX_PERMITS);
        let sender = match &self.inner.state {
            ReconnectState::Connected(svc) => Some(svc.inner.clone()),
            ReconnectState::Idle | ReconnectState::Connecting(_) => None,
        };
        SharedHttp2Connection {
            shared: Arc::new(SharedState {
                uri: self.inner.uri.clone(),
                sender: std::sync::RwLock::new(sender),
                conn: Arc::new(tokio::sync::Mutex::new(self)),
                attempts: std::sync::atomic::AtomicU64::new(0),
                last_failure: std::sync::Mutex::new(None),
            }),
            queue: tokio_util::sync::PollSemaphore::new(Arc::new(tokio::sync::Semaphore::new(
                bound,
            ))),
            permit: None,
            connecting: None,
            deferred_error: None,
        }
    }
}

impl tower::Service<Request<ClientBody>> for SharedHttp2Connection {
    type Response = Response<hyper::body::Incoming>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Response<hyper::body::Incoming>, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.permit.is_none() {
            match futures::ready!(self.queue.poll_acquire(cx)) {
                Some(permit) => self.permit = Some(permit),
                // The semaphore is never closed.
                None => {
                    return Poll::Ready(Err(ConnectError::internal(
                        "SharedHttp2Connection queue closed",
                    )
                    .into()));
                }
            }
        }
        loop {
            if let Some(connecting) = self.connecting.as_mut() {
                let result = futures::ready!(Pin::new(connecting).poll(cx));
                self.connecting = None;
                if let Err(e) = result {
                    // Ready, with the failure reported by `call`: an `Err` here
                    // would tell the caller to discard the service for good.
                    self.deferred_error = Some(e);
                }
                return Poll::Ready(Ok(()));
            }
            if self.deferred_error.is_some() || self.shared.is_connected() {
                return Poll::Ready(Ok(()));
            }
            let shared = Arc::clone(&self.shared);
            self.connecting = Some(sync_wrapper::SyncFuture::new(Box::pin(async move {
                SharedState::reconnect(&shared).await.map(drop)
            })));
        }
    }

    fn call(&mut self, req: Request<ClientBody>) -> Self::Future {
        let Some(permit) = self.permit.take() else {
            return Box::pin(async {
                Err(ConnectError::internal(
                    "SharedHttp2Connection::call before poll_ready returned Ready",
                )
                .into())
            });
        };
        if let Some(e) = self.deferred_error.take() {
            drop(permit);
            return Box::pin(async move { Err(e.into()) });
        }
        let shared = Arc::clone(&self.shared);
        Box::pin(async move { SharedState::send(&shared, req, permit).await })
    }
}

impl ClientTransport for SharedHttp2Connection {
    type ResponseBody = hyper::body::Incoming;
    type Error = ConnectError;

    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
        let shared = Arc::clone(&self.shared);
        let queue = self.queue.clone_inner();
        Box::pin(async move {
            // The semaphore is never closed, so this only waits.
            let permit = queue
                .acquire_owned()
                .await
                .map_err(|_| ConnectError::internal("SharedHttp2Connection queue closed"))?;
            SharedState::send(&shared, request, permit)
                .await
                .map_err(|e| h2_transport_error(e, "h2 send failed"))
        })
    }
}

// ============================================================================
// SendRequest — thin tower wrapper over hyper's raw h2 client half
// ============================================================================

/// hyper's `http2::SendRequest<B>` as a `tower::Service`.
///
/// `poll_ready` returns `Err` if the connection is closed (triggering
/// `Reconnect` to re-establish) and `Ready(Ok)` otherwise. hyper doesn't
/// expose per-stream backpressure here (that happens inside `send_request`'s
/// future), but this is still more honest than the legacy pool's always-Ready.
struct SendRequest {
    inner: hyper::client::conn::http2::SendRequest<ClientBody>,
}

impl tower::Service<Request<ClientBody>> for SendRequest {
    type Response = Response<hyper::body::Incoming>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<ClientBody>) -> Self::Future {
        let fut = self.inner.send_request(req);
        Box::pin(async move { fut.await.map_err(Into::into) })
    }
}

// ============================================================================
// MakeSendRequest — `tower::MakeService` that connects + handshakes
// ============================================================================

/// Given a `Uri`, open a TCP connection (+ optional TLS) and perform the
/// HTTP/2 handshake, returning a ready `SendRequest`. Used by [`Reconnect`]
/// to (re)establish connections.
struct MakeSendRequest {
    connector: hyper_util::client::legacy::connect::HttpConnector,
    builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
    /// TLS config for https:// connections. When `Some`, the URI scheme
    /// must be https:// and a TLS handshake happens after TCP connect.
    /// When `None`, plaintext h2c — URI scheme must be http://.
    #[cfg(feature = "client-tls")]
    tls: Option<Arc<rustls::ClientConfig>>,
    /// Caller-supplied connector. When `Some`, `call()` uses this to dial
    /// instead of the built-in `HttpConnector`; the URI is used only for the
    /// h2 `:authority` pseudo-header. See [`Http2Connection::lazy_with_connector`].
    custom: Option<BoxedConnector>,
    /// Wall-clock bound on connection establishment in `call()`: DNS, TCP
    /// connect, TLS handshake (if any), and the HTTP/2 preface. `None` means
    /// unbounded. `connector`'s `set_connect_timeout` is an additional
    /// per-address TCP bound inside this budget.
    establishment_timeout: Option<Duration>,
}

impl tower::Service<Uri> for MakeSendRequest {
    type Response = SendRequest;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if let Some(c) = &mut self.custom {
            return c.poll_ready(cx);
        }
        <_ as tower::Service<Uri>>::poll_ready(&mut self.connector, cx).map_err(Into::into)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        if let Some(c) = &mut self.custom {
            let io_fut = c.call(uri);
            let builder = self.builder.clone();
            let establishment_timeout = self.establishment_timeout;
            return Box::pin(async move {
                // Dial + HTTP/2 preface as one wall-clock budget — same
                // semantics as the built-in branch's `establish` block below.
                let establish = async move {
                    let io = io_fut.await?;
                    builder.handshake(io).await.map_err(BoxError::from)
                };
                let (send_request, conn) =
                    run_establishment(establish, establishment_timeout).await?;
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::debug!("h2 connection task exited with error: {e}");
                    }
                });
                Ok(SendRequest {
                    inner: send_request,
                })
            });
        }

        // Scheme check based on TLS configuration. Catches mismatched
        // schemes for lazy_* constructors (which defer the check to here
        // via Reconnect's deferred_error mechanism).
        #[cfg(feature = "client-tls")]
        let scheme_check = if self.tls.is_some() {
            require_https_scheme(&uri)
        } else {
            require_http_scheme(&uri)
        };
        #[cfg(not(feature = "client-tls"))]
        let scheme_check = require_http_scheme(&uri);

        if let Err(e) = scheme_check {
            return Box::pin(async move { Err(e.into()) });
        }

        #[cfg(feature = "client-tls")]
        let tls = self.tls.clone();
        #[cfg(feature = "client-tls")]
        let server_name = match self.tls.is_some() {
            true => Some(match server_name_from_uri(&uri) {
                Ok(sn) => sn,
                Err(e) => return Box::pin(async move { Err(e.into()) }),
            }),
            false => None,
        };

        let connect_fut = <_ as tower::Service<Uri>>::call(&mut self.connector, uri);
        let builder = self.builder.clone();
        let establishment_timeout = self.establishment_timeout;

        Box::pin(async move {
            // DNS + TCP connect + TLS handshake (if configured) + HTTP/2
            // preface — bounded together by `establishment_timeout` so a server
            // that accepts the TCP connection but stalls the handshake (or a
            // hung resolver) can't hang `poll_ready` for every caller sharing
            // this connection. The per-address TCP connect is additionally
            // bounded by `tcp_connect_timeout` on the connector.
            let establish = async move {
                let io = connect_fut.await.map_err(Into::<BoxError>::into)?;

                // TLS handshake if configured. This is the same pattern tonic
                // uses for its Channel (transport/channel/service/connector.rs).
                // The two concrete IO types are unified via BoxedIo for handshake().
                #[cfg(feature = "client-tls")]
                let io: BoxedIo = if let (Some(tls), Some(server_name)) = (tls, server_name) {
                    // Unwrap the TokioIo to get the raw TcpStream for TLS.
                    let tcp = io.into_inner();
                    let connector = tokio_rustls::TlsConnector::from(tls);
                    let tls_stream = connector.connect(server_name, tcp).await.map_err(|e| {
                        BoxError::from(ConnectError::unavailable_from_transport(
                            "TLS handshake failed",
                            e,
                        ))
                    })?;

                    // Verify ALPN negotiated h2. A server that doesn't speak h2
                    // would otherwise fail cryptically in the h2 handshake.
                    // Same check tonic does (transport/channel/service/tls.rs:125).
                    let (_, session) = tls_stream.get_ref();
                    if session.alpn_protocol() != Some(b"h2") {
                        return Err(BoxError::from(ConnectError::unavailable(
                            "TLS handshake succeeded but server did not negotiate \
                             HTTP/2 via ALPN (is the server h2-capable?)",
                        )));
                    }

                    Box::pin(hyper_util::rt::TokioIo::new(tls_stream))
                } else {
                    Box::pin(io)
                };
                #[cfg(not(feature = "client-tls"))]
                let io: BoxedIo = Box::pin(io);

                builder.handshake(io).await.map_err(BoxError::from)
            };

            let (send_request, conn) = run_establishment(establish, establishment_timeout).await?;
            // The connection task drives the h2 state machine (reads frames,
            // processes flow control, etc). Detach it — it exits when the
            // connection closes or errors.
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    tracing::debug!("h2 connection task exited with error: {e}");
                }
            });
            Ok(SendRequest {
                inner: send_request,
            })
        })
    }
}

/// Run a connection-establishment future under an optional time bound.
///
/// `None` runs it unbounded; `Some(dur)` cancels it (dropping the in-flight
/// handshake) and returns an `unavailable` error if it doesn't finish within
/// `dur`. The future's own error type is coerced to [`BoxError`].
pub(super) async fn run_establishment<F, T, E>(
    fut: F,
    timeout: Option<Duration>,
) -> Result<T, BoxError>
where
    F: Future<Output = Result<T, E>>,
    E: Into<BoxError>,
{
    match timeout {
        Some(dur) => match tokio::time::timeout(dur, fut).await {
            Ok(res) => res.map_err(Into::into),
            Err(elapsed) => Err(ConnectError::unavailable(format!(
                "connection establishment did not complete within {dur:?}"
            ))
            .with_source(elapsed)
            .into()),
        },
        None => fut.await.map_err(Into::into),
    }
}

// ============================================================================
// Reconnect — state machine that re-establishes a dropped connection
// ============================================================================

/// Wraps a `MakeService` and a `Service` in a state machine that
/// re-establishes the inner service when it errors from `poll_ready`.
///
/// States:
/// - `Idle` — no connection; next `poll_ready` will start connecting
/// - `Connecting` — TCP+h2 handshake in flight
/// - `Connected` — ready to serve; delegates `poll_ready` to inner
///
/// On inner `poll_ready` error (connection dropped), transitions back to
/// `Idle`. Connection errors are buffered and returned from the *next* call
/// so that `tower::balance` can route around a failing endpoint.
struct Reconnect<M>
where
    M: tower::Service<Uri>,
{
    make: M,
    uri: Uri,
    state: ReconnectState<M::Future, M::Response>,
    /// Buffered connect error to surface on next call() instead of failing
    /// poll_ready, so tower::balance can route around us temporarily.
    deferred_error: Option<BoxError>,
    /// Whether we've ever successfully connected. Affects error handling on
    /// initial connect — if `lazy` is false and we've never connected, the
    /// first connect error is returned immediately from `poll_ready`.
    has_connected: bool,
    lazy: bool,
}

enum ReconnectState<F, S> {
    Idle,
    Connecting(Pin<Box<F>>),
    Connected(S),
}

impl<M> Reconnect<M>
where
    M: tower::Service<Uri>,
{
    fn new(make: M, uri: Uri, lazy: bool) -> Self {
        Self {
            make,
            uri,
            state: ReconnectState::Idle,
            deferred_error: None,
            has_connected: false,
            lazy,
        }
    }
}

impl<M, S> Reconnect<M>
where
    M: tower::Service<Uri, Response = S>,
    M::Error: Into<BoxError>,
    S: tower::Service<Request<ClientBody>>,
    S::Error: Into<BoxError>,
    S::Future: Send + 'static,
{
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        // If we have a buffered error from a prior connect attempt, surface
        // it immediately as Ready(Ok) so call() can return it. This matches
        // tonic's behavior — it lets tower::balance route around the failing
        // connection for one request while we retry.
        if self.deferred_error.is_some() {
            return Poll::Ready(Ok(()));
        }

        loop {
            match &mut self.state {
                ReconnectState::Idle => {
                    // Wait for the make service (connector) to be ready.
                    if let Err(e) = futures::ready!(self.make.poll_ready(cx)) {
                        return Poll::Ready(Err(e.into()));
                    }
                    let fut = self.make.call(self.uri.clone());
                    self.state = ReconnectState::Connecting(Box::pin(fut));
                }
                ReconnectState::Connecting(fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(svc)) => {
                        self.state = ReconnectState::Connected(svc);
                        self.has_connected = true;
                    }
                    Poll::Ready(Err(e)) => {
                        let e: BoxError = e.into();
                        self.state = ReconnectState::Idle;
                        if self.has_connected || self.lazy {
                            // Defer the error to call() so balance can route around us.
                            tracing::debug!("h2 reconnect failed (will retry): {e}");
                            self.deferred_error = Some(e);
                            return Poll::Ready(Ok(()));
                        } else {
                            // Eager connect, never succeeded: fail immediately.
                            return Poll::Ready(Err(e));
                        }
                    }
                },
                ReconnectState::Connected(svc) => match svc.poll_ready(cx) {
                    Poll::Ready(Ok(())) => return Poll::Ready(Ok(())),
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(_)) => {
                        // Connection dropped — transition back to Idle and loop
                        // to start reconnecting.
                        tracing::debug!("h2 connection lost; reconnecting");
                        self.state = ReconnectState::Idle;
                    }
                },
            }
        }
    }

    fn call(
        &mut self,
        req: Request<ClientBody>,
    ) -> BoxFuture<'static, Result<S::Response, BoxError>> {
        if let Some(e) = self.deferred_error.take() {
            return Box::pin(async move { Err(e) });
        }
        match &mut self.state {
            ReconnectState::Connected(svc) => {
                let fut = svc.call(req);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
            _ => {
                // Contract violation — poll_ready wasn't called or wasn't Ready.
                Box::pin(async {
                    Err("Http2Connection::call before poll_ready returned Ready"
                        .to_string()
                        .into())
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lazy_plaintext_starts_idle() {
        let conn = Http2Connection::lazy_plaintext("http://localhost:0".parse().unwrap());
        // Can't assert much without a server; just verify construction.
        let _ = conn;
    }

    #[tokio::test]
    async fn connect_plaintext_to_nonexistent_fails() {
        // Port 1 should not have a listener.
        let err = Http2Connection::connect_plaintext("http://127.0.0.1:1".parse().unwrap())
            .await
            .expect_err("expected connect to port 1 to fail");
        assert!(
            std::error::Error::source(&err).is_some(),
            "eager-connect failure must retain its cause as source(): {err:?}"
        );
    }

    #[tokio::test]
    async fn connect_plaintext_rejects_https() {
        let err = Http2Connection::connect_plaintext("https://localhost:8080".parse().unwrap())
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::InvalidArgument);
        assert!(err.message.as_deref().unwrap().contains("http://"));
    }

    #[test]
    fn require_http_scheme_cases() {
        assert!(require_http_scheme(&"http://foo".parse().unwrap()).is_ok());
        // Scheme-less URIs are accepted (path-only, resolved later)
        assert!(require_http_scheme(&"/path".parse().unwrap()).is_ok());
        assert!(require_http_scheme(&"https://foo".parse().unwrap()).is_err());
    }

    #[cfg(feature = "client-tls")]
    #[test]
    fn require_https_scheme_cases() {
        assert!(require_https_scheme(&"https://foo".parse().unwrap()).is_ok());
        assert!(require_https_scheme(&"http://foo".parse().unwrap()).is_err());
        // Scheme-less is rejected for TLS (we need a host for SNI anyway)
        assert!(require_https_scheme(&"/path".parse().unwrap()).is_err());
    }

    #[cfg(feature = "client-tls")]
    #[test]
    fn prepare_tls_for_h2_sets_alpn() {
        let cfg = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let prepared = prepare_tls_for_h2(&cfg);
        assert_eq!(prepared.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[cfg(feature = "client-tls")]
    #[test]
    fn prepare_tls_for_h2_shares_cert_resolver() {
        // The clone should share the Arc<dyn ResolvesClientCert> so cert
        // rotation via a shared resolver keeps working across the clone.
        let cfg = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let prepared = prepare_tls_for_h2(&cfg);
        // The resolver Arc pointers should be equal (same instance).
        assert!(Arc::ptr_eq(
            &cfg.client_auth_cert_resolver,
            &prepared.client_auth_cert_resolver
        ));
    }

    #[cfg(feature = "client-tls")]
    #[test]
    fn server_name_from_uri_extracts_host() {
        let name = server_name_from_uri(&"https://example.com:8080/path".parse().unwrap()).unwrap();
        assert_eq!(format!("{name:?}"), "DnsName(\"example.com\")");
    }

    #[cfg(feature = "client-tls")]
    #[test]
    fn server_name_from_uri_ipv4() {
        let name = server_name_from_uri(&"https://10.0.0.1:8443".parse().unwrap()).unwrap();
        assert!(matches!(name, rustls_pki_types::ServerName::IpAddress(_)));
    }

    #[cfg(feature = "client-tls")]
    #[test]
    fn server_name_from_uri_ipv6_strips_brackets() {
        let name = server_name_from_uri(&"https://[::1]:8443".parse().unwrap()).unwrap();
        assert!(matches!(name, rustls_pki_types::ServerName::IpAddress(_)));
    }

    #[cfg(feature = "client-tls")]
    #[tokio::test]
    async fn connect_tls_rejects_http_scheme() {
        let cfg = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let result =
            Http2Connection::connect_tls("http://localhost:8080".parse().unwrap(), cfg).await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("expected http:// to be rejected"),
        };
        assert_eq!(err.code, crate::error::ErrorCode::InvalidArgument);
    }

    #[cfg(feature = "client-tls")]
    #[tokio::test]
    async fn connect_tls_handshake_failure_preserves_source() {
        // A listener that accepts the TCP connection and immediately closes
        // it, before any TLS bytes are exchanged — the client's handshake
        // fails fast on the resulting EOF, without needing a timeout.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });

        let tls_config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let uri: Uri = format!("https://{addr}").parse().unwrap();
        let err = Http2Connection::connect_tls(uri, tls_config)
            .await
            .expect_err("handshake against a closed connection must fail");

        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
        assert!(
            err.message
                .as_deref()
                .unwrap()
                .contains("TLS handshake failed"),
            "expected a TLS-handshake-failure message, got: {err:?}"
        );
        assert!(
            std::error::Error::source(&err).is_some(),
            "TLS handshake failure must retain its cause as source(): {err:?}"
        );

        server.abort();
    }

    fn empty_request() -> Request<ClientBody> {
        Request::builder()
            .uri("http://test.invalid/")
            .body(crate::client::full_body(bytes::Bytes::new()))
            .unwrap()
    }

    #[tokio::test]
    async fn shared_send_after_deferred_connect_failure_preserves_source() {
        // A lazy connection defers its first connect failure (see
        // Reconnect::poll_ready) — that deferred-error path is exactly what
        // SharedHttp2Connection::send goes through for a shared connection
        // that never manages to connect.
        let conn = Http2Connection::lazy_plaintext("http://127.0.0.1:1".parse().unwrap());
        let shared = conn.shared(1);

        let err = ClientTransport::send(&shared, empty_request())
            .await
            .expect_err("send over a connection that can never connect must fail");
        assert_eq!(err.code, crate::error::ErrorCode::Unavailable, "{err:?}");
        assert!(
            err.message
                .as_deref()
                .unwrap()
                .contains("h2 connect failed"),
            "unexpected message: {err:?}"
        );
        assert!(
            std::error::Error::source(&err).is_some(),
            "connect failure must retain its cause as source(): {err:?}"
        );
    }

    /// A connect failure that the transport classified itself (here the
    /// plaintext/TLS scheme check, `invalid_argument`) reaches the caller
    /// with its own code rather than re-wrapped as `unavailable`.
    #[cfg(feature = "client-tls")]
    #[tokio::test]
    async fn shared_send_surfaces_classified_connect_errors_verbatim() {
        let tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let shared =
            Http2Connection::lazy_tls("http://127.0.0.1:1".parse().unwrap(), tls).shared(1);
        let err = ClientTransport::send(&shared, empty_request())
            .await
            .unwrap_err();
        assert_eq!(
            err.code,
            crate::error::ErrorCode::InvalidArgument,
            "{err:?}"
        );
    }

    /// `poll_ready` on a shared handle drives the connect itself and keeps
    /// `Http2Connection`'s contract for a failed one: ready, with the failure
    /// returned by the next `call`, and a fresh dial on the next `poll_ready`
    /// (an `Err` from `poll_ready` would have a balancer discard the endpoint
    /// for good).
    #[tokio::test]
    async fn shared_poll_ready_defers_connect_failure_to_call_and_redials() {
        use std::future::poll_fn;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tower::Service as _;

        let dials = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&dials);
        let refuse = tower::service_fn(move |_uri: Uri| {
            counter.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<hyper_util::rt::TokioIo<tokio::io::DuplexStream>, _>(std::io::Error::other(
                    "dial refused",
                ))
            }
        });
        let mut shared = Http2Connection::builder()
            .lazy_with_connector(refuse, "http://test.invalid".parse().unwrap())
            .shared(4);

        for expected_dials in [1, 2] {
            poll_fn(|cx| shared.poll_ready(cx))
                .await
                .expect("a failed connect still reports ready");
            assert_eq!(dials.load(Ordering::SeqCst), expected_dials);
            let err = shared.call(empty_request()).await.unwrap_err();
            let err = err.downcast_ref::<ConnectError>().expect("a ConnectError");
            assert_eq!(err.code, crate::error::ErrorCode::Unavailable, "{err:?}");
            assert!(
                err.message.as_deref().unwrap().contains("dial refused"),
                "{err:?}"
            );
            assert_eq!(
                dials.load(Ordering::SeqCst),
                expected_dials,
                "call does not dial"
            );
            assert_eq!(shared.queue.available_permits(), 4, "the slot is returned");
        }
    }

    /// `bound` counts requests waiting for the connection to be established:
    /// each `poll_ready` parked on the connect holds a slot, a clone never
    /// inherits one, a handle that gives up returns its slot, and a caller
    /// past the bound waits for a slot rather than for the connect.
    #[tokio::test]
    async fn shared_bound_limits_requests_queued_for_a_connection() {
        use tower::Service as _;

        let stall = tower::service_fn(|_uri: Uri| async {
            std::future::pending::<()>().await;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tokio::io::duplex(1).0))
        });
        let mut a = Http2Connection::builder()
            .no_establishment_timeout()
            .lazy_with_connector(stall, "http://test.invalid".parse().unwrap())
            .shared(2);
        let mut b = a.clone();
        let mut c = a.clone();
        let is_pending = |handle: &mut SharedHttp2Connection| {
            let polled = handle.poll_ready(&mut Context::from_waker(std::task::Waker::noop()));
            polled.is_pending()
        };

        assert!(is_pending(&mut a), "a drives the connect");
        assert!(is_pending(&mut a), "and asking again takes no second slot");
        assert_eq!(a.queue.available_permits(), 1);
        assert!(is_pending(&mut b), "b queues behind the same connect");
        assert_eq!(a.queue.available_permits(), 0);
        assert!(is_pending(&mut c), "c waits for a slot");
        assert!(c.permit.is_none() && c.connecting.is_none());

        // b gives up; its slot goes straight to c, which joins the connect.
        drop(b);
        assert!(is_pending(&mut c));
        assert!(c.permit.is_some() && c.connecting.is_some());
        assert_eq!(a.queue.available_permits(), 0);
    }

    /// `bound` does not cap concurrent streams: with a bound of 1, two
    /// requests are open on the server at the same time (the server answers
    /// neither until it has both, so a cap of one in-flight request would
    /// deadlock here and trip the timeout).
    #[cfg(feature = "server")]
    #[tokio::test]
    async fn shared_bound_does_not_cap_concurrent_streams() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let both_arrived = Arc::new(tokio::sync::Barrier::new(2));
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let both_arrived = Arc::clone(&both_arrived);
                tokio::spawn(async move {
                    let service =
                        hyper::service::service_fn(move |_req: Request<hyper::body::Incoming>| {
                            let both_arrived = Arc::clone(&both_arrived);
                            async move {
                                both_arrived.wait().await;
                                Ok::<_, std::convert::Infallible>(Response::new(
                                    http_body_util::Full::new(bytes::Bytes::from_static(b"ok")),
                                ))
                            }
                        });
                    let _ = hyper::server::conn::http2::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await;
                });
            }
        });

        let uri: Uri = format!("http://{addr}").parse().unwrap();
        let shared = Http2Connection::lazy_plaintext(uri.clone()).shared(1);
        let request = || {
            Request::builder()
                .uri(uri.clone())
                .body(crate::client::full_body(bytes::Bytes::new()))
                .unwrap()
        };
        let (first, second) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                ClientTransport::send(&shared, request()),
                ClientTransport::send(&shared, request())
            )
        })
        .await
        .expect("two concurrent streams over a bound of 1");
        assert_eq!(first.unwrap().status(), http::StatusCode::OK);
        assert_eq!(second.unwrap().status(), http::StatusCode::OK);

        // The same over the tower interface, one handle per request.
        use tower::ServiceExt as _;
        let (first, second) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                shared.clone().oneshot(request()),
                shared.clone().oneshot(request())
            )
        })
        .await
        .expect("two concurrent streams over a bound of 1, via tower");
        assert_eq!(first.unwrap().status(), http::StatusCode::OK);
        assert_eq!(second.unwrap().status(), http::StatusCode::OK);
        server.abort();
    }

    #[test]
    fn shared_debug_reports_endpoint_and_connection_state() {
        let shared =
            Http2Connection::lazy_plaintext("http://127.0.0.1:1".parse().unwrap()).shared(3);
        let debug = format!("{shared:?}");
        assert!(debug.contains("uri: http://127.0.0.1:1/"), "{debug}");
        assert!(debug.contains("connected: false"), "{debug}");
    }

    /// A burst of requests against a shared connection that cannot connect
    /// dials once: the requests queued behind the attempt share its failure
    /// instead of each dialling in turn. A request made after the burst
    /// dials again. (Paused time: the connector's delay elapses only once
    /// every request in the burst is parked, however slow the test host.)
    #[tokio::test(start_paused = true)]
    async fn shared_burst_during_outage_dials_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dials = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&dials);
        let refuse = tower::service_fn(move |_uri: Uri| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Err::<hyper_util::rt::TokioIo<tokio::io::DuplexStream>, _>(std::io::Error::other(
                    "dial refused",
                ))
            }
        });
        let shared = Http2Connection::builder()
            .no_establishment_timeout()
            .lazy_with_connector(refuse, "http://test.invalid".parse().unwrap())
            .shared(64);

        let burst = futures::future::join_all(
            (0..16).map(|_| ClientTransport::send(&shared, empty_request())),
        )
        .await;
        assert_eq!(
            dials.load(Ordering::SeqCst),
            1,
            "one dial for the whole burst"
        );
        for result in &burst {
            let err = result.as_ref().unwrap_err();
            assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
            assert!(
                err.message.as_deref().unwrap().contains("dial refused"),
                "every request in the burst reports the shared failure: {err:?}"
            );
        }

        let _ = ClientTransport::send(&shared, empty_request()).await;
        assert_eq!(
            dials.load(Ordering::SeqCst),
            2,
            "a later request dials afresh"
        );
    }

    /// A request abandoned mid-handshake (its deadline passed) neither kills
    /// the attempt nor leaves it half-finished for the next request to
    /// resume: the attempt runs to its own conclusion, requests arriving
    /// meanwhile wait on it rather than dialling again, and a request made
    /// after it has failed dials afresh instead of inheriting its
    /// establishment timeout.
    #[tokio::test(start_paused = true)]
    async fn shared_connect_attempt_outlives_the_request_that_started_it() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dials = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&dials);
        let stall = tower::service_fn(move |_uri: Uri| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                std::future::pending::<()>().await;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tokio::io::duplex(1).0))
            }
        });
        const ESTABLISHMENT: Duration = Duration::from_secs(3);
        let shared = Http2Connection::builder()
            .establishment_timeout(ESTABLISHMENT)
            .lazy_with_connector(stall, "http://test.invalid".parse().unwrap())
            .shared(4);
        let send_within = |limit: Duration| {
            tokio::time::timeout(limit, ClientTransport::send(&shared, empty_request()))
        };

        // Give up on the first request well inside the establishment timeout.
        assert!(send_within(ESTABLISHMENT / 10).await.is_err());
        assert_eq!(dials.load(Ordering::SeqCst), 1);

        // A second request while that attempt is still running joins it.
        assert!(send_within(ESTABLISHMENT / 10).await.is_err());
        assert_eq!(
            dials.load(Ordering::SeqCst),
            1,
            "no second dial mid-attempt"
        );

        // Once the attempt has timed out on its own, the next request dials
        // afresh, and stalls afresh rather than failing at once with the
        // previous attempt's timeout.
        tokio::time::sleep(ESTABLISHMENT).await;
        assert!(
            send_within(ESTABLISHMENT / 10).await.is_err(),
            "a fresh attempt, still stalled"
        );
        assert_eq!(dials.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn shared_call_without_poll_ready_is_an_error_not_a_panic() {
        use tower::Service as _;
        let mut shared =
            Http2Connection::lazy_plaintext("http://127.0.0.1:1".parse().unwrap()).shared(4);
        let err = shared.call(empty_request()).await.unwrap_err();
        assert!(err.to_string().contains("before poll_ready"), "{err}");
    }

    #[test]
    #[should_panic(expected = "bound must be positive")]
    fn shared_rejects_a_zero_bound() {
        let _ = Http2Connection::lazy_plaintext("http://127.0.0.1:1".parse().unwrap()).shared(0);
    }

    #[test]
    fn lazy_with_connector_starts_idle() {
        let conn = Http2Connection::lazy_with_connector(
            tower::service_fn(|_uri: Uri| async {
                Err::<hyper_util::rt::TokioIo<tokio::net::TcpStream>, _>(std::io::Error::other(
                    "unreachable",
                ))
            }),
            "http://localhost".parse().unwrap(),
        );
        let _ = conn;
    }

    #[tokio::test]
    async fn connect_with_connector_propagates_error() {
        let err = Http2Connection::connect_with_connector(
            tower::service_fn(|_uri: Uri| async {
                Err::<hyper_util::rt::TokioIo<tokio::net::TcpStream>, _>(std::io::Error::other(
                    "dial refused",
                ))
            }),
            "http://localhost".parse().unwrap(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
        assert!(
            err.message.as_deref().unwrap().contains("dial refused"),
            "error should propagate connector message, got: {err:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn lazy_unix_starts_idle() {
        let conn = Http2Connection::lazy_unix(
            "/nonexistent/test.sock",
            "http://localhost".parse().unwrap(),
        );
        let _ = conn;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connect_unix_nonexistent_fails() {
        let path = "/nonexistent/buffa-test.sock";
        let err = Http2Connection::connect_unix(path, "http://localhost".parse().unwrap())
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
        assert!(
            err.message.as_deref().unwrap().contains(path),
            "error should include socket path, got: {err:?}"
        );
        assert!(
            std::error::Error::source(&err).is_some(),
            "unix-connect failure must retain its cause as source(): {err:?}"
        );
    }

    #[test]
    fn builder_defaults_are_finite() {
        let builder = Http2Connection::builder();
        assert_eq!(
            builder.tcp_connect_timeout,
            Some(DEFAULT_TCP_CONNECT_TIMEOUT)
        );
        assert_eq!(
            builder.establishment_timeout,
            Some(DEFAULT_ESTABLISHMENT_TIMEOUT)
        );
    }

    #[test]
    fn builder_setters_record_durations() {
        let builder = Http2Connection::builder()
            .tcp_connect_timeout(Duration::from_millis(10))
            .establishment_timeout(Duration::from_millis(20));
        assert_eq!(builder.tcp_connect_timeout, Some(Duration::from_millis(10)));
        assert_eq!(
            builder.establishment_timeout,
            Some(Duration::from_millis(20))
        );

        // Explicit no_* methods are the documented opt-out.
        let unbounded = Http2Connection::builder()
            .no_tcp_connect_timeout()
            .no_establishment_timeout();
        assert_eq!(unbounded.tcp_connect_timeout, None);
        assert_eq!(unbounded.establishment_timeout, None);

        // Duration::MAX is also normalized to None so no saturated timer is
        // armed (back-compat with the original opt-out spelling).
        let max = Http2Connection::builder()
            .tcp_connect_timeout(Duration::MAX)
            .establishment_timeout(Duration::MAX);
        assert_eq!(max.tcp_connect_timeout, None);
        assert_eq!(max.establishment_timeout, None);
    }

    /// `local_address` is what the peer observes as the connection's source.
    /// Linux routes all of 127/8 to `lo`, so 127.0.0.2 is bindable without
    /// setup and distinguishable from the default 127.0.0.1 source; macOS/BSD
    /// configure only 127.0.0.1 on loopback, hence the cfg.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn builder_local_address_is_the_observed_peer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri: Uri = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let local: IpAddr = "127.0.0.2".parse().unwrap();
        // TCP completes into the listen backlog and the plaintext h2c
        // handshake resolves locally, so this finishes before `accept()` is
        // polled — and a bind failure surfaces here as an error, not a hang.
        let _conn = Http2Connection::builder()
            .local_address(local)
            .connect_plaintext(uri)
            .await
            .unwrap();
        let (_stream, peer) = listener.accept().await.unwrap();
        assert_eq!(peer.ip(), local);
    }

    /// A `local_address` whose family the peer has no address in fails the
    /// connect rather than falling back to a kernel-chosen source. The peer
    /// is a live v4 listener, so a fallback would *succeed* — `expect_err`
    /// is what pins the no-fallback behaviour (and the happy-eyeballs
    /// dependency noted in `http_connector`).
    #[tokio::test]
    async fn builder_local_address_family_mismatch_fails() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri: Uri = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let err = Http2Connection::builder()
            .local_address("::1".parse().unwrap())
            .connect_plaintext(uri)
            .await
            .expect_err("v6 local address to a v4-only peer must not connect");
        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
    }

    #[tokio::test]
    async fn builder_tcp_connect_timeout_bounds_tcp_connect() {
        use std::time::Instant;

        // RFC 5737 TEST-NET-1: reserved for documentation. Most hosts drop SYNs
        // to it (so an unbounded connect stalls on kernel retransmits, ~130s),
        // but RFC 5737 doesn't mandate that — some CI hosts actively reject
        // (ENETUNREACH / ICMP) and a transparent proxy may even accept. The
        // assertion that matters is the upper bound: a 100ms tcp_connect_timeout
        // must abort well before the kernel retry floor.
        let start = Instant::now();
        let result = Http2Connection::builder()
            .tcp_connect_timeout(Duration::from_millis(100))
            .connect_plaintext("http://192.0.2.1:9".parse().unwrap())
            .await;
        let elapsed = start.elapsed();

        let err = match result {
            Err(e) => e,
            Ok(_) => {
                // Transparent proxy accepted the connect; the h2c preface
                // resolves locally so this succeeds. Nothing to assert.
                eprintln!("skipping: TEST-NET-1 connect succeeded (proxy?) in {elapsed:?}");
                return;
            }
        };
        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
        // Widened skip window: an ICMP reject on a slow path can land anywhere
        // under the budget without exercising the timeout. Only assert the
        // lower bound when we clearly waited it out.
        if elapsed < Duration::from_millis(90) {
            eprintln!(
                "skipping lower-bound check: host rejected TEST-NET-1 \
                 in {elapsed:?} ({err:?})"
            );
            return;
        }
        assert!(
            elapsed < Duration::from_secs(2),
            "tcp_connect_timeout(100ms) should abort within ~2s, took {elapsed:?}: {err:?}"
        );
    }

    // hyper's plaintext h2c handshake resolves locally (it sends the client
    // preface without waiting for the server's SETTINGS), so a stalled cleartext
    // server stalls the first *request*, not the handshake. The TLS handshake,
    // by contrast, genuinely blocks on the server, so that is where
    // establishment_timeout has observable effect — exercised here.
    #[cfg(feature = "client-tls")]
    #[tokio::test]
    async fn establishment_timeout_fires_when_tls_server_stalls_after_accept() {
        use std::time::Instant;

        // A listener that accepts the TCP connection but never performs the TLS
        // handshake. The TCP connect succeeds, so only establishment_timeout can
        // release the stalled connect.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });

        let tls_config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let uri: Uri = format!("https://{addr}").parse().unwrap();
        let start = Instant::now();
        let err = Http2Connection::builder()
            .establishment_timeout(Duration::from_millis(150))
            .connect_tls(uri, tls_config)
            .await
            .unwrap_err();
        let elapsed = start.elapsed();

        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
        assert!(
            err.message
                .as_deref()
                .unwrap()
                .contains("establishment did not complete"),
            "expected a handshake-timeout message, got: {err:?}"
        );
        assert!(
            std::error::Error::source(&err).is_some(),
            "establishment-timeout failure must retain its cause as source(): {err:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "establishment_timeout(150ms) should fire within ~2s, took {elapsed:?}"
        );

        server.abort();
    }

    #[cfg(feature = "client-tls")]
    #[tokio::test]
    async fn establishment_timeout_applies_with_custom_h2_settings() {
        use std::time::Instant;

        // Same stalled-TLS scenario, but constructed via the proxied keep-alive
        // setters — the path callers use to set h2 keep-alive. Regression-guards
        // the setter-exposure gap that left `with_builder_tls` unbounded.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });

        let tls_config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let uri: Uri = format!("https://{addr}").parse().unwrap();
        let start = Instant::now();
        let err = Http2Connection::builder()
            .keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .h2_settings(|b| {
                b.max_frame_size(1 << 14);
            })
            .establishment_timeout(Duration::from_millis(150))
            .connect_tls(uri, tls_config)
            .await
            .unwrap_err();
        let elapsed = start.elapsed();

        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
        assert!(
            err.message
                .as_deref()
                .unwrap()
                .contains("establishment did not complete"),
            "expected a handshake-timeout message, got: {err:?}"
        );
        assert!(
            std::error::Error::source(&err).is_some(),
            "establishment-timeout failure must retain its cause as source(): {err:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "establishment_timeout(150ms) should fire within ~2s, took {elapsed:?}"
        );

        server.abort();
    }

    #[tokio::test]
    async fn establishment_timeout_bounds_custom_connector_dial() {
        use std::time::Instant;

        // A connector that never resolves — establishment_timeout must bound the
        // caller's dial, not just the h2 preface, so this fires.
        let never = tower::service_fn(|_uri: Uri| async move {
            std::future::pending::<()>().await;
            // Unreachable; concrete type for inference.
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tokio::io::duplex(1).0))
        });
        let start = Instant::now();
        let err = Http2Connection::builder()
            .establishment_timeout(Duration::from_millis(150))
            .connect_with_connector(never, "http://localhost".parse().unwrap())
            .await
            .unwrap_err();
        let elapsed = start.elapsed();

        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
        assert!(
            err.message
                .as_deref()
                .unwrap()
                .contains("establishment did not complete"),
            "expected a handshake-timeout message, got: {err:?}"
        );
        assert!(
            std::error::Error::source(&err).is_some(),
            "establishment-timeout failure must retain its cause as source(): {err:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "establishment_timeout(150ms) should fire within ~2s, took {elapsed:?}"
        );
    }

    #[cfg(feature = "server")]
    #[tokio::test]
    async fn handshake_succeeds_within_generous_bound() {
        // A real h2c server completes the preface promptly, so a generous
        // establishment_timeout must not interfere with normal establishment.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let service =
                        hyper::service::service_fn(|_req: Request<hyper::body::Incoming>| async {
                            Ok::<_, std::convert::Infallible>(Response::new(
                                http_body_util::Full::new(bytes::Bytes::from_static(b"ok")),
                            ))
                        });
                    let _ = hyper::server::conn::http2::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(io, service)
                    .await;
                });
            }
        });

        let uri: Uri = format!("http://{addr}").parse().unwrap();
        let conn = Http2Connection::builder()
            .tcp_connect_timeout(Duration::from_secs(5))
            .establishment_timeout(Duration::from_secs(5))
            .connect_plaintext(uri)
            .await
            .expect("establishment should succeed within a generous bound");
        let _ = conn;

        server.abort();
    }
}
