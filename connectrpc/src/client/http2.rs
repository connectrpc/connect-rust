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

#[cfg(feature = "client-tls")]
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
                ConnectError::unavailable(format!(
                    "unix socket connect to {} failed: {e}",
                    path.display()
                ))
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
    /// Returns a builder for bounding connection establishment (TCP connect,
    /// TLS handshake, HTTP/2 preface) before choosing a transport flavour.
    ///
    /// The constructors on `Http2Connection` are shortcuts for the builder
    /// with no bounds set, so `Http2Connection::lazy_plaintext(uri)` is exactly
    /// `Http2Connection::builder().lazy_plaintext(uri)`. Reach for the builder
    /// when you want to bound a stalled connect or handshake — see
    /// [`Http2ConnectionBuilder::connect_timeout`] and
    /// [`Http2ConnectionBuilder::handshake_timeout`].
    #[must_use]
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
    /// — the connection establishes on first `poll_ready`. To also bound
    /// connection establishment, use
    /// [`builder()`](Self::builder)`.h2_settings(h2).lazy_plaintext(uri)`.
    #[must_use]
    pub fn with_builder_plaintext(
        uri: Uri,
        builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
    ) -> Self {
        Self::builder().h2_settings(builder).lazy_plaintext(uri)
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
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    #[must_use]
    pub fn lazy_unix(path: impl Into<std::path::PathBuf>, authority: Uri) -> Self {
        Self::builder().lazy_unix(path, authority)
    }

    /// Eagerly establish an h2c connection over a **Unix domain socket**.
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
    /// cert rotation details. To also bound connection establishment, use
    /// [`builder()`](Self::builder)`.h2_settings(h2).lazy_tls(uri, tls)`.
    #[cfg(feature = "client-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client-tls")))]
    #[must_use]
    pub fn with_builder_tls(
        uri: Uri,
        builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
        tls_config: Arc<rustls::ClientConfig>,
    ) -> Self {
        Self::builder()
            .h2_settings(builder)
            .lazy_tls(uri, tls_config)
    }
}

/// Builder for [`Http2Connection`] connection-establishment bounds.
///
/// Obtain one via [`Http2Connection::builder`]. The terminal methods mirror the
/// `Http2Connection` constructors; the bare constructors delegate here with no
/// bounds set, so behaviour is unchanged unless you call a setter.
///
/// Both bounds default to unset (no time limit). Establishment then runs until
/// it succeeds, errors, or the kernel gives up — a server that accepts the TCP
/// connection but stalls during the TLS handshake would otherwise stall
/// `poll_ready` for every caller sharing the connection. The bounds here are the
/// *establishment* budget; [`CallOptions::with_timeout`] remains the end-to-end
/// per-request bound.
///
/// # Scope
///
/// The builder covers every [`Http2Connection`] transport: plaintext, TLS,
/// caller-supplied connectors, and Unix sockets. HTTP/2 keep-alive and
/// flow-control knobs are proxied directly; for hyper settings not surfaced
/// here use [`h2_settings`](Self::h2_settings). [`connect_timeout`] is the
/// per-address TCP bound on the built-in connector and is ignored by the
/// custom-connector / Unix-socket terminals — use [`handshake_timeout`] there
/// as the establishment bound.
///
/// [`connect_timeout`]: Self::connect_timeout
/// [`handshake_timeout`]: Self::handshake_timeout
///
/// [`CallOptions::with_timeout`]: super::CallOptions::with_timeout
#[derive(Debug, Clone)]
pub struct Http2ConnectionBuilder {
    connect_timeout: Option<Duration>,
    handshake_timeout: Option<Duration>,
    h2_builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
}

impl Default for Http2ConnectionBuilder {
    /// A fresh builder with no establishment bounds and default HTTP/2
    /// settings. A [`TokioTimer`](hyper_util::rt::TokioTimer) is pre-wired so
    /// the keep-alive setters work without the caller having to install one
    /// (hyper otherwise panics at handshake time if a keep-alive interval is
    /// set without a timer).
    fn default() -> Self {
        let mut h2_builder =
            hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
        h2_builder.timer(hyper_util::rt::TokioTimer::new());
        Self {
            connect_timeout: None,
            handshake_timeout: None,
            h2_builder,
        }
    }
}

impl Http2ConnectionBuilder {
    /// Bound the TCP connect phase.
    ///
    /// Applied to the built-in connector via hyper's
    /// [`HttpConnector::set_connect_timeout`][hyper-ct]; it covers only the TCP
    /// `connect(2)` call (per resolved address — the budget is divided across
    /// the address set). It does **not** cover DNS resolution, the TLS
    /// handshake, or the HTTP/2 preface — set
    /// [`handshake_timeout`](Self::handshake_timeout) too to bound those — and
    /// is **ignored** by the custom-connector / Unix-socket terminals (which
    /// have no built-in `HttpConnector` to apply it to).
    ///
    /// Unset (the default) means TCP connect is governed by the kernel's
    /// `tcp_syn_retries` (typically ~130s on Linux).
    ///
    /// [hyper-ct]: hyper_util::client::legacy::connect::HttpConnector::set_connect_timeout
    #[must_use]
    pub fn connect_timeout(mut self, dur: Duration) -> Self {
        self.connect_timeout = Some(dur);
        self
    }

    /// Bound the connection handshake: DNS resolution, TCP connect, the TLS
    /// handshake (for `tls` connections), and the HTTP/2 preface, as one
    /// wall-clock budget. [`connect_timeout`](Self::connect_timeout) is an
    /// additional per-address TCP bound *inside* this budget. It mirrors the
    /// server-side TLS handshake timeout (`Server::with_tls_handshake_timeout`).
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
    ///
    /// Unset (the default) means the handshake is unbounded.
    #[must_use]
    pub fn handshake_timeout(mut self, dur: Duration) -> Self {
        self.handshake_timeout = Some(dur);
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
    /// [`handshake_timeout`](Self::handshake_timeout) it bounds the transport
    /// against a peer that goes silent at any point.
    #[must_use]
    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.h2_builder.keep_alive_interval(interval);
        self
    }

    /// Set how long to wait for a keep-alive PING acknowledgement before
    /// closing the connection. Only applies when
    /// [`keep_alive_interval`](Self::keep_alive_interval) is set. hyper
    /// defaults to 20 seconds.
    #[must_use]
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
    /// [`handshake_timeout`](Self::handshake_timeout) (already ended) and
    /// keep-alive (not armed).
    #[must_use]
    pub fn keep_alive_while_idle(mut self, enabled: bool) -> Self {
        self.h2_builder.keep_alive_while_idle(enabled);
        self
    }

    /// Set the initial HTTP/2 stream-level flow-control window size.
    /// hyper defaults to 65,535 bytes.
    #[must_use]
    pub fn initial_stream_window_size(mut self, size: u32) -> Self {
        self.h2_builder.initial_stream_window_size(size);
        self
    }

    /// Set the initial HTTP/2 connection-level flow-control window size.
    /// hyper defaults to 65,535 bytes.
    #[must_use]
    pub fn initial_connection_window_size(mut self, size: u32) -> Self {
        self.h2_builder.initial_connection_window_size(size);
        self
    }

    /// Enable hyper's adaptive flow-control window (BDP-based auto-tuning).
    /// When enabled, the explicit window-size setters are ignored.
    #[must_use]
    pub fn adaptive_window(mut self, enabled: bool) -> Self {
        self.h2_builder.adaptive_window(enabled);
        self
    }

    /// Replace the underlying hyper HTTP/2 builder wholesale.
    ///
    /// This is the escape hatch for hyper knobs not proxied above. It
    /// **replaces** any prior keep-alive / window-size settings on this
    /// builder, so call it before the individual setters if you use both. A
    /// [`TokioTimer`](hyper_util::rt::TokioTimer) is re-applied to the passed
    /// builder so keep-alive works without the caller wiring one. Prefer the
    /// individual setters above for the common cases.
    #[must_use]
    pub fn h2_settings(
        mut self,
        mut builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
    ) -> Self {
        builder.timer(hyper_util::rt::TokioTimer::new());
        self.h2_builder = builder;
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
    /// `handshake_timeout` bounds the connector's dial *and* the HTTP/2 preface
    /// as one wall-clock budget — the same semantics as the built-in
    /// transports. `connect_timeout` is the per-address TCP bound on the
    /// built-in connector and is **ignored** here; to bound the dial separately
    /// from the preface, wrap the connector in `tower::timeout::Timeout`.
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
    /// exceeding a configured `handshake_timeout`).
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
    /// fails (including exceeding a configured `handshake_timeout`).
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
    /// `connect_timeout` applied. Mirrors `HttpClientBuilder::http_connector`.
    fn http_connector(&self) -> hyper_util::client::legacy::connect::HttpConnector {
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_connect_timeout(self.connect_timeout);
        connector
    }

    fn make_plaintext(self) -> MakeSendRequest {
        MakeSendRequest {
            connector: self.http_connector(),
            builder: self.h2_builder,
            #[cfg(feature = "client-tls")]
            tls: None,
            custom: None,
            handshake_timeout: self.handshake_timeout,
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
            handshake_timeout: self.handshake_timeout,
        }
    }

    fn make_custom(self, conn: BoxedConnector) -> MakeSendRequest {
        MakeSendRequest {
            // Unused when `custom` is Some — `call()` branches to the custom
            // connector before touching it. `connect_timeout` is therefore
            // dropped here (it's the per-address TCP bound on this connector).
            connector: hyper_util::client::legacy::connect::HttpConnector::new(),
            builder: self.h2_builder,
            #[cfg(feature = "client-tls")]
            tls: None,
            custom: Some(conn),
            handshake_timeout: self.handshake_timeout,
        }
    }
}

/// Drive a connection's `poll_ready` to completion, forcing the eager connect.
async fn drive_connect(conn: &mut Http2Connection, ctx: &str) -> Result<(), ConnectError> {
    std::future::poll_fn(|cx| conn.inner.poll_ready(cx))
        .await
        .map_err(|e| ConnectError::unavailable(format!("{ctx}: {e}")))
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

// Http2Connection needs Clone to satisfy ClientTransport, but the inner
// Reconnect state machine is !Clone by design (each instance tracks one
// connection). For ClientTransport's shared-access semantics we use an
// Arc<tokio::Mutex> — but that would serialize requests and defeat the
// purpose. Instead, we only implement ClientTransport for the wrapped
// ServiceTransport version, and let users who want to share wrap in
// Buffer or Balance themselves.
//
// For the direct "single connection" use case, the generated FooServiceClient
// takes `T: ClientTransport` which requires Clone — so direct Http2Connection
// can't be used without a Buffer layer. This is intentional: a raw h2
// connection IS !Clone; sharing it requires coordination.
//
// Workaround for the common case: provide `Http2Connection::shared()` that
// returns a Buffer-wrapped, Clone-able handle.

/// A `Clone + ClientTransport` handle to a shared [`Http2Connection`].
///
/// Created via [`Http2Connection::shared`]. The underlying connection is
/// driven by a background worker task; callers get a cheap-to-clone channel
/// handle. Unlike [`HttpClient`](super::HttpClient), the underlying readiness
/// still backpressures correctly through the buffer.
#[derive(Clone)]
#[allow(clippy::type_complexity)] // Buffer's type param is what it is
pub struct SharedHttp2Connection {
    inner: tower::buffer::Buffer<
        Request<ClientBody>,
        BoxFuture<'static, Result<Response<hyper::body::Incoming>, BoxError>>,
    >,
}

impl Http2Connection {
    /// Wrap this connection in a [`tower::buffer::Buffer`] for `Clone +
    /// ClientTransport` use.
    ///
    /// `bound` is the channel capacity — requests beyond this backpressure
    /// through `poll_ready`. For a single-connection gRPC client, 1024 is
    /// a reasonable default (covers typical `max_concurrent_streams`).
    ///
    /// Requires being called from within a tokio runtime (to spawn the
    /// buffer's worker task).
    pub fn shared(self, bound: usize) -> SharedHttp2Connection {
        let (buffer, worker) = tower::buffer::Buffer::pair(self, bound);
        tokio::spawn(worker);
        SharedHttp2Connection { inner: buffer }
    }
}

impl tower::Service<Request<ClientBody>> for SharedHttp2Connection {
    type Response = Response<hyper::body::Incoming>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Response<hyper::body::Incoming>, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <_ as tower::Service<Request<ClientBody>>>::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, req: Request<ClientBody>) -> Self::Future {
        let fut = <_ as tower::Service<Request<ClientBody>>>::call(&mut self.inner, req);
        Box::pin(fut)
    }
}

impl ClientTransport for SharedHttp2Connection {
    type ResponseBody = hyper::body::Incoming;
    type Error = ConnectError;

    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
        use tower::ServiceExt;
        let svc = self.clone();
        Box::pin(async move {
            svc.oneshot(request)
                .await
                .map_err(|e| ConnectError::unavailable(format!("h2 send failed: {e}")))
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
    handshake_timeout: Option<Duration>,
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
            let handshake_timeout = self.handshake_timeout;
            return Box::pin(async move {
                // Dial + HTTP/2 preface as one wall-clock budget — same
                // semantics as the built-in branch's `establish` block below.
                let establish = async move {
                    let io = io_fut.await?;
                    builder.handshake(io).await.map_err(BoxError::from)
                };
                let (send_request, conn) = run_handshake(establish, handshake_timeout).await?;
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
        let handshake_timeout = self.handshake_timeout;

        Box::pin(async move {
            // DNS + TCP connect + TLS handshake (if configured) + HTTP/2
            // preface — bounded together by `handshake_timeout` so a server
            // that accepts the TCP connection but stalls the handshake (or a
            // hung resolver) can't hang `poll_ready` for every caller sharing
            // this connection. The per-address TCP connect is additionally
            // bounded by `connect_timeout` on the connector.
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
                        BoxError::from(ConnectError::unavailable(format!(
                            "TLS handshake failed: {e}"
                        )))
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

            let (send_request, conn) = run_handshake(establish, handshake_timeout).await?;
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
pub(super) async fn run_handshake<F, T, E>(fut: F, timeout: Option<Duration>) -> Result<T, BoxError>
where
    F: Future<Output = Result<T, E>>,
    E: Into<BoxError> + 'static,
{
    match timeout {
        Some(dur) => match tokio::time::timeout(dur, fut).await {
            Ok(res) => res.map_err(Into::into),
            Err(_) => Err(ConnectError::unavailable(format!(
                "connection handshake did not complete within {dur:?}"
            ))
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
        let err = Http2Connection::connect_plaintext("http://127.0.0.1:1".parse().unwrap()).await;
        assert!(err.is_err(), "expected connect to port 1 to fail");
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
    }

    #[test]
    fn builder_defaults_are_unset() {
        let builder = Http2Connection::builder();
        assert!(builder.connect_timeout.is_none());
        assert!(builder.handshake_timeout.is_none());
    }

    #[test]
    fn builder_setters_record_durations() {
        let builder = Http2Connection::builder()
            .connect_timeout(Duration::from_millis(10))
            .handshake_timeout(Duration::from_millis(20));
        assert_eq!(builder.connect_timeout, Some(Duration::from_millis(10)));
        assert_eq!(builder.handshake_timeout, Some(Duration::from_millis(20)));
    }

    #[tokio::test]
    async fn builder_connect_timeout_bounds_tcp_connect() {
        use std::time::Instant;

        // RFC 5737 TEST-NET-1: guaranteed unroutable, so SYNs are dropped and an
        // unbounded connect would stall on kernel retransmits (~130s). A 100ms
        // connect_timeout must abort well before that.
        let start = Instant::now();
        let err = Http2Connection::builder()
            .connect_timeout(Duration::from_millis(100))
            .connect_plaintext("http://192.0.2.1:9".parse().unwrap())
            .await
            .unwrap_err();
        let elapsed = start.elapsed();

        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
        // Some CI hosts actively reject TEST-NET-1 (ENETUNREACH / ICMP) instead
        // of dropping SYNs; that fails in <1ms without exercising the timeout.
        // Skip the lower-bound check there rather than hard-fail.
        if elapsed < Duration::from_millis(50) {
            eprintln!(
                "skipping lower-bound check: host rejected TEST-NET-1 \
                 in {elapsed:?} ({err:?})"
            );
            return;
        }
        assert!(
            elapsed >= Duration::from_millis(100),
            "expected to wait out the 100ms budget, took {elapsed:?}: {err:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "connect_timeout(100ms) should abort within ~2s, took {elapsed:?}: {err:?}"
        );
    }

    // hyper's plaintext h2c handshake resolves locally (it sends the client
    // preface without waiting for the server's SETTINGS), so a stalled cleartext
    // server stalls the first *request*, not the handshake. The TLS handshake,
    // by contrast, genuinely blocks on the server, so that is where
    // handshake_timeout has observable effect — exercised here.
    #[cfg(feature = "client-tls")]
    #[tokio::test]
    async fn handshake_timeout_fires_when_tls_server_stalls_after_accept() {
        use std::time::Instant;

        // A listener that accepts the TCP connection but never performs the TLS
        // handshake. The TCP connect succeeds, so only handshake_timeout can
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
            .handshake_timeout(Duration::from_millis(150))
            .connect_tls(uri, tls_config)
            .await
            .unwrap_err();
        let elapsed = start.elapsed();

        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
        assert!(
            err.message
                .as_deref()
                .unwrap()
                .contains("handshake did not complete"),
            "expected a handshake-timeout message, got: {err:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "handshake_timeout(150ms) should fire within ~2s, took {elapsed:?}"
        );

        server.abort();
    }

    #[cfg(feature = "client-tls")]
    #[tokio::test]
    async fn handshake_timeout_applies_with_custom_h2_settings() {
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
            .handshake_timeout(Duration::from_millis(150))
            .connect_tls(uri, tls_config)
            .await
            .unwrap_err();
        let elapsed = start.elapsed();

        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
        assert!(
            err.message
                .as_deref()
                .unwrap()
                .contains("handshake did not complete"),
            "expected a handshake-timeout message, got: {err:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "handshake_timeout(150ms) should fire within ~2s, took {elapsed:?}"
        );

        server.abort();
    }

    #[tokio::test]
    async fn handshake_timeout_bounds_custom_connector_dial() {
        use std::time::Instant;

        // A connector that never resolves — handshake_timeout must bound the
        // caller's dial, not just the h2 preface, so this fires.
        let never = tower::service_fn(|_uri: Uri| async move {
            std::future::pending::<()>().await;
            // Unreachable; concrete type for inference.
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tokio::io::duplex(1).0))
        });
        let start = Instant::now();
        let err = Http2Connection::builder()
            .handshake_timeout(Duration::from_millis(150))
            .connect_with_connector(never, "http://localhost".parse().unwrap())
            .await
            .unwrap_err();
        let elapsed = start.elapsed();

        assert_eq!(err.code, crate::error::ErrorCode::Unavailable);
        assert!(
            err.message
                .as_deref()
                .unwrap()
                .contains("handshake did not complete"),
            "expected a handshake-timeout message, got: {err:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "handshake_timeout(150ms) should fire within ~2s, took {elapsed:?}"
        );
    }

    #[cfg(feature = "server")]
    #[tokio::test]
    async fn handshake_succeeds_within_generous_bound() {
        // A real h2c server completes the preface promptly, so a generous
        // handshake_timeout must not interfere with normal establishment.
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
            .connect_timeout(Duration::from_secs(5))
            .handshake_timeout(Duration::from_secs(5))
            .connect_plaintext(uri)
            .await
            .expect("establishment should succeed within a generous bound");
        let _ = conn;

        server.abort();
    }
}
