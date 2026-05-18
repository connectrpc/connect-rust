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
    pub fn lazy_plaintext(uri: Uri) -> Self {
        // Scheme check is deferred to the first poll_ready via the
        // Reconnect state machine's deferred_error mechanism, so this
        // constructor is infallible and balance pools can be built uniformly.
        // The actual check is in MakeSendRequest::call.
        Self {
            inner: Reconnect::new(MakeSendRequest::new(), uri, true),
        }
    }

    /// Eagerly establish a **plaintext** h2c connection now.
    /// Only for `http://` URIs.
    ///
    /// Returns an error if the URI scheme is `https://` (use
    /// [`connect_tls`](Self::connect_tls) instead), or if the initial TCP
    /// connect or h2 handshake fails. After the initial connect succeeds,
    /// reconnect-on-failure is handled automatically by the next `poll_ready`.
    pub async fn connect_plaintext(uri: Uri) -> Result<Self, ConnectError> {
        require_http_scheme(&uri)?;
        let mut conn = Self {
            inner: Reconnect::new(MakeSendRequest::new(), uri, false),
        };
        // Drive poll_ready to completion to force the connect now.
        std::future::poll_fn(|cx| conn.inner.poll_ready(cx))
            .await
            .map_err(|e| ConnectError::unavailable(format!("connect failed: {e}")))?;
        Ok(conn)
    }

    /// Customize the HTTP/2 settings (window sizes, keep-alive, etc).
    ///
    /// Plaintext only; use with [`lazy_plaintext`](Self::lazy_plaintext)
    /// semantics — the connection establishes on first `poll_ready`.
    pub fn with_builder_plaintext(
        uri: Uri,
        builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
    ) -> Self {
        Self {
            inner: Reconnect::new(MakeSendRequest::with_builder(builder), uri, true),
        }
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
    pub fn lazy_with_connector<C>(connector: C, authority: Uri) -> Self
    where
        C: tower::Service<Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin + 'static,
        C::Error: Into<BoxError>,
        C::Future: Send + 'static,
    {
        Self {
            inner: Reconnect::new(
                MakeSendRequest::new_custom(box_connector(connector)),
                authority,
                true,
            ),
        }
    }

    /// Eagerly establish an h2c connection using a **caller-supplied connector**.
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
        let mut conn = Self {
            inner: Reconnect::new(
                MakeSendRequest::new_custom(box_connector(connector)),
                authority,
                false,
            ),
        };
        std::future::poll_fn(|cx| conn.inner.poll_ready(cx))
            .await
            .map_err(|e| ConnectError::unavailable(format!("connect failed: {e}")))?;
        Ok(conn)
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
    pub fn lazy_unix(path: impl Into<std::path::PathBuf>, authority: Uri) -> Self {
        Self::lazy_with_connector(unix_connector(path.into()), authority)
    }

    /// Eagerly establish an h2c connection over a **Unix domain socket**.
    ///
    /// Returns an error if the socket path doesn't exist or the h2
    /// handshake fails. See [`lazy_unix`](Self::lazy_unix) for details.
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    pub async fn connect_unix(
        path: impl Into<std::path::PathBuf>,
        authority: Uri,
    ) -> Result<Self, ConnectError> {
        Self::connect_with_connector(unix_connector(path.into()), authority).await
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
    pub fn lazy_tls(uri: Uri, tls_config: Arc<rustls::ClientConfig>) -> Self {
        Self {
            inner: Reconnect::new(MakeSendRequest::new_tls(tls_config), uri, true),
        }
    }

    /// Eagerly establish a **TLS** h2 connection now. Only for `https://` URIs.
    ///
    /// See [`lazy_tls`](Self::lazy_tls) for ALPN and cert rotation details.
    ///
    /// Returns an error if the URI scheme is `http://`, if the TCP/TLS
    /// handshake fails, or if the server doesn't negotiate h2 via ALPN.
    #[cfg(feature = "client-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client-tls")))]
    pub async fn connect_tls(
        uri: Uri,
        tls_config: Arc<rustls::ClientConfig>,
    ) -> Result<Self, ConnectError> {
        require_https_scheme(&uri)?;
        let mut conn = Self {
            inner: Reconnect::new(MakeSendRequest::new_tls(tls_config), uri, false),
        };
        std::future::poll_fn(|cx| conn.inner.poll_ready(cx))
            .await
            .map_err(|e| ConnectError::unavailable(format!("TLS connect failed: {e}")))?;
        Ok(conn)
    }

    /// Customize the HTTP/2 settings (window sizes, keep-alive, etc) with TLS.
    ///
    /// TLS-only; uses lazy semantics — the connection establishes on
    /// first `poll_ready`. See [`lazy_tls`](Self::lazy_tls) for ALPN and
    /// cert rotation details.
    #[cfg(feature = "client-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client-tls")))]
    pub fn with_builder_tls(
        uri: Uri,
        builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
        tls_config: Arc<rustls::ClientConfig>,
    ) -> Self {
        Self {
            inner: Reconnect::new(
                MakeSendRequest::with_builder_tls(builder, tls_config),
                uri,
                true,
            ),
        }
    }
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
}

impl MakeSendRequest {
    fn new() -> Self {
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        let builder =
            hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
        Self {
            connector,
            builder,
            #[cfg(feature = "client-tls")]
            tls: None,
            custom: None,
        }
    }

    fn with_builder(
        builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
    ) -> Self {
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        Self {
            connector,
            builder,
            #[cfg(feature = "client-tls")]
            tls: None,
            custom: None,
        }
    }

    fn new_custom(conn: BoxedConnector) -> Self {
        // `connector` is unused when `custom` is Some — `call()` branches
        // to the custom connector before touching it. Kept to avoid
        // restructuring the shared struct; HttpConnector::new() is cheap.
        let connector = hyper_util::client::legacy::connect::HttpConnector::new();
        let builder =
            hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
        Self {
            connector,
            builder,
            #[cfg(feature = "client-tls")]
            tls: None,
            custom: Some(conn),
        }
    }

    #[cfg(feature = "client-tls")]
    fn new_tls(tls: Arc<rustls::ClientConfig>) -> Self {
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        // Allow https:// scheme to pass through (default enforces http).
        connector.enforce_http(false);
        let builder =
            hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
        Self {
            connector,
            builder,
            tls: Some(prepare_tls_for_h2(&tls)),
            custom: None,
        }
    }

    #[cfg(feature = "client-tls")]
    fn with_builder_tls(
        builder: hyper::client::conn::http2::Builder<hyper_util::rt::TokioExecutor>,
        tls: Arc<rustls::ClientConfig>,
    ) -> Self {
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        connector.enforce_http(false);
        Self {
            connector,
            builder,
            tls: Some(prepare_tls_for_h2(&tls)),
            custom: None,
        }
    }
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
            return Box::pin(async move {
                let io = io_fut.await?;
                let (send_request, conn) = builder.handshake(io).await?;
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

        Box::pin(async move {
            let io = connect_fut.await.map_err(Into::<BoxError>::into)?;

            // TLS handshake if configured. This is the same pattern tonic
            // uses for its Channel (transport/channel/service/connector.rs).
            // The two concrete IO types are unified via BoxedIo for handshake().
            #[cfg(feature = "client-tls")]
            let io: BoxedIo = if let (Some(tls), Some(server_name)) = (tls, server_name) {
                // Unwrap the TokioIo to get the raw TcpStream for TLS.
                let tcp = io.into_inner();
                let connector = tokio_rustls::TlsConnector::from(tls);
                let tls_stream = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| ConnectError::unavailable(format!("TLS handshake failed: {e}")))?;

                // Verify ALPN negotiated h2. A server that doesn't speak h2
                // would otherwise fail cryptically in the h2 handshake.
                // Same check tonic does (transport/channel/service/tls.rs:125).
                let (_, session) = tls_stream.get_ref();
                if session.alpn_protocol() != Some(b"h2") {
                    return Err(ConnectError::unavailable(
                        "TLS handshake succeeded but server did not negotiate \
                         HTTP/2 via ALPN (is the server h2-capable?)",
                    )
                    .into());
                }

                Box::pin(hyper_util::rt::TokioIo::new(tls_stream))
            } else {
                Box::pin(io)
            };

            let (send_request, conn) = builder.handshake(io).await?;
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
}
