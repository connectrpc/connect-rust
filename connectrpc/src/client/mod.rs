//! Tower-based HTTP client transports for ConnectRPC.
//!
//! Generated `FooServiceClient<T>` structs are generic over
//! `T: `[`ClientTransport`] — any tower-compatible HTTP client works. This
//! module provides two concrete transports:
//!
//! | Transport | Protocol | Use when |
//! |---|---|---|
//! | [`SharedHttp2Connection`] | HTTP/2 only | **Default for gRPC.** Honest `poll_ready`, composes with `tower::balance`. |
//! | [`HttpClient`] | HTTP/1.1 + HTTP/2 (ALPN) | Connect protocol over h/1.1, or you genuinely don't know which protocol the server speaks. |
//!
//! # For gRPC: `SharedHttp2Connection`
//!
//! gRPC mandates HTTP/2. Use [`Http2Connection`] (or its `Clone`-able
//! [`SharedHttp2Connection`] wrapper), which holds a single raw h2 connection
//! with a reconnect state machine and *honest* readiness reporting:
//!
//! ```rust,ignore
//! use connectrpc::client::{Http2Connection, ClientConfig};
//! use connectrpc::Protocol;
//!
//! let uri: http::Uri = "http://localhost:8080".parse()?;
//! let conn = Http2Connection::connect_plaintext(uri.clone()).await?.shared(1024);
//! let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
//!
//! // Generated clients take any T: ClientTransport — shared handle is cheap to clone.
//! let greet = GreetServiceClient::new(conn.clone(), config.clone());
//! let math  = MathServiceClient::new(conn.clone(), config.clone());
//!
//! let response = greet.greet(GreetRequest { name: "World".into() }).await?;
//! ```
//!
//! ## Scaling past single-connection contention
//!
//! A single HTTP/2 connection has a throughput ceiling set by `h2`'s internal
//! `Mutex<Inner>` ([h2 #531]) — typically ~30–40k req/s regardless of handler
//! work. To scale past that, spread load across N connections:
//!
//! ```rust,ignore
//! // Simple static round-robin: worker i uses connection i % N.
//! let conns: Vec<_> = futures::future::try_join_all(
//!     (0..8).map(|_| Http2Connection::connect_plaintext(uri.clone()))
//! ).await?
//!  .into_iter().map(|c| c.shared(1024)).collect();
//!
//! // Or: tower::balance::p2c::Balance + tower::load::PendingRequests
//! // for dynamic load-aware routing. See the http2 module docs.
//! ```
//!
//! Because `Http2Connection::poll_ready` honestly reports connection state
//! (connecting / closed / ready), `tower::balance` can route around
//! failed connections and p2c can make useful decisions.
//!
//! # For Connect over HTTP/1.1: `HttpClient`
//!
//! The Connect protocol works over both HTTP/1.1 and HTTP/2. If you need
//! HTTP/1.1 (older reverse proxies, edge environments without h2c) or
//! ALPN-based protocol negotiation for TLS connections, use [`HttpClient`]:
//!
//! ```rust,ignore
//! use connectrpc::client::{HttpClient, ClientConfig};
//!
//! // Auto-negotiates HTTP/1.1 or HTTP/2 via ALPN (for https://) or uses
//! // HTTP/1.1 by default for cleartext http://.
//! let http = HttpClient::plaintext();
//! let config = ClientConfig::new("http://localhost:8080".parse()?);
//!
//! let greet = GreetServiceClient::new(http.clone(), config);
//! ```
//!
//! ## Caveats for `HttpClient` with `tower::balance`
//!
//! `HttpClient` wraps `hyper_util::client::legacy::Client`, whose `poll_ready`
//! is **always `Ready(Ok)`** (it manages queueing and connection reuse
//! internally). This means `tower::balance::p2c` has no real load signal and
//! degrades to ~random selection. For HTTP/1.1 this is usually fine — the
//! internal pool already load-balances across idle connections — but for
//! HTTP/2 it pins all requests to a single shared connection with no way for
//! balance to spread load. Prefer [`SharedHttp2Connection`] for that.
//!
//! # Tower middleware
//!
//! Both transports are `tower::Service`s, so standard layers compose:
//!
//! ```rust,ignore
//! use tower::ServiceBuilder;
//! use tower_http::timeout::TimeoutLayer;
//!
//! let conn = Http2Connection::connect_plaintext(uri).await?.shared(1024);
//! let stacked = ServiceBuilder::new()
//!     .layer(TimeoutLayer::new(Duration::from_secs(30)))
//!     .service(conn);
//!
//! let client = GreetServiceClient::new(
//!     connectrpc::client::ServiceTransport::new(stacked),
//!     config,
//! );
//! ```
//!
//! [h2 #531]: https://github.com/hyperium/h2/issues/531

use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use bytes::BytesMut;
use http::Request;
use http::Response;
use http::Uri;
use http_body::Body;
use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::combinators::BoxBody;

use buffa::view::HasMessageView;
use buffa::view::MessageView;
use buffa::view::OwnedView;
use buffa::view::ViewReborrow;
/// Re-export of [`futures::Stream`] (the `futures` 0.3 / `futures-core` 0.3
/// trait), which [`ClientRequestStream`] builds on. Re-exported so generic
/// code can name the trait without a direct `futures` dependency.
pub use futures::Stream;
/// Re-export of [`futures::stream::iter`]: adapts a collection that is
/// already in hand into a request stream for a client-streaming call,
/// without a direct `futures` dependency.
pub use futures::stream::iter as stream_iter;

mod sealed {
    pub trait Sealed {}
    impl<S> Sealed for S where S: super::Stream + Send + 'static {}
}

/// The request-stream bound for client-streaming calls.
///
/// Implemented automatically for every `Stream<Item = Req> + Send + 'static`
/// — it cannot (and never needs to) be implemented by hand. The trait exists
/// so the compiler can point at the two usual fixes when the bound is not
/// met: wrap a ready collection with [`stream_iter`], and make a borrowing
/// stream yield owned messages (the stream backs the request body, which can
/// outlive the call frame and move across threads — hence `Send + 'static`).
///
/// Not to be confused with the server-side
/// [`dispatcher::RequestStream`](crate::dispatcher::RequestStream), a boxed
/// stream of raw request bytes.
///
/// # Panics in `poll_next`
///
/// The stream backs the request body, so it is polled on the task driving
/// the HTTP request rather than on the caller's. A panic in `poll_next`
/// therefore does not propagate to the caller: it surfaces as a generic
/// transport error, and where that driver task is shared between calls
/// (such as [`SharedHttp2Connection`]) it can fault every RPC on that
/// connection, not just this one. The stream yields `Req`, not a
/// `Result`, so it has no channel for reporting its own failure: end the
/// stream early instead of panicking, and surface the reason through your
/// own application protocol.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be used as the request stream of a client-streaming call",
    label = "expected an async `Stream<Item = {Req}> + Send + 'static`",
    note = "for a collection that is already in hand, wrap it with `connectrpc::stream_iter(...)`",
    note = "the stream backs the request body, so it must be `Send + 'static`: yield owned messages (no borrows of local data) or feed the call from a channel-backed stream"
)]
pub trait ClientRequestStream<Req>: sealed::Sealed + Stream<Item = Req> + Send + 'static {}

impl<S, Req> ClientRequestStream<Req> for S where S: Stream<Item = Req> + Send + 'static {}

use crate::codec::CodecFormat;
use crate::codec::content_type;
use crate::codec::encode_json;
use crate::codec::header as connect_header;
use crate::compression::CompressionPolicy;
use crate::compression::CompressionRegistry;
use crate::envelope::Envelope;
use crate::error::ConnectError;
use crate::error::ErrorCode;
use crate::error::ErrorDetail;
use crate::protocol::Protocol;

/// Type alias for a boxed future, used in service implementations.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The erased request body type accepted by [`ClientTransport::send`].
///
/// Non-streaming call sites construct this via [`full_body`]. Bidi streaming
/// uses a channel-backed body so the request can be sent incrementally.
pub type ClientBody = BoxBody<Bytes, ConnectError>;

/// Wrap a fully-known buffer in a [`ClientBody`].
///
/// Used by unary and server-streaming calls where the complete request body
/// is available before sending.
#[inline]
pub fn full_body(b: Bytes) -> ClientBody {
    Full::new(b).map_err(|never| match never {}).boxed()
}

/// Walk an error's `source()` chain looking for a [`ConnectError`].
///
/// Boxed trait objects cannot appear as links in the chain: `Box<dyn Error>`
/// does not itself implement `Error` (the blanket impl requires a sized
/// type), so every link is a concrete error type and a plain `downcast_ref`
/// at each link is exhaustive.
fn find_connect_error_in_chain(
    mut err: &(dyn std::error::Error + 'static),
) -> Option<ConnectError> {
    loop {
        if let Some(connect_err) = err.downcast_ref::<ConnectError>() {
            return Some(connect_err.clone());
        }
        err = err.source()?;
    }
}

/// Map a [`ClientTransport::send`] failure into the error surfaced to the
/// caller.
///
/// Policy: a [`ConnectError`] found anywhere in the transport error's source
/// chain is returned verbatim (preserving its code, message, details, and
/// attached metadata; any outer wrappers' `Display` context is dropped).
/// Both built-in transports already produce classified `ConnectError`s
/// directly, so for them `context` never appears in the surfaced error.
/// Errors with no `ConnectError` in their chain are wrapped as `unavailable`
/// with the `context` prefix.
fn map_transport_send_error<E>(err: E, context: &str) -> ConnectError
where
    E: std::error::Error + Send + Sync + 'static,
{
    find_connect_error_in_chain(&err)
        .unwrap_or_else(|| ConnectError::unavailable(format!("{context}: {err}")))
}

/// Extra slack added to client-side response buffer caps beyond the message
/// size itself, to accommodate gRPC-Web trailer frames (which arrive as a
/// separate 0x80-flagged body frame, not a standard envelope). 64 KiB is
/// generous: the gRPC best-practices guide recommends keeping metadata
/// under 8 KiB per header set.
const RESPONSE_BUFFER_TRAILER_SLACK: usize = 64 * 1024;

/// Return the end offset of a complete gRPC-Web trailer frame, if present.
fn grpc_web_trailer_frame_end(data: &[u8]) -> Option<usize> {
    let mut offset = 0;

    while data.len().saturating_sub(offset) >= crate::envelope::HEADER_SIZE {
        let length = u32::from_be_bytes([
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
        ]) as usize;
        let frame_end = offset
            .checked_add(crate::envelope::HEADER_SIZE)?
            .checked_add(length)?;
        if frame_end > data.len() {
            return None;
        }
        if data[offset] & crate::envelope::flags::GRPC_WEB_TRAILER != 0 {
            return Some(frame_end);
        }
        offset = frame_end;
    }

    None
}

/// Trait for types that can be used as ConnectRPC client transports.
///
/// This is automatically implemented for any `tower::Service` that handles
/// HTTP requests with compatible body types.
pub trait ClientTransport: Clone + Send + Sync + 'static {
    /// The response body type.
    type ResponseBody: Body<Data = Bytes> + Send + 'static;
    /// The error type.
    ///
    /// If a [`ConnectError`] appears anywhere in this error's `source()`
    /// chain (or is the error itself), the client call paths surface it to
    /// the caller verbatim — code, message, details, and attached metadata —
    /// discarding any outer wrappers' `Display` context. A transport can use
    /// this to control the surfaced error classification, for example
    /// returning `deadline_exceeded` from a timeout middleware. Errors with
    /// no `ConnectError` in their chain are wrapped as `unavailable`.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Send an HTTP request and receive a response.
    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>>;
}

/// Wrapper that implements `ClientTransport` for any compatible tower service.
#[derive(Clone)]
pub struct ServiceTransport<S> {
    service: S,
}

impl<S> ServiceTransport<S> {
    /// Create a new service transport.
    pub fn new(service: S) -> Self {
        Self { service }
    }

    /// Get a reference to the inner service.
    pub fn inner(&self) -> &S {
        &self.service
    }

    /// Get a mutable reference to the inner service.
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.service
    }

    /// Consume this transport and return the inner service.
    pub fn into_inner(self) -> S {
        self.service
    }
}

impl<S, ResBody> ClientTransport for ServiceTransport<S>
where
    S: tower::Service<Request<ClientBody>, Response = Response<ResBody>>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send + 'static,
    ResBody: Body<Data = Bytes> + Send + 'static,
    ResBody::Error: std::error::Error + Send + Sync + 'static,
{
    type ResponseBody = ResBody;
    type Error = S::Error;

    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
        // Use ServiceExt::oneshot to satisfy the tower contract: poll_ready()
        // must return Ready(Ok(())) before call(). Many services (buffered,
        // rate-limited, concurrency-limited) panic or deadlock if call() is
        // invoked without readiness. oneshot handles this handshake correctly.
        use tower::ServiceExt;
        let service = self.service.clone();
        Box::pin(service.oneshot(request))
    }
}

// Raw HTTP/2 connection transport — see module docs for when to use vs HttpClient.
#[cfg(feature = "client")]
mod http2;
#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub use http2::Http2Connection;
#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub use http2::Http2ConnectionBuilder;
#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub use http2::SharedHttp2Connection;
#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub use http2::{DEFAULT_ESTABLISHMENT_TIMEOUT, DEFAULT_TCP_CONNECT_TIMEOUT};

/// General-purpose HTTP client supporting both HTTP/1.1 and HTTP/2.
///
/// This wraps `hyper_util::client::legacy::Client` with sensible defaults.
/// It auto-negotiates HTTP/1.1 vs HTTP/2 via ALPN (for `https://`) or
/// defaults to HTTP/1.1 for cleartext `http://`.
///
/// # When to use this
///
/// - **Connect protocol over HTTP/1.1** (the main use case)
/// - You genuinely don't know whether the server speaks h/1.1 or h/2
/// - You want ALPN-based protocol negotiation for TLS
///
/// # When NOT to use this
///
/// For **gRPC** (which mandates HTTP/2), prefer [`SharedHttp2Connection`]:
///
/// - `HttpClient`'s `poll_ready` is always `Ready` (internal pool/queue) —
///   `tower::balance` degrades to random selection.
/// - For HTTP/2, the internal pool holds exactly ONE shared connection —
///   all requests contend on a single h2 `Mutex<Inner>`, creating a
///   throughput ceiling at ~30–40k req/s.
///
/// [`Http2Connection`] has honest `poll_ready` and is a single connection
/// by design, so you can create N of them and balance across them properly.
///
/// Available when the `client` feature is enabled.
#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
#[derive(Clone)]
pub struct HttpClient {
    inner: HttpClientInner,
}

// Manual impl: hyper's `Client` doesn't impl `Debug`. Print the mode so
// tests can identify which transport variant unexpectedly succeeded.
#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = match self.inner {
            HttpClientInner::Plain(_) => "plaintext",
            #[cfg(feature = "client-tls")]
            HttpClientInner::Tls(_) => "tls",
        };
        f.debug_struct("HttpClient").field("mode", &mode).finish()
    }
}

/// Inner hyper client, parameterized over connector type via an enum.
///
/// Both connectors are wrapped in [`TimeoutConnector`] so an optional
/// `establishment_timeout` can bound the whole connector establishment. When no
/// timeout is set the wrapper forwards each connect unchanged (a single boxed
/// future per *connection*, not per request — negligible).
#[cfg(feature = "client")]
#[derive(Clone)]
enum HttpClientInner {
    /// Plaintext HTTP (http:// only). Rejects https:// at send-time.
    Plain(
        hyper_util::client::legacy::Client<
            TimeoutConnector<hyper_util::client::legacy::connect::HttpConnector>,
            ClientBody,
        >,
    ),
    /// TLS HTTP (https:// only). hyper-rustls's https_only mode rejects
    /// http:// at the connector level.
    #[cfg(feature = "client-tls")]
    Tls(
        hyper_util::client::legacy::Client<
            TimeoutConnector<
                hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
            >,
            ClientBody,
        >,
    ),
}

/// A `tower::Service<Uri>` connector wrapper that bounds connection
/// establishment with an optional timeout.
///
/// Wraps the built-in `HttpConnector` (plaintext) or hyper-rustls's
/// `HttpsConnector` (TLS). When `timeout` is `Some`, a connect that doesn't
/// resolve in time is cancelled (dropping the in-flight TCP/TLS work) and
/// surfaced as an `unavailable` error. When `None`, the inner connector's
/// future is awaited unchanged.
#[cfg(feature = "client")]
#[derive(Clone)]
struct TimeoutConnector<C> {
    inner: C,
    timeout: Option<Duration>,
}

#[cfg(feature = "client")]
impl<C> tower::Service<Uri> for TimeoutConnector<C>
where
    C: tower::Service<Uri>,
    C::Error: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    C::Future: Send + 'static,
    C::Response: Send + 'static,
{
    type Response = C::Response;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = BoxFuture<'static, Result<C::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let fut = self.inner.call(uri);
        let timeout = self.timeout;
        Box::pin(http2::run_establishment(fut, timeout))
    }
}

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
impl HttpClient {
    /// Returns a builder for configuring connector-level options before
    /// choosing a transport flavour.
    ///
    /// `HttpClient::plaintext()` is equivalent to
    /// `HttpClient::builder().plaintext()`, and likewise for the other
    /// constructors.
    pub fn builder() -> HttpClientBuilder {
        HttpClientBuilder::default()
    }

    /// Create a **plaintext** HTTP client. Only for `http://` URIs.
    ///
    /// Errors at send-time if given an `https://` URI — use
    /// [`with_tls`](Self::with_tls) for TLS.
    ///
    /// The client uses connection pooling and supports HTTP/1.1 and HTTP/2
    /// over cleartext. TCP_NODELAY is enabled to avoid Nagle + delayed ACK
    /// latency on small messages.
    ///
    /// Connection establishment is bounded by [`DEFAULT_ESTABLISHMENT_TIMEOUT`]
    /// (and [`DEFAULT_TCP_CONNECT_TIMEOUT`] per address); use
    /// [`builder()`](Self::builder) to adjust or opt out.
    pub fn plaintext() -> Self {
        Self::builder().plaintext()
    }

    /// Create a **plaintext** HTTP client with HTTP/2 prior-knowledge (h2c) only.
    ///
    /// Only for `http://` URIs. Errors at send-time on `https://`.
    /// For **TLS + HTTP/2-only** (e.g. gRPC over TLS), use
    /// [`Http2Connection::connect_tls`] instead — there is no TLS equivalent
    /// of this constructor.
    ///
    /// Uses HTTP/2 prior knowledge for cleartext connections. Required for
    /// gRPC over cleartext (gRPC mandates HTTP/2).
    ///
    /// **Note:** For gRPC, prefer [`SharedHttp2Connection`] over this —
    /// it has honest `poll_ready` and composes with `tower::balance`. This
    /// method pins you to one connection per host with no way to scale out.
    ///
    /// Connection establishment is bounded by [`DEFAULT_ESTABLISHMENT_TIMEOUT`]
    /// (and [`DEFAULT_TCP_CONNECT_TIMEOUT`] per address); use
    /// [`builder()`](Self::builder) to adjust or opt out.
    pub fn plaintext_http2_only() -> Self {
        Self::builder().plaintext_http2_only()
    }

    /// Create a **TLS** HTTP client. Only for `https://` URIs.
    ///
    /// Errors at send-time if given an `http://` URI — use
    /// [`plaintext`](Self::plaintext) for cleartext.
    ///
    /// ALPN is set to `["h2", "http/1.1"]` for HTTP/2-with-fallback
    /// auto-negotiation. TCP_NODELAY is enabled.
    ///
    /// # Certificate rotation
    ///
    /// The config may contain a custom `ResolvesClientCert` for dynamic
    /// cert rotation. `rustls::ClientConfig` stores the resolver as
    /// `Arc<dyn ResolvesClientCert>`, so when this function clones the
    /// config to set ALPN, the **same** resolver instance is shared — a
    /// background rotation task holding its own `Arc` continues working.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use connectrpc::client::HttpClient;
    /// use connectrpc::rustls;
    /// use std::sync::Arc;
    ///
    /// let tls_config = Arc::new(
    ///     rustls::ClientConfig::builder()
    ///         .with_root_certificates(roots)
    ///         .with_no_client_auth(),
    /// );
    ///
    /// let http = HttpClient::with_tls(tls_config);
    /// let client = GreetServiceClient::new(http, config);
    /// ```
    ///
    /// Connection establishment is bounded by [`DEFAULT_ESTABLISHMENT_TIMEOUT`]
    /// (and [`DEFAULT_TCP_CONNECT_TIMEOUT`] per address); use
    /// [`builder()`](Self::builder) to adjust or opt out.
    #[cfg(feature = "client-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client-tls")))]
    pub fn with_tls(tls_config: std::sync::Arc<rustls::ClientConfig>) -> Self {
        Self::builder().with_tls(tls_config)
    }
}

/// Builder for [`HttpClient`] connector-level options.
///
/// Use [`HttpClient::builder`] to obtain one. The terminal methods mirror the
/// associated constructors on `HttpClient`; the existing constructors delegate
/// here, so `HttpClient::plaintext()` is exactly `HttpClient::builder().plaintext()`.
#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
#[derive(Debug, Clone)]
#[must_use = "call a terminal (plaintext / plaintext_http2_only / with_tls) to build the client"]
pub struct HttpClientBuilder {
    tcp_connect_timeout: Option<Duration>,
    establishment_timeout: Option<Duration>,
}

#[cfg(feature = "client")]
impl Default for HttpClientBuilder {
    /// A fresh builder with [`DEFAULT_ESTABLISHMENT_TIMEOUT`] /
    /// [`DEFAULT_TCP_CONNECT_TIMEOUT`] applied, so a hung server cannot stall
    /// connection establishment indefinitely. Use
    /// [`no_establishment_timeout`](HttpClientBuilder::no_establishment_timeout) /
    /// [`no_tcp_connect_timeout`](HttpClientBuilder::no_tcp_connect_timeout) to
    /// opt out.
    fn default() -> Self {
        Self {
            tcp_connect_timeout: Some(DEFAULT_TCP_CONNECT_TIMEOUT),
            establishment_timeout: Some(DEFAULT_ESTABLISHMENT_TIMEOUT),
        }
    }
}

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
impl HttpClientBuilder {
    /// Bound the TCP connect phase.
    ///
    /// This is hyper's [`HttpConnector::set_connect_timeout`][hyper-ct],
    /// applied to the inner connector for all three transport flavours. It
    /// covers only the TCP `connect(2)` call (per resolved address — hyper
    /// divides the timeout evenly across the address set). It does **not**
    /// cover DNS resolution or, for [`with_tls`](Self::with_tls), the TLS
    /// handshake — set [`establishment_timeout`](Self::establishment_timeout) too to
    /// bound those. Use a per-request timeout (e.g.
    /// [`CallOptions::with_timeout`]) to bound DNS+connect+TLS+request as a
    /// whole.
    ///
    /// Defaults to [`DEFAULT_TCP_CONNECT_TIMEOUT`]. To disable, use
    /// [`no_tcp_connect_timeout`](Self::no_tcp_connect_timeout). Passing
    /// `Duration::ZERO` causes every per-address connect to fail immediately.
    ///
    /// [hyper-ct]: hyper_util::client::legacy::connect::HttpConnector::set_connect_timeout
    #[doc(alias = "connect_timeout")]
    pub fn tcp_connect_timeout(mut self, dur: Duration) -> Self {
        self.tcp_connect_timeout = http2::finite(dur);
        self
    }

    /// Alias for [`tcp_connect_timeout`](Self::tcp_connect_timeout).
    pub fn connect_timeout(self, dur: Duration) -> Self {
        self.tcp_connect_timeout(dur)
    }

    /// Disable the per-address TCP connect bound (the
    /// [`DEFAULT_TCP_CONNECT_TIMEOUT`] default). The whole-connector
    /// [`establishment_timeout`](Self::establishment_timeout) still applies.
    pub fn no_tcp_connect_timeout(mut self) -> Self {
        self.tcp_connect_timeout = None;
        self
    }

    /// Bound the whole connector establishment: DNS resolution, the TCP connect,
    /// and, for [`with_tls`](Self::with_tls), the TLS handshake.
    ///
    /// Unlike [`tcp_connect_timeout`](Self::tcp_connect_timeout) (which bounds only the
    /// per-address TCP `connect(2)` call), this is a single wall-clock bound on
    /// everything the connector does to produce a usable stream — so on the TLS
    /// transport the two bounds overlap on the TCP phase.
    ///
    /// # What it does and does not cover
    ///
    /// Because `HttpClient` pools connections through hyper's legacy client, the
    /// HTTP/2 preface runs *inside* the pool and is not separately observable
    /// here — this bound covers **DNS, TCP and TLS, not the h2 preface**.
    /// [`Http2Connection`]'s handshake bound additionally covers the h2
    /// preface. Use a per-request timeout (e.g.
    /// [`CallOptions::with_timeout`]) for a true end-to-end bound. For a
    /// transport that bounds the h2 preface too, use [`Http2Connection`].
    ///
    /// Exceeding this bound surfaces as a [`ConnectError`] with
    /// [`ErrorCode::Unavailable`] (the connect is retryable); the message names
    /// the establishment phase.
    ///
    /// Defaults to [`DEFAULT_ESTABLISHMENT_TIMEOUT`]. To disable, use
    /// [`no_establishment_timeout`](Self::no_establishment_timeout). Passing
    /// `Duration::ZERO` causes every establishment to fail immediately.
    pub fn establishment_timeout(mut self, dur: Duration) -> Self {
        self.establishment_timeout = http2::finite(dur);
        self
    }

    /// Disable the wall-clock connector-establishment bound (the
    /// [`DEFAULT_ESTABLISHMENT_TIMEOUT`] default). With both this and
    /// [`no_tcp_connect_timeout`](Self::no_tcp_connect_timeout), a hung server
    /// can stall connection establishment indefinitely — the pre-0.8.0
    /// behaviour.
    pub fn no_establishment_timeout(mut self) -> Self {
        self.establishment_timeout = None;
        self
    }

    fn http_connector(&self) -> hyper_util::client::legacy::connect::HttpConnector {
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_connect_timeout(self.tcp_connect_timeout);
        connector
    }

    /// Wrap a connector so `establishment_timeout` (if set) bounds its establishment.
    fn wrap<C>(&self, connector: C) -> TimeoutConnector<C> {
        TimeoutConnector {
            inner: connector,
            timeout: self.establishment_timeout,
        }
    }

    /// Finish building as a plaintext client. See [`HttpClient::plaintext`].
    #[must_use]
    pub fn plaintext(self) -> HttpClient {
        use hyper_util::client::legacy::Client;
        use hyper_util::rt::TokioExecutor;

        let connector = self.wrap(self.http_connector());
        let client = Client::builder(TokioExecutor::new()).build(connector);
        HttpClient {
            inner: HttpClientInner::Plain(client),
        }
    }

    /// Finish building as an h2c-only plaintext client. See
    /// [`HttpClient::plaintext_http2_only`].
    #[must_use]
    pub fn plaintext_http2_only(self) -> HttpClient {
        use hyper_util::client::legacy::Client;
        use hyper_util::rt::TokioExecutor;

        let connector = self.wrap(self.http_connector());
        let client = Client::builder(TokioExecutor::new())
            .http2_only(true)
            .build(connector);
        HttpClient {
            inner: HttpClientInner::Plain(client),
        }
    }

    /// Finish building as a TLS client. See [`HttpClient::with_tls`].
    #[cfg(feature = "client-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client-tls")))]
    #[must_use]
    pub fn with_tls(self, tls_config: std::sync::Arc<rustls::ClientConfig>) -> HttpClient {
        use hyper_util::client::legacy::Client;
        use hyper_util::rt::TokioExecutor;

        let mut http = self.http_connector();
        // HttpConnector rejects https:// by default; disable so the scheme
        // passes through to the HttpsConnector for TLS handling.
        http.enforce_http(false);

        // Clone config to set ALPN. The inner Arc<dyn ResolvesClientCert>
        // is shared through the clone — cert rotation is unaffected.
        // hyper-rustls's builder requires alpn_protocols to be EMPTY on
        // input (it sets them based on enable_http1/enable_http2), so if
        // the caller already set ALPN, clear it first.
        let mut cfg = (*tls_config).clone();
        cfg.alpn_protocols.clear();

        // Builder in https_only mode rejects http:// at the connector level
        // (force_https = true), backing up our send()-time scheme check.
        // enable_all_versions sets ALPN = [h2, http/1.1].
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(cfg)
            .https_only()
            .enable_all_versions()
            .wrap_connector(http);

        let connector = self.wrap(https);
        let client = Client::builder(TokioExecutor::new()).build(connector);
        HttpClient {
            inner: HttpClientInner::Tls(client),
        }
    }
}

// No `Default` impl for HttpClient — there's no sensible default when the
// choice between plaintext and TLS is security-relevant. Users must
// explicitly choose plaintext() or with_tls().

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
impl ClientTransport for HttpClient {
    type ResponseBody = hyper::body::Incoming;
    type Error = ConnectError;

    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
        let scheme = request.uri().scheme_str();

        match &self.inner {
            HttpClientInner::Plain(client) => {
                // Plaintext variant: reject https:// to prevent accidental
                // cleartext connections to TLS endpoints.
                if scheme == Some("https") {
                    return Box::pin(async {
                        Err(ConnectError::invalid_argument(
                            "HttpClient::plaintext() received https:// URI; \
                             use HttpClient::with_tls for TLS",
                        ))
                    });
                }
                let client = client.clone();
                Box::pin(async move {
                    client
                        .request(request)
                        .await
                        .map_err(|e| ConnectError::unavailable(format!("HTTP request failed: {e}")))
                })
            }
            #[cfg(feature = "client-tls")]
            HttpClientInner::Tls(client) => {
                // TLS variant: reject http:// — user explicitly chose TLS,
                // silently falling back to cleartext is a security footgun.
                if scheme == Some("http") {
                    return Box::pin(async {
                        Err(ConnectError::invalid_argument(
                            "HttpClient::with_tls() received http:// URI; \
                             use HttpClient::plaintext for cleartext",
                        ))
                    });
                }
                let client = client.clone();
                Box::pin(async move {
                    client.request(request).await.map_err(|e| {
                        ConnectError::unavailable(format!("HTTPS request failed: {e}"))
                    })
                })
            }
        }
    }
}

/// Configuration for a ConnectRPC client.
///
/// Construct with [`ClientConfig::new`] and the `with_*` builder methods,
/// then read settings back through the accessor methods of the same name:
///
/// ```rust
/// use connectrpc::client::ClientConfig;
/// use connectrpc::Protocol;
///
/// let config = ClientConfig::new("http://localhost:8080".parse().unwrap())
///     .with_protocol(Protocol::Grpc);
/// assert_eq!(config.protocol(), Protocol::Grpc);
/// ```
///
/// `ClientConfig` is `#[non_exhaustive]`: new fields may be added in minor
/// releases. Struct-literal and functional-update construction are not
/// available outside the crate; use the builder methods.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ClientConfig {
    pub(crate) base_uri: Uri,
    pub(crate) protocol: Protocol,
    pub(crate) codec_format: CodecFormat,
    pub(crate) compression: CompressionRegistry,
    pub(crate) request_compression: Option<String>,
    pub(crate) compression_policy: CompressionPolicy,
    pub(crate) default_timeout: Option<Duration>,
    pub(crate) default_max_message_size: Option<usize>,
    pub(crate) default_headers: http::HeaderMap,
}

impl ClientConfig {
    /// Create a new client configuration with the given base URI.
    ///
    /// Uses Connect protocol with protobuf encoding by default.
    pub fn new(base_uri: Uri) -> Self {
        Self {
            base_uri,
            protocol: Protocol::Connect,
            codec_format: CodecFormat::Proto,
            compression: CompressionRegistry::default(),
            request_compression: None,
            compression_policy: CompressionPolicy::default(),
            default_timeout: None,
            default_max_message_size: None,
            default_headers: http::HeaderMap::new(),
        }
    }

    // ---- builders ---------------------------------------------------------

    /// Set the wire protocol (Connect, gRPC, or gRPC-Web).
    ///
    /// Read via [`Self::protocol`].
    #[must_use]
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Set the codec format (proto or json).
    ///
    /// Read via [`Self::codec_format`].
    ///
    /// In a proto-only build (the `json` feature disabled) selecting
    /// [`CodecFormat::Json`] produces a client whose every RPC returns
    /// [`Unimplemented`](crate::ErrorCode::Unimplemented) before any
    /// network I/O — the JSON codec is not compiled in. Prefer the default
    /// [`CodecFormat::Proto`]; the [`json`](Self::json) shorthand is removed
    /// from the API entirely in that build.
    #[must_use]
    pub fn with_codec_format(mut self, format: CodecFormat) -> Self {
        self.codec_format = format;
        self
    }

    /// Use JSON encoding. Shorthand for `with_codec_format(CodecFormat::Json)`.
    ///
    /// Only available when the `json` feature is enabled; a proto-only build
    /// omits it so JSON cannot be selected through this shorthand.
    #[cfg(feature = "json")]
    #[cfg_attr(docsrs, doc(cfg(feature = "json")))]
    #[must_use]
    pub fn json(mut self) -> Self {
        self.codec_format = CodecFormat::Json;
        self
    }

    /// Use protobuf encoding. Shorthand for `with_codec_format(CodecFormat::Proto)`.
    #[must_use]
    pub fn proto(mut self) -> Self {
        self.codec_format = CodecFormat::Proto;
        self
    }

    /// Set the compression registry.
    ///
    /// Read via [`Self::compression`].
    #[must_use]
    pub fn with_compression(mut self, registry: CompressionRegistry) -> Self {
        self.compression = registry;
        self
    }

    /// Enable request compression with the specified encoding.
    ///
    /// Read via [`Self::request_compression`].
    #[must_use]
    pub fn compress_requests(mut self, encoding: impl Into<String>) -> Self {
        self.request_compression = Some(encoding.into());
        self
    }

    /// Set the compression policy.
    ///
    /// Read via [`Self::compression_policy`].
    #[must_use]
    pub fn with_compression_policy(mut self, policy: CompressionPolicy) -> Self {
        self.compression_policy = policy;
        self
    }

    /// Set a default request timeout for all calls through this config.
    ///
    /// Read via [`Self::default_timeout`]. Per-call
    /// [`CallOptions::with_timeout`] overrides this.
    #[must_use]
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }

    /// Set a default maximum decompressed response message size.
    ///
    /// Read via [`Self::default_max_message_size`]. Per-call
    /// [`CallOptions::with_max_message_size`] overrides this.
    #[must_use]
    pub fn with_default_max_message_size(mut self, size: usize) -> Self {
        self.default_max_message_size = Some(size);
        self
    }

    /// Add a default header applied to every request through this config.
    ///
    /// If the name or value cannot be converted to valid HTTP header components,
    /// the header is silently ignored. Per-call [`CallOptions::with_header`]
    /// entries with the same name **replace** this value (options win over
    /// config defaults).
    ///
    /// Read via [`Self::default_headers`].
    #[must_use]
    pub fn with_default_header(
        mut self,
        name: impl TryInto<http::header::HeaderName>,
        value: impl TryInto<http::header::HeaderValue>,
    ) -> Self {
        if let (Ok(name), Ok(value)) = (name.try_into(), value.try_into()) {
            self.default_headers.append(name, value);
        }
        self
    }

    /// Set all default headers at once (replaces any prior default headers).
    ///
    /// Read via [`Self::default_headers`].
    #[must_use]
    pub fn with_default_headers(mut self, headers: http::HeaderMap) -> Self {
        self.default_headers = headers;
        self
    }

    // ---- accessors --------------------------------------------------------

    /// The base URI for the service (e.g., `http://localhost:8080`).
    ///
    /// Set via [`Self::new`].
    pub fn base_uri(&self) -> &Uri {
        &self.base_uri
    }

    /// The wire protocol (Connect, gRPC, or gRPC-Web).
    ///
    /// Set via [`Self::with_protocol`].
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// The codec format (proto or json).
    ///
    /// Set via [`Self::with_codec_format`], [`Self::json`], or [`Self::proto`].
    pub fn codec_format(&self) -> CodecFormat {
        self.codec_format
    }

    /// The compression registry used for request/response compression.
    ///
    /// Set via [`Self::with_compression`].
    pub fn compression(&self) -> &CompressionRegistry {
        &self.compression
    }

    /// The request compression encoding (e.g., `"gzip"`), if enabled.
    ///
    /// Set via [`Self::compress_requests`].
    pub fn request_compression(&self) -> Option<&str> {
        self.request_compression.as_deref()
    }

    /// The compression policy controlling when messages are compressed.
    ///
    /// Set via [`Self::with_compression_policy`].
    pub fn compression_policy(&self) -> CompressionPolicy {
        self.compression_policy
    }

    /// The default request timeout for all calls through this config, if set.
    ///
    /// Set via [`Self::with_default_timeout`]. Per-call
    /// [`CallOptions::with_timeout`] overrides this when set.
    pub fn default_timeout(&self) -> Option<Duration> {
        self.default_timeout
    }

    /// The default maximum decompressed response message size, if set.
    ///
    /// Set via [`Self::with_default_max_message_size`]. Per-call
    /// [`CallOptions::with_max_message_size`] overrides this when set.
    pub fn default_max_message_size(&self) -> Option<usize> {
        self.default_max_message_size
    }

    /// The headers applied to every request through this config.
    ///
    /// Useful for auth tokens, user-agent, tracing context.
    /// Per-call [`CallOptions::with_header`] entries with the same name
    /// **replace** these (options win over config defaults).
    ///
    /// Set via [`Self::with_default_header`] / [`Self::with_default_headers`].
    pub fn default_headers(&self) -> &http::HeaderMap {
        &self.default_headers
    }
}

/// Per-request options for an RPC call.
///
/// Provides per-call configuration such as additional headers and timeouts.
/// Use [`CallOptions::default()`] for no additional options.
///
/// `CallOptions` is `#[non_exhaustive]`: new fields may be added in minor
/// releases. Construct with [`CallOptions::default()`] and the `with_*`
/// builder methods, then read settings back through the accessor methods.
///
/// # Example
///
/// ```rust
/// use connectrpc::client::CallOptions;
/// use std::time::Duration;
///
/// let options = CallOptions::default()
///     .with_timeout(Duration::from_secs(5))
///     .with_header("x-request-id", "abc123");
/// assert_eq!(options.timeout(), Some(Duration::from_secs(5)));
/// assert_eq!(options.headers().get("x-request-id").unwrap(), "abc123");
/// ```
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CallOptions {
    pub(crate) headers: http::HeaderMap,
    pub(crate) timeout: Option<Duration>,
    pub(crate) max_message_size: Option<usize>,
    pub(crate) compress: Option<bool>,
}

impl CallOptions {
    // ---- builders ---------------------------------------------------------

    /// Set the request timeout.
    ///
    /// Read via [`Self::timeout`].
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Add a request header.
    ///
    /// If the name or value cannot be converted to valid HTTP header components,
    /// the header is silently ignored. Use [`try_with_header`](Self::try_with_header)
    /// for fallible insertion.
    ///
    /// Read via [`Self::headers`].
    #[must_use]
    pub fn with_header(
        mut self,
        name: impl TryInto<http::header::HeaderName>,
        value: impl TryInto<http::header::HeaderValue>,
    ) -> Self {
        if let (Ok(name), Ok(value)) = (name.try_into(), value.try_into()) {
            self.headers.append(name, value);
        }
        self
    }

    /// Add a request header, returning an error if the name or value is invalid.
    ///
    /// Read via [`Self::headers`].
    ///
    /// # Errors
    ///
    /// Returns [`ErrorCode::Internal`] if the name or value cannot be
    /// converted to a valid HTTP header component.
    pub fn try_with_header(
        mut self,
        name: impl TryInto<http::header::HeaderName>,
        value: impl TryInto<http::header::HeaderValue>,
    ) -> Result<Self, ConnectError> {
        let name = name
            .try_into()
            .map_err(|_| ConnectError::internal("invalid header name"))?;
        let value = value
            .try_into()
            .map_err(|_| ConnectError::internal("invalid header value"))?;
        self.headers.append(name, value);
        Ok(self)
    }

    /// Add multiple request headers from an iterator.
    ///
    /// Read via [`Self::headers`].
    #[must_use]
    pub fn with_headers(
        mut self,
        headers: impl IntoIterator<Item = (http::header::HeaderName, http::header::HeaderValue)>,
    ) -> Self {
        for (name, value) in headers {
            self.headers.append(name, value);
        }
        self
    }

    /// Set the maximum decompressed message size in bytes.
    ///
    /// Read via [`Self::max_message_size`].
    #[must_use]
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Override compression for this call. `Some(true)` forces compression,
    /// `Some(false)` disables it; not calling this defers to the configured
    /// [`CompressionPolicy`].
    ///
    /// Read via [`Self::compress`].
    #[must_use]
    pub fn with_compress(mut self, enabled: bool) -> Self {
        self.compress = Some(enabled);
        self
    }

    // ---- accessors --------------------------------------------------------

    /// Additional headers to include in the request.
    ///
    /// These are merged into the HTTP request after protocol headers,
    /// allowing override of any header for advanced use cases.
    ///
    /// Set via [`Self::with_header`] / [`Self::with_headers`].
    pub fn headers(&self) -> &http::HeaderMap {
        &self.headers
    }

    /// The request timeout, sent as `connect-timeout-ms` / `grpc-timeout`.
    ///
    /// Set via [`Self::with_timeout`].
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// The maximum decompressed message size in bytes.
    ///
    /// When set, messages exceeding this size after decompression will
    /// result in a `ResourceExhausted` error. Applies per-message for streaming.
    ///
    /// Set via [`Self::with_max_message_size`].
    pub fn max_message_size(&self) -> Option<usize> {
        self.max_message_size
    }

    /// The per-call compression override. `Some(true)` forces compression,
    /// `Some(false)` disables it, `None` defers to the policy.
    ///
    /// Set via [`Self::with_compress`].
    pub fn compress(&self) -> Option<bool> {
        self.compress
    }
}

/// Merge `options` over `config` defaults: where `options` has a value, use it;
/// where `options` is unset/empty, use the config default.
///
/// Headers: config defaults are applied first, then options. For any header
/// name present in `options`, the config's values for that name are removed
/// and replaced with the options' values (options override config).
///
/// `compress` has no config-level default — [`ClientConfig::compression_policy`]
/// already provides that control at a more appropriate granularity.
fn effective_options(config: &ClientConfig, options: CallOptions) -> CallOptions {
    CallOptions {
        timeout: options.timeout.or(config.default_timeout),
        max_message_size: options.max_message_size.or(config.default_max_message_size),
        compress: options.compress,
        headers: merge_headers(&config.default_headers, options.headers),
    }
}

/// Merge headers: config defaults, then options override.
///
/// For each header name present in `options`, all config values for that
/// name are removed before appending the options' values. This ensures
/// per-call options fully replace config defaults for that header name
/// (no duplicate values leaking through).
fn merge_headers(config_defaults: &http::HeaderMap, options: http::HeaderMap) -> http::HeaderMap {
    // Fast path: no config defaults → just use options as-is (most common case).
    if config_defaults.is_empty() {
        return options;
    }
    // Fast path: no options → clone config defaults.
    if options.is_empty() {
        return config_defaults.clone();
    }

    let mut merged = config_defaults.clone();
    // For each name in options, remove ALL config entries for that name, then
    // append all options' values. keys() deduplicates so remove runs once/name.
    for name in options.keys() {
        merged.remove(name);
    }
    for (name, value) in options.iter() {
        merged.append(name.clone(), value.clone());
    }
    merged
}

const CONNECT_TIMEOUT_MAX_MILLIS: u64 = 9_999_999_999;
const GRPC_TIMEOUT_MAX_SECONDS: u64 = 99_999_999;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EncodedTimeout {
    Connect {
        millis: u64,
    },
    Grpc {
        value: u64,
        unit: char,
        duration: Duration,
    },
}

impl EncodedTimeout {
    fn duration(self) -> Duration {
        match self {
            Self::Connect { millis } => Duration::from_millis(millis),
            Self::Grpc { duration, .. } => duration,
        }
    }

    fn header_value(self) -> String {
        match self {
            Self::Connect { millis } => millis.to_string(),
            Self::Grpc { value, unit, .. } => format!("{value}{unit}"),
        }
    }
}

fn grpc_encoded_timeout(value: u128, unit: char, duration: Duration) -> EncodedTimeout {
    EncodedTimeout::Grpc {
        value: value as u64,
        unit,
        duration,
    }
}

/// Encode a timeout for the wire and retain the exact duration that encoding
/// represents so local deadline enforcement matches the transmitted budget.
#[allow(clippy::manual_is_multiple_of)]
fn encoded_timeout(timeout: Duration, protocol: Protocol) -> EncodedTimeout {
    match protocol {
        Protocol::Connect => EncodedTimeout::Connect {
            millis: timeout.as_millis().min(CONNECT_TIMEOUT_MAX_MILLIS as u128) as u64,
        },
        Protocol::Grpc | Protocol::GrpcWeb => {
            let max = GRPC_TIMEOUT_MAX_SECONDS as u128;
            let nanos = timeout.as_nanos();
            let secs = timeout.as_secs() as u128;
            let millis = timeout.as_millis();
            let micros = timeout.as_micros();

            if nanos == 0 {
                grpc_encoded_timeout(0, 'n', Duration::ZERO)
            } else if nanos % 1_000_000_000 == 0 && secs <= max {
                grpc_encoded_timeout(secs, 'S', Duration::from_secs(secs as u64))
            } else if nanos % 1_000_000 == 0 && millis <= max {
                grpc_encoded_timeout(millis, 'm', Duration::from_millis(millis as u64))
            } else if nanos % 1_000 == 0 && micros <= max {
                grpc_encoded_timeout(micros, 'u', Duration::from_micros(micros as u64))
            } else if nanos <= max {
                grpc_encoded_timeout(nanos, 'n', Duration::from_nanos(nanos as u64))
            } else if micros <= max {
                grpc_encoded_timeout(micros, 'u', Duration::from_micros(micros as u64))
            } else if millis <= max {
                grpc_encoded_timeout(millis, 'm', Duration::from_millis(millis as u64))
            } else if secs <= max {
                grpc_encoded_timeout(secs, 'S', Duration::from_secs(secs as u64))
            } else {
                grpc_encoded_timeout(max, 'S', Duration::from_secs(GRPC_TIMEOUT_MAX_SECONDS))
            }
        }
    }
}

fn client_deadline(timeout: Option<Duration>, protocol: Protocol) -> Option<std::time::Instant> {
    timeout
        .map(|t| encoded_timeout(t, protocol).duration())
        .and_then(|t| std::time::Instant::now().checked_add(t))
}

/// Enforce a client-side deadline by wrapping a future in `timeout_at`.
///
/// gRPC deadline semantics: the deadline applies to the **entire call** from
/// start to completion, not per-message (grpc-java #4814, confirmed by
/// maintainers). When the deadline fires, all subsequent operations on the
/// call return `DEADLINE_EXCEEDED` — matching grpc-java's `onError` behavior
/// and connect-go's `ctx.Err()` check before each body read.
///
/// Returns the future's result if it completes before deadline, or
/// `Err(ConnectError::deadline_exceeded)` if the deadline fires first. If
/// `deadline` is `None`, the future runs unbounded.
async fn with_deadline<F, T>(
    deadline: Option<std::time::Instant>,
    fut: F,
) -> Result<T, ConnectError>
where
    F: Future<Output = Result<T, ConnectError>>,
{
    match deadline {
        None => fut.await,
        Some(d) => {
            // std::time::Instant → tokio::time::Instant (tokio's timer needs its own type).
            let tokio_deadline = tokio::time::Instant::from_std(d);
            tokio::time::timeout_at(tokio_deadline, fut)
                .await
                .map_err(|_| ConnectError::deadline_exceeded("client-side deadline exceeded"))?
        }
    }
}

/// Response from a unary RPC call.
///
/// Contains the decoded response message along with response headers and
/// trailing metadata.
#[derive(Debug)]
pub struct UnaryResponse<Resp> {
    headers: http::HeaderMap,
    body: Resp,
    trailers: http::HeaderMap,
}

impl<Resp> UnaryResponse<Resp> {
    /// Returns the response headers.
    #[must_use]
    pub fn headers(&self) -> &http::HeaderMap {
        &self.headers
    }

    /// Consume the response, returning just the body.
    ///
    /// For generated clients this is an [`OwnedView`] — zero-copy, move
    /// semantics, suitable for keeping the decoded body around without
    /// copying. Field access on it goes through
    /// [`reborrow()`](OwnedView::reborrow); for inline reads prefer
    /// [`view()`](Self::view), and for an owned struct use
    /// [`into_owned()`](Self::into_owned).
    #[must_use]
    pub fn into_view(self) -> Resp {
        self.body
    }

    /// Returns the trailing metadata.
    #[must_use]
    pub fn trailers(&self) -> &http::HeaderMap {
        &self.trailers
    }

    /// Consume the response, returning `(headers, body, trailers)`.
    #[must_use]
    pub fn into_parts(self) -> (http::HeaderMap, Resp, http::HeaderMap) {
        (self.headers, self.body, self.trailers)
    }
}

/// Convenience for the common generated-client case where the body is an
/// [`OwnedView`]. Generated unary client methods always return this shape.
impl<V> UnaryResponse<OwnedView<V>>
where
    V: MessageView<'static>,
{
    /// Consume the response and return the fully-owned message, discarding
    /// headers and trailers.
    ///
    /// This allocates and copies all borrowed fields (strings, bytes, nested
    /// messages). Prefer zero-copy view access via
    /// [`view()`](UnaryResponse::view) unless you need to pass the owned
    /// struct to code that expects it, or store it in a collection.
    ///
    /// ```rust,ignore
    /// let owned: FooResponse = client.foo(req).await?.into_owned();
    /// ```
    ///
    /// Infallible — see [`into_owned_parts()`](Self::into_owned_parts) for
    /// the argument.
    #[must_use]
    pub fn into_owned(self) -> V::Owned {
        self.into_owned_parts().1
    }

    /// Consume the response, returning `(headers, owned message, trailers)`.
    ///
    /// The metadata-preserving sibling of [`into_owned()`](Self::into_owned),
    /// for callers that also need the response's header and trailer
    /// metadata.
    ///
    /// Infallible: [`OwnedView::to_owned_message`] cannot fail, because an
    /// `OwnedView` can only come from buffa's wire decoder and conversion
    /// replays under the budget the decode already charged.
    #[must_use]
    pub fn into_owned_parts(self) -> (http::HeaderMap, V::Owned, http::HeaderMap) {
        (self.headers, self.body.to_owned_message(), self.trailers)
    }
}

/// Zero-copy read access for [`OwnedView`] bodies whose view supports
/// reborrowing (every buffa-generated view does).
impl<V> UnaryResponse<OwnedView<V>>
where
    V: ViewReborrow,
{
    /// Borrow the response message view, tied to `&self`.
    ///
    /// Field access on the returned view is zero-copy:
    ///
    /// ```rust,ignore
    /// let resp = client.foo(req).await?;
    /// assert_eq!(resp.view().name, "expected");  // &str, no allocation
    /// ```
    ///
    /// See also [`into_view()`](UnaryResponse::into_view) to keep the decoded
    /// body and [`into_owned()`](UnaryResponse::into_owned) for an owned
    /// struct.
    #[must_use]
    pub fn view(&self) -> &V::Reborrowed<'_> {
        self.body.reborrow()
    }
}

/// Decode a response message as an `OwnedView` from bytes.
///
/// For proto-encoded responses, this is a true zero-copy decode — the view borrows
/// directly from the response bytes. For JSON-encoded responses, the data is first
/// deserialized to an owned message, then re-encoded to proto bytes and decoded as
/// a view. This JSON round-trip adds overhead relative to owned-type decoding, but
/// is negligible compared to JSON parsing itself.
fn decode_response_view<RespView>(
    data: Bytes,
    format: CodecFormat,
) -> Result<OwnedView<RespView>, ConnectError>
where
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    match format {
        CodecFormat::Proto => OwnedView::<RespView>::decode(data)
            .map_err(|e| ConnectError::internal(format!("failed to decode response: {e}"))),
        #[cfg(feature = "json")]
        CodecFormat::Json => {
            let owned: RespView::Owned = serde_json::from_slice(&data).map_err(|e| {
                ConnectError::internal(format!("failed to decode JSON response: {e}"))
            })?;
            OwnedView::<RespView>::from_owned(&owned)
                .map_err(|e| ConnectError::internal(format!("failed to re-encode for view: {e}")))
        }
        #[cfg(not(feature = "json"))]
        CodecFormat::Json => Err(ConnectError::unimplemented(
            crate::codec::JSON_FEATURE_DISABLED,
        )),
    }
}

/// Make a unary RPC call.
///
/// This is the core function used by generated clients to make RPC calls.
/// It handles encoding, compression, and protocol details.
pub async fn call_unary<T, Req, RespView>(
    transport: &T,
    config: &ClientConfig,
    service: &str,
    method: &str,
    request: Req,
    options: CallOptions,
) -> Result<UnaryResponse<OwnedView<RespView>>, ConnectError>
where
    T: ClientTransport,
    <T::ResponseBody as Body>::Error: std::fmt::Display,
    Req: buffa::Message + crate::codec::JsonSerialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    let options = effective_options(config, options);

    // Build the full URI from base_uri and service/method path
    let base_str = config.base_uri.to_string();
    let base_str = base_str.trim_end_matches('/');
    let full_uri = format!("{base_str}/{service}/{method}");
    let uri: Uri = full_uri
        .parse()
        .map_err(|e| ConnectError::internal(format!("invalid URI: {e}")))?;

    // Encode the request body
    let body = match config.codec_format {
        CodecFormat::Proto => request.encode_to_bytes(),
        CodecFormat::Json => encode_json(&request)?,
    };

    // Apply compression and framing based on protocol.
    // Connect unary: compression at HTTP level (Content-Encoding) — the
    //   header must only be set when the body is ACTUALLY compressed (bug
    //   if the compression policy skips small messages but we still send
    //   Content-Encoding).
    // gRPC/gRPC-Web: compression at envelope level — the `grpc-encoding`
    //   header declares the algorithm used WHEN the per-message envelope
    //   flag is set, so it's fine to send even if the policy decides not
    //   to compress a particular message.
    let (body, applied_content_encoding) = match config.protocol {
        Protocol::Grpc | Protocol::GrpcWeb => {
            let compression_for_encoder = config.request_compression.as_ref().map(|enc| {
                (
                    std::sync::Arc::new(config.compression.clone()),
                    enc.as_str(),
                )
            });
            let mut encoder = crate::envelope::EnvelopeEncoder::new(
                compression_for_encoder,
                config.compression_policy.with_override(options.compress),
            );
            let mut buf = bytes::BytesMut::new();
            tokio_util::codec::Encoder::encode(&mut encoder, body, &mut buf)
                .map_err(|e| ConnectError::internal(format!("envelope encode failed: {e}")))?;
            (buf.freeze(), None)
        }
        Protocol::Connect => {
            if let Some(ref encoding) = config.request_compression {
                let effective_policy = config.compression_policy.with_override(options.compress);
                if effective_policy.should_compress(body.len()) {
                    let compressed = config.compression.compress(encoding, &body)?;
                    (compressed, Some(encoding.as_str()))
                } else {
                    (body, None)
                }
            } else {
                (body, None)
            }
        }
    };

    // Compute deadline BEFORE sending the request, matching how Go's
    // ctx.Deadline() works. The server enforces the same deadline via
    // grpc-timeout, so by the time we check, the elapsed time since
    // request start is what matters.
    let deadline = client_deadline(options.timeout, config.protocol);

    // Build the HTTP request with protocol-aware headers
    let mut builder = Request::builder().method(http::Method::POST).uri(uri);
    builder = add_unary_request_headers(builder, config, options.timeout, applied_content_encoding);

    // Merge user-provided headers (last, so they can override anything)
    let headers = builder.headers_mut().unwrap();
    for (name, value) in &options.headers {
        headers.append(name.clone(), value.clone());
    }

    let http_request = builder
        .body(full_body(body))
        .map_err(|e| ConnectError::internal(format!("failed to build request: {e}")))?;

    // Enforce client-side deadline on send + parse. The server also
    // enforces via grpc-timeout/connect-timeout-ms header, but a hung or
    // misbehaving server shouldn't block the client indefinitely.
    with_deadline(deadline, async {
        let response = transport
            .send(http_request)
            .await
            .map_err(|e| map_transport_send_error(e, "request failed"))?;

        match config.protocol {
            Protocol::Connect => parse_connect_unary_response(response, config, &options).await,
            Protocol::Grpc | Protocol::GrpcWeb => {
                parse_grpc_unary_response(response, config, &options, deadline).await
            }
        }
    })
    .await
}

/// Make an idempotent unary RPC call via HTTP GET (Connect protocol only).
///
/// The request is encoded into URL query parameters per the Connect spec:
/// `?connect=v1[&base64=1][&compression=<enc>]&encoding=<codec>&message=<payload>`.
///
/// For proto (or any binary codec), the message is URL-safe base64-encoded
/// without padding and `base64=1` is set. For JSON, the message is
/// percent-encoded directly (no base64). Compression adds `compression=`
/// and always uses base64 (compressed bytes are binary).
///
/// GET requests are cacheable by browsers/proxies/CDNs — useful for
/// side-effect-free queries. Only the Connect protocol supports this;
/// gRPC/gRPC-Web are POST-only.
///
/// # Deterministic encoding
///
/// For effective caching, the encoded message should be deterministic (same
/// domain object → same bytes). buffa's proto encoder is deterministic
/// (fields walked in field-number order). serde_json is NOT guaranteed
/// deterministic — if you need JSON + caching, consider a custom serializer.
///
/// # Errors
///
/// Returns `invalid_argument` if `config.protocol` is not `Connect`.
pub async fn call_unary_get<T, Req, RespView>(
    transport: &T,
    config: &ClientConfig,
    service: &str,
    method: &str,
    request: Req,
    options: CallOptions,
) -> Result<UnaryResponse<OwnedView<RespView>>, ConnectError>
where
    T: ClientTransport,
    <T::ResponseBody as Body>::Error: std::fmt::Display,
    Req: buffa::Message + crate::codec::JsonSerialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    // Connect GET is a Connect-protocol-only feature.
    if !matches!(config.protocol, Protocol::Connect) {
        return Err(ConnectError::invalid_argument(
            "call_unary_get requires Protocol::Connect (gRPC/gRPC-Web are POST-only)",
        ));
    }

    let options = effective_options(config, options);

    // Build the base URI (no query yet)
    let base_str = config.base_uri.to_string();
    let base_str = base_str.trim_end_matches('/');

    // Encode the request body
    let body = match config.codec_format {
        CodecFormat::Proto => request.encode_to_bytes(),
        CodecFormat::Json => encode_json(&request)?,
    };

    // Apply compression if configured (compression makes base64 mandatory).
    let (payload, compressed_with) = if let Some(ref encoding) = config.request_compression {
        let effective_policy = config.compression_policy.with_override(options.compress);
        if effective_policy.should_compress(body.len()) {
            let compressed = config.compression.compress(encoding, &body)?;
            (compressed, Some(encoding.as_str()))
        } else {
            (body, None)
        }
    } else {
        (body, None)
    };

    // Build the query string. Per spec:
    // - proto/binary OR compressed → URL-safe base64 (no padding) + base64=1
    // - uncompressed JSON → percent-encode directly (it's UTF-8 text)
    let is_binary_codec = matches!(config.codec_format, CodecFormat::Proto);
    let use_base64 = is_binary_codec || compressed_with.is_some();

    let encoded_message = if use_base64 {
        // RFC 4648 §5 URL-safe base64, no padding (matching connect-go's
        // base64.RawURLEncoding.EncodeToString).
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&payload)
    } else {
        // Percent-encode the JSON bytes directly. It's valid UTF-8 by
        // construction (serde_json::to_vec produces UTF-8). Use a
        // conservative encode set — query component.
        percent_encoding::percent_encode(&payload, percent_encoding::NON_ALPHANUMERIC).to_string()
    };

    let encoding_name = match config.codec_format {
        CodecFormat::Proto => "proto",
        CodecFormat::Json => "json",
    };

    let query =
        build_connect_get_query(use_base64, compressed_with, encoding_name, &encoded_message);

    let full_uri = format!("{base_str}/{service}/{method}?{query}");
    let uri: Uri = full_uri
        .parse()
        .map_err(|e| ConnectError::internal(format!("invalid GET URI: {e}")))?;

    let deadline = client_deadline(options.timeout, Protocol::Connect);

    // GET request: no body, no Content-Type, no Content-Encoding.
    // Timeout still goes in the header (spec: "timeouts, if specified,
    // remain specified using HTTP headers rather than query parameters").
    let mut builder = Request::builder().method(http::Method::GET).uri(uri);
    if let Some(timeout) = options.timeout {
        builder = builder.header(
            crate::codec::header::TIMEOUT_MS,
            format_timeout(timeout, Protocol::Connect),
        );
    }
    // Accept-Encoding so the server can compress the response.
    let accept = config.compression.accept_encoding_header();
    if !accept.is_empty() {
        builder = builder.header(http::header::ACCEPT_ENCODING, accept);
    }

    // Merge user-provided headers
    let headers = builder.headers_mut().unwrap();
    for (name, value) in &options.headers {
        headers.append(name.clone(), value.clone());
    }

    let http_request = builder
        .body(full_body(Bytes::new()))
        .map_err(|e| ConnectError::internal(format!("failed to build GET request: {e}")))?;

    with_deadline(deadline, async {
        let response = transport
            .send(http_request)
            .await
            .map_err(|e| map_transport_send_error(e, "GET request failed"))?;

        // Response format is identical to POST unary Connect.
        parse_connect_unary_response(response, config, &options).await
    })
    .await
}

/// Assemble the Connect Unary-Get query string.
///
/// Servers must accept any parameter order; the spec's Query-Get ABNF rule
/// fixes the order so the variable-length `message` comes last and the
/// prefix is stable for shared HTTP caches: `connect`, `base64`,
/// `compression`, `encoding`, `message` ("Clients should order parameters as
/// shown in the Query-Get rule above to maximize hit rates on shared
/// caches" — <https://connectrpc.com/docs/protocol#unary-get-request>).
/// connect-go and the conformance reference-server order check both follow
/// this rule.
fn build_connect_get_query(
    use_base64: bool,
    compression: Option<&str>,
    encoding: &str,
    encoded_message: &str,
) -> String {
    let mut query = String::with_capacity(
        "connect=v1&encoding=&message=".len()
            + if use_base64 { "&base64=1".len() } else { 0 }
            + compression.map_or(0, |c| "&compression=".len() + c.len())
            + encoding.len()
            + encoded_message.len(),
    );
    query.push_str("connect=v1");
    if use_base64 {
        query.push_str("&base64=1");
    }
    if let Some(enc) = compression {
        query.push_str("&compression=");
        query.push_str(enc);
    }
    query.push_str("&encoding=");
    query.push_str(encoding);
    query.push_str("&message=");
    query.push_str(encoded_message);
    query
}

/// Remap decompression error codes for payloads received from the server.
///
/// The compression providers classify malformed input as `invalid_argument`
/// and unknown encodings as `unimplemented`, which is the right attribution
/// when a server decompresses a request. On the client the payload is a
/// response, so the fault lies with the server (or an intermediary), not
/// with the caller:
///
/// - `unimplemented` (unknown encoding) becomes `internal`: the server
///   chose an encoding the client never advertised.
/// - `invalid_argument` (malformed payload) becomes `data_loss`: the bytes
///   arrived but were corrupt. This deliberately diverges from connect-go,
///   which reports `invalid_argument` in both directions; `data_loss`
///   describes the failure without implying the request was at fault.
fn map_response_decompression_error(mut e: ConnectError) -> ConnectError {
    match e.code {
        ErrorCode::Unimplemented => e.code = ErrorCode::Internal,
        ErrorCode::InvalidArgument => e.code = ErrorCode::DataLoss,
        _ => {}
    }
    e
}

/// Parse a Connect protocol unary response.
async fn parse_connect_unary_response<B, RespView>(
    response: Response<B>,
    config: &ClientConfig,
    options: &CallOptions,
) -> Result<UnaryResponse<OwnedView<RespView>>, ConnectError>
where
    B: Body<Data = Bytes> + Send,
    B::Error: std::fmt::Display,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    let status = response.status();
    if !status.is_success() {
        let response_headers = response.headers().clone();
        let mut trailers = http::HeaderMap::new();
        let mut headers = http::HeaderMap::new();
        for (name, value) in &response_headers {
            if let Some(trailer_name) = name.as_str().strip_prefix("trailer-") {
                if let Ok(name) = http::header::HeaderName::from_bytes(trailer_name.as_bytes()) {
                    trailers.append(name, value.clone());
                }
            } else {
                headers.append(name.clone(), value.clone());
            }
        }

        let error_encoding = response_headers
            .get(http::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        let max_err_body_size = options
            .max_message_size
            .unwrap_or(crate::service::DEFAULT_MAX_MESSAGE_SIZE);

        let body = collect_body_bounded(response.into_body(), max_err_body_size)
            .await
            .map_err(|mut e| {
                e.set_response_headers(headers.clone());
                e.set_trailers(trailers.clone());
                e
            })?;

        // Decompress if the server set Content-Encoding. If decompression
        // fails (unknown encoding, corrupt data), the body is unusable — skip
        // JSON parsing and fall through to the HTTP-status-based error below.
        let body = match error_encoding {
            Some(encoding) => {
                match config
                    .compression
                    .decompress_with_limit(&encoding, body, max_err_body_size)
                {
                    Ok(decompressed) => decompressed,
                    Err(e) => {
                        tracing::debug!(
                            "failed to decompress Connect error response ({encoding}): {e}"
                        );
                        let mut err = ConnectError::new(
                            http_status_to_error_code(status),
                            format!("HTTP error {}", status.as_u16()),
                        );
                        err.set_response_headers(headers);
                        err.set_trailers(trailers);
                        return Err(err);
                    }
                }
            }
            None => body,
        };

        if let Ok(error) = serde_json::from_slice::<ConnectErrorResponse>(&body) {
            let code = error
                .code
                .as_deref()
                .and_then(|s| s.parse::<ErrorCode>().ok())
                .unwrap_or_else(|| http_status_to_error_code(status));
            let mut err = ConnectError::new(code, error.message.unwrap_or_default());
            err.details = error.details;
            err.set_response_headers(headers);
            err.set_trailers(trailers);
            return Err(err);
        }

        let code = http_status_to_error_code(status);
        let mut err = ConnectError::new(
            code,
            format!(
                "HTTP error {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&body)
            ),
        );
        err.set_response_headers(headers);
        err.set_trailers(trailers);
        return Err(err);
    }

    let mut resp_headers = http::HeaderMap::new();
    let mut resp_trailers = http::HeaderMap::new();
    for (name, value) in response.headers() {
        if let Some(trailer_name) = name.as_str().strip_prefix("trailer-") {
            if let Ok(name) = http::header::HeaderName::from_bytes(trailer_name.as_bytes()) {
                resp_trailers.append(name, value.clone());
            }
        } else {
            resp_headers.append(name.clone(), value.clone());
        }
    }

    let expected_content_type = config.codec_format.content_type();
    if let Some(resp_content_type) = response.headers().get(http::header::CONTENT_TYPE) {
        let ct = resp_content_type.to_str().unwrap_or("");
        if !ct.starts_with(expected_content_type) {
            let code = if ct.starts_with(content_type::PROTO) || ct.starts_with(content_type::JSON)
            {
                ErrorCode::Internal
            } else {
                ErrorCode::Unknown
            };
            return Err(ConnectError::new(
                code,
                format!("unexpected content-type: {ct}"),
            ));
        }
    }

    let response_encoding = response
        .headers()
        .get(http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    let max_message_size = options
        .max_message_size
        .unwrap_or(crate::service::DEFAULT_MAX_MESSAGE_SIZE);

    let body = collect_body_bounded(response.into_body(), max_message_size).await?;

    let body = if let Some(encoding) = response_encoding {
        config
            .compression
            .decompress_with_limit(&encoding, body, max_message_size)
            .map_err(map_response_decompression_error)?
    } else {
        body
    };

    if body.len() > max_message_size {
        return Err(ConnectError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "message size {} exceeds limit {}",
                body.len(),
                max_message_size
            ),
        ));
    }

    let message = decode_response_view::<RespView>(body, config.codec_format)?;

    Ok(UnaryResponse {
        headers: resp_headers,
        body: message,
        trailers: resp_trailers,
    })
}

/// Parse a gRPC/gRPC-Web unary response.
///
/// For gRPC: body is a single envelope, trailers via HTTP/2 trailers.
/// For gRPC-Web: body contains envelope + 0x80 trailer frame.
async fn parse_grpc_unary_response<B, RespView>(
    response: Response<B>,
    config: &ClientConfig,
    options: &CallOptions,
    deadline: Option<std::time::Instant>,
) -> Result<UnaryResponse<OwnedView<RespView>>, ConnectError>
where
    B: Body<Data = Bytes> + Send,
    B::Error: std::fmt::Display,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    let status = response.status();
    let resp_headers = response.headers().clone();

    // A non-200 status is reported below rather than here: a proxy that
    // synthesizes an error reply (an Envoy local reply sends 503 alongside
    // `grpc-status: 14`) puts the gRPC status in the initial headers, and that
    // status is only readable once the body has shown the response to be
    // trailers-only.
    validate_grpc_response_content_type(&resp_headers, config)?;

    // Check for unsupported compression before reading the body
    let response_encoding = resp_headers
        .get("grpc-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    if let Some(ref enc) = response_encoding
        && enc != "identity"
        && !config.compression.supports(enc)
    {
        let mut err = ConnectError::internal(format!("unsupported response compression: {enc}"));
        err.set_response_headers(resp_headers);
        return Err(err);
    }

    // Read response body frame-by-frame to capture both data and HTTP/2 trailers.
    // Using collect().to_bytes() would lose the trailers.
    let mut body = std::pin::pin!(response.into_body());
    let mut buf = BytesMut::new();
    let mut grpc_trailers = http::HeaderMap::new();
    let mut has_body_data = false;
    // For unary/client-stream, expect at most one envelope + trailer.
    // Cap buffer to prevent a malicious server from forcing unbounded allocation.
    let max_buf_size = options
        .max_message_size
        .unwrap_or(crate::service::DEFAULT_MAX_MESSAGE_SIZE)
        .saturating_add(crate::envelope::HEADER_SIZE)
        .saturating_add(RESPONSE_BUFFER_TRAILER_SLACK);

    loop {
        match std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
            Some(Ok(frame)) => {
                if frame.is_data() {
                    if let Ok(data) = frame.into_data() {
                        if !data.is_empty() {
                            has_body_data = true;
                        }
                        let remaining = max_buf_size.saturating_sub(buf.len());
                        let append_len = data.len().min(remaining);
                        buf.extend_from_slice(&data[..append_len]);
                        if matches!(config.protocol, Protocol::GrpcWeb)
                            && let Some(trailer_end) = grpc_web_trailer_frame_end(&buf)
                        {
                            buf.truncate(trailer_end);
                            break;
                        }
                        if append_len < data.len() {
                            return Err(ConnectError::resource_exhausted(format!(
                                "response body size exceeds limit {max_buf_size}"
                            )));
                        }
                    }
                } else if frame.is_trailers()
                    && let Ok(trailers) = frame.into_trailers()
                {
                    grpc_trailers = trailers;
                }
            }
            Some(Err(e)) => {
                // The body of a non-200 response is read only to find a gRPC
                // status, and the HTTP status is reported below when there is
                // none, so a read that dies part-way through (a proxy that
                // resets the stream after its error headers) still reports the
                // status rather than the read failure.
                if status.is_success() {
                    return Err(ConnectError::internal(format!(
                        "failed to read response body: {e}"
                    )));
                }
                break;
            }
            None => break,
        }
    }

    // Determine the authoritative source for grpc-status:
    // - If HTTP/2 trailers have grpc-status, use those (highest priority)
    // - If body has a gRPC-Web trailer frame, use that
    // - Only if no body data AND no HTTP/2 trailers, fall back to initial headers
    //   (trailers-only response)
    let mut message_data: Option<Bytes> = None;
    let mut message_count = 0u32;

    while !buf.is_empty() {
        // A set high bit marks a gRPC-Web trailer frame.
        if buf[0] & crate::envelope::flags::GRPC_WEB_TRAILER != 0 {
            if !matches!(config.protocol, Protocol::GrpcWeb) {
                // Plain gRPC defines no flag in the high bit, and envelope
                // decoding ignores unknown bits, so accepting the frame here
                // would let a data envelope overwrite the HTTP/2 trailers.
                let mut err = ConnectError::internal(format!(
                    "invalid gRPC response framing: envelope flag {:#04x} \
                     (gRPC-Web trailer marker) is not valid on a plain gRPC response",
                    buf[0]
                ));
                err.set_response_headers(resp_headers);
                return Err(err);
            }
            let decompression = response_encoding
                .as_deref()
                .map(|enc| (&config.compression, enc));
            if let Some(trailers) =
                parse_grpc_web_trailer_frame_with_compression(&buf, decompression)
            {
                grpc_trailers = trailers;
            }
            break;
        }

        let grpc_max_msg = options
            .max_message_size
            .unwrap_or(crate::service::DEFAULT_MAX_MESSAGE_SIZE);

        let envelope = match Envelope::decode_with_limit(&mut buf, grpc_max_msg) {
            Ok(Some(env)) => env,
            Ok(None) => break,
            Err(e) => {
                return Err(ConnectError::internal(format!(
                    "envelope decode failed: {e}"
                )));
            }
        };

        if message_count > 0 {
            let mut err = ConnectError::unimplemented(
                "received multiple response messages where exactly one was expected",
            );
            err.set_response_headers(resp_headers);
            return Err(err);
        }

        let data = if envelope.is_compressed() {
            let enc = response_encoding.as_deref().ok_or_else(|| {
                ConnectError::internal("received compressed message without grpc-encoding header")
            })?;
            if enc == "identity" {
                return Err(ConnectError::internal(
                    "received compressed message with identity encoding",
                ));
            }
            config
                .compression
                .decompress_with_limit(enc, envelope.data, grpc_max_msg)
                .map_err(map_response_decompression_error)?
        } else {
            envelope.data
        };

        message_count += 1;
        message_data = Some(data);
    }

    // Check for errors in trailers (HTTP/2 trailers or gRPC-Web trailer frame).
    // If we have trailers from HTTP/2 or gRPC-Web, those take precedence.
    // Only fall back to initial headers if no body data was received (trailers-only).
    let status_from_headers = grpc_trailers.is_empty() && !has_body_data;
    let effective_trailers = if status_from_headers {
        // Trailers-only response: initial headers contain the status
        &resp_headers
    } else {
        &grpc_trailers // empty when no trailers were found
    };

    if let Some(mut err) = parse_grpc_error_from_trailers(effective_trailers) {
        if status_from_headers {
            // The status came from the initial headers, so the entries copied
            // alongside it are response metadata, not trailing metadata:
            // `content-type` and friends must not surface from
            // `ConnectError::trailers()`. They stay reachable through
            // `response_headers()`, set below.
            err.trailers_mut().clear();
        }
        err.set_response_headers(resp_headers);
        return Err(err);
    }

    // A non-200 response is an error even when the gRPC status says otherwise
    // or is missing entirely; an error status found above is the more specific
    // report, so this is only reached when there was none.
    if !status.is_success() {
        let code = http_status_to_error_code(status);
        let mut err = ConnectError::new(code, format!("HTTP error {}", status.as_u16()));
        err.set_response_headers(resp_headers);
        return Err(err);
    }

    // For missing grpc-status, synthesize an error.
    // If a deadline was set and has passed, map to DEADLINE_EXCEEDED per the gRPC
    // spec: RST_STREAM CANCEL is upgraded to DeadlineExceeded when the deadline
    // has elapsed (matching grpc-go and connect-go behavior).
    if effective_trailers.get("grpc-status").is_none() {
        let is_deadline_exceeded = deadline.is_some_and(|d| std::time::Instant::now() >= d);
        let mut err = if is_deadline_exceeded {
            ConnectError::deadline_exceeded("request timeout")
        } else {
            ConnectError::internal("gRPC response missing grpc-status trailer")
        };
        err.set_response_headers(resp_headers);
        return Err(err);
    }

    let data = match message_data {
        Some(data) => data,
        None => {
            // No message data — this is an error for unary/client-stream RPCs.
            let mut err = ConnectError::unimplemented("gRPC response contained no message data");
            err.set_response_headers(resp_headers);
            return Err(err);
        }
    };

    if let Some(max_size) = options.max_message_size
        && data.len() > max_size
    {
        return Err(ConnectError::new(
            ErrorCode::ResourceExhausted,
            format!("message size {} exceeds limit {}", data.len(), max_size),
        ));
    }

    let message = decode_response_view::<RespView>(data, config.codec_format)?;

    Ok(UnaryResponse {
        headers: resp_headers,
        body: message,
        trailers: grpc_trailers,
    })
}

/// Validate the `content-type` of a gRPC / gRPC-Web response against the
/// client's configured protocol and codec, mirroring connect-go's
/// `grpcValidateResponseContentType`.
///
/// Parameters (`; charset=...`) are stripped before comparison. The bare
/// family types `application/grpc` / `application/grpc-web` are accepted for
/// any codec, because the bare type means "proto by default" and proxies that
/// synthesize trailers-only error responses (such as Envoy local replies)
/// send it regardless of the request's subtype. A missing `content-type`
/// header is also accepted, preserving this client's previous leniency. A
/// same-family subtype that doesn't match the configured codec is rejected as
/// `internal` (a broken server or intermediary); anything else is `unknown`
/// (not a gRPC response at all), matching connect-go's classification.
fn validate_grpc_response_content_type(
    resp_headers: &http::HeaderMap,
    config: &ClientConfig,
) -> Result<(), ConnectError> {
    debug_assert!(
        matches!(config.protocol, Protocol::Grpc | Protocol::GrpcWeb),
        "gRPC response content-type validation is only for gRPC/gRPC-Web"
    );

    let Some(resp_content_type) = resp_headers.get(http::header::CONTENT_TYPE) else {
        return Ok(());
    };

    let ct = resp_content_type.to_str().unwrap_or("");
    let ct_normalized = ct
        .split_once(';')
        .map_or(ct, |(media_type, _params)| media_type)
        .trim();
    let expected = config
        .protocol
        .response_content_type(config.codec_format, false);
    let (bare, family_prefix) = match config.protocol {
        Protocol::Grpc => ("application/grpc", "application/grpc+"),
        Protocol::GrpcWeb => ("application/grpc-web", "application/grpc-web+"),
        // Unreachable per the debug_assert above; treat as valid rather than
        // misclassify a Connect response in release builds.
        Protocol::Connect => return Ok(()),
    };

    if ct_normalized == expected || ct_normalized == bare {
        return Ok(());
    }

    let code = if ct_normalized.starts_with(family_prefix) {
        ErrorCode::Internal
    } else {
        ErrorCode::Unknown
    };
    let mut err = ConnectError::new(
        code,
        format!("unexpected content-type: {ct} (expected {expected})"),
    );
    err.set_response_headers(resp_headers.clone());
    Err(err)
}

/// Terminal record for a client stream: why it ended and what trailing
/// metadata arrived. Written exactly once (by `message()`); read by the
/// sticky replay, [`ServerStream::error()`], and
/// [`ServerStream::trailers()`] — one fact, three readers, so they cannot
/// disagree.
#[derive(Debug)]
struct StreamEnd {
    /// `Ok(())` is a clean end (the RPC succeeded).
    outcome: Result<(), ConnectError>,
    trailers: Option<http::HeaderMap>,
}

impl StreamEnd {
    fn replay<T>(&self) -> Result<Option<T>, ConnectError> {
        match &self.outcome {
            Ok(()) => Ok(None),
            Err(e) => Err(e.clone()),
        }
    }
}

/// Lets `?` lift decode/transport/deadline errors out of the decode loop.
/// Every such site fires before any termination metadata exists, so
/// `trailers: None` is correct at all of them; ends that carry trailers
/// construct their `StreamEnd` explicitly.
impl From<ConnectError> for StreamEnd {
    fn from(e: ConnectError) -> Self {
        StreamEnd {
            outcome: Err(e),
            trailers: None,
        }
    }
}

/// What one body poll produced.
enum BodyPoll {
    /// A DATA frame was appended to the decode buffer.
    Data,
    /// HTTP/2 (or HTTP/1.1 chunked) trailers — the body's final frame.
    Trailers(http::HeaderMap),
    /// Body exhausted.
    Eof,
}

/// Response from a server-streaming RPC.
///
/// Provides incremental access to response messages as they arrive from the server.
/// Messages are decoded one at a time from the HTTP response body using the
/// [`message()`](ServerStream::message) method, which returns `Ok(None)` for
/// a clean end and `Err` for a failed RPC — `?` is the complete error
/// handling. Trailing metadata becomes available after the stream ends.
///
/// # Example
///
/// ```rust,ignore
/// let mut stream = call_server_stream(&transport, &config, "svc", "method", req, CallOptions::default()).await?;
/// println!("headers: {:?}", stream.headers());
/// while let Some(msg) = stream.message().await? {
///     println!("got message: {:?}", msg);
/// }
/// if let Some(trailers) = stream.trailers() {
///     println!("trailers: {:?}", trailers);
/// }
/// ```
pub struct ServerStream<B, RespView> {
    headers: http::HeaderMap,
    body: B,
    buf: BytesMut,
    encoding: Option<String>,
    compression: CompressionRegistry,
    codec_format: CodecFormat,
    protocol: Protocol,
    max_message_size: Option<usize>,
    deadline: Option<std::time::Instant>,
    /// The terminal record; `Some` once the stream has ended, by any cause.
    end: Option<StreamEnd>,
    /// Whether any body DATA frame arrived. Distinguishes a true
    /// Trailers-Only response (empty body; status rides the headers)
    /// from a stream that produced data and was then cut off.
    saw_body_data: bool,
    _phantom: PhantomData<RespView>,
}

// Manual impl: the body type `B` (typically `hyper::body::Incoming`) isn't
// `Debug`, and we don't want to dump the partially-consumed `buf` anyway.
// Print the stream's observable state for test diagnostics.
impl<B, RespView> std::fmt::Debug for ServerStream<B, RespView> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerStream")
            .field("protocol", &self.protocol)
            .field("codec_format", &self.codec_format)
            .field("encoding", &self.encoding)
            .field("ended", &self.end.is_some())
            .field(
                "error",
                &self.end.as_ref().and_then(|e| e.outcome.as_ref().err()),
            )
            .field(
                "has_trailers",
                &self.end.as_ref().is_some_and(|e| e.trailers.is_some()),
            )
            .field("buffered_bytes", &self.buf.len())
            .finish_non_exhaustive()
    }
}

impl<B, RespView> ServerStream<B, RespView>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    /// Returns the response headers.
    #[must_use]
    pub fn headers(&self) -> &http::HeaderMap {
        &self.headers
    }

    /// Fetch the next message from the stream.
    ///
    /// Returns `Ok(Some(msg))` for each message, `Ok(None)` when the stream
    /// ends **cleanly** (gRPC status OK / error-free END_STREAM), or
    /// `Err(...)` for everything else: protocol/decode/deadline errors *and*
    /// a server error carried in the stream's termination metadata (gRPC
    /// trailers, gRPC-Web trailer frame, or Connect END_STREAM envelope).
    /// `Ok(None)` means the RPC succeeded. Terminal errors arrive from
    /// `message()` itself, as in `tonic`.
    ///
    /// Every `Err` is terminal and sticky: the stream will never yield
    /// another message, subsequent calls return the same `Err` (the same
    /// policy as a failed stream construction; stronger than `tonic`, which
    /// yields the error once and then reads as a clean end), and recovery
    /// means making a new call — not re-polling this one. The terminal error also remains
    /// inspectable via [`error()`](Self::error), and
    /// [`trailers()`](Self::trailers) is populated when termination metadata
    /// was received — for both the `Ok(None)` and `Err` ends.
    ///
    /// If a deadline was set on this call (via [`CallOptions::with_timeout`]
    /// or [`ClientConfig::with_default_timeout`]), each `message()` poll is
    /// bounded by it — gRPC deadline semantics are whole-call, so a hung
    /// server won't block indefinitely (matching grpc-java and connect-go).
    ///
    /// # Errors
    ///
    /// A response body that ends without its protocol's termination
    /// metadata is not a clean end and returns `Err` rather than
    /// `Ok(None)`: `internal` for a Connect stream missing its
    /// END_STREAM envelope; for gRPC/gRPC-Web, `internal` when no
    /// trailers arrived at all, `unknown` when trailers arrived without a
    /// `grpc-status`, and `unknown` for a malformed `grpc-status` value —
    /// matching grpc-go's treatment of each case. A Trailers-Only response
    /// carrying `grpc-status: 0` in the headers (empty body) is a clean
    /// end.
    pub async fn message<M>(&mut self) -> Result<Option<crate::StreamMessage<M>>, ConnectError>
    where
        // `M` is an output parameter pinned to `RespView`'s owned message —
        // spelled this way round (rather than bounding `RespView::Owned`
        // directly) so the future stays `Send` for concrete generated view
        // types: projecting through the GAT in the bound trips rustc's
        // coroutine-witness auto-trait check (#214).
        RespView: MessageView<'static, Owned = M>,
        M: HasMessageView<View<'static> = RespView>,
    {
        // The outcome is immutable once reported: replay the terminal
        // record without re-entering the body.
        if let Some(end) = &self.end {
            return end.replay();
        }
        match self.next_message_or_end().await {
            Ok(msg) => Ok(Some(crate::StreamMessage::from_owned_view(msg))),
            // The single writer of the terminal record. `message_inner`
            // cannot end the stream without producing one — "ended without
            // recording why" is unrepresentable.
            Err(end) => {
                debug_assert!(self.end.is_none(), "terminal record written twice");
                self.end.get_or_insert(end).replay()
            }
        }
    }

    /// The decode loop. An `Err` here means **"the stream ended"**, not
    /// "failure" — the [`StreamEnd`] record says whether the end was
    /// clean (`outcome: Ok(())`) or a failure. Plain
    /// decode/transport/deadline errors lift into a `StreamEnd` via
    /// `From`. There is deliberately no way to exit this loop without
    /// producing the terminal record.
    async fn next_message_or_end(&mut self) -> Result<OwnedView<RespView>, StreamEnd> {
        loop {
            // For gRPC-Web, check for a complete trailer frame (flag 0x80)
            // before attempting envelope decode (which would treat 0x80 as
            // a data envelope flag rather than the gRPC-Web trailer sentinel).
            if matches!(self.protocol, Protocol::GrpcWeb)
                && self.buf.len() >= 5
                && self.buf[0] & crate::envelope::flags::GRPC_WEB_TRAILER != 0
            {
                let trailer_len =
                    u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]])
                        as usize;
                // `saturating_add`: `trailer_len` is a server-controlled u32, so
                // on a 32-bit target (e.g. the supported `wasm32` gRPC-Web
                // client) `5 + trailer_len` can overflow `usize` and panic in a
                // debug build. Matches the sibling framing sites, which already
                // saturate. A saturated sum is never `<= buf.len()`, so an
                // over-large prefix simply waits for bytes that never arrive.
                if self.buf.len() >= trailer_len.saturating_add(5) {
                    // Complete trailer frame — parse and classify. An
                    // unparseable frame classifies as `None` (no usable
                    // termination metadata).
                    let decompression =
                        self.encoding.as_deref().map(|enc| (&self.compression, enc));
                    let parsed =
                        parse_grpc_web_trailer_frame_with_compression(&self.buf, decompression);
                    return Err(self.classify_grpc_end(parsed));
                }
                // Incomplete trailer frame — need more data, fall through
                // to poll_body below
            }

            // Try to decode a complete envelope from the buffer.
            // Skip this for gRPC-Web when the buffer starts with 0x80 (trailer
            // flag) to avoid misinterpreting the trailer frame as a data message.
            let envelope_result = if matches!(self.protocol, Protocol::GrpcWeb)
                && !self.buf.is_empty()
                && self.buf[0] & crate::envelope::flags::GRPC_WEB_TRAILER != 0
            {
                // We know the trailer frame is incomplete (checked above),
                // so signal that more data is needed.
                None
            } else {
                Envelope::decode_with_limit(
                    &mut self.buf,
                    self.max_message_size
                        .unwrap_or(crate::service::DEFAULT_MAX_MESSAGE_SIZE),
                )?
            };

            match envelope_result {
                Some(envelope) => {
                    if envelope.is_end_stream() {
                        // Connect protocol end-of-stream envelope
                        return Err(self.process_end_stream(envelope));
                    }

                    // Data envelope — decompress and decode
                    let data = self.decompress_envelope(envelope)?;

                    // Check message size limit
                    if let Some(max_size) = self.max_message_size
                        && data.len() > max_size
                    {
                        return Err(ConnectError::new(
                            ErrorCode::ResourceExhausted,
                            format!("message size {} exceeds limit {}", data.len(), max_size),
                        )
                        .into());
                    }

                    let msg = decode_response_view::<RespView>(data, self.codec_format)?;
                    return Ok(msg);
                }
                None => match self.poll_body().await? {
                    BodyPoll::Data => {} // loop back to try decoding again
                    BodyPoll::Trailers(trailers) => {
                        return Err(self.classify_grpc_end(Some(trailers)));
                    }
                    BodyPoll::Eof => {
                        if matches!(self.protocol, Protocol::Connect) {
                            // The HTTP body completed cleanly but the Connect
                            // envelope sequence is missing its terminus: a
                            // wire-level error, classified as `internal` the
                            // same way connect-go and other gRPC stacks treat a
                            // failed decompression or an unparseable response.
                            return Err(ConnectError::internal(
                                "Connect streaming response ended without END_STREAM envelope",
                            )
                            .into());
                        }
                        // gRPC-Web: preserved verbatim from the
                        // pre-refactor shape, and provably dead — the
                        // loop-top completeness check consumes any complete
                        // trailer frame before poll_body runs, and EOF
                        // appends nothing, so the remnant here is absent or
                        // incomplete and the parse returns `None`. Removal
                        // is a follow-up; either way classification sees
                        // "no usable termination metadata".
                        let parsed = if matches!(self.protocol, Protocol::GrpcWeb)
                            && !self.buf.is_empty()
                            && self.buf[0] & crate::envelope::flags::GRPC_WEB_TRAILER != 0
                        {
                            let decompression =
                                self.encoding.as_deref().map(|enc| (&self.compression, enc));
                            parse_grpc_web_trailer_frame_with_compression(&self.buf, decompression)
                        } else {
                            None
                        };
                        return Err(self.classify_grpc_end(parsed));
                    }
                },
            }
        }
    }

    /// The single classification site for gRPC/gRPC-Web stream ends:
    /// given the termination metadata that arrived (HTTP/2 trailers, a
    /// parsed gRPC-Web trailer frame, or `None` when nothing usable did),
    /// decide clean vs failed and produce the terminal record. Connect
    /// ends never come here — they classify in `process_end_stream` or
    /// the missing-END_STREAM arm.
    ///
    /// An end is only clean if a `grpc-status` actually arrived: in the
    /// trailers, or — for Trailers-Only responses (grpc-go emits these
    /// for OK ends with zero messages) — in the response headers, honored
    /// only while no body data has flowed (the unary path's
    /// has_body_data guard: a mid-stream cut after eager headers must not
    /// read as success). No status anywhere is indistinguishable from a
    /// mid-stream cut: past the whole-call deadline the deadline is what
    /// cut the stream (matches grpc-go / connect-go RST_STREAM CANCEL
    /// handling); with trailers present it's `unknown`, without any it's
    /// `internal` — each matching grpc-go.
    fn classify_grpc_end(&self, trailers: Option<http::HeaderMap>) -> StreamEnd {
        debug_assert!(
            matches!(self.protocol, Protocol::Grpc | Protocol::GrpcWeb),
            "Connect ends classify in process_end_stream / the missing-END_STREAM arm"
        );
        let outcome = match trailers.as_ref().and_then(parse_grpc_error_from_trailers) {
            // A server error — or a present-but-malformed status, which
            // the parse maps to `unknown` — ends the RPC in failure.
            Some(err) => Err(err),
            None => {
                let has_status = |h: &http::HeaderMap| h.contains_key("grpc-status");
                let trailers_only = !self.saw_body_data;
                if trailers.as_ref().is_some_and(has_status)
                    || (trailers_only && has_status(&self.headers))
                {
                    Ok(())
                } else if self
                    .deadline
                    .is_some_and(|d| std::time::Instant::now() >= d)
                {
                    Err(ConnectError::deadline_exceeded("request timeout"))
                } else if trailers.is_some() {
                    Err(ConnectError::new(
                        ErrorCode::Unknown,
                        "protocol error: grpc-status missing from trailers",
                    ))
                } else {
                    Err(ConnectError::internal("stream ended without grpc-status"))
                }
            }
        };
        StreamEnd { outcome, trailers }
    }

    /// Returns the trailing metadata, if available.
    ///
    /// Only populated after [`message()`](Self::message) reports the end of
    /// the stream, and only when termination metadata was received — for
    /// both the `Ok(None)` and `Err` ends.
    #[must_use]
    pub fn trailers(&self) -> Option<&http::HeaderMap> {
        self.end.as_ref().and_then(|e| e.trailers.as_ref())
    }

    /// Returns the terminal error that ended the stream, if any — a server
    /// error from the termination metadata (gRPC trailers / Connect
    /// END_STREAM), or a decode/transport/deadline failure.
    ///
    /// [`message()`](Self::message) already returns this same error, so most
    /// callers never need this accessor; it exists for post-hoc inspection
    /// alongside [`trailers()`](Self::trailers).
    #[must_use]
    pub fn error(&self) -> Option<&ConnectError> {
        self.end.as_ref().and_then(|e| e.outcome.as_ref().err())
    }

    /// Poll the body for the next frame. A pure transport reader: it
    /// buffers data, returns trailers as a value, and never touches the
    /// terminal record.
    ///
    /// Buffer growth is bounded: if the accumulated bytes exceed the expected
    /// maximum in-flight envelope size, return `ResourceExhausted` rather than
    /// continuing to buffer. This prevents a malicious server from trickling
    /// bytes indefinitely without ever completing an envelope.
    async fn poll_body(&mut self) -> Result<BodyPoll, ConnectError> {
        // Enough for one complete envelope at the max message size, plus
        // one header's worth of slack (next envelope's header may arrive in
        // the same TCP frame), plus 64 KiB for gRPC-Web trailer frames.
        let max_buf_size = self
            .max_message_size
            .unwrap_or(crate::service::DEFAULT_MAX_MESSAGE_SIZE)
            .saturating_add(2 * crate::envelope::HEADER_SIZE)
            .saturating_add(RESPONSE_BUFFER_TRAILER_SLACK);

        loop {
            // The whole-call deadline bounds each frame poll. The
            // equivalence with bounding the entire decode loop rests on
            // three facts: the deadline is an absolute instant, all work
            // between frame polls is non-yielding, and `timeout_at` polls
            // the inner future before the timer (a Ready frame at the
            // deadline wins, in both shapes). It is what lets every
            // terminal cause exit `next_message_or_end` as a `StreamEnd`. (A
            // relative per-poll timeout would break it;
            // `deadline_bounds_multi_frame_message` pins that.)
            let deadline = self.deadline;
            let frame = with_deadline(deadline, async {
                Ok(Pin::new(&mut self.body).frame().await)
            })
            .await?;

            match frame {
                None => return Ok(BodyPoll::Eof),
                Some(Ok(frame)) => {
                    if frame.is_data() {
                        if let Ok(data) = frame.into_data() {
                            if !data.is_empty() {
                                self.saw_body_data = true;
                            }
                            if self.buf.len().saturating_add(data.len()) > max_buf_size {
                                return Err(ConnectError::resource_exhausted(format!(
                                    "response buffer exceeds limit {max_buf_size}"
                                )));
                            }
                            self.buf.extend_from_slice(&data);
                            return Ok(BodyPoll::Data);
                        }
                    } else if frame.is_trailers()
                        && let Ok(trailers) = frame.into_trailers()
                        && matches!(self.protocol, Protocol::Grpc | Protocol::GrpcWeb)
                    {
                        // HTTP/2 or HTTP/1.1 chunked trailers — used by
                        // gRPC/gRPC-Web. (Connect has no trailer semantics;
                        // such frames are skipped.)
                        return Ok(BodyPoll::Trailers(trailers));
                    }
                }
                Some(Err(e)) => {
                    return Err(ConnectError::internal(format!(
                        "error reading response body: {e}"
                    )));
                }
            }
        }
    }

    /// Decompress a data envelope if needed.
    fn decompress_envelope(&self, envelope: Envelope) -> Result<Bytes, ConnectError> {
        if envelope.is_compressed() {
            let encoding = self.encoding.as_deref().ok_or_else(|| {
                ConnectError::internal(
                    "received compressed message without content-encoding header",
                )
            })?;
            let max_size = self
                .max_message_size
                .unwrap_or(crate::service::DEFAULT_MAX_MESSAGE_SIZE);
            self.compression
                .decompress_with_limit(encoding, envelope.data, max_size)
                .map_err(map_response_decompression_error)
        } else {
            Ok(envelope.data)
        }
    }

    /// Classify the Connect END_STREAM envelope into the terminal record.
    fn process_end_stream(&self, envelope: Envelope) -> StreamEnd {
        let end_stream_data = match self.decompress_envelope(envelope) {
            Ok(data) => data,
            Err(e) => return e.into(),
        };

        let end_stream = match parse_connect_end_stream(&end_stream_data) {
            Ok(end_stream) => end_stream,
            Err(mut e) => {
                e.set_response_headers(self.headers.clone());
                return e.into();
            }
        };

        let trailers = end_stream.metadata.map(|metadata| {
            let mut trailers = http::HeaderMap::new();
            append_metadata_capped(&mut trailers, metadata);
            trailers
        });

        let outcome = match end_stream.error {
            Some(err) => Err(end_stream_error_to_connect_error(err)),
            None => Ok(()),
        };

        StreamEnd { outcome, trailers }
    }
}

/// Make a server-streaming RPC call.
///
/// Sends a single request and returns a [`ServerStream`] that yields response
/// messages incrementally as they arrive. Use [`ServerStream::message()`] to
/// read messages one at a time.
///
/// # Errors
///
/// Returns immediately with an error if:
/// - The request cannot be encoded or sent
/// - The server responds with a non-200 status (protocol-level error)
///
/// Errors that occur during the stream (e.g., in gRPC trailers or the
/// END_STREAM envelope) are returned by [`ServerStream::message()`].
///
/// # Cancellation
///
/// Dropping the returned future or hitting its deadline drops the in-flight
/// transport send with it, so a request that had not finished sending may
/// never reach the server.
pub async fn call_server_stream<T, Req, RespView>(
    transport: &T,
    config: &ClientConfig,
    service: &str,
    method: &str,
    request: Req,
    options: CallOptions,
) -> Result<ServerStream<T::ResponseBody, RespView>, ConnectError>
where
    T: ClientTransport,
    <T::ResponseBody as Body>::Error: std::fmt::Display,
    Req: buffa::Message + crate::codec::JsonSerialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    let options = effective_options(config, options);

    // Build the full URI from base_uri and service/method path
    let base_str = config.base_uri.to_string();
    let base_str = base_str.trim_end_matches('/');
    let full_uri = format!("{base_str}/{service}/{method}");
    let uri: Uri = full_uri
        .parse()
        .map_err(|e| ConnectError::internal(format!("invalid URI: {e}")))?;

    // Encode the request body
    let body = match config.codec_format {
        CodecFormat::Proto => request.encode_to_bytes(),
        CodecFormat::Json => encode_json(&request)?,
    };

    // Compress and envelope-frame the request body (streaming protocol
    // requires envelope framing).
    let compression_for_encoder = config.request_compression.as_ref().map(|enc| {
        (
            std::sync::Arc::new(config.compression.clone()),
            enc.as_str(),
        )
    });
    let mut encoder = crate::envelope::EnvelopeEncoder::new(
        compression_for_encoder,
        config.compression_policy.with_override(options.compress),
    );
    let mut request_buf = bytes::BytesMut::new();
    tokio_util::codec::Encoder::encode(&mut encoder, body, &mut request_buf)?;
    let request_body = request_buf.freeze();

    // Compute deadline BEFORE sending, matching Go's ctx.Deadline() semantics
    let deadline = client_deadline(options.timeout, config.protocol);

    // Build the HTTP request with protocol-aware streaming headers
    let mut builder = Request::builder().method(http::Method::POST).uri(uri);
    builder = add_streaming_request_headers(builder, config, options.timeout);

    // Merge user-provided headers (last, so they can override anything)
    let headers = builder.headers_mut().unwrap();
    for (name, value) in &options.headers {
        headers.append(name.clone(), value.clone());
    }

    let http_request = builder
        .body(full_body(request_body))
        .map_err(|e| ConnectError::internal(format!("failed to build request: {e}")))?;

    // Enforce client-side deadline on send + header parsing. Ongoing
    // message() reads are also bounded by the same deadline (inside
    // ServerStream::message) — gRPC deadline semantics are whole-call.
    with_deadline(deadline, async {
        let response = transport
            .send(http_request)
            .await
            .map_err(|e| map_transport_send_error(e, "request failed"))?;

        make_server_stream(
            response,
            config.protocol,
            &config.compression,
            config.codec_format,
            options.max_message_size,
            deadline,
        )
        .await
    })
    .await
}

/// Construct a [`ServerStream`] from a streaming HTTP response.
///
/// Handles trailers-only gRPC error detection, non-200 Connect error body
/// parsing, and response encoding extraction. Used by both [`call_server_stream`]
/// (passing fields from `&ClientConfig`) and [`BidiStream::message`] (passing
/// fields from its owned `StreamConfig` snapshot).
///
/// Takes individual config fields instead of `&ClientConfig` so callers that
/// need to capture config by value (like `BidiStream`, which outlives the
/// borrow) can share the same code path.
async fn make_server_stream<B, RespView>(
    response: Response<B>,
    protocol: Protocol,
    compression: &CompressionRegistry,
    codec_format: CodecFormat,
    max_message_size: Option<usize>,
    deadline: Option<std::time::Instant>,
) -> Result<ServerStream<B, RespView>, ConnectError>
where
    B: Body<Data = Bytes> + Send,
    B::Error: std::fmt::Display,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    let response_headers = response.headers().clone();
    let status = response.status();

    // For gRPC, check for trailers-only error response
    if matches!(protocol, Protocol::Grpc | Protocol::GrpcWeb)
        && let Some(mut err) = parse_grpc_error_from_trailers(&response_headers)
    {
        err.set_response_headers(response_headers);
        return Err(err);
    }

    // Non-200 responses are protocol errors
    if !status.is_success() {
        if matches!(protocol, Protocol::Connect) {
            let error_encoding = response_headers
                .get(http::header::CONTENT_ENCODING)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_owned());

            let stream_max_err_size =
                max_message_size.unwrap_or(crate::service::DEFAULT_MAX_MESSAGE_SIZE);

            let body = collect_body_bounded(response.into_body(), stream_max_err_size).await?;

            // Decompress if the server set Content-Encoding. On failure,
            // fall through to the generic HTTP-status error below.
            let body = match error_encoding {
                Some(encoding) => {
                    match compression.decompress_with_limit(&encoding, body, stream_max_err_size) {
                        Ok(decompressed) => Some(decompressed),
                        Err(e) => {
                            tracing::debug!(
                                "failed to decompress Connect error response ({encoding}): {e}"
                            );
                            None
                        }
                    }
                }
                None => Some(body),
            };

            if let Some(body) = body
                && let Ok(error) = serde_json::from_slice::<ConnectErrorResponse>(&body)
            {
                let code = error
                    .code
                    .as_deref()
                    .and_then(|s| s.parse::<ErrorCode>().ok())
                    .unwrap_or_else(|| http_status_to_error_code(status));
                let mut err = ConnectError::new(code, error.message.unwrap_or_default());
                err.details = error.details;
                err.set_response_headers(response_headers);
                return Err(err);
            }
        }

        let code = http_status_to_error_code(status);
        let mut err = ConnectError::new(code, format!("HTTP error {}", status.as_u16()));
        err.set_response_headers(response_headers);
        return Err(err);
    }

    // Get the response encoding for compressed envelopes (protocol-aware header)
    let encoding = response_headers
        .get(protocol.content_encoding_header())
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    Ok(ServerStream {
        headers: response_headers,
        body: response.into_body(),
        buf: BytesMut::new(),
        encoding,
        compression: compression.clone(),
        codec_format,
        protocol,
        max_message_size,
        deadline,
        end: None,
        saw_body_data: false,
        _phantom: PhantomData,
    })
}

// ============================================================================
// BidiStream — bidirectional streaming client
// ============================================================================

/// A request body that pulls envelope-encoded frames from an mpsc channel.
///
/// Used as the request body for bidirectional streaming calls.
/// [`BidiSendHalf::send`] pushes encoded envelopes to the channel's sender;
/// dropping the sender (via [`BidiSendHalf::close_send`]) closes the body,
/// signalling EOF to the server.
struct ChannelBody {
    rx: tokio::sync::mpsc::Receiver<Result<Bytes, ConnectError>>,
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = ConnectError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, ConnectError>>> {
        self.rx
            .poll_recv(cx)
            .map(|opt| opt.map(|r| r.map(http_body::Frame::data)))
    }
}

/// A request body that lazily encodes messages from the caller's stream
/// into envelope frames as the transport polls for body data.
///
/// Used by [`call_client_stream`]: making the stream *be* the body hands
/// upload liveness to the HTTP layer. The transport polls for the next
/// frame only while it can send (backpressure is HTTP/2 flow control), a
/// server that ends the RPC early makes the transport stop polling and
/// drop the body, and a server that sends response headers early while
/// still reading the upload keeps receiving frames — none of which needs a
/// library-side pump loop.
///
/// The stream is held in a [`sync_wrapper::SyncWrapper`] so the body is
/// `Sync` (as [`ClientBody`]'s boxing requires) without demanding `Sync`
/// of the caller's stream — the wrapper only ever hands out `&mut` access.
#[pin_project::pin_project]
struct EncodingBody<S> {
    #[pin]
    stream: sync_wrapper::SyncWrapper<S>,
    encoder: crate::envelope::EnvelopeEncoder,
    codec_format: CodecFormat,
    /// Mirror of an encode error also emitted through the body, letting the
    /// call report the precise error instead of a transport-level failure.
    error: std::sync::Arc<std::sync::Mutex<Option<ConnectError>>>,
    /// Set on an encode error or stream exhaustion; the body then reports
    /// end-of-stream without polling the (possibly non-fused) stream again.
    done: bool,
}

impl<S, Req> Body for EncodingBody<S>
where
    S: Stream<Item = Req>,
    Req: buffa::Message + crate::codec::JsonSerialize,
{
    type Data = Bytes;
    type Error = ConnectError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, ConnectError>>> {
        use std::task::Poll;

        let this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }

        let Some(request) = std::task::ready!(this.stream.get_pin_mut().poll_next(cx)) else {
            // `Stream` gives no post-`None` guarantee, so never poll the
            // (possibly non-fused) stream again.
            *this.done = true;
            return Poll::Ready(None);
        };

        let mut record_error = |err: &ConnectError| {
            *this.done = true;
            *this
                .error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(err.clone());
        };

        let msg_bytes = match this.codec_format {
            CodecFormat::Proto => request.encode_to_bytes(),
            CodecFormat::Json => match encode_json(&request) {
                Ok(bytes) => bytes,
                Err(err) => {
                    record_error(&err);
                    return Poll::Ready(Some(Err(err)));
                }
            },
        };

        let mut envelope_buf = BytesMut::new();
        match tokio_util::codec::Encoder::encode(this.encoder, msg_bytes, &mut envelope_buf) {
            Ok(()) => Poll::Ready(Some(Ok(http_body::Frame::data(envelope_buf.freeze())))),
            Err(err) => {
                record_error(&err);
                Poll::Ready(Some(Err(err)))
            }
        }
    }
}

/// State machine for [`BidiRecvHalf`], the receive side of a [`BidiStream`].
///
/// The transport send is spawned so the HTTP request makes progress
/// immediately (connect, handshake, start streaming the request body from
/// [`ChannelBody`]) regardless of when the caller first calls
/// [`BidiStream::message`]. Without the spawn, a transport whose `send()`
/// future contains the actual connect/stream work (e.g.,
/// [`SharedHttp2Connection`]) would not initiate the request until
/// `message()` is called — so the half-duplex pattern (send all, then
/// read) would buffer into the 32-deep mpsc with nobody draining it, and
/// deadlock on the 33rd send.
///
/// Response initialization is still lazy: `message()` first awaits response
/// HEADERS, then constructs the [`ServerStream`]. Both pending operations stay
/// in this state machine while awaited, so cancelling `message()` does not
/// discard either the response task or a suspended construction step such as
/// Connect error-body parsing. Dropping the [`BidiRecvHalf`] that owns this
/// state (or failing the call at its deadline) aborts the in-flight task
/// instead.
enum RecvState<B, RespView> {
    /// Request initiated in a spawned task; response HEADERS not yet
    /// received. Awaiting the handle yields the [`Response`] once hyper
    /// reads the HEADERS frame.
    AwaitingHeaders(tokio::task::JoinHandle<Result<Response<B>, ConnectError>>),
    /// HEADERS received; response-side stream construction is in progress.
    Constructing(tokio::task::JoinHandle<Result<Box<ServerStream<B, RespView>>, ConnectError>>),
    /// HEADERS received; response-side decoding delegates to [`ServerStream`].
    Ready(Box<ServerStream<B, RespView>>),
    /// Transport error, deadline, or make_server_stream error. Terminal state.
    Failed(ConnectError),
}

/// A bidirectional streaming RPC in progress.
///
/// Returned from [`call_bidi_stream`]. Provides a `send`/`close_send`/`message`
/// API modeled on connect-go's `BidiStreamForClient`.
///
/// # Half-duplex vs full-duplex
///
/// The Connect spec supports both. Half-duplex (send all, then receive all)
/// works on HTTP/1.1 and HTTP/2. Full-duplex (interleaved send/receive) requires
/// HTTP/2. This type does not distinguish — it's the caller's responsibility to
/// respect the protocol in use. On HTTP/1.1, calling `message()` before
/// `close_send()` will block until the request body is complete.
///
/// To drive the two sides from separate tasks, split the stream into
/// independently owned halves with [`into_split()`](Self::into_split).
///
/// # Cancellation
///
/// Dropping the `BidiStream` cancels the call: any in-flight initialization
/// task is aborted, which resets the underlying transport stream. Request
/// messages accepted by [`send()`](Self::send) but not yet transmitted may
/// never reach the server — a caller that needs the request delivered must
/// drive the call to completion via [`message()`](Self::message) before
/// dropping. Cancelling an individual `message()` future is safe and
/// resumable — see [`message()`](Self::message).
///
/// # Example
///
/// ```rust,ignore
/// let mut stream = call_bidi_stream(&transport, &config, "svc", "method", CallOptions::default()).await?;
/// stream.send(request1).await?;
/// stream.send(request2).await?;
/// stream.close_send();
/// // `Ok(None)` means a clean end; a failed RPC surfaces as `Err`,
/// // so `?` is the complete error handling.
/// while let Some(msg) = stream.message().await? {
///     println!("got: {msg:?}");
/// }
/// ```
pub struct BidiStream<B, Req, RespView> {
    // Field order is load-bearing for drop: `send` drops first (clean
    // request-body EOF), then `recv`'s Drop aborts any in-flight
    // initialization task. The glue between the two field drops is
    // synchronous, so the spawned task cannot advance in between — the
    // abort still catches anything the old whole-struct Drop would have.
    send: BidiSendHalf<Req>,
    recv: BidiRecvHalf<B, RespView>,
}

// Manual impl: delegate to the halves, which carry the useful state.
impl<B, Req, RespView> std::fmt::Debug for BidiStream<B, Req, RespView> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BidiStream")
            .field("send", &self.send)
            .field("recv", &self.recv)
            .finish()
    }
}

/// The send half of a [`BidiStream`], returned by
/// [`BidiStream::into_split`].
///
/// Owns the request side of the RPC: [`send()`](Self::send) and
/// [`close_send()`](Self::close_send). Dropping the half without calling
/// `close_send` closes the send side the same way (the request body ends
/// cleanly); the RPC itself stays alive as long as the [`BidiRecvHalf`]
/// does. The halves cannot be recombined into a [`BidiStream`].
pub struct BidiSendHalf<Req> {
    tx: Option<tokio::sync::mpsc::Sender<Result<Bytes, ConnectError>>>,
    encoder: crate::envelope::EnvelopeEncoder,
    codec_format: CodecFormat,
    /// Copy of the whole-call deadline; checked before each send.
    deadline: Option<std::time::Instant>,
    _req: PhantomData<Req>,
}

impl<Req> std::fmt::Debug for BidiSendHalf<Req> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BidiSendHalf")
            .field("send_closed", &self.tx.is_none())
            .field("codec_format", &self.codec_format)
            .finish_non_exhaustive()
    }
}

/// The receive half of a [`BidiStream`], returned by
/// [`BidiStream::into_split`].
///
/// Owns the response side of the RPC: [`message()`](Self::message) plus the
/// [`headers()`](Self::headers), [`trailers()`](Self::trailers), and
/// [`error()`](Self::error) accessors. Dropping this half cancels the RPC
/// (any in-flight initialization task is aborted and the transport stream
/// is reset), after which sends on the [`BidiSendHalf`] fail. The halves
/// cannot be recombined into a [`BidiStream`].
pub struct BidiRecvHalf<B, RespView> {
    // State machine: AwaitingHeaders -> Constructing -> Ready or Failed
    recv: RecvState<B, RespView>,
    /// Config snapshot for constructing ServerStream when headers arrive.
    /// Captured by value (not &) because the stream outlives call_bidi_stream.
    stream_config: StreamConfig,
}

impl<B, RespView> std::fmt::Debug for BidiRecvHalf<B, RespView> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (recv_state, recv_error) = match &self.recv {
            RecvState::AwaitingHeaders(_) => ("AwaitingHeaders", None),
            RecvState::Constructing(_) => ("Constructing", None),
            RecvState::Ready(_) => ("Ready", None),
            RecvState::Failed(err) => ("Failed", Some(err)),
        };
        f.debug_struct("BidiRecvHalf")
            .field("recv_state", &recv_state)
            .field("protocol", &self.stream_config.protocol)
            .field("codec_format", &self.stream_config.codec_format)
            .field("recv_error", &recv_error)
            .finish_non_exhaustive()
    }
}

// Dropping the receive half aborts any in-flight initialization task.
// Without this, a task left in `AwaitingHeaders` or `Constructing` would
// detach on drop and — absent a call deadline — could be pinned indefinitely
// by a server that stalls response HEADERS or a Connect error body without
// ever ending the stream. Abandoning the receive half abandons the RPC, so
// nothing can consume the task's result anyway. (This also covers dropping
// a whole `BidiStream`, which contains this half.)
impl<B, RespView> Drop for BidiRecvHalf<B, RespView> {
    fn drop(&mut self) {
        match &self.recv {
            RecvState::AwaitingHeaders(task) => task.abort(),
            RecvState::Constructing(task) => task.abort(),
            RecvState::Ready(_) | RecvState::Failed(_) => {}
        }
    }
}

/// Snapshot of ClientConfig fields needed to construct the inner ServerStream
/// once response headers arrive. Avoids holding a borrow across awaits.
#[derive(Debug)]
struct StreamConfig {
    protocol: Protocol,
    codec_format: CodecFormat,
    compression: CompressionRegistry,
    max_message_size: Option<usize>,
    deadline: Option<std::time::Instant>,
}

impl<Req> BidiSendHalf<Req>
where
    Req: buffa::Message + crate::codec::JsonSerialize,
{
    /// Send a request message.
    ///
    /// # Errors
    ///
    /// Returns an error if [`close_send`](Self::close_send) was already
    /// called, if the whole-call deadline has passed, or if the server has
    /// closed the stream. In the latter case, receive on the other half —
    /// [`BidiRecvHalf::message()`] — to retrieve the server's error. (The
    /// same error is returned when the [`BidiRecvHalf`] was dropped, which
    /// cancels the RPC.)
    pub async fn send(&mut self, msg: Req) -> Result<(), ConnectError> {
        // Check the whole-call deadline before each send, matching
        // connect-go's ctx.Err() check in duplexHTTPCall.Send().
        if let Some(d) = self.deadline
            && std::time::Instant::now() >= d
        {
            return Err(ConnectError::deadline_exceeded(
                "client-side deadline exceeded",
            ));
        }

        let Some(tx) = &self.tx else {
            return Err(ConnectError::internal("send after close_send"));
        };

        // Encode message (proto or JSON) then envelope-frame (with optional
        // compression). Same logic as call_server_stream's request encoding.
        let msg_bytes = match self.codec_format {
            CodecFormat::Proto => msg.encode_to_bytes(),
            CodecFormat::Json => encode_json(&msg)?,
        };

        let mut envelope_buf = BytesMut::new();
        tokio_util::codec::Encoder::encode(&mut self.encoder, msg_bytes, &mut envelope_buf)?;

        tx.send(Ok(envelope_buf.freeze())).await.map_err(|_| {
            // Channel receiver dropped — the body stream has been consumed
            // or the HTTP request task has exited. The server likely sent
            // an error; message() will surface it.
            ConnectError::unavailable("stream closed by server (call message() for error)")
        })
    }

    /// Close the send side of the stream. Idempotent.
    ///
    /// After this, only receiving is possible. For half-duplex use
    /// (HTTP/1.1), this must be called before receiving. Dropping the half
    /// has the same effect.
    pub fn close_send(&mut self) {
        self.tx = None; // drop sender → channel closes → body signals EOF
    }
}

impl<B, RespView> BidiRecvHalf<B, RespView>
where
    B: Body<Data = Bytes> + Send + Unpin,
    B::Error: std::fmt::Display,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    /// Receive the next response message.
    ///
    /// The first call awaits response headers (lazily, so full-duplex
    /// servers that wait for a request before sending headers don't deadlock).
    /// Subsequent calls decode envelopes from the response body stream.
    /// If this future is dropped while response initialization is still pending,
    /// the initialization remains in the stream and the next `message()` call
    /// resumes it. Actual initialization failures remain terminal and sticky.
    ///
    /// # Errors
    ///
    /// Returns `Ok(None)` only when the server finished **cleanly**; a
    /// server error carried in the termination metadata is returned as
    /// `Err`, sticky across calls — see [`ServerStream::message()`] for the
    /// full contract.
    pub async fn message<M>(&mut self) -> Result<Option<crate::StreamMessage<M>>, ConnectError>
    where
        // Same output-parameter shape as `ServerStream::message` — see the
        // bound comment there (#214). `B` and `RespView` must also be
        // `'static` because response-side construction is retained in a
        // spawned task while the caller's `message()` future may be dropped.
        B: 'static,
        RespView: MessageView<'static, Owned = M> + 'static,
        M: HasMessageView<View<'static> = RespView>,
    {
        loop {
            match &mut self.recv {
                RecvState::AwaitingHeaders(task) => {
                    // Bound the response-HEADERS wait by the whole-call
                    // deadline. The JoinHandle stays in `self.recv` while it
                    // is pending, so cancelling this `message()` future does
                    // not detach the task or lose its eventual response.
                    let response = match with_deadline(self.stream_config.deadline, async {
                        // Reborrow rather than move so `task` stays usable
                        // for the abort in the failure arm below.
                        (&mut *task).await.map_err(|e| {
                            // JoinError's Display already distinguishes
                            // panic from cancellation.
                            ConnectError::internal(format!("transport send task failed: {e}"))
                        })?
                    })
                    .await
                    {
                        Ok(response) => response,
                        Err(e) => {
                            // Deadline (or join) failure is terminal: abort
                            // the response task rather than detaching it —
                            // the RPC is dead, so nothing will ever consume
                            // its result. No-op if the task already finished.
                            task.abort();
                            self.recv = RecvState::Failed(e.clone());
                            return Err(e);
                        }
                    };

                    let protocol = self.stream_config.protocol;
                    let codec_format = self.stream_config.codec_format;
                    let compression = self.stream_config.compression.clone();
                    let max_message_size = self.stream_config.max_message_size;
                    let deadline = self.stream_config.deadline;

                    let construct_task = tokio::spawn(async move {
                        // `make_server_stream` can await while collecting a
                        // non-200 Connect error body, before a `ServerStream`
                        // exists to enforce the call deadline.
                        let stream = with_deadline(
                            deadline,
                            make_server_stream(
                                response,
                                protocol,
                                &compression,
                                codec_format,
                                max_message_size,
                                deadline,
                            ),
                        )
                        .await?;

                        Ok(Box::new(stream))
                    });

                    self.recv = RecvState::Constructing(construct_task);
                }
                RecvState::Constructing(task) => {
                    // The construction task stays in `self.recv` while it is
                    // pending, so cancellation during Connect error-body
                    // collection can be resumed by the next `message()` call.
                    let result = match task.await {
                        Ok(result) => result,
                        Err(e) => Err(ConnectError::internal(format!(
                            "response stream construction task failed: {e}"
                        ))),
                    };

                    match result {
                        Ok(stream) => self.recv = RecvState::Ready(stream),
                        Err(e) => {
                            self.recv = RecvState::Failed(e.clone());
                            return Err(e);
                        }
                    }
                }
                RecvState::Ready(stream) => return stream.message().await,
                RecvState::Failed(e) => return Err(e.clone()),
            }
        }
    }

    /// Response headers. `None` until the first [`message()`](Self::message)
    /// call completes response initialization (a cancelled first `message()`
    /// can leave this `None` even after the HEADERS frame arrived).
    #[must_use]
    pub fn headers(&self) -> Option<&http::HeaderMap> {
        match &self.recv {
            RecvState::Ready(s) => Some(s.headers()),
            _ => None,
        }
    }

    /// Trailing metadata. Only populated after [`message()`](Self::message)
    /// reports the end of the stream (`Ok(None)` or the terminal `Err`).
    #[must_use]
    pub fn trailers(&self) -> Option<&http::HeaderMap> {
        match &self.recv {
            RecvState::Ready(s) => s.trailers(),
            _ => None,
        }
    }

    /// Terminal error that ended the stream, if any — a server error from
    /// the END_STREAM envelope (Connect) or trailers (gRPC), or a
    /// decode/transport/deadline failure. [`message()`](Self::message)
    /// already returns this same error, so most callers never need this
    /// accessor; it exists for post-hoc inspection alongside
    /// [`trailers()`](Self::trailers). Returns `None` while response
    /// initialization is still in progress — including after a cancelled
    /// first `message()` call whose retained initialization has since
    /// failed; call `message()` again to surface that error.
    #[must_use]
    pub fn error(&self) -> Option<&ConnectError> {
        match &self.recv {
            RecvState::Ready(s) => s.error(),
            RecvState::Failed(e) => Some(e),
            _ => None,
        }
    }
}

impl<B, Req, RespView> BidiStream<B, Req, RespView> {
    /// Split the stream into independently owned send and receive halves,
    /// so the two sides can be driven from separate tasks (full duplex).
    ///
    /// Interleaved, response-dependent use — receiving an answer before
    /// sending the next message — requires an HTTP/2 transport, exactly as
    /// with an unsplit stream: on HTTP/1.1 no response arrives until the
    /// request body is complete, so a task waiting on the other half's
    /// progress deadlocks. Prefer moving each half into its own spawned
    /// task (as below) over storing them in named struct fields — the
    /// halves' full type parameters include the transport body type, which
    /// task-local inference names for you.
    ///
    /// The halves are plain moves of the stream's two sides — no locking is
    /// added — and there is no way to reassemble them. Semantics carried by
    /// each half:
    ///
    /// - Dropping the [`BidiSendHalf`] (or calling
    ///   [`close_send()`](BidiSendHalf::close_send)) ends the request body
    ///   cleanly; the RPC continues until the receive half finishes.
    /// - Dropping the [`BidiRecvHalf`] cancels the RPC — as when dropping a
    ///   whole `BidiStream` — after which sends on the other half fail.
    /// - When [`send()`](BidiSendHalf::send) fails because the server closed
    ///   the stream, the server's error is retrieved from the *receive* half
    ///   via [`message()`](BidiRecvHalf::message).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let (mut send, mut recv) = stream.into_split();
    /// let reader = tokio::spawn(async move {
    ///     while let Some(msg) = recv.message().await? {
    ///         println!("got: {msg:?}");
    ///     }
    ///     Ok::<_, connectrpc::ConnectError>(())
    /// });
    /// for req in requests {
    ///     send.send(req).await?;
    /// }
    /// send.close_send();
    /// reader.await.expect("reader task")?;
    /// ```
    #[must_use]
    pub fn into_split(self) -> (BidiSendHalf<Req>, BidiRecvHalf<B, RespView>) {
        (self.send, self.recv)
    }
}

impl<B, Req, RespView> BidiStream<B, Req, RespView>
where
    B: Body<Data = Bytes> + Send + Unpin,
    B::Error: std::fmt::Display,
    Req: buffa::Message + crate::codec::JsonSerialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    /// Send a request message.
    ///
    /// # Errors
    ///
    /// See [`BidiSendHalf::send`] for the error contract.
    pub async fn send(&mut self, msg: Req) -> Result<(), ConnectError> {
        self.send.send(msg).await
    }

    /// Close the send side of the stream. Idempotent.
    /// See [`BidiSendHalf::close_send`].
    pub fn close_send(&mut self) {
        self.send.close_send();
    }

    /// Receive the next response message.
    ///
    /// # Errors
    ///
    /// See [`BidiRecvHalf::message`] for the full contract.
    pub async fn message<M>(&mut self) -> Result<Option<crate::StreamMessage<M>>, ConnectError>
    where
        B: 'static,
        RespView: MessageView<'static, Owned = M> + 'static,
        M: HasMessageView<View<'static> = RespView>,
    {
        self.recv.message().await
    }

    /// Response headers. See [`BidiRecvHalf::headers`].
    #[must_use]
    pub fn headers(&self) -> Option<&http::HeaderMap> {
        self.recv.headers()
    }

    /// Trailing metadata. See [`BidiRecvHalf::trailers`].
    #[must_use]
    pub fn trailers(&self) -> Option<&http::HeaderMap> {
        self.recv.trailers()
    }

    /// Terminal error that ended the stream, if any.
    /// See [`BidiRecvHalf::error`].
    #[must_use]
    pub fn error(&self) -> Option<&ConnectError> {
        self.recv.error()
    }
}

/// Make a bidirectional-streaming RPC call.
///
/// Opens a stream to the server and returns a [`BidiStream`] handle for
/// sending request messages and receiving responses. No messages are sent
/// until the first [`BidiStream::send`] call.
///
/// The response future is stored and awaited lazily on the first
/// [`BidiStream::message`] call — this supports full-duplex servers that
/// wait for the first request message before sending response headers.
///
/// # Example
///
/// ```rust,ignore
/// let mut stream = call_bidi_stream::<_, MyReq, MyRespView>(
///     &transport, &config, "my.Service", "Method", CallOptions::default(),
/// ).await?;
/// stream.send(req).await?;
/// stream.close_send();
/// while let Some(msg) = stream.message().await? { /* ... */ }
/// ```
pub async fn call_bidi_stream<T, Req, RespView>(
    transport: &T,
    config: &ClientConfig,
    service: &str,
    method: &str,
    options: CallOptions,
) -> Result<BidiStream<T::ResponseBody, Req, RespView>, ConnectError>
where
    T: ClientTransport,
    <T::ResponseBody as Body>::Error: std::fmt::Display,
    Req: buffa::Message + crate::codec::JsonSerialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    let options = effective_options(config, options);

    // Build the full URI from base_uri and service/method path
    let base_str = config.base_uri.to_string();
    let base_str = base_str.trim_end_matches('/');
    let full_uri = format!("{base_str}/{service}/{method}");
    let uri: Uri = full_uri
        .parse()
        .map_err(|e| ConnectError::internal(format!("invalid URI: {e}")))?;

    // Set up the channel-backed request body. Channel depth 32 matches
    // typical h2 stream window; sends beyond this backpressure naturally.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, ConnectError>>(32);
    let body: ClientBody = ChannelBody { rx }.boxed();

    // Envelope encoder for send() — same compression setup as server-stream.
    let compression_for_encoder = config.request_compression.as_ref().map(|enc| {
        (
            std::sync::Arc::new(config.compression.clone()),
            enc.as_str(),
        )
    });
    let encoder = crate::envelope::EnvelopeEncoder::new(
        compression_for_encoder,
        config.compression_policy.with_override(options.compress),
    );

    let deadline = client_deadline(options.timeout, config.protocol);

    // Build the HTTP request with protocol-aware streaming headers
    let mut builder = Request::builder().method(http::Method::POST).uri(uri);
    builder = add_streaming_request_headers(builder, config, options.timeout);

    let headers = builder.headers_mut().unwrap();
    for (name, value) in &options.headers {
        headers.append(name.clone(), value.clone());
    }

    let http_request = builder
        .body(body)
        .map_err(|e| ConnectError::internal(format!("failed to build request: {e}")))?;

    // Spawn the transport send so the request initiates immediately and
    // ChannelBody gets polled as sends happen, independent of when the
    // caller first calls message(). See RecvState doc for the deadlock
    // this avoids.
    //
    // Uses tokio::spawn directly (not spawn_detached) because
    // RecvState::AwaitingHeaders needs JoinHandle<Result<...>>. There is a
    // second such site: `message()` spawns the RecvState::Constructing task.
    // If wasm32+client becomes supported, factor both into a
    // spawn_with_result helper that bridges via oneshot on wasm.
    let response_fut = transport.send(http_request);
    let response_task = tokio::spawn(async move {
        response_fut
            .await
            .map_err(|e| map_transport_send_error(e, "request failed"))
    });

    Ok(BidiStream {
        send: BidiSendHalf {
            tx: Some(tx),
            encoder,
            codec_format: config.codec_format,
            deadline,
            _req: PhantomData,
        },
        recv: BidiRecvHalf {
            recv: RecvState::AwaitingHeaders(response_task),
            stream_config: StreamConfig {
                protocol: config.protocol,
                codec_format: config.codec_format,
                compression: config.compression.clone(),
                max_message_size: options.max_message_size,
                deadline,
            },
        },
    })
}

/// Make a client-streaming RPC call.
///
/// Sends multiple request messages as envelope-framed data and receives a single
/// envelope-framed response with END_STREAM. Returns a [`UnaryResponse`] containing
/// the decoded response message along with headers and trailers.
///
/// The request body IS the stream: each item yielded by `requests` is
/// encoded into an envelope frame as the transport asks for the next chunk
/// of body data. The transport begins sending as soon as the first message
/// is available, backpressure is the HTTP layer's own flow control, and
/// peak memory stays around one envelope rather than the full concatenated
/// body.
///
/// `requests` is an asynchronous [`Stream`], so messages can be produced as
/// they become available (paced by timers, read from sockets, forwarded from
/// channels) without buffering the whole request up front. The
/// [`ClientRequestStream`] bound additionally requires `Send + 'static`
/// because the stream backs the request body, which can outlive the call
/// frame and move across threads — yield owned messages (no borrows of
/// local data), or feed the call from a channel-backed stream. For a
/// collection that is already in hand, wrap it with [`stream_iter`]:
///
/// ```rust,ignore
/// let resp = call_client_stream(
///     &transport, &config, "svc", "Method",
///     connectrpc::stream_iter(vec![req1, req2]),
///     CallOptions::default(),
/// ).await?;
/// ```
///
/// Because the transport owns the polling of `requests`, upload liveness
/// follows HTTP semantics: a server that ends the RPC while `requests` is
/// still pending (for example, rejecting the call partway through the
/// upload) produces a response and the call returns without draining the
/// stream, while a server that merely sends response headers early and
/// keeps consuming the upload keeps receiving messages.
///
/// # Cancellation
///
/// Dropping the returned future (caller cancellation) or letting its deadline
/// expire drops the in-flight transport send — and with it the request body
/// and the caller's stream — even if the transport is still waiting for
/// response headers. As a consequence, a request that was still being sent
/// when the call was abandoned may never reach the server; a caller that
/// needs the request delivered must drive the call to completion.
///
/// # Errors
///
/// Returns an error if a request message cannot be encoded, the transport
/// fails, the whole-call deadline expires, the server responds with an
/// error, or the response cannot be decoded.
pub async fn call_client_stream<T, Req, RespView>(
    transport: &T,
    config: &ClientConfig,
    service: &str,
    method: &str,
    requests: impl ClientRequestStream<Req>,
    options: CallOptions,
) -> Result<UnaryResponse<OwnedView<RespView>>, ConnectError>
where
    T: ClientTransport,
    <T::ResponseBody as Body>::Error: std::fmt::Display,
    Req: buffa::Message + crate::codec::JsonSerialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    let options = effective_options(config, options);

    // Build the full URI from base_uri and service/method path
    let base_str = config.base_uri.to_string();
    let base_str = base_str.trim_end_matches('/');
    let full_uri = format!("{base_str}/{service}/{method}");
    let uri: Uri = full_uri
        .parse()
        .map_err(|e| ConnectError::internal(format!("invalid URI: {e}")))?;

    let compression_for_encoder = config.request_compression.as_ref().map(|enc| {
        (
            std::sync::Arc::new(config.compression.clone()),
            enc.as_str(),
        )
    });
    let encoder = crate::envelope::EnvelopeEncoder::new(
        compression_for_encoder,
        config.compression_policy.with_override(options.compress),
    );

    // The stream backs the request body directly: the transport polls it for
    // the next frame as it is able to send. An encode failure is reported
    // through the body (aborting the request) and stashed here so the call
    // can surface the precise error instead of a generic transport failure.
    let encode_error: std::sync::Arc<std::sync::Mutex<Option<ConnectError>>> =
        std::sync::Arc::default();
    let body: ClientBody = EncodingBody {
        stream: sync_wrapper::SyncWrapper::new(requests),
        encoder,
        codec_format: config.codec_format,
        error: encode_error.clone(),
        done: false,
    }
    .boxed();

    // Compute deadline BEFORE sending, matching Go's ctx.Deadline() semantics
    let deadline = client_deadline(options.timeout, config.protocol);

    // Build the HTTP request with protocol-aware streaming headers
    let mut builder = Request::builder().method(http::Method::POST).uri(uri);
    builder = add_streaming_request_headers(builder, config, options.timeout);

    // Merge user-provided headers
    let headers = builder.headers_mut().unwrap();
    for (name, value) in &options.headers {
        headers.append(name.clone(), value.clone());
    }

    let http_request = builder
        .body(body)
        .map_err(|e| ConnectError::internal(format!("failed to build request: {e}")))?;

    // Enforce the client-side deadline on send + parse. The transport polls
    // the request body (and therefore the caller's stream) while this send
    // future — or, for connection-driver transports, their background task —
    // makes progress; there is no library-side pump that could hang on an
    // idle stream or cut off an upload the server is still consuming.
    // Abandonment (dropping the call future, or the deadline firing) drops
    // the send future — and with it the request — directly: there is no
    // detached task to outlive the call (#224).
    let result = with_deadline(deadline, async {
        let response = transport
            .send(http_request)
            .await
            .map_err(|e| map_transport_send_error(e, "request failed"))?;

        // For gRPC, the response is envelope-framed like a unary gRPC response
        // (single data envelope + trailers). Reuse parse_grpc_unary_response.
        match config.protocol {
            Protocol::Grpc | Protocol::GrpcWeb => {
                parse_grpc_unary_response(response, config, &options, deadline).await
            }
            Protocol::Connect => {
                parse_connect_client_stream_response(response, config, &options).await
            }
        }
    })
    .await;

    // An encode failure aborts the request at the transport level; surface
    // the precise encode error instead of the generic transport failure —
    // unconditionally, because a server's early response can race the abort
    // and produce an `Ok` result for a truncated, encode-aborted upload.
    //
    // The race runs the other way too, and that direction is left alone on
    // purpose: with the body driven in the background, an encode failure can
    // land after this check and is then never read, so the call reports the
    // server's `Ok`. That is the intended outcome — the server had already
    // produced a complete response, so the truncated tail did not affect it.
    if let Some(err) = encode_error
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        return Err(err);
    }
    result
}

/// Parse a Connect protocol client-streaming response.
async fn parse_connect_client_stream_response<B, RespView>(
    response: Response<B>,
    config: &ClientConfig,
    options: &CallOptions,
) -> Result<UnaryResponse<OwnedView<RespView>>, ConnectError>
where
    B: Body<Data = Bytes> + Send,
    B::Error: std::fmt::Display,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + crate::codec::JsonDeserialize,
{
    let status = response.status();

    if !status.is_success() {
        let response_headers = response.headers().clone();

        let error_encoding = response_headers
            .get(http::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        let max_err_size = options
            .max_message_size
            .unwrap_or(crate::service::DEFAULT_MAX_MESSAGE_SIZE);

        let body = collect_body_bounded(response.into_body(), max_err_size).await?;

        // Decompress if the server set Content-Encoding. On failure,
        // fall through to the generic HTTP-status error below.
        let body = match error_encoding {
            Some(encoding) => {
                match config
                    .compression
                    .decompress_with_limit(&encoding, body, max_err_size)
                {
                    Ok(decompressed) => Some(decompressed),
                    Err(e) => {
                        tracing::debug!(
                            "failed to decompress Connect error response ({encoding}): {e}"
                        );
                        None
                    }
                }
            }
            None => Some(body),
        };

        if let Some(body) = body
            && let Ok(error) = serde_json::from_slice::<ConnectErrorResponse>(&body)
        {
            let code = error
                .code
                .as_deref()
                .and_then(|s| s.parse::<ErrorCode>().ok())
                .unwrap_or_else(|| http_status_to_error_code(status));
            let mut err = ConnectError::new(code, error.message.unwrap_or_default());
            err.details = error.details;
            err.set_response_headers(response_headers);
            return Err(err);
        }

        let code = http_status_to_error_code(status);
        let mut err = ConnectError::new(code, format!("HTTP error {}", status.as_u16()));
        err.set_response_headers(response_headers);
        return Err(err);
    }

    let encoding = response
        .headers()
        .get(config.protocol.content_encoding_header())
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    let resp_headers = response.headers().clone();

    let max_msg_size = options
        .max_message_size
        .unwrap_or(crate::service::DEFAULT_MAX_MESSAGE_SIZE);

    // The Connect client-stream response body holds a data envelope (header +
    // payload up to max_msg_size) followed by an END_STREAM envelope (header +
    // JSON trailers/error). Add slack so a max-sized message is not falsely
    // rejected by the whole-body cap.
    let body_limit = max_msg_size
        .saturating_add(2 * crate::envelope::HEADER_SIZE)
        .saturating_add(RESPONSE_BUFFER_TRAILER_SLACK);
    let body = collect_body_bounded(response.into_body(), body_limit).await?;

    let (data, trailers) = parse_connect_client_stream_envelopes(
        body,
        &config.compression,
        encoding.as_deref(),
        max_msg_size,
        &resp_headers,
    )?;
    let message = decode_response_view::<RespView>(data, config.codec_format)?;

    Ok(UnaryResponse {
        headers: resp_headers,
        body: message,
        trailers,
    })
}

/// Scan a collected Connect client-streaming response body.
///
/// The body must contain exactly one data envelope followed by an END_STREAM
/// envelope, the protocol-level terminus: it supplies the trailers (or a
/// terminal Connect error) and marks the response complete. A body that
/// yields the data message and then ends before END_STREAM is truncated, not
/// successful, and is rejected with `internal` (matching the `ServerStream`
/// Connect EOF behavior and connect-go's classification of a missing
/// terminus as a wire-level error). Returns the (still encoded) message
/// payload and any
/// trailers carried in the END_STREAM metadata.
///
/// Scanning stops at END_STREAM, so anything after it is ignored rather than
/// decoded. A second data envelope is rejected before its payload is
/// decompressed, so the client never spends decompression work or memory on
/// more than the single message the RPC allows.
fn parse_connect_client_stream_envelopes(
    body: Bytes,
    compression: &crate::compression::CompressionRegistry,
    encoding: Option<&str>,
    max_msg_size: usize,
    resp_headers: &http::HeaderMap,
) -> Result<(Bytes, http::HeaderMap), ConnectError> {
    let mut buf = BytesMut::from(body.as_ref());
    let mut message: Option<Bytes> = None;
    let mut trailers = http::HeaderMap::new();
    let mut saw_end_stream = false;

    while !buf.is_empty() {
        let envelope = match Envelope::decode_with_limit(&mut buf, max_msg_size)? {
            Some(env) => env,
            None => break,
        };

        if envelope.is_end_stream() {
            saw_end_stream = true;
            let end_stream_data = if envelope.is_compressed() {
                let enc = encoding.ok_or_else(|| {
                    ConnectError::internal("received compressed END_STREAM without encoding header")
                })?;
                compression
                    .decompress_with_limit(enc, envelope.data, max_msg_size)
                    .map_err(map_response_decompression_error)?
            } else {
                envelope.data
            };

            let end_stream = parse_connect_end_stream(&end_stream_data).map_err(|mut err| {
                err.set_response_headers(resp_headers.clone());
                err
            })?;

            if let Some(metadata) = end_stream.metadata {
                append_metadata_capped(&mut trailers, metadata);
            }

            if let Some(err) = end_stream.error {
                let mut connect_error = end_stream_error_to_connect_error(err);
                connect_error.set_response_headers(resp_headers.clone());
                connect_error.set_trailers(trailers);
                return Err(connect_error);
            }

            // END_STREAM is the end of the logical response stream; stop
            // scanning so trailing bytes after it are ignored.
            if !buf.is_empty() {
                tracing::debug!(
                    trailing_bytes = buf.len(),
                    "ignoring response data after the END_STREAM envelope"
                );
            }
            break;
        }

        // Reject a second data message before doing any further work on it —
        // in particular before decompressing its payload.
        if message.is_some() {
            return Err(ConnectError::unimplemented(
                "client streaming response contains multiple data messages",
            ));
        }

        let data = if envelope.is_compressed() {
            let enc = encoding.ok_or_else(|| {
                ConnectError::internal("received compressed message without encoding header")
            })?;
            compression
                .decompress_with_limit(enc, envelope.data, max_msg_size)
                .map_err(map_response_decompression_error)?
        } else {
            envelope.data
        };

        if data.len() > max_msg_size {
            return Err(ConnectError::new(
                ErrorCode::ResourceExhausted,
                format!("message size {} exceeds limit {}", data.len(), max_msg_size),
            ));
        }

        message = Some(data);
    }

    let message = message.ok_or_else(|| {
        ConnectError::unimplemented("client streaming response contains no data messages")
    })?;

    // The data message is present, but the body ended before END_STREAM. That
    // is a truncated response, not a completed one — match ServerStream's
    // Connect EOF handling rather than reporting success with no trailers.
    if !saw_end_stream {
        return Err(ConnectError::internal(
            "Connect streaming response ended without END_STREAM envelope",
        ));
    }

    Ok((message, trailers))
}

/// EndStreamResponse as received by the client.
#[derive(serde::Deserialize)]
struct ClientEndStreamResponse {
    error: Option<ClientEndStreamError>,
    metadata: Option<HashMap<String, Vec<String>>>,
}

/// Error in the EndStreamResponse.
#[derive(serde::Deserialize)]
struct ClientEndStreamError {
    code: Option<String>,
    message: Option<String>,
    #[serde(default)]
    details: Vec<ErrorDetail>,
}

/// Parse the body of a Connect END_STREAM envelope. A malformed body is a
/// wire-protocol violation, so it surfaces as `Internal` (matching connect-go)
/// rather than being silently treated as a clean close.
fn parse_connect_end_stream(data: &[u8]) -> Result<ClientEndStreamResponse, ConnectError> {
    serde_json::from_slice(data).map_err(|e| {
        ConnectError::internal(format!(
            "protocol error: malformed Connect END_STREAM JSON: {e}"
        ))
    })
}

/// Convert the `error` member of a Connect END_STREAM body into the
/// caller-facing [`ConnectError`], with `Unknown` as the fallback code.
fn end_stream_error_to_connect_error(err: ClientEndStreamError) -> ConnectError {
    let mut connect_error = ConnectError::new(
        err.code
            .as_deref()
            .and_then(|c| c.parse().ok())
            .unwrap_or(ErrorCode::Unknown),
        err.message.unwrap_or_default(),
    );
    connect_error.details = err.details;
    connect_error
}

/// Error response structure from ConnectRPC.
#[derive(serde::Deserialize)]
struct ConnectErrorResponse {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    details: Vec<ErrorDetail>,
}

/// Maps an HTTP status code to a Connect error code per the Connect protocol spec.
///
/// Only specific HTTP status codes have defined mappings. All other codes map to
/// `Unknown` per the specification.
fn http_status_to_error_code(status: http::StatusCode) -> ErrorCode {
    match status.as_u16() {
        400 => ErrorCode::Internal,
        401 => ErrorCode::Unauthenticated,
        403 => ErrorCode::PermissionDenied,
        404 => ErrorCode::Unimplemented,
        408 => ErrorCode::DeadlineExceeded,
        429 => ErrorCode::Unavailable,
        502 => ErrorCode::Unavailable,
        503 => ErrorCode::Unavailable,
        504 => ErrorCode::Unavailable,
        _ => ErrorCode::Unknown,
    }
}

// ============================================================================
// Protocol-aware client helpers
// ============================================================================

/// Get the content type for a unary request.
fn unary_request_content_type(config: &ClientConfig) -> &'static str {
    match config.protocol {
        Protocol::Connect => config.codec_format.content_type(),
        Protocol::Grpc | Protocol::GrpcWeb => config
            .protocol
            .response_content_type(config.codec_format, false),
    }
}

/// Get the content type for a streaming request.
fn streaming_request_content_type(config: &ClientConfig) -> &'static str {
    config
        .protocol
        .response_content_type(config.codec_format, true)
}

/// Format a timeout value for the protocol's timeout header.
fn format_timeout(timeout: Duration, protocol: Protocol) -> String {
    encoded_timeout(timeout, protocol).header_value()
}

/// Add protocol-specific headers to a request builder for unary RPCs.
///
/// `applied_content_encoding` is the encoding that was ACTUALLY applied to
/// the Connect unary body (or `None` if the body was sent uncompressed,
/// e.g. because the compression policy's size threshold was not met). For
/// gRPC/gRPC-Web this is ignored — `grpc-encoding` is a capability
/// declaration and the per-message envelope flag signals actual compression.
fn add_unary_request_headers(
    mut builder: http::request::Builder,
    config: &ClientConfig,
    timeout: Option<Duration>,
    applied_content_encoding: Option<&str>,
) -> http::request::Builder {
    builder = builder.header(
        http::header::CONTENT_TYPE,
        unary_request_content_type(config),
    );

    match config.protocol {
        Protocol::Connect => {
            builder = builder.header(connect_header::PROTOCOL_VERSION, "1");
            // Connect unary uses standard content-encoding / accept-encoding.
            // Only set Content-Encoding if compression was actually applied.
            if let Some(encoding) = applied_content_encoding {
                builder = builder.header(http::header::CONTENT_ENCODING, encoding);
            }
            let accept = config.compression.accept_encoding_header();
            if !accept.is_empty() {
                builder = builder.header(http::header::ACCEPT_ENCODING, accept);
            }
        }
        Protocol::Grpc => {
            builder = builder.header("te", "trailers");
            if let Some(ref encoding) = config.request_compression {
                builder = builder.header("grpc-encoding", encoding.as_str());
            }
            let accept = config.compression.accept_encoding_header();
            if !accept.is_empty() {
                builder = builder.header("grpc-accept-encoding", accept);
            }
        }
        Protocol::GrpcWeb => {
            if let Some(ref encoding) = config.request_compression {
                builder = builder.header("grpc-encoding", encoding.as_str());
            }
            let accept = config.compression.accept_encoding_header();
            if !accept.is_empty() {
                builder = builder.header("grpc-accept-encoding", accept);
            }
        }
    }

    if let Some(timeout) = timeout {
        builder = builder.header(
            config.protocol.timeout_header(),
            format_timeout(timeout, config.protocol),
        );
    }

    builder
}

/// Add protocol-specific headers to a request builder for streaming RPCs.
fn add_streaming_request_headers(
    mut builder: http::request::Builder,
    config: &ClientConfig,
    timeout: Option<Duration>,
) -> http::request::Builder {
    builder = builder.header(
        http::header::CONTENT_TYPE,
        streaming_request_content_type(config),
    );

    match config.protocol {
        Protocol::Connect => {
            builder = builder.header(connect_header::PROTOCOL_VERSION, "1");
        }
        Protocol::Grpc => {
            builder = builder.header("te", "trailers");
        }
        Protocol::GrpcWeb => {}
    }

    let encoding_header = config.protocol.content_encoding_header();
    let accept_header = config.protocol.accept_encoding_header();

    if let Some(ref encoding) = config.request_compression {
        builder = builder.header(encoding_header, encoding.as_str());
    }
    let accept = config.compression.accept_encoding_header();
    if !accept.is_empty() {
        builder = builder.header(accept_header, accept);
    }

    if let Some(timeout) = timeout {
        builder = builder.header(
            config.protocol.timeout_header(),
            format_timeout(timeout, config.protocol),
        );
    }

    builder
}

/// Parse a gRPC error from HTTP/2 trailers or gRPC-Web trailer frame headers.
fn parse_grpc_error_from_trailers(trailers: &http::HeaderMap) -> Option<ConnectError> {
    let raw = trailers.get("grpc-status")?;
    // A present-but-unparseable status is a protocol error, not an absent
    // status — it must not read as success. grpc-go maps malformed
    // grpc-status to Unknown.
    let Some(status) = raw.to_str().ok().and_then(|s| s.parse::<u32>().ok()) else {
        return Some(ConnectError::new(
            ErrorCode::Unknown,
            format!("protocol error: malformed grpc-status: {raw:?}"),
        ));
    };

    if status == 0 {
        return None; // OK
    }

    let code = ErrorCode::from_grpc_code(status).unwrap_or(ErrorCode::Unknown);
    let message = trailers
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .map(grpc_percent_decode);

    let mut err = ConnectError::new(code, message.unwrap_or_default());

    // Parse error details from grpc-status-details-bin
    if let Some(details_b64) = trailers
        .get("grpc-status-details-bin")
        .and_then(|v| v.to_str().ok())
    {
        use base64::Engine;
        if let Ok(details_bytes) = base64::engine::general_purpose::STANDARD
            .decode(details_b64)
            .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(details_b64))
        {
            err.details = crate::grpc_status::decode_details(&details_bytes);
        }
    }

    // Include other trailers as error metadata
    let out = err.trailers_mut();
    for (key, value) in trailers.iter() {
        let name = key.as_str();
        if name != "grpc-status" && name != "grpc-message" && name != "grpc-status-details-bin" {
            out.append(key, value.clone());
        }
    }

    Some(err)
}

/// Collect an HTTP response body into `Bytes`, enforcing a size limit.
///
/// Returns `ResourceExhausted` if the accumulated data exceeds `max_size`.
async fn collect_body_bounded<B>(body: B, max_size: usize) -> Result<Bytes, ConnectError>
where
    B: Body<Data = Bytes>,
    B::Error: std::fmt::Display,
{
    let mut buf = BytesMut::new();
    let mut stream = std::pin::pin!(body);
    loop {
        match std::future::poll_fn(|cx| stream.as_mut().poll_frame(cx)).await {
            Some(Ok(frame)) => {
                // Trailer frames are skipped: Connect unary/error bodies don't
                // use HTTP trailers (those come via `trailer-` prefixed headers
                // or the JSON body).
                if let Ok(data) = frame.into_data() {
                    if buf.len().saturating_add(data.len()) > max_size {
                        return Err(ConnectError::new(
                            ErrorCode::ResourceExhausted,
                            format!("response body size exceeds limit {max_size}"),
                        ));
                    }
                    buf.extend_from_slice(&data);
                }
            }
            Some(Err(e)) => {
                return Err(ConnectError::internal(format!(
                    "failed to read response body: {e}",
                )));
            }
            None => break,
        }
    }
    Ok(buf.freeze())
}

/// Percent-decode a gRPC message string.
///
/// Decode a gRPC percent-encoded message string back to UTF-8.
fn grpc_percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

/// Parse a gRPC-Web trailer frame from response body data.
///
/// Parse a gRPC-Web trailer frame, optionally decompressing with the given registry.
fn parse_grpc_web_trailer_frame_with_compression(
    data: &[u8],
    decompression: Option<(&CompressionRegistry, &str)>,
) -> Option<http::HeaderMap> {
    if data.len() < 5 || data[0] & crate::envelope::flags::GRPC_WEB_TRAILER == 0 {
        return None;
    }
    let is_compressed = data[0] & crate::envelope::flags::COMPRESSED != 0;
    let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
    // Cap trailer frame size to prevent a malicious server from forcing
    // unbounded memory allocation. 1 MB is generous for trailer metadata.
    const MAX_TRAILER_SIZE: usize = 1024 * 1024;
    if len > MAX_TRAILER_SIZE || data.len() < 5 + len {
        return None;
    }
    let raw_payload = &data[5..5 + len];

    // Decompress if the compressed flag is set
    let payload_bytes;
    let payload = if is_compressed {
        if let Some((registry, encoding)) = decompression {
            payload_bytes = registry
                .decompress_with_limit(
                    encoding,
                    Bytes::copy_from_slice(raw_payload),
                    MAX_TRAILER_SIZE,
                )
                .ok()?;
            std::str::from_utf8(&payload_bytes).ok()?
        } else {
            return None;
        }
    } else {
        std::str::from_utf8(raw_payload).ok()?
    };

    let mut headers = http::HeaderMap::new();
    // Split on \r\n or \n to handle both formats
    for line in payload.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        // Support both "key: value" and "key:value" formats
        if let Some((key, value)) = line.split_once(':')
            && let (Ok(name), Ok(val)) = (
                http::header::HeaderName::from_bytes(key.trim().as_bytes()),
                http::HeaderValue::from_str(value.trim()),
            )
        {
            // Use the fallible `try_append`: `HeaderMap` panics in
            // `append` once the number of stored entries would exceed its
            // hard ceiling (`MAX_SIZE = 1 << 15`). A hostile server can pack
            // tens of thousands of short trailer lines into a payload that
            // stays under `MAX_TRAILER_SIZE` (bytes, not entries), so the
            // byte cap alone does not prevent the panic. Stop accumulating at
            // the ceiling rather than crashing the RPC task.
            if headers.try_append(name, val).is_err() {
                break;
            }
        }
    }
    Some(headers)
}

/// Append Connect end-stream `metadata` into `trailers`, capping at the
/// `HeaderMap` entry ceiling.
///
/// `metadata` is deserialized from a server-supplied JSON end-stream frame, so
/// its size is attacker-controlled. `HeaderMap::append` panics once the number
/// of stored entries would exceed its hard ceiling (`MAX_SIZE = 1 << 15`); a
/// hostile server could send tens of thousands of distinct keys in a few
/// hundred KB of JSON and crash the RPC task. Use the fallible `try_append`
/// and stop at the ceiling instead.
fn append_metadata_capped(trailers: &mut http::HeaderMap, metadata: HashMap<String, Vec<String>>) {
    'outer: for (name, values) in metadata {
        for value in values {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::from_bytes(name.as_bytes()),
                http::header::HeaderValue::from_str(&value),
            ) && trailers.try_append(name, value).is_err()
            {
                break 'outer;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_payload_is_internal_at_response_decode() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        // The pathological payload never reaches `into_owned` — the client's
        // response decode boundary rejects it with the same classification
        // the deleted fallible `into_owned` used, so the wire-visible
        // behavior for an over-limit response is pinned here.
        let body = crate::request::tests::unknown_field_overflow_body();
        let err =
            decode_response_view::<StringValueView<'static>>(body, CodecFormat::Proto).unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
    }

    #[test]
    fn into_owned_parts_preserves_metadata() {
        use buffa::Message;
        use buffa::view::OwnedView;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let bytes = Bytes::from(StringValue::from("with-metadata").encode_to_vec());
        let view: OwnedView<StringValueView<'static>> = OwnedView::decode(bytes).unwrap();
        let mut headers = http::HeaderMap::new();
        headers.insert("x-probe", http::HeaderValue::from_static("h"));
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-trailer", http::HeaderValue::from_static("t"));
        let resp = UnaryResponse {
            headers,
            body: view,
            trailers,
        };

        let (headers, owned, trailers) = resp.into_owned_parts();
        assert_eq!(owned.value, "with-metadata");
        assert_eq!(headers.get("x-probe").unwrap(), "h");
        assert_eq!(trailers.get("x-trailer").unwrap(), "t");
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_client_config() {
        let config = ClientConfig::new("http://localhost:8080".parse().unwrap())
            .json()
            .compress_requests("gzip");

        assert_eq!(config.codec_format, CodecFormat::Json);
        assert_eq!(config.request_compression, Some("gzip".to_string()));
    }

    #[cfg(not(feature = "json"))]
    #[test]
    fn test_client_config_proto_only() {
        // The `.json()` shorthand is removed in a proto-only build; the default
        // codec is proto and the rest of the builder is unaffected.
        let config =
            ClientConfig::new("http://localhost:8080".parse().unwrap()).compress_requests("gzip");

        assert_eq!(config.codec_format, CodecFormat::Proto);
        assert_eq!(config.request_compression, Some("gzip".to_string()));
    }

    #[cfg(feature = "client")]
    #[tokio::test]
    async fn http_client_connect_timeout_bounds_tcp_connect() {
        use std::time::Instant;

        // RFC 5737 TEST-NET-1: reserved for documentation. Most hosts drop
        // SYNs to it (so an unbounded connect stalls on kernel retransmits,
        // ~130s on Linux defaults), but RFC 5737 doesn't mandate that — some
        // CI hosts actively reject. The assertion that matters is the upper
        // bound: a 100ms timeout must abort well before the kernel retry floor.
        let target = "http://192.0.2.1:9/";
        let timeout = Duration::from_millis(100);

        let http = HttpClient::builder().connect_timeout(timeout).plaintext();
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(target)
            .body(full_body(Bytes::new()))
            .unwrap();

        let start = Instant::now();
        // Outer timeout guards a transparent-proxy host that accepts the
        // connect (so the bound under test never fires) and then never answers
        // the HTTP/1.1 request — without this the test would hang.
        let result = tokio::time::timeout(Duration::from_secs(3), http.send(req)).await;
        let elapsed = start.elapsed();
        let Ok(Err(err)) = result else {
            eprintln!("skipping: TEST-NET-1 reachable on this host (proxy?) in {elapsed:?}");
            return;
        };

        // Generous slack for CI scheduling jitter — but well under the
        // multi-second kernel SYN-retry floor we'd hit without the bound.
        assert!(
            elapsed < Duration::from_secs(2),
            "connect_timeout(100ms) should abort within ~2s, took {elapsed:?}: {err}"
        );
    }

    #[cfg(feature = "client")]
    #[tokio::test]
    async fn http_client_establishment_timeout_bounds_plaintext_connector() {
        use std::time::Instant;

        // The establishment_timeout wrapper bounds the whole connector. For
        // plaintext that's just the TCP connect, so an unroutable TEST-NET-1
        // address must fail fast rather than stall on kernel SYN retransmits.
        let target = "http://192.0.2.1:9/";
        let http = HttpClient::builder()
            .establishment_timeout(Duration::from_millis(100))
            .plaintext();
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(target)
            .body(full_body(Bytes::new()))
            .unwrap();

        let start = Instant::now();
        // Outer timeout guards a transparent-proxy host that accepts the
        // connect (so the bound under test never fires) and then never answers
        // the HTTP/1.1 request — without this the test would hang.
        let result = tokio::time::timeout(Duration::from_secs(3), http.send(req)).await;
        let elapsed = start.elapsed();
        let Ok(Err(err)) = result else {
            eprintln!("skipping: TEST-NET-1 reachable on this host (proxy?) in {elapsed:?}");
            return;
        };

        assert!(
            elapsed < Duration::from_secs(2),
            "establishment_timeout(100ms) should abort within ~2s, took {elapsed:?}: {err}"
        );
    }

    #[cfg(feature = "client-tls")]
    #[tokio::test]
    async fn http_client_establishment_timeout_bounds_stalled_tls() {
        use std::time::Instant;

        // A listener that accepts the TCP connection but never performs the TLS
        // handshake. The TCP connect succeeds, so only establishment_timeout (which
        // covers TCP + TLS for the connector) can release the stalled connect.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });

        let tls_config = std::sync::Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let http = HttpClient::builder()
            .establishment_timeout(Duration::from_millis(150))
            .with_tls(tls_config);
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!("https://{addr}/"))
            .body(full_body(Bytes::new()))
            .unwrap();

        let start = Instant::now();
        let err = http.send(req).await.expect_err("stalled TLS must fail");
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(2),
            "establishment_timeout(150ms) should fire within ~2s, took {elapsed:?}: {err}"
        );

        server.abort();
    }

    #[test]
    fn client_config_builders_round_trip_through_accessors() {
        // Each `with_*` builder must be readable back through the bare-name
        // accessor — the public read contract that replaces direct field
        // access (#90).
        let mut headers = http::HeaderMap::new();
        headers.insert("x-base", "v".parse().unwrap());

        let config = ClientConfig::new("http://example.com:8080".parse().unwrap())
            .with_protocol(Protocol::Grpc)
            .with_codec_format(CodecFormat::Json)
            .with_compression(CompressionRegistry::default())
            .compress_requests("gzip")
            .with_compression_policy(CompressionPolicy::default())
            .with_default_timeout(Duration::from_secs(30))
            .with_default_max_message_size(4096)
            .with_default_headers(headers.clone())
            .with_default_header("x-extra", "1");

        assert_eq!(config.base_uri().to_string(), "http://example.com:8080/");
        assert_eq!(config.protocol(), Protocol::Grpc);
        assert_eq!(config.codec_format(), CodecFormat::Json);
        assert_eq!(config.request_compression(), Some("gzip"));
        assert_eq!(config.default_timeout(), Some(Duration::from_secs(30)));
        assert_eq!(config.default_max_message_size(), Some(4096));
        assert_eq!(config.default_headers().get("x-base").unwrap(), "v");
        assert_eq!(config.default_headers().get("x-extra").unwrap(), "1");
        // `compression()` and `compression_policy()` return references / copies
        // of the registry/policy; they don't impl PartialEq, so we just exercise
        // that the accessors compile and don't panic.
        let _ = config.compression();
        let _ = config.compression_policy();
    }

    #[test]
    fn client_config_defaults() {
        let config = ClientConfig::new("http://localhost".parse().unwrap());
        assert_eq!(config.protocol(), Protocol::Connect);
        assert_eq!(config.codec_format(), CodecFormat::Proto);
        assert_eq!(config.request_compression(), None);
        assert_eq!(config.default_timeout(), None);
        assert_eq!(config.default_max_message_size(), None);
        assert!(config.default_headers().is_empty());
    }

    #[test]
    fn call_options_builders_round_trip_through_accessors() {
        let options = CallOptions::default()
            .with_timeout(Duration::from_secs(5))
            .with_header("x-request-id", "abc")
            .with_max_message_size(2048)
            .with_compress(true);

        assert_eq!(options.timeout(), Some(Duration::from_secs(5)));
        assert_eq!(options.headers().get("x-request-id").unwrap(), "abc");
        assert_eq!(options.max_message_size(), Some(2048));
        assert_eq!(options.compress(), Some(true));
    }

    #[test]
    fn call_options_defaults() {
        let options = CallOptions::default();
        assert!(options.headers().is_empty());
        assert_eq!(options.timeout(), None);
        assert_eq!(options.max_message_size(), None);
        assert_eq!(options.compress(), None);
    }

    #[cfg(feature = "client")]
    #[test]
    fn test_http_client_plaintext_creation() {
        let _client = HttpClient::plaintext();
        let _client = HttpClient::plaintext_http2_only();
    }

    /// Compile-time assertion that public client types all satisfy `Debug`.
    ///
    /// `Result::unwrap_err()` requires `T: Debug` (so that an unexpected `Ok`
    /// can be printed in the panic message). Without these impls, integration
    /// tests doing `client.foo(req).await.unwrap_err()` won't compile.
    #[test]
    fn client_types_are_debug() {
        fn assert_debug<T: std::fmt::Debug>() {}

        // UnaryResponse<Resp> — derived; the bound `Resp: Debug` is satisfied
        // by `OwnedView<V>` whenever `V: Debug` (which all generated view
        // types are).
        assert_debug::<UnaryResponse<()>>();

        // Stream types — manual impls that print state summary (body type `B`
        // is typically `hyper::body::Incoming` which isn't Debug).
        assert_debug::<ServerStream<http_body_util::Empty<Bytes>, ()>>();
        assert_debug::<BidiStream<http_body_util::Empty<Bytes>, (), ()>>();
        assert_debug::<BidiSendHalf<()>>();
        assert_debug::<BidiRecvHalf<http_body_util::Empty<Bytes>, ()>>();

        // Transports — manual impls that print mode/connection state.
        #[cfg(feature = "client")]
        assert_debug::<HttpClient>();
    }

    #[test]
    fn bidi_stream_auto_traits() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        fn assert_unpin<T: Unpin>() {}

        type TestBidi = BidiStream<http_body_util::Empty<Bytes>, (), ()>;

        assert_send::<TestBidi>();
        assert_sync::<TestBidi>();
        assert_unpin::<TestBidi>();

        // The halves are moved into separate spawned tasks, so their auto
        // traits are individually load-bearing, not just via containment.
        type TestSend = BidiSendHalf<()>;
        type TestRecv = BidiRecvHalf<http_body_util::Empty<Bytes>, ()>;

        assert_send::<TestSend>();
        assert_sync::<TestSend>();
        assert_unpin::<TestSend>();
        assert_send::<TestRecv>();
        assert_sync::<TestRecv>();
        assert_unpin::<TestRecv>();
    }

    fn connect_success_body(message: &str) -> Bytes {
        use buffa::Message;
        use buffa_types::google::protobuf::StringValue;

        let mut body = BytesMut::new();
        body.extend_from_slice(
            &Envelope::data(StringValue::from(message).encode_to_bytes()).encode(),
        );
        body.extend_from_slice(&Envelope::end_stream(Bytes::from_static(b"{}")).encode());
        body.freeze()
    }

    fn connect_response<B>(status: http::StatusCode, body: B) -> Response<B> {
        Response::builder().status(status).body(body).unwrap()
    }

    fn bidi_stream_with_response_task<B>(
        response_task: tokio::task::JoinHandle<Result<Response<B>, ConnectError>>,
        deadline: Option<std::time::Instant>,
    ) -> BidiStream<
        B,
        buffa_types::google::protobuf::StringValue,
        buffa_types::google::protobuf::__buffa::view::StringValueView<'static>,
    > {
        BidiStream {
            send: BidiSendHalf {
                tx: None,
                encoder: crate::envelope::EnvelopeEncoder::uncompressed(),
                codec_format: CodecFormat::Proto,
                deadline,
                _req: PhantomData,
            },
            recv: BidiRecvHalf {
                recv: RecvState::AwaitingHeaders(response_task),
                stream_config: StreamConfig {
                    protocol: Protocol::Connect,
                    codec_format: CodecFormat::Proto,
                    compression: CompressionRegistry::new(),
                    max_message_size: Some(1024),
                    deadline,
                },
            },
        }
    }

    struct GatedBody {
        shared: std::sync::Arc<std::sync::Mutex<GatedBodyState>>,
    }

    struct GatedBodyRelease {
        shared: std::sync::Arc<std::sync::Mutex<GatedBodyState>>,
    }

    struct GatedBodyState {
        first_poll: Option<tokio::sync::oneshot::Sender<()>>,
        released: Option<Bytes>,
        done: bool,
        waker: Option<std::task::Waker>,
    }

    impl GatedBody {
        fn new() -> (Self, tokio::sync::oneshot::Receiver<()>, GatedBodyRelease) {
            let (first_poll_tx, first_poll_rx) = tokio::sync::oneshot::channel();
            let shared = std::sync::Arc::new(std::sync::Mutex::new(GatedBodyState {
                first_poll: Some(first_poll_tx),
                released: None,
                done: false,
                waker: None,
            }));

            (
                Self {
                    shared: shared.clone(),
                },
                first_poll_rx,
                GatedBodyRelease { shared },
            )
        }
    }

    impl GatedBodyRelease {
        fn release(self, bytes: Bytes) {
            let mut state = self.shared.lock().unwrap();
            state.released = Some(bytes);
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
        }
    }

    impl Body for GatedBody {
        type Data = Bytes;
        type Error = ConnectError;

        fn poll_frame(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, ConnectError>>> {
            let mut state = self.shared.lock().unwrap();
            if let Some(first_poll) = state.first_poll.take() {
                let _ = first_poll.send(());
            }

            if state.done {
                return std::task::Poll::Ready(None);
            }

            if let Some(bytes) = state.released.take() {
                state.done = true;
                return std::task::Poll::Ready(Some(Ok(http_body::Frame::data(bytes))));
            }

            state.waker = Some(cx.waker().clone());
            std::task::Poll::Pending
        }
    }

    #[tokio::test]
    async fn bidi_message_cancel_before_headers_resumes() {
        use buffa_types::google::protobuf::StringValue;

        let (response_tx, response_rx) =
            tokio::sync::oneshot::channel::<Result<Response<Full<Bytes>>, ConnectError>>();
        let response_task = tokio::spawn(async move {
            response_rx
                .await
                .expect("test response sender should stay alive")
        });
        let mut stream = bidi_stream_with_response_task(response_task, None);

        let mut first_message = Box::pin(stream.message::<StringValue>());
        assert!(matches!(
            futures::poll!(&mut first_message),
            std::task::Poll::Pending
        ));
        drop(first_message);

        assert!(stream.error().is_none());
        assert!(stream.headers().is_none());

        response_tx
            .send(Ok(connect_response(
                http::StatusCode::OK,
                Full::new(connect_success_body("hello")),
            )))
            .expect("detached response task should still receive headers");

        let msg = stream
            .message::<StringValue>()
            .await
            .expect("cancelled header wait should resume")
            .expect("stream should yield first response message");
        assert_eq!(msg.view().value, "hello");
        assert!(stream.message::<StringValue>().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn bidi_message_cancel_during_connect_error_body_resumes() {
        use buffa_types::google::protobuf::StringValue;

        let (body, body_polled, body_release) = GatedBody::new();
        let (response_ready_tx, response_ready_rx) = tokio::sync::oneshot::channel();
        let response_task = tokio::spawn(async move {
            let response = connect_response(http::StatusCode::BAD_REQUEST, body);
            let _ = response_ready_tx.send(());
            Ok(response)
        });
        response_ready_rx
            .await
            .expect("response task should have prepared headers");
        let mut stream = bidi_stream_with_response_task(response_task, None);

        let mut first_message = Box::pin(stream.message::<StringValue>());
        assert!(matches!(
            futures::poll!(&mut first_message),
            std::task::Poll::Pending
        ));
        body_polled
            .await
            .expect("Connect error body should be polled");
        drop(first_message);

        assert!(stream.error().is_none());
        assert!(stream.headers().is_none());

        body_release.release(Bytes::from_static(
            br#"{"code":"invalid_argument","message":"bad request"}"#,
        ));

        let err = stream
            .message::<StringValue>()
            .await
            .expect_err("server Connect error should be preserved");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert_eq!(err.message.as_deref(), Some("bad request"));
        let again = stream
            .message::<StringValue>()
            .await
            .expect_err("initialization error should be sticky");
        assert_eq!(again.code, ErrorCode::InvalidArgument);
        assert_eq!(stream.error().unwrap().code, ErrorCode::InvalidArgument);
    }

    #[tokio::test(start_paused = true)]
    async fn bidi_message_deadline_before_headers_stays_sticky() {
        use buffa_types::google::protobuf::StringValue;

        let (_response_tx, response_rx) =
            tokio::sync::oneshot::channel::<Result<Response<Full<Bytes>>, ConnectError>>();
        let response_task = tokio::spawn(async move {
            response_rx
                .await
                .expect("test intentionally keeps response pending")
        });
        let deadline = std::time::Instant::now() + Duration::from_millis(100);
        let mut stream = bidi_stream_with_response_task(response_task, Some(deadline));

        let err = stream
            .message::<StringValue>()
            .await
            .expect_err("deadline should fail receive initialization");
        assert_eq!(err.code, ErrorCode::DeadlineExceeded);

        let again = stream
            .message::<StringValue>()
            .await
            .expect_err("deadline failure should be sticky");
        assert_eq!(again.code, ErrorCode::DeadlineExceeded);
        assert_eq!(stream.error().unwrap().code, ErrorCode::DeadlineExceeded);
    }

    #[tokio::test(start_paused = true)]
    async fn bidi_message_deadline_during_connect_error_body_stays_sticky() {
        use buffa_types::google::protobuf::StringValue;

        let (body, body_polled, _body_release) = GatedBody::new();
        let (response_ready_tx, response_ready_rx) = tokio::sync::oneshot::channel();
        let response_task = tokio::spawn(async move {
            let response = connect_response(http::StatusCode::BAD_REQUEST, body);
            let _ = response_ready_tx.send(());
            Ok(response)
        });
        response_ready_rx
            .await
            .expect("response task should have prepared headers");

        let deadline = std::time::Instant::now() + Duration::from_millis(100);
        let mut stream = bidi_stream_with_response_task(response_task, Some(deadline));

        let mut first_message = Box::pin(stream.message::<StringValue>());
        assert!(matches!(
            futures::poll!(&mut first_message),
            std::task::Poll::Pending
        ));
        body_polled
            .await
            .expect("Connect error body should be polled");

        // Keep `_body_release` alive and unused so the gated body remains
        // pending until the call deadline fires.
        let err = first_message
            .await
            .expect_err("deadline should fail response construction");
        assert_eq!(err.code, ErrorCode::DeadlineExceeded);

        let again = stream
            .message::<StringValue>()
            .await
            .expect_err("construction deadline should be sticky");
        assert_eq!(again.code, ErrorCode::DeadlineExceeded);
        assert_eq!(stream.error().unwrap().code, ErrorCode::DeadlineExceeded);
    }

    #[tokio::test]
    async fn bidi_message_transport_failure_is_sticky() {
        use buffa_types::google::protobuf::StringValue;

        let response_task = tokio::spawn(async {
            Err::<Response<Full<Bytes>>, _>(ConnectError::unavailable("request failed: boom"))
        });
        let mut stream = bidi_stream_with_response_task(response_task, None);

        let err = stream
            .message::<StringValue>()
            .await
            .expect_err("transport failure should fail receive initialization");
        assert_eq!(err.code, ErrorCode::Unavailable);
        assert_eq!(err.message.as_deref(), Some("request failed: boom"));

        let again = stream
            .message::<StringValue>()
            .await
            .expect_err("transport failure should be sticky");
        assert_eq!(again.code, ErrorCode::Unavailable);
        assert_eq!(again.message.as_deref(), Some("request failed: boom"));
        assert_eq!(stream.error().unwrap().code, ErrorCode::Unavailable);
    }

    #[tokio::test]
    async fn bidi_drop_aborts_headers_task() {
        let (guard_tx, guard_rx) = tokio::sync::oneshot::channel::<()>();
        let (_never_tx, never_rx) =
            tokio::sync::oneshot::channel::<Result<Response<Full<Bytes>>, ConnectError>>();
        let response_task = tokio::spawn(async move {
            let _guard = guard_tx;
            never_rx.await.expect("test never resolves the response")
        });
        let stream = bidi_stream_with_response_task(response_task, None);
        drop(stream);

        // The abort drops the task and with it `_guard`, erroring the
        // receiver. Without the abort the task stays parked and this times out.
        tokio::time::timeout(Duration::from_secs(5), guard_rx)
            .await
            .expect("dropped BidiStream should abort the headers task")
            .expect_err("guard sender should be dropped by the abort");
    }

    #[tokio::test]
    async fn bidi_drop_aborts_pending_construction() {
        use buffa_types::google::protobuf::StringValue;

        let (body, body_polled, body_release) = GatedBody::new();
        let (response_ready_tx, response_ready_rx) = tokio::sync::oneshot::channel();
        let response_task = tokio::spawn(async move {
            let response = connect_response(http::StatusCode::BAD_REQUEST, body);
            let _ = response_ready_tx.send(());
            Ok(response)
        });
        response_ready_rx
            .await
            .expect("response task should have prepared headers");
        let mut stream = bidi_stream_with_response_task(response_task, None);

        let mut first_message = Box::pin(stream.message::<StringValue>());
        assert!(matches!(
            futures::poll!(&mut first_message),
            std::task::Poll::Pending
        ));
        body_polled
            .await
            .expect("Connect error body should be polled");
        drop(first_message);
        drop(stream);

        // Aborting the construction task drops the gated response body; the
        // release handle then holds the only reference to the shared state.
        tokio::time::timeout(Duration::from_secs(5), async {
            while std::sync::Arc::strong_count(&body_release.shared) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped BidiStream should abort construction and drop the body");
    }

    #[tokio::test(start_paused = true)]
    async fn bidi_deadline_aborts_headers_task() {
        use buffa_types::google::protobuf::StringValue;

        let (guard_tx, guard_rx) = tokio::sync::oneshot::channel::<()>();
        let (_never_tx, never_rx) =
            tokio::sync::oneshot::channel::<Result<Response<Full<Bytes>>, ConnectError>>();
        let response_task = tokio::spawn(async move {
            let _guard = guard_tx;
            never_rx.await.expect("test never resolves the response")
        });
        let deadline = std::time::Instant::now() + Duration::from_millis(100);
        let mut stream = bidi_stream_with_response_task(response_task, Some(deadline));

        let err = stream
            .message::<StringValue>()
            .await
            .expect_err("deadline should fail receive initialization");
        assert_eq!(err.code, ErrorCode::DeadlineExceeded);

        // Failing the call at the deadline aborts (not detaches) the headers
        // task, dropping `_guard`.
        tokio::time::timeout(Duration::from_secs(5), guard_rx)
            .await
            .expect("deadline failure should abort the headers task")
            .expect_err("guard sender should be dropped by the abort");
    }

    #[tokio::test]
    async fn connect_server_stream_truncated_after_data_errors() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let body = Full::new(Envelope::data(StringValue::from("hello").encode_to_bytes()).encode());
        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers: http::HeaderMap::new(),
            body,
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Connect,
            max_message_size: Some(1024),
            deadline: None,
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        let msg = stream
            .message()
            .await
            .expect("first message should decode")
            .expect("stream should yield the data envelope before EOF");
        assert_eq!(msg.view().value, "hello");

        let err = match stream.message().await {
            Err(err) => err,
            Ok(Some(_)) => panic!("truncated stream unexpectedly yielded another message"),
            Ok(None) => panic!("truncated stream ended cleanly without END_STREAM"),
        };
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(
            err.to_string().contains("END_STREAM"),
            "unexpected error: {err}"
        );

        // Sticky: a re-poll must not degrade truncation to a clean-looking
        // `Ok(None)`.
        let again = stream
            .message()
            .await
            .expect_err("truncation error must be sticky");
        assert_eq!(again.code, ErrorCode::Internal);
    }

    /// A Connect streaming response whose body is empty (zero envelopes,
    /// immediate EOF) is also missing its END_STREAM envelope and must
    /// error rather than report a clean end of stream.
    #[tokio::test]
    async fn connect_server_stream_empty_body_errors() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        let body = Full::new(Bytes::new());
        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers: http::HeaderMap::new(),
            body,
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Connect,
            max_message_size: Some(1024),
            deadline: None,
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        let err = match stream.message().await {
            Err(err) => err,
            Ok(Some(_)) => panic!("empty body unexpectedly yielded a message"),
            Ok(None) => panic!("empty body without END_STREAM ended cleanly"),
        };
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(
            err.to_string().contains("END_STREAM"),
            "unexpected error: {err}"
        );

        let again = stream
            .message()
            .await
            .expect_err("truncation error must be sticky");
        assert_eq!(again.code, ErrorCode::Internal);
    }

    /// `Ok(None)` means the RPC succeeded — a Connect END_STREAM envelope
    /// carrying an error must come back as `Err` from `message()`, sticky
    /// across calls, with `error()` still available for inspection.
    #[tokio::test]
    async fn connect_end_stream_error_returned_from_message() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let mut body = BytesMut::new();
        body.extend_from_slice(
            &Envelope::data(StringValue::from("hello").encode_to_bytes()).encode(),
        );
        body.extend_from_slice(
            &Envelope::end_stream(Bytes::from_static(
                b"{\"error\":{\"code\":\"out_of_range\",\"message\":\"requested position no longer retained\"},\
                  \"metadata\":{\"x-detail\":[\"42\"]}}",
            ))
            .encode(),
        );
        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers: http::HeaderMap::new(),
            body: Full::new(body.freeze()),
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Connect,
            max_message_size: Some(1024),
            deadline: None,
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        let msg = stream
            .message()
            .await
            .expect("data envelope should decode")
            .expect("stream should yield the data message first");
        assert_eq!(msg.view().value, "hello");

        let err = stream
            .message()
            .await
            .expect_err("errored END_STREAM must surface as Err, not Ok(None)");
        assert_eq!(err.code, ErrorCode::OutOfRange);
        assert_eq!(
            err.message.as_deref(),
            Some("requested position no longer retained")
        );

        // Sticky: re-polling a failed stream re-reports the failure.
        let again = stream
            .message()
            .await
            .expect_err("terminal error is sticky");
        assert_eq!(again.code, ErrorCode::OutOfRange);

        // Post-hoc accessors still work.
        assert_eq!(stream.error().map(|e| e.code), Some(ErrorCode::OutOfRange));
        assert_eq!(
            stream
                .trailers()
                .and_then(|t| t.get("x-detail"))
                .and_then(|v| v.to_str().ok()),
            Some("42")
        );
    }

    /// Malformed Connect END_STREAM JSON is a protocol error. It must not be
    /// treated as an empty successful end-stream payload.
    #[tokio::test]
    async fn connect_malformed_end_stream_json_errors() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let mut body = BytesMut::new();
        body.extend_from_slice(
            &Envelope::data(StringValue::from("hello").encode_to_bytes()).encode(),
        );
        body.extend_from_slice(&Envelope::end_stream(Bytes::from_static(b"not json")).encode());

        let mut headers = http::HeaderMap::new();
        headers.insert("x-from-headers", http::HeaderValue::from_static("yes"));

        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers,
            body: Full::new(body.freeze()),
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Connect,
            max_message_size: Some(1024),
            deadline: None,
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        let msg = stream
            .message()
            .await
            .expect("data envelope should decode")
            .expect("stream should yield the data message first");
        assert_eq!(msg.view().value, "hello");

        let err = stream
            .message()
            .await
            .expect_err("malformed END_STREAM must surface as Err, not Ok(None)");
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(
            err.to_string()
                .contains("malformed Connect END_STREAM JSON"),
            "unexpected error: {err}"
        );
        assert_eq!(err.response_headers().get("x-from-headers").unwrap(), "yes");

        let again = stream
            .message()
            .await
            .expect_err("malformed END_STREAM error must be sticky");
        assert_eq!(again.code, ErrorCode::Internal);
        assert_eq!(
            again.response_headers().get("x-from-headers").unwrap(),
            "yes"
        );
    }

    /// Same contract for gRPC: an error in HTTP/2 trailers is a failed RPC
    /// and must come back as `Err` from `message()` — not the silent
    /// `Ok(None)` that callers mistake for a clean close.
    #[tokio::test]
    async fn grpc_trailer_error_returned_from_message() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;
        use http_body::Frame;
        use http_body_util::StreamBody;

        let data = Envelope::data(StringValue::from("hello").encode_to_bytes()).encode();
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "11".parse().unwrap()); // OUT_OF_RANGE
        trailers.insert(
            "grpc-message",
            "requested position no longer retained".parse().unwrap(),
        );
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> =
            vec![Ok(Frame::data(data)), Ok(Frame::trailers(trailers))];
        let body = StreamBody::new(futures::stream::iter(frames));

        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers: http::HeaderMap::new(),
            body,
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Grpc,
            max_message_size: Some(1024),
            deadline: None,
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        let msg = stream
            .message()
            .await
            .expect("data envelope should decode")
            .expect("stream should yield the data message first");
        assert_eq!(msg.view().value, "hello");

        let err = stream
            .message()
            .await
            .expect_err("gRPC trailer error must surface as Err, not Ok(None)");
        assert_eq!(err.code, ErrorCode::OutOfRange);
        assert_eq!(
            err.message.as_deref(),
            Some("requested position no longer retained")
        );

        let again = stream
            .message()
            .await
            .expect_err("terminal error is sticky");
        assert_eq!(again.code, ErrorCode::OutOfRange);
        assert!(stream.trailers().is_some());
    }

    /// A gRPC stream ending with `grpc-status: 0` is the one true clean end —
    /// `Ok(None)`, no error. Runs with an unexpired deadline set, so an
    /// implementation that errors eagerly on any deadline would fail here.
    #[tokio::test]
    async fn grpc_ok_trailers_end_as_ok_none() {
        use std::time::Duration;

        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use http_body::Frame;
        use http_body_util::StreamBody;

        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> =
            vec![Ok(Frame::trailers(trailers))];
        let body = StreamBody::new(futures::stream::iter(frames));

        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers: http::HeaderMap::new(),
            body,
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Grpc,
            max_message_size: Some(1024),
            deadline: Some(std::time::Instant::now() + Duration::from_secs(5)),
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        assert!(stream.message().await.unwrap().is_none());
        assert!(stream.error().is_none());
        assert!(stream.trailers().is_some());
    }

    /// gRPC EOF with no trailers at all is a protocol violation — it must
    /// not read as a clean end (it is indistinguishable from a mid-stream
    /// cut; grpc-go errors here too).
    #[tokio::test]
    async fn grpc_eof_without_trailers_errors() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers: http::HeaderMap::new(),
            body: Full::new(Bytes::new()),
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Grpc,
            max_message_size: Some(1024),
            deadline: None,
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        let err = stream
            .message()
            .await
            .expect_err("EOF without grpc-status must surface as Err");
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(
            err.to_string().contains("grpc-status"),
            "unexpected error: {err}"
        );

        let again = stream
            .message()
            .await
            .expect_err("missing-status error must be sticky");
        assert_eq!(again.code, ErrorCode::Internal);
    }

    /// `grpc-status: 0` in the response HEADERS only certifies a true
    /// Trailers-Only response (empty body). If data flowed afterwards,
    /// the real trailers are still required — a cut after eager headers
    /// must not read as success.
    #[tokio::test]
    async fn grpc_header_status_does_not_excuse_truncation_after_data() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", "0".parse().unwrap());
        let body = Full::new(Envelope::data(StringValue::from("hello").encode_to_bytes()).encode());
        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers,
            body,
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Grpc,
            max_message_size: Some(1024),
            deadline: None,
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        let msg = stream
            .message()
            .await
            .expect("data envelope should decode")
            .expect("stream should yield the data message first");
        assert_eq!(msg.view().value, "hello");

        let err = stream
            .message()
            .await
            .expect_err("truncation after data must surface as Err despite header status");
        assert_eq!(err.code, ErrorCode::Internal);
    }

    /// A gRPC Trailers-Only OK response — `grpc-status: 0` in the response
    /// HEADERS, empty body, no HTTP trailers — is how grpc-go ends a
    /// server-stream cleanly with zero messages. It must stay a clean
    /// `Ok(None)`, not a missing-status error.
    #[tokio::test]
    async fn grpc_trailers_only_ok_response_ends_cleanly() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", "0".parse().unwrap());
        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers,
            body: Full::new(Bytes::new()),
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Grpc,
            max_message_size: Some(1024),
            deadline: None,
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        assert!(stream.message().await.unwrap().is_none());
        assert!(stream.error().is_none());
        // Stays clean on re-poll, too.
        assert!(stream.message().await.unwrap().is_none());
    }

    /// Trailers that arrive without any `grpc-status` are as broken as no
    /// trailers at all — the status is the termination signal, and its
    /// absence must not read as success (grpc-go maps this to an error).
    #[tokio::test]
    async fn grpc_trailers_without_status_errors() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use http_body::Frame;
        use http_body_util::StreamBody;

        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-meta", "1".parse().unwrap());
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> =
            vec![Ok(Frame::trailers(trailers))];
        let body = StreamBody::new(futures::stream::iter(frames));

        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers: http::HeaderMap::new(),
            body,
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Grpc,
            max_message_size: Some(1024),
            deadline: None,
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        let err = stream
            .message()
            .await
            .expect_err("trailers without grpc-status must surface as Err");
        // Unknown, not Internal: trailers arrived, just without a status —
        // grpc-go / connect-go / conformance-primary semantics.
        assert_eq!(err.code, ErrorCode::Unknown);
        // The malformed trailers are still inspectable.
        assert!(stream.trailers().is_some());

        let again = stream
            .message()
            .await
            .expect_err("missing-status error must be sticky");
        assert_eq!(again.code, ErrorCode::Unknown);
    }

    /// The whole-call deadline is ABSOLUTE across frame polls: a server
    /// trickling frames forever, each arriving well inside any plausible
    /// per-poll window, is stopped at the deadline. This pins the
    /// equivalence that licenses bounding each frame poll instead of the
    /// whole decode loop — a per-poll *relative* timeout would let every
    /// 40ms frame through and this test would fail (the trickled bytes
    /// eventually complete an envelope and yield `Ok(Some)`).
    #[tokio::test(start_paused = true)]
    async fn deadline_bounds_multi_frame_message() {
        use std::time::Duration;

        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use http_body::Frame;
        use http_body_util::StreamBody;

        // One opaque byte every 40ms, forever (bounded at 32 so a
        // regression fails fast instead of hanging) — never enough to
        // matter before a 100ms absolute deadline.
        let frames: std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<Frame<Bytes>, std::convert::Infallible>> + Send>,
        > = Box::pin(futures::stream::unfold(0u32, |n| async move {
            if n >= 32 {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
            Some((
                Ok::<_, std::convert::Infallible>(Frame::data(Bytes::from_static(&[0u8]))),
                n + 1,
            ))
        }));
        let body = StreamBody::new(frames);

        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers: http::HeaderMap::new(),
            body,
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Grpc,
            max_message_size: Some(1024),
            deadline: Some(std::time::Instant::now() + Duration::from_millis(100)),
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        let start = tokio::time::Instant::now();
        let err = stream
            .message()
            .await
            .expect_err("absolute deadline must fire mid-trickle");
        assert_eq!(err.code, ErrorCode::DeadlineExceeded);
        assert!(
            start.elapsed() >= Duration::from_millis(100),
            "deadline must not fire early (elapsed {:?})",
            start.elapsed()
        );
        assert!(
            start.elapsed() <= Duration::from_millis(150),
            "deadline must be absolute across polls, not per-poll relative (elapsed {:?})",
            start.elapsed()
        );

        let again = stream
            .message()
            .await
            .expect_err("deadline error is sticky");
        assert_eq!(again.code, ErrorCode::DeadlineExceeded);
    }

    /// A present-but-garbage `grpc-status` is a protocol error (`unknown`,
    /// grpc-go parity) — it must not satisfy the status-presence check and
    /// read as a clean end.
    #[tokio::test]
    async fn grpc_malformed_status_errors() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use http_body::Frame;
        use http_body_util::StreamBody;

        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "banana".parse().unwrap());
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> =
            vec![Ok(Frame::trailers(trailers))];
        let body = StreamBody::new(futures::stream::iter(frames));

        let mut stream: ServerStream<_, StringValueView<'static>> = ServerStream {
            headers: http::HeaderMap::new(),
            body,
            buf: BytesMut::new(),
            encoding: None,
            compression: CompressionRegistry::new(),
            codec_format: CodecFormat::Proto,
            protocol: Protocol::Grpc,
            max_message_size: Some(1024),
            deadline: None,
            end: None,
            saw_body_data: false,
            _phantom: PhantomData,
        };

        let err = stream
            .message()
            .await
            .expect_err("malformed grpc-status must surface as Err");
        assert_eq!(err.code, ErrorCode::Unknown);
        assert!(
            err.to_string().contains("malformed grpc-status"),
            "unexpected error: {err}"
        );

        let again = stream
            .message()
            .await
            .expect_err("malformed-status error must be sticky");
        assert_eq!(again.code, ErrorCode::Unknown);
    }

    #[cfg(feature = "client")]
    fn plaintext_client_with_https_base() -> (HttpClient, ClientConfig) {
        let client = HttpClient::plaintext();
        let config = ClientConfig::new("https://localhost:8080".parse().unwrap());
        (client, config)
    }

    #[test]
    fn transport_send_error_mapper_preserves_connect_error_in_source_chain() {
        #[derive(Debug)]
        struct WrappedTransportError(ConnectError);

        impl std::fmt::Display for WrappedTransportError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "wrapped transport failure")
            }
        }

        impl std::error::Error for WrappedTransportError {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }

        let mapped = map_transport_send_error(
            WrappedTransportError(ConnectError::invalid_argument("bad client config")),
            "request failed",
        );
        assert_eq!(mapped.code, ErrorCode::InvalidArgument);
        assert_eq!(mapped.message.as_deref(), Some("bad client config"));
    }

    #[cfg(feature = "client")]
    #[tokio::test]
    async fn call_unary_preserves_transport_connect_error() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let (client, config) = plaintext_client_with_https_base();
        let err = call_unary::<_, StringValue, StringValueView<'static>>(
            &client,
            &config,
            "test.Service",
            "Unary",
            StringValue::from("hello"),
            CallOptions::default(),
        )
        .await
        .expect_err("transport config error must surface from unary call");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.as_deref().unwrap().contains("with_tls"));
    }

    #[cfg(feature = "client")]
    #[tokio::test]
    async fn call_unary_get_preserves_transport_connect_error() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let (client, config) = plaintext_client_with_https_base();
        let err = call_unary_get::<_, StringValue, StringValueView<'static>>(
            &client,
            &config,
            "test.Service",
            "UnaryGet",
            StringValue::from("hello"),
            CallOptions::default(),
        )
        .await
        .expect_err("transport config error must surface from unary GET");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.as_deref().unwrap().contains("with_tls"));
    }

    #[cfg(feature = "client")]
    #[tokio::test]
    async fn call_server_stream_preserves_transport_connect_error() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let (client, config) = plaintext_client_with_https_base();
        let err = call_server_stream::<_, StringValue, StringValueView<'static>>(
            &client,
            &config,
            "test.Service",
            "ServerStream",
            StringValue::from("hello"),
            CallOptions::default(),
        )
        .await
        .expect_err("transport config error must surface from server stream");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.as_deref().unwrap().contains("with_tls"));
    }

    #[cfg(feature = "client")]
    #[tokio::test]
    async fn call_bidi_stream_preserves_transport_connect_error() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let (client, config) = plaintext_client_with_https_base();
        let mut stream = call_bidi_stream::<_, StringValue, StringValueView<'static>>(
            &client,
            &config,
            "test.Service",
            "Bidi",
            CallOptions::default(),
        )
        .await
        .expect("constructing bidi stream should succeed until the first receive");
        let err = stream
            .message()
            .await
            .expect_err("transport config error must surface from bidi receive");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.as_deref().unwrap().contains("with_tls"));
        assert_eq!(
            stream.error().map(|e| e.code),
            Some(ErrorCode::InvalidArgument)
        );
    }

    #[cfg(feature = "client")]
    #[tokio::test]
    async fn call_client_stream_preserves_transport_connect_error() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let (client, config) = plaintext_client_with_https_base();
        let err = call_client_stream::<_, StringValue, StringValueView<'static>>(
            &client,
            &config,
            "test.Service",
            "ClientStream",
            futures::stream::iter([StringValue::from("hello")]),
            CallOptions::default(),
        )
        .await
        .expect_err("transport config error must surface from client stream");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.as_deref().unwrap().contains("with_tls"));
    }

    // A transport whose `send()` future never resolves. It signals when it is
    // first polled and again when it is dropped, so a test can assert that
    // abandoning `call_client_stream` actually drops the in-flight transport
    // send future rather than leaking it in a detached task.
    #[cfg(feature = "client")]
    #[derive(Clone)]
    struct PendingSendTransport {
        started: std::sync::Arc<std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
        dropped: std::sync::Arc<std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    }

    #[cfg(feature = "client")]
    struct PendingSendFuture {
        // Hold the request (and thus the stream-backed body) without ever
        // reading it, so any request messages stay unread inside the body.
        // Dropping this future drops the request too.
        _request: Request<ClientBody>,
        started: Option<tokio::sync::oneshot::Sender<()>>,
        dropped: Option<tokio::sync::oneshot::Sender<()>>,
    }

    #[cfg(feature = "client")]
    impl std::future::Future for PendingSendFuture {
        type Output = Result<Response<Full<Bytes>>, std::io::Error>;

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if let Some(tx) = self.started.take() {
                let _ = tx.send(());
            }
            std::task::Poll::Pending
        }
    }

    #[cfg(feature = "client")]
    impl Drop for PendingSendFuture {
        fn drop(&mut self) {
            if let Some(tx) = self.dropped.take() {
                let _ = tx.send(());
            }
        }
    }

    #[cfg(feature = "client")]
    impl ClientTransport for PendingSendTransport {
        type ResponseBody = Full<Bytes>;
        type Error = std::io::Error;

        fn send(
            &self,
            request: Request<ClientBody>,
        ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
            // Single-shot: the streaming call paths invoke `send` exactly once.
            // Panic loudly rather than silently swallow the started/dropped
            // signals if that ever stops holding, which would otherwise hang the
            // test.
            let started = Some(
                self.started
                    .lock()
                    .unwrap()
                    .take()
                    .expect("PendingSendTransport::send called more than once"),
            );
            let dropped = Some(
                self.dropped
                    .lock()
                    .unwrap()
                    .take()
                    .expect("PendingSendTransport::send called more than once"),
            );
            Box::pin(PendingSendFuture {
                _request: request,
                started,
                dropped,
            })
        }
    }

    #[cfg(feature = "client")]
    fn pending_send_transport() -> (
        PendingSendTransport,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
        let transport = PendingSendTransport {
            started: std::sync::Arc::new(std::sync::Mutex::new(Some(started_tx))),
            dropped: std::sync::Arc::new(std::sync::Mutex::new(Some(dropped_tx))),
        };
        (transport, started_rx, dropped_rx)
    }

    // When the call deadline fires while the transport is still waiting for
    // response headers, the in-flight transport send must be dropped with the
    // call — nothing may keep polling it.
    #[cfg(feature = "client")]
    #[tokio::test(start_paused = true)]
    async fn client_stream_deadline_drops_transport_send() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let (transport, started_rx, dropped_rx) = pending_send_transport();
        let config = ClientConfig::new("http://localhost:8080".parse().unwrap());

        let mut call = Box::pin(
            call_client_stream::<_, StringValue, StringValueView<'static>>(
                &transport,
                &config,
                "test.Service",
                "ClientStream",
                futures::stream::empty::<StringValue>(),
                CallOptions::default().with_timeout(Duration::from_millis(100)),
            ),
        );

        // Drive the call until the transport send future is actually polled, so
        // the drop we assert below is provably the deadline path abandoning an
        // in-flight send rather than an unpolled future.
        tokio::select! {
            res = &mut call => panic!("call resolved before the transport was polled: {res:?}"),
            started = started_rx => started.expect("transport send future was never polled"),
        }

        // Now let the deadline fire.
        let err = call
            .await
            .expect_err("deadline must fire while the transport waits for headers");
        assert_eq!(err.code, ErrorCode::DeadlineExceeded);

        // The transport send future must be dropped now that the caller has
        // stopped waiting.
        tokio::time::timeout(Duration::from_secs(5), dropped_rx)
            .await
            .expect("transport send future was not dropped after the deadline fired")
            .expect("drop signal sender vanished without firing");
    }

    // When the caller drops the `call_client_stream` future (cancellation)
    // while the transport is still waiting for response headers, the in-flight
    // transport send must likewise be dropped. Cancellation is a distinct path
    // from deadline expiry — no deadline machinery fires here, so dropping the
    // call future must stop the in-flight send on its own.
    #[cfg(feature = "client")]
    #[tokio::test]
    async fn client_stream_cancellation_drops_transport_send() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let (transport, started_rx, dropped_rx) = pending_send_transport();
        let config = ClientConfig::new("http://localhost:8080".parse().unwrap());

        let mut call = Box::pin(
            call_client_stream::<_, StringValue, StringValueView<'static>>(
                &transport,
                &config,
                "test.Service",
                "ClientStream",
                futures::stream::empty::<StringValue>(),
                CallOptions::default(),
            ),
        );

        // Drive the call until the transport send future is polled, then
        // abandon it. `call` completing here would be a bug (the transport
        // never resolves), so treat that as a failure.
        tokio::select! {
            _ = &mut call => panic!("call completed though the transport never responded"),
            started = started_rx => started.expect("transport send future was never polled"),
        }
        drop(call);

        tokio::time::timeout(Duration::from_secs(5), dropped_rx)
            .await
            .expect("transport send future was not dropped after caller cancellation")
            .expect("drop signal sender vanished without firing");
    }

    // The earlier abandonment tests use an empty request stream. This one
    // abandons the call while the stream-backed request body still holds
    // unsent messages — the transport holds the request but never polls the
    // body. Proves an unfinished upload does not prevent the deadline from
    // dropping the in-flight send.
    #[cfg(feature = "client")]
    #[tokio::test(start_paused = true)]
    async fn client_stream_deadline_drops_send_with_unread_request_body() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let (transport, started_rx, dropped_rx) = pending_send_transport();
        let config = ClientConfig::new("http://localhost:8080".parse().unwrap());

        // Plenty of messages the transport will never pull from the body.
        let requests: Vec<StringValue> = (0..256)
            .map(|i| StringValue::from(format!("m{i}")))
            .collect();

        let mut call = Box::pin(
            call_client_stream::<_, StringValue, StringValueView<'static>>(
                &transport,
                &config,
                "test.Service",
                "ClientStream",
                stream_iter(requests),
                CallOptions::default().with_timeout(Duration::from_millis(100)),
            ),
        );

        // Drive the call until the transport send future is polled: by now the
        // caller is parked mid-drain on channel backpressure.
        tokio::select! {
            res = &mut call => panic!("call resolved before the transport was polled: {res:?}"),
            started = started_rx => started.expect("transport send future was never polled"),
        }

        let err = call
            .await
            .expect_err("deadline must fire while the upload is unfinished");
        assert_eq!(err.code, ErrorCode::DeadlineExceeded);

        tokio::time::timeout(Duration::from_secs(5), dropped_rx)
            .await
            .expect("transport send future was not dropped despite the unread request body")
            .expect("drop signal sender vanished without firing");
    }

    // Success path: a well-formed Connect client-streaming response decodes
    // normally through the directly-awaited transport send.
    #[cfg(feature = "client")]
    #[tokio::test]
    async fn client_stream_returns_well_formed_response() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        #[derive(Clone)]
        struct FixedResponseTransport {
            body: Bytes,
        }

        impl ClientTransport for FixedResponseTransport {
            type ResponseBody = Full<Bytes>;
            type Error = std::io::Error;

            fn send(
                &self,
                _request: Request<ClientBody>,
            ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
                let body = self.body.clone();
                Box::pin(async move {
                    let response = Response::builder()
                        .status(http::StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, "application/connect+proto")
                        .body(Full::new(body))
                        .unwrap();
                    Ok(response)
                })
            }
        }

        // DATA envelope carrying the response message, then an END_STREAM
        // envelope with empty (`{}`) trailers — the Connect client-stream
        // terminus.
        let data =
            crate::envelope::Envelope::data(Bytes::from(StringValue::from("ok").encode_to_vec()))
                .encode();
        let end = crate::envelope::Envelope::end_stream(Bytes::from_static(b"{}")).encode();
        let mut body = BytesMut::new();
        body.extend_from_slice(&data);
        body.extend_from_slice(&end);
        let transport = FixedResponseTransport {
            body: body.freeze(),
        };
        let config = ClientConfig::new("http://localhost:8080".parse().unwrap());

        let response = call_client_stream::<_, StringValue, StringValueView<'static>>(
            &transport,
            &config,
            "test.Service",
            "ClientStream",
            stream_iter([StringValue::from("req")]),
            CallOptions::default(),
        )
        .await
        .expect("well-formed client-streaming response must decode");
        assert_eq!(response.view().value, "ok");
    }

    // A transport send failure surfaces through the
    // `map_transport_send_error(e, "request failed")` branch.
    #[cfg(feature = "client")]
    #[tokio::test]
    async fn client_stream_transport_send_error_still_surfaces() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        #[derive(Clone)]
        struct FailingTransport;

        impl ClientTransport for FailingTransport {
            type ResponseBody = Full<Bytes>;
            type Error = std::io::Error;

            fn send(
                &self,
                _request: Request<ClientBody>,
            ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
                Box::pin(async { Err(std::io::Error::other("boom")) })
            }
        }

        let config = ClientConfig::new("http://localhost:8080".parse().unwrap());
        let err = call_client_stream::<_, StringValue, StringValueView<'static>>(
            &FailingTransport,
            &config,
            "test.Service",
            "ClientStream",
            stream_iter([StringValue::from("req")]),
            CallOptions::default(),
        )
        .await
        .expect_err("transport send failure must surface");
        assert_eq!(err.code, ErrorCode::Unavailable);
        let message = err.message.as_deref().unwrap_or_default();
        assert!(message.contains("request failed"), "unexpected: {message}");
        assert!(message.contains("boom"), "unexpected: {message}");
    }

    #[cfg(feature = "client")]
    #[tokio::test]
    async fn http_client_plaintext_rejects_https() {
        let client = HttpClient::plaintext();
        let req = Request::builder()
            .uri("https://localhost:8080/foo")
            .body(full_body(Bytes::new()))
            .unwrap();
        let err = client.send(req).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.as_deref().unwrap().contains("with_tls"));
    }

    #[cfg(all(feature = "client", feature = "client-tls"))]
    #[tokio::test]
    async fn http_client_with_tls_rejects_http() {
        let tls_config = std::sync::Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let client = HttpClient::with_tls(tls_config);
        let req = Request::builder()
            .uri("http://localhost:8080/foo")
            .body(full_body(Bytes::new()))
            .unwrap();
        let err = client.send(req).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.as_deref().unwrap().contains("plaintext"));
    }

    #[cfg(all(feature = "client", feature = "client-tls"))]
    #[test]
    fn http_client_with_tls_construction() {
        // Just verify construction with a minimal config doesn't panic.
        // Full TLS round-trip is in tests/streaming integration tests.
        let tls_config = std::sync::Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        let _client = HttpClient::with_tls(tls_config);
    }

    // ========================================================================
    // format_timeout tests
    // ========================================================================

    #[test]
    fn test_format_timeout_connect() {
        assert_eq!(
            format_timeout(Duration::from_millis(5000), Protocol::Connect),
            "5000"
        );
        assert_eq!(
            format_timeout(Duration::from_secs(0), Protocol::Connect),
            "0"
        );
    }

    #[test]
    fn test_format_timeout_connect_caps_at_10_digits() {
        // At the spec boundary (10 digits, ≈ 115 days).
        assert_eq!(
            format_timeout(Duration::from_millis(9_999_999_999), Protocol::Connect),
            "9999999999"
        );
        // Over the boundary → clamp at spec max. Without this, a large
        // Duration (e.g. from a test harness) produces an 11+ digit header
        // that both our own server and connect-go reject as malformed,
        // silently dropping the timeout.
        assert_eq!(
            format_timeout(Duration::from_secs(365 * 86400), Protocol::Connect),
            "9999999999"
        );
        assert_eq!(
            format_timeout(Duration::MAX, Protocol::Connect),
            "9999999999"
        );
    }

    #[test]
    fn connect_huge_timeout_clamps_before_deadline() {
        let encoded = encoded_timeout(Duration::MAX, Protocol::Connect);
        assert_eq!(encoded.header_value(), "9999999999");
        assert_eq!(
            encoded.duration(),
            Duration::from_millis(CONNECT_TIMEOUT_MAX_MILLIS)
        );
        assert!(client_deadline(Some(Duration::MAX), Protocol::Connect).is_some());
    }

    #[test]
    fn test_format_timeout_grpc_seconds() {
        assert_eq!(
            format_timeout(Duration::from_secs(30), Protocol::Grpc),
            "30S"
        );
    }

    #[test]
    fn test_format_timeout_grpc_milliseconds() {
        assert_eq!(
            format_timeout(Duration::from_millis(500), Protocol::Grpc),
            "500m"
        );
    }

    #[test]
    fn test_format_timeout_grpc_microseconds() {
        assert_eq!(
            format_timeout(Duration::from_micros(100), Protocol::Grpc),
            "100u"
        );
    }

    #[test]
    fn test_format_timeout_grpc_nanoseconds() {
        assert_eq!(
            format_timeout(Duration::from_nanos(999), Protocol::Grpc),
            "999n"
        );
    }

    #[test]
    fn test_format_timeout_grpc_zero() {
        assert_eq!(format_timeout(Duration::from_secs(0), Protocol::Grpc), "0n");
    }

    #[test]
    fn test_format_timeout_grpc_8_digit_limit() {
        // 99999999 seconds fits in 8 digits
        assert_eq!(
            format_timeout(Duration::from_secs(99_999_999), Protocol::Grpc),
            "99999999S"
        );
        // 100000000 seconds exceeds 8 digits — truncated to seconds
        assert_eq!(
            format_timeout(Duration::from_secs(100_000_000), Protocol::Grpc),
            "99999999S"
        );
    }

    #[test]
    fn grpc_huge_timeout_clamps_before_deadline() {
        let encoded = encoded_timeout(Duration::MAX, Protocol::Grpc);
        assert_eq!(encoded.header_value(), "99999999S");
        assert_eq!(
            encoded.duration(),
            Duration::from_secs(GRPC_TIMEOUT_MAX_SECONDS)
        );
        assert!(client_deadline(Some(Duration::MAX), Protocol::Grpc).is_some());
    }

    #[test]
    fn test_format_timeout_grpc_web_same_as_grpc() {
        assert_eq!(
            format_timeout(Duration::from_millis(500), Protocol::GrpcWeb),
            "500m"
        );
    }

    #[test]
    fn test_format_timeout_grpc_subsecond_nanosecond_residue() {
        // Instant arithmetic naturally produces sub-microsecond residue.
        // 100ms + 1ns must NOT truncate to "0S" (secs=0) — that would
        // tell the server the deadline already expired.
        assert_eq!(
            format_timeout(Duration::from_nanos(100_000_001), Protocol::Grpc),
            "100000u" // 100ms, 1ns truncated
        );
        let encoded = encoded_timeout(Duration::from_nanos(100_000_001), Protocol::Grpc);
        assert_eq!(encoded.duration(), Duration::from_micros(100_000));
        // Boundary: exactly 1ns over the 8-digit nano limit.
        assert_eq!(
            format_timeout(Duration::from_nanos(100_000_000), Protocol::Grpc),
            "100m" // exact millisecond, no-loss branch
        );
        // Longer duration with ns residue falls back to millis.
        assert_eq!(
            format_timeout(Duration::from_nanos(200_000_000_001), Protocol::Grpc),
            "200000m" // 200s, 1ns truncated; micros=200M would overflow 8 digits
        );
    }

    // ========================================================================
    // grpc_percent_decode tests
    // ========================================================================

    #[test]
    fn test_grpc_percent_decode_passthrough() {
        assert_eq!(grpc_percent_decode("hello world"), "hello world");
    }

    #[test]
    fn test_grpc_percent_decode_percent() {
        assert_eq!(grpc_percent_decode("100%25"), "100%");
    }

    #[test]
    fn test_grpc_percent_decode_newlines() {
        assert_eq!(grpc_percent_decode("a%0Ab"), "a\nb");
        assert_eq!(grpc_percent_decode("a%0D%0Ab"), "a\r\nb");
    }

    #[test]
    fn test_grpc_percent_decode_utf8_multibyte() {
        // café encoded as percent-encoded UTF-8 bytes
        assert_eq!(grpc_percent_decode("caf%C3%A9"), "café");
        // Unicode BMP: ☺ = U+263A = E2 98 BA
        assert_eq!(grpc_percent_decode("%E2%98%BA"), "☺");
        // Non-BMP: 😈 = U+1F608 = F0 9F 98 88
        assert_eq!(grpc_percent_decode("%F0%9F%98%88"), "😈");
    }

    #[test]
    fn test_grpc_percent_decode_partial_percent() {
        // Incomplete percent sequences are passed through
        assert_eq!(grpc_percent_decode("100%"), "100%");
        assert_eq!(grpc_percent_decode("a%2"), "a%2");
    }

    #[test]
    fn test_grpc_percent_decode_invalid_hex() {
        assert_eq!(grpc_percent_decode("a%ZZb"), "a%ZZb");
    }

    // ========================================================================
    // parse_grpc_error_from_trailers tests
    // ========================================================================

    #[test]
    fn test_parse_grpc_error_ok_returns_none() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        assert!(parse_grpc_error_from_trailers(&trailers).is_none());
    }

    #[test]
    fn test_parse_grpc_error_missing_status_returns_none() {
        let trailers = http::HeaderMap::new();
        assert!(parse_grpc_error_from_trailers(&trailers).is_none());
    }

    #[test]
    fn test_parse_grpc_error_with_code_and_message() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("5"));
        trailers.insert(
            "grpc-message",
            http::HeaderValue::from_static("not%20found"),
        );
        let err = parse_grpc_error_from_trailers(&trailers).unwrap();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.message.as_deref(), Some("not found"));
    }

    #[test]
    fn test_parse_grpc_error_unknown_code() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("99"));
        let err = parse_grpc_error_from_trailers(&trailers).unwrap();
        assert_eq!(err.code, ErrorCode::Unknown);
    }

    #[test]
    fn test_parse_grpc_error_custom_trailers() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("13"));
        trailers.insert("x-custom", http::HeaderValue::from_static("value"));
        let err = parse_grpc_error_from_trailers(&trailers).unwrap();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(
            err.trailers().get("x-custom").unwrap().to_str().unwrap(),
            "value"
        );
    }

    // grpc_status encode/decode tests are in the grpc_status module

    // ========================================================================
    // parse_grpc_web_trailer_frame_with_compression tests
    // ========================================================================

    #[test]
    fn test_parse_grpc_web_trailer_uncompressed() {
        let payload = b"grpc-status: 0\r\n";
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(0x80);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);

        let headers = parse_grpc_web_trailer_frame_with_compression(&frame, None).unwrap();
        assert_eq!(headers.get("grpc-status").unwrap().to_str().unwrap(), "0");
    }

    #[test]
    fn test_parse_grpc_web_trailer_with_error() {
        let payload = b"grpc-status: 13\r\ngrpc-message: internal error\r\n";
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(0x80);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);

        let headers = parse_grpc_web_trailer_frame_with_compression(&frame, None).unwrap();
        assert_eq!(headers.get("grpc-status").unwrap().to_str().unwrap(), "13");
        assert_eq!(
            headers.get("grpc-message").unwrap().to_str().unwrap(),
            "internal error"
        );
    }

    #[test]
    fn test_parse_grpc_web_trailer_truncated() {
        // Less than 5 bytes
        assert!(parse_grpc_web_trailer_frame_with_compression(&[0x80, 0, 0], None).is_none());
    }

    #[test]
    fn test_parse_grpc_web_trailer_not_trailer() {
        // Flag byte 0x00 — not a trailer
        let frame = [0x00, 0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o'];
        assert!(parse_grpc_web_trailer_frame_with_compression(&frame, None).is_none());
    }

    #[test]
    fn test_parse_grpc_web_trailer_compressed_no_registry() {
        // Flag 0x81 (compressed trailer) but no compression registry
        let payload = b"grpc-status: 0\r\n";
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(0x81);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        // Without compression registry, should return None
        assert!(parse_grpc_web_trailer_frame_with_compression(&frame, None).is_none());
    }

    #[test]
    fn test_parse_grpc_web_trailer_floods_do_not_panic() {
        // A hostile server can pack far more distinct trailer names than
        // `HeaderMap` can hold (`MAX_SIZE = 1 << 15 = 32_768`) into a payload
        // that stays under the 1 MiB byte cap. Each `hN:` line is short, so
        // 40_000 distinct names is only ~300 KiB. The parser must not panic.
        let mut payload = String::new();
        for i in 0..40_000u32 {
            payload.push_str(&format!("h{i}:\n"));
        }
        let payload = payload.into_bytes();
        assert!(
            payload.len() < 1024 * 1024,
            "payload must stay under byte cap"
        );

        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(0x80);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);

        // Before the fix this panicked with "size overflows MAX_SIZE".
        let headers = parse_grpc_web_trailer_frame_with_compression(&frame, None)
            .expect("flood frame is well-formed and should parse");
        // The map fills up to the type's hard ceiling and stops: it accepts
        // entries (the loop didn't drop everything) but never exceeds the cap.
        assert!(headers.keys_len() > 0, "early trailers should be retained");
        assert!(headers.keys_len() <= 1 << 15);
    }

    #[test]
    fn test_append_metadata_capped_copies_entries() {
        let mut metadata = HashMap::new();
        metadata.insert("grpc-status".to_string(), vec!["0".to_string()]);
        metadata.insert(
            "x-custom".to_string(),
            vec!["a".to_string(), "b".to_string()],
        );
        let mut trailers = http::HeaderMap::new();
        append_metadata_capped(&mut trailers, metadata);
        assert_eq!(trailers.get("grpc-status").unwrap(), "0");
        assert_eq!(trailers.get_all("x-custom").iter().count(), 2);
    }

    #[test]
    fn test_append_metadata_capped_does_not_panic_on_flood() {
        // A Connect end-stream `metadata` map is deserialized from server JSON,
        // so its key count is attacker-controlled. Feeding more distinct keys
        // than the `HeaderMap` ceiling (`MAX_SIZE = 1 << 15`) must cap rather
        // than panic with "size overflows MAX_SIZE".
        let mut metadata = HashMap::new();
        for i in 0..40_000u32 {
            metadata.insert(format!("h{i}"), vec![String::new()]);
        }
        let mut trailers = http::HeaderMap::new();
        append_metadata_capped(&mut trailers, metadata);
        assert!(trailers.keys_len() > 0, "early entries should be retained");
        assert!(trailers.keys_len() <= 1 << 15);
    }

    #[test]
    fn test_parse_grpc_web_trailer_newline_only() {
        // Some implementations use \n instead of \r\n
        let payload = b"grpc-status: 0\n";
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(0x80);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);

        let headers = parse_grpc_web_trailer_frame_with_compression(&frame, None).unwrap();
        assert_eq!(headers.get("grpc-status").unwrap().to_str().unwrap(), "0");
    }

    #[tokio::test]
    async fn grpc_unary_rejects_second_message_before_decompression() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        let mut body = BytesMut::new();
        body.extend_from_slice(&Envelope::data(Bytes::new()).encode());
        body.extend_from_slice(&Envelope::compressed(Bytes::from_static(b"not-gzip")).encode());

        let response = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc+proto")
            .body(Full::new(body.freeze()))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Unimplemented);
        assert_eq!(
            err.message.as_deref(),
            Some("received multiple response messages where exactly one was expected")
        );
    }

    #[tokio::test]
    async fn grpc_web_unary_stops_reading_after_trailer_frame() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        let mut body = BytesMut::new();
        body.extend_from_slice(&Envelope::data(Bytes::from_static(b"\x0a\x02hi")).encode());
        let trailer_payload = b"grpc-status: 0\r\n";
        body.extend_from_slice(&[0x80]);
        body.extend_from_slice(&(trailer_payload.len() as u32).to_be_bytes());
        body.extend_from_slice(trailer_payload);

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.send(Ok(body.freeze())).await.unwrap();
        tx.send(Ok(Bytes::from_static(b"server is still writing")))
            .await
            .unwrap();

        // Keep the sender alive: a complete trailers frame must finish the
        // response without waiting for EOF or consuming the queued bytes.
        let response = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc-web+proto")
            .body(ChannelBody { rx })
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::GrpcWeb);

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            parse_grpc_unary_response::<_, StringValueView<'static>>(
                response,
                &config,
                &CallOptions::default(),
                None,
            ),
        )
        .await
        .expect("parser should stop after the gRPC-Web trailer frame")
        .unwrap();
        assert_eq!(
            response.trailers().get("grpc-status").unwrap(),
            http::HeaderValue::from_static("0")
        );
    }

    #[tokio::test]
    async fn grpc_unary_trailers_only_status_wins_over_http_status() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        // A proxy pairs a non-200 status with a trailers-only gRPC error in the
        // initial headers. The gRPC code is the one reported, which for this
        // pairing is not the code the HTTP status maps to (500 is `unknown`).
        let response = Response::builder()
            .status(http::StatusCode::INTERNAL_SERVER_ERROR)
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-status", "3")
            .header("grpc-message", "malformed request")
            .header("x-request-id", "abc123")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("a trailers-only gRPC error must surface as an error");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert_eq!(err.message.as_deref(), Some("malformed request"));
        assert_eq!(
            err.response_headers().get("x-request-id").unwrap(),
            "abc123"
        );
        // Response headers are not trailing metadata, so nothing from the
        // header block may surface through `trailers()`.
        assert!(
            err.trailers().is_empty(),
            "initial headers must not be reported as trailers: {:?}",
            err.trailers()
        );
    }

    #[tokio::test]
    async fn grpc_unary_ignores_header_status_when_body_present() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        // Conformance `trailers-only/ignore-header-if-body-present`: a status
        // in the initial headers is only the status of a trailers-only
        // response. With a message in the body and no trailers, the status is
        // missing, not `9`.
        let response = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-status", "9")
            .body(Full::new(
                Envelope::data(StringValue::from("hi").encode_to_bytes()).encode(),
            ))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("a response with body data and no trailers has no status");
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(
            err.message.as_deref(),
            Some("gRPC response missing grpc-status trailer")
        );
    }

    #[tokio::test]
    async fn grpc_unary_ignores_header_status_when_trailer_present() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;
        use http_body::Frame;
        use http_body_util::StreamBody;

        // Conformance `trailers-only/ignore-header-if-trailer-present`: the
        // HTTP/2 trailer wins over the header.
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "9".parse().unwrap());
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> = vec![
            Ok(Frame::data(
                Envelope::data(StringValue::from("hi").encode_to_bytes()).encode(),
            )),
            Ok(Frame::trailers(trailers)),
        ];

        let response = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-status", "8")
            .body(StreamBody::new(futures::stream::iter(frames)))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("the trailer status must be reported");
        assert_eq!(err.code, ErrorCode::FailedPrecondition);
    }

    #[tokio::test]
    async fn grpc_web_unary_ignores_header_status_when_body_present() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        // Conformance `trailers-only/ignore-header-if-body-present` for
        // gRPC-Web: the trailer frame in the body wins over the header.
        let mut body = BytesMut::new();
        body.extend_from_slice(&Envelope::data(StringValue::from("hi").encode_to_bytes()).encode());
        let trailer_payload = b"grpc-status: 9\r\n";
        body.extend_from_slice(&[0x80]);
        body.extend_from_slice(&(trailer_payload.len() as u32).to_be_bytes());
        body.extend_from_slice(trailer_payload);

        let response = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc-web")
            .header("grpc-status", "8")
            .body(Full::new(body.freeze()))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::GrpcWeb);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("the trailer-frame status must be reported");
        assert_eq!(err.code, ErrorCode::FailedPrecondition);
    }

    #[tokio::test]
    async fn grpc_unary_non_200_with_body_reports_http_status() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;
        use http_body::Frame;
        use http_body_util::StreamBody;

        // A body means the header status does not apply, and a non-200 is an
        // error however successful the trailers claim the call was.
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> = vec![
            Ok(Frame::data(
                Envelope::data(StringValue::from("hi").encode_to_bytes()).encode(),
            )),
            Ok(Frame::trailers(trailers)),
        ];

        let response = Response::builder()
            .status(http::StatusCode::INTERNAL_SERVER_ERROR)
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-status", "3")
            .body(StreamBody::new(futures::stream::iter(frames)))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("a non-200 response is an error");
        assert_eq!(err.code, ErrorCode::Unknown);
        assert_eq!(err.message.as_deref(), Some("HTTP error 500"));
    }

    #[tokio::test]
    async fn grpc_web_unary_trailers_only_status_wins_over_http_status() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        // The header check is protocol-agnostic, so gRPC-Web reads a
        // trailers-only error the same way plain gRPC does.
        let response = Response::builder()
            .status(http::StatusCode::BAD_GATEWAY)
            .header(http::header::CONTENT_TYPE, "application/grpc-web")
            .header("grpc-status", "14")
            .header("grpc-message", "upstream connect error")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::GrpcWeb);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("a trailers-only gRPC-Web error must surface as an error");
        assert_eq!(err.code, ErrorCode::Unavailable);
        assert_eq!(err.message.as_deref(), Some("upstream connect error"));
    }

    #[tokio::test]
    async fn grpc_unary_malformed_grpc_status_on_non_200_is_unknown() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        // A present-but-unparseable status must not read as the HTTP status
        // either: it is a protocol error in its own right.
        let response = Response::builder()
            .status(http::StatusCode::INTERNAL_SERVER_ERROR)
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-status", "banana")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("a malformed gRPC status must not read as success");
        assert_eq!(err.code, ErrorCode::Unknown);
        assert_eq!(
            err.message.as_deref(),
            Some("protocol error: malformed grpc-status: \"banana\"")
        );
    }

    #[tokio::test]
    async fn grpc_unary_non_200_without_grpc_status_uses_http_status() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        let response = Response::builder()
            .status(http::StatusCode::SERVICE_UNAVAILABLE)
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("a non-200 response without a gRPC status is an error");
        assert_eq!(err.code, ErrorCode::Unavailable);
        assert_eq!(err.message.as_deref(), Some("HTTP error 503"));
    }

    #[tokio::test]
    async fn grpc_unary_rejects_trailer_flag_in_body() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use http_body::Frame;
        use http_body_util::StreamBody;

        // 0x80 is the gRPC-Web trailer marker and is not a gRPC envelope flag.
        // Honouring it here would replace the HTTP/2 trailers with whatever the
        // body claims.
        let trailer_payload = b"grpc-status: 5\r\n";
        let mut body = BytesMut::new();
        body.extend_from_slice(&[0x80]);
        body.extend_from_slice(&(trailer_payload.len() as u32).to_be_bytes());
        body.extend_from_slice(trailer_payload);

        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> = vec![
            Ok(Frame::data(body.freeze())),
            Ok(Frame::trailers(trailers)),
        ];

        let response = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc+proto")
            .body(StreamBody::new(futures::stream::iter(frames)))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("the trailer flag must be rejected on plain gRPC");
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(
            err.message.as_deref(),
            Some(
                "invalid gRPC response framing: envelope flag 0x80 (gRPC-Web trailer marker) is not valid on a plain gRPC response"
            )
        );
    }

    #[tokio::test]
    async fn grpc_unary_framing_error_names_the_offending_flag_byte() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        // Any byte with the high bit set takes the trailer branch, so the
        // error has to report the byte received rather than a bare 0x80.
        let mut body = BytesMut::new();
        body.extend_from_slice(&[0xC1]);
        body.extend_from_slice(&0u32.to_be_bytes());

        let response = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc+proto")
            .body(Full::new(body.freeze()))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("a high-bit flag byte must be rejected on plain gRPC");
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(
            err.message.as_deref(),
            Some(
                "invalid gRPC response framing: envelope flag 0xc1 (gRPC-Web trailer marker) is not valid on a plain gRPC response"
            )
        );
    }

    #[tokio::test]
    async fn grpc_web_unary_parses_trailer_frame_after_message() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;

        let mut body = BytesMut::new();
        body.extend_from_slice(&Envelope::data(StringValue::from("hi").encode_to_bytes()).encode());
        let trailer_payload = b"grpc-status: 0\r\nx-trailer: v\r\n";
        body.extend_from_slice(&[0x80]);
        body.extend_from_slice(&(trailer_payload.len() as u32).to_be_bytes());
        body.extend_from_slice(trailer_payload);

        let response = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc-web+proto")
            .body(Full::new(body.freeze()))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::GrpcWeb);

        let response = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect("gRPC-Web trailer frames must still be parsed");
        assert_eq!(response.view().value, "hi");
        assert_eq!(response.trailers().get("x-trailer").unwrap(), "v");
    }

    #[tokio::test]
    async fn grpc_unary_accepts_bare_application_grpc_with_parameters() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;
        use http_body::Frame;
        use http_body_util::StreamBody;

        let data = Envelope::data(StringValue::from("hi").encode_to_bytes()).encode();
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        let frames: Vec<Result<Frame<Bytes>, std::convert::Infallible>> =
            vec![Ok(Frame::data(data)), Ok(Frame::trailers(trailers))];

        let response = Response::builder()
            .header(
                http::header::CONTENT_TYPE,
                "application/grpc; charset=utf-8",
            )
            .body(StreamBody::new(futures::stream::iter(frames)))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let response = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect("bare application/grpc must be accepted as proto");
        assert_eq!(response.view().value, "hi");
        assert_eq!(response.trailers().get("grpc-status").unwrap(), "0");
    }

    #[tokio::test]
    async fn grpc_unary_rejects_grpc_web_content_type() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        let response = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc-web+proto")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("gRPC client must reject gRPC-Web content type");
        // Cross-family mismatches are `unknown` (not a gRPC response at all),
        // matching connect-go; only same-family codec mismatches are
        // `internal`.
        assert_eq!(err.code, ErrorCode::Unknown);
        assert_eq!(
            err.message.as_deref(),
            Some(
                "unexpected content-type: application/grpc-web+proto (expected application/grpc+proto)"
            )
        );
    }

    #[tokio::test]
    async fn grpc_unary_rejects_mismatched_codec_content_type() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;

        let response = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc+json")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = parse_grpc_unary_response::<_, StringValueView<'static>>(
            response,
            &config,
            &CallOptions::default(),
            None,
        )
        .await
        .expect_err("gRPC client must reject mismatched response codec");
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(
            err.message.as_deref(),
            Some(
                "unexpected content-type: application/grpc+json (expected application/grpc+proto)"
            )
        );
    }

    #[test]
    fn grpc_response_content_type_rejects_non_grpc_as_unknown() {
        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::CONTENT_TYPE, "text/html".parse().unwrap());
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);

        let err = validate_grpc_response_content_type(&headers, &config)
            .expect_err("non-gRPC content type must be rejected");
        assert_eq!(err.code, ErrorCode::Unknown);
        assert_eq!(
            err.message.as_deref(),
            Some("unexpected content-type: text/html (expected application/grpc+proto)")
        );
        assert!(
            err.response_headers()
                .contains_key(http::header::CONTENT_TYPE)
        );
    }

    #[test]
    fn grpc_response_content_type_accepts_missing_header() {
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);
        validate_grpc_response_content_type(&http::HeaderMap::new(), &config)
            .expect("missing content-type must remain accepted");
    }

    #[test]
    fn grpc_response_content_type_accepts_bare_for_json_codec() {
        // Proxies that synthesize trailers-only error replies (e.g. Envoy)
        // send bare `application/grpc` regardless of the request subtype, so
        // the bare type must be accepted for every codec, as in connect-go.
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            "application/grpc".parse().unwrap(),
        );
        let config = ClientConfig::new("http://localhost".parse().unwrap())
            .with_protocol(Protocol::Grpc)
            .with_codec_format(CodecFormat::Json);
        validate_grpc_response_content_type(&headers, &config)
            .expect("bare application/grpc must be accepted for a json-codec client");
    }

    #[test]
    fn grpc_web_response_content_type_accepts_bare() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            "application/grpc-web".parse().unwrap(),
        );
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::GrpcWeb);
        validate_grpc_response_content_type(&headers, &config)
            .expect("bare application/grpc-web must be accepted as proto");
    }

    #[test]
    fn grpc_web_response_content_type_rejects_grpc_as_unknown() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            "application/grpc+proto".parse().unwrap(),
        );
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::GrpcWeb);

        let err = validate_grpc_response_content_type(&headers, &config)
            .expect_err("gRPC-Web client must reject plain gRPC content type");
        assert_eq!(err.code, ErrorCode::Unknown);
        assert_eq!(
            err.message.as_deref(),
            Some(
                "unexpected content-type: application/grpc+proto (expected application/grpc-web+proto)"
            )
        );
    }

    // ========================================================================
    // Content type helper tests
    // ========================================================================

    #[test]
    fn test_unary_request_content_type_connect() {
        let config = ClientConfig::new("http://localhost".parse().unwrap());
        assert_eq!(unary_request_content_type(&config), "application/proto");

        let config = config.with_codec_format(CodecFormat::Json);
        assert_eq!(unary_request_content_type(&config), "application/json");
    }

    #[cfg(not(feature = "json"))]
    #[test]
    fn decode_response_view_json_is_unimplemented_without_feature() {
        use buffa::Message;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        use buffa_types::google::protobuf::StringValue;
        // Proto-only client: the response-decode JSON arm is compiled out and
        // surfaces `Unimplemented` instead of attempting serde. The
        // request-encode paths are the symmetric `return Err(Unimplemented)`
        // guards that fire before any transport I/O.
        let err = decode_response_view::<StringValueView>(
            Bytes::from_static(b"\"x\""),
            CodecFormat::Json,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Unimplemented);

        // Proto decoding still works.
        let bytes = StringValue::from("ok").encode_to_bytes();
        assert!(decode_response_view::<StringValueView>(bytes, CodecFormat::Proto).is_ok());
    }

    #[test]
    fn test_unary_request_content_type_grpc() {
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);
        assert_eq!(
            unary_request_content_type(&config),
            "application/grpc+proto"
        );

        let config = config.with_codec_format(CodecFormat::Json);
        assert_eq!(unary_request_content_type(&config), "application/grpc+json");
    }

    #[test]
    fn test_streaming_request_content_type() {
        let config = ClientConfig::new("http://localhost".parse().unwrap());
        assert_eq!(
            streaming_request_content_type(&config),
            "application/connect+proto"
        );

        let config = config.with_protocol(Protocol::Grpc);
        assert_eq!(
            streaming_request_content_type(&config),
            "application/grpc+proto"
        );

        let config = config.with_protocol(Protocol::GrpcWeb);
        assert_eq!(
            streaming_request_content_type(&config),
            "application/grpc-web+proto"
        );
    }

    // ========================================================================
    // http_status_to_error_code tests
    // ========================================================================

    #[test]
    fn test_http_status_to_error_code() {
        assert_eq!(
            http_status_to_error_code(http::StatusCode::BAD_REQUEST),
            ErrorCode::Internal
        );
        assert_eq!(
            http_status_to_error_code(http::StatusCode::UNAUTHORIZED),
            ErrorCode::Unauthenticated
        );
        assert_eq!(
            http_status_to_error_code(http::StatusCode::FORBIDDEN),
            ErrorCode::PermissionDenied
        );
        assert_eq!(
            http_status_to_error_code(http::StatusCode::NOT_FOUND),
            ErrorCode::Unimplemented
        );
        assert_eq!(
            http_status_to_error_code(http::StatusCode::SERVICE_UNAVAILABLE),
            ErrorCode::Unavailable
        );
        assert_eq!(
            http_status_to_error_code(http::StatusCode::INTERNAL_SERVER_ERROR),
            ErrorCode::Unknown
        );
    }

    // ========================================================================
    // add_unary_request_headers tests
    // ========================================================================

    #[test]
    fn test_add_unary_request_headers_connect() {
        let config = ClientConfig::new("http://localhost".parse().unwrap());
        let builder = http::Request::builder();
        let builder = add_unary_request_headers(builder, &config, None, None);
        let req = builder.body(()).unwrap();
        assert_eq!(
            req.headers().get("content-type").unwrap(),
            "application/proto"
        );
        assert_eq!(req.headers().get("connect-protocol-version").unwrap(), "1");
        assert!(req.headers().get("te").is_none());
    }

    #[test]
    fn test_add_unary_request_headers_grpc() {
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);
        let builder = http::Request::builder();
        let builder = add_unary_request_headers(builder, &config, None, None);
        let req = builder.body(()).unwrap();
        assert_eq!(
            req.headers().get("content-type").unwrap(),
            "application/grpc+proto"
        );
        assert_eq!(req.headers().get("te").unwrap(), "trailers");
        assert!(req.headers().get("connect-protocol-version").is_none());
    }

    #[test]
    fn test_add_unary_request_headers_grpc_web() {
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::GrpcWeb);
        let builder = http::Request::builder();
        let builder = add_unary_request_headers(builder, &config, None, None);
        let req = builder.body(()).unwrap();
        assert_eq!(
            req.headers().get("content-type").unwrap(),
            "application/grpc-web+proto"
        );
        assert!(req.headers().get("te").is_none());
        assert!(req.headers().get("connect-protocol-version").is_none());
    }

    #[test]
    fn test_add_unary_request_headers_with_timeout() {
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);
        let builder = http::Request::builder();
        let builder =
            add_unary_request_headers(builder, &config, Some(Duration::from_millis(500)), None);
        let req = builder.body(()).unwrap();
        assert_eq!(req.headers().get("grpc-timeout").unwrap(), "500m");
    }

    // ========================================================================
    // with_deadline (client-side timeout enforcement)
    // ========================================================================

    #[tokio::test]
    async fn with_deadline_none_passes_through() {
        let result: Result<i32, ConnectError> = with_deadline(None, async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn with_deadline_completes_before_deadline() {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let result: Result<i32, ConnectError> =
            with_deadline(Some(deadline), async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test(start_paused = true)]
    async fn with_deadline_fires_on_slow_future() {
        let deadline = std::time::Instant::now() + Duration::from_millis(100);
        let slow = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok::<i32, ConnectError>(42)
        };
        let result = with_deadline(Some(deadline), slow).await;
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::DeadlineExceeded);
    }

    #[tokio::test(start_paused = true)]
    async fn with_deadline_already_passed_returns_immediately() {
        // Deadline in the past — should return DeadlineExceeded without
        // polling the future (or at most once).
        let deadline = std::time::Instant::now() - Duration::from_secs(1);
        let result: Result<i32, ConnectError> =
            with_deadline(Some(deadline), std::future::pending()).await;
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::DeadlineExceeded);
    }

    #[tokio::test]
    async fn with_deadline_propagates_inner_error() {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let failing = async { Err::<i32, _>(ConnectError::internal("inner")) };
        let result = with_deadline(Some(deadline), failing).await;
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
    }

    // ========================================================================
    // ChannelBody (bidi request body)
    // ========================================================================

    #[tokio::test]
    async fn channel_body_delivers_frames_then_eof() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let body = ChannelBody { rx };

        tx.send(Ok(Bytes::from_static(b"hello"))).await.unwrap();
        tx.send(Ok(Bytes::from_static(b"world"))).await.unwrap();
        drop(tx); // close send side

        let collected = body.collect().await.unwrap().to_bytes();
        assert_eq!(&collected[..], b"helloworld");
    }

    #[tokio::test]
    async fn channel_body_surfaces_error() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut body = ChannelBody { rx };

        tx.send(Err(ConnectError::internal("boom"))).await.unwrap();
        drop(tx);

        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        assert!(matches!(frame, Some(Err(_))));
    }

    // ========================================================================
    // collect_body_bounded
    // ========================================================================

    #[tokio::test]
    async fn collect_body_bounded_within_limit() {
        let body = Full::new(Bytes::from_static(b"hello"));
        let got = collect_body_bounded(body, 10).await.unwrap();
        assert_eq!(&got[..], b"hello");
    }

    #[tokio::test]
    async fn collect_body_bounded_at_exact_limit() {
        let body = Full::new(Bytes::from_static(b"hello"));
        let got = collect_body_bounded(body, 5).await.unwrap();
        assert_eq!(&got[..], b"hello");
    }

    #[tokio::test]
    async fn collect_body_bounded_exceeds_limit() {
        let body = Full::new(Bytes::from_static(b"hello world"));
        let err = collect_body_bounded(body, 5).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
    }

    #[tokio::test]
    async fn collect_body_bounded_empty() {
        let body = Full::new(Bytes::new());
        let got = collect_body_bounded(body, 0).await.unwrap();
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn collect_body_bounded_multi_frame_exceeds_mid_stream() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let body = ChannelBody { rx };
        tx.send(Ok(Bytes::from_static(b"aaa"))).await.unwrap();
        tx.send(Ok(Bytes::from_static(b"bbb"))).await.unwrap();
        tx.send(Ok(Bytes::from_static(b"ccc"))).await.unwrap();
        drop(tx);
        // limit 7: first two frames (6 bytes) fit, third (3 more → 9) exceeds
        let err = collect_body_bounded(body, 7).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
    }

    #[tokio::test]
    async fn collect_body_bounded_multi_frame_within_limit() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let body = ChannelBody { rx };
        tx.send(Ok(Bytes::from_static(b"foo"))).await.unwrap();
        tx.send(Ok(Bytes::from_static(b"bar"))).await.unwrap();
        drop(tx);
        let got = collect_body_bounded(body, 10).await.unwrap();
        assert_eq!(&got[..], b"foobar");
    }

    #[tokio::test]
    async fn collect_body_bounded_propagates_body_error() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let body = ChannelBody { rx };
        tx.send(Err(ConnectError::internal("io"))).await.unwrap();
        drop(tx);
        let err = collect_body_bounded(body, 1024).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
    }

    #[test]
    fn test_add_streaming_request_headers_grpc() {
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);
        let builder = http::Request::builder();
        let builder = add_streaming_request_headers(builder, &config, None);
        let req = builder.body(()).unwrap();
        assert_eq!(
            req.headers().get("content-type").unwrap(),
            "application/grpc+proto"
        );
        assert_eq!(req.headers().get("te").unwrap(), "trailers");
    }

    // ========================================================================
    // ClientConfig builder tests
    // ========================================================================

    #[test]
    fn test_client_config_protocol() {
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).with_protocol(Protocol::Grpc);
        assert_eq!(config.protocol, Protocol::Grpc);
    }

    #[test]
    fn test_client_config_default_protocol() {
        let config = ClientConfig::new("http://localhost".parse().unwrap());
        assert_eq!(config.protocol, Protocol::Connect);
    }

    // ========================================================================
    // add_unary_request_headers — Content-Encoding only when actually compressed
    // ========================================================================

    fn headers_for(protocol: Protocol, applied_encoding: Option<&str>) -> http::HeaderMap {
        let config = ClientConfig::new("http://localhost".parse().unwrap())
            .with_protocol(protocol)
            .compress_requests("gzip");
        let builder = http::Request::builder();
        add_unary_request_headers(builder, &config, None, applied_encoding)
            .body(())
            .unwrap()
            .headers()
            .clone()
    }

    #[test]
    fn connect_unary_no_content_encoding_when_compression_skipped() {
        // Compression policy skipped this small body → NO Content-Encoding header.
        let headers = headers_for(Protocol::Connect, None);
        assert!(
            !headers.contains_key(http::header::CONTENT_ENCODING),
            "Content-Encoding must not be set when compression policy skipped the body"
        );
    }

    #[test]
    fn connect_unary_content_encoding_when_compressed() {
        let headers = headers_for(Protocol::Connect, Some("gzip"));
        assert_eq!(headers.get(http::header::CONTENT_ENCODING).unwrap(), "gzip");
    }

    #[test]
    fn grpc_unary_encoding_header_independent_of_applied() {
        // gRPC's grpc-encoding declares the algorithm used WHEN the envelope
        // flag is set — it's fine to send even if the policy didn't compress
        // this particular message. It's driven by config, not applied.
        let headers = headers_for(Protocol::Grpc, None);
        assert_eq!(headers.get("grpc-encoding").unwrap(), "gzip");
    }

    // ========================================================================
    // effective_options + merge_headers
    // ========================================================================

    fn test_config() -> ClientConfig {
        ClientConfig::new("http://localhost:8080".parse().unwrap())
    }

    #[test]
    fn effective_options_uses_config_defaults_when_options_unset() {
        let config = test_config()
            .with_default_timeout(Duration::from_secs(30))
            .with_default_max_message_size(1024)
            .with_default_header("x-trace-id", "cfg-trace");

        let eff = effective_options(&config, CallOptions::default());

        assert_eq!(eff.timeout, Some(Duration::from_secs(30)));
        assert_eq!(eff.max_message_size, Some(1024));
        assert_eq!(eff.headers.get("x-trace-id").unwrap(), "cfg-trace");
    }

    #[test]
    fn effective_options_options_override_config_defaults() {
        let config = test_config()
            .with_default_timeout(Duration::from_secs(30))
            .with_default_max_message_size(1024);

        let options = CallOptions::default()
            .with_timeout(Duration::from_secs(5))
            .with_max_message_size(512);

        let eff = effective_options(&config, options);

        assert_eq!(eff.timeout, Some(Duration::from_secs(5)));
        assert_eq!(eff.max_message_size, Some(512));
    }

    #[test]
    fn effective_options_compress_has_no_config_default() {
        let config = test_config();
        let options = CallOptions::default().with_compress(true);
        let eff = effective_options(&config, options);
        assert_eq!(eff.compress, Some(true));
    }

    #[test]
    fn merge_headers_options_override_config_same_name() {
        let mut cfg = http::HeaderMap::new();
        cfg.insert("x-token", "cfg-token".parse().unwrap());

        let mut opts = http::HeaderMap::new();
        opts.insert("x-token", "opt-token".parse().unwrap());

        let merged = merge_headers(&cfg, opts);
        let vals: Vec<_> = merged.get_all("x-token").iter().collect();
        assert_eq!(vals.len(), 1);
        assert_eq!(vals[0], "opt-token");
    }

    #[test]
    fn merge_headers_config_only_names_preserved() {
        let mut cfg = http::HeaderMap::new();
        cfg.insert("x-cfg-only", "kept".parse().unwrap());

        let mut opts = http::HeaderMap::new();
        opts.insert("x-opt-only", "also-kept".parse().unwrap());

        let merged = merge_headers(&cfg, opts);
        assert_eq!(merged.get("x-cfg-only").unwrap(), "kept");
        assert_eq!(merged.get("x-opt-only").unwrap(), "also-kept");
    }

    #[test]
    fn merge_headers_options_multivalue_replaces_config() {
        let mut cfg = http::HeaderMap::new();
        cfg.append("x-thing", "cfg-a".parse().unwrap());
        cfg.append("x-thing", "cfg-b".parse().unwrap());

        let mut opts = http::HeaderMap::new();
        opts.append("x-thing", "opt-1".parse().unwrap());
        opts.append("x-thing", "opt-2".parse().unwrap());

        let merged = merge_headers(&cfg, opts);
        let vals: Vec<_> = merged
            .get_all("x-thing")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(vals, vec!["opt-1", "opt-2"]);
    }

    #[test]
    fn merge_headers_empty_config_fast_path() {
        let cfg = http::HeaderMap::new();
        let mut opts = http::HeaderMap::new();
        opts.insert("x", "y".parse().unwrap());

        let merged = merge_headers(&cfg, opts);
        assert_eq!(merged.get("x").unwrap(), "y");
    }

    #[test]
    fn merge_headers_empty_options_fast_path() {
        let mut cfg = http::HeaderMap::new();
        cfg.insert("x", "y".parse().unwrap());
        let opts = http::HeaderMap::new();

        let merged = merge_headers(&cfg, opts);
        assert_eq!(merged.get("x").unwrap(), "y");
    }

    // ========================================================================
    // call_unary_get query encoding (Connect GET protocol)
    // ========================================================================

    /// The order the conformance suite checks for: `connect`, `base64`,
    /// `compression`, `encoding`, `message`. Servers accept any order; the
    /// recommended order keeps the variable-length `message` last so the
    /// prefix is stable for shared caches.
    fn assert_connect_get_param_order(query: &str) {
        const RANK: &[&str] = &["connect", "base64", "compression", "encoding", "message"];
        let mut last = 0;
        for pair in query.split('&') {
            let key = pair.split_once('=').map_or(pair, |(k, _)| k);
            let rank = RANK
                .iter()
                .position(|k| *k == key)
                .unwrap_or_else(|| panic!("unknown query parameter {key:?} in {query:?}"));
            assert!(
                rank >= last,
                "parameter {key:?} out of recommended order in {query:?}",
            );
            last = rank;
        }
    }

    #[test]
    fn get_query_param_order_proto() {
        let q = build_connect_get_query(true, None, "proto", "AAAA");
        assert_eq!(q, "connect=v1&base64=1&encoding=proto&message=AAAA");
        assert_connect_get_param_order(&q);
    }

    #[test]
    fn get_query_param_order_json_uncompressed() {
        let q = build_connect_get_query(false, None, "json", "%7B%7D");
        assert_eq!(q, "connect=v1&encoding=json&message=%7B%7D");
        assert_connect_get_param_order(&q);
    }

    #[test]
    fn get_query_param_order_compressed() {
        let q = build_connect_get_query(true, Some("gzip"), "proto", "H4sI");
        assert_eq!(
            q,
            "connect=v1&base64=1&compression=gzip&encoding=proto&message=H4sI",
        );
        assert_connect_get_param_order(&q);
    }

    #[test]
    fn get_query_param_order_json_compressed() {
        // Compressed JSON forces base64 (compressed bytes are binary).
        let q = build_connect_get_query(true, Some("gzip"), "json", "H4sI");
        assert_eq!(
            q,
            "connect=v1&base64=1&compression=gzip&encoding=json&message=H4sI",
        );
        assert_connect_get_param_order(&q);
    }

    #[test]
    fn get_base64_encoding_matches_rfc4648_urlsafe_no_pad() {
        // Verify we use the exact encoding the spec requires: RFC 4648 §5
        // URL-safe base64, no padding. Matches connect-go's
        // base64.RawURLEncoding.EncodeToString.
        use base64::Engine;
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"\xfa\xfb\xfc");
        // Standard base64 would be +vv8 (with +/). URL-safe: -_ instead.
        // 0xfa = 11111010, 0xfb = 11111011, 0xfc = 11111100
        // → 111110 101111 101111 1100(00) = 62 47 47 48 in b64 alphabet
        // URL-safe: 62='-', 47='v', 48='w' (wait, let me just check the output)
        assert!(!encoded.contains('+'), "URL-safe must not contain +");
        assert!(!encoded.contains('/'), "URL-safe must not contain /");
        assert!(!encoded.contains('='), "no-pad must not contain =");

        // Round-trip: our server's decode_get_message must accept this.
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&encoded)
            .unwrap();
        assert_eq!(decoded, b"\xfa\xfb\xfc");
    }

    // ========================================================================
    // parse_connect_client_stream_envelopes tests
    // ========================================================================

    /// A second data envelope is rejected before its payload is touched: the
    /// second envelope here is flagged compressed but contains garbage, so if
    /// the parser tried to decompress it the error would be a decompression
    /// failure rather than the multiple-messages error asserted below.
    #[cfg(feature = "gzip")]
    #[test]
    fn client_stream_response_rejects_second_message_before_decompression() {
        let registry = crate::compression::CompressionRegistry::default();

        let mut body = Envelope::data(Bytes::from_static(b"first"))
            .encode()
            .to_vec();
        body.extend_from_slice(
            &Envelope::compressed(Bytes::from_static(b"not gzip data")).encode(),
        );
        body.extend_from_slice(&Envelope::end_stream(Bytes::from_static(b"{}")).encode());

        let err = parse_connect_client_stream_envelopes(
            Bytes::from(body),
            &registry,
            Some("gzip"),
            1024 * 1024,
            &http::HeaderMap::new(),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Unimplemented);
        assert!(
            err.to_string().contains("multiple data messages"),
            "unexpected error: {err}"
        );
    }

    /// A malformed compressed response payload surfaces as `data_loss` on
    /// the client: the compression provider classifies malformed input as
    /// `invalid_argument` (sender fault), and on the response path the
    /// sender is the server, so the code is remapped rather than blaming
    /// the caller.
    #[cfg(feature = "gzip")]
    #[test]
    fn malformed_compressed_response_payload_is_data_loss() {
        let registry = crate::compression::CompressionRegistry::default();

        let mut body = Envelope::compressed(Bytes::from_static(b"not gzip data"))
            .encode()
            .to_vec();
        body.extend_from_slice(&Envelope::end_stream(Bytes::from_static(b"{}")).encode());

        let err = parse_connect_client_stream_envelopes(
            Bytes::from(body),
            &registry,
            Some("gzip"),
            1024 * 1024,
            &http::HeaderMap::new(),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::DataLoss, "unexpected error: {err}");
    }

    /// The response-path remap touches only the two decompression codes:
    /// `invalid_argument` → `data_loss`, `unimplemented` → `internal`;
    /// everything else passes through unchanged.
    #[test]
    fn response_decompression_error_remap() {
        let e = map_response_decompression_error(ConnectError::invalid_argument("corrupt"));
        assert_eq!(e.code, ErrorCode::DataLoss);
        let e = map_response_decompression_error(ConnectError::unimplemented("unknown encoding"));
        assert_eq!(e.code, ErrorCode::Internal);
        let e = map_response_decompression_error(ConnectError::resource_exhausted("too big"));
        assert_eq!(e.code, ErrorCode::ResourceExhausted);
    }

    /// Scanning stops at the END_STREAM envelope: the single message and the
    /// metadata trailers are returned, and trailing bytes after END_STREAM
    /// are ignored rather than decoded.
    #[test]
    fn client_stream_response_stops_at_end_stream() {
        let registry = crate::compression::CompressionRegistry::new();

        let mut body = Envelope::data(Bytes::from_static(b"only"))
            .encode()
            .to_vec();
        body.extend_from_slice(
            &Envelope::end_stream(Bytes::from_static(b"{\"metadata\":{\"x-extra\":[\"1\"]}}"))
                .encode(),
        );
        body.extend_from_slice(&[0xAA_u8; 256]);

        let (message, trailers) = parse_connect_client_stream_envelopes(
            Bytes::from(body),
            &registry,
            None,
            1024,
            &http::HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(&message[..], b"only");
        assert_eq!(trailers.get("x-extra").unwrap(), "1");
    }

    /// An END_STREAM envelope carrying an error surfaces it with the response
    /// headers and metadata trailers attached.
    #[test]
    fn client_stream_response_end_stream_error() {
        let registry = crate::compression::CompressionRegistry::new();

        let mut body = Envelope::data(Bytes::from_static(b"only"))
            .encode()
            .to_vec();
        body.extend_from_slice(
            &Envelope::end_stream(Bytes::from_static(
                b"{\"metadata\":{\"x-meta\":[\"m\"]},\"error\":{\"code\":\"resource_exhausted\",\"message\":\"too much\"}}",
            ))
            .encode(),
        );

        let mut resp_headers = http::HeaderMap::new();
        resp_headers.insert("x-from-headers", http::HeaderValue::from_static("yes"));

        let err = parse_connect_client_stream_envelopes(
            Bytes::from(body),
            &registry,
            None,
            1024,
            &resp_headers,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
        assert_eq!(err.message.as_deref(), Some("too much"));
        assert_eq!(
            err.response_headers().get("x-from-headers").unwrap(),
            "yes",
            "response headers must be attached to the END_STREAM error"
        );
        assert_eq!(
            err.trailers().get("x-meta").unwrap(),
            "m",
            "END_STREAM metadata must be attached to the error as trailers"
        );
    }

    /// Malformed Connect END_STREAM JSON is a protocol error. It must not be
    /// treated as an empty successful end-stream payload.
    #[test]
    fn client_stream_response_malformed_end_stream_json_errors() {
        let registry = crate::compression::CompressionRegistry::new();

        let mut body = Envelope::data(Bytes::from_static(b"only"))
            .encode()
            .to_vec();
        body.extend_from_slice(&Envelope::end_stream(Bytes::from_static(b"not json")).encode());

        let mut resp_headers = http::HeaderMap::new();
        resp_headers.insert("x-from-headers", http::HeaderValue::from_static("yes"));

        let err = parse_connect_client_stream_envelopes(
            Bytes::from(body),
            &registry,
            None,
            1024,
            &resp_headers,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(
            err.to_string()
                .contains("malformed Connect END_STREAM JSON"),
            "unexpected error: {err}"
        );
        assert_eq!(err.response_headers().get("x-from-headers").unwrap(), "yes");
        assert!(err.trailers().is_empty());
    }

    /// A body with no data envelope (END_STREAM only) is rejected.
    #[test]
    fn client_stream_response_requires_a_message() {
        let registry = crate::compression::CompressionRegistry::new();
        let body = Envelope::end_stream(Bytes::from_static(b"{}")).encode();

        let err = parse_connect_client_stream_envelopes(
            body,
            &registry,
            None,
            1024,
            &http::HeaderMap::new(),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Unimplemented);
        assert!(
            err.to_string().contains("no data messages"),
            "unexpected error: {err}"
        );
    }

    /// Compressed data and END_STREAM envelopes decompress through the
    /// registry and behave like their uncompressed equivalents.
    #[cfg(feature = "gzip")]
    #[test]
    fn client_stream_response_compressed_envelopes() {
        use crate::compression::{CompressionProvider, GzipProvider};

        let registry = crate::compression::CompressionRegistry::default();
        let gzip = GzipProvider::default();

        let mut body = Envelope::compressed(gzip.compress(b"only").unwrap())
            .encode()
            .to_vec();
        let mut end_stream = Envelope::compressed(
            gzip.compress(b"{\"metadata\":{\"x-extra\":[\"1\"]}}")
                .unwrap(),
        )
        .encode()
        .to_vec();
        end_stream[0] |= 0x02; // also set the END_STREAM flag
        body.extend_from_slice(&end_stream);

        let (message, trailers) = parse_connect_client_stream_envelopes(
            Bytes::from(body),
            &registry,
            Some("gzip"),
            1024 * 1024,
            &http::HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(&message[..], b"only");
        assert_eq!(trailers.get("x-extra").unwrap(), "1");
    }

    /// A data envelope that only appears after the END_STREAM envelope does
    /// not count as the response message: the scan stops at END_STREAM, so
    /// the response is rejected for having no data message.
    #[test]
    fn client_stream_response_data_after_end_stream_is_not_a_message() {
        let registry = crate::compression::CompressionRegistry::new();

        let mut body = Envelope::end_stream(Bytes::from_static(b"{}"))
            .encode()
            .to_vec();
        body.extend_from_slice(&Envelope::data(Bytes::from_static(b"late")).encode());

        let err = parse_connect_client_stream_envelopes(
            Bytes::from(body),
            &registry,
            None,
            1024,
            &http::HeaderMap::new(),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Unimplemented);
        assert!(
            err.to_string().contains("no data messages"),
            "unexpected error: {err}"
        );
    }

    /// A data envelope followed by EOF, with no END_STREAM envelope, is a
    /// truncated response rather than a completed one: it is rejected with
    /// `internal` instead of succeeding with empty trailers, matching the
    /// `ServerStream` Connect EOF behavior.
    #[test]
    fn client_stream_response_requires_end_stream_after_message() {
        let registry = crate::compression::CompressionRegistry::new();

        let body = Envelope::data(Bytes::from_static(b"only")).encode();

        let err = parse_connect_client_stream_envelopes(
            body,
            &registry,
            None,
            1024,
            &http::HeaderMap::new(),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(
            err.message.as_deref(),
            Some("Connect streaming response ended without END_STREAM envelope"),
        );
    }

    /// A data envelope followed by a truncated END_STREAM envelope (its
    /// declared payload never arrives) is also a truncated response: the
    /// partial envelope decodes to "needs more data", so END_STREAM is never
    /// observed and the response is rejected with `internal`.
    #[test]
    fn client_stream_response_requires_complete_end_stream_after_message() {
        let registry = crate::compression::CompressionRegistry::new();

        let mut body = Envelope::data(Bytes::from_static(b"only"))
            .encode()
            .to_vec();
        let end_stream = Envelope::end_stream(Bytes::from_static(b"{}")).encode();
        // Append everything but the final byte of the END_STREAM envelope.
        body.extend_from_slice(&end_stream[..end_stream.len() - 1]);

        let err = parse_connect_client_stream_envelopes(
            Bytes::from(body),
            &registry,
            None,
            1024,
            &http::HeaderMap::new(),
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(
            err.message.as_deref(),
            Some("Connect streaming response ended without END_STREAM envelope"),
        );
    }

    /// A data envelope followed by an empty END_STREAM envelope (`{}`) is a
    /// complete response: the message is returned with empty trailers.
    #[test]
    fn client_stream_response_end_stream_completes_the_response() {
        let registry = crate::compression::CompressionRegistry::new();

        let mut body = Envelope::data(Bytes::from_static(b"only"))
            .encode()
            .to_vec();
        body.extend_from_slice(&Envelope::end_stream(Bytes::from_static(b"{}")).encode());

        let (message, trailers) = parse_connect_client_stream_envelopes(
            Bytes::from(body),
            &registry,
            None,
            1024,
            &http::HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(&message[..], b"only");
        assert!(trailers.is_empty());
    }
}
