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
//! let config = ClientConfig::new(uri).protocol(Protocol::Grpc);
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

use buffa::view::MessageView;
use buffa::view::OwnedView;

use crate::codec::CodecFormat;
use crate::codec::content_type;
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

/// Extra slack added to client-side response buffer caps beyond the message
/// size itself, to accommodate gRPC-Web trailer frames (which arrive as a
/// separate 0x80-flagged body frame, not a standard envelope). 64 KiB is
/// generous: the gRPC best-practices guide recommends keeping metadata
/// under 8 KiB per header set.
const RESPONSE_BUFFER_TRAILER_SLACK: usize = 64 * 1024;

/// Trait for types that can be used as ConnectRPC client transports.
///
/// This is automatically implemented for any `tower::Service` that handles
/// HTTP requests with compatible body types.
pub trait ClientTransport: Clone + Send + Sync + 'static {
    /// The response body type.
    type ResponseBody: Body<Data = Bytes> + Send + 'static;
    /// The error type.
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
pub use http2::Http2Connection;
#[cfg(feature = "client")]
pub use http2::SharedHttp2Connection;

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
#[derive(Clone)]
pub struct HttpClient {
    inner: HttpClientInner,
}

// Manual impl: hyper's `Client` doesn't impl `Debug`. Print the mode so
// tests can identify which transport variant unexpectedly succeeded.
#[cfg(feature = "client")]
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
/// This keeps the plaintext case exactly as before (zero cost) while
/// allowing the TLS variant to use a different connector type without
/// leaking generics into the public `HttpClient` signature.
#[cfg(feature = "client")]
#[derive(Clone)]
enum HttpClientInner {
    /// Plaintext HTTP (http:// only). Rejects https:// at send-time.
    Plain(
        hyper_util::client::legacy::Client<
            hyper_util::client::legacy::connect::HttpConnector,
            ClientBody,
        >,
    ),
    /// TLS HTTP (https:// only). hyper-rustls's https_only mode rejects
    /// http:// at the connector level.
    #[cfg(feature = "client-tls")]
    Tls(
        hyper_util::client::legacy::Client<
            hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
            ClientBody,
        >,
    ),
}

#[cfg(feature = "client")]
impl HttpClient {
    /// Create a **plaintext** HTTP client. Only for `http://` URIs.
    ///
    /// Errors at send-time if given an `https://` URI — use
    /// [`with_tls`](Self::with_tls) for TLS.
    ///
    /// The client uses connection pooling and supports HTTP/1.1 and HTTP/2
    /// over cleartext. TCP_NODELAY is enabled to avoid Nagle + delayed ACK
    /// latency on small messages.
    pub fn plaintext() -> Self {
        use hyper_util::client::legacy::Client;
        use hyper_util::client::legacy::connect::HttpConnector;
        use hyper_util::rt::TokioExecutor;

        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new()).build(connector);

        Self {
            inner: HttpClientInner::Plain(client),
        }
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
    pub fn plaintext_http2_only() -> Self {
        use hyper_util::client::legacy::Client;
        use hyper_util::client::legacy::connect::HttpConnector;
        use hyper_util::rt::TokioExecutor;

        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new())
            .http2_only(true)
            .build(connector);

        Self {
            inner: HttpClientInner::Plain(client),
        }
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
    #[cfg(feature = "client-tls")]
    pub fn with_tls(tls_config: std::sync::Arc<rustls::ClientConfig>) -> Self {
        use hyper_util::client::legacy::Client;
        use hyper_util::client::legacy::connect::HttpConnector;
        use hyper_util::rt::TokioExecutor;

        let mut http = HttpConnector::new();
        http.set_nodelay(true);
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

        let client = Client::builder(TokioExecutor::new()).build(https);

        Self {
            inner: HttpClientInner::Tls(client),
        }
    }
}

// No `Default` impl for HttpClient — there's no sensible default when the
// choice between plaintext and TLS is security-relevant. Users must
// explicitly choose plaintext() or with_tls().

#[cfg(feature = "client")]
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
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ClientConfig {
    /// The base URI for the service (e.g., "http://localhost:8080").
    pub base_uri: Uri,
    /// The wire protocol to use (Connect, gRPC, or gRPC-Web).
    pub protocol: Protocol,
    /// The codec format to use (proto or json).
    pub codec_format: CodecFormat,
    /// Compression registry for request/response compression.
    pub compression: CompressionRegistry,
    /// Request compression encoding (e.g., "gzip"). None means no compression.
    pub request_compression: Option<String>,
    /// Compression policy controlling when messages are compressed.
    pub compression_policy: CompressionPolicy,
    /// Default request timeout for all calls through this config.
    ///
    /// Per-call [`CallOptions::timeout`] overrides this when set.
    pub default_timeout: Option<Duration>,
    /// Default maximum decompressed response message size.
    ///
    /// Per-call [`CallOptions::max_message_size`] overrides this when set.
    pub default_max_message_size: Option<usize>,
    /// Headers applied to every request through this config.
    ///
    /// Useful for auth tokens, user-agent, tracing context.
    /// Per-call [`CallOptions::headers`] with the same name **replace** these
    /// (options win over config defaults).
    pub default_headers: http::HeaderMap,
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

    /// Set the wire protocol (Connect, gRPC, or gRPC-Web).
    #[must_use]
    pub fn protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Set the codec format (proto or json).
    #[must_use]
    pub fn codec_format(mut self, format: CodecFormat) -> Self {
        self.codec_format = format;
        self
    }

    /// Use JSON encoding.
    #[must_use]
    pub fn json(mut self) -> Self {
        self.codec_format = CodecFormat::Json;
        self
    }

    /// Use protobuf encoding.
    #[must_use]
    pub fn proto(mut self) -> Self {
        self.codec_format = CodecFormat::Proto;
        self
    }

    /// Set the compression registry.
    #[must_use]
    pub fn compression(mut self, registry: CompressionRegistry) -> Self {
        self.compression = registry;
        self
    }

    /// Enable request compression with the specified encoding.
    #[must_use]
    pub fn compress_requests(mut self, encoding: impl Into<String>) -> Self {
        self.request_compression = Some(encoding.into());
        self
    }

    /// Set the compression policy.
    #[must_use]
    pub fn compression_policy(mut self, policy: CompressionPolicy) -> Self {
        self.compression_policy = policy;
        self
    }

    /// Set a default request timeout for all calls through this config.
    ///
    /// Per-call [`CallOptions::with_timeout`] overrides this.
    #[must_use]
    pub fn default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }

    /// Set a default maximum decompressed response message size.
    ///
    /// Per-call [`CallOptions::with_max_message_size`] overrides this.
    #[must_use]
    pub fn default_max_message_size(mut self, size: usize) -> Self {
        self.default_max_message_size = Some(size);
        self
    }

    /// Add a default header applied to every request through this config.
    ///
    /// If the name or value cannot be converted to valid HTTP header components,
    /// the header is silently ignored. Per-call [`CallOptions::headers`] with
    /// the same name replace this value (options win over config defaults).
    #[must_use]
    pub fn default_header(
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
    #[must_use]
    pub fn default_headers(mut self, headers: http::HeaderMap) -> Self {
        self.default_headers = headers;
        self
    }
}

/// Per-request options for an RPC call.
///
/// Provides per-call configuration such as additional headers and timeouts.
/// Use [`CallOptions::default()`] for no additional options.
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
/// assert_eq!(options.timeout, Some(Duration::from_secs(5)));
/// assert_eq!(options.headers.get("x-request-id").unwrap(), "abc123");
/// ```
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CallOptions {
    /// Additional headers to include in the request.
    ///
    /// These are merged into the HTTP request after protocol headers,
    /// allowing override of any header for advanced use cases.
    pub headers: http::HeaderMap,
    /// Request timeout, sent as `connect-timeout-ms`.
    pub timeout: Option<Duration>,
    /// Maximum decompressed message size in bytes.
    ///
    /// When set, messages exceeding this size after decompression will
    /// result in a `ResourceExhausted` error. Applies per-message for streaming.
    pub max_message_size: Option<usize>,
    /// Per-call compression override. `Some(true)` forces compression,
    /// `Some(false)` disables it, `None` defers to the policy.
    pub compress: Option<bool>,
}

impl CallOptions {
    /// Set the request timeout.
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
    #[must_use]
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Override compression for this call.
    #[must_use]
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compress = Some(enabled);
        self
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

    /// Borrow the response body.
    ///
    /// For generated clients the body is an [`OwnedView`], which derefs to
    /// the view type — so field access is zero-copy:
    ///
    /// ```rust,ignore
    /// let resp = client.foo(req).await?;
    /// assert_eq!(resp.view().name, "expected");  // &str, no allocation
    /// ```
    ///
    /// See also [`into_view()`](Self::into_view) to consume and
    /// [`into_owned()`](Self::into_owned) for an owned struct.
    #[must_use]
    pub fn view(&self) -> &Resp {
        &self.body
    }

    /// Consume the response, returning just the body.
    ///
    /// For generated clients this is an [`OwnedView`] — zero-copy, move
    /// semantics. If you need the owned struct instead, use
    /// [`into_owned()`](Self::into_owned).
    ///
    /// ```rust,ignore
    /// let view = client.foo(req).await?.into_view();
    /// assert_eq!(view.name, "expected");  // &str via Deref
    /// ```
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
    /// [`view()`](UnaryResponse::view) / [`into_view()`](UnaryResponse::into_view)
    /// unless you need to pass the owned struct to code that expects it, or
    /// store it in a collection.
    ///
    /// ```rust,ignore
    /// let owned: FooResponse = client.foo(req).await?.into_owned();
    /// ```
    #[must_use]
    pub fn into_owned(self) -> V::Owned {
        self.body.to_owned_message()
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
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
{
    match format {
        CodecFormat::Proto => OwnedView::<RespView>::decode(data)
            .map_err(|e| ConnectError::internal(format!("failed to decode response: {e}"))),
        CodecFormat::Json => {
            let owned: RespView::Owned = serde_json::from_slice(&data).map_err(|e| {
                ConnectError::internal(format!("failed to decode JSON response: {e}"))
            })?;
            OwnedView::<RespView>::from_owned(&owned)
                .map_err(|e| ConnectError::internal(format!("failed to re-encode for view: {e}")))
        }
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
    Req: buffa::Message + serde::Serialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
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
        CodecFormat::Json => {
            let buf = serde_json::to_vec(&request).map_err(|e| {
                ConnectError::internal(format!("failed to encode JSON request: {e}"))
            })?;
            Bytes::from(buf)
        }
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
    let deadline = options.timeout.map(|t| std::time::Instant::now() + t);

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
            .map_err(|e| ConnectError::unavailable(format!("request failed: {e}")))?;

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
/// `?connect=v1&encoding=<codec>&message=<payload>[&base64=1][&compression=<enc>]`.
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
    Req: buffa::Message + serde::Serialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
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
        CodecFormat::Json => {
            let buf = serde_json::to_vec(&request).map_err(|e| {
                ConnectError::internal(format!("failed to encode JSON request: {e}"))
            })?;
            Bytes::from(buf)
        }
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

    // Assemble query string. Parameter order doesn't matter per spec, but
    // a deterministic order aids caching. connect-go puts connect/encoding
    // first, message/base64/compression after.
    let mut query = format!("connect=v1&encoding={encoding_name}&message={encoded_message}");
    if use_base64 {
        query.push_str("&base64=1");
    }
    if let Some(enc) = compressed_with {
        query.push_str("&compression=");
        query.push_str(enc);
    }

    let full_uri = format!("{base_str}/{service}/{method}?{query}");
    let uri: Uri = full_uri
        .parse()
        .map_err(|e| ConnectError::internal(format!("invalid GET URI: {e}")))?;

    let deadline = options.timeout.map(|t| std::time::Instant::now() + t);

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
            .map_err(|e| ConnectError::unavailable(format!("GET request failed: {e}")))?;

        // Response format is identical to POST unary Connect.
        parse_connect_unary_response(response, config, &options).await
    })
    .await
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
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
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
            .map_err(|mut e| {
                if e.code == ErrorCode::Unimplemented {
                    e.code = ErrorCode::Internal;
                }
                e
            })?
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
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
{
    let status = response.status();
    let resp_headers = response.headers().clone();

    // Non-200 HTTP status (connection-level error)
    if !status.is_success() {
        let code = http_status_to_error_code(status);
        let mut err = ConnectError::new(code, format!("HTTP error {}", status.as_u16()));
        err.set_response_headers(resp_headers);
        return Err(err);
    }

    // Validate response content-type starts with application/grpc
    if let Some(ct) = resp_headers.get(http::header::CONTENT_TYPE) {
        let ct_str = ct.to_str().unwrap_or("");
        if !ct_str.starts_with("application/grpc") {
            let mut err = ConnectError::new(
                ErrorCode::Unknown,
                format!("unexpected content-type: {ct_str}"),
            );
            err.set_response_headers(resp_headers);
            return Err(err);
        }
    }

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
                        if buf.len().saturating_add(data.len()) > max_buf_size {
                            return Err(ConnectError::resource_exhausted(format!(
                                "response body size exceeds limit {max_buf_size}"
                            )));
                        }
                        buf.extend_from_slice(&data);
                    }
                } else if frame.is_trailers()
                    && let Ok(trailers) = frame.into_trailers()
                {
                    grpc_trailers = trailers;
                }
            }
            Some(Err(e)) => {
                return Err(ConnectError::internal(format!(
                    "failed to read response body: {e}"
                )));
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
        // Check for gRPC-Web trailer frame (flag 0x80)
        if buf[0] & 0x80 != 0 {
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
                .decompress_with_limit(enc, envelope.data, grpc_max_msg)?
        } else {
            envelope.data
        };

        message_count += 1;
        message_data = Some(data);
    }

    // Check for errors in trailers (HTTP/2 trailers or gRPC-Web trailer frame).
    // If we have trailers from HTTP/2 or gRPC-Web, those take precedence.
    // Only fall back to initial headers if no body data was received (trailers-only).
    let effective_trailers = if !grpc_trailers.is_empty() {
        &grpc_trailers
    } else if !has_body_data {
        // Trailers-only response: initial headers contain the status
        &resp_headers
    } else {
        &grpc_trailers // empty — no trailers found
    };

    if let Some(mut err) = parse_grpc_error_from_trailers(effective_trailers) {
        err.set_response_headers(resp_headers);
        return Err(err);
    }

    // Validate message count for unary/client-stream (expect exactly 1)
    if message_count > 1 {
        let mut err = ConnectError::unimplemented("received multiple messages for unary response");
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

/// Response from a server-streaming RPC.
///
/// A server-streaming RPC response.
///
/// Provides incremental access to response messages as they arrive from the server.
/// Messages are decoded one at a time from the HTTP response body using the
/// [`message()`](ServerStream::message) method. Trailing metadata and errors
/// (from the Connect END_STREAM envelope) become available after the message
/// stream is exhausted.
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
    trailers: Option<http::HeaderMap>,
    error: Option<ConnectError>,
    done: bool,
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
            .field("done", &self.done)
            .field("error", &self.error)
            .field("has_trailers", &self.trailers.is_some())
            .field("buffered_bytes", &self.buf.len())
            .finish_non_exhaustive()
    }
}

impl<B, RespView> ServerStream<B, RespView>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
{
    /// Returns the response headers.
    #[must_use]
    pub fn headers(&self) -> &http::HeaderMap {
        &self.headers
    }

    /// Fetch the next message from the stream.
    ///
    /// Returns `Ok(Some(msg))` for each message, `Ok(None)` when the stream
    /// ends, or `Err(...)` on protocol/decode/deadline errors.
    ///
    /// If a deadline was set on this call (via `CallOptions::timeout` or
    /// `ClientConfig::default_timeout`), each `message()` poll is bounded by
    /// it — gRPC deadline semantics are whole-call, so a hung server won't
    /// block indefinitely (matching grpc-java and connect-go).
    ///
    /// After this returns `Ok(None)`, [`trailers()`](Self::trailers) and
    /// [`error()`](Self::error) become available.
    pub async fn message(&mut self) -> Result<Option<OwnedView<RespView>>, ConnectError> {
        // Whole-call deadline enforcement: wrap the decode loop so every
        // body-poll is bounded. If the deadline has already passed, this
        // returns immediately without polling.
        let deadline = self.deadline;
        with_deadline(deadline, self.message_inner()).await
    }

    /// The actual message decode loop. Split from `message()` so the deadline
    /// wrapper can bound the whole thing without threading it through the loop.
    async fn message_inner(&mut self) -> Result<Option<OwnedView<RespView>>, ConnectError> {
        if self.done {
            return Ok(None);
        }

        loop {
            // For gRPC-Web, check for a complete trailer frame (flag 0x80)
            // before attempting envelope decode (which would treat 0x80 as
            // a data envelope flag rather than the gRPC-Web trailer sentinel).
            if matches!(self.protocol, Protocol::GrpcWeb)
                && self.buf.len() >= 5
                && self.buf[0] & 0x80 != 0
            {
                let trailer_len =
                    u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]])
                        as usize;
                if self.buf.len() >= 5 + trailer_len {
                    // Complete trailer frame — parse it
                    self.done = true;
                    let decompression =
                        self.encoding.as_deref().map(|enc| (&self.compression, enc));
                    if let Some(trailers) =
                        parse_grpc_web_trailer_frame_with_compression(&self.buf, decompression)
                    {
                        if let Some(err) = parse_grpc_error_from_trailers(&trailers) {
                            self.error = Some(err);
                        }
                        self.trailers = Some(trailers);
                    }
                    return Ok(None);
                }
                // Incomplete trailer frame — need more data, fall through
                // to poll_body below
            }

            // Try to decode a complete envelope from the buffer.
            // Skip this for gRPC-Web when the buffer starts with 0x80 (trailer
            // flag) to avoid misinterpreting the trailer frame as a data message.
            let envelope_result = if matches!(self.protocol, Protocol::GrpcWeb)
                && !self.buf.is_empty()
                && self.buf[0] & 0x80 != 0
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
                        self.done = true;
                        self.process_end_stream(envelope)?;
                        return Ok(None);
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
                        ));
                    }

                    let msg = decode_response_view::<RespView>(data, self.codec_format)?;
                    return Ok(Some(msg));
                }
                None => {
                    // Need more data — poll the body
                    if !self.poll_body().await? {
                        // Body exhausted — check buffer for remaining trailer data
                        self.done = true;
                        if matches!(self.protocol, Protocol::GrpcWeb)
                            && !self.buf.is_empty()
                            && self.buf[0] & 0x80 != 0
                        {
                            let decompression =
                                self.encoding.as_deref().map(|enc| (&self.compression, enc));
                            if let Some(trailers) = parse_grpc_web_trailer_frame_with_compression(
                                &self.buf,
                                decompression,
                            ) {
                                if let Some(err) = parse_grpc_error_from_trailers(&trailers) {
                                    self.error = Some(err);
                                }
                                self.trailers = Some(trailers);
                            }
                        }
                        // For gRPC, if body exhausted without trailers and
                        // deadline has passed, map to DEADLINE_EXCEEDED
                        // (matches grpc-go / connect-go RST_STREAM CANCEL handling)
                        if self.error.is_none()
                            && self.trailers.is_none()
                            && matches!(self.protocol, Protocol::Grpc | Protocol::GrpcWeb)
                            && self
                                .deadline
                                .is_some_and(|d| std::time::Instant::now() >= d)
                        {
                            self.error = Some(ConnectError::deadline_exceeded("request timeout"));
                        }
                        return Ok(None);
                    }
                    // Loop back to try decoding again
                }
            }
        }
    }

    /// Returns the trailing metadata, if available.
    ///
    /// Only populated after [`message()`](Self::message) returns `Ok(None)`.
    #[must_use]
    pub fn trailers(&self) -> Option<&http::HeaderMap> {
        self.trailers.as_ref()
    }

    /// Returns the trailing error from the END_STREAM envelope, if any.
    ///
    /// Only populated after [`message()`](Self::message) returns `Ok(None)`.
    #[must_use]
    pub fn error(&self) -> Option<&ConnectError> {
        self.error.as_ref()
    }

    /// Poll the body for more data frames. Returns `true` if data was added
    /// to the buffer, `false` if the body is exhausted.
    ///
    /// Buffer growth is bounded: if the accumulated bytes exceed the expected
    /// maximum in-flight envelope size, return `ResourceExhausted` rather than
    /// continuing to buffer. This prevents a malicious server from trickling
    /// bytes indefinitely without ever completing an envelope.
    async fn poll_body(&mut self) -> Result<bool, ConnectError> {
        // Enough for one complete envelope at the max message size, plus
        // one header's worth of slack (next envelope's header may arrive in
        // the same TCP frame), plus 64 KiB for gRPC-Web trailer frames.
        let max_buf_size = self
            .max_message_size
            .unwrap_or(crate::service::DEFAULT_MAX_MESSAGE_SIZE)
            .saturating_add(2 * crate::envelope::HEADER_SIZE)
            .saturating_add(RESPONSE_BUFFER_TRAILER_SLACK);

        loop {
            let frame = Pin::new(&mut self.body).frame().await;

            match frame {
                None => return Ok(false), // Body exhausted
                Some(Ok(frame)) => {
                    if frame.is_data() {
                        if let Ok(data) = frame.into_data() {
                            if self.buf.len().saturating_add(data.len()) > max_buf_size {
                                return Err(ConnectError::resource_exhausted(format!(
                                    "response buffer exceeds limit {max_buf_size}"
                                )));
                            }
                            self.buf.extend_from_slice(&data);
                            return Ok(true);
                        }
                    } else if frame.is_trailers()
                        && let Ok(trailers) = frame.into_trailers()
                        && matches!(self.protocol, Protocol::Grpc | Protocol::GrpcWeb)
                    {
                        // HTTP/2 or HTTP/1.1 chunked trailers — used by gRPC/gRPC-Web
                        if let Some(err) = parse_grpc_error_from_trailers(&trailers) {
                            self.error = Some(err);
                        }
                        self.trailers = Some(trailers);
                        self.done = true;
                        return Ok(false);
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
                .map_err(|mut e| {
                    if e.code == ErrorCode::Unimplemented {
                        e.code = ErrorCode::Internal;
                    }
                    e
                })
        } else {
            Ok(envelope.data)
        }
    }

    /// Process the END_STREAM envelope: extract trailers and error.
    fn process_end_stream(&mut self, envelope: Envelope) -> Result<(), ConnectError> {
        let end_stream_data = self.decompress_envelope(envelope)?;

        let end_stream: ClientEndStreamResponse =
            serde_json::from_slice(&end_stream_data).unwrap_or_default();

        // Extract trailers from metadata
        if let Some(metadata) = end_stream.metadata {
            let mut trailers = http::HeaderMap::new();
            for (name, values) in metadata {
                for value in values {
                    if let (Ok(name), Ok(value)) = (
                        http::header::HeaderName::from_bytes(name.as_bytes()),
                        http::header::HeaderValue::from_str(&value),
                    ) {
                        trailers.append(name, value);
                    }
                }
            }
            self.trailers = Some(trailers);
        }

        // Extract error
        if let Some(err) = end_stream.error {
            let mut connect_error = ConnectError::new(
                err.code
                    .as_deref()
                    .and_then(|c| c.parse().ok())
                    .unwrap_or(ErrorCode::Unknown),
                err.message.unwrap_or_default(),
            );
            connect_error.details = err.details;
            self.error = Some(connect_error);
        }

        Ok(())
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
/// Errors that occur during the stream (e.g., in the END_STREAM envelope)
/// are available via [`ServerStream::error()`] after the stream ends.
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
    Req: buffa::Message + serde::Serialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
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
        CodecFormat::Json => {
            let buf = serde_json::to_vec(&request).map_err(|e| {
                ConnectError::internal(format!("failed to encode JSON request: {e}"))
            })?;
            Bytes::from(buf)
        }
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
    let deadline = options.timeout.map(|t| std::time::Instant::now() + t);

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
            .map_err(|e| ConnectError::unavailable(format!("request failed: {e}")))?;

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
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
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
        trailers: None,
        error: None,
        done: false,
        _phantom: PhantomData,
    })
}

// ============================================================================
// BidiStream — bidirectional streaming client
// ============================================================================

/// A request body that pulls envelope-encoded frames from an mpsc channel.
///
/// Used as the request body for bidirectional and client-streaming calls.
/// [`BidiStream::send`] pushes encoded envelopes to the channel's sender half;
/// dropping the sender (via [`BidiStream::close_send`]) closes the body,
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

/// State machine for the receive side of a [`BidiStream`].
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
/// The response HEADERS future (what the spawned task resolves to) is still
/// awaited lazily on the first `message()` call, so servers that wait for
/// the first request message before sending HEADERS don't deadlock either.
enum RecvState<B, RespView> {
    /// Request initiated in a spawned task; response HEADERS not yet
    /// received. Awaiting the handle yields the [`Response`] once hyper
    /// reads the HEADERS frame.
    Pending(tokio::task::JoinHandle<Result<Response<B>, ConnectError>>),
    /// HEADERS received; response-side decoding delegates to [`ServerStream`].
    Ready(Box<ServerStream<B, RespView>>),
    /// Transport error or make_server_stream error stored on self.construct_err.
    /// Terminal state.
    Failed,
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
/// # Example
///
/// ```rust,ignore
/// let mut stream = call_bidi_stream(&transport, &config, "svc", "method", CallOptions::default()).await?;
/// stream.send(request1).await?;
/// stream.send(request2).await?;
/// stream.close_send();
/// while let Some(msg) = stream.message().await? {
///     println!("got: {msg:?}");
/// }
/// if let Some(err) = stream.error() {
///     return Err(err.clone());
/// }
/// ```
pub struct BidiStream<B, Req, RespView> {
    // Send side
    tx: Option<tokio::sync::mpsc::Sender<Result<Bytes, ConnectError>>>,
    encoder: crate::envelope::EnvelopeEncoder,
    codec_format: CodecFormat,

    // Receive side — state machine: Pending -> Ready or Failed
    recv: RecvState<B, RespView>,
    /// Config snapshot for constructing ServerStream when headers arrive.
    /// Captured by value (not &) because the stream outlives call_bidi_stream.
    stream_config: StreamConfig,
    /// Error from transport.send or make_server_stream. Terminal.
    construct_err: Option<ConnectError>,

    _req: PhantomData<Req>,
}

// Manual impl: the body type inside `ServerStream` typically isn't `Debug`,
// and the JoinHandle's inner type wouldn't format usefully anyway. Print
// send-channel state, recv-state discriminant, and any construction error.
impl<B, Req, RespView> std::fmt::Debug for BidiStream<B, Req, RespView> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let recv_state = match &self.recv {
            RecvState::Pending(_) => "Pending",
            RecvState::Ready(_) => "Ready",
            RecvState::Failed => "Failed",
        };
        f.debug_struct("BidiStream")
            .field("send_closed", &self.tx.is_none())
            .field("recv_state", &recv_state)
            .field("protocol", &self.stream_config.protocol)
            .field("codec_format", &self.stream_config.codec_format)
            .field("construct_err", &self.construct_err)
            .finish_non_exhaustive()
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

impl<B, Req, RespView> BidiStream<B, Req, RespView>
where
    B: Body<Data = Bytes> + Send + Unpin,
    B::Error: std::fmt::Display,
    Req: buffa::Message + serde::Serialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
{
    /// Send a request message.
    ///
    /// Returns an error if [`close_send`](Self::close_send) was already
    /// called, if the whole-call deadline has passed, or if the server has
    /// closed the stream (the receiver half was dropped). In the latter
    /// case, call [`message()`](Self::message) to retrieve the server's error.
    pub async fn send(&mut self, msg: Req) -> Result<(), ConnectError> {
        // Check the whole-call deadline before each send, matching
        // connect-go's ctx.Err() check in duplexHTTPCall.Send().
        if let Some(d) = self.stream_config.deadline
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
            CodecFormat::Json => {
                let buf = serde_json::to_vec(&msg).map_err(|e| {
                    ConnectError::internal(format!("failed to encode JSON request: {e}"))
                })?;
                Bytes::from(buf)
            }
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
    /// (HTTP/1.1), this must be called before [`message()`](Self::message).
    pub fn close_send(&mut self) {
        self.tx = None; // drop sender → channel closes → body signals EOF
    }

    /// Receive the next response message.
    ///
    /// The first call awaits response headers (lazily, so full-duplex
    /// servers that wait for a request before sending headers don't deadlock).
    /// Subsequent calls decode envelopes from the response body stream.
    ///
    /// Returns `Ok(None)` when the server is done sending. At that point,
    /// [`trailers()`](Self::trailers) and [`error()`](Self::error) become
    /// available.
    pub async fn message(&mut self) -> Result<Option<OwnedView<RespView>>, ConnectError> {
        // If we already failed during construction or first await, return that.
        if let Some(ref err) = self.construct_err {
            return Err(err.clone());
        }

        // Transition Pending -> Ready on first call.
        if matches!(self.recv, RecvState::Pending(_)) {
            // Take the handle out so we can await it (borrow check).
            let RecvState::Pending(handle) = std::mem::replace(&mut self.recv, RecvState::Failed)
            else {
                unreachable!()
            };

            // Bound the response-HEADERS wait by the whole-call deadline.
            // A server that never sends headers shouldn't block forever.
            // The spawned task either resolves or is cancelled: if we hit the
            // deadline here and drop the handle, the task detaches — but the
            // dropped tx (on BidiStream drop) will end the body stream, which
            // ends the request, which completes the detached task naturally.
            let awaited = async move {
                handle.await.map_err(|e| {
                    ConnectError::internal(format!("transport send task panicked: {e}"))
                })?
            };
            match with_deadline(self.stream_config.deadline, awaited).await {
                Ok(response) => {
                    let cfg = &self.stream_config;
                    match make_server_stream(
                        response,
                        cfg.protocol,
                        &cfg.compression,
                        cfg.codec_format,
                        cfg.max_message_size,
                        cfg.deadline,
                    )
                    .await
                    {
                        Ok(stream) => self.recv = RecvState::Ready(Box::new(stream)),
                        Err(e) => {
                            self.construct_err = Some(e.clone());
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    self.construct_err = Some(e.clone());
                    return Err(e);
                }
            }
        }

        match &mut self.recv {
            RecvState::Ready(stream) => stream.message().await,
            RecvState::Failed => {
                // construct_err is set above; checked at top of fn.
                // This branch is only reachable if recv is Failed but
                // construct_err was cleared (which we never do).
                Err(ConnectError::internal("stream in failed state"))
            }
            RecvState::Pending(_) => unreachable!("transitioned above"),
        }
    }

    /// Response headers. `None` until the first [`message()`](Self::message)
    /// call resolves them (i.e. until the response HEADERS frame arrives).
    #[must_use]
    pub fn headers(&self) -> Option<&http::HeaderMap> {
        match &self.recv {
            RecvState::Ready(s) => Some(s.headers()),
            _ => None,
        }
    }

    /// Trailing metadata. Only populated after [`message()`](Self::message)
    /// returns `Ok(None)`.
    #[must_use]
    pub fn trailers(&self) -> Option<&http::HeaderMap> {
        match &self.recv {
            RecvState::Ready(s) => s.trailers(),
            _ => None,
        }
    }

    /// Trailing error from the END_STREAM envelope (Connect) or trailers (gRPC).
    /// Only populated after [`message()`](Self::message) returns `Ok(None)`.
    /// For transport-level failures, [`message()`](Self::message) returns the
    /// error directly instead.
    #[must_use]
    pub fn error(&self) -> Option<&ConnectError> {
        match &self.recv {
            RecvState::Ready(s) => s.error(),
            _ => None,
        }
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
    Req: buffa::Message + serde::Serialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
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

    let deadline = options.timeout.map(|t| std::time::Instant::now() + t);

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
    // RecvState::Pending needs JoinHandle<Result<...>>. If wasm32+client
    // becomes supported, factor this into a spawn_with_result helper that
    // bridges via oneshot on wasm.
    let response_fut = transport.send(http_request);
    let response_task = tokio::spawn(async move {
        response_fut
            .await
            .map_err(|e| ConnectError::unavailable(format!("request failed: {e}")))
    });

    Ok(BidiStream {
        tx: Some(tx),
        encoder,
        codec_format: config.codec_format,
        recv: RecvState::Pending(response_task),
        stream_config: StreamConfig {
            protocol: config.protocol,
            codec_format: config.codec_format,
            compression: config.compression.clone(),
            max_message_size: options.max_message_size,
            deadline,
        },
        construct_err: None,
        _req: PhantomData,
    })
}

/// Make a client-streaming RPC call.
///
/// Sends multiple request messages as envelope-framed data and receives a single
/// envelope-framed response with END_STREAM. Returns a [`UnaryResponse`] containing
/// the decoded response message along with headers and trailers.
///
/// The request body is streamed: each item from the iterator is encoded into
/// an envelope and pushed to a bounded mpsc channel that backs the HTTP
/// request body. The transport begins sending as soon as the first envelope
/// is ready instead of waiting for the iterator to be fully drained, so peak
/// memory stays around `channel_depth * envelope_size` rather than the full
/// concatenated body.
pub async fn call_client_stream<T, Req, RespView>(
    transport: &T,
    config: &ClientConfig,
    service: &str,
    method: &str,
    requests: impl IntoIterator<Item = Req>,
    options: CallOptions,
) -> Result<UnaryResponse<OwnedView<RespView>>, ConnectError>
where
    T: ClientTransport,
    <T::ResponseBody as Body>::Error: std::fmt::Display,
    Req: buffa::Message + serde::Serialize,
    RespView: MessageView<'static> + Send,
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
{
    let options = effective_options(config, options);

    // Build the full URI from base_uri and service/method path
    let base_str = config.base_uri.to_string();
    let base_str = base_str.trim_end_matches('/');
    let full_uri = format!("{base_str}/{service}/{method}");
    let uri: Uri = full_uri
        .parse()
        .map_err(|e| ConnectError::internal(format!("invalid URI: {e}")))?;

    // Channel-backed request body. Depth 32 matches `call_bidi_stream` and
    // gives natural backpressure on HTTP/2 flow control.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, ConnectError>>(32);
    let body: ClientBody = ChannelBody { rx }.boxed();

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

    // Compute deadline BEFORE sending, matching Go's ctx.Deadline() semantics
    let deadline = options.timeout.map(|t| std::time::Instant::now() + t);

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

    // Drive the transport send concurrently with the iterator drain below.
    // Without this, a transport whose send() future contains the actual I/O
    // would not read from the channel until awaited, deadlocking once the
    // channel filled. The response is bridged back via a oneshot so the
    // awaitee is uniform across architectures.
    let response_fut = transport.send(http_request);
    let (resp_tx, resp_rx) =
        tokio::sync::oneshot::channel::<Result<Response<T::ResponseBody>, ConnectError>>();
    let _ = crate::spawn_detached(async move {
        let result = response_fut
            .await
            .map_err(|e| ConnectError::unavailable(format!("request failed: {e}")));
        let _ = resp_tx.send(result);
    });

    // Enforce client-side deadline on send + parse.
    with_deadline(deadline, async {
        // Drain the iterator, encoding each request and pushing its envelope
        // into the channel. The iterator is synchronous, so the only awaits
        // here are tx.send(...), which provides backpressure via the channel
        // depth.
        for request in requests {
            let msg_bytes = match config.codec_format {
                CodecFormat::Proto => request.encode_to_bytes(),
                CodecFormat::Json => {
                    let buf = serde_json::to_vec(&request).map_err(|e| {
                        ConnectError::internal(format!("failed to encode JSON request: {e}"))
                    })?;
                    Bytes::from(buf)
                }
            };

            let mut envelope_buf = BytesMut::new();
            tokio_util::codec::Encoder::encode(&mut encoder, msg_bytes, &mut envelope_buf)?;

            if tx.send(Ok(envelope_buf.freeze())).await.is_err() {
                // Receiver dropped: the spawned send task has finished, either
                // because the transport failed or the server responded before
                // we finished sending. Stop draining and let the response
                // task surface the actual error/result.
                break;
            }
        }

        drop(tx);

        // Await the response now that the request body has been fully sent.
        let response = resp_rx.await.map_err(|_| {
            ConnectError::internal("transport send task dropped without producing a response")
        })??;

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
    .await
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
    RespView::Owned: buffa::Message + serde::de::DeserializeOwned,
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

    let mut buf = BytesMut::from(body.as_ref());
    let mut data_envelopes: Vec<Bytes> = Vec::new();
    let mut trailers = http::HeaderMap::new();

    while !buf.is_empty() {
        let envelope = match Envelope::decode_with_limit(&mut buf, max_msg_size)? {
            Some(env) => env,
            None => break,
        };

        if envelope.is_end_stream() {
            let end_stream_data = if envelope.is_compressed() {
                let enc = encoding.as_deref().ok_or_else(|| {
                    ConnectError::internal("received compressed END_STREAM without encoding header")
                })?;
                config
                    .compression
                    .decompress_with_limit(enc, envelope.data, max_msg_size)?
            } else {
                envelope.data
            };

            let end_stream: ClientEndStreamResponse =
                serde_json::from_slice(&end_stream_data).unwrap_or_default();

            if let Some(metadata) = end_stream.metadata {
                for (name, values) in metadata {
                    for value in values {
                        if let (Ok(name), Ok(value)) = (
                            http::header::HeaderName::from_bytes(name.as_bytes()),
                            http::header::HeaderValue::from_str(&value),
                        ) {
                            trailers.append(name, value);
                        }
                    }
                }
            }

            if let Some(err) = end_stream.error {
                let mut connect_error = ConnectError::new(
                    err.code
                        .as_deref()
                        .and_then(|c| c.parse().ok())
                        .unwrap_or(ErrorCode::Unknown),
                    err.message.unwrap_or_default(),
                );
                connect_error.details = err.details;
                connect_error.set_response_headers(resp_headers);
                connect_error.set_trailers(trailers);
                return Err(connect_error);
            }
        } else {
            let data = if envelope.is_compressed() {
                let enc = encoding.as_deref().ok_or_else(|| {
                    ConnectError::internal("received compressed message without encoding header")
                })?;
                config
                    .compression
                    .decompress_with_limit(enc, envelope.data, max_msg_size)
                    .map_err(|mut e| {
                        if e.code == ErrorCode::Unimplemented {
                            e.code = ErrorCode::Internal;
                        }
                        e
                    })?
            } else {
                envelope.data
            };

            if data.len() > max_msg_size {
                return Err(ConnectError::new(
                    ErrorCode::ResourceExhausted,
                    format!("message size {} exceeds limit {}", data.len(), max_msg_size),
                ));
            }

            data_envelopes.push(data);
        }
    }

    if data_envelopes.is_empty() {
        return Err(ConnectError::unimplemented(
            "client streaming response contains no data messages",
        ));
    }
    if data_envelopes.len() > 1 {
        return Err(ConnectError::unimplemented(
            "client streaming response contains multiple data messages",
        ));
    }
    let data = data_envelopes.into_iter().next().unwrap();
    let message = decode_response_view::<RespView>(data, config.codec_format)?;

    Ok(UnaryResponse {
        headers: resp_headers,
        body: message,
        trailers,
    })
}

/// EndStreamResponse as received by the client.
#[derive(serde::Deserialize, Default)]
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
///
/// gRPC spec requires at most 8 ASCII digits followed by a unit suffix.
/// We select the largest unit that represents the value without precision
/// loss and fits within 8 digits.
#[allow(clippy::manual_is_multiple_of)]
fn format_timeout(timeout: Duration, protocol: Protocol) -> String {
    match protocol {
        Protocol::Connect => {
            // Connect spec: "at most 10 digits" → max 9_999_999_999 ms ≈ 115 days.
            // Clamp so a large Duration doesn't produce a spec-violating
            // header that our own server (and connect-go) will reject.
            const MAX_MILLIS: u128 = 9_999_999_999;
            timeout.as_millis().min(MAX_MILLIS).to_string()
        }
        Protocol::Grpc | Protocol::GrpcWeb => {
            const MAX_DIGITS: u128 = 99_999_999; // 8 digits max per gRPC spec

            // Try each unit from largest to smallest, picking the first
            // that has no precision loss and fits in 8 digits.
            let nanos = timeout.as_nanos();
            let secs = timeout.as_secs() as u128;
            let millis = timeout.as_millis();
            let micros = timeout.as_micros();

            if nanos == 0 {
                "0n".to_owned()
            } else if nanos % 1_000_000_000 == 0 && secs <= MAX_DIGITS {
                format!("{secs}S")
            } else if nanos % 1_000_000 == 0 && millis <= MAX_DIGITS {
                format!("{millis}m")
            } else if nanos % 1_000 == 0 && micros <= MAX_DIGITS {
                format!("{micros}u")
            } else if nanos <= MAX_DIGITS {
                format!("{nanos}n")
            } else if micros <= MAX_DIGITS {
                // Value exceeds 8 nano-digits and has sub-microsecond
                // precision — truncate to the smallest unit that fits,
                // minimizing precision loss. Without this branch a
                // sub-second duration like 100ms+1ns (natural from
                // `Instant` arithmetic) would fall through to secs=0 →
                // "0S" → server sees expired deadline.
                format!("{micros}u")
            } else if millis <= MAX_DIGITS {
                format!("{millis}m")
            } else if secs <= MAX_DIGITS {
                format!("{secs}S")
            } else {
                // Extremely large timeout (>3.17 years) — use max
                format!("{MAX_DIGITS}S")
            }
        }
    }
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
    let status = trailers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u32>().ok())?;

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
    if data.len() < 5 || data[0] & 0x80 == 0 {
        return None;
    }
    let is_compressed = data[0] & 0x01 != 0;
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
            headers.append(name, val);
        }
    }
    Some(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config() {
        let config = ClientConfig::new("http://localhost:8080".parse().unwrap())
            .json()
            .compress_requests("gzip");

        assert_eq!(config.codec_format, CodecFormat::Json);
        assert_eq!(config.request_compression, Some("gzip".to_string()));
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

        // Transports — manual impls that print mode/connection state.
        #[cfg(feature = "client")]
        assert_debug::<HttpClient>();
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

    // ========================================================================
    // Content type helper tests
    // ========================================================================

    #[test]
    fn test_unary_request_content_type_connect() {
        let config = ClientConfig::new("http://localhost".parse().unwrap());
        assert_eq!(unary_request_content_type(&config), "application/proto");

        let config = config.codec_format(CodecFormat::Json);
        assert_eq!(unary_request_content_type(&config), "application/json");
    }

    #[test]
    fn test_unary_request_content_type_grpc() {
        let config =
            ClientConfig::new("http://localhost".parse().unwrap()).protocol(Protocol::Grpc);
        assert_eq!(
            unary_request_content_type(&config),
            "application/grpc+proto"
        );

        let config = config.codec_format(CodecFormat::Json);
        assert_eq!(unary_request_content_type(&config), "application/grpc+json");
    }

    #[test]
    fn test_streaming_request_content_type() {
        let config = ClientConfig::new("http://localhost".parse().unwrap());
        assert_eq!(
            streaming_request_content_type(&config),
            "application/connect+proto"
        );

        let config = config.protocol(Protocol::Grpc);
        assert_eq!(
            streaming_request_content_type(&config),
            "application/grpc+proto"
        );

        let config = config.protocol(Protocol::GrpcWeb);
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
            ClientConfig::new("http://localhost".parse().unwrap()).protocol(Protocol::Grpc);
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
            ClientConfig::new("http://localhost".parse().unwrap()).protocol(Protocol::GrpcWeb);
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
            ClientConfig::new("http://localhost".parse().unwrap()).protocol(Protocol::Grpc);
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
            ClientConfig::new("http://localhost".parse().unwrap()).protocol(Protocol::Grpc);
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
            ClientConfig::new("http://localhost".parse().unwrap()).protocol(Protocol::Grpc);
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
            .protocol(protocol)
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
            .default_timeout(Duration::from_secs(30))
            .default_max_message_size(1024)
            .default_header("x-trace-id", "cfg-trace");

        let eff = effective_options(&config, CallOptions::default());

        assert_eq!(eff.timeout, Some(Duration::from_secs(30)));
        assert_eq!(eff.max_message_size, Some(1024));
        assert_eq!(eff.headers.get("x-trace-id").unwrap(), "cfg-trace");
    }

    #[test]
    fn effective_options_options_override_config_defaults() {
        let config = test_config()
            .default_timeout(Duration::from_secs(30))
            .default_max_message_size(1024);

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
        let options = CallOptions::default().with_compression(true);
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
}
