//! Tower service integration for ConnectRPC.
//!
//! This module provides a [`tower::Service`] implementation that allows
//! ConnectRPC handlers to be integrated into existing web servers.
//!
//! # Example with hyper
//!
//! ```rust,ignore
//! use connectrpc::{Router, ConnectRpcService};
//! use std::sync::Arc;
//! // `register` is provided by the generated `<Service>Ext` extension trait:
//! use my_proto::greet::v1::GreetServiceExt;
//!
//! let router = Arc::new(MyGreetService).register(Router::new());
//! let service = ConnectRpcService::new(router);
//! // Use with hyper or any tower-compatible framework
//! ```
//!
//! # Example with axum (requires `axum` feature)
//!
//! ```rust,ignore
//! use axum::{Router, routing::get};
//! use connectrpc::Router as ConnectRouter;
//! use std::sync::Arc;
//!
//! let connect_router = Arc::new(MyGreetService).register(ConnectRouter::new());
//!
//! let app = Router::new()
//!     .route("/health", get(health_handler))
//!     .merge(connect_router.into_axum_router());
//! ```

use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context as TaskContext;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header;
use http_body::Body;
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::Full;
use serde::Serialize;
use tracing::Instrument;

use crate::codec::CodecFormat;
use crate::codec::content_type;
use crate::codec::header as connect_header;
use crate::compression::CompressionPolicy;
use crate::compression::CompressionRegistry;
use crate::deadline::DeadlinePolicy;
use crate::dispatcher::{Dispatcher, MethodDescriptor};
use crate::envelope::Decoded;
use crate::envelope::Envelope;
use crate::envelope::EnvelopeDecoder;
use crate::error::ConnectError;
use crate::handler::BoxStream;
use crate::interceptor::{
    Interceptor, InterceptorChain, RequestHead, call_bidi_streaming_intercepted,
    call_client_streaming_intercepted, call_server_streaming_intercepted, call_unary_intercepted,
};
use crate::protocol::Protocol;
use crate::response::{EncodedResponse, RequestContext};
use crate::router::MethodKind;
use crate::router::Router;

// ============================================================================
// GET Request Query Parameter Handling
// ============================================================================

/// Parsed query parameters from a GET request.
///
/// According to the Connect protocol, GET requests encode the message and metadata
/// in query parameters instead of the request body.
#[derive(Debug, Default)]
struct GetQueryParams {
    /// The encoded message (percent-encoded or base64).
    message: Option<String>,
    /// The message encoding format ("proto" or "json").
    encoding: Option<String>,
    /// Whether the message is base64-encoded.
    base64: bool,
    /// The compression algorithm used on the message.
    compression: Option<String>,
    /// The Connect protocol version ("v1").
    connect_version: Option<String>,
}

/// Parse query parameters from a GET request URL.
///
/// Extracts the Connect-specific parameters: `message`, `encoding`, `base64`,
/// `compression`, and `connect`.
fn parse_get_query_params(query: Option<&str>) -> Result<GetQueryParams, ConnectError> {
    let Some(query) = query else {
        return Err(ConnectError::invalid_argument(
            "GET request requires query parameters",
        ));
    };

    let mut params = GetQueryParams::default();

    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        let key = parts.next().unwrap_or("");
        let value = parts.next().unwrap_or("");

        match key {
            "message" => params.message = Some(value.to_owned()),
            "encoding" => params.encoding = Some(value.to_owned()),
            "base64" => params.base64 = value == "1",
            "compression" => params.compression = Some(value.to_owned()),
            "connect" => params.connect_version = Some(value.to_owned()),
            _ => {} // Ignore unknown parameters per spec
        }
    }

    // Encoding is required
    if params.encoding.is_none() {
        return Err(ConnectError::invalid_argument(
            "GET request requires 'encoding' query parameter",
        ));
    }

    Ok(params)
}

// ============================================================================
// Request Metadata Extraction
// ============================================================================

/// Metadata extracted from request headers.
///
/// This struct captures the common headers needed by both unary and streaming
/// handlers, avoiding duplication of header extraction logic.
#[derive(Debug)]
struct RequestMetadata {
    /// The Content-Type header value.
    content_type: Option<String>,
    /// The timeout parsed from the protocol's timeout header.
    timeout: Option<Duration>,
    /// Compression encoding for unary requests (from Content-Encoding header).
    unary_encoding: Option<String>,
    /// Compression encoding for streaming requests (protocol-specific header).
    streaming_encoding: Option<String>,
    /// Client's accepted encodings for unary responses (from Accept-Encoding header).
    unary_accept_encoding: Option<String>,
    /// Client's accepted encodings for streaming responses (protocol-specific header).
    streaming_accept_encoding: Option<String>,
    /// The Connect protocol version from connect-protocol-version header.
    /// Only meaningful for the Connect protocol.
    protocol_version: Option<String>,
    /// The original request headers, handed on to the handler's
    /// `RequestContext`.
    headers: http::HeaderMap,
}

impl RequestMetadata {
    /// Extract metadata from the request's headers, using the detected
    /// protocol to determine which header names to read, and keep the map for
    /// the handler.
    fn from_headers(headers: http::HeaderMap, protocol: Protocol) -> Self {
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        let timeout = timeout_from_headers(&headers, protocol);

        let unary_encoding = headers
            .get(header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        let streaming_encoding = headers
            .get(protocol.content_encoding_header())
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        let unary_accept_encoding = headers
            .get(header::ACCEPT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        let streaming_accept_encoding = headers
            .get(protocol.accept_encoding_header())
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        let protocol_version = headers
            .get(connect_header::PROTOCOL_VERSION)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        Self {
            content_type,
            timeout,
            unary_encoding,
            streaming_encoding,
            unary_accept_encoding,
            streaming_accept_encoding,
            protocol_version,
            headers,
        }
    }
}

/// Parse a timeout value according to the protocol's format.
///
/// - Connect: value is milliseconds (e.g., "5000")
/// - gRPC/gRPC-Web: value is digits + unit suffix (e.g., "5000m", "5S", "1H")
///   Units: H (hours), M (minutes), S (seconds), m (milliseconds),
///   u (microseconds), n (nanoseconds)
fn parse_timeout(s: &str, protocol: Protocol) -> Option<Duration> {
    match protocol {
        Protocol::Connect => {
            // Connect spec: "positive integer as ASCII string of at most 10
            // digits" (max 9_999_999_999 ms ≈ 115 days). Enforcing this bound
            // also guarantees `Instant::now() + d` cannot overflow.
            if s.is_empty() || s.len() > 10 {
                return None;
            }
            let ms = s.parse::<u64>().ok()?;
            Some(Duration::from_millis(ms))
        }
        Protocol::Grpc | Protocol::GrpcWeb => {
            // gRPC spec: value is at most 8 ASCII digits + single ASCII unit.
            // Reject non-ASCII early to avoid a char-boundary panic in split_at
            // (HeaderValue::to_str() already filters this, but this function
            // must be safe for any &str it is given).
            if s.is_empty() || !s.is_ascii() {
                return None;
            }
            let (digits, unit) = s.split_at(s.len() - 1);
            // Max 8 digits per spec → max 99_999_999 of any unit. Even at
            // hours (~11415 years) this is well within `Instant + Duration`
            // bounds. Rejecting over-length input prevents the overflow panic
            // that `Duration::from_secs(u64::MAX)` would trigger downstream.
            if digits.is_empty() || digits.len() > 8 {
                return None;
            }
            let value = digits.parse::<u64>().ok()?;
            match unit {
                "H" => value.checked_mul(3600).map(Duration::from_secs),
                "M" => value.checked_mul(60).map(Duration::from_secs),
                "S" => Some(Duration::from_secs(value)),
                "m" => Some(Duration::from_millis(value)),
                "u" => Some(Duration::from_micros(value)),
                "n" => Some(Duration::from_nanos(value)),
                _ => None,
            }
        }
    }
}

/// Collect a request body with an enforced on-wire size limit.
///
/// Wraps the body in `http_body_util::Limited` so that allocation is bounded
/// *before* collection completes — a malicious client cannot force unbounded
/// buffering by sending an oversized body. Returns `ResourceExhausted` if the
/// limit is exceeded, `Internal` for underlying body read errors.
///
/// **Trade-off:** On the limit-exceeded path, the body is NOT fully drained
/// before returning. For HTTP/1.1 this may disable keep-alive on that
/// connection (hyper's `poll_drain_or_close_read` race). This is deliberate:
/// draining an oversized body to preserve keep-alive would require reading
/// potentially gigabytes from a malicious client, defeating the limit. The
/// connection will close cleanly after the error response is sent.
async fn collect_body_limited<B>(body: B, limit: usize) -> Result<Bytes, ConnectError>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    match http_body_util::Limited::new(body, limit).collect().await {
        Ok(collected) => Ok(collected.to_bytes()),
        Err(err) => {
            if err
                .downcast_ref::<http_body_util::LengthLimitError>()
                .is_some()
            {
                Err(ConnectError::resource_exhausted(format!(
                    "request body size exceeds limit {limit}"
                )))
            } else {
                Err(ConnectError::internal(format!(
                    "failed to read request body: {err}"
                )))
            }
        }
    }
}

/// Run a request future within an absolute server-side deadline.
///
/// Callers compute the deadline once after parsing headers so the same budget
/// covers body receipt and handler execution.
async fn with_request_deadline<F, T>(
    deadline: Option<std::time::Instant>,
    future: F,
) -> Result<T, ConnectError>
where
    F: Future<Output = Result<T, ConnectError>>,
{
    match deadline {
        Some(deadline) => tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
            .await
            .map_err(|_| ConnectError::deadline_exceeded("request timeout"))?,
        None => future.await,
    }
}

fn absolute_deadline(timeout: Option<Duration>) -> Option<std::time::Instant> {
    timeout.and_then(|t| std::time::Instant::now().checked_add(t))
}

/// The client-requested timeout from the protocol's timeout header, if
/// present and well-formed.
fn timeout_from_headers(headers: &http::HeaderMap, protocol: Protocol) -> Option<Duration> {
    headers
        .get(protocol.timeout_header())
        .and_then(|v| v.to_str().ok())
        .and_then(|s| parse_timeout(s, protocol))
}

/// The request deadline for paths that never build a [`RequestMetadata`]
/// (early rejections that still drain the body under the client's timeout):
/// the same timeout `RequestMetadata` would parse, moderated by the policy.
fn deadline_from_headers(
    headers: &http::HeaderMap,
    protocol: Protocol,
    path: &str,
    deadline_policy: &DeadlinePolicy,
) -> Option<std::time::Instant> {
    let timeout = timeout_from_headers(headers, protocol);
    absolute_deadline(deadline_policy.moderate(timeout, path))
}

/// Decode the message from GET request query parameters.
///
/// The message may be:
/// - Percent-encoded UTF-8 (for JSON without compression)
/// - Base64-encoded (for binary proto or compressed data)
fn decode_get_message(
    params: &GetQueryParams,
    compression: &CompressionRegistry,
    max_message_size: usize,
) -> Result<Bytes, ConnectError> {
    let Some(ref encoded_message) = params.message else {
        // Empty message is valid (e.g., for requests with no fields)
        return Ok(Bytes::new());
    };

    // Decode the message based on base64 flag
    let decoded = if params.base64 {
        // Base64-decode (URL-safe alphabet, optional padding)
        use base64::Engine;
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;

        // Handle both padded and unpadded base64
        let message = if encoded_message.contains('%') {
            // Percent-decode first (padding chars may be encoded)
            percent_decode(encoded_message)?
        } else {
            encoded_message.as_bytes().to_vec()
        };

        URL_SAFE_NO_PAD
            .decode(&message)
            .or_else(|_| {
                // Try with standard padding handling
                use base64::engine::general_purpose::URL_SAFE;
                URL_SAFE.decode(&message)
            })
            .map_err(|e| ConnectError::invalid_argument(format!("invalid base64 encoding: {e}")))?
    } else {
        // Percent-decode as UTF-8
        percent_decode(encoded_message)?
    };

    // Decompress if needed
    let body = if let Some(ref encoding) = params.compression {
        if encoding != "identity" {
            compression.decompress_with_limit(encoding, Bytes::from(decoded), max_message_size)?
        } else {
            Bytes::from(decoded)
        }
    } else {
        Bytes::from(decoded)
    };

    // Check message size limit (for uncompressed messages; compressed ones
    // are already bounded by decompress_with_limit)
    if body.len() > max_message_size {
        return Err(ConnectError::resource_exhausted(format!(
            "message size {} exceeds limit {}",
            body.len(),
            max_message_size
        )));
    }

    Ok(body)
}

/// Percent-decode a URL-encoded string.
///
/// Handles both standard percent-encoding (`%XX`) and the `+`-as-space
/// convention used in query strings.
fn percent_decode(input: &str) -> Result<Vec<u8>, ConnectError> {
    // Replace '+' with space first (query string convention), then
    // use the percent-encoding crate for the standard %XX decoding.
    let with_spaces = input.replace('+', " ");
    Ok(percent_encoding::percent_decode_str(&with_spaces).collect())
}

// ============================================================================
// Limits and Configuration
// ============================================================================

/// Default maximum request body size (4 MB).
///
/// This matches the default in tonic and grpc-go.
pub const DEFAULT_MAX_REQUEST_BODY_SIZE: usize = 4 * 1024 * 1024;

/// Default maximum message size (4 MB).
///
/// This limits the final (post-decompression) message size for both unary
/// and streaming RPCs. It also serves as the decompression limit, preventing
/// compression bomb attacks.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// Configuration limits for ConnectRPC requests.
///
/// These limits protect against denial-of-service attacks by bounding
/// memory usage for request processing.
///
/// # Relationship between limits
///
/// `max_request_body_size` bounds the total on-wire bytes read from the
/// network, while `max_message_size` bounds individual logical messages
/// after decompression.
///
/// For **unary RPCs**, the request body contains a single message (possibly
/// compressed). Keep `max_request_body_size >= max_message_size` to allow
/// uncompressed messages up to the message limit. Compressed messages may
/// have a smaller on-wire body that expands up to `max_message_size`.
///
/// For **server streaming RPCs**, the request body still contains a single
/// envelope-framed message (only the response is streamed), so the same
/// guidance as unary applies — the body is read in full before processing.
///
/// For **client and bidirectional streaming RPCs**, messages are processed
/// incrementally from the body stream — the full body is never buffered.
/// In that case `max_request_body_size` does not apply, and
/// `max_message_size` is the primary per-message protection.
///
/// `element_memory_limit` is the odd one out, and the distinction from
/// `max_message_size` is the one worth getting right: `max_message_size`
/// bounds *decompressed bytes on the wire*, while `element_memory_limit`
/// bounds the *in-memory footprint of the element count* those bytes ask
/// for. A repeated field of empty messages costs two bytes each encoded and
/// a whole struct each decoded, so a body comfortably inside
/// `max_message_size` can still expand by orders of magnitude. Raising one
/// does not raise the other.
///
/// # Where limits are set
///
/// Service-wide, on [`ConnectRpcService::with_limits`] (or the `Server`
/// builder's `with_limits`), applying to every route. A single route on a
/// [`Router`] can carry its own `Limits` via [`Router::with_route_limits`],
/// which replace the service-wide ones for that method in full — so a
/// method whose requests are always small can be sized to that, and an
/// upload-shaped method can exceed the default without raising it for the
/// rest. The bundled `connectrpc-health` and `connectrpc-reflection`
/// services set a 16 KiB profile on their own routes this way and expose
/// `apply_request_limits(router, limits)` for tuning it.
///
/// These are **server** limits, applied to received requests. The client
/// side has its own equivalents for received *responses*:
/// [`ClientConfig::with_default_element_memory_limit`](crate::client::ClientConfig::with_default_element_memory_limit)
/// and [`CallOptions::with_element_memory_limit`](crate::client::CallOptions::with_element_memory_limit).
///
/// # Construction
///
/// Start from [`Limits::default`] or [`Limits::unlimited`] and apply the
/// `with_*` builder methods, then read settings back through the accessor
/// methods of the same name without the prefix:
///
/// ```rust
/// use connectrpc::Limits;
///
/// let limits = Limits::default().with_max_message_size(8 * 1024 * 1024);
/// assert_eq!(limits.max_message_size(), 8 * 1024 * 1024);
/// ```
///
/// `Limits` is `#[non_exhaustive]`: new limits may be added in minor
/// releases (it is and will stay `Copy` — a small bundle of plain numeric
/// bounds, carried by value on [`MethodDescriptor`]).
/// Struct-literal and functional-update construction are not available
/// outside the crate; use the builder methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Limits {
    max_request_body_size: usize,
    max_message_size: usize,
    element_memory_limit: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_request_body_size: DEFAULT_MAX_REQUEST_BODY_SIZE,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            element_memory_limit: buffa::DEFAULT_ELEMENT_MEMORY_LIMIT,
        }
    }
}

impl Limits {
    /// Create limits with no restrictions (unlimited).
    ///
    /// **Warning:** This disables DoS protection. Only use in trusted environments.
    pub fn unlimited() -> Self {
        Self {
            max_request_body_size: usize::MAX,
            max_message_size: usize::MAX,
            element_memory_limit: usize::MAX,
        }
    }

    // ---- builders ---------------------------------------------------------

    /// Set the maximum size of the request body on the wire (before
    /// decompression).
    ///
    /// Applies to RPCs where the body is read in full (unary and server
    /// streaming). Should be at least the message size limit to allow
    /// uncompressed messages up to that limit. Does not apply to client/bidi
    /// streaming where messages are processed incrementally.
    ///
    /// Read via [`Self::max_request_body_size`]. Default: 4 MB (matches
    /// tonic/grpc-go).
    #[must_use]
    pub fn with_max_request_body_size(mut self, size: usize) -> Self {
        self.max_request_body_size = size;
        self
    }

    /// Set the maximum size of a single message after decompression.
    ///
    /// This applies uniformly to both unary and streaming RPCs, and to both
    /// compressed and uncompressed messages. Compressed payloads are bounded
    /// during decompression — the decompressor will never allocate more than
    /// this limit.
    ///
    /// Read via [`Self::max_message_size`]. Default: 4 MB.
    #[must_use]
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Set the maximum memory a single decode may commit to repeated, map,
    /// string and bytes *elements*.
    ///
    /// This is an amplification defence, and it is charged on element
    /// footprint rather than on contents: a few bytes on the wire can ask
    /// the decoder to materialize a very large number of small elements,
    /// each with its own allocation overhead, while staying well under the
    /// message size limit. A single large payload is unaffected however big
    /// it grows, because its contents are not charged.
    ///
    /// Raise it for a trusted peer that legitimately sends messages with
    /// very many small elements; lower it to tighten the defence.
    ///
    /// Read via [`Self::element_memory_limit`]. Default: 32 MiB (buffa's
    /// `DEFAULT_ELEMENT_MEMORY_LIMIT`).
    #[must_use]
    pub fn with_element_memory_limit(mut self, bytes: usize) -> Self {
        self.element_memory_limit = bytes;
        self
    }

    // ---- accessors --------------------------------------------------------

    /// The maximum size of the request body on the wire, before decompression.
    ///
    /// Set via [`Self::with_max_request_body_size`].
    #[must_use]
    pub fn max_request_body_size(&self) -> usize {
        self.max_request_body_size
    }

    /// The maximum size of a single message after decompression.
    ///
    /// Set via [`Self::with_max_message_size`].
    #[must_use]
    pub fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    /// The maximum memory a single decode may commit to repeated, map,
    /// string and bytes elements.
    ///
    /// Set via [`Self::with_element_memory_limit`].
    #[must_use]
    pub fn element_memory_limit(&self) -> usize {
        self.element_memory_limit
    }

    /// The buffa decode options these limits imply.
    #[doc(hidden)] // read by generated dispatch via `RequestContext`
    #[must_use]
    pub fn decode_options(&self) -> buffa::DecodeOptions {
        buffa::DecodeOptions::new().with_element_memory_limit(self.element_memory_limit)
    }
}

// ============================================================================
// Streaming Response Types
// ============================================================================

/// End of stream message for streaming RPCs.
///
/// This is the final message in a streaming response, encoded as JSON
/// and sent with the END_STREAM flag (0x02) in the envelope.
#[derive(Debug, Clone, Serialize)]
struct EndStreamResponse {
    /// The error, if any (omit for success).
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<EndStreamError>,
    /// Trailing metadata (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<std::collections::HashMap<String, Vec<String>>>,
}

/// Error in the EndStreamResponse.
#[derive(Debug, Clone, Serialize)]
struct EndStreamError {
    code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<Vec<serde_json::Value>>,
}

impl EndStreamResponse {
    /// Convert trailers to metadata format, returning None if empty.
    fn metadata_from_trailers(
        trailers: &http::HeaderMap,
    ) -> Option<std::collections::HashMap<String, Vec<String>>> {
        if trailers.is_empty() {
            None
        } else {
            Some(headers_to_metadata(trailers))
        }
    }

    /// Create a successful end-stream response with no error.
    fn success(trailers: &http::HeaderMap) -> Self {
        Self {
            error: None,
            metadata: Self::metadata_from_trailers(trailers),
        }
    }

    /// Create an end-stream response with an error.
    ///
    /// Matches gRPC's `build_grpc_trailers` precedence: if the error carries
    /// its own trailers (via `ConnectError::with_trailers()`), those populate
    /// the metadata and the handler-context trailers are skipped to avoid
    /// duplication. Otherwise the handler's context trailers are used.
    fn error(err: &ConnectError, context_trailers: &http::HeaderMap) -> Self {
        let trailers_source = if err.trailers().is_empty() {
            context_trailers
        } else {
            err.trailers()
        };
        let metadata = Self::metadata_from_trailers(trailers_source);
        Self {
            error: Some(EndStreamError {
                code: err.code.as_str().to_owned(),
                message: err.message.clone(),
                details: if err.details.is_empty() {
                    None
                } else {
                    // Serialize via ErrorDetail's derive so all fields (type,
                    // value, debug) are included and the wire format stays in
                    // lockstep with unary error serialization.
                    Some(
                        err.details
                            .iter()
                            .filter_map(|d| serde_json::to_value(d).ok())
                            .collect(),
                    )
                },
            }),
            metadata,
        }
    }

    /// Encode this response to JSON bytes.
    fn to_json(&self) -> Bytes {
        // EndStreamResponse is always JSON, regardless of the message codec
        serde_json::to_vec(self)
            .map(Bytes::from)
            .unwrap_or_else(|_| Bytes::from_static(b"{}"))
    }
}

/// Response headers that may never be carried from a propagated error
/// onto this response. Three classes, none of which describe the response
/// being built.
///
/// Hop-by-hop headers (RFC 9110 §7.6.1) belong to the connection the error
/// arrived on. Body-framing headers would misdescribe our own body: a
/// forwarded `content-length` makes hyper's HTTP/1 encoder refuse to
/// serialize the response at all, and a forwarded content coding tells the
/// client to decode bytes that were never encoded. `date` would report the
/// upstream's generation time as ours — hyper emits no `Date` of its own
/// once one is set, so the false value is the only one on the wire.
///
/// `grpc-status-details-bin` is here because the server writes its status
/// into the trailers, never the header block, so the already-set rule in
/// `echo_error_headers` cannot save it the way it saves `grpc-status` and
/// `grpc-message` on the trailers-only path.
fn is_unforwardable_header(name: &http::HeaderName) -> bool {
    static KEEP_ALIVE: http::HeaderName = http::HeaderName::from_static("keep-alive");

    // Hop-by-hop: scoped to a single connection.
    *name == header::CONNECTION
        || *name == KEEP_ALIVE
        || *name == header::PROXY_AUTHENTICATE
        || *name == header::PROXY_AUTHORIZATION
        || *name == header::TE
        || *name == header::TRAILER
        || *name == header::TRANSFER_ENCODING
        || *name == header::UPGRADE
        // Body framing: describes bytes other than the ones we write.
        || *name == header::CONTENT_LENGTH
        || *name == header::CONTENT_TYPE
        || *name == header::CONTENT_ENCODING
        || *name == crate::protocol::hdr::GRPC_ENCODING
        || *name == crate::protocol::hdr::CONNECT_CONTENT_ENCODING
        // Provenance and status.
        || *name == header::DATE
        || *name == crate::protocol::hdr::GRPC_STATUS_DETAILS_BIN
}

/// Echo an error's response headers onto a response being built.
///
/// A `ConnectError` that came back from a client call carries the headers
/// of the response it was parsed from, so a handler that propagates one —
/// the ordinary `client.call(..).await?` in a gateway — is asking to
/// re-emit another connection's headers. Two rules keep that from
/// corrupting this response. A header this response already set wins,
/// because `Builder::header` appends rather than replaces, and a second
/// `content-type` on the wire is a framing error rather than extra
/// metadata. Connection-scoped headers are dropped outright. Everything
/// else is the server metadata the caller meant to forward, and passes
/// through unchanged.
fn echo_error_headers(
    mut response: http::response::Builder,
    err: &ConnectError,
) -> http::response::Builder {
    // Snapshot before appending: a multi-valued error header must still
    // append all of its values, so the test cannot be against the map as
    // it grows.
    let already_set: Vec<http::HeaderName> = response
        .headers_ref()
        .map(|headers| headers.keys().cloned().collect())
        .unwrap_or_default();

    for (key, value) in err.response_headers() {
        if is_unforwardable_header(key) || already_set.contains(key) {
            continue;
        }
        response = response.header(key, value);
    }
    response
}

/// Create a streaming error response.
///
/// For streaming RPCs, errors should still return HTTP 200 with the error
/// encoded in an EndStreamResponse envelope. This function creates such a
/// response for errors that occur before the handler is invoked.
fn streaming_error_response(
    err: &ConnectError,
    protocol: Protocol,
    codec_format: CodecFormat,
) -> Response<StreamingResponseBody> {
    match protocol {
        Protocol::Connect => connect_streaming_error_response(err, codec_format),
        Protocol::Grpc | Protocol::GrpcWeb => grpc_error_response(err, protocol, codec_format),
    }
}

/// Build a Connect streaming error response with EndStreamResponse envelope.
fn connect_streaming_error_response(
    err: &ConnectError,
    codec_format: CodecFormat,
) -> Response<StreamingResponseBody> {
    use futures::stream::StreamExt as _;

    let end_stream = EndStreamResponse::error(err, err.trailers());
    let mut encoder = crate::envelope::EnvelopeEncoder::uncompressed();
    let mut buf = bytes::BytesMut::new();
    // encode_end_stream is infallible for uncompressed data
    let _ = encoder.encode_end_stream(end_stream.to_json(), &mut buf);
    let encoded = buf.freeze();

    // Create a simple stream that yields just the error envelope
    // Use .fuse() to make it safe to poll after returning None
    let body_stream = futures::stream::unfold(Some(encoded), async |data| {
        data.map(|bytes| (Ok(Frame::data(bytes)), None))
    })
    .fuse();

    let body = StreamingResponseBody {
        inner: Box::pin(body_stream),
    };

    let response = Response::builder().status(StatusCode::OK).header(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static(Protocol::Connect.response_content_type(codec_format, true)),
    );

    let response = echo_error_headers(response, err);

    response.body(body).unwrap_or_else(|_| {
        Response::new(StreamingResponseBody {
            inner: Box::pin(futures::stream::empty()),
        })
    })
}

/// Build a gRPC/gRPC-Web "trailers-only" error response.
///
/// For gRPC: HTTP 200 with grpc-status and grpc-message as HTTP/2 trailers
/// in a response with no data frames.
/// For gRPC-Web: same but trailers are encoded in the body.
fn grpc_error_response(
    err: &ConnectError,
    protocol: Protocol,
    codec_format: CodecFormat,
) -> Response<StreamingResponseBody> {
    let grpc_trailers = build_grpc_trailers(Some(err), err.trailers());

    let body_stream: Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, Infallible>> + Send>> =
        match protocol {
            Protocol::Grpc => {
                // For gRPC: emit HTTP/2 trailers frame (no data)
                Box::pin(
                    futures::stream::once(async move { Ok(Frame::trailers(grpc_trailers)) }).fuse(),
                )
            }
            Protocol::GrpcWeb => {
                // For gRPC-Web: encode trailers as a body frame with flag 0x80
                let trailer_bytes = encode_grpc_web_trailers(&grpc_trailers);
                Box::pin(
                    futures::stream::once(async move { Ok(Frame::data(trailer_bytes)) }).fuse(),
                )
            }
            Protocol::Connect => unreachable!("Connect handled separately"),
        };

    let body = StreamingResponseBody { inner: body_stream };

    let mut response = Response::builder().status(StatusCode::OK).header(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static(protocol.response_content_type(codec_format, true)),
    );

    // For trailers-only gRPC responses, also include grpc-status in headers
    // so that clients can detect this as a trailers-only response
    if protocol == Protocol::Grpc {
        response = response.header(&GRPC_STATUS, err.code.grpc_code());
        if let Some(val) = err
            .message
            .as_deref()
            .and_then(|m| http::HeaderValue::from_str(&grpc_percent_encode(m)).ok())
        {
            response = response.header(&GRPC_MESSAGE, val);
        }
    }

    let response = echo_error_headers(response, err);

    response.body(body).unwrap_or_else(|_| {
        Response::new(StreamingResponseBody {
            inner: Box::pin(futures::stream::empty()),
        })
    })
}

/// Encode gRPC trailers as a gRPC-Web trailer frame (flag byte 0x80).
///
/// The trailer frame body is HTTP/1-style headers: `key: value\r\n`.
fn encode_grpc_web_trailers(trailers: &http::HeaderMap) -> Bytes {
    let mut trailer_payload = Vec::new();
    for (key, value) in trailers.iter() {
        trailer_payload.extend_from_slice(key.as_str().as_bytes());
        trailer_payload.extend_from_slice(b": ");
        trailer_payload.extend_from_slice(value.as_bytes());
        trailer_payload.extend_from_slice(b"\r\n");
    }

    // Envelope: trailer flag, 4-byte big-endian length, payload
    let len = trailer_payload.len() as u32;
    let mut frame = Vec::with_capacity(5 + trailer_payload.len());
    frame.push(crate::envelope::flags::GRPC_WEB_TRAILER);
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&trailer_payload);
    Bytes::from(frame)
}

/// Convert headers to metadata format (keys -> list of values).
fn headers_to_metadata(
    headers: &http::HeaderMap,
) -> std::collections::HashMap<String, Vec<String>> {
    let mut metadata = std::collections::HashMap::new();
    for (key, value) in headers.iter() {
        let key_str = key.as_str().to_owned();
        let value_str = value.to_str().unwrap_or("").to_owned();
        metadata
            .entry(key_str)
            .or_insert_with(Vec::new)
            .push(value_str);
    }
    metadata
}

/// A streaming response body for server streaming RPCs.
///
/// This wraps a stream of encoded response bytes and handles envelope framing.
pub struct StreamingResponseBody {
    inner: Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, Infallible>> + Send>>,
}

impl StreamingResponseBody {
    /// Create a new streaming response body from a stream of encoded messages.
    ///
    /// For Connect protocol, trailers are encoded as a JSON EndStreamResponse
    /// in the final envelope. For gRPC, trailers are sent as HTTP/2 trailing
    /// HEADERS frames.
    fn new(
        response_stream: crate::EncodedStream,
        trailers: http::HeaderMap,
        protocol: Protocol,
        compression: Option<(Arc<CompressionRegistry>, &'static str)>,
        compression_policy: CompressionPolicy,
    ) -> Self {
        let inner: Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, Infallible>> + Send>> =
            match protocol {
                Protocol::Grpc => Box::pin(create_grpc_envelope_stream(
                    response_stream,
                    trailers,
                    compression,
                    compression_policy,
                )),
                Protocol::GrpcWeb => Box::pin(create_grpc_web_envelope_stream(
                    response_stream,
                    trailers,
                    compression,
                    compression_policy,
                )),
                Protocol::Connect => Box::pin(create_envelope_stream(
                    response_stream,
                    trailers,
                    compression,
                    compression_policy,
                )),
            };
        Self { inner }
    }
}

impl Body for StreamingResponseBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Default threshold for flushing the accumulated envelope buffer to an
/// h2 DATA frame. 16 KiB matches h2's default `max_frame_size` — any larger
/// and hyper splits into multiple frames anyway, so there's no additional
/// batching benefit.
const STREAM_BATCH_THRESHOLD: usize = 16 * 1024;

/// How the stream terminates when the source is exhausted or errors.
///
/// Protocol-dependent: Connect sends an END_STREAM envelope with JSON
/// error/metadata; gRPC sends HTTP/2 trailers; gRPC-Web encodes trailers
/// as a 0x80-flagged body frame.
enum StreamFinalizer {
    /// Connect protocol: emit an END_STREAM-flagged envelope with JSON payload.
    ConnectEndStream,
    /// gRPC: emit HTTP/2 trailing HEADERS frame.
    GrpcTrailers,
    /// gRPC-Web: emit a DATA frame with the 0x80 flag and HTTP/1-style headers.
    GrpcWebTrailers,
}

impl StreamFinalizer {
    /// Build the terminal frame for a successful stream end.
    fn success(&self, trailers: &http::HeaderMap) -> Frame<Bytes> {
        match self {
            StreamFinalizer::ConnectEndStream => {
                let end_stream = EndStreamResponse::success(trailers);
                let mut buf = bytes::BytesMut::new();
                let mut enc = crate::envelope::EnvelopeEncoder::uncompressed();
                let _ = enc.encode_end_stream(end_stream.to_json(), &mut buf);
                Frame::data(buf.freeze())
            }
            StreamFinalizer::GrpcTrailers => Frame::trailers(build_grpc_trailers(None, trailers)),
            StreamFinalizer::GrpcWebTrailers => {
                let t = build_grpc_trailers(None, trailers);
                Frame::data(encode_grpc_web_trailers(&t))
            }
        }
    }

    /// Build the terminal frame for an error stream end.
    fn error(&self, err: &ConnectError, trailers: &http::HeaderMap) -> Frame<Bytes> {
        match self {
            StreamFinalizer::ConnectEndStream => {
                let end_stream = EndStreamResponse::error(err, trailers);
                let mut buf = bytes::BytesMut::new();
                let mut enc = crate::envelope::EnvelopeEncoder::uncompressed();
                let _ = enc.encode_end_stream(end_stream.to_json(), &mut buf);
                Frame::data(buf.freeze())
            }
            StreamFinalizer::GrpcTrailers => {
                Frame::trailers(build_grpc_trailers(Some(err), trailers))
            }
            StreamFinalizer::GrpcWebTrailers => {
                let t = build_grpc_trailers(Some(err), trailers);
                Frame::data(encode_grpc_web_trailers(&t))
            }
        }
    }
}

/// A `Stream<Item = Frame<Bytes>>` that batches source items into a single
/// h2 DATA frame per poll cycle.
///
/// The `poll_next` loop drains the source stream until it returns `Pending` or
/// `None`, accumulating encoded envelopes in `buf`. This means:
///
/// - A **synchronous producer** (e.g. `stream::iter` or `unfold` with no `.await`)
///   gets all items drained in one `poll_next` → one DATA frame → one h2 lock.
/// - An **async producer** (e.g. channel receiver, DB cursor) naturally yields
///   `Pending` between items → one flush per scheduler tick.
///
/// The 16 KiB threshold bounds memory for the pathological case (fast
/// synchronous producer with large items).
///
/// A message at or above `MIN_CHAIN_SIZE` is not batched: its envelope header
/// is flushed with whatever precedes it and each of its large segments becomes
/// a data frame of its own, by reference count. The small fragments between
/// or after those segments ride in `buf`, so such a message can add a short
/// data frame per large field rather than one per poll cycle.
///
/// # Terminal state machine
///
/// At stream end (source returns `None` or `Err`), if `buf` is non-empty,
/// the data frame is emitted FIRST, and the finalizer frame is staged for
/// the next poll. This ensures partial batches aren't dropped.
struct BatchingEnvelopeStream {
    /// Source of encoded messages (already proto/JSON encoded).
    source: futures::stream::Fuse<crate::EncodedStream>,
    /// Accumulation buffer — envelopes are appended here until flush.
    buf: bytes::BytesMut,
    /// Envelope encoder — writes 5-byte header + optional compression.
    encoder: crate::envelope::EnvelopeEncoder,
    /// Context trailers carried through to the finalizer frame.
    trailers: http::HeaderMap,
    /// How to build the terminal frame.
    finalizer: StreamFinalizer,
    /// Finalizer frame staged for the next poll (when buf was non-empty at end).
    pending_final: Option<Frame<Bytes>>,
    /// Segments of the current envelope not yet emitted, in wire order. Its
    /// header (and everything before it) is already in `buf` or flushed, so
    /// these precede anything else the stream produces. See
    /// [`EnvelopeEncoder::encode_chained`] and [`Self::drain_segments`].
    ///
    /// [`EnvelopeEncoder::encode_chained`]: crate::envelope::EnvelopeEncoder::encode_chained
    pending_segments: VecDeque<Bytes>,
    /// Fused-done flag.
    done: bool,
}

impl BatchingEnvelopeStream {
    fn new(
        source: crate::EncodedStream,
        trailers: http::HeaderMap,
        compression: Option<(Arc<CompressionRegistry>, &'static str)>,
        compression_policy: CompressionPolicy,
        finalizer: StreamFinalizer,
    ) -> Self {
        Self {
            source: source.fuse(),
            buf: bytes::BytesMut::new(),
            encoder: crate::envelope::EnvelopeEncoder::new(compression, compression_policy),
            trailers,
            finalizer,
            pending_final: None,
            pending_segments: VecDeque::new(),
            done: false,
        }
    }

    /// Flush whatever's accumulated in `buf` as a data frame.
    #[inline]
    fn flush_buf(&mut self) -> Frame<Bytes> {
        Frame::data(self.buf.split().freeze())
    }

    /// Advance through `pending_segments`, returning the next data frame to
    /// emit if one is due.
    ///
    /// A segment of at least `MIN_CHAIN_SIZE` becomes its own data frame by
    /// reference count, after flushing `buf` so the envelope header and any
    /// earlier bytes go out ahead of it. A smaller segment (the tag/length
    /// fragment between two large fields, or a short tail) is copied into
    /// `buf` and rides with whatever is batched next, since a frame of its own
    /// would cost a 9-byte HTTP/2 frame header to save a copy of a few bytes.
    /// `None` means every pending segment has been consumed and `buf` may be
    /// extended with the next envelope.
    fn drain_segments(&mut self) -> Option<Frame<Bytes>> {
        while let Some(segment) = self.pending_segments.pop_front() {
            if segment.len() < crate::envelope::MIN_CHAIN_SIZE {
                self.buf.extend_from_slice(&segment);
                // A body made only of small segments must not grow `buf`
                // past the batch bound the source loop enforces.
                if self.buf.len() >= STREAM_BATCH_THRESHOLD {
                    return Some(self.flush_buf());
                }
            } else if self.buf.is_empty() {
                return Some(Frame::data(segment));
            } else {
                self.pending_segments.push_front(segment);
                return Some(self.flush_buf());
            }
        }
        None
    }
}

impl Stream for BatchingEnvelopeStream {
    type Item = Result<Frame<Bytes>, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        // Segments staged by a prior poll: their envelope header was already
        // flushed, so they precede everything else (including a staged
        // finalizer).
        if let Some(frame) = self.drain_segments() {
            return Poll::Ready(Some(Ok(frame)));
        }

        // Staged finalizer from a prior poll (buf was non-empty at stream end).
        // Only ever staged once the source has ended, which cannot happen
        // while segments are pending: the source is not polled until they
        // drain.
        if let Some(frame) = self.pending_final.take() {
            debug_assert!(self.pending_segments.is_empty());
            self.done = true;
            return Poll::Ready(Some(Ok(frame)));
        }

        loop {
            match self.source.poll_next_unpin(cx) {
                Poll::Pending if self.buf.is_empty() => {
                    // Nothing buffered, nothing ready — just wait.
                    return Poll::Pending;
                }
                Poll::Pending => {
                    // Producer not ready for more *right now* — flush what
                    // we have so the client sees it without artificial delay.
                    return Poll::Ready(Some(Ok(self.flush_buf())));
                }
                Poll::Ready(None) => {
                    // Source exhausted. If buf is non-empty, flush it and
                    // stage the finalizer for next poll; otherwise emit the
                    // finalizer directly.
                    let final_frame = self.finalizer.success(&self.trailers);
                    if self.buf.is_empty() {
                        self.done = true;
                        return Poll::Ready(Some(Ok(final_frame)));
                    } else {
                        self.pending_final = Some(final_frame);
                        return Poll::Ready(Some(Ok(self.flush_buf())));
                    }
                }
                Poll::Ready(Some(Err(err))) => {
                    // Stream error — emit error finalizer. Same split-then-
                    // finalize staging as the None case.
                    tracing::debug!(
                        error = %err,
                        "streaming response: source error, emitting error trailers"
                    );
                    let final_frame = self.finalizer.error(&err, &self.trailers);
                    if self.buf.is_empty() {
                        self.done = true;
                        return Poll::Ready(Some(Ok(final_frame)));
                    } else {
                        self.pending_final = Some(final_frame);
                        return Poll::Ready(Some(Ok(self.flush_buf())));
                    }
                }
                Poll::Ready(Some(Ok(body))) => {
                    let me = &mut *self;
                    debug_assert!(me.pending_segments.is_empty());
                    match me.encoder.encode_chained(
                        body,
                        &mut me.buf,
                        &mut me.pending_segments,
                        crate::envelope::MIN_CHAIN_SIZE,
                    ) {
                        Err(err) => {
                            // Envelope encoding/compression failed mid-stream.
                            // The buffer may contain prior successfully-encoded
                            // envelopes — don't drop them.
                            tracing::debug!(
                                error = %err,
                                "streaming response: envelope encoding failed"
                            );
                            let final_frame = self.finalizer.error(&err, &self.trailers);
                            if self.buf.is_empty() {
                                self.done = true;
                                return Poll::Ready(Some(Ok(final_frame)));
                            } else {
                                self.pending_final = Some(final_frame);
                                return Poll::Ready(Some(Ok(self.flush_buf())));
                            }
                        }
                        Ok(()) => {
                            // Segments are pending only for a large payload:
                            // `buf` now ends with its envelope header and the
                            // segments follow, the large ones unmoved.
                            if let Some(frame) = self.drain_segments() {
                                return Poll::Ready(Some(Ok(frame)));
                            }
                        }
                    }
                    if self.buf.len() >= STREAM_BATCH_THRESHOLD {
                        return Poll::Ready(Some(Ok(self.flush_buf())));
                    }
                    // Threshold not reached and producer still had data —
                    // loop and try to drain more.
                }
            }
        }
    }
}

/// Create a Connect-protocol envelope stream (ends with END_STREAM envelope).
fn create_envelope_stream(
    response_stream: crate::EncodedStream,
    trailers: http::HeaderMap,
    compression: Option<(Arc<CompressionRegistry>, &'static str)>,
    compression_policy: CompressionPolicy,
) -> impl Stream<Item = Result<Frame<Bytes>, Infallible>> + Send {
    BatchingEnvelopeStream::new(
        response_stream,
        trailers,
        compression,
        compression_policy,
        StreamFinalizer::ConnectEndStream,
    )
}

/// Create a gRPC envelope stream that sends HTTP/2 trailers.
fn create_grpc_envelope_stream(
    response_stream: crate::EncodedStream,
    trailers: http::HeaderMap,
    compression: Option<(Arc<CompressionRegistry>, &'static str)>,
    compression_policy: CompressionPolicy,
) -> impl Stream<Item = Result<Frame<Bytes>, Infallible>> + Send {
    BatchingEnvelopeStream::new(
        response_stream,
        trailers,
        compression,
        compression_policy,
        StreamFinalizer::GrpcTrailers,
    )
}

/// Create a gRPC-Web envelope stream that encodes trailers as a body frame.
fn create_grpc_web_envelope_stream(
    response_stream: crate::EncodedStream,
    trailers: http::HeaderMap,
    compression: Option<(Arc<CompressionRegistry>, &'static str)>,
    compression_policy: CompressionPolicy,
) -> impl Stream<Item = Result<Frame<Bytes>, Infallible>> + Send {
    BatchingEnvelopeStream::new(
        response_stream,
        trailers,
        compression,
        compression_policy,
        StreamFinalizer::GrpcWebTrailers,
    )
}

/// A tower Service that handles ConnectRPC requests.
///
/// This service can be composed with other tower services or used directly
/// with frameworks like axum, tonic, or hyper.
///
/// # Example
///
/// ```rust,ignore
/// let router = Arc::new(MyGreetService).register(Router::new());
/// let service = ConnectRpcService::new(router);
/// ```
///
/// # Request Limits
///
/// By default, the service applies security limits to prevent DoS attacks:
/// - Request body: 4 MB (on-wire, before decompression)
/// - Message size: 4 MB (after decompression, applied uniformly)
/// - Element memory: 32 MiB per decode (the footprint of repeated, map,
///   string and bytes elements, which a small body can inflate — see
///   [`Limits`])
///
/// These can be configured using [`with_limits`](Self::with_limits):
///
/// ```rust,ignore
/// let service = ConnectRpcService::new(router)
///     .with_limits(Limits::default().with_max_message_size(8 * 1024 * 1024));
/// ```
pub struct ConnectRpcService<D = Router> {
    dispatcher: Arc<D>,
    limits: Limits,
    /// Wrapped in `Arc` so the per-request clone in `call()` is one atomic
    /// op instead of the registry's three internal Arc fields.
    compression: Arc<CompressionRegistry>,
    compression_policy: CompressionPolicy,
    deadline_policy: DeadlinePolicy,
    /// Interceptor chain, outermost first. One pointer to clone per
    /// request regardless of chain length.
    interceptors: InterceptorChain,
}

// Manual Clone impl because `#[derive(Clone)]` would add a `D: Clone` bound,
// but we hold `Arc<D>` so no such bound is needed.
impl<D> Clone for ConnectRpcService<D> {
    fn clone(&self) -> Self {
        Self {
            dispatcher: Arc::clone(&self.dispatcher),
            limits: self.limits,
            compression: Arc::clone(&self.compression),
            compression_policy: self.compression_policy,
            deadline_policy: self.deadline_policy.clone(),
            interceptors: self.interceptors.clone(),
        }
    }
}

impl<D: Dispatcher> ConnectRpcService<D> {
    /// Create a new ConnectRPC service from a dispatcher.
    ///
    /// The dispatcher can be either:
    /// - a [`Router`] built via generated `FooServiceExt::register`, or
    /// - a code-generated `FooServiceServer<T>` struct for monomorphic
    ///   dispatch with no HashMap lookup or trait-object indirection.
    pub fn new(dispatcher: D) -> Self {
        Self::from_arc(Arc::new(dispatcher))
    }

    /// Create a new ConnectRPC service from an Arc'd dispatcher.
    pub fn from_arc(dispatcher: Arc<D>) -> Self {
        Self {
            dispatcher,
            limits: Limits::default(),
            compression: Arc::new(CompressionRegistry::default()),
            compression_policy: CompressionPolicy::default(),
            deadline_policy: DeadlinePolicy::new(),
            interceptors: InterceptorChain::default(),
        }
    }

    /// Configure the service-wide request limits.
    ///
    /// See [`Limits`] for available options. A route registered on a
    /// [`Router`] can carry its own limits via
    /// [`Router::with_route_limits`], and those replace these entirely for
    /// that method — including upward; [`MethodDescriptor::limits`] reports
    /// which routes do.
    ///
    /// [`MethodDescriptor::limits`]: crate::dispatcher::MethodDescriptor::limits
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Configure the compression registry.
    ///
    /// The registry determines which compression algorithms are available
    /// for request decompression and response compression.
    #[must_use]
    pub fn with_compression(mut self, compression: CompressionRegistry) -> Self {
        self.compression = Arc::new(compression);
        self
    }

    /// Configure the compression policy.
    ///
    /// The policy controls when compression is applied (e.g., minimum message
    /// size threshold). See [`CompressionPolicy`] for details.
    #[must_use]
    pub fn with_compression_policy(mut self, policy: CompressionPolicy) -> Self {
        self.compression_policy = policy;
        self
    }

    /// Configure server-side moderation of client-asserted RPC deadlines.
    ///
    /// The default [`DeadlinePolicy::new`] is a no-op: the client's
    /// `Connect-Timeout-Ms` / `grpc-timeout` header is honored verbatim
    /// for request receipt and handler execution, but streaming response
    /// bodies are not bounded by it. Set a policy to clamp client values
    /// to an operationally sane range, supply a default when the client
    /// asserts nothing, or extend enforcement to streaming response
    /// bodies. See [`DeadlinePolicy`] for details and recommendations.
    #[must_use]
    pub fn with_deadline_policy(mut self, policy: DeadlinePolicy) -> Self {
        self.deadline_policy = policy;
        self
    }

    /// Get the current deadline policy.
    #[must_use]
    pub fn deadline_policy(&self) -> &DeadlinePolicy {
        &self.deadline_policy
    }

    /// Append a unary [`Interceptor`] to the chain.
    ///
    /// The first interceptor registered runs **outermost**: first on the
    /// way in, last on the way out (matching `connect-go`'s
    /// `WithInterceptors`). `intercept_unary` and `intercept_streaming` run
    /// after the request body has been read and decompressed under this
    /// service's [`Limits`] and before it is decoded; a check that should
    /// run before any body byte is read belongs in
    /// [`Interceptor::intercept_head`] or in Tower middleware around the
    /// service. See [`Interceptor`]'s "When it runs".
    ///
    /// When no interceptors are registered the dispatch path allocates
    /// nothing for them and only checks that the chain is empty.
    ///
    /// Interceptors run for unary and streaming calls alike. The unary
    /// surface is [`Interceptor::intercept_unary`]; the streaming surface
    /// (covering server-streaming, client-streaming, and bidi) is
    /// [`Interceptor::intercept_streaming`].
    ///
    /// To share one interceptor instance across multiple services, use
    /// [`with_interceptor_arc`](Self::with_interceptor_arc).
    #[must_use]
    pub fn with_interceptor(self, interceptor: impl Interceptor) -> Self {
        self.with_interceptor_arc(Arc::new(interceptor))
    }

    /// Append an already-`Arc`'d unary [`Interceptor`] to the chain.
    ///
    /// Same ordering and semantics as
    /// [`with_interceptor`](Self::with_interceptor). Use this when one
    /// interceptor instance is shared across multiple `ConnectRpcService`s
    /// — e.g. an auth interceptor whose state (a connection pool, a token
    /// cache) is process-wide and should not be duplicated per service.
    /// Most callers want [`with_interceptor`](Self::with_interceptor),
    /// which takes ownership and wraps for you.
    #[must_use]
    pub fn with_interceptor_arc(mut self, interceptor: Arc<dyn Interceptor>) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    /// Get the current limits configuration.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Get a reference to the underlying dispatcher.
    pub fn dispatcher(&self) -> &D {
        &self.dispatcher
    }
}

/// A lightweight body for gRPC unary responses.
///
/// Yields one data frame carrying the gRPC envelope — or, for a large
/// response, just its 5-byte header followed by one data frame per payload
/// segment, each passed through by refcount — then one trailers frame (for
/// gRPC) or a final data frame (for gRPC-Web trailer encoding).
/// This avoids the overhead of `Pin<Box<dyn Stream>>` + `stream::unfold` +
/// `EnvelopeEncoder` for the common unary case.
pub struct GrpcUnaryBody {
    data: Option<Bytes>,
    /// Large message payload (post-compression, when negotiated) emitted as
    /// its own data frame after `data` (which then carries only the 5-byte
    /// envelope header), so the payload is passed through by refcount
    /// instead of copied.
    ///
    /// More than one when the encoder handed back several segments — a view
    /// re-encode captures each large borrowed field separately rather than
    /// gathering them, so they stay separate all the way to the socket.
    payload: std::collections::VecDeque<Bytes>,
    trailers: Option<GrpcUnaryTrailers>,
}

/// How trailers are delivered in a gRPC unary response.
enum GrpcUnaryTrailers {
    /// HTTP/2 trailers frame (native gRPC).
    Http2(http::HeaderMap),
    /// Body data frame with flag 0x80 (gRPC-Web).
    WebBody(Bytes),
}

impl Body for GrpcUnaryBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let me = self.get_mut();
        if let Some(data) = me.data.take() {
            Poll::Ready(Some(Ok(Frame::data(data))))
        } else if let Some(payload) = me.payload.pop_front() {
            Poll::Ready(Some(Ok(Frame::data(payload))))
        } else if let Some(trailers) = me.trailers.take() {
            match trailers {
                GrpcUnaryTrailers::Http2(map) => Poll::Ready(Some(Ok(Frame::trailers(map)))),
                GrpcUnaryTrailers::WebBody(bytes) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
            }
        } else {
            Poll::Ready(None)
        }
    }
}

/// Response body type that can be Connect unary, gRPC unary, or streaming.
#[non_exhaustive]
pub enum ConnectRpcBody {
    /// Connect protocol unary response (single `Full<Bytes>` body).
    Full(Full<Bytes>),
    /// gRPC/gRPC-Web unary response (data frame + trailers, no stream overhead).
    GrpcUnary(GrpcUnaryBody),
    /// Streaming response (server streaming, client streaming, bidi, or gRPC unary fallback).
    Streaming(StreamingResponseBody),
}

impl Body for ConnectRpcBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.get_mut() {
            ConnectRpcBody::Full(inner) => Pin::new(inner).poll_frame(cx),
            ConnectRpcBody::GrpcUnary(inner) => Pin::new(inner).poll_frame(cx),
            ConnectRpcBody::Streaming(inner) => Pin::new(inner).poll_frame(cx),
        }
    }
}

impl<D, B> tower::Service<Request<B>> for ConnectRpcService<D>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    type Response = Response<ConnectRpcBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let dispatcher = Arc::clone(&self.dispatcher);
        let limits = self.limits;
        let compression = Arc::clone(&self.compression);
        let compression_policy = self.compression_policy;
        let deadline_policy = self.deadline_policy.clone();
        let interceptors = self.interceptors.clone();

        // Only create and attach the tracing span when a subscriber would
        // actually observe it. For disabled-debug (the common production case),
        // `.instrument()` still wraps the future and calls span.enter()/exit()
        // on every poll — profiling showed ~0.8% CPU even with the span itself
        // disabled. Branching on enabled! avoids the wrapper entirely.
        //
        // The span must be built BEFORE the async-move captures `req`.
        let span = if tracing::enabled!(tracing::Level::DEBUG) {
            Some(tracing::debug_span!(
                "connectrpc_request",
                path = %req.uri().path(),
                method = %req.method(),
                protocol = tracing::field::Empty,
                codec = tracing::field::Empty,
            ))
        } else {
            None
        };

        let fut = async move {
            let response = match handle_request(
                dispatcher,
                req,
                limits,
                compression,
                &compression_policy,
                &deadline_policy,
                &interceptors,
            )
            .await
            {
                Ok(response) => response,
                Err(err) => error_response_either(err),
            };
            Ok(response)
        };

        match span {
            Some(span) => Box::pin(fut.instrument(span)),
            None => Box::pin(fut),
        }
    }
}

/// Handle a ConnectRPC request (unary or streaming).
#[allow(clippy::too_many_arguments)]
async fn handle_request<D, B>(
    dispatcher: Arc<D>,
    mut req: Request<B>,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
    deadline_policy: &DeadlinePolicy,
    interceptors: &[Arc<dyn Interceptor>],
) -> Result<Response<ConnectRpcBody>, ConnectError>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    // Detect protocol and codec from Content-Type and record into the
    // tracing span created by ConnectRpcService::call.
    let request_protocol = Protocol::detect(req.headers());
    if let Some(ref rp) = request_protocol {
        let span = tracing::Span::current();
        span.record("protocol", tracing::field::display(rp.protocol));
        span.record("codec", tracing::field::display(rp.codec_format));
    }

    // Resolve the route once. When it declares its own limits they govern
    // every body read below, the error-path drains included; the path and
    // descriptor are handed on so no later stage derives them again.
    let path = req.uri().path();
    let path = path.strip_prefix('/').unwrap_or(path).to_owned();
    let desc = dispatcher.lookup(&path);
    let limits = desc.and_then(|d| d.limits).unwrap_or(limits);

    // Only GET and POST carry RPCs. Reject every other verb uniformly with
    // 405 Method Not Allowed plus an `Allow` header (connect-go parity),
    // regardless of whether a Content-Type is present. Without this, a bodyless
    // OPTIONS/HEAD request (no Content-Type, so protocol detection returns
    // None) would fall through to the unsupported-media-type path and be
    // misreported as 415. An unknown path still maps to 404.
    if req.method() != Method::GET && req.method() != Method::POST {
        return reject_unsupported_method(&path, desc, req, limits, deadline_policy).await;
    }

    // Connect GET requests don't have a Content-Type header, so protocol
    // detection returns None. Route GET requests directly to the unary handler
    // which handles Connect GET query parameter parsing.
    if req.method() == Method::GET {
        if !interceptors.is_empty() {
            intercept_heads(interceptors, &mut req, desc, Protocol::Connect).await?;
        }
        return handle_unary_request(
            &*dispatcher,
            &path,
            desc,
            req,
            limits,
            compression,
            compression_policy,
            deadline_policy,
            interceptors,
        )
        .await
        .map(|r| r.map(ConnectRpcBody::Full));
    }

    // Content type didn't resolve to a known protocol+codec. A gRPC/gRPC-Web
    // prefix with an unsupported codec (e.g. application/grpc+thrift, or
    // application/grpc+json in a proto-only build) returns a gRPC `unimplemented`
    // error so the client gets gRPC framing it can parse (the compression axis
    // returns `unimplemented` for an unsupported encoding the same way). Any
    // other unrecognized content
    // type returns HTTP 415 Unsupported Media Type with an `Accept-Post` header
    // advertising the content types this server does accept.
    if request_protocol.is_none() {
        let ct = req
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let timeout_protocol = if ct.starts_with("application/grpc-web") {
            Protocol::GrpcWeb
        } else if ct.starts_with("application/grpc") {
            Protocol::Grpc
        } else {
            Protocol::Connect
        };
        let deadline =
            deadline_from_headers(req.headers(), timeout_protocol, &path, deadline_policy);

        // Drain the request body to avoid broken pipe on HTTP/1.1.
        let (_parts, body) = req.into_parts();
        let _ = with_request_deadline(
            deadline,
            collect_body_limited(body, limits.max_request_body_size),
        )
        .await;

        if ct.starts_with("application/grpc-web") {
            let err = ConnectError::unimplemented("unsupported content type");
            let response = grpc_error_response(&err, Protocol::GrpcWeb, CodecFormat::Proto);
            return Ok(response.map(ConnectRpcBody::Streaming));
        } else if ct.starts_with("application/grpc") {
            let err = ConnectError::unimplemented("unsupported content type");
            let response = grpc_error_response(&err, Protocol::Grpc, CodecFormat::Proto);
            return Ok(response.map(ConnectRpcBody::Streaming));
        } else {
            // Unknown content type that doesn't match any protocol.
            // Return HTTP 415 Unsupported Media Type with no body, advertising
            // the content types we do accept via `Accept-Post` (connect-go
            // parity). The empty body means clients map the 415 status to an
            // error code (Connect: unknown) per the protocol's HTTP-status
            // mapping.
            let response = Response::builder()
                .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
                .header("accept-post", ACCEPT_POST)
                .body(Full::new(Bytes::new()))
                .unwrap();
            return Ok(response.map(ConnectRpcBody::Full));
        }
    }

    match request_protocol {
        Some(rp) if rp.is_streaming => {
            // Head checks run before anything reads the body, the text-mode
            // rejection's drain included.
            if !interceptors.is_empty()
                && let Err(err) = intercept_heads(interceptors, &mut req, desc, rp.protocol).await
            {
                return Ok(streaming_error_response(&err, rp.protocol, rp.codec_format)
                    .map(ConnectRpcBody::Streaming));
            }

            // gRPC-Web text mode (application/grpc-web-text) base64-encodes the
            // entire body. We detect it (protocol.rs) but don't decode it — reject
            // explicitly with a clear error rather than failing with garbage-envelope
            // noise. Text mode is primarily for legacy browsers without binary
            // body support.
            if rp.is_text_mode {
                let err = ConnectError::unimplemented(
                    "gRPC-Web text mode (application/grpc-web-text) is not supported",
                );
                // Drain the body to preserve HTTP/1.1 keep-alive for the error response.
                let deadline =
                    deadline_from_headers(req.headers(), rp.protocol, &path, deadline_policy);
                let (_parts, body) = req.into_parts();
                let _ = with_request_deadline(
                    deadline,
                    collect_body_limited(body, limits.max_request_body_size),
                )
                .await;
                let response = grpc_error_response(&err, Protocol::GrpcWeb, rp.codec_format);
                return Ok(response.map(ConnectRpcBody::Streaming));
            }

            // If so, take the fast path that avoids stream wrapping overhead.
            if matches!(rp.protocol, Protocol::Grpc | Protocol::GrpcWeb)
                && let Some(desc) = desc
                && desc.kind == MethodKind::Unary
            {
                let response = handle_grpc_unary_request(
                    &*dispatcher,
                    &path,
                    desc.spec,
                    req,
                    rp.protocol,
                    rp.codec_format,
                    limits,
                    compression,
                    compression_policy,
                    deadline_policy,
                    interceptors,
                )
                .await;
                return Ok(response.map(ConnectRpcBody::GrpcUnary));
            }

            // Streaming request (Connect streaming, gRPC, or gRPC-Web)
            let response = handle_streaming_request(
                &*dispatcher,
                &path,
                desc,
                req,
                rp.protocol,
                rp.codec_format,
                limits,
                compression,
                compression_policy,
                deadline_policy,
                interceptors,
            )
            .await;
            Ok(response.map(ConnectRpcBody::Streaming))
        }
        Some(_) | None => {
            if !interceptors.is_empty() {
                intercept_heads(interceptors, &mut req, desc, Protocol::Connect).await?;
            }
            // Unary request (Connect unary) or unknown content type (for error reporting)
            handle_unary_request(
                &*dispatcher,
                &path,
                desc,
                req,
                limits,
                compression,
                compression_policy,
                deadline_policy,
                interceptors,
            )
            .await
            .map(|r| r.map(ConnectRpcBody::Full))
        }
    }
}

/// Run every interceptor's [`Interceptor::intercept_head`] on a request whose
/// body has not been read, stopping at the first error. Callers skip it when
/// no interceptor is registered, so that path builds no [`RequestHead`].
///
/// The request's extensions are moved into the head for the duration and put
/// back afterwards, so a value a head check inserts reaches the handler.
/// Taking `&mut Request<B>` needs only `B: Send`; a shared reference held
/// across the `.await` would need `B: Sync`.
async fn intercept_heads<B>(
    interceptors: &[Arc<dyn Interceptor>],
    req: &mut Request<B>,
    desc: Option<MethodDescriptor>,
    protocol: Protocol,
) -> Result<(), ConnectError> {
    let mut extensions = std::mem::take(req.extensions_mut());
    let result = {
        let mut head = RequestHead::new(req.uri().path(), req.headers(), &mut extensions)
            .with_spec(desc.and_then(|desc| desc.spec))
            .with_protocol(protocol);
        let mut result = Ok(());
        for interceptor in interceptors {
            result = interceptor.intercept_head(&mut head).await;
            if result.is_err() {
                break;
            }
        }
        result
    };
    *req.extensions_mut() = extensions;
    result
}

/// `Accept-Post` advertised on a 415 response: the content types this server
/// accepts. JSON media types appear only when the `json` feature is enabled — a
/// proto-only build advertises proto-only. gRPC-Web text mode is intentionally
/// omitted: the server detects but rejects it (`Unimplemented`), so it must not
/// be advertised as accepted.
#[cfg(feature = "json")]
const ACCEPT_POST: &str = "application/connect+json, application/connect+proto, \
application/grpc, application/grpc+json, application/grpc+proto, \
application/grpc-web, application/grpc-web+json, application/grpc-web+proto, \
application/json, application/proto";

/// See the `json`-enabled variant; a proto-only build drops every JSON media
/// type so it never advertises a codec it cannot serve.
#[cfg(not(feature = "json"))]
const ACCEPT_POST: &str = "application/connect+proto, application/grpc, \
application/grpc+proto, application/grpc-web, application/grpc-web+proto, \
application/proto";

/// Reject an HTTP method other than GET or POST.
///
/// Mirrors connect-go: a known procedure path returns 405 Method Not Allowed
/// with an `Allow` header listing the methods it accepts (always `POST`; plus
/// `GET` for idempotent unary methods). An unknown path returns the same
/// `unimplemented` (HTTP 404) as any other route miss. The request body is
/// drained first so HTTP/1.1 connections stay reusable.
async fn reject_unsupported_method<B>(
    path: &str,
    desc: Option<MethodDescriptor>,
    req: Request<B>,
    limits: Limits,
    deadline_policy: &DeadlinePolicy,
) -> Result<Response<ConnectRpcBody>, ConnectError>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let allow = desc.map(|desc| {
        if desc.kind == MethodKind::Unary && desc.idempotent {
            "GET, POST"
        } else {
            "POST"
        }
    });

    // Drain the request body so an early response doesn't break HTTP/1.1
    // keep-alive (see `request_body_drain`).
    let deadline = deadline_from_headers(req.headers(), Protocol::Connect, path, deadline_policy);
    let (_parts, body) = req.into_parts();
    let _ = with_request_deadline(
        deadline,
        collect_body_limited(body, limits.max_request_body_size),
    )
    .await;

    match allow {
        // Bare 405 with `Allow`: the empty body makes the client map the 405
        // status to an error code (Connect: unknown) per the HTTP-status table.
        Some(allow) => {
            let response = Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .header(header::ALLOW, allow)
                .body(Full::new(Bytes::new()))
                .unwrap();
            Ok(response.map(ConnectRpcBody::Full))
        }
        None => Err(
            ConnectError::unimplemented(format!("method not found: {path}"))
                .with_http_status(StatusCode::NOT_FOUND),
        ),
    }
}

/// Handle a unary ConnectRPC request.
#[allow(clippy::too_many_arguments)]
async fn handle_unary_request<D, B>(
    dispatcher: &D,
    path: &str,
    desc: Option<MethodDescriptor>,
    req: Request<B>,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
    deadline_policy: &DeadlinePolicy,
    interceptors: &[Arc<dyn Interceptor>],
) -> Result<Response<Full<Bytes>>, ConnectError>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let (parts, body) = req.into_parts();
    let method = parts.method;
    let extensions = parts.extensions;

    // Extract metadata from headers using the Connect protocol (unary is always Connect)
    let mut metadata = RequestMetadata::from_headers(parts.headers, Protocol::Connect);
    metadata.timeout = deadline_policy.moderate(metadata.timeout, path);
    let deadline = absolute_deadline(metadata.timeout);

    // IMPORTANT: Read the full request body BEFORE returning any errors.
    // For HTTP/1.1, returning an error without reading the body causes
    // "broken pipe" errors because the client is still sending data.
    // collect_body_limited bounds allocation *during* the read, so an
    // oversized body is rejected before it is fully buffered.
    let post_body = with_request_deadline(
        deadline,
        collect_body_limited(body, limits.max_request_body_size),
    )
    .await?;

    // A miss is reported only now that the body has been drained.
    // (Non-unary kinds are allowed through — they'll error at the dispatch call.)
    let desc = desc.ok_or_else(|| {
        ConnectError::unimplemented(format!("method not found: {path}"))
            .with_http_status(StatusCode::NOT_FOUND)
    })?;
    let is_idempotent = desc.idempotent;

    // Handle GET vs POST requests
    let (body, codec_format) = if method == Method::GET {
        // GET requests are only allowed for idempotent methods
        if !is_idempotent {
            return Err(ConnectError::method_not_allowed(
                "GET requests are only supported for idempotent methods",
            ));
        }

        // Parse query parameters for GET request
        let params = parse_get_query_params(parts.uri.query())?;

        // Validate connect version from query param
        if let Some(ref version) = params.connect_version
            && version != "v1"
        {
            return Err(ConnectError::invalid_argument(
                "unsupported protocol version",
            ));
        }

        // Decode message from query parameter
        let message = decode_get_message(&params, &compression, limits.max_message_size)?;

        // Get codec format from encoding query param
        let encoding = params.encoding.as_deref().unwrap_or("proto");
        let codec_format = CodecFormat::from_codec(encoding).ok_or_else(|| {
            ConnectError::unsupported_media_type(format!("unsupported encoding: {encoding}"))
        })?;

        (message, codec_format)
    } else if method == Method::POST {
        // Validate Connect-Protocol-Version header for POST
        if let Some(ref version) = metadata.protocol_version
            && version != "1"
        {
            return Err(ConnectError::invalid_argument(
                "unsupported protocol version",
            ));
        }

        // Check content type and determine codec format
        let content_type_str = metadata
            .content_type
            .as_deref()
            .unwrap_or(content_type::PROTO);

        let codec_format = CodecFormat::from_content_type(content_type_str).ok_or_else(|| {
            ConnectError::unsupported_media_type(format!(
                "unsupported content type: {content_type_str}"
            ))
        })?;

        // Body size was already enforced by collect_body_limited above.

        // Decompress if needed
        let body = if let Some(ref encoding) = metadata.unary_encoding {
            compression.decompress_with_limit(encoding, post_body, limits.max_message_size)?
        } else {
            post_body
        };

        // Check message size limit (for uncompressed messages; compressed ones
        // are already bounded by decompress_with_limit)
        if body.len() > limits.max_message_size {
            return Err(ConnectError::resource_exhausted(format!(
                "message size {} exceeds limit {}",
                body.len(),
                limits.max_message_size
            )));
        }

        (body, codec_format)
    } else {
        // Backstop: `handle_request` rejects every non-GET/POST verb upstream
        // (with 405 + `Allow`), so this arm is unreachable in normal dispatch.
        return Err(ConnectError::method_not_allowed(
            "only GET and POST methods are supported",
        ));
    };

    // Create handler context with the request headers from metadata.
    let ctx = RequestContext::new(metadata.headers)
        .with_deadline(deadline)
        .with_extensions(extensions)
        .with_spec(desc.spec)
        .with_protocol(Some(Protocol::Connect))
        // The leading slash was stripped for the Dispatcher::lookup key;
        // restore it so RequestContext::path() matches http::Uri::path()
        // and Spec::procedure.
        .with_path(["/", path].concat())
        .with_decode_options(limits.decode_options());

    // Call the handler with the appropriate codec format.
    let resp: EncodedResponse = with_request_deadline(
        deadline,
        call_unary_intercepted(dispatcher, interceptors, path, ctx, body, codec_format),
    )
    .await?;

    // Negotiate response compression
    let response_encoding = compression.negotiate_encoding(
        metadata.unary_accept_encoding.as_deref(),
        metadata.unary_encoding.as_deref(),
    );

    // Compress response body if negotiated, respecting the compression policy
    let effective_policy = compression_policy.with_override(resp.compress);
    // Connect unary flattens. Compression needs one contiguous input, and the
    // uncompressed case would need `Full<Bytes>` replaced with a multi-frame
    // body to carry segments — Connect puts the message straight in the HTTP
    // body, so nothing here splits it for us the way an envelope does. The
    // segmented encode therefore reaches only gRPC and gRPC-Web unary.
    // Flattening is a no-op unless the encoder segmented.
    let body_len = resp.body.len();
    let resp_body = resp.body.into_contiguous();
    let (final_body, content_encoding) = if let Some(encoding) = response_encoding {
        if effective_policy.should_compress(body_len) {
            match compression.compress(encoding, &resp_body) {
                Ok(compressed) => (compressed, Some(encoding)),
                Err(_) => (resp_body, None), // Fall back to uncompressed
            }
        } else {
            (resp_body, None)
        }
    } else {
        (resp_body, None)
    };

    // Build response with the same content type as the request
    let mut response = response_head(
        codec_format.content_type(),
        &header::CONTENT_ENCODING,
        content_encoding,
        &header::ACCEPT_ENCODING,
        &compression,
    );

    // Add response headers set by the handler
    for (key, value) in resp.headers.iter() {
        response = response.header(key, value);
    }

    // Add trailers as trailer- prefixed headers
    let response = add_trailers(response, &resp.trailers);

    response
        .body(Full::new(final_body))
        .map_err(|e| ConnectError::internal(format!("failed to build response: {e}")))
}

/// Start a `200 OK` response carrying the negotiated content type, the
/// response encoding when one was applied, and the accept-encoding
/// advertisement (optional per spec, informational). Every value is a static
/// or registry-cached `HeaderValue`, so none is allocated per response.
fn response_head(
    content_type: &'static str,
    encoding_header: &header::HeaderName,
    encoding: Option<&'static str>,
    accept_encoding_header: &header::HeaderName,
    compression: &CompressionRegistry,
) -> http::response::Builder {
    let mut response = Response::builder().status(StatusCode::OK).header(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static(content_type),
    );
    if let Some(encoding) = encoding {
        // Encoding names are validated as HTTP tokens by
        // `CompressionRegistry::register`, which is what `from_static` needs.
        response = response.header(encoding_header, http::HeaderValue::from_static(encoding));
    }
    if let Some(accept) = compression.accept_encoding_value() {
        response = response.header(accept_encoding_header, accept.clone());
    }
    response
}

/// Handle a gRPC/gRPC-Web unary request via the fast path.
///
/// This avoids the overhead of `handle_streaming_request` for unary RPCs:
/// no `BoxStream`, no `stream::unfold`, no `EnvelopeEncoder` — just inline
/// envelope decoding/encoding and a lightweight two-frame body.
#[allow(clippy::too_many_arguments)]
async fn handle_grpc_unary_request<D, B>(
    dispatcher: &D,
    path: &str,
    spec: Option<crate::spec::Spec>,
    req: Request<B>,
    protocol: Protocol,
    codec_format: CodecFormat,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
    deadline_policy: &DeadlinePolicy,
    interceptors: &[Arc<dyn Interceptor>],
) -> Response<GrpcUnaryBody>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    // Take the request apart first: the headers move into the metadata (and
    // from there into the handler context), the body is read below.
    let (parts, body) = req.into_parts();
    let extensions = parts.extensions;
    let mut metadata = RequestMetadata::from_headers(parts.headers, protocol);
    metadata.timeout = deadline_policy.moderate(metadata.timeout, path);
    let deadline = absolute_deadline(metadata.timeout);

    // Helper to build a gRPC trailers-only error response using GrpcUnaryBody.
    let grpc_unary_error = |err: &ConnectError| -> Response<GrpcUnaryBody> {
        let grpc_trailers = build_grpc_trailers(Some(err), err.trailers());
        let trailers = match protocol {
            Protocol::Grpc => GrpcUnaryTrailers::Http2(grpc_trailers),
            Protocol::GrpcWeb => {
                GrpcUnaryTrailers::WebBody(encode_grpc_web_trailers(&grpc_trailers))
            }
            Protocol::Connect => unreachable!("gRPC unary fast path is gRPC/gRPC-Web only"),
        };
        let mut response = Response::builder().status(StatusCode::OK).header(
            header::CONTENT_TYPE,
            http::HeaderValue::from_static(protocol.response_content_type(codec_format, true)),
        );
        // For trailers-only gRPC responses, include grpc-status in headers
        if protocol == Protocol::Grpc {
            response = response.header(&GRPC_STATUS, err.code.grpc_code());
            if let Some(val) = err
                .message
                .as_deref()
                .and_then(|m| http::HeaderValue::from_str(&grpc_percent_encode(m)).ok())
            {
                response = response.header(&GRPC_MESSAGE, val);
            }
        }
        let response = echo_error_headers(response, err);
        let body = GrpcUnaryBody {
            data: None,
            payload: std::collections::VecDeque::new(),
            trailers: Some(trailers),
        };
        response.body(body).unwrap_or_else(|_| {
            Response::new(GrpcUnaryBody {
                data: None,
                payload: std::collections::VecDeque::new(),
                trailers: None,
            })
        })
    };

    // gRPC requires POST. Backstop: `handle_request` already rejects non-GET/
    // POST verbs upstream, and GET never routes here, so this is defensive.
    if parts.method != Method::POST {
        let err = ConnectError::internal(format!("invalid method for gRPC: {}", parts.method));
        // Drain the request body to avoid broken pipe on HTTP/1.1.
        let _ = with_request_deadline(
            deadline,
            collect_body_limited(body, limits.max_request_body_size),
        )
        .await;
        return grpc_unary_error(&err);
    }

    // Validate request compression
    if let Some(ref encoding) = metadata.streaming_encoding
        && encoding != "identity"
        && !compression.supports(encoding)
    {
        let err = ConnectError::unimplemented(format!("unsupported compression: {encoding}"));
        // Drain the request body to avoid broken pipe on HTTP/1.1.
        let _ = with_request_deadline(
            deadline,
            collect_body_limited(body, limits.max_request_body_size),
        )
        .await;
        return grpc_unary_error(&err);
    }

    // Read the full body. collect_body_limited bounds allocation during the
    // read, so an oversized body is rejected before it is fully buffered.
    let post_body = match with_request_deadline(
        deadline,
        collect_body_limited(body, limits.max_request_body_size),
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(err) => return grpc_unary_error(&err),
    };

    // Decode the gRPC envelope (5-byte header + payload)
    let request_body = if post_body.is_empty() {
        // Empty body means no envelope at all — this is an error for unary RPCs
        let err = ConnectError::unimplemented("request body is empty: expected a message");
        return grpc_unary_error(&err);
    } else {
        let mut post_body = post_body;
        let envelope =
            match Envelope::decode_bytes_with_limit(&mut post_body, limits.max_message_size) {
                Ok(Some(env)) => env,
                Ok(None) => {
                    let err = ConnectError::invalid_argument("incomplete request envelope");
                    return grpc_unary_error(&err);
                }
                Err(e) => return grpc_unary_error(&e),
            };

        // Reject anything after the one envelope of a unary request
        if !post_body.is_empty() {
            let err = ConnectError::unimplemented("unary request must have exactly one message");
            return grpc_unary_error(&err);
        }

        // Decompress if needed
        if envelope.is_compressed() {
            let encoding = match metadata.streaming_encoding.as_deref() {
                Some(enc) if enc != "identity" => enc,
                _ => {
                    let err = ConnectError::internal(format!(
                        "received compressed message without {} header",
                        protocol.content_encoding_header()
                    ));
                    return grpc_unary_error(&err);
                }
            };
            match compression.decompress_with_limit(
                encoding,
                envelope.data,
                limits.max_message_size,
            ) {
                Ok(data) => data,
                Err(e) => return grpc_unary_error(&e),
            }
        } else {
            envelope.data
        }
    };

    // Create handler context
    let ctx = RequestContext::new(metadata.headers)
        .with_deadline(deadline)
        .with_extensions(extensions)
        .with_spec(spec)
        .with_protocol(Some(protocol))
        .with_path(["/", path].concat())
        .with_decode_options(limits.decode_options());

    // Call the handler with the same deadline used while receiving the body.
    let resp = match with_request_deadline(
        deadline,
        call_unary_intercepted(
            dispatcher,
            interceptors,
            path,
            ctx,
            request_body,
            codec_format,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(e) => return grpc_unary_error(&e),
    };

    // Negotiate response compression
    let response_encoding = compression.negotiate_encoding(
        metadata.streaming_accept_encoding.as_deref(),
        metadata.streaming_encoding.as_deref(),
    );

    // Encode response into a gRPC envelope (5-byte header + payload). Large
    // payloads are chained (header segment + payload by
    // refcount) rather than copied into a contiguous buffer.
    let effective_policy = compression_policy.with_override(resp.compress);
    let min_chain = crate::envelope::MIN_CHAIN_SIZE;
    let (encoded_data, chained_payload) = if let Some(encoding) = response_encoding
        && effective_policy.should_compress(resp.body.len())
    {
        // Compression needs one contiguous input and produces one contiguous
        // output, so any segmentation the encoder managed ends here. That is
        // the right trade: the compressor was going to read every byte anyway.
        let flat = resp.body.into_contiguous();
        match compression.compress(encoding, &flat) {
            Ok(compressed) => Envelope::encode_body_parts(
                crate::envelope::flags::COMPRESSED,
                compressed.into(),
                min_chain,
            ),
            Err(_) => {
                Envelope::encode_body_parts(crate::envelope::flags::DATA, flat.into(), min_chain)
            }
        }
    } else {
        Envelope::encode_body_parts(crate::envelope::flags::DATA, resp.body, min_chain)
    };

    // Build gRPC trailers
    let grpc_trailers = build_grpc_trailers(None, &resp.trailers);

    // Build the trailers in the appropriate format for the protocol
    let trailers = match protocol {
        Protocol::Grpc => GrpcUnaryTrailers::Http2(grpc_trailers),
        Protocol::GrpcWeb => GrpcUnaryTrailers::WebBody(encode_grpc_web_trailers(&grpc_trailers)),
        Protocol::Connect => unreachable!("Connect unary uses handle_unary_request"),
    };

    // Build response headers
    let mut response = response_head(
        protocol.response_content_type(codec_format, true),
        protocol.content_encoding_header(),
        response_encoding,
        protocol.accept_encoding_header(),
        &compression,
    );

    // Add response headers set by the handler
    for (key, value) in resp.headers.iter() {
        response = response.header(key, value);
    }

    let body = GrpcUnaryBody {
        data: Some(encoded_data),
        payload: chained_payload.into(),
        trailers: Some(trailers),
    };

    response.body(body).unwrap_or_else(|_| {
        let err = ConnectError::internal("failed to build response");
        grpc_unary_error(&err)
    })
}

/// For server-streaming + unary-via-gRPC-streaming, whether the method
/// is server-streaming or unary. Decides whether the single response is
/// wrapped in a one-item stream (unary) or the response stream is
/// forwarded directly (server-streaming).
#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamingDispatchKind {
    ServerStreaming,
    Unary,
}

/// Handle a streaming ConnectRPC request.
///
/// For streaming RPCs, errors are returned in the EndStreamResponse envelope
/// with HTTP 200 status, not as HTTP error responses.
///
/// For gRPC, this also handles unary RPCs since all gRPC RPCs use envelope
/// framing. When a unary handler is found, the single request envelope is
/// decoded, the handler is called, and the response is envelope-framed.
#[allow(clippy::too_many_arguments)]
async fn handle_streaming_request<D, B>(
    dispatcher: &D,
    path: &str,
    method_desc: Option<MethodDescriptor>,
    req: Request<B>,
    protocol: Protocol,
    codec_format: CodecFormat,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
    deadline_policy: &DeadlinePolicy,
    interceptors: &[Arc<dyn Interceptor>],
) -> Response<StreamingResponseBody>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    // Take the request apart first: the headers move into the metadata (and
    // from there into the handler context), the body is read or streamed below.
    let (parts, body) = req.into_parts();
    let extensions = parts.extensions;
    let mut metadata = RequestMetadata::from_headers(parts.headers, protocol);
    metadata.timeout = deadline_policy.moderate(metadata.timeout, path);

    // gRPC and gRPC-Web require POST method. Backstop: non-GET/POST verbs are
    // already rejected upstream in `handle_request`, and GET never routes here.
    if matches!(protocol, Protocol::Grpc | Protocol::GrpcWeb) && parts.method != Method::POST {
        let err = ConnectError::internal(format!("invalid method for gRPC: {}", parts.method));
        // Drain the request body to avoid broken pipe on HTTP/1.1.
        let deadline = absolute_deadline(metadata.timeout);
        let _ = with_request_deadline(
            deadline,
            collect_body_limited(body, limits.max_request_body_size),
        )
        .await;
        return streaming_error_response(&err, protocol, codec_format);
    }

    // Validate request compression is supported (if specified)
    if let Some(ref encoding) = metadata.streaming_encoding
        && encoding != "identity"
        && !compression.supports(encoding)
    {
        let err = ConnectError::unimplemented(format!("unsupported compression: {encoding}"));
        // Drain the request body to avoid broken pipe on HTTP/1.1.
        let deadline = absolute_deadline(metadata.timeout);
        let _ = with_request_deadline(
            deadline,
            collect_body_limited(body, limits.max_request_body_size),
        )
        .await;
        return streaming_error_response(&err, protocol, codec_format);
    }

    // For bidi streaming, pass the raw body stream directly (no buffering)
    if matches!(method_desc, Some(d) if d.kind == MethodKind::BidiStreaming) {
        return handle_bidi_streaming_request(
            dispatcher,
            path,
            method_desc.and_then(|d| d.spec),
            metadata,
            body,
            extensions,
            protocol,
            codec_format,
            limits,
            compression,
            compression_policy,
            deadline_policy,
            interceptors,
        )
        .await;
    }

    // For client streaming, pass the raw body stream directly (no buffering)
    if matches!(method_desc, Some(d) if d.kind == MethodKind::ClientStreaming) {
        return handle_client_streaming_request(
            dispatcher,
            path,
            method_desc.and_then(|d| d.spec),
            metadata,
            body,
            extensions,
            protocol,
            codec_format,
            limits,
            compression,
            compression_policy,
            interceptors,
        )
        .await;
    }

    let deadline = absolute_deadline(metadata.timeout);

    // For server streaming (or errors), read the full body (single envelope expected).
    // collect_body_limited bounds allocation during the read.
    let post_body = match with_request_deadline(
        deadline,
        collect_body_limited(body, limits.max_request_body_size),
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(err) => return streaming_error_response(&err, protocol, codec_format),
    };

    // Remaining kinds: server-streaming (forward response stream) or
    // unary-through-streaming (gRPC only; wrap response in one-item stream).
    let dispatch_kind = match method_desc.map(|d| d.kind) {
        Some(MethodKind::ServerStreaming) => StreamingDispatchKind::ServerStreaming,
        Some(MethodKind::Unary) => match protocol {
            // gRPC sends unary RPCs with streaming framing, so fall through.
            Protocol::Grpc | Protocol::GrpcWeb => StreamingDispatchKind::Unary,
            Protocol::Connect => {
                let err =
                    ConnectError::invalid_argument("streaming content type used for unary method");
                return streaming_error_response(&err, protocol, codec_format);
            }
        },
        None => {
            let err = ConnectError::unimplemented(format!("method not found: {path}"));
            return streaming_error_response(&err, protocol, codec_format);
        }
        // BidiStreaming and ClientStreaming already handled above
        Some(MethodKind::BidiStreaming | MethodKind::ClientStreaming) => {
            unreachable!("bidi and client streaming handled before body buffering")
        }
    };

    // Body size was already enforced by collect_body_limited above.

    // For server streaming, the request is envelope-framed
    // Decode the envelope to get the request message
    let request_body = if post_body.is_empty() {
        // Server streaming requires exactly one request envelope
        let err = ConnectError::unimplemented("server streaming request requires a message");
        return streaming_error_response(&err, protocol, codec_format);
    } else {
        let mut post_body = post_body;
        let envelope =
            match Envelope::decode_bytes_with_limit(&mut post_body, limits.max_message_size) {
                Ok(Some(env)) => env,
                Ok(None) => {
                    let err = ConnectError::invalid_argument("incomplete request envelope");
                    return streaming_error_response(&err, protocol, codec_format);
                }
                Err(e) => {
                    return streaming_error_response(&e, protocol, codec_format);
                }
            };

        // Check for multiple request envelopes (server streaming only allows one)
        if !post_body.is_empty() {
            let err = ConnectError::unimplemented(
                "server streaming request must have exactly one message",
            );
            return streaming_error_response(&err, protocol, codec_format);
        }

        // Decompress if needed
        if envelope.is_compressed() {
            // Compressed flag requires content-encoding header
            let encoding = match metadata.streaming_encoding.as_deref() {
                Some(enc) if enc != "identity" => enc,
                _ => {
                    let err = ConnectError::internal(format!(
                        "received compressed message without {} header",
                        protocol.content_encoding_header()
                    ));
                    return streaming_error_response(&err, protocol, codec_format);
                }
            };
            match compression.decompress_with_limit(
                encoding,
                envelope.data,
                limits.max_message_size,
            ) {
                Ok(data) => data,
                Err(e) => {
                    return streaming_error_response(&e, protocol, codec_format);
                }
            }
        } else {
            envelope.data
        }
    };

    // Create handler context with the request headers from metadata
    let ctx = RequestContext::new(metadata.headers)
        .with_deadline(deadline)
        .with_extensions(extensions)
        .with_spec(method_desc.and_then(|d| d.spec))
        .with_protocol(Some(protocol))
        .with_path(["/", path].concat())
        .with_decode_options(limits.decode_options());

    // Call the handler with the appropriate codec format.
    // For gRPC unary handlers, we wrap the single response in a one-item stream.
    let resp = match dispatch_kind {
        StreamingDispatchKind::ServerStreaming => {
            let fut = call_server_streaming_intercepted(
                dispatcher,
                interceptors,
                path,
                ctx,
                request_body,
                codec_format,
            );
            match with_request_deadline(deadline, fut).await {
                Ok(result) => result,
                Err(e) => return streaming_error_response(&e, protocol, codec_format),
            }
        }
        StreamingDispatchKind::Unary => {
            let fut = call_unary_intercepted(
                dispatcher,
                interceptors,
                path,
                ctx,
                request_body,
                codec_format,
            );
            match with_request_deadline(deadline, fut).await {
                // Wrap the single response in a one-item stream. Its
                // segments, if any, ride through to the framing layer.
                Ok(r) => r.map_body(|body| -> crate::EncodedStream {
                    Box::pin(futures::stream::once(async move { Ok(body) }))
                }),
                Err(e) => return streaming_error_response(&e, protocol, codec_format),
            }
        }
    };

    // Negotiate response compression for streaming
    let response_encoding = compression.negotiate_encoding(
        metadata.streaming_accept_encoding.as_deref(),
        metadata.streaming_encoding.as_deref(),
    );

    // Build streaming response
    let mut response = response_head(
        protocol.response_content_type(codec_format, true),
        protocol.content_encoding_header(),
        response_encoding,
        protocol.accept_encoding_header(),
        &compression,
    );

    // Add response headers set by the handler
    for (key, value) in resp.headers.iter() {
        response = response.header(key, value);
    }

    let stream_compression = response_encoding.map(|encoding| (compression, encoding));
    let effective_policy = compression_policy.with_override(resp.compress);
    // Time remaining in the absolute deadline budget at the point the
    // stream starts; the wrapper arms a `tokio::time::sleep` from this so
    // the deadline does not shift relative to request arrival.
    let remaining = deadline.map(|d| d.saturating_duration_since(std::time::Instant::now()));
    let resp_body = deadline_policy.enforce_on_response_stream(resp.body, remaining);
    let body = StreamingResponseBody::new(
        resp_body,
        resp.trailers,
        protocol,
        stream_compression,
        effective_policy,
    );

    response.body(body).unwrap_or_else(|_| {
        let err = ConnectError::internal("failed to build streaming response");
        streaming_error_response(&err, protocol, codec_format)
    })
}

/// Handle a client streaming ConnectRPC request.
///
/// Client streaming RPCs receive multiple envelope-framed request messages
/// and return a single envelope-framed response with END_STREAM.
///
/// The handler's request stream is built by [`decode_request_body`], which
/// also drains whatever the handler leaves unread.
#[allow(clippy::too_many_arguments)]
async fn handle_client_streaming_request<D, B>(
    dispatcher: &D,
    path: &str,
    spec: Option<crate::spec::Spec>,
    metadata: RequestMetadata,
    body: B,
    extensions: http::Extensions,
    protocol: Protocol,
    codec_format: CodecFormat,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
    interceptors: &[Arc<dyn Interceptor>],
) -> Response<StreamingResponseBody>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    let request_stream = decode_request_body(
        body,
        limits.max_message_size,
        metadata.streaming_encoding.clone(),
        Arc::clone(&compression),
    );

    let deadline = metadata
        .timeout
        .and_then(|t| std::time::Instant::now().checked_add(t));
    let ctx = RequestContext::new(metadata.headers)
        .with_deadline(deadline)
        .with_extensions(extensions)
        .with_spec(spec)
        .with_protocol(Some(protocol))
        .with_path(["/", path].concat())
        .with_decode_options(limits.decode_options());

    let handler_result = if let Some(timeout) = metadata.timeout {
        match tokio::time::timeout(
            timeout,
            call_client_streaming_intercepted(
                dispatcher,
                interceptors,
                path,
                ctx,
                request_stream,
                codec_format,
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let err = ConnectError::deadline_exceeded("request timeout");
                return streaming_error_response(&err, protocol, codec_format);
            }
        }
    } else {
        call_client_streaming_intercepted(
            dispatcher,
            interceptors,
            path,
            ctx,
            request_stream,
            codec_format,
        )
        .await
    };

    let resp = match handler_result {
        Ok(result) => result,
        Err(e) => {
            return streaming_error_response(&e, protocol, codec_format);
        }
    };

    // Negotiate response compression for streaming
    let response_encoding = compression.negotiate_encoding(
        metadata.streaming_accept_encoding.as_deref(),
        metadata.streaming_encoding.as_deref(),
    );

    // Build streaming response with a single data envelope + END_STREAM
    let mut response = response_head(
        protocol.response_content_type(codec_format, true),
        protocol.content_encoding_header(),
        response_encoding,
        protocol.accept_encoding_header(),
        &compression,
    );

    // Add response headers set by the handler
    for (key, value) in resp.headers.iter() {
        response = response.header(key, value);
    }

    let stream_compression = response_encoding.map(|encoding| (compression, encoding));
    let response_stream: crate::EncodedStream =
        Box::pin(futures::stream::once(async { Ok(resp.body) }));
    let effective_policy = compression_policy.with_override(resp.compress);
    let body = StreamingResponseBody::new(
        response_stream,
        resp.trailers,
        protocol,
        stream_compression,
        effective_policy,
    );

    response.body(body).unwrap_or_else(|_| {
        let err = ConnectError::internal("failed to build client streaming response");
        streaming_error_response(&err, protocol, codec_format)
    })
}

/// Maximum bytes to drain from the request body after the decoder finishes.
/// This prevents a malicious client from forcing the server to consume unbounded
/// data after a size-limit error, another decoder failure, the END_STREAM
/// envelope (after which any further body bytes are trailing garbage), or the
/// handler dropping its request stream.
const MAX_DRAIN_BYTES: usize = 1024 * 1024; // 1 MiB

/// How long a drain may take, however few bytes arrive. Bytes alone do not
/// bound the drain: a client that sends less than [`MAX_DRAIN_BYTES`] and then
/// stalls would otherwise hold the drain task and the request body for as
/// long as it keeps the connection open.
///
/// The deadline is absolute, not an idle timeout, so a client that trickles
/// data cannot extend it. A client still uploading when it passes loses the
/// stream (HTTP/2, reset with `NO_ERROR`) or the connection (HTTP/1.x). If the
/// response outlives the drain, as in a bidi call whose handler stopped reading
/// requests, the stream is not reset and the body is dropped anyway; see
/// [`request_body_drain`] for what that costs an HTTP/2 connection.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// A timer for one drain. `wasm32-unknown-unknown` has no clock, so a drain
/// there is bounded by [`MAX_DRAIN_BYTES`] alone.
fn drain_timeout() -> impl Future<Output = ()> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        tokio::time::sleep(DRAIN_TIMEOUT)
    }
    #[cfg(target_arch = "wasm32")]
    {
        std::future::pending()
    }
}

/// Turn an envelope-framed request body into the stream of decoded messages
/// that a client-streaming or bidi handler consumes.
///
/// Decoding happens in whichever task polls the stream, normally the
/// handler's, so a message passes through no extra task or channel between
/// hyper and the handler, and the body is read only while the stream is
/// polled. The stream ends at the end of the body or at the END_STREAM
/// envelope. A decode error, a body that ends part-way through an envelope,
/// or a transport-level body error is yielded as one `Err`, after which the
/// stream ends.
///
/// Whatever the handler does not read is still consumed. When the stream
/// finishes decoding (END_STREAM or a decode error), or is dropped before the
/// body has ended (the handler returned, an interceptor rejected the call, or
/// the request timeout fired), it drops the decoder, with any partial message
/// it holds, and drains the rest of the body (see [`request_body_drain`]) on
/// a detached task. The task runs on the runtime the stream was created on,
/// normally the server's, wherever the stream is dropped.
fn decode_request_body<B>(
    body: B,
    max_message_size: usize,
    streaming_encoding: Option<String>,
    compression: Arc<CompressionRegistry>,
) -> BoxStream<Result<Bytes, ConnectError>>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    Box::pin(RequestBodyStream {
        decoding: Some(DecodeState {
            body: Box::pin(body),
            decoder: EnvelopeDecoder::new(max_message_size, streaming_encoding, compression),
            frame: Bytes::new(),
        }),
        #[cfg(not(target_arch = "wasm32"))]
        runtime: tokio::runtime::Handle::try_current().ok(),
    })
}

/// The stream behind [`decode_request_body`].
struct RequestBodyStream<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    /// `None` once the stream has ended.
    decoding: Option<DecodeState<B>>,
    /// Where the drain runs. `None` if the stream was created outside a Tokio
    /// runtime; the drain then runs on the runtime the stream is dropped in,
    /// and is skipped outside one.
    #[cfg(not(target_arch = "wasm32"))]
    runtime: Option<tokio::runtime::Handle>,
}

/// What a [`RequestBodyStream`] holds until it ends.
struct DecodeState<B> {
    body: Pin<Box<B>>,
    decoder: EnvelopeDecoder,
    /// The part of the last body frame the decoder has not consumed yet.
    frame: Bytes,
}

impl<B> RequestBodyStream<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    /// End the stream and drain the rest of the body. `end_stream` marks the
    /// END_STREAM case, after which any further request data is a protocol
    /// violation by the client.
    fn finish(&mut self, end_stream: bool) {
        let Some(DecodeState { body, frame, .. }) = self.decoding.take() else {
            return;
        };
        let Some(drain) = request_body_drain(body, frame.len(), end_stream) else {
            return;
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(runtime) = &self.runtime {
            drop(runtime.spawn(drain));
            return;
        }
        crate::spawn_detached(drain);
    }
}

impl<B> Drop for RequestBodyStream<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    fn drop(&mut self) {
        // The handler let go of the stream before it ended.
        self.finish(false);
    }
}

impl<B> Stream for RequestBodyStream<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    type Item = Result<Bytes, ConnectError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let Some(state) = this.decoding.as_mut() else {
                return Poll::Ready(None);
            };
            match state.decoder.decode(&mut state.frame) {
                Ok(Some(Decoded::Message(data))) => return Poll::Ready(Some(Ok(data))),
                Ok(Some(Decoded::EndStream)) => {
                    this.finish(true);
                    return Poll::Ready(None);
                }
                Ok(None) => {} // the frame is used up
                Err(e) => {
                    this.finish(false);
                    return Poll::Ready(Some(Err(e)));
                }
            }
            match std::task::ready!(state.body.as_mut().poll_frame(cx)) {
                Some(Ok(frame)) => {
                    debug_assert!(state.frame.is_empty(), "undecoded bytes overwritten");
                    // Trailers carry nothing for the decoder.
                    if let Ok(data) = frame.into_data() {
                        state.frame = data;
                    }
                }
                Some(Err(e)) => {
                    // The body is over, so there is nothing to drain. The code
                    // is `internal` deliberately, for parity with the unary
                    // body-read path (`collect_body_limited`): the same
                    // transport failure reports the same code regardless of
                    // RPC shape. connect-go reports `unknown` here; if the
                    // attribution is ever revisited (a broken inbound
                    // transport is arguably `unavailable`), change both paths
                    // together.
                    tracing::debug!(error = %e, "request body error, ending request stream");
                    this.decoding = None;
                    return Poll::Ready(Some(Err(ConnectError::internal(format!(
                        "failed to read request body: {e}"
                    )))));
                }
                None => {
                    // A client may end the body without an END_STREAM
                    // envelope, but not part-way through an envelope.
                    let finished = state.decoder.finish();
                    this.decoding = None;
                    return Poll::Ready(finished.err().map(Err));
                }
            }
        }
    }
}

/// The drain of what a request stream left of its body: a future that reads
/// and discards the rest of the body, then drops it. The drain stops when the
/// body ends or fails, when more than [`MAX_DRAIN_BYTES`] have been discarded
/// (counting `discarded`, the bytes of the last frame the decoder left
/// behind), or when [`DRAIN_TIMEOUT`] has passed. `None`, with the body
/// dropped at once, if the body has already ended or `discarded` alone
/// exceeds the limit. `warn_trailing` logs a warning for the first request
/// data seen after the END_STREAM envelope.
///
/// The drain matters on HTTP/1.1, where the server must read the request body
/// before the connection can serve another request, and dropping an unread
/// body makes hyper close the connection. On HTTP/2 dropping the body resets
/// the stream once the response is done, so the stream's slot is held until
/// the drain ends. The drain is not only a delay: h2 charges each small DATA
/// frame it receives against connection-wide budgets, refunded only when the
/// frame is read or discarded, and answers with `GOAWAY(ENHANCE_YOUR_CALM)`
/// when they run out. Dropping the body at once therefore takes the connection
/// down once enough calls end early while their clients are still sending (see
/// `http2_early_return_keeps_connection_alive_under_small_frames`). The same
/// applies to frames that arrive while the stream is still open and nothing
/// reads them: while a bidi handler holds a request stream it no longer
/// polls, or after the drain has ended, if the handler keeps streaming its
/// response for longer than [`DRAIN_TIMEOUT`].
fn request_body_drain<B>(
    mut body: Pin<Box<B>>,
    discarded: usize,
    warn_trailing: bool,
) -> Option<impl Future<Output = ()> + Send + 'static>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    let mut drained = Drained {
        bytes: 0,
        warn_trailing,
    };
    if discarded > 0 && drained.add(discarded).is_break() {
        return None;
    }
    if body.is_end_stream() {
        return None;
    }
    Some(async move {
        // One timer for the whole drain, so trickled data cannot extend it.
        let mut timeout = std::pin::pin!(drain_timeout());
        loop {
            let frame = tokio::select! {
                // The timer first, so a body that always has data ready
                // cannot postpone it.
                biased;
                () = &mut timeout => {
                    tracing::debug!("body drain timed out, stopping");
                    return;
                }
                frame = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)) => frame,
            };
            let data = match frame {
                // Trailers discard nothing, like an empty data frame.
                Some(Ok(frame)) => frame.into_data().unwrap_or_default(),
                Some(Err(e)) => {
                    tracing::debug!(error = %e, "request body error while draining");
                    return;
                }
                None => return,
            };
            if !data.is_empty() && drained.add(data.len()).is_break() {
                return;
            }
        }
    })
}

/// What one drain has discarded so far.
struct Drained {
    bytes: usize,
    /// The next data is the first the client sent after END_STREAM.
    warn_trailing: bool,
}

impl Drained {
    /// Count `len` more discarded bytes: [`ControlFlow::Break`] once past
    /// [`MAX_DRAIN_BYTES`].
    fn add(&mut self, len: usize) -> ControlFlow<()> {
        if self.warn_trailing {
            tracing::warn!(
                trailing_bytes = len,
                "client sent request data after the END_STREAM envelope; discarding"
            );
            self.warn_trailing = false;
        }
        self.bytes = self.bytes.saturating_add(len);
        if self.bytes > MAX_DRAIN_BYTES {
            tracing::debug!(
                drained_bytes = self.bytes,
                "body drain limit reached, stopping"
            );
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// Handle a bidi streaming ConnectRPC request.
///
/// Bidi streaming RPCs receive multiple envelope-framed request messages
/// and return multiple envelope-framed response messages with END_STREAM.
///
/// The handler's request stream is built by [`decode_request_body`], which
/// also drains whatever the handler leaves unread.
#[allow(clippy::too_many_arguments)]
async fn handle_bidi_streaming_request<D, B>(
    dispatcher: &D,
    path: &str,
    spec: Option<crate::spec::Spec>,
    metadata: RequestMetadata,
    body: B,
    extensions: http::Extensions,
    protocol: Protocol,
    codec_format: CodecFormat,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
    deadline_policy: &DeadlinePolicy,
    interceptors: &[Arc<dyn Interceptor>],
) -> Response<StreamingResponseBody>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    let request_stream = decode_request_body(
        body,
        limits.max_message_size,
        metadata.streaming_encoding.clone(),
        Arc::clone(&compression),
    );

    // Create handler context
    let deadline = metadata
        .timeout
        .and_then(|t| std::time::Instant::now().checked_add(t));
    let ctx = RequestContext::new(metadata.headers)
        .with_deadline(deadline)
        .with_extensions(extensions)
        .with_spec(spec)
        .with_protocol(Some(protocol))
        .with_path(["/", path].concat())
        .with_decode_options(limits.decode_options());

    // Call the handler with timeout if configured
    let handler_result = if let Some(timeout) = metadata.timeout {
        match tokio::time::timeout(
            timeout,
            call_bidi_streaming_intercepted(
                dispatcher,
                interceptors,
                path,
                ctx,
                request_stream,
                codec_format,
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let err = ConnectError::deadline_exceeded("request timeout");
                return streaming_error_response(&err, protocol, codec_format);
            }
        }
    } else {
        call_bidi_streaming_intercepted(
            dispatcher,
            interceptors,
            path,
            ctx,
            request_stream,
            codec_format,
        )
        .await
    };

    let resp = match handler_result {
        Ok(result) => result,
        Err(e) => {
            return streaming_error_response(&e, protocol, codec_format);
        }
    };

    // Negotiate response compression for streaming
    let response_encoding = compression.negotiate_encoding(
        metadata.streaming_accept_encoding.as_deref(),
        metadata.streaming_encoding.as_deref(),
    );

    // Build streaming response
    let mut response = response_head(
        protocol.response_content_type(codec_format, true),
        protocol.content_encoding_header(),
        response_encoding,
        protocol.accept_encoding_header(),
        &compression,
    );

    // Add response headers set by the handler
    for (key, value) in resp.headers.iter() {
        response = response.header(key, value);
    }

    let stream_compression = response_encoding.map(|encoding| (compression, encoding));
    let effective_policy = compression_policy.with_override(resp.compress);
    // Time remaining in the absolute deadline budget at the point the
    // stream starts; the wrapper arms a `tokio::time::sleep` from this so
    // the deadline does not shift relative to request arrival.
    let remaining = deadline.map(|d| d.saturating_duration_since(std::time::Instant::now()));
    let resp_body = deadline_policy.enforce_on_response_stream(resp.body, remaining);
    let body = StreamingResponseBody::new(
        resp_body,
        resp.trailers,
        protocol,
        stream_compression,
        effective_policy,
    );

    response.body(body).unwrap_or_else(|_| {
        let err = ConnectError::internal("failed to build bidi streaming response");
        streaming_error_response(&err, protocol, codec_format)
    })
}

/// Add trailers to a response builder using the Connect protocol's trailer- prefix convention.
fn add_trailers(
    mut response: http::response::Builder,
    trailers: &http::HeaderMap,
) -> http::response::Builder {
    for (key, value) in trailers.iter() {
        let trailer_key = format!("trailer-{}", key.as_str());
        response = response.header(trailer_key, value);
    }
    response
}

// ============================================================================
// gRPC Trailer Helpers
// ============================================================================

// Re-export pre-parsed gRPC header name statics from protocol::hdr so the
// response-building paths below don't re-parse the names on every request.
use crate::protocol::hdr::GRPC_MESSAGE;
use crate::protocol::hdr::GRPC_STATUS;
use crate::protocol::hdr::GRPC_STATUS_DETAILS_BIN;

/// Build an HTTP/2 trailers `HeaderMap` for a gRPC response.
///
/// For success: `grpc-status: 0`
/// For errors: `grpc-status: <code>`, `grpc-message: <percent-encoded message>`
///
/// Custom trailing metadata from the handler context is also included.
fn build_grpc_trailers(
    error: Option<&ConnectError>,
    custom_trailers: &http::HeaderMap,
) -> http::HeaderMap {
    let mut trailers = http::HeaderMap::new();

    match error {
        Some(err) => {
            trailers.insert(&GRPC_STATUS, http::HeaderValue::from(err.code.grpc_code()));
            if let Some(val) = err
                .message
                .as_deref()
                .and_then(|m| http::HeaderValue::from_str(&grpc_percent_encode(m)).ok())
            {
                trailers.insert(&GRPC_MESSAGE, val);
            }
            // Encode error details as grpc-status-details-bin (base64-encoded
            // google.rpc.Status protobuf containing the error details)
            {
                use base64::Engine;
                let status_bytes = crate::grpc_status::encode(err);
                let b64 = base64::engine::general_purpose::STANDARD_NO_PAD.encode(&status_bytes);
                if let Ok(val) = http::HeaderValue::from_str(&b64) {
                    trailers.insert(&GRPC_STATUS_DETAILS_BIN, val);
                }
            }
            // Include error-level trailers
            for (key, value) in err.trailers() {
                trailers.append(key, value.clone());
            }
        }
        None => {
            trailers.insert(&GRPC_STATUS, http::HeaderValue::from_static("0"));
        }
    }

    // Include custom trailing metadata from the handler context.
    // If the error already carries its own trailers (via .with_trailers()),
    // skip adding context trailers to avoid duplication.
    let error_has_own_trailers = error.is_some_and(|e| !e.trailers().is_empty());
    if !error_has_own_trailers {
        for (key, value) in custom_trailers.iter() {
            trailers.append(key, value.clone());
        }
    }

    trailers
}

/// Percent-encode a gRPC error message per the gRPC spec.
///
/// The gRPC spec requires that `grpc-message` values are percent-encoded,
/// specifically encoding characters outside the printable ASCII range and
/// the `%` character itself.
/// Percent-encoding set for gRPC `grpc-message` trailer values.
/// Encodes control characters (0x00-0x1F, 0x7F), `%` (0x25), and non-ASCII.
/// All other printable ASCII (0x20-0x7E except `%`) passes through.
const GRPC_MESSAGE_ENCODE_SET: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS.add(b'%');

fn grpc_percent_encode(message: &str) -> String {
    percent_encoding::utf8_percent_encode(message, GRPC_MESSAGE_ENCODE_SET).to_string()
}

/// Build an error response with ConnectRpcBody body type.
fn error_response_either(err: ConnectError) -> Response<ConnectRpcBody> {
    error_response(err).map(ConnectRpcBody::Full)
}

/// Build an error response.
fn error_response(err: ConnectError) -> Response<Full<Bytes>> {
    let status = err.http_status();
    let body = err.to_json();

    let response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type::JSON);

    // Add response headers from the error
    let response = echo_error_headers(response, &err);

    // Add trailers as trailer- prefixed headers
    let response = add_trailers(response, err.trailers());

    response.body(Full::new(body)).unwrap_or_else(|_| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Full::new(Bytes::new()))
            .unwrap()
    })
}

impl ConnectError {
    /// Render this error as a complete HTTP response in the wire format
    /// matching the inbound request's protocol.
    ///
    /// This is the building block for `tower::Layer`s that short-circuit a
    /// request (auth, rate limiting, validation) before it reaches
    /// [`ConnectRpcService`]. A layer cannot reuse the service's internal error
    /// rendering, but still needs to produce a response that the calling
    /// client will decode as a structured error rather than a transport
    /// failure. Each protocol expects a different wire shape:
    ///
    /// - **Connect unary** (`application/proto`, `application/json`, or absent
    ///   — Connect GET requests carry no request body or `Content-Type`):
    ///   a non-200 HTTP status with a JSON error body. Connect unary error
    ///   bodies are spec-required JSON regardless of the request codec.
    /// - **Connect streaming** (`application/connect+{proto,json}`): HTTP 200
    ///   with the error in an `EndStreamResponse` envelope.
    /// - **gRPC / gRPC-Web** (`application/grpc*`): HTTP 200 with `grpc-status`
    ///   and `grpc-message` trailers (as HTTP/2 trailers for gRPC, encoded in
    ///   the body for gRPC-Web).
    ///
    /// The protocol is detected from `request_headers` via
    /// [`Protocol::detect`]. When detection fails — an unrecognized
    /// `Content-Type`, or none at all — the Connect unary JSON shape is used,
    /// which is the most universally parseable fallback.
    ///
    /// gRPC and gRPC-Web use the same framed wire shape for unary and
    /// streaming calls, so the trailers-only response is always correct
    /// regardless of the method's cardinality.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use connectrpc::ConnectError;
    ///
    /// // Inside a tower::Service::call wrapping ConnectRpcService:
    /// fn call(&mut self, req: http::Request<B>) -> Self::Future {
    ///     if !self.is_authorized(&req) {
    ///         let resp = ConnectError::permission_denied("access denied")
    ///             .into_http_response(req.headers());
    ///         return Box::pin(std::future::ready(Ok(resp)));
    ///     }
    ///     // ... call inner service ...
    /// }
    /// ```
    #[must_use]
    pub fn into_http_response(self, request_headers: &http::HeaderMap) -> Response<ConnectRpcBody> {
        match Protocol::detect(request_headers) {
            Some(rp) if rp.is_streaming => {
                streaming_error_response(&self, rp.protocol, rp.codec_format)
                    .map(ConnectRpcBody::Streaming)
            }
            // Connect unary, or unknown/absent Content-Type: a non-200 HTTP
            // status with a JSON body is the only universally parseable shape.
            _ => error_response_either(self),
        }
    }
}

// ============================================================================
// Axum Integration
// ============================================================================

/// Axum router integration for ConnectRPC.
///
/// Available when the `axum` feature is enabled.
#[cfg(feature = "axum")]
#[cfg_attr(docsrs, doc(cfg(feature = "axum")))]
pub mod axum_integration {
    use super::*;
    use axum::body::Body;
    use axum::response::IntoResponse;

    impl Router {
        /// Convert this ConnectRPC router into an axum Router.
        ///
        /// The returned router handles all ConnectRPC paths registered with this router.
        /// It can be merged with other axum routes or used as a fallback.
        ///
        /// # Example
        ///
        /// ```rust,ignore
        /// use axum::{Router, routing::get};
        /// use connectrpc::Router as ConnectRouter;
        /// use std::sync::Arc;
        ///
        /// async fn health() -> &'static str {
        ///     "OK"
        /// }
        ///
        /// let connect_router =
        ///     Arc::new(MyGreetService).register(ConnectRouter::new());
        ///
        /// let app = Router::new()
        ///     .route("/health", get(health))
        ///     .fallback_service(connect_router.into_axum_service());
        /// ```
        pub fn into_axum_service(self) -> ConnectRpcService {
            ConnectRpcService::new(self)
        }

        /// Create an axum Router with the ConnectRPC handlers.
        ///
        /// This creates a catch-all route that handles all paths. Use this
        /// with `Router::merge()` or `Router::fallback()`.
        ///
        /// # Example
        ///
        /// ```rust,ignore
        /// use axum::{Router, routing::get};
        /// use connectrpc::Router as ConnectRouter;
        /// use std::sync::Arc;
        ///
        /// let connect_router =
        ///     Arc::new(MyGreetService).register(ConnectRouter::new());
        ///
        /// // Use as fallback for unmatched routes
        /// let app = Router::new()
        ///     .route("/health", get(health))
        ///     .fallback_service(connect_router.into_axum_service());
        /// ```
        pub fn into_axum_router(self) -> axum::Router {
            let service = ConnectRpcService::new(self);
            axum::Router::new().fallback_service(service)
        }
    }

    impl IntoResponse for ConnectError {
        fn into_response(self) -> axum::response::Response {
            let status = self.http_status();
            let body = self.to_json();

            let mut response = axum::response::Response::new(Body::from(body));
            *response.status_mut() = status;
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                http::HeaderValue::from_static(content_type::JSON),
            );
            response
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Every limit must survive the builder chain and come back out of the
    /// accessor of the same name. Setting all three in one chain also pins
    /// that no setter clobbers a sibling.
    #[test]
    fn limits_builders_round_trip_through_the_accessors() {
        let limits = Limits::default()
            .with_max_request_body_size(16 * 1024 * 1024)
            .with_max_message_size(8 * 1024 * 1024)
            .with_element_memory_limit(64 * 1024 * 1024);

        assert_eq!(limits.max_request_body_size(), 16 * 1024 * 1024);
        assert_eq!(limits.max_message_size(), 8 * 1024 * 1024);
        assert_eq!(limits.element_memory_limit(), 64 * 1024 * 1024);

        // `decode_options` reads the element budget, not one of its siblings.
        assert_eq!(
            limits.decode_options().element_memory_limit(),
            64 * 1024 * 1024
        );
    }

    /// The defaults an untouched `Limits` reports must be the documented
    /// constants, so a caller who reads one back before setting it is not
    /// misled about what the server is enforcing.
    #[test]
    fn default_limits_report_the_documented_defaults() {
        let limits = Limits::default();
        assert_eq!(
            limits.max_request_body_size(),
            DEFAULT_MAX_REQUEST_BODY_SIZE
        );
        assert_eq!(limits.max_message_size(), DEFAULT_MAX_MESSAGE_SIZE);
        assert_eq!(
            limits.element_memory_limit(),
            buffa::DEFAULT_ELEMENT_MEMORY_LIMIT
        );
    }

    /// A payload at or above `envelope::MIN_CHAIN_SIZE` must reach the body
    /// frames by refcount, not by copy: the emitted data frame references
    /// the handler's original allocation. Small envelopes (the 5-byte
    /// header) still batch into the framing buffer.
    #[tokio::test]
    async fn streaming_large_payload_is_chained_not_copied() {
        use futures::StreamExt as _;

        let payload = Bytes::from(vec![0x42u8; crate::envelope::MIN_CHAIN_SIZE]);
        let original_ptr = payload.as_ptr();
        let source = futures::stream::iter([Ok::<_, ConnectError>(payload.clone().into())]).boxed();
        let mut stream = std::pin::pin!(create_grpc_envelope_stream(
            source,
            http::HeaderMap::new(),
            None,
            CompressionPolicy::disabled(),
        ));

        // Frame 1: the 5-byte envelope header (batched buffer flush).
        let head = stream.next().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(head.len(), crate::envelope::HEADER_SIZE);
        assert_eq!(head[0], crate::envelope::flags::DATA);
        assert_eq!(
            u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize,
            payload.len()
        );

        // Frame 2: the payload, by refcount — same allocation, no copy.
        let data = stream.next().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data.len(), payload.len());
        assert!(
            std::ptr::eq(data.as_ptr(), original_ptr),
            "payload frame must reference the original allocation"
        );

        // Frame 3: gRPC trailers.
        let trailers = stream.next().await.unwrap().unwrap();
        assert!(trailers.is_trailers());
        assert!(stream.next().await.is_none());
    }

    /// Payloads below the chaining threshold are emitted as one
    /// contiguous data frame containing header + payload.
    #[tokio::test]
    async fn streaming_small_payload_stays_contiguous() {
        use futures::StreamExt as _;

        let payload = Bytes::from_static(b"small");
        let source = futures::stream::iter([Ok::<_, ConnectError>(payload.clone().into())]).boxed();
        let mut stream = std::pin::pin!(create_grpc_envelope_stream(
            source,
            http::HeaderMap::new(),
            None,
            CompressionPolicy::disabled(),
        ));

        let frame = stream.next().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(frame.len(), crate::envelope::HEADER_SIZE + payload.len());
        assert_eq!(&frame[crate::envelope::HEADER_SIZE..], &payload[..]);

        let trailers = stream.next().await.unwrap().unwrap();
        assert!(trailers.is_trailers());
        assert!(stream.next().await.is_none());
    }

    /// The chained split must be invisible to an envelope decoder: bytes
    /// reassembled from the frames decode identically to the contiguous path.
    #[tokio::test]
    async fn streaming_chained_frames_reassemble_to_same_wire_bytes() {
        use futures::StreamExt as _;

        let large = Bytes::from(vec![0x5Au8; crate::envelope::MIN_CHAIN_SIZE + 7]);
        let small = Bytes::from_static(b"tail");
        let items = [
            Ok::<_, ConnectError>(small.clone()),
            Ok(large.clone()),
            Ok(small.clone()),
        ];
        let source = futures::stream::iter(items)
            .map(|r| r.map(Into::into))
            .boxed();
        let mut stream = std::pin::pin!(create_envelope_stream(
            source,
            http::HeaderMap::new(),
            None,
            CompressionPolicy::disabled(),
        ));

        let mut wire = bytes::BytesMut::new();
        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap();
            if let Ok(data) = frame.into_data() {
                wire.extend_from_slice(&data);
            }
        }

        // Decode all envelopes back out and compare to the source messages.
        let mut decoded = Vec::new();
        while let Some(env) = Envelope::decode(&mut wire).unwrap() {
            decoded.push(env);
        }
        assert_eq!(decoded.len(), 4, "3 data envelopes + 1 end-stream");
        assert_eq!(decoded[0].data, small);
        assert_eq!(decoded[1].data, large);
        assert_eq!(decoded[2].data, small);
        assert!(decoded[3].is_end_stream());
    }

    /// A source error right after a chained payload must still emit the
    /// staged payload before the error finalizer: header frame, payload
    /// frame, then trailers.
    #[tokio::test]
    async fn streaming_error_after_chained_payload_preserves_order() {
        use futures::StreamExt as _;

        let large = Bytes::from(vec![0x77u8; crate::envelope::MIN_CHAIN_SIZE]);
        let items = [
            Ok::<_, ConnectError>(large.clone()),
            Err(ConnectError::internal("boom")),
        ];
        let source = futures::stream::iter(items)
            .map(|r| r.map(Into::into))
            .boxed();
        let mut stream = std::pin::pin!(create_grpc_envelope_stream(
            source,
            http::HeaderMap::new(),
            None,
            CompressionPolicy::disabled(),
        ));

        let head = stream.next().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(head.len(), crate::envelope::HEADER_SIZE);
        let payload = stream.next().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(payload, large);
        let trailers = stream.next().await.unwrap().unwrap();
        let map = trailers.into_trailers().unwrap();
        assert_eq!(map.get("grpc-status").unwrap(), "13", "internal = 13");
        assert!(stream.next().await.is_none());
    }

    /// An item the encoder split into segments keeps its large segments as
    /// their own frames, by refcount, while the tag/length fragments between
    /// them ride in the batch buffer: header+lead, large A, fragment,
    /// large B, then the next (small) envelope with the trailing fragment
    /// ahead of it, then trailers. Reassembled, the bytes decode to the same
    /// envelopes a contiguous encode would have produced.
    #[tokio::test]
    async fn streaming_segmented_item_chains_each_large_segment() {
        use crate::response::EncodedBody;
        use futures::StreamExt as _;

        let min = crate::envelope::MIN_CHAIN_SIZE;
        let lead = Bytes::from_static(b"tag");
        let a = Bytes::from(vec![0xA1u8; min]);
        let mid = Bytes::from_static(b"ln");
        let b = Bytes::from(vec![0xB2u8; min + 1]);
        let tail = Bytes::from_static(b"t");
        let segmented = EncodedBody::Segmented(vec![
            lead.clone(),
            a.clone(),
            mid.clone(),
            b.clone(),
            tail.clone(),
        ]);
        let first_message = segmented.clone().into_contiguous();
        let small = Bytes::from_static(b"next");
        let items = [
            Ok::<_, ConnectError>(segmented),
            Ok(EncodedBody::Contiguous(small.clone())),
        ];
        let mut stream = std::pin::pin!(create_grpc_envelope_stream(
            futures::stream::iter(items).boxed(),
            http::HeaderMap::new(),
            None,
            CompressionPolicy::disabled(),
        ));

        let mut frames = Vec::new();
        let mut terminal = None;
        while let Some(frame) = stream.next().await {
            match frame.unwrap().into_data() {
                Ok(data) => frames.push(data),
                Err(frame) => terminal = Some(frame),
            }
        }

        let hdr = crate::envelope::HEADER_SIZE;
        let lens: Vec<usize> = frames.iter().map(Bytes::len).collect();
        assert_eq!(
            lens,
            [
                hdr + lead.len(),
                a.len(),
                mid.len(),
                b.len(),
                tail.len() + hdr + small.len()
            ]
        );
        assert!(
            std::ptr::eq(frames[1].as_ptr(), a.as_ptr()),
            "A by refcount"
        );
        assert!(
            std::ptr::eq(frames[3].as_ptr(), b.as_ptr()),
            "B by refcount"
        );
        assert!(terminal.expect("trailers frame").is_trailers());

        let mut wire = bytes::BytesMut::new();
        for frame in &frames {
            wire.extend_from_slice(frame);
        }
        let first = Envelope::decode(&mut wire).unwrap().unwrap();
        let second = Envelope::decode(&mut wire).unwrap().unwrap();
        assert_eq!(first.data, first_message);
        assert_eq!(second.data, small);
        assert!(wire.is_empty());
    }

    /// With an async producer the trailing fragment of a segmented item must
    /// be flushed when the source goes `Pending`, so the peer can decode the
    /// message before the next item exists: header+lead, large, tail, then
    /// `Pending`.
    #[tokio::test]
    async fn streaming_segmented_tail_flushes_before_pending() {
        use crate::response::EncodedBody;
        use futures::StreamExt as _;

        let min = crate::envelope::MIN_CHAIN_SIZE;
        let lead = Bytes::from_static(b"tag");
        let big = Bytes::from(vec![9u8; min]);
        let tail = Bytes::from_static(b"end");
        let item = EncodedBody::Segmented(vec![lead.clone(), big.clone(), tail.clone()]);
        let source = futures::stream::iter([Ok::<_, ConnectError>(item)])
            .chain(futures::stream::pending())
            .boxed();
        let mut stream = Box::pin(create_envelope_stream(
            source,
            http::HeaderMap::new(),
            None,
            CompressionPolicy::disabled(),
        ));

        let hdr = crate::envelope::HEADER_SIZE;
        let f1 = stream.next().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(f1.len(), hdr + lead.len());
        let f2 = stream.next().await.unwrap().unwrap().into_data().unwrap();
        assert!(std::ptr::eq(f2.as_ptr(), big.as_ptr()));
        let f3 = stream.next().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(f3, tail, "tail must not wait for the next item");
        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    }

    /// A source error right after a segmented item still emits every staged
    /// segment, in order, before the error finalizer.
    #[tokio::test]
    async fn streaming_error_after_segmented_item_preserves_order() {
        use crate::response::EncodedBody;
        use futures::StreamExt as _;

        let min = crate::envelope::MIN_CHAIN_SIZE;
        let a = Bytes::from(vec![1u8; min]);
        let b = Bytes::from(vec![2u8; min]);
        let items = [
            Ok::<_, ConnectError>(EncodedBody::Segmented(vec![a.clone(), b.clone()])),
            Err(ConnectError::internal("boom")),
        ];
        let mut stream = std::pin::pin!(create_grpc_envelope_stream(
            futures::stream::iter(items).boxed(),
            http::HeaderMap::new(),
            None,
            CompressionPolicy::disabled(),
        ));

        let head = stream.next().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(head.len(), crate::envelope::HEADER_SIZE);
        assert_eq!(
            stream.next().await.unwrap().unwrap().into_data().unwrap(),
            a
        );
        assert_eq!(
            stream.next().await.unwrap().unwrap().into_data().unwrap(),
            b
        );
        let trailers = stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_trailers()
            .unwrap();
        assert_eq!(trailers.get("grpc-status").unwrap(), "13", "internal = 13");
        assert!(stream.next().await.is_none());
    }

    /// GrpcUnaryBody with a chained payload yields header, payload, then
    /// trailers, in that order; the payload frame is the original
    /// allocation.
    #[tokio::test]
    async fn grpc_unary_body_chained_frame_order() {
        use http_body_util::BodyExt as _;

        let payload = Bytes::from(vec![0x33u8; crate::envelope::MIN_CHAIN_SIZE]);
        let ptr = payload.as_ptr();
        let (head, chained) = Envelope::encode_body_parts(
            crate::envelope::flags::DATA,
            payload.clone().into(),
            crate::envelope::MIN_CHAIN_SIZE,
        );
        let mut body = GrpcUnaryBody {
            data: Some(head.clone()),
            payload: chained.into(),
            trailers: Some(GrpcUnaryTrailers::Http2(http::HeaderMap::new())),
        };

        let f1 = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(f1, head);
        assert_eq!(f1.len(), crate::envelope::HEADER_SIZE);
        let f2 = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert!(std::ptr::eq(f2.as_ptr(), ptr), "payload must not be copied");
        let f3 = body.frame().await.unwrap().unwrap();
        assert!(f3.is_trailers());
        assert!(body.frame().await.is_none());
    }

    #[test]
    fn test_service_creation() {
        let router = Router::new();
        let _service = ConnectRpcService::new(router);
    }

    #[test]
    fn test_service_clone() {
        let router = Router::new();
        let service = ConnectRpcService::new(router);
        let _cloned = service.clone();
    }

    // ========================================================================
    // collect_body_limited tests
    // ========================================================================

    #[tokio::test]
    async fn test_collect_body_limited_under_limit() {
        let body = Full::new(Bytes::from_static(b"hello"));
        let result = collect_body_limited(body, 1024).await.unwrap();
        assert_eq!(&result[..], b"hello");
    }

    #[tokio::test]
    async fn test_collect_body_limited_exact_limit() {
        // Limited uses strict > comparison — body exactly at limit succeeds.
        let body = Full::new(Bytes::from_static(b"hello"));
        let result = collect_body_limited(body, 5).await.unwrap();
        assert_eq!(&result[..], b"hello");
    }

    #[tokio::test]
    async fn test_collect_body_limited_over_limit() {
        let body = Full::new(Bytes::from_static(b"hello world"));
        let err = collect_body_limited(body, 5).await.unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::ResourceExhausted);
        assert!(err.message.as_deref().unwrap().contains("limit 5"));
    }

    #[tokio::test(start_paused = true)]
    async fn test_unary_deadline_bounds_stalled_body_collection() {
        let router = Router::new();
        let body = http_body_util::StreamBody::new(futures::stream::pending::<
            Result<Frame<Bytes>, std::io::Error>,
        >());
        let req = Request::builder()
            .method(Method::POST)
            .uri("/svc/Method")
            .header(header::CONTENT_TYPE, "application/proto")
            .body(body)
            .unwrap();
        let deadline_policy = DeadlinePolicy::new().with_default_timeout(Duration::from_millis(1));

        let err = handle_unary_request(
            &router,
            "svc/Method",
            router.lookup("svc/Method"),
            req,
            Limits::default(),
            Arc::new(CompressionRegistry::new()),
            &CompressionPolicy::default(),
            &deadline_policy,
            &[],
        )
        .await
        .expect_err("stalled body must exceed the request deadline");
        assert_eq!(err.code, crate::error::ErrorCode::DeadlineExceeded);
    }

    #[tokio::test(start_paused = true)]
    async fn test_server_streaming_deadline_bounds_stalled_body_collection() {
        let router = Router::new();
        let body = http_body_util::StreamBody::new(futures::stream::pending::<
            Result<Frame<Bytes>, std::io::Error>,
        >());
        let req = Request::builder()
            .method(Method::POST)
            .uri("/svc/Method")
            .body(body)
            .unwrap();
        let deadline_policy = DeadlinePolicy::new().with_default_timeout(Duration::from_millis(1));

        let resp = handle_streaming_request(
            &router,
            "svc/Method",
            router.lookup("svc/Method"),
            req,
            Protocol::Grpc,
            CodecFormat::Proto,
            Limits::default(),
            Arc::new(CompressionRegistry::new()),
            &CompressionPolicy::default(),
            &deadline_policy,
            &[],
        )
        .await;
        assert_eq!(
            resp.headers().get(&GRPC_STATUS).unwrap(),
            &crate::ErrorCode::DeadlineExceeded.grpc_code().to_string()
        );
    }

    #[test]
    fn test_parse_get_query_params_basic() {
        let params = parse_get_query_params(Some("message=%7B%7D&encoding=json&connect=v1"))
            .expect("should parse");
        assert_eq!(params.message, Some("%7B%7D".to_string()));
        assert_eq!(params.encoding, Some("json".to_string()));
        assert_eq!(params.connect_version, Some("v1".to_string()));
        assert!(!params.base64);
        assert!(params.compression.is_none());
    }

    #[test]
    fn test_parse_get_query_params_with_base64() {
        let params = parse_get_query_params(Some("message=e30&encoding=proto&base64=1&connect=v1"))
            .expect("should parse");
        assert_eq!(params.message, Some("e30".to_string()));
        assert_eq!(params.encoding, Some("proto".to_string()));
        assert!(params.base64);
    }

    #[test]
    fn test_parse_get_query_params_with_compression() {
        let params =
            parse_get_query_params(Some("message=abc&encoding=json&compression=gzip&base64=1"))
                .expect("should parse");
        assert_eq!(params.compression, Some("gzip".to_string()));
    }

    #[test]
    fn test_parse_get_query_params_missing_encoding() {
        let result = parse_get_query_params(Some("message=test&connect=v1"));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_get_query_params_no_query() {
        let result = parse_get_query_params(None);
        assert!(result.is_err());
    }

    #[test]
    fn test_percent_decode_basic() {
        let decoded = percent_decode("%7B%22name%22%3A%22test%22%7D").expect("should decode");
        assert_eq!(decoded, b"{\"name\":\"test\"}");
    }

    #[test]
    fn test_percent_decode_plus_as_space() {
        let decoded = percent_decode("hello+world").expect("should decode");
        assert_eq!(decoded, b"hello world");
    }

    #[test]
    fn test_percent_decode_passthrough() {
        let decoded = percent_decode("hello").expect("should decode");
        assert_eq!(decoded, b"hello");
    }

    #[test]
    fn test_decode_get_message_json() {
        let params = GetQueryParams {
            message: Some("%7B%7D".to_string()),
            encoding: Some("json".to_string()),
            base64: false,
            compression: None,
            connect_version: Some("v1".to_string()),
        };
        let compression = CompressionRegistry::default();
        let result = decode_get_message(&params, &compression, 1024 * 1024).expect("should decode");
        assert_eq!(result.as_ref(), b"{}");
    }

    #[test]
    fn test_decode_get_message_base64() {
        let params = GetQueryParams {
            message: Some("e30".to_string()), // base64 for "{}"
            encoding: Some("json".to_string()),
            base64: true,
            compression: None,
            connect_version: Some("v1".to_string()),
        };
        let compression = CompressionRegistry::default();
        let result = decode_get_message(&params, &compression, 1024 * 1024).expect("should decode");
        assert_eq!(result.as_ref(), b"{}");
    }

    #[test]
    fn test_decode_get_message_empty() {
        let params = GetQueryParams {
            message: None,
            encoding: Some("json".to_string()),
            base64: false,
            compression: None,
            connect_version: Some("v1".to_string()),
        };
        let compression = CompressionRegistry::default();
        let result = decode_get_message(&params, &compression, 1024 * 1024).expect("should decode");
        assert!(result.is_empty());
    }

    // ========================================================================
    // parse_timeout tests
    // ========================================================================

    #[test]
    fn test_parse_timeout_connect_milliseconds() {
        assert_eq!(
            parse_timeout("5000", Protocol::Connect),
            Some(Duration::from_millis(5000))
        );
    }

    #[test]
    fn test_parse_timeout_connect_zero() {
        assert_eq!(
            parse_timeout("0", Protocol::Connect),
            Some(Duration::from_millis(0))
        );
    }

    #[test]
    fn test_parse_timeout_connect_invalid() {
        assert_eq!(parse_timeout("abc", Protocol::Connect), None);
        assert_eq!(parse_timeout("", Protocol::Connect), None);
    }

    #[test]
    fn test_parse_timeout_grpc_hours() {
        assert_eq!(
            parse_timeout("1H", Protocol::Grpc),
            Some(Duration::from_secs(3600))
        );
    }

    #[test]
    fn test_parse_timeout_grpc_minutes() {
        assert_eq!(
            parse_timeout("5M", Protocol::Grpc),
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn test_parse_timeout_grpc_seconds() {
        assert_eq!(
            parse_timeout("30S", Protocol::Grpc),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn test_parse_timeout_grpc_milliseconds() {
        assert_eq!(
            parse_timeout("500m", Protocol::Grpc),
            Some(Duration::from_millis(500))
        );
    }

    #[test]
    fn test_parse_timeout_grpc_microseconds() {
        assert_eq!(
            parse_timeout("100u", Protocol::Grpc),
            Some(Duration::from_micros(100))
        );
    }

    #[test]
    fn test_parse_timeout_grpc_nanoseconds() {
        assert_eq!(
            parse_timeout("999n", Protocol::Grpc),
            Some(Duration::from_nanos(999))
        );
    }

    #[test]
    fn test_parse_timeout_grpc_zero() {
        assert_eq!(
            parse_timeout("0S", Protocol::Grpc),
            Some(Duration::from_secs(0))
        );
    }

    #[test]
    fn test_parse_timeout_grpc_invalid_unit() {
        assert_eq!(parse_timeout("5X", Protocol::Grpc), None);
    }

    #[test]
    fn test_parse_timeout_grpc_no_digits() {
        assert_eq!(parse_timeout("H", Protocol::Grpc), None);
    }

    #[test]
    fn test_parse_timeout_grpc_empty() {
        assert_eq!(parse_timeout("", Protocol::Grpc), None);
    }

    #[test]
    fn test_parse_timeout_grpc_over_8_digits_rejected() {
        // gRPC spec caps at 8 digits. Before this was enforced, a header
        // like "18446744073709551615S" would parse to Duration::from_secs(u64::MAX)
        // and then panic downstream at `Instant::now() + d` (overflow).
        assert_eq!(parse_timeout("123456789S", Protocol::Grpc), None);
        let huge = format!("{}S", u64::MAX);
        assert_eq!(parse_timeout(&huge, Protocol::Grpc), None);
        // 8 digits is the boundary — this is the spec max
        assert_eq!(
            parse_timeout("99999999S", Protocol::Grpc),
            Some(Duration::from_secs(99_999_999))
        );
        // Verify the spec-max value is usable with Instant arithmetic
        let d = parse_timeout("99999999H", Protocol::Grpc).unwrap();
        assert!(std::time::Instant::now().checked_add(d).is_some());
    }

    #[test]
    fn test_parse_timeout_connect_over_10_digits_rejected() {
        // Connect spec caps at 10 digits.
        assert_eq!(parse_timeout("12345678901", Protocol::Connect), None);
        let huge = format!("{}", u64::MAX);
        assert_eq!(parse_timeout(&huge, Protocol::Connect), None);
        // 10 digits is the boundary — spec max is ≈ 115 days
        assert_eq!(
            parse_timeout("9999999999", Protocol::Connect),
            Some(Duration::from_millis(9_999_999_999))
        );
        // Verify the spec-max value is usable with Instant arithmetic
        let d = parse_timeout("9999999999", Protocol::Connect).unwrap();
        assert!(std::time::Instant::now().checked_add(d).is_some());
    }

    #[test]
    fn test_parse_timeout_grpc_non_ascii_rejected() {
        // Multi-byte trailing char would panic split_at without the is_ascii guard.
        // "5é" is 3 bytes (5 + 0xC3 0xA9), len-1 lands mid-codepoint.
        assert_eq!(parse_timeout("5é", Protocol::Grpc), None);
        // Non-ASCII in the digit portion
        assert_eq!(parse_timeout("é5m", Protocol::Grpc), None);
        // Emoji trailing
        assert_eq!(parse_timeout("5☺", Protocol::Grpc), None);
        // Connect protocol path doesn't use split_at, but verify anyway
        assert_eq!(parse_timeout("5é", Protocol::Connect), None);
    }

    #[test]
    fn test_parse_timeout_grpc_web_same_as_grpc() {
        assert_eq!(
            parse_timeout("500m", Protocol::GrpcWeb),
            Some(Duration::from_millis(500))
        );
    }

    // ========================================================================
    // gRPC helper tests
    // ========================================================================

    #[test]
    fn test_grpc_percent_encode_passthrough() {
        assert_eq!(grpc_percent_encode("hello world"), "hello world");
        assert_eq!(grpc_percent_encode("a-b_c.d"), "a-b_c.d");
    }

    #[test]
    fn test_grpc_percent_encode_percent() {
        assert_eq!(grpc_percent_encode("100%"), "100%25");
    }

    #[test]
    fn test_grpc_percent_encode_non_ascii() {
        assert_eq!(grpc_percent_encode("café"), "caf%C3%A9");
    }

    #[test]
    fn test_grpc_percent_encode_control_chars() {
        assert_eq!(grpc_percent_encode("a\nb"), "a%0Ab");
        assert_eq!(grpc_percent_encode("a\tb"), "a%09b");
    }

    #[test]
    fn test_build_grpc_trailers_success() {
        let custom = http::HeaderMap::new();
        let trailers = build_grpc_trailers(None, &custom);
        assert_eq!(trailers.get(&GRPC_STATUS).unwrap().to_str().unwrap(), "0");
        assert!(!trailers.contains_key(&GRPC_MESSAGE));
    }

    #[test]
    fn test_build_grpc_trailers_error() {
        let err = ConnectError::not_found("thing not found");
        let custom = http::HeaderMap::new();
        let trailers = build_grpc_trailers(Some(&err), &custom);
        assert_eq!(
            trailers.get(&GRPC_STATUS).unwrap().to_str().unwrap(),
            "5" // NOT_FOUND
        );
        assert_eq!(
            trailers.get(&GRPC_MESSAGE).unwrap().to_str().unwrap(),
            "thing not found"
        );
        assert!(trailers.contains_key("grpc-status-details-bin"));
    }

    #[test]
    fn test_build_grpc_trailers_custom_metadata() {
        let mut custom = http::HeaderMap::new();
        custom.insert("x-custom", http::HeaderValue::from_static("value1"));
        custom.append("x-custom", http::HeaderValue::from_static("value2"));
        let trailers = build_grpc_trailers(None, &custom);
        assert_eq!(trailers.get(&GRPC_STATUS).unwrap().to_str().unwrap(), "0");
        let values: Vec<_> = trailers
            .get_all("x-custom")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(values, vec!["value1", "value2"]);
    }

    #[test]
    fn test_build_grpc_trailers_dedup_error_trailers() {
        // When error has its own trailers, context trailers should be skipped
        let mut err = ConnectError::internal("error");
        err.trailers_mut()
            .insert("x-trailer", http::HeaderValue::from_static("from-error"));
        let mut custom = http::HeaderMap::new();
        custom.insert("x-trailer", http::HeaderValue::from_static("from-context"));
        let trailers = build_grpc_trailers(Some(&err), &custom);
        let values: Vec<_> = trailers
            .get_all("x-trailer")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        // Should only have the error's trailer, not the context's
        assert_eq!(values, vec!["from-error"]);
    }

    #[test]
    fn test_build_grpc_trailers_no_dedup_when_error_has_no_trailers() {
        // When error has no trailers, context trailers should be included
        let err = ConnectError::internal("error");
        let mut custom = http::HeaderMap::new();
        custom.insert("x-trailer", http::HeaderValue::from_static("from-context"));
        let trailers = build_grpc_trailers(Some(&err), &custom);
        assert_eq!(
            trailers.get("x-trailer").unwrap().to_str().unwrap(),
            "from-context"
        );
    }

    #[test]
    fn test_encode_grpc_web_trailers() {
        let mut headers = http::HeaderMap::new();
        headers.insert(&GRPC_STATUS, http::HeaderValue::from_static("0"));
        let frame = encode_grpc_web_trailers(&headers);
        // Should start with 0x80 (trailer flag)
        assert_eq!(frame[0], 0x80);
        // Next 4 bytes are big-endian length
        let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        assert_eq!(frame.len(), 5 + len);
        // Payload should contain the header
        let payload = std::str::from_utf8(&frame[5..]).unwrap();
        assert!(payload.contains("grpc-status: 0\r\n"));
    }

    #[test]
    fn test_encode_grpc_web_trailers_multi_header() {
        let mut headers = http::HeaderMap::new();
        headers.insert(&GRPC_STATUS, http::HeaderValue::from_static("13"));
        headers.insert(&GRPC_MESSAGE, http::HeaderValue::from_static("internal"));
        headers.insert(
            "grpc-status-details-bin",
            http::HeaderValue::from_static("abc123"),
        );
        let frame = encode_grpc_web_trailers(&headers);
        assert_eq!(frame[0], 0x80);
        let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        let payload = std::str::from_utf8(&frame[5..5 + len]).unwrap();
        assert!(payload.contains("grpc-status: 13\r\n"));
        assert!(payload.contains("grpc-message: internal\r\n"));
        assert!(payload.contains("grpc-status-details-bin: abc123\r\n"));
    }

    #[test]
    fn test_encode_grpc_status_details_basic() {
        let err = ConnectError::internal("test error");
        let bytes = crate::grpc_status::encode(&err);
        // Should be valid protobuf - verify field 1 (code) is present
        // Field 1, wire type 0 (varint): tag = (1 << 3) | 0 = 8
        assert!(bytes.len() > 2);
        assert_eq!(bytes[0], 8); // tag for field 1 varint
        assert_eq!(bytes[1], 13); // INTERNAL = 13
    }

    // ========================================================================
    // BatchingEnvelopeStream tests
    // ========================================================================

    /// Drain a BatchingEnvelopeStream and count data frames vs the terminal frame.
    fn collect_frames(mut stream: BatchingEnvelopeStream) -> (Vec<Bytes>, Option<Frame<Bytes>>) {
        use futures::task::noop_waker_ref;
        let mut cx = std::task::Context::from_waker(noop_waker_ref());
        let mut data_frames = Vec::new();
        let mut terminal = None;
        loop {
            match Pin::new(&mut stream).poll_next(&mut cx) {
                Poll::Ready(Some(Ok(f))) if f.is_data() => {
                    data_frames.push(f.into_data().unwrap());
                }
                Poll::Ready(Some(Ok(f))) => {
                    terminal = Some(f);
                }
                Poll::Ready(None) => break,
                Poll::Pending => panic!("synchronous source should never be Pending"),
            }
        }
        (data_frames, terminal)
    }

    #[test]
    fn batching_sync_source_one_data_frame() {
        // 10 small synchronous items should batch into a single data frame,
        // then one trailers frame.
        let items: Vec<Result<Bytes, ConnectError>> =
            (0..10).map(|_| Ok(Bytes::from_static(b"msg"))).collect();
        let source: crate::EncodedStream =
            Box::pin(futures::stream::iter(items).map(|r| r.map(Into::into)));

        let stream = BatchingEnvelopeStream::new(
            source,
            http::HeaderMap::new(),
            None,
            CompressionPolicy::default(),
            StreamFinalizer::GrpcTrailers,
        );

        let (data_frames, terminal) = collect_frames(stream);
        assert_eq!(
            data_frames.len(),
            1,
            "10 synchronous items should produce 1 batched data frame, got {}",
            data_frames.len()
        );
        // Each item is 5 (header) + 3 ("msg") = 8 bytes × 10 = 80 bytes.
        assert_eq!(data_frames[0].len(), 80);
        assert!(terminal.is_some());
        assert!(terminal.unwrap().is_trailers());
    }

    #[test]
    fn batching_threshold_splits_frames() {
        // Items large enough that the 16KB threshold forces a split.
        // 9KB items: first fills buf to 9K < 16K → loop, second fills to 18K ≥ 16K → flush.
        let big = Bytes::from(vec![b'x'; 9 * 1024]);
        let items: Vec<Result<Bytes, ConnectError>> = (0..4).map(|_| Ok(big.clone())).collect();
        let source: crate::EncodedStream =
            Box::pin(futures::stream::iter(items).map(|r| r.map(Into::into)));

        let stream = BatchingEnvelopeStream::new(
            source,
            http::HeaderMap::new(),
            None,
            CompressionPolicy::default(),
            StreamFinalizer::GrpcTrailers,
        );

        let (data_frames, terminal) = collect_frames(stream);
        // 4 items of 9KB+5 each = 36,020 bytes. Threshold is 16KB.
        // After item 2: 18,010 ≥ 16K → flush frame 1.
        // After item 4: 18,010 ≥ 16K → flush frame 2.
        // Source None → buf empty → trailers directly.
        assert_eq!(data_frames.len(), 2);
        assert!(terminal.unwrap().is_trailers());
    }

    #[test]
    fn batching_empty_source_just_finalizer() {
        let source: BoxStream<_> = Box::pin(futures::stream::empty());
        let stream = BatchingEnvelopeStream::new(
            source,
            http::HeaderMap::new(),
            None,
            CompressionPolicy::default(),
            StreamFinalizer::GrpcTrailers,
        );
        let (data_frames, terminal) = collect_frames(stream);
        assert!(data_frames.is_empty());
        assert!(terminal.unwrap().is_trailers());
    }

    #[test]
    fn batching_connect_finalizer_is_data_frame() {
        // Connect protocol finalizer is an END_STREAM envelope (data frame),
        // not an HTTP/2 trailers frame.
        let source: crate::EncodedStream = Box::pin(futures::stream::once(async {
            Ok(Bytes::from_static(b"x").into())
        }));
        let stream = BatchingEnvelopeStream::new(
            source,
            http::HeaderMap::new(),
            None,
            CompressionPolicy::default(),
            StreamFinalizer::ConnectEndStream,
        );
        let (data_frames, terminal) = collect_frames(stream);
        // One data frame with the item, then the END_STREAM envelope arrives
        // as a second data frame (terminal is None because it IS a data frame).
        assert_eq!(data_frames.len(), 2);
        assert!(terminal.is_none());
        // Second frame should have the END_STREAM flag set in its envelope header.
        let end_frame = &data_frames[1];
        assert_eq!(end_frame[0], crate::envelope::flags::END_STREAM);
    }

    #[test]
    fn batching_error_after_items_stages_final() {
        // 3 items then an error: items should be flushed in one frame,
        // error trailers in the next.
        let items: Vec<Result<Bytes, ConnectError>> = vec![
            Ok(Bytes::from_static(b"a")),
            Ok(Bytes::from_static(b"b")),
            Ok(Bytes::from_static(b"c")),
            Err(ConnectError::internal("boom")),
        ];
        let source: crate::EncodedStream =
            Box::pin(futures::stream::iter(items).map(|r| r.map(Into::into)));
        let stream = BatchingEnvelopeStream::new(
            source,
            http::HeaderMap::new(),
            None,
            CompressionPolicy::default(),
            StreamFinalizer::GrpcTrailers,
        );
        let (data_frames, terminal) = collect_frames(stream);
        // 3 items batched into 1 data frame, then error trailers.
        assert_eq!(data_frames.len(), 1);
        assert_eq!(data_frames[0].len(), 3 * (5 + 1)); // 3× (header + 1 byte)
        let trailers = terminal.unwrap().into_trailers().unwrap();
        // Error trailers should have non-zero grpc-status.
        let status = trailers.get("grpc-status").unwrap().to_str().unwrap();
        assert_ne!(status, "0");
    }

    // ========================================================================
    // EndStreamResponse::error — trailer precedence + detail serialization
    // ========================================================================

    #[test]
    fn end_stream_error_includes_error_trailers() {
        // Error-level trailers must populate the END_STREAM metadata when
        // present, matching gRPC's build_grpc_trailers (error trailers
        // take precedence over context trailers).
        let mut err_trailers = http::HeaderMap::new();
        err_trailers.insert("x-error-info", "from-err".parse().unwrap());
        let err = ConnectError::internal("boom").with_trailers(err_trailers);

        let mut context_trailers = http::HeaderMap::new();
        context_trailers.insert("x-ctx", "from-context".parse().unwrap());

        let end = EndStreamResponse::error(&err, &context_trailers);
        let metadata = end.metadata.expect("metadata should be Some");
        assert!(
            metadata.contains_key("x-error-info"),
            "error-level trailer should be in metadata: {metadata:?}"
        );
        assert!(
            !metadata.contains_key("x-ctx"),
            "context trailer should NOT be in metadata when err has own trailers: {metadata:?}"
        );
    }

    #[test]
    fn end_stream_error_falls_back_to_context_trailers() {
        // When err has no trailers, use context trailers (pre-existing behavior).
        let err = ConnectError::internal("boom");
        let mut context_trailers = http::HeaderMap::new();
        context_trailers.insert("x-ctx", "from-context".parse().unwrap());

        let end = EndStreamResponse::error(&err, &context_trailers);
        let metadata = end.metadata.expect("metadata should be Some");
        assert!(metadata.contains_key("x-ctx"));
    }

    #[test]
    fn end_stream_error_details_include_debug_field() {
        // Details are serialized via ErrorDetail's Serialize derive so
        // all fields (including debug) appear. A hand-rolled JSON
        // construction previously dropped debug.
        let detail = crate::error::ErrorDetail {
            type_url: "test.Detail".into(),
            value: Some("YmFzZTY0".into()),
            debug: Some(serde_json::json!({"hint": "turn it off and on again"})),
        };
        let err = ConnectError::internal("boom").with_detail(detail);

        let end = EndStreamResponse::error(&err, &http::HeaderMap::new());
        let json = serde_json::to_string(&end).unwrap();

        assert!(
            json.contains("\"type\":\"test.Detail\""),
            "type missing: {json}"
        );
        assert!(
            json.contains("\"value\":\"YmFzZTY0\""),
            "value missing: {json}"
        );
        assert!(json.contains("\"debug\":"), "debug field missing: {json}");
        assert!(
            json.contains("turn it off and on again"),
            "debug content missing: {json}"
        );
    }

    // ========================================================================
    // Context.extensions passthrough
    // ========================================================================

    /// Prove that `http::Request` extensions survive the unary dispatch
    /// path and reach the handler via `Context.extensions`. A tower layer
    /// in front of `ConnectRpcService` inserts peer info this way.
    #[tokio::test]
    async fn extensions_flow_to_handler_context() {
        use std::sync::Mutex;

        #[derive(Clone, Debug, PartialEq)]
        struct PeerTag(&'static str);

        let captured = Arc::new(Mutex::new(None::<PeerTag>));
        let handler_captured = Arc::clone(&captured);
        let router = Router::new().route(
            "svc",
            "Method",
            crate::handler_fn(move |ctx: RequestContext, _req: buffa_types::Empty| {
                let cap = Arc::clone(&handler_captured);
                async move {
                    *cap.lock().unwrap() = ctx.extensions().get::<PeerTag>().cloned();
                    crate::Response::ok(buffa_types::Empty::default())
                }
            }),
        );

        let mut req = Request::builder()
            .method(Method::POST)
            .uri("/svc/Method")
            .header(header::CONTENT_TYPE, "application/proto")
            .body(Full::new(Bytes::new()))
            .unwrap();
        req.extensions_mut().insert(PeerTag("10.0.0.1:54321"));

        handle_unary_request(
            &router,
            "svc/Method",
            router.lookup("svc/Method"),
            req,
            Limits::default(),
            Arc::new(CompressionRegistry::new()),
            &CompressionPolicy::default(),
            &DeadlinePolicy::new(),
            &[],
        )
        .await
        .expect("dispatch should succeed");

        assert_eq!(
            captured.lock().unwrap().take(),
            Some(PeerTag("10.0.0.1:54321")),
            "extension inserted on the http::Request must reach Context.extensions"
        );
    }

    /// Register a `svc/Method` handler that captures
    /// `(ctx.path(), ctx.spec())`, apply `finalize` (e.g. `with_spec`),
    /// drive a request through `handle_unary_request`, and return the
    /// captured tuple.
    async fn capture_path_and_spec(
        finalize: impl FnOnce(Router) -> Router,
    ) -> (Option<String>, Option<crate::spec::Spec>) {
        use std::sync::Mutex;

        let captured = Arc::new(Mutex::new(None));
        let handler_captured = Arc::clone(&captured);
        let router = Router::new().route(
            "svc",
            "Method",
            crate::handler_fn(move |ctx: RequestContext, _req: buffa_types::Empty| {
                let cap = Arc::clone(&handler_captured);
                async move {
                    *cap.lock().unwrap() = Some((ctx.path().map(str::to_owned), ctx.spec()));
                    crate::Response::ok(buffa_types::Empty::default())
                }
            }),
        );
        let router = finalize(router);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/svc/Method")
            .header(header::CONTENT_TYPE, "application/proto")
            .body(Full::new(Bytes::new()))
            .unwrap();

        handle_unary_request(
            &router,
            "svc/Method",
            router.lookup("svc/Method"),
            req,
            Limits::default(),
            Arc::new(CompressionRegistry::new()),
            &CompressionPolicy::default(),
            &DeadlinePolicy::new(),
            &[],
        )
        .await
        .expect("dispatch should succeed");

        captured.lock().unwrap().take().expect("handler ran")
    }

    /// Prove that `RequestContext::path()` carries the request URI path
    /// through the dispatch path, in the leading-slash form, *even when*
    /// `RequestContext::spec()` is `None`.
    ///
    /// A `Router` route registered without [`Router::with_spec`] supplies
    /// no [`Spec`](crate::spec::Spec) — `path()` is the only source for
    /// the procedure name in that case, and it's the one an auth
    /// interceptor or span builder must read.
    #[tokio::test]
    async fn path_flows_to_handler_context() {
        let (path, spec) = capture_path_and_spec(|r| r).await;
        assert_eq!(
            path.as_deref(),
            Some("/svc/Method"),
            "path() must carry the leading-slash request URI path"
        );
        assert!(
            spec.is_none(),
            "Router route without with_spec supplies no Spec — path() must still be populated"
        );
    }

    /// `Router::with_spec` flows through the dispatch path to
    /// `RequestContext::spec()`. A `Router` built through the generated
    /// `register()` (which chains `.with_spec(...)` after every
    /// `route_view*`) surfaces `Spec` to handlers and interceptors
    /// exactly like the monomorphic `FooServiceServer<T>` dispatcher.
    /// `path()` and `spec().procedure` must agree when both are present.
    #[tokio::test]
    async fn spec_flows_to_handler_context() {
        use crate::spec::{Spec, StreamType};
        const SPEC: Spec = Spec::server("/svc/Method", StreamType::Unary);

        let (path, spec) = capture_path_and_spec(|r| r.with_spec(SPEC)).await;
        assert_eq!(
            spec,
            Some(SPEC),
            "Router::with_spec must flow to ctx.spec()"
        );
        assert_eq!(
            path.as_deref(),
            spec.map(|s| s.procedure),
            "path() and spec().procedure must agree when both are present"
        );
    }

    /// Interceptors must run on the Connect GET path. An idempotent unary
    /// RPC accessed via HTTP GET must not bypass an authz interceptor —
    /// every dispatch shape (Connect POST, Connect GET, gRPC fast path,
    /// gRPC streaming-frame unary) routes through `call_unary_intercepted`.
    /// A regression in any one of them is silent until a client uses that
    /// shape, which for GET only happens for `idempotency_level =
    /// NO_SIDE_EFFECTS` methods.
    #[tokio::test]
    async fn interceptor_runs_on_connect_get_request() {
        use crate::interceptor::{Interceptor, Next, UnaryRequest, UnaryResponse};
        use std::sync::Mutex;

        let intercepted = Arc::new(AtomicBool::new(false));
        let handler_ran = Arc::new(Mutex::new(false));

        let handler_ran_inner = Arc::clone(&handler_ran);
        let router = Router::new().route_idempotent(
            "svc",
            "Get",
            crate::handler_fn(move |_ctx: RequestContext, _req: buffa_types::Empty| {
                let h = Arc::clone(&handler_ran_inner);
                async move {
                    *h.lock().unwrap() = true;
                    crate::Response::ok(buffa_types::Empty::default())
                }
            }),
        );

        struct Recorder(Arc<AtomicBool>);
        #[async_trait::async_trait]
        impl Interceptor for Recorder {
            async fn intercept_unary(
                &self,
                req: UnaryRequest,
                next: Next<'_>,
            ) -> Result<UnaryResponse, ConnectError> {
                self.0.store(true, Ordering::SeqCst);
                next.run(req).await
            }
        }
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(Recorder(Arc::clone(&intercepted)))];

        // A Connect GET request: empty proto message, base64-encoded
        // (empty), encoding declared. Idempotent methods opt into GET so
        // CDNs and browsers can cache them — those requests must still be
        // intercepted.
        let req = Request::builder()
            .method(Method::GET)
            .uri("/svc/Get?message=&encoding=proto&base64=1&connect=v1")
            .body(Full::new(Bytes::new()))
            .unwrap();

        handle_unary_request(
            &router,
            "svc/Get",
            router.lookup("svc/Get"),
            req,
            Limits::default(),
            Arc::new(CompressionRegistry::new()),
            &CompressionPolicy::default(),
            &DeadlinePolicy::new(),
            &chain,
        )
        .await
        .expect("dispatch should succeed");

        assert!(
            intercepted.load(Ordering::SeqCst),
            "interceptor must run on Connect GET requests"
        );
        assert!(
            *handler_ran.lock().unwrap(),
            "handler must run after the interceptor passes through"
        );
    }

    /// `with_interceptor_arc` shares one interceptor across services.
    /// `with_interceptor` would `Arc::new` a fresh allocation per call,
    /// which is correct for unique state but wasteful (and surprising)
    /// when the interceptor carries process-wide shared state — pin the
    /// distinction with a strong-count check.
    #[test]
    fn with_interceptor_arc_shares_one_instance() {
        use crate::interceptor::Interceptor;

        struct Noop;
        #[async_trait::async_trait]
        impl Interceptor for Noop {}

        let shared: Arc<dyn Interceptor> = Arc::new(Noop);
        assert_eq!(Arc::strong_count(&shared), 1);

        let svc_a = ConnectRpcService::new(Router::new()).with_interceptor_arc(Arc::clone(&shared));
        let svc_b = ConnectRpcService::new(Router::new()).with_interceptor_arc(Arc::clone(&shared));

        // The caller's handle plus one per service.
        assert_eq!(
            Arc::strong_count(&shared),
            3,
            "with_interceptor_arc must store the supplied Arc, not a fresh allocation"
        );
        // Cloning a service is one Arc<[..]> bump, not a per-interceptor bump.
        let _svc_a2 = svc_a.clone();
        assert_eq!(Arc::strong_count(&shared), 3);
        drop(svc_a);
        drop(svc_b);
        drop(_svc_a2);
        assert_eq!(Arc::strong_count(&shared), 1);
    }

    // ========================================================================
    // ConnectError::into_http_response tests
    // ========================================================================

    fn headers_with_content_type(ct: &str) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            http::HeaderValue::from_str(ct).unwrap(),
        );
        headers
    }

    async fn body_bytes(body: ConnectRpcBody) -> Bytes {
        body.collect().await.unwrap().to_bytes()
    }

    #[tokio::test]
    async fn into_http_response_connect_unary() {
        let err = ConnectError::permission_denied("nope");
        let resp = err.into_http_response(&headers_with_content_type("application/proto"));

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            content_type::JSON
        );
        assert!(resp.headers().get(&GRPC_STATUS).is_none());

        let body = body_bytes(resp.into_body()).await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "permission_denied");
        assert_eq!(json["message"], "nope");
    }

    #[tokio::test]
    async fn into_http_response_connect_streaming() {
        let err = ConnectError::permission_denied("nope");
        let resp = err.into_http_response(&headers_with_content_type("application/connect+proto"));

        // Connect streaming errors are HTTP 200 with an EndStreamResponse envelope.
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/connect+proto"
        );

        let body = body_bytes(resp.into_body()).await;
        // Envelope: 1 flag byte (0x02 end-stream) + 4 length bytes + JSON payload.
        assert!(body.len() > 5, "envelope must contain a payload");
        assert_eq!(body[0], 0x02, "end-stream flag bit must be set");
        let json: serde_json::Value = serde_json::from_slice(&body[5..]).unwrap();
        assert_eq!(json["error"]["code"], "permission_denied");
    }

    #[tokio::test]
    async fn into_http_response_grpc() {
        let err = ConnectError::permission_denied("nope");
        let resp = err.into_http_response(&headers_with_content_type("application/grpc+proto"));

        // gRPC trailers-only error response: HTTP 200, grpc-status echoed in headers.
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/grpc+proto"
        );
        assert_eq!(
            resp.headers().get(&GRPC_STATUS).unwrap(),
            &crate::ErrorCode::PermissionDenied.grpc_code().to_string()
        );
    }

    #[tokio::test]
    async fn into_http_response_grpc_web() {
        let err = ConnectError::permission_denied("nope");
        let resp = err.into_http_response(&headers_with_content_type("application/grpc-web+proto"));

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/grpc-web+proto"
        );
        // gRPC-Web encodes trailers in the body, not response headers.
        assert!(resp.headers().get(&GRPC_STATUS).is_none());

        let body = body_bytes(resp.into_body()).await;
        // Trailer frame: flag byte 0x80 + 4 length bytes + headers.
        assert!(body.len() > 5, "trailer frame must contain a payload");
        assert_eq!(body[0], 0x80, "trailer flag bit must be set");
        let trailer_text = std::str::from_utf8(&body[5..]).unwrap();
        assert!(trailer_text.contains("grpc-status: 7"), "{trailer_text:?}");
    }

    // Echoing a JSON request codec into the error response only applies when
    // the `json` feature is on; a proto-only build declines JSON content types
    // at negotiation, so these scenarios are unreachable there.
    #[cfg(feature = "json")]
    #[tokio::test]
    async fn into_http_response_connect_streaming_json_codec() {
        // The codec format from the request Content-Type must round-trip into
        // the response Content-Type, even though the EndStreamResponse payload
        // itself is always JSON.
        let err = ConnectError::permission_denied("nope");
        let resp = err.into_http_response(&headers_with_content_type("application/connect+json"));

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/connect+json"
        );
    }

    #[cfg(feature = "json")]
    #[tokio::test]
    async fn into_http_response_grpc_json_codec() {
        let err = ConnectError::permission_denied("nope");
        let resp = err.into_http_response(&headers_with_content_type("application/grpc+json"));

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/grpc+json"
        );
        assert_eq!(
            resp.headers().get(&GRPC_STATUS).unwrap(),
            &crate::ErrorCode::PermissionDenied.grpc_code().to_string()
        );
    }

    // The Connect protocol requires HTTP 415 Unsupported Media Type when the
    // server does not support the requested message codec. A proto-only build
    // (no `json` feature) declines every JSON content type at content
    // negotiation, before the request body is touched.
    #[cfg(not(feature = "json"))]
    #[tokio::test]
    async fn proto_only_server_rejects_json_content_types_with_415() {
        let dispatcher = Arc::new(Router::new());

        for ct in ["application/json", "application/connect+json"] {
            let req = Request::builder()
                .method(Method::POST)
                .uri("/svc/Method")
                .header(header::CONTENT_TYPE, ct)
                .body(Full::new(Bytes::new()))
                .unwrap();
            let resp = handle_request(
                Arc::clone(&dispatcher),
                req,
                Limits::default(),
                Arc::new(CompressionRegistry::new()),
                &CompressionPolicy::default(),
                &DeadlinePolicy::new(),
                &[],
            )
            .await
            .expect("415 is returned as an Ok response, not an Err");
            assert_eq!(
                resp.status(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "{ct} must be rejected with HTTP 415 in a proto-only build"
            );
        }

        // A proto request passes content negotiation (it fails later as
        // route-not-found, not as a 415) — proving only JSON is declined.
        let req = Request::builder()
            .method(Method::POST)
            .uri("/svc/Method")
            .header(header::CONTENT_TYPE, "application/proto")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let status = match handle_request(
            Arc::clone(&dispatcher),
            req,
            Limits::default(),
            Arc::new(CompressionRegistry::new()),
            &CompressionPolicy::default(),
            &DeadlinePolicy::new(),
            &[],
        )
        .await
        {
            Ok(resp) => resp.status(),
            Err(err) => err.http_status(),
        };
        assert_ne!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    // A valid `application/grpc` / `application/grpc-web` prefix paired with an
    // unsupported message codec (e.g. `+thrift`) is rejected with the gRPC
    // status `unimplemented` (code 12), matching the compression axis. The
    // gRPC spec leaves this code unspecified and the conformance suite accepts
    // either `internal` or `unimplemented`; we use `unimplemented` because the
    // server genuinely does not implement the requested codec.
    #[tokio::test]
    async fn unsupported_grpc_codec_returns_unimplemented() {
        let dispatcher = Arc::new(Router::new());

        // gRPC: the trailers-only error echoes grpc-status in the response
        // headers.
        let req = Request::builder()
            .method(Method::POST)
            .uri("/svc/Method")
            .header(header::CONTENT_TYPE, "application/grpc+thrift")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = handle_request(
            Arc::clone(&dispatcher),
            req,
            Limits::default(),
            Arc::new(CompressionRegistry::new()),
            &CompressionPolicy::default(),
            &DeadlinePolicy::new(),
            &[],
        )
        .await
        .expect("a gRPC codec rejection is returned as an Ok response");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("grpc-status").unwrap(),
            "12",
            "unsupported gRPC codec must map to unimplemented (12)"
        );

        // gRPC-Web: grpc-status is carried in the trailer frame in the body.
        let req = Request::builder()
            .method(Method::POST)
            .uri("/svc/Method")
            .header(header::CONTENT_TYPE, "application/grpc-web+thrift")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = handle_request(
            Arc::clone(&dispatcher),
            req,
            Limits::default(),
            Arc::new(CompressionRegistry::new()),
            &CompressionPolicy::default(),
            &DeadlinePolicy::new(),
            &[],
        )
        .await
        .expect("a gRPC-Web codec rejection is returned as an Ok response");
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collecting the gRPC-Web error body must not fail")
            .to_bytes();
        let body_text = String::from_utf8_lossy(&body);
        assert!(
            body_text.contains("grpc-status: 12\r\n"),
            "unsupported gRPC-Web codec must map to unimplemented (12); body was {body_text:?}"
        );
    }

    // Mirrors connect-go: an unsupported HTTP verb on a known procedure returns
    // 405 with an `Allow` header (POST, plus GET for idempotent unary methods);
    // an unknown path returns 404. Also covers the bug where a bodyless
    // OPTIONS/HEAD (no Content-Type) was previously misreported as 415.
    #[tokio::test]
    async fn unsupported_http_verb_returns_405_with_allow() {
        let dispatcher = Arc::new(
            Router::new()
                .route(
                    "svc",
                    "Unary",
                    crate::handler_fn(|_ctx: RequestContext, _req: buffa_types::Empty| async {
                        crate::Response::ok(buffa_types::Empty::default())
                    }),
                )
                .route_idempotent(
                    "svc",
                    "Get",
                    crate::handler_fn(|_ctx: RequestContext, _req: buffa_types::Empty| async {
                        crate::Response::ok(buffa_types::Empty::default())
                    }),
                ),
        );

        async fn call(
            dispatcher: &Arc<Router>,
            req: Request<Full<Bytes>>,
        ) -> Result<Response<ConnectRpcBody>, ConnectError> {
            handle_request(
                Arc::clone(dispatcher),
                req,
                Limits::default(),
                Arc::new(CompressionRegistry::new()),
                &CompressionPolicy::default(),
                &DeadlinePolicy::new(),
                &[],
            )
            .await
        }

        // Non-idempotent unary procedure: Allow lists POST only. Includes a
        // bodyless OPTIONS/HEAD with no Content-Type (the original 415 bug).
        for (verb, with_ct) in [
            (Method::DELETE, true),
            (Method::PUT, true),
            (Method::OPTIONS, false),
            (Method::HEAD, false),
        ] {
            let mut builder = Request::builder().method(verb.clone()).uri("/svc/Unary");
            if with_ct {
                builder = builder.header(header::CONTENT_TYPE, "application/proto");
            }
            let req = builder.body(Full::new(Bytes::new())).unwrap();
            let resp = call(&dispatcher, req)
                .await
                .expect("405 is returned as an Ok response");
            assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED, "{verb}");
            assert_eq!(resp.headers().get(header::ALLOW).unwrap(), "POST", "{verb}");
        }

        // Idempotent unary procedure: Allow also lists GET.
        let req = Request::builder()
            .method(Method::DELETE)
            .uri("/svc/Get")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = call(&dispatcher, req).await.expect("405");
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(resp.headers().get(header::ALLOW).unwrap(), "GET, POST");

        // Unknown path with a bad verb maps to 404 route-not-found.
        let req = Request::builder()
            .method(Method::DELETE)
            .uri("/svc/Missing")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let status = match call(&dispatcher, req).await {
            Ok(resp) => resp.status(),
            Err(err) => err.http_status(),
        };
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    // A 415 advertises the content types the server accepts via `Accept-Post`
    // (connect-go parity); a proto-only build never lists a JSON media type.
    #[tokio::test]
    async fn unknown_content_type_returns_415_with_accept_post() {
        let dispatcher = Arc::new(Router::new());
        let req = Request::builder()
            .method(Method::POST)
            .uri("/svc/Method")
            .header(header::CONTENT_TYPE, "application/foo")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = handle_request(
            Arc::clone(&dispatcher),
            req,
            Limits::default(),
            Arc::new(CompressionRegistry::new()),
            &CompressionPolicy::default(),
            &DeadlinePolicy::new(),
            &[],
        )
        .await
        .expect("415 is returned as an Ok response");
        assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let accept_post = resp
            .headers()
            .get("accept-post")
            .expect("415 advertises Accept-Post")
            .to_str()
            .unwrap();
        assert!(accept_post.contains("application/proto"), "{accept_post}");
        #[cfg(feature = "json")]
        assert!(accept_post.contains("application/json"), "{accept_post}");
        #[cfg(not(feature = "json"))]
        assert!(
            !accept_post.contains("json"),
            "proto-only must not advertise json: {accept_post}"
        );
    }

    #[tokio::test]
    async fn into_http_response_grpc_web_text_mode() {
        // gRPC-Web text mode (`application/grpc-web-text`) is detected as
        // gRPC-Web; the trailers-only error body has the same wire shape as
        // binary mode (the service rejects text-mode requests, but a layer
        // running before dispatch must still produce *something* parseable).
        let err = ConnectError::permission_denied("nope");
        let resp = err.into_http_response(&headers_with_content_type("application/grpc-web-text"));

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get(&GRPC_STATUS).is_none());

        let body = body_bytes(resp.into_body()).await;
        assert_eq!(body[0], 0x80, "trailer flag bit must be set");
    }

    #[tokio::test]
    async fn into_http_response_content_type_with_parameters() {
        // Parameters after `;` must not defeat protocol detection.
        let err = ConnectError::permission_denied("nope");
        let resp = err.into_http_response(&headers_with_content_type(
            "application/grpc+proto; foo=bar",
        ));

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(&GRPC_STATUS).unwrap(),
            &crate::ErrorCode::PermissionDenied.grpc_code().to_string()
        );
    }

    #[tokio::test]
    async fn into_http_response_connect_unary_proto_request_returns_json_error() {
        // Connect unary errors are spec-required to be JSON regardless of the
        // request codec. A proto request must not get a proto-encoded error.
        let err = ConnectError::permission_denied("nope");
        let resp = err.into_http_response(&headers_with_content_type("application/proto"));

        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            content_type::JSON,
            "Connect unary error body must be JSON regardless of request codec"
        );
    }

    #[tokio::test]
    async fn into_http_response_no_content_type_falls_back_to_unary() {
        // Connect GET requests have no request Content-Type; they expect the
        // Connect unary JSON error shape.
        let err = ConnectError::permission_denied("nope");
        let resp = err.into_http_response(&http::HeaderMap::new());

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            content_type::JSON
        );
    }

    #[tokio::test]
    async fn into_http_response_unknown_content_type_falls_back_to_unary() {
        let err = ConnectError::unauthenticated("who?");
        let resp = err.into_http_response(&headers_with_content_type("text/plain"));

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            content_type::JSON
        );

        let body = body_bytes(resp.into_body()).await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "unauthenticated");
    }

    // ========================================================================
    // decode_request_body tests
    // ========================================================================

    /// Wait until the test body has been dropped: the request stream, and any
    /// drain it handed the body to, are finished with it. The body holds a
    /// clone of `token`.
    async fn body_released<T>(token: &Arc<T>) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while Arc::strong_count(token) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request body released");
        assert_no_drain_panicked();
    }

    /// Test body that yields a fixed sequence of data frames and records how
    /// many bytes the reader actually pulls from it.
    struct CountingBody {
        frames: std::collections::VecDeque<Bytes>,
        pulled: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl CountingBody {
        fn new(
            frames: impl IntoIterator<Item = Bytes>,
            pulled: &Arc<std::sync::atomic::AtomicUsize>,
        ) -> Self {
            count_panics();
            Self {
                frames: frames.into_iter().collect(),
                pulled: Arc::clone(pulled),
            }
        }
    }

    thread_local! {
        /// Panics on this thread, counted by the hook [`count_panics`]
        /// installs.
        static PANICS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    /// Count panics per thread from now on, then run the previous hook. A
    /// `#[tokio::test]` runtime polls spawned tasks on the test's own
    /// thread, so this sees a drain task that panics, which Tokio catches.
    fn count_panics() {
        static HOOK: std::sync::Once = std::sync::Once::new();
        HOOK.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                let _ = PANICS.try_with(|panics| panics.set(panics.get() + 1));
                previous(info);
            }));
        });
    }

    /// Fail the test if a drain task on this thread has panicked.
    fn assert_no_drain_panicked() {
        assert_eq!(
            PANICS.with(std::cell::Cell::get),
            0,
            "a drain task panicked"
        );
    }

    impl Body for CountingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            let this = self.get_mut();
            match this.frames.pop_front() {
                Some(data) => {
                    this.pulled
                        .fetch_add(data.len(), std::sync::atomic::Ordering::Relaxed);
                    Poll::Ready(Some(Ok(Frame::data(data))))
                }
                None => Poll::Ready(None),
            }
        }
    }

    /// Regression test: a client that sends a valid END_STREAM envelope and
    /// then keeps sending request body data must not cause unbounded
    /// buffering. The request stream must treat END_STREAM as terminal and
    /// hand the rest of the body to the bounded drain, which stops once
    /// `MAX_DRAIN_BYTES` is exceeded.
    #[tokio::test]
    async fn test_body_reader_bounds_data_after_end_stream() {
        const CHUNK_SIZE: usize = 64 * 1024;
        const JUNK_TOTAL: usize = 4 * 1024 * 1024;

        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // A valid END_STREAM envelope followed by 4 MiB of trailing junk.
        let mut frames = std::collections::VecDeque::new();
        frames.push_back(Envelope::end_stream(Bytes::from_static(b"{}")).encode());
        for _ in 0..(JUNK_TOTAL / CHUNK_SIZE) {
            frames.push_back(Bytes::from(vec![0xAA_u8; CHUNK_SIZE]));
        }

        let body = CountingBody::new(frames, &pulled);

        let mut request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );

        // The handler-facing stream sees a clean end of stream: no messages,
        // no error. Trailing junk after END_STREAM must not surface to the
        // handler.
        assert!(
            request_stream.next().await.is_none(),
            "request stream must end cleanly after END_STREAM"
        );
        body_released(&pulled).await;

        // The rest of the body is drained (for HTTP/1.1 keep-alive), but the
        // drain stops shortly after its limit instead of reading the trailing
        // data without bound.
        let pulled = pulled.load(std::sync::atomic::Ordering::Relaxed);
        let max_expected = MAX_DRAIN_BYTES + CHUNK_SIZE + crate::envelope::HEADER_SIZE + 2;
        assert!(
            pulled > MAX_DRAIN_BYTES && pulled <= max_expected,
            "reader pulled {pulled} bytes after END_STREAM (expected more than \
             {MAX_DRAIN_BYTES} and at most {max_expected})"
        );
    }

    /// Trailing data in the END_STREAM envelope's own frame counts towards
    /// the drain limit: when it alone exceeds the limit, the body is dropped
    /// on the spot and nothing more is read.
    #[tokio::test]
    async fn test_body_reader_oversized_trailing_frame_stops_the_drain() {
        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut first = Envelope::end_stream(Bytes::from_static(b"{}"))
            .encode()
            .to_vec();
        first.resize(first.len() + MAX_DRAIN_BYTES + 1, 0xAA);
        let first_len = first.len();
        let body = CountingBody::new(
            [Bytes::from(first), Bytes::from_static(b"never read")],
            &pulled,
        );

        let mut request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );
        assert!(request_stream.next().await.is_none());
        assert_eq!(Arc::strong_count(&pulled), 1, "body dropped at once");
        assert_eq!(pulled.load(std::sync::atomic::Ordering::Relaxed), first_len);
    }

    /// Test body that yields its frames, then stays pending forever, and
    /// records when it is dropped: a client that stalls part-way through a
    /// request.
    struct StalledBody {
        frames: std::collections::VecDeque<Bytes>,
        _dropped: DropFlag,
    }

    impl Body for StalledBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            match self.get_mut().frames.pop_front() {
                Some(data) => Poll::Ready(Some(Ok(Frame::data(data)))),
                None => Poll::Pending,
            }
        }
    }

    /// Sets its flag when dropped.
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    /// Body fed by the test through a channel.
    struct FedBody {
        rx: tokio::sync::mpsc::UnboundedReceiver<Bytes>,
        _dropped: DropFlag,
    }

    impl Body for FedBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            self.get_mut()
                .rx
                .poll_recv(cx)
                .map(|data| data.map(|data| Ok(Frame::data(data))))
        }
    }

    /// A request stream on a test body, seen from the handler's side.
    struct Reader {
        request_stream: Option<BoxStream<Result<Bytes, ConnectError>>>,
        body_dropped: Arc<AtomicBool>,
        /// Held by the decoder while the stream is decoding.
        registry: Arc<CompressionRegistry>,
    }

    impl Reader {
        /// A reader on a body that yields `frames` and then stalls.
        fn stalled(frames: impl IntoIterator<Item = Bytes>) -> Self {
            let body_dropped = Arc::new(AtomicBool::new(false));
            Self::on(
                StalledBody {
                    frames: frames.into_iter().collect(),
                    _dropped: DropFlag(Arc::clone(&body_dropped)),
                },
                body_dropped,
            )
        }

        fn on<B>(body: B, body_dropped: Arc<AtomicBool>) -> Self
        where
            B: Body<Data = Bytes, Error = Infallible> + Send + 'static,
        {
            count_panics();
            let registry = Arc::new(CompressionRegistry::new());
            let request_stream =
                decode_request_body(body, DEFAULT_MAX_MESSAGE_SIZE, None, Arc::clone(&registry));
            Self {
                request_stream: Some(request_stream),
                body_dropped,
                registry,
            }
        }

        /// The next item the handler would see.
        async fn next(&mut self) -> Option<Result<Bytes, ConnectError>> {
            self.request_stream
                .as_mut()
                .expect("stream not dropped")
                .next()
                .await
        }

        /// Poll the stream once, as a handler waiting for a message does, and
        /// assert that nothing is ready yet.
        fn assert_pending(&mut self) {
            assert!(
                self.next().now_or_never().is_none(),
                "the handler's stream must be waiting for the client"
            );
        }

        /// Assert that the handler's stream has ended now, not when a timer
        /// fires: with time paused, awaiting the stream would auto-advance the
        /// clock and hide the difference.
        async fn assert_stream_ended(&mut self) {
            Self::settle().await;
            assert!(
                matches!(self.next().now_or_never(), Some(None)),
                "the handler's stream must have ended"
            );
        }

        /// The handler is done with the request stream.
        fn handler_drops_stream(&mut self) {
            self.request_stream = None;
        }

        fn body_dropped(&self) -> bool {
            self.body_dropped.load(Ordering::Relaxed)
        }

        /// Let a drain task run until it is waiting on the body.
        async fn settle() {
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
        }

        /// Assert that the stalled body is drained for [`DRAIN_TIMEOUT`] and
        /// then dropped. The decoder, and with it any partial message, is
        /// gone from the start of the drain. Time is paused, so a drain that
        /// never lets go fails at an `assert` instead of hanging.
        async fn assert_drains_then_releases(self) {
            Self::settle().await;
            assert_eq!(
                Arc::strong_count(&self.registry),
                1,
                "the decoder must be dropped when the drain starts"
            );
            tokio::time::advance(DRAIN_TIMEOUT - Duration::from_secs(1)).await;
            Self::settle().await;
            assert!(!self.body_dropped(), "the body is held while draining");
            tokio::time::advance(Duration::from_secs(2)).await;
            Self::settle().await;
            assert!(self.body_dropped(), "the body must be dropped");
            assert_no_drain_panicked();
        }
    }

    /// A client declares a message, sends all but its last byte, and
    /// stalls. Once the handler drops the request stream the partial message
    /// is freed without waiting for the rest of it.
    #[tokio::test(start_paused = true)]
    async fn test_body_reader_partial_envelope_released_when_handler_drops() {
        let mut wire = Envelope::data(Bytes::from(vec![7_u8; 4096]))
            .encode()
            .to_vec();
        wire.pop();
        let mut reader = Reader::stalled([Bytes::from(wire)]);
        reader.assert_pending();
        assert!(
            !reader.body_dropped(),
            "held while the handler holds the stream"
        );
        assert_eq!(
            Arc::strong_count(&reader.registry),
            2,
            "the decoder is alive while the handler holds the stream"
        );

        reader.handler_drops_stream();
        reader.assert_drains_then_releases().await;
    }

    /// The handler is gone before the client has sent anything.
    #[tokio::test(start_paused = true)]
    async fn test_body_reader_no_data_released_when_handler_drops() {
        let mut reader = Reader::stalled([]);
        reader.assert_pending();
        reader.handler_drops_stream();
        reader.assert_drains_then_releases().await;
    }

    /// The handler has read a whole message and then goes away while the
    /// client stalls.
    #[tokio::test(start_paused = true)]
    async fn test_body_reader_complete_message_then_stall_released_when_handler_drops() {
        let frame = Envelope::data(Bytes::from_static(b"hello")).encode();
        let mut reader = Reader::stalled([frame]);
        let first = reader.next().await.expect("a message").expect("decodes");
        assert_eq!(&first[..], b"hello");
        reader.handler_drops_stream();
        reader.assert_drains_then_releases().await;
    }

    /// A handler that keeps its request stream open and idle is not cut off:
    /// waiting for the client is the handler's business (and its deadline's).
    #[tokio::test(start_paused = true)]
    async fn test_body_reader_waits_while_handler_holds_stream() {
        let mut reader = Reader::stalled([]);
        reader.assert_pending();
        tokio::time::advance(Duration::from_secs(3600)).await;
        Reader::settle().await;
        reader.assert_pending();
        assert!(!reader.body_dropped());
    }

    /// A handler waiting on its request stream is woken by each frame that
    /// arrives later, including one that completes a message, and skips
    /// empty frames.
    #[tokio::test(start_paused = true)]
    async fn test_body_reader_wakes_for_frames_arriving_later() {
        let (feed, rx) = tokio::sync::mpsc::unbounded_channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let mut reader = Reader::on(
            FedBody {
                rx,
                _dropped: DropFlag(Arc::clone(&dropped)),
            },
            dropped,
        );
        let handler = tokio::spawn(async move { reader.next().await });
        Reader::settle().await;
        let wire = Envelope::data(Bytes::from_static(b"late")).encode();
        feed.send(wire.slice(..3)).unwrap();
        Reader::settle().await;
        feed.send(Bytes::new()).unwrap();
        tokio::time::advance(Duration::from_secs(60)).await;
        feed.send(wire.slice(3..)).unwrap();
        let msg = tokio::time::timeout(Duration::from_secs(1), handler)
            .await
            .expect("the handler was not woken")
            .expect("handler task must not panic")
            .expect("a message")
            .expect("decodes");
        assert_eq!(&msg[..], b"late");
    }

    /// After a decode error the decoder is finished whether or not the
    /// handler has dropped the stream: the body is drained, bounded in time.
    #[tokio::test(start_paused = true)]
    async fn test_body_reader_decode_error_then_stall_released() {
        // A header declaring more than the message limit.
        let mut header = vec![0_u8];
        header.extend_from_slice(&u32::MAX.to_be_bytes());
        let mut reader = Reader::stalled([Bytes::from(header)]);
        let err = reader
            .next()
            .await
            .expect("an item")
            .expect_err("oversize message is an error");
        assert_eq!(err.code, crate::error::ErrorCode::ResourceExhausted);
        reader.assert_stream_ended().await;
        reader.assert_drains_then_releases().await;
    }

    /// Likewise after the END_STREAM envelope: the client has said it is
    /// done, and anything it sends afterwards is discarded.
    #[tokio::test(start_paused = true)]
    async fn test_body_reader_end_stream_then_stall_released() {
        let frame = Envelope::end_stream(Bytes::from_static(b"{}")).encode();
        let mut reader = Reader::stalled([frame]);
        reader.assert_stream_ended().await;
        reader.assert_drains_then_releases().await;
    }

    /// A message that arrives in the same frame as END_STREAM is still
    /// delivered, ahead of the end of the stream.
    #[tokio::test(start_paused = true)]
    async fn test_body_reader_message_before_end_stream_is_delivered() {
        let mut wire = Envelope::data(Bytes::from_static(b"hello"))
            .encode()
            .to_vec();
        wire.extend_from_slice(&Envelope::end_stream(Bytes::from_static(b"{}")).encode());
        let mut reader = Reader::stalled([Bytes::from(wire)]);
        let first = reader.next().await.expect("a message").expect("decodes");
        assert_eq!(&first[..], b"hello");
        reader.assert_stream_ended().await;
        reader.assert_drains_then_releases().await;
    }

    /// A client that trickles data during the drain cannot extend it: the
    /// deadline is absolute, not an idle timeout.
    #[tokio::test(start_paused = true)]
    async fn test_body_reader_drain_deadline_is_not_extended_by_trickled_data() {
        let (feed, rx) = tokio::sync::mpsc::unbounded_channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let mut reader = Reader::on(
            FedBody {
                rx,
                _dropped: DropFlag(Arc::clone(&dropped)),
            },
            dropped,
        );
        reader.handler_drops_stream();
        Reader::settle().await;

        for _ in 0..4 {
            feed.send(Bytes::from_static(&[0xAA; 8])).unwrap();
            tokio::time::advance(Duration::from_secs(1)).await;
            Reader::settle().await;
        }
        assert!(!reader.body_dropped(), "still draining before the deadline");
        tokio::time::advance(Duration::from_secs(2)).await;
        Reader::settle().await;
        assert!(reader.body_dropped(), "trickled data extended the drain");
        assert_no_drain_panicked();
    }

    /// A body that yields `wire` in frames of `chunk` bytes, then ends.
    fn chunked_body(wire: &[u8], chunk: usize) -> CountingBody {
        CountingBody::new(
            wire.chunks(chunk).map(Bytes::copy_from_slice),
            &Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
    }

    /// A large message that arrives over many transport frames reaches the
    /// handler as the sole owner of its allocation: the request stream keeps
    /// no buffer that shares it, and nothing is left buffered once it is out.
    #[tokio::test]
    async fn test_body_reader_large_message_owns_its_allocation() {
        let payload = Bytes::from(vec![0x42_u8; 1024 * 1024]);
        let wire = Envelope::data(payload.clone()).encode();
        let mut request_stream = decode_request_body(
            chunked_body(&wire, 16 * 1024),
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );

        let msg = request_stream
            .next()
            .await
            .expect("message")
            .expect("decodes");
        assert_eq!(msg, payload, "message must arrive intact");
        assert!(msg.is_unique(), "reader shares the message's allocation");
        assert!(
            request_stream.next().await.is_none(),
            "nothing left buffered"
        );
    }

    /// A compressed envelope split across frames is reassembled and
    /// decompressed.
    #[cfg(feature = "gzip")]
    #[tokio::test]
    async fn test_body_reader_compressed_message_across_frames() {
        let registry = Arc::new(CompressionRegistry::default());
        let payload = Bytes::from(vec![b'z'; 64 * 1024]);
        let compressed = registry
            .compress("gzip", &payload)
            .expect("gzip compresses");
        let wire = Envelope::compressed(compressed).encode();

        let mut request_stream = decode_request_body(
            chunked_body(&wire, 7),
            DEFAULT_MAX_MESSAGE_SIZE,
            Some("gzip".to_owned()),
            registry,
        );
        let msg = request_stream
            .next()
            .await
            .expect("message")
            .expect("decodes");
        assert_eq!(msg, payload);
        assert!(request_stream.next().await.is_none());
    }

    /// gRPC request streams have no END_STREAM envelope: the body simply
    /// ends. Messages are delivered however the frames slice them (two in one
    /// frame, one split across frames), the end of the body ends the stream,
    /// and nothing is left to drain, so the body is released there and then.
    #[tokio::test]
    async fn test_body_reader_frames_need_not_align_with_envelopes() {
        let mut wire = Vec::new();
        for payload in [&b"one"[..], b"two", b"three"] {
            wire.extend_from_slice(&Envelope::data(Bytes::copy_from_slice(payload)).encode());
        }
        // "one" + "two" + the header and first byte of "three" | the rest.
        let split = wire.len() - 4;
        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let body = CountingBody::new(
            [
                Bytes::copy_from_slice(&wire[..split]),
                Bytes::copy_from_slice(&wire[split..]),
            ],
            &pulled,
        );

        let mut request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );
        let mut got = Vec::new();
        while let Some(item) = request_stream.next().await {
            got.push(item.expect("message decodes"));
        }
        assert_eq!(got, [&b"one"[..], b"two", b"three"]);
        assert_eq!(
            Arc::strong_count(&pulled),
            1,
            "body released at its end, no drain involved"
        );
        assert_eq!(
            pulled.load(std::sync::atomic::Ordering::Relaxed),
            wire.len()
        );
    }

    /// Test body that replays a fixed sequence of frames (data or trailers)
    /// and can claim `is_end_stream()` from the start; records bytes pulled.
    struct ScriptedBody {
        frames: std::collections::VecDeque<Frame<Bytes>>,
        claims_end_stream: bool,
        pulled: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Body for ScriptedBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            let this = self.get_mut();
            let frame = this.frames.pop_front();
            if let Some(data) = frame.as_ref().and_then(Frame::data_ref) {
                this.pulled
                    .fetch_add(data.len(), std::sync::atomic::Ordering::Relaxed);
            }
            Poll::Ready(frame.map(Ok))
        }

        fn is_end_stream(&self) -> bool {
            self.claims_end_stream
        }
    }

    /// A trailers frame between the messages and the end of the body carries
    /// nothing for the decoder and is skipped.
    #[tokio::test]
    async fn test_body_reader_skips_trailers_frames() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-checksum", http::HeaderValue::from_static("abc"));
        let body = ScriptedBody {
            frames: [
                Frame::data(Envelope::data(Bytes::from_static(b"hello")).encode()),
                Frame::trailers(trailers),
            ]
            .into(),
            claims_end_stream: false,
            pulled: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let mut request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );
        assert_eq!(&request_stream.next().await.unwrap().unwrap()[..], b"hello");
        assert!(request_stream.next().await.is_none());
    }

    /// A body that already reports its end when the handler drops the
    /// request stream needs no drain: it is released on the spot.
    #[tokio::test]
    async fn test_body_reader_drop_with_body_at_end_of_stream_spawns_nothing() {
        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let body = ScriptedBody {
            frames: [Frame::data(Bytes::from_static(b"never read"))].into(),
            claims_end_stream: true,
            pulled: Arc::clone(&pulled),
        };
        let request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );
        drop(request_stream);
        assert_eq!(Arc::strong_count(&pulled), 1, "released synchronously");
        assert_eq!(pulled.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    /// A request stream dropped on a thread outside any runtime is still
    /// drained, on the runtime it was created on.
    #[tokio::test]
    async fn test_body_reader_dropped_off_runtime_drains_on_its_own() {
        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let body = CountingBody::new(
            [Envelope::data(Bytes::from_static(b"unread")).encode()],
            &pulled,
        );
        let request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );
        std::thread::spawn(move || drop(request_stream))
            .join()
            .expect("dropping the stream must not panic");
        body_released(&pulled).await;
        assert_eq!(
            pulled.load(std::sync::atomic::Ordering::Relaxed),
            Envelope::data(Bytes::from_static(b"unread")).encode().len(),
            "the body was drained"
        );
    }

    /// A request stream created and dropped outside any Tokio runtime
    /// releases the body instead of panicking for want of a runtime to drain
    /// it on.
    #[test]
    fn test_body_reader_dropped_outside_a_runtime_does_not_panic() {
        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let body = CountingBody::new(
            [Envelope::data(Bytes::from_static(b"unread")).encode()],
            &pulled,
        );
        let request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );
        drop(request_stream);
        assert_eq!(Arc::strong_count(&pulled), 1);
        assert_eq!(pulled.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    /// A body that ends part-way through an envelope surfaces
    /// `invalid_argument` to the handler rather than a clean end of stream.
    #[tokio::test]
    async fn test_body_reader_incomplete_envelope_at_eof() {
        let wire = Envelope::data(Bytes::from_static(b"hello world")).encode();
        let mut frames = std::collections::VecDeque::new();
        frames.push_back(wire.slice(..3));
        frames.push_back(wire.slice(3..9));
        let body = CountingBody::new(frames, &Arc::new(std::sync::atomic::AtomicUsize::new(0)));

        let mut request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );
        let err = request_stream
            .next()
            .await
            .expect("an item must be delivered")
            .expect_err("a truncated envelope is an error");
        assert_eq!(err.code, crate::error::ErrorCode::InvalidArgument);
        assert_eq!(err.message.as_deref(), Some("incomplete request envelope"));
        assert!(request_stream.next().await.is_none());
    }

    /// An END_STREAM envelope with no preceding messages (the typical
    /// "no client messages" request body) ends the request stream cleanly.
    #[tokio::test]
    async fn test_body_reader_end_stream_only() {
        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let end_stream_frame = Envelope::end_stream(Bytes::from_static(b"{}")).encode();
        let frame_len = end_stream_frame.len();
        let mut frames = std::collections::VecDeque::new();
        frames.push_back(end_stream_frame);

        let body = CountingBody::new(frames, &pulled);

        let mut request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );

        assert!(
            request_stream.next().await.is_none(),
            "request stream must end cleanly with no messages"
        );
        body_released(&pulled).await;
        assert_eq!(
            pulled.load(std::sync::atomic::Ordering::Relaxed),
            frame_len,
            "the reader consumes exactly the END_STREAM frame"
        );
    }

    /// Trailing junk that arrives in the same body frame as the END_STREAM
    /// envelope is discarded without surfacing an error to the handler.
    #[tokio::test]
    async fn test_body_reader_junk_in_same_frame_as_end_stream() {
        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut combined = Envelope::end_stream(Bytes::from_static(b"{}"))
            .encode()
            .to_vec();
        combined.extend_from_slice(&[0xAA_u8; 1024]);
        let frame = Bytes::from(combined);
        let frame_len = frame.len();
        let mut frames = std::collections::VecDeque::new();
        frames.push_back(frame);

        let body = CountingBody::new(frames, &pulled);

        let mut request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );

        assert!(
            request_stream.next().await.is_none(),
            "trailing junk after END_STREAM must not surface to the handler"
        );
        body_released(&pulled).await;
        assert_eq!(
            pulled.load(std::sync::atomic::Ordering::Relaxed),
            frame_len,
            "the reader consumes exactly the single combined frame"
        );
    }

    /// A data envelope followed by END_STREAM and EOF still delivers the
    /// message and then ends the request stream cleanly.
    #[tokio::test]
    async fn test_body_reader_message_then_end_stream() {
        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let data_frame = Envelope::data(Bytes::from_static(b"hello")).encode();
        let end_stream_frame = Envelope::end_stream(Bytes::from_static(b"{}")).encode();
        let total_len = data_frame.len() + end_stream_frame.len();
        let mut frames = std::collections::VecDeque::new();
        frames.push_back(data_frame);
        frames.push_back(end_stream_frame);

        let body = CountingBody::new(frames, &pulled);

        let mut request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );

        let first = request_stream
            .next()
            .await
            .expect("one message before END_STREAM")
            .expect("message decodes without error");
        assert_eq!(&first[..], b"hello");
        assert!(
            request_stream.next().await.is_none(),
            "request stream must end after END_STREAM"
        );

        body_released(&pulled).await;
        assert_eq!(
            pulled.load(std::sync::atomic::Ordering::Relaxed),
            total_len,
            "the reader consumes exactly the two frames"
        );
    }

    /// When the handler drops the request stream unread, the remaining body
    /// is drained, bounded by `MAX_DRAIN_BYTES`, rather than abandoned,
    /// buffered or decoded.
    #[tokio::test]
    async fn test_body_reader_bounds_data_after_stream_dropped() {
        const CHUNK_SIZE: usize = 64 * 1024;
        const JUNK_TOTAL: usize = 4 * 1024 * 1024;

        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // One decodable message followed by 4 MiB of junk.
        let mut frames = std::collections::VecDeque::new();
        frames.push_back(Envelope::data(Bytes::from_static(b"hello")).encode());
        for _ in 0..(JUNK_TOTAL / CHUNK_SIZE) {
            frames.push_back(Bytes::from(vec![0xAA_u8; CHUNK_SIZE]));
        }

        let body = CountingBody::new(frames, &pulled);

        let request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );
        // The handler gives up on the request stream immediately.
        drop(request_stream);

        body_released(&pulled).await;

        let pulled = pulled.load(std::sync::atomic::Ordering::Relaxed);
        let max_expected = MAX_DRAIN_BYTES + 2 * CHUNK_SIZE;
        assert!(
            pulled > MAX_DRAIN_BYTES && pulled <= max_expected,
            "reader pulled {pulled} bytes after the stream was dropped \
             (expected more than {MAX_DRAIN_BYTES} and at most {max_expected})"
        );
    }

    /// A message that exceeds `max_message_size` surfaces a single error to
    /// the handler, and the remaining body is drained bounded by
    /// `MAX_DRAIN_BYTES`.
    #[tokio::test]
    async fn test_body_reader_bounds_data_after_decode_error() {
        const MAX_MESSAGE_SIZE: usize = 1024;
        const CHUNK_SIZE: usize = 64 * 1024;
        const JUNK_TOTAL: usize = 4 * 1024 * 1024;

        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // An envelope header declaring a payload larger than the limit,
        // followed by 4 MiB of junk.
        let mut oversized = vec![0_u8; crate::envelope::HEADER_SIZE];
        oversized[1..].copy_from_slice(&4096_u32.to_be_bytes());
        let mut frames = std::collections::VecDeque::new();
        frames.push_back(Bytes::from(oversized));
        for _ in 0..(JUNK_TOTAL / CHUNK_SIZE) {
            frames.push_back(Bytes::from(vec![0xAA_u8; CHUNK_SIZE]));
        }

        let body = CountingBody::new(frames, &pulled);

        let mut request_stream = decode_request_body(
            body,
            MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );

        request_stream
            .next()
            .await
            .expect("an item must be delivered")
            .expect_err("the oversized message must surface as an error");
        assert!(
            request_stream.next().await.is_none(),
            "no further items after the decode error"
        );

        body_released(&pulled).await;

        let pulled = pulled.load(std::sync::atomic::Ordering::Relaxed);
        let max_expected = MAX_DRAIN_BYTES + 2 * CHUNK_SIZE;
        assert!(
            pulled > MAX_DRAIN_BYTES && pulled <= max_expected,
            "reader pulled {pulled} bytes after the decode error \
             (expected more than {MAX_DRAIN_BYTES} and at most {max_expected})"
        );
    }

    /// Test body that yields a fixed sequence of data frames, then fails with
    /// a single transport-level body error, then stays pending.
    struct ErrorAfterFramesBody {
        frames: std::collections::VecDeque<Bytes>,
        errored: bool,
        _dropped: DropFlag,
    }

    impl ErrorAfterFramesBody {
        fn new(frame: Bytes, dropped: &Arc<AtomicBool>) -> Self {
            count_panics();
            Self {
                frames: [frame].into(),
                errored: false,
                _dropped: DropFlag(Arc::clone(dropped)),
            }
        }
    }

    impl Body for ErrorAfterFramesBody {
        type Data = Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            let this = self.get_mut();

            if let Some(data) = this.frames.pop_front() {
                return Poll::Ready(Some(Ok(Frame::data(data))));
            }

            if !this.errored {
                this.errored = true;
                return Poll::Ready(Some(Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "simulated body read failure",
                ))));
            }

            Poll::Pending
        }
    }

    /// A request body that yields a valid streaming message and then fails at
    /// the transport level (while the reader is still decoding) must surface
    /// the failure to the handler as an internal error — not a clean EOF that
    /// is indistinguishable from a complete client stream.
    #[tokio::test]
    async fn test_body_reader_surfaces_body_error_while_decoding() {
        let dropped = Arc::new(AtomicBool::new(false));
        let body = ErrorAfterFramesBody::new(
            Envelope::data(Bytes::from_static(b"hello")).encode(),
            &dropped,
        );

        let mut request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );

        let first = request_stream
            .next()
            .await
            .expect("one message before the body error")
            .expect("message decodes before the body error");
        assert_eq!(&first[..], b"hello");

        let err = request_stream
            .next()
            .await
            .expect("body error must be delivered")
            .expect_err("body error must not be converted to clean EOF");

        assert_eq!(err.code, crate::error::ErrorCode::Internal);
        assert!(
            err.message
                .as_deref()
                .unwrap_or_default()
                .contains("failed to read request body: simulated body read failure"),
            "unexpected error message: {:?}",
            err.message
        );
        assert!(
            dropped.load(Ordering::Relaxed),
            "a failed body is released at once, not drained"
        );

        assert!(
            request_stream.next().await.is_none(),
            "no further items after the body error"
        );
    }

    /// Once the stream has finished decoding (here: a clean END_STREAM), a
    /// transport-level body error during the drain is diagnostic-only — the
    /// handler already observed the terminal end of the stream and must not
    /// then receive a spurious error — and ends the drain.
    #[tokio::test(start_paused = true)]
    async fn test_body_reader_body_error_after_end_stream_is_suppressed() {
        let dropped = Arc::new(AtomicBool::new(false));
        let body = ErrorAfterFramesBody::new(
            Envelope::end_stream(Bytes::from_static(b"{}")).encode(),
            &dropped,
        );

        let mut request_stream = decode_request_body(
            body,
            DEFAULT_MAX_MESSAGE_SIZE,
            None,
            Arc::new(CompressionRegistry::new()),
        );

        // END_STREAM ended the stream cleanly; the body error that follows is
        // met by the drain, so the handler sees a clean end with no error item.
        assert!(
            request_stream.next().await.is_none(),
            "a body error after END_STREAM must not surface to the handler"
        );
        // The body stays pending after its error: only a drain that stopped
        // at the error has released it without the clock moving.
        Reader::settle().await;
        assert!(
            dropped.load(Ordering::Relaxed),
            "the drain ends at the error"
        );
        assert_no_drain_panicked();
    }

    /// An error propagated out of a client call carries the upstream
    /// response's headers, and a gateway handler returns it as its own
    /// error. Echoing that block verbatim would put a second
    /// `content-type` on the wire and describe this response's body with
    /// the upstream's framing, so the server's own headers win and the
    /// unforwardable ones are dropped — while the server metadata the
    /// caller wanted forwarded still gets through.
    ///
    /// `content-length` is the one that actually breaks a client: hyper's
    /// HTTP/1 encoder refuses to serialize a response whose declared
    /// length contradicts the body, closing the connection with nothing
    /// written, so the caller sees a transport failure rather than the
    /// handler's error code.
    fn upstream_error() -> ConnectError {
        let mut upstream = http::HeaderMap::new();
        upstream.insert(
            header::CONTENT_TYPE,
            "application/grpc+proto".parse().unwrap(),
        );
        upstream.insert(header::CONTENT_LENGTH, "4096".parse().unwrap());
        upstream.insert(header::CONNECTION, "close".parse().unwrap());
        upstream.insert(header::TRANSFER_ENCODING, "chunked".parse().unwrap());
        upstream.insert(
            header::DATE,
            "Thu, 01 Jan 1970 00:00:00 GMT".parse().unwrap(),
        );
        upstream.insert(
            crate::protocol::hdr::GRPC_ENCODING.clone(),
            "gzip".parse().unwrap(),
        );
        // An upstream status proto whose code contradicts the one we send.
        upstream.insert(
            crate::protocol::hdr::GRPC_STATUS_DETAILS_BIN.clone(),
            "CAcSBW5vcGU".parse().unwrap(),
        );
        upstream.insert("x-upstream-region", "eu-west-1".parse().unwrap());
        upstream.append("x-upstream-tag", "a".parse().unwrap());
        upstream.append("x-upstream-tag", "b".parse().unwrap());

        let mut err = ConnectError::unavailable("upstream is down");
        err.set_response_headers(upstream);
        err
    }

    fn assert_safe_echo(headers: &http::HeaderMap, expected_content_type: &str) {
        assert_eq!(
            headers.get_all(header::CONTENT_TYPE).iter().count(),
            1,
            "exactly one content-type must reach the wire: {headers:?}"
        );
        assert_eq!(
            headers.get(header::CONTENT_TYPE).unwrap(),
            expected_content_type
        );
        for dropped in [
            header::CONTENT_LENGTH,
            header::CONNECTION,
            header::TRANSFER_ENCODING,
            header::DATE,
            crate::protocol::hdr::GRPC_ENCODING.clone(),
            crate::protocol::hdr::GRPC_STATUS_DETAILS_BIN.clone(),
        ] {
            assert!(
                !headers.contains_key(&dropped),
                "{dropped} describes the upstream response and must not be forwarded"
            );
        }

        // The point of attaching headers to an error is that this survives.
        assert_eq!(headers.get("x-upstream-region").unwrap(), "eu-west-1");
        let tags: Vec<_> = headers
            .get_all("x-upstream-tag")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(tags, ["a", "b"], "multi-valued metadata keeps every value");
    }

    #[test]
    fn propagated_upstream_headers_do_not_corrupt_the_connect_unary_response() {
        let response = error_response(upstream_error());
        assert_safe_echo(response.headers(), content_type::JSON);
    }

    #[test]
    fn propagated_upstream_headers_do_not_corrupt_the_connect_streaming_response() {
        let response =
            streaming_error_response(&upstream_error(), Protocol::Connect, CodecFormat::Proto);
        assert_safe_echo(
            response.headers(),
            Protocol::Connect.response_content_type(CodecFormat::Proto, true),
        );
    }

    #[test]
    fn propagated_upstream_headers_do_not_corrupt_the_grpc_streaming_response() {
        let response =
            streaming_error_response(&upstream_error(), Protocol::Grpc, CodecFormat::Proto);
        assert_safe_echo(
            response.headers(),
            Protocol::Grpc.response_content_type(CodecFormat::Proto, true),
        );
        // The trailers-only status headers are the server's own and stay.
        assert_eq!(response.headers().get(&GRPC_STATUS).unwrap(), "14");
    }

    /// Per-route limits: a route carrying its own `Limits` is held to them on
    /// every dispatch path, other routes keep the service-wide limits, and a
    /// route may be looser than the service as well as tighter.
    mod route_limits {
        use super::*;
        use buffa_types::google::protobuf::StringValue;

        const TIGHT: usize = 64;
        /// gRPC status text for `resource_exhausted`.
        fn exhausted() -> String {
            crate::ErrorCode::ResourceExhausted.grpc_code().to_string()
        }

        fn echo() -> impl crate::Handler<StringValue, StringValue> {
            crate::handler_fn(|_ctx: RequestContext, req: StringValue| async move {
                crate::Response::ok(req)
            })
        }

        fn echo_stream() -> impl crate::handler::StreamingHandler<StringValue, StringValue> {
            crate::handler::streaming_handler_fn(
                |_ctx: RequestContext, req: StringValue| async move {
                    crate::Response::stream_ok(futures::stream::iter([Ok(req)]))
                },
            )
        }

        /// `svc/Tight` and `svc/TightStream` are limited to `TIGHT` bytes;
        /// `svc/Default` uses whatever the service is configured with.
        fn router() -> Arc<Router> {
            let tight = Limits::default()
                .with_max_message_size(TIGHT)
                .with_max_request_body_size(TIGHT);
            Arc::new(
                Router::new()
                    .route("svc", "Tight", echo())
                    .route("svc", "Default", echo())
                    .route_server_stream("svc", "TightStream", echo_stream())
                    .with_route_limits("/svc/Tight", tight)
                    .with_route_limits("svc/TightStream", tight),
            )
        }

        fn message(len: usize) -> Bytes {
            crate::codec::encode_proto(&StringValue {
                value: "x".repeat(len),
                ..Default::default()
            })
            .unwrap()
        }

        fn enveloped(msg: Bytes) -> Bytes {
            Envelope::data(msg).encode()
        }

        fn post(path: &str, content_type: &str, body: Bytes) -> Request<Full<Bytes>> {
            Request::builder()
                .method(Method::POST)
                .uri(path)
                .header(header::CONTENT_TYPE, content_type)
                .body(Full::new(body))
                .unwrap()
        }

        async fn call<B>(
            router: &Arc<Router>,
            service_limits: Limits,
            req: Request<B>,
        ) -> Result<Response<ConnectRpcBody>, ConnectError>
        where
            B: Body<Data = Bytes> + Send + 'static,
            B::Error: std::error::Error + Send + Sync + 'static,
        {
            handle_request(
                Arc::clone(router),
                req,
                service_limits,
                Arc::new(CompressionRegistry::new()),
                &CompressionPolicy::default(),
                &DeadlinePolicy::new(),
                &[],
            )
            .await
        }

        /// `grpc-status` from the response headers (a trailers-only error) —
        /// `None` for a response that carries messages.
        fn grpc_status_header(resp: &Response<ConnectRpcBody>) -> Option<String> {
            resp.headers()
                .get("grpc-status")
                .map(|v| v.to_str().unwrap().to_owned())
        }

        /// `grpc-status` from the body trailers of a response that started
        /// streaming.
        async fn grpc_status_trailer(resp: Response<ConnectRpcBody>) -> Option<String> {
            let body = resp.into_body().collect().await.unwrap();
            body.trailers()
                .and_then(|t| t.get("grpc-status"))
                .map(|v| v.to_str().unwrap().to_owned())
        }

        #[tokio::test]
        async fn connect_unary_route_limit_rejects_oversized_and_spares_other_routes() {
            let router = router();
            let big = message(2 * TIGHT);
            let default = Limits::default();

            let err = call(
                &router,
                default,
                post("/svc/Tight", "application/proto", big.clone()),
            )
            .await
            .err()
            .expect("route limit must reject the oversized body");
            assert_eq!(err.code, crate::ErrorCode::ResourceExhausted);

            let ok = call(
                &router,
                default,
                post("/svc/Default", "application/proto", big),
            )
            .await
            .expect("service-wide limits admit it on another route");
            assert_eq!(ok.status(), StatusCode::OK);

            let ok = call(
                &router,
                default,
                post("/svc/Tight", "application/proto", message(8)),
            )
            .await
            .expect("a request within the route limit is served");
            assert_eq!(ok.status(), StatusCode::OK);
        }

        #[tokio::test]
        async fn grpc_unary_fast_path_honours_route_limit() {
            let router = router();
            let big = enveloped(message(2 * TIGHT));
            let default = Limits::default();

            let resp = call(
                &router,
                default,
                post("/svc/Tight", "application/grpc", big.clone()),
            )
            .await
            .expect("gRPC errors are trailers-only responses");
            assert_eq!(grpc_status_header(&resp), Some(exhausted()));

            let resp = call(
                &router,
                default,
                post("/svc/Default", "application/grpc", big),
            )
            .await
            .unwrap();
            assert_eq!(grpc_status_header(&resp), None);
            assert_eq!(grpc_status_trailer(resp).await.as_deref(), Some("0"));
        }

        #[tokio::test]
        async fn streaming_path_honours_route_limit() {
            let router = router();
            let default = Limits::default();

            let big = enveloped(message(2 * TIGHT));
            let resp = call(
                &router,
                default,
                post("/svc/TightStream", "application/grpc", big),
            )
            .await
            .expect("gRPC errors are trailers-only responses");
            assert_eq!(grpc_status_header(&resp), Some(exhausted()));

            let small = enveloped(message(8));
            let resp = call(
                &router,
                default,
                post("/svc/TightStream", "application/grpc", small),
            )
            .await
            .unwrap();
            assert_eq!(grpc_status_header(&resp), None);
            assert_eq!(grpc_status_trailer(resp).await.as_deref(), Some("0"));
        }

        /// The route's limits replace the service-wide ones outright, so a
        /// route can admit what the rest of the service refuses.
        #[tokio::test]
        async fn route_limit_can_loosen_service_limit() {
            let service_tight = Limits::default()
                .with_max_message_size(TIGHT)
                .with_max_request_body_size(TIGHT);
            let router = Arc::new(
                Router::new()
                    .route("svc", "Upload", echo())
                    .route("svc", "Default", echo())
                    .with_route_limits("/svc/Upload", Limits::default()),
            );
            let big = message(2 * TIGHT);

            let ok = call(
                &router,
                service_tight,
                post("/svc/Upload", "application/proto", big.clone()),
            )
            .await
            .expect("the route's own (default) limits admit it");
            assert_eq!(ok.status(), StatusCode::OK);

            let err = call(
                &router,
                service_tight,
                post("/svc/Default", "application/proto", big),
            )
            .await
            .err()
            .expect("the service-wide limit still governs other routes");
            assert_eq!(err.code, crate::ErrorCode::ResourceExhausted);
        }

        /// Bytes the server read from a 64 × 1 KiB body before answering `req`.
        async fn bytes_polled(router: &Arc<Router>, req: http::request::Builder) -> usize {
            let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let body = CountingBody::new(
                std::iter::repeat_n(Bytes::from(vec![0u8; 1024]), 64),
                &pulled,
            );
            let _ = call(router, Limits::default(), req.body(body).unwrap()).await;
            pulled.load(std::sync::atomic::Ordering::Relaxed)
        }

        /// Requests refused before dispatch — wrong verb, unknown content
        /// type, unsupported encoding — still drain the body for keep-alive,
        /// and on a route with its own limits that drain stops at the route's
        /// limit (after the first 1 KiB frame here) rather than the
        /// service-wide one (all 64 KiB).
        #[tokio::test]
        async fn pre_dispatch_error_drains_honour_route_limit() {
            let router = router();
            let put = || Request::builder().method(Method::PUT);
            let post = || Request::builder().method(Method::POST);

            // 405: verb not allowed.
            assert_eq!(bytes_polled(&router, put().uri("/svc/Tight")).await, 1024);
            assert_eq!(
                bytes_polled(&router, put().uri("/svc/Default")).await,
                64 * 1024
            );
            // 415: unknown content type.
            let unknown_ct =
                |b: http::request::Builder| b.header(header::CONTENT_TYPE, "application/foo");
            assert_eq!(
                bytes_polled(&router, unknown_ct(post().uri("/svc/Tight"))).await,
                1024
            );
            assert_eq!(
                bytes_polled(&router, unknown_ct(post().uri("/svc/Default"))).await,
                64 * 1024
            );
            // Unsupported request compression on a streaming route.
            let bogus_encoding = |b: http::request::Builder| {
                b.header(header::CONTENT_TYPE, "application/grpc")
                    .header("grpc-encoding", "bogus")
            };
            assert_eq!(
                bytes_polled(&router, bogus_encoding(post().uri("/svc/TightStream"))).await,
                1024
            );
            assert_eq!(
                bytes_polled(&router, bogus_encoding(post().uri("/svc/Default"))).await,
                64 * 1024
            );
        }

        #[tokio::test]
        async fn connect_get_honours_route_limit() {
            use base64::Engine as _;
            let tight = Limits::default().with_max_message_size(TIGHT);
            let router = Arc::new(
                Router::new()
                    .route_idempotent("svc", "TightGet", echo())
                    .route_idempotent("svc", "DefaultGet", echo())
                    .with_route_limits("svc/TightGet", tight),
            );
            let msg = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(message(2 * TIGHT));
            let get = |path: &str| {
                Request::builder()
                    .method(Method::GET)
                    .uri(format!(
                        "{path}?encoding=proto&base64=1&connect=v1&message={msg}"
                    ))
                    .body(Full::new(Bytes::new()))
                    .unwrap()
            };
            let err = call(&router, Limits::default(), get("/svc/TightGet"))
                .await
                .err()
                .expect("over the route limit");
            assert_eq!(err.code, crate::ErrorCode::ResourceExhausted);
            let ok = call(&router, Limits::default(), get("/svc/DefaultGet"))
                .await
                .unwrap();
            assert_eq!(ok.status(), StatusCode::OK);
        }

        #[tokio::test]
        async fn client_streaming_path_honours_route_limit() {
            let tight = Limits::default().with_max_message_size(TIGHT);
            let concat = crate::handler::client_streaming_handler_fn(
                |_ctx: RequestContext, mut reqs: crate::ServiceStream<StringValue>| async move {
                    use futures::StreamExt as _;
                    let mut out = String::new();
                    while let Some(r) = reqs.next().await {
                        out.push_str(&r?.value);
                    }
                    crate::Response::ok(StringValue {
                        value: out,
                        ..Default::default()
                    })
                },
            );
            let router = Arc::new(
                Router::new()
                    .route_client_stream("svc", "Concat", concat)
                    .with_route_limits("svc/Concat", tight),
            );
            let mut two = enveloped(message(8)).to_vec();
            two.extend_from_slice(&enveloped(message(2 * TIGHT)));
            let resp = call(
                &router,
                Limits::default(),
                post("/svc/Concat", "application/grpc", Bytes::from(two)),
            )
            .await
            .unwrap();
            // The first message is within the limit; the second is not, and
            // the error arrives in the trailers of an already-started stream.
            assert_eq!(grpc_status_trailer(resp).await, Some(exhausted()));
        }

        /// The third `Limits` field, the decode budget, follows the route too:
        /// a message that is tiny on the wire but allocates 64 elements is
        /// refused on a route whose budget is one byte and served elsewhere.
        #[tokio::test]
        async fn route_limit_carries_the_element_budget() {
            use buffa_types::google::protobuf::{ListValue, Value};
            let count = || {
                crate::handler_fn(|_ctx: RequestContext, req: ListValue| async move {
                    crate::Response::ok(StringValue {
                        value: req.values.len().to_string(),
                        ..Default::default()
                    })
                })
            };
            let router = Arc::new(
                Router::new()
                    .route("svc", "TightCount", count())
                    .route("svc", "Count", count())
                    .with_route_limits(
                        "svc/TightCount",
                        Limits::default().with_element_memory_limit(1),
                    ),
            );
            let list = Bytes::from(buffa::Message::encode_to_vec(&ListValue {
                values: (0..64).map(|_| Value::default()).collect(),
                ..Default::default()
            }));
            let err = call(
                &router,
                Limits::default(),
                post("/svc/TightCount", "application/proto", list.clone()),
            )
            .await
            .err()
            .expect("over the route's element budget");
            assert_eq!(err.code, crate::ErrorCode::InvalidArgument);
            let ok = call(
                &router,
                Limits::default(),
                post("/svc/Count", "application/proto", list),
            )
            .await
            .unwrap();
            assert_eq!(ok.status(), StatusCode::OK);
        }

        /// The lookup runs before the body read, but a miss must still surface
        /// as `unimplemented` after the drain, not as a limits error.
        #[tokio::test]
        async fn unknown_route_still_reports_not_found() {
            let router = router();
            let err = call(
                &router,
                Limits::default(),
                post("/svc/Missing", "application/proto", message(8)),
            )
            .await
            .err()
            .expect("miss");
            assert_eq!(err.code, crate::ErrorCode::Unimplemented);
        }
    }

    /// The request's header map is moved into the handler's `RequestContext`
    /// on every dispatch path. Each path takes the request apart at a
    /// different point, so each is driven separately; client- and
    /// bidi-streaming receive the parsed metadata from
    /// `handle_streaming_request` rather than the request itself.
    mod request_headers_reach_handler {
        use super::*;
        use buffa_types::google::protobuf::StringValue;
        use std::sync::Mutex;

        const PROBE: &str = "x-probe";

        type Seen = Arc<Mutex<Vec<Option<String>>>>;

        fn record(seen: &Seen, ctx: &RequestContext) {
            seen.lock().unwrap().push(
                ctx.header(PROBE)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned),
            );
        }

        fn router(seen: &Seen) -> Arc<Router> {
            let unary = {
                let seen = Arc::clone(seen);
                crate::handler_fn(move |ctx: RequestContext, req: StringValue| {
                    record(&seen, &ctx);
                    async move { crate::Response::ok(req) }
                })
            };
            let unary_get = {
                let seen = Arc::clone(seen);
                crate::handler_fn(move |ctx: RequestContext, req: StringValue| {
                    record(&seen, &ctx);
                    async move { crate::Response::ok(req) }
                })
            };
            let server_stream = {
                let seen = Arc::clone(seen);
                crate::handler::streaming_handler_fn(
                    move |ctx: RequestContext, req: StringValue| {
                        record(&seen, &ctx);
                        async move { crate::Response::stream_ok(futures::stream::iter([Ok(req)])) }
                    },
                )
            };
            let client_stream = {
                let seen = Arc::clone(seen);
                crate::handler::client_streaming_handler_fn(
                    move |ctx: RequestContext, _reqs: crate::ServiceStream<StringValue>| {
                        record(&seen, &ctx);
                        async move { crate::Response::ok(StringValue::default()) }
                    },
                )
            };
            let bidi = {
                let seen = Arc::clone(seen);
                crate::handler::bidi_streaming_handler_fn(
                    move |ctx: RequestContext, reqs: crate::ServiceStream<StringValue>| {
                        record(&seen, &ctx);
                        async move { crate::Response::stream_ok(reqs) }
                    },
                )
            };
            Arc::new(
                Router::new()
                    .route("svc", "Unary", unary)
                    .route_idempotent("svc", "Get", unary_get)
                    .route_server_stream("svc", "ServerStream", server_stream)
                    .route_client_stream("svc", "ClientStream", client_stream)
                    .route_bidi_stream("svc", "Bidi", bidi),
            )
        }

        fn message() -> Bytes {
            crate::codec::encode_proto(&StringValue {
                value: "v".into(),
                ..Default::default()
            })
            .unwrap()
        }

        fn post(path: &str, content_type: &str, body: Bytes, probe: &str) -> Request<Full<Bytes>> {
            Request::builder()
                .method(Method::POST)
                .uri(path)
                .header(header::CONTENT_TYPE, content_type)
                .header(PROBE, probe)
                .body(Full::new(body))
                .unwrap()
        }

        async fn call(router: &Arc<Router>, req: Request<Full<Bytes>>) {
            let resp = handle_request(
                Arc::clone(router),
                req,
                Limits::default(),
                Arc::new(CompressionRegistry::new()),
                &CompressionPolicy::default(),
                &DeadlinePolicy::new(),
                &[],
            )
            .await
            .expect("dispatch succeeds");
            assert_eq!(resp.status(), StatusCode::OK);
            // Drive the body so streaming handlers run to completion.
            let _ = resp.into_body().collect().await;
        }

        #[tokio::test]
        async fn on_every_dispatch_path() {
            use base64::Engine as _;
            let seen: Seen = Arc::default();
            let router = router(&seen);
            let enveloped = Envelope::data(message()).encode();

            call(
                &router,
                post(
                    "/svc/Unary",
                    "application/proto",
                    message(),
                    "connect-unary",
                ),
            )
            .await;
            let get = Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/svc/Get?encoding=proto&base64=1&connect=v1&message={}",
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(message())
                ))
                .header(PROBE, "connect-get")
                .body(Full::new(Bytes::new()))
                .unwrap();
            call(&router, get).await;
            call(
                &router,
                post(
                    "/svc/Unary",
                    "application/grpc",
                    enveloped.clone(),
                    "grpc-unary",
                ),
            )
            .await;
            call(
                &router,
                post(
                    "/svc/ServerStream",
                    "application/grpc",
                    enveloped.clone(),
                    "server-stream",
                ),
            )
            .await;
            call(
                &router,
                post(
                    "/svc/ClientStream",
                    "application/grpc",
                    enveloped.clone(),
                    "client-stream",
                ),
            )
            .await;
            call(
                &router,
                post("/svc/Bidi", "application/grpc", enveloped, "bidi"),
            )
            .await;

            assert_eq!(
                *seen.lock().unwrap(),
                [
                    "connect-unary",
                    "connect-get",
                    "grpc-unary",
                    "server-stream",
                    "client-stream",
                    "bidi"
                ]
                .map(|s| Some(s.to_owned())),
            );
        }
    }
}

#[cfg(test)]
mod intercept_head_tests {
    use super::*;
    use crate::interceptor::Interceptor;
    use crate::spec::{Spec, StreamType};
    use crate::{ServiceStream, client_streaming_handler_fn, handler_fn};
    use buffa_types::Empty;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    const UNARY: Spec = Spec::server("/svc/Unary", StreamType::Unary);

    /// A request body that counts how often it is polled.
    struct CountedBody(Arc<AtomicUsize>);

    impl Body for CountedBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(None)
        }
    }

    type Log = Arc<Mutex<Vec<&'static str>>>;

    #[derive(Clone, Copy)]
    enum Verdict {
        Accept,
        Reject,
    }

    /// Records its name in `log`, then accepts or rejects in `intercept_head`.
    struct Head {
        name: &'static str,
        verdict: Verdict,
        log: Log,
    }

    #[async_trait::async_trait]
    impl Interceptor for Head {
        async fn intercept_head(&self, _head: &mut RequestHead<'_>) -> Result<(), ConnectError> {
            self.log.lock().unwrap().push(self.name);
            match self.verdict {
                Verdict::Accept => Ok(()),
                Verdict::Reject => Err(ConnectError::unauthenticated("no credential")),
            }
        }
    }

    fn head(name: &'static str, verdict: Verdict, log: &Log) -> Arc<dyn Interceptor> {
        Arc::new(Head {
            name,
            verdict,
            log: Arc::clone(log),
        })
    }

    /// One handler of each kind; each records that it ran.
    fn router(ran: &Arc<AtomicBool>) -> Router {
        let unary = Arc::clone(ran);
        let get = Arc::clone(ran);
        let client = Arc::clone(ran);
        Router::new()
            .route(
                "svc",
                "Unary",
                handler_fn(move |_ctx: RequestContext, _req: Empty| {
                    unary.store(true, Ordering::SeqCst);
                    async { crate::Response::ok(Empty::default()) }
                }),
            )
            .with_spec(UNARY)
            .route_idempotent(
                "svc",
                "Get",
                handler_fn(move |_ctx: RequestContext, _req: Empty| {
                    get.store(true, Ordering::SeqCst);
                    async { crate::Response::ok(Empty::default()) }
                }),
            )
            .route_client_stream(
                "svc",
                "Client",
                client_streaming_handler_fn(
                    move |_ctx: RequestContext, requests: ServiceStream<Empty>| {
                        client.store(true, Ordering::SeqCst);
                        async move {
                            drop(requests);
                            crate::Response::ok(Empty::default())
                        }
                    },
                ),
            )
    }

    async fn dispatch(
        router: Router,
        interceptors: &[Arc<dyn Interceptor>],
        req: Request<CountedBody>,
    ) -> Result<Response<ConnectRpcBody>, ConnectError> {
        handle_request(
            Arc::new(router),
            req,
            Limits::default(),
            Arc::new(CompressionRegistry::new()),
            &CompressionPolicy::default(),
            &DeadlinePolicy::new(),
            interceptors,
        )
        .await
    }

    fn post(path: &str, content_type: &str, polls: &Arc<AtomicUsize>) -> Request<CountedBody> {
        Request::post(path)
            .header(header::CONTENT_TYPE, content_type)
            .body(CountedBody(Arc::clone(polls)))
            .unwrap()
    }

    /// How a rejection reaches the client in each protocol.
    #[derive(Clone, Copy)]
    enum Reply {
        /// Connect unary: an HTTP error status.
        HttpStatus,
        /// gRPC: `grpc-status: 16` in the trailers.
        GrpcTrailer,
        /// gRPC-Web: a trailers frame in the body carrying `grpc-status: 16`.
        GrpcWebFrame,
        /// Connect streaming: an end-of-stream envelope carrying the code.
        ConnectEndStream,
    }

    async fn assert_rejected(result: Result<Response<ConnectRpcBody>, ConnectError>, reply: Reply) {
        match reply {
            Reply::HttpStatus => {
                let Err(err) = result else {
                    panic!("a Connect unary rejection is an error");
                };
                assert_eq!(err.http_status(), StatusCode::UNAUTHORIZED);
            }
            Reply::GrpcTrailer => {
                let response = result.expect("a gRPC rejection is a response");
                let collected = response.into_body().collect().await.unwrap();
                let trailers = collected.trailers().expect("gRPC status trailers");
                assert_eq!(trailers.get("grpc-status").unwrap(), "16");
            }
            Reply::GrpcWebFrame => {
                let response = result.expect("a gRPC-Web rejection is a response");
                let body = response.into_body().collect().await.unwrap().to_bytes();
                assert!(
                    String::from_utf8_lossy(&body).contains("grpc-status: 16"),
                    "the trailers frame must carry the code: {body:?}"
                );
            }
            Reply::ConnectEndStream => {
                let response = result.expect("a Connect streaming rejection is a response");
                let body = response.into_body().collect().await.unwrap().to_bytes();
                assert!(
                    String::from_utf8_lossy(&body).contains("unauthenticated"),
                    "the end-of-stream envelope must carry the code: {body:?}"
                );
            }
        }
    }

    /// A rejection in `intercept_head` must leave the body unread and the
    /// handler unrun, in every dispatch shape, and reach the client in that
    /// shape's own error format. In the streaming shapes a request stream
    /// dropped after it was created would drain the body, so the polls are
    /// counted after the runtime has had time to run a spawned drain. The
    /// gRPC-Web text-mode shape is one the service rejects with its own drain
    /// unless the hook runs first.
    #[tokio::test]
    async fn head_rejection_reads_no_body_and_runs_no_handler() {
        const SHAPES: [(&str, &str, &str, Reply); 6] = [
            (
                "connect unary",
                "/svc/Unary",
                "application/proto",
                Reply::HttpStatus,
            ),
            (
                "grpc unary",
                "/svc/Unary",
                "application/grpc+proto",
                Reply::GrpcTrailer,
            ),
            (
                "connect client stream",
                "/svc/Client",
                "application/connect+proto",
                Reply::ConnectEndStream,
            ),
            (
                "grpc client stream",
                "/svc/Client",
                "application/grpc+proto",
                Reply::GrpcTrailer,
            ),
            (
                "grpc-web client stream",
                "/svc/Client",
                "application/grpc-web+proto",
                Reply::GrpcWebFrame,
            ),
            (
                "grpc-web text mode",
                "/svc/Client",
                "application/grpc-web-text+proto",
                Reply::GrpcWebFrame,
            ),
        ];

        for (name, path, content_type, reply) in SHAPES {
            let polls = Arc::new(AtomicUsize::new(0));
            let ran = Arc::new(AtomicBool::new(false));
            let log = Log::default();
            let chain = [head("gate", Verdict::Reject, &log)];

            let result = dispatch(router(&ran), &chain, post(path, content_type, &polls)).await;
            for _ in 0..YIELDS {
                tokio::task::yield_now().await;
            }

            assert_rejected(result, reply).await;
            assert_eq!(polls.load(Ordering::SeqCst), 0, "{name}: body was read");
            assert!(!ran.load(Ordering::SeqCst), "{name}: handler ran");
            assert_eq!(*log.lock().unwrap(), ["gate"], "{name}");
        }

        // Connect GET has no content type, so it gets its own request.
        let polls = Arc::new(AtomicUsize::new(0));
        let ran = Arc::new(AtomicBool::new(false));
        let log = Log::default();
        let chain = [head("gate", Verdict::Reject, &log)];
        let req = Request::get("/svc/Get?message=&encoding=proto&base64=1&connect=v1")
            .body(CountedBody(Arc::clone(&polls)))
            .unwrap();
        let result = dispatch(router(&ran), &chain, req).await;
        assert_rejected(result, Reply::HttpStatus).await;
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "connect get: body was read"
        );
        assert!(!ran.load(Ordering::SeqCst), "connect get: handler ran");
    }

    /// Enough turns for a spawned drain to poll the body.
    const YIELDS: usize = 5;

    /// Every interceptor's head check runs, outermost first, until one
    /// rejects; interceptors after the rejecting one are not consulted.
    #[tokio::test]
    async fn head_checks_run_in_registration_order_and_stop_at_the_first_error() {
        let polls = Arc::new(AtomicUsize::new(0));
        let ran = Arc::new(AtomicBool::new(false));
        let log = Log::default();
        let chain = [
            head("first", Verdict::Accept, &log),
            head("second", Verdict::Reject, &log),
            head("third", Verdict::Accept, &log),
        ];

        let result = dispatch(
            router(&ran),
            &chain,
            post("/svc/Unary", "application/proto", &polls),
        )
        .await;

        assert_rejected(result, Reply::HttpStatus).await;
        assert_eq!(*log.lock().unwrap(), ["first", "second"]);
    }

    /// What the head shows for one request.
    #[derive(Debug, PartialEq)]
    struct Seen {
        path: String,
        spec: Option<Spec>,
        protocol: Protocol,
        authorization: Option<String>,
        peer: Option<Peer>,
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    struct Peer(u32);

    struct Inspect(Arc<Mutex<Option<Seen>>>);

    #[async_trait::async_trait]
    impl Interceptor for Inspect {
        async fn intercept_head(&self, head: &mut RequestHead<'_>) -> Result<(), ConnectError> {
            *self.0.lock().unwrap() = Some(Seen {
                path: head.path().to_owned(),
                spec: head.spec(),
                protocol: head.protocol(),
                authorization: head
                    .header("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned),
                peer: head.extensions().get::<Peer>().copied(),
            });
            Ok(())
        }
    }

    /// Dispatch `req` through an accepting `Inspect` and return what it saw.
    async fn inspect(
        req: Request<CountedBody>,
    ) -> (Option<Seen>, Result<StatusCode, ConnectError>) {
        let seen = Arc::new(Mutex::new(None));
        let chain: [Arc<dyn Interceptor>; 1] = [Arc::new(Inspect(Arc::clone(&seen)))];
        let ran = Arc::new(AtomicBool::new(false));
        let status = dispatch(router(&ran), &chain, req)
            .await
            .map(|response| response.status());
        (seen.lock().unwrap().take(), status)
    }

    /// The head shows the path, the resolved `Spec`, the headers and the
    /// transport's extensions, and accepting it lets the call run.
    #[tokio::test]
    async fn head_exposes_the_request_and_an_accepted_call_proceeds() {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut req = post("/svc/Unary", "application/proto", &polls);
        req.headers_mut()
            .insert(header::AUTHORIZATION, "Bearer token".parse().unwrap());
        req.extensions_mut().insert(Peer(7));

        let (seen, status) = inspect(req).await;

        assert_eq!(
            status.expect("an accepted call is dispatched"),
            StatusCode::OK
        );
        assert_eq!(
            seen,
            Some(Seen {
                path: "/svc/Unary".to_owned(),
                spec: Some(UNARY),
                protocol: Protocol::Connect,
                authorization: Some("Bearer token".to_owned()),
                peer: Some(Peer(7)),
            })
        );
    }

    /// The protocol the head reports follows the request's content type.
    #[tokio::test]
    async fn head_reports_the_protocol_of_the_request() {
        for (path, content_type, expected) in [
            ("/svc/Unary", "application/proto", Protocol::Connect),
            (
                "/svc/Client",
                "application/connect+proto",
                Protocol::Connect,
            ),
            ("/svc/Unary", "application/grpc+proto", Protocol::Grpc),
            (
                "/svc/Client",
                "application/grpc-web+proto",
                Protocol::GrpcWeb,
            ),
        ] {
            let polls = Arc::new(AtomicUsize::new(0));
            let (seen, _) = inspect(post(path, content_type, &polls)).await;
            let seen = seen.unwrap_or_else(|| panic!("{content_type}: the hook did not run"));
            assert_eq!(seen.protocol, expected, "{content_type}");
        }
    }

    /// A path that matches no method still reaches the head, with no `Spec`,
    /// so a rejecting hook answers before the not-found error does.
    #[tokio::test]
    async fn head_runs_for_an_unknown_path_before_the_not_found_error() {
        let polls = Arc::new(AtomicUsize::new(0));
        let (seen, status) = inspect(post("/svc/Missing", "application/proto", &polls)).await;
        let seen = seen.expect("the hook must run for an unknown path");
        assert_eq!((seen.path.as_str(), seen.spec), ("/svc/Missing", None));
        assert_eq!(status.unwrap_err().http_status(), StatusCode::NOT_FOUND);

        let ran = Arc::new(AtomicBool::new(false));
        let log = Log::default();
        let chain = [head("gate", Verdict::Reject, &log)];
        let result = dispatch(
            router(&ran),
            &chain,
            post("/svc/Missing", "application/proto", &polls),
        )
        .await;
        assert_rejected(result, Reply::HttpStatus).await;
    }

    /// A value a head check inserts reaches the handler through the request
    /// context, so authentication can hand the caller's identity on.
    #[tokio::test]
    async fn head_extensions_reach_the_handler() {
        #[derive(Clone)]
        struct Caller(&'static str);

        struct Authenticate;

        #[async_trait::async_trait]
        impl Interceptor for Authenticate {
            async fn intercept_head(&self, head: &mut RequestHead<'_>) -> Result<(), ConnectError> {
                head.extensions_mut().insert(Caller("alice"));
                Ok(())
            }
        }

        let seen = Arc::new(Mutex::new(None));
        let handler_seen = Arc::clone(&seen);
        let router = Router::new().route(
            "svc",
            "Unary",
            handler_fn(move |ctx: RequestContext, _req: Empty| {
                *handler_seen.lock().unwrap() = ctx.extensions().get::<Caller>().map(|c| c.0);
                async { crate::Response::ok(Empty::default()) }
            }),
        );
        let chain: [Arc<dyn Interceptor>; 1] = [Arc::new(Authenticate)];
        let polls = Arc::new(AtomicUsize::new(0));

        dispatch(
            router,
            &chain,
            post("/svc/Unary", "application/proto", &polls),
        )
        .await
        .expect("dispatch should succeed");

        assert_eq!(*seen.lock().unwrap(), Some("alice"));
    }
}
