//! Tower service integration for ConnectRPC.
//!
//! This module provides a [`tower::Service`] implementation that allows
//! ConnectRPC handlers to be integrated into existing web servers.
//!
//! # Example with hyper
//!
//! ```rust,ignore
//! use connectrpc::{Router, ConnectRpcService};
//! use hyper::service::service_fn;
//!
//! let router = Router::new()
//!     .route("example.v1.GreetService", "Greet", greet_handler);
//!
//! let service = router.into_service();
//! // Use with hyper or any tower-compatible framework
//! ```
//!
//! # Example with axum (requires `axum` feature)
//!
//! ```rust,ignore
//! use axum::{Router, routing::get};
//! use connectrpc::Router as ConnectRouter;
//!
//! let connect_router = ConnectRouter::new()
//!     .route("example.v1.GreetService", "Greet", greet_handler);
//!
//! let app = Router::new()
//!     .route("/health", get(health_handler))
//!     .merge(connect_router.into_axum_router());
//! ```

use std::convert::Infallible;
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
use crate::dispatcher::Dispatcher;
use crate::envelope::Envelope;
use crate::error::ConnectError;
use crate::handler::BoxStream;
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
    /// The detected protocol. Used for protocol-specific behavior in handlers.
    #[allow(dead_code)]
    protocol: Protocol,
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
    /// The original request headers (for passing to handlers).
    headers: http::HeaderMap,
}

impl RequestMetadata {
    /// Extract metadata from request headers, using the detected protocol to
    /// determine which header names to read.
    fn from_headers(headers: &http::HeaderMap, protocol: Protocol) -> Self {
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        let timeout = headers
            .get(protocol.timeout_header())
            .and_then(|v| v.to_str().ok())
            .and_then(|s| parse_timeout(s, protocol));

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
            protocol,
            timeout,
            unary_encoding,
            streaming_encoding,
            unary_accept_encoding,
            streaming_accept_encoding,
            protocol_version,
            headers: headers.clone(),
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
/// compressed). Set `max_request_body_size >= max_message_size` to allow
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
#[derive(Debug, Clone)]
pub struct Limits {
    /// Maximum size of the request body on the wire (before decompression).
    ///
    /// Applies to RPCs where the body is read in full (unary and server
    /// streaming). Should be at least `max_message_size` to allow
    /// uncompressed messages up to the message limit. Does not apply to
    /// client/bidi streaming where messages are processed incrementally.
    ///
    /// Default: 4 MB (matches tonic/grpc-go).
    pub max_request_body_size: usize,

    /// Maximum size of a single message after decompression.
    ///
    /// This applies uniformly to both unary and streaming RPCs, and to both
    /// compressed and uncompressed messages. Compressed payloads are bounded
    /// during decompression — the decompressor will never allocate more than
    /// this limit.
    ///
    /// Default: 4 MB.
    pub max_message_size: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_request_body_size: DEFAULT_MAX_REQUEST_BODY_SIZE,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
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
        }
    }

    /// Set the maximum request body size (on-wire, before decompression).
    ///
    /// Applies to unary and server streaming RPCs where the body is buffered.
    /// See [`Limits`] for details on the relationship between limits.
    #[must_use]
    pub fn max_request_body_size(mut self, size: usize) -> Self {
        self.max_request_body_size = size;
        self
    }

    /// Set the maximum message size (after decompression).
    ///
    /// This bounds individual messages in both unary and streaming RPCs.
    /// It also serves as the decompression limit.
    /// See [`Limits`] for details on the relationship between limits.
    #[must_use]
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
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
        _reader_task: None,
    };

    let mut response = Response::builder().status(StatusCode::OK).header(
        header::CONTENT_TYPE,
        Protocol::Connect.response_content_type(codec_format, true),
    );

    for (key, value) in err.response_headers() {
        response = response.header(key, value);
    }

    response.body(body).unwrap_or_else(|_| {
        Response::new(StreamingResponseBody {
            inner: Box::pin(futures::stream::empty()),
            _reader_task: None,
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

    let body = StreamingResponseBody {
        inner: body_stream,
        _reader_task: None,
    };

    let mut response = Response::builder().status(StatusCode::OK).header(
        header::CONTENT_TYPE,
        protocol.response_content_type(codec_format, true),
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

    for (key, value) in err.response_headers() {
        response = response.header(key, value);
    }

    response.body(body).unwrap_or_else(|_| {
        Response::new(StreamingResponseBody {
            inner: Box::pin(futures::stream::empty()),
            _reader_task: None,
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

    // Envelope: flag=0x80 (trailer), 4-byte big-endian length, payload
    let len = trailer_payload.len() as u32;
    let mut frame = Vec::with_capacity(5 + trailer_payload.len());
    frame.push(0x80); // trailer flag
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
    /// Optional background reader task handle. The task is detached (not aborted)
    /// when the response body is dropped — it continues draining the request body
    /// until EOF or `MAX_DRAIN_BYTES`, which is critical for HTTP/1.1 keep-alive.
    /// Aborting the task early was found to cause a race where hyper's dispatcher
    /// sees the `Incoming` body dropped before EOF and closes the connection.
    _reader_task: Option<tokio::task::JoinHandle<()>>,
}

impl StreamingResponseBody {
    /// Create a new streaming response body from a stream of encoded messages.
    ///
    /// For Connect protocol, trailers are encoded as a JSON EndStreamResponse
    /// in the final envelope. For gRPC, trailers are sent as HTTP/2 trailing
    /// HEADERS frames.
    fn new(
        response_stream: BoxStream<Result<Bytes, ConnectError>>,
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
        Self {
            inner,
            _reader_task: None,
        }
    }

    /// Attach the background reader-task handle returned by [`spawn_body_reader`].
    /// The handle is stored but never read or aborted — the task is owned by
    /// the runtime and runs to completion regardless of what happens to this
    /// `StreamingResponseBody`. The field exists to make the non-aborting
    /// policy explicit at the call site (see the `_reader_task` field comment
    /// for the HTTP/1.1 keep-alive race that motivated it). `task` is `None`
    /// on platforms without a joinable handle (see [`spawn_detached`]).
    fn with_reader_task(mut self, task: Option<tokio::task::JoinHandle<()>>) -> Self {
        self._reader_task = task;
        self
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
/// # Terminal state machine
///
/// At stream end (source returns `None` or `Err`), if `buf` is non-empty,
/// the data frame is emitted FIRST, and the finalizer frame is staged for
/// the next poll. This ensures partial batches aren't dropped.
struct BatchingEnvelopeStream {
    /// Source of encoded message bytes (already proto/JSON encoded).
    source: futures::stream::Fuse<BoxStream<Result<Bytes, ConnectError>>>,
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
    /// Fused-done flag.
    done: bool,
}

impl BatchingEnvelopeStream {
    fn new(
        source: BoxStream<Result<Bytes, ConnectError>>,
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
            done: false,
        }
    }

    /// Flush whatever's accumulated in `buf` as a data frame.
    #[inline]
    fn flush_buf(&mut self) -> Frame<Bytes> {
        Frame::data(self.buf.split().freeze())
    }
}

impl Stream for BatchingEnvelopeStream {
    type Item = Result<Frame<Bytes>, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        use tokio_util::codec::Encoder;

        if self.done {
            return Poll::Ready(None);
        }

        // Staged finalizer from a prior poll (buf was non-empty at stream end).
        if let Some(frame) = self.pending_final.take() {
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
                Poll::Ready(Some(Ok(data))) => {
                    let me = &mut *self;
                    if let Err(err) = me.encoder.encode(data, &mut me.buf) {
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
    response_stream: BoxStream<Result<Bytes, ConnectError>>,
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
    response_stream: BoxStream<Result<Bytes, ConnectError>>,
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
    response_stream: BoxStream<Result<Bytes, ConnectError>>,
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
/// let router = Router::new()
///     .route("example.v1.GreetService", "Greet", greet_handler);
///
/// let service = ConnectRpcService::new(router);
/// ```
///
/// # Request Limits
///
/// By default, the service applies security limits to prevent DoS attacks:
/// - Request body: 4 MB (on-wire, before decompression)
/// - Message size: 4 MB (after decompression, applied uniformly)
///
/// These can be configured using [`with_limits`](Self::with_limits):
///
/// ```rust,ignore
/// let service = ConnectRpcService::new(router)
///     .with_limits(Limits::default().max_message_size(8 * 1024 * 1024));
/// ```
pub struct ConnectRpcService<D = Router> {
    dispatcher: Arc<D>,
    limits: Limits,
    /// Wrapped in `Arc` so the per-request clone in `call()` is one atomic
    /// op instead of the registry's three internal Arc fields.
    compression: Arc<CompressionRegistry>,
    compression_policy: CompressionPolicy,
}

// Manual Clone impl because `#[derive(Clone)]` would add a `D: Clone` bound,
// but we hold `Arc<D>` so no such bound is needed.
impl<D> Clone for ConnectRpcService<D> {
    fn clone(&self) -> Self {
        Self {
            dispatcher: Arc::clone(&self.dispatcher),
            limits: self.limits.clone(),
            compression: Arc::clone(&self.compression),
            compression_policy: self.compression_policy,
        }
    }
}

impl<D: Dispatcher> ConnectRpcService<D> {
    /// Create a new ConnectRPC service from a dispatcher.
    ///
    /// The dispatcher can be either:
    /// - a [`Router`] built with `.route()` / `.route_view()` calls (or via
    ///   generated `FooServiceExt::register`), or
    /// - a code-generated `FooServiceServer<T>` struct for monomorphic
    ///   dispatch with no HashMap lookup or trait-object indirection.
    pub fn new(dispatcher: D) -> Self {
        Self {
            dispatcher: Arc::new(dispatcher),
            limits: Limits::default(),
            compression: Arc::new(CompressionRegistry::default()),
            compression_policy: CompressionPolicy::default(),
        }
    }

    /// Create a new ConnectRPC service from an Arc'd dispatcher.
    pub fn from_arc(dispatcher: Arc<D>) -> Self {
        Self {
            dispatcher,
            limits: Limits::default(),
            compression: Arc::new(CompressionRegistry::default()),
            compression_policy: CompressionPolicy::default(),
        }
    }

    /// Configure request limits.
    ///
    /// See [`Limits`] for available options.
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
/// Yields exactly two frames: one data frame (the gRPC envelope) and one
/// trailers frame (for gRPC) or a second data frame (for gRPC-Web trailer encoding).
/// This avoids the overhead of `Pin<Box<dyn Stream>>` + `stream::unfold` +
/// `EnvelopeEncoder` for the common unary case.
pub struct GrpcUnaryBody {
    data: Option<Bytes>,
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
        let limits = self.limits.clone();
        let compression = Arc::clone(&self.compression);
        let compression_policy = self.compression_policy;

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
            let response =
                match handle_request(dispatcher, req, limits, compression, &compression_policy)
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
async fn handle_request<D, B>(
    dispatcher: Arc<D>,
    req: Request<B>,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
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

    // Connect GET requests don't have a Content-Type header, so protocol
    // detection returns None. Route GET requests directly to the unary handler
    // which handles Connect GET query parameter parsing.
    if req.method() == Method::GET {
        return handle_unary_request(&*dispatcher, req, limits, compression, compression_policy)
            .await
            .map(|r| r.map(ConnectRpcBody::Full));
    }

    // Check for gRPC/gRPC-Web content type prefix with unsupported codec
    // (e.g., application/grpc+thrift). We need to return a gRPC-style error
    // rather than falling through to the Connect unary handler.
    //
    // For HTTP/2 requests with completely unknown content types, also return
    // a gRPC error since gRPC requires HTTP/2 and the client likely expects
    // gRPC framing. For HTTP/1.1, fall through to the Connect handler.
    if request_protocol.is_none() {
        let ct = req
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();

        // Drain the request body to avoid broken pipe on HTTP/1.1.
        // Use Limited to cap the drain at max_request_body_size.
        let (_parts, body) = req.into_parts();
        let limited = http_body_util::Limited::new(body, limits.max_request_body_size);
        let _ = limited.collect().await;

        if ct.starts_with("application/grpc-web") {
            let err = ConnectError::internal("unsupported content type");
            let response = grpc_error_response(&err, Protocol::GrpcWeb, CodecFormat::Proto);
            return Ok(response.map(ConnectRpcBody::Streaming));
        } else if ct.starts_with("application/grpc") {
            let err = ConnectError::internal("unsupported content type");
            let response = grpc_error_response(&err, Protocol::Grpc, CodecFormat::Proto);
            return Ok(response.map(ConnectRpcBody::Streaming));
        } else {
            // Unknown content type that doesn't match any protocol.
            // Return HTTP 415 Unsupported Media Type with no body.
            // All protocol clients (Connect, gRPC, gRPC-Web) can handle
            // non-200 HTTP status codes by mapping to an appropriate error.
            let response = Response::builder()
                .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
                .body(Full::new(Bytes::new()))
                .unwrap();
            return Ok(response.map(ConnectRpcBody::Full));
        }
    }

    match request_protocol {
        Some(rp) if rp.is_streaming => {
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
                let (_parts, body) = req.into_parts();
                let limited = http_body_util::Limited::new(body, limits.max_request_body_size);
                let _ = limited.collect().await;
                let response = grpc_error_response(&err, Protocol::GrpcWeb, rp.codec_format);
                return Ok(response.map(ConnectRpcBody::Streaming));
            }

            // If so, take the fast path that avoids stream wrapping overhead.
            if matches!(rp.protocol, Protocol::Grpc | Protocol::GrpcWeb) {
                let path = req.uri().path();
                let path = path.strip_prefix('/').unwrap_or(path);
                if let Some(desc) = dispatcher.lookup(path)
                    && desc.kind == MethodKind::Unary
                {
                    let path = path.to_owned();
                    let response = handle_grpc_unary_request(
                        &*dispatcher,
                        &path,
                        req,
                        rp.protocol,
                        rp.codec_format,
                        limits,
                        compression,
                        compression_policy,
                    )
                    .await;
                    return Ok(response.map(ConnectRpcBody::GrpcUnary));
                }
            }

            // Streaming request (Connect streaming, gRPC, or gRPC-Web)
            let response = handle_streaming_request(
                &*dispatcher,
                req,
                rp.protocol,
                rp.codec_format,
                limits,
                compression,
                compression_policy,
            )
            .await;
            Ok(response.map(ConnectRpcBody::Streaming))
        }
        Some(_) | None => {
            // Unary request (Connect unary) or unknown content type (for error reporting)
            handle_unary_request(&*dispatcher, req, limits, compression, compression_policy)
                .await
                .map(|r| r.map(ConnectRpcBody::Full))
        }
    }
}

/// Handle a unary ConnectRPC request.
async fn handle_unary_request<D, B>(
    dispatcher: &D,
    req: Request<B>,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
) -> Result<Response<Full<Bytes>>, ConnectError>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    // Parse the path to extract service/method (owned to avoid borrowing req)
    let path = req.uri().path();
    let path = path.strip_prefix('/').unwrap_or(path).to_owned();
    let query_string = req.uri().query().map(|s| s.to_owned());
    let method = req.method().clone();

    // Extract metadata from headers using the Connect protocol (unary is always Connect)
    let metadata = RequestMetadata::from_headers(req.headers(), Protocol::Connect);

    // Split request to consume the body
    let (parts, body) = req.into_parts();
    let extensions = parts.extensions;

    // IMPORTANT: Read the full request body BEFORE returning any errors.
    // For HTTP/1.1, returning an error without reading the body causes
    // "broken pipe" errors because the client is still sending data.
    // collect_body_limited bounds allocation *during* the read, so an
    // oversized body is rejected before it is fully buffered.
    let post_body = collect_body_limited(body, limits.max_request_body_size).await?;

    // Look up the method descriptor to check idempotency.
    // (Non-unary kinds are allowed through — they'll error at the dispatch call.)
    let desc = dispatcher.lookup(&path).ok_or_else(|| {
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
        let params = parse_get_query_params(query_string.as_deref())?;

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
        return Err(ConnectError::method_not_allowed(
            "only GET and POST methods are supported",
        ));
    };

    // Create handler context with the request headers from metadata.
    // Compute the absolute deadline once so handlers can propagate it;
    // the tokio::time::timeout wrapper below still uses the relative duration.
    let deadline = metadata
        .timeout
        .and_then(|t| std::time::Instant::now().checked_add(t));
    let ctx = RequestContext::new(metadata.headers)
        .with_deadline(deadline)
        .with_extensions(extensions);

    // Call the handler with the appropriate codec format, applying timeout if specified
    let resp: EncodedResponse = if let Some(timeout) = metadata.timeout {
        tokio::time::timeout(
            timeout,
            dispatcher.call_unary(&path, ctx, body, codec_format),
        )
        .await
        .map_err(|_| ConnectError::deadline_exceeded("request timeout"))?
    } else {
        dispatcher.call_unary(&path, ctx, body, codec_format).await
    }?;

    // Negotiate response compression
    let response_encoding = compression.negotiate_encoding(
        metadata.unary_accept_encoding.as_deref(),
        metadata.unary_encoding.as_deref(),
    );

    // Compress response body if negotiated, respecting the compression policy
    let effective_policy = compression_policy.with_override(resp.compress);
    let (final_body, content_encoding) = if let Some(encoding) = response_encoding {
        if !effective_policy.should_compress(resp.body.len()) {
            (resp.body, None)
        } else {
            match compression.compress(encoding, &resp.body) {
                Ok(compressed) => (compressed, Some(encoding)),
                Err(_) => (resp.body, None), // Fall back to uncompressed
            }
        }
    } else {
        (resp.body, None)
    };

    // Build response with the same content type as the request
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, codec_format.content_type());

    if let Some(encoding) = content_encoding {
        response = response.header(header::CONTENT_ENCODING, encoding);
    }

    // Advertise what encodings we accept (optional per spec, informational)
    let accept = compression.accept_encoding_header();
    if !accept.is_empty() {
        response = response.header(header::ACCEPT_ENCODING, accept);
    }

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

/// Handle a gRPC/gRPC-Web unary request via the fast path.
///
/// This avoids the overhead of `handle_streaming_request` for unary RPCs:
/// no `BoxStream`, no `stream::unfold`, no `EnvelopeEncoder` — just inline
/// envelope decoding/encoding and a lightweight two-frame body.
#[allow(clippy::too_many_arguments)]
async fn handle_grpc_unary_request<D, B>(
    dispatcher: &D,
    path: &str,
    req: Request<B>,
    protocol: Protocol,
    codec_format: CodecFormat,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
) -> Response<GrpcUnaryBody>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
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
            protocol.response_content_type(codec_format, true),
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
        for (key, value) in err.response_headers() {
            response = response.header(key, value);
        }
        let body = GrpcUnaryBody {
            data: None,
            trailers: Some(trailers),
        };
        response.body(body).unwrap_or_else(|_| {
            Response::new(GrpcUnaryBody {
                data: None,
                trailers: None,
            })
        })
    };

    // gRPC requires POST
    if req.method() != Method::POST {
        let err = ConnectError::internal(format!("invalid method for gRPC: {}", req.method()));
        // Drain the request body to avoid broken pipe on HTTP/1.1.
        let (_parts, body) = req.into_parts();
        let limited = http_body_util::Limited::new(body, limits.max_request_body_size);
        let _ = limited.collect().await;
        return grpc_unary_error(&err);
    }

    // Extract metadata
    let metadata = RequestMetadata::from_headers(req.headers(), protocol);

    // Validate request compression
    if let Some(ref encoding) = metadata.streaming_encoding
        && encoding != "identity"
        && !compression.supports(encoding)
    {
        let err = ConnectError::unimplemented(format!("unsupported compression: {encoding}"));
        // Drain the request body to avoid broken pipe on HTTP/1.1.
        let (_parts, body) = req.into_parts();
        let limited = http_body_util::Limited::new(body, limits.max_request_body_size);
        let _ = limited.collect().await;
        return grpc_unary_error(&err);
    }

    // Read the full body. collect_body_limited bounds allocation during the
    // read, so an oversized body is rejected before it is fully buffered.
    let (parts, body) = req.into_parts();
    let extensions = parts.extensions;
    let post_body = match collect_body_limited(body, limits.max_request_body_size).await {
        Ok(bytes) => bytes,
        Err(err) => return grpc_unary_error(&err),
    };

    // Decode the gRPC envelope (5-byte header + payload)
    let request_body = if post_body.is_empty() {
        // Empty body means no envelope at all — this is an error for unary RPCs
        let err = ConnectError::unimplemented("request body is empty: expected a message");
        return grpc_unary_error(&err);
    } else {
        let mut buf = bytes::BytesMut::from(&post_body[..]);
        let envelope = match Envelope::decode_with_limit(&mut buf, limits.max_message_size) {
            Ok(Some(env)) => env,
            Ok(None) => {
                let err = ConnectError::invalid_argument("incomplete request envelope");
                return grpc_unary_error(&err);
            }
            Err(e) => return grpc_unary_error(&e),
        };

        // Reject multiple envelopes in a unary request
        if !buf.is_empty() {
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
    let deadline = metadata
        .timeout
        .and_then(|t| std::time::Instant::now().checked_add(t));
    let ctx = RequestContext::new(metadata.headers)
        .with_deadline(deadline)
        .with_extensions(extensions);

    // Call the handler with timeout if configured
    let handler_result = if let Some(timeout) = metadata.timeout {
        match tokio::time::timeout(
            timeout,
            dispatcher.call_unary(path, ctx, request_body, codec_format),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let err = ConnectError::deadline_exceeded("request timeout");
                return grpc_unary_error(&err);
            }
        }
    } else {
        dispatcher
            .call_unary(path, ctx, request_body, codec_format)
            .await
    };

    let resp = match handler_result {
        Ok(result) => result,
        Err(e) => return grpc_unary_error(&e),
    };

    // Negotiate response compression
    let response_encoding = compression.negotiate_encoding(
        metadata.streaming_accept_encoding.as_deref(),
        metadata.streaming_encoding.as_deref(),
    );

    // Encode response into a gRPC envelope (5-byte header + payload)
    let effective_policy = compression_policy.with_override(resp.compress);
    let encoded_data = if let Some(encoding) = response_encoding
        && effective_policy.should_compress(resp.body.len())
    {
        match compression.compress(encoding, &resp.body) {
            Ok(compressed) => Envelope::compressed(compressed).encode(),
            Err(_) => Envelope::data(resp.body).encode(),
        }
    } else {
        Envelope::data(resp.body).encode()
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
    let mut response = Response::builder().status(StatusCode::OK).header(
        header::CONTENT_TYPE,
        protocol.response_content_type(codec_format, true),
    );

    if let Some(encoding) = response_encoding {
        response = response.header(protocol.content_encoding_header(), encoding);
    }

    let accept = compression.accept_encoding_header();
    if !accept.is_empty() {
        response = response.header(protocol.accept_encoding_header(), accept);
    }

    // Add response headers set by the handler
    for (key, value) in resp.headers.iter() {
        response = response.header(key, value);
    }

    let body = GrpcUnaryBody {
        data: Some(encoded_data),
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
    req: Request<B>,
    protocol: Protocol,
    codec_format: CodecFormat,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
) -> Response<StreamingResponseBody>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    // Parse the path to extract service/method
    let path = req.uri().path();
    let path = path.strip_prefix('/').unwrap_or(path).to_owned();

    // gRPC and gRPC-Web require POST method
    if matches!(protocol, Protocol::Grpc | Protocol::GrpcWeb) && req.method() != Method::POST {
        let err = ConnectError::internal(format!("invalid method for gRPC: {}", req.method()));
        // Drain the request body to avoid broken pipe on HTTP/1.1.
        let (_parts, body) = req.into_parts();
        let limited = http_body_util::Limited::new(body, limits.max_request_body_size);
        let _ = limited.collect().await;
        return streaming_error_response(&err, protocol, codec_format);
    }

    // Extract metadata from headers using the detected protocol
    let metadata = RequestMetadata::from_headers(req.headers(), protocol);

    // Validate request compression is supported (if specified)
    if let Some(ref encoding) = metadata.streaming_encoding
        && encoding != "identity"
        && !compression.supports(encoding)
    {
        let err = ConnectError::unimplemented(format!("unsupported compression: {encoding}"));
        // Drain the request body to avoid broken pipe on HTTP/1.1.
        let (_parts, body) = req.into_parts();
        let limited = http_body_util::Limited::new(body, limits.max_request_body_size);
        let _ = limited.collect().await;
        return streaming_error_response(&err, protocol, codec_format);
    }

    // Single lookup to determine method kind.
    let method_desc = dispatcher.lookup(&path);

    // Split request to consume the body
    let (parts, body) = req.into_parts();
    let extensions = parts.extensions;

    // For bidi streaming, pass the raw body stream directly (no buffering)
    if matches!(method_desc, Some(d) if d.kind == MethodKind::BidiStreaming) {
        return handle_bidi_streaming_request(
            dispatcher,
            &path,
            metadata,
            body,
            extensions,
            protocol,
            codec_format,
            limits,
            compression,
            compression_policy,
        )
        .await;
    }

    // For client streaming, pass the raw body stream directly (no buffering)
    if matches!(method_desc, Some(d) if d.kind == MethodKind::ClientStreaming) {
        return handle_client_streaming_request(
            dispatcher,
            &path,
            metadata,
            body,
            extensions,
            protocol,
            codec_format,
            limits,
            compression,
            compression_policy,
        )
        .await;
    }

    // For server streaming (or errors), read the full body (single envelope expected).
    // collect_body_limited bounds allocation during the read.
    let post_body = match collect_body_limited(body, limits.max_request_body_size).await {
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
        let mut buf = bytes::BytesMut::from(&post_body[..]);
        let envelope = match Envelope::decode_with_limit(&mut buf, limits.max_message_size) {
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
        if !buf.is_empty() {
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
    let deadline = metadata
        .timeout
        .and_then(|t| std::time::Instant::now().checked_add(t));
    let ctx = RequestContext::new(metadata.headers)
        .with_deadline(deadline)
        .with_extensions(extensions);

    // Call the handler with the appropriate codec format.
    // For gRPC unary handlers, we wrap the single response in a one-item stream.
    let resp = match dispatch_kind {
        StreamingDispatchKind::ServerStreaming => {
            let fut = dispatcher.call_server_streaming(&path, ctx, request_body, codec_format);
            let handler_result = if let Some(timeout) = metadata.timeout {
                match tokio::time::timeout(timeout, fut).await {
                    Ok(result) => result,
                    Err(_) => {
                        let err = ConnectError::deadline_exceeded("request timeout");
                        return streaming_error_response(&err, protocol, codec_format);
                    }
                }
            } else {
                fut.await
            };
            match handler_result {
                Ok(result) => result,
                Err(e) => return streaming_error_response(&e, protocol, codec_format),
            }
        }
        StreamingDispatchKind::Unary => {
            let fut = dispatcher.call_unary(&path, ctx, request_body, codec_format);
            let handler_result = if let Some(timeout) = metadata.timeout {
                match tokio::time::timeout(timeout, fut).await {
                    Ok(result) => result,
                    Err(_) => {
                        let err = ConnectError::deadline_exceeded("request timeout");
                        return streaming_error_response(&err, protocol, codec_format);
                    }
                }
            } else {
                fut.await
            };
            match handler_result {
                // Wrap single response in a one-item stream
                Ok(r) => r.map_body(|bytes| -> BoxStream<Result<Bytes, ConnectError>> {
                    Box::pin(futures::stream::once(async move { Ok(bytes) }))
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
    let mut response = Response::builder().status(StatusCode::OK).header(
        header::CONTENT_TYPE,
        protocol.response_content_type(codec_format, true),
    );

    if let Some(encoding) = response_encoding {
        response = response.header(protocol.content_encoding_header(), encoding);
    }

    // Advertise what encodings we accept (optional per spec, informational)
    let accept = compression.accept_encoding_header();
    if !accept.is_empty() {
        response = response.header(protocol.accept_encoding_header(), accept);
    }

    // Add response headers set by the handler
    for (key, value) in resp.headers.iter() {
        response = response.header(key, value);
    }

    let stream_compression = response_encoding.map(|encoding| (compression, encoding));
    let effective_policy = compression_policy.with_override(resp.compress);
    let body = StreamingResponseBody::new(
        resp.body,
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
/// # Incremental Dispatch
///
/// The body is consumed incrementally via [`spawn_body_reader`], which runs
/// a background task that reads HTTP body frames, decodes envelopes, and
/// sends decoded payloads through an `mpsc` channel. The handler receives
/// these as a truly async stream.
///
/// # Body Draining
///
/// If the handler returns early (e.g., on error) without consuming the full
/// request stream, the reader task drains remaining body bytes (up to
/// [`MAX_DRAIN_BYTES`]) to allow HTTP/1.1 connection reuse.
#[allow(clippy::too_many_arguments)]
async fn handle_client_streaming_request<D, B>(
    dispatcher: &D,
    path: &str,
    metadata: RequestMetadata,
    body: B,
    extensions: http::Extensions,
    protocol: Protocol,
    codec_format: CodecFormat,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
) -> Response<StreamingResponseBody>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    let (request_stream, reader_task) = spawn_body_reader(
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
        .with_extensions(extensions);

    // Call the handler. On error paths, the reader task is left running
    // (detached) so it can finish draining the request body — aborting it
    // early races with hyper's body-EOF detection and breaks HTTP/1.1
    // keep-alive. The task is bounded by MAX_DRAIN_BYTES.
    let handler_result = if let Some(timeout) = metadata.timeout {
        match tokio::time::timeout(
            timeout,
            dispatcher.call_client_streaming(path, ctx, request_stream, codec_format),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                drop(reader_task);
                let err = ConnectError::deadline_exceeded("request timeout");
                return streaming_error_response(&err, protocol, codec_format);
            }
        }
    } else {
        dispatcher
            .call_client_streaming(path, ctx, request_stream, codec_format)
            .await
    };

    let resp = match handler_result {
        Ok(result) => result,
        Err(e) => {
            drop(reader_task);
            return streaming_error_response(&e, protocol, codec_format);
        }
    };

    // Negotiate response compression for streaming
    let response_encoding = compression.negotiate_encoding(
        metadata.streaming_accept_encoding.as_deref(),
        metadata.streaming_encoding.as_deref(),
    );

    // Build streaming response with a single data envelope + END_STREAM
    let mut response = Response::builder().status(StatusCode::OK).header(
        header::CONTENT_TYPE,
        protocol.response_content_type(codec_format, true),
    );

    if let Some(encoding) = response_encoding {
        response = response.header(protocol.content_encoding_header(), encoding);
    }

    let accept = compression.accept_encoding_header();
    if !accept.is_empty() {
        response = response.header(protocol.accept_encoding_header(), accept);
    }

    // Add response headers set by the handler
    for (key, value) in resp.headers.iter() {
        response = response.header(key, value);
    }

    let stream_compression = response_encoding.map(|encoding| (compression, encoding));
    let response_stream: BoxStream<Result<Bytes, ConnectError>> =
        Box::pin(futures::stream::once(async { Ok(resp.body) }));
    let effective_policy = compression_policy.with_override(resp.compress);
    let body = StreamingResponseBody::new(
        response_stream,
        resp.trailers,
        protocol,
        stream_compression,
        effective_policy,
    )
    .with_reader_task(reader_task);

    response.body(body).unwrap_or_else(|_| {
        let err = ConnectError::internal("failed to build client streaming response");
        streaming_error_response(&err, protocol, codec_format)
    })
}

/// Maximum bytes to drain from the request body after the decoder finishes.
/// This prevents a malicious client from forcing the server to consume unbounded
/// data after a size-limit error or other decoder failure.
const MAX_DRAIN_BYTES: usize = 1024 * 1024; // 1 MiB

/// Spawn a background task that reads envelope-framed messages from an HTTP
/// body and forwards them to a channel.
///
/// Returns a `(request_stream, reader_task)` pair. The `request_stream` yields
/// decoded message payloads. The `reader_task` should be attached to the
/// response via [`StreamingResponseBody::with_reader_task`]. The handle is
/// held for lifetime association but is NOT aborted when the response drops —
/// the task must be allowed to finish draining or hyper's dispatcher will see
/// the body dropped before EOF and close the connection.
///
/// When the channel receiver is dropped (e.g., the handler finishes or
/// encounters an error), the reader task continues consuming remaining body
/// bytes (up to [`MAX_DRAIN_BYTES`]) before completing. This is critical for
/// HTTP/1.1 where the server must read the entire request body before it can
/// send the response.
fn spawn_body_reader<B>(
    body: B,
    max_message_size: usize,
    streaming_encoding: Option<String>,
    compression: Arc<CompressionRegistry>,
) -> (
    BoxStream<Result<Bytes, ConnectError>>,
    Option<tokio::task::JoinHandle<()>>,
)
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, ConnectError>>(1);

    let reader_future = async move {
        use tokio_util::codec::Decoder as _;

        let mut body = std::pin::pin!(body);
        let mut decoder = crate::envelope::EnvelopeDecoder::new(
            max_message_size,
            streaming_encoding,
            compression,
        );
        let mut buf = bytes::BytesMut::new();
        let mut decoder_done = false;
        let mut drained_bytes: usize = 0;

        // Read body frames and decode envelopes. When the decoder stops
        // (e.g., after a size limit error or end-of-stream), continue
        // reading body frames to drain the HTTP body. This is critical
        // for HTTP/1.1 where the server must consume the request body
        // before the response can be sent.
        loop {
            // Try to decode messages from the buffer before reading more
            if !decoder_done {
                match decoder.decode(&mut buf) {
                    Ok(Some(data)) => {
                        if tx.send(Ok(data)).await.is_err() {
                            decoder_done = true;
                        }
                        continue;
                    }
                    Ok(None) => {} // need more data from body
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        decoder_done = true;
                        continue;
                    }
                }
            }

            // Read the next frame from the body
            match std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        if decoder_done {
                            drained_bytes = drained_bytes.saturating_add(data.len());
                            if drained_bytes > MAX_DRAIN_BYTES {
                                tracing::debug!(
                                    drained_bytes,
                                    "body drain limit reached, stopping"
                                );
                                break;
                            }
                        } else {
                            buf.extend_from_slice(&data);
                        }
                    }
                    // When decoder_done, we discard data (draining the body)
                }
                Some(Err(_)) => break, // body error, stop reading
                None => {
                    // Body EOF — try to decode any remaining data
                    if !decoder_done {
                        loop {
                            match decoder.decode_eof(&mut buf) {
                                Ok(Some(data)) => {
                                    if tx.send(Ok(data)).await.is_err() {
                                        break;
                                    }
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    break;
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }
    };

    // The reader runs detached — it has to outlive the response stream so it
    // can finish draining the request body.
    let reader_task = crate::spawn_detached(reader_future);

    let request_stream: BoxStream<Result<Bytes, ConnectError>> =
        Box::pin(futures::stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|item| (item, rx))
        }));

    (request_stream, reader_task)
}

/// Handle a bidi streaming ConnectRPC request.
///
/// Bidi streaming RPCs receive multiple envelope-framed request messages
/// and return multiple envelope-framed response messages with END_STREAM.
///
/// The reader task is attached to the response body via
/// [`StreamingResponseBody::with_reader_task`]. When the response body drops,
/// the task is **detached** (not aborted) — it continues draining the request
/// body until EOF or [`MAX_DRAIN_BYTES`]. Aborting early was found to race
/// with hyper's HTTP/1.1 body-EOF detection (see `_reader_task` field docs).
#[allow(clippy::too_many_arguments)]
async fn handle_bidi_streaming_request<D, B>(
    dispatcher: &D,
    path: &str,
    metadata: RequestMetadata,
    body: B,
    extensions: http::Extensions,
    protocol: Protocol,
    codec_format: CodecFormat,
    limits: Limits,
    compression: Arc<CompressionRegistry>,
    compression_policy: &CompressionPolicy,
) -> Response<StreamingResponseBody>
where
    D: Dispatcher,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    let (request_stream, reader_task) = spawn_body_reader(
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
        .with_extensions(extensions);

    // Call the handler with timeout if configured
    let handler_result = if let Some(timeout) = metadata.timeout {
        match tokio::time::timeout(
            timeout,
            dispatcher.call_bidi_streaming(path, ctx, request_stream, codec_format),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                let err = ConnectError::deadline_exceeded("request timeout");
                drop(reader_task);
                return streaming_error_response(&err, protocol, codec_format);
            }
        }
    } else {
        dispatcher
            .call_bidi_streaming(path, ctx, request_stream, codec_format)
            .await
    };

    let resp = match handler_result {
        Ok(result) => result,
        Err(e) => {
            drop(reader_task);
            return streaming_error_response(&e, protocol, codec_format);
        }
    };

    // Negotiate response compression for streaming
    let response_encoding = compression.negotiate_encoding(
        metadata.streaming_accept_encoding.as_deref(),
        metadata.streaming_encoding.as_deref(),
    );

    // Build streaming response
    let mut response = Response::builder().status(StatusCode::OK).header(
        header::CONTENT_TYPE,
        protocol.response_content_type(codec_format, true),
    );

    if let Some(encoding) = response_encoding {
        response = response.header(protocol.content_encoding_header(), encoding);
    }

    // Advertise what encodings we accept (optional per spec, informational)
    let accept = compression.accept_encoding_header();
    if !accept.is_empty() {
        response = response.header(protocol.accept_encoding_header(), accept);
    }

    // Add response headers set by the handler
    for (key, value) in resp.headers.iter() {
        response = response.header(key, value);
    }

    let stream_compression = response_encoding.map(|encoding| (compression, encoding));
    let effective_policy = compression_policy.with_override(resp.compress);
    let body = StreamingResponseBody::new(
        resp.body,
        resp.trailers,
        protocol,
        stream_compression,
        effective_policy,
    )
    .with_reader_task(reader_task);

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

    let mut response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type::JSON);

    // Add response headers from the error
    for (key, value) in err.response_headers() {
        response = response.header(key, value);
    }

    // Add trailers as trailer- prefixed headers
    let response = add_trailers(response, err.trailers());

    response.body(Full::new(body)).unwrap_or_else(|_| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Full::new(Bytes::new()))
            .unwrap()
    })
}

// ============================================================================
// Axum Integration
// ============================================================================

/// Axum router integration for ConnectRPC.
///
/// Available when the `axum` feature is enabled.
#[cfg(feature = "axum")]
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
        ///
        /// async fn health() -> &'static str {
        ///     "OK"
        /// }
        ///
        /// let connect_router = ConnectRouter::new()
        ///     .route("example.v1.GreetService", "Greet", greet_handler);
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
        ///
        /// let connect_router = ConnectRouter::new()
        ///     .route("example.v1.GreetService", "Greet", greet_handler);
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
        let source: BoxStream<_> = Box::pin(futures::stream::iter(items));

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
        let source: BoxStream<_> = Box::pin(futures::stream::iter(items));

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
        let source: BoxStream<_> = Box::pin(futures::stream::once(async {
            Ok(Bytes::from_static(b"x"))
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
        let source: BoxStream<_> = Box::pin(futures::stream::iter(items));
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
                    *cap.lock().unwrap() = ctx.extensions.get::<PeerTag>().cloned();
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
            req,
            Limits::default(),
            Arc::new(CompressionRegistry::new()),
            &CompressionPolicy::default(),
        )
        .await
        .expect("dispatch should succeed");

        assert_eq!(
            captured.lock().unwrap().take(),
            Some(PeerTag("10.0.0.1:54321")),
            "extension inserted on the http::Request must reach Context.extensions"
        );
    }
}
