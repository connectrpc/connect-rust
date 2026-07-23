//! ConnectRPC error types and HTTP status mapping.
//!
//! This module provides error types that conform to the ConnectRPC protocol
//! specification, including proper error code mappings to HTTP status codes.

use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use serde::Deserialize;
use serde::Serialize;

/// ConnectRPC error codes.
///
/// These codes follow the ConnectRPC protocol specification and map to
/// corresponding HTTP status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCode {
    /// The operation was cancelled.
    Canceled,
    /// Unknown error.
    Unknown,
    /// Invalid argument provided by the client.
    InvalidArgument,
    /// Deadline expired before operation completed.
    DeadlineExceeded,
    /// Requested entity was not found.
    NotFound,
    /// Entity already exists.
    AlreadyExists,
    /// Permission denied.
    PermissionDenied,
    /// Resource exhausted (e.g., rate limit).
    ResourceExhausted,
    /// Operation rejected due to system state.
    FailedPrecondition,
    /// Operation was aborted.
    Aborted,
    /// Operation was out of range.
    OutOfRange,
    /// Operation is not implemented.
    Unimplemented,
    /// Internal error.
    Internal,
    /// Service is unavailable.
    Unavailable,
    /// Unrecoverable data loss.
    DataLoss,
    /// Request is unauthenticated.
    Unauthenticated,
}

impl ErrorCode {
    /// Get the string representation of this error code.
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Canceled => "canceled",
            Self::Unknown => "unknown",
            Self::InvalidArgument => "invalid_argument",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::NotFound => "not_found",
            Self::AlreadyExists => "already_exists",
            Self::PermissionDenied => "permission_denied",
            Self::ResourceExhausted => "resource_exhausted",
            Self::FailedPrecondition => "failed_precondition",
            Self::Aborted => "aborted",
            Self::OutOfRange => "out_of_range",
            Self::Unimplemented => "unimplemented",
            Self::Internal => "internal",
            Self::Unavailable => "unavailable",
            Self::DataLoss => "data_loss",
            Self::Unauthenticated => "unauthenticated",
        }
    }

    /// Get the HTTP status code for this error code.
    #[inline]
    pub fn http_status(&self) -> StatusCode {
        match self {
            // 499 Client Closed Request (nginx-style, used by Connect protocol for Canceled)
            Self::Canceled => {
                // 499 is always valid (100-999 range), but avoid panic in library code.
                StatusCode::from_u16(499).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            }
            Self::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidArgument => StatusCode::BAD_REQUEST,
            Self::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::AlreadyExists => StatusCode::CONFLICT,
            Self::PermissionDenied => StatusCode::FORBIDDEN,
            Self::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            Self::FailedPrecondition => StatusCode::BAD_REQUEST,
            Self::Aborted => StatusCode::CONFLICT,
            Self::OutOfRange => StatusCode::BAD_REQUEST,
            Self::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
        }
    }
}

impl ErrorCode {
    /// Get the gRPC numeric status code for this error code.
    #[inline]
    pub fn grpc_code(&self) -> u32 {
        match self {
            Self::Canceled => 1,
            Self::Unknown => 2,
            Self::InvalidArgument => 3,
            Self::DeadlineExceeded => 4,
            Self::NotFound => 5,
            Self::AlreadyExists => 6,
            Self::PermissionDenied => 7,
            Self::ResourceExhausted => 8,
            Self::FailedPrecondition => 9,
            Self::Aborted => 10,
            Self::OutOfRange => 11,
            Self::Unimplemented => 12,
            Self::Internal => 13,
            Self::Unavailable => 14,
            Self::DataLoss => 15,
            Self::Unauthenticated => 16,
        }
    }

    /// Create an error code from a gRPC numeric status code.
    ///
    /// Returns `None` for unknown codes. Code 0 (OK) returns `None` since
    /// it represents success, not an error.
    #[inline]
    pub fn from_grpc_code(code: u32) -> Option<Self> {
        match code {
            1 => Some(Self::Canceled),
            2 => Some(Self::Unknown),
            3 => Some(Self::InvalidArgument),
            4 => Some(Self::DeadlineExceeded),
            5 => Some(Self::NotFound),
            6 => Some(Self::AlreadyExists),
            7 => Some(Self::PermissionDenied),
            8 => Some(Self::ResourceExhausted),
            9 => Some(Self::FailedPrecondition),
            10 => Some(Self::Aborted),
            11 => Some(Self::OutOfRange),
            12 => Some(Self::Unimplemented),
            13 => Some(Self::Internal),
            14 => Some(Self::Unavailable),
            15 => Some(Self::DataLoss),
            16 => Some(Self::Unauthenticated),
            _ => None,
        }
    }
}

impl std::str::FromStr for ErrorCode {
    type Err = ();

    /// Parse an error code from a string.
    ///
    /// Returns `Err(())` if the string doesn't match any known error code.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "canceled" => Ok(Self::Canceled),
            "unknown" => Ok(Self::Unknown),
            "invalid_argument" => Ok(Self::InvalidArgument),
            "deadline_exceeded" => Ok(Self::DeadlineExceeded),
            "not_found" => Ok(Self::NotFound),
            "already_exists" => Ok(Self::AlreadyExists),
            "permission_denied" => Ok(Self::PermissionDenied),
            "resource_exhausted" => Ok(Self::ResourceExhausted),
            "failed_precondition" => Ok(Self::FailedPrecondition),
            "aborted" => Ok(Self::Aborted),
            "out_of_range" => Ok(Self::OutOfRange),
            "unimplemented" => Ok(Self::Unimplemented),
            "internal" => Ok(Self::Internal),
            "unavailable" => Ok(Self::Unavailable),
            "data_loss" => Ok(Self::DataLoss),
            "unauthenticated" => Ok(Self::Unauthenticated),
            _ => Err(()),
        }
    }
}

/// Additional error details that can be attached to a ConnectRPC error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDetail {
    /// The type URL for this detail.
    /// Named `type` per Connect protocol (distinct from protobuf JSON `@type`).
    #[serde(rename = "type")]
    pub type_url: String,
    /// Base64-encoded protobuf message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Debug information (JSON representation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub debug: Option<serde_json::Value>,
}

impl ErrorDetail {
    /// Build a detail from a protobuf message, handling the base64 encoding
    /// the Connect protocol requires.
    ///
    /// `type_name` is the message's bare fully-qualified name — e.g.
    /// `google.rpc.RetryInfo` — which is what the Connect protocol's JSON
    /// `type` field carries; the gRPC path prepends the standard
    /// `type.googleapis.com/` `Any` prefix automatically (a value already
    /// carrying a prefix is passed through unchanged on that path). The
    /// message is encoded to protobuf wire bytes and base64'd with the
    /// protocol's canonical unpadded-standard alphabet — prefer this over
    /// populating [`value`](Self::value) by hand, where a wrong alphabet is
    /// dropped from the gRPC status (with a logged warning).
    pub fn from_message(type_name: impl Into<String>, message: &impl buffa::Message) -> Self {
        Self {
            type_url: type_name.into(),
            value: Some(detail_b64::encode(&buffa::Message::encode_to_vec(message))),
            debug: None,
        }
    }
}

/// The base64 form the Connect protocol uses for error-detail values:
/// unpadded standard alphabet on encode, padding accepted on decode.
/// Single-sourced so [`ErrorDetail::from_message`] and the gRPC status
/// encoder can never drift apart.
pub(crate) mod detail_b64 {
    use base64::Engine as _;
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};

    pub(crate) fn encode(bytes: &[u8]) -> String {
        STANDARD_NO_PAD.encode(bytes)
    }

    pub(crate) fn decode_lenient(s: &str) -> Result<Vec<u8>, base64::DecodeError> {
        STANDARD_NO_PAD.decode(s).or_else(|_| STANDARD.decode(s))
    }
}

/// A ConnectRPC error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectError {
    /// The error code.
    pub code: ErrorCode,
    /// Human-readable error message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Additional error details.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub details: Vec<ErrorDetail>,
    /// Optional HTTP status override (not serialized).
    /// When set, this overrides the default HTTP status for the error code.
    #[serde(skip)]
    http_status_override: Option<StatusCode>,
    /// Response headers to include in error response (not serialized).
    ///
    /// Boxed to keep `ConnectError` small enough to pass by value in
    /// `Result` without tripping `clippy::result_large_err`. `None` means
    /// no extra headers.
    #[serde(skip)]
    pub(crate) response_headers: Option<Box<http::HeaderMap>>,
    /// Response trailers to include in error response (not serialized).
    ///
    /// Boxed for the same reason as `response_headers`. `None` means no
    /// extra trailers.
    #[serde(skip)]
    pub(crate) trailers: Option<Box<http::HeaderMap>>,
    /// The underlying cause, if this error was converted from another error
    /// (not serialized — never sent over the wire).
    ///
    /// Surfaced through [`Error::source`](std::error::Error::source).
    /// `Arc` (rather than `Box`) so `ConnectError` stays `Clone` without
    /// requiring the wrapped error to be.
    #[serde(skip)]
    source: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

/// Shared empty `HeaderMap` for the `None` arm of the read accessors, so
/// callers can iterate / `.get()` unconditionally.
static EMPTY_HEADERS: std::sync::LazyLock<http::HeaderMap> =
    std::sync::LazyLock::new(http::HeaderMap::new);

fn box_headers(h: http::HeaderMap) -> Option<Box<http::HeaderMap>> {
    if h.is_empty() {
        None
    } else {
        Some(Box::new(h))
    }
}

impl ConnectError {
    /// Create a new error with the given code and message.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: Some(message.into()),
            details: Vec::new(),
            http_status_override: None,
            response_headers: None,
            trailers: None,
            source: None,
        }
    }

    /// Add response headers to be included in the error response.
    #[must_use]
    pub fn with_headers(mut self, headers: http::HeaderMap) -> Self {
        self.response_headers = box_headers(headers);
        self
    }

    /// Add response trailers to be included in the error response.
    #[must_use]
    pub fn with_trailers(mut self, trailers: http::HeaderMap) -> Self {
        self.trailers = box_headers(trailers);
        self
    }

    /// Borrow the response headers. Returns an empty map if none were set.
    pub fn response_headers(&self) -> &http::HeaderMap {
        self.response_headers.as_deref().unwrap_or(&EMPTY_HEADERS)
    }

    /// Borrow the response trailers. Returns an empty map if none were set.
    pub fn trailers(&self) -> &http::HeaderMap {
        self.trailers.as_deref().unwrap_or(&EMPTY_HEADERS)
    }

    /// Mutably borrow the response headers, allocating an empty map if
    /// none were set.
    pub fn response_headers_mut(&mut self) -> &mut http::HeaderMap {
        self.response_headers.get_or_insert_default()
    }

    /// Mutably borrow the response trailers, allocating an empty map if
    /// none were set.
    pub fn trailers_mut(&mut self) -> &mut http::HeaderMap {
        self.trailers.get_or_insert_default()
    }

    /// Replace the response headers. An empty map is stored as `None`.
    pub fn set_response_headers(&mut self, headers: http::HeaderMap) {
        self.response_headers = box_headers(headers);
    }

    /// Replace the response trailers. An empty map is stored as `None`.
    pub fn set_trailers(&mut self, trailers: http::HeaderMap) {
        self.trailers = box_headers(trailers);
    }

    /// Set an HTTP status override for this error.
    ///
    /// When set, this overrides the default HTTP status derived from the error code.
    /// This is useful for HTTP-level errors like 415 Unsupported Media Type.
    #[must_use]
    pub fn with_http_status(mut self, status: StatusCode) -> Self {
        self.http_status_override = Some(status);
        self
    }

    /// Create an error for unsupported media type (HTTP 415).
    ///
    /// This is used when the client sends a content type that the server doesn't support.
    pub fn unsupported_media_type(message: impl Into<String>) -> Self {
        // Connect protocol specifies Unknown for unsupported content types
        Self::new(ErrorCode::Unknown, message).with_http_status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
    }

    /// Create an error for method not allowed (HTTP 405).
    ///
    /// This is used when the client uses an HTTP method other than POST.
    pub fn method_not_allowed(message: impl Into<String>) -> Self {
        // Connect protocol specifies Unknown for wrong HTTP method
        Self::new(ErrorCode::Unknown, message).with_http_status(StatusCode::METHOD_NOT_ALLOWED)
    }

    /// Create a canceled error.
    pub fn canceled(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Canceled, message)
    }

    /// Create an unknown error.
    pub fn unknown(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unknown, message)
    }

    /// Create an invalid argument error.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidArgument, message)
    }

    /// Create a deadline exceeded error.
    pub fn deadline_exceeded(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::DeadlineExceeded, message)
    }

    /// Create a not found error.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    /// Create an already exists error.
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::AlreadyExists, message)
    }

    /// Create a permission denied error.
    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::PermissionDenied, message)
    }

    /// Create a resource exhausted error.
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::ResourceExhausted, message)
    }

    /// Create a failed precondition error.
    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::FailedPrecondition, message)
    }

    /// Create an aborted error.
    pub fn aborted(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Aborted, message)
    }

    /// Create an out of range error.
    pub fn out_of_range(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::OutOfRange, message)
    }

    /// Create an unimplemented error.
    pub fn unimplemented(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unimplemented, message)
    }

    /// Create an internal error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    /// Create an unavailable error.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unavailable, message)
    }

    /// Create a data loss error.
    pub fn data_loss(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::DataLoss, message)
    }

    /// Create an unauthenticated error.
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthenticated, message)
    }

    /// Add an error detail.
    #[must_use]
    pub fn with_detail(mut self, detail: ErrorDetail) -> Self {
        self.details.push(detail);
        self
    }

    /// Attach the underlying cause, surfaced through
    /// [`Error::source`](std::error::Error::source).
    ///
    /// Unlike `message` (which is sent over the wire and shown to callers),
    /// the source is local-only — useful for logging/observability without
    /// leaking internal detail to the client. It is never populated by
    /// decoding a `ConnectError` received over the wire (there is nothing to
    /// attach), only by local code that calls this method — so `source()`
    /// on an error a client parsed from a server response is always `None`.
    /// Accepts either a concrete error or an already-boxed one, so it
    /// composes with transport errors that are type-erased before reaching
    /// this call. Replaces any source attached by a previous call.
    #[must_use]
    pub fn with_source(
        mut self,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        self.source = Some(Arc::from(source.into()));
        self
    }

    /// Get the HTTP status code for this error.
    ///
    /// Returns the HTTP status override if set, otherwise derives it from the error code.
    pub fn http_status(&self) -> StatusCode {
        self.http_status_override
            .unwrap_or_else(|| self.code.http_status())
    }

    /// Encode this error as JSON bytes.
    pub fn to_json(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(self).unwrap_or_else(|_| {
            // Fallback: produce minimal valid Connect error JSON.
            format!(r#"{{"code":"{}"}}"#, self.code.as_str()).into_bytes()
        }))
    }
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code.as_str())?;
        if let Some(ref message) = self.message {
            write!(f, ": {message}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| &**e as &(dyn std::error::Error + 'static))
    }
}

impl From<std::io::Error> for ConnectError {
    fn from(err: std::io::Error) -> Self {
        Self::internal(err.to_string()).with_source(err)
    }
}

/// Lets `Response::try_with_header(..)?` propagate naturally inside a
/// handler.
impl From<http::Error> for ConnectError {
    fn from(err: http::Error) -> Self {
        Self::internal(err.to_string()).with_source(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_http_error_is_internal() {
        let http_err: http::Error = http::HeaderValue::from_bytes(b"bad\nval")
            .unwrap_err()
            .into();
        let e: ConnectError = http_err.into();
        assert_eq!(e.code, ErrorCode::Internal);
    }

    #[test]
    fn from_io_error_preserves_source() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let e: ConnectError = io_err.into();
        let source = std::error::Error::source(&e).expect("source must be preserved");
        assert_eq!(source.to_string(), "refused");
    }

    #[test]
    fn from_http_error_preserves_source() {
        let http_err: http::Error = http::HeaderValue::from_bytes(b"bad\nval")
            .unwrap_err()
            .into();
        let e: ConnectError = http_err.into();
        assert!(std::error::Error::source(&e).is_some());
    }

    #[test]
    fn with_source_is_returned_by_error_source() {
        let cause = std::io::Error::other("boom");
        let e = ConnectError::unavailable("wrapped").with_source(cause);
        let source = std::error::Error::source(&e).expect("source must be set");
        assert_eq!(source.to_string(), "boom");
    }

    #[test]
    fn with_source_accepts_already_boxed_error() {
        let boxed: Box<dyn std::error::Error + Send + Sync> =
            Box::new(std::io::Error::other("boxed boom"));
        let e = ConnectError::unavailable("wrapped").with_source(boxed);
        let source = std::error::Error::source(&e).expect("source must be set");
        assert_eq!(source.to_string(), "boxed boom");
    }

    #[test]
    fn no_source_by_default() {
        let e = ConnectError::internal("plain");
        assert!(std::error::Error::source(&e).is_none());
    }

    #[test]
    fn with_source_survives_clone() {
        let cause = std::io::Error::other("boom");
        let e = ConnectError::unavailable("wrapped")
            .with_source(cause)
            .clone();
        assert!(std::error::Error::source(&e).is_some());
    }

    #[test]
    fn test_grpc_code_round_trip() {
        let codes = [
            ErrorCode::Canceled,
            ErrorCode::Unknown,
            ErrorCode::InvalidArgument,
            ErrorCode::DeadlineExceeded,
            ErrorCode::NotFound,
            ErrorCode::AlreadyExists,
            ErrorCode::PermissionDenied,
            ErrorCode::ResourceExhausted,
            ErrorCode::FailedPrecondition,
            ErrorCode::Aborted,
            ErrorCode::OutOfRange,
            ErrorCode::Unimplemented,
            ErrorCode::Internal,
            ErrorCode::Unavailable,
            ErrorCode::DataLoss,
            ErrorCode::Unauthenticated,
        ];

        for code in codes {
            let grpc = code.grpc_code();
            let back = ErrorCode::from_grpc_code(grpc);
            assert_eq!(
                back,
                Some(code),
                "round-trip failed for {code:?} (grpc code {grpc})"
            );
        }
    }

    #[test]
    fn test_grpc_code_values() {
        assert_eq!(ErrorCode::Canceled.grpc_code(), 1);
        assert_eq!(ErrorCode::Unknown.grpc_code(), 2);
        assert_eq!(ErrorCode::Internal.grpc_code(), 13);
        assert_eq!(ErrorCode::Unauthenticated.grpc_code(), 16);
    }

    #[test]
    fn test_from_grpc_code_ok_returns_none() {
        assert_eq!(ErrorCode::from_grpc_code(0), None);
    }

    #[test]
    fn test_from_grpc_code_unknown_returns_none() {
        assert_eq!(ErrorCode::from_grpc_code(17), None);
        assert_eq!(ErrorCode::from_grpc_code(999), None);
    }

    #[test]
    fn connect_error_stays_under_result_large_err_threshold() {
        // clippy::result_large_err fires at 128 bytes. Keep some headroom so
        // adding a small field doesn't immediately re-trip the lint.
        const THRESHOLD: usize = 96;
        let size = std::mem::size_of::<ConnectError>();
        assert!(
            size <= THRESHOLD,
            "ConnectError is {size} bytes (threshold {THRESHOLD}); \
             box large fields to keep Result<_, ConnectError> cheap to move"
        );
    }

    #[test]
    fn header_accessors() {
        let mut e = ConnectError::internal("x");
        assert!(e.response_headers().is_empty());
        assert!(e.trailers().is_empty());

        // Setting an empty map stays None.
        e.set_response_headers(http::HeaderMap::new());
        assert!(e.response_headers.is_none());
        assert!(
            ConnectError::new(ErrorCode::Internal, "x")
                .with_headers(http::HeaderMap::new())
                .response_headers
                .is_none()
        );

        e.trailers_mut()
            .insert("x-t", http::HeaderValue::from_static("v"));
        assert_eq!(e.trailers().get("x-t").unwrap(), "v");

        let mut h = http::HeaderMap::new();
        h.insert("x-h", http::HeaderValue::from_static("w"));
        let e = e.with_headers(h);
        assert_eq!(e.response_headers().get("x-h").unwrap(), "w");
    }
}
