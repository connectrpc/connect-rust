//! Protocol detection and abstraction for multi-protocol support.
//!
//! ConnectRPC implementations can support three wire protocols:
//! - **Connect** - HTTP-native protocol with standard HTTP semantics
//! - **gRPC** - Standard gRPC over HTTP/2
//! - **gRPC-Web** - gRPC-Web for browser compatibility over HTTP/1.1+
//!
//! All three share the same RPC semantics (services, methods, error codes,
//! streaming types) but differ in wire encoding: content types, framing,
//! header conventions, trailer delivery, and error representation.
//!
//! Protocol detection is performed by examining the `Content-Type` header
//! of incoming requests.

use http::HeaderMap;
use http::header::CONTENT_TYPE;

use crate::codec::CodecFormat;

/// Pre-parsed header names for response building.
///
/// `Response::Builder::header(&str, _)` re-parses the header name via
/// `HeaderName::from_bytes` every call (validate each byte + possible
/// alloc). Profiling showed this at ~0.7% CPU on the echo hot path.
/// Using pre-built `HeaderName` statics skips the parse.
pub(crate) mod hdr {
    use http::HeaderName;

    pub static CONNECT_CONTENT_ENCODING: HeaderName =
        HeaderName::from_static("connect-content-encoding");
    pub static CONNECT_ACCEPT_ENCODING: HeaderName =
        HeaderName::from_static("connect-accept-encoding");
    pub static GRPC_ENCODING: HeaderName = HeaderName::from_static("grpc-encoding");
    pub static GRPC_ACCEPT_ENCODING: HeaderName = HeaderName::from_static("grpc-accept-encoding");
    pub static GRPC_STATUS: HeaderName = HeaderName::from_static("grpc-status");
    pub static GRPC_MESSAGE: HeaderName = HeaderName::from_static("grpc-message");
    pub static GRPC_STATUS_DETAILS_BIN: HeaderName =
        HeaderName::from_static("grpc-status-details-bin");
}

/// The wire protocol used for an RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Protocol {
    /// Connect protocol - HTTP-native with standard HTTP semantics.
    ///
    /// Unary RPCs use bare message bodies and real HTTP status codes.
    /// Streaming RPCs use envelope framing with JSON end-of-stream messages.
    Connect,
    /// gRPC protocol - standard gRPC over HTTP/2.
    ///
    /// All RPCs use envelope framing. Status is communicated via HTTP/2
    /// trailing HEADERS (`grpc-status`, `grpc-message`). HTTP status is
    /// always 200.
    Grpc,
    /// gRPC-Web protocol - gRPC-Web for browser compatibility.
    ///
    /// Like gRPC but works over HTTP/1.1. Trailers are encoded in the
    /// response body as a final frame with flag byte 0x80.
    GrpcWeb,
}

/// The result of detecting a protocol from a request's Content-Type.
#[derive(Debug, Clone, Copy)]
pub struct RequestProtocol {
    /// The detected wire protocol.
    pub protocol: Protocol,
    /// The codec format (proto or JSON) for message serialization.
    pub codec_format: CodecFormat,
    /// Whether this is a streaming request (determined by content-type).
    ///
    /// For Connect, streaming uses `application/connect+{proto,json}`.
    /// For gRPC and gRPC-Web, all RPCs use the same content type
    /// (streaming is implicit in the method definition).
    pub is_streaming: bool,
    /// Whether this is a gRPC-Web text-mode request (base64-encoded).
    pub is_text_mode: bool,
}

impl Protocol {
    /// Detect the protocol, codec format, and streaming mode from request headers.
    ///
    /// Returns `None` if the Content-Type doesn't match any known protocol.
    pub fn detect(headers: &HeaderMap) -> Option<RequestProtocol> {
        let content_type = headers.get(CONTENT_TYPE)?.to_str().ok()?;
        Self::detect_from_content_type(content_type)
    }

    /// Detect the protocol from a Content-Type string.
    ///
    /// Returns `None` if the content type doesn't match any known protocol.
    pub fn detect_from_content_type(content_type: &str) -> Option<RequestProtocol> {
        // Strip any parameters (e.g., charset) after semicolon
        let content_type = content_type
            .split(';')
            .next()
            .unwrap_or(content_type)
            .trim();

        // Check gRPC-Web first (more specific prefix than gRPC)
        if let Some(rest) = content_type.strip_prefix("application/grpc-web-text") {
            // Text mode is only meaningful for binary proto — reject +json
            let codec_format = match rest {
                "" | "+proto" => CodecFormat::Proto,
                _ => return None,
            };
            return Some(RequestProtocol {
                protocol: Protocol::GrpcWeb,
                codec_format,
                is_streaming: true,
                is_text_mode: true,
            });
        }

        if let Some(rest) = content_type.strip_prefix("application/grpc-web") {
            let codec_format = Self::grpc_subtype_to_codec(rest)?;
            return Some(RequestProtocol {
                protocol: Protocol::GrpcWeb,
                codec_format,
                is_streaming: true,
                is_text_mode: false,
            });
        }

        // Check gRPC (must come after gRPC-Web since "application/grpc" is
        // a prefix of "application/grpc-web")
        if let Some(rest) = content_type.strip_prefix("application/grpc") {
            let codec_format = Self::grpc_subtype_to_codec(rest)?;
            return Some(RequestProtocol {
                protocol: Protocol::Grpc,
                codec_format,
                is_streaming: true,
                is_text_mode: false,
            });
        }

        // Check Connect streaming
        if let Some(rest) = content_type.strip_prefix("application/connect+") {
            // The `json` arm is gated: with the `json` feature off this is a
            // proto-only build, so a JSON content type is an unsupported media
            // type and must be rejected here at negotiation (the caller maps a
            // `None` to HTTP 415 / a gRPC "unsupported content type" error)
            // rather than accepted and failed late at decode.
            let codec_format = match rest {
                "proto" => CodecFormat::Proto,
                #[cfg(feature = "json")]
                "json" => CodecFormat::Json,
                _ => return None,
            };
            return Some(RequestProtocol {
                protocol: Protocol::Connect,
                codec_format,
                is_streaming: true,
                is_text_mode: false,
            });
        }

        // Check Connect unary. `application/json` is gated for the same reason
        // as the streaming `+json` arm above: a proto-only build rejects it as
        // an unsupported media type.
        match content_type {
            "application/proto" => Some(RequestProtocol {
                protocol: Protocol::Connect,
                codec_format: CodecFormat::Proto,
                is_streaming: false,
                is_text_mode: false,
            }),
            #[cfg(feature = "json")]
            "application/json" => Some(RequestProtocol {
                protocol: Protocol::Connect,
                codec_format: CodecFormat::Json,
                is_streaming: false,
                is_text_mode: false,
            }),
            _ => None,
        }
    }

    /// Parse gRPC subtype suffix to codec format.
    ///
    /// - `""` or `"+proto"` → Proto (default)
    /// - `"+json"` → Json
    /// - anything else → None
    fn grpc_subtype_to_codec(suffix: &str) -> Option<CodecFormat> {
        match suffix {
            "" => Some(CodecFormat::Proto),
            "+proto" => Some(CodecFormat::Proto),
            // Gated: a proto-only build (no `json` feature) rejects
            // `application/grpc+json` / `application/grpc-web+json` as an
            // unsupported codec at negotiation.
            #[cfg(feature = "json")]
            "+json" => Some(CodecFormat::Json),
            _ => None,
        }
    }

    /// Get the Content-Type header value for a response with this protocol and codec.
    #[inline]
    pub fn response_content_type(&self, format: CodecFormat, is_streaming: bool) -> &'static str {
        match (self, format, is_streaming) {
            // Connect unary
            (Protocol::Connect, CodecFormat::Proto, false) => "application/proto",
            (Protocol::Connect, CodecFormat::Json, false) => "application/json",
            // Connect streaming
            (Protocol::Connect, CodecFormat::Proto, true) => "application/connect+proto",
            (Protocol::Connect, CodecFormat::Json, true) => "application/connect+json",
            // gRPC (always "streaming" framing)
            (Protocol::Grpc, CodecFormat::Proto, _) => "application/grpc+proto",
            (Protocol::Grpc, CodecFormat::Json, _) => "application/grpc+json",
            // gRPC-Web
            (Protocol::GrpcWeb, CodecFormat::Proto, _) => "application/grpc-web+proto",
            (Protocol::GrpcWeb, CodecFormat::Json, _) => "application/grpc-web+json",
        }
    }

    /// Get the timeout header name for this protocol.
    #[inline]
    pub fn timeout_header(&self) -> &'static str {
        match self {
            Protocol::Connect => "connect-timeout-ms",
            Protocol::Grpc | Protocol::GrpcWeb => "grpc-timeout",
        }
    }

    /// Get the content-encoding header name for this protocol (streaming).
    ///
    /// Returns a pre-parsed `HeaderName` so callers can pass it directly to
    /// `HeaderMap::insert` without the `from_bytes` re-parse that
    /// `Response::Builder::header(&str, _)` does.
    #[inline]
    pub fn content_encoding_header(&self) -> &'static http::HeaderName {
        match self {
            Protocol::Connect => &hdr::CONNECT_CONTENT_ENCODING,
            Protocol::Grpc | Protocol::GrpcWeb => &hdr::GRPC_ENCODING,
        }
    }

    /// Get the accept-encoding header name for this protocol (streaming).
    #[inline]
    pub fn accept_encoding_header(&self) -> &'static http::HeaderName {
        match self {
            Protocol::Connect => &hdr::CONNECT_ACCEPT_ENCODING,
            Protocol::Grpc | Protocol::GrpcWeb => &hdr::GRPC_ACCEPT_ENCODING,
        }
    }

    /// Whether this protocol uses real HTTP status codes for errors.
    ///
    /// Connect uses standard HTTP status codes for unary errors.
    /// gRPC and gRPC-Web always return HTTP 200, with errors in trailers.
    #[inline]
    pub fn uses_http_status_codes(&self) -> bool {
        matches!(self, Protocol::Connect)
    }

    /// Whether this protocol requires HTTP/2.
    #[inline]
    pub fn requires_http2(&self) -> bool {
        matches!(self, Protocol::Grpc)
    }

    /// Whether this protocol sends trailers via HTTP/2 HEADERS frames.
    ///
    /// gRPC uses HTTP/2 trailers; Connect and gRPC-Web encode trailers in the body.
    #[inline]
    pub fn uses_http_trailers(&self) -> bool {
        matches!(self, Protocol::Grpc)
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Protocol::Connect => write!(f, "connect"),
            Protocol::Grpc => write!(f, "grpc"),
            Protocol::GrpcWeb => write!(f, "grpc-web"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_connect_unary_proto() {
        let result = Protocol::detect_from_content_type("application/proto").unwrap();
        assert_eq!(result.protocol, Protocol::Connect);
        assert_eq!(result.codec_format, CodecFormat::Proto);
        assert!(!result.is_streaming);
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_detect_connect_unary_json() {
        let result = Protocol::detect_from_content_type("application/json").unwrap();
        assert_eq!(result.protocol, Protocol::Connect);
        assert_eq!(result.codec_format, CodecFormat::Json);
        assert!(!result.is_streaming);
    }

    #[test]
    fn test_detect_connect_streaming_proto() {
        let result = Protocol::detect_from_content_type("application/connect+proto").unwrap();
        assert_eq!(result.protocol, Protocol::Connect);
        assert_eq!(result.codec_format, CodecFormat::Proto);
        assert!(result.is_streaming);
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_detect_connect_streaming_json() {
        let result = Protocol::detect_from_content_type("application/connect+json").unwrap();
        assert_eq!(result.protocol, Protocol::Connect);
        assert_eq!(result.codec_format, CodecFormat::Json);
        assert!(result.is_streaming);
    }

    #[test]
    fn test_detect_grpc_default() {
        let result = Protocol::detect_from_content_type("application/grpc").unwrap();
        assert_eq!(result.protocol, Protocol::Grpc);
        assert_eq!(result.codec_format, CodecFormat::Proto);
        assert!(result.is_streaming);
    }

    #[test]
    fn test_detect_grpc_proto() {
        let result = Protocol::detect_from_content_type("application/grpc+proto").unwrap();
        assert_eq!(result.protocol, Protocol::Grpc);
        assert_eq!(result.codec_format, CodecFormat::Proto);
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_detect_grpc_json() {
        let result = Protocol::detect_from_content_type("application/grpc+json").unwrap();
        assert_eq!(result.protocol, Protocol::Grpc);
        assert_eq!(result.codec_format, CodecFormat::Json);
    }

    #[test]
    fn test_detect_grpc_web_default() {
        let result = Protocol::detect_from_content_type("application/grpc-web").unwrap();
        assert_eq!(result.protocol, Protocol::GrpcWeb);
        assert_eq!(result.codec_format, CodecFormat::Proto);
        assert!(!result.is_text_mode);
    }

    #[test]
    fn test_detect_grpc_web_proto() {
        let result = Protocol::detect_from_content_type("application/grpc-web+proto").unwrap();
        assert_eq!(result.protocol, Protocol::GrpcWeb);
        assert_eq!(result.codec_format, CodecFormat::Proto);
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_detect_grpc_web_json() {
        let result = Protocol::detect_from_content_type("application/grpc-web+json").unwrap();
        assert_eq!(result.protocol, Protocol::GrpcWeb);
        assert_eq!(result.codec_format, CodecFormat::Json);
    }

    #[test]
    fn test_detect_grpc_web_text() {
        let result = Protocol::detect_from_content_type("application/grpc-web-text").unwrap();
        assert_eq!(result.protocol, Protocol::GrpcWeb);
        assert_eq!(result.codec_format, CodecFormat::Proto);
        assert!(result.is_text_mode);
    }

    #[test]
    fn test_detect_grpc_web_text_proto() {
        let result = Protocol::detect_from_content_type("application/grpc-web-text+proto").unwrap();
        assert_eq!(result.protocol, Protocol::GrpcWeb);
        assert_eq!(result.codec_format, CodecFormat::Proto);
        assert!(result.is_text_mode);
    }

    #[test]
    fn test_detect_unknown() {
        assert!(Protocol::detect_from_content_type("text/html").is_none());
        assert!(Protocol::detect_from_content_type("application/xml").is_none());
    }

    #[test]
    fn test_detect_grpc_web_text_json_rejected() {
        // Text mode is only meaningful for binary proto, not JSON
        assert!(Protocol::detect_from_content_type("application/grpc-web-text+json").is_none());
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_detect_with_charset_parameter() {
        let result = Protocol::detect_from_content_type("application/json; charset=utf-8").unwrap();
        assert_eq!(result.protocol, Protocol::Connect);
        assert_eq!(result.codec_format, CodecFormat::Json);
    }

    #[cfg(not(feature = "json"))]
    #[test]
    fn test_detect_with_charset_parameter_proto_only() {
        // Charset stripping still applies in a proto-only build.
        let result =
            Protocol::detect_from_content_type("application/proto; charset=utf-8").unwrap();
        assert_eq!(result.protocol, Protocol::Connect);
        assert_eq!(result.codec_format, CodecFormat::Proto);
    }

    // Proto-only build (no `json` feature): every JSON content type is an
    // unsupported media type and must be declined at negotiation, so the
    // dispatch layer maps it to HTTP 415 / a gRPC "unsupported content type"
    // error rather than accepting it and failing late at decode.
    #[cfg(not(feature = "json"))]
    #[test]
    fn test_detect_json_content_types_rejected_without_feature() {
        for ct in [
            "application/json",
            "application/json; charset=utf-8",
            "application/connect+json",
            "application/grpc+json",
            "application/grpc-web+json",
        ] {
            assert!(
                Protocol::detect_from_content_type(ct).is_none(),
                "{ct} must not be detected in a proto-only build"
            );
        }
        // Proto content types are still detected.
        assert!(Protocol::detect_from_content_type("application/proto").is_some());
        assert!(Protocol::detect_from_content_type("application/connect+proto").is_some());
        assert!(Protocol::detect_from_content_type("application/grpc+proto").is_some());
    }

    #[test]
    fn test_detect_grpc_not_confused_with_grpc_web() {
        // "application/grpc" must not match as gRPC-Web
        let result = Protocol::detect_from_content_type("application/grpc").unwrap();
        assert_eq!(result.protocol, Protocol::Grpc);

        let result = Protocol::detect_from_content_type("application/grpc-web").unwrap();
        assert_eq!(result.protocol, Protocol::GrpcWeb);
    }

    #[test]
    fn test_response_content_types() {
        assert_eq!(
            Protocol::Connect.response_content_type(CodecFormat::Proto, false),
            "application/proto"
        );
        assert_eq!(
            Protocol::Connect.response_content_type(CodecFormat::Json, true),
            "application/connect+json"
        );
        assert_eq!(
            Protocol::Grpc.response_content_type(CodecFormat::Proto, true),
            "application/grpc+proto"
        );
        assert_eq!(
            Protocol::GrpcWeb.response_content_type(CodecFormat::Json, false),
            "application/grpc-web+json"
        );
    }

    #[test]
    fn test_protocol_properties() {
        assert!(Protocol::Connect.uses_http_status_codes());
        assert!(!Protocol::Grpc.uses_http_status_codes());
        assert!(!Protocol::GrpcWeb.uses_http_status_codes());

        assert!(!Protocol::Connect.requires_http2());
        assert!(Protocol::Grpc.requires_http2());
        assert!(!Protocol::GrpcWeb.requires_http2());

        assert!(!Protocol::Connect.uses_http_trailers());
        assert!(Protocol::Grpc.uses_http_trailers());
        assert!(!Protocol::GrpcWeb.uses_http_trailers());
    }

    #[test]
    fn test_header_names() {
        assert_eq!(Protocol::Connect.timeout_header(), "connect-timeout-ms");
        assert_eq!(Protocol::Grpc.timeout_header(), "grpc-timeout");
        assert_eq!(Protocol::GrpcWeb.timeout_header(), "grpc-timeout");

        assert_eq!(
            Protocol::Connect.content_encoding_header().as_str(),
            "connect-content-encoding"
        );
        assert_eq!(
            Protocol::Grpc.content_encoding_header().as_str(),
            "grpc-encoding"
        );

        assert_eq!(
            Protocol::Connect.accept_encoding_header().as_str(),
            "connect-accept-encoding"
        );
        assert_eq!(
            Protocol::Grpc.accept_encoding_header().as_str(),
            "grpc-accept-encoding"
        );
    }
}
