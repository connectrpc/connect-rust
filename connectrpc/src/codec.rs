//! Message encoding and decoding for ConnectRPC.
//!
//! This module provides codec implementations for serializing and deserializing
//! protobuf messages in both binary proto and JSON formats.

use buffa::Message;
use bytes::Bytes;
#[cfg(feature = "json")]
use serde::Serialize;
#[cfg(feature = "json")]
use serde::de::DeserializeOwned;

use crate::error::ConnectError;

/// Content types supported by ConnectRPC.
pub mod content_type {
    /// Binary protobuf content type.
    pub const PROTO: &str = "application/proto";
    /// JSON content type.
    pub const JSON: &str = "application/json";
    /// Connect streaming proto content type.
    pub const CONNECT_PROTO: &str = "application/connect+proto";
    /// Connect streaming JSON content type.
    pub const CONNECT_JSON: &str = "application/connect+json";
}

/// Connect protocol header names.
pub mod header {
    /// Declares the Connect protocol version (always `"1"`).
    pub const PROTOCOL_VERSION: &str = "connect-protocol-version";
    /// Request timeout in milliseconds.
    pub const TIMEOUT_MS: &str = "connect-timeout-ms";
    /// Content encoding for Connect streaming requests/responses.
    pub const CONTENT_ENCODING: &str = "connect-content-encoding";
    /// Accepted content encodings for Connect streaming requests/responses.
    pub const ACCEPT_ENCODING: &str = "connect-accept-encoding";
}

/// Marker bound for message types the JSON codec can **serialize**.
///
/// When the `json` feature is enabled this is exactly [`serde::Serialize`]:
/// if a bound such as `T: Message + JsonSerialize` fails to hold, derive
/// `serde::Serialize` on `T` (generated code does this unless you pass the
/// codegen `no_json` option). When the feature is disabled it is an empty
/// bound satisfied by every type, so proto-only message types generated
/// without serde derives still qualify and the JSON codec is simply
/// unavailable at runtime.
///
/// Auto-implemented for every qualifying type — do not implement it manually.
#[cfg(feature = "json")]
pub trait JsonSerialize: Serialize {}
#[cfg(feature = "json")]
impl<T: Serialize> JsonSerialize for T {}

/// Marker bound for message types the JSON codec can **serialize**.
///
/// With the `json` feature disabled this is an empty bound, so message types
/// without serde derives satisfy it. See the `json`-enabled definition for
/// the full contract.
///
/// Auto-implemented for every type — do not implement it manually.
#[cfg(not(feature = "json"))]
pub trait JsonSerialize {}
#[cfg(not(feature = "json"))]
impl<T> JsonSerialize for T {}

/// Marker bound for message types the JSON codec can **deserialize**.
///
/// When the `json` feature is enabled this is exactly
/// [`serde::de::DeserializeOwned`]: if a bound such as
/// `T: Message + JsonDeserialize` fails to hold, derive `serde::Deserialize`
/// on `T` (generated code does this unless you pass the codegen `no_json`
/// option). When the feature is disabled it is an empty bound satisfied by
/// every type, so proto-only message types generated without serde derives
/// still qualify.
///
/// Auto-implemented for every qualifying type — do not implement it manually.
#[cfg(feature = "json")]
pub trait JsonDeserialize: DeserializeOwned {}
#[cfg(feature = "json")]
impl<T: DeserializeOwned> JsonDeserialize for T {}

/// Marker bound for message types the JSON codec can **deserialize**.
///
/// With the `json` feature disabled this is an empty bound. See the
/// `json`-enabled definition for the full contract.
///
/// Auto-implemented for every type — do not implement it manually.
#[cfg(not(feature = "json"))]
pub trait JsonDeserialize {}
#[cfg(not(feature = "json"))]
impl<T> JsonDeserialize for T {}

/// Encode a protobuf message to binary format.
pub fn encode_proto<M: Message>(message: &M) -> Result<Bytes, ConnectError> {
    Ok(message.encode_to_bytes())
}

/// Decode bytes into a protobuf message.
pub fn decode_proto<M: Message>(data: &[u8]) -> Result<M, ConnectError> {
    M::decode_from_slice(data)
        .map_err(|e| ConnectError::invalid_argument(format!("failed to decode proto: {e}")))
}

/// Message shared by the JSON codec entry points when the `json` feature is
/// disabled.
#[cfg(not(feature = "json"))]
pub(crate) const JSON_FEATURE_DISABLED: &str =
    "JSON codec not compiled in (connectrpc built without the `json` feature)";

/// Encode a message to JSON format.
///
/// This (with [`decode_json`]) is the single place the `json` feature is gated:
/// with it disabled, the JSON codec is unavailable and this returns
/// [`ErrorCode::Unimplemented`](crate::ErrorCode::Unimplemented) without
/// requiring `M: serde::Serialize`, so proto-only callers compile. Callers can
/// therefore invoke it unconditionally on their `CodecFormat::Json` arm.
#[cfg(feature = "json")]
pub fn encode_json<M: Serialize>(message: &M) -> Result<Bytes, ConnectError> {
    serde_json::to_vec(message)
        .map(Bytes::from)
        .map_err(|e| ConnectError::internal(format!("failed to encode JSON: {e}")))
}

/// Encode a message to JSON format — proto-only build: always `Unimplemented`.
#[cfg(not(feature = "json"))]
pub fn encode_json<M>(_message: &M) -> Result<Bytes, ConnectError> {
    Err(ConnectError::unimplemented(JSON_FEATURE_DISABLED))
}

/// Decode JSON bytes into a message.
///
/// See [`encode_json`]: with the `json` feature disabled this returns
/// [`ErrorCode::Unimplemented`](crate::ErrorCode::Unimplemented) without
/// requiring `M: serde::de::DeserializeOwned`.
#[cfg(feature = "json")]
pub fn decode_json<M: DeserializeOwned>(data: &[u8]) -> Result<M, ConnectError> {
    serde_json::from_slice(data)
        .map_err(|e| ConnectError::invalid_argument(format!("failed to decode JSON: {e}")))
}

/// Decode JSON bytes into a message — proto-only build: always `Unimplemented`.
#[cfg(not(feature = "json"))]
pub fn decode_json<M>(_data: &[u8]) -> Result<M, ConnectError> {
    Err(ConnectError::unimplemented(JSON_FEATURE_DISABLED))
}

/// Codec for binary protobuf encoding.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProtoCodec;

impl ProtoCodec {
    /// Get the content type for this codec.
    pub fn content_type() -> &'static str {
        content_type::PROTO
    }

    /// Encode a protobuf message to bytes.
    pub fn encode<M: Message>(message: &M) -> Result<Bytes, ConnectError> {
        encode_proto(message)
    }

    /// Decode bytes into a protobuf message.
    pub fn decode<M: Message>(data: &[u8]) -> Result<M, ConnectError> {
        decode_proto(data)
    }
}

/// Codec for JSON encoding of protobuf messages.
#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonCodec;

#[cfg(feature = "json")]
impl JsonCodec {
    /// Get the content type for this codec.
    pub fn content_type() -> &'static str {
        content_type::JSON
    }

    /// Encode a message to JSON bytes.
    pub fn encode<M: Serialize>(message: &M) -> Result<Bytes, ConnectError> {
        encode_json(message)
    }

    /// Decode JSON bytes into a message.
    pub fn decode<M: DeserializeOwned>(data: &[u8]) -> Result<M, ConnectError> {
        decode_json(data)
    }
}

/// Supported codec formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CodecFormat {
    /// Binary protobuf format.
    Proto,
    /// JSON format.
    ///
    /// Fully supported only when the `json` feature is enabled. With it
    /// disabled the variant still exists (so content-type negotiation can
    /// recognize JSON requests), but encoding or decoding a *message* in this
    /// format returns [`ErrorCode::Unimplemented`](crate::ErrorCode::Unimplemented)
    /// at runtime. Connect *error* bodies are always JSON regardless.
    Json,
}

impl std::fmt::Display for CodecFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Proto => write!(f, "proto"),
            Self::Json => write!(f, "json"),
        }
    }
}

impl CodecFormat {
    /// Parse codec format from content type string.
    ///
    /// These parsers stay codec-agnostic regardless of the `json` feature:
    /// a JSON content type still parses to [`CodecFormat::Json`] in a
    /// proto-only build so negotiation can recognize the request and the
    /// codec layer can surface a precise `Unimplemented` error. The feature
    /// gating lives at encode/decode, not here.
    pub fn from_content_type(content_type: &str) -> Option<Self> {
        if content_type.starts_with(content_type::PROTO)
            || content_type.starts_with(content_type::CONNECT_PROTO)
        {
            Some(Self::Proto)
        } else if content_type.starts_with(content_type::JSON)
            || content_type.starts_with(content_type::CONNECT_JSON)
        {
            Some(Self::Json)
        } else {
            None
        }
    }

    /// Parse codec format from encoding name (used in GET request query params).
    ///
    /// Accepts "proto" or "json" (the values used in the `encoding` query parameter).
    pub fn from_codec(codec: &str) -> Option<Self> {
        match codec {
            "proto" => Some(Self::Proto),
            "json" => Some(Self::Json),
            _ => None,
        }
    }

    /// Get the content type string for this format (unary RPC).
    #[inline]
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Proto => content_type::PROTO,
            Self::Json => content_type::JSON,
        }
    }

    /// Get the streaming content type string for this format.
    #[inline]
    pub fn streaming_content_type(&self) -> &'static str {
        match self {
            Self::Proto => content_type::CONNECT_PROTO,
            Self::Json => content_type::CONNECT_JSON,
        }
    }

    /// Check if the given content type indicates a streaming request.
    #[inline]
    pub fn is_streaming_content_type(content_type: &str) -> bool {
        content_type.starts_with(content_type::CONNECT_PROTO)
            || content_type.starts_with(content_type::CONNECT_JSON)
    }
}

#[cfg(test)]
mod tests {
    /// With the `json` feature disabled, the message-type markers must be
    /// empty bounds: a type with no serde derives — as emitted by the codegen
    /// `no_json` option — still satisfies them. This is exactly what lets
    /// proto-only generated code compile against this crate. (When `json` is
    /// enabled the markers are `serde::Serialize` / `DeserializeOwned`, so the
    /// assertion below would not even build — hence the `cfg`.)
    #[cfg(not(feature = "json"))]
    #[test]
    fn markers_are_empty_bounds_without_json() {
        use super::{JsonDeserialize, JsonSerialize};

        // Derives neither `Serialize` nor `Deserialize`.
        struct NoSerde;

        fn assert_serialize<T: JsonSerialize>() {}
        fn assert_deserialize<T: JsonDeserialize>() {}

        assert_serialize::<NoSerde>();
        assert_deserialize::<NoSerde>();
    }
}
