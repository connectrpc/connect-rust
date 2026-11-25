//! ConnectRPC conformance test harness for the connectrpc crate.
//!
//! This crate implements the [ConnectRPC conformance test suite](https://github.com/connectrpc/conformance)
//! to validate that connectrpc correctly implements the Connect protocol.
//!
//! ## Architecture
//!
//! The conformance test runner communicates with implementations via stdin/stdout
//! using length-prefixed protobuf messages.
//!
//! ### Server Testing
//! ```text
//! connectconformance --mode server -- ./conformance-server
//! ```
//! 1. Runner sends `ServerCompatRequest` to stdin
//! 2. Server starts HTTP server with `ConformanceService`
//! 3. Server writes `ServerCompatResponse` (host:port) to stdout
//! 4. Runner makes test RPC calls to the server
//!
//! ### Client Testing
//! ```text
//! connectconformance --mode client -- ./conformance-client
//! ```
//! 1. Runner starts reference server
//! 2. Runner sends `ClientCompatRequest` messages to stdin
//! 3. Client makes RPC calls and writes `ClientCompatResponse` to stdout

#[path = "generated/connect/mod.rs"]
pub mod connect;
#[path = "generated/buffa/mod.rs"]
pub mod proto;

// Re-export commonly used types
pub use connect::connectrpc::conformance::v1::*;
pub use proto::connectrpc::conformance::v1::*;

use buffa::Message;
use std::io;
use std::io::Read;
use std::io::Write;

/// Initialize the global `TypeRegistry` with conformance message types.
///
/// Must be called before any JSON serialization/deserialization of messages
/// containing `google.protobuf.Any` fields (e.g., `ConformancePayload.RequestInfo.requests`).
pub fn init_type_registry() {
    use buffa::type_registry::{JsonAnyEntry, TypeRegistry};

    macro_rules! register {
        ($registry:expr, $ty:ty) => {
            $registry.register_json_any(JsonAnyEntry {
                type_url: <$ty>::TYPE_URL,
                to_json: |bytes| {
                    let msg = <$ty>::decode_from_slice(bytes).map_err(|e| e.to_string())?;
                    serde_json::to_value(&msg).map_err(|e| e.to_string())
                },
                from_json: |value| {
                    let msg: $ty = serde_json::from_value(value).map_err(|e| e.to_string())?;
                    Ok(msg.encode_to_vec())
                },
                is_wkt: false,
            });
        };
    }

    let mut registry = TypeRegistry::new();
    buffa_types::register_wkt_types(&mut registry);
    register!(registry, UnaryRequest);
    register!(registry, ServerStreamRequest);
    register!(registry, ClientStreamRequest);
    register!(registry, BidiStreamRequest);
    register!(registry, IdempotentUnaryRequest);
    register!(registry, UnaryResponse);
    register!(registry, ServerStreamResponse);
    register!(registry, ClientStreamResponse);
    register!(registry, BidiStreamResponse);
    register!(registry, IdempotentUnaryResponse);
    register!(registry, ConformancePayload);
    register!(registry, conformance_payload::RequestInfo);
    register!(registry, self::Error);
    register!(registry, Header);
    register!(registry, RawHTTPRequest);
    register!(registry, RawHTTPResponse);
    buffa::type_registry::set_type_registry(registry);
}

/// Read a length-prefixed protobuf message from stdin.
///
/// The conformance runner uses a simple framing protocol:
/// - 4 bytes (big-endian): message length
/// - N bytes: protobuf-encoded message
pub fn read_message<M: Message>() -> io::Result<Option<M>> {
    let stdin = io::stdin();
    let mut handle = stdin.lock();

    // Read length prefix (4 bytes, big-endian)
    let mut len_buf = [0u8; 4];
    match handle.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;

    // Read message bytes
    let mut msg_buf = vec![0u8; len];
    handle.read_exact(&mut msg_buf)?;

    // Decode protobuf
    let msg = M::decode_from_slice(&msg_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(Some(msg))
}

/// Write a length-prefixed protobuf message to stdout.
pub fn write_message<M: Message>(msg: &M) -> io::Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    // Encode message
    let buf = msg.encode_to_vec();

    // Write length prefix
    let len = buf.len() as u32;
    handle.write_all(&len.to_be_bytes())?;

    // Write message bytes
    handle.write_all(&buf)?;
    handle.flush()?;

    Ok(())
}
