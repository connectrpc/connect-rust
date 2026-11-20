//! The Connect-over-WebSocket wire format shared by the server session and
//! the client transport: message markers, the Leading-Metadata JSON object,
//! the names a client may not set, and the close codes.

use std::cell::Cell;
use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::{SplitSink, SplitStream};
use futures::{Sink, SinkExt, StreamExt};
use http::{HeaderMap, HeaderName, HeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig as FrameConfig;
use tokio_tungstenite::tungstenite::protocol::frame::{CloseFrame, Utf8Bytes};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use crate::codec::CodecFormat;
use crate::error::{ConnectError, ErrorCode};

/// The single byte that opens every message and names its type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Marker {
    Body,
    Metadata,
    ServerEnd,
    ClientEnd,
}

impl Marker {
    pub(crate) fn byte(self) -> u8 {
        match self {
            Self::Body => b'B',
            Self::Metadata => b'M',
            Self::ServerEnd => b'S',
            Self::ClientEnd => b'C',
        }
    }
}

/// Which side of the connection is reading, so a marker valid only in the
/// other direction is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Server,
    #[cfg_attr(not(feature = "client"), allow(dead_code))]
    Client,
}

/// What kind of mistake a peer made. It selects the close code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fault {
    /// A protocol error (§5): reported in `S` by a server.
    Protocol,
    /// A text message that is not UTF-8 (§6.1): the connection fails with
    /// 1007 and no `S`.
    Encoding,
    /// A message over the size limit (§12.1).
    TooLarge,
}

impl Fault {
    /// The close code the reading side sends for this fault (§13). Only a
    /// protocol error differs by side: a client sends 3102, since a browser
    /// cannot send 1002.
    pub(crate) fn close_code(self, side: Side) -> u16 {
        match (self, side) {
            (Self::Protocol, Side::Server) => close_code::PROTOCOL_ERROR,
            (Self::Protocol, Side::Client) => close_code::CLIENT_ERROR,
            (Self::Encoding, _) => close_code::INVALID_PAYLOAD,
            (Self::TooLarge, _) => close_code::MESSAGE_TOO_BIG,
        }
    }
}

/// A peer's framing mistake and the error that reports it.
#[derive(Debug)]
pub(crate) struct WireError {
    pub(crate) fault: Fault,
    pub(crate) error: ConnectError,
}

impl WireError {
    /// A fault found by either side. Both report a peer's protocol error as
    /// `internal` (§5), and an oversized message as `resource_exhausted`.
    pub(crate) fn new(fault: Fault, message: impl Into<String>) -> Self {
        let code = match fault {
            Fault::TooLarge => ErrorCode::ResourceExhausted,
            Fault::Protocol | Fault::Encoding => ErrorCode::Internal,
        };
        Self {
            fault,
            error: ConnectError::new(code, message),
        }
    }

    /// A protocol error described by `message`.
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::new(Fault::Protocol, message)
    }

    /// A `B` payload that did not decode: a protocol error, but reported as
    /// `invalid_argument` to match Connect over HTTP (§6.1).
    pub(crate) fn undecodable(message: impl Into<String>) -> Self {
        Self {
            fault: Fault::Protocol,
            error: ConnectError::invalid_argument(message),
        }
    }
}

pub(crate) mod close_code {
    pub(crate) const NORMAL: u16 = 1000;
    pub(crate) const GOING_AWAY: u16 = 1001;
    pub(crate) const PROTOCOL_ERROR: u16 = 1002;
    pub(crate) const INVALID_PAYLOAD: u16 = 1007;
    pub(crate) const MESSAGE_TOO_BIG: u16 = 1009;
    pub(crate) const INTERNAL_ERROR: u16 = 1011;
    /// The only error code a client sends (§13).
    pub(crate) const CLIENT_ERROR: u16 = 3102;
}

/// The longest Close-frame reason RFC 6455 allows, in bytes.
const MAX_CLOSE_REASON: usize = 123;

/// A Close frame with `code`, and `reason` cut to the length the frame can
/// carry without splitting a character.
pub(crate) fn close_frame(code: u16, reason: &str) -> CloseFrame {
    let mut end = reason.len().min(MAX_CLOSE_REASON);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    CloseFrame {
        code: code.into(),
        reason: reason[..end].into(),
    }
}

/// One application message with its marker removed.
#[derive(Debug)]
pub(crate) struct WireMessage {
    pub(crate) marker: Marker,
    pub(crate) payload: Bytes,
}

/// Classify a data message read by `side` under `codec`. `Ok(None)` is a
/// control message (Ping, Pong, Close, raw frame), which carries no marker.
///
/// Every rule of the spec's §5 and §6.1 that can be checked without
/// connection state is checked here: the marker, the direction, the frame
/// type for the marker and codec, and the payloads a marker must or must
/// not have.
pub(crate) fn classify(
    message: Message,
    side: Side,
    codec: CodecFormat,
) -> Result<Option<WireMessage>, WireError> {
    let (data, binary) = match message {
        Message::Text(text) => (Bytes::from(text), false),
        Message::Binary(data) => (data, true),
        _ => return Ok(None),
    };
    let Some(&first) = data.first() else {
        return Err(WireError::protocol("empty message has no marker"));
    };
    let marker = match (first, side) {
        (b'B', _) => Marker::Body,
        (b'M', _) => Marker::Metadata,
        (b'S', Side::Client) => Marker::ServerEnd,
        (b'C', Side::Server) => Marker::ClientEnd,
        (b'S' | b'C', _) => {
            return Err(WireError::protocol(format!(
                "marker '{}' is not valid in this direction",
                first as char
            )));
        }
        _ => {
            return Err(WireError::protocol(format!("unknown marker 0x{first:02x}")));
        }
    };
    let payload = data.slice(1..);
    let want_binary = marker == Marker::Body && codec == CodecFormat::Proto;
    if binary != want_binary {
        return Err(WireError::protocol(format!(
            "'{}' message arrived in a {} frame; expected a {} frame",
            first as char,
            frame_type(binary),
            frame_type(want_binary),
        )));
    }
    match marker {
        Marker::Metadata | Marker::ServerEnd if payload.is_empty() => {
            return Err(WireError::protocol(format!(
                "'{}' message has no JSON payload",
                first as char
            )));
        }
        Marker::ClientEnd if !payload.is_empty() => {
            return Err(WireError::protocol("'C' message carries a payload"));
        }
        _ => {}
    }
    Ok(Some(WireMessage { marker, payload }))
}

fn frame_type(binary: bool) -> &'static str {
    if binary { "binary" } else { "text" }
}

/// Build the message for `marker` with `payload`: a text frame for JSON, a
/// binary frame for a Protobuf body.
///
/// # Errors
///
/// `internal` when a text payload is not UTF-8, which a peer must reject.
pub(crate) fn encode(
    marker: Marker,
    payload: &[u8],
    binary: bool,
) -> Result<Message, ConnectError> {
    let mut buf = BytesMut::with_capacity(payload.len() + 1);
    buf.put_u8(marker.byte());
    buf.extend_from_slice(payload);
    let data = buf.freeze();
    if binary {
        return Ok(Message::Binary(data));
    }
    Utf8Bytes::try_from(data)
        .map(Message::Text)
        .map_err(|_| ConnectError::internal("a JSON message is not valid UTF-8"))
}

/// A `B` message under `codec`.
pub(crate) fn encode_body(payload: &[u8], codec: CodecFormat) -> Result<Message, ConnectError> {
    encode(Marker::Body, payload, codec == CodecFormat::Proto)
}

/// A JSON message (`M` or `S`) built from JSON this crate serialized, which
/// is always UTF-8, so the lossy conversion never changes it.
pub(crate) fn encode_json(marker: Marker, payload: &[u8]) -> Message {
    let mut text = String::with_capacity(payload.len() + 1);
    text.push(marker.byte() as char);
    text.push_str(&String::from_utf8_lossy(payload));
    Message::Text(text.into())
}

/// The messages waiting to be written, driven from a `select!` branch so a
/// stalled write never blocks reads, the deadline, or keep-alive. Polling is
/// cancel-safe: a message leaves the queue only once the sink accepts it.
#[derive(Default)]
pub(crate) struct Outbound {
    queue: VecDeque<Message>,
    queued_bytes: usize,
    flushing: bool,
}

impl Outbound {
    /// Whether nothing is queued, so the caller may produce more.
    #[cfg_attr(not(feature = "client"), allow(dead_code))]
    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// The payload bytes queued and not yet handed to the sink.
    pub(crate) fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }

    /// Whether there is anything to write or flush.
    pub(crate) fn is_busy(&self) -> bool {
        !self.queue.is_empty() || self.flushing
    }

    pub(crate) fn push(&mut self, message: Message) {
        self.queued_bytes += message.len();
        self.queue.push_back(message);
    }

    /// Drop what has not been handed to the sink yet.
    #[cfg_attr(not(feature = "client"), allow(dead_code))]
    pub(crate) fn clear(&mut self) {
        self.queue.clear();
        self.queued_bytes = 0;
    }

    /// Write and flush everything queued.
    pub(crate) fn poll_write<S>(
        &mut self,
        sink: &mut S,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WsError>>
    where
        S: Sink<Message, Error = WsError> + Unpin,
    {
        while !self.queue.is_empty() {
            ready!(sink.poll_ready_unpin(cx))?;
            if let Some(message) = self.queue.pop_front() {
                self.queued_bytes -= message.len();
                sink.start_send_unpin(message)?;
                self.flushing = true;
            }
        }
        ready!(sink.poll_flush_unpin(cx))?;
        self.flushing = false;
        Poll::Ready(Ok(()))
    }

    pub(crate) async fn write<S>(&mut self, sink: &mut S) -> Result<(), WsError>
    where
        S: Sink<Message, Error = WsError> + Unpin,
    {
        std::future::poll_fn(|cx| self.poll_write(sink, cx)).await
    }
}

/// The frame limits for messages of up to `max_message_size` bytes, plus
/// one for the marker.
pub(crate) fn frame_config(max_message_size: usize) -> FrameConfig {
    let max = max_message_size.saturating_add(1);
    FrameConfig::default()
        .max_message_size(Some(max))
        .max_frame_size(Some(max))
}

/// Queue `item` for the peer's side of the RPC. Returns it back when the
/// queue is full, so the caller holds it and stops reading until there is
/// room. A receiver that has gone gets nothing more.
pub(crate) fn offer<T>(tx: Option<&mpsc::Sender<T>>, item: T) -> Option<T> {
    match tx?.try_send(item) {
        Err(mpsc::error::TrySendError::Full(item)) => Some(item),
        _ => None,
    }
}

pub(crate) async fn reserve<T>(tx: Option<&mpsc::Sender<T>>) -> Option<mpsc::OwnedPermit<T>> {
    tx?.clone().reserve_owned().await.ok()
}

/// Fail an End-of-Stream payload whose metadata has a `connect-` key, which a
/// server must not send (§9): the keys are left out, and the error `error`
/// builds from the first key replaces the payload's. `None` if there is none.
pub(crate) fn fail_connect_metadata(
    json: &[u8],
    error: impl FnOnce(&str) -> Bytes,
) -> Option<Bytes> {
    if !json.windows(9).any(|w| w == b"\"connect-") {
        return None;
    }
    let mut value = serde_json::from_slice::<serde_json::Value>(json).ok()?;
    let metadata = value.get_mut("metadata")?.as_object_mut()?;
    let key = metadata
        .keys()
        .find(|key| key.starts_with("connect-"))?
        .clone();
    metadata.retain(|key, _| !key.starts_with("connect-"));
    let error = serde_json::from_slice::<serde_json::Value>(&error(&key)).ok()?;
    value["error"] = error.get("error")?.clone();
    serde_json::to_vec(&value).ok().map(Bytes::from)
}

/// An End-of-Stream payload without the `connect-` metadata keys no peer
/// may send (§9). It is parsed only when one may be present.
pub(crate) fn without_connect_metadata(json: Bytes) -> Bytes {
    if !json.windows(9).any(|w| w == b"\"connect-") {
        return json;
    }
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&json) else {
        return json;
    };
    if let Some(metadata) = value.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        metadata.retain(|key, _| !key.starts_with("connect-"));
    }
    serde_json::to_vec(&value).map_or(json, Bytes::from)
}

/// Read and discard until the peer closes or the drain window passes, so
/// that a TCP reset over unread data cannot destroy what was sent (§13.2),
/// and so that tungstenite answers the peer's Close. After a read error
/// tungstenite reads nothing more, so the raw connection is drained instead.
pub(crate) async fn drain<T>(
    sink: SplitSink<WebSocketStream<T>, Message>,
    mut stream: SplitStream<WebSocketStream<T>>,
    mut read_failed: bool,
) where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let _ = tokio::time::timeout(crate::service::DRAIN_TIMEOUT, async {
        while !read_failed {
            match stream.next().await {
                Some(Ok(_)) => {}
                Some(Err(_)) => read_failed = true,
                None => return,
            }
        }
        let Ok(mut ws) = stream.reunite(sink) else {
            return;
        };
        let mut buf = [0; 8192];
        while matches!(ws.get_mut().read(&mut buf).await, Ok(n) if n > 0) {}
    })
    .await;
}

/// Frame `payload` as a Connect envelope with `flag`.
///
/// # Errors
///
/// `resource_exhausted` for a payload too long for the 32-bit length.
pub(crate) fn envelope(flag: u8, payload: &[u8]) -> Result<Bytes, ConnectError> {
    let len = u32::try_from(payload.len()).map_err(|_| {
        ConnectError::resource_exhausted("a message is too large for a Connect envelope")
    })?;
    let mut buf = BytesMut::with_capacity(crate::envelope::HEADER_SIZE + payload.len());
    buf.put_u8(flag);
    buf.put_u32(len);
    buf.extend_from_slice(payload);
    Ok(buf.freeze())
}

// ============================================================================
// Leading-Metadata
// ============================================================================

/// The most values a Leading-Metadata message may carry. It keeps a peer's
/// metadata well inside a `HeaderMap`'s capacity, past which it panics.
pub(crate) const MAX_METADATA_VALUES: usize = 1024;

/// The most entries and strings that metadata within [`MAX_METADATA_VALUES`]
/// can hold: one entry and key per value.
const MAX_METADATA_ITEMS: usize = 3 * MAX_METADATA_VALUES;

/// Decode a Leading-Metadata payload into header entries, enforcing §7.3.1:
/// an array of entries, each a key and at least one value, keys that are
/// lower-case field names and appear once, and values of visible ASCII,
/// space and tab, trimmed of surrounding whitespace.
///
/// A `-bin` value is not checked for base64 here: the spec leaves that
/// to the application that decodes it.
pub(crate) fn decode_metadata(
    payload: &[u8],
) -> Result<Vec<(HeaderName, Vec<HeaderValue>)>, WireError> {
    let budget = Cell::new(MAX_METADATA_ITEMS);
    let mut parser = serde_json::Deserializer::from_slice(payload);
    let raw = serde::de::DeserializeSeed::deserialize(Entries(&budget), &mut parser)
        .and_then(|raw| parser.end().map(|()| raw))
        .map_err(|e| {
            if budget.get() == 0 {
                return WireError::new(
                    Fault::TooLarge,
                    format!("metadata has more than {MAX_METADATA_VALUES} values"),
                );
            }
            WireError::protocol(format!("metadata is not an array of string arrays: {e}"))
        })?;
    let total: usize = raw.iter().map(|entry| entry.len().saturating_sub(1)).sum();
    if total > MAX_METADATA_VALUES {
        return Err(WireError::new(
            Fault::TooLarge,
            format!("metadata has {total} values; at most {MAX_METADATA_VALUES} are allowed"),
        ));
    }
    let mut entries: Vec<(HeaderName, Vec<HeaderValue>)> = Vec::with_capacity(raw.len());
    let mut seen = HashSet::with_capacity(raw.len());
    for entry in raw {
        let mut parts = entry.into_iter();
        let Some(key) = parts.next() else {
            return Err(WireError::protocol("metadata entry is empty"));
        };
        if key.bytes().any(|b| b.is_ascii_uppercase()) {
            return Err(WireError::protocol(format!(
                "metadata key {key:?} is not lower-case"
            )));
        }
        let name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|_| WireError::protocol(format!("invalid metadata key {key:?}")))?;
        if !seen.insert(name.clone()) {
            return Err(WireError::protocol(format!(
                "metadata key {key:?} appears in more than one entry"
            )));
        }
        let values = parts
            .map(|value| {
                metadata_value(&value).ok_or_else(|| {
                    WireError::protocol(format!("invalid metadata value for {key:?}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if values.is_empty() {
            return Err(WireError::protocol(format!(
                "metadata key {key:?} has no values"
            )));
        }
        entries.push((name, values));
    }
    Ok(entries)
}

/// Add decoded metadata to `headers`, keeping each key's values in order.
pub(crate) fn append_metadata(
    headers: &mut HeaderMap,
    metadata: Vec<(HeaderName, Vec<HeaderValue>)>,
) {
    for (name, values) in metadata {
        for value in values {
            headers.append(name.clone(), value);
        }
    }
}

/// Parses the metadata array, failing once it has taken more entries and
/// strings than the budget allows, so that a peer cannot make it allocate
/// many times the payload's size.
struct Entries<'a>(&'a Cell<usize>);

/// One entry's strings, charged to the same budget.
struct Strings<'a>(&'a Cell<usize>);

fn charge<E: serde::de::Error>(budget: &Cell<usize>) -> Result<(), E> {
    let left = budget
        .get()
        .checked_sub(1)
        .ok_or_else(|| E::custom("too many items"))?;
    budget.set(left);
    Ok(())
}

impl<'de> serde::de::DeserializeSeed<'de> for Entries<'_> {
    type Value = Vec<Vec<String>>;

    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> serde::de::Visitor<'de> for Entries<'_> {
    type Value = Vec<Vec<String>>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an array of string arrays")
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut entries = Vec::new();
        while let Some(entry) = seq.next_element_seed(Strings(self.0))? {
            charge(self.0)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl<'de> serde::de::DeserializeSeed<'de> for Strings<'_> {
    type Value = Vec<String>;

    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> serde::de::Visitor<'de> for Strings<'_> {
    type Value = Vec<String>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an array of strings")
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut strings = Vec::new();
        while let Some(string) = seq.next_element::<String>()? {
            charge(self.0)?;
            strings.push(string);
        }
        Ok(strings)
    }
}

/// A metadata value: visible ASCII, space and tab only, with surrounding
/// space and tab removed.
fn metadata_value(value: &str) -> Option<HeaderValue> {
    let valid = value
        .bytes()
        .all(|b| b == b'\t' || (b' '..=b'~').contains(&b));
    if !valid {
        return None;
    }
    HeaderValue::from_str(value.trim_matches([' ', '\t'])).ok()
}

/// Encode `headers` as a Leading-Metadata array, leaving out names `skip`
/// rejects, the `connect-` names no peer may send (§7.3.2), and values that
/// are not text.
pub(crate) fn encode_metadata(headers: &HeaderMap, skip: impl Fn(&HeaderName) -> bool) -> Bytes {
    let mut entries = Vec::new();
    for name in headers.keys() {
        if skip(name) || name.as_str().starts_with("connect-") {
            continue;
        }
        let mut entry = vec![serde_json::Value::String(name.as_str().to_owned())];
        entry.extend(
            headers
                .get_all(name)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .map(|value| serde_json::Value::String(value.to_owned())),
        );
        if entry.len() > 1 {
            entries.push(serde_json::Value::Array(entry));
        }
    }
    Bytes::from(serde_json::Value::Array(entries).to_string())
}

// ============================================================================
// Reserved request metadata names (§7.3.2)
// ============================================================================

/// The default infrastructure deny list: what a proxy in front of the server
/// sets and a client must not be able to forge.
pub(crate) const DEFAULT_INFRASTRUCTURE_HEADERS: &[&str] =
    &["forwarded", "x-forwarded-*", "x-real-ip"];

const FORBIDDEN_REQUEST_HEADERS: &[&str] = &[
    "accept-charset",
    "accept-encoding",
    "access-control-request-headers",
    "access-control-request-method",
    "connection",
    "content-length",
    "cookie",
    "cookie2",
    "date",
    "dnt",
    "expect",
    "host",
    "keep-alive",
    "origin",
    "referer",
    "set-cookie",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "via",
    "x-http-method",
    "x-http-method-override",
    "x-method-override",
];

/// Whether a client may not set `name` in its Leading-Metadata message.
/// `name` is already lower-case, as `HeaderName` always is.
pub(crate) fn is_reserved_request_key<S: AsRef<str>>(name: &str, infrastructure: &[S]) -> bool {
    FORBIDDEN_REQUEST_HEADERS.contains(&name)
        || is_protocol_header(name)
        || name.starts_with("proxy-")
        || name.starts_with("sec-")
        || infrastructure
            .iter()
            .any(|pattern| matches_pattern(name, pattern.as_ref()))
}

/// Whether a name is one the protocol controls (§7.3.2), so a client leaves
/// it out of the metadata it sends and a server out of the metadata it
/// echoes.
pub(crate) fn is_protocol_header(name: &str) -> bool {
    name.starts_with("connect-") || matches!(name, "content-type" | "content-encoding")
}

/// Match a lower-case name against a deny-list entry, where a trailing `*`
/// matches any suffix.
fn matches_pattern(name: &str, pattern: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => {
            name.len() >= prefix.len() && name[..prefix.len()].eq_ignore_ascii_case(prefix)
        }
        None => name.eq_ignore_ascii_case(pattern),
    }
}

// ============================================================================
// Handshake helpers
// ============================================================================

/// The subprotocol token selecting the Protobuf binary codec.
pub(crate) const SUBPROTOCOL_PROTO: &str = "connectrpc.1+proto";
/// The subprotocol token selecting the Protobuf JSON codec.
pub(crate) const SUBPROTOCOL_JSON: &str = "connectrpc.1+json";

/// The query parameter that carries a client's deadline.
pub(crate) const TIMEOUT_PARAM: &str = "connect-timeout-ms";

#[cfg_attr(not(feature = "client"), allow(dead_code))]
pub(crate) fn subprotocol_token(codec: CodecFormat) -> &'static str {
    match codec {
        CodecFormat::Json => SUBPROTOCOL_JSON,
        _ => SUBPROTOCOL_PROTO,
    }
}

/// Select the codec for a handshake (§4.3): the first offered token whose
/// codec this server supports, skipping any it does not recognize. Returns
/// the codec and the token exactly as offered, which the server echoes.
pub(crate) fn negotiate_subprotocol(headers: &HeaderMap) -> Option<(CodecFormat, String)> {
    headers
        .get_all(http::header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .find_map(|token| Some((supported_codec(token)?, token.to_owned())))
}

fn supported_codec(token: &str) -> Option<CodecFormat> {
    if token.eq_ignore_ascii_case(SUBPROTOCOL_PROTO) {
        Some(CodecFormat::Proto)
    } else if cfg!(feature = "json") && token.eq_ignore_ascii_case(SUBPROTOCOL_JSON) {
        Some(CodecFormat::Json)
    } else {
        None
    }
}

/// Whether a comma-separated header contains `token`, ignoring case.
pub(crate) fn header_has_token(headers: &HeaderMap, name: &HeaderName, token: &str) -> bool {
    headers.get_all(name).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .any(|t| t.trim().eq_ignore_ascii_case(token))
        })
    })
}

/// Parse the `connect-timeout-ms` query parameter (§11). Absent is `Ok(None)`;
/// anything but one to ten decimal digits, or a repeated parameter, is
/// `invalid_argument`. Zero is valid; the caller fails it as
/// `deadline_exceeded` without dispatching.
pub(crate) fn parse_timeout_param(query: Option<&str>) -> Result<Option<Duration>, ConnectError> {
    let mut found = None;
    for pair in query.unwrap_or("").split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key != TIMEOUT_PARAM {
            continue;
        }
        if found.is_some() {
            return Err(ConnectError::invalid_argument(format!(
                "{TIMEOUT_PARAM} appears more than once"
            )));
        }
        if value.is_empty() || value.len() > 10 || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ConnectError::invalid_argument(format!(
                "invalid {TIMEOUT_PARAM} value {value:?}: want 1 to 10 decimal digits"
            )));
        }
        let millis: u64 = value.parse().map_err(|_| {
            ConnectError::invalid_argument(format!("invalid {TIMEOUT_PARAM} value {value:?}"))
        })?;
        found = Some(Duration::from_millis(millis));
    }
    Ok(found)
}

/// Whether an `Origin` names the same host and port as `Host` (§4.4). The
/// scheme is ignored and default ports are not removed.
pub(crate) fn is_same_host(origin: &str, host: &str) -> bool {
    let Some((_, authority)) = origin.split_once("://") else {
        return false;
    };
    let authority = authority.split('/').next().unwrap_or(authority);
    !authority.is_empty() && authority.eq_ignore_ascii_case(host)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(s: &str) -> Message {
        Message::Text(s.into())
    }

    fn binary(data: &'static [u8]) -> Message {
        Message::Binary(Bytes::from_static(data))
    }

    #[test]
    fn classify_accepts_each_marker_in_its_direction() {
        let m = classify(text("M[]"), Side::Server, CodecFormat::Json)
            .unwrap()
            .unwrap();
        assert_eq!(m.marker, Marker::Metadata);
        assert_eq!(&m.payload[..], b"[]");
        let c = classify(text("C"), Side::Server, CodecFormat::Proto)
            .unwrap()
            .unwrap();
        assert_eq!(c.marker, Marker::ClientEnd);
        let s = classify(text("S{}"), Side::Client, CodecFormat::Proto)
            .unwrap()
            .unwrap();
        assert_eq!(s.marker, Marker::ServerEnd);
        let b = classify(binary(b"B"), Side::Client, CodecFormat::Proto)
            .unwrap()
            .unwrap();
        assert_eq!(b.marker, Marker::Body);
        assert!(b.payload.is_empty());
    }

    #[test]
    fn classify_ignores_control_messages() {
        let ping = Message::Ping(Bytes::new());
        assert!(
            classify(ping, Side::Server, CodecFormat::Proto)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn classify_rejects_markers_from_the_wrong_direction() {
        let err = classify(text("S{}"), Side::Server, CodecFormat::Json).unwrap_err();
        assert_eq!(err.fault, Fault::Protocol);
        assert_eq!(err.error.code, ErrorCode::Internal);
        let err = classify(text("C"), Side::Client, CodecFormat::Json).unwrap_err();
        assert_eq!(err.fault, Fault::Protocol);
    }

    #[test]
    fn classify_rejects_unknown_and_missing_markers() {
        for message in [
            text("X{}"),
            text("é{}"),
            binary(b"\x80abc"),
            binary(b""),
            text(""),
        ] {
            let err = classify(message, Side::Server, CodecFormat::Proto).unwrap_err();
            assert_eq!(err.fault, Fault::Protocol);
        }
    }

    #[test]
    fn classify_checks_the_frame_type() {
        for (message, codec, expected) in [
            (text("B{}"), CodecFormat::Proto, "expected a binary frame"),
            (
                binary(b"B\x08\x01"),
                CodecFormat::Json,
                "expected a text frame",
            ),
            (binary(b"M[]"), CodecFormat::Proto, "expected a text frame"),
            (binary(b"C"), CodecFormat::Proto, "expected a text frame"),
        ] {
            let err = classify(message, Side::Server, codec).unwrap_err();
            assert_eq!(err.fault, Fault::Protocol);
            let message = err.error.message.unwrap_or_default();
            assert!(message.contains(expected), "{message}");
        }
    }

    #[test]
    fn classify_checks_payload_presence() {
        for (message, side) in [
            (text("M"), Side::Client),
            (text("S"), Side::Client),
            (text("C{}"), Side::Server),
        ] {
            let err = classify(message, side, CodecFormat::Json).unwrap_err();
            assert_eq!(err.fault, Fault::Protocol);
        }
        assert!(classify(text("C"), Side::Server, CodecFormat::Json).is_ok());
        // An empty body is the codec's to reject (§6.1).
        assert!(classify(text("B"), Side::Client, CodecFormat::Json).is_ok());
    }

    #[test]
    fn encode_picks_the_frame_type() {
        assert!(matches!(
            encode_body(b"\x08\x01", CodecFormat::Proto),
            Ok(Message::Binary(_))
        ));
        let Ok(Message::Text(text)) = encode_body(b"{}", CodecFormat::Json) else {
            panic!("expected text");
        };
        assert_eq!(text.as_str(), "B{}");
        assert!(encode_body(b"\xff", CodecFormat::Json).is_err());
    }

    #[test]
    fn close_codes_follow_the_fault() {
        assert_eq!(Fault::Protocol.close_code(Side::Server), 1002);
        assert_eq!(Fault::Protocol.close_code(Side::Client), 3102);
        for side in [Side::Server, Side::Client] {
            assert_eq!(Fault::Encoding.close_code(side), 1007);
            assert_eq!(Fault::TooLarge.close_code(side), 1009);
        }
    }

    #[test]
    fn close_reason_fits_the_frame() {
        let frame = close_frame(3102, "short");
        assert_eq!(frame.reason.as_str(), "short");
        let long = "é".repeat(100);
        let frame = close_frame(3102, &long);
        assert!(frame.reason.len() <= 123);
        assert!(long.starts_with(frame.reason.as_str()));

        let exact = "a".repeat(123);
        assert_eq!(close_frame(1002, &exact).reason.as_str(), exact);
        // A four-byte character that would straddle byte 123 is dropped whole.
        let straddle = format!("{}🦀", "a".repeat(121));
        assert_eq!(
            close_frame(1002, &straddle).reason.as_str(),
            "a".repeat(121)
        );
        assert_eq!(close_frame(1000, "").reason.as_str(), "");
    }

    #[test]
    fn decode_metadata_accepts_valid_arrays() {
        let entries = decode_metadata(
            br#"[["acme-tenant","t-42"],["empty",""],["x-bin","!!!","AAECAw=="],["pad"," \ta\tb\t "]]"#,
        )
        .unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].0, "acme-tenant");
        assert_eq!(entries[0].1, vec![HeaderValue::from_static("t-42")]);
        assert_eq!(entries[1].1, vec![HeaderValue::from_static("")]);
        // A -bin value is left for the application to decode.
        assert_eq!(entries[2].1.len(), 2);
        assert_eq!(entries[3].1, vec![HeaderValue::from_static("a\tb")]);
        assert!(decode_metadata(b"[]").unwrap().is_empty());
    }

    #[test]
    fn decode_metadata_caps_the_value_count() {
        let entry = |i: usize| format!(r#"["k{i}","v"]"#);
        let at_cap: Vec<String> = (0..MAX_METADATA_VALUES).map(entry).collect();
        let payload = format!("[{}]", at_cap.join(","));
        assert_eq!(
            decode_metadata(payload.as_bytes()).unwrap().len(),
            MAX_METADATA_VALUES
        );
        let over: Vec<String> = (0..=MAX_METADATA_VALUES).map(entry).collect();
        let payload = format!("[{}]", over.join(","));
        let err = decode_metadata(payload.as_bytes()).unwrap_err();
        assert_eq!(err.fault, Fault::TooLarge);
        assert_eq!(err.error.code, ErrorCode::ResourceExhausted);
    }

    #[test]
    fn end_stream_metadata_loses_connect_keys() {
        let json = Bytes::from_static(
            br#"{"error":{"code":"internal","message":"\"connect-x\""},"metadata":{"connect-a":["1"],"x":["2"]}}"#,
        );
        let out: serde_json::Value =
            serde_json::from_slice(&without_connect_metadata(json)).unwrap();
        assert_eq!(out["metadata"], serde_json::json!({"x": ["2"]}));
        assert_eq!(out["error"]["message"], "\"connect-x\"");
        let plain = Bytes::from_static(br#"{"metadata":{"x":["2"]}}"#);
        assert_eq!(without_connect_metadata(plain.clone()), plain);
    }

    #[test]
    fn decode_metadata_stops_parsing_at_the_item_budget() {
        // Entries with no values, which the value count alone would not see.
        for entry in ["[]", r#"[""]"#] {
            let payload = format!("[{}]", vec![entry; MAX_METADATA_ITEMS + 1].join(","));
            let err = decode_metadata(payload.as_bytes()).unwrap_err();
            assert_eq!(err.fault, Fault::TooLarge, "{entry}");
        }
        let one_entry = format!(
            r#"[["k",{}]]"#,
            vec![r#""v""#; MAX_METADATA_ITEMS].join(",")
        );
        let err = decode_metadata(one_entry.as_bytes()).unwrap_err();
        assert_eq!(err.fault, Fault::TooLarge);
    }

    #[test]
    fn decode_metadata_rejects_malformed_arrays() {
        let cases: &[&[u8]] = &[
            b"",
            b"{}",
            b"null",
            br#"{"a":["x"]}"#,
            br#"["a"]"#,
            br#"[[]]"#,
            br#"[["a"]]"#,
            br#"[["a",1]]"#,
            br#"[["a",["x"]]]"#,
            br#"[["A","x"]]"#,
            br#"[["a","x"],["a","y"]]"#,
            br#"[["bad key","x"]]"#,
            br#"[["a","line\nbreak"]]"#,
            br#"[["a","nul\u0000"]]"#,
            br#"[["a","caf\u00e9"]]"#,
            br#"[] trailing"#,
        ];
        for case in cases {
            let err = decode_metadata(case).unwrap_err();
            assert_eq!(
                err.fault,
                Fault::Protocol,
                "{}",
                String::from_utf8_lossy(case)
            );
        }
    }

    #[test]
    fn encode_metadata_groups_values_and_skips() {
        let mut headers = HeaderMap::new();
        headers.append("x-a", HeaderValue::from_static("1"));
        headers.append("x-a", HeaderValue::from_static("2"));
        headers.append("content-type", HeaderValue::from_static("application/json"));
        headers.append("connect-x", HeaderValue::from_static("1"));
        headers.append("x-opaque", HeaderValue::from_bytes(b"\xff").unwrap());
        let json = encode_metadata(&headers, |name| is_protocol_header(name.as_str()));
        let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(value, serde_json::json!([["x-a", "1", "2"]]));
        assert_eq!(&encode_metadata(&HeaderMap::new(), |_| false)[..], b"[]");
    }

    #[test]
    fn reserved_names_cover_all_three_classes() {
        let infra: Vec<String> = DEFAULT_INFRASTRUCTURE_HEADERS
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        for name in [
            "cookie",
            "host",
            "sec-websocket-key",
            "proxy-authorization",
            "x-http-method-override",
            "forwarded",
            "x-forwarded-for",
            "x-real-ip",
            "connect-timeout-ms",
            "connect-anything",
            "content-type",
            "content-encoding",
        ] {
            assert!(is_reserved_request_key(name, &infra), "{name}");
        }
        for name in ["authorization", "acme-tenant", "user-agent", "x-forwarded"] {
            assert!(!is_reserved_request_key(name, &infra), "{name}");
        }
        assert!(!is_reserved_request_key::<&str>("x-real-ip", &[]));
    }

    #[test]
    fn subprotocol_selects_the_first_supported_token() {
        let offer = |tokens: &[&str]| {
            let mut headers = HeaderMap::new();
            for token in tokens {
                headers.append(
                    http::header::SEC_WEBSOCKET_PROTOCOL,
                    HeaderValue::from_str(token).unwrap(),
                );
            }
            negotiate_subprotocol(&headers)
        };
        assert_eq!(
            offer(&["connectrpc.1+proto"]),
            Some((CodecFormat::Proto, "connectrpc.1+proto".to_owned()))
        );
        assert_eq!(
            offer(&["connectrpc.1+thrift", "connectrpc.1+proto"]),
            Some((CodecFormat::Proto, "connectrpc.1+proto".to_owned()))
        );
        #[cfg(feature = "json")]
        {
            assert_eq!(
                offer(&["connectrpc.2+json, ConnectRPC.1+JSON"]),
                Some((CodecFormat::Json, "ConnectRPC.1+JSON".to_owned()))
            );
            assert_eq!(
                offer(&["connectrpc.1+json, connectrpc.1+proto"]).map(|(codec, _)| codec),
                Some(CodecFormat::Json)
            );
        }
        #[cfg(not(feature = "json"))]
        assert_eq!(
            offer(&["connectrpc.1+json, connectrpc.1+proto"]).map(|(codec, _)| codec),
            Some(CodecFormat::Proto)
        );
        assert_eq!(offer(&["connectrpc.1+thrift"]), None);
        assert_eq!(offer(&[]), None);
    }

    #[test]
    fn timeout_param_parsing() {
        assert_eq!(parse_timeout_param(None).unwrap(), None);
        assert_eq!(parse_timeout_param(Some("a=b")).unwrap(), None);
        assert_eq!(
            parse_timeout_param(Some("connect-timeout-ms=1500")).unwrap(),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(
            parse_timeout_param(Some("x=1&connect-timeout-ms=0")).unwrap(),
            Some(Duration::ZERO)
        );
        assert_eq!(
            parse_timeout_param(Some("connect-timeout-ms=9999999999")).unwrap(),
            Some(Duration::from_millis(9_999_999_999))
        );
        for bad in [
            "connect-timeout-ms=",
            "connect-timeout-ms=-1",
            "connect-timeout-ms=12345678901",
            "connect-timeout-ms=1.5",
            "connect-timeout-ms=1&connect-timeout-ms=2",
        ] {
            let err = parse_timeout_param(Some(bad)).unwrap_err();
            assert_eq!(err.code, ErrorCode::InvalidArgument, "{bad}");
        }
    }

    #[test]
    fn same_host_ignores_scheme_but_not_port() {
        assert!(is_same_host("https://example.com", "example.com"));
        assert!(is_same_host("http://Example.com:8080", "example.COM:8080"));
        assert!(!is_same_host("https://example.com", "example.com:443"));
        assert!(!is_same_host("https://evil.com", "example.com"));
        assert!(!is_same_host("null", "example.com"));
    }
}
