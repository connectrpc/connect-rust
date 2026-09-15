//! Envelope framing for ConnectRPC streaming.
//!
//! ConnectRPC streaming uses envelope framing where each message is prefixed
//! with a 5-byte header:
//! - 1 byte: flags (0x00 for data, 0x02 for end-stream)
//! - 4 bytes: message length (big-endian uint32)

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use std::sync::Arc;

use crate::compression::CompressionPolicy;
use crate::compression::CompressionRegistry;
use crate::error::ConnectError;

/// Envelope flags.
pub mod flags {
    /// Normal data message.
    pub const DATA: u8 = 0x00;
    /// Compressed data message.
    pub const COMPRESSED: u8 = 0x01;
    /// End of stream (trailers follow).
    pub const END_STREAM: u8 = 0x02;
    /// gRPC-Web trailer frame. The payload is an HTTP/1-style trailer block
    /// rather than a message, and the flag is defined only for gRPC-Web: on
    /// plain gRPC a set high bit is a framing error.
    pub const GRPC_WEB_TRAILER: u8 = 0x80;
}

/// Size of the envelope header in bytes.
pub const HEADER_SIZE: usize = 5;

/// Minimum payload size for chaining a payload as its own body frame
/// instead of copying it into the contiguous framing buffer.
///
/// The trade-off is a payload-sized memcpy (tens of GiB/s) against the cost
/// of an extra body frame: one more `poll_frame` cycle, a 9-byte HTTP/2
/// frame header for the 5-byte envelope-header frame, and refcount
/// bookkeeping. The crossover is low (single-digit KiB); 16 KiB is
/// conservative and matches h2's default `max_frame_size`, above which the
/// transport splits the payload into multiple DATA frames anyway.
pub(crate) const MIN_CHAIN_SIZE: usize = 16 * 1024;

/// Size of the per-stream slab that small streamed payloads are copied into
/// (see [`EnvelopeAssembler`]). 8 KiB is the read block hyper and h2 use
/// themselves (`tokio_util::codec::FramedRead`, hyper's h1 `INIT_BUFFER_SIZE`).
/// It bounds what a held small message can keep alive and what an idle
/// stream retains; it does not decide correctness.
pub(crate) const READ_SLAB_SIZE: usize = 8 * 1024;

// A shared, exhausted slab is replaced by `BytesMut::reserve` with a block of
// its *original* capacity, which `bytes` records only as a power-of-two bucket
// between 1 KiB and 64 KiB; outside that range a rolled slab would not be
// READ_SLAB_SIZE again.
const _: () = assert!(
    READ_SLAB_SIZE.is_power_of_two() && READ_SLAB_SIZE >= 1024 && READ_SLAB_SIZE <= 64 * 1024
);

/// An envelope-framed message.
#[derive(Debug, Clone)]
pub struct Envelope {
    /// Flags indicating message type and compression.
    pub flags: u8,
    /// The message payload.
    pub data: Bytes,
}

impl Envelope {
    /// Create a new data envelope.
    pub fn data(data: Bytes) -> Self {
        Self {
            flags: flags::DATA,
            data,
        }
    }

    /// Create a new compressed data envelope.
    pub fn compressed(data: Bytes) -> Self {
        Self {
            flags: flags::COMPRESSED,
            data,
        }
    }

    /// Create a new end-stream envelope.
    pub fn end_stream(data: Bytes) -> Self {
        Self {
            flags: flags::END_STREAM,
            data,
        }
    }

    /// Check if this is a compressed message.
    pub fn is_compressed(&self) -> bool {
        self.flags & flags::COMPRESSED != 0
    }

    /// Check if this is an end-of-stream message.
    pub fn is_end_stream(&self) -> bool {
        self.flags & flags::END_STREAM != 0
    }

    /// Encode this envelope to bytes.
    ///
    /// # Panics
    ///
    /// Panics if the payload exceeds `u32::MAX` bytes. In practice this is
    /// unreachable because message size limits are enforced well below this
    /// threshold.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(HEADER_SIZE + self.data.len());
        write_envelope(self.flags, &self.data, &mut buf)
            .expect("envelope payload exceeds u32::MAX");
        buf.freeze()
    }

    /// Frame an already-encoded body, keeping any segments it arrived in.
    ///
    /// A body that the encoder split — because it could hand a large field
    /// over by reference count rather than copy it — stays split all the way
    /// to the socket, one body frame per segment, instead of being flattened
    /// back into a single buffer here and undoing the saving.
    ///
    /// The envelope header still declares the total length across every
    /// segment, so the framing on the wire is byte-for-byte what a contiguous
    /// encode would have produced. Envelope framing has never depended on
    /// HTTP frame boundaries.
    ///
    /// A body below `min_chain` is written into the head buffer as before: a
    /// segment that small would be copied into the framing buffer downstream
    /// regardless, so splitting it buys nothing and costs a frame.
    pub(crate) fn encode_body_parts(
        flags: u8,
        body: crate::response::EncodedBody,
        min_chain: usize,
    ) -> (Bytes, Vec<Bytes>) {
        let mut head = BytesMut::new();
        let mut segments = Vec::new();
        write_envelope_chained(flags, body, &mut head, &mut segments, min_chain)
            .expect("envelope payload exceeds u32::MAX");
        (head.freeze(), segments)
    }

    /// Decode an envelope from bytes.
    ///
    /// Returns `Ok(Some(envelope))` if a complete envelope was decoded,
    /// `Ok(None)` if more data is needed, or an error if the data is invalid.
    ///
    /// **Warning:** This method has no size limit. Use [`decode_with_limit`](Self::decode_with_limit)
    /// for untrusted input to prevent denial-of-service attacks.
    pub fn decode(buf: &mut BytesMut) -> Result<Option<Self>, ConnectError> {
        Self::decode_with_limit(buf, usize::MAX)
    }

    /// Decode an envelope from bytes with a maximum message size.
    ///
    /// Returns `Ok(Some(envelope))` if a complete envelope was decoded,
    /// `Ok(None)` if more data is needed, or an error if:
    /// - The declared message size exceeds `max_size`
    /// - The data is otherwise invalid
    ///
    /// This protects against malicious clients declaring very large message
    /// sizes in the envelope header.
    pub fn decode_with_limit(
        buf: &mut BytesMut,
        max_size: usize,
    ) -> Result<Option<Self>, ConnectError> {
        Self::decode_contiguous(buf, max_size)
    }

    /// [`decode_with_limit`](Self::decode_with_limit) for a body already held
    /// as immutable [`Bytes`] (e.g. from `Collected::to_bytes`): on success the
    /// envelope is consumed from `buf` and its payload is a zero-copy slice of
    /// it; if more data is needed `buf` is left untouched.
    ///
    /// # Errors
    ///
    /// [`ResourceExhausted`](crate::ErrorCode::ResourceExhausted) if the
    /// declared message size exceeds `max_size`.
    pub fn decode_bytes_with_limit(
        buf: &mut Bytes,
        max_size: usize,
    ) -> Result<Option<Self>, ConnectError> {
        Self::decode_contiguous(buf, max_size)
    }

    /// Shared body for the contiguous buffers above (`chunk()` is the whole
    /// unread region; `copy_to_bytes` splits without copying on both).
    fn decode_contiguous<B: Buf>(
        buf: &mut B,
        max_size: usize,
    ) -> Result<Option<Self>, ConnectError> {
        let head = buf.chunk();
        debug_assert_eq!(head.len(), buf.remaining(), "contiguous buffer");
        if head.len() < HEADER_SIZE {
            return Ok(None);
        }

        let flags = head[0];
        let length = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;

        // Check size limit before waiting for more data
        if length > max_size {
            return Err(ConnectError::resource_exhausted(format!(
                "message size {length} exceeds limit {max_size}"
            )));
        }

        // `saturating_add`: `length` is an untrusted u32 from the wire. On a
        // 32-bit target `HEADER_SIZE + length` can overflow `usize` and panic
        // in a debug build. Via `decode` (max_size = usize::MAX) the size check
        // above does not bound `length`, so saturate here. A saturated sum is
        // never <= buf.len(), so an over-large frame waits for more data.
        if head.len() < HEADER_SIZE.saturating_add(length) {
            return Ok(None);
        }

        buf.advance(HEADER_SIZE);
        let data = buf.copy_to_bytes(length);

        Ok(Some(Self { flags, data }))
    }
}

/// Incremental envelope decoder fed one transport body frame at a time.
///
/// Unlike [`Envelope::decode_with_limit`], which needs the whole envelope in
/// one contiguous buffer, this carries at most the 5 header bytes between
/// frames and checks the declared length against the limit before anything
/// is allocated. Payloads larger than half of [`READ_SLAB_SIZE`] are
/// assembled into their own exactly-sized allocation (at most
/// `min(declared length, 2 × bytes received)` while in flight), so the
/// emitted [`data`](field@Envelope::data) is the sole owner of its memory
/// (`Bytes::is_unique`). Smaller payloads are copied into a per-stream slab
/// of [`READ_SLAB_SIZE`] bytes and split off its front, so the common small
/// message costs no allocation of its own; a handler that keeps one alive
/// keeps at most that slab alive, never a large message or the transport's
/// read buffer, and an idle stream retains at most one slab.
pub(crate) struct EnvelopeAssembler {
    max_message_size: usize,
    /// Limit for frames flagged [`flags::GRPC_WEB_TRAILER`], which carry a
    /// trailer block rather than a message. `None` applies `max_message_size`
    /// to them like any other envelope (server side, plain gRPC).
    grpc_web_trailer_limit: Option<usize>,
    header: [u8; HEADER_SIZE],
    /// `< HEADER_SIZE` while collecting the header; `== HEADER_SIZE` while
    /// filling `slab` or `body` up to `expected`.
    header_len: usize,
    expected: usize,
    /// Assembly buffer for a payload too large for the slab.
    body: Vec<u8>,
    /// Small payloads are assembled here and split off the front; allocated
    /// on the first one.
    slab: Option<BytesMut>,
}

impl EnvelopeAssembler {
    pub(crate) fn new(max_message_size: usize) -> Self {
        Self {
            max_message_size,
            grpc_web_trailer_limit: None,
            header: [0; HEADER_SIZE],
            header_len: 0,
            expected: 0,
            body: Vec::new(),
            slab: None,
        }
    }

    /// Accept gRPC-Web trailer frames up to `limit` bytes regardless of
    /// `max_message_size` (trailers are metadata, not a message).
    pub(crate) fn with_grpc_web_trailer_limit(mut self, limit: usize) -> Self {
        self.grpc_web_trailer_limit = Some(limit);
        self
    }

    /// Whether an envelope has started (at least one header byte) and not
    /// yet been emitted.
    pub(crate) fn has_partial(&self) -> bool {
        self.header_len > 0
    }

    /// Consume bytes from the front of `frame` until one envelope completes
    /// (`Ok(Some)`; `frame` keeps any bytes after it, so call again) or the
    /// frame is exhausted (`Ok(None)`). An exhausted `frame` is replaced with
    /// an empty `Bytes` so the caller's slot never keeps a consumed transport
    /// frame alive.
    ///
    /// # Errors
    ///
    /// [`ResourceExhausted`](crate::ErrorCode::ResourceExhausted) as soon as a
    /// header declares more than the limit, before anything is allocated for
    /// it. Framing is lost at that point; the stream must be abandoned.
    pub(crate) fn feed(&mut self, frame: &mut Bytes) -> Result<Option<Envelope>, ConnectError> {
        if self.header_len < HEADER_SIZE {
            let take = (HEADER_SIZE - self.header_len).min(frame.len());
            self.header[self.header_len..self.header_len + take].copy_from_slice(&frame[..take]);
            frame.advance(take);
            self.header_len += take;
            if self.header_len < HEADER_SIZE {
                *frame = Bytes::new(); // exhausted mid-header
                return Ok(None);
            }
            let [flag_byte, len @ ..] = self.header;
            let length = u32::from_be_bytes(len) as usize;
            let (what, limit) = match self.grpc_web_trailer_limit {
                Some(limit) if flag_byte & flags::GRPC_WEB_TRAILER != 0 => {
                    ("grpc-web trailer", limit)
                }
                _ => ("message", self.max_message_size),
            };
            if length > limit {
                return Err(ConnectError::resource_exhausted(format!(
                    "{what} size {length} exceeds limit {limit}"
                )));
            }
            self.expected = length;
        }

        let data = if self.expected == 0 {
            Some(Bytes::new())
        } else if self.expected <= READ_SLAB_SIZE / 2 {
            self.fill_slab(frame)
        } else {
            self.fill_body(frame)
        };
        if frame.is_empty() {
            *frame = Bytes::new();
        }
        Ok(data.map(|data| {
            self.header_len = 0;
            Envelope {
                flags: self.header[0],
                data,
            }
        }))
    }

    /// Copy a small payload into the slab; split it off once complete.
    fn fill_slab(&mut self, frame: &mut Bytes) -> Option<Bytes> {
        let slab = self
            .slab
            .get_or_insert_with(|| BytesMut::with_capacity(READ_SLAB_SIZE));
        if slab.is_empty() {
            // Start of a payload. A no-op while the slab has room; once it is
            // used up this reuses it in place if every message cut from it
            // has been dropped, and otherwise leaves it to those messages and
            // allocates a fresh block of the original READ_SLAB_SIZE.
            slab.reserve(self.expected);
        }
        let take = (self.expected - slab.len()).min(frame.len());
        slab.extend_from_slice(&frame[..take]);
        frame.advance(take);
        (slab.len() == self.expected).then(|| slab.split_to(self.expected).freeze())
    }

    /// Assemble a larger payload into its own exact allocation.
    fn fill_body(&mut self, frame: &mut Bytes) -> Option<Bytes> {
        let take = (self.expected - self.body.len()).min(frame.len());
        if self.body.capacity() - self.body.len() < take {
            // Reserving the declared length up front would let a peer
            // allocate `max_message_size` with 5 bytes; doubling from
            // what has arrived keeps the allocation within 2x of bytes
            // received, and capping at the declared length makes the
            // final allocation exact.
            let target = self.expected.min(
                self.body
                    .capacity()
                    .saturating_mul(2)
                    .max(self.body.len() + take),
            );
            self.body.reserve_exact(target - self.body.len());
        }
        self.body.extend_from_slice(&frame[..take]);
        frame.advance(take);
        if self.body.len() < self.expected {
            return None;
        }
        // Capacity was capped at `expected` and std's `RawVec` reserves
        // exactly what is asked, so `len == capacity` here and `Bytes::from`
        // takes its no-copy, no-`Shared` path; an allocator-aware `Vec` that
        // over-provided would only cost a `Shared` header, still no copy.
        Some(Bytes::from(std::mem::take(&mut self.body)))
    }
}

/// One item decoded from a streaming request body.
#[derive(Debug)]
pub(crate) enum Decoded {
    Message(Bytes),
    /// The END_STREAM envelope: terminal. Any further body data is trailing
    /// garbage to drain (bounded) or reject, never to decode.
    EndStream,
}

/// Decoder for Connect envelope-framed messages on a streaming request:
/// an [`EnvelopeAssembler`] plus END_STREAM detection and per-message
/// decompression.
pub(crate) struct EnvelopeDecoder {
    assembler: EnvelopeAssembler,
    streaming_encoding: Option<String>,
    compression: Arc<CompressionRegistry>,
}

impl EnvelopeDecoder {
    pub(crate) fn new(
        max_message_size: usize,
        streaming_encoding: Option<String>,
        compression: Arc<CompressionRegistry>,
    ) -> Self {
        Self {
            assembler: EnvelopeAssembler::new(max_message_size),
            streaming_encoding,
            compression,
        }
    }

    /// Consume bytes from `frame` until one message or END_STREAM is decoded
    /// (`Ok(Some)`; call again for the rest of the frame, which after
    /// END_STREAM is trailing bytes) or the frame is exhausted (`Ok(None)`).
    pub(crate) fn decode(&mut self, frame: &mut Bytes) -> Result<Option<Decoded>, ConnectError> {
        let Some(envelope) = self.assembler.feed(frame)? else {
            return Ok(None); // need more data
        };

        if envelope.is_end_stream() {
            tracing::trace!("client stream: received end-stream envelope");
            return Ok(Some(Decoded::EndStream));
        }

        // Decompress if needed
        let data = if envelope.is_compressed() {
            let encoding = match self.streaming_encoding.as_deref() {
                Some(enc) if enc != "identity" => enc,
                _ => {
                    return Err(ConnectError::internal(
                        "received compressed message without connect-content-encoding header",
                    ));
                }
            };
            self.compression.decompress_with_limit(
                encoding,
                envelope.data,
                self.assembler.max_message_size,
            )?
        } else {
            envelope.data
        };

        tracing::trace!(
            size = data.len(),
            "client stream: dispatching message to handler"
        );

        Ok(Some(Decoded::Message(data)))
    }

    /// The body ended. A client may close the HTTP body without sending an
    /// END_STREAM envelope — the body ending is itself the end-of-stream
    /// signal — but ending part-way through an envelope is an error.
    pub(crate) fn finish(&self) -> Result<(), ConnectError> {
        if self.assembler.has_partial() {
            tracing::debug!("client stream: body ended with incomplete envelope");
            return Err(ConnectError::invalid_argument(
                "incomplete request envelope",
            ));
        }
        Ok(())
    }
}

/// Encoder for Connect envelope-framed messages on a streaming response.
///
/// Implements [`tokio_util::codec::Encoder<Bytes>`] so it can be used with
/// [`FramedWrite`](tokio_util::codec::FramedWrite) in future contexts (e.g.
/// bidi streaming). For the current response path it is used directly via
/// its [`Encoder::encode`] method within a stream combinator.
///
/// Handles optional compression: when configured, data envelopes are
/// compressed and sent with the [`flags::COMPRESSED`] flag. Empty payloads
/// skip compression per the Connect spec.
pub(crate) struct EnvelopeEncoder {
    compression: Option<(Arc<CompressionRegistry>, String)>,
    policy: CompressionPolicy,
}

impl EnvelopeEncoder {
    /// Create an encoder with optional compression and a policy.
    pub(crate) fn new(
        compression: Option<(Arc<CompressionRegistry>, impl Into<String>)>,
        policy: CompressionPolicy,
    ) -> Self {
        Self {
            compression: compression.map(|(reg, enc)| (reg, enc.into())),
            policy,
        }
    }

    /// Create an encoder without compression.
    pub(crate) fn uncompressed() -> Self {
        Self {
            compression: None,
            policy: CompressionPolicy::disabled(),
        }
    }

    /// Encode an end-stream envelope into `dst`. End-stream envelopes are
    /// never compressed.
    pub(crate) fn encode_end_stream(
        &mut self,
        data: Bytes,
        dst: &mut BytesMut,
    ) -> Result<(), ConnectError> {
        write_envelope(flags::END_STREAM, &data, dst)
    }

    /// Encode a data envelope, avoiding the payload copy for large messages.
    ///
    /// When the on-wire payload (post-compression, if negotiated) is at
    /// least `min_chain` bytes in total, only the 5-byte envelope header is
    /// written into `dst` and the payload's segments are appended to
    /// `chained`, in wire order, for the caller to emit after `dst`: a
    /// refcount move instead of a payload-sized memcpy. A body the encoder
    /// already split keeps its segments, so a large field handed over by
    /// reference count reaches the socket without being flattened here.
    /// Smaller payloads are written contiguously into `dst` and nothing is
    /// appended.
    ///
    /// Compression needs one contiguous input, so a segmented body is
    /// flattened before compressing and the compressed output chains as a
    /// single segment on its post-compression size.
    ///
    /// The [`Encoder`](tokio_util::codec::Encoder) impl delegates here with
    /// `min_chain = usize::MAX` (never chain), so the compression decision and
    /// the chaining decision cannot drift apart on the streaming path.
    /// Unary responses take [`Envelope::encode_body_parts`], which applies the
    /// same threshold to an already-encoded body.
    ///
    /// gRPC/Connect envelope framing is independent of HTTP-level frame
    /// boundaries, so splitting the header and payload across body frames
    /// does not change the wire protocol.
    pub(crate) fn encode_chained(
        &mut self,
        body: crate::response::EncodedBody,
        dst: &mut BytesMut,
        chained: &mut impl Extend<Bytes>,
        min_chain: usize,
    ) -> Result<(), ConnectError> {
        let (flag, body) = if let Some((ref comp, ref encoding)) = self.compression
            && self.policy.should_compress(body.len())
        {
            let compressed = comp.compress(encoding, &body.into_contiguous())?;
            (flags::COMPRESSED, compressed.into())
        } else {
            (flags::DATA, body)
        };
        write_envelope_chained(flag, body, dst, chained, min_chain)
    }
}

impl tokio_util::codec::Encoder<Bytes> for EnvelopeEncoder {
    type Error = ConnectError;

    fn encode(&mut self, data: Bytes, dst: &mut BytesMut) -> Result<(), ConnectError> {
        // `usize::MAX` threshold: the contiguous entry point never chains.
        let mut chained = Vec::new();
        self.encode_chained(data.into(), dst, &mut chained, usize::MAX)?;
        debug_assert!(chained.is_empty(), "usize::MAX threshold cannot chain");
        Ok(())
    }
}

/// Write a single envelope (header + payload) into a `BytesMut` buffer.
/// The length is validated (via [`envelope_length`]) before any buffer
/// growth, so an oversized payload errors without allocating.
fn write_envelope(flag: u8, data: &[u8], dst: &mut BytesMut) -> Result<(), ConnectError> {
    put_envelope_header(flag, envelope_length(data.len())?, dst);
    dst.put_slice(data);
    Ok(())
}

/// Write the envelope header for `body` into `dst`, then either copy the
/// body in after it (below `min_chain` bytes in total) or append the body's
/// segments to `chained` in wire order for the caller to emit after `dst`.
/// The header always declares the total across every segment, and the
/// length is validated before any buffer growth.
fn write_envelope_chained(
    flag: u8,
    body: crate::response::EncodedBody,
    dst: &mut BytesMut,
    chained: &mut impl Extend<Bytes>,
    min_chain: usize,
) -> Result<(), ConnectError> {
    let total = body.len();
    let declared = envelope_length(total)?;
    if total < min_chain {
        dst.reserve(HEADER_SIZE + total);
        put_envelope_header(flag, declared, dst);
        for segment in body.segments() {
            dst.put_slice(segment);
        }
        return Ok(());
    }
    put_envelope_header(flag, declared, dst);
    match body {
        crate::response::EncodedBody::Contiguous(bytes) => chained.extend([bytes]),
        crate::response::EncodedBody::Segmented(segments) => chained.extend(segments),
    }
    Ok(())
}

/// The envelope length field for a payload of `len` bytes, or
/// `ResourceExhausted` when it does not fit the 32-bit field.
fn envelope_length(len: usize) -> Result<u32, ConnectError> {
    u32::try_from(len).map_err(|_| {
        ConnectError::resource_exhausted(format!("envelope payload {len} bytes exceeds u32::MAX"))
    })
}

/// Write only the 5-byte envelope header (flag + big-endian length).
fn put_envelope_header(flag: u8, len: u32, dst: &mut BytesMut) {
    dst.reserve(HEADER_SIZE);
    dst.put_u8(flag);
    dst.put_u32(len);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::codec::Encoder;

    /// Helper: create a decoder with no compression support, suitable for
    /// testing uncompressed envelope framing.
    fn decoder(max_message_size: usize) -> EnvelopeDecoder {
        EnvelopeDecoder::new(
            max_message_size,
            None,
            Arc::new(CompressionRegistry::default()),
        )
    }

    /// Helper: decode the next item and expect it to be a message.
    fn message(dec: &mut EnvelopeDecoder, frame: &mut Bytes) -> Bytes {
        match dec.decode(frame).unwrap() {
            Some(Decoded::Message(data)) => data,
            other => panic!("expected a message, got {other:?}"),
        }
    }

    // ── Envelope tests ──────────────────────────────────────────────

    #[test]
    fn encode_body_parts_chains_large_payload_by_refcount() {
        let payload = Bytes::from(vec![7u8; 64]);
        let ptr = payload.as_ptr();
        let (head, chained) = Envelope::encode_body_parts(flags::DATA, payload.clone().into(), 64);
        assert_eq!(head.len(), HEADER_SIZE);
        let [chained] = &chained[..] else {
            panic!("payload at threshold must chain as one segment");
        };
        assert!(std::ptr::eq(chained.as_ptr(), ptr), "must not copy");

        // Reassembled bytes are identical to the contiguous encoding.
        let mut reassembled = BytesMut::from(&head[..]);
        reassembled.put_slice(chained);
        assert_eq!(
            reassembled.freeze(),
            Envelope::data(payload).encode(),
            "chained wire bytes must match contiguous encoding"
        );
    }

    /// A payload that compresses is chained on the COMPRESSED payload's
    /// size, with the COMPRESSED flag in the header segment.
    #[test]
    #[cfg(feature = "gzip")]
    fn encode_chained_chains_large_compressed_payload() {
        let registry = Arc::new(CompressionRegistry::default());
        let mut enc = EnvelopeEncoder::new(
            Some((Arc::clone(&registry), "gzip")),
            CompressionPolicy::default().with_min_size(0),
        );
        // Incompressible-ish random-ish payload so the compressed form stays
        // above the chain threshold.
        let data: Vec<u8> = (0..64 * 1024u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect();
        let mut dst = BytesMut::new();
        let mut chained = Vec::new();
        enc.encode_chained(Bytes::from(data).into(), &mut dst, &mut chained, 1024)
            .unwrap();
        let [chained] = &chained[..] else {
            panic!("large compressed payload must chain as one segment");
        };

        assert_eq!(dst.len(), HEADER_SIZE);
        assert_eq!(dst[0], flags::COMPRESSED);
        assert_eq!(
            u32::from_be_bytes([dst[1], dst[2], dst[3], dst[4]]) as usize,
            chained.len()
        );

        // Reassembled envelope decodes back to the original payload.
        let mut wire = dst;
        wire.put_slice(chained);
        let mut dec = EnvelopeDecoder::new(1024 * 1024, Some("gzip".to_owned()), registry);
        let decoded = message(&mut dec, &mut wire.freeze());
        assert_eq!(decoded.len(), 64 * 1024);
    }

    /// A body the encoder already split keeps its segments through
    /// `encode_chained`: only the header lands in `dst`, and every segment,
    /// large or small, comes back as the same allocation in wire order. The
    /// framing stream decides which of them to copy.
    #[test]
    fn encode_chained_keeps_segments_of_a_large_body() {
        use crate::response::EncodedBody;

        let lead = Bytes::from_static(b"tag");
        let big = Bytes::from(vec![3u8; 64]);
        let tail = Bytes::from_static(b"end");
        let body = EncodedBody::Segmented(vec![lead.clone(), big.clone(), tail.clone()]);
        let total = body.len();

        let mut enc = EnvelopeEncoder::uncompressed();
        let mut dst = BytesMut::new();
        let mut segments = Vec::new();
        enc.encode_chained(body, &mut dst, &mut segments, 32)
            .unwrap();

        assert_eq!(dst.len(), HEADER_SIZE, "only the header is copied");
        assert_eq!(
            u32::from_be_bytes([dst[1], dst[2], dst[3], dst[4]]) as usize,
            total,
            "header declares the total across segments"
        );
        let [s0, s1, s2] = &segments[..] else {
            panic!("expected three segments, got {}", segments.len());
        };
        assert!(std::ptr::eq(s0.as_ptr(), lead.as_ptr()));
        assert!(std::ptr::eq(s1.as_ptr(), big.as_ptr()));
        assert!(std::ptr::eq(s2.as_ptr(), tail.as_ptr()));
    }

    /// Below the threshold a segmented body is copied in after the header,
    /// exactly as a contiguous one would be.
    #[test]
    fn encode_chained_copies_a_small_segmented_body() {
        use crate::response::EncodedBody;

        let body =
            EncodedBody::Segmented(vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")]);
        let mut enc = EnvelopeEncoder::uncompressed();
        let mut dst = BytesMut::new();
        let mut segments = Vec::new();
        enc.encode_chained(body, &mut dst, &mut segments, 32)
            .unwrap();

        assert!(segments.is_empty());
        assert_eq!(
            dst.freeze(),
            Envelope::data(Bytes::from_static(b"abcd")).encode()
        );
    }

    /// Compression flattens a segmented body first, so the compressed
    /// envelope decodes to the concatenation of the segments.
    #[test]
    #[cfg(feature = "gzip")]
    fn encode_chained_compresses_a_segmented_body_as_one() {
        use crate::response::EncodedBody;

        let registry = Arc::new(CompressionRegistry::default());
        let mut enc = EnvelopeEncoder::new(
            Some((Arc::clone(&registry), "gzip")),
            CompressionPolicy::default().with_min_size(0),
        );
        let body = EncodedBody::Segmented(vec![
            Bytes::from(vec![b'a'; 4096]),
            Bytes::from(vec![b'b'; 4096]),
        ]);
        let expected = body.clone().into_contiguous();

        let mut wire = BytesMut::new();
        let mut segments = Vec::new();
        enc.encode_chained(body, &mut wire, &mut segments, usize::MAX)
            .unwrap();
        assert!(segments.is_empty());
        assert_eq!(wire[0], flags::COMPRESSED);

        let mut dec = EnvelopeDecoder::new(1024 * 1024, Some("gzip".to_owned()), registry);
        let decoded = message(&mut dec, &mut wire.freeze());
        assert_eq!(decoded, expected);
    }

    #[test]
    fn encode_body_parts_small_payload_stays_contiguous() {
        let payload = Bytes::from_static(b"tiny");
        let (head, chained) = Envelope::encode_body_parts(flags::DATA, payload.clone().into(), 64);
        assert!(chained.is_empty());
        assert_eq!(head, Envelope::data(payload).encode());
    }

    /// The property every branch has to hold: the header declares the total
    /// length across all segments, and concatenating what is emitted equals
    /// the contiguous envelope. A header that disagreed with the bytes after
    /// it would desynchronize the peer's framing rather than fail locally, so
    /// each branch is pinned rather than left to the conformance suite.
    #[test]
    fn encode_body_parts_declares_the_length_it_emits() {
        use crate::response::EncodedBody;

        let cases: Vec<(&str, EncodedBody)> = vec![
            ("empty", EncodedBody::Contiguous(Bytes::new())),
            (
                "sub-threshold contiguous",
                EncodedBody::Contiguous(Bytes::from_static(b"small")),
            ),
            (
                "sub-threshold segmented",
                EncodedBody::Segmented(vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")]),
            ),
            (
                "over-threshold contiguous",
                EncodedBody::Contiguous(Bytes::from(vec![9u8; 128])),
            ),
            (
                "over-threshold two segments",
                EncodedBody::Segmented(vec![
                    Bytes::from(vec![1u8; 64]),
                    Bytes::from(vec![2u8; 64]),
                ]),
            ),
            (
                "over-threshold many segments",
                EncodedBody::Segmented((0..5).map(|i| Bytes::from(vec![i as u8; 40])).collect()),
            ),
        ];

        for (name, body) in cases {
            let total = body.len();
            let expected = Envelope::data(body.clone().into_contiguous()).encode();

            let (head, segments) = Envelope::encode_body_parts(flags::DATA, body, 64);

            let declared = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
            assert_eq!(declared, total, "{name}: header must declare the total");

            let emitted: usize =
                head.len() - HEADER_SIZE + segments.iter().map(Bytes::len).sum::<usize>();
            assert_eq!(
                emitted, total,
                "{name}: emitted payload bytes must match the declared length"
            );

            let mut reassembled = BytesMut::from(&head[..]);
            for segment in &segments {
                assert!(!segment.is_empty(), "{name}: no empty segments");
                reassembled.put_slice(segment);
            }
            assert_eq!(
                reassembled.freeze(),
                expected,
                "{name}: must reassemble to the contiguous envelope"
            );
        }
    }

    /// De-framing an immutable `Bytes` body hands the payload over as a
    /// slice of the input rather than a copy, and advances past the envelope.
    #[test]
    fn decode_bytes_with_limit_is_zero_copy() {
        let mut wire = BytesMut::new();
        write_envelope(flags::COMPRESSED, b"payload", &mut wire).unwrap();
        wire.put_slice(b"next");
        let wire = wire.freeze();

        let mut buf = wire.clone();
        let envelope = Envelope::decode_bytes_with_limit(&mut buf, 64)
            .unwrap()
            .unwrap();
        assert_eq!(envelope.flags, flags::COMPRESSED);
        assert_eq!(envelope.data, "payload");
        assert!(std::ptr::eq(
            envelope.data.as_ptr(),
            wire[HEADER_SIZE..].as_ptr()
        ));
        assert_eq!(buf, "next");

        let mut short = wire.slice(..HEADER_SIZE + 3);
        assert!(
            Envelope::decode_bytes_with_limit(&mut short, 64)
                .unwrap()
                .is_none()
        );
        assert_eq!(short.len(), HEADER_SIZE + 3);
        assert!(Envelope::decode_bytes_with_limit(&mut wire.clone(), 6).is_err());
    }

    #[test]
    fn test_envelope_roundtrip() {
        let original = Envelope::data(Bytes::from_static(b"hello world"));
        let encoded = original.encode();

        let mut buf = BytesMut::from(&encoded[..]);
        let decoded = Envelope::decode(&mut buf).unwrap().unwrap();

        assert_eq!(decoded.flags, original.flags);
        assert_eq!(decoded.data, original.data);
    }

    #[test]
    fn test_envelope_partial() {
        let mut buf = BytesMut::from(&[0u8, 0, 0, 0][..]);
        assert!(Envelope::decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_envelope_size_limit() {
        // Create an envelope header claiming a 1MB message
        let mut buf = BytesMut::new();
        buf.put_u8(0); // flags
        buf.put_u32(1024 * 1024); // 1MB length

        // With a 512KB limit, this should fail immediately
        let result = Envelope::decode_with_limit(&mut buf, 512 * 1024);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::ResourceExhausted);
    }

    #[test]
    fn test_envelope_size_limit_ok() {
        // Create a small envelope
        let original = Envelope::data(Bytes::from_static(b"small"));
        let encoded = original.encode();
        let mut buf = BytesMut::from(&encoded[..]);

        // With a 1MB limit, this should succeed
        let result = Envelope::decode_with_limit(&mut buf, 1024 * 1024);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_envelope_unlimited_decode_huge_length_no_panic() {
        // `decode` uses max_size = usize::MAX, so the size check does not bound
        // `length`. A header claiming u32::MAX bytes must return `Ok(None)`
        // (waiting for data that never comes), not panic on `HEADER_SIZE +
        // length`. On a 32-bit target the unsaturated add would overflow.
        let mut buf = BytesMut::new();
        buf.put_u8(0); // flags
        buf.put_u32(u32::MAX); // length prefix
        let result = Envelope::decode(&mut buf);
        assert!(matches!(result, Ok(None)));
    }

    // ── EnvelopeAssembler tests ─────────────────────────────────────

    /// Feed `frames` in order and collect every envelope, asserting after
    /// each frame that the in-flight allocation stays within
    /// `min(declared, 2 × received)`.
    fn assemble_all(asm: &mut EnvelopeAssembler, frames: &[&[u8]]) -> Vec<Envelope> {
        let mut out = Vec::new();
        for f in frames {
            let mut frame = Bytes::copy_from_slice(f);
            while let Some(env) = asm.feed(&mut frame).unwrap() {
                out.push(env);
            }
            assert!(
                frame.is_empty(),
                "feed returns None only once the frame is consumed"
            );
            assert!(
                asm.body.capacity() <= asm.expected.min(2 * asm.body.len().max(1)),
                "cap {} len {} expected {}",
                asm.body.capacity(),
                asm.body.len(),
                asm.expected
            );
        }
        out
    }

    #[test]
    fn assembler_partial_header_is_partial() {
        let mut asm = EnvelopeAssembler::new(1024);
        assert!(!asm.has_partial());
        assert!(assemble_all(&mut asm, &[&[0u8, 0, 0]]).is_empty());
        assert!(asm.has_partial());
    }

    #[test]
    fn assembler_multiple_envelopes_in_one_frame() {
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&Envelope::data(Bytes::from_static(b"first")).encode());
        wire.extend_from_slice(&Envelope::end_stream(Bytes::from_static(b"{}")).encode());
        wire.extend_from_slice(&Envelope::data(Bytes::new()).encode());
        wire.extend_from_slice(&Envelope::data(Bytes::from_static(b"last")).encode()[..3]);
        let mut asm = EnvelopeAssembler::new(1024);
        let out = assemble_all(&mut asm, &[&wire]);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].data, Bytes::from_static(b"first"));
        assert!(out[1].is_end_stream());
        assert_eq!(out[1].data, Bytes::from_static(b"{}"));
        assert!(
            out[2].data.is_empty(),
            "zero-length payload is a valid envelope"
        );
        assert!(asm.has_partial(), "head of the next envelope is carried");
    }

    /// A large payload split across many transport frames comes out as one
    /// exactly-sized, uniquely-owned `Bytes`; the assembler keeps nothing.
    #[test]
    fn assembler_payload_split_across_many_frames() {
        const LEN: usize = 1024 * 1024 + 3;
        let payload: Vec<u8> = (0..LEN).map(|i| i as u8).collect();
        let wire = Envelope::data(Bytes::from(payload.clone())).encode();
        let frames: Vec<&[u8]> = wire.chunks(16 * 1024).collect();
        let mut asm = EnvelopeAssembler::new(LEN);
        let out = assemble_all(&mut asm, &frames);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].data.len(), LEN);
        assert_eq!(&out[0].data[..], &payload[..]);
        assert!(out[0].data.is_unique(), "message must own its allocation");
        assert!(!asm.has_partial());
        assert_eq!(asm.body.capacity(), 0, "assembler retains nothing");
    }

    /// Small payloads are cut from a per-stream slab: they do not reference
    /// the transport frame, a held one shares at most its own slab, and the
    /// assembler keeps no reference to a slab it has moved past.
    #[test]
    fn assembler_small_messages_come_from_a_bounded_slab() {
        let envelope = Envelope::data(Bytes::from(vec![7u8; 100])).encode();
        let per_slab = READ_SLAB_SIZE / 100;
        let mut wire = BytesMut::new();
        for _ in 0..2 * per_slab {
            wire.extend_from_slice(&envelope);
        }
        let mut frame = wire.freeze();
        let backing = frame.clone();
        let mut asm = EnvelopeAssembler::new(1024);
        let mut out = Vec::new();
        while let Some(env) = asm.feed(&mut frame).unwrap() {
            out.push(env.data);
        }
        assert_eq!(out.len(), 2 * per_slab);
        drop(frame);
        assert!(backing.is_unique(), "no message references the frame");
        let held = out.swap_remove(0);
        assert!(!held.is_unique(), "neighbours share the first slab");
        drop(out);
        assert!(held.is_unique(), "assembler moved past the first slab");
        assert_eq!(&held[..], &[7u8; 100][..]);
    }

    /// The slab/own-allocation threshold is half the slab: a 4096-byte
    /// payload is cut from the slab (and shares it with its neighbour), a
    /// 4097-byte payload is its own allocation.
    #[test]
    fn assembler_slab_threshold() {
        let at = vec![3u8; READ_SLAB_SIZE / 2];
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&Envelope::data(Bytes::from(at.clone())).encode());
        wire.extend_from_slice(&Envelope::data(Bytes::from(at.clone())).encode());
        let mut asm = EnvelopeAssembler::new(READ_SLAB_SIZE);
        let out = assemble_all(&mut asm, &[&wire]);
        assert_eq!(out.len(), 2);
        assert_eq!(&out[0].data[..], &at[..]);
        assert!(
            !out[0].data.is_unique(),
            "slab-cut, shared with its neighbour"
        );
        assert_eq!(
            out[1].data.as_ptr() as usize,
            out[0].data.as_ptr() as usize + at.len(),
            "both halves of one slab"
        );
        assert_eq!(asm.body.capacity(), 0);

        let over = vec![4u8; READ_SLAB_SIZE / 2 + 1];
        let wire = Envelope::data(Bytes::from(over.clone())).encode();
        let mut asm = EnvelopeAssembler::new(READ_SLAB_SIZE);
        let out = assemble_all(&mut asm, &[&wire]);
        assert_eq!(&out[0].data[..], &over[..]);
        assert!(out[0].data.is_unique(), "own exact allocation");
        assert!(asm.slab.is_none(), "large path never touches the slab");
        assert_eq!(asm.body.capacity(), 0);
    }

    /// A large payload between two small ones goes around the slab: the
    /// second small message is cut from the same slab, right after the first.
    /// The frames that start and complete the large payload also carry the
    /// small ones (straddling), and every envelope is still delivered.
    #[test]
    fn assembler_large_payload_bypasses_the_slab() {
        let big = vec![1u8; 100 * 1024];
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&Envelope::data(Bytes::from_static(b"one")).encode());
        wire.extend_from_slice(&Envelope::data(Bytes::from(big.clone())).encode());
        wire.extend_from_slice(&Envelope::data(Bytes::from_static(b"two")).encode());
        let frames: Vec<&[u8]> = wire.chunks(16 * 1024).collect();
        let mut asm = EnvelopeAssembler::new(1024 * 1024);
        let out = assemble_all(&mut asm, &frames);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].data, Bytes::from_static(b"one"));
        assert_eq!(&out[1].data[..], &big[..]);
        assert_eq!(out[2].data, Bytes::from_static(b"two"));
        assert!(out[1].data.is_unique(), "large payload owns its allocation");
        assert_eq!(
            out[2].data.as_ptr() as usize,
            out[0].data.as_ptr() as usize + 3,
            "second small message continues the first slab"
        );
    }

    /// A small payload split across frames whose first fragment arrives when
    /// the slab is nearly full is still assembled contiguously: the slab is
    /// rolled to exactly one fresh READ_SLAB_SIZE block (messages held) or
    /// reclaimed in place at its original size (messages dropped) at the
    /// start of the payload, never part-way through it.
    #[test]
    fn assembler_split_small_payload_at_slab_roll() {
        let filler = Envelope::data(Bytes::from(vec![7u8; 100])).encode();
        let per_slab = READ_SLAB_SIZE / 100;
        let payload: Vec<u8> = (0..READ_SLAB_SIZE / 2).map(|i| i as u8).collect();
        let wire = Envelope::data(Bytes::from(payload.clone())).encode();
        let third = wire.len() / 3;
        for hold in [true, false] {
            let mut asm = EnvelopeAssembler::new(READ_SLAB_SIZE);
            let mut fill = BytesMut::new();
            for _ in 0..per_slab {
                fill.extend_from_slice(&filler);
            }
            let held = assemble_all(&mut asm, &[&fill]);
            assert_eq!(held.len(), per_slab);
            let first = held[0].data.as_ptr() as usize;
            assert!(asm.slab.as_ref().unwrap().capacity() < payload.len());
            let held = hold.then_some(held);
            let out = assemble_all(
                &mut asm,
                &[&wire[..third], &wire[third..2 * third], &wire[2 * third..]],
            );
            assert_eq!(out.len(), 1);
            assert_eq!(&out[0].data[..], &payload[..], "hold={hold}");
            let at = out[0].data.as_ptr() as usize;
            if hold {
                assert!(
                    !(first..first + READ_SLAB_SIZE).contains(&at),
                    "rolled to a fresh slab"
                );
            } else {
                assert_eq!(at, first, "reclaimed the released slab in place");
            }
            assert_eq!(
                asm.slab.as_ref().unwrap().capacity(),
                READ_SLAB_SIZE - payload.len(),
                "rolled or reclaimed slab has the original READ_SLAB_SIZE (hold={hold})"
            );
            drop(held);
        }
    }

    /// A zero-length payload allocates nothing: no slab, no body.
    #[test]
    fn assembler_empty_payload_allocates_nothing() {
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&Envelope::data(Bytes::new()).encode());
        wire.extend_from_slice(&Envelope::end_stream(Bytes::new()).encode());
        let mut asm = EnvelopeAssembler::new(1024);
        let out = assemble_all(&mut asm, &[&wire]);
        assert_eq!(out.len(), 2);
        assert!(out[0].data.is_empty() && out[1].data.is_empty());
        assert!(out[1].is_end_stream());
        assert!(asm.slab.is_none());
        assert_eq!(asm.body.capacity(), 0);
    }

    /// An over-limit length is rejected on the header, before any payload
    /// byte is buffered or allocated for.
    #[test]
    fn assembler_rejects_over_limit_before_allocating() {
        let mut wire = vec![0u8; HEADER_SIZE];
        wire[1..].copy_from_slice(&4096u32.to_be_bytes());
        wire.extend_from_slice(&[0xAA; 4096]);
        let mut asm = EnvelopeAssembler::new(1024);
        let mut frame = Bytes::from(wire);
        let err = asm.feed(&mut frame).unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::ResourceExhausted);
        assert_eq!(
            err.message.as_deref(),
            Some("message size 4096 exceeds limit 1024")
        );
        assert_eq!(asm.body.capacity(), 0, "nothing allocated for it");
        assert_eq!(frame.len(), 4096, "payload left unread");
    }

    #[test]
    fn assembler_grpc_web_trailer_limit() {
        let block = vec![b'x'; 2000];
        let trailer = Envelope {
            flags: flags::GRPC_WEB_TRAILER,
            data: Bytes::from(block.clone()),
        }
        .encode();
        // Without the allowance the trailer frame is just an envelope.
        let mut asm = EnvelopeAssembler::new(1024);
        let err = asm.feed(&mut trailer.clone()).unwrap_err();
        assert_eq!(
            err.message.as_deref(),
            Some("message size 2000 exceeds limit 1024")
        );
        // With it, trailer frames get their own limit; data frames do not.
        let mut asm = EnvelopeAssembler::new(1024).with_grpc_web_trailer_limit(4096);
        let out = assemble_all(&mut asm, &[&trailer]);
        assert_eq!(out[0].flags, flags::GRPC_WEB_TRAILER);
        assert_eq!(&out[0].data[..], &block[..]);
        let data = Envelope::data(Bytes::from(block)).encode();
        assert!(asm.feed(&mut data.clone()).is_err());
        // A trailer frame over its own limit says so.
        let mut asm = EnvelopeAssembler::new(1024).with_grpc_web_trailer_limit(1500);
        let err = asm.feed(&mut trailer.clone()).unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::ResourceExhausted);
        assert_eq!(
            err.message.as_deref(),
            Some("grpc-web trailer size 2000 exceeds limit 1500")
        );
    }

    /// Whatever the frame boundaries, the assembler yields exactly what
    /// `Envelope::decode` yields over the contiguous wire.
    #[test]
    fn assembler_matches_contiguous_decode_at_every_split() {
        let mut wire = BytesMut::new();
        for env in [
            Envelope::data(Bytes::new()),
            Envelope::data(Bytes::from_static(b"x")),
            Envelope::data(Bytes::from(vec![9u8; 3000])),
            Envelope::end_stream(Bytes::from_static(b"{}")),
            Envelope {
                flags: flags::GRPC_WEB_TRAILER,
                data: Bytes::from_static(b"grpc-status: 0\r\n"),
            },
            Envelope::data(Bytes::from_static(b"tail")),
        ] {
            wire.extend_from_slice(&env.encode());
        }
        let mut contiguous = wire.clone();
        let mut expected = Vec::new();
        while let Some(env) = Envelope::decode(&mut contiguous).unwrap() {
            expected.push((env.flags, env.data));
        }
        assert_eq!(expected.len(), 6);

        let irregular: Vec<usize> = [0, 1, 6, 7, 13, 2900, 3016, 3017, 3030, wire.len()].into();
        let mut splits: Vec<Vec<&[u8]>> = [1, 3, HEADER_SIZE, 7, 4096]
            .iter()
            .map(|&n| wire.chunks(n).collect())
            .collect();
        splits.push(irregular.windows(2).map(|w| &wire[w[0]..w[1]]).collect());
        for frames in splits {
            let mut asm = EnvelopeAssembler::new(4096);
            let got: Vec<_> = assemble_all(&mut asm, &frames)
                .into_iter()
                .map(|e| (e.flags, e.data))
                .collect();
            assert_eq!(
                got,
                expected,
                "frames of {:?}",
                frames.first().map(|f| f.len())
            );
            assert!(!asm.has_partial());
        }
    }

    /// `feed` never leaves an exhausted transport frame alive in the caller's
    /// slot, whether it stopped mid-header or mid-payload.
    #[test]
    fn assembler_releases_exhausted_frame() {
        let wire = Envelope::data(Bytes::from(vec![7u8; 64])).encode();
        for end in [3, 40] {
            let mut asm = EnvelopeAssembler::new(1024);
            let mut slot = Bytes::copy_from_slice(&wire[..end]);
            let backing = slot.clone();
            assert!(asm.feed(&mut slot).unwrap().is_none());
            assert!(slot.is_empty());
            assert!(
                backing.is_unique(),
                "exhausted frame still referenced from the slot (end={end})"
            );
        }
    }

    // ── EnvelopeDecoder tests ───────────────────────────────────────

    #[test]
    fn test_decoder_complete_message() {
        let mut dec = decoder(1024);
        let mut frame = Envelope::data(Bytes::from_static(b"hello")).encode();

        assert_eq!(message(&mut dec, &mut frame), Bytes::from_static(b"hello"));
        assert!(frame.is_empty());
        assert!(dec.finish().is_ok());
    }

    #[test]
    fn test_decoder_incomplete_header() {
        let mut dec = decoder(1024);
        // Only 3 bytes — not enough for the 5-byte header
        let mut frame = Bytes::from_static(&[0u8, 0, 0]);

        assert!(dec.decode(&mut frame).unwrap().is_none());
        assert!(frame.is_empty(), "frame is consumed into the header carry");
    }

    #[test]
    fn test_decoder_incomplete_payload() {
        let mut dec = decoder(1024);
        // Header says 10 bytes of payload, but we only provide 3
        let mut buf = BytesMut::new();
        buf.put_u8(flags::DATA);
        buf.put_u32(10);
        buf.put_slice(&[1, 2, 3]);
        let mut frame = buf.freeze();

        assert!(dec.decode(&mut frame).unwrap().is_none());
        assert!(frame.is_empty());
        // The rest arrives in a later frame.
        let mut rest = Bytes::from_static(&[4, 5, 6, 7, 8, 9, 10]);
        let msg = message(&mut dec, &mut rest);
        assert_eq!(&msg[..], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_decoder_end_stream_signals_eof() {
        let mut dec = decoder(1024);
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&Envelope::end_stream(Bytes::from_static(b"{}")).encode());
        wire.extend_from_slice(b"trailing");
        let mut frame = wire.freeze();

        // End-stream is terminal and leaves the trailing bytes in the frame
        // for the caller to account for.
        assert!(matches!(
            dec.decode(&mut frame).unwrap(),
            Some(Decoded::EndStream)
        ));
        assert_eq!(&frame[..], b"trailing");
        assert!(dec.finish().is_ok());
    }

    #[test]
    fn test_decoder_message_exceeds_size_limit() {
        let mut dec = decoder(4); // max 4 bytes per message
        let mut frame = Envelope::data(Bytes::from_static(b"too long")).encode();

        let err = dec.decode(&mut frame).unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::ResourceExhausted);
    }

    #[test]
    fn test_decoder_multiple_envelopes_in_frame() {
        let mut dec = decoder(1024);
        let e1 = Envelope::data(Bytes::from_static(b"first"));
        let e2 = Envelope::data(Bytes::from_static(b"second"));
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&e1.encode());
        buf.extend_from_slice(&e2.encode());
        let mut frame = buf.freeze();

        let r1 = message(&mut dec, &mut frame);
        assert_eq!(r1, Bytes::from_static(b"first"));
        let r2 = message(&mut dec, &mut frame);
        assert_eq!(r2, Bytes::from_static(b"second"));
        assert!(dec.decode(&mut frame).unwrap().is_none());
        assert!(frame.is_empty());
    }

    #[test]
    fn test_decoder_data_then_end_stream() {
        let mut dec = decoder(1024);
        let data_env = Envelope::data(Bytes::from_static(b"msg"));
        let end_env = Envelope::end_stream(Bytes::from_static(b"{}"));
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&data_env.encode());
        buf.extend_from_slice(&end_env.encode());
        let mut frame = buf.freeze();

        let r1 = message(&mut dec, &mut frame);
        assert_eq!(r1, Bytes::from_static(b"msg"));
        assert!(matches!(
            dec.decode(&mut frame).unwrap(),
            Some(Decoded::EndStream)
        ));
        assert!(frame.is_empty());
    }

    #[test]
    fn test_finish_without_data() {
        let dec = decoder(1024);
        // Body ending before any envelope is a clean end of stream
        assert!(dec.finish().is_ok());
    }

    #[test]
    fn test_finish_with_partial_header() {
        let mut dec = decoder(1024);
        // Partial header — body ended with incomplete envelope
        assert!(
            dec.decode(&mut Bytes::from_static(&[0u8, 0, 0]))
                .unwrap()
                .is_none()
        );

        let err = dec.finish().unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::InvalidArgument);
        assert_eq!(err.message.as_deref(), Some("incomplete request envelope"));
    }

    #[test]
    fn test_finish_with_partial_payload() {
        let mut dec = decoder(1024);
        let wire = Envelope::data(Bytes::from_static(b"hello")).encode();
        assert!(dec.decode(&mut wire.slice(..7)).unwrap().is_none());

        let err = dec.finish().unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_decoder_compressed_without_encoding_header() {
        let mut dec = decoder(1024);
        // Compressed flag set but decoder has no streaming_encoding
        let mut frame = Envelope::compressed(Bytes::from_static(b"data")).encode();

        let err = dec.decode(&mut frame).unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Internal);
    }

    // ── EnvelopeEncoder tests ───────────────────────────────────────

    #[test]
    fn test_encoder_uncompressed() {
        let mut enc = EnvelopeEncoder::uncompressed();
        let mut buf = BytesMut::new();
        enc.encode(Bytes::from_static(b"hello"), &mut buf).unwrap();

        // Should produce a DATA envelope: [0x00, len=5, "hello"]
        assert_eq!(buf.len(), HEADER_SIZE + 5);
        assert_eq!(buf[0], flags::DATA);
        assert_eq!(u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]), 5);
        assert_eq!(&buf[HEADER_SIZE..], b"hello");
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_encoder_empty_payload_skips_compression() {
        // Empty payload stays uncompressed under default policy (0 < min_size=1024).
        let registry = Arc::new(CompressionRegistry::default());
        let mut enc = EnvelopeEncoder::new(Some((registry, "gzip")), CompressionPolicy::default());
        let mut buf = BytesMut::new();
        enc.encode(Bytes::new(), &mut buf).unwrap();

        assert_eq!(buf[0], flags::DATA, "empty payload should use DATA flag");
        assert_eq!(u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]), 0);
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_encoder_with_compression() {
        let registry = Arc::new(CompressionRegistry::default());
        let mut enc = EnvelopeEncoder::new(
            Some((registry, "gzip")),
            CompressionPolicy::default().with_min_size(0),
        );
        let mut buf = BytesMut::new();
        enc.encode(Bytes::from_static(b"compress me"), &mut buf)
            .unwrap();

        assert_eq!(buf[0], flags::COMPRESSED, "should use COMPRESSED flag");
        let payload_len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        assert!(payload_len > 0);
        assert_eq!(buf.len(), HEADER_SIZE + payload_len);
    }

    #[test]
    fn test_encoder_end_stream() {
        let mut enc = EnvelopeEncoder::uncompressed();
        let mut buf = BytesMut::new();
        enc.encode_end_stream(Bytes::from_static(b"{}"), &mut buf)
            .unwrap();

        assert_eq!(buf[0], flags::END_STREAM);
        assert_eq!(u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]), 2);
        assert_eq!(&buf[HEADER_SIZE..], b"{}");
    }

    #[test]
    #[cfg(feature = "gzip")]
    fn test_encoder_decoder_roundtrip() {
        let registry = Arc::new(CompressionRegistry::default());
        let mut enc = EnvelopeEncoder::new(
            Some((Arc::clone(&registry), "gzip")),
            CompressionPolicy::default(),
        );
        let mut dec = EnvelopeDecoder::new(1024, Some("gzip".to_owned()), registry);

        let original = Bytes::from_static(b"roundtrip test data");
        let mut buf = BytesMut::new();
        enc.encode(original.clone(), &mut buf).unwrap();
        let mut frame = buf.freeze();

        let decoded = message(&mut dec, &mut frame);
        assert_eq!(decoded, original);
        assert!(frame.is_empty());
    }

    #[test]
    fn test_encoder_multiple_messages() {
        let mut enc = EnvelopeEncoder::uncompressed();
        let mut buf = BytesMut::new();
        enc.encode(Bytes::from_static(b"one"), &mut buf).unwrap();
        enc.encode(Bytes::from_static(b"two"), &mut buf).unwrap();

        // Two envelopes back-to-back
        assert_eq!(buf.len(), 2 * HEADER_SIZE + 3 + 3);

        // Decode both with a decoder
        let mut dec = decoder(1024);
        let mut frame = buf.freeze();
        let r1 = message(&mut dec, &mut frame);
        assert_eq!(r1, Bytes::from_static(b"one"));
        let r2 = message(&mut dec, &mut frame);
        assert_eq!(r2, Bytes::from_static(b"two"));
        assert!(frame.is_empty());
    }
}
