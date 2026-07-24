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
        let total = body.len();
        if total < min_chain {
            let mut buf = BytesMut::with_capacity(HEADER_SIZE + total);
            write_envelope_header(flags, total, &mut buf)
                .expect("envelope payload exceeds u32::MAX");
            for segment in body.segments() {
                buf.extend_from_slice(segment);
            }
            return (buf.freeze(), Vec::new());
        }

        let mut head = BytesMut::with_capacity(HEADER_SIZE);
        write_envelope_header(flags, total, &mut head).expect("envelope payload exceeds u32::MAX");

        let segments = match body {
            crate::response::EncodedBody::Contiguous(bytes) => vec![bytes],
            crate::response::EncodedBody::Segmented(segments) => segments,
        };
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
        if buf.len() < HEADER_SIZE {
            return Ok(None);
        }

        let flags = buf[0];
        let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

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
        if buf.len() < HEADER_SIZE.saturating_add(length) {
            return Ok(None);
        }

        buf.advance(HEADER_SIZE);
        let data = buf.split_to(length).freeze();

        Ok(Some(Self { flags, data }))
    }
}

/// Decoder for Connect envelope-framed messages on a streaming request.
///
/// Implements [`tokio_util::codec::Decoder`] so it can be used with
/// [`FramedRead`](tokio_util::codec::FramedRead) to turn a raw byte stream
/// into a stream of decoded (and optionally decompressed) message payloads.
///
/// Returns `Ok(None)` when more data is needed — `FramedRead` handles the
/// async waiting automatically, eliminating manual buffer/loop management.
pub(crate) struct EnvelopeDecoder {
    max_message_size: usize,
    streaming_encoding: Option<String>,
    compression: Arc<CompressionRegistry>,
    /// Set to `true` once we receive an end-stream envelope; signals EOF.
    done: bool,
}

impl EnvelopeDecoder {
    pub(crate) fn new(
        max_message_size: usize,
        streaming_encoding: Option<String>,
        compression: Arc<CompressionRegistry>,
    ) -> Self {
        Self {
            max_message_size,
            streaming_encoding,
            compression,
            done: false,
        }
    }

    /// Returns `true` once an end-stream envelope has been decoded.
    ///
    /// After this point [`decode`](tokio_util::codec::Decoder::decode) always
    /// returns `Ok(None)` — the decoder will never produce another message.
    /// Callers must treat this as a terminal state and stop buffering body
    /// bytes for the decoder; any further data is trailing garbage that
    /// should be drained (bounded) or rejected, never accumulated.
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }
}

impl tokio_util::codec::Decoder for EnvelopeDecoder {
    type Item = Bytes;
    type Error = ConnectError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Bytes>, ConnectError> {
        if self.done {
            return Ok(None);
        }

        let envelope = match Envelope::decode_with_limit(buf, self.max_message_size)? {
            Some(envelope) => envelope,
            None => return Ok(None), // need more data
        };

        if envelope.is_end_stream() {
            tracing::trace!("client stream: received end-stream envelope");
            self.done = true;
            return Ok(None);
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
                self.max_message_size,
            )?
        } else {
            envelope.data
        };

        tracing::trace!(
            size = data.len(),
            "client stream: dispatching message to handler"
        );

        Ok(Some(data))
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Bytes>, ConnectError> {
        // Try to decode any remaining complete envelope in the buffer.
        match self.decode(buf)? {
            some @ Some(_) => Ok(some),
            None => {
                // Body ended. A client may close the HTTP body without sending
                // an END_STREAM envelope — the body ending is itself the
                // end-of-stream signal. Leftover bytes mean a truncated envelope.
                if !buf.is_empty() {
                    tracing::debug!(
                        remaining_bytes = buf.len(),
                        "client stream: body ended with incomplete envelope"
                    );
                    Err(ConnectError::invalid_argument(
                        "incomplete request envelope",
                    ))
                } else {
                    Ok(None)
                }
            }
        }
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
    /// least `min_chain` bytes, only the 5-byte envelope header is written
    /// into `dst` and the payload is returned for the caller to emit as its
    /// own body frame — a refcount move instead of a payload-sized memcpy.
    /// Smaller payloads are written contiguously into `dst` and `None` is
    /// returned.
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
        data: Bytes,
        dst: &mut BytesMut,
        min_chain: usize,
    ) -> Result<Option<Bytes>, ConnectError> {
        let (flag, payload) = if let Some((ref comp, ref encoding)) = self.compression
            && self.policy.should_compress(data.len())
        {
            (flags::COMPRESSED, comp.compress(encoding, &data)?)
        } else {
            (flags::DATA, data)
        };
        if payload.len() < min_chain {
            write_envelope(flag, &payload, dst)?;
            return Ok(None);
        }
        write_envelope_header(flag, payload.len(), dst)?;
        Ok(Some(payload))
    }
}

impl tokio_util::codec::Encoder<Bytes> for EnvelopeEncoder {
    type Error = ConnectError;

    fn encode(&mut self, data: Bytes, dst: &mut BytesMut) -> Result<(), ConnectError> {
        // `usize::MAX` threshold: the contiguous entry point never chains.
        let chained = self.encode_chained(data, dst, usize::MAX)?;
        debug_assert!(chained.is_none(), "usize::MAX threshold cannot chain");
        Ok(())
    }
}

/// Write a single envelope (header + payload) into a `BytesMut` buffer.
/// The length is validated (via [`write_envelope_header`]) before any
/// buffer growth, so an oversized payload errors without allocating.
fn write_envelope(flag: u8, data: &[u8], dst: &mut BytesMut) -> Result<(), ConnectError> {
    write_envelope_header(flag, data.len(), dst)?;
    dst.put_slice(data);
    Ok(())
}

/// Write only the 5-byte envelope header (flag + big-endian length) into
/// `dst`, for callers that emit the payload as its own body frame.
fn write_envelope_header(flag: u8, len: usize, dst: &mut BytesMut) -> Result<(), ConnectError> {
    if len > u32::MAX as usize {
        return Err(ConnectError::resource_exhausted(format!(
            "envelope payload {len} bytes exceeds u32::MAX"
        )));
    }
    dst.reserve(HEADER_SIZE);
    dst.put_u8(flag);
    dst.put_u32(len as u32);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::codec::{Decoder, Encoder};

    /// Helper: create a decoder with no compression support, suitable for
    /// testing uncompressed envelope framing.
    fn decoder(max_message_size: usize) -> EnvelopeDecoder {
        EnvelopeDecoder::new(
            max_message_size,
            None,
            Arc::new(CompressionRegistry::default()),
        )
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
        let chained = enc
            .encode_chained(Bytes::from(data), &mut dst, 1024)
            .unwrap()
            .expect("large compressed payload must chain");

        assert_eq!(dst.len(), HEADER_SIZE);
        assert_eq!(dst[0], flags::COMPRESSED);
        assert_eq!(
            u32::from_be_bytes([dst[1], dst[2], dst[3], dst[4]]) as usize,
            chained.len()
        );

        // Reassembled envelope decodes back to the original payload.
        let mut wire = dst;
        wire.put_slice(&chained);
        let mut dec = EnvelopeDecoder::new(1024 * 1024, Some("gzip".to_owned()), registry);
        let decoded = Decoder::decode(&mut dec, &mut wire).unwrap().unwrap();
        assert_eq!(decoded.len(), 64 * 1024);
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

    // ── EnvelopeDecoder tests ───────────────────────────────────────

    #[test]
    fn test_decoder_complete_message() {
        let mut dec = decoder(1024);
        let envelope = Envelope::data(Bytes::from_static(b"hello"));
        let mut buf = BytesMut::from(&envelope.encode()[..]);

        let result = dec.decode(&mut buf).unwrap();
        assert_eq!(result.unwrap(), Bytes::from_static(b"hello"));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decoder_incomplete_header() {
        let mut dec = decoder(1024);
        // Only 3 bytes — not enough for the 5-byte header
        let mut buf = BytesMut::from(&[0u8, 0, 0][..]);

        assert!(dec.decode(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 3, "buffer should be untouched");
    }

    #[test]
    fn test_decoder_incomplete_payload() {
        let mut dec = decoder(1024);
        // Header says 10 bytes of payload, but we only provide 3
        let mut buf = BytesMut::new();
        buf.put_u8(flags::DATA);
        buf.put_u32(10);
        buf.put_slice(&[1, 2, 3]);

        assert!(dec.decode(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 8, "buffer should be untouched");
    }

    #[test]
    fn test_decoder_end_stream_signals_eof() {
        let mut dec = decoder(1024);
        let envelope = Envelope::end_stream(Bytes::from_static(b"{}"));
        let mut buf = BytesMut::from(&envelope.encode()[..]);

        // End-stream envelope yields None (EOF signal)
        assert!(dec.decode(&mut buf).unwrap().is_none());
        // Subsequent calls also yield None
        assert!(dec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_decoder_message_exceeds_size_limit() {
        let mut dec = decoder(4); // max 4 bytes per message
        let envelope = Envelope::data(Bytes::from_static(b"too long"));
        let mut buf = BytesMut::from(&envelope.encode()[..]);

        let err = dec.decode(&mut buf).unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::ResourceExhausted);
    }

    #[test]
    fn test_decoder_multiple_envelopes_in_buffer() {
        let mut dec = decoder(1024);
        let e1 = Envelope::data(Bytes::from_static(b"first"));
        let e2 = Envelope::data(Bytes::from_static(b"second"));
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&e1.encode());
        buf.extend_from_slice(&e2.encode());

        let r1 = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(r1, Bytes::from_static(b"first"));
        let r2 = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(r2, Bytes::from_static(b"second"));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decoder_data_then_end_stream() {
        let mut dec = decoder(1024);
        let data_env = Envelope::data(Bytes::from_static(b"msg"));
        let end_env = Envelope::end_stream(Bytes::from_static(b"{}"));
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&data_env.encode());
        buf.extend_from_slice(&end_env.encode());

        let r1 = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(r1, Bytes::from_static(b"msg"));
        // End-stream yields None
        assert!(dec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_decode_eof_empty_buffer() {
        let mut dec = decoder(1024);
        let mut buf = BytesMut::new();
        // Empty buffer at EOF is fine — clean end of stream
        assert!(dec.decode_eof(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_decode_eof_with_complete_envelope() {
        let mut dec = decoder(1024);
        let envelope = Envelope::data(Bytes::from_static(b"final"));
        let mut buf = BytesMut::from(&envelope.encode()[..]);

        let result = dec.decode_eof(&mut buf).unwrap();
        assert_eq!(result.unwrap(), Bytes::from_static(b"final"));
    }

    #[test]
    fn test_decode_eof_with_leftover_bytes() {
        let mut dec = decoder(1024);
        // Partial header — body ended with incomplete envelope
        let mut buf = BytesMut::from(&[0u8, 0, 0][..]);

        let err = dec.decode_eof(&mut buf).unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_decoder_compressed_without_encoding_header() {
        let mut dec = decoder(1024);
        // Compressed flag set but decoder has no streaming_encoding
        let envelope = Envelope::compressed(Bytes::from_static(b"data"));
        let mut buf = BytesMut::from(&envelope.encode()[..]);

        let err = dec.decode(&mut buf).unwrap_err();
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

        let decoded = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, original);
        assert!(buf.is_empty());
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
        let r1 = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(r1, Bytes::from_static(b"one"));
        let r2 = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(r2, Bytes::from_static(b"two"));
        assert!(buf.is_empty());
    }
}
