//! Type-erased, lazily-decoded RPC message bodies.
//!
//! Interceptors run on every RPC and most of them never look inside the
//! request or response message — they read the [`Spec`](crate::Spec),
//! the headers, or the deadline and pass the call through. Decoding the
//! message eagerly would tax every call to pay for the rare interceptor
//! that inspects fields.
//!
//! [`Payload`] solves this by holding the wire bytes (reference-counted)
//! and decoding to a typed message on first access — or, when built from
//! a typed message with [`Payload::from_message`], encoding on first need.
//! Interceptors that want a typed message call [`Payload::message`]
//! (owned, works for both proto and JSON wires) or [`Payload::view`]
//! (zero-copy, proto only). Interceptors that want to *replace* the
//! message call [`Payload::set_message`]; the dispatch path re-encodes
//! the replacement on the way out.
//!
//! [`AnyMessage`] is the object-safe surface that lets a `Payload` cache
//! a decoded message without knowing its concrete type. It has a blanket
//! impl over every protobuf message, so user code never implements it
//! directly.

use std::any::Any;
use std::fmt;
use std::sync::OnceLock;

use buffa::Message;
use buffa::view::{MessageView, OwnedView};
use bytes::Bytes;

use crate::codec::{
    CodecFormat, JsonDeserialize, JsonSerialize, decode_json, decode_proto_with_options,
    encode_json, encode_proto,
};
use crate::error::ConnectError;

/// Object-safe, type-erased RPC message.
///
/// `AnyMessage` is what a [`Payload`] caches once it has decoded its wire
/// bytes — a `Box<dyn AnyMessage>` that can be downcast back to the
/// concrete request/response type and re-encoded for the wire if an
/// interceptor swaps it.
///
/// You will almost never implement this trait directly. A blanket
/// implementation covers every type that is `Message + JsonSerialize`, which
/// includes every owned message the code generator emits. A manual impl
/// must uphold a round-trip invariant: bytes returned by
/// [`encode`](AnyMessage::encode)`(format)` must decode back to an
/// equivalent value in that same format — the dispatch path relies on it
/// when re-encoding a replacement set via [`Payload::set_message`].
/// Violating it does not panic or error — [`Payload::encoded`] silently
/// produces wrong-shape bytes — so a manual impl must be tested for the
/// round-trip explicitly.
pub trait AnyMessage: Send + Sync + 'static {
    /// Borrow the message as `dyn Any` for downcasting.
    fn as_any(&self) -> &dyn Any;
    /// Mutably borrow the message as `dyn Any` for downcasting.
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// Convert the boxed message into a `Box<dyn Any>` for owned downcasting.
    ///
    /// [`Payload::take_message`] uses this to move the cached decode out
    /// of the `Payload` and into the handler without a clone.
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    /// Serialize the message to wire bytes in the given format.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding fails. Proto encoding is infallible
    /// for valid messages; JSON encoding can fail on non-UTF-8 `bytes`
    /// fields and similar serde edge cases.
    fn encode(&self, format: CodecFormat) -> Result<Bytes, ConnectError>;
    /// The concrete type's name, for diagnostics. The default uses
    /// [`std::any::type_name`].
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

impl<T> AnyMessage for T
where
    // Message already requires Send + Sync as supertraits.
    T: Message + JsonSerialize + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
    fn encode(&self, format: CodecFormat) -> Result<Bytes, ConnectError> {
        match format {
            CodecFormat::Proto => encode_proto(self),
            CodecFormat::Json => encode_json(self),
        }
    }
}

/// A lazily-decoded, replaceable RPC message body.
///
/// A `Payload` holds the wire-encoded body bytes ([`Bytes`], so clones
/// are reference-counted) and the [`CodecFormat`] they came in — or, when
/// built with [`from_message`](Payload::from_message), a typed message
/// that is encoded only on demand. Typed access happens on demand:
///
/// - [`message`](Payload::message) — decode once into an owned message,
///   cache it, return a borrow. Works for both `Proto` and `Json` wires.
/// - [`view`](Payload::view) — zero-copy view borrowing the wire bytes
///   directly. `Proto` wires only; returns an error for `Json`.
/// - [`set_message`](Payload::set_message) — replace the body. The
///   replacement takes priority for all subsequent reads, and the
///   dispatch path re-encodes it on the way out.
/// - [`encoded`](Payload::encoded) — the wire bytes that will actually be
///   sent: either the original `bytes` or the re-encoded replacement.
///
/// `Payload` is normally constructed by the dispatch path and received
/// by user code through [`UnaryRequest`](crate::interceptor::UnaryRequest)
/// and [`UnaryResponse`](crate::interceptor::UnaryResponse).
/// [`Payload::new`] (from wire bytes) and [`Payload::from_message`] (from
/// a typed message) are `pub` so test fixtures, custom transports, and
/// short-circuiting interceptors can build one directly.
///
/// `message` borrows the cached owned decode; `view` returns a fresh
/// self-contained [`OwnedView`] (a [`Bytes`] refcount bump, not a copy)
/// because a zero-copy view cannot be stored in the type-erased cache.
///
/// `Payload` is intentionally not `Clone`: a clone would either drop the
/// decode cache (surprising) or duplicate it (defeating the laziness).
/// Pass it by reference, or move it through the call chain; when a
/// second body is genuinely needed, [`try_clone`](Payload::try_clone) is
/// the explicit copy.
pub struct Payload {
    bytes: Bytes,
    format: CodecFormat,
    decoded: OnceLock<Box<dyn AnyMessage>>,
    /// Boxed so a payload without a replacement (the server fast path)
    /// carries one pointer, not the message box plus its encode memo.
    replaced: Option<Box<Replacement>>,
    decode_options: buffa::DecodeOptions,
}

/// A message set by [`Payload::set_message`] / [`Payload::from_message`],
/// with its wire encoding memoized on first [`Payload::encoded`]. A new
/// replacement is a new `Replacement`, so the memo can never be stale.
struct Replacement {
    message: Box<dyn AnyMessage>,
    encoded: OnceLock<Bytes>,
}

impl Replacement {
    fn new(message: impl AnyMessage) -> Box<Self> {
        Box::new(Self {
            message: Box::new(message),
            encoded: OnceLock::new(),
        })
    }
}

impl Payload {
    /// Wrap wire bytes in a `Payload`. No decoding happens until a typed
    /// accessor is called.
    pub fn new(bytes: Bytes, format: CodecFormat) -> Self {
        Self {
            bytes,
            format,
            decoded: OnceLock::new(),
            replaced: None,
            decode_options: buffa::DecodeOptions::new(),
        }
    }

    /// Wrap an owned message in a `Payload` without encoding it.
    ///
    /// This is the *lazily-encoded* counterpart of [`new`](Payload::new)
    /// for a caller that starts from a typed message rather than wire
    /// bytes — an interceptor synthesizing a response, or an outbound
    /// request. The message is stored as the payload's replacement:
    /// [`message`](Payload::message) and
    /// [`take_message`](Payload::take_message) downcast it without a
    /// decode, and [`encoded`](Payload::encoded) encodes it on first call
    /// and memoizes the result, so later `encoded()` /
    /// [`try_clone`](Payload::try_clone) calls are refcount bumps.
    /// [`view`](Payload::view) encodes to proto and views it.
    ///
    /// [`bytes`](Payload::bytes) is **empty** for a payload built this way:
    /// there are no peer-supplied wire bytes. Use
    /// [`encoded`](Payload::encoded) for the bytes that will be sent.
    ///
    /// Short-circuit example (a cache hit, a test double):
    /// `Ok(Response::new(Payload::from_message(reply, req.payload.format())))`.
    pub fn from_message<M: AnyMessage>(message: M, format: CodecFormat) -> Self {
        let mut payload = Self::new(Bytes::new(), format);
        payload.replaced = Some(Replacement::new(message));
        payload
    }

    /// Produce an independent `Payload` carrying the same body — the
    /// explicit copy for an interceptor that runs the rest of the chain
    /// more than once (see
    /// [`UnaryRequest::try_clone`](crate::interceptor::UnaryRequest::try_clone)).
    /// The copy holds this payload's [`encoded`](Payload::encoded) bytes
    /// (a refcount bump; a replacement is encoded once and memoized on
    /// `self` first), the same format and decode options, and an empty
    /// decode cache.
    ///
    /// The copy is equivalent **on the wire**, not through the typed API:
    /// it carries bytes, not the replacement (a replacement's bytes keep
    /// buffa's default decode limits rather than this payload's wire
    /// limits, as [`view`](Payload::view) already grants them), so on the copy
    /// [`message`](Payload::message) / [`take_message`](Payload::take_message)
    /// perform a real decode, a wrong-type read reports `InvalidArgument`
    /// rather than `Internal`, and [`view`](Payload::view) follows the
    /// [`format`](Payload::format) rule (errors for JSON).
    ///
    /// # Errors
    ///
    /// Returns an error if encoding a replacement fails.
    pub fn try_clone(&self) -> Result<Self, ConnectError> {
        let copy = Self::new(self.encoded()?, self.format);
        Ok(if self.replaced.is_some() {
            // The body was encoded from a message this process holds, so the
            // wire-facing limits (an amplification defence against peer
            // bytes) do not apply to it — the same exemption `view` gives
            // the replacement before the copy is taken.
            copy
        } else {
            copy.with_decode_options(self.decode_options.clone())
        })
    }

    /// Attach the decode limits a typed accessor should honour.
    ///
    /// The server sets this from its [`Limits`](crate::Limits); a payload
    /// built anywhere else decodes under buffa's defaults.
    #[doc(hidden)] // set by the service from its configured `Limits`
    #[must_use]
    pub fn with_decode_options(mut self, options: buffa::DecodeOptions) -> Self {
        self.decode_options = options;
        self
    }

    /// The decode limits this payload's typed accessors honour.
    #[doc(hidden)]
    #[must_use]
    pub fn decode_options(&self) -> &buffa::DecodeOptions {
        &self.decode_options
    }

    /// The wire bytes as received, **ignoring** any replacement set with
    /// [`set_message`](Payload::set_message) (so stale after one), and
    /// **empty** for a payload built with
    /// [`from_message`](Payload::from_message) — nothing was received. Do
    /// not hash, sign, size-check, or log this as "the body"; for the
    /// bytes the dispatch path will actually send, use
    /// [`encoded()`](Payload::encoded).
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// The codec format of the body: the format the wire bytes arrived in,
    /// or, for a [`from_message`](Payload::from_message) payload, the format
    /// it will be encoded in.
    pub fn format(&self) -> CodecFormat {
        self.format
    }

    /// Decode and cache the body as an owned `M`, returning a borrow.
    ///
    /// The decode runs at most once per `Payload`; subsequent calls
    /// return the cached value. If a replacement has been set with
    /// [`set_message`](Payload::set_message), that is returned instead.
    ///
    /// # Errors
    ///
    /// - [`invalid_argument`](ConnectError::invalid_argument) if the wire
    ///   bytes fail to decode as `M` — peer-supplied data, not a server
    ///   bug.
    /// - [`internal`](ConnectError::internal) if a replacement set with
    ///   [`set_message`](Payload::set_message) is not an `M`, or if a
    ///   prior `message::<N>()` cached a different type than `M` — both
    ///   are server-side programming errors (interceptors and handlers
    ///   for the same RPC must agree on the message types), and the cache
    ///   holds whichever type decoded first.
    pub fn message<M>(&self) -> Result<&M, ConnectError>
    where
        M: Message + JsonSerialize + JsonDeserialize + 'static,
    {
        if let Some(replaced) = &self.replaced {
            let replaced = &replaced.message;
            return replaced.as_any().downcast_ref::<M>().ok_or_else(|| {
                ConnectError::internal(format!(
                    "payload replacement is a {}, not a {}",
                    replaced.type_name(),
                    std::any::type_name::<M>()
                ))
            });
        }
        // `get_or_try_init` is unstable, so probe-then-set. Two threads
        // racing decode the same bytes; only one `set` wins and the loser
        // discards its copy. With the same `M` (the normal case), the loser
        // still returns the winner's cached value. With *different* `M`
        // (a caller-side type bug), only the winner's type is cached and
        // the other caller gets the wrong-type error below.
        if self.decoded.get().is_none() {
            let m: M = match self.format {
                CodecFormat::Proto => decode_proto_with_options(&self.bytes, &self.decode_options)?,
                CodecFormat::Json => decode_json(&self.bytes)?,
            };
            let _ = self.decoded.set(Box::new(m));
        }
        // The `set` above (or a concurrent winner's) guarantees the cell is
        // now populated. The downcast can still miss if a prior call cached
        // a different `M`; that's a caller-side type mismatch, not a panic.
        let cached = self.decoded.get().expect("decoded cell populated above");
        cached.as_any().downcast_ref::<M>().ok_or_else(|| {
            ConnectError::internal(format!(
                "payload was previously decoded as a {}, not a {}",
                cached.type_name(),
                std::any::type_name::<M>()
            ))
        })
    }

    /// Decode the body into an owned `M`, consuming `self`.
    ///
    /// If the body was already decoded — an interceptor called
    /// [`message`](Payload::message) — and the cached value is an `M`,
    /// it is moved out: no second decode, no clone. If a replacement was
    /// set with [`set_message`](Payload::set_message), it is moved out
    /// instead. Otherwise the wire bytes are decoded fresh.
    ///
    /// The dispatch path uses this to hand the request to the handler
    /// without re-decoding bytes an interceptor already decoded. Because
    /// it consumes the `Payload`, it must be the last access — call
    /// [`message`](Payload::message) (which caches a borrow) for repeated
    /// reads.
    ///
    /// # Errors
    ///
    /// The same error contract as [`message`](Payload::message), with one
    /// behavioral difference worth noting: a wrong-typed cache that an
    /// interceptor created is now an error *for the handler*. Before
    /// `take_message`, the handler decoded the wire bytes independently
    /// and never saw the interceptor's cache, so an interceptor that
    /// decoded as the wrong `M` failed silently. Surfacing it loudly is
    /// the intent — interceptors and handlers for the same RPC must agree
    /// on the message types.
    ///
    /// - [`invalid_argument`](ConnectError::invalid_argument) if there is
    ///   no cache and the wire bytes fail to decode as `M` — peer-supplied
    ///   data, not a server bug.
    /// - [`internal`](ConnectError::internal) if a cached decode or a
    ///   replacement set with [`set_message`](Payload::set_message) is not
    ///   an `M` — a server-side type bug.
    pub fn take_message<M>(self) -> Result<M, ConnectError>
    where
        // Unlike `message()`, this never *populates* the cache, so it
        // does not need `M: JsonSerialize` (the bound `message()` carries to
        // box `M` as `dyn AnyMessage`). It only reads the cache or
        // decodes fresh.
        M: Message + JsonDeserialize + 'static,
    {
        if let Some(replaced) = self.replaced {
            let replaced = replaced.message;
            let type_name = replaced.type_name();
            return replaced
                .into_any()
                .downcast::<M>()
                .map(|b| *b)
                .map_err(|_| {
                    ConnectError::internal(format!(
                        "payload replacement is a {}, not a {}",
                        type_name,
                        std::any::type_name::<M>()
                    ))
                });
        }
        if let Some(cached) = self.decoded.into_inner() {
            let type_name = cached.type_name();
            return cached.into_any().downcast::<M>().map(|b| *b).map_err(|_| {
                ConnectError::internal(format!(
                    "payload was previously decoded as a {}, not a {}",
                    type_name,
                    std::any::type_name::<M>()
                ))
            });
        }
        match self.format {
            CodecFormat::Proto => decode_proto_with_options(&self.bytes, &self.decode_options),
            CodecFormat::Json => decode_json(&self.bytes),
        }
    }

    /// Decode the body as a zero-copy [`OwnedView`].
    ///
    /// Borrows directly from the wire bytes — no copy, no allocation
    /// beyond the [`Bytes`] refcount bump. If a replacement has been set
    /// with [`set_message`](Payload::set_message) (or the payload came
    /// from [`from_message`](Payload::from_message)), it is encoded to
    /// proto and decoded as a view. For a proto-format payload that
    /// encode is the memoized [`encoded`](Payload::encoded); for a JSON
    /// payload it is a separate proto encode on **every** call. Either
    /// way the view decode itself is not cached (unlike
    /// [`message`](Payload::message)), so hold onto the returned
    /// `OwnedView` rather than re-fetching in a loop.
    ///
    /// # Errors
    ///
    /// - [`internal`](ConnectError::internal) for JSON-encoded wires:
    ///   JSON cannot back a zero-copy proto view. This is a server-side
    ///   programming error and escapes to the peer as a 500 if uncaught —
    ///   branch on [`format()`](Payload::format) and call
    ///   [`message()`](Payload::message) for JSON wires instead.
    /// - [`invalid_argument`](ConnectError::invalid_argument) if the wire
    ///   bytes exceed one of the payload's
    ///   [decode options](Payload::with_decode_options)' limits, or fail to
    ///   decode as `V` — peer-supplied data, not a server bug.
    /// - [`internal`](ConnectError::internal) if a replacement set with
    ///   [`set_message`](Payload::set_message) fails to re-encode or
    ///   decode as `V` — server-supplied data, so the asymmetry with the
    ///   wire-bytes case is intentional. A replacement is decoded under
    ///   buffa's defaults rather than these limits for the same reason.
    pub fn view<V>(&self) -> Result<OwnedView<V>, ConnectError>
    where
        V: MessageView<'static>,
    {
        if let Some(replaced) = &self.replaced {
            // A replacement was just encoded from a message this process
            // already holds, so the wire-facing decode limits do not apply:
            // the element-memory budget is an amplification defence against a
            // small peer payload materializing a huge one, and a server-built
            // message can legitimately exceed it. `StreamMessage::from_message`
            // lifts the same limits for the same reason.
            //
            // Reuse the memoized wire encode when it *is* proto; a JSON
            // payload's replacement needs a separate proto encode.
            let bytes = match self.format {
                CodecFormat::Proto => self.encoded()?,
                CodecFormat::Json => replaced.message.encode(CodecFormat::Proto)?,
            };
            return OwnedView::decode(bytes).map_err(|e| {
                ConnectError::internal(format!("failed to decode replacement as view: {e}"))
            });
        }
        if self.format != CodecFormat::Proto {
            return Err(ConnectError::internal(
                "Payload::view requires a proto-encoded wire; use Payload::message for JSON",
            ));
        }
        OwnedView::decode_with_options(self.bytes.clone(), &self.decode_options).map_err(|e| {
            ConnectError::invalid_argument(format!("failed to decode payload as view: {e}"))
        })
    }

    /// Replace the body with a new message.
    ///
    /// Subsequent [`message`](Payload::message) and
    /// [`view`](Payload::view) calls return the replacement, and
    /// [`encoded`](Payload::encoded) re-encodes it for the wire.
    pub fn set_message<M>(&mut self, message: M)
    where
        M: AnyMessage,
    {
        self.replaced = Some(Replacement::new(message));
        // Drop the prior decode cache so the original message doesn't pin
        // memory for the Payload's lifetime. `replaced` is checked first,
        // so a stale cache would never be visible — this is purely a
        // memory concern.
        self.decoded.take();
    }

    /// The wire bytes the dispatch path should actually send.
    ///
    /// Returns the original `bytes` (a cheap [`Bytes`] clone) unless a
    /// replacement was set with [`set_message`](Payload::set_message) or
    /// the payload was built with [`from_message`](Payload::from_message),
    /// in which case the message is encoded in [`format`](Payload::format)
    /// on the first call and memoized — later calls are refcount bumps.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding a replacement fails.
    pub fn encoded(&self) -> Result<Bytes, ConnectError> {
        let Some(replaced) = &self.replaced else {
            return Ok(self.bytes.clone());
        };
        // Probe-then-set, as in `message()`: `get_or_try_init` is
        // unstable. Two racing callers both encode; one `set` wins and
        // both return equal bytes.
        if let Some(cached) = replaced.encoded.get() {
            return Ok(cached.clone());
        }
        let bytes = replaced.message.encode(self.format)?;
        // A racing loser returns the winner's allocation, so later
        // `try_clone` / `view` calls share one memo.
        Ok(replaced.encoded.get_or_init(|| bytes).clone())
    }

    /// The body encoded in `format`, which the dispatch path negotiated with
    /// the peer and which may differ from this payload's own
    /// [`format`](Payload::format).
    ///
    /// A payload built from a message is simply encoded in the requested
    /// format (memoized only when it matches the payload's own format). A
    /// payload carrying wire bytes in another format cannot be transcoded
    /// without knowing its message type, so that is an error rather than a
    /// body the peer cannot parse.
    ///
    /// # Errors
    ///
    /// `internal` if a replacement fails to encode, or if the payload holds
    /// wire bytes in a format other than `format`.
    pub fn encoded_as(&self, format: CodecFormat) -> Result<Bytes, ConnectError> {
        if format == self.format {
            return self.encoded();
        }
        match &self.replaced {
            Some(replaced) => replaced.message.encode(format),
            None => Err(ConnectError::internal(format!(
                "payload carries {:?} wire bytes but the call negotiated {format:?}",
                self.format
            ))),
        }
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `wire_len` (not `len`): it is the received-bytes length, which
        // is 0 for a `from_message` payload and stale after `set_message`
        // — `replaced: true` alongside it says the body lives elsewhere.
        f.debug_struct("Payload")
            .field("wire_len", &self.bytes.len())
            .field("format", &self.format)
            .field("decoded", &self.decoded.get().is_some())
            .field("replaced", &self.replaced.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffa_types::google::protobuf::__buffa::view::StringValueView;
    use buffa_types::google::protobuf::StringValue;

    fn proto_payload(value: &str) -> Payload {
        let msg = StringValue {
            value: value.into(),
            ..Default::default()
        };
        Payload::new(encode_proto(&msg).unwrap(), CodecFormat::Proto)
    }

    #[test]
    fn message_decodes_and_caches() {
        let p = proto_payload("hello");
        let m1: &StringValue = p.message().unwrap();
        assert_eq!(m1.value, "hello");
        // Second call returns the same cached value (same address).
        let m2: &StringValue = p.message().unwrap();
        assert!(std::ptr::eq(m1, m2), "second call should hit the cache");
    }

    #[cfg(feature = "json")]
    #[test]
    fn message_decodes_json() {
        let bytes = encode_json(&StringValue {
            value: "json".into(),
            ..Default::default()
        })
        .unwrap();
        let p = Payload::new(bytes, CodecFormat::Json);
        let m: &StringValue = p.message().unwrap();
        assert_eq!(m.value, "json");
    }

    #[test]
    fn view_zero_copy_proto() {
        let p = proto_payload("zero copy");
        let v = p.view::<StringValueView>().unwrap();
        assert_eq!(v.reborrow().value, "zero copy");
        // Borrows the payload's bytes — same backing storage.
        let value_ptr = v.reborrow().value.as_ptr() as usize;
        let bytes_range =
            p.bytes().as_ptr() as usize..p.bytes().as_ptr() as usize + p.bytes().len();
        assert!(
            bytes_range.contains(&value_ptr),
            "view should borrow from the payload's wire bytes"
        );
    }

    #[cfg(feature = "json")]
    #[test]
    fn view_errors_on_json() {
        let bytes = encode_json(&StringValue {
            value: "x".into(),
            ..Default::default()
        })
        .unwrap();
        let p = Payload::new(bytes, CodecFormat::Json);
        let err = p.view::<StringValueView>().unwrap_err();
        assert!(
            err.message
                .as_deref()
                .unwrap_or_default()
                .contains("requires a proto-encoded wire"),
            "{err:?}"
        );
    }

    #[test]
    fn set_message_round_trips() {
        let mut p = proto_payload("before");
        p.set_message(StringValue {
            value: "after".into(),
            ..Default::default()
        });
        // message() returns the replacement.
        let m: &StringValue = p.message().unwrap();
        assert_eq!(m.value, "after");
        // view() re-encodes the replacement and views it.
        let v = p.view::<StringValueView>().unwrap();
        assert_eq!(v.reborrow().value, "after");
        // encoded() re-encodes for the original format.
        let encoded = p.encoded().unwrap();
        let rt: StringValue = crate::codec::decode_proto(&encoded).unwrap();
        assert_eq!(rt.value, "after");
        // bytes() is unchanged.
        let orig: StringValue = crate::codec::decode_proto(p.bytes()).unwrap();
        assert_eq!(orig.value, "before");
    }

    #[cfg(feature = "json")]
    #[test]
    fn set_message_round_trips_json_format() {
        let bytes = encode_json(&StringValue {
            value: "before".into(),
            ..Default::default()
        })
        .unwrap();
        let mut p = Payload::new(bytes, CodecFormat::Json);
        p.set_message(StringValue {
            value: "after".into(),
            ..Default::default()
        });
        // encoded() re-encodes in the original (JSON) format.
        let encoded = p.encoded().unwrap();
        let rt: StringValue = decode_json(&encoded).unwrap();
        assert_eq!(rt.value, "after");
    }

    #[test]
    fn encoded_without_replacement_returns_original() {
        let p = proto_payload("x");
        // Same backing storage — refcounted Bytes clone, not a copy.
        assert!(std::ptr::eq(
            p.encoded().unwrap().as_ptr(),
            p.bytes().as_ptr()
        ));
    }

    /// `from_message` is the lazily-encoded constructor: no wire bytes,
    /// typed access is a downcast, `encoded()` produces the wire form.
    #[test]
    fn from_message_is_lazily_encoded() {
        let p = Payload::from_message(StringValue::from("typed"), CodecFormat::Proto);
        // The documented trap: `bytes()` is what was *received* (nothing);
        // `encoded()` is what will be *sent*.
        assert!(
            p.bytes().is_empty(),
            "no peer bytes for a from_message payload"
        );
        assert!(
            !p.encoded().unwrap().is_empty(),
            "encoded() is the real body"
        );
        let dbg = format!("{p:?}");
        assert!(
            dbg.contains("wire_len: 0") && dbg.contains("replaced: true"),
            "{dbg}"
        );
        assert_eq!(p.format(), CodecFormat::Proto);
        // Typed read is a downcast of the stored message, not a decode of
        // the (empty) bytes — a decode of `b""` would yield the default "".
        let m: &StringValue = p.message().unwrap();
        assert_eq!(m.value, "typed");
        // View path encodes then views.
        assert_eq!(
            p.view::<StringValueView>().unwrap().reborrow().value,
            "typed"
        );
        // Wire form round-trips.
        let rt: StringValue = crate::codec::decode_proto(&p.encoded().unwrap()).unwrap();
        assert_eq!(rt.value, "typed");
        // take_message moves the stored message out without decoding.
        let owned: StringValue = p.take_message().unwrap();
        assert_eq!(owned.value, "typed");
    }

    #[cfg(feature = "json")]
    #[test]
    fn from_message_encodes_in_requested_format() {
        let p = Payload::from_message(StringValue::from("j"), CodecFormat::Json);
        let rt: StringValue = decode_json(&p.encoded().unwrap()).unwrap();
        assert_eq!(rt.value, "j");
    }

    /// The dispatch path answers in the format the call negotiated, not the
    /// one the interceptor happened to build the payload with.
    #[cfg(feature = "json")]
    #[test]
    fn encoded_as_transcodes_a_message_payload() {
        let p = Payload::from_message(StringValue::from("x"), CodecFormat::Proto);
        let json = p.encoded_as(CodecFormat::Json).unwrap();
        let rt: StringValue = decode_json(&json).unwrap();
        assert_eq!(rt.value, "x");
        // Same-format requests still hit the memo.
        let a = p.encoded_as(CodecFormat::Proto).unwrap();
        let b = p.encoded().unwrap();
        assert!(std::ptr::eq(a.as_ptr(), b.as_ptr()));
    }

    #[test]
    fn encoded_as_rejects_wire_bytes_in_another_format() {
        let p = proto_payload("x");
        let err = p.encoded_as(CodecFormat::Json).unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::Internal, "{err:?}");
        assert!(
            err.message.as_deref().unwrap_or("").contains("negotiated"),
            "{err:?}"
        );
    }

    #[test]
    fn from_message_wrong_type_is_internal_error() {
        let p = Payload::from_message(StringValue::from("s"), CodecFormat::Proto);
        let err = p
            .message::<buffa_types::google::protobuf::Int64Value>()
            .unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::Internal, "{err:?}");
    }

    /// `try_clone` of a wire payload shares the backing bytes and starts
    /// with an empty cache; `try_clone` after a replacement bakes the
    /// replacement into the copy's bytes.
    #[test]
    fn try_clone_copies_body_not_cache() {
        let p = proto_payload("orig");
        let _ = p.message::<StringValue>().unwrap(); // populate cache
        let c = p.try_clone().unwrap();
        assert!(
            std::ptr::eq(c.bytes().as_ptr(), p.bytes().as_ptr()),
            "no replacement: clone is a Bytes refcount bump"
        );
        assert!(
            c.decoded.get().is_none(),
            "clone starts with an empty cache"
        );
        assert_eq!(c.message::<StringValue>().unwrap().value, "orig");

        let mut replaced = proto_payload("orig");
        replaced.set_message(StringValue::from("new"));
        let c = replaced.try_clone().unwrap();
        assert!(c.replaced.is_none(), "replacement is baked into bytes");
        let rt: StringValue = crate::codec::decode_proto(c.bytes()).unwrap();
        assert_eq!(rt.value, "new");
    }

    /// A replacement (or `from_message` body) is encoded at most once:
    /// `encoded()`, `try_clone()`, and proto `view()` all serve the memo,
    /// and `set_message` invalidates it.
    #[test]
    fn replacement_encode_is_memoized_and_invalidated() {
        let mut p = Payload::from_message(StringValue::from("once"), CodecFormat::Proto);
        let first = p.encoded().unwrap();
        let again = p.encoded().unwrap();
        assert!(
            std::ptr::eq(first.as_ptr(), again.as_ptr()),
            "second encoded() must be a refcount bump, not a re-encode"
        );
        let clone = p.try_clone().unwrap();
        assert!(
            std::ptr::eq(clone.bytes().as_ptr(), first.as_ptr()),
            "try_clone shares the memoized encode"
        );
        // Proto view reuses the memo too (its bytes borrow from it).
        let v = p.view::<StringValueView>().unwrap();
        let ptr = v.reborrow().value.as_ptr() as usize;
        let range = first.as_ptr() as usize..first.as_ptr() as usize + first.len();
        assert!(
            range.contains(&ptr),
            "view should borrow the memoized bytes"
        );

        p.set_message(StringValue::from("twice"));
        let rt: StringValue = crate::codec::decode_proto(&p.encoded().unwrap()).unwrap();
        assert_eq!(rt.value, "twice", "set_message must invalidate the memo");
    }

    /// `try_clone` keeps the decode options (the service's limits) rather
    /// than resetting to buffa defaults. `DecodeOptions` has no `PartialEq`,
    /// so compare a distinctive `Debug` rendering.
    #[test]
    fn try_clone_preserves_decode_options() {
        let opts = buffa::DecodeOptions::new().with_recursion_limit(7);
        let p = proto_payload("x").with_decode_options(opts.clone());
        let c = p.try_clone().unwrap();
        assert_eq!(format!("{:?}", c.decode_options()), format!("{:?}", opts));
        assert_ne!(
            format!("{:?}", c.decode_options()),
            format!("{:?}", buffa::DecodeOptions::new()),
            "fixture must differ from the default for this test to mean anything"
        );
    }

    /// The typed-API divergence `try_clone` documents for JSON payloads:
    /// the original (holding a replacement) can `view()`; the copy (holding
    /// JSON bytes) cannot, but is identical on the wire.
    #[cfg(feature = "json")]
    #[test]
    fn try_clone_json_replacement_is_wire_equal_not_api_equal() {
        let mut p = Payload::new(
            encode_json(&StringValue::from("before")).unwrap(),
            CodecFormat::Json,
        );
        p.set_message(StringValue::from("after"));
        assert!(
            p.view::<StringValueView>().is_ok(),
            "original: replacement can be viewed"
        );
        let c = p.try_clone().unwrap();
        assert_eq!(c.encoded().unwrap(), p.encoded().unwrap(), "wire-equal");
        assert_eq!(c.format(), CodecFormat::Json);
        assert!(
            c.view::<StringValueView>().is_err(),
            "copy: JSON bytes cannot back a view"
        );
        assert_eq!(c.message::<StringValue>().unwrap().value, "after");
    }

    #[test]
    fn message_wrong_type_errors() {
        use buffa_types::google::protobuf::Int32Value;
        let p = proto_payload("x");
        // Cache as StringValue.
        let _: &StringValue = p.message().unwrap();
        // Now ask for a different type — downcast fails. The error names
        // both types so the bug is locatable.
        let err = p.message::<Int32Value>().unwrap_err();
        let msg = err.message.as_deref().unwrap_or_default();
        assert!(msg.contains("previously decoded as a"), "{err:?}");
        assert!(msg.contains("StringValue"), "{err:?}");
        assert!(msg.contains("Int32Value"), "{err:?}");
    }

    #[test]
    fn message_decode_error_is_invalid_argument() {
        use crate::ErrorCode;
        // Bytes that cannot decode as a StringValue — peer-supplied data,
        // so the error code blames the peer, not the server.
        let p = Payload::new(Bytes::from_static(&[0xff, 0xff, 0xff]), CodecFormat::Proto);
        let err = p.message::<StringValue>().unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument, "{err:?}");
    }

    #[cfg(not(feature = "json"))]
    #[test]
    fn message_json_format_is_unimplemented_without_feature() {
        // A JSON-format payload can't be decoded in a proto-only build; the
        // codec reports `Unimplemented` rather than attempting serde.
        let p = Payload::new(Bytes::from_static(b"{}"), CodecFormat::Json);
        assert_eq!(
            p.message::<StringValue>().unwrap_err().code,
            crate::ErrorCode::Unimplemented
        );
        // Proto-format payloads still decode normally.
        assert!(proto_payload("ok").message::<StringValue>().is_ok());
    }

    #[cfg(not(feature = "json"))]
    #[test]
    fn take_message_json_format_is_unimplemented_without_feature() {
        let p = Payload::new(Bytes::from_static(b"{}"), CodecFormat::Json);
        assert_eq!(
            p.take_message::<StringValue>().unwrap_err().code,
            crate::ErrorCode::Unimplemented
        );
        assert!(proto_payload("ok").take_message::<StringValue>().is_ok());
    }

    #[test]
    fn message_replacement_wrong_type_errors() {
        use buffa_types::google::protobuf::Int32Value;
        let mut p = proto_payload("x");
        p.set_message(Int32Value {
            value: 7,
            ..Default::default()
        });
        // The replacement path has a distinct error message from the
        // post-decode wrong-type path covered by message_wrong_type_errors.
        let err = p.message::<StringValue>().unwrap_err();
        let msg = err.message.as_deref().unwrap_or_default();
        assert!(msg.contains("replacement is a"), "{err:?}");
        assert!(msg.contains("Int32Value"), "{err:?}");
        assert!(msg.contains("StringValue"), "{err:?}");
    }

    #[cfg(feature = "json")]
    #[test]
    fn view_replaced_json_format_payload() {
        // A replacement is always re-encoded to proto for view(), so a
        // JSON-format payload with a replacement still views successfully.
        let bytes = encode_json(&StringValue::from("before")).unwrap();
        let mut p = Payload::new(bytes, CodecFormat::Json);
        p.set_message(StringValue::from("after"));
        let v = p.view::<StringValueView>().unwrap();
        assert_eq!(v.reborrow().value, "after");
    }

    #[test]
    fn set_message_twice_supersedes() {
        let mut p = proto_payload("original");
        p.set_message(StringValue::from("first"));
        p.set_message(StringValue::from("second"));
        let m: &StringValue = p.message().unwrap();
        assert_eq!(m.value, "second");
    }

    #[test]
    fn take_message_decodes_fresh_when_no_cache() {
        let p = proto_payload("fresh");
        let m: StringValue = p.take_message().unwrap();
        assert_eq!(m.value, "fresh");
    }

    #[test]
    fn take_message_reuses_cache() {
        let p = proto_payload("cached");
        // Populate the cache (an interceptor would do this).
        let _ = p.message::<StringValue>().unwrap();
        // `take_message` reads the cache, not the wire bytes. The
        // no-second-decode property has no observable proof in safe Rust
        // (the cached value and a fresh decode are bitwise identical), so
        // it's pinned indirectly by `take_message_returns_replacement` —
        // if `take_message` decoded the bytes there, it would never see
        // the replacement. The wrong-type test below pins the other
        // direction (the cache, not the bytes, is the source of truth).
        let m: StringValue = p.take_message().unwrap();
        assert_eq!(m.value, "cached");
    }

    #[test]
    fn take_message_returns_replacement() {
        // Build a payload whose wire bytes are *garbage* — not a valid
        // proto. If `take_message` decoded the bytes instead of moving
        // the replacement out, this would error.
        let mut p = Payload::new(Bytes::from_static(&[0xff, 0xff, 0xff]), CodecFormat::Proto);
        p.set_message(StringValue::from("replaced"));
        let m: StringValue = p.take_message().unwrap();
        assert_eq!(m.value, "replaced");
    }

    #[test]
    fn take_message_wrong_cached_type_errors() {
        use buffa_types::google::protobuf::Int32Value;
        let p = proto_payload("x");
        // An interceptor cached the wrong type for this route. Before
        // `take_message`, the handler would silently re-decode and never
        // notice. Now the bug is loud.
        let _: &StringValue = p.message().unwrap();
        let err = p.take_message::<Int32Value>().unwrap_err();
        let msg = err.message.as_deref().unwrap_or_default();
        assert!(msg.contains("previously decoded as a"), "{err:?}");
        assert!(msg.contains("StringValue"), "{err:?}");
        assert!(msg.contains("Int32Value"), "{err:?}");
    }

    #[test]
    fn take_message_wrong_replacement_type_errors() {
        use buffa_types::google::protobuf::Int32Value;
        let mut p = proto_payload("x");
        p.set_message(Int32Value {
            value: 7,
            ..Default::default()
        });
        let err = p.take_message::<StringValue>().unwrap_err();
        let msg = err.message.as_deref().unwrap_or_default();
        assert!(msg.contains("replacement is a"), "{err:?}");
    }

    #[test]
    fn take_message_decode_error_is_invalid_argument() {
        use crate::ErrorCode;
        let p = Payload::new(Bytes::from_static(&[0xff, 0xff, 0xff]), CodecFormat::Proto);
        let err = p.take_message::<StringValue>().unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument, "{err:?}");
    }

    #[test]
    fn payload_debug_redacts_body() {
        let p = proto_payload("secret");
        let dbg = format!("{p:?}");
        assert!(!dbg.contains("secret"), "Debug must not leak body: {dbg}");
        assert!(dbg.contains("Proto"), "{dbg}");
    }

    /// `Payload` and `Box<dyn AnyMessage>` cross task boundaries inside
    /// the dispatch path; assert the auto-trait bounds hold.
    #[test]
    fn payload_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Payload>();
        assert_send_sync::<Box<dyn AnyMessage>>();
    }

    /// Hammer the `OnceLock` probe-then-set race from multiple threads.
    /// Same `M` everywhere: every thread must succeed and all returned
    /// borrows must alias the same cache slot.
    #[test]
    fn message_concurrent_same_type() {
        let p = proto_payload("race");
        std::thread::scope(|s| {
            let handles: Vec<_> = (0..16)
                .map(|_| {
                    let p = &p;
                    // Return the cache slot's address as an integer so the
                    // closure stays `Send` (raw pointers are not).
                    s.spawn(move || p.message::<StringValue>().unwrap() as *const _ as usize)
                })
                .collect();
            let addrs: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            assert!(
                addrs.iter().all(|&a| a == addrs[0]),
                "all callers should observe the same cached value"
            );
        });
    }
}
