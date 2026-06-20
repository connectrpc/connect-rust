//! Owned per-item wrapper for inbound streaming messages: [`StreamMessage`].
//!
//! Items on a streaming RPC arrive over time, so unlike the unary
//! [`ServiceRequest`](crate::ServiceRequest) they cannot borrow from a buffer
//! owned by the dispatch glue — each item must own its bytes. `StreamMessage`
//! is that owner: one received message on a streaming RPC, holding its
//! decoded zero-copy view together with the buffer it borrows from.

use buffa::view::{OwnedView, ViewReborrow};
use bytes::Bytes;

use crate::codec::CodecFormat;
use crate::codec::JsonSerialize;
use crate::codec::encode_json;
use crate::error::ConnectError;
use crate::request::HasMessageView;
use crate::response::Encodable;

/// One received message on a streaming RPC, owning its decoded buffer.
///
/// `StreamMessage` dereferences to the buffa-generated `FooOwnedView`
/// wrapper, so message fields are read zero-copy through accessor methods
/// (`msg.name()`, `msg.id()`); [`view()`](Self::view) gives the full view for
/// struct patterns and iteration, and
/// [`to_owned_message()`](Self::to_owned_message) converts for data that must
/// be mutated or stored. Received items can also be forwarded as-is —
/// `StreamMessage<M>` implements [`Encodable<M>`], so an echo/relay handler
/// can yield them directly without re-encoding (the retained wire bytes are
/// reused on the proto path).
///
/// The wrapper is `Send + Sync + 'static`, so items can be moved into
/// spawned tasks or buffered freely.
///
/// # Field-name collisions
///
/// `StreamMessage`'s own methods (`view`, `to_owned_message`, `bytes`) and
/// the wrapper's reserved methods take precedence over generated field
/// accessors. A proto field with one of those names has no accessor (buffa
/// emits a build warning for it); read it through the view instead:
/// `msg.view().bytes`.
pub struct StreamMessage<M: HasMessageView> {
    inner: M::ViewHandle,
}

impl<M: HasMessageView> StreamMessage<M> {
    /// Wrap an already-decoded [`OwnedView`].
    ///
    /// Called by the generated dispatch glue; not normally used directly.
    #[doc(hidden)]
    pub fn from_owned_view(inner: OwnedView<M::View<'static>>) -> Self {
        Self {
            inner: M::ViewHandle::from(inner),
        }
    }

    /// The zero-copy view of this message, borrowed from the retained buffer.
    #[must_use]
    pub fn view<'b>(&'b self) -> &'b M::View<'b>
    where
        M::View<'static>: ViewReborrow<Reborrowed<'b> = M::View<'b>>,
    {
        self.inner.as_ref().reborrow()
    }

    /// Convert to the owned message.
    ///
    /// `bytes` fields are sliced zero-copy out of the retained buffer where
    /// possible; string and repeated fields are allocated.
    #[must_use]
    pub fn to_owned_message(&self) -> M {
        self.inner.as_ref().to_owned_message()
    }

    /// The message's protobuf wire bytes.
    ///
    /// For JSON-encoded streams this is the message re-encoded to protobuf
    /// (the buffer the view borrows from), not the original JSON text.
    #[must_use]
    pub fn bytes(&self) -> &Bytes {
        self.inner.as_ref().bytes()
    }
}

/// Per-field accessor methods (`msg.name()`, `msg.id()`, …) come from the
/// buffa-generated `FooOwnedView` wrapper via `Deref`.
impl<M: HasMessageView> core::ops::Deref for StreamMessage<M> {
    type Target = M::ViewHandle;

    fn deref(&self) -> &M::ViewHandle {
        &self.inner
    }
}

impl<M: HasMessageView> Clone for StreamMessage<M>
where
    M::ViewHandle: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<M: HasMessageView> core::fmt::Debug for StreamMessage<M>
where
    M::ViewHandle: core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.inner.fmt(f)
    }
}

/// Forward a received message without re-encoding.
///
/// The proto path reuses the retained wire bytes (a cheap `Bytes` clone); the
/// JSON path converts to the owned message and serializes it, matching the
/// owned-message [`Encodable`] impl.
///
/// # Codec asymmetry
///
/// The two codecs are deliberately not byte-for-byte symmetric. On the proto
/// path the *original* wire bytes are forwarded, so unknown fields and any
/// non-canonical encoding the peer produced are preserved. On the JSON path
/// the message is re-serialized from the decoded form, so unknown fields are
/// dropped and the output is canonical — the original JSON text is not
/// retained after decoding (keeping it would mean buffering every inbound
/// message twice), so byte-preserving JSON forwarding is not possible.
/// Handlers that need exact relay semantics for both codecs should forward at
/// the byte/HTTP layer instead.
impl<M> Encodable<M> for StreamMessage<M>
where
    M: HasMessageView + JsonSerialize,
{
    fn encode(&self, codec: CodecFormat) -> Result<Bytes, ConnectError> {
        match codec {
            CodecFormat::Proto => Ok(self.inner.as_ref().bytes().clone()),
            CodecFormat::Json => encode_json(&self.to_owned_message()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffa::Message;
    use buffa_types::google::protobuf::StringValue;

    fn message(value: &str) -> StreamMessage<StringValue> {
        let bytes = Bytes::from(
            StringValue {
                value: value.into(),
                ..Default::default()
            }
            .encode_to_vec(),
        );
        StreamMessage::from_owned_view(OwnedView::decode(bytes).expect("decode"))
    }

    #[test]
    fn view_to_owned_and_bytes() {
        let msg = message("streamed");
        assert_eq!(msg.view().value, "streamed");
        assert_eq!(msg.to_owned_message().value, "streamed");

        // The view borrows from the retained buffer, not a copy.
        let range = msg.bytes().as_ptr_range();
        assert!(range.contains(&msg.view().value.as_ptr()));

        // Clone + Debug forward to the inner view.
        let cloned = msg.clone();
        assert_eq!(format!("{msg:?}"), format!("{cloned:?}"));
    }

    #[cfg(feature = "json")]
    #[test]
    fn encodable_forwards_proto_bytes_without_reencoding() {
        let msg = message("forward me");
        let original = msg.bytes().clone();

        let proto = msg.encode(CodecFormat::Proto).expect("proto encode");
        assert_eq!(proto, original);
        // Zero re-encode: same backing allocation, not just equal contents.
        assert_eq!(proto.as_ptr(), original.as_ptr());

        // JSON matches what the owned message would produce.
        let json = msg.encode(CodecFormat::Json).expect("json encode");
        let owned_json = serde_json::to_vec(&msg.to_owned_message()).unwrap();
        assert_eq!(json.as_ref(), owned_json.as_slice());
    }

    #[cfg(not(feature = "json"))]
    #[test]
    fn encode_json_is_unimplemented_without_feature() {
        let msg = message("forward me");
        // Proto forwarding still works in a proto-only build...
        assert!(msg.encode(CodecFormat::Proto).is_ok());
        // ...the JSON arm is compiled out and reports `Unimplemented`.
        let err = msg.encode(CodecFormat::Json).unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::Unimplemented);
    }
}
