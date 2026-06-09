//! Borrowed request wrapper for single-request RPCs: [`ServiceRequest`].
//!
//! Unary and server-streaming handler methods receive a
//! [`ServiceRequest<'_, Req>`] — a zero-copy view of the request message plus
//! the raw request body, both borrowed from buffers that the generated
//! dispatch glue owns for the duration of the call. The name mirrors
//! [`ServiceResult`](crate::response::ServiceResult) on the response side.
//!
//! Not to be confused with the interceptor-facing request types
//! (`UnaryRequest` / `StreamRequest`), which wrap the *wire-level* request a
//! middleware sees; `ServiceRequest` is what a service trait implementation
//! receives after decoding.

use buffa::view::MessageView;
use buffa::view::OwnedView;
use bytes::Bytes;

/// Re-export of buffa's view-family trait.
///
/// Generated buffa code implements this for every message (when views are
/// generated), linking the owned type to its borrowed view (`Req::View<'a>`)
/// and its `'static` handle (`Req::ViewHandle`, the `FooOwnedView` wrapper).
/// [`ServiceRequest<'_, Req>`] and
/// [`StreamMessage<Req>`](crate::StreamMessage) are bounded on it.
///
/// The per-field accessor methods (`msg.name()`, …) are inherent on the
/// concrete generated wrapper and on the view's public fields; code that is
/// generic over `M: HasMessageView` reaches the message through
/// `as_ref()`/[`view`](crate::StreamMessage::view) /
/// [`to_owned_message`](crate::StreamMessage::to_owned_message) instead of
/// named fields.
pub use buffa::HasMessageView;

/// A borrowed single-message RPC request (unary and server-streaming): a
/// zero-copy view of `Req` plus the raw request body, valid for the duration
/// of the handler call.
///
/// `ServiceRequest` dereferences to the request's view type, so message
/// fields are read directly (`request.name`, `request.id`) with their
/// borrows tied to the call frame — they cannot outlive the request body.
/// Anything that must outlive the call (`tokio::spawn`, channels, server
/// state) takes owned data: call [`to_owned_message`](Self::to_owned_message)
/// (or copy the specific fields) first and move the owned value instead.
///
/// The wrapper is `Copy` (it is two references), so it can be passed to
/// helper functions freely without `&` ceremony.
pub struct ServiceRequest<'a, Req: HasMessageView> {
    view: &'a Req::View<'a>,
    body: &'a Bytes,
}

impl<'a, Req: HasMessageView> ServiceRequest<'a, Req> {
    /// Assemble a request from a decoded view and the buffer it borrows from.
    ///
    /// Called by the generated dispatch glue; not normally used directly.
    /// `view` should have been decoded from `body`: that is what makes
    /// [`to_owned_message`](Self::to_owned_message)'s zero-copy `bytes`-field
    /// path apply. If the buffers don't match, conversion still succeeds —
    /// `bytes` fields are copied instead of sliced.
    #[doc(hidden)]
    pub fn from_parts(view: &'a Req::View<'a>, body: &'a Bytes) -> Self {
        Self { view, body }
    }

    /// The zero-copy view of the request message.
    ///
    /// Equivalent to the `Deref` target; useful when a `&FooRequestView<'_>`
    /// is needed explicitly (struct patterns, lifetime-parameterised helpers).
    #[must_use]
    pub fn view(&self) -> &'a Req::View<'a> {
        self.view
    }

    /// Convert to the owned request message.
    ///
    /// `bytes` fields are sliced zero-copy out of the retained body where
    /// possible; string and repeated fields are allocated. Use this for data
    /// that must outlive the handler call.
    #[must_use]
    pub fn to_owned_message(&self) -> Req {
        self.view.to_owned_from_source(Some(self.body))
    }

    /// The request body as protobuf wire bytes.
    ///
    /// For JSON-encoded requests this is the body re-encoded to protobuf
    /// (the same buffer the view borrows from), not the original JSON text.
    #[must_use]
    pub fn bytes(&self) -> &'a Bytes {
        self.body
    }

    /// Rebuild a `'static` owned view of the request from the retained body.
    ///
    /// Zero-copy despite the `to_owned_` name: a `Bytes` refcount bump plus
    /// a decode walk, with no per-field allocation. Use this to return the
    /// request as a response body (e.g.
    /// `MaybeBorrowed::Borrowed(req.to_owned_view())` in a pass-through
    /// handler) — the response must be `'static`, so the borrowed view
    /// itself cannot be returned.
    ///
    /// View response bodies encode for the proto codec only; JSON clients
    /// receive `Unimplemented`. See
    /// [`MaybeBorrowed`](crate::MaybeBorrowed)'s codec note.
    ///
    /// # Panics
    ///
    /// Panics if the body is not a valid encoding of `Req`. This cannot
    /// happen for requests built by the generated dispatch glue, which
    /// decodes the view from exactly these bytes before constructing the
    /// `ServiceRequest`.
    #[must_use]
    pub fn to_owned_view(&self) -> OwnedView<Req::View<'static>> {
        OwnedView::decode(self.body.clone())
            .expect("ServiceRequest body was already view-decoded by the dispatch glue")
    }
}

// Manual `Clone`/`Copy`: a derive would bound `Req` itself rather than the
// stored references, and `ServiceRequest` is two references regardless of `Req`.
impl<Req: HasMessageView> Clone for ServiceRequest<'_, Req> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<Req: HasMessageView> Copy for ServiceRequest<'_, Req> {}

impl<'a, Req: HasMessageView> core::ops::Deref for ServiceRequest<'a, Req> {
    type Target = Req::View<'a>;

    fn deref(&self) -> &Req::View<'a> {
        self.view
    }
}

impl<'a, Req: HasMessageView> core::fmt::Debug for ServiceRequest<'a, Req>
where
    Req::View<'a>: core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.view.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffa::Message;
    use buffa_types::google::protobuf::__buffa::view::StringValueView;
    use buffa_types::google::protobuf::StringValue;

    // `StringValue: HasMessageView` comes from buffa-types' generated code —
    // no local glue impls are needed.

    fn encode(value: &str) -> Bytes {
        Bytes::from(
            StringValue {
                value: value.into(),
                ..Default::default()
            }
            .encode_to_vec(),
        )
    }

    #[test]
    fn deref_field_access_and_view() {
        let body = encode("zero-copy");
        let view = StringValueView::decode_view(&body).unwrap();
        let req = ServiceRequest::<StringValue>::from_parts(&view, &body);

        // Field access through Deref, plus the explicit view() escape hatch.
        assert_eq!(req.value, "zero-copy");
        assert_eq!(req.view().value, "zero-copy");

        // The borrow points into the request body, not a copy.
        let range = body.as_ptr_range();
        assert!(range.contains(&req.value.as_ptr()));
    }

    #[test]
    fn to_owned_message_and_bytes() {
        let body = encode("keep me");
        let view = StringValueView::decode_view(&body).unwrap();
        let req = ServiceRequest::<StringValue>::from_parts(&view, &body);

        let owned: StringValue = req.to_owned_message();
        assert_eq!(owned.value, "keep me");
        assert_eq!(req.bytes().as_ref(), body.as_ref());

        // to_owned_view rebuilds a 'static view backed by the same buffer.
        let owned_view = req.to_owned_view();
        assert_eq!(owned_view.reborrow().value, "keep me");
        let range = body.as_ptr_range();
        assert!(range.contains(&owned_view.reborrow().value.as_ptr()));

        // Copy semantics: passing by value doesn't consume the original.
        let copy = req;
        assert_eq!(copy.value, req.value);
        assert_eq!(format!("{req:?}"), format!("{copy:?}"));
    }
}
