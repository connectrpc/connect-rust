//! Handler traits for implementing RPC methods.
//!
//! This module defines the traits that RPC method implementations must
//! satisfy. Generated `FooService` traits are the primary surface; these
//! lower-level traits are the building blocks that generated
//! `<Service>Ext::register` wires into a [`Router`](crate::Router).
//!
//! Handlers receive a read-only [`RequestContext`] and return a
//! [`Response<B>`](crate::Response) carrying the body plus any response
//! headers/trailers/compression hint. See [`crate::response`] for the
//! type definitions.
//!
//! # Why response metadata lives on `Response<B>`
//!
//! The earlier `Context` design conflated request-side reads
//! (`headers`, `deadline`, `extensions`) with response-side writes
//! (`response_headers`, `trailers`, `compress_response`) on one struct
//! that the handler took ownership of and threaded back. Splitting it
//! gives a clean in/out separation: handlers that don't touch response
//! metadata bind `_ctx` and return `Ok(body.into())` with no `mut`
//! ceremony, while handlers that do attach metadata get a fluent
//! builder (`Response::new(body).with_header(..).with_trailer(..)`)
//! instead of field-mutation followed by `Ok((body, ctx))`.

use std::pin::Pin;
use std::sync::Arc;

use buffa::Message;
use buffa::view::MessageView;
use buffa::view::OwnedView;
use bytes::Bytes;
use futures::Stream;

use crate::codec::CodecFormat;
use crate::codec::decode_json;
use crate::codec::{JsonDeserialize, JsonSerialize};
use crate::error::ConnectError;
use crate::response::{
    Encodable, EncodedResponse, RequestContext, Response, ServiceResult, ServiceStream,
};

/// Report a failed request decode as `invalid_argument`.
///
/// Exceeding the element-memory budget is the one decode failure a server
/// operator can fix without the peer changing anything, so it says which
/// limit to raise. Every other variant is a malformed request, where naming
/// a limit would misdirect. `DecodeError` is `#[non_exhaustive]`, hence the
/// catch-all arm.
fn decode_request_error(e: &buffa::DecodeError) -> ConnectError {
    match e {
        buffa::DecodeError::ElementMemoryLimitExceeded => ConnectError::invalid_argument(format!(
            "failed to decode proto request: {e}; if this peer is trusted, \
             raise Limits::element_memory_limit"
        )),
        _ => ConnectError::invalid_argument(format!("failed to decode proto request: {e}")),
    }
}

/// Decode a request message from bytes using the specified codec format.
pub(crate) fn decode_request<Req>(
    request: &Bytes,
    format: CodecFormat,
    options: &buffa::DecodeOptions,
) -> Result<Req, ConnectError>
where
    Req: Message + JsonDeserialize,
{
    match format {
        CodecFormat::Proto => options
            .decode_from_slice(&request[..])
            .map_err(|e| decode_request_error(&e)),
        CodecFormat::Json => decode_json(&request[..]),
    }
}

/// Type alias for a boxed future used in handlers.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Type alias for a boxed stream of encoded response bytes.
pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

/// Map a stream of typed responses through [`Encodable`].
///
/// `B` is any [`Encodable<Res>`] — typically `Res` itself, but may be
/// [`PreEncoded`](crate::PreEncoded) or [`MaybeBorrowed`](crate::MaybeBorrowed)
/// for handlers that encode borrowing views per item.
///
/// Thin re-export wrapper so the four `*StreamingHandlerWrapper`
/// `call_erased` impls below don't have to spell out the
/// `dispatcher::codegen` path; the implementation is shared with the
/// codegen-emitted dispatcher arms (see
/// [`encode_response_stream`](crate::dispatcher::codegen::encode_response_stream)).
fn encode_body_stream<Res, B, S>(
    stream: S,
    format: CodecFormat,
) -> BoxStream<Result<Bytes, ConnectError>>
where
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
    S: Stream<Item = Result<B, ConnectError>> + Send + 'static,
{
    crate::dispatcher::codegen::encode_response_stream::<Res, B, S>(stream, format)
}

// ============================================================================
// Type-erased handler boundaries (Router → service.rs)
// ============================================================================

/// Type-erased unary handler for use in the router.
pub(crate) trait ErasedHandler: Send + Sync {
    /// Handle a request, decoding the [`Payload`] to the concrete request
    /// type. Owned-message handlers should call [`Payload::take_message`]
    /// to reuse a decode an interceptor may already have cached; view
    /// handlers should call [`Payload::encoded`] for the wire bytes.
    fn call_erased(
        &self,
        ctx: RequestContext,
        request: crate::Payload,
        format: CodecFormat,
    ) -> BoxFuture<'static, Result<EncodedResponse, ConnectError>>;

    /// Check if this is a streaming handler.
    #[allow(dead_code)]
    fn is_streaming(&self) -> bool;
}

/// Result type for erased streaming handlers.
pub(crate) type StreamingHandlerResult =
    BoxFuture<'static, Result<Response<BoxStream<Result<Bytes, ConnectError>>>, ConnectError>>;

/// Type-erased server-streaming handler for use in the router.
pub(crate) trait ErasedStreamingHandler: Send + Sync {
    /// Handle a streaming request with raw bytes and specified codec format.
    fn call_erased(
        &self,
        ctx: RequestContext,
        request: Bytes,
        format: CodecFormat,
    ) -> StreamingHandlerResult;
}

/// Type-erased client-streaming handler for use in the router.
pub(crate) trait ErasedClientStreamingHandler: Send + Sync {
    /// Handle a client streaming request with a stream of raw message bytes.
    fn call_erased(
        &self,
        ctx: RequestContext,
        requests: BoxStream<Result<Bytes, ConnectError>>,
        format: CodecFormat,
    ) -> BoxFuture<'static, Result<EncodedResponse, ConnectError>>;
}

/// Type-erased bidi-streaming handler for use in the router.
pub(crate) trait ErasedBidiStreamingHandler: Send + Sync {
    /// Handle a bidi streaming request with a stream of raw message bytes.
    fn call_erased(
        &self,
        ctx: RequestContext,
        requests: BoxStream<Result<Bytes, ConnectError>>,
        format: CodecFormat,
    ) -> StreamingHandlerResult;
}

// ============================================================================
// Unary handler (owned request)
// ============================================================================

/// Trait for unary RPC handlers (owned request type).
///
/// Handlers return a [`Response<Self::Body>`](crate::Response) where
/// `Body` is any type [`Encodable`] as `Res` — typically `Res` itself.
/// The happy path is `Ok(res.into())`.
pub trait Handler<Req, Res>: Send + Sync + 'static
where
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
{
    /// The response body type. Typically `Res`, or any
    /// [`Encodable<Res>`](Encodable) (e.g.
    /// [`MaybeBorrowed`](crate::MaybeBorrowed)).
    type Body: Encodable<Res> + Send + 'static;

    /// Handle a unary RPC request.
    fn call(
        &self,
        ctx: RequestContext,
        request: Req,
    ) -> BoxFuture<'static, ServiceResult<Self::Body>>;
}

/// Wrapper that implements [`Handler`] for async functions.
pub struct FnHandler<F> {
    f: Arc<F>,
}

impl<F> FnHandler<F> {
    /// Create a new function handler.
    pub fn new(f: F) -> Self {
        Self { f: Arc::new(f) }
    }
}

impl<F, Fut, Req, Res, B> Handler<Req, Res> for FnHandler<F>
where
    F: Fn(RequestContext, Req) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<B>> + Send + 'static,
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    type Body = B;

    fn call(&self, ctx: RequestContext, request: Req) -> BoxFuture<'static, ServiceResult<B>> {
        let f = Arc::clone(&self.f);
        Box::pin(async move { f(ctx, request).await })
    }
}

/// Helper function to create a handler from an async function.
pub fn handler_fn<F, Fut, Req, Res, B>(f: F) -> FnHandler<F>
where
    F: Fn(RequestContext, Req) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<B>> + Send + 'static,
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    FnHandler::new(f)
}

/// Wrapper to erase the types from a unary handler.
pub(crate) struct UnaryHandlerWrapper<H, Req, Res>
where
    H: Handler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + JsonSerialize + Send + 'static,
{
    handler: Arc<H>,
    _phantom: std::marker::PhantomData<fn(Req) -> Res>,
}

impl<H, Req, Res> UnaryHandlerWrapper<H, Req, Res>
where
    H: Handler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + JsonSerialize + Send + 'static,
{
    /// Create a new wrapper around the given handler.
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<H, Req, Res> ErasedHandler for UnaryHandlerWrapper<H, Req, Res>
where
    H: Handler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + JsonSerialize + Send + 'static,
{
    fn call_erased(
        &self,
        ctx: RequestContext,
        request: crate::Payload,
        format: CodecFormat,
    ) -> BoxFuture<'static, Result<EncodedResponse, ConnectError>> {
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            // `take_message` reuses an interceptor's decode when one ran
            // and cached this `Req`, instead of decoding the bytes again.
            let req: Req = request.take_message()?;
            handler.call(ctx, req).await?.encode::<Res>(format)
        })
    }

    fn is_streaming(&self) -> bool {
        false
    }
}

// ============================================================================
// Server-streaming handler (owned request)
// ============================================================================

/// Trait for server streaming RPC handlers.
///
/// # Migrating from connectrpc 0.4.x
///
/// `Item` is new in 0.5: a hand-written `impl StreamingHandler` previously
/// returned `ServiceStream<Res>`; add `type Item = Res;` to keep the same
/// behavior. Generated traits and the [`streaming_handler_fn`] helper
/// infer it.
pub trait StreamingHandler<Req, Res>: Send + Sync + 'static
where
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
{
    /// The stream item type. Typically `Res` itself; may be
    /// [`PreEncoded`](crate::PreEncoded) or
    /// [`MaybeBorrowed`](crate::MaybeBorrowed) for handlers that encode
    /// borrowing views per item.
    ///
    /// Items must be `'static` — a stream item cannot borrow `&self` or a
    /// per-call snapshot. To stream view-encoded data, encode each item
    /// inside the stream's body and yield [`PreEncoded`](crate::PreEncoded).
    type Item: Encodable<Res> + Send + 'static;

    /// Handle a server streaming RPC request.
    fn call(
        &self,
        ctx: RequestContext,
        request: Req,
    ) -> BoxFuture<'static, ServiceResult<ServiceStream<Self::Item>>>;
}

/// Wrapper that implements [`StreamingHandler`] for async functions.
pub struct FnStreamingHandler<F> {
    f: Arc<F>,
}

impl<F> FnStreamingHandler<F> {
    /// Create a new function streaming handler.
    pub fn new(f: F) -> Self {
        Self { f: Arc::new(f) }
    }
}

impl<F, Fut, Req, Res, B> StreamingHandler<Req, Res> for FnStreamingHandler<F>
where
    F: Fn(RequestContext, Req) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<ServiceStream<B>>> + Send + 'static,
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    type Item = B;

    fn call(
        &self,
        ctx: RequestContext,
        request: Req,
    ) -> BoxFuture<'static, ServiceResult<ServiceStream<B>>> {
        let f = Arc::clone(&self.f);
        Box::pin(async move { f(ctx, request).await })
    }
}

/// Helper function to create a streaming handler from an async function.
///
/// `Res` is inferred from the stream item type `B` whenever the closure
/// pins `B` to a concrete type — yielding an owned `Res`,
/// [`PreEncoded::from_view(&view)`](crate::PreEncoded::from_view), or
/// [`PreEncoded::<MyResponse>::from_bytes_unchecked(bytes)`](crate::PreEncoded::from_bytes_unchecked)
/// all infer cleanly. Inference only fails when the closure leaves the
/// message type itself open (e.g. `PreEncoded::from_bytes_unchecked(bytes)`
/// with no `::<M>`); the simplest fix is to name `M` at the construction
/// site rather than turbofishing this helper:
///
/// ```rust,ignore
/// // `M` named at the construction site — `Res` is inferred:
/// PreEncoded::<MyResponse>::from_bytes_unchecked(bytes)
/// ```
///
/// Generated server-streaming registrations always pin `Res` because the
/// trait method's stream item is the *opaque* `impl Encodable<Out>`, which
/// can't be unified against the `Encodable<Res>` impls. Hand-written
/// `Router` registrations don't hit this unless they leave the message type
/// open.
pub fn streaming_handler_fn<F, Fut, Req, Res, B>(f: F) -> FnStreamingHandler<F>
where
    F: Fn(RequestContext, Req) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<ServiceStream<B>>> + Send + 'static,
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    FnStreamingHandler::new(f)
}

/// Wrapper to erase the types from a server streaming handler.
pub(crate) struct ServerStreamingHandlerWrapper<H, Req, Res>
where
    H: StreamingHandler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + Send + 'static,
{
    handler: Arc<H>,
    _phantom: std::marker::PhantomData<fn(Req) -> Res>,
}

impl<H, Req, Res> ServerStreamingHandlerWrapper<H, Req, Res>
where
    H: StreamingHandler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + Send + 'static,
{
    /// Create a new wrapper around the given streaming handler.
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<H, Req, Res> ErasedStreamingHandler for ServerStreamingHandlerWrapper<H, Req, Res>
where
    H: StreamingHandler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + Send + 'static,
{
    fn call_erased(
        &self,
        ctx: RequestContext,
        request: Bytes,
        format: CodecFormat,
    ) -> StreamingHandlerResult {
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            let req: Req = decode_request(&request, format, ctx.decode_options())?;
            let resp = handler.call(ctx, req).await?;
            Ok(resp.map_body(|s| encode_body_stream(s, format)))
        })
    }
}

// ============================================================================
// Client-streaming handler (owned request)
// ============================================================================

/// Trait for client streaming RPC handlers.
pub trait ClientStreamingHandler<Req, Res>: Send + Sync + 'static
where
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
{
    /// The response body type. Typically `Res`.
    type Body: Encodable<Res> + Send + 'static;

    /// Handle a client streaming RPC request.
    fn call(
        &self,
        ctx: RequestContext,
        requests: ServiceStream<Req>,
    ) -> BoxFuture<'static, ServiceResult<Self::Body>>;
}

/// Wrapper that implements [`ClientStreamingHandler`] for async functions.
pub struct FnClientStreamingHandler<F> {
    f: Arc<F>,
}

impl<F> FnClientStreamingHandler<F> {
    /// Create a new function client streaming handler.
    pub fn new(f: F) -> Self {
        Self { f: Arc::new(f) }
    }
}

impl<F, Fut, Req, Res, B> ClientStreamingHandler<Req, Res> for FnClientStreamingHandler<F>
where
    F: Fn(RequestContext, ServiceStream<Req>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<B>> + Send + 'static,
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    type Body = B;

    fn call(
        &self,
        ctx: RequestContext,
        requests: ServiceStream<Req>,
    ) -> BoxFuture<'static, ServiceResult<B>> {
        let f = Arc::clone(&self.f);
        Box::pin(async move { f(ctx, requests).await })
    }
}

/// Helper function to create a client streaming handler from an async function.
pub fn client_streaming_handler_fn<F, Fut, Req, Res, B>(f: F) -> FnClientStreamingHandler<F>
where
    F: Fn(RequestContext, ServiceStream<Req>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<B>> + Send + 'static,
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    FnClientStreamingHandler::new(f)
}

/// Wrapper to erase the types from a client streaming handler.
pub(crate) struct ClientStreamingHandlerWrapper<H, Req, Res>
where
    H: ClientStreamingHandler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + JsonSerialize + Send + 'static,
{
    handler: Arc<H>,
    _phantom: std::marker::PhantomData<fn(Req) -> Res>,
}

impl<H, Req, Res> ClientStreamingHandlerWrapper<H, Req, Res>
where
    H: ClientStreamingHandler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + JsonSerialize + Send + 'static,
{
    /// Create a new wrapper around the given client streaming handler.
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<H, Req, Res> ErasedClientStreamingHandler for ClientStreamingHandlerWrapper<H, Req, Res>
where
    H: ClientStreamingHandler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + JsonSerialize + Send + 'static,
{
    fn call_erased(
        &self,
        ctx: RequestContext,
        requests: BoxStream<Result<Bytes, ConnectError>>,
        format: CodecFormat,
    ) -> BoxFuture<'static, Result<EncodedResponse, ConnectError>> {
        use futures::StreamExt as _;
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            // The stream outlives this frame, so it owns its limits rather
            // than borrowing them from `ctx`, which is moved into the call.
            let options = ctx.decode_options().clone();
            let request_stream: ServiceStream<Req> =
                Box::pin(requests.map(move |result| {
                    result.and_then(|raw| decode_request(&raw, format, &options))
                }));
            handler
                .call(ctx, request_stream)
                .await?
                .encode::<Res>(format)
        })
    }
}

// ============================================================================
// Bidi-streaming handler (owned request)
// ============================================================================

/// Trait for bidirectional streaming RPC handlers.
///
/// # Migrating from connectrpc 0.4.x
///
/// `Item` is new in 0.5: hand-written impls add `type Item = Res;`.
/// See [`StreamingHandler`] for details.
pub trait BidiStreamingHandler<Req, Res>: Send + Sync + 'static
where
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
{
    /// The stream item type. Typically `Res` itself; may be
    /// [`PreEncoded`](crate::PreEncoded) or
    /// [`MaybeBorrowed`](crate::MaybeBorrowed) for handlers that encode
    /// borrowing views per item. See [`StreamingHandler::Item`].
    type Item: Encodable<Res> + Send + 'static;

    /// Handle a bidi streaming RPC request.
    fn call(
        &self,
        ctx: RequestContext,
        requests: ServiceStream<Req>,
    ) -> BoxFuture<'static, ServiceResult<ServiceStream<Self::Item>>>;
}

/// Wrapper that implements [`BidiStreamingHandler`] for async functions.
pub struct FnBidiStreamingHandler<F> {
    f: Arc<F>,
}

impl<F> FnBidiStreamingHandler<F> {
    /// Create a new function bidi streaming handler.
    pub fn new(f: F) -> Self {
        Self { f: Arc::new(f) }
    }
}

impl<F, Fut, Req, Res, B> BidiStreamingHandler<Req, Res> for FnBidiStreamingHandler<F>
where
    F: Fn(RequestContext, ServiceStream<Req>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<ServiceStream<B>>> + Send + 'static,
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    type Item = B;

    fn call(
        &self,
        ctx: RequestContext,
        requests: ServiceStream<Req>,
    ) -> BoxFuture<'static, ServiceResult<ServiceStream<B>>> {
        let f = Arc::clone(&self.f);
        Box::pin(async move { f(ctx, requests).await })
    }
}

/// Helper function to create a bidi streaming handler from an async function.
pub fn bidi_streaming_handler_fn<F, Fut, Req, Res, B>(f: F) -> FnBidiStreamingHandler<F>
where
    F: Fn(RequestContext, ServiceStream<Req>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<ServiceStream<B>>> + Send + 'static,
    Req: Message + Send + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    FnBidiStreamingHandler::new(f)
}

/// Wrapper to erase the types from a bidi streaming handler.
pub(crate) struct BidiStreamingHandlerWrapper<H, Req, Res>
where
    H: BidiStreamingHandler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + Send + 'static,
{
    handler: Arc<H>,
    _phantom: std::marker::PhantomData<fn(Req) -> Res>,
}

impl<H, Req, Res> BidiStreamingHandlerWrapper<H, Req, Res>
where
    H: BidiStreamingHandler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + Send + 'static,
{
    /// Create a new wrapper around the given bidi streaming handler.
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<H, Req, Res> ErasedBidiStreamingHandler for BidiStreamingHandlerWrapper<H, Req, Res>
where
    H: BidiStreamingHandler<Req, Res>,
    Req: Message + JsonDeserialize + Send + 'static,
    Res: Message + Send + 'static,
{
    fn call_erased(
        &self,
        ctx: RequestContext,
        requests: BoxStream<Result<Bytes, ConnectError>>,
        format: CodecFormat,
    ) -> StreamingHandlerResult {
        use futures::StreamExt as _;
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            // The stream outlives this frame, so it owns its limits rather
            // than borrowing them from `ctx`, which is moved into the call.
            let options = ctx.decode_options().clone();
            let request_stream: ServiceStream<Req> =
                Box::pin(requests.map(move |result| {
                    result.and_then(|raw| decode_request(&raw, format, &options))
                }));
            let resp = handler.call(ctx, request_stream).await?;
            Ok(resp.map_body(|s| encode_body_stream(s, format)))
        })
    }
}

// ============================================================================
// View-based handlers (zero-copy request views)
// ============================================================================

/// Decode a request as an `OwnedView` from bytes using the specified codec format.
///
/// Normalizes the body to proto wire bytes via [`request_proto_bytes`],
/// then decodes the view over that buffer — a true zero-copy decode for
/// proto-encoded requests. The JSON round-trip adds overhead relative to
/// owned-type decoding, but is negligible compared to JSON parsing itself.
pub(crate) fn decode_request_view<ReqView>(
    request: Bytes,
    format: CodecFormat,
    options: &buffa::DecodeOptions,
) -> Result<OwnedView<ReqView>, ConnectError>
where
    ReqView: MessageView<'static> + Send,
    ReqView::Owned: Message + JsonDeserialize,
{
    let body = request_proto_bytes::<ReqView::Owned>(request, format)?;
    OwnedView::<ReqView>::decode_with_options(body, options).map_err(|e| decode_request_error(&e))
}

/// Normalize a request body to protobuf wire bytes.
///
/// For proto-encoded requests this is a pass-through of the input `Bytes`.
/// For JSON-encoded requests the body is deserialized to the owned message
/// and re-encoded to proto bytes. The returned buffer is what a request
/// view borrows from — in the generated unary dispatch glue the dispatcher
/// keeps it alive for the duration of the handler call, so a scoped view's
/// borrows are tied to the call frame; on the streaming and Router paths it
/// backs an [`OwnedView`].
///
/// # Errors
///
/// Returns `ConnectError::invalid_argument` if the JSON body cannot be
/// deserialized into the request message.
#[doc(hidden)] // exposed only for dispatcher::codegen (generated code)
pub fn request_proto_bytes<Req>(request: Bytes, format: CodecFormat) -> Result<Bytes, ConnectError>
where
    Req: Message + JsonDeserialize,
{
    match format {
        CodecFormat::Proto => Ok(request),
        CodecFormat::Json => {
            let owned: Req = decode_json(&request[..])?;
            Ok(Bytes::from(owned.encode_to_vec()))
        }
    }
}

/// Decode a scoped (borrowed) request view from normalized proto bytes.
///
/// Companion to [`request_proto_bytes`]: the generated dispatch glue
/// keeps the returned view's backing buffer alive across the handler call,
/// so the view's borrows are tied to the call frame rather than promoted to
/// a synthetic `'static`.
///
/// `options` carries the service's configured decode limits; see
/// [`Limits`](crate::Limits).
///
/// # Errors
///
/// Returns `ConnectError::invalid_argument` if the bytes exceed one of
/// `options`' limits, or if the bytes are not a valid
/// encoding of the request message.
#[doc(hidden)] // exposed only for dispatcher::codegen (generated code)
pub fn decode_borrowed_request_view<'a, ReqView>(
    body: &'a [u8],
    options: &buffa::DecodeOptions,
) -> Result<ReqView, ConnectError>
where
    ReqView: MessageView<'a>,
{
    options
        .decode_view(body)
        .map_err(|e| decode_request_error(&e))
}

/// Trait for unary RPC handlers using zero-copy request views.
///
/// `call` returns the response **already encoded** so the body's
/// lifetime can be tied to data the handler borrows from `&self` (or
/// from the request) without surfacing in the trait object boundary.
pub trait ViewHandler<ReqView>: Send + Sync + 'static
where
    ReqView: MessageView<'static> + Send + Sync + 'static,
{
    /// Handle a unary RPC request with a zero-copy view, encoding the
    /// response in `format`.
    fn call(
        &self,
        ctx: RequestContext,
        request: OwnedView<ReqView>,
        format: CodecFormat,
    ) -> BoxFuture<'static, Result<EncodedResponse, ConnectError>>;
}

/// Wrapper that implements [`ViewHandler`] for async functions.
pub struct FnViewHandler<F> {
    f: Arc<F>,
}

impl<F> FnViewHandler<F> {
    /// Create a new function view handler.
    pub fn new(f: F) -> Self {
        Self { f: Arc::new(f) }
    }
}

impl<F, Fut, ReqView> ViewHandler<ReqView> for FnViewHandler<F>
where
    F: Fn(RequestContext, OwnedView<ReqView>, CodecFormat) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<EncodedResponse, ConnectError>> + Send + 'static,
    ReqView: MessageView<'static> + Send + Sync + 'static,
{
    fn call(
        &self,
        ctx: RequestContext,
        request: OwnedView<ReqView>,
        format: CodecFormat,
    ) -> BoxFuture<'static, Result<EncodedResponse, ConnectError>> {
        let f = Arc::clone(&self.f);
        Box::pin(async move { f(ctx, request, format).await })
    }
}

/// Helper function to create a view handler from an async function.
///
/// The closure receives the negotiated [`CodecFormat`] and returns the
/// response **already encoded**, so a body that borrows from `&svc` is
/// encoded before the borrow ends. Generated service registration uses this
/// adapter for unary handlers that operate on borrowed request views.
pub fn view_handler_fn<F, Fut, ReqView>(f: F) -> FnViewHandler<F>
where
    F: Fn(RequestContext, OwnedView<ReqView>, CodecFormat) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<EncodedResponse, ConnectError>> + Send + 'static,
    ReqView: MessageView<'static> + Send + Sync + 'static,
{
    FnViewHandler::new(f)
}

/// Wrapper to erase the types from a unary view handler.
pub(crate) struct UnaryViewHandlerWrapper<H, ReqView>
where
    H: ViewHandler<ReqView>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
{
    handler: Arc<H>,
    _phantom: std::marker::PhantomData<fn(ReqView)>,
}

impl<H, ReqView> UnaryViewHandlerWrapper<H, ReqView>
where
    H: ViewHandler<ReqView>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
{
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<H, ReqView> ErasedHandler for UnaryViewHandlerWrapper<H, ReqView>
where
    H: ViewHandler<ReqView>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
{
    fn call_erased(
        &self,
        ctx: RequestContext,
        request: crate::Payload,
        format: CodecFormat,
    ) -> BoxFuture<'static, Result<EncodedResponse, ConnectError>> {
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            // The cache stores owned messages, not views, so it can't help
            // here. `encoded()` is the wire bytes — a cheap `Bytes` clone
            // unless an interceptor replaced the body, in which case it
            // re-encodes the replacement.
            let req =
                decode_request_view::<ReqView>(request.encoded()?, format, ctx.decode_options())?;
            handler.call(ctx, req, format).await
        })
    }

    fn is_streaming(&self) -> bool {
        false
    }
}

/// Trait for server streaming RPC handlers using zero-copy request views.
pub trait ViewStreamingHandler<ReqView, Res>: Send + Sync + 'static
where
    ReqView: MessageView<'static> + Send + Sync + 'static,
    Res: Message + Send + 'static,
{
    /// The stream item type. Typically `Res` itself; may be
    /// [`PreEncoded`](crate::PreEncoded) or
    /// [`MaybeBorrowed`](crate::MaybeBorrowed) for handlers that encode
    /// borrowing views per item.
    type Item: Encodable<Res> + Send + 'static;

    /// Handle a server streaming RPC request with a zero-copy view.
    fn call(
        &self,
        ctx: RequestContext,
        request: OwnedView<ReqView>,
    ) -> BoxFuture<'static, ServiceResult<ServiceStream<Self::Item>>>;
}

/// Wrapper that implements [`ViewStreamingHandler`] for async functions.
pub struct FnViewStreamingHandler<F> {
    f: Arc<F>,
}

impl<F> FnViewStreamingHandler<F> {
    /// Create a new function view streaming handler.
    pub fn new(f: F) -> Self {
        Self { f: Arc::new(f) }
    }
}

impl<F, Fut, ReqView, Res, B> ViewStreamingHandler<ReqView, Res> for FnViewStreamingHandler<F>
where
    F: Fn(RequestContext, OwnedView<ReqView>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<ServiceStream<B>>> + Send + 'static,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    type Item = B;

    fn call(
        &self,
        ctx: RequestContext,
        request: OwnedView<ReqView>,
    ) -> BoxFuture<'static, ServiceResult<ServiceStream<B>>> {
        let f = Arc::clone(&self.f);
        Box::pin(async move { f(ctx, request).await })
    }
}

/// Helper function to create a view streaming handler from an async function.
pub fn view_streaming_handler_fn<F, Fut, ReqView, Res, B>(f: F) -> FnViewStreamingHandler<F>
where
    F: Fn(RequestContext, OwnedView<ReqView>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<ServiceStream<B>>> + Send + 'static,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    FnViewStreamingHandler::new(f)
}

/// Wrapper to erase the types from a server streaming view handler.
pub(crate) struct ServerStreamingViewHandlerWrapper<H, ReqView, Res>
where
    H: ViewStreamingHandler<ReqView, Res>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
    Res: Message + Send + 'static,
{
    handler: Arc<H>,
    _phantom: std::marker::PhantomData<fn(ReqView) -> Res>,
}

impl<H, ReqView, Res> ServerStreamingViewHandlerWrapper<H, ReqView, Res>
where
    H: ViewStreamingHandler<ReqView, Res>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
    Res: Message + Send + 'static,
{
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<H, ReqView, Res> ErasedStreamingHandler for ServerStreamingViewHandlerWrapper<H, ReqView, Res>
where
    H: ViewStreamingHandler<ReqView, Res>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
    Res: Message + Send + 'static,
{
    fn call_erased(
        &self,
        ctx: RequestContext,
        request: Bytes,
        format: CodecFormat,
    ) -> StreamingHandlerResult {
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            let req = decode_request_view::<ReqView>(request, format, ctx.decode_options())?;
            let resp = handler.call(ctx, req).await?;
            Ok(resp.map_body(|s| encode_body_stream(s, format)))
        })
    }
}

/// Trait for client streaming RPC handlers using zero-copy request views.
///
/// `call` returns the response **already encoded**; see [`ViewHandler`].
pub trait ViewClientStreamingHandler<ReqView>: Send + Sync + 'static
where
    ReqView: MessageView<'static> + Send + Sync + 'static,
{
    /// Handle a client streaming RPC request with zero-copy view items,
    /// encoding the response in `format`.
    fn call(
        &self,
        ctx: RequestContext,
        requests: ServiceStream<OwnedView<ReqView>>,
        format: CodecFormat,
    ) -> BoxFuture<'static, Result<EncodedResponse, ConnectError>>;
}

/// Wrapper that implements [`ViewClientStreamingHandler`] for async functions.
pub struct FnViewClientStreamingHandler<F> {
    f: Arc<F>,
}

impl<F> FnViewClientStreamingHandler<F> {
    /// Create a new function view client streaming handler.
    pub fn new(f: F) -> Self {
        Self { f: Arc::new(f) }
    }
}

impl<F, Fut, ReqView> ViewClientStreamingHandler<ReqView> for FnViewClientStreamingHandler<F>
where
    F: Fn(RequestContext, ServiceStream<OwnedView<ReqView>>, CodecFormat) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<EncodedResponse, ConnectError>> + Send + 'static,
    ReqView: MessageView<'static> + Send + Sync + 'static,
{
    fn call(
        &self,
        ctx: RequestContext,
        requests: ServiceStream<OwnedView<ReqView>>,
        format: CodecFormat,
    ) -> BoxFuture<'static, Result<EncodedResponse, ConnectError>> {
        let f = Arc::clone(&self.f);
        Box::pin(async move { f(ctx, requests, format).await })
    }
}

/// Helper function to create a view client streaming handler from an async function.
pub fn view_client_streaming_handler_fn<F, Fut, ReqView>(f: F) -> FnViewClientStreamingHandler<F>
where
    F: Fn(RequestContext, ServiceStream<OwnedView<ReqView>>, CodecFormat) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<EncodedResponse, ConnectError>> + Send + 'static,
    ReqView: MessageView<'static> + Send + Sync + 'static,
{
    FnViewClientStreamingHandler::new(f)
}

/// Wrapper to erase the types from a client streaming view handler.
pub(crate) struct ClientStreamingViewHandlerWrapper<H, ReqView>
where
    H: ViewClientStreamingHandler<ReqView>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
{
    handler: Arc<H>,
    _phantom: std::marker::PhantomData<fn(ReqView)>,
}

impl<H, ReqView> ClientStreamingViewHandlerWrapper<H, ReqView>
where
    H: ViewClientStreamingHandler<ReqView>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
{
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<H, ReqView> ErasedClientStreamingHandler for ClientStreamingViewHandlerWrapper<H, ReqView>
where
    H: ViewClientStreamingHandler<ReqView>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
{
    fn call_erased(
        &self,
        ctx: RequestContext,
        requests: BoxStream<Result<Bytes, ConnectError>>,
        format: CodecFormat,
    ) -> BoxFuture<'static, Result<EncodedResponse, ConnectError>> {
        use futures::StreamExt as _;
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            // The stream outlives this frame, so it owns its limits rather
            // than borrowing them from `ctx`, which is moved into the call.
            let options = ctx.decode_options().clone();
            let request_stream: ServiceStream<OwnedView<ReqView>> =
                Box::pin(requests.map(move |result| {
                    result.and_then(|raw| decode_request_view::<ReqView>(raw, format, &options))
                }));
            handler.call(ctx, request_stream, format).await
        })
    }
}

/// Trait for bidi streaming RPC handlers using zero-copy request views.
pub trait ViewBidiStreamingHandler<ReqView, Res>: Send + Sync + 'static
where
    ReqView: MessageView<'static> + Send + Sync + 'static,
    Res: Message + Send + 'static,
{
    /// The stream item type. Typically `Res` itself; may be
    /// [`PreEncoded`](crate::PreEncoded) or
    /// [`MaybeBorrowed`](crate::MaybeBorrowed) for handlers that encode
    /// borrowing views per item.
    type Item: Encodable<Res> + Send + 'static;

    /// Handle a bidi streaming RPC request with zero-copy view items.
    fn call(
        &self,
        ctx: RequestContext,
        requests: ServiceStream<OwnedView<ReqView>>,
    ) -> BoxFuture<'static, ServiceResult<ServiceStream<Self::Item>>>;
}

/// Wrapper that implements [`ViewBidiStreamingHandler`] for async functions.
pub struct FnViewBidiStreamingHandler<F> {
    f: Arc<F>,
}

impl<F> FnViewBidiStreamingHandler<F> {
    /// Create a new function view bidi streaming handler.
    pub fn new(f: F) -> Self {
        Self { f: Arc::new(f) }
    }
}

impl<F, Fut, ReqView, Res, B> ViewBidiStreamingHandler<ReqView, Res>
    for FnViewBidiStreamingHandler<F>
where
    F: Fn(RequestContext, ServiceStream<OwnedView<ReqView>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<ServiceStream<B>>> + Send + 'static,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    type Item = B;

    fn call(
        &self,
        ctx: RequestContext,
        requests: ServiceStream<OwnedView<ReqView>>,
    ) -> BoxFuture<'static, ServiceResult<ServiceStream<B>>> {
        let f = Arc::clone(&self.f);
        Box::pin(async move { f(ctx, requests).await })
    }
}

/// Helper function to create a view bidi streaming handler from an async function.
pub fn view_bidi_streaming_handler_fn<F, Fut, ReqView, Res, B>(
    f: F,
) -> FnViewBidiStreamingHandler<F>
where
    F: Fn(RequestContext, ServiceStream<OwnedView<ReqView>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServiceResult<ServiceStream<B>>> + Send + 'static,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    Res: Message + Send + 'static,
    B: Encodable<Res> + Send + 'static,
{
    FnViewBidiStreamingHandler::new(f)
}

/// Wrapper to erase the types from a bidi streaming view handler.
pub(crate) struct BidiStreamingViewHandlerWrapper<H, ReqView, Res>
where
    H: ViewBidiStreamingHandler<ReqView, Res>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
    Res: Message + Send + 'static,
{
    handler: Arc<H>,
    _phantom: std::marker::PhantomData<fn(ReqView) -> Res>,
}

impl<H, ReqView, Res> BidiStreamingViewHandlerWrapper<H, ReqView, Res>
where
    H: ViewBidiStreamingHandler<ReqView, Res>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
    Res: Message + Send + 'static,
{
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<H, ReqView, Res> ErasedBidiStreamingHandler
    for BidiStreamingViewHandlerWrapper<H, ReqView, Res>
where
    H: ViewBidiStreamingHandler<ReqView, Res>,
    ReqView: MessageView<'static> + Send + Sync + 'static,
    ReqView::Owned: Message + JsonDeserialize,
    Res: Message + Send + 'static,
{
    fn call_erased(
        &self,
        ctx: RequestContext,
        requests: BoxStream<Result<Bytes, ConnectError>>,
        format: CodecFormat,
    ) -> StreamingHandlerResult {
        use futures::StreamExt as _;
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            // The stream outlives this frame, so it owns its limits rather
            // than borrowing them from `ctx`, which is moved into the call.
            let options = ctx.decode_options().clone();
            let request_stream: ServiceStream<OwnedView<ReqView>> =
                Box::pin(requests.map(move |result| {
                    result.and_then(|raw| decode_request_view::<ReqView>(raw, format, &options))
                }));
            let resp = handler.call(ctx, request_stream).await?;
            Ok(resp.map_body(|s| encode_body_stream(s, format)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_budget::elements_over_default_budget;
    use buffa_types::google::protobuf::__buffa::view::StringValueView;
    use buffa_types::google::protobuf::StringValue;

    #[test]
    fn test_decode_request_proto() {
        let msg = StringValue::from("hello");
        let encoded = Bytes::from(msg.encode_to_vec());
        let decoded: StringValue =
            decode_request(&encoded, CodecFormat::Proto, &buffa::DecodeOptions::new()).unwrap();
        assert_eq!(decoded.value, "hello");
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_decode_request_json() {
        let encoded = Bytes::from_static(b"\"world\"");
        let decoded: StringValue =
            decode_request(&encoded, CodecFormat::Json, &buffa::DecodeOptions::new()).unwrap();
        assert_eq!(decoded.value, "world");
    }

    #[test]
    fn test_decode_request_proto_invalid() {
        let garbage = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
        let err = decode_request::<StringValue>(
            &garbage,
            CodecFormat::Proto,
            &buffa::DecodeOptions::new(),
        )
        .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::InvalidArgument);
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_decode_request_json_invalid() {
        let garbage = Bytes::from_static(b"not json");
        let err = decode_request::<StringValue>(
            &garbage,
            CodecFormat::Json,
            &buffa::DecodeOptions::new(),
        )
        .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::InvalidArgument);
    }

    #[test]
    fn overflow_payload_is_invalid_argument_at_decode_boundary() {
        // The wire-visible contract behind the infallible to_owned_message:
        // a request whose unknown fields exceed the allowance fails here,
        // classified like any other malformed request, before any handler
        // (and its owned conversion) runs.
        let body = crate::request::tests::unknown_field_overflow_body();
        let err = decode_request_view::<StringValueView<'static>>(
            body,
            CodecFormat::Proto,
            &buffa::DecodeOptions::new(),
        )
        .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_decode_request_view_proto() {
        let msg = StringValue::from("view-test");
        let encoded = Bytes::from(msg.encode_to_vec());
        let view = decode_request_view::<StringValueView>(
            encoded,
            CodecFormat::Proto,
            &buffa::DecodeOptions::new(),
        )
        .unwrap();
        assert_eq!(view.reborrow().value, "view-test");
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_decode_request_view_json() {
        let encoded = Bytes::from_static(b"\"json-view\"");
        let view = decode_request_view::<StringValueView>(
            encoded,
            CodecFormat::Json,
            &buffa::DecodeOptions::new(),
        )
        .unwrap();
        assert_eq!(view.reborrow().value, "json-view");
    }

    // Proto-only build: the JSON request-decode arms (`decode_request` and
    // `request_proto_bytes`, the latter reached via `decode_request_view`)
    // are compiled out and report `Unimplemented`; proto decoding is
    // unaffected (covered by the `*_proto` tests above).

    #[cfg(not(feature = "json"))]
    #[test]
    fn decode_request_json_is_unimplemented_without_feature() {
        let body = Bytes::from_static(b"\"world\"");
        let err =
            decode_request::<StringValue>(&body, CodecFormat::Json, &buffa::DecodeOptions::new())
                .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Unimplemented);
    }

    #[cfg(not(feature = "json"))]
    #[test]
    fn decode_request_view_json_is_unimplemented_without_feature() {
        let body = Bytes::from_static(b"\"world\"");
        let err = decode_request_view::<StringValueView>(
            body,
            CodecFormat::Json,
            &buffa::DecodeOptions::new(),
        )
        .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Unimplemented);
    }

    #[test]
    fn test_decode_request_view_proto_invalid() {
        let garbage = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
        let err = decode_request_view::<StringValueView>(
            garbage,
            CodecFormat::Proto,
            &buffa::DecodeOptions::new(),
        )
        .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn encode_body_stream_owned_items() {
        use futures::StreamExt as _;
        let s = futures::stream::iter([
            Ok(StringValue::from("a")),
            Ok(StringValue::from("b")),
            Err(ConnectError::internal("boom")),
        ]);
        let mut out = encode_body_stream::<StringValue, _, _>(s, CodecFormat::Proto);
        let a = out.next().await.unwrap().unwrap();
        let b = out.next().await.unwrap().unwrap();
        assert_eq!(StringValue::decode_from_slice(&a).unwrap().value, "a");
        assert_eq!(StringValue::decode_from_slice(&b).unwrap().value, "b");
        assert!(out.next().await.unwrap().is_err());
        assert!(out.next().await.is_none());
    }

    #[tokio::test]
    async fn encode_body_stream_pre_encoded_items() {
        use crate::PreEncoded;
        use futures::StreamExt as _;
        // A `StreamingHandler` (or `ViewStreamingHandler`) with
        // `type Item = PreEncoded` yields bytes the handler encoded
        // internally; the proto codec must pass them through verbatim.
        let bytes_a = StringValue::from("a").encode_to_bytes();
        let bytes_b = StringValue::from("b").encode_to_bytes();
        let s = futures::stream::iter([
            Ok(PreEncoded::<StringValue>::from_bytes_unchecked(
                bytes_a.clone(),
            )),
            Ok(PreEncoded::<StringValue>::from_bytes_unchecked(
                bytes_b.clone(),
            )),
        ]);
        let mut out =
            encode_body_stream::<StringValue, PreEncoded<StringValue>, _>(s, CodecFormat::Proto);
        assert_eq!(out.next().await.unwrap().unwrap(), bytes_a);
        assert_eq!(out.next().await.unwrap().unwrap(), bytes_b);
        assert!(out.next().await.is_none());
    }

    #[cfg(feature = "json")]
    #[tokio::test]
    async fn encode_body_stream_pre_encoded_json_decodes_per_item() {
        use crate::PreEncoded;
        use futures::StreamExt as _;
        // The JSON path decodes the proto bytes back to `M` per item and
        // re-serializes — slow but correct. Each item should match what
        // serializing the owned message directly would produce.
        let m_a = StringValue::from("a");
        let m_b = StringValue::from("b");
        let s = futures::stream::iter([
            Ok(PreEncoded::<StringValue>::from_bytes_unchecked(
                m_a.encode_to_bytes(),
            )),
            Ok(PreEncoded::<StringValue>::from_bytes_unchecked(
                m_b.encode_to_bytes(),
            )),
        ]);
        let mut out =
            encode_body_stream::<StringValue, PreEncoded<StringValue>, _>(s, CodecFormat::Json);
        assert_eq!(
            out.next().await.unwrap().unwrap(),
            Bytes::from(serde_json::to_vec(&m_a).unwrap())
        );
        assert_eq!(
            out.next().await.unwrap().unwrap(),
            Bytes::from(serde_json::to_vec(&m_b).unwrap())
        );
        assert!(out.next().await.is_none());
    }

    #[test]
    fn streaming_handler_item_is_inferred_from_closure() {
        // `streaming_handler_fn` infers `Item` from the closure's stream
        // type. This is a compile-only test: the call type-checks iff
        // `FnStreamingHandler<F>: StreamingHandler<Req, Res, Item = B>`
        // unifies for both an owned-message and a `PreEncoded` stream.
        use crate::PreEncoded;

        fn assert_handler<H, Req, Res, B>(_: &H)
        where
            H: StreamingHandler<Req, Res, Item = B>,
            Req: Message + Send + 'static,
            Res: Message + Send + 'static,
            B: Encodable<Res> + Send + 'static,
        {
        }

        let owned = streaming_handler_fn(|_ctx: RequestContext, _req: StringValue| async move {
            Response::stream_ok(futures::stream::iter([Ok(StringValue::from("x"))]))
        });
        assert_handler::<_, StringValue, StringValue, StringValue>(&owned);

        // When the closure pins the `PreEncoded` message type concretely,
        // `Res` is inferred from the unique `Encodable<M> for PreEncoded<M>`
        // impl. No turbofish needed on `streaming_handler_fn`. (The codegen
        // path is different: the trait method's `impl Encodable<Out>` item
        // is opaque, so the generated `register_routes` impl pins `Res` at
        // the `route_view_*_stream::<_, _, Res>(...)` call site instead.)
        let pre = streaming_handler_fn(|_ctx: RequestContext, _req: StringValue| async move {
            Response::stream_ok(futures::stream::iter([Ok(
                PreEncoded::<StringValue>::from_bytes_unchecked(
                    StringValue::from("x").encode_to_bytes(),
                ),
            )]))
        });
        assert_handler::<_, StringValue, StringValue, PreEncoded<StringValue>>(&pre);
    }

    /// Owned-message handlers decode through `Payload`/`decode_request`
    /// rather than the view helpers, so they read their budget from the
    /// `DecodeOptions` carried on `Payload`. That is easy to leave
    /// unattached, which silently falls back to buffa's defaults, so both
    /// owned entry points are pinned here.
    #[test]
    fn owned_message_decoding_honours_the_configured_limit() {
        use buffa_types::google::protobuf::{ListValue, Value};

        // Decoded as owned `Value`s, so the owned footprint sets the count.
        let n = elements_over_default_budget::<Value>();
        let list = ListValue {
            values: (0..n).map(|_| Value::default()).collect(),
            ..Default::default()
        };
        let encoded = Bytes::from(buffa::Message::encode_to_vec(&list));
        let raised = crate::Limits::default().element_memory_limit(usize::MAX);

        // `decode_request`, used by the owned-message streaming wrappers.
        assert!(
            decode_request::<ListValue>(
                &encoded,
                CodecFormat::Proto,
                &crate::Limits::default().decode_options()
            )
            .is_err(),
            "the default budget must still reject"
        );
        let decoded: ListValue =
            decode_request(&encoded, CodecFormat::Proto, &raised.decode_options())
                .expect("raised budget must admit");
        assert_eq!(decoded.values.len(), n);

        // `Payload::take_message`, used by the owned-message unary wrapper.
        let payload = crate::Payload::new(encoded.clone(), CodecFormat::Proto);
        assert!(
            payload.take_message::<ListValue>().is_err(),
            "a payload with no limits attached decodes under buffa defaults"
        );
        let payload = crate::Payload::new(encoded, CodecFormat::Proto)
            .with_decode_options(raised.decode_options());
        let decoded: ListValue = payload
            .take_message()
            .expect("a payload carrying raised limits must admit");
        assert_eq!(decoded.values.len(), n);
    }

    /// The budget rejection names the limit to raise, since it is the one
    /// decode failure an operator can fix without the peer changing.
    #[test]
    fn an_over_budget_decode_says_which_limit_to_raise() {
        use buffa_types::google::protobuf::__buffa::view::{ListValueView, ValueView};
        use buffa_types::google::protobuf::{ListValue, Value};

        // Decoded as borrowed `ValueView`s, so the view footprint — the
        // smaller of the two — sets the count.
        let list = ListValue {
            values: (0..elements_over_default_budget::<ValueView<'_>>())
                .map(|_| Value::default())
                .collect(),
            ..Default::default()
        };
        let encoded = Bytes::from(buffa::Message::encode_to_vec(&list));
        let err = decode_borrowed_request_view::<ListValueView<'_>>(
            &encoded,
            &crate::Limits::default().decode_options(),
        )
        .expect_err("over budget");
        let message = err.message.unwrap_or_default();
        assert!(
            message.contains("element_memory_limit"),
            "the budget rejection must name the knob, got {message:?}"
        );

        // A malformed request must NOT suggest raising a limit — that would
        // send an operator chasing a setting that cannot help.
        let garbage = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
        let err = decode_borrowed_request_view::<ListValueView<'_>>(
            &garbage,
            &crate::Limits::default().decode_options(),
        )
        .expect_err("malformed");
        let message = err.message.unwrap_or_default();
        assert!(
            !message.contains("element_memory_limit"),
            "a malformed request must not point at a limit, got {message:?}"
        );
    }

    /// The element-memory budget is a *configured* limit, not a constant:
    /// the same bytes must be rejected at the default and accepted once the
    /// service raises it. Without the second half, wiring the knob to
    /// nothing would still pass.
    #[test]
    fn element_memory_limit_is_taken_from_the_configured_limits() {
        use buffa_types::google::protobuf::__buffa::view::{ListValueView, ValueView};
        use buffa_types::google::protobuf::{ListValue, Value};

        // Element footprint is what the budget charges, not element
        // contents, so this stays small on the wire.
        let n = elements_over_default_budget::<ValueView<'_>>();
        let list = ListValue {
            values: (0..n).map(|_| Value::default()).collect(),
            ..Default::default()
        };
        let encoded = Bytes::from(buffa::Message::encode_to_vec(&list));

        let defaults = crate::Limits::default();
        let err =
            decode_borrowed_request_view::<ListValueView<'_>>(&encoded, &defaults.decode_options())
                .expect_err("the fixture must exceed the default element-memory budget");
        assert_eq!(err.code, crate::error::ErrorCode::InvalidArgument);

        let raised = crate::Limits::default().element_memory_limit(usize::MAX);
        let view =
            decode_borrowed_request_view::<ListValueView<'_>>(&encoded, &raised.decode_options())
                .expect("raising the limit must admit the same bytes");
        assert_eq!(view.values.len(), n);
    }

    /// `unlimited()` must lift the decode budget too — a caller who asks for
    /// no restrictions and still gets a 32 MiB element ceiling has been
    /// silently ignored.
    #[test]
    fn unlimited_limits_lift_the_element_budget() {
        assert_eq!(crate::Limits::unlimited().element_memory_limit, usize::MAX);
    }

    /// A context built outside the service carries buffa's defaults rather
    /// than no limits at all.
    #[test]
    fn a_bare_request_context_decodes_under_buffa_defaults() {
        let ctx = RequestContext::new(http::HeaderMap::new());
        let listing = format!("{:?}", ctx.decode_options());
        assert!(
            listing.contains(&buffa::DEFAULT_ELEMENT_MEMORY_LIMIT.to_string()),
            "expected buffa's default element-memory budget, got {listing}"
        );
    }
}
