///Shorthand for `OwnedView<UnaryRequestView<'static>>`.
pub type OwnedUnaryRequestView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::UnaryRequestView<'static>,
>;
///Shorthand for `OwnedView<UnaryResponseView<'static>>`.
pub type OwnedUnaryResponseView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::UnaryResponseView<'static>,
>;
///Shorthand for `OwnedView<ServerStreamRequestView<'static>>`.
pub type OwnedServerStreamRequestView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::ServerStreamRequestView<
        'static,
    >,
>;
///Shorthand for `OwnedView<ServerStreamResponseView<'static>>`.
pub type OwnedServerStreamResponseView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::ServerStreamResponseView<
        'static,
    >,
>;
///Shorthand for `OwnedView<ClientStreamRequestView<'static>>`.
pub type OwnedClientStreamRequestView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::ClientStreamRequestView<
        'static,
    >,
>;
///Shorthand for `OwnedView<ClientStreamResponseView<'static>>`.
pub type OwnedClientStreamResponseView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::ClientStreamResponseView<
        'static,
    >,
>;
///Shorthand for `OwnedView<BidiStreamRequestView<'static>>`.
pub type OwnedBidiStreamRequestView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::BidiStreamRequestView<
        'static,
    >,
>;
///Shorthand for `OwnedView<BidiStreamResponseView<'static>>`.
pub type OwnedBidiStreamResponseView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::BidiStreamResponseView<
        'static,
    >,
>;
///Shorthand for `OwnedView<UnimplementedRequestView<'static>>`.
pub type OwnedUnimplementedRequestView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::UnimplementedRequestView<
        'static,
    >,
>;
///Shorthand for `OwnedView<UnimplementedResponseView<'static>>`.
pub type OwnedUnimplementedResponseView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::UnimplementedResponseView<
        'static,
    >,
>;
///Shorthand for `OwnedView<IdempotentUnaryRequestView<'static>>`.
pub type OwnedIdempotentUnaryRequestView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::IdempotentUnaryRequestView<
        'static,
    >,
>;
///Shorthand for `OwnedView<IdempotentUnaryResponseView<'static>>`.
pub type OwnedIdempotentUnaryResponseView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::IdempotentUnaryResponseView<
        'static,
    >,
>;
impl ::connectrpc::Encodable<crate::proto::connectrpc::conformance::v1::UnaryResponse>
for crate::proto::connectrpc::conformance::v1::__buffa::view::UnaryResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::connectrpc::conformance::v1::UnaryResponse>
for ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::UnaryResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::connectrpc::conformance::v1::ServerStreamResponse,
>
for crate::proto::connectrpc::conformance::v1::__buffa::view::ServerStreamResponseView<
    '_,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::connectrpc::conformance::v1::ServerStreamResponse,
>
for ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::ServerStreamResponseView<
        'static,
    >,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::connectrpc::conformance::v1::ClientStreamResponse,
>
for crate::proto::connectrpc::conformance::v1::__buffa::view::ClientStreamResponseView<
    '_,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::connectrpc::conformance::v1::ClientStreamResponse,
>
for ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::ClientStreamResponseView<
        'static,
    >,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::connectrpc::conformance::v1::BidiStreamResponse,
>
for crate::proto::connectrpc::conformance::v1::__buffa::view::BidiStreamResponseView<
    '_,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::connectrpc::conformance::v1::BidiStreamResponse,
>
for ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::BidiStreamResponseView<
        'static,
    >,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::connectrpc::conformance::v1::UnimplementedResponse,
>
for crate::proto::connectrpc::conformance::v1::__buffa::view::UnimplementedResponseView<
    '_,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::connectrpc::conformance::v1::UnimplementedResponse,
>
for ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::UnimplementedResponseView<
        'static,
    >,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::connectrpc::conformance::v1::IdempotentUnaryResponse,
>
for crate::proto::connectrpc::conformance::v1::__buffa::view::IdempotentUnaryResponseView<
    '_,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::connectrpc::conformance::v1::IdempotentUnaryResponse,
>
for ::buffa::view::OwnedView<
    crate::proto::connectrpc::conformance::v1::__buffa::view::IdempotentUnaryResponseView<
        'static,
    >,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
    }
}
/// Full service name for this service.
pub const CONFORMANCE_SERVICE_SERVICE_NAME: &str = "connectrpc.conformance.v1.ConformanceService";
/// Static [`Spec`](::connectrpc::Spec) for the server-side `Unary` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const CONFORMANCE_SERVICE_UNARY_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/connectrpc.conformance.v1.ConformanceService/Unary",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `ServerStream` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const CONFORMANCE_SERVICE_SERVER_STREAM_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/connectrpc.conformance.v1.ConformanceService/ServerStream",
        ::connectrpc::StreamType::ServerStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `ClientStream` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const CONFORMANCE_SERVICE_CLIENT_STREAM_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/connectrpc.conformance.v1.ConformanceService/ClientStream",
        ::connectrpc::StreamType::ClientStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `BidiStream` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const CONFORMANCE_SERVICE_BIDI_STREAM_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/connectrpc.conformance.v1.ConformanceService/BidiStream",
        ::connectrpc::StreamType::BidiStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `Unimplemented` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const CONFORMANCE_SERVICE_UNIMPLEMENTED_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/connectrpc.conformance.v1.ConformanceService/Unimplemented",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `IdempotentUnary` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const CONFORMANCE_SERVICE_IDEMPOTENT_UNARY_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/connectrpc.conformance.v1.ConformanceService/IdempotentUnary",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// The service implemented by conformance test servers. This is implemented by
/// the reference servers, used to test clients, and is expected to be implemented
/// by test servers, since this is the service used by reference clients.
/// Test servers must implement the service as described.
///
/// # Implementing handlers
///
/// Implement methods with plain `async fn`; the returned future satisfies
/// the `Send` bound automatically.
///
/// **Unary and server-streaming requests** arrive as
/// [`ServiceRequest<'_, Req>`](::connectrpc::ServiceRequest): a zero-copy
/// view of the request plus its body, valid for the duration of the call.
/// Fields are read directly (`request.name` is a `&str` into the decoded
/// buffer) and the borrow may be held across `.await` points. Anything
/// that must outlive the call — `tokio::spawn`, channels, server state,
/// or data captured by a returned response stream — takes owned data:
/// call `request.to_owned_message()?` (or copy the specific fields)
/// first.
///
/// **Client-streaming and bidi requests** arrive as
/// `ServiceStream<`[`StreamMessage<Req>`](::connectrpc::StreamMessage)`>`.
/// Each item owns its decoded buffer and is `Send + 'static`, so items
/// can be buffered or moved into spawned tasks; read fields zero-copy
/// through the generated accessor methods (`item.name()`) or `.view()`,
/// convert with `.to_owned_message()?`, or yield an item back unchanged —
/// `StreamMessage<M>` implements `Encodable<M>`.
///
/// Request types resolved through `extern_path` (e.g. well-known types
/// from another crate) use the same wrappers; the crate that owns the
/// type must be generated with buffa ≥ 0.8.0 and views enabled so the
/// backing `HasMessageView` impl exists.
///
/// The `impl Encodable<Out>` return bound accepts the owned `Out`, the
/// generated `OutView<'_>` / `OwnedOutView`,
/// [`MaybeBorrowed`](::connectrpc::MaybeBorrowed), or
/// [`PreEncoded`](::connectrpc::PreEncoded) for handlers that encode a
/// non-`'static` view internally and pass the bytes across the handler
/// boundary. View bodies are not emitted for output types mapped via
/// `extern_path` (the impl would be an orphan); return owned for
/// WKT/extern outputs.
///
/// Server-streaming and bidi-streaming methods return
/// `ServiceStream<impl Encodable<Out> + Send + use<Self>>`. The
/// `use<Self>` precise-capturing clause excludes `&self`'s lifetime and
/// the request's lifetime (unary methods use `use<'a, Self>` and may
/// borrow from `&self`), so stream items must be `'static` and cannot
/// borrow from the request. To stream view-encoded data, encode each
/// item inside the stream body and yield
/// [`PreEncoded`](::connectrpc::PreEncoded) — see its `# Streaming
/// example` doc.
#[allow(clippy::type_complexity)]
pub trait ConformanceService: Send + Sync + 'static {
    /// A unary operation. The request indicates the response headers and trailers
    /// and also indicates either a response message or an error to send back.
    /// Response message data is specified as bytes. The service should echo back
    /// request properties in the ConformancePayload and then include the message
    /// data in the data field.
    /// If the response_delay_ms duration is specified, the server should wait the
    /// given duration after reading the request before sending the corresponding
    /// response.
    /// Servers should allow the response definition to be unset in the request and
    /// if it is, set no response headers or trailers and return no response data.
    /// The returned payload should only contain the request info.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()?` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn unary<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::connectrpc::conformance::v1::UnaryRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::connectrpc::conformance::v1::UnaryResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// A server-streaming operation. The request indicates the response headers,
    /// response messages, trailers, and an optional error to send back. The
    /// response data should be sent in the order indicated, and the server should
    /// wait between sending response messages as indicated.
    /// Response message data is specified as bytes. The service should echo back
    /// request properties in the first ConformancePayload, and then include the
    /// message data in the data field. Subsequent messages after the first one
    /// should contain only the data field.
    /// Servers should immediately send response headers on the stream before sleeping
    /// for any specified response delay and/or sending the first message so that
    /// clients can be unblocked reading response headers.
    /// If a response definition is not specified OR is specified, but response data
    /// is empty, the server should skip sending anything on the stream. When there
    /// are no responses to send, servers should throw an error if one is provided
    /// and return without error if one is not. Stream headers and trailers should
    /// still be set on the stream if provided regardless of whether a response is
    /// sent or an error is thrown.
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call (until the response stream is returned);
    /// message fields are read directly on it (zero-copy). Data the
    /// returned stream needs must be copied out or converted via
    /// `.to_owned_message()?`.
    fn server_stream(
        &self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::connectrpc::conformance::v1::ServerStreamRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            ::connectrpc::ServiceStream<
                impl ::connectrpc::Encodable<
                    crate::proto::connectrpc::conformance::v1::ServerStreamResponse,
                > + Send + use<Self>,
            >,
        >,
    > + Send;
    /// A client-streaming operation. The first request indicates the response
    /// headers and trailers and also indicates either a response message or an
    /// error to send back.
    /// Response message data is specified as bytes. The service should echo back
    /// request properties, including all request messages in the order they were
    /// received, in the ConformancePayload and then include the message data in
    /// the data field.
    /// If the input stream is empty, the server's response will include no data,
    /// only the request properties (headers, timeout).
    /// Servers should only read the response definition from the first message in
    /// the stream and should ignore any definition set in subsequent messages.
    /// Servers should allow the response definition to be unset in the request and
    /// if it is, set no response headers or trailers and return no response data.
    /// The returned payload should only contain the request info.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// Each `requests` item is a [`StreamMessage`](::connectrpc::StreamMessage):
    /// it owns its buffer, is `Send + 'static`, and exposes zero-copy
    /// accessor methods (`item.name()`), `.view()`, and
    /// `.to_owned_message()?`.
    fn client_stream<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::ServiceStream<
            ::connectrpc::StreamMessage<
                crate::proto::connectrpc::conformance::v1::ClientStreamRequest,
            >,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::connectrpc::conformance::v1::ClientStreamResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// A bidirectional-streaming operation. The first request indicates the response
    /// headers, response messages, trailers, and an optional error to send back.
    /// The response data should be sent in the order indicated, and the server
    /// should wait between sending response messages as indicated.
    /// Response message data is specified as bytes and should be included in the
    /// data field of the ConformancePayload in each response.
    /// Servers should send responses indicated according to the rules of half duplex
    /// vs. full duplex streams. Once all responses are sent, the server should either
    /// return an error if specified or close the stream without error.
    /// Servers should immediately send response headers on the stream before sleeping
    /// for any specified response delay and/or sending the first message so that
    /// clients can be unblocked reading response headers.
    /// If a response definition is not specified OR is specified, but response data
    /// is empty, the server should skip sending anything on the stream. Stream
    /// headers and trailers should always be set on the stream if provided
    /// regardless of whether a response is sent or an error is thrown.
    /// If the full_duplex field is true:
    /// - the handler should read one request and then send back one response, and
    /// then alternate, reading another request and then sending back another response, etc.
    /// - if the server receives a request and has no responses to send, it
    /// should throw the error specified in the request.
    /// - the service should echo back all request properties in the first response
    /// including the last received request. Subsequent responses should only
    /// echo back the last received request.
    /// - if the response_delay_ms duration is specified, the server should wait the given
    /// duration after reading the request before sending the corresponding
    /// response.
    /// If the full_duplex field is false:
    /// - the handler should read all requests until the client is done sending.
    /// Once all requests are read, the server should then send back any responses
    /// specified in the response definition.
    /// - the server should echo back all request properties, including all request
    /// messages in the order they were received, in the first response. Subsequent
    /// responses should only include the message data in the data field.
    /// - if the response_delay_ms duration is specified, the server should wait that
    /// long in between sending each response message.
    ///
    /// Each `requests` item is a [`StreamMessage`](::connectrpc::StreamMessage):
    /// it owns its buffer, is `Send + 'static`, and exposes zero-copy
    /// accessor methods (`item.name()`), `.view()`, and
    /// `.to_owned_message()?`.
    fn bidi_stream(
        &self,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::ServiceStream<
            ::connectrpc::StreamMessage<
                crate::proto::connectrpc::conformance::v1::BidiStreamRequest,
            >,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            ::connectrpc::ServiceStream<
                impl ::connectrpc::Encodable<
                    crate::proto::connectrpc::conformance::v1::BidiStreamResponse,
                > + Send + use<Self>,
            >,
        >,
    > + Send;
    /// A unary endpoint that the server should not implement and should instead
    /// return an unimplemented error when invoked.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()?` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn unimplemented<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::connectrpc::conformance::v1::UnimplementedRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::connectrpc::conformance::v1::UnimplementedResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// A unary endpoint denoted as having no side effects (i.e. idempotent).
    /// Implementations should use an HTTP GET when invoking this endpoint and
    /// leverage query parameters to send data.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    ///
    /// `request` is borrowed from the request body and is valid for the
    /// duration of the call; message fields are read directly on it
    /// (zero-copy). The response cannot borrow from `request` — use
    /// `.to_owned_message()?` (or copy the specific fields) for anything
    /// returned, stored, or moved into `tokio::spawn`.
    fn idempotent_unary<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::ServiceRequest<
            '_,
            crate::proto::connectrpc::conformance::v1::IdempotentUnaryRequest,
        >,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::connectrpc::conformance::v1::IdempotentUnaryResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
}
/// Extension trait for registering a service implementation with a Router.
///
/// This trait is automatically implemented for all types that implement the service trait.
/// Prefer [`Router::add_service`](::connectrpc::Router::add_service) for
/// top-down registration; `register` remains available for compatibility
/// and cases where the service-first call shape is more convenient.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// let service = Arc::new(MyServiceImpl);
/// let router = service.register(Router::new());
/// ```
pub trait ConformanceServiceExt: ConformanceService {
    /// Register this service implementation with a Router.
    ///
    /// Takes ownership of the `Arc<Self>` and returns a new Router with
    /// this service's methods registered.
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router;
}
impl<S: ConformanceService> ConformanceServiceExt for S {
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router {
        router
            .route_view(
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "Unary",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::connectrpc::conformance::v1::__buffa::view::UnaryRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::connectrpc::conformance::v1::UnaryRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.unary(ctx, sreq)
                                .await?
                                .encode::<
                                    crate::proto::connectrpc::conformance::v1::UnaryResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(CONFORMANCE_SERVICE_UNARY_SPEC)
            .route_view_server_stream::<
                _,
                _,
                crate::proto::connectrpc::conformance::v1::ServerStreamResponse,
            >(
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "ServerStream",
                ::connectrpc::view_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::connectrpc::conformance::v1::__buffa::view::ServerStreamRequestView<
                                'static,
                            >,
                        >|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::connectrpc::conformance::v1::ServerStreamRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.server_stream(ctx, sreq).await
                        }
                    }
                }),
            )
            .with_spec(CONFORMANCE_SERVICE_SERVER_STREAM_SPEC)
            .route_view_client_stream(
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "ClientStream",
                ::connectrpc::view_client_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |ctx, req, format| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let req = ::connectrpc::dispatcher::codegen::into_stream_messages::<
                                crate::proto::connectrpc::conformance::v1::ClientStreamRequest,
                            >(req);
                            svc.client_stream(ctx, req)
                                .await?
                                .encode::<
                                    crate::proto::connectrpc::conformance::v1::ClientStreamResponse,
                                >(format)
                        }
                    }
                }),
            )
            .with_spec(CONFORMANCE_SERVICE_CLIENT_STREAM_SPEC)
            .route_view_bidi_stream::<
                _,
                _,
                crate::proto::connectrpc::conformance::v1::BidiStreamResponse,
            >(
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "BidiStream",
                ::connectrpc::view_bidi_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |ctx, req| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let req = ::connectrpc::dispatcher::codegen::into_stream_messages::<
                                crate::proto::connectrpc::conformance::v1::BidiStreamRequest,
                            >(req);
                            svc.bidi_stream(ctx, req).await
                        }
                    }
                }),
            )
            .with_spec(CONFORMANCE_SERVICE_BIDI_STREAM_SPEC)
            .route_view(
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "Unimplemented",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::connectrpc::conformance::v1::__buffa::view::UnimplementedRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::connectrpc::conformance::v1::UnimplementedRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.unimplemented(ctx, sreq)
                                .await?
                                .encode::<
                                    crate::proto::connectrpc::conformance::v1::UnimplementedResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(CONFORMANCE_SERVICE_UNIMPLEMENTED_SPEC)
            .route_view_idempotent(
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "IdempotentUnary",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |
                        ctx,
                        req: ::buffa::view::OwnedView<
                            crate::proto::connectrpc::conformance::v1::__buffa::view::IdempotentUnaryRequestView<
                                'static,
                            >,
                        >,
                        format|
                    {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            let sreq = ::connectrpc::ServiceRequest::<
                                crate::proto::connectrpc::conformance::v1::IdempotentUnaryRequest,
                            >::from_parts(req.reborrow(), req.bytes());
                            svc.idempotent_unary(ctx, sreq)
                                .await?
                                .encode::<
                                    crate::proto::connectrpc::conformance::v1::IdempotentUnaryResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(CONFORMANCE_SERVICE_IDEMPOTENT_UNARY_SPEC)
    }
}
/// Type-inference marker used by [`Router::add_service`](::connectrpc::Router::add_service).
#[doc(hidden)]
pub struct ConformanceServiceRegisterMarker;
impl<
    S: ConformanceService,
> ::connectrpc::ServiceRegister<ConformanceServiceRegisterMarker>
for ::std::sync::Arc<S> {
    fn register_service(self, router: ::connectrpc::Router) -> ::connectrpc::Router {
        <S as ConformanceServiceExt>::register(self, router)
    }
}
/// Monomorphic dispatcher for `ConformanceService`.
///
/// Unlike `.register(Router)` which type-erases each method into an `Arc<dyn ErasedHandler>` stored in a `HashMap`, this struct dispatches via a compile-time `match` on method name: no vtable, no hash lookup.
///
/// # Example
///
/// ```rust,ignore
/// use connectrpc::ConnectRpcService;
///
/// let server = ConformanceServiceServer::new(MyImpl);
/// let service = ConnectRpcService::new(server);
/// // hand `service` to axum/hyper as a fallback_service
/// ```
pub struct ConformanceServiceServer<T> {
    inner: ::std::sync::Arc<T>,
}
impl<T: ConformanceService> ConformanceServiceServer<T> {
    /// Wrap a service implementation in a monomorphic dispatcher.
    pub fn new(service: T) -> Self {
        Self {
            inner: ::std::sync::Arc::new(service),
        }
    }
    /// Wrap an already-`Arc`'d service implementation.
    pub fn from_arc(inner: ::std::sync::Arc<T>) -> Self {
        Self { inner }
    }
}
impl<T> Clone for ConformanceServiceServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: ::std::sync::Arc::clone(&self.inner),
        }
    }
}
impl<T: ConformanceService> ::connectrpc::Dispatcher for ConformanceServiceServer<T> {
    #[inline]
    fn lookup(
        &self,
        path: &str,
    ) -> Option<::connectrpc::dispatcher::codegen::MethodDescriptor> {
        let method = path.strip_prefix("connectrpc.conformance.v1.ConformanceService/")?;
        match method {
            "Unary" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(CONFORMANCE_SERVICE_UNARY_SPEC),
                )
            }
            "ServerStream" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::server_streaming()
                        .with_spec(CONFORMANCE_SERVICE_SERVER_STREAM_SPEC),
                )
            }
            "ClientStream" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::client_streaming()
                        .with_spec(CONFORMANCE_SERVICE_CLIENT_STREAM_SPEC),
                )
            }
            "BidiStream" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::bidi_streaming()
                        .with_spec(CONFORMANCE_SERVICE_BIDI_STREAM_SPEC),
                )
            }
            "Unimplemented" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(CONFORMANCE_SERVICE_UNIMPLEMENTED_SPEC),
                )
            }
            "IdempotentUnary" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(true)
                        .with_spec(CONFORMANCE_SERVICE_IDEMPOTENT_UNARY_SPEC),
                )
            }
            _ => None,
        }
    }
    fn call_unary(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::Payload,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path
            .strip_prefix("connectrpc.conformance.v1.ConformanceService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "Unary" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::connectrpc::conformance::v1::UnaryRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::connectrpc::conformance::v1::__buffa::view::UnaryRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::connectrpc::conformance::v1::UnaryRequest,
                    >::from_parts(&req, &body);
                    svc.unary(ctx, req)
                        .await?
                        .encode::<
                            crate::proto::connectrpc::conformance::v1::UnaryResponse,
                        >(format)
                })
            }
            "Unimplemented" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::connectrpc::conformance::v1::UnimplementedRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::connectrpc::conformance::v1::__buffa::view::UnimplementedRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::connectrpc::conformance::v1::UnimplementedRequest,
                    >::from_parts(&req, &body);
                    svc.unimplemented(ctx, req)
                        .await?
                        .encode::<
                            crate::proto::connectrpc::conformance::v1::UnimplementedResponse,
                        >(format)
                })
            }
            "IdempotentUnary" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::connectrpc::conformance::v1::IdempotentUnaryRequest,
                    >(request.encoded()?, format)?;
                    let req: crate::proto::connectrpc::conformance::v1::__buffa::view::IdempotentUnaryRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::connectrpc::conformance::v1::IdempotentUnaryRequest,
                    >::from_parts(&req, &body);
                    svc.idempotent_unary(ctx, req)
                        .await?
                        .encode::<
                            crate::proto::connectrpc::conformance::v1::IdempotentUnaryResponse,
                        >(format)
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_server_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::buffa::bytes::Bytes,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path
            .strip_prefix("connectrpc.conformance.v1.ConformanceService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "ServerStream" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<
                        crate::proto::connectrpc::conformance::v1::ServerStreamRequest,
                    >(request, format)?;
                    let req: crate::proto::connectrpc::conformance::v1::__buffa::view::ServerStreamRequestView<
                        '_,
                    > = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(
                        &body,
                    )?;
                    let req = ::connectrpc::ServiceRequest::<
                        crate::proto::connectrpc::conformance::v1::ServerStreamRequest,
                    >::from_parts(&req, &body);
                    let resp = svc.server_stream(ctx, req).await?;
                    Ok(
                        resp
                            .map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<
                                crate::proto::connectrpc::conformance::v1::ServerStreamResponse,
                                _,
                                _,
                            >(s, format)),
                    )
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
    fn call_client_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path
            .strip_prefix("connectrpc.conformance.v1.ConformanceService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            "ClientStream" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req_stream = ::connectrpc::dispatcher::codegen::decode_message_request_stream::<
                        crate::proto::connectrpc::conformance::v1::ClientStreamRequest,
                    >(requests, format);
                    svc.client_stream(ctx, req_stream)
                        .await?
                        .encode::<
                            crate::proto::connectrpc::conformance::v1::ClientStreamResponse,
                        >(format)
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_bidi_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path
            .strip_prefix("connectrpc.conformance.v1.ConformanceService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            "BidiStream" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req_stream = ::connectrpc::dispatcher::codegen::decode_message_request_stream::<
                        crate::proto::connectrpc::conformance::v1::BidiStreamRequest,
                    >(requests, format);
                    let resp = svc.bidi_stream(ctx, req_stream).await?;
                    Ok(
                        resp
                            .map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<
                                crate::proto::connectrpc::conformance::v1::BidiStreamResponse,
                                _,
                                _,
                            >(s, format)),
                    )
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
}
/// Client for this service.
///
/// Generic over `T: ClientTransport`. For **gRPC** (HTTP/2), use
/// `Http2Connection` — it has honest `poll_ready` and composes with
/// `tower::balance` for multi-connection load balancing. For **Connect
/// over HTTP/1.1** (or unknown protocol), use `HttpClient`.
///
/// # Example (gRPC / HTTP/2)
///
/// ```rust,ignore
/// use connectrpc::client::{Http2Connection, ClientConfig};
/// use connectrpc::Protocol;
///
/// let uri: http::Uri = "http://localhost:8080".parse()?;
/// let conn = Http2Connection::connect_plaintext(uri.clone()).await?.shared(1024);
/// let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
///
/// let client = ConformanceServiceClient::new(conn, config);
/// let response = client.unary(request).await?;
/// ```
///
/// # Example (Connect / HTTP/1.1 or ALPN)
///
/// ```rust,ignore
/// use connectrpc::client::{HttpClient, ClientConfig};
///
/// let http = HttpClient::plaintext();  // cleartext http:// only
/// let config = ClientConfig::new("http://localhost:8080".parse()?);
///
/// let client = ConformanceServiceClient::new(http, config);
/// let response = client.unary(request).await?;
/// ```
///
/// # Working with the response
///
/// Unary calls return [`UnaryResponse<OwnedView<FooView>>`](::connectrpc::client::UnaryResponse).
/// [`view()`](::connectrpc::client::UnaryResponse::view) borrows the response
/// message, so field access is zero-copy:
///
/// ```rust,ignore
/// let resp = client.unary(request).await?;
/// let name: &str = resp.view().name;  // borrow into the response buffer
/// ```
///
/// If you need the owned struct (e.g. to store or pass by value), use
/// [`into_owned()`](::connectrpc::client::UnaryResponse::into_owned) — fallible,
/// since rebuilding preserved unknown fields can exceed the unknown-field
/// allowance:
///
/// ```rust,ignore
/// let owned = client.unary(request).await?.into_owned()?;
/// ```
///
/// [`into_view()`](::connectrpc::client::UnaryResponse::into_view) keeps the
/// zero-copy decoded body (an `OwnedView`) without copying; field access on it
/// goes through `.reborrow()`. Streaming responses yield one `OwnedView` per
/// received message from `.message().await` — bind `msg.reborrow()` for field
/// access, or convert with `.to_owned_message()?`.
#[derive(Clone)]
pub struct ConformanceServiceClient<T> {
    transport: T,
    config: ::connectrpc::client::ClientConfig,
}
impl<T> ConformanceServiceClient<T>
where
    T: ::connectrpc::client::ClientTransport,
    <T::ResponseBody as ::http_body::Body>::Error: ::std::fmt::Display,
{
    /// Create a new client with the given transport and configuration.
    pub fn new(transport: T, config: ::connectrpc::client::ClientConfig) -> Self {
        Self { transport, config }
    }
    /// Get the client configuration.
    pub fn config(&self) -> &::connectrpc::client::ClientConfig {
        &self.config
    }
    /// Get a mutable reference to the client configuration.
    pub fn config_mut(&mut self) -> &mut ::connectrpc::client::ClientConfig {
        &mut self.config
    }
    /// Call the Unary RPC. Sends a request to /connectrpc.conformance.v1.ConformanceService/Unary.
    pub async fn unary(
        &self,
        request: crate::proto::connectrpc::conformance::v1::UnaryRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::connectrpc::conformance::v1::__buffa::view::UnaryResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.unary_with_options(request, ::connectrpc::client::CallOptions::default())
            .await
    }
    /// Call the Unary RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn unary_with_options(
        &self,
        request: crate::proto::connectrpc::conformance::v1::UnaryRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::connectrpc::conformance::v1::__buffa::view::UnaryResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "Unary",
                request,
                options,
            )
            .await
    }
    /// Call the ServerStream RPC. Sends a request to /connectrpc.conformance.v1.ConformanceService/ServerStream.
    pub async fn server_stream(
        &self,
        request: crate::proto::connectrpc::conformance::v1::ServerStreamRequest,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            crate::proto::connectrpc::conformance::v1::__buffa::view::ServerStreamResponseView<
                'static,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.server_stream_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the ServerStream RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn server_stream_with_options(
        &self,
        request: crate::proto::connectrpc::conformance::v1::ServerStreamRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            crate::proto::connectrpc::conformance::v1::__buffa::view::ServerStreamResponseView<
                'static,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_server_stream(
                &self.transport,
                &self.config,
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "ServerStream",
                request,
                options,
            )
            .await
    }
    /// Call the ClientStream RPC. Sends a request to /connectrpc.conformance.v1.ConformanceService/ClientStream.
    pub async fn client_stream(
        &self,
        requests: impl IntoIterator<
            Item = crate::proto::connectrpc::conformance::v1::ClientStreamRequest,
        >,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::connectrpc::conformance::v1::__buffa::view::ClientStreamResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.client_stream_with_options(
                requests,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the ClientStream RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn client_stream_with_options(
        &self,
        requests: impl IntoIterator<
            Item = crate::proto::connectrpc::conformance::v1::ClientStreamRequest,
        >,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::connectrpc::conformance::v1::__buffa::view::ClientStreamResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_client_stream(
                &self.transport,
                &self.config,
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "ClientStream",
                requests,
                options,
            )
            .await
    }
    /// Call the BidiStream RPC. Sends a request to /connectrpc.conformance.v1.ConformanceService/BidiStream.
    pub async fn bidi_stream(
        &self,
    ) -> Result<
        ::connectrpc::client::BidiStream<
            T::ResponseBody,
            crate::proto::connectrpc::conformance::v1::BidiStreamRequest,
            crate::proto::connectrpc::conformance::v1::__buffa::view::BidiStreamResponseView<
                'static,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.bidi_stream_with_options(::connectrpc::client::CallOptions::default()).await
    }
    /// Call the BidiStream RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn bidi_stream_with_options(
        &self,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::BidiStream<
            T::ResponseBody,
            crate::proto::connectrpc::conformance::v1::BidiStreamRequest,
            crate::proto::connectrpc::conformance::v1::__buffa::view::BidiStreamResponseView<
                'static,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_bidi_stream(
                &self.transport,
                &self.config,
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "BidiStream",
                options,
            )
            .await
    }
    /// Call the Unimplemented RPC. Sends a request to /connectrpc.conformance.v1.ConformanceService/Unimplemented.
    pub async fn unimplemented(
        &self,
        request: crate::proto::connectrpc::conformance::v1::UnimplementedRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::connectrpc::conformance::v1::__buffa::view::UnimplementedResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.unimplemented_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the Unimplemented RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn unimplemented_with_options(
        &self,
        request: crate::proto::connectrpc::conformance::v1::UnimplementedRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::connectrpc::conformance::v1::__buffa::view::UnimplementedResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "Unimplemented",
                request,
                options,
            )
            .await
    }
    /// Call the IdempotentUnary RPC. Sends a request to /connectrpc.conformance.v1.ConformanceService/IdempotentUnary.
    pub async fn idempotent_unary(
        &self,
        request: crate::proto::connectrpc::conformance::v1::IdempotentUnaryRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::connectrpc::conformance::v1::__buffa::view::IdempotentUnaryResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.idempotent_unary_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the IdempotentUnary RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn idempotent_unary_with_options(
        &self,
        request: crate::proto::connectrpc::conformance::v1::IdempotentUnaryRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::connectrpc::conformance::v1::__buffa::view::IdempotentUnaryResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                CONFORMANCE_SERVICE_SERVICE_NAME,
                "IdempotentUnary",
                request,
                options,
            )
            .await
    }
}
