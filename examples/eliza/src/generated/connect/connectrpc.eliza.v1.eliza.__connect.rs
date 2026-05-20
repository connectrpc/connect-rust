///Shorthand for `OwnedView<SayRequestView<'static>>`.
pub type OwnedSayRequestView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::eliza::v1::__buffa::view::SayRequestView<'static>,
>;
///Shorthand for `OwnedView<SayResponseView<'static>>`.
pub type OwnedSayResponseView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::eliza::v1::__buffa::view::SayResponseView<'static>,
>;
///Shorthand for `OwnedView<ConverseRequestView<'static>>`.
pub type OwnedConverseRequestView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::eliza::v1::__buffa::view::ConverseRequestView<'static>,
>;
///Shorthand for `OwnedView<ConverseResponseView<'static>>`.
pub type OwnedConverseResponseView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::eliza::v1::__buffa::view::ConverseResponseView<'static>,
>;
///Shorthand for `OwnedView<IntroduceRequestView<'static>>`.
pub type OwnedIntroduceRequestView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::eliza::v1::__buffa::view::IntroduceRequestView<'static>,
>;
///Shorthand for `OwnedView<IntroduceResponseView<'static>>`.
pub type OwnedIntroduceResponseView = ::buffa::view::OwnedView<
    crate::proto::connectrpc::eliza::v1::__buffa::view::IntroduceResponseView<'static>,
>;
impl ::connectrpc::Encodable<crate::proto::connectrpc::eliza::v1::SayResponse>
for crate::proto::connectrpc::eliza::v1::__buffa::view::SayResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::connectrpc::eliza::v1::SayResponse>
for ::buffa::view::OwnedView<
    crate::proto::connectrpc::eliza::v1::__buffa::view::SayResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::connectrpc::eliza::v1::ConverseResponse>
for crate::proto::connectrpc::eliza::v1::__buffa::view::ConverseResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::connectrpc::eliza::v1::ConverseResponse>
for ::buffa::view::OwnedView<
    crate::proto::connectrpc::eliza::v1::__buffa::view::ConverseResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::connectrpc::eliza::v1::IntroduceResponse>
for crate::proto::connectrpc::eliza::v1::__buffa::view::IntroduceResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<crate::proto::connectrpc::eliza::v1::IntroduceResponse>
for ::buffa::view::OwnedView<
    crate::proto::connectrpc::eliza::v1::__buffa::view::IntroduceResponseView<'static>,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
/// Full service name for this service.
pub const ELIZA_SERVICE_SERVICE_NAME: &str = "connectrpc.eliza.v1.ElizaService";
/// Static [`Spec`](::connectrpc::Spec) for the server-side `Say` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const ELIZA_SERVICE_SAY_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/connectrpc.eliza.v1.ElizaService/Say",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::NoSideEffects);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `Converse` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const ELIZA_SERVICE_CONVERSE_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/connectrpc.eliza.v1.ElizaService/Converse",
        ::connectrpc::StreamType::BidiStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `Introduce` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const ELIZA_SERVICE_INTRODUCE_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/connectrpc.eliza.v1.ElizaService/Introduce",
        ::connectrpc::StreamType::ServerStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// ElizaService provides a way to talk to Eliza, a port of the DOCTOR script
/// for Joseph Weizenbaum's original ELIZA program. Created in the mid-1960s at
/// the MIT Artificial Intelligence Laboratory, ELIZA demonstrates the
/// superficiality of human-computer communication. DOCTOR simulates a
/// psychotherapist, and is commonly found as an Easter egg in emacs
/// distributions.
///
/// # Implementing handlers
///
/// Handlers receive requests as `OwnedFooView` (an alias for
/// `OwnedView<FooView<'static>>`), which gives zero-copy borrowed access
/// to fields (e.g. `request.name` is a `&str` into the decoded buffer).
/// The view can be held across `.await` points. When two RPC types in
/// the same package would alias to the same `Owned<…>View` name (e.g.
/// a local message plus an imported one with the same short name), the
/// alias is suppressed for both and the request type is spelled as
/// `OwnedView<…View<'static>>` directly in the trait signature.
///
/// Implement methods with plain `async fn`; the returned future satisfies
/// the `Send` bound automatically. See the
/// [buffa user guide](https://github.com/anthropics/buffa/blob/main/docs/guide.md#ownedview-in-async-trait-implementations)
/// for zero-copy access patterns and when `to_owned_message()` is needed.
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
/// `use<Self>` precise-capturing clause excludes `&self`'s lifetime
/// (unary methods use `use<'a, Self>` and may borrow), so stream items
/// must be `'static`. To stream view-encoded data, encode each item
/// inside the stream body and yield
/// [`PreEncoded`](::connectrpc::PreEncoded) — see its `# Streaming
/// example` doc.
#[allow(clippy::type_complexity)]
pub trait ElizaService: Send + Sync + 'static {
    /// Say is a unary RPC. Eliza responds to the prompt with a single sentence.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    fn say<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: OwnedSayRequestView,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::connectrpc::eliza::v1::SayResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// Converse is a bidirectional RPC. The caller may exchange multiple
    /// back-and-forth messages with Eliza over a long-lived connection. Eliza
    /// responds to each ConverseRequest with a ConverseResponse.
    fn converse(
        &self,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::ServiceStream<OwnedConverseRequestView>,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            ::connectrpc::ServiceStream<
                impl ::connectrpc::Encodable<
                    crate::proto::connectrpc::eliza::v1::ConverseResponse,
                > + Send + use<Self>,
            >,
        >,
    > + Send;
    /// Introduce is a server streaming RPC. Given the caller's name, Eliza
    /// returns a stream of sentences to introduce itself.
    fn introduce(
        &self,
        ctx: ::connectrpc::RequestContext,
        request: OwnedIntroduceRequestView,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            ::connectrpc::ServiceStream<
                impl ::connectrpc::Encodable<
                    crate::proto::connectrpc::eliza::v1::IntroduceResponse,
                > + Send + use<Self>,
            >,
        >,
    > + Send;
}
/// Extension trait for registering a service implementation with a Router.
///
/// This trait is automatically implemented for all types that implement the service trait.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// let service = Arc::new(MyServiceImpl);
/// let router = service.register(Router::new());
/// ```
pub trait ElizaServiceExt: ElizaService {
    /// Register this service implementation with a Router.
    ///
    /// Takes ownership of the `Arc<Self>` and returns a new Router with
    /// this service's methods registered.
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router;
}
impl<S: ElizaService> ElizaServiceExt for S {
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router {
        router
            .route_view_idempotent(
                ELIZA_SERVICE_SERVICE_NAME,
                "Say",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |ctx, req, format| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            svc.say(ctx, req)
                                .await?
                                .encode::<
                                    crate::proto::connectrpc::eliza::v1::SayResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(ELIZA_SERVICE_SAY_SPEC)
            .route_view_bidi_stream::<
                _,
                _,
                crate::proto::connectrpc::eliza::v1::ConverseResponse,
            >(
                ELIZA_SERVICE_SERVICE_NAME,
                "Converse",
                ::connectrpc::view_bidi_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |ctx, req| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move { svc.converse(ctx, req).await }
                    }
                }),
            )
            .with_spec(ELIZA_SERVICE_CONVERSE_SPEC)
            .route_view_server_stream::<
                _,
                _,
                crate::proto::connectrpc::eliza::v1::IntroduceResponse,
            >(
                ELIZA_SERVICE_SERVICE_NAME,
                "Introduce",
                ::connectrpc::view_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |ctx, req| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move { svc.introduce(ctx, req).await }
                    }
                }),
            )
            .with_spec(ELIZA_SERVICE_INTRODUCE_SPEC)
    }
}
/// Monomorphic dispatcher for `ElizaService`.
///
/// Unlike `.register(Router)` which type-erases each method into an `Arc<dyn ErasedHandler>` stored in a `HashMap`, this struct dispatches via a compile-time `match` on method name: no vtable, no hash lookup.
///
/// # Example
///
/// ```rust,ignore
/// use connectrpc::ConnectRpcService;
///
/// let server = ElizaServiceServer::new(MyImpl);
/// let service = ConnectRpcService::new(server);
/// // hand `service` to axum/hyper as a fallback_service
/// ```
pub struct ElizaServiceServer<T> {
    inner: ::std::sync::Arc<T>,
}
impl<T: ElizaService> ElizaServiceServer<T> {
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
impl<T> Clone for ElizaServiceServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: ::std::sync::Arc::clone(&self.inner),
        }
    }
}
impl<T: ElizaService> ::connectrpc::Dispatcher for ElizaServiceServer<T> {
    #[inline]
    fn lookup(
        &self,
        path: &str,
    ) -> Option<::connectrpc::dispatcher::codegen::MethodDescriptor> {
        let method = path.strip_prefix("connectrpc.eliza.v1.ElizaService/")?;
        match method {
            "Say" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(true)
                        .with_spec(ELIZA_SERVICE_SAY_SPEC),
                )
            }
            "Converse" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::bidi_streaming()
                        .with_spec(ELIZA_SERVICE_CONVERSE_SPEC),
                )
            }
            "Introduce" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::server_streaming()
                        .with_spec(ELIZA_SERVICE_INTRODUCE_SPEC),
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
        let Some(method) = path.strip_prefix("connectrpc.eliza.v1.ElizaService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "Say" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req = ::connectrpc::dispatcher::codegen::decode_request_view::<
                        crate::proto::connectrpc::eliza::v1::__buffa::view::SayRequestView,
                    >(request.encoded()?, format)?;
                    svc.say(ctx, req)
                        .await?
                        .encode::<
                            crate::proto::connectrpc::eliza::v1::SayResponse,
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
        let Some(method) = path.strip_prefix("connectrpc.eliza.v1.ElizaService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "Introduce" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req = ::connectrpc::dispatcher::codegen::decode_request_view::<
                        crate::proto::connectrpc::eliza::v1::__buffa::view::IntroduceRequestView,
                    >(request, format)?;
                    let resp = svc.introduce(ctx, req).await?;
                    Ok(
                        resp
                            .map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<
                                crate::proto::connectrpc::eliza::v1::IntroduceResponse,
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
        let Some(method) = path.strip_prefix("connectrpc.eliza.v1.ElizaService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
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
        let Some(method) = path.strip_prefix("connectrpc.eliza.v1.ElizaService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            "Converse" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req_stream = ::connectrpc::dispatcher::codegen::decode_view_request_stream::<
                        crate::proto::connectrpc::eliza::v1::__buffa::view::ConverseRequestView,
                    >(requests, format);
                    let resp = svc.converse(ctx, req_stream).await?;
                    Ok(
                        resp
                            .map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<
                                crate::proto::connectrpc::eliza::v1::ConverseResponse,
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
/// let client = ElizaServiceClient::new(conn, config);
/// let response = client.say(request).await?;
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
/// let client = ElizaServiceClient::new(http, config);
/// let response = client.say(request).await?;
/// ```
///
/// # Working with the response
///
/// Unary calls return [`UnaryResponse<OwnedView<FooView>>`](::connectrpc::client::UnaryResponse).
/// The `OwnedView` derefs to the view, so field access is zero-copy:
///
/// ```rust,ignore
/// let resp = client.say(request).await?.into_view();
/// let name: &str = resp.name;  // borrow into the response buffer
/// ```
///
/// If you need the owned struct (e.g. to store or pass by value), use
/// [`into_owned()`](::connectrpc::client::UnaryResponse::into_owned):
///
/// ```rust,ignore
/// let owned = client.say(request).await?.into_owned();
/// ```
#[derive(Clone)]
pub struct ElizaServiceClient<T> {
    transport: T,
    config: ::connectrpc::client::ClientConfig,
}
impl<T> ElizaServiceClient<T>
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
    /// Call the Say RPC. Sends a request to /connectrpc.eliza.v1.ElizaService/Say.
    pub async fn say(
        &self,
        request: crate::proto::connectrpc::eliza::v1::SayRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::connectrpc::eliza::v1::__buffa::view::SayResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.say_with_options(request, ::connectrpc::client::CallOptions::default())
            .await
    }
    /// Call the Say RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn say_with_options(
        &self,
        request: crate::proto::connectrpc::eliza::v1::SayRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::connectrpc::eliza::v1::__buffa::view::SayResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                ELIZA_SERVICE_SERVICE_NAME,
                "Say",
                request,
                options,
            )
            .await
    }
    /// Call the Converse RPC. Sends a request to /connectrpc.eliza.v1.ElizaService/Converse.
    pub async fn converse(
        &self,
    ) -> Result<
        ::connectrpc::client::BidiStream<
            T::ResponseBody,
            crate::proto::connectrpc::eliza::v1::ConverseRequest,
            crate::proto::connectrpc::eliza::v1::__buffa::view::ConverseResponseView<
                'static,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.converse_with_options(::connectrpc::client::CallOptions::default()).await
    }
    /// Call the Converse RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn converse_with_options(
        &self,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::BidiStream<
            T::ResponseBody,
            crate::proto::connectrpc::eliza::v1::ConverseRequest,
            crate::proto::connectrpc::eliza::v1::__buffa::view::ConverseResponseView<
                'static,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_bidi_stream(
                &self.transport,
                &self.config,
                ELIZA_SERVICE_SERVICE_NAME,
                "Converse",
                options,
            )
            .await
    }
    /// Call the Introduce RPC. Sends a request to /connectrpc.eliza.v1.ElizaService/Introduce.
    pub async fn introduce(
        &self,
        request: crate::proto::connectrpc::eliza::v1::IntroduceRequest,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            crate::proto::connectrpc::eliza::v1::__buffa::view::IntroduceResponseView<
                'static,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.introduce_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the Introduce RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn introduce_with_options(
        &self,
        request: crate::proto::connectrpc::eliza::v1::IntroduceRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            crate::proto::connectrpc::eliza::v1::__buffa::view::IntroduceResponseView<
                'static,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_server_stream(
                &self.transport,
                &self.config,
                ELIZA_SERVICE_SERVICE_NAME,
                "Introduce",
                request,
                options,
            )
            .await
    }
}
