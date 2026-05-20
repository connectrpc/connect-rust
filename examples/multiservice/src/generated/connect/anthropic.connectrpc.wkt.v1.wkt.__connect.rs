///Shorthand for `OwnedView<CreateEventRequestView<'static>>`.
pub type OwnedCreateEventRequestView = ::buffa::view::OwnedView<
    crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CreateEventRequestView<
        'static,
    >,
>;
///Shorthand for `OwnedView<CreateEventResponseView<'static>>`.
pub type OwnedCreateEventResponseView = ::buffa::view::OwnedView<
    crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CreateEventResponseView<
        'static,
    >,
>;
///Shorthand for `OwnedView<CalculateDurationRequestView<'static>>`.
pub type OwnedCalculateDurationRequestView = ::buffa::view::OwnedView<
    crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CalculateDurationRequestView<
        'static,
    >,
>;
///Shorthand for `OwnedView<CalculateDurationResponseView<'static>>`.
pub type OwnedCalculateDurationResponseView = ::buffa::view::OwnedView<
    crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CalculateDurationResponseView<
        'static,
    >,
>;
///Shorthand for `OwnedView<ProcessMetadataRequestView<'static>>`.
pub type OwnedProcessMetadataRequestView = ::buffa::view::OwnedView<
    crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::ProcessMetadataRequestView<
        'static,
    >,
>;
///Shorthand for `OwnedView<ProcessMetadataResponseView<'static>>`.
pub type OwnedProcessMetadataResponseView = ::buffa::view::OwnedView<
    crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::ProcessMetadataResponseView<
        'static,
    >,
>;
impl ::connectrpc::Encodable<
    crate::proto::anthropic::connectrpc::wkt::v1::CreateEventResponse,
>
for crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CreateEventResponseView<
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
    crate::proto::anthropic::connectrpc::wkt::v1::CreateEventResponse,
>
for ::buffa::view::OwnedView<
    crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CreateEventResponseView<
        'static,
    >,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::anthropic::connectrpc::wkt::v1::CalculateDurationResponse,
>
for crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CalculateDurationResponseView<
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
    crate::proto::anthropic::connectrpc::wkt::v1::CalculateDurationResponse,
>
for ::buffa::view::OwnedView<
    crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CalculateDurationResponseView<
        'static,
    >,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
impl ::connectrpc::Encodable<
    crate::proto::anthropic::connectrpc::wkt::v1::ProcessMetadataResponse,
>
for crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::ProcessMetadataResponseView<
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
    crate::proto::anthropic::connectrpc::wkt::v1::ProcessMetadataResponse,
>
for ::buffa::view::OwnedView<
    crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::ProcessMetadataResponseView<
        'static,
    >,
> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
/// Full service name for this service.
pub const WELL_KNOWN_TYPES_SERVICE_SERVICE_NAME: &str = "anthropic.connectrpc.wkt.v1.WellKnownTypesService";
/// Static [`Spec`](::connectrpc::Spec) for the server-side `CreateEvent` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const WELL_KNOWN_TYPES_SERVICE_CREATE_EVENT_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/anthropic.connectrpc.wkt.v1.WellKnownTypesService/CreateEvent",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `CalculateDuration` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const WELL_KNOWN_TYPES_SERVICE_CALCULATE_DURATION_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/anthropic.connectrpc.wkt.v1.WellKnownTypesService/CalculateDuration",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `ProcessMetadata` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const WELL_KNOWN_TYPES_SERVICE_PROCESS_METADATA_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/anthropic.connectrpc.wkt.v1.WellKnownTypesService/ProcessMetadata",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// WellKnownTypesService provides operations using Timestamp, Duration, and Struct.
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
pub trait WellKnownTypesService: Send + Sync + 'static {
    /// CreateEvent creates an event with a timestamp.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    fn create_event<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: OwnedCreateEventRequestView,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::anthropic::connectrpc::wkt::v1::CreateEventResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// CalculateDuration calculates the duration between two timestamps.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    fn calculate_duration<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: OwnedCalculateDurationRequestView,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::anthropic::connectrpc::wkt::v1::CalculateDurationResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
    /// ProcessMetadata processes arbitrary metadata as a Struct.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    fn process_metadata<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: OwnedProcessMetadataRequestView,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                crate::proto::anthropic::connectrpc::wkt::v1::ProcessMetadataResponse,
            > + Send + use<'a, Self>,
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
pub trait WellKnownTypesServiceExt: WellKnownTypesService {
    /// Register this service implementation with a Router.
    ///
    /// Takes ownership of the `Arc<Self>` and returns a new Router with
    /// this service's methods registered.
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router;
}
impl<S: WellKnownTypesService> WellKnownTypesServiceExt for S {
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router {
        router
            .route_view(
                WELL_KNOWN_TYPES_SERVICE_SERVICE_NAME,
                "CreateEvent",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |ctx, req, format| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            svc.create_event(ctx, req)
                                .await?
                                .encode::<
                                    crate::proto::anthropic::connectrpc::wkt::v1::CreateEventResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(WELL_KNOWN_TYPES_SERVICE_CREATE_EVENT_SPEC)
            .route_view(
                WELL_KNOWN_TYPES_SERVICE_SERVICE_NAME,
                "CalculateDuration",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |ctx, req, format| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            svc.calculate_duration(ctx, req)
                                .await?
                                .encode::<
                                    crate::proto::anthropic::connectrpc::wkt::v1::CalculateDurationResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(WELL_KNOWN_TYPES_SERVICE_CALCULATE_DURATION_SPEC)
            .route_view(
                WELL_KNOWN_TYPES_SERVICE_SERVICE_NAME,
                "ProcessMetadata",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |ctx, req, format| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            svc.process_metadata(ctx, req)
                                .await?
                                .encode::<
                                    crate::proto::anthropic::connectrpc::wkt::v1::ProcessMetadataResponse,
                                >(format)
                        }
                    })
                },
            )
            .with_spec(WELL_KNOWN_TYPES_SERVICE_PROCESS_METADATA_SPEC)
    }
}
/// Monomorphic dispatcher for `WellKnownTypesService`.
///
/// Unlike `.register(Router)` which type-erases each method into an `Arc<dyn ErasedHandler>` stored in a `HashMap`, this struct dispatches via a compile-time `match` on method name: no vtable, no hash lookup.
///
/// # Example
///
/// ```rust,ignore
/// use connectrpc::ConnectRpcService;
///
/// let server = WellKnownTypesServiceServer::new(MyImpl);
/// let service = ConnectRpcService::new(server);
/// // hand `service` to axum/hyper as a fallback_service
/// ```
pub struct WellKnownTypesServiceServer<T> {
    inner: ::std::sync::Arc<T>,
}
impl<T: WellKnownTypesService> WellKnownTypesServiceServer<T> {
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
impl<T> Clone for WellKnownTypesServiceServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: ::std::sync::Arc::clone(&self.inner),
        }
    }
}
impl<T: WellKnownTypesService> ::connectrpc::Dispatcher
for WellKnownTypesServiceServer<T> {
    #[inline]
    fn lookup(
        &self,
        path: &str,
    ) -> Option<::connectrpc::dispatcher::codegen::MethodDescriptor> {
        let method = path
            .strip_prefix("anthropic.connectrpc.wkt.v1.WellKnownTypesService/")?;
        match method {
            "CreateEvent" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(WELL_KNOWN_TYPES_SERVICE_CREATE_EVENT_SPEC),
                )
            }
            "CalculateDuration" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(WELL_KNOWN_TYPES_SERVICE_CALCULATE_DURATION_SPEC),
                )
            }
            "ProcessMetadata" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(WELL_KNOWN_TYPES_SERVICE_PROCESS_METADATA_SPEC),
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
            .strip_prefix("anthropic.connectrpc.wkt.v1.WellKnownTypesService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "CreateEvent" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req = ::connectrpc::dispatcher::codegen::decode_request_view::<
                        crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CreateEventRequestView,
                    >(request.encoded()?, format)?;
                    svc.create_event(ctx, req)
                        .await?
                        .encode::<
                            crate::proto::anthropic::connectrpc::wkt::v1::CreateEventResponse,
                        >(format)
                })
            }
            "CalculateDuration" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req = ::connectrpc::dispatcher::codegen::decode_request_view::<
                        crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CalculateDurationRequestView,
                    >(request.encoded()?, format)?;
                    svc.calculate_duration(ctx, req)
                        .await?
                        .encode::<
                            crate::proto::anthropic::connectrpc::wkt::v1::CalculateDurationResponse,
                        >(format)
                })
            }
            "ProcessMetadata" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req = ::connectrpc::dispatcher::codegen::decode_request_view::<
                        crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::ProcessMetadataRequestView,
                    >(request.encoded()?, format)?;
                    svc.process_metadata(ctx, req)
                        .await?
                        .encode::<
                            crate::proto::anthropic::connectrpc::wkt::v1::ProcessMetadataResponse,
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
            .strip_prefix("anthropic.connectrpc.wkt.v1.WellKnownTypesService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
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
            .strip_prefix("anthropic.connectrpc.wkt.v1.WellKnownTypesService/") else {
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
        let Some(method) = path
            .strip_prefix("anthropic.connectrpc.wkt.v1.WellKnownTypesService/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
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
/// let client = WellKnownTypesServiceClient::new(conn, config);
/// let response = client.create_event(request).await?;
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
/// let client = WellKnownTypesServiceClient::new(http, config);
/// let response = client.create_event(request).await?;
/// ```
///
/// # Working with the response
///
/// Unary calls return [`UnaryResponse<OwnedView<FooView>>`](::connectrpc::client::UnaryResponse).
/// The `OwnedView` derefs to the view, so field access is zero-copy:
///
/// ```rust,ignore
/// let resp = client.create_event(request).await?.into_view();
/// let name: &str = resp.name;  // borrow into the response buffer
/// ```
///
/// If you need the owned struct (e.g. to store or pass by value), use
/// [`into_owned()`](::connectrpc::client::UnaryResponse::into_owned):
///
/// ```rust,ignore
/// let owned = client.create_event(request).await?.into_owned();
/// ```
#[derive(Clone)]
pub struct WellKnownTypesServiceClient<T> {
    transport: T,
    config: ::connectrpc::client::ClientConfig,
}
impl<T> WellKnownTypesServiceClient<T>
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
    /// Call the CreateEvent RPC. Sends a request to /anthropic.connectrpc.wkt.v1.WellKnownTypesService/CreateEvent.
    pub async fn create_event(
        &self,
        request: crate::proto::anthropic::connectrpc::wkt::v1::CreateEventRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CreateEventResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.create_event_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the CreateEvent RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn create_event_with_options(
        &self,
        request: crate::proto::anthropic::connectrpc::wkt::v1::CreateEventRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CreateEventResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                WELL_KNOWN_TYPES_SERVICE_SERVICE_NAME,
                "CreateEvent",
                request,
                options,
            )
            .await
    }
    /// Call the CalculateDuration RPC. Sends a request to /anthropic.connectrpc.wkt.v1.WellKnownTypesService/CalculateDuration.
    pub async fn calculate_duration(
        &self,
        request: crate::proto::anthropic::connectrpc::wkt::v1::CalculateDurationRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CalculateDurationResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.calculate_duration_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the CalculateDuration RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn calculate_duration_with_options(
        &self,
        request: crate::proto::anthropic::connectrpc::wkt::v1::CalculateDurationRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::CalculateDurationResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                WELL_KNOWN_TYPES_SERVICE_SERVICE_NAME,
                "CalculateDuration",
                request,
                options,
            )
            .await
    }
    /// Call the ProcessMetadata RPC. Sends a request to /anthropic.connectrpc.wkt.v1.WellKnownTypesService/ProcessMetadata.
    pub async fn process_metadata(
        &self,
        request: crate::proto::anthropic::connectrpc::wkt::v1::ProcessMetadataRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::ProcessMetadataResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.process_metadata_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the ProcessMetadata RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn process_metadata_with_options(
        &self,
        request: crate::proto::anthropic::connectrpc::wkt::v1::ProcessMetadataRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                crate::proto::anthropic::connectrpc::wkt::v1::__buffa::view::ProcessMetadataResponseView<
                    'static,
                >,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                WELL_KNOWN_TYPES_SERVICE_SERVICE_NAME,
                "ProcessMetadata",
                request,
                options,
            )
            .await
    }
}
