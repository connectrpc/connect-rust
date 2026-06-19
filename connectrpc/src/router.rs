//! Request routing and service registration.
//!
//! This module provides the router for mapping RPC method paths to handlers.

use std::collections::HashMap;
use std::sync::Arc;

use buffa::Message;

use buffa::view::MessageView;

use crate::codec::{JsonDeserialize, JsonSerialize};
use crate::handler::BidiStreamingHandler;
use crate::handler::BidiStreamingHandlerWrapper;
use crate::handler::BidiStreamingViewHandlerWrapper;
use crate::handler::ClientStreamingHandler;
use crate::handler::ClientStreamingHandlerWrapper;
use crate::handler::ClientStreamingViewHandlerWrapper;
use crate::handler::ErasedBidiStreamingHandler;
use crate::handler::ErasedClientStreamingHandler;
use crate::handler::ErasedHandler;
use crate::handler::ErasedStreamingHandler;
use crate::handler::Handler;
use crate::handler::ServerStreamingHandlerWrapper;
use crate::handler::ServerStreamingViewHandlerWrapper;
use crate::handler::StreamingHandler;
use crate::handler::UnaryHandlerWrapper;
use crate::handler::UnaryViewHandlerWrapper;
use crate::handler::ViewBidiStreamingHandler;
use crate::handler::ViewClientStreamingHandler;
use crate::handler::ViewHandler;
use crate::handler::ViewStreamingHandler;
use crate::spec::IdempotencyLevel;
use crate::spec::Spec;

/// The kind of RPC method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MethodKind {
    /// Unary RPC: single request, single response.
    Unary,
    /// Server streaming RPC: single request, stream of responses.
    ServerStreaming,
    /// Client streaming RPC: stream of requests, single response.
    ClientStreaming,
    /// Bidirectional streaming RPC: stream of requests, stream of responses.
    BidiStreaming,
}

/// A registered unary RPC method.
struct UnaryMethod {
    handler: Arc<dyn ErasedHandler>,
    /// Whether this method has no side effects and can be called via GET.
    idempotent: bool,
}

/// A registered streaming RPC method.
struct StreamingMethod {
    handler: Arc<dyn ErasedStreamingHandler>,
    kind: MethodKind,
}

/// A registered client streaming RPC method.
struct ClientStreamingMethod {
    handler: Arc<dyn ErasedClientStreamingHandler>,
}

/// A registered bidi streaming RPC method.
struct BidiStreamingMethod {
    handler: Arc<dyn ErasedBidiStreamingHandler>,
}

/// A registered RPC method (either unary or streaming).
enum Method {
    Unary(UnaryMethod),
    Streaming(StreamingMethod),
    ClientStreaming(ClientStreamingMethod),
    BidiStreaming(BidiStreamingMethod),
}

/// A registered method plus its optional static [`Spec`].
///
/// `spec` is `None` until [`Router::with_spec`] runs for the route. The
/// generated `FooServiceExt::register` always chains `.with_spec(...)`
/// after the `route_*` call; hand-written registrations may omit it
/// (their paths are usually owned `String`s, not `&'static str`s, so
/// they cannot construct a [`Spec`] anyway).
struct RegisteredMethod {
    method: Method,
    spec: Option<Spec>,
}

impl From<Method> for RegisteredMethod {
    fn from(method: Method) -> Self {
        Self { method, spec: None }
    }
}

/// Router for ConnectRPC services.
///
/// The router maps service/method paths to their handlers and manages
/// request dispatching.
///
/// `Router` is the *dynamic* dispatch path: method paths are owned `String`
/// keys. It can still carry a [`Spec`] when one is attached with
/// [`with_spec`](Self::with_spec) — the generated
/// `FooServiceExt::register` does this for every method, so a `Router`
/// built through codegen surfaces [`RequestContext::spec`] just like the
/// monomorphic `FooServiceServer<T>` dispatcher does. Hand-written
/// `route_*` registrations without a `Spec` surface `None`.
///
/// [`RequestContext::spec`]: crate::RequestContext::spec
#[derive(Default)]
pub struct Router {
    /// Map from "service_name/method_name" to handler.
    methods: HashMap<String, RegisteredMethod>,
}

impl Router {
    /// Create a new empty router.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a unary RPC handler.
    ///
    /// # Arguments
    ///
    /// * `service_name` - The fully qualified service name (e.g., "example.v1.GreetService")
    /// * `method_name` - The method name (e.g., "Greet")
    /// * `handler` - The handler function or closure
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let router = Router::new()
    ///     .route("example.v1.GreetService", "Greet", |ctx, req: GreetRequest| async move {
    ///         Ok(GreetResponse { message: format!("Hello, {}!", req.name) })
    ///     });
    /// ```
    pub fn route<H, Req, Res>(self, service_name: &str, method_name: &str, handler: H) -> Self
    where
        H: Handler<Req, Res>,
        Req: Message + JsonDeserialize + Send + 'static,
        Res: Message + JsonSerialize + Send + 'static,
    {
        self.route_unary_internal(service_name, method_name, handler, false)
    }

    /// Register an idempotent unary RPC handler that supports GET requests.
    ///
    /// Idempotent methods have no side effects and can be called via HTTP GET.
    /// This is typically used for methods marked with `idempotency_level = NO_SIDE_EFFECTS`
    /// in the protobuf definition.
    ///
    /// # Arguments
    ///
    /// * `service_name` - The fully qualified service name (e.g., "example.v1.GreetService")
    /// * `method_name` - The method name (e.g., "Greet")
    /// * `handler` - The handler function or closure
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let router = Router::new()
    ///     .route_idempotent("example.v1.QueryService", "GetUser", |ctx, req: GetUserRequest| async move {
    ///         Ok(GetUserResponse { ... })
    ///     });
    /// ```
    pub fn route_idempotent<H, Req, Res>(
        self,
        service_name: &str,
        method_name: &str,
        handler: H,
    ) -> Self
    where
        H: Handler<Req, Res>,
        Req: Message + JsonDeserialize + Send + 'static,
        Res: Message + JsonSerialize + Send + 'static,
    {
        self.route_unary_internal(service_name, method_name, handler, true)
    }

    /// Internal helper for registering unary handlers with configurable idempotency.
    fn route_unary_internal<H, Req, Res>(
        mut self,
        service_name: &str,
        method_name: &str,
        handler: H,
        idempotent: bool,
    ) -> Self
    where
        H: Handler<Req, Res>,
        Req: Message + JsonDeserialize + Send + 'static,
        Res: Message + JsonSerialize + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = UnaryHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::Unary(UnaryMethod {
                handler: Arc::new(wrapper),
                idempotent,
            })
            .into(),
        );
        self
    }

    /// Register a server streaming RPC handler.
    ///
    /// A server streaming handler takes a single request and returns a stream of responses.
    ///
    /// # Arguments
    ///
    /// * `service_name` - The fully qualified service name (e.g., "example.v1.GreetService")
    /// * `method_name` - The method name (e.g., "GreetMany")
    /// * `handler` - The streaming handler function or closure
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let router = Router::new()
    ///     .route_server_stream("example.v1.GreetService", "GreetMany", streaming_handler_fn(my_handler));
    /// ```
    pub fn route_server_stream<H, Req, Res>(
        mut self,
        service_name: &str,
        method_name: &str,
        handler: H,
    ) -> Self
    where
        H: StreamingHandler<Req, Res>,
        Req: Message + JsonDeserialize + Send + 'static,
        Res: Message + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = ServerStreamingHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::Streaming(StreamingMethod {
                handler: Arc::new(wrapper),
                kind: MethodKind::ServerStreaming,
            })
            .into(),
        );
        self
    }

    /// Register a client streaming RPC handler.
    ///
    /// A client streaming handler receives a stream of requests and returns a single response.
    pub fn route_client_stream<H, Req, Res>(
        mut self,
        service_name: &str,
        method_name: &str,
        handler: H,
    ) -> Self
    where
        H: ClientStreamingHandler<Req, Res>,
        Req: Message + JsonDeserialize + Send + 'static,
        Res: Message + JsonSerialize + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = ClientStreamingHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::ClientStreaming(ClientStreamingMethod {
                handler: Arc::new(wrapper),
            })
            .into(),
        );
        self
    }

    /// Register a bidi streaming RPC handler.
    ///
    /// A bidi streaming handler receives a stream of requests and returns a stream of responses.
    pub fn route_bidi_stream<H, Req, Res>(
        mut self,
        service_name: &str,
        method_name: &str,
        handler: H,
    ) -> Self
    where
        H: BidiStreamingHandler<Req, Res>,
        Req: Message + JsonDeserialize + Send + 'static,
        Res: Message + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = BidiStreamingHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::BidiStreaming(BidiStreamingMethod {
                handler: Arc::new(wrapper),
            })
            .into(),
        );
        self
    }

    // ====================================================================
    // View-based route methods (zero-copy request deserialization)
    // ====================================================================

    /// Register a unary RPC handler that uses zero-copy request views.
    pub fn route_view<H, ReqView>(self, service_name: &str, method_name: &str, handler: H) -> Self
    where
        H: ViewHandler<ReqView>,
        ReqView: MessageView<'static> + Send + Sync + 'static,
        ReqView::Owned: Message + JsonDeserialize,
    {
        self.route_view_internal(service_name, method_name, handler, false)
    }

    /// Register an idempotent unary RPC handler that uses zero-copy request views.
    pub fn route_view_idempotent<H, ReqView>(
        self,
        service_name: &str,
        method_name: &str,
        handler: H,
    ) -> Self
    where
        H: ViewHandler<ReqView>,
        ReqView: MessageView<'static> + Send + Sync + 'static,
        ReqView::Owned: Message + JsonDeserialize,
    {
        self.route_view_internal(service_name, method_name, handler, true)
    }

    /// Internal helper for registering view handlers with configurable idempotency.
    fn route_view_internal<H, ReqView>(
        mut self,
        service_name: &str,
        method_name: &str,
        handler: H,
        idempotent: bool,
    ) -> Self
    where
        H: ViewHandler<ReqView>,
        ReqView: MessageView<'static> + Send + Sync + 'static,
        ReqView::Owned: Message + JsonDeserialize,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = UnaryViewHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::Unary(UnaryMethod {
                handler: Arc::new(wrapper),
                idempotent,
            })
            .into(),
        );
        self
    }

    /// Register a server streaming RPC handler that uses zero-copy request views.
    pub fn route_view_server_stream<H, ReqView, Res>(
        mut self,
        service_name: &str,
        method_name: &str,
        handler: H,
    ) -> Self
    where
        H: ViewStreamingHandler<ReqView, Res>,
        ReqView: MessageView<'static> + Send + Sync + 'static,
        ReqView::Owned: Message + JsonDeserialize,
        Res: Message + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = ServerStreamingViewHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::Streaming(StreamingMethod {
                handler: Arc::new(wrapper),
                kind: MethodKind::ServerStreaming,
            })
            .into(),
        );
        self
    }

    /// Register a client streaming RPC handler that uses zero-copy request views.
    pub fn route_view_client_stream<H, ReqView>(
        mut self,
        service_name: &str,
        method_name: &str,
        handler: H,
    ) -> Self
    where
        H: ViewClientStreamingHandler<ReqView>,
        ReqView: MessageView<'static> + Send + Sync + 'static,
        ReqView::Owned: Message + JsonDeserialize,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = ClientStreamingViewHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::ClientStreaming(ClientStreamingMethod {
                handler: Arc::new(wrapper),
            })
            .into(),
        );
        self
    }

    /// Register a bidi streaming RPC handler that uses zero-copy request views.
    pub fn route_view_bidi_stream<H, ReqView, Res>(
        mut self,
        service_name: &str,
        method_name: &str,
        handler: H,
    ) -> Self
    where
        H: ViewBidiStreamingHandler<ReqView, Res>,
        ReqView: MessageView<'static> + Send + Sync + 'static,
        ReqView::Owned: Message + JsonDeserialize,
        Res: Message + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = BidiStreamingViewHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::BidiStreaming(BidiStreamingMethod {
                handler: Arc::new(wrapper),
            })
            .into(),
        );
        self
    }

    /// Attach a [`Spec`] to the route registered at `spec.procedure`.
    ///
    /// The route must already exist — [`Spec::procedure`] is the lookup
    /// key (with the leading slash stripped, matching the `route_*`
    /// methods' `format!("{service}/{method}")` keying). Generated
    /// `FooServiceExt::register` chains this after each `route_view*`
    /// call so the dynamic `Router` carries the same per-method
    /// metadata as the monomorphic `FooServiceServer<T>`. Hand-written
    /// registrations may call it too when they have a `&'static`
    /// procedure path:
    ///
    /// ```rust,ignore
    /// const SAY_SPEC: Spec = Spec::server("/eliza.v1.Eliza/Say", StreamType::Unary);
    /// let router = Router::new()
    ///     .route_view_idempotent("eliza.v1.Eliza", "Say", handler)
    ///     .with_spec(SAY_SPEC);
    /// ```
    ///
    /// # Panics
    ///
    /// Debug builds panic if no route is registered at `spec.procedure`,
    /// or if the route is unary and `spec.idempotency_level` disagrees
    /// with the `route` / `route_idempotent` choice. Both indicate a
    /// programming error in `register()` (a typo, or calling `with_spec`
    /// before the matching `route_*`); release builds skip the check and
    /// silently drop the `Spec`.
    #[must_use]
    pub fn with_spec(mut self, spec: Spec) -> Self {
        let key = spec.procedure.strip_prefix('/').unwrap_or(spec.procedure);
        match self.methods.get_mut(key) {
            Some(m) => {
                if let Method::Unary(u) = &m.method {
                    debug_assert_eq!(
                        u.idempotent,
                        spec.idempotency_level == IdempotencyLevel::NoSideEffects,
                        "route {key:?} idempotency disagrees with Spec::idempotency_level — \
                         pick `route` vs `route_idempotent` to match the Spec"
                    );
                }
                m.spec = Some(spec);
            }
            None => {
                debug_assert!(
                    false,
                    "Router::with_spec: no route registered at {key:?} — \
                     call the matching `route_*` first"
                );
            }
        }
        self
    }

    /// Get all registered method paths.
    pub fn methods(&self) -> impl Iterator<Item = &str> {
        self.methods.keys().map(String::as_str)
    }

    /// Check if a path is registered.
    pub fn has_method(&self, path: &str) -> bool {
        self.methods.contains_key(path)
    }
}

// ============================================================================
// Dispatcher implementation — backward-compat dynamic dispatch
// ============================================================================

impl crate::dispatcher::Dispatcher for Router {
    fn lookup(&self, path: &str) -> Option<crate::dispatcher::MethodDescriptor> {
        use crate::dispatcher::MethodDescriptor;
        let m = self.methods.get(path)?;
        let mut desc = match &m.method {
            Method::Unary(u) => MethodDescriptor::unary(u.idempotent),
            Method::Streaming(s) => MethodDescriptor::from_kind(s.kind),
            Method::ClientStreaming(_) => MethodDescriptor::client_streaming(),
            Method::BidiStreaming(_) => MethodDescriptor::bidi_streaming(),
        };
        if let Some(spec) = m.spec {
            desc = desc.with_spec(spec);
        }
        Some(desc)
    }

    fn call_unary(
        &self,
        path: &str,
        ctx: crate::response::RequestContext,
        request: crate::Payload,
        format: crate::codec::CodecFormat,
    ) -> crate::dispatcher::UnaryResult {
        match self.methods.get(path).map(|m| &m.method) {
            Some(Method::Unary(m)) => m.handler.call_erased(ctx, request, format),
            _ => crate::dispatcher::unimplemented_unary(path),
        }
    }

    fn call_server_streaming(
        &self,
        path: &str,
        ctx: crate::response::RequestContext,
        request: bytes::Bytes,
        format: crate::codec::CodecFormat,
    ) -> crate::dispatcher::StreamingResult {
        match self.methods.get(path).map(|m| &m.method) {
            Some(Method::Streaming(m)) => m.handler.call_erased(ctx, request, format),
            _ => crate::dispatcher::unimplemented_streaming(path),
        }
    }

    fn call_client_streaming(
        &self,
        path: &str,
        ctx: crate::response::RequestContext,
        requests: crate::dispatcher::RequestStream,
        format: crate::codec::CodecFormat,
    ) -> crate::dispatcher::UnaryResult {
        match self.methods.get(path).map(|m| &m.method) {
            Some(Method::ClientStreaming(m)) => m.handler.call_erased(ctx, requests, format),
            _ => crate::dispatcher::unimplemented_unary(path),
        }
    }

    fn call_bidi_streaming(
        &self,
        path: &str,
        ctx: crate::response::RequestContext,
        requests: crate::dispatcher::RequestStream,
        format: crate::codec::CodecFormat,
    ) -> crate::dispatcher::StreamingResult {
        match self.methods.get(path).map(|m| &m.method) {
            Some(Method::BidiStreaming(m)) => m.handler.call_erased(ctx, requests, format),
            _ => crate::dispatcher::unimplemented_streaming(path),
        }
    }
}

/// Merge multiple routers into one.
pub fn merge_routers(routers: impl IntoIterator<Item = Router>) -> Router {
    let mut merged = Router::new();
    for router in routers {
        merged.methods.extend(router.methods);
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::Dispatcher;
    use crate::handler_fn;
    use crate::spec::StreamType;
    use buffa_types::Empty;

    #[test]
    fn test_router_registration() {
        // This test just verifies the API compiles correctly
        // Full testing requires actual proto types
        let router = Router::new();
        assert!(!router.has_method("test.Service/Method"));
    }

    fn unary_handler() -> impl Handler<Empty, Empty> {
        handler_fn(|_ctx, _req: Empty| async { crate::Response::ok(Empty::default()) })
    }

    /// `with_spec` attaches the `Spec` and `lookup` returns it. This is
    /// the property the codegen relies on: a `Router` built through
    /// `register()` should surface `RequestContext::spec` exactly like
    /// the monomorphic `FooServiceServer<T>` dispatcher.
    #[test]
    fn with_spec_round_trips_through_lookup() {
        const SPEC: Spec = Spec::server("/test.Svc/Method", StreamType::Unary);
        let router = Router::new()
            .route("test.Svc", "Method", unary_handler())
            .with_spec(SPEC);

        let desc = router.lookup("test.Svc/Method").expect("route exists");
        assert_eq!(
            desc.spec,
            Some(SPEC),
            "lookup must return the attached Spec"
        );
        assert_eq!(desc.kind, MethodKind::Unary);
        assert!(!desc.idempotent);
    }

    /// A route registered without `with_spec` keeps the pre-existing
    /// `spec: None` behavior — back-compat.
    #[test]
    fn route_without_with_spec_is_unchanged() {
        let router = Router::new().route("test.Svc", "Method", unary_handler());
        let desc = router.lookup("test.Svc/Method").expect("route exists");
        assert_eq!(desc.spec, None);
    }

    /// `merge_routers` carries each route's `Spec` across the merge.
    #[test]
    fn merge_routers_preserves_specs() {
        const A: Spec = Spec::server("/svc.A/M", StreamType::Unary);
        const B: Spec = Spec::server("/svc.B/N", StreamType::Unary);
        let merged = merge_routers([
            Router::new()
                .route("svc.A", "M", unary_handler())
                .with_spec(A),
            Router::new()
                .route("svc.B", "N", unary_handler())
                .with_spec(B),
        ]);
        assert_eq!(merged.lookup("svc.A/M").unwrap().spec, Some(A));
        assert_eq!(merged.lookup("svc.B/N").unwrap().spec, Some(B));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "no route registered")]
    fn with_spec_unknown_route_panics_in_debug() {
        const SPEC: Spec = Spec::server("/test.Svc/Nope", StreamType::Unary);
        let _ = Router::new()
            .route("test.Svc", "Method", unary_handler())
            .with_spec(SPEC);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "idempotency disagrees")]
    fn with_spec_idempotency_mismatch_panics_in_debug() {
        // Spec declares NoSideEffects but the route was registered via
        // `route` (idempotent: false). The `idempotent` flag is what the
        // dispatch path consults to allow Connect GET, so a mismatch
        // here would silently disable GET for an idempotent method.
        const SPEC: Spec = Spec::server("/test.Svc/Method", StreamType::Unary)
            .with_idempotency_level(IdempotencyLevel::NoSideEffects);
        let _ = Router::new()
            .route("test.Svc", "Method", unary_handler())
            .with_spec(SPEC);
    }
}
