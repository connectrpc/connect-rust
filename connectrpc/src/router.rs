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

/// Registers a generated service implementation with a [`Router`].
///
/// Implementations are emitted by `connectrpc-codegen`. The `Marker` type
/// distinguishes implementations for different generated service traits,
/// allowing a concrete Rust type to implement more than one service without
/// conflicting blanket implementations.
///
/// Most callers do not name this trait or its marker type directly. Pass an
/// `Arc`-wrapped service to [`Router::add_service`] and type inference selects
/// the generated implementation.
pub trait ServiceRegister<Marker> {
    /// Register this service's RPC methods with `router`.
    fn register_service(self, router: Router) -> Router;
}

/// Error returned by [`Router::try_merge`] and
/// [`Router::try_merge_in_place`] when both routers register one or more of
/// the same method paths.
///
/// Only produced when [`Router::allow_overrides`] was not set; with overrides
/// enabled, colliding routes are replaced and no error is returned. Read the
/// conflicting paths with [`conflicting_paths`](Self::conflicting_paths).
#[derive(Debug, Clone, thiserror::Error)]
#[error("router merge conflict on path(s): {conflicts:?}")]
#[non_exhaustive]
pub struct RouterMergeError {
    conflicts: Vec<String>,
}

impl RouterMergeError {
    /// The procedure paths registered by both routers, sorted.
    #[must_use]
    pub fn conflicting_paths(&self) -> &[String] {
        &self.conflicts
    }
}

/// Router for ConnectRPC services.
///
/// The router maps service/method paths to their handlers and manages
/// request dispatching.
///
/// Most users should register generated services with the generated
/// `<Service>Ext::register` extension trait instead of manually calling the
/// low-level route registration helpers:
///
/// ```rust,ignore
/// let router = Arc::new(MyService).register(Router::new());
/// ```
///
/// `Router` is the *dynamic* dispatch path: method paths are owned `String`
/// keys. It can still carry a [`Spec`] when one is attached with
/// [`with_spec`](Self::with_spec). Generated registration does this for every
/// method, so a `Router` built through codegen surfaces
/// [`RequestContext::spec`] just like the monomorphic
/// `FooServiceServer<T>` dispatcher does.
///
/// [`RequestContext::spec`]: crate::RequestContext::spec
#[derive(Default)]
pub struct Router {
    /// Map from "service_name/method_name" to handler.
    methods: HashMap<String, RegisteredMethod>,
    /// When `true`, [`merge`](Self::merge) replaces colliding routes instead
    /// of panicking. Off by default so an accidental path collision fails
    /// loudly. Set via [`allow_overrides`](Self::allow_overrides).
    allow_overrides: bool,
}

impl Router {
    /// Create a new empty router.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a generated service implementation.
    ///
    /// This is the top-down equivalent of calling the generated
    /// `FooServiceExt::register` extension method:
    ///
    /// ```rust,ignore
    /// let router = Router::new()
    ///     .add_service(Arc::new(greeter))
    ///     .add_service(Arc::new(echo));
    /// ```
    ///
    /// The generated marker type is normally inferred. If one concrete type
    /// implements multiple generated service traits, `add_service` cannot
    /// infer which one; register through the generated extension trait
    /// directly instead — it is public and takes no marker:
    ///
    /// ```rust,ignore
    /// let router = FooServiceExt::register(Arc::new(service), router);
    /// ```
    ///
    /// If the compiler reports that `ServiceRegister` is not satisfied, the
    /// service type does not implement the generated service trait — check
    /// that the value is `Arc`-wrapped and that the trait `impl` is in scope.
    ///
    /// # Panics
    ///
    /// Panics if a registered method path already exists on this router — for
    /// example registering the same service twice — unless
    /// [`allow_overrides`](Self::allow_overrides) was called first. Distinct
    /// services never collide, since each path is prefixed with the
    /// fully-qualified service name.
    #[must_use]
    // `?Sized` keeps the door open for a hand-written
    // `impl ServiceRegister<M> for Arc<dyn Trait>`; codegen only ever emits
    // impls for `Arc<S>` with a concrete, sized `S`.
    pub fn add_service<S: ?Sized, Marker>(self, service: Arc<S>) -> Self
    where
        Arc<S>: ServiceRegister<Marker>,
    {
        <Arc<S> as ServiceRegister<Marker>>::register_service(service, self)
    }

    /// Allow registrations and merges into this router to replace routes whose
    /// paths already exist, instead of failing on the collision.
    ///
    /// By default any operation that would overwrite an existing path fails, so
    /// a duplicate surfaces instead of silently shadowing a route. This governs
    /// the whole router: [`add_service`](Self::add_service) and the generated
    /// `register` panic on a duplicate path; [`merge`](Self::merge) /
    /// [`merge_in_place`](Self::merge_in_place) panic; and
    /// [`try_merge`](Self::try_merge) / [`try_merge_in_place`](Self::try_merge_in_place)
    /// return a [`RouterMergeError`]. Opt into last-wins replacement when that
    /// is what you intend — for example layering an override router over
    /// defaults:
    ///
    /// ```rust,ignore
    /// let router = defaults.allow_overrides().merge(overrides);
    /// ```
    ///
    /// The mode is read from the router being merged *into*, so call it on that
    /// router (`defaults.allow_overrides().merge(overrides)`, not
    /// `defaults.merge(overrides.allow_overrides())` — the latter drops the
    /// flag and still fails). It is also sticky: once set it stays on for every
    /// later merge into this router.
    #[must_use]
    pub fn allow_overrides(mut self) -> Self {
        self.allow_overrides = true;
        self
    }

    /// Merge another router into this router, returning the combined router.
    ///
    /// The owned, chainable counterpart of
    /// [`merge_in_place`](Self::merge_in_place); see
    /// [`merge_routers`] to combine many routers at once, and
    /// [`try_merge`](Self::try_merge) for the non-panicking variant.
    ///
    /// # Panics
    ///
    /// Panics if both routers register the same method path, unless
    /// [`allow_overrides`](Self::allow_overrides) was called on `self` first —
    /// in which case the route from `other` replaces the existing one.
    #[must_use]
    pub fn merge(mut self, other: Self) -> Self {
        self.merge_in_place(other);
        self
    }

    /// Move all routes from another router into this router in place.
    ///
    /// The `&mut` counterpart of [`merge`](Self::merge), for accumulating into
    /// an existing `Router` binding.
    ///
    /// # Panics
    ///
    /// Panics if both routers register the same method path, unless
    /// [`allow_overrides`](Self::allow_overrides) was called on `self` first —
    /// in which case the route from `other` replaces the existing one. Use
    /// [`try_merge_in_place`](Self::try_merge_in_place) to handle collisions
    /// without panicking.
    pub fn merge_in_place(&mut self, other: Self) {
        if let Err(err) = self.try_merge_in_place(other) {
            panic!(
                "router merge conflict on path(s) {:?} — both routers register \
                 these paths. Call `allow_overrides()` if replacing the existing \
                 routes is intended.",
                err.conflicting_paths()
            );
        }
    }

    /// Merge another router into this router, returning an error instead of
    /// panicking on a path collision.
    ///
    /// The fallible counterpart of [`merge`](Self::merge), for assembling a
    /// router from dynamic or untrusted input where a collision is a condition
    /// to handle rather than a programming error.
    ///
    /// # Errors
    ///
    /// Returns [`RouterMergeError`] listing every path registered by both
    /// routers, unless [`allow_overrides`](Self::allow_overrides) was called on
    /// `self` first. On error `self` is dropped; to keep the original router so
    /// you can retry (for example with overrides enabled), use
    /// [`try_merge_in_place`](Self::try_merge_in_place), which leaves `self`
    /// untouched on error.
    pub fn try_merge(mut self, other: Self) -> Result<Self, RouterMergeError> {
        self.try_merge_in_place(other)?;
        Ok(self)
    }

    /// Move all routes from another router into this router in place, returning
    /// an error instead of panicking on a path collision.
    ///
    /// The `&mut` counterpart of [`try_merge`](Self::try_merge).
    ///
    /// # Errors
    ///
    /// Returns [`RouterMergeError`] listing every path registered by both
    /// routers, unless [`allow_overrides`](Self::allow_overrides) was called on
    /// `self` first. The operation is transactional: on error `self` is left
    /// unchanged and no routes from `other` are added.
    pub fn try_merge_in_place(&mut self, other: Self) -> Result<(), RouterMergeError> {
        if !self.allow_overrides {
            let mut conflicts: Vec<String> = other
                .methods
                .keys()
                .filter(|path| self.methods.contains_key(*path))
                .cloned()
                .collect();
            if !conflicts.is_empty() {
                conflicts.sort();
                return Err(RouterMergeError { conflicts });
            }
        }
        self.methods.extend(other.methods);
        Ok(())
    }

    /// Register a unary RPC handler.
    ///
    #[doc(hidden)]
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
    #[doc(hidden)]
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
    /// Insert a registered method, enforcing the same path-collision rule as
    /// [`merge`](Self::merge): registering a path that already exists panics
    /// unless [`allow_overrides`](Self::allow_overrides) was set, in which case
    /// the new route replaces the old one.
    ///
    /// # Panics
    ///
    /// Panics if a route is already registered at `path` and overrides are not
    /// enabled. This catches double-registering a service (e.g. calling
    /// [`add_service`](Self::add_service) twice with the same service).
    fn insert_method(&mut self, path: String, method: RegisteredMethod) {
        if !self.allow_overrides && self.methods.contains_key(&path) {
            panic!(
                "router registration conflict on path {path:?} — a route is \
                 already registered here (registering the same service twice?). \
                 Call `allow_overrides()` if replacing it is intended."
            );
        }
        self.methods.insert(path, method);
    }

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
        self.insert_method(
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
    #[doc(hidden)]
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
        self.insert_method(
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
    #[doc(hidden)]
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
        self.insert_method(
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
    #[doc(hidden)]
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
        self.insert_method(
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
    #[doc(hidden)]
    pub fn route_view<H, ReqView>(self, service_name: &str, method_name: &str, handler: H) -> Self
    where
        H: ViewHandler<ReqView>,
        ReqView: MessageView<'static> + Send + Sync + 'static,
        ReqView::Owned: Message + JsonDeserialize,
    {
        self.route_view_internal(service_name, method_name, handler, false)
    }

    /// Register an idempotent unary RPC handler that uses zero-copy request views.
    #[doc(hidden)]
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
        self.insert_method(
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
    #[doc(hidden)]
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
        self.insert_method(
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
    #[doc(hidden)]
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
        self.insert_method(
            path,
            Method::ClientStreaming(ClientStreamingMethod {
                handler: Arc::new(wrapper),
            })
            .into(),
        );
        self
    }

    /// Register a bidi streaming RPC handler that uses zero-copy request views.
    #[doc(hidden)]
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
        self.insert_method(
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
    /// The route must already exist. [`Spec::procedure`] is used as the lookup
    /// key after its leading slash is stripped. Generated
    /// `FooServiceExt::register` calls this after registering each method so
    /// the dynamic `Router` carries the same per-method metadata as the
    /// monomorphic `FooServiceServer<T>`. Most users do not need to call this
    /// directly.
    ///
    /// # Panics
    ///
    /// Debug builds panic if no route is registered at `spec.procedure`, or if
    /// a unary route's Connect GET eligibility disagrees with
    /// `spec.idempotency_level`. Both indicate a programming error in
    /// generated registration or a call to `with_spec` before the matching
    /// handler was registered.
    ///
    /// Release builds leave the router unchanged when no matching route
    /// exists. The idempotency consistency check is debug-only.
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
///
/// Equivalent to folding [`Router::merge_in_place`] over `routers`.
///
/// # Panics
///
/// Panics if two of the routers register the same method path. To combine
/// routers with intentionally overlapping paths (last wins), fold them onto a
/// router built with [`Router::allow_overrides`] instead, e.g.
/// `routers.into_iter().fold(Router::new().allow_overrides(), Router::merge)`.
pub fn merge_routers(routers: impl IntoIterator<Item = Router>) -> Router {
    let mut merged = Router::new();
    for router in routers {
        merged.merge_in_place(router);
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

    struct TestService;
    struct TestServiceRegisterMarker;

    impl ServiceRegister<TestServiceRegisterMarker> for Arc<TestService> {
        fn register_service(self, router: Router) -> Router {
            router.route("test.Service", "Method", unary_handler())
        }
    }

    #[test]
    fn add_service_forwards_to_generated_registration() {
        let router = Router::new().add_service(Arc::new(TestService));
        assert!(router.has_method("test.Service/Method"));
    }

    struct DualService;
    struct DualServiceAMarker;
    struct DualServiceBMarker;

    impl ServiceRegister<DualServiceAMarker> for Arc<DualService> {
        fn register_service(self, router: Router) -> Router {
            router.route("test.DualA", "Call", unary_handler())
        }
    }

    impl ServiceRegister<DualServiceBMarker> for Arc<DualService> {
        fn register_service(self, router: Router) -> Router {
            router.route("test.DualB", "Call", unary_handler())
        }
    }

    #[test]
    fn add_service_disambiguates_multi_impl_with_turbofish() {
        // One concrete type implementing two service traits needs the marker
        // turbofish; this locks in that the documented disambiguation compiles.
        let router = Router::new()
            .add_service::<_, DualServiceAMarker>(Arc::new(DualService))
            .add_service::<_, DualServiceBMarker>(Arc::new(DualService));
        assert!(router.has_method("test.DualA/Call"));
        assert!(router.has_method("test.DualB/Call"));
    }

    #[test]
    #[should_panic(expected = "router registration conflict")]
    fn add_service_panics_on_double_registration() {
        // Registering the same service twice collides on its method path and
        // fails loudly, like a conflicting merge.
        let _ = Router::new()
            .add_service(Arc::new(TestService))
            .add_service(Arc::new(TestService));
    }

    #[test]
    fn add_service_with_allow_overrides_permits_re_registration() {
        let router = Router::new()
            .allow_overrides()
            .add_service(Arc::new(TestService))
            .add_service(Arc::new(TestService));
        assert!(router.has_method("test.Service/Method"));
    }

    #[test]
    #[should_panic(expected = "router registration conflict")]
    fn route_panics_on_duplicate_path() {
        let _ = Router::new()
            .route("test.Service", "Method", unary_handler())
            .route("test.Service", "Method", unary_handler());
    }

    #[test]
    fn merge_and_merge_in_place_combine_routes() {
        let first = Router::new().route("test.First", "Call", unary_handler());
        let second = Router::new().route("test.Second", "Call", unary_handler());
        let mut router = first.merge(second);

        router.merge_in_place(Router::new().route("test.Third", "Call", unary_handler()));

        assert!(router.has_method("test.First/Call"));
        assert!(router.has_method("test.Second/Call"));
        assert!(router.has_method("test.Third/Call"));
    }

    #[test]
    #[should_panic(expected = "router merge conflict")]
    fn merge_panics_on_duplicate_path_by_default() {
        let original = Router::new().route("test.Service", "Method", unary_handler());
        let replacement = Router::new().route("test.Service", "Method", unary_handler());
        let _ = original.merge(replacement);
    }

    #[test]
    fn merge_with_allow_overrides_replaces_duplicate_routes() {
        let original = Router::new().route("test.Service", "Method", unary_handler());
        let replacement = Router::new().route_idempotent("test.Service", "Method", unary_handler());

        let router = original.allow_overrides().merge(replacement);
        let descriptor = router.lookup("test.Service/Method").expect("route exists");
        assert!(descriptor.idempotent);
    }

    #[test]
    fn try_merge_ok_when_paths_are_disjoint() {
        let first = Router::new().route("test.First", "Call", unary_handler());
        let second = Router::new().route("test.Second", "Call", unary_handler());

        let router = first.try_merge(second).expect("disjoint merge succeeds");
        assert!(router.has_method("test.First/Call"));
        assert!(router.has_method("test.Second/Call"));
    }

    #[test]
    fn try_merge_reports_conflicting_paths_without_panicking() {
        let original = Router::new().route("test.Service", "Method", unary_handler());
        let other = Router::new().route("test.Service", "Method", unary_handler());

        // `Router` is not `Debug`, so match rather than `expect_err`.
        let Err(err) = original.try_merge(other) else {
            panic!("conflict must error");
        };
        assert_eq!(err.conflicting_paths(), ["test.Service/Method".to_string()]);
    }

    #[test]
    fn try_merge_in_place_is_transactional_on_conflict() {
        let mut router = Router::new()
            .route("test.Keep", "Call", unary_handler())
            .route("test.Service", "Method", unary_handler());
        let other = Router::new()
            .route("test.Service", "Method", unary_handler())
            .route("test.New", "Call", unary_handler());

        let err = router
            .try_merge_in_place(other)
            .expect_err("conflict must error");
        assert_eq!(err.conflicting_paths(), ["test.Service/Method".to_string()]);
        // On error nothing from `other` is added; existing routes are untouched.
        assert!(router.has_method("test.Keep/Call"));
        assert!(!router.has_method("test.New/Call"));
    }

    #[test]
    fn try_merge_with_allow_overrides_replaces_and_returns_ok() {
        let original = Router::new().route("test.Service", "Method", unary_handler());
        let replacement = Router::new().route_idempotent("test.Service", "Method", unary_handler());

        let router = original
            .allow_overrides()
            .try_merge(replacement)
            .expect("overrides suppress the conflict error");
        assert!(router.lookup("test.Service/Method").unwrap().idempotent);
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
