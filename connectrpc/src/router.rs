//! Request routing and service registration.
//!
//! This module provides the router for mapping RPC method paths to handlers.

use std::collections::HashMap;
use std::sync::Arc;

use buffa::Message;
use serde::Serialize;
use serde::de::DeserializeOwned;

use buffa::view::MessageView;

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

/// Router for ConnectRPC services.
///
/// The router maps service/method paths to their handlers and manages
/// request dispatching.
///
/// `Router` is the *dynamic* dispatch path: method paths are owned `String`
/// keys, so it cannot supply [`Spec::procedure`](crate::Spec::procedure)'s
/// `&'static str` and handlers receive [`RequestContext::spec`] as `None`.
/// Code that needs `Spec` (interceptors, OTel labels) should use the
/// generated `FooServiceServer<T>` dispatcher instead.
///
/// [`RequestContext::spec`]: crate::RequestContext::spec
#[derive(Default)]
pub struct Router {
    /// Map from "service_name/method_name" to handler.
    methods: HashMap<String, Method>,
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
        Req: Message + DeserializeOwned + Send + 'static,
        Res: Message + Serialize + Send + 'static,
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
        Req: Message + DeserializeOwned + Send + 'static,
        Res: Message + Serialize + Send + 'static,
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
        Req: Message + DeserializeOwned + Send + 'static,
        Res: Message + Serialize + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = UnaryHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::Unary(UnaryMethod {
                handler: Arc::new(wrapper),
                idempotent,
            }),
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
        Req: Message + DeserializeOwned + Send + 'static,
        Res: Message + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = ServerStreamingHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::Streaming(StreamingMethod {
                handler: Arc::new(wrapper),
                kind: MethodKind::ServerStreaming,
            }),
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
        Req: Message + DeserializeOwned + Send + 'static,
        Res: Message + Serialize + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = ClientStreamingHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::ClientStreaming(ClientStreamingMethod {
                handler: Arc::new(wrapper),
            }),
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
        Req: Message + DeserializeOwned + Send + 'static,
        Res: Message + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = BidiStreamingHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::BidiStreaming(BidiStreamingMethod {
                handler: Arc::new(wrapper),
            }),
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
        ReqView::Owned: Message + DeserializeOwned,
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
        ReqView::Owned: Message + DeserializeOwned,
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
        ReqView::Owned: Message + DeserializeOwned,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = UnaryViewHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::Unary(UnaryMethod {
                handler: Arc::new(wrapper),
                idempotent,
            }),
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
        ReqView::Owned: Message + DeserializeOwned,
        Res: Message + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = ServerStreamingViewHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::Streaming(StreamingMethod {
                handler: Arc::new(wrapper),
                kind: MethodKind::ServerStreaming,
            }),
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
        ReqView::Owned: Message + DeserializeOwned,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = ClientStreamingViewHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::ClientStreaming(ClientStreamingMethod {
                handler: Arc::new(wrapper),
            }),
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
        ReqView::Owned: Message + DeserializeOwned,
        Res: Message + Send + 'static,
    {
        let path = format!("{service_name}/{method_name}");
        let wrapper = BidiStreamingViewHandlerWrapper::new(handler);
        self.methods.insert(
            path,
            Method::BidiStreaming(BidiStreamingMethod {
                handler: Arc::new(wrapper),
            }),
        );
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
        match self.methods.get(path)? {
            Method::Unary(m) => Some(MethodDescriptor::unary(m.idempotent)),
            Method::Streaming(m) => Some(MethodDescriptor::from_kind(m.kind)),
            Method::ClientStreaming(_) => Some(MethodDescriptor::client_streaming()),
            Method::BidiStreaming(_) => Some(MethodDescriptor::bidi_streaming()),
        }
    }

    fn call_unary(
        &self,
        path: &str,
        ctx: crate::response::RequestContext,
        request: bytes::Bytes,
        format: crate::codec::CodecFormat,
    ) -> crate::dispatcher::UnaryResult {
        match self.methods.get(path) {
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
        match self.methods.get(path) {
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
        match self.methods.get(path) {
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
        match self.methods.get(path) {
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

    #[test]
    fn test_router_registration() {
        // This test just verifies the API compiles correctly
        // Full testing requires actual proto types
        let router = Router::new();
        assert!(!router.has_method("test.Service/Method"));
    }
}
