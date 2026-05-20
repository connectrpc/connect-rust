//! RPC-level interceptors.
//!
//! Interceptors are the typed equivalent of `tower` middleware: they wrap
//! a single RPC *after* envelope decoding, decompression, and header
//! parsing, and *before* the handler runs. Two surfaces:
//!
//! - **Unary** ([`Interceptor::intercept_unary`]): sees a [`UnaryRequest`]
//!   (the [`Spec`](crate::Spec), headers, deadline, extensions, and a
//!   lazily-decoded [`Payload`]) and a [`Next`] continuation, and returns
//!   a [`UnaryResponse`].
//! - **Streaming** ([`Interceptor::intercept_streaming`]): sees a
//!   [`StreamRequest`], an inbound [`PayloadStream`], and a [`NextStream`]
//!   continuation, and returns a [`StreamResponse`] carrying the outbound
//!   [`PayloadStream`]. One method covers server-streaming,
//!   client-streaming, and bidi by treating "one" as "stream of one".
//!
//! Streaming interceptors are **`Stream`-shaped, not connection-shaped.**
//! `connect-go` exposes a `StreamingHandlerConn` with `Receive()`/`Send()`
//! because Go handlers *push* — they call `stream.Send(res)`. Rust handlers
//! *produce* — they return a [`Stream`](futures::Stream) the framework
//! polls. There is no per-item `send()` call site to hook, so a conn
//! wrapper would need a channel intermediary plus a pump task. Wrapping
//! the inbound and outbound streams with adapters is the same expressive
//! power without that cost, and matches the rest of the Rust ecosystem
//! (`tower`, `tonic`, `axum` all work with `Stream`-shaped bodies).
//!
//! The first interceptor registered is the outermost: it runs first on
//! the way in and last on the way out, exactly like wrapping a function
//! call. This matches `connect-go`'s `WithInterceptors` ordering.
//!
//! ```text
//! request ──▶ interceptor[0] ──▶ interceptor[1] ──▶ handler
//!                  │                  │                 │
//! response ◀───────┴──────────◀───────┴────────◀───────┘
//! ```
//!
//! Register interceptors with
//! [`ConnectRpcService::with_interceptor`](crate::ConnectRpcService::with_interceptor).
//! When no interceptors are registered the dispatch path is byte-for-byte
//! identical to a build without this module — there is no per-request
//! cost for opting out.

use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::StreamExt;

use crate::codec::CodecFormat;
use crate::dispatcher::RequestStream;
use crate::error::ConnectError;
use crate::handler::BoxStream;
use crate::payload::Payload;
use crate::response::{EncodedResponse, RequestContext, Response};

/// Re-export of [`async_trait::async_trait`] so interceptor authors don't
/// need a direct `async-trait` dependency.
///
/// ```rust,ignore
/// #[connectrpc::async_trait]
/// impl connectrpc::Interceptor for MyInterceptor { /* ... */ }
/// ```
///
/// The macro expansion references only `core` and the prelude — there is
/// no runtime `async-trait` requirement.
pub use async_trait::async_trait;

/// A unary RPC interceptor.
///
/// Implement [`intercept_unary`](Interceptor::intercept_unary) to wrap a
/// call. The default implementation is a passthrough — calling
/// [`next.run(req)`](Next::run) — so an interceptor that only cares about
/// (say) streaming RPCs in a future release is forwards-compatible.
///
/// Use [`unary_interceptor`] for a closure-shaped interceptor without a
/// dedicated type.
///
/// `Interceptor` is an async trait. Annotate the impl with the
/// [`connectrpc::async_trait`](crate::async_trait) re-export — there is
/// no separate `async-trait` dependency to add.
///
/// # Example
///
/// ```rust,ignore
/// struct LoggingInterceptor;
///
/// #[connectrpc::async_trait]
/// impl Interceptor for LoggingInterceptor {
///     async fn intercept_unary(
///         &self,
///         req: UnaryRequest,
///         next: Next<'_>,
///     ) -> Result<UnaryResponse, ConnectError> {
///         // `ctx.path()` is the requested procedure path. The dispatch
///         // path always sets it before an interceptor runs, including
///         // for dynamic `Router` routes (which never carry a `Spec`) —
///         // the `expect` documents that invariant rather than hiding a
///         // default. Use `ctx.spec()` for the *resolved* method's static
///         // metadata (`stream_type`, `idempotency`), not the name.
///         //
///         // `to_owned()` because `path()` borrows `req.ctx`, and `req`
///         // is moved into `next.run` below.
///         let path = req
///             .ctx
///             .path()
///             .expect("dispatch sets path before interceptors run")
///             .to_owned();
///         tracing::info!(%path, "rpc start");
///         let resp = next.run(req).await;
///         tracing::info!(%path, ok = resp.is_ok(), "rpc end");
///         resp
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait Interceptor: Send + Sync + 'static {
    /// Wrap a unary RPC. The default is a passthrough.
    ///
    /// Call [`next.run(req)`](Next::run) to continue. Returning without
    /// calling it short-circuits the chain — neither inner interceptors
    /// nor the handler run.
    ///
    /// # Errors
    ///
    /// Forward errors from `next.run` (handler or inner-interceptor
    /// failures), or return your own to short-circuit.
    async fn intercept_unary(
        &self,
        req: UnaryRequest,
        next: Next<'_>,
    ) -> Result<UnaryResponse, ConnectError> {
        next.run(req).await
    }

    /// Wrap a streaming RPC. The default is a passthrough.
    ///
    /// Called once at stream establishment, before any messages flow.
    /// Wrap `inbound` with a [`Stream`](futures::Stream) adapter to
    /// observe, mutate, or filter incoming messages; wrap the body of the
    /// returned [`StreamResponse`] the same way. Returning without calling
    /// `next.run()` short-circuits the chain — neither inner interceptors
    /// nor the handler run. Returning `Err` aborts the stream with an
    /// error rendered in the protocol's streaming error format
    /// (`EndStreamResponse` envelope for Connect, `grpc-status` trailer
    /// for gRPC/gRPC-Web).
    ///
    /// All three streaming shapes route through this method. For
    /// server-streaming `inbound` yields exactly one item; for
    /// client-streaming the returned outbound stream yields exactly one
    /// item. Read [`Spec::stream_type`](crate::Spec::stream_type) (when
    /// [`spec()`](RequestContext::spec) is present) to branch on
    /// cardinality.
    ///
    /// Cross-stream coordination — making a decision on an outbound item
    /// based on what was observed inbound — needs shared state between the
    /// two stream adapters (e.g. an `Arc<Mutex<..>>` captured by both).
    /// This is rare; most interceptors observe one direction or none.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// #[connectrpc::async_trait]
    /// impl Interceptor for AuthInterceptor {
    ///     async fn intercept_streaming(
    ///         &self,
    ///         req: StreamRequest,
    ///         inbound: PayloadStream,
    ///         next: NextStream<'_>,
    ///     ) -> Result<StreamResponse, ConnectError> {
    ///         // Auth runs once at establishment, not per message.
    ///         let path = req.ctx.path().expect("dispatch sets path");
    ///         self.authorize(path, req.ctx.headers()).await?;
    ///         next.run(req, inbound).await
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Forward errors from `next.run`, or return your own to short-circuit.
    async fn intercept_streaming(
        &self,
        req: StreamRequest,
        inbound: PayloadStream,
        next: NextStream<'_>,
    ) -> Result<StreamResponse, ConnectError> {
        next.run(req, inbound).await
    }
}

/// Construct an [`Interceptor`] from a closure.
///
/// The closure must be a higher-ranked `Fn` over the [`Next`] lifetime
/// that returns a boxed future. The boilerplate is unavoidable — the
/// trait method returns a boxed future — but the closure body is what
/// you'd write in an `impl Interceptor` block:
///
/// ```rust,ignore
/// let timing = unary_interceptor(|req, next| Box::pin(async move {
///     let started = std::time::Instant::now();
///     let resp = next.run(req).await;
///     tracing::debug!(elapsed = ?started.elapsed(), "rpc");
///     resp
/// }));
/// ```
pub fn unary_interceptor<F>(f: F) -> impl Interceptor
where
    F: for<'a> Fn(UnaryRequest, Next<'a>) -> BoxFuture<'a, Result<UnaryResponse, ConnectError>>
        + Send
        + Sync
        + 'static,
{
    struct FnInterceptor<F>(F);

    #[async_trait::async_trait]
    impl<F> Interceptor for FnInterceptor<F>
    where
        F: for<'a> Fn(UnaryRequest, Next<'a>) -> BoxFuture<'a, Result<UnaryResponse, ConnectError>>
            + Send
            + Sync
            + 'static,
    {
        async fn intercept_unary(
            &self,
            req: UnaryRequest,
            next: Next<'_>,
        ) -> Result<UnaryResponse, ConnectError> {
            (self.0)(req, next).await
        }
    }

    FnInterceptor(f)
}

/// The continuation an [`Interceptor`] calls to run the rest of the chain.
///
/// `Next` holds the still-to-run interceptors and the terminal handler.
/// [`run`](Next::run) consumes it: an interceptor can call `next.run(req)`
/// at most once. Not calling it at all short-circuits the chain.
pub struct Next<'a> {
    rest: &'a [Arc<dyn Interceptor>],
    terminal: &'a (dyn UnaryTerminal + 'a),
}

impl<'a> Next<'a> {
    /// Construct the head of a chain.
    pub(crate) fn new(
        rest: &'a [Arc<dyn Interceptor>],
        terminal: &'a (dyn UnaryTerminal + 'a),
    ) -> Self {
        Self { rest, terminal }
    }

    /// Run the rest of the chain — the next interceptor if any, otherwise
    /// the terminal handler — and return its response.
    ///
    /// # Errors
    ///
    /// Returns whatever error the next interceptor or handler produced.
    pub async fn run(self, req: UnaryRequest) -> Result<UnaryResponse, ConnectError> {
        match self.rest.split_first() {
            Some((head, tail)) => {
                head.intercept_unary(
                    req,
                    Next {
                        rest: tail,
                        terminal: self.terminal,
                    },
                )
                .await
            }
            None => self.terminal.call(req).await,
        }
    }
}

impl std::fmt::Debug for Next<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Next")
            .field("remaining", &self.rest.len())
            .finish_non_exhaustive()
    }
}

/// The terminal step of an interceptor chain: decode the request body,
/// invoke the handler, encode the response.
///
/// `pub(crate)` because the only producer is the dispatch path. Tests
/// inside the crate can supply mocks.
#[async_trait::async_trait]
pub(crate) trait UnaryTerminal: Send + Sync {
    async fn call(&self, req: UnaryRequest) -> Result<UnaryResponse, ConnectError>;
}

/// A unary RPC request as seen by an [`Interceptor`].
///
/// Carries the dispatch [`RequestContext`] (headers, deadline,
/// extensions, [`Spec`](crate::Spec), negotiated protocol) and the
/// lazily-decoded body. Both fields are public so an interceptor can
/// rewrite headers, inject extensions, or replace the message and pass
/// the mutated request to [`Next::run`].
///
/// `ctx.spec` is `Some(..)` for generated `FooServiceServer<T>`
/// dispatchers and `None` for the dynamic [`Router`](crate::Router)
/// (its method paths are owned `String`s, not `&'static str`).
///
/// `#[non_exhaustive]` so future fields can be added without a
/// breaking change. Construct with [`UnaryRequest::new`]; destructure
/// with a trailing `..`.
#[derive(Debug)]
#[non_exhaustive]
pub struct UnaryRequest {
    /// The dispatch context. Mutating `ctx.headers` or `ctx.extensions`
    /// before `next.run` propagates to the handler.
    pub ctx: RequestContext,
    /// The lazily-decoded request body. Call
    /// [`set_message`](Payload::set_message) to replace it.
    pub payload: Payload,
}

impl UnaryRequest {
    /// Build a `UnaryRequest` from a dispatch context and wire-encoded
    /// body. Used by the dispatch path and by test fixtures.
    pub fn new(ctx: RequestContext, body: Bytes, format: CodecFormat) -> Self {
        Self {
            ctx,
            payload: Payload::new(body, format),
        }
    }
}

/// A unary RPC response as seen by an [`Interceptor`].
///
/// Carries response metadata (headers, trailers, compression hint) and
/// a lazily-decoded body, with the same shape as the handler-facing
/// [`Response<B>`](crate::Response). All fields are public so an
/// interceptor can read or rewrite the response on the way out.
pub type UnaryResponse = Response<Payload>;

impl UnaryResponse {
    /// Build a `UnaryResponse` from an encoded handler response.
    pub fn from_encoded(resp: EncodedResponse, format: CodecFormat) -> Self {
        Response {
            body: Payload::new(resp.body, format),
            headers: resp.headers,
            trailers: resp.trailers,
            compress: resp.compress,
        }
    }

    /// Convert back to the dispatch path's encoded form.
    ///
    /// # Errors
    ///
    /// Returns an error if a replacement set with
    /// [`Payload::set_message`] fails to re-encode.
    pub fn into_encoded(self) -> Result<EncodedResponse, ConnectError> {
        Ok(Response {
            body: self.body.encoded()?,
            headers: self.headers,
            trailers: self.trailers,
            compress: self.compress,
        })
    }
}

// ============================================================================
// Streaming
// ============================================================================

/// A stream of lazily-decoded message bodies, as seen by an
/// [`Interceptor::intercept_streaming`].
///
/// The same type is used for both the inbound (client → server) and
/// outbound (server → client) directions. Each item is a [`Payload`] —
/// lazy decode, so an interceptor that never inspects message bodies pays
/// only the per-item struct construction (no decode, no allocation beyond
/// the wire `Bytes` refcount the dispatch path already holds).
pub type PayloadStream = BoxStream<Result<Payload, ConnectError>>;

/// A streaming RPC request as seen by an [`Interceptor`].
///
/// Mirrors [`UnaryRequest`] minus the `Payload` — stream messages arrive
/// through the `inbound` [`PayloadStream`] passed to
/// [`Interceptor::intercept_streaming`].
///
/// `#[non_exhaustive]` so future fields can be added without a breaking
/// change. Construct with [`StreamRequest::new`]; destructure with a
/// trailing `..`.
#[derive(Debug)]
#[non_exhaustive]
pub struct StreamRequest {
    /// The dispatch context. Mutating `ctx.headers` or `ctx.extensions`
    /// before `next.run` propagates to the handler.
    pub ctx: RequestContext,
}

impl StreamRequest {
    /// Build a `StreamRequest` from a dispatch context. Used by the
    /// dispatch path and by test fixtures.
    pub fn new(ctx: RequestContext) -> Self {
        Self { ctx }
    }
}

/// A streaming RPC response as seen by an [`Interceptor`].
///
/// Carries response metadata (headers, trailers, compression hint) and
/// the outbound [`PayloadStream`]. All fields are public so an interceptor
/// can read or rewrite the response on the way out — wrap `body` with a
/// [`Stream`](futures::Stream) adapter to observe or mutate outbound
/// messages, or [`with_header`](Response::with_header) /
/// [`with_trailer`](Response::with_trailer) to set metadata.
///
/// Note the trailers are set by the handler **before** the body stream is
/// drained — an interceptor cannot delay setting a trailer until it has
/// seen the last outbound item.
pub type StreamResponse = Response<PayloadStream>;

impl StreamResponse {
    /// Build a `StreamResponse` from a dispatcher's encoded streaming
    /// response by wrapping each body item in a [`Payload`].
    pub fn from_encoded(
        resp: Response<BoxStream<Result<Bytes, ConnectError>>>,
        format: CodecFormat,
    ) -> Self {
        resp.map_body(move |stream| -> PayloadStream {
            Box::pin(stream.map(move |item| item.map(|bytes| Payload::new(bytes, format))))
        })
    }

    /// Convert back to the dispatch path's encoded form by re-encoding
    /// each [`Payload`].
    ///
    /// Items whose [`Payload::encoded`] fails (a replacement that failed
    /// to re-encode) become `Err` entries in the stream, which the
    /// dispatch path renders as a streaming error and then ends the
    /// stream. There is no fallible up-front conversion: stream items
    /// haven't been produced yet.
    pub fn into_encoded(self) -> Response<BoxStream<Result<Bytes, ConnectError>>> {
        self.map_body(|stream| -> BoxStream<Result<Bytes, ConnectError>> {
            Box::pin(stream.map(|item| item.and_then(|payload| payload.encoded())))
        })
    }
}

/// Construct an [`Interceptor`] from a streaming closure.
///
/// The streaming counterpart of [`unary_interceptor`]. The closure must be
/// a higher-ranked `Fn` over the [`NextStream`] lifetime that returns a
/// boxed future. The boilerplate is unavoidable — the trait method returns
/// a boxed future — but the closure body is what you'd write in an
/// `impl Interceptor` block:
///
/// ```rust,ignore
/// let logging = streaming_interceptor(|req, inbound, next| Box::pin(async move {
///     tracing::info!(path = req.ctx.path(), "stream open");
///     next.run(req, inbound).await
/// }));
/// ```
pub fn streaming_interceptor<F>(f: F) -> impl Interceptor
where
    F: for<'a> Fn(
            StreamRequest,
            PayloadStream,
            NextStream<'a>,
        ) -> BoxFuture<'a, Result<StreamResponse, ConnectError>>
        + Send
        + Sync
        + 'static,
{
    struct FnInterceptor<F>(F);

    #[async_trait::async_trait]
    impl<F> Interceptor for FnInterceptor<F>
    where
        F: for<'a> Fn(
                StreamRequest,
                PayloadStream,
                NextStream<'a>,
            ) -> BoxFuture<'a, Result<StreamResponse, ConnectError>>
            + Send
            + Sync
            + 'static,
    {
        async fn intercept_streaming(
            &self,
            req: StreamRequest,
            inbound: PayloadStream,
            next: NextStream<'_>,
        ) -> Result<StreamResponse, ConnectError> {
            (self.0)(req, inbound, next).await
        }
    }

    FnInterceptor(f)
}

/// The continuation an [`Interceptor`] calls to run the rest of a
/// streaming chain.
///
/// `NextStream` holds the still-to-run interceptors and the terminal
/// handler. [`run`](NextStream::run) consumes it: an interceptor can call
/// `next.run(req, inbound)` at most once. Not calling it short-circuits.
pub struct NextStream<'a> {
    rest: &'a [Arc<dyn Interceptor>],
    terminal: &'a (dyn StreamTerminal + 'a),
}

impl<'a> NextStream<'a> {
    /// Construct the head of a chain.
    pub(crate) fn new(
        rest: &'a [Arc<dyn Interceptor>],
        terminal: &'a (dyn StreamTerminal + 'a),
    ) -> Self {
        Self { rest, terminal }
    }

    /// Run the rest of the chain — the next interceptor if any, otherwise
    /// the terminal handler — and return its response.
    ///
    /// # Errors
    ///
    /// Returns whatever error the next interceptor or handler produced.
    pub async fn run(
        self,
        req: StreamRequest,
        inbound: PayloadStream,
    ) -> Result<StreamResponse, ConnectError> {
        match self.rest.split_first() {
            Some((head, tail)) => {
                head.intercept_streaming(
                    req,
                    inbound,
                    NextStream {
                        rest: tail,
                        terminal: self.terminal,
                    },
                )
                .await
            }
            None => self.terminal.call(req, inbound).await,
        }
    }
}

impl std::fmt::Debug for NextStream<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextStream")
            .field("remaining", &self.rest.len())
            .finish_non_exhaustive()
    }
}

/// The terminal step of a streaming interceptor chain: hand the inbound
/// stream to the dispatcher and wrap the outbound stream.
///
/// `pub(crate)` because the only producer is the dispatch path. Tests
/// inside the crate can supply mocks.
#[async_trait::async_trait]
pub(crate) trait StreamTerminal: Send + Sync {
    async fn call(
        &self,
        req: StreamRequest,
        inbound: PayloadStream,
    ) -> Result<StreamResponse, ConnectError>;
}

/// Run a streaming interceptor chain against a closure terminal.
///
/// The streaming counterpart of [`run_chain`]. For **unit-testing** an
/// [`Interceptor`] without a tower service or TCP listener.
///
/// ```rust,ignore
/// let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(MyInterceptor)];
/// let resp = connectrpc::interceptor::run_chain_streaming(
///     &chain,
///     my_req,
///     my_inbound_stream,
///     |req, inbound| async move {
///         // assert what the handler would see
///         Ok(StreamResponse::from_encoded(/* ... */, CodecFormat::Proto))
///     },
/// )
/// .await?;
/// ```
///
/// # Errors
///
/// Returns whatever error the chain or terminal produces.
pub async fn run_chain_streaming<F, Fut>(
    interceptors: &[Arc<dyn Interceptor>],
    req: StreamRequest,
    inbound: PayloadStream,
    terminal: F,
) -> Result<StreamResponse, ConnectError>
where
    F: Fn(StreamRequest, PayloadStream) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<StreamResponse, ConnectError>> + Send,
{
    struct FnTerminal<F>(F);

    #[async_trait::async_trait]
    impl<F, Fut> StreamTerminal for FnTerminal<F>
    where
        F: Fn(StreamRequest, PayloadStream) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<StreamResponse, ConnectError>> + Send,
    {
        async fn call(
            &self,
            req: StreamRequest,
            inbound: PayloadStream,
        ) -> Result<StreamResponse, ConnectError> {
            (self.0)(req, inbound).await
        }
    }

    let terminal = FnTerminal(terminal);
    NextStream::new(interceptors, &terminal)
        .run(req, inbound)
        .await
}

// ============================================================================
// Streaming dispatch glue
// ============================================================================

/// Convert a [`PayloadStream`] back to the dispatcher's `RequestStream`
/// (raw `Bytes`) by re-encoding each [`Payload`].
fn payload_stream_to_request_stream(stream: PayloadStream) -> RequestStream {
    Box::pin(stream.map(|item| item.and_then(|payload| payload.encoded())))
}

/// Wrap a dispatcher inbound `RequestStream` (raw `Bytes`) as a
/// [`PayloadStream`] for the interceptor chain.
fn request_stream_to_payload_stream(stream: RequestStream, format: CodecFormat) -> PayloadStream {
    Box::pin(stream.map(move |item| item.map(|bytes| Payload::new(bytes, format))))
}

/// Run a server-streaming call through the interceptor chain, or skip
/// straight to the dispatcher when there are no interceptors.
///
/// Server-streaming has a single request `Bytes`, presented to the
/// interceptor as a 1-item inbound stream. The terminal pulls the single
/// item back out and hands it to [`Dispatcher::call_server_streaming`].
pub(crate) async fn call_server_streaming_intercepted<D: crate::Dispatcher>(
    dispatcher: &D,
    interceptors: &[Arc<dyn Interceptor>],
    path: &str,
    ctx: RequestContext,
    body: Bytes,
    format: CodecFormat,
) -> Result<Response<BoxStream<Result<Bytes, ConnectError>>>, ConnectError> {
    if interceptors.is_empty() {
        return dispatcher
            .call_server_streaming(path, ctx, body, format)
            .await;
    }
    let terminal = ServerStreamingTerminal {
        dispatcher,
        path,
        format,
    };
    let req = StreamRequest::new(ctx);
    let inbound: PayloadStream = Box::pin(futures::stream::once(async move {
        Ok(Payload::new(body, format))
    }));
    let resp = NextStream::new(interceptors, &terminal)
        .run(req, inbound)
        .await?;
    Ok(resp.into_encoded())
}

/// Run a client-streaming call through the interceptor chain.
///
/// Client-streaming has an inbound stream and a single response, presented
/// to the chain as a 1-item outbound stream. `call_client_streaming_intercepted`
/// pulls the single item back out for the dispatch path.
pub(crate) async fn call_client_streaming_intercepted<D: crate::Dispatcher>(
    dispatcher: &D,
    interceptors: &[Arc<dyn Interceptor>],
    path: &str,
    ctx: RequestContext,
    requests: RequestStream,
    format: CodecFormat,
) -> Result<EncodedResponse, ConnectError> {
    if interceptors.is_empty() {
        return dispatcher
            .call_client_streaming(path, ctx, requests, format)
            .await;
    }
    let terminal = ClientStreamingTerminal {
        dispatcher,
        path,
        format,
    };
    let req = StreamRequest::new(ctx);
    let inbound = request_stream_to_payload_stream(requests, format);
    let resp = NextStream::new(interceptors, &terminal)
        .run(req, inbound)
        .await?;
    // The terminal produced a 1-item outbound stream; collapse it. The
    // single item carries the (possibly replaced) response body. An
    // interceptor that filtered the item away is a programming error;
    // fail with `Internal` rather than send an empty response.
    let Response {
        body: mut stream,
        headers,
        trailers,
        compress,
    } = resp;
    let body = match stream.next().await {
        Some(Ok(payload)) => payload.encoded()?,
        Some(Err(e)) => return Err(e),
        None => {
            return Err(ConnectError::internal(
                "client-streaming interceptor consumed the response without replacing it",
            ));
        }
    };
    Ok(Response {
        body,
        headers,
        trailers,
        compress,
    })
}

/// Run a bidi-streaming call through the interceptor chain.
pub(crate) async fn call_bidi_streaming_intercepted<D: crate::Dispatcher>(
    dispatcher: &D,
    interceptors: &[Arc<dyn Interceptor>],
    path: &str,
    ctx: RequestContext,
    requests: RequestStream,
    format: CodecFormat,
) -> Result<Response<BoxStream<Result<Bytes, ConnectError>>>, ConnectError> {
    if interceptors.is_empty() {
        return dispatcher
            .call_bidi_streaming(path, ctx, requests, format)
            .await;
    }
    let terminal = BidiStreamingTerminal {
        dispatcher,
        path,
        format,
    };
    let req = StreamRequest::new(ctx);
    let inbound = request_stream_to_payload_stream(requests, format);
    let resp = NextStream::new(interceptors, &terminal)
        .run(req, inbound)
        .await?;
    Ok(resp.into_encoded())
}

/// `StreamTerminal` that hands off to the dispatcher's
/// `call_server_streaming`.
struct ServerStreamingTerminal<'a, D> {
    dispatcher: &'a D,
    path: &'a str,
    format: CodecFormat,
}

#[async_trait::async_trait]
impl<D: crate::Dispatcher> StreamTerminal for ServerStreamingTerminal<'_, D> {
    async fn call(
        &self,
        req: StreamRequest,
        mut inbound: PayloadStream,
    ) -> Result<StreamResponse, ConnectError> {
        // The dispatch path provided a 1-item inbound stream. An
        // interceptor that filtered it away is a programming error;
        // fail rather than dispatch with no body.
        let body = match inbound.next().await {
            Some(Ok(payload)) => payload.encoded()?,
            Some(Err(e)) => return Err(e),
            None => {
                return Err(ConnectError::internal(
                    "server-streaming interceptor consumed the request without replacing it",
                ));
            }
        };
        let resp = self
            .dispatcher
            .call_server_streaming(self.path, req.ctx, body, self.format)
            .await?;
        Ok(StreamResponse::from_encoded(resp, self.format))
    }
}

/// `StreamTerminal` that hands off to the dispatcher's
/// `call_client_streaming`.
struct ClientStreamingTerminal<'a, D> {
    dispatcher: &'a D,
    path: &'a str,
    format: CodecFormat,
}

#[async_trait::async_trait]
impl<D: crate::Dispatcher> StreamTerminal for ClientStreamingTerminal<'_, D> {
    async fn call(
        &self,
        req: StreamRequest,
        inbound: PayloadStream,
    ) -> Result<StreamResponse, ConnectError> {
        let requests = payload_stream_to_request_stream(inbound);
        let resp = self
            .dispatcher
            .call_client_streaming(self.path, req.ctx, requests, self.format)
            .await?;
        let format = self.format;
        // Wrap the single response in a 1-item outbound stream so the
        // chain has a uniform type. `call_client_streaming_intercepted`
        // pulls it back out for the dispatch path.
        Ok(resp.map_body(move |bytes| -> PayloadStream {
            Box::pin(futures::stream::once(async move {
                Ok(Payload::new(bytes, format))
            }))
        }))
    }
}

/// `StreamTerminal` that hands off to the dispatcher's
/// `call_bidi_streaming`.
struct BidiStreamingTerminal<'a, D> {
    dispatcher: &'a D,
    path: &'a str,
    format: CodecFormat,
}

#[async_trait::async_trait]
impl<D: crate::Dispatcher> StreamTerminal for BidiStreamingTerminal<'_, D> {
    async fn call(
        &self,
        req: StreamRequest,
        inbound: PayloadStream,
    ) -> Result<StreamResponse, ConnectError> {
        let requests = payload_stream_to_request_stream(inbound);
        let resp = self
            .dispatcher
            .call_bidi_streaming(self.path, req.ctx, requests, self.format)
            .await?;
        Ok(StreamResponse::from_encoded(resp, self.format))
    }
}

/// Run an interceptor chain against a closure terminal.
///
/// The dispatch path constructs [`Next`] internally; this helper is for
/// **unit-testing** an [`Interceptor`] without spinning up a tower
/// service or a TCP listener. The `terminal` closure stands in for the
/// handler.
///
/// ```rust,ignore
/// let trace = Arc::new(Mutex::new(Vec::new()));
/// let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(MyInterceptor)];
/// let resp = connectrpc::interceptor::run_chain(&chain, my_req, |req| async move {
///     // assert what the handler would see
///     Ok(UnaryResponse::from_encoded(EncodedResponse::new(Bytes::new()), CodecFormat::Proto))
/// })
/// .await?;
/// ```
///
/// # Errors
///
/// Returns whatever error the chain or terminal produces.
pub async fn run_chain<F, Fut>(
    interceptors: &[Arc<dyn Interceptor>],
    req: UnaryRequest,
    terminal: F,
) -> Result<UnaryResponse, ConnectError>
where
    F: Fn(UnaryRequest) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<UnaryResponse, ConnectError>> + Send,
{
    struct FnTerminal<F>(F);

    #[async_trait::async_trait]
    impl<F, Fut> UnaryTerminal for FnTerminal<F>
    where
        F: Fn(UnaryRequest) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<UnaryResponse, ConnectError>> + Send,
    {
        async fn call(&self, req: UnaryRequest) -> Result<UnaryResponse, ConnectError> {
            (self.0)(req).await
        }
    }

    let terminal = FnTerminal(terminal);
    Next::new(interceptors, &terminal).run(req).await
}

/// Run a unary call through the interceptor chain, or skip straight to the
/// dispatcher when there are no interceptors.
///
/// The empty-chain path makes a single `is_empty` check and delegates;
/// it does not build a [`UnaryRequest`], a [`Next`], or any chain
/// machinery. The [`Payload::new`] wrap is plain struct construction —
/// no allocation, no decode — so a service with no interceptors pays
/// nothing.
pub(crate) async fn call_unary_intercepted<D: crate::Dispatcher>(
    dispatcher: &D,
    interceptors: &[Arc<dyn Interceptor>],
    path: &str,
    ctx: RequestContext,
    body: Bytes,
    format: CodecFormat,
) -> Result<EncodedResponse, ConnectError> {
    if interceptors.is_empty() {
        return dispatcher
            .call_unary(path, ctx, Payload::new(body, format), format)
            .await;
    }
    let terminal = DispatchTerminal {
        dispatcher,
        path,
        format,
    };
    let req = UnaryRequest::new(ctx, body, format);
    let resp = Next::new(interceptors, &terminal).run(req).await?;
    resp.into_encoded()
}

/// `UnaryTerminal` that hands off to the dispatcher's `call_unary`.
struct DispatchTerminal<'a, D> {
    dispatcher: &'a D,
    path: &'a str,
    format: CodecFormat,
}

#[async_trait::async_trait]
impl<D: crate::Dispatcher> UnaryTerminal for DispatchTerminal<'_, D> {
    async fn call(&self, req: UnaryRequest) -> Result<UnaryResponse, ConnectError> {
        let UnaryRequest { ctx, payload } = req;
        // Hand the Payload — not raw bytes — to the dispatcher, so an
        // owned-message handler can reuse a decode an interceptor cached.
        let resp = self
            .dispatcher
            .call_unary(self.path, ctx, payload, self.format)
            .await?;
        Ok(UnaryResponse::from_encoded(resp, self.format))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_proto;
    use buffa_types::google::protobuf::StringValue;
    use std::sync::Mutex;

    /// A terminal that records whether it ran and returns a fixed body.
    struct RecordingTerminal {
        ran: Mutex<bool>,
        respond_with: &'static str,
    }

    #[async_trait::async_trait]
    impl UnaryTerminal for RecordingTerminal {
        async fn call(&self, req: UnaryRequest) -> Result<UnaryResponse, ConnectError> {
            *self.ran.lock().unwrap() = true;
            // Echo the (possibly replaced) request body length in a header
            // so tests can verify mutation reached the terminal.
            let in_len = req.payload.encoded()?.len().to_string();
            let body = encode_proto(&StringValue {
                value: self.respond_with.into(),
                ..Default::default()
            })?;
            let mut resp = EncodedResponse::new(body);
            resp.headers.insert("x-in-len", in_len.parse().unwrap());
            Ok(UnaryResponse::from_encoded(resp, CodecFormat::Proto))
        }
    }

    fn req() -> UnaryRequest {
        let body = encode_proto(&StringValue {
            value: "hi".into(),
            ..Default::default()
        })
        .unwrap();
        UnaryRequest::new(RequestContext::default(), body, CodecFormat::Proto)
    }

    /// Interceptor that pushes a label into the request extensions on the
    /// way in and prepends it to a response header on the way out, so the
    /// test can assert nesting order.
    struct Tagger(&'static str);

    #[derive(Clone, Default)]
    struct Trace(Arc<Mutex<Vec<&'static str>>>);

    #[async_trait::async_trait]
    impl Interceptor for Tagger {
        async fn intercept_unary(
            &self,
            mut req: UnaryRequest,
            next: Next<'_>,
        ) -> Result<UnaryResponse, ConnectError> {
            req.ctx
                .extensions
                .get_or_insert_default::<Trace>()
                .0
                .lock()
                .unwrap()
                .push(self.0);
            let resp = next.run(req).await?;
            Ok(resp.with_header("x-trace", format!("{}-out", self.0)))
        }
    }

    #[tokio::test]
    async fn ordering_first_registered_is_outermost() {
        let trace = Trace::default();
        let chain: Vec<Arc<dyn Interceptor>> = vec![
            Arc::new(Tagger("a")),
            Arc::new(Tagger("b")),
            Arc::new(Tagger("c")),
        ];
        let terminal = RecordingTerminal {
            ran: Mutex::new(false),
            respond_with: "ok",
        };
        let mut request = req();
        request.ctx.extensions.insert(trace.clone());
        let resp = Next::new(&chain, &terminal).run(request).await.unwrap();
        assert!(*terminal.ran.lock().unwrap(), "terminal should have run");
        // Way in: outermost first.
        assert_eq!(*trace.0.lock().unwrap(), vec!["a", "b", "c"]);
        // Way out: innermost appends to headers first (HeaderMap::append
        // preserves insertion order), so "c-out" is first and "a-out" last.
        let outs: Vec<_> = resp
            .headers
            .get_all("x-trace")
            .iter()
            .map(|v| v.to_str().unwrap().to_owned())
            .collect();
        assert_eq!(outs, vec!["c-out", "b-out", "a-out"]);
    }

    #[tokio::test]
    async fn short_circuit_skips_terminal() {
        struct Reject;
        #[async_trait::async_trait]
        impl Interceptor for Reject {
            async fn intercept_unary(
                &self,
                _req: UnaryRequest,
                _next: Next<'_>,
            ) -> Result<UnaryResponse, ConnectError> {
                // Auth interceptors attach diagnostic headers (e.g. an
                // operator-facing "which policy denied" hint) to the deny
                // error. Those must reach the wire response.
                let mut headers = http::HeaderMap::new();
                headers.insert("x-deny-policy", "p1".parse().unwrap());
                Err(ConnectError::permission_denied("nope").with_headers(headers))
            }
        }
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(Reject), Arc::new(Tagger("never"))];
        let terminal = RecordingTerminal {
            ran: Mutex::new(false),
            respond_with: "ok",
        };
        let err = Next::new(&chain, &terminal).run(req()).await.unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::PermissionDenied);
        assert!(!*terminal.ran.lock().unwrap(), "terminal must not run");
        // The chain must not strip response headers off a short-circuit
        // error: they reach the dispatch path and the protocol-aware error
        // renderers (`error_response`, `grpc_error_response`,
        // `ConnectError::into_http_response`) walk `response_headers()`
        // when building the wire response.
        assert_eq!(
            err.response_headers().get("x-deny-policy").unwrap(),
            "p1",
            "diagnostic headers on a short-circuit error must survive the chain"
        );
    }

    /// `call_unary_intercepted` propagates a short-circuit error verbatim,
    /// including response headers, so the caller's error renderer can put
    /// them on the wire. Pinned because an auth interceptor relies on it.
    #[tokio::test]
    async fn call_unary_intercepted_propagates_error_headers() {
        struct Reject;
        #[async_trait::async_trait]
        impl Interceptor for Reject {
            async fn intercept_unary(
                &self,
                _req: UnaryRequest,
                _next: Next<'_>,
            ) -> Result<UnaryResponse, ConnectError> {
                let mut headers = http::HeaderMap::new();
                headers.insert("x-deny-policy", "p1".parse().unwrap());
                Err(ConnectError::permission_denied("nope").with_headers(headers))
            }
        }
        struct PanickyDispatcher;
        impl crate::Dispatcher for PanickyDispatcher {
            fn lookup(&self, _: &str) -> Option<crate::dispatcher::MethodDescriptor> {
                None
            }
            fn call_unary(
                &self,
                _: &str,
                _: RequestContext,
                _: Payload,
                _: CodecFormat,
            ) -> crate::dispatcher::UnaryResult {
                unreachable!("dispatcher must not be reached when an interceptor short-circuits")
            }
            fn call_server_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: Bytes,
                _: CodecFormat,
            ) -> crate::dispatcher::StreamingResult {
                unreachable!()
            }
            fn call_client_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: crate::dispatcher::RequestStream,
                _: CodecFormat,
            ) -> crate::dispatcher::UnaryResult {
                unreachable!()
            }
            fn call_bidi_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: crate::dispatcher::RequestStream,
                _: CodecFormat,
            ) -> crate::dispatcher::StreamingResult {
                unreachable!()
            }
        }
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(Reject)];
        let err = call_unary_intercepted(
            &PanickyDispatcher,
            &chain,
            "p",
            RequestContext::default(),
            Bytes::new(),
            CodecFormat::Proto,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::PermissionDenied);
        assert_eq!(err.response_headers().get("x-deny-policy").unwrap(), "p1");
    }

    #[tokio::test]
    async fn mutation_replaces_request_body() {
        struct Replace;
        #[async_trait::async_trait]
        impl Interceptor for Replace {
            async fn intercept_unary(
                &self,
                mut req: UnaryRequest,
                next: Next<'_>,
            ) -> Result<UnaryResponse, ConnectError> {
                req.payload.set_message(StringValue {
                    value: "rewritten by interceptor".into(),
                    ..Default::default()
                });
                next.run(req).await
            }
        }
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(Replace)];
        let terminal = RecordingTerminal {
            ran: Mutex::new(false),
            respond_with: "ok",
        };
        let resp = Next::new(&chain, &terminal).run(req()).await.unwrap();
        // The terminal re-encoded the replaced message; its length differs
        // from the original ("hi" -> 4 bytes) and is recorded in the header.
        let in_len: usize = resp
            .headers
            .get("x-in-len")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let original_len = req().payload.encoded().unwrap().len();
        assert_ne!(in_len, original_len, "terminal should see the replacement");
    }

    #[tokio::test]
    async fn closure_interceptor_works() {
        let i = unary_interceptor(|req, next| {
            Box::pin(async move {
                let resp = next.run(req).await?;
                Ok(resp.with_header("x-fn", "1"))
            })
        });
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(i)];
        // Exercise the public test helper that downstream crates use.
        let resp = run_chain(&chain, req(), |_| async {
            Ok(UnaryResponse::from_encoded(
                EncodedResponse::new(Bytes::new()),
                CodecFormat::Proto,
            ))
        })
        .await
        .unwrap();
        assert_eq!(resp.headers.get("x-fn").unwrap(), "1");
    }

    /// Trailers and the compression hint must round-trip through a
    /// passthrough chain — `into_encoded` preserves all `Response`
    /// metadata, not just the body.
    #[tokio::test]
    async fn passthrough_chain_preserves_response_metadata() {
        struct Passthrough;
        #[async_trait::async_trait]
        impl Interceptor for Passthrough {}
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(Passthrough)];
        let resp = run_chain(&chain, req(), |_| async {
            let mut r = EncodedResponse::new(Bytes::from_static(b"x"));
            r.headers.insert("x-h", "1".parse().unwrap());
            r.trailers.insert("x-t", "2".parse().unwrap());
            r.compress = Some(true);
            Ok(UnaryResponse::from_encoded(r, CodecFormat::Proto))
        })
        .await
        .unwrap();
        let encoded = resp.into_encoded().unwrap();
        assert_eq!(encoded.headers.get("x-h").unwrap(), "1");
        assert_eq!(encoded.trailers.get("x-t").unwrap(), "2");
        assert_eq!(encoded.compress, Some(true));
        assert_eq!(&*encoded.body, b"x");
    }

    #[tokio::test]
    async fn empty_chain_is_no_op() {
        // `call_unary_intercepted` with an empty slice delegates straight
        // to the dispatcher. The response bytes must come straight from
        // the dispatcher (refcount-shared with the request that the echo
        // dispatcher returned). Note: a `Bytes` clone shares the backing
        // pointer, so this test alone doesn't *uniquely* prove the
        // `UnaryRequest`-free fast path — that property is guarded by the
        // conformance suite, which only ever runs the empty chain.
        struct Echo;
        impl crate::Dispatcher for Echo {
            fn lookup(&self, _: &str) -> Option<crate::dispatcher::MethodDescriptor> {
                None
            }
            fn call_unary(
                &self,
                _: &str,
                _: RequestContext,
                request: Payload,
                _: CodecFormat,
            ) -> crate::dispatcher::UnaryResult {
                Box::pin(async move { Ok(EncodedResponse::new(request.encoded()?)) })
            }
            fn call_server_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: Bytes,
                _: CodecFormat,
            ) -> crate::dispatcher::StreamingResult {
                unimplemented!()
            }
            fn call_client_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: crate::dispatcher::RequestStream,
                _: CodecFormat,
            ) -> crate::dispatcher::UnaryResult {
                unimplemented!()
            }
            fn call_bidi_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: crate::dispatcher::RequestStream,
                _: CodecFormat,
            ) -> crate::dispatcher::StreamingResult {
                unimplemented!()
            }
        }
        let body = Bytes::from_static(b"x");
        let resp = call_unary_intercepted(
            &Echo,
            &[],
            "p",
            RequestContext::default(),
            body.clone(),
            CodecFormat::Proto,
        )
        .await
        .unwrap();
        // Same backing storage — no copy through Payload.
        assert!(std::ptr::eq(resp.body.as_ptr(), body.as_ptr()));
    }

    /// `DispatchTerminal` hands the `Payload` — not raw bytes — to the
    /// dispatcher, so an owned-message handler can `take_message()` and
    /// reuse the decode an interceptor cached.
    ///
    /// Pinned by handing the dispatcher a `Payload` whose wire bytes are
    /// *garbage* but whose cache an interceptor populated by replacement.
    /// If the terminal stripped the `Payload` to bytes (the pre-this-PR
    /// behavior), the dispatcher's `take_message` would error on the
    /// garbage; if it forwards the `Payload`, the dispatcher sees the
    /// replacement.
    #[tokio::test]
    async fn dispatch_terminal_forwards_payload_to_handler() {
        let captured = Arc::new(Mutex::new(None::<String>));

        // A dispatcher that decodes via `take_message` — the path an
        // owned-message `Router::route` handler takes.
        struct Capture(Arc<Mutex<Option<String>>>);
        impl crate::Dispatcher for Capture {
            fn lookup(&self, _: &str) -> Option<crate::dispatcher::MethodDescriptor> {
                None
            }
            fn call_unary(
                &self,
                _: &str,
                _: RequestContext,
                request: Payload,
                _: CodecFormat,
            ) -> crate::dispatcher::UnaryResult {
                let captured = Arc::clone(&self.0);
                Box::pin(async move {
                    let m: StringValue = request.take_message()?;
                    *captured.lock().unwrap() = Some(m.value);
                    Ok(EncodedResponse::new(Bytes::new()))
                })
            }
            fn call_server_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: Bytes,
                _: CodecFormat,
            ) -> crate::dispatcher::StreamingResult {
                unreachable!()
            }
            fn call_client_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: crate::dispatcher::RequestStream,
                _: CodecFormat,
            ) -> crate::dispatcher::UnaryResult {
                unreachable!()
            }
            fn call_bidi_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: crate::dispatcher::RequestStream,
                _: CodecFormat,
            ) -> crate::dispatcher::StreamingResult {
                unreachable!()
            }
        }

        // Interceptor that replaces the request body. The original wire
        // bytes are garbage — only the replacement is valid.
        struct Replace;
        #[async_trait::async_trait]
        impl Interceptor for Replace {
            async fn intercept_unary(
                &self,
                mut req: UnaryRequest,
                next: Next<'_>,
            ) -> Result<UnaryResponse, ConnectError> {
                req.payload.set_message(StringValue {
                    value: "from interceptor".into(),
                    ..Default::default()
                });
                next.run(req).await
            }
        }

        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(Replace)];
        call_unary_intercepted(
            &Capture(Arc::clone(&captured)),
            &chain,
            "p",
            RequestContext::default(),
            // Garbage wire bytes: would error on a fresh decode.
            Bytes::from_static(&[0xff, 0xff, 0xff]),
            CodecFormat::Proto,
        )
        .await
        .unwrap();

        assert_eq!(
            captured.lock().unwrap().as_deref(),
            Some("from interceptor"),
            "the dispatcher must see the interceptor's replacement, not re-decode the wire bytes"
        );
    }

    // ========================================================================
    // Streaming interceptor tests
    // ========================================================================

    /// Build a `PayloadStream` from string values.
    fn payload_stream(values: &[&'static str]) -> PayloadStream {
        let items: Vec<Result<Payload, ConnectError>> = values
            .iter()
            .map(|v| {
                let bytes = encode_proto(&StringValue {
                    value: (*v).into(),
                    ..Default::default()
                })
                .unwrap();
                Ok(Payload::new(bytes, CodecFormat::Proto))
            })
            .collect();
        Box::pin(futures::stream::iter(items))
    }

    /// Drain a `PayloadStream` to decoded `StringValue`s.
    async fn collect_strings(stream: PayloadStream) -> Vec<String> {
        stream
            .map(|item| {
                item.unwrap()
                    .message::<StringValue>()
                    .unwrap()
                    .value
                    .clone()
            })
            .collect()
            .await
    }

    /// Streaming counterpart of `Tagger`: pushes a label into request
    /// extensions inbound, appends to a header outbound.
    struct StreamTagger(&'static str);

    #[async_trait::async_trait]
    impl Interceptor for StreamTagger {
        async fn intercept_streaming(
            &self,
            mut req: StreamRequest,
            inbound: PayloadStream,
            next: NextStream<'_>,
        ) -> Result<StreamResponse, ConnectError> {
            req.ctx
                .extensions
                .get_or_insert_default::<Trace>()
                .0
                .lock()
                .unwrap()
                .push(self.0);
            let resp = next.run(req, inbound).await?;
            Ok(resp.with_header("x-trace", format!("{}-out", self.0)))
        }
    }

    /// A streaming terminal that records whether it ran, drains the
    /// inbound stream into a header, and produces a fixed outbound stream.
    struct RecordingStreamTerminal {
        ran: Mutex<bool>,
        respond_with: Vec<&'static str>,
    }

    #[async_trait::async_trait]
    impl StreamTerminal for RecordingStreamTerminal {
        async fn call(
            &self,
            _req: StreamRequest,
            inbound: PayloadStream,
        ) -> Result<StreamResponse, ConnectError> {
            *self.ran.lock().unwrap() = true;
            let inbound_values = collect_strings(inbound).await;
            let body: PayloadStream = payload_stream(&self.respond_with);
            let resp = Response {
                body,
                headers: http::HeaderMap::new(),
                trailers: http::HeaderMap::new(),
                compress: None,
            };
            Ok(resp.with_header("x-inbound", inbound_values.join(",")))
        }
    }

    fn stream_req() -> StreamRequest {
        StreamRequest::new(RequestContext::default())
    }

    #[tokio::test]
    async fn streaming_ordering_first_registered_is_outermost() {
        let trace = Trace::default();
        let chain: Vec<Arc<dyn Interceptor>> = vec![
            Arc::new(StreamTagger("a")),
            Arc::new(StreamTagger("b")),
            Arc::new(StreamTagger("c")),
        ];
        let terminal = RecordingStreamTerminal {
            ran: Mutex::new(false),
            respond_with: vec!["ok"],
        };
        let mut request = stream_req();
        request.ctx.extensions.insert(trace.clone());
        let resp = NextStream::new(&chain, &terminal)
            .run(request, payload_stream(&["x"]))
            .await
            .unwrap();
        assert!(*terminal.ran.lock().unwrap(), "terminal should have run");
        // Way in: outermost first.
        assert_eq!(*trace.0.lock().unwrap(), vec!["a", "b", "c"]);
        // Way out: innermost appends first.
        let outs: Vec<_> = resp
            .headers
            .get_all("x-trace")
            .iter()
            .map(|v| v.to_str().unwrap().to_owned())
            .collect();
        assert_eq!(outs, vec!["c-out", "b-out", "a-out"]);
    }

    #[tokio::test]
    async fn streaming_short_circuit_skips_terminal() {
        struct Reject;
        #[async_trait::async_trait]
        impl Interceptor for Reject {
            async fn intercept_streaming(
                &self,
                _req: StreamRequest,
                _inbound: PayloadStream,
                _next: NextStream<'_>,
            ) -> Result<StreamResponse, ConnectError> {
                let mut headers = http::HeaderMap::new();
                headers.insert("x-deny-policy", "p1".parse().unwrap());
                Err(ConnectError::permission_denied("nope").with_headers(headers))
            }
        }
        let chain: Vec<Arc<dyn Interceptor>> =
            vec![Arc::new(Reject), Arc::new(StreamTagger("never"))];
        let terminal = RecordingStreamTerminal {
            ran: Mutex::new(false),
            respond_with: vec!["ok"],
        };
        let err = match NextStream::new(&chain, &terminal)
            .run(stream_req(), payload_stream(&["x"]))
            .await
        {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert_eq!(err.code, crate::ErrorCode::PermissionDenied);
        assert!(!*terminal.ran.lock().unwrap(), "terminal must not run");
        // Diagnostic headers on a short-circuit error must survive the
        // chain so the dispatch path's streaming error renderer can put
        // them on the wire.
        assert_eq!(
            err.response_headers().get("x-deny-policy").unwrap(),
            "p1",
            "diagnostic headers must survive a streaming short-circuit"
        );
    }

    #[tokio::test]
    async fn streaming_passthrough_preserves_items_and_metadata() {
        struct Passthrough;
        #[async_trait::async_trait]
        impl Interceptor for Passthrough {}
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(Passthrough)];
        let resp = run_chain_streaming(
            &chain,
            stream_req(),
            payload_stream(&["a", "b"]),
            |_req, inbound| async move {
                let inbound_values = collect_strings(inbound).await;
                let body: PayloadStream = payload_stream(&["x", "y", "z"]);
                let mut r = Response {
                    body,
                    headers: http::HeaderMap::new(),
                    trailers: http::HeaderMap::new(),
                    compress: Some(true),
                };
                r.headers.insert("x-h", "1".parse().unwrap());
                r.trailers.insert("x-t", "2".parse().unwrap());
                r.headers
                    .insert("x-inbound", inbound_values.join(",").parse().unwrap());
                Ok(r)
            },
        )
        .await
        .unwrap();
        assert_eq!(resp.headers.get("x-h").unwrap(), "1");
        assert_eq!(resp.trailers.get("x-t").unwrap(), "2");
        assert_eq!(resp.compress, Some(true));
        assert_eq!(resp.headers.get("x-inbound").unwrap(), "a,b");
        let out = collect_strings(resp.body).await;
        assert_eq!(out, vec!["x", "y", "z"]);
    }

    #[tokio::test]
    async fn streaming_interceptor_wraps_inbound() {
        /// Replaces every inbound message with `"redacted"`.
        struct RedactInbound;
        #[async_trait::async_trait]
        impl Interceptor for RedactInbound {
            async fn intercept_streaming(
                &self,
                req: StreamRequest,
                inbound: PayloadStream,
                next: NextStream<'_>,
            ) -> Result<StreamResponse, ConnectError> {
                let wrapped: PayloadStream = Box::pin(inbound.map(|item| {
                    item.map(|mut payload| {
                        payload.set_message(StringValue {
                            value: "redacted".into(),
                            ..Default::default()
                        });
                        payload
                    })
                }));
                next.run(req, wrapped).await
            }
        }
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(RedactInbound)];
        let resp = run_chain_streaming(
            &chain,
            stream_req(),
            payload_stream(&["secret-a", "secret-b"]),
            |_req, inbound| async move {
                let inbound_values = collect_strings(inbound).await;
                let body: PayloadStream = payload_stream(&[]);
                let resp = Response {
                    body,
                    headers: http::HeaderMap::new(),
                    trailers: http::HeaderMap::new(),
                    compress: None,
                };
                Ok(resp.with_header("x-inbound", inbound_values.join(",")))
            },
        )
        .await
        .unwrap();
        // The terminal saw the wrapped inbound stream.
        assert_eq!(resp.headers.get("x-inbound").unwrap(), "redacted,redacted");
    }

    #[tokio::test]
    async fn streaming_interceptor_wraps_outbound() {
        /// Replaces every outbound message with `"redacted"`.
        struct RedactOutbound;
        #[async_trait::async_trait]
        impl Interceptor for RedactOutbound {
            async fn intercept_streaming(
                &self,
                req: StreamRequest,
                inbound: PayloadStream,
                next: NextStream<'_>,
            ) -> Result<StreamResponse, ConnectError> {
                let resp = next.run(req, inbound).await?;
                Ok(resp.map_body(|stream| -> PayloadStream {
                    Box::pin(stream.map(|item| {
                        item.map(|mut payload| {
                            payload.set_message(StringValue {
                                value: "redacted".into(),
                                ..Default::default()
                            });
                            payload
                        })
                    }))
                }))
            }
        }
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(RedactOutbound)];
        let terminal = RecordingStreamTerminal {
            ran: Mutex::new(false),
            respond_with: vec!["secret-1", "secret-2"],
        };
        let resp = NextStream::new(&chain, &terminal)
            .run(stream_req(), payload_stream(&["x"]))
            .await
            .unwrap();
        let out = collect_strings(resp.body).await;
        assert_eq!(out, vec!["redacted", "redacted"]);
    }

    #[tokio::test]
    async fn streaming_closure_interceptor_works() {
        let i = streaming_interceptor(|req, inbound, next| {
            Box::pin(async move {
                let resp = next.run(req, inbound).await?;
                Ok(resp.with_header("x-fn", "1"))
            })
        });
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(i)];
        let resp = run_chain_streaming(
            &chain,
            stream_req(),
            payload_stream(&[]),
            |_req, _in| async {
                let body: PayloadStream = payload_stream(&[]);
                Ok(Response {
                    body,
                    headers: http::HeaderMap::new(),
                    trailers: http::HeaderMap::new(),
                    compress: None,
                })
            },
        )
        .await
        .unwrap();
        assert_eq!(resp.headers.get("x-fn").unwrap(), "1");
    }

    /// A `Dispatcher` mock for testing the streaming dispatch glue.
    /// Echoes the inbound items as the outbound stream.
    struct StreamEcho;
    impl crate::Dispatcher for StreamEcho {
        fn lookup(&self, _: &str) -> Option<crate::dispatcher::MethodDescriptor> {
            None
        }
        fn call_unary(
            &self,
            _: &str,
            _: RequestContext,
            _: Payload,
            _: CodecFormat,
        ) -> crate::dispatcher::UnaryResult {
            unimplemented!()
        }
        fn call_server_streaming(
            &self,
            _: &str,
            _: RequestContext,
            request: Bytes,
            _: CodecFormat,
        ) -> crate::dispatcher::StreamingResult {
            Box::pin(async move {
                let body: BoxStream<Result<Bytes, ConnectError>> =
                    Box::pin(futures::stream::once(async move { Ok(request) }));
                Ok(Response {
                    body,
                    headers: http::HeaderMap::new(),
                    trailers: http::HeaderMap::new(),
                    compress: None,
                })
            })
        }
        fn call_client_streaming(
            &self,
            _: &str,
            _: RequestContext,
            requests: crate::dispatcher::RequestStream,
            _: CodecFormat,
        ) -> crate::dispatcher::UnaryResult {
            Box::pin(async move {
                let mut total = 0usize;
                let mut requests = requests;
                while let Some(item) = requests.next().await {
                    total += item?.len();
                }
                Ok(EncodedResponse::new(Bytes::from(total.to_string())))
            })
        }
        fn call_bidi_streaming(
            &self,
            _: &str,
            _: RequestContext,
            requests: crate::dispatcher::RequestStream,
            _: CodecFormat,
        ) -> crate::dispatcher::StreamingResult {
            Box::pin(async move {
                Ok(Response {
                    body: requests,
                    headers: http::HeaderMap::new(),
                    trailers: http::HeaderMap::new(),
                    compress: None,
                })
            })
        }
    }

    /// All three `call_*_streaming_intercepted` empty-chain fast paths
    /// delegate straight to the dispatcher with no `PayloadStream` /
    /// `NextStream` overhead. Verified by pointer-equality on the
    /// echoed body bytes.
    #[tokio::test]
    async fn streaming_empty_chain_is_no_op() {
        // Server-streaming.
        let body = Bytes::from_static(b"x");
        let resp = call_server_streaming_intercepted(
            &StreamEcho,
            &[],
            "p",
            RequestContext::default(),
            body.clone(),
            CodecFormat::Proto,
        )
        .await
        .unwrap();
        let out: Vec<_> = resp.body.collect().await;
        assert_eq!(out.len(), 1);
        assert!(std::ptr::eq(
            out[0].as_ref().unwrap().as_ptr(),
            body.as_ptr()
        ));

        // Client-streaming.
        let inbound: RequestStream = Box::pin(futures::stream::iter(vec![
            Ok(Bytes::from_static(b"ab")),
            Ok(Bytes::from_static(b"cd")),
        ]));
        let resp = call_client_streaming_intercepted(
            &StreamEcho,
            &[],
            "p",
            RequestContext::default(),
            inbound,
            CodecFormat::Proto,
        )
        .await
        .unwrap();
        assert_eq!(&*resp.body, b"4");

        // Bidi-streaming.
        let body = Bytes::from_static(b"z");
        let inbound: RequestStream = Box::pin(futures::stream::once({
            let body = body.clone();
            async move { Ok(body) }
        }));
        let resp = call_bidi_streaming_intercepted(
            &StreamEcho,
            &[],
            "p",
            RequestContext::default(),
            inbound,
            CodecFormat::Proto,
        )
        .await
        .unwrap();
        let out: Vec<_> = resp.body.collect().await;
        assert_eq!(out.len(), 1);
        assert!(std::ptr::eq(
            out[0].as_ref().unwrap().as_ptr(),
            body.as_ptr()
        ));
    }

    /// `call_*_streaming_intercepted` propagates a short-circuit error
    /// verbatim, including response headers, without invoking the
    /// dispatcher. Pinned because an auth interceptor relies on it.
    #[tokio::test]
    async fn call_streaming_intercepted_propagates_error_headers() {
        struct Reject;
        #[async_trait::async_trait]
        impl Interceptor for Reject {
            async fn intercept_streaming(
                &self,
                _req: StreamRequest,
                _inbound: PayloadStream,
                _next: NextStream<'_>,
            ) -> Result<StreamResponse, ConnectError> {
                let mut headers = http::HeaderMap::new();
                headers.insert("x-deny-policy", "p1".parse().unwrap());
                Err(ConnectError::permission_denied("nope").with_headers(headers))
            }
        }
        struct PanickyDispatcher;
        impl crate::Dispatcher for PanickyDispatcher {
            fn lookup(&self, _: &str) -> Option<crate::dispatcher::MethodDescriptor> {
                None
            }
            fn call_unary(
                &self,
                _: &str,
                _: RequestContext,
                _: Payload,
                _: CodecFormat,
            ) -> crate::dispatcher::UnaryResult {
                unreachable!()
            }
            fn call_server_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: Bytes,
                _: CodecFormat,
            ) -> crate::dispatcher::StreamingResult {
                unreachable!("dispatcher must not run when an interceptor short-circuits")
            }
            fn call_client_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: crate::dispatcher::RequestStream,
                _: CodecFormat,
            ) -> crate::dispatcher::UnaryResult {
                unreachable!("dispatcher must not run when an interceptor short-circuits")
            }
            fn call_bidi_streaming(
                &self,
                _: &str,
                _: RequestContext,
                _: crate::dispatcher::RequestStream,
                _: CodecFormat,
            ) -> crate::dispatcher::StreamingResult {
                unreachable!("dispatcher must not run when an interceptor short-circuits")
            }
        }
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(Reject)];

        let err = match call_server_streaming_intercepted(
            &PanickyDispatcher,
            &chain,
            "p",
            RequestContext::default(),
            Bytes::new(),
            CodecFormat::Proto,
        )
        .await
        {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert_eq!(err.code, crate::ErrorCode::PermissionDenied);
        assert_eq!(err.response_headers().get("x-deny-policy").unwrap(), "p1");

        let err = call_client_streaming_intercepted(
            &PanickyDispatcher,
            &chain,
            "p",
            RequestContext::default(),
            Box::pin(futures::stream::empty()),
            CodecFormat::Proto,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::PermissionDenied);

        let err = match call_bidi_streaming_intercepted(
            &PanickyDispatcher,
            &chain,
            "p",
            RequestContext::default(),
            Box::pin(futures::stream::empty()),
            CodecFormat::Proto,
        )
        .await
        {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert_eq!(err.code, crate::ErrorCode::PermissionDenied);
    }

    /// The three `call_*_streaming_intercepted` un-unify correctly —
    /// server-streaming pulls a 1-item inbound stream, client-streaming
    /// collapses a 1-item outbound stream — through a passthrough chain.
    #[tokio::test]
    async fn streaming_intercepted_un_unifies_through_passthrough_chain() {
        struct Passthrough;
        #[async_trait::async_trait]
        impl Interceptor for Passthrough {}
        let chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(Passthrough)];

        // Server-streaming: single body in → 1-item inbound stream → terminal
        // pulls it → echo dispatcher returns a 1-item outbound stream.
        let body = Bytes::from_static(b"ss");
        let resp = call_server_streaming_intercepted(
            &StreamEcho,
            &chain,
            "p",
            RequestContext::default(),
            body.clone(),
            CodecFormat::Proto,
        )
        .await
        .unwrap();
        let out: Vec<_> = resp.body.collect().await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].as_ref().unwrap(), &body);

        // Client-streaming: 2-item inbound stream → terminal hands stream
        // to dispatcher → dispatcher's single response collapses to body.
        let inbound: RequestStream = Box::pin(futures::stream::iter(vec![
            Ok(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"de")),
        ]));
        let resp = call_client_streaming_intercepted(
            &StreamEcho,
            &chain,
            "p",
            RequestContext::default(),
            inbound,
            CodecFormat::Proto,
        )
        .await
        .unwrap();
        assert_eq!(&*resp.body, b"5");

        // Bidi: 2-item inbound → echo dispatcher returns it as outbound.
        let inbound: RequestStream = Box::pin(futures::stream::iter(vec![
            Ok(Bytes::from_static(b"1")),
            Ok(Bytes::from_static(b"2")),
        ]));
        let resp = call_bidi_streaming_intercepted(
            &StreamEcho,
            &chain,
            "p",
            RequestContext::default(),
            inbound,
            CodecFormat::Proto,
        )
        .await
        .unwrap();
        let out: Vec<_> = resp.body.collect().await;
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].as_ref().unwrap(), &Bytes::from_static(b"1"));
        assert_eq!(out[1].as_ref().unwrap(), &Bytes::from_static(b"2"));
    }
}
