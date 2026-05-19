//! RPC-level interceptors for unary calls.
//!
//! Interceptors are the typed equivalent of `tower` middleware: they wrap
//! a single RPC *after* envelope decoding, decompression, and header
//! parsing, and *before* the handler runs. Each interceptor sees a
//! [`UnaryRequest`] (the [`Spec`](crate::Spec), headers, deadline,
//! extensions, and a lazily-decoded [`Payload`]) and a [`Next`]
//! continuation, and returns a [`UnaryResponse`].
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

use crate::codec::CodecFormat;
use crate::error::ConnectError;
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
/// machinery. A service with no interceptors pays nothing.
pub(crate) async fn call_unary_intercepted<D: crate::Dispatcher>(
    dispatcher: &D,
    interceptors: &[Arc<dyn Interceptor>],
    path: &str,
    ctx: RequestContext,
    body: Bytes,
    format: CodecFormat,
) -> Result<EncodedResponse, ConnectError> {
    if interceptors.is_empty() {
        return dispatcher.call_unary(path, ctx, body, format).await;
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
        let body = payload.encoded()?;
        let resp = self
            .dispatcher
            .call_unary(self.path, ctx, body, self.format)
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
                _: Bytes,
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
                request: Bytes,
                _: CodecFormat,
            ) -> crate::dispatcher::UnaryResult {
                Box::pin(async move { Ok(EncodedResponse::new(request)) })
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
}
