//! Handler request/response types.
//!
//! This module splits the old `Context` struct into a read-only
//! [`RequestContext`] (passed *into* handlers) and a [`Response<B>`]
//! wrapper (returned *from* handlers). The body type `B` is bounded by
//! [`Encodable<M>`] in the generated trait so handlers can return either
//! the owned message `M`, a borrowing `MView<'_>` /
//! [`OwnedView<MView<'static>>`](buffa::view::OwnedView), or
//! [`MaybeBorrowed`] for the conditional case.

use std::marker::PhantomData;
use std::pin::Pin;
use std::time::{Duration, Instant};

use buffa::Message;
use buffa::view::{MessageView, ViewEncode};
use bytes::Bytes;
use futures::Stream;
use http::HeaderMap;
use http::header::{HeaderName, HeaderValue};

use crate::codec::CodecFormat;
use crate::codec::JsonSerialize;
use crate::codec::encode_json;
use crate::error::ConnectError;

// ---------------------------------------------------------------------------
// RequestContext
// ---------------------------------------------------------------------------

/// Read-only request context passed to RPC handlers.
///
/// Carries the request headers, parsed deadline, and any
/// connection-scoped extensions (peer address, TLS certs, auth context)
/// inserted by a tower layer in front of the service. Handlers do *not*
/// return this; response-side metadata lives on [`Response`].
///
/// `RequestContext` is `#[non_exhaustive]`: construct it with
/// [`RequestContext::new`] and the `with_*` builders, and read fields
/// through the accessor methods (`headers()`, `deadline()`,
/// `extensions()`, …). New request-scoped metadata can be added in minor
/// releases without breaking downstream code.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct RequestContext {
    /// Request headers (after protocol-prefix stripping).
    pub(crate) headers: HeaderMap,
    /// Absolute request deadline parsed from the protocol's timeout header,
    /// if any. Propagate to downstream calls.
    ///
    /// If a [`DeadlinePolicy`](crate::DeadlinePolicy) is configured on the
    /// service, this is the *moderated* value — clamped to the policy's
    /// `[min, max]` range, or the policy default when the client asserted
    /// nothing — not the raw client header.
    pub(crate) deadline: Option<Instant>,
    /// Request extensions carried from the underlying `http::Request`.
    pub(crate) extensions: http::Extensions,
    /// Static metadata for the dispatched RPC method, when known.
    pub(crate) spec: Option<crate::spec::Spec>,
    /// The wire protocol negotiated for this request, when known.
    pub(crate) protocol: Option<crate::Protocol>,
    /// The procedure path the client requested, with a leading slash.
    pub(crate) path: Option<String>,
}

impl RequestContext {
    /// Create a new context with the given request headers.
    pub fn new(headers: HeaderMap) -> Self {
        Self {
            headers,
            deadline: None,
            extensions: http::Extensions::new(),
            spec: None,
            protocol: None,
            path: None,
        }
    }

    /// Set the request deadline (absolute `Instant`).
    #[must_use]
    pub fn with_deadline(mut self, deadline: Option<Instant>) -> Self {
        self.deadline = deadline;
        self
    }

    /// Attach request extensions captured from the underlying `http::Request`.
    #[must_use]
    pub fn with_extensions(mut self, extensions: http::Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Attach the static method metadata for the dispatched RPC.
    #[must_use]
    pub fn with_spec(mut self, spec: Option<crate::spec::Spec>) -> Self {
        self.spec = spec;
        self
    }

    /// Attach the negotiated wire protocol.
    #[must_use]
    pub fn with_protocol(mut self, protocol: Option<crate::Protocol>) -> Self {
        self.protocol = protocol;
        self
    }

    /// Attach the procedure path the client requested. The dispatch path
    /// always supplies the leading-slash form (`"/package.Service/Method"`),
    /// matching [`Spec::procedure`](crate::Spec::procedure); custom
    /// dispatch shims and test fixtures should do the same so consumers
    /// of [`path()`](Self::path) see a consistent shape.
    #[must_use]
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Request headers (after protocol-prefix stripping).
    ///
    /// For a single header lookup, [`header`](Self::header) is simpler.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Get a request header value.
    pub fn header(&self, key: impl http::header::AsHeaderName) -> Option<&HeaderValue> {
        self.headers.get(key)
    }

    /// Absolute request deadline parsed from the protocol's timeout header
    /// (`Connect-Timeout-Ms` or `grpc-timeout`), if the client asserted one.
    ///
    /// Propagate this to downstream calls so the whole call chain shares a
    /// single budget. For the remaining budget as a `Duration`, see
    /// [`time_remaining`](Self::time_remaining).
    ///
    /// If a [`DeadlinePolicy`](crate::DeadlinePolicy) is configured on the
    /// service, this is the *moderated* value — clamped to the policy's
    /// `[min, max]` range, or the policy default when the client asserted
    /// nothing — not the raw client header.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Time remaining until the request deadline, saturating at zero.
    ///
    /// `None` if the client did not assert a timeout. Use this to budget
    /// downstream calls — for example, subtract a margin before passing the
    /// remainder as a downstream RPC's per-call timeout. See also issue
    /// [#92](https://github.com/anthropics/connect-rust/issues/92) for
    /// server-side deadline enforcement.
    pub fn time_remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
    }

    /// Request extensions carried from the underlying `http::Request`.
    ///
    /// This is the passthrough for connection-scoped metadata that a tower
    /// layer in front of the service can attach — TLS peer certificates,
    /// remote socket address, auth context, etc. The dispatch path moves
    /// `parts.extensions` here verbatim; handlers read it with
    /// `ctx.extensions().get::<T>()`. For the well-known peer types, prefer
    /// the typed accessors `peer_addr()` and `peer_certs()` (gated on the
    /// `server` and `server-tls` features respectively) — they return
    /// `None` instead of panicking when the transport didn't insert the
    /// extension.
    pub fn extensions(&self) -> &http::Extensions {
        &self.extensions
    }

    /// Mutable access to the request extensions.
    ///
    /// Useful for code that constructs a `RequestContext` directly — e.g.
    /// a custom dispatch shim or test fixture — and needs to insert
    /// connection-scoped values before calling a handler.
    ///
    /// # Note
    ///
    /// Handlers receive `RequestContext` **by value**, so calling
    /// `ctx.extensions_mut().insert(...)` inside a handler mutates a local
    /// copy that the framework never sees again — it has no effect on the
    /// dispatch path or on downstream layers. To pass values *into* a
    /// handler from middleware, mutate `http::Request::extensions_mut()`
    /// in the layer instead; the dispatcher moves request extensions into
    /// `RequestContext` automatically before dispatch.
    pub fn extensions_mut(&mut self) -> &mut http::Extensions {
        &mut self.extensions
    }

    /// Static metadata for the dispatched RPC method, when known.
    ///
    /// Populated by code-generated `FooServiceServer<T>` dispatchers and
    /// by the dynamic [`Router`](crate::Router) when registered through
    /// the generated `register()` (which chains
    /// [`Router::with_spec`](crate::Router::with_spec) per route).
    /// `None` only for low-level manual registrations that do not attach a
    /// [`Spec`](crate::Spec). See [`path`](Self::path) for the always-present
    /// procedure path.
    pub fn spec(&self) -> Option<crate::spec::Spec> {
        self.spec
    }

    /// The wire protocol negotiated for this request, when known.
    ///
    /// `None` if the runtime constructed the context outside the dispatch
    /// path (e.g. unit tests calling handlers directly).
    pub fn protocol(&self) -> Option<crate::Protocol> {
        self.protocol
    }

    /// The procedure path the client requested, `"/package.Service/Method"`.
    ///
    /// Always present when constructed by the dispatch path: it is taken
    /// from the request URI, so it is populated whenever a handler is
    /// dispatched — including dispatch through the dynamic
    /// [`Router`](crate::Router), which does not supply a
    /// [`Spec`](crate::Spec). `None` only for hand-built contexts (unit
    /// tests calling handlers directly, custom dispatch shims). Code that
    /// must label or gate every request — auth interceptors, span
    /// builders, rate limiters — should read `path()`, not `spec()`, and
    /// treat `None` as a misconfigured or synthetic context rather than a
    /// real RPC.
    ///
    /// Compare [`spec()`](Self::spec): that is the registered method's
    /// *static* metadata, populated when a generated
    /// `FooServiceServer<T>` dispatcher — or a `register()`-built
    /// [`Router`](crate::Router) — resolved the route, and
    /// [`Spec::procedure`](crate::Spec::procedure) is its `&'static str`
    /// procedure name. When both are present they are identical strings;
    /// `path()` exists for the cases where `spec()` cannot be.
    ///
    /// The leading slash is included to match `Spec::procedure`, the
    /// `connect-go` `Spec.Procedure` convention, and `http::Uri::path()`
    /// for any HTTP request that reached the dispatch layer. To compare
    /// against [`Dispatcher::lookup`](crate::Dispatcher::lookup) keys
    /// (which omit it), use `path.strip_prefix('/').unwrap_or(path)`.
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    /// Remote peer socket address, if the transport recorded one.
    ///
    /// Present when the request arrived through
    /// [`Server::serve`](crate::server::Server::serve) (plain) or
    /// `Server::with_tls(...)` (TLS), or any integration that inserts
    /// [`PeerAddr`](crate::server::PeerAddr) into the request extensions
    /// (`connectrpc::axum::serve_tls` does).
    /// Returns `None` otherwise (e.g. an axum app without a layer that
    /// captures the connect info), so prefer this over
    /// `ctx.extensions().get::<PeerAddr>().unwrap()` — the latter compiles,
    /// passes in unit tests, and panics in production behind a transport
    /// that didn't insert it.
    #[cfg(feature = "server")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server")))]
    pub fn peer_addr(&self) -> Option<std::net::SocketAddr> {
        self.extensions
            .get::<crate::server::PeerAddr>()
            .map(|p| p.0)
    }

    /// TLS client certificate chain presented by the peer (leaf first), if any.
    ///
    /// Present only when the request arrived over a TLS listener that
    /// requested a client certificate and the client presented one — see
    /// [`Server::with_tls`](crate::server::Server::with_tls) and
    /// `connectrpc::axum::serve_tls`. Returns `None` for plaintext
    /// transports, for TLS without mutual auth, and for integrations that
    /// don't insert [`PeerCerts`](crate::server::PeerCerts) into the
    /// request extensions. Like [`peer_addr`](Self::peer_addr), prefer
    /// this over a raw `extensions().get()` + `unwrap()`.
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    pub fn peer_certs(&self) -> Option<&[rustls::pki_types::CertificateDer<'static>]> {
        self.extensions
            .get::<crate::server::PeerCerts>()
            .map(|p| &p.0[..])
    }
}

// ---------------------------------------------------------------------------
// Response<B>
// ---------------------------------------------------------------------------

/// Handler response wrapper: a body plus optional response headers,
/// trailers, and compression hint.
///
/// `B` is bounded by [`Encodable<M>`] in the generated service trait so
/// handlers can return the owned message `M` (the common case), or any
/// type that encodes to the same wire bytes.
///
/// # Happy path
///
/// [`Response::ok`] is the bare-body shorthand:
///
/// ```rust,ignore
/// async fn say(&self, _ctx: RequestContext, req: OwnedSayRequestView)
///     -> ServiceResult<SayResponse>
/// {
///     Response::ok(SayResponse { sentence: reply, ..Default::default() })
/// }
/// ```
///
/// # With metadata
///
/// ```rust,ignore
/// Ok(Response::new(reply)
///     .with_header("x-request-id", id)
///     .with_trailer("x-timing", elapsed))
/// ```
#[derive(Debug, Clone)]
pub struct Response<B> {
    /// The response body.
    pub body: B,
    /// Response headers to send before the body.
    pub headers: HeaderMap,
    /// Trailers to send after the body. Sent as HTTP/2 trailing
    /// HEADERS for gRPC, or as `trailer-`-prefixed headers / the
    /// EndStreamResponse JSON for Connect.
    pub trailers: HeaderMap,
    /// Whether to compress the response. `None` uses the server's
    /// compression policy; `Some(false)` disables compression for this
    /// response, `Some(true)` forces it.
    pub compress: Option<bool>,
}

impl<B> Response<B> {
    /// Shorthand for `Ok(Response::from(body))` — the bare-body happy
    /// path.
    ///
    /// Use `Ok(Response::new(body).with_header(...))` when setting
    /// response metadata; this constructor is for the common case of
    /// "just the body".
    pub fn ok(body: B) -> ServiceResult<B> {
        Ok(Self::from(body))
    }

    /// Wrap a body with empty response metadata.
    pub fn new(body: B) -> Self {
        Self {
            body,
            headers: HeaderMap::new(),
            trailers: HeaderMap::new(),
            compress: None,
        }
    }

    /// Append a response header.
    ///
    /// Uses [`HeaderMap::append`], so calling twice with the same name
    /// accumulates values rather than replacing.
    ///
    /// # Panics
    ///
    /// Panics if `name` or `value` cannot be converted into the
    /// corresponding header type (invalid characters, non-ASCII name,
    /// etc.). Use [`try_with_header`](Self::try_with_header) for
    /// dynamic values, or the `headers` field directly for full
    /// control.
    #[must_use]
    pub fn with_header<K, V>(mut self, name: K, value: V) -> Self
    where
        K: TryInto<HeaderName>,
        K::Error: std::fmt::Debug,
        V: TryInto<HeaderValue>,
        V::Error: std::fmt::Debug,
    {
        self.headers
            .append(name.try_into().unwrap(), value.try_into().unwrap());
        self
    }

    /// Append a response header, returning an error if `name` or
    /// `value` is invalid.
    ///
    /// Non-panicking sibling of [`with_header`](Self::with_header) for
    /// dynamic values. Uses [`HeaderMap::append`], so repeated calls
    /// accumulate.
    pub fn try_with_header<K, V>(mut self, name: K, value: V) -> Result<Self, http::Error>
    where
        K: TryInto<HeaderName>,
        K::Error: Into<http::Error>,
        V: TryInto<HeaderValue>,
        V::Error: Into<http::Error>,
    {
        self.headers.append(
            name.try_into().map_err(Into::into)?,
            value.try_into().map_err(Into::into)?,
        );
        Ok(self)
    }

    /// Append a response trailer.
    ///
    /// Uses [`HeaderMap::append`], so calling twice with the same name
    /// accumulates values rather than replacing.
    ///
    /// # Panics
    ///
    /// Panics if `name` or `value` cannot be converted into the
    /// corresponding header type. Use
    /// [`try_with_trailer`](Self::try_with_trailer) for dynamic
    /// values, or the `trailers` field directly for full control.
    #[must_use]
    pub fn with_trailer<K, V>(mut self, name: K, value: V) -> Self
    where
        K: TryInto<HeaderName>,
        K::Error: std::fmt::Debug,
        V: TryInto<HeaderValue>,
        V::Error: std::fmt::Debug,
    {
        self.trailers
            .append(name.try_into().unwrap(), value.try_into().unwrap());
        self
    }

    /// Append a response trailer, returning an error if `name` or
    /// `value` is invalid.
    ///
    /// Non-panicking sibling of [`with_trailer`](Self::with_trailer)
    /// for dynamic values. Uses [`HeaderMap::append`], so repeated
    /// calls accumulate.
    pub fn try_with_trailer<K, V>(mut self, name: K, value: V) -> Result<Self, http::Error>
    where
        K: TryInto<HeaderName>,
        K::Error: Into<http::Error>,
        V: TryInto<HeaderValue>,
        V::Error: Into<http::Error>,
    {
        self.trailers.append(
            name.try_into().map_err(Into::into)?,
            value.try_into().map_err(Into::into)?,
        );
        Ok(self)
    }

    /// Override the server's compression policy for this response.
    ///
    /// `true` forces compression, `false` disables it, `None` (or
    /// never calling this) defers to the server's policy.
    #[must_use]
    pub fn compress(mut self, enabled: impl Into<Option<bool>>) -> Self {
        self.compress = enabled.into();
        self
    }

    /// Replace the body, preserving headers/trailers/compression.
    pub fn map_body<C>(self, f: impl FnOnce(B) -> C) -> Response<C> {
        Response {
            body: f(self.body),
            headers: self.headers,
            trailers: self.trailers,
            compress: self.compress,
        }
    }
}

impl<B> From<B> for Response<B> {
    fn from(body: B) -> Self {
        Self::new(body)
    }
}

impl<T> Response<ServiceStream<T>> {
    /// Wrap a streaming body, boxing and unsize-coercing it to
    /// [`ServiceStream<T>`]. Handles the explicit coercion that
    /// `Ok(Box::pin(s).into())` would otherwise need.
    pub fn stream(s: impl Stream<Item = Result<T, ConnectError>> + Send + 'static) -> Self {
        Self::new(Box::pin(s))
    }

    /// Shorthand for `Ok(Response::stream(s))` — the bare-stream
    /// happy path.
    pub fn stream_ok(
        s: impl Stream<Item = Result<T, ConnectError>> + Send + 'static,
    ) -> ServiceResult<ServiceStream<T>> {
        Ok(Self::stream(s))
    }
}

/// Result type returned by handler trait methods.
///
/// `B` is the body type — typically the owned response message, or any
/// `impl Encodable<M>`.
pub type ServiceResult<B> = Result<Response<B>, ConnectError>;

/// Boxed `Send` stream of `Result<T, ConnectError>`.
///
/// Used as the request type for client/bidi-streaming handlers and the
/// body type for server/bidi-streaming responses.
///
/// For an inbound request stream, `None` means the client finished the
/// stream cleanly; `Some(Err(..))` means the stream ended abnormally — a
/// decode failure or a request body that failed mid-upload (truncated or
/// broken transport). Treat only `None` as a complete stream; propagating
/// the error with `?` fails the RPC, which is the right default for
/// handlers that aggregate inbound messages.
pub type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, ConnectError>> + Send>>;

// ---------------------------------------------------------------------------
// Encodable<M>
// ---------------------------------------------------------------------------

/// Encodes to the same wire bytes as proto message `M`.
///
/// This is the bound on the response body in generated trait methods.
/// Provided implementations:
/// - the owned `M` itself (blanket `M: Message + JsonSerialize` below);
/// - `MView<'_>` and [`OwnedView<MView<'static>>`](buffa::view::OwnedView),
///   emitted by codegen per RPC output type;
/// - [`MaybeBorrowed<M, V>`] for handlers that conditionally return
///   either;
/// - [`StreamMessage<M>`](crate::StreamMessage) for echoing inbound
///   stream items back out (re-encodes from the retained wire bytes);
/// - [`PreEncoded`] for handlers that encode a non-`'static` view
///   internally and pass the bytes across the handler boundary.
///
/// # Contract
///
/// Implementations must produce bytes that decode as a valid `M` in
/// the given format.
///
/// `encode` is fallible: the owned-message impl never errors. The
/// view-body impls are proto-only (view types lack `Serialize`) and return
/// [`ErrorCode::Unimplemented`](crate::ErrorCode::Unimplemented) for
/// `CodecFormat::Json`. [`PreEncoded`] supports both codecs but the JSON
/// path is a slow fallback (decode + re-serialize) — see its
/// `# Codec behaviour` doc.
pub trait Encodable<M> {
    /// Encode `self` as wire bytes for `M` in the requested format.
    fn encode(&self, codec: CodecFormat) -> Result<Bytes, ConnectError>;
}

impl<M: Message + JsonSerialize> Encodable<M> for M {
    fn encode(&self, codec: CodecFormat) -> Result<Bytes, ConnectError> {
        match codec {
            CodecFormat::Proto => Ok(self.encode_to_bytes()),
            CodecFormat::Json => encode_json(self),
        }
    }
}

/// Encode a view body via [`ViewEncode`] for [`CodecFormat::Proto`], or
/// return [`ErrorCode::Unimplemented`](crate::ErrorCode::Unimplemented)
/// for [`CodecFormat::Json`] (view types don't implement `Serialize`).
///
/// Used by codegen-emitted `impl Encodable<Foo> for FooView<'_>` /
/// `impl Encodable<Foo> for OwnedView<FooView<'static>>` blocks. A
/// runtime blanket on [`OwnedView`](buffa::view::OwnedView) would
/// conflict with the `M: Message + JsonSerialize` blanket above (coherence
/// can't rule out upstream adding `Message`/`Serialize` for
/// `OwnedView`), so the impls are emitted per output type instead.
#[doc(hidden)]
pub fn encode_view_body<'a, V: ViewEncode<'a>>(
    view: &V,
    codec: CodecFormat,
) -> Result<Bytes, ConnectError> {
    match codec {
        CodecFormat::Proto => Ok(view.encode_to_bytes()),
        CodecFormat::Json => Err(ConnectError::unimplemented(
            "view-body responses do not support the JSON codec; return the owned message type for JSON-serving handlers",
        )),
    }
}

// ---------------------------------------------------------------------------
// MaybeBorrowed
// ---------------------------------------------------------------------------

/// Either an owned message `M` or a borrowing view `V`, both
/// [`Encodable<M>`].
///
/// Use this when a handler conditionally passes the request through
/// unchanged (return the view, zero allocations) versus modifying it
/// (clone to owned, mutate, return owned). The single concrete return
/// type satisfies the `impl Encodable<M>` bound on the generated trait.
///
/// This is not [`std::borrow::Cow`]: `V` is a separate
/// [`Encodable<M>`] type (e.g. `MView<'a>` or `OwnedView<MView>`),
/// not a `&M`, and there is no `ToOwned` relationship between the
/// arms — each encodes independently.
///
/// ```rust,ignore
/// async fn redact(&self, _ctx: RequestContext, req: OwnedRecordView)
///     -> ServiceResult<MaybeBorrowed<Record, OwnedRecordView>>
/// {
///     if req.email.is_empty() && req.ssn.is_empty() {
///         // pass-through: re-encode straight from the request bytes
///         return Response::ok(MaybeBorrowed::Borrowed(req));
///     }
///     let mut owned = req.to_owned_message()?;
///     owned.email.clear();
///     owned.ssn.clear();
///     Response::ok(MaybeBorrowed::Owned(owned))
/// }
/// ```
///
/// # Codec compatibility
///
/// The `Borrowed` arm only encodes for [`CodecFormat::Proto`]. JSON
/// clients receive an `unimplemented` error; if your service must
/// support JSON, return `Owned` (or just the owned message) on every
/// path.
#[derive(Debug, Clone)]
pub enum MaybeBorrowed<M, V> {
    /// An owned message body.
    Owned(M),
    /// A borrowing body that encodes to the same wire bytes as `M`.
    Borrowed(V),
}

impl<M, V> Encodable<M> for MaybeBorrowed<M, V>
where
    // satisfied via the blanket impl for M: Message + JsonSerialize
    M: Encodable<M>,
    V: Encodable<M>,
{
    fn encode(&self, codec: CodecFormat) -> Result<Bytes, ConnectError> {
        match self {
            Self::Owned(m) => m.encode(codec),
            Self::Borrowed(v) => v.encode(codec),
        }
    }
}

// ---------------------------------------------------------------------------
// PreEncoded
// ---------------------------------------------------------------------------

/// Pre-encoded protobuf response body for message type `M`.
///
/// Use when the handler builds and encodes a borrowing view internally —
/// e.g. a `FooView<'a>` borrowing from a local snapshot — rather than
/// returning the view itself. The `'static` bound on `Handler::Body` (and
/// on streaming items, see the `use<Self>` note in the
/// [`StreamingHandler`](crate::StreamingHandler) docs) means a view with a
/// non-`'static` lifetime can't cross the handler
/// boundary; `PreEncoded` carries the bytes across instead.
///
/// The `M` type parameter is a compile-time witness for which RPC output
/// type the bytes encode. Three construction paths, in decreasing order
/// of compile-time guarantee:
///
/// - [`from_message(&m)`](PreEncoded::from_message) — encodes an owned
///   `M`; the receiver type *is* the witness.
/// - [`from_view(&view)`](PreEncoded::from_view) — encodes a borrowing
///   view; `MessageView::Owned = M` is the witness.
/// - [`from_bytes_unchecked(bytes)`](PreEncoded::from_bytes_unchecked) —
///   wraps already-encoded bytes from elsewhere (a cache, storage,
///   another service). No witness; you're asserting the bytes decode as
///   `M`.
///
/// `from_message` and `from_view` produce the same `PreEncoded<M>` type,
/// so a stream can mix items built either way (e.g. a cache-hit path
/// returning the cached owned `M`, a cache-miss path building a view from
/// a snapshot) — the same role [`MaybeBorrowed`] fills for unary
/// handlers, but with the encode happening eagerly inside the stream
/// body.
///
/// # Streaming example
///
/// The motivating shape — a server-streaming handler that builds and
/// encodes per-item views borrowing from a local store snapshot, then
/// yields the bytes:
///
/// ```rust,ignore
/// use connectrpc::{PreEncoded, Response, RequestContext, ServiceResult, ServiceStream};
///
/// async fn watch(
///     &self,
///     _ctx: RequestContext,
///     req: OwnedWatchRequestView,
/// ) -> ServiceResult<ServiceStream<PreEncoded<WatchResponse>>> {
///     let store = self.store.clone();
///     let stream = futures::stream::unfold(store, |store| async move {
///         let snapshot = store.load();
///         // `view` borrows from `snapshot`; encode while the borrow is live.
///         let view = build_view_from_snapshot(&snapshot);
///         let item = PreEncoded::from_view(&view);
///         Some((Ok(item), store))
///     });
///     Response::stream_ok(stream)
/// }
/// ```
///
/// For a unary handler, the same pattern applies — return
/// `ServiceResult<PreEncoded<MyResponse>>`.
///
/// # Codec behaviour
///
/// `PreEncoded` is optimized for the `proto` codec: the wrapped bytes are
/// passed through verbatim with no re-encoding. The motivating use case
/// (high-throughput fanout) is proto-only.
///
/// For the `json` codec, `PreEncoded` falls back to decoding the bytes as
/// `M` and re-serializing as JSON. **This is correct but not fast** — a
/// full proto decode plus a JSON serialize per response (or per stream
/// item). The fallback exists so that registering a `PreEncoded` handler
/// on a JSON-capable router degrades gracefully instead of returning a
/// runtime error. If your service serves a meaningful JSON traffic share,
/// build and return the owned message (or [`MaybeBorrowed::Owned`])
/// instead — that lets the codec layer pick the right encoding without
/// the proto round-trip.
///
/// If the wrapped bytes don't decode as `M` (e.g. you passed mismatched
/// bytes to [`from_bytes_unchecked`](PreEncoded::from_bytes_unchecked)),
/// the JSON path returns an [`internal`](crate::ErrorCode::Internal)
/// error at the server; the proto path passes the bytes through and the
/// client sees a decode error.
///
/// ## Codec-dependent fidelity
///
/// The proto path is byte-exact; the JSON path is **only as faithful as
/// decoding the bytes to an owned `M` and re-serializing**. The two
/// diverge when the wrapped bytes carry information not representable in
/// `M` itself:
///
/// - **Unknown fields** (proto bytes encoded against a *newer* schema
///   than the server's `M`) are preserved on the proto path and dropped
///   on the JSON path. This matters only for
///   [`from_bytes_unchecked`](PreEncoded::from_bytes_unchecked) bytes
///   sourced externally; bytes produced by
///   [`from_message`](PreEncoded::from_message) /
///   [`from_view`](PreEncoded::from_view) cannot carry unknown fields.
/// - **Non-canonical proto encodings** (out-of-order fields, redundant
///   length prefixes, repeated non-`repeated` fields) are passed through
///   verbatim on the proto path and normalized by the decode on the JSON
///   path.
///
/// If byte-exact fidelity across codecs matters (e.g. signature
/// verification, content-addressed storage), do not use `PreEncoded` with
/// JSON-capable routes.
///
/// ## Cost is selected by the client
///
/// The codec is chosen per-request by the client's `Content-Type` header.
/// For a service that adopted `PreEncoded` for proto throughput, a client
/// sending JSON requests (intentionally, by misconfiguration, or
/// adversarially) shifts those requests onto the slow decode-reserialize
/// path. The marginal cost is bounded by the response size and is usually
/// small relative to the handler's own work, but a streaming RPC pays it
/// per item. A service that wants to *enforce* proto-only should reject
/// non-proto `Content-Type` at the middleware layer (e.g. an axum
/// middleware that returns `415 Unsupported Media Type`) rather than rely
/// on the body type — that keeps the policy outside the handler and
/// applies before the request body is read.
///
/// # Contract
///
/// `PreEncoded` is a transparent byte container — it does not validate
/// the wrapped bytes on the proto path. [`PreEncoded::from_view`] gives a
/// compile-time witness via `MessageView::Owned = M`;
/// [`PreEncoded::from_bytes_unchecked`] trusts the caller. Returning bytes
/// that don't decode as `M` will produce decode errors on the client (or,
/// for JSON clients, an `internal` error from the server-side fallback
/// decode).
#[must_use = "PreEncoded must be returned from a handler to take effect"]
pub struct PreEncoded<M> {
    bytes: Bytes,
    // `fn() -> M` keeps `PreEncoded<M>` `Send + Sync` regardless of `M`'s
    // auto-trait surface (the bytes are owned; `M` is only a type witness).
    _marker: PhantomData<fn() -> M>,
}

// Manual derives: `#[derive(Debug, Clone)]` would add a spurious `M: Debug` /
// `M: Clone` bound (PhantomData carries it through to the where-clause).
impl<M> std::fmt::Debug for PreEncoded<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PreEncoded").field(&self.bytes).finish()
    }
}

impl<M> Clone for PreEncoded<M> {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            _marker: PhantomData,
        }
    }
}

impl<M: Message> PreEncoded<M> {
    /// Encode an owned `M` to protobuf bytes.
    ///
    /// The receiver type is the compile-time witness — there's no way to
    /// produce a `PreEncoded<M>` from a `&Other`. This is the right
    /// constructor when the handler builds an owned `M` and wants to
    /// share the encoding (e.g. encode once, clone the
    /// [`Bytes`]-backed `PreEncoded` for N readers in a fanout) or when a
    /// stream needs to mix owned-message and view-built items under a
    /// single `type Item = PreEncoded<M>`.
    ///
    /// Equivalent to `PreEncoded::from_bytes_unchecked(m.encode_to_bytes())`,
    /// but with `M` enforced by the type system rather than asserted by
    /// the caller.
    pub fn from_message(msg: &M) -> Self {
        Self {
            bytes: msg.encode_to_bytes(),
            _marker: PhantomData,
        }
    }

    /// Encode a [`ViewEncode`] view to protobuf bytes.
    ///
    /// The `MessageView<'a, Owned = M>` bound is the compile-time witness
    /// that the bytes decode as `M` — passing `OtherView<'a>` won't
    /// type-check unless `OtherView::Owned == M`.
    pub fn from_view<'a, V>(view: &V) -> Self
    where
        V: ViewEncode<'a> + MessageView<'a, Owned = M>,
    {
        Self {
            bytes: view.encode_to_bytes(),
            _marker: PhantomData,
        }
    }

    /// Wrap already-encoded protobuf bytes without validating them.
    ///
    /// Use when the bytes come from somewhere with no structural type
    /// guarantee — a byte cache, a blob store, a sidecar service. You are
    /// asserting the bytes decode as `M`; the proto path does not
    /// validate this. In debug builds, the bytes are decoded once as a
    /// `debug_assert!` to surface mismatches early.
    ///
    /// Prefer [`from_message`](PreEncoded::from_message) when you have an
    /// owned `M` in hand and [`from_view`](PreEncoded::from_view) when
    /// you have a view — both enforce `M` at compile time.
    ///
    /// Zero-copy for `Bytes` and `Vec<u8>`; passing `&[u8]` allocates and
    /// copies.
    pub fn from_bytes_unchecked(bytes: impl Into<Bytes>) -> Self {
        let bytes = bytes.into();
        debug_assert!(
            M::decode_from_slice(&bytes).is_ok(),
            "PreEncoded::from_bytes_unchecked: bytes do not decode as {}",
            std::any::type_name::<M>(),
        );
        Self {
            bytes,
            _marker: PhantomData,
        }
    }
}

/// Encode an owned `M` to a [`PreEncoded<M>`].
///
/// Equivalent to [`PreEncoded::from_message`]; provided for `.into()`
/// ergonomics.
impl<M: Message> From<&M> for PreEncoded<M> {
    fn from(msg: &M) -> Self {
        Self::from_message(msg)
    }
}

// Coherence: this impl is non-overlapping with the
// `impl<M: Message + JsonSerialize> Encodable<M> for M` blanket above for
// structural reasons. For the two to overlap, some `T` would have to satisfy
// both `T: Encodable<T>` (blanket, with `T: Message + JsonSerialize`) and
// `T = PreEncoded<U>` with `T: Encodable<U>` (this impl) for the *same* trait
// parameter — i.e. `T = U`, i.e. `PreEncoded<U> = U`, which is infinite. So
// the impls cannot overlap even if a future change made `PreEncoded` a
// `Message` (which would only add `PreEncoded<M>: Encodable<PreEncoded<M>>` —
// a different trait instantiation). No invariant to maintain here.
//
// The `M: Message + JsonSerialize` bound matches the blanket so a `PreEncoded<M>`
// is `Encodable<M>` exactly when an owned `M` would be — and is what makes the
// JSON fallback path possible (decode as `M`, re-serialize).
impl<M: Message + JsonSerialize> Encodable<M> for PreEncoded<M> {
    fn encode(&self, codec: CodecFormat) -> Result<Bytes, ConnectError> {
        match codec {
            CodecFormat::Proto => Ok(self.bytes.clone()),
            // Slow path: decode the proto bytes back to `M`, then serialize
            // as JSON. This exists for correctness (JSON clients should get
            // a response, not `unimplemented`), not throughput; the owned
            // message path skips the proto round-trip and is preferable for
            // JSON-heavy services. See the type-level docs.
            CodecFormat::Json => {
                let msg = M::decode_from_slice(&self.bytes).map_err(|e| {
                    ConnectError::internal(format!(
                        "pre-encoded bytes did not decode as {}: {e}",
                        std::any::type_name::<M>(),
                    ))
                })?;
                encode_json(&msg)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// EncodedResponse (dispatcher boundary)
// ---------------------------------------------------------------------------

/// A [`Response`] with the body already encoded to bytes.
///
/// This is what the [`Dispatcher`](crate::Dispatcher) returns to the
/// protocol layer — encoding happens inside the dispatcher so the body
/// type stays generic across the trait boundary.
pub type EncodedResponse = Response<Bytes>;

impl<B> Response<B> {
    /// Encode the body to bytes via [`Encodable<M>`], preserving
    /// response metadata.
    #[doc(hidden)] // exposed for dispatcher::codegen (generated code)
    pub fn encode<M>(self, codec: CodecFormat) -> Result<EncodedResponse, ConnectError>
    where
        B: Encodable<M>,
    {
        let bytes = self.body.encode(codec)?;
        Ok(Response {
            body: bytes,
            headers: self.headers,
            trailers: self.trailers,
            compress: self.compress,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffa_types::google::protobuf::StringValue;

    #[tokio::test]
    async fn response_stream_ok_shorthand() {
        use futures::StreamExt;
        let r: ServiceResult<ServiceStream<i32>> =
            Response::stream_ok(futures::stream::iter([Ok(7)]));
        let collected: Vec<_> = r.unwrap().body.map(|x| x.unwrap()).collect().await;
        assert_eq!(collected, vec![7]);
    }

    #[test]
    fn compress_tristate() {
        assert_eq!(Response::new(()).compress(true).compress, Some(true));
        assert_eq!(Response::new(()).compress(false).compress, Some(false));
        assert_eq!(Response::new(()).compress(None).compress, None);
    }

    #[test]
    fn header_accepts_str() {
        let mut h = HeaderMap::new();
        h.insert("x-custom", HeaderValue::from_static("v"));
        let ctx = RequestContext::new(h);
        assert_eq!(ctx.header("x-custom").unwrap(), "v");
    }

    #[test]
    fn response_ok_shorthand() {
        let r: ServiceResult<u32> = Response::ok(42);
        let r = r.unwrap();
        assert_eq!(r.body, 42);
        assert!(r.headers.is_empty());
    }

    #[test]
    fn response_from_body() {
        let r: Response<StringValue> = StringValue::from("hi").into();
        assert_eq!(r.body.value, "hi");
        assert!(r.headers.is_empty());
        assert!(r.trailers.is_empty());
        assert_eq!(r.compress, None);
    }

    #[test]
    fn response_builder() {
        let r = Response::new(StringValue::from("hi"))
            .with_header("x-a", "1")
            .with_trailer("x-b", "2")
            .compress(true);
        assert_eq!(r.headers.get("x-a").unwrap(), "1");
        assert_eq!(r.trailers.get("x-b").unwrap(), "2");
        assert_eq!(r.compress, Some(true));
    }

    #[test]
    fn encodable_owned_proto() {
        let m = StringValue::from("hello");
        let bytes = Encodable::<StringValue>::encode(&m, CodecFormat::Proto).unwrap();
        assert_eq!(
            StringValue::decode_from_slice(&bytes).unwrap().value,
            "hello"
        );
    }

    #[cfg(feature = "json")]
    #[test]
    fn encodable_owned_json() {
        let m = StringValue::from("hello");
        let bytes = Encodable::<StringValue>::encode(&m, CodecFormat::Json).unwrap();
        assert_eq!(&bytes[..], b"\"hello\"");
    }

    #[test]
    fn response_encode() {
        let r = Response::new(StringValue::from("hi")).with_header("x-a", "1");
        let enc = r.encode::<StringValue>(CodecFormat::Proto).unwrap();
        assert_eq!(enc.headers.get("x-a").unwrap(), "1");
        assert_eq!(
            StringValue::decode_from_slice(&enc.body).unwrap().value,
            "hi"
        );
    }

    #[test]
    fn request_context_new() {
        let mut h = HeaderMap::new();
        h.insert("x-custom", HeaderValue::from_static("v"));
        let ctx = RequestContext::new(h);
        assert_eq!(
            ctx.header(HeaderName::from_static("x-custom")).unwrap(),
            "v"
        );
        assert_eq!(ctx.headers().get("x-custom").unwrap(), "v");
        assert!(ctx.deadline().is_none());
        assert!(ctx.time_remaining().is_none());
        assert!(ctx.extensions().is_empty());
    }

    #[test]
    fn request_context_with_deadline() {
        let d = Instant::now();
        let ctx = RequestContext::new(HeaderMap::new()).with_deadline(Some(d));
        assert_eq!(ctx.deadline(), Some(d));
    }

    #[test]
    fn request_context_time_remaining_saturates_at_zero() {
        // Deadline in the past — `time_remaining()` should clamp to zero,
        // not underflow.
        let past = Instant::now() - Duration::from_secs(60);
        let ctx = RequestContext::new(HeaderMap::new()).with_deadline(Some(past));
        assert_eq!(ctx.time_remaining(), Some(Duration::ZERO));
    }

    #[test]
    fn request_context_time_remaining_future() {
        let future = Instant::now() + Duration::from_secs(60);
        let ctx = RequestContext::new(HeaderMap::new()).with_deadline(Some(future));
        let remaining = ctx.time_remaining().unwrap();
        // Some elapsed time between `with_deadline` and the assertion is
        // expected; just bound it.
        assert!(remaining > Duration::from_secs(55));
        assert!(remaining <= Duration::from_secs(60));
    }

    #[test]
    fn request_context_extensions_mut() {
        #[derive(Clone, Debug, PartialEq)]
        struct Tag(u8);
        let mut ctx = RequestContext::new(HeaderMap::new());
        ctx.extensions_mut().insert(Tag(1));
        assert_eq!(ctx.extensions().get::<Tag>(), Some(&Tag(1)));
    }

    #[cfg(feature = "server")]
    #[test]
    fn request_context_peer_addr_absent() {
        // No transport inserted `PeerAddr`; the typed accessor returns
        // `None` rather than panicking.
        let ctx = RequestContext::new(HeaderMap::new());
        assert_eq!(ctx.peer_addr(), None);
    }

    #[cfg(feature = "server")]
    #[test]
    fn request_context_peer_addr_present() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
        let mut ext = http::Extensions::new();
        ext.insert(crate::server::PeerAddr(addr));
        let ctx = RequestContext::new(HeaderMap::new()).with_extensions(ext);
        assert_eq!(ctx.peer_addr(), Some(addr));
    }

    #[cfg(feature = "server-tls")]
    #[test]
    fn request_context_peer_certs_absent() {
        let ctx = RequestContext::new(HeaderMap::new());
        assert!(ctx.peer_certs().is_none());
    }

    #[test]
    fn response_map_body_preserves_metadata() {
        let r = Response::new(2u32)
            .with_header("x-h", "1")
            .with_trailer("x-t", "2")
            .compress(true);
        let r = r.map_body(|n| n.to_string());
        assert_eq!(r.body, "2");
        assert_eq!(r.headers.get("x-h").unwrap(), "1");
        assert_eq!(r.trailers.get("x-t").unwrap(), "2");
        assert_eq!(r.compress, Some(true));
    }

    #[tokio::test]
    async fn response_stream_yields_items() {
        use futures::StreamExt;
        let r: Response<ServiceStream<i32>> =
            Response::stream(futures::stream::iter([Ok(1), Ok(2), Ok(3)]));
        let collected: Vec<_> = r.body.map(|x| x.unwrap()).collect().await;
        assert_eq!(collected, vec![1, 2, 3]);
    }

    #[test]
    #[should_panic]
    fn with_header_panics_on_invalid_name() {
        let _ = Response::new(()).with_header("invalid header name", "v");
    }

    #[test]
    fn try_with_header_errors_on_invalid_name() {
        let err = Response::new(())
            .try_with_header("invalid header name", "v")
            .unwrap_err();
        assert!(err.is::<http::header::InvalidHeaderName>());
    }

    #[test]
    fn try_with_header_ok_appends() {
        let r = Response::new(())
            .try_with_header("x-a", "1")
            .unwrap()
            .try_with_header("x-a", "2")
            .unwrap();
        let vals: Vec<_> = r.headers.get_all("x-a").iter().collect();
        assert_eq!(vals.len(), 2);
    }

    #[test]
    fn try_with_trailer_errors_on_invalid_value() {
        // Newlines are not permitted in header values.
        let err = Response::new(())
            .try_with_trailer("x-t", "bad\nvalue")
            .unwrap_err();
        assert!(err.is::<http::header::InvalidHeaderValue>());
    }

    #[test]
    fn encode_view_body_proto() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        let v = StringValueView {
            value: "hi",
            ..Default::default()
        };
        let bytes = encode_view_body(&v, CodecFormat::Proto).unwrap();
        assert_eq!(StringValue::decode_from_slice(&bytes).unwrap().value, "hi");
    }

    #[test]
    fn encode_view_body_json_errors() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        let v = StringValueView::default();
        let err = encode_view_body(&v, CodecFormat::Json).unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::Unimplemented);
        assert!(err.message.as_deref().unwrap().contains("JSON codec"));
    }

    // Manual Encodable<StringValue> impl modelling what codegen emits
    // for FooView<'_>. Shared by the MaybeBorrowed tests below.
    struct V<'a>(buffa_types::google::protobuf::__buffa::view::StringValueView<'a>);
    impl Encodable<StringValue> for V<'_> {
        fn encode(&self, c: CodecFormat) -> Result<Bytes, ConnectError> {
            encode_view_body(&self.0, c)
        }
    }

    #[test]
    fn maybe_borrowed_dispatch() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        let owned: MaybeBorrowed<StringValue, V<'_>> =
            MaybeBorrowed::Owned(StringValue::from("owned"));
        let borrowed = MaybeBorrowed::Borrowed(V(StringValueView {
            value: "view",
            ..Default::default()
        }));
        assert_eq!(
            StringValue::decode_from_slice(&owned.encode(CodecFormat::Proto).unwrap())
                .unwrap()
                .value,
            "owned"
        );
        assert_eq!(
            StringValue::decode_from_slice(&borrowed.encode(CodecFormat::Proto).unwrap())
                .unwrap()
                .value,
            "view"
        );
    }

    #[test]
    fn maybe_borrowed_borrowed_json_unimplemented() {
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        let borrowed: MaybeBorrowed<StringValue, V<'_>> =
            MaybeBorrowed::Borrowed(V(StringValueView::default()));
        let err = borrowed.encode(CodecFormat::Json).unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::Unimplemented);
    }

    #[test]
    fn pre_encoded_proto_round_trip() {
        let m = StringValue::from("pre-encoded");
        let bytes = m.encode_to_bytes();
        let body = PreEncoded::<StringValue>::from_bytes_unchecked(bytes.clone());
        let out = Encodable::<StringValue>::encode(&body, CodecFormat::Proto).unwrap();
        assert_eq!(out, bytes);
        assert_eq!(
            StringValue::decode_from_slice(&out).unwrap().value,
            "pre-encoded"
        );
    }

    #[cfg(feature = "json")]
    #[test]
    fn pre_encoded_json_decodes_then_serializes() {
        // The JSON path round-trips: proto bytes → owned `M` → JSON. Slow,
        // but correct — see the `# Codec behaviour` doc on `PreEncoded`.
        let m = StringValue::from("hi");
        let body = PreEncoded::<StringValue>::from_bytes_unchecked(m.encode_to_bytes());
        let out = Encodable::<StringValue>::encode(&body, CodecFormat::Json).unwrap();
        // Output should match what serializing the owned message directly
        // would produce.
        assert_eq!(out, Bytes::from(serde_json::to_vec(&m).unwrap()));
    }

    #[cfg(feature = "json")]
    #[test]
    fn pre_encoded_json_decode_failure_is_internal_error() {
        // `from_bytes_unchecked` is unvalidated on the proto path. The JSON
        // fallback necessarily decodes; if that fails (the wrapped bytes
        // were never a valid `M`), the server-side `internal` error surfaces
        // closer to the construction bug than the proto path would.
        //
        // Field 1 (LEN) declares 99 bytes but only 2 follow — guaranteed
        // truncated for `StringValue`.
        let body = PreEncoded::<StringValue> {
            bytes: Bytes::from_static(&[0x0a, 0x63, b'h', b'i']),
            _marker: std::marker::PhantomData,
        };
        let err = Encodable::<StringValue>::encode(&body, CodecFormat::Json).unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::Internal);
        assert!(err.message.as_deref().unwrap().contains("did not decode"));
    }

    #[test]
    fn pre_encoded_from_view() {
        use buffa::view::ViewEncode;
        use buffa_types::google::protobuf::__buffa::view::StringValueView;
        let v = StringValueView {
            value: "from-view",
            ..Default::default()
        };
        // `from_view` infers `M = StringValue` from `StringValueView::Owned`.
        let body = PreEncoded::from_view(&v);
        let out = Encodable::<StringValue>::encode(&body, CodecFormat::Proto).unwrap();
        assert_eq!(out, v.encode_to_bytes());
        assert_eq!(
            StringValue::decode_from_slice(&out).unwrap().value,
            "from-view"
        );
    }

    #[test]
    fn pre_encoded_from_message() {
        let m = StringValue::from("from-message");
        // `from_message` infers `M` from the receiver — no annotation.
        let body = PreEncoded::from_message(&m);
        let out = Encodable::<StringValue>::encode(&body, CodecFormat::Proto).unwrap();
        assert_eq!(out, m.encode_to_bytes());

        // `From<&M>` is the same conversion via `.into()`.
        let body2: PreEncoded<StringValue> = (&m).into();
        let out2 = Encodable::<StringValue>::encode(&body2, CodecFormat::Proto).unwrap();
        assert_eq!(out2, out);
    }

    #[cfg(feature = "json")]
    #[test]
    fn pre_encoded_codec_fidelity_diverges_on_unknown_fields() {
        // Documents the codec-dependent fidelity caveat: the proto path
        // is byte-exact (unknown fields preserved); the JSON path
        // round-trips through `M` (unknown fields dropped). Only relevant
        // for `from_bytes_unchecked` bytes sourced externally.
        //
        // Wire bytes: field 1 = "hi" (the known `StringValue.value`),
        // plus field 2 = varint 42 (unknown to `StringValue`).
        let bytes_with_unknown =
            Bytes::from_static(&[0x0a, 0x02, b'h', b'i', /* tag 2 varint */ 0x10, 42]);
        let body = PreEncoded::<StringValue> {
            bytes: bytes_with_unknown.clone(),
            _marker: std::marker::PhantomData,
        };

        // Proto: byte-exact passthrough, unknown field preserved.
        let proto = Encodable::<StringValue>::encode(&body, CodecFormat::Proto).unwrap();
        assert_eq!(proto, bytes_with_unknown);

        // JSON: round-trips through `StringValue`, which drops the
        // unknown field. Output equals serializing the bare known
        // message.
        let json = Encodable::<StringValue>::encode(&body, CodecFormat::Json).unwrap();
        assert_eq!(
            json,
            Bytes::from(serde_json::to_vec(&StringValue::from("hi")).unwrap())
        );
    }

    // --- proto-only (json feature disabled) fallback behaviour ---

    #[cfg(not(feature = "json"))]
    #[test]
    fn encodable_owned_json_is_unimplemented_without_feature() {
        let m = StringValue::from("hello");
        // Proto still encodes normally...
        assert!(Encodable::<StringValue>::encode(&m, CodecFormat::Proto).is_ok());
        // ...but the JSON codec is compiled out and reports it cleanly.
        let err = Encodable::<StringValue>::encode(&m, CodecFormat::Json).unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::Unimplemented);
    }

    #[cfg(not(feature = "json"))]
    #[test]
    fn pre_encoded_json_is_unimplemented_without_feature() {
        let m = StringValue::from("hi");
        let body = PreEncoded::<StringValue>::from_bytes_unchecked(m.encode_to_bytes());
        assert!(Encodable::<StringValue>::encode(&body, CodecFormat::Proto).is_ok());
        let err = Encodable::<StringValue>::encode(&body, CodecFormat::Json).unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::Unimplemented);
    }

    #[test]
    fn pre_encoded_is_typed() {
        // `PreEncoded<M>` only implements `Encodable<M>` — the type witness
        // means `PreEncoded<StringValue>` cannot be used where
        // `Encodable<Int32Value>` is required. Verified at compile time;
        // this test just exercises the happy path for both types.
        use buffa_types::google::protobuf::Int32Value;
        let s = PreEncoded::<StringValue>::from_bytes_unchecked(
            StringValue::from("a").encode_to_bytes(),
        );
        let i =
            PreEncoded::<Int32Value>::from_bytes_unchecked(Int32Value::from(1).encode_to_bytes());
        Encodable::<StringValue>::encode(&s, CodecFormat::Proto).unwrap();
        Encodable::<Int32Value>::encode(&i, CodecFormat::Proto).unwrap();
        // The following would not compile:
        //   Encodable::<Int32Value>::encode(&s, CodecFormat::Proto)
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "do not decode as")]
    fn pre_encoded_from_bytes_unchecked_debug_asserts() {
        // In debug builds, `from_bytes_unchecked` decodes once to surface
        // mismatched bytes early. Field 1 (LEN) declares 99 bytes; only 2
        // follow.
        let _ = PreEncoded::<StringValue>::from_bytes_unchecked(Bytes::from_static(&[
            0x0a, 0x63, b'h', b'i',
        ]));
    }

    #[test]
    fn request_context_with_extensions() {
        #[derive(Clone, Debug, PartialEq)]
        struct Peer(u32);
        let mut ext = http::Extensions::new();
        ext.insert(Peer(7));
        let ctx = RequestContext::new(HeaderMap::new()).with_extensions(ext);
        assert_eq!(ctx.extensions().get::<Peer>(), Some(&Peer(7)));
    }

    #[test]
    fn request_context_with_spec_and_protocol() {
        use crate::spec::{Spec, StreamType};

        // Default-constructed context has neither.
        let ctx = RequestContext::new(HeaderMap::new());
        assert_eq!(ctx.spec(), None);
        assert_eq!(ctx.protocol(), None);

        // Both round-trip through the builders.
        const SPEC: Spec = Spec::server("/pkg.Svc/M", StreamType::Unary);
        let ctx = RequestContext::new(HeaderMap::new())
            .with_spec(Some(SPEC))
            .with_protocol(Some(crate::Protocol::Grpc));
        assert_eq!(ctx.spec(), Some(SPEC));
        assert_eq!(ctx.protocol(), Some(crate::Protocol::Grpc));

        // Builders accept `None` to clear (matches `with_deadline`).
        let ctx = ctx.with_spec(None).with_protocol(None);
        assert_eq!(ctx.spec(), None);
        assert_eq!(ctx.protocol(), None);
    }

    #[test]
    fn request_context_with_path() {
        // Hand-built contexts (tests, custom dispatchers) have no path.
        let ctx = RequestContext::new(HeaderMap::new());
        assert_eq!(ctx.path(), None);

        // Round-trips through the builder.
        let ctx = RequestContext::new(HeaderMap::new()).with_path("/pkg.Svc/M");
        assert_eq!(ctx.path(), Some("/pkg.Svc/M"));

        // The builder takes ownership (Into<String>) so callers can pass
        // borrowed or owned without an extra clone.
        let owned = String::from("/pkg.Svc/Other");
        let ctx = RequestContext::new(HeaderMap::new()).with_path(owned);
        assert_eq!(ctx.path(), Some("/pkg.Svc/Other"));

        // The builder does not normalize or validate — `Some("")` is
        // preserved verbatim. The dispatch path always supplies a non-empty
        // leading-slash form; `Some("")` only reaches consumers from a
        // misconfigured custom dispatch shim, which is a wiring bug they
        // should surface rather than silently coerce to `None`.
        let ctx = RequestContext::new(HeaderMap::new()).with_path("");
        assert_eq!(ctx.path(), Some(""));
    }
}
