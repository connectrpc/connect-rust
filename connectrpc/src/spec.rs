//! Static RPC method metadata.
//!
//! [`Spec`] describes a single RPC procedure independent of any particular
//! request: its fully-qualified path, stream type, idempotency level, and
//! whether the artifact carrying the spec sits on the client or server side
//! of the wire. Code generation emits one `Spec` constant per method; the
//! runtime threads it through to handlers and (in a later release) to RPC
//! interceptors so they can label spans, route, and gate behaviour without
//! re-parsing the request URL.
//!
//! `Spec` deliberately carries only **registration-time** facts. Per-request
//! state — negotiated protocol, codec, deadline — lives on
//! [`RequestContext`](crate::RequestContext). This mirrors the split in
//! `connect-go`, where `Spec` describes the method and `Peer` describes the
//! connection.

use crate::router::MethodKind;

/// The shape of an RPC: how many messages flow in each direction.
///
/// This is the interceptor-facing equivalent of [`MethodKind`] and uses the
/// `connect-go` naming so cross-runtime interceptor logic ports cleanly.
/// Convert with [`From`] in either direction.
///
/// `StreamType` is intentionally exhaustive — the four shapes are fixed by
/// the gRPC and Connect protocols. [`MethodKind`] is the routing-table
/// equivalent used by [`Router`](crate::Router) registration; prefer
/// `StreamType` in code that consumes a [`Spec`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StreamType {
    /// One request message, one response message.
    Unary,
    /// A stream of request messages, one response message.
    ClientStream,
    /// One request message, a stream of response messages.
    ServerStream,
    /// Streams of request and response messages.
    BidiStream,
}

impl From<MethodKind> for StreamType {
    fn from(kind: MethodKind) -> Self {
        match kind {
            MethodKind::Unary => Self::Unary,
            MethodKind::ClientStreaming => Self::ClientStream,
            MethodKind::ServerStreaming => Self::ServerStream,
            MethodKind::BidiStreaming => Self::BidiStream,
        }
    }
}

impl From<StreamType> for MethodKind {
    fn from(st: StreamType) -> Self {
        match st {
            StreamType::Unary => Self::Unary,
            StreamType::ClientStream => Self::ClientStreaming,
            StreamType::ServerStream => Self::ServerStreaming,
            StreamType::BidiStream => Self::BidiStreaming,
        }
    }
}

/// The idempotency contract a method declares via
/// `option idempotency_level` in its proto definition.
///
/// Connect uses this to decide whether a unary call may be retried or sent
/// over an HTTP `GET` request. Interceptors can use it to make the same
/// decision — for example, a retry interceptor should only retry calls that
/// declare [`NoSideEffects`](IdempotencyLevel::NoSideEffects) or
/// [`Idempotent`](IdempotencyLevel::Idempotent).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum IdempotencyLevel {
    /// The method makes no idempotency guarantee. This is the proto default.
    #[default]
    Unknown,
    /// The method is read-only and safe to retry or send via `GET`.
    NoSideEffects,
    /// The method may have side effects, but repeating it with the same
    /// request is safe.
    Idempotent,
}

/// Which generated artifact produced a [`Spec`].
///
/// `Spec` constants are emitted into both the server-side dispatcher
/// (`FooServiceServer<T>`) and the generated client (`FooServiceClient<T>`).
/// `SpecOrigin` records which artifact a particular `Spec` value came from,
/// so an interceptor that runs on both sides can distinguish — e.g. open a
/// `client` span on one side and a `server` span on the other, or inject
/// trace-context headers only when [`Client`](SpecOrigin::Client).
///
/// This is an enum rather than a `bool` (`is_client`) because the domain is
/// closed and two-valued: the variant name carries the meaning at the read
/// site (`spec.origin == SpecOrigin::Client` reads better than
/// `spec.is_client`), and codegen constructs the right value via
/// [`Spec::server`] / [`Spec::client`] without a builder.
///
/// `SpecOrigin` is intentionally exhaustive — RPC artifacts are either a
/// client or a server. It is **unrelated to the HTTP `Origin` header** or
/// CORS; the name carries the `Spec` prefix to keep the distinction clear.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SpecOrigin {
    /// The `Spec` was emitted by a generated server-side dispatcher.
    Server,
    /// The `Spec` was emitted by a generated client.
    Client,
}

/// Static description of an RPC method.
///
/// One `Spec` value exists per generated method, emitted as a
/// `pub const … : Spec` in the generated service module and surfaced on
/// [`RequestContext::spec`](crate::RequestContext::spec) for handlers. It
/// names the method (`/package.Service/Method`), its stream shape, its
/// proto-declared idempotency contract, and which generated artifact
/// (server or client) produced it.
///
/// `Spec` is `Copy` and contains only `'static` data, so it can be stored,
/// captured in closures, and compared freely with no allocation.
///
/// Construct one with [`Spec::server`] or [`Spec::client`]. The struct is
/// `#[non_exhaustive]` so future fields can be added without a breaking
/// change; destructure with a trailing `..`
/// (e.g. `let Spec { procedure, stream_type, .. } = spec`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Spec {
    /// The fully-qualified procedure path, `"/package.Service/Method"`.
    ///
    /// Includes the leading slash to match the HTTP request URI and the
    /// OpenTelemetry `rpc.method` convention. The runtime strips the leading
    /// slash before [`Dispatcher::lookup`](crate::Dispatcher::lookup); use
    /// `procedure.trim_start_matches('/')` to compare against routing keys.
    pub procedure: &'static str,
    /// The message-flow shape of the method.
    pub stream_type: StreamType,
    /// Which generated artifact produced this `Spec`.
    ///
    /// Server-side dispatchers (`FooServiceServer<T>`) emit
    /// [`SpecOrigin::Server`]; generated clients emit
    /// [`SpecOrigin::Client`]. An interceptor registered on both sides
    /// reads this to pick the right span kind or trace-propagation
    /// direction.
    pub origin: SpecOrigin,
    /// The idempotency contract declared in the proto definition.
    ///
    /// This is the full three-valued proto enum. The boolean
    /// [`MethodDescriptor::idempotent`](crate::dispatcher::MethodDescriptor::idempotent)
    /// is a *derived* "Connect GET-eligible" flag that is only `true` for
    /// [`NoSideEffects`](IdempotencyLevel::NoSideEffects) — `Idempotent`
    /// methods are safe to retry but not GET-eligible.
    pub idempotency_level: IdempotencyLevel,
}

impl Spec {
    /// Construct a server-side `Spec` ([`SpecOrigin::Server`]) with the
    /// default `idempotency_level` ([`IdempotencyLevel::Unknown`]).
    ///
    /// Generated server-side dispatchers chain
    /// [`with_idempotency_level`](Spec::with_idempotency_level) onto this
    /// constructor in `const` position, so `Spec` constants live in
    /// `.rodata`.
    ///
    /// In debug builds, asserts that `procedure` starts with `/` and
    /// contains a `/Service/Method` separator so a malformed test fixture
    /// fails loudly rather than producing misleading [`service`](Spec::service)
    /// / [`method`](Spec::method) accessor results.
    pub const fn server(procedure: &'static str, stream_type: StreamType) -> Self {
        debug_assert_well_formed(procedure);
        Self {
            procedure,
            stream_type,
            origin: SpecOrigin::Server,
            idempotency_level: IdempotencyLevel::Unknown,
        }
    }

    /// Construct a client-side `Spec` ([`SpecOrigin::Client`]) with the
    /// default `idempotency_level` ([`IdempotencyLevel::Unknown`]).
    ///
    /// Generated clients chain
    /// [`with_idempotency_level`](Spec::with_idempotency_level) onto this
    /// constructor in `const` position, so `Spec` constants live in
    /// `.rodata`.
    ///
    /// In debug builds, asserts that `procedure` starts with `/` and
    /// contains a `/Service/Method` separator so a malformed test fixture
    /// fails loudly rather than producing misleading [`service`](Spec::service)
    /// / [`method`](Spec::method) accessor results.
    pub const fn client(procedure: &'static str, stream_type: StreamType) -> Self {
        debug_assert_well_formed(procedure);
        Self {
            procedure,
            stream_type,
            origin: SpecOrigin::Client,
            idempotency_level: IdempotencyLevel::Unknown,
        }
    }

    /// Set the idempotency level. Returns `self` for chaining in `const`
    /// position.
    #[must_use]
    pub const fn with_idempotency_level(mut self, idempotency_level: IdempotencyLevel) -> Self {
        self.idempotency_level = idempotency_level;
        self
    }

    /// The bare service name (`"package.Service"`) from
    /// [`procedure`](Spec::procedure), without the leading slash or trailing
    /// `/Method`.
    ///
    /// Returns the whole procedure (sans leading `/`) if it contains no
    /// method separator, which never happens for generated specs (the
    /// constructors `debug_assert!` on it).
    // TODO: make `const` once `str::rsplit_once` is const-stable.
    pub fn service(&self) -> &'static str {
        let p = self.procedure.trim_start_matches('/');
        p.rsplit_once('/').map(|(svc, _)| svc).unwrap_or(p)
    }

    /// The bare method name (`"Method"`) from [`procedure`](Spec::procedure).
    ///
    /// Returns the whole procedure (sans leading `/`) if it contains no
    /// method separator, which never happens for generated specs (the
    /// constructors `debug_assert!` on it).
    // TODO: make `const` once `str::rsplit_once` is const-stable.
    pub fn method(&self) -> &'static str {
        let p = self.procedure.trim_start_matches('/');
        p.rsplit_once('/').map(|(_, m)| m).unwrap_or(p)
    }
}

/// `const fn` debug assertion that a procedure path looks like
/// `"/package.Service/Method"`: leading slash and at least one interior
/// slash separating the service from the method.
///
/// This is a `const fn` so [`Spec::server`] / [`Spec::client`] stay
/// const-evaluable: a malformed procedure in a `const SPEC: Spec` will
/// surface as a *compile-time* panic on a debug build of the consuming
/// crate, not a silent mis-parse at runtime. Compiles to nothing in
/// release builds.
const fn debug_assert_well_formed(procedure: &str) {
    if cfg!(debug_assertions) {
        let bytes = procedure.as_bytes();
        // Must start with '/'.
        assert!(
            !bytes.is_empty() && bytes[0] == b'/',
            "Spec procedure must start with '/' (e.g. \"/pkg.Service/Method\")"
        );
        // Must have a second '/' separating Service from Method.
        let mut has_inner_slash = false;
        let mut i = 1;
        while i < bytes.len() {
            if bytes[i] == b'/' {
                has_inner_slash = true;
                break;
            }
            i += 1;
        }
        assert!(
            has_inner_slash,
            "Spec procedure must contain a '/Service/Method' separator (e.g. \"/pkg.Service/Method\")"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_type_round_trips_method_kind() {
        for kind in [
            MethodKind::Unary,
            MethodKind::ServerStreaming,
            MethodKind::ClientStreaming,
            MethodKind::BidiStreaming,
        ] {
            assert_eq!(MethodKind::from(StreamType::from(kind)), kind);
        }
    }

    #[test]
    fn spec_const_construction_and_accessors() {
        const SPEC: Spec = Spec::server("/pkg.Greet/Say", StreamType::Unary)
            .with_idempotency_level(IdempotencyLevel::NoSideEffects);
        assert_eq!(SPEC.procedure, "/pkg.Greet/Say");
        assert_eq!(SPEC.service(), "pkg.Greet");
        assert_eq!(SPEC.method(), "Say");
        assert_eq!(SPEC.stream_type, StreamType::Unary);
        assert_eq!(SPEC.idempotency_level, IdempotencyLevel::NoSideEffects);
        const { assert!(matches!(SPEC.origin, SpecOrigin::Server)) };
    }

    #[test]
    fn spec_client_const_construction() {
        const SPEC: Spec = Spec::client("/pkg.Greet/Say", StreamType::Unary);
        assert_eq!(SPEC.origin, SpecOrigin::Client);
        assert_eq!(SPEC.idempotency_level, IdempotencyLevel::Unknown);
    }

    #[test]
    fn spec_defaults() {
        let s = Spec::server("/a.B/C", StreamType::BidiStream);
        assert_eq!(s.idempotency_level, IdempotencyLevel::Unknown);
        assert_eq!(s.origin, SpecOrigin::Server);
    }

    #[test]
    #[cfg_attr(
        debug_assertions,
        should_panic(expected = "Spec procedure must contain a '/Service/Method' separator")
    )]
    fn spec_malformed_path_no_method_separator_debug_asserts() {
        let _ = Spec::server("/nopath", StreamType::Unary);
    }

    #[test]
    #[cfg_attr(
        debug_assertions,
        should_panic(expected = "Spec procedure must start with '/'")
    )]
    fn spec_malformed_path_no_leading_slash_debug_asserts() {
        let _ = Spec::server("pkg.Service/Method", StreamType::Unary);
    }

    #[test]
    #[cfg(not(debug_assertions))]
    fn spec_service_method_no_separator_release_fallback() {
        // In release builds debug_assert_well_formed is a no-op, so this is
        // the documented fallback behaviour.
        let s = Spec::server("/nopath", StreamType::Unary);
        assert_eq!(s.service(), "nopath");
        assert_eq!(s.method(), "nopath");
    }
}
