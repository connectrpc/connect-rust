//! Server-side moderation and enforcement of client-asserted RPC deadlines.
//!
//! Connect and gRPC clients send a per-request timeout header
//! (`Connect-Timeout-Ms` or `grpc-timeout`). Without moderation, a client
//! controls server resource lifetime: a `Connect-Timeout-Ms: 1` request
//! cancels the handler before it starts, while a `Connect-Timeout-Ms:
//! 86400000` request holds a worker for 24 hours. [`DeadlinePolicy`] gives
//! the server a place to clamp the asserted value to an operationally sane
//! range, supply a default when the client asserts nothing, and (opt-in)
//! extend enforcement to streaming response bodies.
//!
//! The stream-body deadline wrapper bounds work driven by polling the
//! response stream. Handlers that `tokio::spawn` detached tasks must clean
//! those up themselves; the wrapper drops the response stream future,
//! which propagates cancellation to anything awaited inside it but not to
//! spawned tasks.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use pin_project::pin_project;
use tokio::time::Sleep;

use crate::error::ConnectError;
use crate::handler::BoxStream;

/// Server-side moderation of client-asserted RPC deadlines.
///
/// Clients send a `Connect-Timeout-Ms` (or `grpc-timeout`) header that the
/// server uses to bound the request. Without moderation a client can
/// assert an absurdly short timeout — forcing premature cancellation
/// mid-write — or an absurdly long one — holding server resources for
/// hours. `DeadlinePolicy` clamps the client value to a server-controlled
/// range, applies a server-side default when the client asserts nothing,
/// and can extend enforcement to streaming bodies (whose initial setup is
/// already bounded by the server's `tokio::time::timeout`, but whose item
/// stream is unbounded by default).
///
/// Construct via [`DeadlinePolicy::new`] and the `with_*` builders; the
/// field set is `#[non_exhaustive]` so struct-literal construction is not
/// available outside the crate.
///
/// ## Default behavior
///
/// [`DeadlinePolicy::new`] is a no-op policy: `min` is zero, `max` and
/// `default` are unset, and `enforce_on_streams` and `inter_message_timeout`
/// are off. Existing services that don't set a policy see no behavioral
/// change relative to prior releases. Services expecting untrusted clients
/// should set at least a [`with_max`] cap.
///
/// ## Combination matrix
///
/// With `min = 10ms`, `max = 100ms`, `default = 50ms`:
///
/// | Client asserts | Effective timeout | Why |
/// |---|---|---|
/// | `20ms` | `20ms` | in range, passthrough |
/// | `1ms` | `10ms` | clamped up to `min` (debug log) |
/// | `500ms` | `100ms` | clamped down to `max` (debug log) |
/// | (none) | `50ms` | `default` applied |
/// | `garbage` | `50ms` | unparseable header treated as absent |
/// | (none, no `default`) | (none) | unbounded |
///
/// ## Example
///
/// ```rust
/// use connectrpc::DeadlinePolicy;
/// use std::time::Duration;
///
/// // A policy suitable for a low-latency online service: clamp client
/// // values to 5ms–10s, default to 5s when the client asserts nothing,
/// // and cut off any stream that runs past the deadline.
/// let policy = DeadlinePolicy::new()
///     .with_min(Duration::from_millis(5))
///     .with_max(Duration::from_secs(10))
///     .with_default_timeout(Duration::from_secs(5))
///     .with_enforce_on_streams(true);
/// assert_eq!(policy.max(), Some(Duration::from_secs(10)));
/// ```
///
/// [`with_max`]: DeadlinePolicy::with_max
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct DeadlinePolicy {
    /// Floor: client values below this are clamped up. Defaults to
    /// `Duration::ZERO`, which is a no-op floor.
    min: Duration,
    /// Cap: client values above this are clamped down. `None` = no cap.
    max: Option<Duration>,
    /// Applied when the client asserts no timeout (or an unparseable one).
    default: Option<Duration>,
    /// Whether to wrap streaming bodies (server/bidi) in the absolute
    /// deadline. `false` (the default) preserves the prior default
    /// behavior: only the time-to-first-response is bounded. Independent
    /// of [`inter_message_timeout`](Self::inter_message_timeout).
    enforce_on_streams: bool,
    /// Maximum gap between yielded items on a streaming body. Detects
    /// stalled handlers waiting on slow upstreams. `None` = no per-item
    /// bound. Independent of [`enforce_on_streams`](Self::enforce_on_streams):
    /// setting this enables the per-item timer regardless of whether the
    /// absolute deadline is enforced.
    inter_message_timeout: Option<Duration>,
}

impl DeadlinePolicy {
    /// Create a no-op policy — no clamping, no default timeout, no stream
    /// enforcement. Preserves the prior default behavior.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the floor for client-asserted timeouts. Client values below
    /// this are clamped up before enforcement.
    ///
    /// Protects against premature cancellation from a misbehaving or
    /// adversarial client asserting a near-zero timeout. Defaults to
    /// [`Duration::ZERO`], a no-op floor.
    #[must_use]
    pub fn with_min(mut self, min: Duration) -> Self {
        self.min = min;
        self
    }

    /// Set the cap for client-asserted timeouts. Client values above this
    /// are clamped down before enforcement.
    ///
    /// Protects server resources against a client asserting an
    /// unreasonably long timeout. There is no default cap — set one for
    /// any service that accepts untrusted callers.
    #[must_use]
    pub fn with_max(mut self, max: Duration) -> Self {
        self.max = Some(max);
        self
    }

    /// Set the timeout applied when the client asserts none.
    ///
    /// Without a default, a request with no timeout header runs unbounded.
    /// Setting this to your SLA is the cheapest hardening step a service
    /// can take. The default is server-controlled and is not subject to
    /// the `min`/`max` clamps — those guard against the *client*.
    #[must_use]
    pub fn with_default_timeout(mut self, default: Duration) -> Self {
        self.default = Some(default);
        self
    }

    /// Extend the absolute-deadline enforcement to streaming response
    /// bodies.
    ///
    /// By default only the time-to-first-response is bounded by the
    /// deadline: a server- or bidi-streaming handler that returns its
    /// stream quickly can yield items unbounded thereafter. Setting this
    /// to `true` wraps the response body so the next item after the
    /// deadline is a [`deadline_exceeded`](ConnectError::deadline_exceeded)
    /// error, after which the stream ends.
    ///
    /// On the wire: Connect emits `code: deadline_exceeded` in the
    /// trailing `EndStreamResponse` JSON; gRPC emits `grpc-status: 4` in
    /// trailing headers.
    ///
    /// Cancellation drops the inner stream at the next yield point — the
    /// standard Rust idiom, no grace period. A handler that needs to
    /// commit work after the client gives up should `tokio::spawn` the
    /// commit-critical path off the request future, or leave this `false`.
    ///
    /// Independent of [`with_inter_message_timeout`](Self::with_inter_message_timeout):
    /// this knob controls the *absolute* deadline; the inter-message timer
    /// can be set with or without it.
    #[must_use]
    pub fn with_enforce_on_streams(mut self, enforce: bool) -> Self {
        self.enforce_on_streams = enforce;
        self
    }

    /// Set the maximum gap between yielded items on a streaming body.
    ///
    /// Detects stalled handlers — a stream that goes quiet for longer
    /// than this errors with
    /// [`deadline_exceeded`](ConnectError::deadline_exceeded) and ends.
    /// Independent of [`with_enforce_on_streams`](Self::with_enforce_on_streams):
    /// the per-item timer takes effect whenever this is set, regardless of
    /// whether the absolute deadline is also enforced on the stream.
    #[must_use]
    pub fn with_inter_message_timeout(mut self, timeout: Duration) -> Self {
        self.inter_message_timeout = Some(timeout);
        self
    }

    /// The configured floor (defaults to [`Duration::ZERO`]).
    #[must_use]
    pub fn min(&self) -> Duration {
        self.min
    }

    /// The configured cap, or `None` if none is set.
    #[must_use]
    pub fn max(&self) -> Option<Duration> {
        self.max
    }

    /// The configured default timeout, or `None` if none is set.
    #[must_use]
    pub fn default_timeout(&self) -> Option<Duration> {
        self.default
    }

    /// Whether streaming response bodies are wrapped in the absolute
    /// deadline.
    #[must_use]
    pub fn enforce_on_streams(&self) -> bool {
        self.enforce_on_streams
    }

    /// The configured inter-message timeout, or `None` if none is set.
    #[must_use]
    pub fn inter_message_timeout(&self) -> Option<Duration> {
        self.inter_message_timeout
    }

    /// Apply the policy to a client-asserted timeout.
    ///
    /// Returns the *effective* timeout the server will enforce: `client`
    /// clamped to `[min, max]`, or `default` when `client` is `None`
    /// (the request had no timeout header or the header was unparseable).
    /// Returns `None` when there is no effective bound.
    ///
    /// When clamping changes the client's value, emits a
    /// `tracing::debug!` event (target `connectrpc::deadline`) with the
    /// request path and the before/after durations so operators can spot
    /// misbehaving clients.
    pub(crate) fn moderate(&self, client: Option<Duration>, path: &str) -> Option<Duration> {
        match client {
            Some(asserted) => {
                // `Ord::clamp` panics when `min > max`; if a misconfigured
                // policy has `min > max`, treat `max` as the effective bound
                // (the more conservative choice for server resource use).
                let upper = self.max.unwrap_or(Duration::MAX);
                let lower = self.min.min(upper);
                let clamped = asserted.clamp(lower, upper);
                if clamped != asserted {
                    tracing::debug!(
                        target: "connectrpc::deadline",
                        path,
                        client_timeout_ms =
                            u64::try_from(asserted.as_millis()).unwrap_or(u64::MAX),
                        effective_timeout_ms =
                            u64::try_from(clamped.as_millis()).unwrap_or(u64::MAX),
                        "client-asserted timeout clamped by server DeadlinePolicy",
                    );
                }
                Some(clamped)
            }
            None => self.default,
        }
    }

    /// Wrap a streaming response body with deadline enforcement, if the
    /// policy calls for it.
    ///
    /// `remaining` is the time left in the request's absolute deadline
    /// budget at the point the stream starts (`None` if the request has
    /// no deadline). The wrapper is created when either
    /// `enforce_on_streams` is `true` *and* a deadline is in effect, or
    /// `inter_message_timeout` is set — the two are independent. When
    /// neither applies, the stream is returned unchanged so the no-op
    /// policy adds no overhead.
    ///
    /// Requires a tokio runtime; constructs `tokio::time::Sleep` internally.
    pub(crate) fn enforce_on_response_stream(
        &self,
        stream: BoxStream<Result<Bytes, ConnectError>>,
        remaining: Option<Duration>,
    ) -> BoxStream<Result<Bytes, ConnectError>> {
        let absolute = if self.enforce_on_streams {
            remaining
        } else {
            None
        };
        // Independent of `enforce_on_streams` — a per-item timer is useful
        // even on requests that have no absolute deadline.
        let per_item = self.inter_message_timeout;
        if absolute.is_none() && per_item.is_none() {
            return stream;
        }
        Box::pin(DeadlineStream::new(stream, absolute, per_item))
    }
}

/// A streaming body wrapper that enforces an absolute deadline and an
/// optional inter-message timeout.
///
/// On either bound lapsing, yields a single
/// [`deadline_exceeded`](ConnectError::deadline_exceeded) error and then
/// ends. The inner stream is dropped on the first poll after the bound
/// lapses.
#[pin_project]
struct DeadlineStream<S> {
    #[pin]
    inner: Option<S>,
    /// Absolute deadline timer, armed once at construction.
    #[pin]
    absolute: Option<Sleep>,
    /// Inter-message timer, re-armed after each yielded item.
    #[pin]
    per_item: Option<Sleep>,
    inter_message: Option<Duration>,
    finished: bool,
}

impl<S> DeadlineStream<S> {
    fn new(inner: S, absolute: Option<Duration>, inter_message: Option<Duration>) -> Self {
        Self {
            inner: Some(inner),
            absolute: absolute.map(tokio::time::sleep),
            // Do NOT arm the inter-message timer at construction. There is no
            // prior message yet, so starting the timer here would measure
            // stream-setup latency rather than the gap between messages. The
            // lazy arm in `poll_next` starts the timer on the first poll.
            per_item: None,
            inter_message,
            finished: false,
        }
    }
}

impl<S> Stream for DeadlineStream<S>
where
    S: Stream<Item = Result<Bytes, ConnectError>>,
{
    type Item = Result<Bytes, ConnectError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.finished {
            return Poll::Ready(None);
        }

        // Lazily arm the inter-message timer on the first poll so that
        // stream-setup latency before the consumer starts reading is excluded
        // from the first gap measurement.
        if this.per_item.is_none() {
            if let Some(d) = this.inter_message {
                this.per_item.set(Some(tokio::time::sleep(*d)));
            }
        }

        // Check the absolute deadline first — once it lapses the whole
        // request is over regardless of per-item progress.
        if let Some(sleep) = this.absolute.as_mut().as_pin_mut()
            && sleep.poll(cx).is_ready()
        {
            *this.finished = true;
            this.inner.set(None);
            return Poll::Ready(Some(Err(ConnectError::deadline_exceeded(
                "request deadline exceeded while streaming",
            ))));
        }

        // Then the inter-message timer — detects a stalled handler.
        if let Some(sleep) = this.per_item.as_mut().as_pin_mut()
            && sleep.poll(cx).is_ready()
        {
            *this.finished = true;
            this.inner.set(None);
            return Poll::Ready(Some(Err(ConnectError::deadline_exceeded(
                "stream stalled past inter-message timeout",
            ))));
        }

        // Poll the inner stream.
        let Some(inner) = this.inner.as_mut().as_pin_mut() else {
            *this.finished = true;
            return Poll::Ready(None);
        };
        match inner.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                // Re-arm the inter-message timer for the next gap.
                if let Some(d) = this.inter_message {
                    this.per_item.set(Some(tokio::time::sleep(*d)));
                }
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                *this.finished = true;
                this.inner.set(None);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn no_op_policy_is_passthrough() {
        let p = DeadlinePolicy::new();
        assert_eq!(p.moderate(Some(ms(5)), "/svc/m"), Some(ms(5)));
        assert_eq!(p.moderate(None, "/svc/m"), None);
    }

    #[test]
    fn min_clamps_up() {
        let p = DeadlinePolicy::new().with_min(ms(10));
        assert_eq!(p.moderate(Some(ms(1)), "/svc/m"), Some(ms(10)));
        assert_eq!(p.moderate(Some(ms(50)), "/svc/m"), Some(ms(50)));
    }

    #[test]
    fn max_clamps_down() {
        let p = DeadlinePolicy::new().with_max(ms(100));
        assert_eq!(p.moderate(Some(ms(500)), "/svc/m"), Some(ms(100)));
        assert_eq!(p.moderate(Some(ms(50)), "/svc/m"), Some(ms(50)));
    }

    #[test]
    fn default_applies_when_client_absent() {
        let p = DeadlinePolicy::new().with_default_timeout(ms(200));
        assert_eq!(p.moderate(None, "/svc/m"), Some(ms(200)));
        // Default does NOT replace an asserted value.
        assert_eq!(p.moderate(Some(ms(50)), "/svc/m"), Some(ms(50)));
    }

    #[test]
    fn no_default_no_client_is_unbounded() {
        let p = DeadlinePolicy::new().with_min(ms(1)).with_max(ms(100));
        assert_eq!(p.moderate(None, "/svc/m"), None);
    }

    #[test]
    fn misconfigured_min_above_max_does_not_panic() {
        // `Ord::clamp` panics on `min > max`; the policy must not. The
        // conservative choice (smallest server resource use) is to honor
        // `max` as both bounds.
        let p = DeadlinePolicy::new().with_min(ms(500)).with_max(ms(100));
        assert_eq!(p.moderate(Some(ms(1)), "/svc/m"), Some(ms(100)));
        assert_eq!(p.moderate(Some(ms(700)), "/svc/m"), Some(ms(100)));
    }

    #[test]
    fn full_matrix() {
        let p = DeadlinePolicy::new()
            .with_min(ms(10))
            .with_max(ms(100))
            .with_default_timeout(ms(50));
        // In range — passthrough.
        assert_eq!(p.moderate(Some(ms(20)), "/svc/m"), Some(ms(20)));
        // Below min — clamp up.
        assert_eq!(p.moderate(Some(ms(1)), "/svc/m"), Some(ms(10)));
        // Above max — clamp down.
        assert_eq!(p.moderate(Some(ms(500)), "/svc/m"), Some(ms(100)));
        // Absent — default.
        assert_eq!(p.moderate(None, "/svc/m"), Some(ms(50)));
        // Boundary.
        assert_eq!(p.moderate(Some(ms(10)), "/svc/m"), Some(ms(10)));
        assert_eq!(p.moderate(Some(ms(100)), "/svc/m"), Some(ms(100)));
    }

    #[test]
    fn accessors_round_trip() {
        let p = DeadlinePolicy::new()
            .with_min(ms(1))
            .with_max(ms(2))
            .with_default_timeout(ms(3))
            .with_enforce_on_streams(true)
            .with_inter_message_timeout(ms(4));
        assert_eq!(p.min(), ms(1));
        assert_eq!(p.max(), Some(ms(2)));
        assert_eq!(p.default_timeout(), Some(ms(3)));
        assert!(p.enforce_on_streams());
        assert_eq!(p.inter_message_timeout(), Some(ms(4)));
    }

    #[tokio::test(start_paused = true)]
    async fn enforce_no_op_returns_original_stream() {
        // With enforce_on_streams=false and no inter_message_timeout, the
        // stream is returned unchanged (no allocation, no wrapper) and runs
        // unbounded — even when a deadline is nominally in effect.
        let p = DeadlinePolicy::new();
        let inner: BoxStream<Result<Bytes, ConnectError>> = Box::pin(
            futures::stream::iter([Ok(Bytes::from_static(b"a"))]).chain(futures::stream::pending()),
        );
        let mut wrapped = p.enforce_on_response_stream(inner, Some(ms(1)));
        assert_eq!(
            wrapped.next().await.unwrap().unwrap(),
            Bytes::from_static(b"a")
        );
        // Advance well past what would have been the deadline; the
        // unwrapped stream stays pending rather than erroring.
        tokio::time::advance(ms(10)).await;
        let pending = futures::poll!(wrapped.next());
        assert!(pending.is_pending());
    }

    #[tokio::test(start_paused = true)]
    async fn fast_stream_completes_under_deadline() {
        let p = DeadlinePolicy::new().with_enforce_on_streams(true);
        let inner: BoxStream<Result<Bytes, ConnectError>> = Box::pin(futures::stream::iter([
            Ok(Bytes::from_static(b"a")),
            Ok(Bytes::from_static(b"b")),
        ]));
        let mut wrapped = p.enforce_on_response_stream(inner, Some(Duration::from_secs(60)));
        assert_eq!(
            wrapped.next().await.unwrap().unwrap(),
            Bytes::from_static(b"a")
        );
        assert_eq!(
            wrapped.next().await.unwrap().unwrap(),
            Bytes::from_static(b"b")
        );
        assert!(wrapped.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn slow_stream_cut_off_at_deadline() {
        let p = DeadlinePolicy::new().with_enforce_on_streams(true);
        // A stream that yields one item then hangs forever.
        let inner: BoxStream<Result<Bytes, ConnectError>> = Box::pin(
            futures::stream::iter([Ok(Bytes::from_static(b"a"))]).chain(futures::stream::pending()),
        );
        let mut wrapped = p.enforce_on_response_stream(inner, Some(ms(100)));
        assert_eq!(
            wrapped.next().await.unwrap().unwrap(),
            Bytes::from_static(b"a")
        );
        // Advance past the deadline. With `start_paused`, the next poll
        // sees the lapsed deadline.
        tokio::time::advance(ms(200)).await;
        let err = wrapped.next().await.unwrap().unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::DeadlineExceeded);
        // After the error, the stream is finished.
        assert!(wrapped.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn inter_message_timeout_cuts_off_stalled_stream() {
        let p = DeadlinePolicy::new()
            .with_enforce_on_streams(true)
            .with_inter_message_timeout(ms(50));
        // Yields one item, then stalls forever — never reaches the
        // (long) absolute deadline, but the inter-message timer fires.
        let inner: BoxStream<Result<Bytes, ConnectError>> = Box::pin(
            futures::stream::iter([Ok(Bytes::from_static(b"a"))]).chain(futures::stream::pending()),
        );
        let mut wrapped = p.enforce_on_response_stream(inner, Some(Duration::from_secs(3600)));
        assert_eq!(
            wrapped.next().await.unwrap().unwrap(),
            Bytes::from_static(b"a")
        );
        tokio::time::advance(ms(100)).await;
        let err = wrapped.next().await.unwrap().unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::DeadlineExceeded);
        assert!(err.message.as_deref().unwrap().contains("inter-message"));
    }

    #[tokio::test(start_paused = true)]
    async fn inter_message_timeout_independent_of_enforce_on_streams() {
        // `inter_message_timeout` is independent of `enforce_on_streams`:
        // the per-item timer takes effect even when the absolute deadline
        // is not enforced on the stream.
        let p = DeadlinePolicy::new().with_inter_message_timeout(ms(50));
        assert!(!p.enforce_on_streams());
        let inner: BoxStream<Result<Bytes, ConnectError>> = Box::pin(
            futures::stream::iter([Ok(Bytes::from_static(b"a"))]).chain(futures::stream::pending()),
        );
        // No absolute deadline at all — only the per-item timer runs.
        let mut wrapped = p.enforce_on_response_stream(inner, None);
        assert_eq!(
            wrapped.next().await.unwrap().unwrap(),
            Bytes::from_static(b"a")
        );
        tokio::time::advance(ms(100)).await;
        let err = wrapped.next().await.unwrap().unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::DeadlineExceeded);
        assert!(err.message.as_deref().unwrap().contains("inter-message"));
    }

    #[tokio::test(start_paused = true)]
    async fn no_deadline_no_inter_message_streams_unbounded() {
        // enforce_on_streams=true but no deadline (no client timeout, no
        // default) and no inter_message_timeout — the wrapper is skipped.
        let p = DeadlinePolicy::new().with_enforce_on_streams(true);
        let inner: BoxStream<Result<Bytes, ConnectError>> =
            Box::pin(futures::stream::iter([Ok(Bytes::from_static(b"a"))]));
        let mut wrapped = p.enforce_on_response_stream(inner, None);
        assert_eq!(
            wrapped.next().await.unwrap().unwrap(),
            Bytes::from_static(b"a")
        );
        assert!(wrapped.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn setup_latency_before_first_poll_does_not_trigger_timeout() {
        let p = DeadlinePolicy::new().with_inter_message_timeout(ms(50));
        let inner: BoxStream<Result<Bytes, ConnectError>> =
            Box::pin(futures::stream::iter([Ok(Bytes::from_static(b"a"))]));
        let mut wrapped = p.enforce_on_response_stream(inner, None);

        tokio::time::advance(ms(100)).await;

        let item = wrapped.next().await.unwrap();
        assert!(item.is_ok(), "expected first item but got deadline error: {:?}", item);
        assert_eq!(item.unwrap(), Bytes::from_static(b"a"));
    }

    #[tokio::test(start_paused = true)]
    async fn stream_that_never_yields_still_times_out() {
        let p = DeadlinePolicy::new().with_inter_message_timeout(ms(50));
        let inner: BoxStream<Result<Bytes, ConnectError>> =
            Box::pin(futures::stream::pending());
        let mut wrapped = p.enforce_on_response_stream(inner, None);

        let first = futures::poll!(wrapped.next());
        assert!(first.is_pending());

        tokio::time::advance(ms(100)).await;

        let err = wrapped.next().await.unwrap().unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::DeadlineExceeded);
        assert!(err.message.as_deref().unwrap().contains("inter-message"));
        assert!(wrapped.next().await.is_none());
    }
}
