//! Layer 2: serve HTTP on one already-accepted connection with the RPC
//! lifecycle.
//!
//! [`serve_connection`] is a future; whoever holds it decides which runtime
//! runs it and how it is tracked. Everything a production connection needs —
//! HTTP/1.1 and HTTP/2 settings, keepalive, the header-read timeout, max-age /
//! idle / request-count retirement with their grace period, GOAWAY on
//! shutdown, panic isolation, and `PeerAddr` / `PeerCerts` / connection
//! extensions on every request — lives here and nowhere else, so the built-in
//! [`Server`](super::Server), `connectrpc::axum::serve`, and a hand-written
//! accept loop all behave identically.

use std::any::Any;
use std::future::Future;
use std::hash::BuildHasher;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use http::Response;
use http::StatusCode;
use http::header;
use http_body_util::Full;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::rt::TokioTimer;
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use tokio::sync::watch;
use tower::ServiceExt;
use tower_http::catch_panic::CatchPanic;

use super::ConnectionConfig;
use super::ConnectionInfo;
use crate::codec::content_type;
use crate::error::ConnectError;
use crate::error::ErrorCode;

const MAX_CONNECTION_AGE_JITTER_BASIS_POINTS: u128 = 10_000;
const MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS: u128 = 1_000;
const NANOS_PER_SEC: u128 = 1_000_000_000;

/// Serve HTTP/1.1 or HTTP/2 (auto-detected) on one already-accepted,
/// already-authenticated stream, with the RPC connection lifecycle.
///
/// Applies everything in `config`: HTTP/1.1 keep-alive and the header-read
/// timeout, HTTP/2 windows / keepalive / max concurrent streams, and
/// max-age (with ±10% jitter) / idle / request-count retirement followed by
/// the shared grace period. When `shutdown` resolves the connection is told
/// to wind down (HTTP/2 GOAWAY, HTTP/1.1 keep-alive off) and the future
/// completes once in-flight requests have finished — a retirement grace
/// period never caps that drain. A panicking handler becomes a Connect
/// `internal` error response instead of tearing the connection down. Every
/// request carries `info`'s [extensions](ConnectionInfo::extensions) plus
/// [`PeerAddr`](super::PeerAddr) / `PeerCerts` for the peer `info` describes.
///
/// `service` is any tower HTTP service — a
/// [`ConnectRpcService`](crate::ConnectRpcService), an `axum::Router`, or your
/// own stack around either. HTTP upgrades (`hyper::upgrade::on`) are
/// supported; use `hyper_util::server::conn::auto::upgrade::downcast` to
/// recover the IO type.
///
/// The future resolves when the peer closes, when `shutdown` resolved and the
/// connection drained, when a retirement grace period elapsed, or on a
/// connection-level protocol error (logged at `trace`); it never fails.
/// Dropping it closes the socket abruptly.
///
/// # Runtime
///
/// Must be polled inside a tokio runtime with IO and time enabled. Nothing
/// is bound to a runtime until first poll: timers, the HTTP/2 stream tasks
/// hyper spawns, and every handler run on whichever runtime polls this
/// future, so an accept loop chooses where a connection is served simply by
/// choosing where to spawn it. The stream itself stays registered with the
/// runtime that accepted it, which must therefore outlive the connection.
#[allow(clippy::manual_async_fn, reason = "`Send` belongs in the signature")]
pub fn serve_connection<I, S, B, F>(
    io: I,
    info: ConnectionInfo,
    service: S,
    config: ConnectionConfig,
    shutdown: F,
) -> impl Future<Output = ()> + Send + 'static
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    S: tower::Service<http::Request<hyper::body::Incoming>, Response = Response<B>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    F: Future<Output = ()> + Send + 'static,
{
    async move {
        let remote_addr = info.peer_addr();
        let peer = remote_addr.map(tracing::field::display);
        tracing::trace!(remote_addr = peer, "Serving connection");

        let service = CatchPanic::custom(service, InternalErrorForPanic);

        // In-flight accounting is only needed when idle reaping is enabled;
        // when it is off there is no per-request bookkeeping overhead.
        let activity = config
            .max_connection_idle()
            .map(|_| Arc::new(ConnectionActivity::default()));

        // When request-count retirement is enabled, the service counts every
        // dispatched request and flips this latch once the limit is reached;
        // the lifecycle observes it and starts draining.
        let (request_counter, requests_exhausted) = match config.max_requests_per_connection() {
            Some(max) => {
                let (tx, rx) = watch::channel(false);
                (Some(RequestCounter::new(max, tx)), Some(rx))
            }
            None => (None, None),
        };

        // Computed once, before hyper reads the first request; cloned into
        // each request below.
        let request_extensions = info.request_extensions();
        let request_activity = activity.clone();
        let svc =
            hyper::service::service_fn(move |mut req: http::Request<hyper::body::Incoming>| {
                req.extensions_mut().extend(request_extensions.clone());
                if let Some(counter) = &request_counter {
                    counter.record_request();
                }
                // Mark the request in-flight before its future is polled; the
                // guard decrements on completion or drop.
                let guard = request_activity
                    .as_ref()
                    .map(|activity| ActiveRequestGuard::new(Arc::clone(activity)));
                let service = service.clone();
                async move {
                    let _guard = guard;
                    service.oneshot(req).await
                }
            });

        let mut builder = AutoBuilder::new(TokioExecutor::new());
        // Both protocols need a timer for any time-based behaviour (header
        // read timeout, keepalive) to take effect; without one hyper silently
        // ignores the setting or panics the connection task.
        builder
            .http1()
            .timer(TokioTimer::new())
            .keep_alive(config.http1_keep_alive())
            .header_read_timeout(config.header_read_timeout());
        configure_http2(&mut builder, &config);
        let conn = builder
            .serve_connection_with_upgrades(TokioIo::new(io), svc)
            .into_owned();

        // Max age gets per-connection jitter so a fleet of connections opened
        // together does not retire together; idle and request-count retirement
        // are reactive and need none. Each `RandomState` carries fresh keys, so
        // this is a uniform sample without a `rand` dependency.
        let max_age = config.max_connection_age().map(|age| {
            let sample = std::hash::RandomState::new().hash_one(remote_addr);
            jitter_connection_age(age, sample)
        });
        let idle = config.max_connection_idle().zip(activity);
        let grace = config.max_connection_age_grace();

        let mut conn = std::pin::pin!(conn);
        let mut shutdown = std::pin::pin!(shutdown);

        // Serve until the peer closes, the loop asks us to stop, or a
        // retirement trigger fires. `biased` keeps the connection polled first.
        let trigger = tokio::select! {
            biased;
            result = conn.as_mut() => return log_connection_result(remote_addr, result),
            () = shutdown.as_mut() => None,
            trigger = retirement(max_age, idle, requests_exhausted) => Some(trigger),
        };
        conn.as_mut().graceful_shutdown();

        // Whole-server shutdown drains for as long as in-flight requests need.
        // A retired connection drains for at most `grace`, unless shutdown
        // arrives meanwhile and lifts the cap.
        if let Some(trigger) = trigger {
            tracing::trace!(
                remote_addr = peer,
                trigger,
                ?grace,
                "Retiring connection; starting graceful shutdown"
            );
            tokio::select! {
                biased;
                result = conn.as_mut() => return log_connection_result(remote_addr, result),
                () = shutdown.as_mut() => {}
                () = tokio::time::sleep(grace) => {
                    tracing::trace!(remote_addr = peer, ?grace, "Connection retirement grace expired; closing connection");
                    return;
                }
            }
        }
        log_connection_result(remote_addr, conn.await);
    }
}

/// Resolves with the name of the first retirement trigger to fire; pending
/// forever when none is configured. Polled in the order age, idle, requests.
async fn retirement(
    max_age: Option<Duration>,
    idle: Option<(Duration, Arc<ConnectionActivity>)>,
    requests_exhausted: Option<watch::Receiver<bool>>,
) -> &'static str {
    let age = async {
        match max_age {
            Some(age) => tokio::time::sleep(age).await,
            None => std::future::pending().await,
        }
    };
    let idle = async {
        match idle {
            Some((window, activity)) => activity.quiet_for(window).await,
            None => std::future::pending().await,
        }
    };
    let requests = async {
        match requests_exhausted {
            Some(rx) => latched(rx).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        biased;
        () = age => "max age",
        () = idle => "idle",
        () = requests => "max requests",
    }
}

/// Apply the HTTP/2 half of `config` to a connection builder.
///
/// `adaptive_window` is always set explicitly so the default tracks
/// [`DEFAULT_HTTP2_ADAPTIVE_WINDOW`](super::DEFAULT_HTTP2_ADAPTIVE_WINDOW)
/// regardless of hyper's own default. Explicit window sizes are applied only
/// when adaptive sizing is off (see
/// [`ConnectionConfig::effective_http2_windows`]), so the two never reach
/// hyper at once and the precedence does not depend on hyper's internal call
/// ordering.
fn configure_http2(builder: &mut AutoBuilder<TokioExecutor>, config: &ConnectionConfig) {
    let mut http2 = builder.http2();
    http2.timer(TokioTimer::new());
    http2.adaptive_window(config.http2_adaptive_window());
    let (stream_window, connection_window) = config.effective_http2_windows();
    if let Some(size) = stream_window {
        http2.initial_stream_window_size(size);
    }
    if let Some(size) = connection_window {
        http2.initial_connection_window_size(size);
    }
    if let Some(max) = config.max_concurrent_streams() {
        http2.max_concurrent_streams(max);
    }
    // Keepalive is opt-in: when no interval is set, leave hyper's default
    // (disabled) untouched.
    if let Some(interval) = config.http2_keepalive_interval() {
        http2
            .keep_alive_interval(interval)
            .keep_alive_timeout(config.http2_keepalive_timeout());
    }
}

/// Resolves when the watch flips to `true` or its sender is dropped. Both mean
/// "begin graceful shutdown", so a connection drains rather than hangs when
/// whoever owned the sender goes away.
pub(super) async fn latched(mut rx: watch::Receiver<bool>) {
    let _ = rx.wait_for(|fired| *fired).await;
}

/// Shared in-flight request accounting for one connection.
///
/// The per-request `service_fn` wrapper bumps these counters at the dispatch
/// boundary (hyper does not surface per-connection stream counts directly), and
/// the connection lifecycle reads them to decide whether the connection has
/// been idle. `epoch` increments on every request start *and* completion, so a
/// short request that begins and ends entirely within an idle window is still
/// observed as activity and resets the idle timer.
///
/// The two counters use `SeqCst` for clarity; correctness only needs each
/// counter to be individually monotonic, so `Relaxed` would also be sound. The
/// ordering is not load-bearing — in particular [`snapshot`](Self::snapshot)
/// does not read the pair atomically (see its note).
#[derive(Debug, Default)]
struct ConnectionActivity {
    in_flight: AtomicUsize,
    epoch: AtomicU64,
}

impl ConnectionActivity {
    fn request_started(&self) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    fn request_finished(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    /// Current `(in_flight, epoch)` pair.
    ///
    /// The two fields are read as independent loads, so a request arriving in
    /// the instant between them (or between the writer's two increments in
    /// [`request_started`](Self::request_started)) can be observed as
    /// `(0, armed_epoch)` and trigger a reap while a request is actually
    /// landing. This is benign: `graceful_shutdown` sends GOAWAY with the
    /// standard last-stream-id handling and the grace window drains anything
    /// genuinely in flight, so the racing request either completes or the
    /// client retries on a fresh connection.
    fn snapshot(&self) -> (usize, u64) {
        (
            self.in_flight.load(Ordering::SeqCst),
            self.epoch.load(Ordering::SeqCst),
        )
    }

    /// Resolves once a whole `window` passes with no request in flight at its
    /// end and no request started or finished during it. Evaluated lazily —
    /// checked when the window elapses rather than re-armed per request — so
    /// it fires between one and two windows after the last activity.
    async fn quiet_for(&self, window: Duration) {
        let mut armed_epoch = self.snapshot().1;
        loop {
            tokio::time::sleep(window).await;
            let (in_flight, epoch) = self.snapshot();
            if in_flight == 0 && epoch == armed_epoch {
                return;
            }
            armed_epoch = epoch;
        }
    }
}

/// RAII guard that records a request as in-flight for the lifetime of its
/// response future. Decrementing on drop (rather than on a success path) keeps
/// the in-flight count correct even when a request future is cancelled or its
/// handler panics.
struct ActiveRequestGuard(Arc<ConnectionActivity>);

impl ActiveRequestGuard {
    fn new(activity: Arc<ConnectionActivity>) -> Self {
        activity.request_started();
        Self(activity)
    }
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.0.request_finished();
    }
}

/// Per-connection request counter that triggers retirement once the configured
/// limit is reached.
struct RequestCounter {
    served: AtomicU64,
    max: NonZeroU64,
    exhausted: watch::Sender<bool>,
}

impl RequestCounter {
    fn new(max: NonZeroU64, exhausted: watch::Sender<bool>) -> Self {
        Self {
            served: AtomicU64::new(0),
            max,
            exhausted,
        }
    }

    /// Count one dispatched request. Once the count reaches the limit, flip the
    /// retirement signal so the connection lifecycle begins graceful shutdown.
    fn record_request(&self) {
        // The atomic itself wraps on overflow (atomic ops never panic), but the
        // limit is reached long before 2^64 requests and the watch value is
        // sticky-true thereafter. `saturating_add` only clamps the local
        // comparison value so it can't wrap below the limit in that extreme.
        let served = self
            .served
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if served >= self.max.get() {
            // `send` only errs if the receiver was dropped (the connection is
            // already gone), in which case there is nothing left to retire.
            let _ = self.exhausted.send(true);
        }
    }
}

fn log_connection_result<E: std::fmt::Display>(
    remote_addr: Option<SocketAddr>,
    result: Result<(), E>,
) {
    match result {
        Ok(()) => {
            tracing::trace!(
                remote_addr = remote_addr.map(tracing::field::display),
                "Connection completed normally"
            );
        }
        Err(err) => {
            tracing::trace!(
                remote_addr = remote_addr.map(tracing::field::display),
                error = %err,
                "Connection ended with error",
            );
        }
    }
}

/// Scale `age` by a factor in `[0.9, 1.1]` chosen by `sample` (uniform over
/// `u64`), rounding so the result stays within those bounds.
fn jitter_connection_age(age: Duration, sample: u64) -> Duration {
    if age.is_zero() {
        return age;
    }

    let spread = MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS * 2;
    let offset = (u128::from(sample) * spread) / u128::from(u64::MAX);
    let basis_points = MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
        - MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS
        + offset;
    let scaled = age.as_nanos().saturating_mul(basis_points);
    let nanos = if basis_points < MAX_CONNECTION_AGE_JITTER_BASIS_POINTS {
        scaled.saturating_add(MAX_CONNECTION_AGE_JITTER_BASIS_POINTS - 1)
            / MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
    } else {
        scaled / MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
    };

    duration_from_nanos(nanos.min(Duration::MAX.as_nanos()))
}

fn duration_from_nanos(nanos: u128) -> Duration {
    Duration::new(
        (nanos / NANOS_PER_SEC) as u64,
        (nanos % NANOS_PER_SEC) as u32,
    )
}

/// Converts a handler panic into a Connect `internal` error response, logging
/// the message and (if enabled) the backtrace, so one request's panic costs
/// that request and not the connection.
#[derive(Clone, Copy)]
struct InternalErrorForPanic;

impl tower_http::catch_panic::ResponseForPanic for InternalErrorForPanic {
    type ResponseBody = Full<Bytes>;

    fn response_for_panic(&mut self, err: Box<dyn Any + Send + 'static>) -> Response<Full<Bytes>> {
        let backtrace = std::backtrace::Backtrace::capture();

        let message = if let Some(s) = err.downcast_ref::<String>() {
            s.clone()
        } else if let Some(s) = err.downcast_ref::<&str>() {
            (*s).to_string()
        } else {
            "handler panicked".to_string()
        };

        match backtrace.status() {
            std::backtrace::BacktraceStatus::Captured => {
                tracing::error!(
                    "Request handler panicked: {}\n\nBacktrace:\n{}",
                    message,
                    backtrace
                );
            }
            _ => {
                tracing::error!(
                    "Request handler panicked: {} (set RUST_BACKTRACE=1 for backtrace)",
                    message
                );
            }
        }

        let error = ConnectError::new(ErrorCode::Internal, "internal server error");
        let body = error.to_json();

        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header(header::CONTENT_TYPE, content_type::JSON)
            .body(Full::new(body))
            .unwrap_or_else(|_| {
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Full::new(Bytes::new()))
                    .unwrap()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_connection_age_jitter_stays_within_bounds() {
        let samples = [0, 1, u64::MAX / 2, u64::MAX - 1, u64::MAX];
        let ages = [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_secs(10),
            Duration::MAX,
        ];

        assert_eq!(
            jitter_connection_age(Duration::from_secs(10), 0),
            Duration::from_secs(9)
        );
        assert_eq!(
            jitter_connection_age(Duration::from_secs(10), u64::MAX),
            Duration::from_secs(11)
        );

        for age in ages {
            for sample in samples {
                let jittered = jitter_connection_age(age, sample);
                if age.is_zero() {
                    assert_eq!(jittered, Duration::ZERO);
                    continue;
                }

                assert!(
                    jittered
                        .as_nanos()
                        .saturating_mul(MAX_CONNECTION_AGE_JITTER_BASIS_POINTS)
                        >= age.as_nanos().saturating_mul(
                            MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
                                - MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS
                        ),
                    "{jittered:?} was below the 90% jitter bound for {age:?}"
                );
                assert!(
                    jittered
                        .as_nanos()
                        .saturating_mul(MAX_CONNECTION_AGE_JITTER_BASIS_POINTS)
                        <= age.as_nanos().saturating_mul(
                            MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
                                + MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS
                        ),
                    "{jittered:?} was above the 110% jitter bound for {age:?}"
                );
            }
        }
    }

    #[test]
    fn connection_activity_tracks_in_flight_and_epoch() {
        let activity = ConnectionActivity::default();
        assert_eq!(activity.snapshot(), (0, 0));

        let guard = ActiveRequestGuard::new(Arc::new(ConnectionActivity::default()));
        // The guard owns its own activity; exercise the shared-Arc path too.
        drop(guard);

        let shared = Arc::new(ConnectionActivity::default());
        let g1 = ActiveRequestGuard::new(Arc::clone(&shared));
        let g2 = ActiveRequestGuard::new(Arc::clone(&shared));
        // Two starts: in_flight == 2, epoch bumped twice.
        assert_eq!(shared.snapshot(), (2, 2));
        drop(g1);
        // One completion: in_flight back to 1, epoch bumped again.
        assert_eq!(shared.snapshot(), (1, 3));
        drop(g2);
        assert_eq!(shared.snapshot(), (0, 4));
    }

    /// `configure_http2` leaves keepalive untouched when no interval is set, so
    /// hyper's default (keepalive disabled) is preserved unless the user opts
    /// in. There is no public getter on the builder, so this guards the opt-in
    /// contract at the call boundary by exercising the default path without
    /// panicking.
    #[test]
    fn configure_http2_default_leaves_keepalive_disabled() {
        assert!(
            ConnectionConfig::default()
                .http2_keepalive_interval()
                .is_none()
        );
        let mut builder = AutoBuilder::new(TokioExecutor::new());
        configure_http2(&mut builder, &ConnectionConfig::default());
    }

    #[tokio::test]
    async fn latched_resolves_on_signal() {
        let (tx, rx) = watch::channel(false);
        let mut fut = std::pin::pin!(latched(rx));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut fut)
                .await
                .is_err(),
            "latch resolved before any signal",
        );
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), fut)
            .await
            .expect("latch must resolve after send(true)");
    }

    #[tokio::test]
    async fn latched_resolves_when_sender_dropped() {
        // On a fatal accept error the loop drops the sender without sending;
        // connections must still observe shutdown and drain rather than hang.
        let (tx, rx) = watch::channel(false);
        let fut = latched(rx);
        drop(tx);
        tokio::time::timeout(Duration::from_secs(1), fut)
            .await
            .expect("latch must resolve when the sender is dropped");
    }
}
