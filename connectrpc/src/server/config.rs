//! Per-connection and accept-time settings as plain values.
//!
//! [`ConnectionConfig`] is everything the connection driver
//! ([`serve_connection`](super::serve_connection)) applies to one connection;
//! [`AcceptConfig`] is everything the [`Acceptor`](super::Acceptor) applies
//! between `accept(2)` and handing over an authenticated stream. Both have a
//! `Default`, so `Server`, `BoundServer` and custom accept loops are configured
//! from the same value and cannot drift.

use std::num::NonZeroU64;
#[cfg(feature = "server-tls")]
use std::sync::Arc;
use std::time::Duration;

/// Default TLS handshake timeout.
///
/// Bounds how long the server waits after TCP accept for a client to complete
/// the TLS handshake. Prevents slowloris-style connection-exhaustion attacks
/// where a client opens a TCP connection and stalls the handshake indefinitely,
/// holding a task and file descriptor per connection.
///
/// Override via [`AcceptConfig::with_tls_handshake_timeout`] (or the
/// `with_tls_handshake_timeout` shorthand on `Server` / `BoundServer`).
#[cfg(feature = "server-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
pub const DEFAULT_TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default HTTP/1.1 header read timeout.
///
/// Bounds how long the server waits to receive a complete set of request
/// headers, measured from the point hyper begins reading a new request on the
/// connection. On a keep-alive connection this also bounds the idle wait
/// between requests, so a peer that opens a connection (or finishes one
/// request) and then stalls without sending the next request's headers is
/// disconnected rather than holding a task and file descriptor open
/// indefinitely. This mitigates slowloris-style connection-exhaustion attacks.
///
/// Applies to HTTP/1.1 only; it does not bound idle or stalled HTTP/2
/// connections — use `with_max_connection_age` to retire those by age.
///
/// Override via [`ConnectionConfig::with_header_read_timeout`]; pass `None` to
/// disable.
pub const DEFAULT_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for an HTTP/2 keepalive PING acknowledgement.
///
/// Once an HTTP/2 keepalive interval is set via
/// [`ConnectionConfig::with_http2_keepalive_interval`], the server waits this
/// long for the peer to acknowledge a PING before treating the connection as
/// dead and closing it. Matches the 20-second default used by grpc-go,
/// grpc-java, and tonic. Override with
/// [`ConnectionConfig::with_http2_keepalive_timeout`].
pub const DEFAULT_HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// Default for HTTP/2 adaptive (BDP-based) flow-control window sizing.
///
/// Enabled by default so connections over high bandwidth-delay-product links
/// (cross-region, high-throughput streaming) are not throttled by hyper's
/// fixed 64 KiB stream/connection windows. hyper grows the window based on the
/// measured bandwidth-delay product, matching grpc-go and grpc-java, which both
/// autotune by default. The trade-off is slightly higher per-connection memory
/// under load.
///
/// Disable with [`ConnectionConfig::with_http2_adaptive_window`], or override
/// the windows explicitly with the `with_http2_initial_*_window_size` setters
/// (which turn adaptive sizing off).
pub const DEFAULT_HTTP2_ADAPTIVE_WINDOW: bool = true;

/// Default drain window after a per-connection retirement trigger fires.
pub(crate) const DEFAULT_MAX_CONNECTION_AGE_GRACE: Duration = Duration::from_secs(5);

/// Everything the connection driver applies to one connection: HTTP/1.1 and
/// HTTP/2 protocol settings, keepalive, the header-read timeout, and the three
/// retirement triggers (age, idle, request count) with their shared grace
/// period.
///
/// A plain value with a [`Default`]; build one and hand it to
/// [`Server::with_connection_config`](super::Server::with_connection_config),
/// [`BoundServer::with_connection_config`](super::BoundServer::with_connection_config),
/// or [`serve_connection`](super::serve_connection) directly. The `with_*`
/// setters of the same names on `Server` / `BoundServer` are shorthand for
/// editing this value in place.
///
/// HTTP/2 adaptive window sizing and the explicit window sizes are mutually
/// exclusive in hyper; the setters keep them consistent (supplying a size
/// turns adaptive sizing off, re-enabling adaptive sizing wins over any stored
/// size).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectionConfig {
    http1_keep_alive: bool,
    header_read_timeout: Option<Duration>,
    http2_adaptive_window: bool,
    http2_initial_stream_window_size: Option<u32>,
    http2_initial_connection_window_size: Option<u32>,
    max_concurrent_streams: Option<u32>,
    http2_keepalive_interval: Option<Duration>,
    http2_keepalive_timeout: Duration,
    max_connection_age: Option<Duration>,
    max_connection_age_grace: Duration,
    max_connection_idle: Option<Duration>,
    max_requests_per_connection: Option<NonZeroU64>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            http1_keep_alive: true,
            header_read_timeout: Some(DEFAULT_HEADER_READ_TIMEOUT),
            http2_adaptive_window: DEFAULT_HTTP2_ADAPTIVE_WINDOW,
            http2_initial_stream_window_size: None,
            http2_initial_connection_window_size: None,
            max_concurrent_streams: None,
            http2_keepalive_interval: None,
            http2_keepalive_timeout: DEFAULT_HTTP2_KEEPALIVE_TIMEOUT,
            max_connection_age: None,
            max_connection_age_grace: DEFAULT_MAX_CONNECTION_AGE_GRACE,
            max_connection_idle: None,
            max_requests_per_connection: None,
        }
    }
}

impl ConnectionConfig {
    /// The defaults: HTTP/1.1 keep-alive on, a 30-second header read timeout,
    /// adaptive HTTP/2 windows, no keepalive PINGs, and no retirement trigger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable HTTP/1.1 keep-alive (default: enabled).
    ///
    /// When disabled, the server sends `Connection: close` and handles
    /// only one request per TCP connection. This avoids stale-connection
    /// races where the server closes an idle connection at the same time
    /// the client sends a new request on it.
    ///
    /// HTTP/2 multiplexing is unaffected.
    #[must_use]
    pub fn with_http1_keep_alive(mut self, enabled: bool) -> Self {
        self.http1_keep_alive = enabled;
        self
    }

    /// Set the HTTP/1.1 header read timeout.
    ///
    /// Defaults to [`DEFAULT_HEADER_READ_TIMEOUT`] (30 seconds). Bounds how
    /// long the server waits to read a complete set of request headers,
    /// measured from when hyper begins reading a new request; on a keep-alive
    /// connection this also bounds the idle wait between requests. A peer that
    /// connects (or finishes a request) and then stalls without sending the
    /// next request's headers is disconnected, which mitigates slowloris-style
    /// connection-exhaustion attacks. Pass `None` to disable.
    ///
    /// Applies to HTTP/1.1 only; it does not bound idle or stalled HTTP/2
    /// connections — use
    /// [`with_max_connection_age`](Self::with_max_connection_age) to retire
    /// those by age.
    #[must_use]
    pub fn with_header_read_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.header_read_timeout = timeout.into();
        self
    }

    /// Enable or disable HTTP/2 adaptive flow-control window sizing.
    ///
    /// Enabled by default ([`DEFAULT_HTTP2_ADAPTIVE_WINDOW`]). When enabled,
    /// hyper grows the stream and connection flow-control windows based on the
    /// measured bandwidth-delay product, which improves throughput on
    /// high-latency, high-bandwidth links at the cost of slightly higher
    /// per-connection memory under load.
    ///
    /// Adaptive sizing and an explicit window size are mutually exclusive:
    /// enabling adaptive sizing overrides any window set via
    /// [`with_http2_initial_stream_window_size`](Self::with_http2_initial_stream_window_size)
    /// or
    /// [`with_http2_initial_connection_window_size`](Self::with_http2_initial_connection_window_size).
    /// Whichever is set last wins.
    #[must_use]
    pub fn with_http2_adaptive_window(mut self, enabled: bool) -> Self {
        self.http2_adaptive_window = enabled;
        self
    }

    /// Set the HTTP/2 initial stream-level flow-control window size, in bytes.
    ///
    /// Controls the per-stream `SETTINGS_INITIAL_WINDOW_SIZE` advertised to
    /// clients. Supplying a size turns
    /// [adaptive sizing](Self::with_http2_adaptive_window) off, mirroring
    /// grpc-go semantics; passing `None` leaves hyper's default in place and
    /// does not change the adaptive flag. The window can be raised above
    /// hyper's 64 KiB default to improve throughput when adaptive sizing is
    /// not wanted.
    ///
    /// The adaptive toggle and the explicit window are last-write-wins: a later
    /// [`with_http2_adaptive_window(true)`](Self::with_http2_adaptive_window)
    /// re-enables autotuning and the explicit window is ignored. Per HTTP/2,
    /// the window must not exceed `2^31 - 1`; larger values are a protocol error.
    #[must_use]
    pub fn with_http2_initial_stream_window_size(mut self, size: impl Into<Option<u32>>) -> Self {
        self.http2_initial_stream_window_size = size.into();
        if self.http2_initial_stream_window_size.is_some() {
            self.http2_adaptive_window = false;
        }
        self
    }

    /// Set the HTTP/2 initial connection-level flow-control window size, in bytes.
    ///
    /// Controls the whole-connection flow-control window, which bounds the
    /// total unacknowledged data across all streams on the connection.
    /// Supplying a size turns
    /// [adaptive sizing](Self::with_http2_adaptive_window) off, mirroring
    /// grpc-go semantics; passing `None` leaves hyper's default in place and
    /// does not change the adaptive flag.
    ///
    /// The adaptive toggle and the explicit window are last-write-wins: a later
    /// [`with_http2_adaptive_window(true)`](Self::with_http2_adaptive_window)
    /// re-enables autotuning and the explicit window is ignored. Per HTTP/2,
    /// the window must not exceed `2^31 - 1`; larger values are a protocol error.
    #[must_use]
    pub fn with_http2_initial_connection_window_size(
        mut self,
        size: impl Into<Option<u32>>,
    ) -> Self {
        self.http2_initial_connection_window_size = size.into();
        if self.http2_initial_connection_window_size.is_some() {
            self.http2_adaptive_window = false;
        }
        self
    }

    /// Set the maximum number of concurrent HTTP/2 streams per connection.
    ///
    /// This maps to hyper's HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS`, which
    /// the server advertises to each peer. A client may have at most this
    /// many in-flight requests (streams) open at once on a single connection;
    /// attempts to exceed it are refused with a `REFUSED_STREAM` error and
    /// can be safely retried. The setting has no effect on HTTP/1.1
    /// connections, which are not multiplexed.
    ///
    /// Left at hyper's default (200) when unset. Raise it for high-fan-in
    /// internal services that multiplex many concurrent RPCs over one
    /// connection, or lower it as an additional hardening measure when
    /// serving less-trusted clients.
    ///
    /// # Panics
    ///
    /// Panics if `max_streams` is zero — advertising a limit of zero refuses
    /// every stream, leaving a server that accepts connections but rejects
    /// all requests. It is rejected at configuration time rather than
    /// silently producing a dead server.
    #[must_use]
    pub fn with_max_concurrent_streams(mut self, max_streams: u32) -> Self {
        assert!(
            max_streams != 0,
            "with_max_concurrent_streams requires a non-zero value",
        );
        self.max_concurrent_streams = Some(max_streams);
        self
    }

    /// Set the interval between HTTP/2 keepalive PING frames sent on an
    /// otherwise idle connection.
    ///
    /// Disabled by default. When set, the server sends a PING after the
    /// connection has been idle for `interval` and, if the peer fails to
    /// acknowledge it within
    /// [`with_http2_keepalive_timeout`](Self::with_http2_keepalive_timeout),
    /// closes the connection. This detects dead or half-open peers (NAT
    /// timeout, client crash, network partition) on long-lived
    /// server-streaming or bidirectional connections that would otherwise sit
    /// half-open until the OS TCP timeout, holding a task and file descriptor.
    ///
    /// Affects HTTP/2 connections only; HTTP/1.1 is unaffected. Note the
    /// spelling difference from the HTTP/1.1 toggle
    /// [`with_http1_keep_alive`](Self::with_http1_keep_alive) (`keep_alive`):
    /// these HTTP/2 knobs use `keepalive` as a single word.
    ///
    /// # Panics
    ///
    /// Panics if `interval` is zero — a zero interval would request an
    /// unbounded PING flood rather than periodic keepalives.
    #[must_use]
    pub fn with_http2_keepalive_interval(mut self, interval: Duration) -> Self {
        assert!(
            !interval.is_zero(),
            "with_http2_keepalive_interval requires a non-zero duration",
        );
        self.http2_keepalive_interval = Some(interval);
        self
    }

    /// Set how long to wait for an HTTP/2 keepalive PING acknowledgement
    /// before closing the connection.
    ///
    /// Defaults to [`DEFAULT_HTTP2_KEEPALIVE_TIMEOUT`] (20 seconds). This only
    /// takes effect once
    /// [`with_http2_keepalive_interval`](Self::with_http2_keepalive_interval)
    /// is set — setting it without an interval has no effect.
    #[must_use]
    pub fn with_http2_keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.http2_keepalive_timeout = timeout;
        self
    }

    /// Set a maximum age for each accepted HTTP connection.
    ///
    /// Disabled by default. When enabled, the age is measured from the start
    /// of HTTP serving (after any TLS handshake) and each connection gets a
    /// symmetric ±10% jitter to avoid reconnect bursts. Once the age expires,
    /// the server begins graceful shutdown for that connection — HTTP/2
    /// connections receive a GOAWAY, HTTP/1.1 connections have keep-alive
    /// disabled — then waits up to
    /// [`with_max_connection_age_grace`](Self::with_max_connection_age_grace)
    /// for in-flight requests before force-closing it.
    ///
    /// # Panics
    ///
    /// Panics if `max_age` is zero — a zero age is rejected rather than
    /// silently retiring every connection the instant it starts serving.
    #[must_use]
    pub fn with_max_connection_age(mut self, max_age: Duration) -> Self {
        assert!(
            !max_age.is_zero(),
            "with_max_connection_age requires a non-zero duration",
        );
        self.max_connection_age = Some(max_age);
        self
    }

    /// Set the grace period used after a retired connection begins shutdown.
    ///
    /// Defaults to five seconds. This single grace period is shared by all
    /// three retirement triggers —
    /// [`with_max_connection_age`](Self::with_max_connection_age),
    /// [`with_max_connection_idle`](Self::with_max_connection_idle), and
    /// [`with_max_requests_per_connection`](Self::with_max_requests_per_connection)
    /// — and applies to whichever one fires. Setting it without enabling any
    /// trigger has no effect, and the three cannot be tuned independently.
    /// Whole-server graceful shutdown still waits indefinitely for in-flight
    /// requests.
    #[must_use]
    pub fn with_max_connection_age_grace(mut self, grace: Duration) -> Self {
        self.max_connection_age_grace = grace;
        self
    }

    /// Retire a connection that has had no in-flight requests for `duration`.
    ///
    /// Disabled by default. This complements
    /// [`with_max_connection_age`](Self::with_max_connection_age): age caps a
    /// connection's total lifetime regardless of use, while idle reclaims
    /// connections that have gone quiet (clients behind NAT, bursty workloads,
    /// pooled clients holding connections they no longer need).
    ///
    /// A connection is idle when it has zero in-flight requests. The idle
    /// timer resets on activity: any request that starts, or completes, during
    /// an idle window keeps the connection alive. Once a connection stays idle
    /// for the full `duration`, the server begins graceful shutdown for it —
    /// HTTP/2 connections receive a GOAWAY, HTTP/1.1 connections have
    /// keep-alive disabled — then waits up to
    /// [`with_max_connection_age_grace`](Self::with_max_connection_age_grace)
    /// (a grace period shared with maximum age) for any straggling request
    /// before force-closing it.
    ///
    /// The idle window is evaluated lazily — it is re-checked when the timer
    /// expires rather than re-armed at the instant of each request — so a
    /// connection is retired between one and two times `duration` after its
    /// last activity. Size `duration` against that upper bound. Unlike maximum
    /// age, idle reaping applies no jitter.
    ///
    /// When both an idle timeout and a
    /// [`max age`](Self::with_max_connection_age) are configured, whichever
    /// fires first retires the connection. Whole-server graceful shutdown still
    /// waits indefinitely for in-flight requests, and is never capped by the
    /// idle grace period.
    ///
    /// # Panics
    ///
    /// Panics if `duration` is zero — a zero idle timeout is rejected rather
    /// than silently retiring every connection the instant it falls idle.
    #[must_use]
    pub fn with_max_connection_idle(mut self, duration: Duration) -> Self {
        assert!(
            !duration.is_zero(),
            "with_max_connection_idle requires a non-zero duration",
        );
        self.max_connection_idle = Some(duration);
        self
    }

    /// Retire each accepted connection after it has dispatched `max` requests.
    ///
    /// Disabled by default. The request count is per-connection: every
    /// inbound request (each HTTP/2 stream, or each HTTP/1.1 request) is
    /// counted, and once the `max`th request has been dispatched the server
    /// begins graceful shutdown for that connection — HTTP/2 connections
    /// receive a GOAWAY, HTTP/1.1 connections have keep-alive disabled — then
    /// waits up to
    /// [`with_max_connection_age_grace`](Self::with_max_connection_age_grace)
    /// for in-flight requests before force-closing it. The `max`th request
    /// itself still completes; subsequent requests are turned away.
    ///
    /// `max` is a soft floor rather than an exact cap: under HTTP/2 a client
    /// may open several streams concurrently before the GOAWAY takes effect, so
    /// the connection is retired at or after the `max`th request, not strictly
    /// at it.
    ///
    /// This is the count-based complement of
    /// [`with_max_connection_age`](Self::with_max_connection_age); both may be
    /// set at once, in which case whichever trigger fires first retires the
    /// connection. Whole-server graceful shutdown still drains in-flight
    /// requests indefinitely.
    ///
    /// `max` is a [`NonZeroU64`] so that "retire after zero requests" — which
    /// would refuse every connection before it served anything — is
    /// unrepresentable. (This differs from
    /// [`with_max_connection_age`](Self::with_max_connection_age), which takes a
    /// plain [`Duration`] and panics on a zero value.)
    #[must_use]
    pub fn with_max_requests_per_connection(mut self, max: NonZeroU64) -> Self {
        self.max_requests_per_connection = Some(max);
        self
    }

    /// Whether HTTP/1.1 keep-alive is enabled.
    #[must_use]
    pub fn http1_keep_alive(&self) -> bool {
        self.http1_keep_alive
    }

    /// The HTTP/1.1 header read timeout, or `None` if disabled.
    #[must_use]
    pub fn header_read_timeout(&self) -> Option<Duration> {
        self.header_read_timeout
    }

    /// Whether HTTP/2 adaptive flow-control window sizing is enabled.
    #[must_use]
    pub fn http2_adaptive_window(&self) -> bool {
        self.http2_adaptive_window
    }

    /// The explicit HTTP/2 initial stream window, if one was set. Ignored while
    /// [adaptive sizing](Self::http2_adaptive_window) is on.
    #[must_use]
    pub fn http2_initial_stream_window_size(&self) -> Option<u32> {
        self.http2_initial_stream_window_size
    }

    /// The explicit HTTP/2 initial connection window, if one was set. Ignored
    /// while [adaptive sizing](Self::http2_adaptive_window) is on.
    #[must_use]
    pub fn http2_initial_connection_window_size(&self) -> Option<u32> {
        self.http2_initial_connection_window_size
    }

    /// The advertised `SETTINGS_MAX_CONCURRENT_STREAMS`, or `None` for hyper's
    /// default.
    #[must_use]
    pub fn max_concurrent_streams(&self) -> Option<u32> {
        self.max_concurrent_streams
    }

    /// The HTTP/2 keepalive PING interval, or `None` if keepalive is off.
    #[must_use]
    pub fn http2_keepalive_interval(&self) -> Option<Duration> {
        self.http2_keepalive_interval
    }

    /// How long an HTTP/2 keepalive PING may go unacknowledged.
    #[must_use]
    pub fn http2_keepalive_timeout(&self) -> Duration {
        self.http2_keepalive_timeout
    }

    /// The maximum connection age (before jitter), or `None` if disabled.
    #[must_use]
    pub fn max_connection_age(&self) -> Option<Duration> {
        self.max_connection_age
    }

    /// The drain window shared by the three retirement triggers.
    #[must_use]
    pub fn max_connection_age_grace(&self) -> Duration {
        self.max_connection_age_grace
    }

    /// The idle duration after which a connection is retired, or `None` if
    /// disabled.
    #[must_use]
    pub fn max_connection_idle(&self) -> Option<Duration> {
        self.max_connection_idle
    }

    /// The request count after which a connection is retired, or `None` if
    /// disabled.
    #[must_use]
    pub fn max_requests_per_connection(&self) -> Option<NonZeroU64> {
        self.max_requests_per_connection
    }

    /// The `(stream, connection)` explicit window sizes that should actually
    /// reach hyper's builder.
    ///
    /// Adaptive sizing takes precedence: when it is on, no explicit window is
    /// applied, so the two never reach hyper at once regardless of the order
    /// the setters were called in. The setters already clear the adaptive flag
    /// when a size is supplied, but a later `with_http2_adaptive_window(true)`
    /// can leave both set; this resolves that case deterministically in favour
    /// of adaptive sizing.
    pub(crate) fn effective_http2_windows(&self) -> (Option<u32>, Option<u32>) {
        if self.http2_adaptive_window {
            (None, None)
        } else {
            (
                self.http2_initial_stream_window_size,
                self.http2_initial_connection_window_size,
            )
        }
    }

    /// Log (at debug) settings that are present but inert because the setting
    /// they qualify is absent. Called once by the built-in accept loop, not per
    /// connection, so loops that call `serve_connection` directly skip it.
    pub(crate) fn lint(&self) {
        if self.max_connection_age.is_none()
            && self.max_connection_idle.is_none()
            && self.max_requests_per_connection.is_none()
            && self.max_connection_age_grace != DEFAULT_MAX_CONNECTION_AGE_GRACE
        {
            tracing::debug!(
                "max_connection_age_grace is set but none of max_connection_age, \
                 max_connection_idle, or max_requests_per_connection are; the \
                 grace period has no effect",
            );
        }
        if self.http2_keepalive_interval.is_none()
            && self.http2_keepalive_timeout != DEFAULT_HTTP2_KEEPALIVE_TIMEOUT
        {
            tracing::debug!(
                "http2_keepalive_timeout is set but http2_keepalive_interval is not; \
                 HTTP/2 keepalive stays disabled and the timeout has no effect",
            );
        }
    }
}

/// Everything the [`Acceptor`](super::Acceptor) applies between `accept(2)`
/// and handing over an authenticated stream: whether to terminate TLS, and how
/// long a handshake may take.
///
/// `TCP_NODELAY` is always set and has no knob. Without the `server-tls`
/// feature this struct has no settings.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct AcceptConfig {
    #[cfg(feature = "server-tls")]
    tls: Option<Arc<rustls::ServerConfig>>,
    /// `None` means [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`]; stored as an override so
    /// `Default` derives in every feature set.
    #[cfg(feature = "server-tls")]
    tls_handshake_timeout: Option<Duration>,
}

impl AcceptConfig {
    /// The defaults: plaintext, and a 10-second handshake timeout once TLS is
    /// enabled.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Terminate TLS with the given rustls server configuration.
    ///
    /// The configuration controls all TLS behavior including certificate
    /// selection, client authentication, ALPN, and protocol versions. For
    /// dynamic certificate rotation, use a
    /// [`rustls::server::ResolvesServerCert`] implementation in the config.
    /// When it requests client authentication and the peer presents a chain
    /// rustls verifies, that chain reaches handlers as
    /// [`PeerCerts`](super::PeerCerts).
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls(mut self, config: Arc<rustls::ServerConfig>) -> Self {
        self.tls = Some(config);
        self
    }

    /// Set the TLS handshake timeout.
    ///
    /// Defaults to [`DEFAULT_TLS_HANDSHAKE_TIMEOUT`] (10 seconds). A client
    /// that connects via TCP but does not complete the TLS handshake within
    /// this duration is disconnected. Set generously; clients on high-latency
    /// links need a few round trips to complete the handshake.
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_tls_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.tls_handshake_timeout = Some(timeout);
        self
    }

    /// The rustls configuration TLS is terminated with, or `None` for
    /// plaintext.
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn tls(&self) -> Option<&Arc<rustls::ServerConfig>> {
        self.tls.as_ref()
    }

    /// How long a TLS handshake may take before the connection is dropped.
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn tls_handshake_timeout(&self) -> Duration {
        self.tls_handshake_timeout
            .unwrap_or(DEFAULT_TLS_HANDSHAKE_TIMEOUT)
    }

    /// Whether connections are TLS-terminated; only for the "listening on"
    /// log line's scheme.
    pub(crate) fn is_tls(&self) -> bool {
        #[cfg(feature = "server-tls")]
        {
            self.tls.is_some()
        }
        #[cfg(not(feature = "server-tls"))]
        {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let config = ConnectionConfig::default();
        assert!(config.http1_keep_alive());
        assert_eq!(
            config.header_read_timeout(),
            Some(DEFAULT_HEADER_READ_TIMEOUT)
        );
        assert_eq!(
            config.http2_adaptive_window(),
            DEFAULT_HTTP2_ADAPTIVE_WINDOW
        );
        assert_eq!(config.http2_initial_stream_window_size(), None);
        assert_eq!(config.http2_initial_connection_window_size(), None);
        assert_eq!(config.max_concurrent_streams(), None);
        assert_eq!(config.http2_keepalive_interval(), None);
        assert_eq!(
            config.http2_keepalive_timeout(),
            DEFAULT_HTTP2_KEEPALIVE_TIMEOUT
        );
        assert_eq!(config.max_connection_age(), None);
        assert_eq!(
            config.max_connection_age_grace(),
            DEFAULT_MAX_CONNECTION_AGE_GRACE
        );
        assert_eq!(config.max_connection_idle(), None);
        assert_eq!(config.max_requests_per_connection(), None);
        assert_eq!(config, ConnectionConfig::new());
    }

    #[test]
    fn header_read_timeout_overrides() {
        let config = ConnectionConfig::new().with_header_read_timeout(Some(Duration::from_secs(5)));
        assert_eq!(config.header_read_timeout(), Some(Duration::from_secs(5)));
        let config = ConnectionConfig::new().with_header_read_timeout(None::<Duration>);
        assert_eq!(config.header_read_timeout(), None);
    }

    #[test]
    fn with_http2_adaptive_window_toggles_flag() {
        let config = ConnectionConfig::new().with_http2_adaptive_window(false);
        assert!(!config.http2_adaptive_window());
        assert!(
            config
                .with_http2_adaptive_window(true)
                .http2_adaptive_window()
        );
    }

    #[test]
    fn explicit_stream_window_disables_adaptive() {
        let config = ConnectionConfig::new().with_http2_initial_stream_window_size(1 << 20);
        assert_eq!(config.http2_initial_stream_window_size(), Some(1 << 20));
        assert!(
            !config.http2_adaptive_window(),
            "an explicit stream window must turn adaptive sizing off"
        );
    }

    #[test]
    fn explicit_connection_window_disables_adaptive() {
        let config = ConnectionConfig::new().with_http2_initial_connection_window_size(2 << 20);
        assert_eq!(config.http2_initial_connection_window_size(), Some(2 << 20));
        assert!(
            !config.http2_adaptive_window(),
            "an explicit connection window must turn adaptive sizing off"
        );
    }

    #[test]
    fn clearing_window_with_none_keeps_adaptive_flag() {
        // Passing `None` must not flip the adaptive flag in either direction.
        let config = ConnectionConfig::new()
            .with_http2_initial_stream_window_size(None)
            .with_http2_initial_connection_window_size(None);
        assert!(config.http2_adaptive_window());
        assert_eq!(config.http2_initial_stream_window_size(), None);
        assert_eq!(config.http2_initial_connection_window_size(), None);
    }

    #[test]
    fn re_enabling_adaptive_after_explicit_window_wins() {
        // The setters are last-write-wins: re-enabling adaptive after setting a
        // window leaves the window stored but turns adaptive back on, matching
        // the documented precedence (and hyper, where adaptive overrides the
        // explicit window).
        let config = ConnectionConfig::new()
            .with_http2_initial_stream_window_size(1 << 20)
            .with_http2_adaptive_window(true);
        assert!(config.http2_adaptive_window());
        assert_eq!(config.http2_initial_stream_window_size(), Some(1 << 20));
        // ...and the stored window must not reach hyper while adaptive is on.
        assert_eq!(config.effective_http2_windows(), (None, None));
    }

    #[test]
    fn effective_windows_resolves_adaptive_precedence() {
        // Default (adaptive on): no explicit window reaches hyper.
        assert_eq!(
            ConnectionConfig::default().effective_http2_windows(),
            (None, None)
        );

        // Adaptive explicitly off but no sizes set: still nothing to apply.
        let off = ConnectionConfig::new().with_http2_adaptive_window(false);
        assert_eq!(off.effective_http2_windows(), (None, None));

        // Adaptive off with explicit sizes: both windows are applied.
        let fixed = ConnectionConfig::new()
            .with_http2_initial_stream_window_size(1 << 20)
            .with_http2_initial_connection_window_size(2 << 20);
        assert!(!fixed.http2_adaptive_window());
        assert_eq!(
            fixed.effective_http2_windows(),
            (Some(1 << 20), Some(2 << 20))
        );
    }

    #[test]
    fn http2_keepalive_overrides() {
        let config = ConnectionConfig::new()
            .with_http2_keepalive_interval(Duration::from_secs(30))
            .with_http2_keepalive_timeout(Duration::from_secs(5));
        assert_eq!(
            config.http2_keepalive_interval(),
            Some(Duration::from_secs(30))
        );
        assert_eq!(config.http2_keepalive_timeout(), Duration::from_secs(5));

        // Setting only the timeout leaves keepalive disabled (no interval).
        let config = ConnectionConfig::new().with_http2_keepalive_timeout(Duration::from_secs(1));
        assert_eq!(config.http2_keepalive_interval(), None);
    }

    #[test]
    #[should_panic(expected = "non-zero duration")]
    fn with_http2_keepalive_interval_rejects_zero() {
        let _ = ConnectionConfig::new().with_http2_keepalive_interval(Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "non-zero value")]
    fn with_max_concurrent_streams_rejects_zero() {
        let _ = ConnectionConfig::new().with_max_concurrent_streams(0);
    }

    #[test]
    fn retirement_overrides() {
        let config = ConnectionConfig::new()
            .with_max_connection_age(Duration::from_secs(30))
            .with_max_connection_idle(Duration::from_secs(10))
            .with_max_requests_per_connection(NonZeroU64::new(100).unwrap())
            .with_max_connection_age_grace(Duration::from_secs(2));
        assert_eq!(config.max_connection_age(), Some(Duration::from_secs(30)));
        assert_eq!(config.max_connection_idle(), Some(Duration::from_secs(10)));
        assert_eq!(config.max_requests_per_connection(), NonZeroU64::new(100));
        assert_eq!(config.max_connection_age_grace(), Duration::from_secs(2));
    }

    #[test]
    #[should_panic(expected = "non-zero duration")]
    fn with_max_connection_age_rejects_zero() {
        let _ = ConnectionConfig::new().with_max_connection_age(Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "non-zero duration")]
    fn with_max_connection_idle_rejects_zero() {
        let _ = ConnectionConfig::new().with_max_connection_idle(Duration::ZERO);
    }

    #[cfg(feature = "server-tls")]
    #[test]
    fn accept_config_defaults_and_overrides() {
        let config = AcceptConfig::default();
        assert!(config.tls().is_none());
        assert!(!config.is_tls());
        assert_eq!(
            config.tls_handshake_timeout(),
            DEFAULT_TLS_HANDSHAKE_TIMEOUT
        );

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tls = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(rustls::server::ResolvesServerCertUsingSni::new())),
        );
        let config = AcceptConfig::new()
            .with_tls(Arc::clone(&tls))
            .with_tls_handshake_timeout(Duration::from_secs(1));
        assert!(Arc::ptr_eq(config.tls().unwrap(), &tls));
        assert!(config.is_tls());
        assert_eq!(config.tls_handshake_timeout(), Duration::from_secs(1));
    }
}
