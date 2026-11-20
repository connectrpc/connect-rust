//! The server half of Connect over WebSocket.
//!
//! A handshake is validated and answered with `101` from inside
//! [`ConnectRpcService::call`]; the session then runs in its own task. It
//! reads the client's opening metadata message, then dispatches the RPC by
//! handing the same service a synthesized Connect streaming request, whose
//! body is fed from the client's `B` and `C` messages. The streaming
//! response that comes back is translated into `M`, `B` and `S` messages.
//! Every RPC kind, unary included, goes through the service's ordinary
//! streaming path, so interceptors, limits and the deadline policy apply
//! unchanged.

use std::convert::Infallible;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{FutureExt, StreamExt};
use http::header::{self, HeaderName, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use http_body::{Body, Frame};
use http_body_util::{BodyExt, Full};
use hyper::upgrade::{OnUpgrade, Upgraded};
use hyper_util::rt::TokioIo;
use tokio::sync::{Notify, mpsc, watch};
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig as FrameConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tower::Service;

use super::WebSocketConfig;
use super::WebSocketRpc;
use super::wire::{
    self, Fault, Marker, Outbound, Side, WireError, close_code, decode_metadata, encode_metadata,
    is_protocol_header, is_reserved_request_key,
};
use crate::codec::CodecFormat;
use crate::dispatcher::Dispatcher;
use crate::envelope::{EnvelopeAssembler, flags};
use crate::error::ConnectError;
use crate::service::{ConnectRpcBody, ConnectRpcService, DRAIN_TIMEOUT, end_stream_error_json};

/// How many request messages may wait for the handler, beyond the one the
/// session holds, before it stops reading the connection. Each can be up to
/// the message size limit, so this stays small.
const REQUEST_QUEUE: usize = 1;

/// Request extension marking a synthesized request whose unary method may be
/// served with streaming framing. The service reports through it whether the
/// RPC ended because a request message did not decode, which on a WebSocket
/// is a protocol error (§6.1) rather than the handler's `invalid_argument`.
#[derive(Debug, Clone, Default)]
pub(crate) struct WebSocketDispatch {
    undecodable: Arc<OnceLock<String>>,
}

impl WebSocketDispatch {
    /// Record `err` if it reports a request message that did not decode.
    pub(crate) fn note(&self, err: &ConnectError) {
        if err.is_decode_failure() {
            let _ = self
                .undecodable
                .set(err.message.clone().unwrap_or_default());
        }
    }

    fn undecodable(&self) -> Option<&str> {
        self.undecodable.get().map(String::as_str)
    }
}

/// Connection extension the built-in server attaches so that upgraded
/// sessions, which outlive their connection's task, still end on graceful
/// shutdown and hold it open until they have. Only a [`SessionGuard`]
/// counts, so a handler that keeps a copy of its request's extensions does
/// not hold shutdown open.
#[derive(Clone)]
#[cfg_attr(not(feature = "server"), allow(dead_code))]
pub(crate) struct SessionTracker(Arc<TrackerInner>);

struct TrackerInner {
    shutdown: watch::Receiver<bool>,
    live: AtomicUsize,
    idle: Notify,
}

#[cfg_attr(not(feature = "server"), allow(dead_code))]
impl SessionTracker {
    pub(crate) fn new(shutdown: watch::Receiver<bool>) -> Self {
        Self(Arc::new(TrackerInner {
            shutdown,
            live: AtomicUsize::new(0),
            idle: Notify::new(),
        }))
    }

    fn guard(&self) -> SessionGuard {
        self.0.live.fetch_add(1, Ordering::SeqCst);
        SessionGuard(self.clone())
    }

    /// Resolves once no session is live.
    pub(crate) async fn wait_idle(&self) {
        loop {
            let notified = self.0.idle.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if self.0.live.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// One live session, counted until dropped.
struct SessionGuard(SessionTracker);

impl Drop for SessionGuard {
    fn drop(&mut self) {
        if self.0.0.live.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.0.idle.notify_waiters();
        }
    }
}

/// What the service does with a request, given its WebSocket settings.
pub(crate) enum Route {
    /// An ordinary Connect, gRPC or gRPC-Web request.
    Http,
    /// A request answered without dispatch.
    Respond(Box<Response<ConnectRpcBody>>),
    /// A WebSocket handshake for this procedure path.
    Upgrade(String),
}

/// Whether `headers` ask for a WebSocket upgrade (§4.1): `Connection`
/// carries the `upgrade` token and `Upgrade` the `websocket` token.
pub(crate) fn is_upgrade(headers: &http::HeaderMap) -> bool {
    wire::header_has_token(headers, &header::CONNECTION, "upgrade")
        && wire::header_has_token(headers, &header::UPGRADE, "websocket")
}

/// Route a request by its upgrade headers and the configured path prefix
/// (§4.2). Without a prefix, every path serves both bindings; with one,
/// paths under it serve only WebSocket and the rest only HTTP.
pub(crate) fn route<B>(config: &WebSocketConfig, req: &Request<B>) -> Route {
    let upgrade = is_upgrade(req.headers());
    let path = req.uri().path();
    let Some(prefix) = config.path_prefix.as_deref() else {
        return if upgrade {
            Route::Upgrade(path.to_owned())
        } else {
            Route::Http
        };
    };
    match path
        .strip_prefix(prefix)
        .filter(|rest| rest.starts_with('/'))
    {
        Some(rest) if upgrade => Route::Upgrade(rest.to_owned()),
        Some(_) => {
            let mut response = reject(
                StatusCode::UPGRADE_REQUIRED,
                "this path serves Connect over WebSocket; send an upgrade request",
            );
            response
                .headers_mut()
                .insert(header::UPGRADE, HeaderValue::from_static("websocket"));
            response
                .headers_mut()
                .insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
            Route::Respond(Box::new(response))
        }
        None if upgrade => Route::Respond(Box::new(reject(
            StatusCode::BAD_REQUEST,
            "this path serves Connect over HTTP; WebSocket is served under the configured path prefix",
        ))),
        None => Route::Http,
    }
}

fn reject(status: StatusCode, message: &'static str) -> Response<ConnectRpcBody> {
    let mut response = Response::new(ConnectRpcBody::Full(Full::new(Bytes::from_static(
        message.as_bytes(),
    ))));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// Validate a handshake and, if it is acceptable, answer it with `101` and
/// start the session. Connection-level failures are HTTP statuses (§4.5);
/// RPC-level ones are reported in-band once the session is open.
pub(crate) fn accept<D, B>(
    service: ConnectRpcService<D>,
    config: Arc<WebSocketConfig>,
    req: Request<B>,
    procedure: String,
) -> Response<ConnectRpcBody>
where
    D: Dispatcher,
{
    let (mut parts, _body) = req.into_parts();
    if parts.method != Method::GET {
        return reject(
            StatusCode::BAD_REQUEST,
            "a WebSocket handshake must be a GET request",
        );
    }
    let version_ok = parts
        .headers
        .get(header::SEC_WEBSOCKET_VERSION)
        .is_some_and(|v| v.as_bytes() == b"13");
    if !version_ok {
        let mut response = reject(
            StatusCode::UPGRADE_REQUIRED,
            "unsupported WebSocket version; this server speaks version 13",
        );
        response.headers_mut().insert(
            header::SEC_WEBSOCKET_VERSION,
            HeaderValue::from_static("13"),
        );
        return response;
    }
    let Some(key) = parts.headers.get(header::SEC_WEBSOCKET_KEY) else {
        return reject(StatusCode::BAD_REQUEST, "missing Sec-WebSocket-Key");
    };
    let accept_key = derive_accept_key(key.as_bytes());
    let Some((codec, token)) = wire::negotiate_subprotocol(&parts.headers) else {
        return reject(
            StatusCode::BAD_REQUEST,
            if cfg!(feature = "json") {
                "Sec-WebSocket-Protocol offers no supported codec; \
                 this server supports connectrpc.1+proto and connectrpc.1+json"
            } else {
                "Sec-WebSocket-Protocol offers no supported codec; \
                 this server supports connectrpc.1+proto"
            },
        );
    };
    let Ok(token) = HeaderValue::from_str(&token) else {
        return reject(StatusCode::BAD_REQUEST, "invalid Sec-WebSocket-Protocol");
    };
    if !config.origin_allowed(&parts.headers) {
        return reject(
            StatusCode::FORBIDDEN,
            "cross-origin WebSocket handshake refused",
        );
    }
    let Some(on_upgrade) = parts.extensions.remove::<OnUpgrade>() else {
        if parts.version == http::Version::HTTP_11 {
            tracing::error!(
                "Connect over WebSocket needs a server that supports HTTP/1.1 upgrades; \
                 this request carries no upgrade handle"
            );
            return reject(
                StatusCode::INTERNAL_SERVER_ERROR,
                "this server cannot upgrade connections to WebSocket",
            );
        }
        return reject(
            StatusCode::BAD_REQUEST,
            "a WebSocket handshake must be an HTTP/1.1 request",
        );
    };

    let guard = parts
        .extensions
        .remove::<SessionTracker>()
        .map(|tracker| tracker.guard());
    let handshake = Handshake {
        accepted_at: Instant::now(),
        timeout: wire::parse_timeout_param(parts.uri.query()),
        codec,
        procedure,
        headers: parts.headers,
        extensions: parts.extensions,
        config,
    };
    crate::spawn_detached(run_session(service, on_upgrade, handshake, guard));

    let mut response = Response::new(ConnectRpcBody::Full(Full::new(Bytes::new())));
    *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    let headers = response.headers_mut();
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
    if let Ok(value) = HeaderValue::from_str(&accept_key) {
        headers.insert(header::SEC_WEBSOCKET_ACCEPT, value);
    }
    headers.insert(header::SEC_WEBSOCKET_PROTOCOL, token);
    response
}

/// What the session keeps from the handshake.
struct Handshake {
    accepted_at: Instant,
    timeout: Result<Option<Duration>, ConnectError>,
    codec: CodecFormat,
    procedure: String,
    headers: http::HeaderMap,
    extensions: http::Extensions,
    config: Arc<WebSocketConfig>,
}

/// Why the session loop stopped.
enum End {
    /// The server's `S` went out; close normally.
    Finished,
    /// End the RPC with this error and close code.
    Fail(ConnectError, u16),
    /// Close with this code and reason, sending no `S`.
    Abort(u16, String),
    /// The connection ended before `S`: the client canceled.
    Canceled,
    /// The client stopped answering; hang up with 1011.
    Silent,
}

impl From<WireError> for End {
    fn from(err: WireError) -> Self {
        let code = err.fault.close_code(Side::Server);
        match err.fault {
            // The peer cannot be trusted to read a text message (§6.1).
            Fault::Encoding => End::Abort(code, err.error.message.unwrap_or_default()),
            Fault::Protocol | Fault::TooLarge => End::Fail(err.error, code),
        }
    }
}

type ResponseFuture =
    Pin<Box<dyn Future<Output = Result<Response<ConnectRpcBody>, Infallible>> + Send>>;
type RequestItem = Result<Bytes, Canceled>;
type Socket = WebSocketStream<TokioIo<Upgraded>>;

async fn run_session<D: Dispatcher>(
    service: ConnectRpcService<D>,
    on_upgrade: OnUpgrade,
    handshake: Handshake,
    guard: Option<SessionGuard>,
) {
    let upgraded = match on_upgrade.await {
        Ok(upgraded) => upgraded,
        Err(err) => {
            tracing::debug!(error = %err, "WebSocket upgrade did not complete");
            return;
        }
    };
    let path = handshake.procedure.trim_start_matches('/');
    let limits = service
        .dispatcher()
        .lookup(path)
        .and_then(|desc| desc.limits)
        .unwrap_or(*service.limits());
    // One byte more than a message, for the marker.
    let max = limits.max_message_size().saturating_add(1);
    let frame_config = FrameConfig::default()
        .max_message_size(Some(max))
        .max_frame_size(Some(max));
    let ws: Socket =
        WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, Some(frame_config))
            .await;
    let (sink, stream) = ws.split();
    let mut session = Session {
        sink,
        stream,
        outbound: Outbound::default(),
        codec: handshake.codec,
        dispatch: WebSocketDispatch::default(),
        sent_metadata: false,
        sent_end: false,
        max_message_size: limits.max_message_size(),
    };
    let shutdown = guard.as_ref().map(|guard| guard.0.0.shutdown.clone());
    let driven = AssertUnwindSafe(session.drive(service, handshake, shutdown))
        .catch_unwind()
        .await;
    let end = driven.unwrap_or_else(|_| {
        tracing::error!("a WebSocket RPC handler panicked");
        End::Fail(
            ConnectError::internal("internal server error"),
            close_code::INTERNAL_ERROR,
        )
    });
    session.finish(end).await;
    drop(guard);
}

struct Session {
    sink: SplitSink<Socket, Message>,
    stream: SplitStream<Socket>,
    outbound: Outbound,
    codec: CodecFormat,
    dispatch: WebSocketDispatch,
    sent_metadata: bool,
    /// `S` is queued, so nothing may follow it but the close.
    sent_end: bool,
    max_message_size: usize,
}

/// Request-side state: what the client has sent and where it goes.
#[derive(Default)]
struct Inbound {
    tx: Option<mpsc::Sender<RequestItem>>,
    /// A message waiting for room in the handler's queue. While one waits,
    /// the connection is not read.
    pending: Option<Bytes>,
    client_done: bool,
}

/// Response-side state: the handler's future, then its body.
struct Outflow {
    response: Option<ResponseFuture>,
    body: Option<ConnectRpcBody>,
    assembler: EnvelopeAssembler,
    /// The rest of a body frame not yet cut into envelopes.
    leftover: Bytes,
    /// The `S` message is queued; once written, the RPC is finished.
    ended: bool,
}

impl Session {
    /// Run the RPC until it ends. The handler's future and response body are
    /// owned here, so returning drops (cancels) them.
    async fn drive<D: Dispatcher>(
        &mut self,
        service: ConnectRpcService<D>,
        handshake: Handshake,
        mut shutdown: Option<watch::Receiver<bool>>,
    ) -> End {
        let config = Arc::clone(&handshake.config);
        let client_timeout = match handshake.timeout {
            Ok(Some(Duration::ZERO)) => {
                return End::Fail(
                    ConnectError::deadline_exceeded("the client's deadline was zero"),
                    close_code::NORMAL,
                );
            }
            Ok(timeout) => timeout,
            Err(err) => return End::Fail(err, close_code::NORMAL),
        };
        let limit = [client_timeout, config.max_timeout]
            .into_iter()
            .flatten()
            .min();
        let deadline = limit.and_then(|limit| handshake.accepted_at.checked_add(limit));
        let opening_deadline = config
            .opening_timeout
            .and_then(|timeout| handshake.accepted_at.checked_add(timeout))
            .filter(|opening| deadline.is_none_or(|deadline| *opening < deadline));
        let mut deadline_sleep = std::pin::pin!(sleep_until(opening_deadline.or(deadline)));
        let mut opening = opening_deadline.is_some();

        let mut pending_handshake = Some((handshake.headers, handshake.extensions));
        let procedure = handshake.procedure;
        let mut inbound = Inbound::default();
        let mut out = Outflow {
            response: None,
            body: None,
            assembler: EnvelopeAssembler::new(usize::MAX),
            leftover: Bytes::new(),
            ended: false,
        };
        let mut last_heard = Instant::now();
        let silence_limit = config
            .keep_alive_interval
            .map(|interval| config.keep_alive_timeout.max(interval.saturating_mul(2)));
        let mut keep_alive = config.keep_alive_interval.map(|interval| {
            let mut ticker = tokio::time::interval_at(Instant::now() + interval, interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker
        });

        loop {
            if let Err(end) = self.pump(&mut out) {
                return end;
            }
            if out.ended && !self.outbound.is_busy() {
                return End::Finished;
            }
            let can_produce = self.outbound.is_empty() && !out.ended;
            tokio::select! {
                biased;
                () = &mut deadline_sleep => {
                    if opening {
                        return End::Fail(
                            ConnectError::deadline_exceeded(
                                "no Leading-Metadata message arrived before the opening timeout",
                            ),
                            close_code::NORMAL,
                        );
                    }
                    return End::Fail(
                        ConnectError::deadline_exceeded("the RPC deadline expired"),
                        close_code::NORMAL,
                    );
                }
                () = wait_shutdown(shutdown.as_mut()) => {
                    return End::Fail(
                        ConnectError::unavailable("the server is shutting down"),
                        close_code::GOING_AWAY,
                    );
                }
                written = std::future::poll_fn(|cx| self.outbound.poll_write(&mut self.sink, cx)),
                    if self.outbound.is_busy() =>
                {
                    if written.is_err() {
                        return End::Canceled;
                    }
                }
                permit = reserve(inbound.tx.as_ref()), if inbound.pending.is_some() => {
                    match (permit, inbound.pending.take()) {
                        (Some(permit), Some(item)) => {
                            permit.send(Ok(item));
                        }
                        // The handler dropped its request stream: what the
                        // client sends now goes nowhere.
                        _ => inbound.tx = None,
                    }
                    if inbound.client_done {
                        inbound.tx = None;
                    }
                }
                message = self.stream.next(), if inbound.pending.is_none() => {
                    last_heard = Instant::now();
                    let message = match message {
                        Some(Ok(Message::Close(_))) | None => return End::Canceled,
                        Some(Ok(message)) => message,
                        Some(Err(err)) => return self.read_error(err),
                    };
                    if out.ended {
                        continue;
                    }
                    let message = match wire::classify(message, Side::Server, self.codec) {
                        Ok(Some(message)) => message,
                        Ok(None) => continue,
                        Err(err) => return err.into(),
                    };
                    if let Some((headers, extensions)) = pending_handshake.take() {
                        if message.marker != Marker::Metadata {
                            return protocol_error(
                                "the first message must be a Leading-Metadata ('M') message",
                            )
                            .into();
                        }
                        let request = match build_request(
                            &procedure,
                            headers,
                            extensions,
                            self.dispatch.clone(),
                            &message.payload,
                            &config,
                            self.codec,
                            deadline,
                        ) {
                            Ok(request) => request,
                            Err(err) => return err.into(),
                        };
                        let (tx, rx) = mpsc::channel(REQUEST_QUEUE);
                        inbound.tx = Some(tx);
                        out.response = Some(service.clone().call(request.map(|()| RequestBody { rx })));
                        opening = false;
                        deadline_sleep.set(sleep_until(deadline));
                        continue;
                    }
                    if let Err(end) = receive(&mut inbound, message) {
                        return end;
                    }
                }
                result = next_response(&mut out.response), if can_produce => {
                    out.response = None;
                    let (parts, response_body) = result.into_parts();
                    if parts.status != StatusCode::OK {
                        return End::Fail(
                            ConnectError::internal(format!(
                                "unexpected HTTP status {} from the service",
                                parts.status
                            )),
                            close_code::INTERNAL_ERROR,
                        );
                    }
                    let metadata = encode_metadata(&parts.headers, skip_response_header);
                    self.outbound.push(wire::encode_json(Marker::Metadata, &metadata));
                    self.sent_metadata = true;
                    out.body = Some(response_body);
                }
                frame = next_frame(&mut out.body), if can_produce && out.leftover.is_empty() => {
                    match frame {
                        Some(Ok(frame)) => {
                            if let Ok(data) = frame.into_data() {
                                out.leftover = data;
                            }
                        }
                        Some(Err(never)) => match never {},
                        None => {
                            return End::Fail(
                                ConnectError::internal(
                                    "the response ended without an end-of-stream message",
                                ),
                                close_code::INTERNAL_ERROR,
                            );
                        }
                    }
                }
                () = tick(keep_alive.as_mut()) => {
                    if inbound.pending.is_some() {
                        // A message this end has not taken yet is not the
                        // client's silence.
                        last_heard = Instant::now();
                    }
                    if silence_limit.is_some_and(|limit| last_heard.elapsed() > limit) {
                        return End::Silent;
                    }
                    if !self.outbound.is_busy() {
                        self.outbound.push(Message::Ping(Bytes::new()));
                    }
                }
            }
        }
    }

    /// Cut at most one envelope from the response body into the outbound
    /// queue, so that a stalled write stops the handler rather than buffering
    /// its whole stream.
    fn pump(&mut self, out: &mut Outflow) -> Result<(), End> {
        if !self.outbound.is_empty() || out.ended || out.leftover.is_empty() {
            return Ok(());
        }
        let envelope = out.assembler.feed(&mut out.leftover).map_err(|err| {
            End::Fail(
                ConnectError::internal(format!("malformed response from the service: {err}")),
                close_code::INTERNAL_ERROR,
            )
        })?;
        let Some(envelope) = envelope else {
            return Ok(());
        };
        if envelope.is_compressed() {
            return Err(End::Fail(
                ConnectError::internal("the service compressed a WebSocket response message"),
                close_code::INTERNAL_ERROR,
            ));
        }
        if envelope.is_end_stream() {
            if let Some(message) = self.dispatch.undecodable() {
                return Err(WireError::protocol(message).into());
            }
            let message = wire::encode(Marker::ServerEnd, &envelope.data, false)
                .map_err(|err| End::Fail(err, close_code::INTERNAL_ERROR))?;
            self.outbound.push(message);
            self.sent_end = true;
            out.ended = true;
            out.body = None;
            return Ok(());
        }
        let message = wire::encode_body(&envelope.data, self.codec)
            .map_err(|err| End::Fail(err, close_code::INTERNAL_ERROR))?;
        self.outbound.push(message);
        Ok(())
    }

    fn read_error(&self, err: WsError) -> End {
        match err {
            WsError::Capacity(_) => End::from(WireError::new(
                Fault::TooLarge,
                format!("message exceeds the {} byte limit", self.max_message_size),
            )),
            WsError::Utf8(_) => End::from(WireError::new(
                Fault::Encoding,
                "a text message is not valid UTF-8",
            )),
            WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => End::Canceled,
            WsError::Protocol(err) => {
                protocol_error(&format!("WebSocket protocol violation: {err}")).into()
            }
            _ => End::Canceled,
        }
    }

    /// Send whatever the end calls for, then close. Every write is bounded,
    /// since a client that stopped reading can stall them forever.
    async fn finish(&mut self, end: End) {
        let end = match end {
            End::Fail(..) | End::Abort(..) if self.sent_end => End::Finished,
            end => end,
        };
        let close = match end {
            End::Canceled => None,
            End::Finished => Some(wire::close_frame(close_code::NORMAL, "")),
            End::Silent => Some(wire::close_frame(
                close_code::INTERNAL_ERROR,
                "the client stopped responding",
            )),
            End::Abort(code, reason) => Some(wire::close_frame(code, &reason)),
            End::Fail(err, code) => {
                if !self.sent_metadata {
                    self.outbound
                        .push(wire::encode_json(Marker::Metadata, b"[]"));
                }
                self.outbound.push(wire::encode_json(
                    Marker::ServerEnd,
                    &end_stream_error_json(&err),
                ));
                Some(wire::close_frame(
                    code,
                    err.message.as_deref().unwrap_or_default(),
                ))
            }
        };
        let closing = close.is_some();
        if let Some(frame) = close {
            self.outbound.push(Message::Close(Some(frame)));
        }
        let written =
            tokio::time::timeout(DRAIN_TIMEOUT, self.outbound.write(&mut self.sink)).await;
        if matches!(written, Ok(Ok(()))) || !closing {
            self.drain().await;
        }
    }

    /// Read and discard until the client closes or the drain window passes,
    /// so a TCP reset cannot destroy what was sent, and so tungstenite
    /// answers a client's Close.
    async fn drain(&mut self) {
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, async {
            while let Some(Ok(_)) = self.stream.next().await {}
        })
        .await;
    }
}

/// Take one message after the opening `M`.
fn receive(inbound: &mut Inbound, message: wire::WireMessage) -> Result<(), End> {
    match message.marker {
        Marker::Metadata => Err(protocol_error("a second Leading-Metadata ('M') message").into()),
        _ if inbound.client_done => {
            Err(protocol_error("a message after the Client-End-Stream ('C') message").into())
        }
        Marker::Body => {
            let item = wire::envelope(flags::DATA, &message.payload)
                .map_err(|err| End::Fail(err, close_code::MESSAGE_TOO_BIG))?;
            inbound.pending = offer(inbound.tx.as_ref(), item);
            Ok(())
        }
        Marker::ClientEnd => {
            inbound.client_done = true;
            if inbound.pending.is_none() {
                inbound.tx = None;
            }
            Ok(())
        }
        Marker::ServerEnd => Err(protocol_error("'S' is not valid from a client").into()),
    }
}

fn protocol_error(message: &str) -> WireError {
    WireError::protocol(message)
}

/// Build the request the service dispatches, whose headers are the RPC's
/// effective headers (§7.1, §7.3): the handshake's, less the ones that
/// describe the connection, plus the client's metadata.
#[allow(clippy::too_many_arguments)]
fn build_request(
    procedure: &str,
    mut headers: http::HeaderMap,
    mut extensions: http::Extensions,
    dispatch: WebSocketDispatch,
    metadata: &[u8],
    config: &WebSocketConfig,
    codec: CodecFormat,
    deadline: Option<Instant>,
) -> Result<Request<()>, WireError> {
    let metadata = decode_metadata(metadata)?;
    for (name, _) in &metadata {
        if is_reserved_request_key(name.as_str(), &config.infrastructure_headers) {
            return Err(protocol_error(&format!(
                "metadata key {:?} is reserved and may not be set by a client",
                name.as_str()
            )));
        }
        if headers.contains_key(name) {
            return Err(protocol_error(&format!(
                "metadata key {:?} is also a handshake header",
                name.as_str()
            )));
        }
    }
    let connection_headers: Vec<HeaderName> = headers
        .keys()
        .filter(|name| is_connection_header(name.as_str()))
        .cloned()
        .collect();
    for name in connection_headers {
        headers.remove(name);
    }
    for (name, values) in metadata {
        for value in values {
            headers.append(name.clone(), value);
        }
    }
    let content_type = match codec {
        CodecFormat::Json => "application/connect+json",
        _ => "application/connect+proto",
    };
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    if let Some(deadline) = deadline {
        let remaining = deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        headers.insert(
            HeaderName::from_static("connect-timeout-ms"),
            HeaderValue::from(remaining.min(9_999_999_999) as u64),
        );
    }
    extensions.insert(WebSocketRpc);
    extensions.insert(dispatch);
    let mut request = Request::new(());
    *request.method_mut() = Method::POST;
    *request.uri_mut() = procedure
        .parse()
        .map_err(|_| protocol_error("the procedure path is not a valid URI path"))?;
    *request.headers_mut() = headers;
    *request.extensions_mut() = extensions;
    Ok(request)
}

/// Handshake headers that describe the connection, not the RPC (§7.1). The
/// `content-` ones would describe a handshake body, and the RPC has its own.
fn is_connection_header(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "upgrade"
            | "content-type"
            | "content-encoding"
            | "content-length"
            | "transfer-encoding"
    ) || name.starts_with("sec-websocket-")
        || name.starts_with("connect-")
}

/// Response headers that describe the HTTP framing rather than metadata.
fn skip_response_header(name: &HeaderName) -> bool {
    is_protocol_header(name.as_str())
        || matches!(
            name.as_str(),
            "content-length"
                | "transfer-encoding"
                | "connection"
                | "keep-alive"
                | "te"
                | "trailer"
                | "upgrade"
                | "date"
        )
}

/// Queue `item` for the handler. Returns it back when the queue is full, so
/// the caller holds it and stops reading until there is room. A handler
/// that dropped its request stream gets nothing more.
fn offer(tx: Option<&mpsc::Sender<RequestItem>>, item: Bytes) -> Option<Bytes> {
    match tx?.try_send(Ok(item)) {
        Err(mpsc::error::TrySendError::Full(Ok(item))) => Some(item),
        _ => None,
    }
}

async fn reserve(tx: Option<&mpsc::Sender<RequestItem>>) -> Option<mpsc::OwnedPermit<RequestItem>> {
    tx?.clone().reserve_owned().await.ok()
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Resolves when the server begins shutting down, or has gone away.
async fn wait_shutdown(shutdown: Option<&mut watch::Receiver<bool>>) {
    match shutdown {
        Some(shutdown) => {
            let _ = shutdown.wait_for(|fired| *fired).await;
        }
        None => std::future::pending().await,
    }
}

async fn tick(interval: Option<&mut tokio::time::Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}

async fn next_response(response: &mut Option<ResponseFuture>) -> Response<ConnectRpcBody> {
    match response {
        Some(future) => match future.await {
            Ok(response) => response,
            Err(never) => match never {},
        },
        None => std::future::pending().await,
    }
}

async fn next_frame(body: &mut Option<ConnectRpcBody>) -> Option<Result<Frame<Bytes>, Infallible>> {
    match body {
        Some(body) => body.frame().await,
        None => std::future::pending().await,
    }
}

/// The request stream ended because the connection did.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Canceled;

impl std::fmt::Display for Canceled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the WebSocket connection ended before the client finished its request stream")
    }
}

impl std::error::Error for Canceled {}

/// The synthesized request's body: the client's messages as envelopes.
struct RequestBody {
    rx: mpsc::Receiver<RequestItem>,
}

impl Body for RequestBody {
    type Data = Bytes;
    type Error = Canceled;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Canceled>>> {
        self.rx
            .poll_recv(cx)
            .map(|item| item.map(|item| item.map(Frame::data)))
    }
}
