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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, ready};
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
use tokio_tungstenite::tungstenite::protocol::Role;
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
use crate::service::{
    ConnectRpcBody, ConnectRpcService, DRAIN_TIMEOUT, end_stream_error_json, error_response_either,
};

/// How many request messages may wait for the handler, beyond the one the
/// session holds, before it stops reading the connection. Each can be up to
/// the message size limit, so this stays small.
const REQUEST_QUEUE: usize = 1;

/// About how many response bytes are queued before they are written and
/// flushed together.
const WRITE_BATCH: usize = 64 * 1024;

/// Request extension marking a synthesized request whose unary method may be
/// served with streaming framing. The service reports through it whether the
/// RPC ended because a request message did not decode, which on a WebSocket
/// is a protocol error (§6.1): still `invalid_argument`, but closed with 1002.
#[derive(Debug, Clone, Default)]
pub(crate) struct WebSocketDispatch {
    undecodable: Arc<OnceLock<String>>,
    recorded: Arc<Notify>,
}

impl WebSocketDispatch {
    /// Record `err` if it reports a request message that did not decode.
    pub(crate) fn note(&self, err: &ConnectError) {
        if err.is_decode_failure()
            && self
                .undecodable
                .set(err.message.clone().unwrap_or_default())
                .is_ok()
        {
            self.recorded.notify_one();
        }
    }

    fn undecodable(&self) -> Option<&str> {
        self.undecodable.get().map(String::as_str)
    }

    /// The message of the first request message that did not decode, once
    /// one has, whatever the handler then does with the error.
    async fn wait_undecodable(&self) -> &str {
        loop {
            if let Some(message) = self.undecodable() {
                return message;
            }
            self.recorded.notified().await;
        }
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
    if service
        .dispatcher()
        .lookup(procedure.trim_start_matches('/'))
        .is_none()
    {
        return error_response_either(
            ConnectError::unimplemented(format!("method not found: {procedure}"))
                .with_http_status(StatusCode::NOT_FOUND),
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
    let frame_config = wire::frame_config(limits.max_message_size());
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
        read_failed: false,
        max_message_size: limits.max_message_size(),
    };
    let shutdown = guard.as_ref().map(|guard| guard.0.0.shutdown.clone());
    let procedure = handshake.procedure.clone();
    let driven = AssertUnwindSafe(session.drive(service, handshake, shutdown))
        .catch_unwind()
        .await;
    let end = driven.unwrap_or_else(|payload| {
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("handler panicked");
        tracing::error!(procedure, "a WebSocket RPC handler panicked: {message}");
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
    /// tungstenite reads nothing more after an error.
    read_failed: bool,
    max_message_size: usize,
}

/// Request-side state: what the client has sent and where it goes.
#[derive(Default)]
struct Inbound {
    tx: Option<mpsc::Sender<Bytes>>,
    /// A message waiting for room in the handler's queue. While one waits,
    /// the connection is not read.
    pending: Option<Bytes>,
    client_done: bool,
    /// Shared with the request body, so that it can tell a `C` from a
    /// connection that ended.
    client_ended: Arc<AtomicBool>,
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
        mut handshake: Handshake,
        mut shutdown: Option<watch::Receiver<bool>>,
    ) -> End {
        let (deadline, opening_deadline) = match handshake.deadlines() {
            Ok(deadlines) => deadlines,
            Err(err) => return End::Fail(err, close_code::NORMAL),
        };
        let mut deadline_sleep = std::pin::pin!(sleep_until(opening_deadline.or(deadline)));
        let mut opening = opening_deadline.is_some();
        let mut keep_alive = KeepAlive::new(&handshake.config);
        let mut pending_handshake = Some(handshake);
        let dispatch = self.dispatch.clone();
        let mut inbound = Inbound::default();
        let mut out = Outflow {
            response: None,
            body: None,
            assembler: EnvelopeAssembler::new(usize::MAX),
            leftover: Bytes::new(),
            ended: false,
        };

        loop {
            if let Err(end) = self.pump(&mut out) {
                return end;
            }
            if out.ended && !self.outbound.is_busy() {
                return End::Finished;
            }
            let can_produce = self.outbound.queued_bytes() < WRITE_BATCH && !out.ended;
            tokio::select! {
                biased;
                () = &mut deadline_sleep => {
                    let message = if opening {
                        "no Leading-Metadata message arrived before the opening timeout"
                    } else {
                        "the RPC deadline expired"
                    };
                    return End::Fail(ConnectError::deadline_exceeded(message), close_code::NORMAL);
                }
                () = wait_shutdown(shutdown.as_mut()) => {
                    return End::Fail(
                        ConnectError::unavailable("the server is shutting down"),
                        close_code::GOING_AWAY,
                    );
                }
                message = dispatch.wait_undecodable() => {
                    return WireError::undecodable(message).into();
                }
                written = std::future::poll_fn(|cx| self.outbound.poll_write(&mut self.sink, cx)),
                    if self.outbound.is_busy() =>
                {
                    if written.is_err() {
                        return End::Canceled;
                    }
                }
                permit = wire::reserve(inbound.tx.as_ref()), if inbound.pending.is_some() => {
                    match (permit, inbound.pending.take()) {
                        (Some(permit), Some(item)) => {
                            permit.send(item);
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
                    keep_alive.heard();
                    let message = match message {
                        Some(Ok(Message::Close(_))) | None => return End::Canceled,
                        Some(Ok(message)) => message,
                        Some(Err(err)) => {
                            self.read_failed = true;
                            return self.read_error(err);
                        }
                    };
                    if out.ended {
                        continue;
                    }
                    let message = match wire::classify(message, Side::Server, self.codec) {
                        Ok(Some(message)) => message,
                        Ok(None) => continue,
                        Err(err) => return err.into(),
                    };
                    let result = match pending_handshake.take() {
                        Some(handshake) => self
                            .open(&service, handshake, message, &mut inbound, deadline)
                            .map(|response| out.response = Some(response)),
                        None => receive(&mut inbound, message),
                    };
                    if let Err(end) = result {
                        return end;
                    }
                    if out.response.is_some() && opening {
                        opening = false;
                        deadline_sleep.set(sleep_until(deadline));
                    }
                }
                result = next_response(&mut out.response), if can_produce => {
                    out.response = None;
                    if let Err(end) = self.start_response(&mut out, result) {
                        return end;
                    }
                }
                frame = next_frame(&mut out.body), if can_produce && out.leftover.is_empty() => {
                    if let Err(end) = self.take_frames(&mut out, frame) {
                        return end;
                    }
                }
                () = keep_alive.tick() => {
                    if inbound.pending.is_some() && !self.outbound.is_busy() {
                        // The handler, not the client, is slow to take this
                        // message; a stalled write means the client stopped reading.
                        keep_alive.heard();
                    }
                    if keep_alive.silent() {
                        return End::Abort(close_code::INTERNAL_ERROR, "the client stopped responding".into());
                    }
                    if !self.outbound.is_busy() {
                        self.outbound.push(Message::Ping(Bytes::new()));
                    }
                }
            }
        }
    }

    /// Dispatch the RPC once its opening `M` arrives.
    fn open<D: Dispatcher>(
        &mut self,
        service: &ConnectRpcService<D>,
        handshake: Handshake,
        message: wire::WireMessage,
        inbound: &mut Inbound,
        deadline: Option<Instant>,
    ) -> Result<ResponseFuture, End> {
        if message.marker != Marker::Metadata {
            return Err(WireError::protocol(
                "the first message must be a Leading-Metadata ('M') message",
            )
            .into());
        }
        let request = handshake.into_request(&message.payload, self.dispatch.clone(), deadline)?;
        let (tx, rx) = mpsc::channel(REQUEST_QUEUE);
        inbound.tx = Some(tx);
        let body = RequestBody {
            rx,
            client_ended: Arc::clone(&inbound.client_ended),
            canceled: false,
        };
        Ok(service.clone().call(request.map(|()| body)))
    }

    /// Queue the response's `M`, and keep its body to cut into messages.
    fn start_response(
        &mut self,
        out: &mut Outflow,
        response: Response<ConnectRpcBody>,
    ) -> Result<(), End> {
        let (parts, body) = response.into_parts();
        if parts.status != StatusCode::OK {
            return Err(End::Fail(
                ConnectError::internal(format!(
                    "unexpected HTTP status {} from the service",
                    parts.status
                )),
                close_code::INTERNAL_ERROR,
            ));
        }
        let metadata = encode_metadata(&parts.headers, skip_response_header);
        self.outbound
            .push(wire::encode_json(Marker::Metadata, &metadata));
        self.sent_metadata = true;
        if let Some(name) = handler_connect_header(&parts.headers) {
            return Err(End::Fail(
                ConnectError::internal(format!(
                    "response header {:?} is reserved for the protocol",
                    name.as_str()
                )),
                close_code::NORMAL,
            ));
        }
        out.body = Some(body);
        Ok(())
    }

    /// Take `frame` and any further response frames that are ready, cutting
    /// them into messages until a batch is queued, so that one flush carries
    /// them all.
    fn take_frames(
        &mut self,
        out: &mut Outflow,
        mut frame: Option<Result<Frame<Bytes>, Infallible>>,
    ) -> Result<(), End> {
        loop {
            match frame {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        out.leftover = data;
                    }
                }
                Some(Err(never)) => match never {},
                None => {
                    return Err(End::Fail(
                        ConnectError::internal(
                            "the response ended without an end-of-stream message",
                        ),
                        close_code::INTERNAL_ERROR,
                    ));
                }
            }
            self.pump(out)?;
            if !out.leftover.is_empty() || out.ended {
                return Ok(());
            }
            match out
                .body
                .as_mut()
                .and_then(|body| body.frame().now_or_never())
            {
                Some(next) => frame = next,
                None => return Ok(()),
            }
        }
    }

    /// Cut envelopes from the response body into the outbound queue until
    /// about a batch is queued, so that a stalled write stops the handler
    /// rather than buffering its whole stream.
    fn pump(&mut self, out: &mut Outflow) -> Result<(), End> {
        while !out.ended && !out.leftover.is_empty() && self.outbound.queued_bytes() < WRITE_BATCH {
            let envelope = out.assembler.feed(&mut out.leftover).map_err(|err| {
                End::Fail(
                    ConnectError::internal(format!("malformed response from the service: {err}")),
                    close_code::INTERNAL_ERROR,
                )
            })?;
            let Some(envelope) = envelope else {
                continue;
            };
            if envelope.is_compressed() {
                return Err(End::Fail(
                    ConnectError::internal("the service compressed a WebSocket response message"),
                    close_code::INTERNAL_ERROR,
                ));
            }
            if envelope.is_end_stream() {
                if let Some(message) = self.dispatch.undecodable() {
                    return Err(WireError::undecodable(message).into());
                }
                let payload = wire::fail_connect_metadata(&envelope.data, |key| {
                    end_stream_error_json(&ConnectError::internal(format!(
                        "response trailer {key:?} is reserved for the protocol"
                    )))
                })
                .unwrap_or(envelope.data);
                let message = wire::encode(Marker::ServerEnd, &payload, false)
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
        }
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
                WireError::protocol(format!("WebSocket protocol violation: {err}")).into()
            }
            _ => End::Canceled,
        }
    }

    /// Send whatever the end calls for, then close. Every write is bounded,
    /// since a client that stopped reading can stall them forever.
    async fn finish(mut self, end: End) {
        let end = match end {
            End::Fail(..) | End::Abort(..) if self.sent_end => End::Finished,
            end => end,
        };
        let close = match end {
            End::Canceled => None,
            End::Finished => Some(wire::close_frame(close_code::NORMAL, "")),
            End::Abort(code, reason) => Some(wire::close_frame(code, &reason)),
            End::Fail(err, code) => {
                if !self.sent_metadata {
                    self.outbound
                        .push(wire::encode_json(Marker::Metadata, b"[]"));
                }
                self.outbound.push(wire::encode_json(
                    Marker::ServerEnd,
                    &wire::without_connect_metadata(end_stream_error_json(&err)),
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
            wire::drain(self.sink, self.stream, self.read_failed).await;
        }
    }
}

/// Take one message after the opening `M`.
fn receive(inbound: &mut Inbound, message: wire::WireMessage) -> Result<(), End> {
    match message.marker {
        Marker::Metadata => {
            Err(WireError::protocol("a second Leading-Metadata ('M') message").into())
        }
        _ if inbound.client_done => {
            Err(WireError::protocol("a message after the Client-End-Stream ('C') message").into())
        }
        Marker::Body => {
            let item = wire::envelope(flags::DATA, &message.payload)
                .map_err(|err| End::Fail(err, close_code::MESSAGE_TOO_BIG))?;
            inbound.pending = wire::offer(inbound.tx.as_ref(), item);
            Ok(())
        }
        Marker::ClientEnd => {
            inbound.client_done = true;
            inbound.client_ended.store(true, Ordering::Release);
            if inbound.pending.is_none() {
                inbound.tx = None;
            }
            Ok(())
        }
        Marker::ServerEnd => Err(WireError::protocol("'S' is not valid from a client").into()),
    }
}

impl Handshake {
    /// The RPC's deadline (§11.1), and the earlier one by which the opening
    /// `M` must arrive. A timeout the RPC cannot run under is the error.
    fn deadlines(&mut self) -> Result<(Option<Instant>, Option<Instant>), ConnectError> {
        let client_timeout = match std::mem::replace(&mut self.timeout, Ok(None))? {
            Some(Duration::ZERO) => {
                return Err(ConnectError::deadline_exceeded(
                    "the client's deadline was zero",
                ));
            }
            timeout => timeout,
        };
        let limit = [client_timeout, self.config.max_timeout]
            .into_iter()
            .flatten()
            .min();
        let deadline = limit.and_then(|limit| self.accepted_at.checked_add(limit));
        let opening = self
            .config
            .opening_timeout
            .and_then(|timeout| self.accepted_at.checked_add(timeout))
            .filter(|opening| deadline.is_none_or(|deadline| *opening < deadline));
        Ok((deadline, opening))
    }

    /// The request the service dispatches, whose headers are the RPC's
    /// effective headers (§7.1, §7.3): the handshake's, less the ones that
    /// describe the connection, plus the client's metadata.
    fn into_request(
        self,
        metadata: &[u8],
        dispatch: WebSocketDispatch,
        deadline: Option<Instant>,
    ) -> Result<Request<()>, WireError> {
        let Self {
            mut headers,
            mut extensions,
            procedure,
            codec,
            config,
            ..
        } = self;
        let metadata = decode_metadata(metadata)?;
        for (name, _) in &metadata {
            if is_reserved_request_key(name.as_str(), &config.infrastructure_headers) {
                return Err(WireError::protocol(format!(
                    "metadata key {:?} is reserved and may not be set by a client",
                    name.as_str()
                )));
            }
            if headers.contains_key(name) {
                return Err(WireError::protocol(format!(
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
        wire::append_metadata(&mut headers, metadata);
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
                HeaderName::from_static(wire::TIMEOUT_PARAM),
                HeaderValue::from(remaining.min(9_999_999_999) as u64),
            );
        }
        extensions.insert(WebSocketRpc);
        extensions.insert(dispatch);
        let mut request = Request::new(());
        *request.method_mut() = Method::POST;
        *request.uri_mut() = procedure
            .parse()
            .map_err(|_| WireError::protocol("the procedure path is not a valid URI path"))?;
        *request.headers_mut() = headers;
        *request.extensions_mut() = extensions;
        Ok(request)
    }
}

/// The keep-alive Pings, and how long the client may stay silent (§4.6).
struct KeepAlive {
    ticker: Option<tokio::time::Interval>,
    silence_limit: Option<Duration>,
    last_heard: Instant,
}

impl KeepAlive {
    fn new(config: &WebSocketConfig) -> Self {
        let ticker = config.keep_alive_interval.and_then(|interval| {
            let start = Instant::now().checked_add(interval)?;
            let mut ticker = tokio::time::interval_at(start, interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            Some(ticker)
        });
        let silence_limit = config
            .keep_alive_interval
            .map(|interval| config.keep_alive_timeout.max(interval.saturating_mul(2)));
        Self {
            ticker,
            silence_limit,
            last_heard: Instant::now(),
        }
    }

    fn heard(&mut self) {
        self.last_heard = Instant::now();
    }

    fn silent(&self) -> bool {
        self.silence_limit
            .is_some_and(|limit| self.last_heard.elapsed() > limit)
    }

    async fn tick(&mut self) {
        match &mut self.ticker {
            Some(ticker) => {
                ticker.tick().await;
            }
            None => std::future::pending().await,
        }
    }
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

/// The first response header under a `connect-` name that the handler set,
/// which a server must not send (§7.4). The service sets the encoding ones.
fn handler_connect_header(headers: &http::HeaderMap) -> Option<&HeaderName> {
    headers.keys().find(|name| {
        name.as_str().starts_with("connect-")
            && **name != crate::protocol::hdr::CONNECT_ACCEPT_ENCODING
            && **name != crate::protocol::hdr::CONNECT_CONTENT_ENCODING
    })
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

/// The synthesized request's body: the client's messages as envelopes. It
/// fails, rather than ends, if the connection ends before the client's `C`.
struct RequestBody {
    rx: mpsc::Receiver<Bytes>,
    client_ended: Arc<AtomicBool>,
    canceled: bool,
}

impl Body for RequestBody {
    type Data = Bytes;
    type Error = Canceled;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Canceled>>> {
        if self.canceled {
            return Poll::Ready(None);
        }
        let item = ready!(self.rx.poll_recv(cx));
        if item.is_none() && !self.client_ended.load(Ordering::Acquire) {
            self.canceled = true;
            return Poll::Ready(Some(Err(Canceled)));
        }
        Poll::Ready(item.map(|item| Ok(Frame::data(item))))
    }
}

#[cfg(test)]
mod tests {
    use buffa_types::google::protobuf::StringValue;
    use bytes::Bytes;
    use futures::StreamExt;

    use super::WebSocketDispatch;
    use crate::{CodecFormat, ConnectError, RequestContext};

    #[tokio::test]
    async fn generated_stream_decoding_reports_an_undecodable_message() {
        let dispatch = WebSocketDispatch::default();
        let mut extensions = http::Extensions::new();
        extensions.insert(dispatch.clone());
        let ctx = RequestContext::new(http::HeaderMap::new()).with_extensions(extensions);
        let requests = futures::stream::iter([
            Ok::<_, ConnectError>(Bytes::from_static(b"\x0a\x01a")),
            Ok(Bytes::from_static(b"\xff")),
        ]);
        let decoded = crate::dispatcher::codegen::decode_request_stream::<StringValue>(
            Box::pin(requests),
            CodecFormat::Proto,
            &ctx,
        );
        let results: Vec<_> = decoded.collect().await;
        assert!(results[0].is_ok());
        assert!(results[1].is_err());
        assert!(dispatch.undecodable().is_some());
    }
}
