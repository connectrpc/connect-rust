//! The client half of Connect over WebSocket: a [`ClientTransport`] that
//! carries each Connect request over its own WebSocket connection.
//!
//! The call path builds an ordinary Connect request (unary or streaming);
//! this transport translates it into the WebSocket messages and translates
//! what comes back into the Connect response the call path expects, so
//! generated clients need no changes.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use http::header::{self, HeaderName, HeaderValue};
use http::{HeaderMap, Request, Response, StatusCode, Uri};
use http_body::{Body, Frame};
use http_body_util::BodyExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use super::wire::{
    self, DEFAULT_INFRASTRUCTURE_HEADERS, Fault, Marker, Outbound, Side, WireError, close_code,
    decode_metadata, encode_metadata, is_protocol_header, is_reserved_request_key,
};
use crate::client::{BoxFuture, ClientBody, ClientTransport};
use crate::codec::CodecFormat;
use crate::compression::CompressionRegistry;
use crate::envelope::{EnvelopeAssembler, flags};
use crate::error::{ConnectError, ErrorCode};
use crate::protocol::Protocol;
use crate::service::DEFAULT_MAX_MESSAGE_SIZE;

/// The default for [`WebSocketTransport::with_handshake_timeout`]: ten
/// seconds for DNS, TCP, TLS and the upgrade together.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How many response messages may wait for the caller before the transport
/// stops reading the connection.
const RESPONSE_QUEUE: usize = 2;

/// A [`ClientTransport`] that carries Connect RPCs over WebSocket, one
/// connection per RPC.
///
/// Use it with a generated client and a [`ClientConfig`] whose protocol is
/// Connect (the default) and whose base URI is the server's `http://` or
/// `https://` address; the transport switches the scheme to `ws://` or
/// `wss://`:
///
/// ```rust,ignore
/// use connectrpc::client::ClientConfig;
/// use connectrpc::websocket::WebSocketTransport;
///
/// let transport = WebSocketTransport::plaintext();
/// let config = ClientConfig::new("http://localhost:8080".parse()?);
/// let client = ChatServiceClient::new(transport, config);
/// let mut stream = client.chat().await?; // full-duplex bidi
/// ```
///
/// A server that serves WebSocket under its own path, with
/// [`WebSocketConfig::with_path_prefix`](super::WebSocketConfig::with_path_prefix),
/// is reached by putting that path in the base URI
/// (`http://localhost:8080/ws`). An HTTP client for the same server keeps
/// the bare base URI, since the prefixed paths refuse plain HTTP.
///
/// Every RPC kind is supported, unary included, and a bidi stream is full
/// duplex. Messages are always sent uncompressed, since this binding has no
/// message compression: a request compression set on the [`ClientConfig`] is
/// undone, and its encoding headers are not sent. Connect GET and the gRPC
/// protocols are not supported.
///
/// Request headers go to the server in the opening metadata message. Names a
/// server rejects there by default (`Cookie`, `Host`, `Sec-*`, `Forwarded`,
/// `X-Forwarded-*`, ...) and values that are not text fail the call with
/// `invalid_argument` before dialing. A name a server adds to its own deny
/// list fails the RPC when the server reads it.
///
/// A caller that stops reading a stream for longer than the server's
/// keep-alive timeout loses the RPC: an unread connection stops answering
/// the server's Pings.
///
/// Native targets only; in a browser, use the browser's `WebSocket` through
/// a JavaScript Connect client.
///
/// To pick between this and an HTTP transport per client, wrap both in one
/// [`ClientTransport`] whose body is `http_body_util::Either` of the two
/// bodies. Keep this transport's [`ConnectError`] as the wrapper error itself
/// or in its [`source`](std::error::Error::source) chain, so the call path
/// surfaces the RPC's code rather than `unavailable`.
///
/// [`ClientConfig`]: crate::client::ClientConfig
#[derive(Clone)]
pub struct WebSocketTransport {
    options: Arc<Options>,
}

#[derive(Clone)]
struct Options {
    max_message_size: usize,
    handshake_timeout: Option<Duration>,
    #[cfg(feature = "client-tls")]
    tls: Option<Arc<rustls::ClientConfig>>,
}

impl std::fmt::Debug for WebSocketTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("WebSocketTransport");
        debug
            .field("max_message_size", &self.options.max_message_size)
            .field("handshake_timeout", &self.options.handshake_timeout);
        #[cfg(feature = "client-tls")]
        debug.field("tls", &self.options.tls.is_some());
        debug.finish()
    }
}

impl WebSocketTransport {
    /// A transport for cleartext `ws://` connections only.
    #[must_use]
    pub fn plaintext() -> Self {
        Self {
            options: Arc::new(Options {
                max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
                handshake_timeout: Some(DEFAULT_HANDSHAKE_TIMEOUT),
                #[cfg(feature = "client-tls")]
                tls: None,
            }),
        }
    }

    /// A transport for `wss://` (and cleartext `ws://`) connections, with
    /// `tls_config` for TLS. ALPN is set to `http/1.1`, since the WebSocket
    /// handshake requires HTTP/1.1.
    #[cfg(feature = "client-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "client-tls")))]
    #[must_use]
    pub fn with_tls(tls_config: Arc<rustls::ClientConfig>) -> Self {
        let mut config = (*tls_config).clone();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        let mut transport = Self::plaintext();
        transport.options_mut().tls = Some(Arc::new(config));
        transport
    }

    /// The largest message the server may send, in bytes. A larger one
    /// fails the RPC with `resource_exhausted`. Defaults to 4 MiB.
    #[must_use]
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.options_mut().max_message_size = size;
        self
    }

    /// Bound connection setup: DNS, TCP, TLS and the upgrade. Defaults to
    /// [`DEFAULT_HANDSHAKE_TIMEOUT`]; `None` or zero removes the bound. The
    /// RPC itself is bounded by its own deadline, not this.
    #[must_use]
    pub fn with_handshake_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.options_mut().handshake_timeout = timeout.into().filter(|t| !t.is_zero());
        self
    }

    fn options_mut(&mut self) -> &mut Options {
        Arc::make_mut(&mut self.options)
    }
}

impl ClientTransport for WebSocketTransport {
    type ResponseBody = WebSocketBody;
    type Error = ConnectError;

    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> BoxFuture<'static, Result<Response<WebSocketBody>, ConnectError>> {
        let options = Arc::clone(&self.options);
        Box::pin(async move { start(options, request).await })
    }
}

/// The response body [`WebSocketTransport`] returns.
///
/// For a streaming RPC it yields the server's messages as Connect envelopes,
/// ending with the end-of-stream envelope; for a unary RPC it is the single
/// response message. A failure on the connection is its error.
///
/// A unary RPC reports such a failure from the transport's `send`, so its
/// code reaches the caller as is. A streaming RPC reports it while reading
/// this body; the client call path keeps its code (or reports
/// `deadline_exceeded` past the deadline) and keeps this body's
/// [`ConnectError`] as its [source](std::error::Error::source).
pub struct WebSocketBody {
    inner: BodyInner,
}

enum BodyInner {
    Full(Option<Bytes>),
    Stream(mpsc::Receiver<Result<Bytes, ConnectError>>),
}

impl std::fmt::Debug for WebSocketBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketBody").finish_non_exhaustive()
    }
}

impl Body for WebSocketBody {
    type Data = Bytes;
    type Error = ConnectError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, ConnectError>>> {
        match &mut self.inner {
            BodyInner::Full(data) => Poll::Ready(data.take().map(|data| Ok(Frame::data(data)))),
            BodyInner::Stream(rx) => rx
                .poll_recv(cx)
                .map(|item| item.map(|item| item.map(Frame::data))),
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(&self.inner, BodyInner::Full(None))
    }
}

/// What the call path asked for, read from its request.
struct Call {
    codec: CodecFormat,
    unary: bool,
    /// Request compression to undo: `content-encoding` for unary, the
    /// `connect-content-encoding` of compressed envelopes for streaming.
    encoding: Option<String>,
    /// The caller's codecs, which compressed the request.
    compression: CompressionRegistry,
    /// When the client's own timer expires (§11.1).
    deadline: Option<tokio::time::Instant>,
}

/// Refuse headers the opening metadata message cannot carry: names servers
/// reject there by default, and values that are not text.
fn check_request_headers(headers: &HeaderMap) -> Result<(), ConnectError> {
    for (name, value) in headers
        .iter()
        .filter(|(name, _)| !skip_request_header(name))
    {
        if is_reserved_request_key(name.as_str(), DEFAULT_INFRASTRUCTURE_HEADERS) {
            return Err(ConnectError::invalid_argument(format!(
                "header {:?} cannot be sent over WebSocket; servers reject it in request metadata by default",
                name.as_str()
            )));
        }
        if value.to_str().is_err() {
            return Err(ConnectError::invalid_argument(format!(
                "header {:?} has a value that is not text, which WebSocket metadata cannot carry",
                name.as_str()
            )));
        }
    }
    Ok(())
}

async fn start(
    options: Arc<Options>,
    request: Request<ClientBody>,
) -> Result<Response<WebSocketBody>, ConnectError> {
    let (parts, body) = request.into_parts();
    if parts.method == http::Method::GET {
        return Err(ConnectError::unimplemented(
            "Connect GET requests cannot be sent over WebSocket",
        ));
    }
    let detected = Protocol::detect(&parts.headers)
        .filter(|detected| detected.protocol == Protocol::Connect)
        .ok_or_else(|| {
            ConnectError::unimplemented("WebSocketTransport carries only the Connect protocol")
        })?;
    let encoding_header = if detected.is_streaming {
        "connect-content-encoding"
    } else {
        "content-encoding"
    };
    let timeout = parts
        .headers
        .get(wire::TIMEOUT_PARAM)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let call = Call {
        codec: detected.codec_format,
        unary: !detected.is_streaming,
        encoding: parts
            .headers
            .get(encoding_header)
            .and_then(|v| v.to_str().ok())
            .filter(|v| *v != "identity")
            .map(str::to_owned),
        compression: parts
            .extensions
            .get::<CompressionRegistry>()
            .cloned()
            .unwrap_or_default(),
        deadline: timeout
            .as_deref()
            .and_then(|millis| millis.parse().ok())
            .and_then(|millis| {
                tokio::time::Instant::now().checked_add(Duration::from_millis(millis))
            }),
    };
    let uri = websocket_uri(&parts.uri, timeout.as_deref())?;
    check_request_headers(&parts.headers)?;
    let metadata = encode_metadata(&parts.headers, skip_request_header);

    let connecting = connect(&options, uri, call.codec);
    let ws = match options.handshake_timeout {
        Some(timeout) => tokio::time::timeout(timeout, connecting)
            .await
            .map_err(|_| ConnectError::unavailable("WebSocket handshake timed out"))??,
        None => connecting.await?,
    };

    let (head_tx, head_rx) = oneshot::channel();
    let (sink, stream) = ws.split();
    let session = Session {
        sink,
        stream,
        outbound: Outbound::default(),
        codec: call.codec,
        max_message_size: options.max_message_size,
        read_failed: false,
    };
    crate::spawn_detached(session.run(call, metadata, body, head_tx));
    head_rx.await.unwrap_or_else(|_| {
        Err(ConnectError::internal(
            "WebSocket session ended unexpectedly",
        ))
    })
}

/// The handshake URI (§4.2): `ws`/`wss` for `http`/`https`, and the
/// deadline as a query parameter.
fn websocket_uri(uri: &Uri, timeout: Option<&str>) -> Result<Uri, ConnectError> {
    let scheme = match uri.scheme_str() {
        Some("http" | "ws") => "ws",
        Some("https" | "wss") => "wss",
        other => {
            return Err(ConnectError::invalid_argument(format!(
                "unsupported URI scheme {other:?} for WebSocket"
            )));
        }
    };
    let authority = uri
        .authority()
        .ok_or_else(|| ConnectError::invalid_argument("request URI has no authority"))?;
    let path = uri.path();
    let mut target = format!("{scheme}://{authority}{path}");
    if let Some(timeout) = timeout {
        target.push('?');
        target.push_str(wire::TIMEOUT_PARAM);
        target.push('=');
        target.push_str(timeout);
    }
    target.parse().map_err(|e| {
        ConnectError::invalid_argument(format!("invalid WebSocket URI {target:?}: {e}"))
    })
}

/// Request headers the protocol carries some other way, or that describe
/// the HTTP request rather than the RPC. This binding has no message
/// compression, so `accept-encoding` has nothing to negotiate.
fn skip_request_header(name: &HeaderName) -> bool {
    is_protocol_header(name.as_str())
        || matches!(
            name.as_str(),
            "accept-encoding"
                | "content-length"
                | "transfer-encoding"
                | "connection"
                | "te"
                | "host"
                | "upgrade"
        )
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

type Socket = WebSocketStream<Box<dyn Io>>;

/// Dial, optionally wrap in TLS, and perform the WebSocket handshake.
async fn connect(options: &Options, uri: Uri, codec: CodecFormat) -> Result<Socket, ConnectError> {
    let host = uri
        .host()
        .ok_or_else(|| ConnectError::invalid_argument("WebSocket URI has no host"))?;
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let secure = uri.scheme_str() == Some("wss");
    if secure && !has_tls(options) {
        return Err(no_tls());
    }
    let port = uri.port_u16().unwrap_or(if secure { 443 } else { 80 });
    let tcp = tokio::net::TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| ConnectError::unavailable_from_transport(e, "WebSocket connect failed"))?;
    let _ = tcp.set_nodelay(true);
    let io: Box<dyn Io> = if secure {
        tls_wrap(options, &host, tcp).await?
    } else {
        Box::new(tcp)
    };

    let mut request = uri
        .into_client_request()
        .map_err(|e| ConnectError::internal(format!("invalid WebSocket request: {e}")))?;
    request.headers_mut().insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(wire::subprotocol_token(codec)),
    );
    let config = wire::frame_config(options.max_message_size);
    // tungstenite fails the handshake, before the socket opens, when the
    // server echoes no subprotocol or one that was not offered (§4.3).
    let (mut ws, response) = tokio_tungstenite::client_async_with_config(request, io, Some(config))
        .await
        .map_err(handshake_error)?;
    // Compression is never offered, so a negotiated extension is the server
    // imposing one, which §10 forbids without no_context_takeover.
    if let Some(extensions) = response.headers().get(header::SEC_WEBSOCKET_EXTENSIONS) {
        let message = format!(
            "the server negotiated the WebSocket extension {:?}, which was not offered",
            String::from_utf8_lossy(extensions.as_bytes())
        );
        let _ = ws
            .close(Some(wire::close_frame(close_code::CLIENT_ERROR, &message)))
            .await;
        return Err(ConnectError::internal(message));
    }
    Ok(ws)
}

#[cfg(feature = "client-tls")]
async fn tls_wrap(
    options: &Options,
    host: &str,
    tcp: tokio::net::TcpStream,
) -> Result<Box<dyn Io>, ConnectError> {
    let Some(tls) = options.tls.clone() else {
        return Err(no_tls());
    };
    let name = rustls_pki_types::ServerName::try_from(host.to_owned())
        .map_err(|e| ConnectError::internal(format!("invalid TLS server name {host:?}: {e}")))?;
    let stream = tokio_rustls::TlsConnector::from(tls)
        .connect(name, tcp)
        .await
        .map_err(|e| ConnectError::unavailable_from_transport(e, "TLS handshake failed"))?;
    Ok(Box::new(stream))
}

#[cfg(not(feature = "client-tls"))]
async fn tls_wrap(
    _options: &Options,
    _host: &str,
    _tcp: tokio::net::TcpStream,
) -> Result<Box<dyn Io>, ConnectError> {
    Err(no_tls())
}

fn has_tls(options: &Options) -> bool {
    #[cfg(feature = "client-tls")]
    return options.tls.is_some();
    #[cfg(not(feature = "client-tls"))]
    {
        let _ = options;
        false
    }
}

fn no_tls() -> ConnectError {
    ConnectError::invalid_argument(
        "wss:// needs a TLS transport; build it with WebSocketTransport::with_tls \
         (requires the client-tls feature)",
    )
}

/// Map a failed handshake to the error the RPC reports (§4.5). A rejected
/// upgrade reports the Connect error in its body, or else is mapped from its
/// HTTP status as Connect maps any HTTP status.
/// The most bytes of a refused handshake's body that an error quotes.
const MAX_HANDSHAKE_DETAIL: usize = 512;

fn handshake_error(err: WsError) -> ConnectError {
    match err {
        WsError::Http(response) => {
            let status = response.status();
            let body = response.body().as_deref().unwrap_or_default();
            if let Some(err) = crate::client::connect_error_from_body(status, body) {
                return err;
            }
            // A proxy's error page can be long; the start says what it is.
            let shown = &body[..body.len().min(MAX_HANDSHAKE_DETAIL)];
            let detail = String::from_utf8_lossy(shown).trim().to_owned();
            let detail = if detail.is_empty() {
                status.to_string()
            } else {
                detail
            };
            ConnectError::new(
                crate::client::http_status_to_error_code(status),
                format!(
                    "WebSocket handshake rejected with HTTP {}: {detail}",
                    status.as_u16()
                ),
            )
        }
        WsError::Io(err) => {
            ConnectError::unavailable_from_transport(err, "WebSocket handshake failed")
        }
        WsError::Protocol(ProtocolError::SecWebSocketSubProtocolError(err)) => {
            ConnectError::internal(format!("WebSocket subprotocol negotiation failed: {err}"))
        }
        other => ConnectError::unavailable(format!("WebSocket handshake failed: {other}")),
    }
}

type HeadSender = oneshot::Sender<Result<Response<WebSocketBody>, ConnectError>>;
type BodySender = mpsc::Sender<Result<Bytes, ConnectError>>;

/// How the session ended.
enum End {
    /// The server's `S` arrived and was delivered.
    Finished,
    /// Fail the RPC with this error, closing with this code (`None` when
    /// the connection is already gone).
    Fail(ConnectError, Option<u16>),
    /// The server broke the protocol: fail the RPC with this error and close
    /// with this code, telling the server why.
    Fault(ConnectError, u16),
    /// The caller, which has the error already, could not decode a streamed
    /// message: close with [`close_code::CLIENT_ERROR`] and this reason (§6.1).
    Undecodable(String),
    /// The caller dropped the call.
    Abandoned,
}

impl From<WireError> for End {
    fn from(err: WireError) -> Self {
        End::Fault(err.error, err.fault.close_code(Side::Client))
    }
}

/// Response extension through which the call path reports a response message
/// it could not decode, which is a protocol error (§6.1) the session explains
/// in its Close frame.
#[derive(Clone)]
pub(crate) struct DecodeReport(Arc<std::sync::Mutex<Option<oneshot::Sender<String>>>>);

impl DecodeReport {
    fn new() -> (Self, oneshot::Receiver<String>) {
        let (tx, rx) = oneshot::channel();
        (Self(Arc::new(std::sync::Mutex::new(Some(tx)))), rx)
    }

    /// Report that the response message did not decode, with `err`.
    pub(crate) fn fail(&self, err: &ConnectError) {
        let sender = self.0.lock().ok().and_then(|mut sender| sender.take());
        if let Some(sender) = sender {
            let _ = sender.send(err.message.clone().unwrap_or_default());
        }
    }
}

struct Session {
    sink: SplitSink<Socket, Message>,
    stream: SplitStream<Socket>,
    outbound: Outbound,
    codec: CodecFormat,
    max_message_size: usize,
    /// tungstenite reads nothing more after an error.
    read_failed: bool,
}

/// Where the RPC's response goes.
struct Delivery {
    head: Option<HeadSender>,
    body: Option<BodySender>,
    /// A response envelope waiting for room in the caller's queue. While one
    /// waits, the connection is not read.
    pending: Option<Bytes>,
    /// Whether the call path could decode the response.
    verdict: Option<oneshot::Receiver<String>>,
}

/// Request-side state: the caller's body, cut into messages one at a time.
struct Outgoing {
    body: Option<ClientBody>,
    leftover: Bytes,
    assembler: EnvelopeAssembler,
    unary: BytesMut,
}

/// Response-side state for the messages after `M`.
#[derive(Default)]
struct Received {
    headers: Option<HeaderMap>,
    unary_message: Option<Bytes>,
    /// A unary response carried more than one message.
    extra_message: bool,
}

impl Session {
    async fn run(mut self, call: Call, metadata: Bytes, body: ClientBody, head: HeadSender) {
        let mut delivery = Delivery {
            head: Some(head),
            body: None,
            pending: None,
            verdict: None,
        };
        let end = self.drive(&call, metadata, body, &mut delivery).await;
        self.finish(end, delivery).await;
    }

    async fn drive(
        &mut self,
        call: &Call,
        metadata: Bytes,
        body: ClientBody,
        delivery: &mut Delivery,
    ) -> End {
        self.outbound
            .push(wire::encode_json(Marker::Metadata, &metadata));
        let mut request = Outgoing {
            body: Some(body),
            leftover: Bytes::new(),
            assembler: EnvelopeAssembler::new(usize::MAX),
            unary: BytesMut::new(),
        };
        let mut received = Received::default();
        let mut expiry = std::pin::pin!(tokio::time::sleep_until(
            call.deadline.unwrap_or_else(tokio::time::Instant::now)
        ));
        // After a failed write the server may still have sent `S` before it
        // closed, so keep reading until the stream ends.
        let mut write_failed = false;

        loop {
            if let Err(end) = self.pump_request(call, &mut request) {
                return end;
            }
            let can_read_request =
                !write_failed && self.outbound.is_empty() && request.leftover.is_empty();
            tokio::select! {
                biased;
                // Ahead of `caller_gone`: a caller that drops the stream on
                // the error still has the server told why.
                reason = async { delivery.verdict.as_mut()?.await.ok() },
                    if delivery.verdict.is_some() =>
                {
                    delivery.verdict = None;
                    if let Some(reason) = reason {
                        return End::Undecodable(reason);
                    }
                }
                () = caller_gone(delivery.head.as_mut(), delivery.body.as_ref()) => {
                    return End::Abandoned;
                }
                () = &mut expiry, if call.deadline.is_some() => {
                    return End::Fail(
                        ConnectError::deadline_exceeded("the RPC deadline expired"),
                        Some(close_code::NORMAL),
                    );
                }
                written = std::future::poll_fn(|cx| self.outbound.poll_write(&mut self.sink, cx)),
                    if !write_failed && self.outbound.is_busy() =>
                {
                    if written.is_err() {
                        write_failed = true;
                        self.outbound.clear();
                        request.body = None;
                        request.leftover = Bytes::new();
                    }
                }
                permit = wire::reserve(delivery.body.as_ref()), if delivery.pending.is_some() => {
                    match (permit, delivery.pending.take()) {
                        (Some(permit), Some(item)) => {
                            permit.send(Ok(item));
                        }
                        _ => return End::Abandoned,
                    }
                }
                message = self.stream.next(), if delivery.pending.is_none() => {
                    match self.on_message(call, &mut received, delivery, message) {
                        Ok(false) => {}
                        Ok(true) => return End::Finished,
                        Err(end) => return end,
                    }
                }
                frame = next_frame(&mut request.body), if can_read_request => {
                    if let Err(end) = self.on_request_frame(call, &mut request, frame) {
                        return end;
                    }
                }
            }
        }
    }

    /// Take what the server sent. `Ok(true)` once its `S` has been handed on.
    fn on_message(
        &mut self,
        call: &Call,
        received: &mut Received,
        delivery: &mut Delivery,
        message: Option<Result<Message, WsError>>,
    ) -> Result<bool, End> {
        let message = match message {
            Some(Ok(Message::Close(frame))) => return Err(connection_lost(frame)),
            Some(Ok(message)) => message,
            Some(Err(err)) => {
                self.read_failed = true;
                return Err(self.read_error(err));
            }
            None => return Err(connection_lost(None)),
        };
        match wire::classify(message, Side::Client, self.codec)? {
            Some(message) => receive(call, received, delivery, message),
            None => Ok(false),
        }
    }

    /// Take the next frame of the caller's request body.
    fn on_request_frame(
        &mut self,
        call: &Call,
        request: &mut Outgoing,
        frame: Option<Result<Frame<Bytes>, ConnectError>>,
    ) -> Result<(), End> {
        match frame {
            None => {
                request.body = None;
                if request.assembler.has_partial() {
                    return Err(internal(ConnectError::internal(
                        "the request body ended inside a message",
                    )));
                }
                self.end_request(call, &mut request.unary)
            }
            Some(Err(err)) => Err(End::Fail(err, Some(close_code::CLIENT_ERROR))),
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    if call.unary {
                        request.unary.extend_from_slice(&data);
                    } else {
                        request.leftover = data;
                    }
                }
                Ok(())
            }
        }
    }

    /// Queue at most one request message from the caller's body, so that a
    /// stalled write stops the caller rather than buffering its stream.
    fn pump_request(&mut self, call: &Call, request: &mut Outgoing) -> Result<(), End> {
        if !self.outbound.is_empty() || request.leftover.is_empty() {
            return Ok(());
        }
        let Some(envelope) = request
            .assembler
            .feed(&mut request.leftover)
            .map_err(internal)?
        else {
            return Ok(());
        };
        let payload = if envelope.is_compressed() {
            let encoding = call.encoding.as_deref().ok_or_else(|| {
                internal(ConnectError::internal(
                    "compressed request message without connect-content-encoding",
                ))
            })?;
            call.compression
                .decompress_with_limit(encoding, envelope.data, usize::MAX)
                .map_err(internal)?
        } else {
            envelope.data
        };
        let message = wire::encode_body(&payload, call.codec).map_err(internal)?;
        self.outbound.push(message);
        Ok(())
    }

    /// The request body ended: queue the unary message, then `C`.
    fn end_request(&mut self, call: &Call, unary: &mut BytesMut) -> Result<(), End> {
        if call.unary {
            let mut payload = std::mem::take(unary).freeze();
            if let Some(encoding) = &call.encoding {
                payload = call
                    .compression
                    .decompress_with_limit(encoding, payload, usize::MAX)
                    .map_err(internal)?;
            }
            let message = wire::encode_body(&payload, call.codec).map_err(internal)?;
            self.outbound.push(message);
        }
        self.outbound
            .push(wire::encode_json(Marker::ClientEnd, b""));
        Ok(())
    }

    fn read_error(&self, err: WsError) -> End {
        match err {
            WsError::Capacity(_) => End::from(WireError::new(
                Fault::TooLarge,
                format!(
                    "response message exceeds the {} byte limit",
                    self.max_message_size
                ),
            )),
            WsError::Utf8(_) => End::from(WireError::new(
                Fault::Encoding,
                "a text message is not valid UTF-8",
            )),
            WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => connection_lost(None),
            WsError::Protocol(err) => End::from(WireError::protocol(format!(
                "WebSocket protocol violation: {err}"
            ))),
            WsError::Io(err) => End::Fail(
                ConnectError::unavailable_from_transport(err, "WebSocket read failed"),
                None,
            ),
            other => End::Fail(
                ConnectError::unavailable(format!("WebSocket read failed: {other}")),
                None,
            ),
        }
    }

    /// Close, then deliver the outcome. Closing is bounded, so a server that
    /// stopped reading cannot keep the connection open.
    async fn finish(mut self, end: End, mut delivery: Delivery) {
        // Request messages not yet handed to the socket are moot once the
        // RPC is over.
        self.outbound.clear();
        // Only a fault the server caused is explained to it; a local
        // failure's message may hold the caller's own data.
        let (error, close) = match end {
            End::Finished => (None, Some(close_after_end(&mut delivery).await)),
            End::Fail(err, close) => {
                let reason = if close == Some(close_code::NORMAL) {
                    ""
                } else {
                    "client error"
                };
                (Some(err), close.map(|code| (code, reason.to_owned())))
            }
            End::Fault(err, code) => {
                let reason = err.message.clone().unwrap_or_default();
                (Some(err), Some((code, reason)))
            }
            End::Undecodable(reason) => {
                // The caller has its error and will read nothing more.
                delivery.body = None;
                (None, Some((close_code::CLIENT_ERROR, reason)))
            }
            End::Abandoned => (None, Some((close_code::NORMAL, String::new()))),
        };
        tracing::debug!(
            close_code = close.as_ref().map(|(code, _)| *code),
            "WebSocket RPC ended"
        );
        // The caller hears the outcome while the connection closes.
        tokio::join!(self.close(close), deliver(&mut delivery, error));
    }

    /// Send `close` and linger for the server's, bounded in time and bytes.
    /// With none, the server closed first, and only tungstenite's reply to
    /// its Close, which a flush sends, is still owed.
    async fn close(self, close: Option<(u16, String)>) {
        let Self {
            mut sink,
            stream,
            mut outbound,
            read_failed,
            max_message_size,
            ..
        } = self;
        let Some((code, reason)) = close else {
            let _ = tokio::time::timeout(crate::service::DRAIN_TIMEOUT, sink.flush()).await;
            return;
        };
        outbound.push(Message::Close(Some(wire::close_frame(code, &reason))));
        let written =
            tokio::time::timeout(crate::service::DRAIN_TIMEOUT, outbound.write(&mut sink)).await;
        if matches!(written, Ok(Ok(()))) {
            wire::drain(sink, stream, read_failed, max_message_size).await;
        }
    }
}

/// The close for an RPC whose `S` arrived: 1000, unless the call path could
/// not decode the response message, which is a protocol error (§6.1).
async fn close_after_end(delivery: &mut Delivery) -> (u16, String) {
    let normal = (close_code::NORMAL, String::new());
    let Some(mut verdict) = delivery.verdict.take() else {
        return normal;
    };
    let reason = match verdict.try_recv() {
        Ok(reason) => Some(reason),
        Err(TryRecvError::Closed) => None,
        Err(TryRecvError::Empty) => {
            // A client-stream caller decodes only once its body has ended.
            let _ =
                tokio::time::timeout(crate::service::DRAIN_TIMEOUT, deliver(delivery, None)).await;
            tokio::time::timeout(crate::service::DRAIN_TIMEOUT, verdict)
                .await
                .ok()
                .and_then(Result::ok)
        }
    };
    reason.map_or(normal, |reason| (close_code::CLIENT_ERROR, reason))
}

/// Hand the caller what is still queued, then the error, if any: through
/// the head while the caller still awaits it, through the body after. The
/// connection is closed by now, so this waits for as long as the caller
/// holds the stream.
async fn deliver(delivery: &mut Delivery, error: Option<ConnectError>) {
    if let Some(head) = delivery.head.take() {
        if let Some(error) = error {
            let _ = head.send(Err(error));
        }
        return;
    }
    let Some(body) = delivery.body.as_ref() else {
        return;
    };
    // Cancel-safe: `verdict` bounds this, and an item not yet sent stays
    // queued for the unbounded call in `finish`.
    let pending = &mut delivery.pending;
    let send_all = async {
        if pending.is_some() {
            let permit = body.reserve().await.ok()?;
            permit.send(Ok(pending.take()?));
        }
        if let Some(error) = error {
            body.send(Err(error)).await.ok()?;
        }
        Some(())
    };
    tokio::select! {
        _ = send_all => {}
        () = body.closed() => {}
    }
    delivery.body = None;
}

/// Take one message from the server. `Ok(true)` once `S` has been handed on.
fn receive(
    call: &Call,
    received: &mut Received,
    delivery: &mut Delivery,
    message: wire::WireMessage,
) -> Result<bool, End> {
    let Some(headers) = &received.headers else {
        if message.marker != Marker::Metadata {
            return Err(WireError::protocol(
                "the first message must be a Leading-Metadata ('M') message",
            )
            .into());
        }
        let headers = metadata_headers(&message.payload)?;
        if !call.unary {
            let (tx, rx) = mpsc::channel(RESPONSE_QUEUE);
            let mut response = streaming_head(&headers, call.codec, rx);
            watch_decoding(&mut response, delivery);
            let delivered = delivery
                .head
                .take()
                .is_some_and(|head| head.send(Ok(response)).is_ok());
            if !delivered {
                return Err(End::Abandoned);
            }
            delivery.body = Some(tx);
        }
        received.headers = Some(headers);
        return Ok(false);
    };
    match message.marker {
        Marker::Metadata => {
            Err(WireError::protocol("a second Leading-Metadata ('M') message").into())
        }
        Marker::Body if call.unary => {
            if received.unary_message.is_some() {
                received.extra_message = true;
            } else {
                received.unary_message = Some(message.payload);
            }
            Ok(false)
        }
        Marker::Body => {
            forward(delivery, flags::DATA, &message.payload)?;
            Ok(false)
        }
        Marker::ServerEnd if call.unary => {
            let end = parse_end_stream(&message.payload)?;
            let mut response = if received.extra_message && end.error.is_none() {
                Err(ConnectError::unimplemented(
                    "the server sent more than one message for a unary RPC",
                ))
            } else {
                unary_response(
                    headers.clone(),
                    end,
                    received.unary_message.take(),
                    call.codec,
                )
            };
            if let Ok(response) = &mut response {
                watch_decoding(response, delivery);
            }
            if let Some(head) = delivery.head.take() {
                let _ = head.send(response);
            }
            Ok(true)
        }
        Marker::ServerEnd => {
            parse_end_stream(&message.payload)?;
            forward(delivery, flags::END_STREAM, &message.payload)?;
            Ok(true)
        }
        Marker::ClientEnd => Err(WireError::protocol("'C' is not valid from a server").into()),
    }
}

fn parse_end_stream(payload: &[u8]) -> Result<crate::client::ClientEndStreamResponse, WireError> {
    crate::client::parse_connect_end_stream(payload).map_err(|error| WireError {
        fault: Fault::Protocol,
        error,
    })
}

/// Queue a streaming response's message for the caller as an envelope.
fn forward(delivery: &mut Delivery, flag: u8, payload: &[u8]) -> Result<(), End> {
    let item = wire::envelope(flag, payload).map_err(internal)?;
    delivery.pending = wire::offer(delivery.body.as_ref(), Ok(item)).and_then(Result::ok);
    Ok(())
}

/// Let the call path report whether it could decode `response`.
fn watch_decoding<B>(response: &mut Response<B>, delivery: &mut Delivery) {
    let (report, verdict) = DecodeReport::new();
    response.extensions_mut().insert(report);
    delivery.verdict = Some(verdict);
}

fn internal(err: ConnectError) -> End {
    End::Fail(err, Some(close_code::CLIENT_ERROR))
}

/// The connection ended without the server's `S` (§13.1), which always
/// fails the RPC: `internal` for invalid data (1007), `resource_exhausted`
/// for an oversized message (1009), and `unavailable` otherwise.
fn connection_lost(frame: Option<CloseFrame>) -> End {
    let code = match frame.as_ref().map(|frame| u16::from(frame.code)) {
        Some(close_code::INVALID_PAYLOAD) => ErrorCode::Internal,
        Some(close_code::MESSAGE_TOO_BIG) => ErrorCode::ResourceExhausted,
        _ => ErrorCode::Unavailable,
    };
    let message = match frame {
        Some(frame) if frame.reason.is_empty() => format!(
            "the WebSocket closed with code {} before the end of the response",
            u16::from(frame.code)
        ),
        Some(frame) => format!(
            "the WebSocket closed with code {} ({}) before the end of the response",
            u16::from(frame.code),
            frame.reason
        ),
        None => "the WebSocket connection ended before the end of the response".to_owned(),
    };
    End::Fail(ConnectError::new(code, message), None)
}

fn metadata_headers(payload: &[u8]) -> Result<HeaderMap, WireError> {
    let mut headers = HeaderMap::new();
    wire::append_metadata(&mut headers, decode_metadata(payload)?);
    Ok(headers)
}

fn streaming_head(
    headers: &HeaderMap,
    codec: CodecFormat,
    rx: mpsc::Receiver<Result<Bytes, ConnectError>>,
) -> Response<WebSocketBody> {
    let mut response = Response::new(WebSocketBody {
        inner: BodyInner::Stream(rx),
    });
    *response.headers_mut() = headers.clone();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(Protocol::Connect.response_content_type(codec, true)),
    );
    response
}

/// Rebuild what a Connect unary response would have been: `S`'s metadata
/// as `trailer-` headers on success, or the error itself, which the call
/// path surfaces verbatim.
fn unary_response(
    headers: HeaderMap,
    end: crate::client::ClientEndStreamResponse,
    message: Option<Bytes>,
    codec: CodecFormat,
) -> Result<Response<WebSocketBody>, ConnectError> {
    let mut trailers = HeaderMap::new();
    if let Some(metadata) = end.metadata {
        crate::client::append_metadata_capped(&mut trailers, metadata);
    }
    if let Some(err) = end.error {
        return Err(crate::client::end_stream_error_to_connect_error(err)
            .with_headers(headers)
            .with_trailers(trailers));
    }
    let message = message.ok_or_else(|| {
        ConnectError::unimplemented("the server ended a unary RPC without a response message")
    })?;
    let mut response = Response::new(WebSocketBody {
        inner: BodyInner::Full(Some(message)),
    });
    *response.status_mut() = StatusCode::OK;
    *response.headers_mut() = headers;
    for (name, value) in &trailers {
        if let Ok(name) = HeaderName::from_bytes(format!("trailer-{name}").as_bytes()) {
            response.headers_mut().append(name, value.clone());
        }
    }
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(codec.content_type()),
    );
    Ok(response)
}

async fn next_frame(body: &mut Option<ClientBody>) -> Option<Result<Frame<Bytes>, ConnectError>> {
    match body {
        Some(body) => body.frame().await,
        None => std::future::pending().await,
    }
}

/// Resolves once nobody is waiting for the RPC's response any more.
async fn caller_gone(head: Option<&mut HeadSender>, body: Option<&BodySender>) {
    if let Some(head) = head {
        head.closed().await;
    } else if let Some(body) = body {
        body.closed().await;
    } else {
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_uri_maps_scheme_and_timeout() {
        let uri: Uri = "http://localhost:8080/api/pkg.Svc/Method".parse().unwrap();
        assert_eq!(
            websocket_uri(&uri, None).unwrap(),
            "ws://localhost:8080/api/pkg.Svc/Method"
        );
        assert_eq!(
            websocket_uri(&uri, Some("1500")).unwrap(),
            "ws://localhost:8080/api/pkg.Svc/Method?connect-timeout-ms=1500"
        );
        let uri: Uri = "https://example.com/pkg.Svc/Method".parse().unwrap();
        assert_eq!(
            websocket_uri(&uri, None).unwrap(),
            "wss://example.com/pkg.Svc/Method"
        );
        let uri: Uri = "ftp://example.com/pkg.Svc/Method".parse().unwrap();
        assert!(websocket_uri(&uri, None).is_err());
    }

    #[test]
    fn connection_lost_maps_the_close_code() {
        let lost = |code: u16| match connection_lost(Some(wire::close_frame(code, ""))) {
            End::Fail(err, None) => err.code,
            _ => panic!("connection_lost must fail the RPC without closing"),
        };
        assert_eq!(lost(close_code::INVALID_PAYLOAD), ErrorCode::Internal);
        assert_eq!(
            lost(close_code::MESSAGE_TOO_BIG),
            ErrorCode::ResourceExhausted
        );
        for code in [1000, 1001, 1002, 1011] {
            assert_eq!(lost(code), ErrorCode::Unavailable, "{code}");
        }
    }

    #[tokio::test]
    async fn verdict_closes_with_the_decode_failure() {
        let (report, rx) = DecodeReport::new();
        let mut delivery = Delivery {
            head: None,
            body: None,
            pending: None,
            verdict: Some(rx),
        };
        report.fail(&ConnectError::internal("bad bytes"));
        assert_eq!(
            close_after_end(&mut delivery).await,
            (close_code::CLIENT_ERROR, "bad bytes".to_owned())
        );

        let (report, rx) = DecodeReport::new();
        delivery.verdict = Some(rx);
        drop(report);
        assert_eq!(
            close_after_end(&mut delivery).await,
            (close_code::NORMAL, String::new())
        );

        delivery.verdict = None;
        assert_eq!(
            close_after_end(&mut delivery).await,
            (close_code::NORMAL, String::new())
        );

        // A caller that decodes only after the body ends reports late.
        let (tx, mut rx) = mpsc::channel(1);
        let (report, verdict_rx) = DecodeReport::new();
        delivery.body = Some(tx);
        delivery.pending = Some(Bytes::from_static(b"end"));
        delivery.verdict = Some(verdict_rx);
        let caller = tokio::spawn(async move {
            while rx.recv().await.is_some() {}
            report.fail(&ConnectError::internal("late"));
        });
        assert_eq!(
            close_after_end(&mut delivery).await,
            (close_code::CLIENT_ERROR, "late".to_owned())
        );
        caller.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn verdict_timeout_keeps_the_undelivered_item() {
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(Ok(Bytes::from_static(b"queued"))).unwrap();
        let (_report, verdict_rx) = DecodeReport::new();
        let mut delivery = Delivery {
            head: None,
            body: Some(tx),
            pending: Some(Bytes::from_static(b"end")),
            verdict: Some(verdict_rx),
        };
        assert_eq!(
            close_after_end(&mut delivery).await,
            (close_code::NORMAL, String::new())
        );
        assert!(delivery.body.is_some());
        assert_eq!(delivery.pending.as_deref(), Some(&b"end"[..]));
    }

    #[test]
    fn unary_response_maps_success_and_error() {
        let mut headers = HeaderMap::new();
        headers.insert("x-head", HeaderValue::from_static("1"));
        let end = |json: &[u8]| crate::client::parse_connect_end_stream(json).unwrap();
        let response = unary_response(
            headers.clone(),
            end(br#"{"metadata":{"x-tail":["2"]}}"#),
            Some(Bytes::from_static(b"\x08\x01")),
            CodecFormat::Proto,
        )
        .unwrap();
        assert_eq!(response.headers()["x-head"], "1");
        assert_eq!(response.headers()["trailer-x-tail"], "2");
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/proto"
        );

        let err = unary_response(
            headers.clone(),
            end(br#"{"error":{"code":"not_found","message":"gone"},"metadata":{"x-tail":["2"]}}"#),
            None,
            CodecFormat::Proto,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.response_headers()["x-head"], "1");
        assert_eq!(err.trailers()["x-tail"], "2");

        let err = unary_response(headers, end(b"{}"), None, CodecFormat::Proto).unwrap_err();
        assert_eq!(err.code, ErrorCode::Unimplemented);
    }

    #[test]
    fn request_headers_skip_protocol_names() {
        for name in [
            "content-type",
            "connect-timeout-ms",
            "accept-encoding",
            "host",
        ] {
            assert!(
                skip_request_header(&HeaderName::from_static(name)),
                "{name}"
            );
        }
        assert!(!skip_request_header(&HeaderName::from_static(
            "authorization"
        )));
    }
}
