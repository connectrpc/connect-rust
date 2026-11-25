//! End-to-end tests: the built-in server with WebSocket enabled, driven by a
//! raw tungstenite client for the wire-level rules. The client transport's
//! tests are in `transport`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use buffa_types::google::protobuf::StringValue;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use super::{WebSocketConfig, WebSocketRpc};
use crate::handler::{
    bidi_streaming_handler_fn, client_streaming_handler_fn, handler_fn, streaming_handler_fn,
};
use crate::{ConnectError, ConnectRpcService, RequestContext, Response, Router};

#[cfg(feature = "client")]
mod transport;

const SVC: &str = "test.v1.EchoService";

/// Set by the `Hang` handler's guard when the handler is dropped.
type Flag = Arc<AtomicBool>;

struct DropFlag(Flag);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn router(hang_dropped: Flag) -> Router {
    let flood_dropped = Arc::clone(&hang_dropped);
    let mirror_dropped = Arc::clone(&hang_dropped);
    Router::new()
        .route(
            SVC,
            "Echo",
            handler_fn(|ctx: RequestContext, req: StringValue| async move {
                if req.value == "fail" {
                    return Err(ConnectError::not_found("no such thing")
                        .with_trailers(trailer("x-why", "missing")));
                }
                if req.value == "connect-header" {
                    return Ok(Response::new(req)
                        .with_header("x-ok", "1")
                        .with_header("connect-custom", "1"));
                }
                if req.value == "connect-trailer" {
                    return Ok(Response::new(req)
                        .with_trailer("x-ok", "1")
                        .with_trailer("connect-custom", "1"));
                }
                let via = if ctx.extensions().get::<WebSocketRpc>().is_some() {
                    "ws"
                } else {
                    "http"
                };
                let tenant = ctx
                    .header("acme-tenant")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned();
                Ok(
                    Response::new(StringValue::from(format!("{via}:{tenant}:{}", req.value)))
                        .with_header("x-head", "h")
                        .with_trailer("x-tail", "t"),
                )
            }),
        )
        .route_server_stream(
            SVC,
            "Count",
            streaming_handler_fn(|_ctx: RequestContext, req: StringValue| async move {
                let n: usize = req.value.parse().unwrap_or(0);
                Response::stream_ok(futures::stream::iter(
                    (0..n).map(|i| Ok(StringValue::from(i.to_string()))),
                ))
            }),
        )
        .route_client_stream(
            SVC,
            "Join",
            client_streaming_handler_fn(
                |_ctx: RequestContext, mut requests: crate::ServiceStream<StringValue>| async move {
                    let mut joined = Vec::new();
                    while let Some(req) = requests.next().await {
                        joined.push(req?.value);
                    }
                    Response::ok(StringValue::from(joined.join(",")))
                },
            ),
        )
        .route_bidi_stream(
            SVC,
            "Chat",
            bidi_streaming_handler_fn(
                |_ctx: RequestContext, requests: crate::ServiceStream<StringValue>| async move {
                    Response::stream_ok(
                        requests.map(|req| {
                            req.map(|req| StringValue::from(format!("echo:{}", req.value)))
                        }),
                    )
                },
            ),
        )
        .route_client_stream(
            SVC,
            "Lenient",
            client_streaming_handler_fn(
                |_ctx: RequestContext, requests: crate::ServiceStream<StringValue>| async move {
                    let kept = requests.filter_map(|req| async { req.ok() }).count().await;
                    Response::ok(StringValue::from(kept.to_string()))
                },
            ),
        )
        .route_bidi_stream(
            SVC,
            "Forgive",
            bidi_streaming_handler_fn(
                |_ctx: RequestContext, requests: crate::ServiceStream<StringValue>| async move {
                    Response::stream_ok(
                        requests
                            .map(|req| Ok(req.unwrap_or_else(|_| StringValue::from("skipped")))),
                    )
                },
            ),
        )
        .route_bidi_stream(
            SVC,
            "Hang",
            bidi_streaming_handler_fn(
                move |_ctx: RequestContext, _requests: crate::ServiceStream<StringValue>| {
                    let guard = DropFlag(Arc::clone(&hang_dropped));
                    async move {
                        let _guard = guard;
                        std::future::pending::<()>().await;
                        Response::stream_ok(futures::stream::empty::<
                            Result<StringValue, ConnectError>,
                        >())
                    }
                },
            ),
        )
        .route_bidi_stream(
            SVC,
            "Mirror",
            bidi_streaming_handler_fn(
                move |_ctx: RequestContext, requests: crate::ServiceStream<StringValue>| {
                    let guard = DropFlag(Arc::clone(&mirror_dropped));
                    async move {
                        Response::stream_ok(requests.map(move |req| {
                            let _ = &guard;
                            req
                        }))
                    }
                },
            ),
        )
        .route_server_stream(
            SVC,
            "Flood",
            streaming_handler_fn(move |_ctx: RequestContext, _req: StringValue| {
                let guard = DropFlag(Arc::clone(&flood_dropped));
                async move {
                    let big = "z".repeat(256 * 1024);
                    Response::stream_ok(futures::stream::repeat_with(move || {
                        let _ = &guard;
                        Ok(StringValue::from(big.as_str()))
                    }))
                }
            }),
        )
        .route(
            SVC,
            "Panic",
            handler_fn(|_ctx: RequestContext, _req: StringValue| async move {
                if true {
                    panic!("handler panic in a test");
                }
                Response::ok(StringValue::default())
            }),
        )
        .route_client_stream(
            SVC,
            "Spawn",
            client_streaming_handler_fn(
                |_ctx: RequestContext, mut requests: crate::ServiceStream<StringValue>| async move {
                    tokio::spawn(async move {
                        let mut tag = None;
                        let outcome = loop {
                            match requests.next().await {
                                Some(Ok(req)) => {
                                    tag.get_or_insert(req.value);
                                }
                                Some(Err(err)) => break Err(err.code),
                                None => break Ok(()),
                            }
                        };
                        SPAWNED
                            .lock()
                            .unwrap()
                            .push((tag.unwrap_or_default(), outcome));
                    });
                    std::future::pending::<()>().await;
                    Response::ok(StringValue::default())
                },
            ),
        )
        .route(
            SVC,
            "Keep",
            handler_fn(|ctx: RequestContext, req: StringValue| async move {
                KEPT.lock().unwrap().push(ctx.extensions().clone());
                Response::ok(req)
            }),
        )
}

/// What `Spawn`'s detached readers saw at the end of their request streams,
/// by the first message's value.
type SpawnedOutcome = (String, Result<(), crate::ErrorCode>);
static SPAWNED: std::sync::Mutex<Vec<SpawnedOutcome>> = std::sync::Mutex::new(Vec::new());

/// Request extensions a handler keeps past its RPC.
static KEPT: std::sync::Mutex<Vec<http::Extensions>> = std::sync::Mutex::new(Vec::new());

fn trailer(name: &'static str, value: &'static str) -> http::HeaderMap {
    let mut map = http::HeaderMap::new();
    map.insert(name, http::HeaderValue::from_static(value));
    map
}

async fn serve(config: WebSocketConfig) -> (SocketAddr, Flag) {
    let hang_dropped = Flag::default();
    let service = ConnectRpcService::new(router(Arc::clone(&hang_dropped))).with_websocket(config);
    let bound = crate::Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    tokio::spawn(bound.serve_with_service(service));
    (addr, hang_dropped)
}

async fn raw_http_status(addr: SocketAddr, path: &str) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/proto\r\n\
         Content-Length: 0\r\nConnection: close\r\n\r\n"
    );
    tcp.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tcp.read_to_end(&mut response).await.unwrap();
    let line = String::from_utf8_lossy(&response);
    line.split(' ').nth(1).unwrap().parse().unwrap()
}

type Raw = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

async fn raw_connect(
    addr: SocketAddr,
    path: &str,
    token: &str,
    origin: Option<&str>,
) -> Result<Raw, WsError> {
    let origin = origin.map(|origin| ("origin", origin));
    raw_connect_with(addr, path, token, origin.as_slice()).await
}

async fn raw_connect_with(
    addr: SocketAddr,
    path: &str,
    token: &str,
    headers: &[(&'static str, &str)],
) -> Result<Raw, WsError> {
    let mut request = format!("ws://{addr}{path}").into_client_request().unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", token.parse().unwrap());
    for (name, value) in headers {
        request.headers_mut().insert(*name, value.parse().unwrap());
    }
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tokio_tungstenite::client_async(request, tcp)
        .await
        .map(|(ws, _)| ws)
}

/// Read until the close frame, returning the data messages and close code.
async fn read_all(ws: &mut Raw) -> (Vec<Message>, Option<u16>) {
    let mut messages = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("server went quiet")
        {
            Some(Ok(Message::Close(frame))) => {
                return (messages, frame.map(|f| u16::from(f.code)));
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            Some(Ok(message)) => messages.push(message),
            Some(Err(_)) | None => return (messages, None),
        }
    }
}

fn text(s: &str) -> Message {
    Message::Text(s.into())
}

fn end_stream(message: &Message) -> serde_json::Value {
    let Message::Text(text) = message else {
        panic!("S must be text: {message:?}");
    };
    let json = text.as_str().strip_prefix('S').expect("an S message");
    serde_json::from_str(json).unwrap()
}

#[tokio::test]
async fn worked_exchange_under_json() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let mut ws = raw_connect(addr, "/test.v1.EchoService/Echo", "connectrpc.1+json", None)
        .await
        .unwrap();
    ws.send(text("M[]")).await.unwrap();
    ws.send(text(r#"B"7""#)).await.unwrap();
    ws.send(text("C")).await.unwrap();
    let (messages, code) = read_all(&mut ws).await;
    assert_eq!(code, Some(1000));
    assert_eq!(messages.len(), 3, "{messages:?}");
    let Message::Text(m) = &messages[0] else {
        panic!()
    };
    let m: Vec<Vec<String>> = serde_json::from_str(m.as_str().strip_prefix('M').unwrap()).unwrap();
    assert!(
        m.contains(&vec!["x-head".to_owned(), "h".to_owned()]),
        "{m:?}"
    );
    assert!(m.iter().all(|entry| entry[0] != "content-type"), "{m:?}");
    assert_eq!(messages[1], text(r#"B"ws::7""#));
    assert_eq!(
        end_stream(&messages[2]),
        serde_json::json!({"metadata": {"x-tail": ["t"]}})
    );
}

#[tokio::test]
async fn worked_exchange_under_proto_mixes_frame_types() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let mut ws = raw_connect(
        addr,
        "/test.v1.EchoService/Echo",
        "connectrpc.1+proto",
        None,
    )
    .await
    .unwrap();
    ws.send(text("M[]")).await.unwrap();
    // StringValue{value: "7"} is field 1, length 1.
    ws.send(Message::Binary(Bytes::from_static(b"B\x0a\x017")))
        .await
        .unwrap();
    ws.send(text("C")).await.unwrap();
    let (messages, code) = read_all(&mut ws).await;
    assert_eq!(code, Some(1000));
    assert!(matches!(&messages[0], Message::Text(_)));
    assert_eq!(
        messages[1],
        Message::Binary(Bytes::from_static(b"B\x0a\x05ws::7"))
    );
    assert!(matches!(&messages[2], Message::Text(_)));
}

/// Send `messages` to `Chat` and expect the server to end the RPC with
/// `code` and close with `close`.
async fn expect_fault(token: &str, messages: Vec<Message>, code: &str, close: u16) {
    expect_fault_at("Chat", token, &[], messages, code, close).await;
}

async fn expect_fault_at(
    method: &str,
    token: &str,
    headers: &[(&'static str, &str)],
    messages: Vec<Message>,
    code: &str,
    close: u16,
) {
    let (addr, _) = serve(WebSocketConfig::new().with_keep_alive(None)).await;
    let mut ws = raw_connect_with(addr, &format!("/{SVC}/{method}"), token, headers)
        .await
        .unwrap();
    for message in messages {
        if ws.send(message).await.is_err() {
            break;
        }
    }
    let (received, close_code) = read_all(&mut ws).await;
    let last = received.last().expect("an S message");
    assert_eq!(end_stream(last)["error"]["code"], code, "{received:?}");
    assert!(
        matches!(&received[0], Message::Text(t) if t.as_str().starts_with('M')),
        "M must precede S: {received:?}"
    );
    assert_eq!(close_code, Some(close));
}

#[tokio::test]
async fn protocol_errors_end_the_stream() {
    let json = "connectrpc.1+json";
    let proto = "connectrpc.1+proto";
    let cases: &[(&str, Vec<Message>)] = &[
        (json, vec![text(r#"B"x""#)]),
        (json, vec![text("M[]"), text("X{}")]),
        (json, vec![text("M[]"), text("S{}")]),
        (json, vec![text("M[]"), text("M[]")]),
        (proto, vec![text("M[]"), text(r#"B"x""#)]),
        (json, vec![text("M[]"), text("C"), text(r#"B"x""#)]),
        (json, vec![text("M[]"), text("Cx")]),
        (json, vec![text(r#"M{"a":["1"]}"#)]),
        (json, vec![text(r#"M[["cookie","a=b"]]"#)]),
        (json, vec![text(r#"M[["x-forwarded-for","1.2.3.4"]]"#)]),
        (json, vec![text(r#"M[["connect-anything","1"]]"#)]),
        (json, vec![text(r#"M[["a","1"],["a","2"]]"#)]),
        (json, vec![text(r#"M[["A","1"]]"#)]),
        (json, vec![text(r#"M[["a"]]"#)]),
    ];
    for (token, messages) in cases {
        expect_fault(token, messages.clone(), "internal", 1002).await;
    }
    // A reason longer than a Close frame carries is cut, not dropped.
    let long_key = format!(r#"M[["x-forwarded-{}","a"]]"#, "e".repeat(200));
    expect_fault(json, vec![text(&long_key)], "internal", 1002).await;
}

#[tokio::test]
async fn metadata_may_not_repeat_a_handshake_header() {
    expect_fault_at(
        "Echo",
        "connectrpc.1+json",
        &[("acme-tenant", "hs")],
        vec![text(r#"M[["acme-tenant","m"]]"#)],
        "internal",
        1002,
    )
    .await;
}

#[tokio::test]
async fn effective_headers_join_handshake_and_metadata() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let mut ws = raw_connect_with(
        addr,
        "/test.v1.EchoService/Echo",
        "connectrpc.1+json",
        &[("acme-tenant", "hs")],
    )
    .await
    .unwrap();
    ws.send(text(r#"M[["x-other","1"]]"#)).await.unwrap();
    ws.send(text(r#"B"v""#)).await.unwrap();
    ws.send(text("C")).await.unwrap();
    let (received, code) = read_all(&mut ws).await;
    assert_eq!(received[1], text(r#"B"ws:hs:v""#));
    assert_eq!(code, Some(1000));
}

/// §7.4, §9: a server fails an RPC whose handler set a `connect-` header or
/// trailer with `internal`, leaves the key out, and closes with 1000.
#[tokio::test]
async fn a_connect_response_key_fails_the_rpc() {
    for (value, kind) in [("connect-header", "header"), ("connect-trailer", "trailer")] {
        let (addr, _) = serve(WebSocketConfig::new().with_keep_alive(None)).await;
        let mut ws = raw_connect_with(addr, &format!("/{SVC}/Echo"), "connectrpc.1+json", &[])
            .await
            .unwrap();
        for message in [text("M[]"), text(&format!("B\"{value}\"")), text("C")] {
            ws.send(message).await.unwrap();
        }
        let (received, close_code) = read_all(&mut ws).await;
        assert_eq!(close_code, Some(1000), "{received:?}");
        let Message::Text(metadata) = &received[0] else {
            panic!("M must be text: {received:?}");
        };
        let end = end_stream(received.last().expect("an S message"));
        assert_eq!(end["error"]["code"], "internal", "{received:?}");
        assert_eq!(
            end["error"]["message"],
            format!("response {kind} \"connect-custom\" is reserved for the protocol")
        );
        assert!(!metadata.as_str().contains("connect-"), "{received:?}");
        if kind == "header" {
            assert_eq!(metadata.as_str(), r#"M[["x-ok","1"]]"#);
            assert_eq!(received.len(), 2, "no B after a failed M: {received:?}");
        } else {
            assert_eq!(end["metadata"], serde_json::json!({"x-ok": ["1"]}));
        }
    }
}

#[tokio::test]
async fn an_undecodable_request_is_a_protocol_error() {
    for method in ["Echo", "Count", "Join", "Chat"] {
        expect_fault_at(
            method,
            "connectrpc.1+json",
            &[],
            vec![text("M[]"), text("B5"), text("C")],
            "invalid_argument",
            1002,
        )
        .await;
        // §6.1: an empty JSON payload goes to the codec, which rejects it.
        expect_fault_at(
            method,
            "connectrpc.1+json",
            &[],
            vec![text("M[]"), text("B"), text("C")],
            "invalid_argument",
            1002,
        )
        .await;
        expect_fault_at(
            method,
            "connectrpc.1+proto",
            &[],
            vec![
                text("M[]"),
                Message::Binary(Bytes::from_static(b"B\xff")),
                text("C"),
            ],
            "invalid_argument",
            1002,
        )
        .await;
    }
}

#[tokio::test]
async fn a_handler_cannot_swallow_an_undecodable_request() {
    for method in ["Lenient", "Forgive"] {
        expect_fault_at(
            method,
            "connectrpc.1+json",
            &[],
            vec![
                text("M[]"),
                text("B\"a\""),
                text("B5"),
                text("B\"b\""),
                text("C"),
            ],
            "invalid_argument",
            1002,
        )
        .await;
    }
}

/// An interceptor that decodes each request message itself.
struct Inspect;

#[crate::async_trait]
impl crate::Interceptor for Inspect {
    async fn intercept_streaming(
        &self,
        req: crate::interceptor::StreamRequest,
        inbound: crate::interceptor::PayloadStream,
        next: crate::interceptor::NextStream<'_>,
    ) -> Result<crate::interceptor::StreamResponse, ConnectError> {
        let inbound = inbound.map(|item| {
            item.and_then(|payload| {
                payload.message::<StringValue>()?;
                Ok(payload)
            })
        });
        next.run(req, Box::pin(inbound)).await
    }
}

#[tokio::test]
async fn an_interceptor_decode_failure_cannot_be_swallowed() {
    let service = ConnectRpcService::new(router(Flag::default()))
        .with_interceptor(Inspect)
        .with_websocket(WebSocketConfig::new().with_keep_alive(None));
    let bound = crate::Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    tokio::spawn(bound.serve_with_service(service));
    let mut ws = raw_connect_with(addr, &format!("/{SVC}/Lenient"), "connectrpc.1+json", &[])
        .await
        .unwrap();
    for message in [text("M[]"), text("B\"a\""), text("B5"), text("C")] {
        ws.send(message).await.unwrap();
    }
    let (received, close_code) = read_all(&mut ws).await;
    let last = received.last().expect("an S message");
    assert_eq!(
        end_stream(last)["error"]["code"],
        "invalid_argument",
        "{received:?}"
    );
    assert_eq!(close_code, Some(1002));
}

#[tokio::test]
async fn invalid_utf8_closes_with_1007_and_no_s() {
    use tokio_tungstenite::tungstenite::protocol::frame::Frame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::{Data, OpCode};

    let (addr, _) = serve(WebSocketConfig::new().with_keep_alive(None)).await;
    let mut ws = raw_connect(addr, "/test.v1.EchoService/Chat", "connectrpc.1+json", None)
        .await
        .unwrap();
    let invalid = Frame::message(&b"B\xff\xfe"[..], OpCode::Data(Data::Text), true);
    ws.send(text("M[]")).await.unwrap();
    ws.send(Message::Frame(invalid)).await.unwrap();
    let (received, code) = read_all(&mut ws).await;
    assert!(
        !received
            .iter()
            .any(|m| matches!(m, Message::Text(t) if t.as_str().starts_with('S'))),
        "{received:?}"
    );
    assert_eq!(code, Some(1007));
}

#[tokio::test]
async fn oversized_message_is_resource_exhausted() {
    let hang = Flag::default();
    let service = ConnectRpcService::new(router(hang))
        .with_limits(crate::Limits::default().with_max_message_size(16))
        .with_websocket(WebSocketConfig::new());
    let bound = crate::Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    tokio::spawn(bound.serve_with_service(service));
    let mut ws = raw_connect(addr, "/test.v1.EchoService/Chat", "connectrpc.1+json", None)
        .await
        .unwrap();
    ws.send(text("M[]")).await.unwrap();
    ws.send(text(&format!("B\"{}\"", "x".repeat(64))))
        .await
        .unwrap();
    let (received, code) = read_all(&mut ws).await;
    let s = end_stream(received.last().unwrap());
    assert_eq!(s["error"]["code"], "resource_exhausted");
    assert!(
        s["error"]["message"]
            .as_str()
            .unwrap()
            .contains("16 byte limit")
    );
    assert_eq!(code, Some(1009));
}

#[tokio::test]
async fn a_far_oversized_message_still_gets_its_close() {
    let service = ConnectRpcService::new(router(Flag::default()))
        .with_limits(crate::Limits::default().with_max_message_size(16))
        .with_websocket(WebSocketConfig::new());
    let bound = crate::Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    tokio::spawn(bound.serve_with_service(service));
    let ws = raw_connect(addr, "/test.v1.EchoService/Chat", "connectrpc.1+json", None)
        .await
        .unwrap();
    let (mut sink, mut stream) = ws.split();
    sink.send(text("M[]")).await.unwrap();
    // The server stops reading at the frame header and leaves the rest of
    // this unread; it must not reset the connection over it.
    let big = format!("B\"{}\"", "x".repeat(4 << 20));
    let sender = tokio::spawn(async move {
        let _ = sink.send(text(&big)).await;
        sink
    });
    let mut code = None;
    let mut saw_s = false;
    while let Ok(Some(message)) = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .map(|m| m.and_then(Result::ok))
    {
        match message {
            Message::Close(frame) => {
                code = frame.map(|f| u16::from(f.code));
                break;
            }
            Message::Text(ref t) if t.starts_with('S') => {
                saw_s = end_stream(&message)["error"]["code"] == "resource_exhausted";
            }
            _ => {}
        }
    }
    let _ = sender.await;
    assert!(saw_s, "no S");
    assert_eq!(code, Some(1009));
}

#[tokio::test]
async fn invalid_timeout_is_reported_in_band() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let mut ws = raw_connect(
        addr,
        "/test.v1.EchoService/Echo?connect-timeout-ms=soon",
        "connectrpc.1+json",
        None,
    )
    .await
    .unwrap();
    let (received, code) = read_all(&mut ws).await;
    assert_eq!(received.len(), 2, "{received:?}");
    assert_eq!(received[0], text("M[]"));
    assert_eq!(
        end_stream(&received[1])["error"]["code"],
        "invalid_argument"
    );
    assert_eq!(code, Some(1000));
}

#[tokio::test]
async fn an_unknown_procedure_is_reported_in_band() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let mut ws = raw_connect(addr, "/test.v1.EchoService/Nope", "connectrpc.1+json", None)
        .await
        .unwrap();
    let (received, code) = read_all(&mut ws).await;
    assert_eq!(received.len(), 2, "{received:?}");
    assert_eq!(received[0], text("M[]"));
    assert_eq!(end_stream(&received[1])["error"]["code"], "unimplemented");
    assert_eq!(code, Some(1000));
}

#[tokio::test]
async fn zero_timeout_is_deadline_exceeded_without_dispatch() {
    let (addr, hang_dropped) = serve(WebSocketConfig::new()).await;
    let mut ws = raw_connect(
        addr,
        "/test.v1.EchoService/Hang?connect-timeout-ms=0",
        "connectrpc.1+json",
        None,
    )
    .await
    .unwrap();
    let (received, code) = read_all(&mut ws).await;
    assert_eq!(received[0], text("M[]"));
    assert_eq!(
        end_stream(&received[1])["error"]["code"],
        "deadline_exceeded"
    );
    assert_eq!(code, Some(1000));
    assert!(!hang_dropped.load(Ordering::SeqCst), "the handler ran");
}

#[tokio::test]
async fn deadline_bounds_the_whole_rpc() {
    let (addr, hang_dropped) = serve(WebSocketConfig::new()).await;
    let mut ws = raw_connect(
        addr,
        "/test.v1.EchoService/Hang?connect-timeout-ms=100",
        "connectrpc.1+json",
        None,
    )
    .await
    .unwrap();
    ws.send(text("M[]")).await.unwrap();
    let (received, code) = read_all(&mut ws).await;
    assert_eq!(
        end_stream(received.last().unwrap())["error"]["code"],
        "deadline_exceeded"
    );
    assert_eq!(code, Some(1000));
    assert!(hang_dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn closing_before_s_cancels_the_handler() {
    let (addr, hang_dropped) = serve(WebSocketConfig::new()).await;
    let mut ws = raw_connect(addr, "/test.v1.EchoService/Hang", "connectrpc.1+json", None)
        .await
        .unwrap();
    ws.send(text("M[]")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!hang_dropped.load(Ordering::SeqCst));
    ws.close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
        code: CloseCode::Away,
        reason: "".into(),
    }))
    .await
    .unwrap();
    for _ in 0..100 {
        if hang_dropped.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the handler was not canceled");
}

#[tokio::test]
async fn a_dropped_connection_fails_a_detached_request_stream() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    for (tag, finish) in [("ended", true), ("dropped", false)] {
        let mut ws = raw_connect(
            addr,
            "/test.v1.EchoService/Spawn",
            "connectrpc.1+json",
            None,
        )
        .await
        .unwrap();
        ws.send(text("M[]")).await.unwrap();
        ws.send(text(&format!("B\"{tag}\""))).await.unwrap();
        if finish {
            ws.send(text("C")).await.unwrap();
        }
        // Let the handler start, so its reader is running when the client goes.
        tokio::time::sleep(Duration::from_millis(50)).await;
        ws.close(None).await.unwrap();
    }
    for _ in 0..200 {
        let seen = SPAWNED.lock().unwrap().clone();
        if seen.len() == 2 {
            let outcome = |tag: &str| seen.iter().find(|(t, _)| t == tag).map(|(_, o)| *o);
            assert_eq!(outcome("ended"), Some(Ok(())), "{seen:?}");
            assert!(matches!(outcome("dropped"), Some(Err(_))), "{seen:?}");
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "the detached readers did not finish: {:?}",
        SPAWNED.lock().unwrap()
    );
}

#[tokio::test]
async fn handshake_failures_are_http_statuses() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let status = |err: WsError| match err {
        WsError::Http(response) => response.status().as_u16(),
        other => panic!("expected an HTTP rejection, got {other}"),
    };
    let err = raw_connect(
        addr,
        "/test.v1.EchoService/Echo",
        "connectrpc.2+proto",
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(status(err), 400);
    let err = raw_connect(addr, "/test.v1.EchoService/Echo", "chat", None)
        .await
        .unwrap_err();
    assert_eq!(status(err), 400);
    let err = raw_connect(
        addr,
        "/test.v1.EchoService/Echo",
        "connectrpc.1+json",
        Some("https://evil.example"),
    )
    .await
    .unwrap_err();
    assert_eq!(status(err), 403);
    let same_origin = format!("https://{addr}");
    assert!(
        raw_connect(
            addr,
            "/test.v1.EchoService/Echo",
            "connectrpc.1+json",
            Some(&same_origin),
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn the_first_supported_token_is_echoed() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let mut request = format!("ws://{addr}/test.v1.EchoService/Echo")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "sec-websocket-protocol",
        "connectrpc.1+cbor, connectrpc.1+proto, connectrpc.1+json"
            .parse()
            .unwrap(),
    );
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (_, response) = tokio_tungstenite::client_async(request, tcp).await.unwrap();
    assert_eq!(
        response.headers()["sec-websocket-protocol"],
        "connectrpc.1+proto"
    );
}

#[tokio::test]
async fn keep_alive_hangs_up_on_a_silent_client() {
    let config = WebSocketConfig::new()
        .with_keep_alive(Some(Duration::from_millis(20)))
        .with_keep_alive_timeout(Duration::from_millis(60));
    let (addr, hang_dropped) = serve(config).await;

    // A client that keeps reading answers the Pings and stays connected.
    let mut alive = raw_connect(addr, "/test.v1.EchoService/Chat", "connectrpc.1+json", None)
        .await
        .unwrap();
    alive.send(text("M[]")).await.unwrap();
    let mut pings = 0;
    let quiet = tokio::time::timeout(Duration::from_millis(300), async {
        while let Some(Ok(message)) = alive.next().await {
            if matches!(message, Message::Ping(_)) {
                pings += 1;
            }
            if matches!(message, Message::Close(_)) {
                return;
            }
        }
    })
    .await;
    assert!(quiet.is_err(), "a responsive client was disconnected");
    assert!(pings >= 5, "only {pings} pings");

    // One that stops reading after its opening message is hung up on.
    let mut silent = raw_connect(addr, "/test.v1.EchoService/Hang", "connectrpc.1+json", None)
        .await
        .unwrap();
    silent.send(text("M[]")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (_, code) = read_all(&mut silent).await;
    assert_eq!(code, Some(1011));
    assert!(hang_dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn keep_alive_hangs_up_on_a_client_that_sends_but_never_reads() {
    let config = WebSocketConfig::new()
        .with_keep_alive(Some(Duration::from_millis(20)))
        .with_keep_alive_timeout(Duration::from_millis(60))
        .with_max_timeout(None);
    let (addr, mirror_dropped) = serve(config).await;
    let mut ws = raw_connect(
        addr,
        "/test.v1.EchoService/Mirror",
        "connectrpc.1+json",
        None,
    )
    .await
    .unwrap();
    ws.send(text("M[]")).await.unwrap();
    // The echoes fill the socket, so the handler stops taking requests and
    // the server stops reading; this client's sends then stall too.
    let big = format!("B\"{}\"", "y".repeat(64 * 1024));
    let sender = tokio::spawn(async move {
        for _ in 0..512 {
            if ws.send(text(&big)).await.is_err() {
                break;
            }
        }
        ws
    });
    for _ in 0..150 {
        if mirror_dropped.load(Ordering::SeqCst) {
            sender.abort();
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    sender.abort();
    panic!("a client that stopped reading held its session");
}

#[tokio::test]
async fn handler_dropping_its_request_stream_does_not_stall_the_session() {
    // The unary collector gives up past 64 bytes and drops the request
    // stream while the client is still sending.
    let service = ConnectRpcService::new(router(Flag::default()))
        .with_limits(crate::Limits::default().with_max_request_body_size(64))
        .with_websocket(WebSocketConfig::new());
    let bound = crate::Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    tokio::spawn(bound.serve_with_service(service));
    let mut ws = raw_connect(addr, "/test.v1.EchoService/Echo", "connectrpc.1+json", None)
        .await
        .unwrap();
    ws.send(text("M[]")).await.unwrap();
    for _ in 0..32 {
        if ws
            .send(text(&format!("B\"{}\"", "x".repeat(40))))
            .await
            .is_err()
        {
            break;
        }
    }
    let (received, _) = read_all(&mut ws).await;
    let s = end_stream(received.last().expect("an S message"));
    assert_eq!(s["error"]["code"], "resource_exhausted", "{s}");
}

#[tokio::test]
async fn opening_timeout_bounds_a_silent_client() {
    let config = WebSocketConfig::new().with_opening_timeout(Duration::from_millis(100));
    let (addr, _) = serve(config).await;
    let mut ws = raw_connect(addr, "/test.v1.EchoService/Chat", "connectrpc.1+json", None)
        .await
        .unwrap();
    let (received, code) = read_all(&mut ws).await;
    assert_eq!(received[0], text("M[]"));
    assert_eq!(
        end_stream(&received[1])["error"]["code"],
        "deadline_exceeded"
    );
    assert_eq!(code, Some(1000));
}

#[tokio::test]
async fn graceful_shutdown_ends_open_sessions() {
    let hang_dropped = Flag::default();
    let service = ConnectRpcService::new(router(Arc::clone(&hang_dropped)))
        .with_websocket(WebSocketConfig::new());
    let bound = crate::Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(bound.serve_with_service_and_shutdown(service, async {
        let _ = stop_rx.await;
    }));
    let mut ws = raw_connect(addr, "/test.v1.EchoService/Hang", "connectrpc.1+json", None)
        .await
        .unwrap();
    ws.send(text("M[]")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop_tx.send(()).unwrap();
    let (received, code) = read_all(&mut ws).await;
    assert_eq!(
        end_stream(received.last().unwrap())["error"]["code"],
        "unavailable"
    );
    assert_eq!(code, Some(1001));
    drop(ws);
    tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve waits for the session, then returns")
        .unwrap()
        .unwrap();
    assert!(hang_dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn path_prefix_routes_by_url() {
    let (addr, _) = serve(WebSocketConfig::new().with_path_prefix("ws")).await;
    let mut ws = raw_connect(
        addr,
        "/ws/test.v1.EchoService/Echo",
        "connectrpc.1+json",
        None,
    )
    .await
    .unwrap();
    ws.send(text("M[]")).await.unwrap();
    ws.send(text(r#"B"p""#)).await.unwrap();
    ws.send(text("C")).await.unwrap();
    let (received, code) = read_all(&mut ws).await;
    assert_eq!(received[1], text(r#"B"ws::p""#));
    assert_eq!(code, Some(1000));

    let err = raw_connect(addr, "/test.v1.EchoService/Echo", "connectrpc.1+json", None)
        .await
        .unwrap_err();
    assert!(matches!(err, WsError::Http(r) if r.status() == 400));
    assert_eq!(
        raw_http_status(addr, "/ws/test.v1.EchoService/Echo").await,
        426
    );
}

#[tokio::test]
async fn handler_panic_is_reported_in_band() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let mut ws = raw_connect(
        addr,
        "/test.v1.EchoService/Panic",
        "connectrpc.1+json",
        None,
    )
    .await
    .unwrap();
    ws.send(text("M[]")).await.unwrap();
    ws.send(text(r#"B"x""#)).await.unwrap();
    ws.send(text("C")).await.unwrap();
    let (received, code) = read_all(&mut ws).await;
    assert_eq!(
        end_stream(received.last().unwrap())["error"]["code"],
        "internal"
    );
    assert_eq!(code, Some(1011));
}

#[tokio::test]
async fn a_client_that_stops_reading_still_meets_its_deadline() {
    let (addr, flood_dropped) = serve(WebSocketConfig::new().with_keep_alive(None)).await;
    let mut ws = raw_connect(
        addr,
        "/test.v1.EchoService/Flood?connect-timeout-ms=200",
        "connectrpc.1+json",
        None,
    )
    .await
    .unwrap();
    ws.send(text("M[]")).await.unwrap();
    ws.send(text(r#"B"x""#)).await.unwrap();
    ws.send(text("C")).await.unwrap();
    // Never read: the server's writes stall once the socket buffers fill.
    for _ in 0..100 {
        if flood_dropped.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("a stalled write kept the handler past its deadline");
}

#[tokio::test]
async fn shutdown_ignores_extensions_a_handler_keeps() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let service =
        ConnectRpcService::new(router(Flag::default())).with_websocket(WebSocketConfig::new());
    let bound = crate::Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(bound.serve_with_service_and_shutdown(service, async {
        let _ = stop_rx.await;
    }));
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "POST /test.v1.EchoService/Keep HTTP/1.1\r\nHost: {addr}\r\n\
         Content-Type: application/json\r\nContent-Length: 3\r\n\
         Connection: close\r\n\r\n\"k\""
    );
    tcp.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tcp.read_to_end(&mut response).await.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200"));
    assert!(!KEPT.lock().unwrap().is_empty());
    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("shutdown waited on extensions a handler kept")
        .unwrap()
        .unwrap();
}
