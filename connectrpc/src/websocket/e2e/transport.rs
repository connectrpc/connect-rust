//! The client transport against the built-in server.

use std::net::SocketAddr;
use std::time::Duration;

use buffa_types::google::protobuf::__buffa::view::StringValueView;
use buffa_types::google::protobuf::StringValue;
use futures::{SinkExt, StreamExt};

use super::{raw_http_status, serve};
use crate::ErrorCode;
use crate::client::{
    CallOptions, ClientConfig, call_bidi_stream, call_client_stream, call_server_stream, call_unary,
};
use crate::spec::{Spec, StreamType};
use crate::websocket::{WebSocketConfig, WebSocketTransport};

fn client_config(addr: SocketAddr) -> ClientConfig {
    ClientConfig::new(format!("http://{addr}").parse().unwrap())
}

fn spec(method: &'static str, stream_type: StreamType) -> Spec {
    Spec::client(method, stream_type)
}

// A hand-driven server for the proto codec. tungstenite fixes the
// callback's error type, so the large error cannot be avoided.
#[allow(clippy::result_large_err)]
async fn accept_proto(
    listener: &tokio::net::TcpListener,
) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
    let (tcp, _) = listener.accept().await.unwrap();
    let callback =
        |_: &tokio_tungstenite::tungstenite::handshake::server::Request,
         mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            response.headers_mut().insert(
                "sec-websocket-protocol",
                http::HeaderValue::from_static("connectrpc.1+proto"),
            );
            Ok(response)
        };
    tokio_tungstenite::accept_hdr_async(tcp, callback)
        .await
        .unwrap()
}

#[tokio::test]
async fn unary_round_trip_carries_metadata() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let transport = WebSocketTransport::plaintext();
    for config in [
        client_config(addr),
        client_config(addr).with_codec_format(crate::CodecFormat::Json),
    ] {
        let options = CallOptions::default().with_header("acme-tenant", "t-42");
        let response = call_unary::<_, _, StringValueView<'static>>(
            &transport,
            &config,
            spec("/test.v1.EchoService/Echo", StreamType::Unary),
            StringValue::from("hi"),
            options,
        )
        .await
        .unwrap();
        assert_eq!(response.headers()["x-head"], "h");
        assert_eq!(response.trailers()["x-tail"], "t");
        assert_eq!(response.into_owned().value, "ws:t-42:hi");
    }
}

#[tokio::test]
async fn unary_error_keeps_code_and_trailers() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let err = call_unary::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Echo", StreamType::Unary),
        StringValue::from("fail"),
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    assert_eq!(err.message.as_deref(), Some("no such thing"));
    assert_eq!(err.trailers()["x-why"], "missing");
}

#[tokio::test]
async fn server_and_client_streaming() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let transport = WebSocketTransport::plaintext();
    let config = client_config(addr);

    let mut stream = call_server_stream::<_, _, StringValueView<'static>>(
        &transport,
        &config,
        spec("/test.v1.EchoService/Count", StreamType::ServerStream),
        StringValue::from("3"),
        CallOptions::default(),
    )
    .await
    .unwrap();
    let mut received = Vec::new();
    while let Some(message) = stream.message().await.unwrap() {
        received.push(message.value().to_owned());
    }
    assert_eq!(received, ["0", "1", "2"]);

    let response = call_client_stream::<_, _, StringValueView<'static>>(
        &transport,
        &config,
        spec("/test.v1.EchoService/Join", StreamType::ClientStream),
        futures::stream::iter(["a", "b", "c"].map(StringValue::from)),
        CallOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(response.into_owned().value, "a,b,c");
}

#[tokio::test]
async fn bidi_is_full_duplex() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let mut stream = call_bidi_stream::<_, StringValue, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Chat", StreamType::BidiStream),
        CallOptions::default(),
    )
    .await
    .unwrap();
    for word in ["one", "two", "three"] {
        stream.send(StringValue::from(word)).await.unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .expect("reply before the request stream closed")
            .unwrap()
            .unwrap();
        assert_eq!(reply.value(), format!("echo:{word}"));
    }
    stream.close_send();
    assert!(stream.message().await.unwrap().is_none());
}

#[tokio::test]
async fn path_prefix_splits_the_transports() {
    let (addr, _) = serve(WebSocketConfig::new().with_path_prefix("ws")).await;
    let ok = call_unary::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &ClientConfig::new(format!("http://{addr}/ws/").parse().unwrap()),
        spec("/test.v1.EchoService/Echo", StreamType::Unary),
        StringValue::from("p"),
        CallOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(ok.into_owned().value, "ws::p");

    // An upgrade at the bare path is refused with 400.
    let err = call_unary::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Echo", StreamType::Unary),
        StringValue::from("p"),
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(
        err.message.as_deref().unwrap().contains("HTTP 400"),
        "{err}"
    );

    // Plain HTTP still reaches the bare path, and gets 426 under the prefix.
    let http = crate::client::HttpClient::plaintext();
    let ok = call_unary::<_, _, StringValueView<'static>>(
        &http,
        &client_config(addr),
        spec("/test.v1.EchoService/Echo", StreamType::Unary),
        StringValue::from("p"),
        CallOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(ok.into_owned().value, "http::p");
    let status = raw_http_status(addr, "/ws/test.v1.EchoService/Echo").await;
    assert_eq!(status, 426);
}

#[tokio::test]
async fn unknown_procedure_is_unimplemented_in_band() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let err = call_unary::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Missing", StreamType::Unary),
        StringValue::from("x"),
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::Unimplemented);
}

/// A server that answers one call with `messages`, then reports the Close
/// frame the client sends.
async fn scripted(
    messages: Vec<tokio_tungstenite::tungstenite::Message>,
) -> (
    SocketAddr,
    tokio::sync::oneshot::Receiver<Option<tokio_tungstenite::tungstenite::protocol::CloseFrame>>,
) {
    use tokio_tungstenite::tungstenite::Message;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (close_tx, close_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut ws = accept_proto(&listener).await;
        for message in messages {
            let _ = ws.send(message).await;
        }
        while let Some(Ok(message)) = ws.next().await {
            if let Message::Close(frame) = message {
                let _ = close_tx.send(frame);
                break;
            }
        }
    });
    (addr, close_rx)
}

async fn close_code(
    close_rx: tokio::sync::oneshot::Receiver<
        Option<tokio_tungstenite::tungstenite::protocol::CloseFrame>,
    >,
) -> u16 {
    let frame = tokio::time::timeout(Duration::from_secs(10), close_rx)
        .await
        .expect("the client never closed")
        .unwrap()
        .expect("a Close frame with a code");
    u16::from(frame.code)
}

#[tokio::test]
async fn an_undecodable_response_is_a_protocol_error() {
    use tokio_tungstenite::tungstenite::Message;
    let script = || {
        vec![
            Message::Text("M[]".into()),
            Message::Binary(bytes::Bytes::from_static(b"B\xff")),
            Message::Text("S{}".into()),
        ]
    };
    let (addr, close_rx) = scripted(script()).await;
    let err = call_unary::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Echo", StreamType::Unary),
        StringValue::from("x"),
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::Internal, "{err}");
    assert_eq!(close_code(close_rx).await, 3102);

    let (addr, close_rx) = scripted(script()).await;
    let err = call_client_stream::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Join", StreamType::ClientStream),
        futures::stream::iter([StringValue::from("x")]),
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::Internal, "{err}");
    assert_eq!(close_code(close_rx).await, 3102);
}

#[tokio::test]
async fn a_unary_response_needs_exactly_one_message() {
    use tokio_tungstenite::tungstenite::Message;
    let body = || Message::Binary(bytes::Bytes::from_static(b"B\x0a\x01x"));
    let (addr, close_rx) = scripted(vec![
        Message::Text("M[]".into()),
        body(),
        body(),
        Message::Text("S{}".into()),
    ])
    .await;
    let err = call_unary::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Echo", StreamType::Unary),
        StringValue::from("x"),
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::Unimplemented, "{err}");
    assert_eq!(close_code(close_rx).await, 1000);
}

#[tokio::test]
async fn a_decoded_response_closes_normally() {
    use tokio_tungstenite::tungstenite::Message;
    let (addr, close_rx) = scripted(vec![
        Message::Text("M[]".into()),
        Message::Binary(bytes::Bytes::from_static(b"B\x0a\x01x")),
        Message::Text("S{}".into()),
    ])
    .await;
    let ok = call_unary::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Echo", StreamType::Unary),
        StringValue::from("x"),
        CallOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(ok.into_owned().value, "x");
    assert_eq!(close_code(close_rx).await, 1000);
}

#[tokio::test]
async fn client_reports_a_server_that_closes_without_s() {
    // A server that accepts the upgrade and hangs up straight away.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut ws = accept_proto(&listener).await;
        let _ = ws.next().await;
        let _ = ws.close(None).await;
    });
    let err = call_unary::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Echo", StreamType::Unary),
        StringValue::from("x"),
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::Unavailable, "{err}");
}

#[tokio::test]
async fn client_closes_with_3102_and_a_reason_on_a_malformed_end_stream() {
    use tokio_tungstenite::tungstenite::Message;

    // A server that answers a unary call with an S that is not JSON, then
    // reports the client's Close frame.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (close_tx, close_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut ws = accept_proto(&listener).await;
        let _ = ws.send(Message::Text("M[]".into())).await;
        let _ = ws.send(Message::Text("Snot json".into())).await;
        while let Some(Ok(message)) = ws.next().await {
            if let Message::Close(frame) = message {
                let _ = close_tx.send(frame);
                break;
            }
        }
    });
    let err = call_unary::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Echo", StreamType::Unary),
        StringValue::from("x"),
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::Internal, "{err}");
    let frame = close_rx.await.unwrap().expect("a Close frame with a code");
    assert_eq!(u16::from(frame.code), 3102);
    assert!(!frame.reason.is_empty());
}

#[tokio::test]
async fn a_local_failure_is_not_explained_to_the_server() {
    use http_body_util::BodyExt;
    use tokio_tungstenite::tungstenite::Message;

    use crate::client::ClientTransport;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (close_tx, close_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut ws = accept_proto(&listener).await;
        while let Some(Ok(message)) = ws.next().await {
            if let Message::Close(frame) = message {
                let _ = close_tx.send(frame);
                break;
            }
        }
    });
    let failing = futures::stream::iter([Err::<http_body::Frame<bytes::Bytes>, _>(
        crate::ConnectError::internal("secret-token-123"),
    )]);
    let request = http::Request::post(format!("http://{addr}/test.v1.EchoService/Join"))
        .header("content-type", "application/connect+proto")
        .body(BodyExt::boxed(http_body_util::StreamBody::new(failing)))
        .unwrap();
    let err = WebSocketTransport::plaintext()
        .send(request)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("secret-token-123"), "{err}");
    let frame = close_rx.await.unwrap().expect("a Close frame with a code");
    assert_eq!(u16::from(frame.code), 3102);
    assert!(!frame.reason.contains("secret"), "{}", frame.reason);
}

#[tokio::test]
async fn an_oversized_response_is_resource_exhausted() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let transport = WebSocketTransport::plaintext().with_max_message_size(2);
    let config = client_config(addr);

    let err = call_unary::<_, _, StringValueView<'static>>(
        &transport,
        &config,
        spec("/test.v1.EchoService/Echo", StreamType::Unary),
        StringValue::from("too long"),
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::ResourceExhausted, "{err}");

    // A streaming RPC reads the failure from the body, which the call path
    // reports as internal, caused by the transport's error.
    let mut stream = call_server_stream::<_, _, StringValueView<'static>>(
        &transport,
        &config,
        spec("/test.v1.EchoService/Count", StreamType::ServerStream),
        StringValue::from("3"),
        CallOptions::default(),
    )
    .await
    .unwrap();
    let err = loop {
        match stream.message().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("the stream ended without an error"),
            Err(err) => break err,
        }
    };
    assert_eq!(err.code, ErrorCode::ResourceExhausted, "{err}");
    let cause = std::error::Error::source(&err)
        .and_then(|source| source.downcast_ref::<crate::ConnectError>())
        .expect("the transport's error as the source");
    assert_eq!(cause.code, ErrorCode::ResourceExhausted);
}

#[tokio::test]
async fn large_bidi_messages_flow_both_ways_at_once() {
    let (addr, _) = serve(WebSocketConfig::new()).await;
    let stream = call_bidi_stream::<_, StringValue, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Chat", StreamType::BidiStream),
        CallOptions::default(),
    )
    .await
    .unwrap();
    let (mut send, mut recv) = stream.into_split();
    const COUNT: usize = 16;
    let big = "y".repeat(512 * 1024);
    let sender = tokio::spawn(async move {
        for _ in 0..COUNT {
            send.send(StringValue::from(big.as_str())).await.unwrap();
        }
        send.close_send();
    });
    let received = tokio::time::timeout(Duration::from_secs(20), async {
        let mut count = 0;
        while let Some(message) = recv.message().await.unwrap() {
            assert_eq!(message.value().len(), 512 * 1024 + "echo:".len());
            count += 1;
        }
        count
    })
    .await
    .expect("the stream stalled");
    assert_eq!(received, COUNT);
    sender.await.unwrap();
}

#[tokio::test]
async fn client_rejects_reserved_headers_before_dialing() {
    let err = call_unary::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config("127.0.0.1:9".parse().unwrap()),
        spec("/test.v1.EchoService/Echo", StreamType::Unary),
        StringValue::from("x"),
        CallOptions::default().with_header("cookie", "a=b"),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidArgument, "{err}");
}

#[tokio::test]
async fn client_reports_an_early_s_while_still_sending() {
    use tokio_tungstenite::tungstenite::Message;
    // A server that rejects the RPC as soon as it opens, then discards what
    // the client is still sending until it closes.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut ws = accept_proto(&listener).await;
        let _ = ws.next().await;
        ws.send(Message::Text("M[]".into())).await.unwrap();
        ws.send(Message::Text(
            r#"S{"error":{"code":"permission_denied","message":"no"}}"#.into(),
        ))
        .await
        .unwrap();
        let _ = ws.close(None).await;
        while let Some(Ok(_)) = ws.next().await {}
    });
    let big = "w".repeat(1024 * 1024);
    let err = call_client_stream::<_, _, StringValueView<'static>>(
        &WebSocketTransport::plaintext(),
        &client_config(addr),
        spec("/test.v1.EchoService/Join", StreamType::ClientStream),
        futures::stream::iter((0..32).map(move |_| StringValue::from(big.as_str()))),
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::PermissionDenied, "{err}");
}
