pub mod proto {
    connectrpc::include_generated!();
}
pub use proto::test::echo::v1::*;

#[cfg(test)]
mod tests {

    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use connectrpc::client::{ClientConfig, ClientTransport, HttpClient};
    use connectrpc::{
        ConnectError, ConnectRpcService, RequestContext, Response, Router, ServiceResult,
        ServiceStream,
    };
    use futures::StreamExt;
    use tokio::net::TcpListener;

    use super::*;
    use buffa::view::OwnedView;

    const MSG_DELAY: Duration = Duration::from_millis(100);

    /// Test echo service that echoes messages back with configurable delays.
    struct TestEchoService;

    impl EchoService for TestEchoService {
        async fn echo(
            &self,
            _ctx: RequestContext,
            request: OwnedView<EchoRequestView<'static>>,
        ) -> ServiceResult<EchoResponse> {
            let request = request.to_owned_message();
            Response::ok(EchoResponse {
                sequence: request.sequence,
                data: request.data,
                ..Default::default()
            })
        }

        async fn server_stream(
            &self,
            _ctx: RequestContext,
            request: OwnedView<EchoRequestView<'static>>,
        ) -> ServiceResult<ServiceStream<EchoResponse>> {
            let request = request.to_owned_message();
            let count = request.sequence;
            let stream = futures::stream::unfold(0, move |i| async move {
                if i >= count {
                    return None;
                }
                if i > 0 {
                    tokio::time::sleep(MSG_DELAY).await;
                }
                Some((
                    Ok(EchoResponse {
                        sequence: i,
                        data: format!("response-{i}"),
                        ..Default::default()
                    }),
                    i + 1,
                ))
            });
            Response::stream_ok(stream)
        }

        async fn client_stream(
            &self,
            _ctx: RequestContext,
            mut requests: ServiceStream<OwnedView<EchoRequestView<'static>>>,
        ) -> ServiceResult<EchoResponse> {
            let mut count = 0i32;
            let mut parts = Vec::new();
            while let Some(req) = requests.next().await {
                let req = req?.to_owned_message();
                count += 1;
                parts.push(req.data);
            }
            Response::ok(EchoResponse {
                sequence: count,
                data: parts.join(","),
                ..Default::default()
            })
        }

        async fn bidi_stream(
            &self,
            _ctx: RequestContext,
            requests: ServiceStream<OwnedView<EchoRequestView<'static>>>,
        ) -> ServiceResult<ServiceStream<EchoResponse>> {
            // Map stream to owned types before spawning to satisfy Send bounds
            let mut requests = Box::pin(requests.map(|r| r.map(|v| v.to_owned_message())));
            // Echo each request back immediately via an mpsc channel.
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<EchoResponse, ConnectError>>(1);
            tokio::spawn(async move {
                while let Some(req) = requests.next().await {
                    match req {
                        Ok(req) => {
                            let resp = EchoResponse {
                                sequence: req.sequence,
                                data: req.data,
                                ..Default::default()
                            };
                            if tx.send(Ok(resp)).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            break;
                        }
                    }
                }
            });
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            Response::stream_ok(stream)
        }
    }

    /// Start the test server, return the bound address and join handle.
    ///
    /// The `TcpListener` is bound before spawning, so the socket is already
    /// listening and TCP's backlog will queue incoming connections even if
    /// the axum event loop hasn't entered its main accept loop yet.
    async fn start_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let router = Router::new();
        let router = Arc::new(TestEchoService).register(router);
        let app = router.into_axum_router();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, handle)
    }

    /// Same as `start_server` but using the monomorphic `EchoServiceServer<T>`
    /// dispatcher instead of the dynamic `Router`. Exercises the generated
    /// `Dispatcher` impl end-to-end.
    async fn start_server_mono() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let server = EchoServiceServer::new(TestEchoService);
        let service = ConnectRpcService::new(server);
        let app = axum::Router::new().fallback_service(service);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, handle)
    }

    fn make_client(addr: std::net::SocketAddr) -> EchoServiceClient<HttpClient> {
        let config = ClientConfig::new(format!("http://{addr}").parse().unwrap());
        EchoServiceClient::new(HttpClient::plaintext(), config)
    }

    #[tokio::test]
    async fn unary_echo() {
        let (addr, _server) = start_server().await;
        let client = make_client(addr);
        let resp = client
            .echo(EchoRequest {
                sequence: 42,
                data: "hello".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let msg = resp.into_view();
        assert_eq!(msg.sequence, 42);
        assert_eq!(msg.data, "hello");
    }

    /// Documents the three ways to access a unary response, in order of
    /// preference. Migrating code often reaches for the owned-struct shape
    /// (pattern 3) out of prost/tonic habit — patterns 1 and 2 are usually
    /// cheaper and sufficient.
    #[tokio::test]
    async fn unary_response_access_patterns() {
        let (addr, _server) = start_server().await;
        let client = make_client(addr);
        let req = || EchoRequest {
            sequence: 7,
            data: "test".into(),
            ..Default::default()
        };

        // Pattern 1: borrow the view via `.view()`. Zero-copy. Use this for
        // simple assertions when you also need headers/trailers — `OwnedView`
        // derefs to the view, so field access (`.sequence`, `.data` → &str)
        // works directly.
        let resp = client.echo(req()).await.unwrap();
        assert_eq!(resp.view().sequence, 7);
        assert_eq!(resp.view().data, "test");
        // Headers/trailers are still available.
        let _ = resp.headers();

        // Pattern 2: consume via `.into_view()` to get the `OwnedView`.
        // Still zero-copy (Deref-based field access), but discards headers/
        // trailers. Use this when you only care about the body.
        let msg = client.echo(req()).await.unwrap().into_view();
        assert_eq!(msg.sequence, 7);
        assert_eq!(msg.data, "test"); // &str via Deref — no allocation

        // Pattern 3: `.into_owned()` to get the owned struct. Allocates and
        // copies all string/bytes fields. Only needed when you want the
        // prost-style `EchoResponse` (e.g. to pass to `fn(&EchoResponse)`
        // or store in a Vec<EchoResponse>).
        let owned: EchoResponse = client.echo(req()).await.unwrap().into_owned();
        assert_eq!(owned.sequence, 7);
        assert_eq!(owned.data, "test"); // String, allocated
    }

    #[tokio::test]
    async fn server_stream_incremental_delivery() {
        let (addr, _server) = start_server().await;
        let client = make_client(addr);

        let num_messages = 5i32;
        let mut stream = client
            .server_stream(EchoRequest {
                sequence: num_messages,
                data: "test".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut received = Vec::new();
        let start = Instant::now();

        while let Some(msg) = stream.message().await.unwrap() {
            let elapsed = start.elapsed();
            received.push((msg.sequence, elapsed));
        }

        assert_eq!(received.len(), num_messages as usize);

        // Verify incremental delivery: the spread between the first and
        // last message should be approximately (N-1) * MSG_DELAY. If
        // messages were buffered, they'd all arrive at roughly the same
        // time near the end.
        let total_spread = received.last().unwrap().1 - received.first().unwrap().1;
        let expected_spread = MSG_DELAY * (num_messages as u32 - 1);

        // The spread should be at least half the expected value — this is
        // generous enough for loaded CI while still proving messages are
        // not all batched together.
        assert!(
            total_spread >= expected_spread / 2,
            "messages arrived too close together: spread={total_spread:?}, \
             expected at least {:?} (half of {expected_spread:?})",
            expected_spread / 2,
        );

        // And shouldn't take excessively long either.
        assert!(
            total_spread < expected_spread * 3,
            "messages arrived too slowly: spread={total_spread:?}, \
             expected at most {:?}",
            expected_spread * 3,
        );
    }

    #[tokio::test]
    async fn server_stream_zero_messages() {
        let (addr, _server) = start_server().await;
        let client = make_client(addr);

        let mut stream = client
            .server_stream(EchoRequest {
                sequence: 0, // request zero messages
                data: "test".into(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Should get no messages, just end-of-stream.
        assert!(stream.message().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn client_stream_delivers_all_messages() {
        let (addr, _server) = start_server().await;
        let client = make_client(addr);

        let messages: Vec<EchoRequest> = (0..5)
            .map(|i| EchoRequest {
                sequence: i,
                data: format!("msg-{i}"),
                ..Default::default()
            })
            .collect();

        let resp = client.client_stream(messages).await.unwrap();

        let msg = resp.into_view();
        assert_eq!(msg.sequence, 5);
        assert_eq!(msg.data, "msg-0,msg-1,msg-2,msg-3,msg-4");
    }

    #[tokio::test]
    async fn client_stream_empty() {
        let (addr, _server) = start_server().await;
        let client = make_client(addr);

        let resp = client
            .client_stream(Vec::<EchoRequest>::new())
            .await
            .unwrap();

        let msg = resp.into_view();
        assert_eq!(msg.sequence, 0);
        assert_eq!(msg.data, "");
    }

    /// Regression: `call_client_stream` must stream the request body
    /// frame-by-frame instead of buffering the whole concatenated payload
    /// into a single Frame. Each iterator item should produce its own body
    /// frame (one envelope per channel push).
    #[tokio::test]
    async fn client_stream_request_body_is_streamed() {
        use bytes::Bytes;
        use connectrpc::client::{BoxFuture, ClientBody, ClientTransport};
        use http::{Request, Response};
        use http_body::Body;
        use std::pin::Pin;
        use std::sync::Mutex;

        #[derive(Clone)]
        struct FrameCountingTransport {
            frame_sizes: Arc<Mutex<Vec<usize>>>,
        }

        impl ClientTransport for FrameCountingTransport {
            type ResponseBody = http_body_util::Empty<Bytes>;
            type Error = ConnectError;

            fn send(
                &self,
                request: Request<ClientBody>,
            ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
                let recorded = self.frame_sizes.clone();
                Box::pin(async move {
                    let mut body = request.into_body();
                    let mut sizes = Vec::new();
                    while let Some(frame) =
                        std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await
                    {
                        let frame: http_body::Frame<Bytes> = frame?;
                        if let Ok(data) = frame.into_data() {
                            sizes.push(data.len());
                        }
                    }
                    *recorded.lock().unwrap() = sizes;
                    // Short-circuit: the call will surface this as Unavailable.
                    // The assertion is on the captured request framing.
                    Err(ConnectError::unavailable("recorded; not forwarded"))
                })
            }
        }

        let frames: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let transport = FrameCountingTransport {
            frame_sizes: frames.clone(),
        };
        let config = ClientConfig::new("http://localhost/".parse().unwrap());
        let client = EchoServiceClient::new(transport, config);

        let messages: Vec<EchoRequest> = (0..5)
            .map(|i| EchoRequest {
                sequence: i,
                data: format!("msg-{i}"),
                ..Default::default()
            })
            .collect();

        // Expected to fail with the forced transport error. call_client_stream
        // awaits the oneshot internally, so frames are fully captured by return.
        let _ = client.client_stream(messages).await;

        let captured = frames.lock().unwrap().clone();
        assert_eq!(
            captured.len(),
            5,
            "expected one body frame per request message, got {} (sizes: {captured:?})",
            captured.len(),
        );
        // Every envelope carries at minimum the 5-byte header.
        for size in &captured {
            assert!(
                *size >= 5,
                "envelope frame too small ({size} bytes) — header alone is 5 bytes",
            );
        }
    }

    /// Tests bidi streaming at the server level by sending envelope-framed
    /// messages over raw HTTP. This verifies that the server-side bidi handler
    /// correctly receives and echoes all messages.
    ///
    /// Note: this sends all messages in a single HTTP body (not interleaved
    /// with reads), so it tests server-side correctness but not true
    /// concurrent bidirectional streaming. Full incremental bidi testing
    /// requires a client that can write and read the body concurrently,
    /// which the generated client stub does not yet support.
    #[tokio::test]
    async fn bidi_stream_server_echoes_messages() {
        let (addr, _server) = start_server().await;

        use buffa::Message;
        use bytes::{BufMut, BytesMut};

        let messages: Vec<EchoRequest> = (0..3)
            .map(|i| EchoRequest {
                sequence: i,
                data: format!("bidi-{i}"),
                ..Default::default()
            })
            .collect();

        // Encode messages as envelope frames.
        let mut body = BytesMut::new();
        for msg in &messages {
            let data = msg.encode_to_vec();
            body.put_u8(0); // flags: no compression
            body.put_u32(data.len() as u32);
            body.extend_from_slice(&data);
        }

        let client = HttpClient::plaintext();
        let uri: http::Uri = format!("http://{addr}/{ECHO_SERVICE_SERVICE_NAME}/BidiStream")
            .parse()
            .unwrap();

        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri(uri)
            .header("content-type", "application/connect+proto")
            .header("connect-protocol-version", "1")
            .body(connectrpc::client::full_body(body.freeze()))
            .unwrap();

        let response = client.send(request).await.unwrap();
        assert_eq!(response.status(), 200);

        // Read response body and decode envelopes.
        use http_body_util::BodyExt;
        let response_body = response.into_body().collect().await.unwrap().to_bytes();

        let mut cursor = &response_body[..];
        let mut responses = Vec::new();
        while cursor.len() >= 5 {
            let flags = cursor[0];
            let len = u32::from_be_bytes([cursor[1], cursor[2], cursor[3], cursor[4]]) as usize;
            cursor = &cursor[5..];
            if flags & 0x02 != 0 {
                // END_STREAM envelope
                cursor = &cursor[len..];
                continue;
            }
            let data = &cursor[..len];
            cursor = &cursor[len..];
            let resp = EchoResponse::decode_from_slice(data).unwrap();
            responses.push(resp);
        }

        assert!(
            cursor.is_empty(),
            "unexpected trailing bytes: {} remaining",
            cursor.len()
        );
        assert_eq!(responses.len(), 3);
        for (i, resp) in responses.iter().enumerate() {
            assert_eq!(resp.sequence, i as i32);
            assert_eq!(resp.data, format!("bidi-{i}"));
        }
    }

    /// Smoke test that all four RPC kinds work through the generated
    /// monomorphic `EchoServiceServer<T>` dispatcher (not Router).
    #[tokio::test]
    async fn mono_dispatcher_all_kinds() {
        let (addr, _server) = start_server_mono().await;
        let client = make_client(addr);

        // Unary
        let resp = client
            .echo(EchoRequest {
                sequence: 1,
                data: "mono-unary".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.into_view().data, "mono-unary");

        // Server streaming
        let mut stream = client
            .server_stream(EchoRequest {
                sequence: 3,
                data: String::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut ss_count = 0;
        while stream.message().await.unwrap().is_some() {
            ss_count += 1;
        }
        assert_eq!(ss_count, 3);

        // Client streaming
        let messages: Vec<EchoRequest> = (0..4)
            .map(|i| EchoRequest {
                sequence: i,
                data: format!("cs-{i}"),
                ..Default::default()
            })
            .collect();
        let resp = client.client_stream(messages).await.unwrap();
        assert_eq!(resp.into_view().sequence, 4);

        // NotFound — wrong path should produce Unimplemented, not panic.
        // Use the raw HTTP client to hit a nonexistent method.
        let http = HttpClient::plaintext();
        let uri: http::Uri = format!("http://{addr}/{ECHO_SERVICE_SERVICE_NAME}/NoSuchMethod")
            .parse()
            .unwrap();
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(uri)
            .header("content-type", "application/proto")
            .header("connect-protocol-version", "1")
            .body(connectrpc::client::full_body(buffa::bytes::Bytes::new()))
            .unwrap();
        let resp = http.send(req).await.unwrap();
        assert_eq!(resp.status(), 404);
    }

    /// End-to-end test of `Http2Connection` / `SharedHttp2Connection`.
    ///
    /// Uses the raw h2 transport (no legacy pool) against the monomorphic
    /// server. Verifies connect + reconnect-wrapper + buffer + ClientTransport
    /// wiring works for a real unary RPC.
    #[tokio::test]
    async fn http2_connection_transport() {
        use connectrpc::Protocol;
        use connectrpc::client::Http2Connection;

        let (addr, _server) = start_server_mono().await;
        let uri: http::Uri = format!("http://{addr}").parse().unwrap();

        // Eager connect variant.
        let conn = Http2Connection::connect_plaintext(uri.clone())
            .await
            .unwrap();
        let shared = conn.shared(64);

        let config = ClientConfig::new(uri.clone()).protocol(Protocol::Grpc);
        let client = EchoServiceClient::new(shared.clone(), config.clone());

        let resp = client
            .echo(EchoRequest {
                sequence: 7,
                data: "h2-direct".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let msg = resp.into_view();
        assert_eq!(msg.sequence, 7);
        assert_eq!(msg.data, "h2-direct");

        // Lazy connect variant — first request triggers the handshake.
        let lazy = Http2Connection::lazy_plaintext(uri.clone()).shared(64);
        let client = EchoServiceClient::new(lazy, config);
        let resp = client
            .echo(EchoRequest {
                sequence: 8,
                data: "h2-lazy".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.into_view().data, "h2-lazy");

        // Concurrent requests on the shared handle — exercises the Buffer.
        let mut handles = Vec::new();
        for i in 0..16 {
            let c = shared.clone();
            let cfg: ClientConfig = ClientConfig::new(uri.clone()).protocol(Protocol::Grpc);
            handles.push(tokio::spawn(async move {
                let client = EchoServiceClient::new(c, cfg);
                client
                    .echo(EchoRequest {
                        sequence: i,
                        data: format!("concurrent-{i}"),
                        ..Default::default()
                    })
                    .await
                    .map(|r| r.into_view())
            }));
        }
        for (i, h) in handles.into_iter().enumerate() {
            let msg = h.await.unwrap().unwrap();
            assert_eq!(msg.sequence, i as i32);
        }
    }

    /// Regression test for the half-duplex deadlock on `SharedHttp2Connection`.
    ///
    /// Before the fix, `call_bidi_stream` stored the transport's `send()`
    /// future unpolled in `RecvState::Pending`, so the HTTP request never
    /// initiated until the first `message()` call. `BidiStream::send()`
    /// would buffer into the 32-deep mpsc with nobody draining it, and the
    /// 33rd send would hang forever. After the fix, the send future is
    /// spawned so the request streams immediately.
    ///
    /// Timeout-wrapped so a regression fails fast instead of hanging the
    /// suite.
    #[tokio::test]
    async fn bidi_half_duplex_many_sends_before_first_read() {
        use connectrpc::Protocol;
        use connectrpc::client::Http2Connection;

        let (addr, _server) = start_server_mono().await;
        let uri: http::Uri = format!("http://{addr}").parse().unwrap();

        let conn = Http2Connection::connect_plaintext(uri.clone())
            .await
            .unwrap()
            .shared(64);
        let config = ClientConfig::new(uri).protocol(Protocol::Grpc);
        let client = EchoServiceClient::new(conn, config);

        let test = async {
            let mut stream = client.bidi_stream().await.unwrap();
            // More than the 32-deep ChannelBody mpsc. Pre-fix, send #33 hangs
            // here; post-fix, the spawned task drains as we go.
            for i in 0..40 {
                stream
                    .send(EchoRequest {
                        sequence: i,
                        data: format!("half-duplex-{i}"),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
            }
            stream.close_send();

            let mut received = 0;
            while let Some(resp) = stream.message().await.unwrap() {
                assert_eq!(resp.data, format!("half-duplex-{}", resp.sequence));
                received += 1;
            }
            assert_eq!(received, 40);
        };

        tokio::time::timeout(std::time::Duration::from_secs(10), test)
            .await
            .expect("half-duplex bidi deadlocked (send #33 never completed?)");
    }

    // ========================================================================
    // TLS integration tests — dogfood HttpClient::with_tls and
    // Http2Connection::connect_tls against our own Server::with_tls.
    // ========================================================================

    /// Generate a self-signed certificate + key for localhost and build a
    /// rustls ServerConfig (for the server) and ClientConfig (trusting the
    /// self-signed cert, for the client) from them.
    ///
    /// ALPN on the server config advertises both h2 and http/1.1 so both
    /// HttpClient (which negotiates) and Http2Connection (h2-only) work.
    fn gen_tls_configs() -> (
        Arc<connectrpc::rustls::ServerConfig>,
        Arc<connectrpc::rustls::ClientConfig>,
    ) {
        use connectrpc::rustls;

        // Self-signed cert for localhost via rcgen.
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().clone();
        let key_der =
            rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der()).unwrap();

        // Server config: present the self-signed cert, no client auth.
        let mut server_cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap();
        server_cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        // Client config: trust the self-signed cert as a root.
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert_der).unwrap();
        let client_cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        (Arc::new(server_cfg), Arc::new(client_cfg))
    }

    /// Spin up a TLS server on an ephemeral port using Server::with_tls.
    async fn start_tls_server(
        server_cfg: Arc<connectrpc::rustls::ServerConfig>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        use connectrpc::Server;

        let service = Arc::new(TestEchoService);
        let router = service.register(Router::new());

        let bound = Server::bind("127.0.0.1:0")
            .await
            .expect("bind")
            .with_tls(server_cfg);
        let addr = bound.local_addr().expect("local_addr");
        let handle = tokio::spawn(async move {
            bound.serve(router).await.unwrap();
        });
        (addr, handle)
    }

    /// End-to-end TLS round-trip using HttpClient::with_tls.
    ///
    /// Proves the full stack: library's TLS server, library's TLS client,
    /// ALPN negotiation (client advertises h2+http/1.1, server picks one),
    /// generated client over the TLS transport.
    #[tokio::test]
    async fn tls_round_trip_http_client() {
        let (server_cfg, client_cfg) = gen_tls_configs();
        let (addr, _server) = start_tls_server(server_cfg).await;

        let http = HttpClient::with_tls(client_cfg);
        let config = ClientConfig::new(
            format!("https://localhost:{}", addr.port())
                .parse()
                .unwrap(),
        );
        let client = EchoServiceClient::new(http, config);

        let resp = client
            .echo(EchoRequest {
                sequence: 1,
                data: "tls-hello".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let msg = resp.into_view();
        assert_eq!(msg.sequence, 1);
        assert_eq!(msg.data, "tls-hello");
    }

    /// End-to-end TLS round-trip using Http2Connection::connect_tls.
    ///
    /// Exercises the manual tokio_rustls handshake path, the ALPN-negotiated-h2
    /// check, and the BoxedIo unification.
    #[tokio::test]
    async fn tls_round_trip_http2_connection() {
        use connectrpc::Protocol;
        use connectrpc::client::Http2Connection;

        let (server_cfg, client_cfg) = gen_tls_configs();
        let (addr, _server) = start_tls_server(server_cfg).await;

        let uri: http::Uri = format!("https://localhost:{}", addr.port())
            .parse()
            .unwrap();
        let conn = Http2Connection::connect_tls(uri.clone(), client_cfg)
            .await
            .unwrap()
            .shared(64);

        let config = ClientConfig::new(uri).protocol(Protocol::Grpc);
        let client = EchoServiceClient::new(conn, config);

        let resp = client
            .echo(EchoRequest {
                sequence: 2,
                data: "tls-h2-hello".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let msg = resp.into_view();
        assert_eq!(msg.sequence, 2);
        assert_eq!(msg.data, "tls-h2-hello");
    }

    /// Http2Connection::connect_tls must fail when the server doesn't
    /// negotiate h2 via ALPN.
    ///
    /// Two failure modes are covered by the same test setup:
    ///   1. Server advertises ONLY http/1.1 → rustls TLS handshake fails
    ///      with NoApplicationProtocol alert (no common protocol). This is
    ///      what this test observes.
    ///   2. Server doesn't use ALPN at all → handshake succeeds but
    ///      alpn_protocol() returns None → our post-handshake check fires.
    ///      This is harder to exercise in a test since rustls always uses
    ///      ALPN; our check exists as defense-in-depth for servers that
    ///      do TLS without ALPN (rare but possible).
    #[tokio::test]
    async fn tls_http2_connection_rejects_non_h2_alpn() {
        use connectrpc::client::Http2Connection;
        use connectrpc::rustls;

        // Server that ONLY advertises http/1.1 — no h2.
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().clone();
        let key_der =
            rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der()).unwrap();

        let mut server_cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap();
        server_cfg.alpn_protocols = vec![b"http/1.1".to_vec()]; // NO h2

        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert_der).unwrap();
        let client_cfg = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );

        let (addr, _server) = start_tls_server(Arc::new(server_cfg)).await;
        let uri: http::Uri = format!("https://localhost:{}", addr.port())
            .parse()
            .unwrap();

        let result = Http2Connection::connect_tls(uri, client_cfg).await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("expected ALPN mismatch to be rejected"),
        };
        assert_eq!(err.code, connectrpc::ErrorCode::Unavailable);
        // The error surfaces either as a TLS-level NoApplicationProtocol
        // alert (failure mode 1 — no common ALPN protocol) or our own
        // "did not negotiate HTTP/2" message (failure mode 2 — server
        // doesn't use ALPN). Both are acceptable rejections.
        let msg = err.message.as_deref().unwrap_or("");
        assert!(
            msg.contains("NoApplicationProtocol") || msg.contains("ALPN") || msg.contains("HTTP/2"),
            "expected ALPN-related error, got: {msg}"
        );
    }

    /// A client that opens a TCP connection to a TLS server but never sends a
    /// ClientHello should be disconnected after `tls_handshake_timeout`.
    #[tokio::test]
    async fn tls_handshake_timeout_disconnects_stalled_client() {
        use connectrpc::Server;
        use std::time::{Duration, Instant};
        use tokio::io::AsyncReadExt;

        let (server_cfg, _client_cfg) = gen_tls_configs();

        let service = Arc::new(TestEchoService);
        let router = service.register(Router::new());

        // Configure a short timeout so the test runs quickly.
        let handshake_timeout = Duration::from_millis(200);
        let bound = Server::bind("127.0.0.1:0")
            .await
            .expect("bind")
            .with_tls(server_cfg)
            .tls_handshake_timeout(handshake_timeout);
        let addr = bound.local_addr().expect("local_addr");
        let _server = tokio::spawn(async move {
            bound.serve(router).await.unwrap();
        });

        // Connect via raw TCP and stall — send nothing, just wait for the
        // server to close the connection.
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("tcp connect");

        let start = Instant::now();
        let mut buf = [0u8; 1];
        // The server should close the connection after handshake_timeout.
        // read() returns Ok(0) on clean close or Err on reset.
        let read_result = stream.read(&mut buf).await;
        let elapsed = start.elapsed();

        match read_result {
            Ok(0) => {} // Clean close — what we expect
            Ok(n) => panic!("server unexpectedly sent {n} bytes to a stalled client"),
            Err(e) => {
                // Connection reset is also acceptable
                assert!(
                    matches!(
                        e.kind(),
                        std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::BrokenPipe
                    ),
                    "unexpected error kind: {:?}",
                    e.kind()
                );
            }
        }

        // Timeout should fire at ~200ms. Allow generous slop for CI scheduling
        // but verify it fired well before the DEFAULT_TLS_HANDSHAKE_TIMEOUT (10s).
        assert!(
            elapsed >= handshake_timeout,
            "server closed too early: {elapsed:?} < {handshake_timeout:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "timeout took far longer than configured: {elapsed:?}"
        );
    }
}
