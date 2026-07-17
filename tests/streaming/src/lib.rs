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
        ConnectError, ConnectRpcService, RequestContext, Response, Router, ServiceRequest,
        ServiceResult, ServiceStream, StreamMessage,
    };
    use futures::StreamExt;
    use tokio::net::TcpListener;

    use super::*;

    const MSG_DELAY: Duration = Duration::from_millis(100);

    /// Regression pin for issue #214: the `message()` futures must be `Send`
    /// with CONCRETE generated view types, not just generic parameters — the
    /// pre-fix bound shape (projecting `RespView::Owned` in the where-clause)
    /// compiled generically but lost `Send` on monomorphization via rustc's
    /// coroutine-witness auto-trait check. Compile-time-only assertion; never
    /// called.
    #[allow(dead_code)]
    fn stream_message_futures_are_send<B>(
        mut server_stream: connectrpc::client::ServerStream<B, EchoResponseView<'static>>,
        mut bidi: connectrpc::client::BidiStream<B, EchoRequest, EchoResponseView<'static>>,
    ) where
        B: connectrpc::http_body::Body<Data = bytes::Bytes> + Send + Unpin + 'static,
        B::Error: std::fmt::Display,
    {
        fn assert_send<T: Send>(_: T) {}
        // The async-block wrappers are load-bearing: asserting the bare
        // `message()` future is Send passes even with the buggy bound —
        // the failure only manifests in the ENCLOSING coroutine's witness,
        // i.e. exactly the `tokio::spawn(async move { .. })` shape users hit.
        assert_send(async move {
            let _ = server_stream.message().await;
        });
        assert_send(async move {
            let _ = bidi.message().await;
        });
    }

    /// Test echo service that echoes messages back with configurable delays.
    struct TestEchoService;

    impl EchoService for TestEchoService {
        async fn echo(
            &self,
            _ctx: RequestContext,
            request: ServiceRequest<'_, EchoRequest>,
        ) -> ServiceResult<EchoResponse> {
            // Borrowed request view: plain field access, with the borrow tied
            // to the dispatcher-owned request body — no owned round-trip.
            //
            // This handler also exercises the async shapes that matter for the
            // borrowed-request design (every existing unary test runs through
            // it, on both the Router and the monomorphic dispatcher):
            //
            // 1. The borrow is held across an await point: `request` is read
            //    *after* the yield, so the handler future captures the borrow
            //    across the suspension and must still satisfy the trait's
            //    `Send` bound.
            tokio::task::yield_now().await;

            // 2. Inner async blocks may borrow the request freely as long as
            //    they are awaited inside the handler (they are not 'static).
            let data = async { request.data.to_owned() }.await;

            // 3. Anything that needs 'static — tokio::spawn, channels, state —
            //    takes owned data: convert (or copy fields) first, then move
            //    the owned value into the task. Capturing `request` itself in
            //    a spawned task does not compile.
            let owned = request.to_owned_message();
            let sequence = tokio::spawn(async move { owned.sequence })
                .await
                .expect("spawned background task");

            Response::ok(EchoResponse {
                sequence,
                data,
                ..Default::default()
            })
        }

        async fn server_stream(
            &self,
            _ctx: RequestContext,
            request: ServiceRequest<'_, EchoRequest>,
        ) -> ServiceResult<ServiceStream<EchoResponse>> {
            // Borrowed request: copy out the one field the returned stream
            // needs (the stream must be 'static and cannot borrow `request`).
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
            mut requests: ServiceStream<StreamMessage<EchoRequest>>,
        ) -> ServiceResult<EchoResponse> {
            let mut count = 0i32;
            let mut parts = Vec::new();
            while let Some(req) = requests.next().await {
                // Per-item zero-copy reads via the generated accessor methods
                // (Deref to `EchoRequestOwnedView`); copy out only what is kept.
                let req = req?;
                count += 1;
                parts.push(req.data().to_owned());
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
            mut requests: ServiceStream<StreamMessage<EchoRequest>>,
        ) -> ServiceResult<ServiceStream<EchoResponse>> {
            // `StreamMessage` items are Send + 'static, so the stream can be
            // moved into a spawned task and read zero-copy there — no
            // map-to-owned pass is needed any more.
            // Echo each request back immediately via an mpsc channel.
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<EchoResponse, ConnectError>>(1);
            tokio::spawn(async move {
                while let Some(req) = requests.next().await {
                    match req {
                        Ok(req) => {
                            let resp = EchoResponse {
                                sequence: req.sequence(),
                                data: req.data().to_owned(),
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
        assert_eq!(resp.view().sequence, 42);
        assert_eq!(resp.view().data, "hello");
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

        // Pattern 1: borrow the message via `.view()`. Zero-copy field access
        // (`.sequence`, `.data` → &str), and headers/trailers stay available.
        // This is the default for reading a response.
        let resp = client.echo(req()).await.unwrap();
        assert_eq!(resp.view().sequence, 7);
        assert_eq!(resp.view().data, "test");
        // Headers/trailers are still available.
        let _ = resp.headers();

        // Pattern 2: consume via `.into_view()` to keep the decoded body
        // (an `OwnedView`) without copying — e.g. to stash it or send it to
        // another task. Field access goes through `.reborrow()`.
        let msg = client.echo(req()).await.unwrap().into_view();
        assert_eq!(msg.reborrow().sequence, 7);
        assert_eq!(msg.reborrow().data, "test"); // &str, no allocation

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
            received.push((msg.view().sequence, elapsed));
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

        let resp = client
            .client_stream(futures::stream::iter(messages))
            .await
            .unwrap();

        let msg = resp.into_view();
        assert_eq!(msg.reborrow().sequence, 5);
        assert_eq!(msg.reborrow().data, "msg-0,msg-1,msg-2,msg-3,msg-4");
    }

    #[tokio::test]
    async fn client_stream_empty() {
        let (addr, _server) = start_server().await;
        let client = make_client(addr);

        let resp = client
            .client_stream(futures::stream::empty::<EchoRequest>())
            .await
            .unwrap();

        let msg = resp.into_view();
        assert_eq!(msg.reborrow().sequence, 0);
        assert_eq!(msg.reborrow().data, "");
    }

    /// Regression: `call_client_stream` must stream the request body
    /// frame-by-frame instead of buffering the whole concatenated payload
    /// into a single Frame. Each stream item should produce its own body
    /// frame (one envelope per message).
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

        // Expected to fail with the forced transport error. The transport's
        // send future (which reads the whole body) completes before the call
        // returns, so frames are fully captured by then.
        let _ = client.client_stream(futures::stream::iter(messages)).await;

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

    /// End-to-end: the client-stream request source is an async stream, so
    /// messages produced *after* the call starts — here by a separate task
    /// pacing itself with sleeps — are sent as they become available, and
    /// the call completes once the stream ends.
    #[tokio::test]
    async fn client_stream_async_producer() {
        let (addr, _server) = start_server().await;
        let client = make_client(addr);

        let (tx, rx) = tokio::sync::mpsc::channel::<EchoRequest>(1);
        let producer = tokio::spawn(async move {
            for i in 0..4 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                tx.send(EchoRequest {
                    sequence: i,
                    data: format!("async-{i}"),
                    ..Default::default()
                })
                .await
                .expect("request body should keep draining the channel");
            }
            // Dropping tx ends the stream, which ends the request body.
        });

        let resp = client
            .client_stream(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap();

        let msg = resp.into_view();
        assert_eq!(msg.reborrow().sequence, 4);
        assert_eq!(msg.reborrow().data, "async-0,async-1,async-2,async-3");
        producer.await.unwrap();
    }

    /// Regression: a transport whose response resolves *before* the request
    /// stream is drained — a server sending response headers early while it
    /// keeps consuming the upload is a legitimate HTTP/2 pattern — must NOT
    /// cut the upload short. Every request message must still reach the
    /// transport; only the request body being dropped ends the drain.
    #[tokio::test]
    async fn client_stream_early_response_headers_do_not_truncate_upload() {
        use bytes::{BufMut, Bytes, BytesMut};
        use connectrpc::client::{BoxFuture, ClientBody, ClientTransport};
        use http::{Request, Response};
        use http_body::Body;
        use std::sync::Mutex;

        /// Resolves with a complete, valid Connect client-stream response
        /// immediately, while a spawned task keeps consuming the request
        /// body until EOF, recording each frame.
        #[derive(Clone)]
        struct EarlyResponseTransport {
            frames: Arc<Mutex<Vec<usize>>>,
        }

        impl ClientTransport for EarlyResponseTransport {
            type ResponseBody = http_body_util::Full<Bytes>;
            type Error = ConnectError;

            fn send(
                &self,
                request: Request<ClientBody>,
            ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
                let frames = self.frames.clone();
                // Keep the body alive and drain it in the background — the
                // signal a real transport gives while the server is still
                // reading the upload.
                tokio::spawn(async move {
                    let mut body = request.into_body();
                    while let Some(frame) =
                        std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
                            .await
                    {
                        if let Ok(Ok(data)) = frame.map(http_body::Frame::into_data) {
                            frames.lock().unwrap().push(data.len());
                        }
                    }
                });

                // Envelope-framed unary response + END_STREAM, ready at once.
                let mut body = BytesMut::new();
                let msg = buffa::Message::encode_to_vec(&EchoResponse {
                    sequence: 42,
                    ..Default::default()
                });
                body.put_u8(0);
                body.put_u32(msg.len() as u32);
                body.extend_from_slice(&msg);
                body.put_u8(0x02); // END_STREAM
                body.put_u32(2);
                body.extend_from_slice(b"{}");
                let response = Response::builder()
                    .status(200)
                    .header("content-type", "application/connect+proto")
                    .body(http_body_util::Full::new(body.freeze()))
                    .unwrap();
                Box::pin(async move { Ok(response) })
            }
        }

        let frames: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let transport = EarlyResponseTransport {
            frames: frames.clone(),
        };
        let config = ClientConfig::new("http://localhost/".parse().unwrap());
        let client = EchoServiceClient::new(transport, config);

        // A paced producer: every item is momentarily unavailable, so a
        // drain loop that (wrongly) stopped as soon as the response
        // resolved would truncate the upload.
        let (req_tx, req_rx) = tokio::sync::mpsc::channel::<EchoRequest>(1);
        tokio::spawn(async move {
            for i in 0..5 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                if req_tx
                    .send(EchoRequest {
                        sequence: i,
                        ..Default::default()
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });

        let resp = client
            .client_stream(tokio_stream::wrappers::ReceiverStream::new(req_rx))
            .await
            .unwrap();
        assert_eq!(resp.view().sequence, 42);

        // The consumer task races the call's return for the tail frames;
        // give it a moment to observe body EOF before asserting.
        for _ in 0..200 {
            if frames.lock().unwrap().len() >= 5 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            frames.lock().unwrap().len(),
            5,
            "all request messages must be sent even though the response resolved first",
        );
    }

    /// Regression: an encode failure in the request body must surface as the
    /// call's error even when the transport still produces a successful
    /// response (the abort and an early server response can race). Without
    /// the unconditional encode-error check, this call would return `Ok`
    /// for a truncated, encode-aborted upload.
    #[tokio::test]
    async fn client_stream_encode_error_beats_ok_response() {
        use bytes::{BufMut, Bytes, BytesMut};
        use connectrpc::client::{BoxFuture, ClientBody, ClientTransport};
        use http::{Request, Response};
        use http_body::Body;

        /// Reads the request body until EOF or the first body error, then
        /// returns a complete, valid response regardless.
        #[derive(Clone)]
        struct SwallowingTransport;

        impl ClientTransport for SwallowingTransport {
            type ResponseBody = http_body_util::Full<Bytes>;
            type Error = ConnectError;

            fn send(
                &self,
                request: Request<ClientBody>,
            ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
                Box::pin(async move {
                    let mut body = request.into_body();
                    while let Some(frame) =
                        std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
                            .await
                    {
                        if frame.is_err() {
                            break;
                        }
                    }

                    let mut body = BytesMut::new();
                    let msg = buffa::Message::encode_to_vec(&EchoResponse::default());
                    body.put_u8(0);
                    body.put_u32(msg.len() as u32);
                    body.extend_from_slice(&msg);
                    body.put_u8(0x02); // END_STREAM
                    body.put_u32(2);
                    body.extend_from_slice(b"{}");
                    Ok(Response::builder()
                        .status(200)
                        .header("content-type", "application/connect+proto")
                        .body(http_body_util::Full::new(body.freeze()))
                        .unwrap())
                })
            }
        }

        // An unregistered request compression makes envelope encoding fail
        // (the message must exceed the compression min-size to be attempted).
        let config =
            ClientConfig::new("http://localhost/".parse().unwrap()).compress_requests("bogus");
        let client = EchoServiceClient::new(SwallowingTransport, config);

        let err = client
            .client_stream(connectrpc::stream_iter([EchoRequest {
                data: "x".repeat(8 * 1024),
                ..Default::default()
            }]))
            .await
            .expect_err("encode failure must surface despite the 200 response");
        assert_eq!(err.code, connectrpc::ErrorCode::Unimplemented);
        assert!(
            err.message
                .as_deref()
                .unwrap()
                .contains("unsupported compression"),
            "unexpected error: {err:?}"
        );
    }

    /// Regression: a server that ends the RPC while the request stream is
    /// still pending must surface its response instead of waiting for the
    /// next request message. The transport owns the polling of the
    /// stream-backed request body, so when the call ends it simply stops
    /// polling; a library-side pump loop awaiting the next item would hang
    /// this call forever (no timeout is set).
    #[tokio::test]
    async fn client_stream_early_server_response_unblocks_pending_stream() {
        use bytes::Bytes;
        use connectrpc::client::{BoxFuture, ClientBody, ClientTransport};
        use http::{Request, Response};

        /// Fails the call immediately, without consuming the request body —
        /// the shape of a server rejecting the RPC mid-upload.
        #[derive(Clone)]
        struct ImmediateErrorTransport;

        impl ClientTransport for ImmediateErrorTransport {
            type ResponseBody = http_body_util::Empty<Bytes>;
            type Error = ConnectError;

            fn send(
                &self,
                _request: Request<ClientBody>,
            ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
                Box::pin(async { Err(ConnectError::unavailable("rejected mid-upload")) })
            }
        }

        let config = ClientConfig::new("http://localhost/".parse().unwrap());
        let client = EchoServiceClient::new(ImmediateErrorTransport, config);

        let call = client.client_stream(futures::stream::pending::<EchoRequest>());
        let err = tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .expect("call must return once the transport responds, not await the stream")
            .expect_err("transport error must surface");
        assert_eq!(err.code, connectrpc::ErrorCode::Unavailable);
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
        assert_eq!(resp.view().data, "mono-unary");

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
        let resp = client
            .client_stream(futures::stream::iter(messages))
            .await
            .unwrap();
        assert_eq!(resp.view().sequence, 4);

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

    /// Test echo service whose `server_stream` yields `PreEncoded` items —
    /// the handler builds and encodes per-item views (here from owned data,
    /// but the lifetime story is the same as borrowing from a snapshot:
    /// the view is encoded inside the stream body, not returned).
    struct PreEncodedEchoService;

    impl EchoService for PreEncodedEchoService {
        async fn echo(
            &self,
            _ctx: RequestContext,
            request: ServiceRequest<'_, EchoRequest>,
        ) -> ServiceResult<EchoResponse> {
            // Borrowed request view: plain field access, with the borrow tied
            // to the dispatcher-owned request body — no owned round-trip.
            Response::ok(EchoResponse {
                sequence: request.sequence,
                data: request.data.to_owned(),
                ..Default::default()
            })
        }

        async fn server_stream(
            &self,
            _ctx: RequestContext,
            request: ServiceRequest<'_, EchoRequest>,
        ) -> ServiceResult<ServiceStream<connectrpc::PreEncoded<EchoResponse>>> {
            let count = request.sequence;
            let stream = futures::stream::unfold(0, move |i| async move {
                if i >= count {
                    return None;
                }
                // Build a per-item view, encode it while the borrowed `data`
                // is in scope, yield only the bytes. This is the pattern a
                // handler that borrows from a local store snapshot uses.
                let data = format!("pre-encoded-{i}");
                let view = EchoResponseView {
                    sequence: i,
                    data: &data,
                    ..Default::default()
                };
                let item = connectrpc::PreEncoded::from_view(&view);
                Some((Ok(item), i + 1))
            });
            Response::stream_ok(stream)
        }

        async fn client_stream(
            &self,
            _ctx: RequestContext,
            mut requests: ServiceStream<StreamMessage<EchoRequest>>,
        ) -> ServiceResult<EchoResponse> {
            let mut count = 0i32;
            while requests.next().await.is_some() {
                count += 1;
            }
            Response::ok(EchoResponse {
                sequence: count,
                data: String::new(),
                ..Default::default()
            })
        }

        async fn bidi_stream(
            &self,
            _ctx: RequestContext,
            mut requests: ServiceStream<StreamMessage<EchoRequest>>,
        ) -> ServiceResult<ServiceStream<connectrpc::PreEncoded<EchoResponse>>> {
            // Echo each request back as a `PreEncoded` item — same
            // build-view-encode-yield-bytes pattern as `server_stream`,
            // exercising the bidi path through `BidiStreamingViewHandlerWrapper`.
            let (tx, rx) = tokio::sync::mpsc::channel::<
                Result<connectrpc::PreEncoded<EchoResponse>, ConnectError>,
            >(1);
            tokio::spawn(async move {
                while let Some(req) = requests.next().await {
                    match req {
                        Ok(req) => {
                            let data = format!("bidi-{}", req.view().data);
                            let view = EchoResponseView {
                                sequence: req.view().sequence,
                                data: &data,
                                ..Default::default()
                            };
                            let item = connectrpc::PreEncoded::from_view(&view);
                            if tx.send(Ok(item)).await.is_err() {
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

    /// Integration test for [`PreEncoded`](connectrpc::PreEncoded) stream
    /// items: handler → dispatcher → wire frames → client decode. This is
    /// the path the `type Item: Encodable<Res>` change to streaming
    /// handlers unlocks — the unit tests in `handler.rs` cover
    /// `encode_body_stream` in isolation; this exercises the full
    /// `ServerStreamingViewHandlerWrapper` → `EchoServiceServer` →
    /// `ConnectRpcService` chain.
    #[tokio::test]
    async fn server_stream_pre_encoded_round_trip() {
        let server = EchoServiceServer::new(PreEncodedEchoService);
        let service = ConnectRpcService::new(server);
        let app = axum::Router::new().fallback_service(service);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = make_client(addr);
        let mut stream = client
            .server_stream(EchoRequest {
                sequence: 3,
                data: String::new(),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut got = Vec::new();
        while let Some(msg) = stream.message().await.unwrap() {
            got.push((msg.view().sequence, msg.view().data.to_string()));
        }
        assert_eq!(
            got,
            vec![
                (0, "pre-encoded-0".to_string()),
                (1, "pre-encoded-1".to_string()),
                (2, "pre-encoded-2".to_string()),
            ]
        );
    }

    /// Same as [`server_stream_pre_encoded_round_trip`] but for bidi —
    /// covers the `BidiStreamingViewHandlerWrapper` → `EchoServiceServer`
    /// → `ConnectRpcService` chain with `type Item = PreEncoded<…>`. Uses
    /// the same raw-envelope client as
    /// [`bidi_stream_server_echoes_messages`] (the generated `BidiStream`
    /// stub needs HTTP/2; this exercises the server side over HTTP/1.1).
    #[tokio::test]
    async fn bidi_stream_pre_encoded_round_trip() {
        use buffa::Message;
        use bytes::{BufMut, BytesMut};

        let server = EchoServiceServer::new(PreEncodedEchoService);
        let service = ConnectRpcService::new(server);
        let app = axum::Router::new().fallback_service(service);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let messages: Vec<EchoRequest> = (0..3)
            .map(|i| EchoRequest {
                sequence: i,
                data: format!("m{i}"),
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

        use http_body_util::BodyExt;
        let response_body = response.into_body().collect().await.unwrap().to_bytes();
        let mut cursor = &response_body[..];
        let mut got = Vec::new();
        while cursor.len() >= 5 {
            let flags = cursor[0];
            let len = u32::from_be_bytes([cursor[1], cursor[2], cursor[3], cursor[4]]) as usize;
            cursor = &cursor[5..];
            if flags & 0x02 != 0 {
                cursor = &cursor[len..];
                continue;
            }
            let data = &cursor[..len];
            cursor = &cursor[len..];
            let resp = EchoResponse::decode_from_slice(data).unwrap();
            got.push((resp.sequence, resp.data));
        }
        assert_eq!(
            got,
            vec![
                (0, "bidi-m0".to_string()),
                (1, "bidi-m1".to_string()),
                (2, "bidi-m2".to_string()),
            ]
        );
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

        let config = ClientConfig::new(uri.clone()).with_protocol(Protocol::Grpc);
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
        assert_eq!(msg.reborrow().sequence, 7);
        assert_eq!(msg.reborrow().data, "h2-direct");

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
        assert_eq!(resp.view().data, "h2-lazy");

        // Concurrent requests on the shared handle — exercises the Buffer.
        let mut handles = Vec::new();
        for i in 0..16 {
            let c = shared.clone();
            let cfg: ClientConfig = ClientConfig::new(uri.clone()).with_protocol(Protocol::Grpc);
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
            assert_eq!(msg.reborrow().sequence, i as i32);
        }
    }

    /// Regression test for the half-duplex deadlock on `SharedHttp2Connection`.
    ///
    /// Before the fix, `call_bidi_stream` stored the transport's `send()`
    /// future unpolled in the receive state machine, so the HTTP request never
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
        let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
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
                assert_eq!(
                    resp.view().data,
                    format!("half-duplex-{}", resp.view().sequence)
                );
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
        assert_eq!(msg.reborrow().sequence, 1);
        assert_eq!(msg.reborrow().data, "tls-hello");
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

        let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
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
        assert_eq!(msg.reborrow().sequence, 2);
        assert_eq!(msg.reborrow().data, "tls-h2-hello");
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
    /// ClientHello should be disconnected after `with_tls_handshake_timeout`.
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
            .with_tls_handshake_timeout(handshake_timeout);
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

    /// Echo service whose unary handler reflects `ctx.spec()` and `ctx.protocol()`
    /// back through the response `data` field, so an e2e test can assert the
    /// runtime threaded them through.
    struct SpecReflectingService;

    impl EchoService for SpecReflectingService {
        async fn echo(
            &self,
            ctx: RequestContext,
            request: ServiceRequest<'_, EchoRequest>,
        ) -> ServiceResult<EchoResponse> {
            let proto = ctx
                .protocol()
                .map(|p| format!("{p:?}"))
                .unwrap_or_else(|| "<none>".into());
            let data = match ctx.spec() {
                Some(s) => format!(
                    "{}|{:?}|{proto}|{:?}|{:?}",
                    s.procedure, s.stream_type, s.idempotency_level, s.origin
                ),
                None => format!("<none>|{proto}"),
            };
            Response::ok(EchoResponse {
                sequence: request.sequence,
                data,
                ..Default::default()
            })
        }

        async fn server_stream(
            &self,
            _ctx: RequestContext,
            _request: ServiceRequest<'_, EchoRequest>,
        ) -> ServiceResult<ServiceStream<EchoResponse>> {
            unimplemented!()
        }

        async fn client_stream(
            &self,
            _ctx: RequestContext,
            _requests: ServiceStream<StreamMessage<EchoRequest>>,
        ) -> ServiceResult<EchoResponse> {
            unimplemented!()
        }

        async fn bidi_stream(
            &self,
            _ctx: RequestContext,
            _requests: ServiceStream<StreamMessage<EchoRequest>>,
        ) -> ServiceResult<ServiceStream<EchoResponse>> {
            unimplemented!()
        }
    }

    /// End-to-end: a registered interceptor wraps the unary call. The
    /// interceptor sees `path()`, `Spec`, the request payload, and can
    /// rewrite the response before it hits the wire. Also pins the
    /// invariant that `path()` and `spec().procedure` agree when both are
    /// present (codegen dispatch is the only path where both are).
    #[tokio::test]
    async fn interceptor_wraps_unary_call() {
        use connectrpc::interceptor::{UnaryRequest, UnaryResponse};
        use connectrpc::{Interceptor, Next};

        /// Reads `path()` and `Spec`, echoes the path and request `data`
        /// field through response headers, and increments the response
        /// message's `sequence` field.
        struct SpecAndBodyInterceptor;

        #[connectrpc::async_trait]
        impl Interceptor for SpecAndBodyInterceptor {
            async fn intercept_unary(
                &self,
                req: UnaryRequest,
                next: Next<'_>,
            ) -> Result<UnaryResponse, ConnectError> {
                // `path()` is the wire truth; `spec().procedure` is the
                // resolved registration. The codegen dispatcher supplies
                // both and they must agree — pin that invariant here, since
                // this is the only test where both are present.
                let path = req
                    .ctx
                    .path()
                    .expect("dispatch sets path before interceptors run")
                    .to_owned();
                assert_eq!(
                    req.ctx.spec().map(|s| s.procedure),
                    Some(path.as_str()),
                    "Spec::procedure and RequestContext::path() must agree"
                );
                let body = req.payload.message::<EchoRequest>()?.data.clone();
                let mut resp = next.run(req).await?;
                let mut msg = resp.body.message::<EchoResponse>()?.clone();
                msg.sequence += 1000;
                resp.body.set_message(msg);
                Ok(resp
                    .with_header("x-path", path)
                    .with_header("x-req-data", body))
            }
        }

        let server = EchoServiceServer::new(TestEchoService);
        let service = ConnectRpcService::new(server).with_interceptor(SpecAndBodyInterceptor);
        let app = axum::Router::new().fallback_service(service);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _h = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let resp = make_client(addr)
            .echo(EchoRequest {
                sequence: 7,
                data: "ping".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            resp.headers().get("x-path").unwrap(),
            "/test.echo.v1.EchoService/Echo"
        );
        assert_eq!(resp.headers().get("x-req-data").unwrap(), "ping");
        // The handler echoes `sequence`; the interceptor adds 1000.
        assert_eq!(resp.view().sequence, 1007);
    }

    /// End-to-end: a registered streaming interceptor wraps server-streaming,
    /// client-streaming, and bidi calls. Each shape routes through
    /// `intercept_streaming` exactly once at stream establishment, sees
    /// `path()`, and can attach response metadata. An auth interceptor
    /// running this hook is the security boundary for streaming RPCs — a
    /// bypassed shape is a vulnerability, not a gap.
    #[tokio::test]
    async fn interceptor_wraps_streaming_calls() {
        use connectrpc::interceptor::{StreamRequest, StreamResponse};
        use connectrpc::{Interceptor, NextStream, PayloadStream};
        use std::sync::Mutex;

        /// Records the path of every streaming RPC it sees and stamps a
        /// response header so the client can assert the interceptor ran.
        #[derive(Clone)]
        struct StreamRecorder(Arc<Mutex<Vec<String>>>);

        #[connectrpc::async_trait]
        impl Interceptor for StreamRecorder {
            async fn intercept_streaming(
                &self,
                req: StreamRequest,
                inbound: PayloadStream,
                next: NextStream<'_>,
            ) -> Result<StreamResponse, ConnectError> {
                let path = req
                    .ctx
                    .path()
                    .expect("dispatch sets path before interceptors run")
                    .to_owned();
                self.0.lock().unwrap().push(path.clone());
                let resp = next.run(req, inbound).await?;
                Ok(resp.with_header("x-stream-intercepted", path))
            }
        }

        let recorder = StreamRecorder(Arc::new(Mutex::new(Vec::new())));
        let server = EchoServiceServer::new(TestEchoService);
        let service = ConnectRpcService::new(server).with_interceptor(recorder.clone());
        let app = axum::Router::new().fallback_service(service);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _h = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = make_client(addr);

        // Server streaming: 1 request → N responses.
        let mut stream = client
            .server_stream(EchoRequest {
                sequence: 3,
                data: "ss".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            stream.headers().get("x-stream-intercepted").unwrap(),
            "/test.echo.v1.EchoService/ServerStream"
        );
        let mut received = 0;
        while stream.message().await.unwrap().is_some() {
            received += 1;
        }
        assert_eq!(
            received, 3,
            "stream items must flow through the interceptor"
        );

        // Client streaming: N requests → 1 response.
        let messages: Vec<EchoRequest> = (0..2)
            .map(|i| EchoRequest {
                sequence: i,
                data: format!("cs-{i}"),
                ..Default::default()
            })
            .collect();
        let resp = client
            .client_stream(futures::stream::iter(messages))
            .await
            .unwrap();
        assert_eq!(
            resp.headers().get("x-stream-intercepted").unwrap(),
            "/test.echo.v1.EchoService/ClientStream"
        );
        assert_eq!(resp.view().sequence, 2, "all items must reach the handler");

        // Bidi streaming: N requests ↔ N responses (gRPC, full duplex).
        {
            use connectrpc::Protocol;
            use connectrpc::client::Http2Connection;
            let uri: http::Uri = format!("http://{addr}").parse().unwrap();
            let conn = Http2Connection::connect_plaintext(uri.clone())
                .await
                .unwrap()
                .shared(8);
            let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
            let bidi_client = EchoServiceClient::new(conn, config);
            let mut bidi = bidi_client.bidi_stream().await.unwrap();
            bidi.send(EchoRequest {
                sequence: 1,
                data: "bidi".into(),
                ..Default::default()
            })
            .await
            .unwrap();
            bidi.close_send();
            let mut received = 0;
            while bidi.message().await.unwrap().is_some() {
                received += 1;
            }
            assert_eq!(received, 1);
        }

        // The interceptor saw all three streaming RPCs.
        let seen = recorder.0.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![
                "/test.echo.v1.EchoService/ServerStream",
                "/test.echo.v1.EchoService/ClientStream",
                "/test.echo.v1.EchoService/BidiStream",
            ]
        );
    }

    /// A streaming interceptor that returns `Err` at establishment must
    /// short-circuit before the handler runs, and the client must receive a
    /// structured error in the streaming wire format — not a transport
    /// failure.
    #[tokio::test]
    async fn streaming_interceptor_short_circuit_reaches_client() {
        use connectrpc::interceptor::{StreamRequest, StreamResponse};
        use connectrpc::{Interceptor, NextStream, PayloadStream};

        struct DenyAll;
        #[connectrpc::async_trait]
        impl Interceptor for DenyAll {
            async fn intercept_streaming(
                &self,
                _req: StreamRequest,
                _inbound: PayloadStream,
                _next: NextStream<'_>,
            ) -> Result<StreamResponse, ConnectError> {
                Err(ConnectError::permission_denied("not authorized"))
            }
        }

        let server = EchoServiceServer::new(TestEchoService);
        let service = ConnectRpcService::new(server).with_interceptor(DenyAll);
        let app = axum::Router::new().fallback_service(service);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _h = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = make_client(addr);

        // Server streaming: the deny is rendered as an EndStreamResponse
        // envelope (HTTP 200, Connect protocol). The client returns it from
        // `message()` — `Ok(None)` is reserved for a clean end.
        let mut stream = client
            .server_stream(EchoRequest {
                sequence: 1,
                ..Default::default()
            })
            .await
            .expect("Connect-streaming errors arrive via the envelope, not the headers");
        let err = stream
            .message()
            .await
            .expect_err("interceptor deny must surface from message()");
        assert_eq!(err.code, connectrpc::ErrorCode::PermissionDenied);

        // Client streaming: the response is unary-shaped over a streaming
        // wire, so the error surfaces from the call itself.
        let err = client
            .client_stream(futures::stream::iter(vec![EchoRequest::default()]))
            .await
            .expect_err("expected deny");
        assert_eq!(err.code, connectrpc::ErrorCode::PermissionDenied);
    }

    /// End-to-end: both dispatch paths surface `Spec` and `Protocol` on
    /// `RequestContext`. The codegen `FooServiceServer<T>` always did;
    /// the dynamic `Router` now does too because the generated
    /// `register()` chains `.with_spec(...)` after every route. Handlers
    /// and interceptors should see identical metadata regardless of which
    /// dispatch path the host wired up.
    #[tokio::test]
    async fn handler_sees_spec_and_protocol() {
        async fn round_trip(addr: std::net::SocketAddr) -> String {
            make_client(addr)
                .echo(EchoRequest {
                    sequence: 1,
                    data: String::new(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_view()
                .reborrow()
                .data
                .to_owned()
        }
        const EXPECT: &str = "/test.echo.v1.EchoService/Echo|Unary|Connect|Unknown|Server";

        // Codegen dispatcher path → spec is populated.
        let server = EchoServiceServer::new(SpecReflectingService);
        let service = ConnectRpcService::new(server);
        let app = axum::Router::new().fallback_service(service);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _h = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        assert_eq!(round_trip(addr).await, EXPECT);

        // Dynamic Router path → spec is also populated (same Spec const,
        // attached via `Router::with_spec` in the generated `register()`).
        let router = Arc::new(SpecReflectingService).register(Router::new());
        let app = router.into_axum_router();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _h = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        assert_eq!(
            round_trip(addr).await,
            EXPECT,
            "Router path must surface the same Spec as the codegen dispatcher"
        );
    }

    /// Codegen emits a per-method `Spec` const and *both* generated
    /// dispatch paths surface it through `Dispatcher::lookup`: the
    /// monomorphic `FooServiceServer<T>` and the dynamic `Router` (which
    /// gets it via `Router::with_spec` chained in `register()`).
    #[test]
    fn codegen_specs_thread_through_lookup() {
        use connectrpc::{Dispatcher, IdempotencyLevel, MethodKind, SpecOrigin, StreamType};

        // The codegen const carries the proto-level metadata.
        assert_eq!(
            ECHO_SERVICE_ECHO_SPEC.procedure,
            "/test.echo.v1.EchoService/Echo"
        );
        assert_eq!(ECHO_SERVICE_ECHO_SPEC.stream_type, StreamType::Unary);
        assert_eq!(
            ECHO_SERVICE_ECHO_SPEC.idempotency_level,
            IdempotencyLevel::Unknown
        );
        const { assert!(matches!(ECHO_SERVICE_ECHO_SPEC.origin, SpecOrigin::Server)) };
        assert_eq!(ECHO_SERVICE_ECHO_SPEC.service(), "test.echo.v1.EchoService");
        assert_eq!(ECHO_SERVICE_ECHO_SPEC.method(), "Echo");
        assert_eq!(
            ECHO_SERVICE_BIDI_STREAM_SPEC.stream_type,
            StreamType::BidiStream
        );
        assert_eq!(
            ECHO_SERVICE_CLIENT_STREAM_SPEC.stream_type,
            StreamType::ClientStream
        );
        assert_eq!(
            ECHO_SERVICE_SERVER_STREAM_SPEC.stream_type,
            StreamType::ServerStream
        );

        // Both dispatchers' `lookup` return the same spec; the spec's
        // procedure round-trips through the route they matched.
        let server = EchoServiceServer::new(TestEchoService);
        let router = Arc::new(TestEchoService).register(Router::new());
        for (method, spec) in [
            ("Echo", ECHO_SERVICE_ECHO_SPEC),
            ("ServerStream", ECHO_SERVICE_SERVER_STREAM_SPEC),
            ("ClientStream", ECHO_SERVICE_CLIENT_STREAM_SPEC),
            ("BidiStream", ECHO_SERVICE_BIDI_STREAM_SPEC),
        ] {
            let path = format!("test.echo.v1.EchoService/{method}");
            for (label, dispatcher) in [
                ("FooServiceServer", &server as &dyn Dispatcher),
                ("Router", &router as &dyn Dispatcher),
            ] {
                let desc = dispatcher.lookup(&path).expect("registered method");
                assert_eq!(
                    desc.spec,
                    Some(spec),
                    "{label}::lookup({path}) spec mismatch"
                );
                assert_eq!(MethodKind::from(spec.stream_type), desc.kind);
                assert_eq!(spec.procedure.trim_start_matches('/'), path);
            }
        }
        assert!(server.lookup("test.echo.v1.EchoService/Nope").is_none());
        assert!(router.lookup("test.echo.v1.EchoService/Nope").is_none());
    }
}
