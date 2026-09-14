//! HTTP/2 settings: flow-control windows, keepalive, max concurrent streams.

use super::*;

#[test]
fn http2_config_default_enables_adaptive_window() {
    let config = Http2Config::default();
    assert!(config.adaptive_window);
    assert_eq!(config.adaptive_window, DEFAULT_HTTP2_ADAPTIVE_WINDOW);
    assert_eq!(config.initial_stream_window_size, None);
    assert_eq!(config.initial_connection_window_size, None);
}

#[test]
fn server_http2_builder_defaults_match_adaptive_on() {
    let server = Server::new(Router::new());
    assert!(server.http2.adaptive_window);
    assert_eq!(server.http2.initial_stream_window_size, None);
    assert_eq!(server.http2.initial_connection_window_size, None);

    // `from_service` must seed the same defaults as `new`.
    let from_service = Server::from_service(ConnectRpcService::new(Router::new()));
    assert!(from_service.http2.adaptive_window);
}

#[tokio::test]
async fn bound_server_http2_builder_defaults_match_adaptive_on() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = Server::from_listener(listener);
    assert!(bound.http2.adaptive_window);
    assert_eq!(bound.http2.initial_stream_window_size, None);
    assert_eq!(bound.http2.initial_connection_window_size, None);

    let bound = Server::bind("127.0.0.1:0").await.unwrap();
    assert!(bound.http2.adaptive_window);
}

#[test]
fn with_http2_adaptive_window_toggles_flag() {
    let server = Server::new(Router::new()).with_http2_adaptive_window(false);
    assert!(!server.http2.adaptive_window);

    let server = server.with_http2_adaptive_window(true);
    assert!(server.http2.adaptive_window);
}

#[test]
fn explicit_stream_window_disables_adaptive() {
    let server = Server::new(Router::new()).with_http2_initial_stream_window_size(1 << 20);
    assert_eq!(server.http2.initial_stream_window_size, Some(1 << 20));
    assert!(
        !server.http2.adaptive_window,
        "an explicit stream window must turn adaptive sizing off"
    );
}

#[test]
fn explicit_connection_window_disables_adaptive() {
    let server = Server::new(Router::new()).with_http2_initial_connection_window_size(2 << 20);
    assert_eq!(server.http2.initial_connection_window_size, Some(2 << 20));
    assert!(
        !server.http2.adaptive_window,
        "an explicit connection window must turn adaptive sizing off"
    );
}

#[test]
fn clearing_window_with_none_keeps_adaptive_flag() {
    // Passing `None` must not flip the adaptive flag in either direction.
    let server = Server::new(Router::new())
        .with_http2_initial_stream_window_size(None)
        .with_http2_initial_connection_window_size(None);
    assert!(server.http2.adaptive_window);
    assert_eq!(server.http2.initial_stream_window_size, None);
    assert_eq!(server.http2.initial_connection_window_size, None);
}

#[test]
fn re_enabling_adaptive_after_explicit_window_wins() {
    // The setters are last-write-wins: re-enabling adaptive after setting a
    // window leaves the window stored but turns adaptive back on, matching
    // the documented precedence (and hyper, where adaptive overrides the
    // explicit window).
    let server = Server::new(Router::new())
        .with_http2_initial_stream_window_size(1 << 20)
        .with_http2_adaptive_window(true);
    assert!(server.http2.adaptive_window);
    assert_eq!(server.http2.initial_stream_window_size, Some(1 << 20));
    // ...and the stored window must not reach hyper while adaptive is on.
    assert_eq!(server.http2.effective_windows(), (None, None));
}

#[test]
fn effective_windows_resolves_adaptive_precedence() {
    // Default (adaptive on): no explicit window reaches hyper.
    assert_eq!(Http2Config::default().effective_windows(), (None, None));

    // Adaptive explicitly off but no sizes set: still nothing to apply.
    let off = Server::new(Router::new()).with_http2_adaptive_window(false);
    assert_eq!(off.http2.effective_windows(), (None, None));

    // Adaptive off with explicit sizes: both windows are applied.
    let fixed = Server::new(Router::new())
        .with_http2_initial_stream_window_size(1 << 20)
        .with_http2_initial_connection_window_size(2 << 20);
    assert!(!fixed.http2.adaptive_window);
    assert_eq!(
        fixed.http2.effective_windows(),
        (Some(1 << 20), Some(2 << 20))
    );
}

#[tokio::test]
async fn bound_server_http2_window_setters_thread_through() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = Server::from_listener(listener)
        .with_http2_initial_stream_window_size(512 * 1024)
        .with_http2_initial_connection_window_size(1024 * 1024);
    assert_eq!(bound.http2.initial_stream_window_size, Some(512 * 1024));
    assert_eq!(
        bound.http2.initial_connection_window_size,
        Some(1024 * 1024)
    );
    assert!(!bound.http2.adaptive_window);
}

/// End-to-end check that explicit window knobs reach hyper's builder
/// (`configure_http2`) without breaking the connection: a server with
/// custom stream/connection windows still completes an HTTP/2 request.
#[tokio::test]
async fn http2_explicit_windows_serve_request() {
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_http2_initial_stream_window_size(256 * 1024)
        .with_http2_initial_connection_window_size(512 * 1024);
    let addr = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(Router::new(), async {
                shutdown_rx.await.ok();
            })
            .await
    });

    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
    let h2_task = tokio::spawn(h2_conn);

    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Unknown"))
        .body(())
        .unwrap();
    let (resp, _) = send_request.send_request(req, true).unwrap();
    // The route is unknown; we only need the response to resolve, which
    // proves the connection negotiated and served under the configured
    // flow-control windows.
    let _resp = resp.await.expect("h2 request failed");

    drop(send_request);
    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
    h2_task.await.expect("h2 connection task panicked").ok();
}

/// Same as above but with adaptive window left on (the default), proving the
/// default `configure_http2` path also serves requests cleanly.
#[tokio::test]
async fn http2_adaptive_window_default_serves_request() {
    let bound = Server::bind("127.0.0.1:0").await.unwrap();
    assert!(bound.http2.adaptive_window);
    let addr = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(Router::new(), async {
                shutdown_rx.await.ok();
            })
            .await
    });

    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
    let h2_task = tokio::spawn(h2_conn);

    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Unknown"))
        .body(())
        .unwrap();
    let (resp, _) = send_request.send_request(req, true).unwrap();
    // The route is unknown; we only need the response to resolve, which
    // proves the connection negotiated and served under the configured
    // flow-control windows.
    let _resp = resp.await.expect("h2 request failed");

    drop(send_request);
    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
    h2_task.await.expect("h2 connection task panicked").ok();
}

#[tokio::test]
async fn http2_keepalive_builder_defaults_and_overrides() {
    // BoundServer: disabled by default, default timeout.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = Server::from_listener(listener);
    assert_eq!(bound.http2.keepalive_interval, None);
    assert_eq!(
        bound.http2.keepalive_timeout,
        DEFAULT_HTTP2_KEEPALIVE_TIMEOUT
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = Server::from_listener(listener)
        .with_http2_keepalive_interval(Duration::from_secs(30))
        .with_http2_keepalive_timeout(Duration::from_secs(5));
    assert_eq!(
        bound.http2.keepalive_interval,
        Some(Duration::from_secs(30))
    );
    assert_eq!(bound.http2.keepalive_timeout, Duration::from_secs(5));

    // Setting only the timeout leaves keepalive disabled (no interval).
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound =
        Server::from_listener(listener).with_http2_keepalive_timeout(Duration::from_secs(1));
    assert_eq!(bound.http2.keepalive_interval, None);
    assert_eq!(bound.http2.keepalive_timeout, Duration::from_secs(1));
}

#[test]
fn server_http2_keepalive_builder_threads_through() {
    let server = Server::new(Router::new());
    assert_eq!(server.http2.keepalive_interval, None);
    assert_eq!(
        server.http2.keepalive_timeout,
        DEFAULT_HTTP2_KEEPALIVE_TIMEOUT
    );

    let server = Server::new(Router::new())
        .with_http2_keepalive_interval(Duration::from_millis(500))
        .with_http2_keepalive_timeout(Duration::from_millis(250));
    assert_eq!(
        server.http2.keepalive_interval,
        Some(Duration::from_millis(500))
    );
    assert_eq!(server.http2.keepalive_timeout, Duration::from_millis(250));
}

#[test]
#[should_panic(expected = "non-zero duration")]
fn with_http2_keepalive_interval_rejects_zero() {
    let _ = Server::new(Router::new()).with_http2_keepalive_interval(Duration::ZERO);
}

/// `configure_http2` leaves keepalive untouched when no interval is set, so
/// hyper's default (keepalive disabled) is preserved unless the user opts
/// in. There is no public getter on the builder, so this guards the opt-in
/// contract at the call boundary by exercising the default path without
/// panicking.
#[test]
fn configure_http2_default_leaves_keepalive_disabled() {
    assert!(Http2Config::default().keepalive_interval.is_none());
    let mut builder = AutoBuilder::new(TokioExecutor::new());
    configure_http2(&mut builder, Http2Config::default());
}

/// A configured keepalive interval must reach hyper's HTTP/2 builder: once
/// a peer with an active stream stops acknowledging PING frames, the server
/// closes the connection after the keepalive timeout rather than leaving it
/// half-open indefinitely.
#[tokio::test]
async fn http2_keepalive_closes_unresponsive_peer() {
    // The blocked handler keeps a stream active on the server; holding
    // `_release_tx` keeps it blocked for the whole test.
    let (router, entered_rx, _release_tx) = slow_router();
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_http2_keepalive_interval(Duration::from_millis(100))
        .with_http2_keepalive_timeout(Duration::from_millis(100));
    let addr = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(router, async {
                shutdown_rx.await.ok();
            })
            .await
    });

    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut send_request, mut h2_conn) = h2::client::handshake(tcp).await.unwrap();
    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Slow"))
        .header(header::CONTENT_TYPE, "application/proto")
        .body(())
        .unwrap();
    // Keep the response future alive so the stream stays open server-side.
    let (_resp, _) = send_request.send_request(req, true).unwrap();

    // Drive the connection only until the handler starts — this flushes the
    // request and opens an active server-side stream. After this point the
    // client never polls the connection again, so it cannot acknowledge the
    // server's keepalive PINGs, simulating a dead or half-open peer.
    tokio::select! {
        result = &mut h2_conn => panic!("connection closed before handler ran: {result:?}"),
        entered = entered_rx => entered.expect("handler never entered"),
    }

    // Stay frozen for longer than interval + timeout. The server PINGs,
    // gets no ack, and abruptly closes the connection.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Resuming the driver, the connection future must resolve: the server
    // has closed the connection. Without the keepalive being plumbed
    // through, the blocked handler and frozen client would leave it open
    // forever and this timeout would elapse.
    let closed = tokio::time::timeout(Duration::from_secs(5), &mut h2_conn).await;
    assert!(
        closed.is_ok(),
        "server did not close the unresponsive connection; keepalive PINGs were not plumbed through",
    );

    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test]
async fn max_concurrent_streams_builder_defaults_and_overrides() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = Server::from_listener(listener);
    assert_eq!(bound.http2.max_concurrent_streams, None);
    let bound = bound.with_max_concurrent_streams(64);
    assert_eq!(bound.http2.max_concurrent_streams, Some(64));

    let server = Server::new(Router::new());
    assert_eq!(server.http2.max_concurrent_streams, None);
    let server = server.with_max_concurrent_streams(64);
    assert_eq!(server.http2.max_concurrent_streams, Some(64));
}

#[test]
#[should_panic(expected = "non-zero value")]
fn with_max_concurrent_streams_rejects_zero() {
    let _ = Server::new(Router::new()).with_max_concurrent_streams(0);
}

#[tokio::test]
async fn max_concurrent_streams_is_advertised_in_settings() {
    // The server must advertise the configured limit to peers via the
    // HTTP/2 SETTINGS_MAX_CONCURRENT_STREAMS parameter. Read the server's
    // initial SETTINGS frame off the raw connection and assert its value.
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_concurrent_streams(7);
    let addr = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(Router::new(), async {
                shutdown_rx.await.ok();
            })
            .await
    });

    let advertised = read_advertised_max_concurrent_streams(addr).await;
    assert_eq!(
        advertised,
        Some(7),
        "server did not advertise the configured max_concurrent_streams",
    );

    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test]
async fn max_concurrent_streams_unset_uses_hyper_default() {
    // When unset, the value is left to hyper. hyper's HTTP/2 server
    // default is 200, so the advertised value must remain that default.
    // This deliberately tracks hyper's internal default: if a hyper bump
    // changes it (or stops advertising it), this canary fails so the doc
    // comments that quote "200" can be updated in lockstep.
    let bound = Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(Router::new(), async {
                shutdown_rx.await.ok();
            })
            .await
    });

    let advertised = read_advertised_max_concurrent_streams(addr).await;
    assert_eq!(
        advertised,
        Some(200),
        "unset max_concurrent_streams should keep hyper's default of 200",
    );

    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

/// HTTP/2 SETTINGS_MAX_CONCURRENT_STREAMS identifier (RFC 7540 §6.5.2).
const SETTINGS_MAX_CONCURRENT_STREAMS_ID: u16 = 0x3;

/// Open a raw HTTP/2 connection, send the client preface plus an empty
/// SETTINGS frame, then read the server's initial SETTINGS frame and
/// return the advertised `MAX_CONCURRENT_STREAMS` value, if present.
async fn read_advertised_max_concurrent_streams(addr: SocketAddr) -> Option<u32> {
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();

    // Client connection preface, then an empty SETTINGS frame (length 0,
    // type 0x4, flags 0, stream 0) so the server proceeds with the
    // connection.
    tcp.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    tcp.write_all(&[0, 0, 0, 0x4, 0, 0, 0, 0, 0]).await.unwrap();
    tcp.flush().await.unwrap();

    // Scan frames until the first non-ACK SETTINGS frame from the server.
    loop {
        let mut header = [0u8; 9];
        tcp.read_exact(&mut header).await.unwrap();
        let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let frame_type = header[3];
        let flags = header[4];

        let mut payload = vec![0u8; length];
        tcp.read_exact(&mut payload).await.unwrap();

        // SETTINGS = 0x4; skip the ACK (flag 0x1) the server sends for our
        // empty SETTINGS frame.
        if frame_type == 0x4 && flags & 0x1 == 0 {
            return parse_max_concurrent_streams(&payload);
        }
    }
}

/// Parse a SETTINGS frame payload (6-byte id/value entries) for the
/// `MAX_CONCURRENT_STREAMS` value.
fn parse_max_concurrent_streams(payload: &[u8]) -> Option<u32> {
    payload.chunks_exact(6).find_map(|entry| {
        let id = u16::from_be_bytes([entry[0], entry[1]]);
        (id == SETTINGS_MAX_CONCURRENT_STREAMS_ID)
            .then(|| u32::from_be_bytes([entry[2], entry[3], entry[4], entry[5]]))
    })
}
