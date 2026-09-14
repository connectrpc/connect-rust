//! HTTP/1.1 header read timeout.

use super::*;

#[test]
fn header_read_timeout_builder_defaults_and_overrides() {
    // Default is on at DEFAULT_HEADER_READ_TIMEOUT.
    let server = Server::new(Router::new());
    assert_eq!(
        server.header_read_timeout,
        Some(DEFAULT_HEADER_READ_TIMEOUT)
    );

    // An explicit value overrides the default.
    let server = Server::new(Router::new()).with_header_read_timeout(Some(Duration::from_secs(5)));
    assert_eq!(server.header_read_timeout, Some(Duration::from_secs(5)));

    // `None` disables it.
    let server = Server::new(Router::new()).with_header_read_timeout(None::<Duration>);
    assert_eq!(server.header_read_timeout, None);
}

#[tokio::test]
async fn bound_server_header_read_timeout_builder_defaults_and_overrides() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = Server::from_listener(listener);
    assert_eq!(bound.header_read_timeout, Some(DEFAULT_HEADER_READ_TIMEOUT));

    let bound = bound.with_header_read_timeout(Some(Duration::from_secs(2)));
    assert_eq!(bound.header_read_timeout, Some(Duration::from_secs(2)));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = Server::from_listener(listener).with_header_read_timeout(None::<Duration>);
    assert_eq!(bound.header_read_timeout, None);
}

/// A peer that opens a connection and sends an incomplete header block must
/// be disconnected once the header read timeout elapses, rather than
/// holding the connection (and its task and file descriptor) open forever.
#[tokio::test(start_paused = true)]
async fn header_read_timeout_closes_stalled_connection() {
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_header_read_timeout(Some(Duration::from_secs(10)));
    let addr = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(Router::new(), async {
                shutdown_rx.await.ok();
            })
            .await
    });

    // Send a partial request that never terminates the header block, so
    // hyper stays in "reading request headers" and arms the timeout.
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"POST /svc/Echo HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();

    // Relies on tokio's paused-clock auto-advance: once the connection
    // task has armed the header-read timer and parked, the runtime
    // advances to fire it. The explicit advance keeps the intent obvious.
    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;

    let mut buf = [0; 1];
    let read = stream.read(&mut buf).await.unwrap();
    assert_eq!(
        read, 0,
        "stalled connection stayed open past the header read timeout"
    );

    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

/// A complete request well within the header read timeout is served
/// normally — the timer must not interfere with healthy traffic.
#[tokio::test]
async fn header_read_timeout_allows_prompt_requests() {
    let router = Router::new().route(
        "svc",
        "Echo",
        crate::handler_fn(
            |_ctx: crate::RequestContext, _req: buffa_types::Empty| async move {
                crate::Response::ok(buffa_types::Empty::default())
            },
        ),
    );
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_header_read_timeout(Some(Duration::from_secs(30)));
    let addr = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(router, async {
                shutdown_rx.await.ok();
            })
            .await
    });

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(ECHO_REQ).await.unwrap();
    let resp = read_http1_response(&mut stream).await;
    assert!(
        resp.starts_with(b"HTTP/1.1 2"),
        "expected 2xx, got: {}",
        String::from_utf8_lossy(&resp[..resp.len().min(80)])
    );

    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}
