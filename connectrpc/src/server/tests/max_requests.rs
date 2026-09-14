//! Connection retirement by request count.

use super::*;

#[tokio::test(start_paused = true)]
async fn max_requests_per_connection_retires_h2_after_limit() {
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_requests_per_connection(NonZeroU64::new(2).unwrap());
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

    // First request stays under the limit: the connection must remain open.
    send_unary(&mut send_request, addr).await;
    yield_to_tasks().await;
    assert!(
        !h2_task.is_finished(),
        "connection retired before reaching the request limit"
    );

    // Second request reaches the limit and triggers a GOAWAY.
    send_unary(&mut send_request, addr).await;
    yield_to_tasks().await;
    assert!(
        h2_task.is_finished(),
        "connection did not retire after reaching the request limit"
    );
    let conn_result = h2_task.await.expect("h2 connection task panicked");
    if let Err(err) = conn_result {
        assert!(
            err.is_go_away(),
            "h2 connection ended with non-GOAWAY error: {err:?}"
        );
    }

    drop(send_request);
    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test(start_paused = true)]
async fn max_requests_per_connection_unlimited_when_unset() {
    // No request limit configured: the connection serves many requests
    // without being retired.
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

    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
    let h2_task = tokio::spawn(h2_conn);

    for _ in 0..5 {
        send_unary(&mut send_request, addr).await;
    }
    yield_to_tasks().await;
    assert!(
        !h2_task.is_finished(),
        "connection retired despite no request limit being configured"
    );

    drop(send_request);
    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test(start_paused = true)]
async fn max_requests_per_connection_first_trigger_wins_over_age() {
    // A far-off max age combined with a request limit of one: the request
    // count must retire the connection first.
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_connection_age(Duration::from_secs(3600))
        .with_max_requests_per_connection(NonZeroU64::new(1).unwrap());
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

    send_unary(&mut send_request, addr).await;
    yield_to_tasks().await;
    assert!(
        h2_task.is_finished(),
        "request limit should retire the connection before the max age"
    );
    h2_task.await.expect("h2 connection task panicked").ok();

    drop(send_request);
    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test(start_paused = true)]
async fn max_requests_per_connection_retires_http1_after_limit() {
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
        .with_max_requests_per_connection(NonZeroU64::new(1).unwrap());
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
    stream.write_all(KEEPALIVE_ECHO_REQ).await.unwrap();
    let resp = read_http1_response(&mut stream).await;
    assert!(
        resp.starts_with(b"HTTP/1.1 2"),
        "expected 2xx, got: {}",
        String::from_utf8_lossy(&resp[..resp.len().min(80)])
    );

    yield_to_tasks().await;
    // The single request hit the limit, so the keep-alive connection must
    // close even though the client requested keep-alive.
    let mut buf = [0; 1];
    let read = stream.read(&mut buf).await.unwrap();
    assert_eq!(
        read, 0,
        "HTTP/1.1 keep-alive connection stayed open past the request limit"
    );

    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}
