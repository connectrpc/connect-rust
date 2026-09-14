//! Connection retirement by age.

use super::*;

#[test]
fn max_connection_age_jitter_stays_within_bounds() {
    let samples = [0, 1, u64::MAX / 2, u64::MAX - 1, u64::MAX];
    let ages = [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::from_secs(10),
        Duration::MAX,
    ];

    assert_eq!(
        jitter_connection_age(Duration::from_secs(10), 0),
        Duration::from_secs(9)
    );
    assert_eq!(
        jitter_connection_age(Duration::from_secs(10), u64::MAX),
        Duration::from_secs(11)
    );

    for age in ages {
        for sample in samples {
            let jittered = jitter_connection_age(age, sample);
            if age.is_zero() {
                assert_eq!(jittered, Duration::ZERO);
                continue;
            }

            assert!(
                jittered
                    .as_nanos()
                    .saturating_mul(MAX_CONNECTION_AGE_JITTER_BASIS_POINTS)
                    >= age.as_nanos().saturating_mul(
                        MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
                            - MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS
                    ),
                "{jittered:?} was below the 90% jitter bound for {age:?}"
            );
            assert!(
                jittered
                    .as_nanos()
                    .saturating_mul(MAX_CONNECTION_AGE_JITTER_BASIS_POINTS)
                    <= age.as_nanos().saturating_mul(
                        MAX_CONNECTION_AGE_JITTER_BASIS_POINTS
                            + MAX_CONNECTION_AGE_JITTER_SPREAD_BASIS_POINTS
                    ),
                "{jittered:?} was above the 110% jitter bound for {age:?}"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn max_connection_age_sends_h2_goaway_without_global_shutdown() {
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_connection_age(Duration::from_secs(10));
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
    resp.await.unwrap();

    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;

    assert!(
        h2_task.is_finished(),
        "server did not close idle h2 connection after max age"
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
async fn max_connection_age_retiring_one_connection_keeps_listener_running() {
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_connection_age(Duration::from_secs(10));
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
    resp.await.unwrap();

    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    assert!(
        h2_task.is_finished(),
        "aged connection should retire without stopping listener"
    );
    h2_task.await.expect("h2 connection task panicked").ok();
    drop(send_request);

    let second = tokio::net::TcpStream::connect(addr).await;
    assert!(
        second.is_ok(),
        "listener should still accept new connections after one ages out"
    );
    drop(second);

    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test(start_paused = true)]
async fn max_connection_age_inflight_stream_completes_during_grace() {
    let (router, entered_rx, release_tx) = slow_router();
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_connection_age(Duration::from_secs(10))
        .with_max_connection_age_grace(Duration::from_secs(5));
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
    let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
    let h2_task = tokio::spawn(h2_conn);
    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Slow"))
        .header(header::CONTENT_TYPE, "application/proto")
        .body(())
        .unwrap();
    let (resp, _) = send_request.send_request(req, true).unwrap();
    let resp_task = tokio::spawn(resp);
    entered_rx.await.unwrap();

    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    assert!(
        !resp_task.is_finished(),
        "response should remain in-flight during max-age grace"
    );

    release_tx.send(()).unwrap();
    yield_to_tasks().await;
    assert!(
        resp_task.is_finished(),
        "in-flight response did not complete during grace"
    );
    let resp = resp_task
        .await
        .expect("response task panicked")
        .expect("h2 request failed");
    assert!(resp.status().is_success(), "got status {}", resp.status());
    drain_h2_body(resp).await;

    drop(send_request);
    yield_to_tasks().await;
    assert!(
        h2_task.is_finished(),
        "h2 connection should close after graceful max-age drain"
    );
    h2_task.await.expect("h2 connection task panicked").ok();

    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test(start_paused = true)]
async fn max_connection_age_unfinished_stream_closes_after_grace() {
    let (router, entered_rx, _release_tx) = slow_router();
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_connection_age(Duration::from_secs(10))
        .with_max_connection_age_grace(Duration::from_secs(5));
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
    let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
    let h2_task = tokio::spawn(h2_conn);
    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Slow"))
        .header(header::CONTENT_TYPE, "application/proto")
        .body(())
        .unwrap();
    let (resp, _) = send_request.send_request(req, true).unwrap();
    let resp_task = tokio::spawn(resp);
    entered_rx.await.unwrap();

    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    assert!(
        !resp_task.is_finished(),
        "unfinished stream should remain open until age grace expires"
    );

    tokio::time::advance(Duration::from_secs(6)).await;
    yield_to_tasks().await;
    assert!(
        resp_task.is_finished(),
        "unfinished in-flight stream should close after age grace"
    );
    let resp_result = resp_task.await.expect("response task panicked");
    assert!(
        resp_result.is_err(),
        "unfinished stream unexpectedly completed after max-age grace"
    );

    drop(send_request);
    yield_to_tasks().await;
    assert!(
        h2_task.is_finished(),
        "h2 connection should close after max-age grace expires"
    );
    h2_task.await.expect("h2 connection task panicked").ok();

    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test(start_paused = true)]
async fn max_connection_age_http1_keep_alive_connections_retire() {
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
        .with_max_connection_age(Duration::from_secs(10));
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

    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;

    let mut buf = [0; 1];
    let read = stream.read(&mut buf).await.unwrap();
    assert_eq!(read, 0, "HTTP/1.1 keep-alive connection stayed open");

    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test(start_paused = true)]
async fn max_connection_age_grace_does_not_cap_global_shutdown() {
    let (router, entered_rx, release_tx) = slow_router();
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_connection_age(Duration::from_secs(10))
        .with_max_connection_age_grace(Duration::from_secs(1));
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
    let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
    let h2_task = tokio::spawn(h2_conn);
    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Slow"))
        .header(header::CONTENT_TYPE, "application/proto")
        .body(())
        .unwrap();
    let (resp, _) = send_request.send_request(req, true).unwrap();
    let resp_task = tokio::spawn(resp);
    entered_rx.await.unwrap();

    shutdown_tx.send(()).unwrap();
    tokio::time::advance(Duration::from_secs(30)).await;
    yield_to_tasks().await;
    assert!(
        !serve.is_finished(),
        "global shutdown should not be capped by max-age grace"
    );
    assert!(
        !resp_task.is_finished(),
        "global shutdown should keep in-flight request alive"
    );

    release_tx.send(()).unwrap();
    yield_to_tasks().await;
    assert!(
        resp_task.is_finished(),
        "in-flight response did not complete after release"
    );
    let resp = resp_task
        .await
        .expect("response task panicked")
        .expect("h2 request failed");
    assert!(resp.status().is_success(), "got status {}", resp.status());
    drain_h2_body(resp).await;

    drop(send_request);
    yield_to_tasks().await;
    h2_task.await.expect("h2 connection task panicked").ok();

    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test(start_paused = true)]
async fn max_connection_age_global_shutdown_during_age_grace_drains_indefinitely() {
    let (router, entered_rx, release_tx) = slow_router();
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_connection_age(Duration::from_secs(10))
        .with_max_connection_age_grace(Duration::from_secs(1));
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
    let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
    let h2_task = tokio::spawn(h2_conn);
    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Slow"))
        .header(header::CONTENT_TYPE, "application/proto")
        .body(())
        .unwrap();
    let (resp, _) = send_request.send_request(req, true).unwrap();
    let resp_task = tokio::spawn(resp);
    entered_rx.await.unwrap();

    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    assert!(
        !resp_task.is_finished(),
        "request should still be in-flight during age grace"
    );

    shutdown_tx.send(()).unwrap();
    tokio::time::advance(Duration::from_secs(30)).await;
    yield_to_tasks().await;
    assert!(
        !serve.is_finished(),
        "global shutdown during age grace should drain indefinitely"
    );
    assert!(
        !resp_task.is_finished(),
        "global shutdown during age grace should not force-close the request"
    );

    release_tx.send(()).unwrap();
    yield_to_tasks().await;
    assert!(
        resp_task.is_finished(),
        "in-flight response did not complete after release"
    );
    let resp = resp_task
        .await
        .expect("response task panicked")
        .expect("h2 request failed");
    assert!(resp.status().is_success(), "got status {}", resp.status());
    drain_h2_body(resp).await;

    drop(send_request);
    yield_to_tasks().await;
    h2_task.await.expect("h2 connection task panicked").ok();

    let result = tokio::time::timeout(Duration::from_secs(1), serve)
        .await
        .expect("server did not shut down")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}
