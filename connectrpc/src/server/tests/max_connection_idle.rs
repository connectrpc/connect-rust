//! Connection retirement by idleness.

use super::*;

#[test]
fn connection_activity_tracks_in_flight_and_epoch() {
    let activity = ConnectionActivity::default();
    assert_eq!(activity.snapshot(), (0, 0));

    let guard = ActiveRequestGuard::new(Arc::new(ConnectionActivity::default()));
    // The guard owns its own activity; exercise the shared-Arc path too.
    drop(guard);

    let shared = Arc::new(ConnectionActivity::default());
    let g1 = ActiveRequestGuard::new(Arc::clone(&shared));
    let g2 = ActiveRequestGuard::new(Arc::clone(&shared));
    // Two starts: in_flight == 2, epoch bumped twice.
    assert_eq!(shared.snapshot(), (2, 2));
    drop(g1);
    // One completion: in_flight back to 1, epoch bumped again.
    assert_eq!(shared.snapshot(), (1, 3));
    drop(g2);
    assert_eq!(shared.snapshot(), (0, 4));
}

#[tokio::test(start_paused = true)]
async fn max_connection_idle_reaps_quiet_connection() {
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_connection_idle(Duration::from_secs(10));
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

    // One request, then the connection goes quiet. The empty router replies
    // 415 (no Content-Type), but any response establishes activity.
    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Unknown"))
        .body(())
        .unwrap();
    let (resp, _) = send_request.send_request(req, true).unwrap();
    resp.await.unwrap();

    // The request fell inside the first idle window, so that window resets
    // rather than reaping: the connection must survive it.
    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    assert!(
        !h2_task.is_finished(),
        "connection reaped despite activity within the idle window"
    );

    // A second, fully quiet window elapses: now the connection is reaped.
    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    assert!(
        h2_task.is_finished(),
        "idle connection was not reaped after a quiet window"
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
async fn max_connection_idle_inflight_request_prevents_reaping() {
    let (router, entered_rx, release_tx) = slow_router();
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_connection_idle(Duration::from_secs(10));
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

    // A request is in flight the whole time, so the connection is never
    // idle even though the idle timeout elapses several times over.
    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    assert!(
        !h2_task.is_finished(),
        "connection with an in-flight request was retired by the idle timer"
    );
    assert!(
        !resp_task.is_finished(),
        "in-flight request unexpectedly ended"
    );

    // Let the handler finish; the connection then goes quiet and is reaped.
    release_tx.send(()).unwrap();
    yield_to_tasks().await;
    let resp = resp_task
        .await
        .expect("response task panicked")
        .expect("h2 request failed");
    assert!(resp.status().is_success(), "got status {}", resp.status());
    drain_h2_body(resp).await;

    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    assert!(
        h2_task.is_finished(),
        "connection was not reaped after the in-flight request completed"
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
async fn max_connection_idle_fires_before_a_longer_max_age() {
    // Idle (10s) is shorter than age (60s): a quiet connection is retired by
    // the idle timer well before it would reach max age.
    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_max_connection_age(Duration::from_secs(60))
        .with_max_connection_idle(Duration::from_secs(10));
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

    // Two quiet idle windows (22s total) is far short of the 60s max age
    // (even with +10% jitter), so any retirement here is the idle timer.
    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    assert!(
        h2_task.is_finished(),
        "idle timer did not retire the connection before max age"
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
