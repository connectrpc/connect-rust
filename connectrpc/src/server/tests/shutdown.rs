//! Whole-server graceful shutdown.

use super::*;

#[tokio::test]
async fn test_graceful_shutdown_immediate() {
    // Bind to an ephemeral port, trigger shutdown immediately,
    // verify serve returns cleanly without any connections.
    let bound = Server::bind("127.0.0.1:0").await.unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();

    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(Router::new(), async {
                rx.await.ok();
            })
            .await
    });

    // Fire the shutdown signal
    tx.send(()).unwrap();

    // Server should complete cleanly and promptly
    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("server did not shut down in time")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test]
async fn test_graceful_shutdown_drains_inflight_request() {
    // Spawn a server with a handler that blocks until released. Start a
    // request, fire shutdown, verify the server waits for that request to
    // complete (not just the connection to close).
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let chans = Arc::new(Mutex::new(Some((entered_tx, release_rx))));
    let router = Router::new().route(
        "svc",
        "Slow",
        crate::handler_fn(
            move |_ctx: crate::RequestContext, _req: buffa_types::Empty| {
                let chans = Arc::clone(&chans);
                async move {
                    let taken = chans.lock().unwrap().take();
                    if let Some((entered_tx, release_rx)) = taken {
                        entered_tx.send(()).ok();
                        release_rx.await.ok();
                    }
                    crate::Response::ok(buffa_types::Empty::default())
                }
            },
        ),
    );

    let bound = Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(router, async {
                shutdown_rx.await.ok();
            })
            .await
    });

    // Start the slow request over h2 and leave it in flight.
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
    tokio::spawn(h2_conn);
    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Slow"))
        .header(header::CONTENT_TYPE, "application/proto")
        .body(())
        .unwrap();
    let (resp_fut, _) = send_request.send_request(req, true).unwrap();
    let mut resp_fut = tokio::spawn(resp_fut);
    // Wait until the handler has actually started before firing shutdown.
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .expect("handler never entered")
        .unwrap();

    // Fire shutdown — the in-flight request must still be allowed to
    // complete.
    shutdown_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !serve.is_finished(),
        "server shut down before in-flight request completed"
    );
    assert!(!resp_fut.is_finished(), "response arrived too early");

    // Release the handler; the response should arrive and the server
    // should drain.
    release_tx.send(()).unwrap();
    let resp = tokio::time::timeout(Duration::from_secs(5), &mut resp_fut)
        .await
        .expect("response never arrived")
        .expect("join error")
        .expect("h2 request failed");
    assert!(resp.status().is_success(), "got status {}", resp.status());

    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("server did not shut down after in-flight request drained")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");
}

#[tokio::test]
async fn test_graceful_shutdown_rejects_new_connections() {
    // After shutdown signal, new connection attempts should fail.
    let bound = Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();

    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(Router::new(), async {
                rx.await.ok();
            })
            .await
    });

    // Give the server a moment to start the accept loop
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Trigger shutdown
    tx.send(()).unwrap();

    // Wait for serve to complete
    tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // Now a new connection should fail (listener was dropped)
    let connect_result = tokio::net::TcpStream::connect(addr).await;
    assert!(
        connect_result.is_err(),
        "expected connection refused after shutdown"
    );
}

#[tokio::test]
async fn test_graceful_shutdown_sends_h2_goaway() {
    // Regression: on graceful shutdown the server must send HTTP/2 GOAWAY
    // to existing connections so clients learn to stop sending new streams
    // and the server can drain promptly. Prior behaviour just dropped the
    // listener and waited, leaving idle h2 connections open until the
    // client hung up.
    let bound = Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();

    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(Router::new(), async {
                rx.await.ok();
            })
            .await
    });

    // Establish a raw HTTP/2 connection (prior-knowledge, no TLS).
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut send_request, h2_conn) = h2::client::handshake(tcp).await.unwrap();
    let h2_task = tokio::spawn(h2_conn);

    // Round-trip a request to prove the h2 connection is fully established
    // on the server side before we fire the shutdown signal. The router is
    // empty so this errors (415, no Content-Type), but any response will do.
    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Unknown"))
        .body(())
        .unwrap();
    let (resp, _) = send_request.send_request(req, true).unwrap();
    resp.await.unwrap();

    // Fire shutdown.
    tx.send(()).unwrap();

    // Expectation: the server sends GOAWAY(NO_ERROR) on this connection.
    // The h2 client surfaces that on the connection task and on subsequent
    // SendRequest readiness. We assert on the connection task: it must
    // complete (server closed cleanly after GOAWAY) within the timeout,
    // without us dropping our end first.
    let conn_result = tokio::time::timeout(Duration::from_secs(2), h2_task)
        .await
        .expect("server did not close idle h2 connection (no GOAWAY?)")
        .expect("h2 connection task panicked");
    if let Err(e) = conn_result {
        assert!(
            e.is_go_away(),
            "h2 connection ended with non-GOAWAY error: {e:?}"
        );
    }

    // And the server itself should now drain promptly — the only open
    // connection has been closed via GOAWAY.
    let result = tokio::time::timeout(Duration::from_secs(2), serve)
        .await
        .expect("server did not shut down after GOAWAY drained the connection")
        .expect("join error");
    assert!(result.is_ok(), "serve returned error: {result:?}");

    // Keep send_request alive until here so the client doesn't initiate
    // close before the server gets a chance to GOAWAY.
    drop(send_request);
}

/// Connections are owned by the serving future: dropping it (here, aborting
/// the task that runs it) closes them, in-flight request or not, rather than
/// leaving them running detached.
#[tokio::test]
async fn dropping_the_server_future_aborts_live_connections() {
    let (router, entered_rx, _release_tx) = slow_router();
    let bound = Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    let serve = tokio::spawn(async move { bound.serve(router).await });

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
    let resp = tokio::spawn(resp);
    entered_rx.await.expect("handler entered");

    serve.abort();
    let _ = serve.await;

    let conn = tokio::time::timeout(Duration::from_secs(5), h2_task)
        .await
        .expect("client connection outlived the dropped server")
        .unwrap();
    assert!(conn.is_err(), "connection was closed abruptly, not drained");
    let resp = tokio::time::timeout(Duration::from_secs(5), resp)
        .await
        .expect("in-flight request outlived the dropped server")
        .unwrap();
    assert!(resp.is_err(), "in-flight request was cut off");
}
