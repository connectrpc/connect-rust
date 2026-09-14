//! Layer 2 on its own: `serve_connection` over in-memory streams, with no
//! listener, no acceptor and no `Server`.

use std::convert::Infallible;

use http_body_util::Full;

use super::*;

/// `POST /svc/Panic` on a keep-alive HTTP/1.1 connection.
const KEEPALIVE_PANIC_REQ: &[u8] = concat!(
    "POST /svc/Panic HTTP/1.1\r\n",
    "Host: localhost\r\n",
    "Content-Type: application/proto\r\n",
    "Content-Length: 0\r\n",
    "Connection: keep-alive\r\n",
    "\r\n",
)
.as_bytes();

fn echo_router() -> Router {
    Router::new().route(
        "svc",
        "Echo",
        crate::handler_fn(
            |_ctx: crate::RequestContext, _req: buffa_types::Empty| async move {
                crate::Response::ok(buffa_types::Empty::default())
            },
        ),
    )
}

/// Serve `router` on one half of an in-memory stream with `config`; returns the
/// client half, the connection task, and the sender that ends it.
fn serve_in_memory(
    router: Router,
    config: ConnectionConfig,
) -> (
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (client_io, server_io) = tokio::io::duplex(64 << 10);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let conn = tokio::spawn(serve_connection(
        server_io,
        ConnectionInfo::new(),
        ConnectRpcService::new(router),
        config,
        async {
            stop_rx.await.ok();
        },
    ));
    (client_io, conn, stop_tx)
}

async fn h2_connect(
    io: tokio::io::DuplexStream,
) -> (
    h2::client::SendRequest<Bytes>,
    tokio::task::JoinHandle<Result<(), h2::Error>>,
) {
    let (send_request, connection) = h2::client::handshake(io).await.unwrap();
    (send_request, tokio::spawn(connection))
}

fn post(path: &str) -> http::Request<()> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://in-memory/{path}"))
        .header(header::CONTENT_TYPE, "application/proto")
        .body(())
        .unwrap()
}

/// The shutdown future is honoured: GOAWAY goes out, the in-flight request
/// still completes, then the connection future resolves.
#[tokio::test]
async fn shutdown_signal_drains_in_flight_then_completes() {
    let (router, entered_rx, release_tx) = slow_router();
    let (client_io, conn, stop_tx) = serve_in_memory(router, ConnectionConfig::new());
    let (mut send_request, h2_task) = h2_connect(client_io).await;

    let (resp, _) = send_request.send_request(post("svc/Slow"), true).unwrap();
    let resp = tokio::spawn(resp);
    entered_rx.await.unwrap();

    stop_tx.send(()).unwrap();
    yield_to_tasks().await;
    assert!(
        !conn.is_finished(),
        "connection closed with a request in flight"
    );

    release_tx.send(()).unwrap();
    let resp = tokio::time::timeout(Duration::from_secs(5), resp)
        .await
        .expect("response never arrived")
        .unwrap()
        .expect("h2 request failed");
    assert!(resp.status().is_success(), "{}", resp.status());

    tokio::time::timeout(Duration::from_secs(5), conn)
        .await
        .expect("connection did not finish after draining")
        .unwrap();
    if let Err(err) = h2_task.await.unwrap() {
        assert!(err.is_go_away(), "{err:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn max_age_retires_without_a_listener() {
    let config = ConnectionConfig::new()
        .with_max_connection_age(Duration::from_secs(10))
        .with_max_connection_age_grace(Duration::from_secs(1));
    let (client_io, conn, _stop) = serve_in_memory(Router::new(), config);
    let (mut send_request, h2_task) = h2_connect(client_io).await;
    let (resp, _) = send_request
        .send_request(post("svc/Unknown"), true)
        .unwrap();
    resp.await.unwrap();

    // 10s ±10% jitter, then GOAWAY; the idle client connection closes.
    tokio::time::advance(Duration::from_secs(12)).await;
    yield_to_tasks().await;
    assert!(h2_task.is_finished(), "aged connection was not retired");
    if let Err(err) = h2_task.await.unwrap() {
        assert!(err.is_go_away(), "{err:?}");
    }
    drop(send_request);
    tokio::time::timeout(Duration::from_secs(5), conn)
        .await
        .expect("connection future did not resolve")
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn idle_retires_without_a_listener() {
    let config = ConnectionConfig::new().with_max_connection_idle(Duration::from_secs(10));
    let (client_io, _conn, _stop) = serve_in_memory(Router::new(), config);
    let (mut send_request, h2_task) = h2_connect(client_io).await;
    let (resp, _) = send_request
        .send_request(post("svc/Unknown"), true)
        .unwrap();
    resp.await.unwrap();

    // Activity inside the first window re-arms it...
    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    assert!(
        !h2_task.is_finished(),
        "reaped despite activity in the window"
    );
    // ...a fully quiet window reaps.
    tokio::time::advance(Duration::from_secs(11)).await;
    yield_to_tasks().await;
    assert!(h2_task.is_finished(), "idle connection was not reaped");
}

#[tokio::test(start_paused = true)]
async fn request_count_retires_without_a_listener() {
    let config =
        ConnectionConfig::new().with_max_requests_per_connection(NonZeroU64::new(2).unwrap());
    let (client_io, conn, _stop) = serve_in_memory(Router::new(), config);
    let (mut send_request, h2_task) = h2_connect(client_io).await;
    for _ in 0..2 {
        let (resp, _) = send_request
            .send_request(post("svc/Unknown"), true)
            .unwrap();
        resp.await.unwrap();
    }
    yield_to_tasks().await;
    // Past the limit the server has sent GOAWAY; the grace period then closes.
    tokio::time::advance(Duration::from_secs(6)).await;
    yield_to_tasks().await;
    assert!(
        h2_task.is_finished(),
        "connection not retired after its request budget"
    );
    drop(send_request);
    tokio::time::timeout(Duration::from_secs(5), conn)
        .await
        .expect("driver future did not resolve after the grace period")
        .unwrap();
}

/// `PeerAddr` (and `PeerCerts`) are stamped after the connection's own
/// extensions, so an accept loop cannot spoof the transport-observed peer,
/// while everything else it inserted reaches the handler.
#[tokio::test]
async fn builtins_are_stamped_over_connection_extensions() {
    #[derive(Clone, Debug, PartialEq)]
    struct Tenant(&'static str);

    type Captured = Arc<Mutex<Option<(Option<SocketAddr>, Option<Tenant>)>>>;
    let captured: Captured = Arc::new(Mutex::new(None));
    let seen = Arc::clone(&captured);
    let router = Router::new().route(
        "svc",
        "Echo",
        crate::handler_fn(
            move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                let seen = Arc::clone(&seen);
                async move {
                    *seen.lock().unwrap() =
                        Some((ctx.peer_addr(), ctx.extensions().get::<Tenant>().cloned()));
                    crate::Response::ok(buffa_types::Empty::default())
                }
            },
        ),
    );

    let real: SocketAddr = "192.0.2.7:443".parse().unwrap();
    let mut info = ConnectionInfo::new().with_peer_addr(real);
    info.extensions_mut().insert(Tenant("acme"));
    info.extensions_mut()
        .insert(PeerAddr("10.0.0.1:1".parse().unwrap()));

    let (mut client_io, server_io) = tokio::io::duplex(64 << 10);
    let conn = tokio::spawn(serve_connection(
        server_io,
        info,
        ConnectRpcService::new(router),
        ConnectionConfig::new(),
        std::future::pending(),
    ));
    client_io.write_all(ECHO_REQ).await.unwrap();
    let resp = read_http1_response(&mut client_io).await;
    assert!(resp.starts_with(b"HTTP/1.1 200"));
    drop(client_io);
    conn.await.unwrap();

    assert_eq!(
        captured.lock().unwrap().take(),
        Some((Some(real), Some(Tenant("acme"))))
    );
}

/// A panicking handler costs its request (a Connect `internal` error), not
/// the connection: the next request on the same keep-alive connection is
/// served.
#[tokio::test]
async fn panicking_handler_yields_internal_error_and_connection_survives() {
    let router = echo_router().route(
        "svc",
        "Panic",
        crate::handler_fn(
            |_ctx: crate::RequestContext, _req: buffa_types::Empty| async move {
                if true {
                    panic!("handler bug");
                }
                crate::Response::ok(buffa_types::Empty::default())
            },
        ),
    );
    let (mut client_io, conn, _stop) = serve_in_memory(router, ConnectionConfig::new());

    client_io.write_all(KEEPALIVE_PANIC_REQ).await.unwrap();
    let resp = read_http1_response(&mut client_io).await;
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 500"), "{text}");
    assert!(text.contains(r#""code":"internal""#), "{text}");

    client_io.write_all(KEEPALIVE_ECHO_REQ).await.unwrap();
    let resp = read_http1_response(&mut client_io).await;
    assert!(
        resp.starts_with(b"HTTP/1.1 200"),
        "connection did not survive the panic: {}",
        String::from_utf8_lossy(&resp)
    );
    assert!(!conn.is_finished());
}

/// The future binds to the runtime that polls it, not the one (or none) it
/// was created on: handlers run on the runtime it is spawned onto.
#[test]
fn connection_runs_on_the_runtime_that_polls_it() {
    let served_on: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&served_on);
    let router = Router::new().route(
        "svc",
        "Echo",
        crate::handler_fn(
            move |_ctx: crate::RequestContext, _req: buffa_types::Empty| {
                let slot = Arc::clone(&slot);
                async move {
                    *slot.lock().unwrap() = std::thread::current().name().map(str::to_owned);
                    crate::Response::ok(buffa_types::Empty::default())
                }
            },
        ),
    );

    let client_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let serving_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("chosen-by-the-loop")
        .enable_all()
        .build()
        .unwrap();

    let (mut client_io, server_io) = tokio::io::duplex(64 << 10);
    // Constructed outside any runtime: nothing may touch a reactor or clock
    // until first poll.
    let conn = serve_connection(
        server_io,
        ConnectionInfo::new(),
        ConnectRpcService::new(router),
        ConnectionConfig::new().with_max_connection_age(Duration::from_secs(60)),
        std::future::pending(),
    );
    serving_rt.spawn(conn);

    client_rt.block_on(async {
        client_io.write_all(ECHO_REQ).await.unwrap();
        let resp = read_http1_response(&mut client_io).await;
        assert!(resp.starts_with(b"HTTP/1.1 200"));
    });
    assert_eq!(
        served_on.lock().unwrap().as_deref(),
        Some("chosen-by-the-loop")
    );
}

/// Any tower HTTP service is served, not just `ConnectRpcService`.
#[tokio::test]
async fn serves_a_plain_tower_service() {
    let service = tower::service_fn(|req: http::Request<hyper::body::Incoming>| async move {
        let body = format!("{} {}", req.method(), req.uri().path());
        Ok::<_, Infallible>(http::Response::new(Full::new(Bytes::from(body))))
    });
    let (mut client_io, server_io) = tokio::io::duplex(64 << 10);
    let conn = tokio::spawn(serve_connection(
        server_io,
        ConnectionInfo::new(),
        service,
        ConnectionConfig::new(),
        std::future::pending(),
    ));

    client_io
        .write_all(b"GET /hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let resp = read_http1_response(&mut client_io).await;
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.ends_with("GET /hello"), "{text}");
    drop(client_io);
    conn.await.unwrap();
}
