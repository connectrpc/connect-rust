//! `PeerAddr` / `PeerCerts` reach handlers.

use super::*;

#[tokio::test]
async fn peer_addr_reaches_handler() {
    // Handler stashes the PeerAddr it sees into a shared slot.
    let captured: Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));
    let handler_captured = Arc::clone(&captured);
    let router = Router::new().route(
        "svc",
        "Echo",
        crate::handler_fn(
            move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                let cap = Arc::clone(&handler_captured);
                async move {
                    *cap.lock().unwrap() = ctx.peer_addr();
                    crate::Response::ok(buffa_types::Empty::default())
                }
            },
        ),
    );

    let bound = Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(router, async {
                rx.await.ok();
            })
            .await
    });

    // Hand-crafted Connect unary request over raw TCP (HTTP/1.1).
    // Body is an empty-serialized `Empty` message (zero bytes).
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let client_local = stream.local_addr().unwrap();
    stream.write_all(ECHO_REQ).await.unwrap();
    // Drain the response so the server-side connection can complete.
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();
    // Sanity: 2xx status.
    assert!(
        resp.starts_with(b"HTTP/1.1 2"),
        "expected 2xx, got: {}",
        String::from_utf8_lossy(&resp[..resp.len().min(80)])
    );

    tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let peer = captured
        .lock()
        .unwrap()
        .take()
        .expect("handler should have captured PeerAddr");
    // The server sees the client's local_addr() as the remote peer.
    assert_eq!(peer, client_local);
}

/// End-to-end mTLS: client presents a cert; handler reads it from
/// `ctx.peer_certs()` and the DER bytes round-trip.
#[cfg(feature = "server-tls")]
#[tokio::test]
async fn peer_certs_reach_handler() {
    let (server_cfg, client_cfg, expected_client_der) = pki();

    type CapturedCerts = Vec<rustls::pki_types::CertificateDer<'static>>;
    let captured: Arc<Mutex<Option<CapturedCerts>>> = Arc::new(Mutex::new(None));
    let handler_captured = Arc::clone(&captured);
    let router = Router::new().route(
        "svc",
        "Echo",
        crate::handler_fn(
            move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                let cap = Arc::clone(&handler_captured);
                async move {
                    *cap.lock().unwrap() = ctx.peer_certs().map(<[_]>::to_vec);
                    crate::Response::ok(buffa_types::Empty::default())
                }
            },
        ),
    );

    let bound = Server::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_tls(server_cfg);
    let addr = bound.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        bound
            .serve_with_graceful_shutdown(router, async {
                rx.await.ok();
            })
            .await
    });

    // TLS-over-raw-TCP + hand-crafted HTTP/1.1 Connect unary request.
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let connector = tokio_rustls::TlsConnector::from(client_cfg);
    let sni = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(sni, tcp).await.unwrap();
    tls.write_all(ECHO_REQ).await.unwrap();
    let mut resp = Vec::new();
    tls.read_to_end(&mut resp).await.unwrap();
    assert!(
        resp.starts_with(b"HTTP/1.1 2"),
        "expected 2xx, got: {}",
        String::from_utf8_lossy(&resp[..resp.len().min(80)])
    );

    tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let certs = captured
        .lock()
        .unwrap()
        .take()
        .expect("handler should have captured PeerCerts");
    // The exact DER bytes the client presented.
    assert_eq!(certs.len(), 1);
    assert_eq!(certs[0].as_ref(), expected_client_der.as_ref());
}
