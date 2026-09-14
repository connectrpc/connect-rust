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
    // Inline minimal mTLS PKI: one CA → one server leaf + one client leaf.
    // Returns (server_config, client_config, client_cert_der).
    fn pki() -> (
        Arc<rustls::ServerConfig>,
        Arc<rustls::ClientConfig>,
        rustls::pki_types::CertificateDer<'static>,
    ) {
        use rcgen::CertificateParams;
        use rcgen::KeyPair;
        use rcgen::SanType;
        use rustls::pki_types::CertificateDer;
        use rustls::pki_types::PrivatePkcs8KeyDer;

        // Idempotent; err = already installed (tests share process state).
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = rcgen::CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();

        let issue = |sans: &[SanType]| {
            let k = KeyPair::generate().unwrap();
            let mut p = CertificateParams::default();
            p.subject_alt_names = sans.to_vec();
            let c = p.signed_by(&k, &ca).unwrap();
            (
                CertificateDer::from(c.der().to_vec()),
                PrivatePkcs8KeyDer::from(k.serialized_der().to_vec()).into(),
            )
        };

        let (srv_cert, srv_key) = issue(&[SanType::DnsName("localhost".try_into().unwrap())]);
        let (cli_cert, cli_key) = issue(&[]);
        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from(ca.der().to_vec())).unwrap();
        let roots = Arc::new(roots);

        let cv = rustls::server::WebPkiClientVerifier::builder(Arc::clone(&roots))
            .build()
            .unwrap();
        let server = rustls::ServerConfig::builder()
            .with_client_cert_verifier(cv)
            .with_single_cert(vec![srv_cert], srv_key)
            .unwrap();
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(vec![cli_cert.clone()], cli_key)
            .unwrap();
        (Arc::new(server), Arc::new(client), cli_cert)
    }

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
