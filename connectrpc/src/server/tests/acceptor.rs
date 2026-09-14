//! Layer 3 on its own: `Acceptor` / `Accepted::handshake` without any HTTP.

use super::*;

#[tokio::test]
async fn plaintext_handshake_yields_stream_and_peer_addr() {
    let acceptor = Acceptor::bind("127.0.0.1:0", AcceptConfig::new())
        .await
        .unwrap();
    let addr = acceptor.local_addr().unwrap();

    let client = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let local = stream.local_addr().unwrap();
        stream.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
        local
    });

    let (mut io, info) = acceptor.accept().await.unwrap().handshake().await.unwrap();
    let mut buf = [0u8; 4];
    io.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
    io.write_all(b"pong").await.unwrap();

    let client_addr = client.await.unwrap();
    assert_eq!(info.peer_addr(), Some(client_addr));
    #[cfg(feature = "server-tls")]
    assert!(info.peer_certs().is_none());
    assert!(info.extensions().is_empty());
}

#[cfg(feature = "server-tls")]
#[tokio::test]
async fn mtls_handshake_captures_peer_certs() {
    let (server_cfg, client_cfg, expected_client_der) = pki();
    let acceptor = Acceptor::bind("127.0.0.1:0", AcceptConfig::new().with_tls(server_cfg))
        .await
        .unwrap();
    let addr = acceptor.local_addr().unwrap();

    let client = tokio::spawn(async move {
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let connector = tokio_rustls::TlsConnector::from(client_cfg);
        let sni = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(sni, tcp).await.unwrap();
        tls.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        tls.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
    });

    let (mut io, info) = acceptor.accept().await.unwrap().handshake().await.unwrap();
    let mut buf = [0u8; 4];
    io.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping", "ServerIo must yield decrypted bytes");
    io.write_all(b"pong").await.unwrap();
    io.flush().await.unwrap();
    client.await.unwrap();

    let certs = info.peer_certs().expect("verified client chain captured");
    assert_eq!(certs.len(), 1);
    assert_eq!(certs[0].as_ref(), expected_client_der.as_ref());
    assert!(info.peer_addr().is_some());
}

#[cfg(feature = "server-tls")]
#[tokio::test]
async fn garbage_instead_of_client_hello_is_a_handshake_error() {
    let (server_cfg, _, _) = pki();
    let acceptor = Acceptor::bind("127.0.0.1:0", AcceptConfig::new().with_tls(server_cfg))
        .await
        .unwrap();
    let addr = acceptor.local_addr().unwrap();

    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    client.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();

    let err = acceptor
        .accept()
        .await
        .unwrap()
        .handshake()
        .await
        .expect_err("plaintext bytes must fail the TLS handshake");
    assert!(!err.is_timeout());
    assert_eq!(err.peer_addr().ip(), addr.ip());
    assert!(
        std::error::Error::source(&err).is_some(),
        "the rustls error is the source"
    );
    assert_eq!(
        err.to_string(),
        format!("TLS handshake with {} failed", err.peer_addr())
    );
}

#[cfg(feature = "server-tls")]
#[tokio::test(start_paused = true)]
async fn stalled_handshake_times_out() {
    let (server_cfg, _, _) = pki();
    let config = AcceptConfig::new()
        .with_tls(server_cfg)
        .with_tls_handshake_timeout(Duration::from_secs(3));
    let acceptor = Acceptor::bind("127.0.0.1:0", config).await.unwrap();
    let addr = acceptor.local_addr().unwrap();

    // Connect and say nothing.
    let _stalled = tokio::net::TcpStream::connect(addr).await.unwrap();
    let accepted = acceptor.accept().await.unwrap();
    let handshake = tokio::spawn(accepted.handshake());
    tokio::time::advance(Duration::from_secs(4)).await;

    let err = handshake
        .await
        .unwrap()
        .expect_err("a silent peer must hit the handshake timeout");
    assert!(err.is_timeout());
    let source = std::error::Error::source(&err).expect("timeout detail is the source");
    assert!(source.to_string().contains("3s"), "{source}");
    assert!(err.to_string().ends_with("timed out"), "{err}");
}

/// TLS without client authentication: the handshake succeeds and
/// `peer_certs()` is `None`, exactly as on plaintext.
#[cfg(feature = "server-tls")]
#[tokio::test]
async fn tls_without_client_cert_yields_no_peer_certs() {
    let (server_cfg, client_cfg) = pki_without_client_cert();
    let acceptor = Acceptor::bind("127.0.0.1:0", AcceptConfig::new().with_tls(server_cfg))
        .await
        .unwrap();
    let addr = acceptor.local_addr().unwrap();

    let client = tokio::spawn(async move {
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let connector = tokio_rustls::TlsConnector::from(client_cfg);
        let sni = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(sni, tcp).await.unwrap();
        tls.write_all(b"ping").await.unwrap();
        tls.flush().await.unwrap();
    });

    let (mut io, info) = acceptor.accept().await.unwrap().handshake().await.unwrap();
    let mut buf = [0u8; 4];
    io.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
    client.await.unwrap();
    assert!(info.peer_certs().is_none());
    assert!(info.peer_addr().is_some());
}
