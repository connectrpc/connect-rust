use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use http::header;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use super::*;
use crate::router::Router;
use crate::service::ConnectRpcService;

mod acceptor;
mod builders;
mod connection;
mod header_read_timeout;
mod http2;
mod max_connection_age;
mod max_connection_idle;
mod max_requests;
mod peer;
mod shutdown;

/// The public types keep their auto traits, and the serving futures are
/// `Send` so they can be spawned. Constructing the futures needs no runtime.
#[test]
fn public_types_are_send_and_futures_spawnable() {
    fn send<T: Send>(_: &T) {}
    fn sync<T: Sync>(_: &T) {}
    fn unpin<T: Unpin>(_: &T) {}
    fn error<T: std::error::Error + Send + Sync + 'static>() {}

    let server = Server::new(Router::new());
    send(&server);
    sync(&server);
    send(&ConnectionConfig::new());
    sync(&ConnectionConfig::new());
    send(&AcceptConfig::new());
    sync(&AcceptConfig::new());
    send(&ConnectionInfo::new());
    sync(&ConnectionInfo::new());
    error::<HandshakeError>();

    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let listener = {
        let _guard = runtime.enter();
        TcpListener::from_std(std_listener).unwrap()
    };
    let acceptor = Acceptor::new(listener, AcceptConfig::new());
    send(&acceptor);
    sync(&acceptor);
    let accept = acceptor.accept();
    send(&accept);
    // Type-checks without a value: an `Accepted` only exists once a peer has
    // connected, so assert over a parameter instead of constructing one.
    fn handshake_is_send(accepted: Accepted) {
        fn send<T: Send>(_: &T) {}
        send(&accepted);
        let handshake = accepted.handshake();
        send(&handshake);
    }
    let _ = handshake_is_send;
    fn server_io_is_send_unpin(io: &ServerIo) {
        fn send<T: Send>(_: &T) {}
        fn unpin<T: Unpin>(_: &T) {}
        send(io);
        unpin(io);
    }
    let _ = server_io_is_send_unpin;

    let (io, _) = tokio::io::duplex(1);
    let connection = serve_connection(
        io,
        ConnectionInfo::new(),
        ConnectRpcService::new(Router::new()),
        ConnectionConfig::new(),
        std::future::pending(),
    );
    send(&connection);
    let (io, _) = tokio::io::duplex(1);
    let connection = server.serve_connection(io, ConnectionInfo::new(), std::future::ready(()));
    send(&connection);
    drop(server); // the future does not borrow the `Server`
    unpin(&Box::pin(connection));
}

/// Hand-crafted Connect unary request (`POST /svc/Echo`, empty proto
/// body, `Connection: close`). Used by the peer-info tests to probe the
/// server over raw TCP/TLS without pulling in an HTTP client dep.
const ECHO_REQ: &[u8] = concat!(
    "POST /svc/Echo HTTP/1.1\r\n",
    "Host: localhost\r\n",
    "Content-Type: application/proto\r\n",
    "Content-Length: 0\r\n",
    "Connection: close\r\n",
    "\r\n",
)
.as_bytes();

const KEEPALIVE_ECHO_REQ: &[u8] = concat!(
    "POST /svc/Echo HTTP/1.1\r\n",
    "Host: localhost\r\n",
    "Content-Type: application/proto\r\n",
    "Content-Length: 0\r\n",
    "Connection: keep-alive\r\n",
    "\r\n",
)
.as_bytes();

/// Send one unary request over an h2 connection and await its response.
async fn send_unary(send_request: &mut h2::client::SendRequest<Bytes>, addr: SocketAddr) {
    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Unknown"))
        .body(())
        .unwrap();
    let (resp, _) = send_request.send_request(req, true).unwrap();
    resp.await.unwrap();
}

fn slow_router() -> (
    Router,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
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
    (router, entered_rx, release_tx)
}

async fn yield_to_tasks() {
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
}

async fn drain_h2_body(mut resp: http::Response<h2::RecvStream>) {
    while let Some(chunk) = resp.body_mut().data().await {
        chunk.expect("h2 response body failed");
    }
}

async fn read_http1_response(stream: &mut (impl tokio::io::AsyncRead + Unpin)) -> Vec<u8> {
    let mut resp = Vec::new();
    let mut buf = [0; 1024];
    loop {
        let read = stream.read(&mut buf).await.unwrap();
        assert!(read > 0, "connection closed before full response arrived");
        resp.extend_from_slice(&buf[..read]);

        let Some(header_end) = find_header_end(&resp) else {
            continue;
        };
        let body_start = header_end + 4;
        let content_length = content_length(&resp[..header_end]).unwrap_or(0);
        if resp.len() >= body_start + content_length {
            return resp;
        }
    }
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(headers: &[u8]) -> Option<usize> {
    std::str::from_utf8(headers).ok()?.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().ok())
            .flatten()
    })
}

/// Minimal mTLS PKI: one CA → one server leaf + one client leaf. The server
/// requires a verified client certificate and the client presents one.
/// Returns (server_config, client_config, client_cert_der).
#[cfg(feature = "server-tls")]
pub(crate) fn pki() -> (
    Arc<rustls::ServerConfig>,
    Arc<rustls::ClientConfig>,
    rustls::pki_types::CertificateDer<'static>,
) {
    let (server, client, client_cert) = make_pki(true);
    (server, client, client_cert.expect("client cert issued"))
}

/// As [`pki`], but client authentication is optional on the server and the
/// client presents no certificate.
#[cfg(feature = "server-tls")]
fn pki_without_client_cert() -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
    let (server, client, _) = make_pki(false);
    (server, client)
}

#[cfg(feature = "server-tls")]
fn make_pki(
    client_cert: bool,
) -> (
    Arc<rustls::ServerConfig>,
    Arc<rustls::ClientConfig>,
    Option<rustls::pki_types::CertificateDer<'static>>,
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
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from(ca.der().to_vec())).unwrap();
    let roots = Arc::new(roots);

    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::clone(&roots));
    let verifier = if client_cert {
        verifier
    } else {
        verifier.allow_unauthenticated()
    };
    let server = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier.build().unwrap())
        .with_single_cert(vec![srv_cert], srv_key)
        .unwrap();
    let client = rustls::ClientConfig::builder().with_root_certificates(roots);
    let (client, cli_cert) = if client_cert {
        let (cli_cert, cli_key) = issue(&[]);
        let client = client
            .with_client_auth_cert(vec![cli_cert.clone()], cli_key)
            .unwrap();
        (client, Some(cli_cert))
    } else {
        (client.with_no_client_auth(), None)
    };
    (Arc::new(server), Arc::new(client), cli_cert)
}
