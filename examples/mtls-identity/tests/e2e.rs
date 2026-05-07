//! End-to-end test: spin up IdentityService behind
//! `connectrpc::axum::serve_tls`, make calls with two client identities,
//! and check that the cert-SAN identity flows through `PeerCerts` to
//! the handler and gates the ACL correctly.

use std::sync::Arc;

use connectrpc::ErrorCode;
use connectrpc::client::{ClientConfig, HttpClient};
use mtls_identity_example::{GetSecretRequest, IdentityServiceClient, WhoAmIRequest, pki, serve};

struct Harness {
    pki: Arc<pki::Pki>,
    addr: std::net::SocketAddr,
    shutdown: tokio::sync::oneshot::Sender<()>,
    serve_task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Harness {
    async fn start(workloads: &[&str]) -> Self {
        let pki = Arc::new(pki::generate(workloads));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let serve_task = tokio::spawn(serve(listener, Arc::clone(&pki.server_config), async {
            rx.await.ok();
        }));
        Harness {
            pki,
            addr,
            shutdown: tx,
            serve_task,
        }
    }

    fn client(&self, workload: &str) -> IdentityServiceClient<HttpClient> {
        let http = HttpClient::with_tls(self.pki.client_config(workload));
        let cfg = ClientConfig::new(
            format!("https://localhost:{}", self.addr.port())
                .parse()
                .unwrap(),
        );
        IdentityServiceClient::new(http, cfg)
    }

    async fn shutdown(self) {
        self.shutdown.send(()).ok();
        tokio::time::timeout(std::time::Duration::from_secs(5), self.serve_task)
            .await
            .expect("server should shut down within timeout")
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn whoami_reflects_cert_san_and_remote_addr() {
    let h = Harness::start(&["alice"]).await;
    let resp = h
        .client("alice")
        .who_am_i(WhoAmIRequest::default())
        .await
        .unwrap();
    let v = resp.view();
    assert_eq!(v.identity, Some("alice"));
    assert_eq!(v.san, Some("alice.workloads.example.com"));
    // PeerAddr should be the actual TCP source address.
    let remote = v.remote_addr.unwrap();
    assert!(remote.starts_with("127.0.0.1:"), "remote_addr={remote}");
    h.shutdown().await;
}

#[tokio::test]
async fn authorized_call_returns_value_and_trailer() {
    let h = Harness::start(&["alice", "bob"]).await;
    let resp = h
        .client("alice")
        .get_secret(GetSecretRequest {
            name: Some("shared".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(resp.view().value, Some("the value of teamwork"));
    assert_eq!(
        resp.trailers()
            .get("x-served-by")
            .unwrap()
            .to_str()
            .unwrap(),
        "alice"
    );
    h.shutdown().await;
}

#[tokio::test]
async fn permission_denied_for_other_workloads_secret() {
    let h = Harness::start(&["alice", "bob"]).await;
    let err = h
        .client("bob")
        .get_secret(GetSecretRequest {
            name: Some("alice-only".into()),
            ..Default::default()
        })
        .await
        .expect_err("bob cannot read alice's secret");
    assert_eq!(err.code, ErrorCode::PermissionDenied);
    h.shutdown().await;
}

#[tokio::test]
async fn client_without_cert_is_rejected_at_handshake() {
    // WebPkiClientVerifier requires a client cert; a TLS client that
    // presents none never reaches the handler. Distinct from the
    // `extract_identity(None)` branch, which only fires for hosting
    // setups that make client auth optional.
    let h = Harness::start(&["alice"]).await;
    let no_cert_cfg = Arc::new(
        connectrpc::rustls::ClientConfig::builder()
            .with_root_certificates(Arc::clone(&h.pki.roots))
            .with_no_client_auth(),
    );
    let http = HttpClient::with_tls(no_cert_cfg);
    let cfg = ClientConfig::new(
        format!("https://localhost:{}", h.addr.port())
            .parse()
            .unwrap(),
    );
    let client = IdentityServiceClient::new(http, cfg);
    let err = client
        .who_am_i(WhoAmIRequest::default())
        .await
        .expect_err("server must reject a client with no cert");
    // The handshake failure surfaces as a connection-level Unavailable,
    // not a Connect-protocol error: the request never reaches HTTP.
    assert_eq!(err.code, ErrorCode::Unavailable, "got: {err}");
    h.shutdown().await;
}
