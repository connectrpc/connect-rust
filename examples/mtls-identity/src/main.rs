//! Self-contained mTLS identity demo.
//!
//! Generates an in-memory PKI, serves IdentityService on axum behind
//! `connectrpc::axum::serve_tls`, then calls it with two client
//! certificates to show the cert-SAN identity flow:
//!
//!   - `alice` reads `shared` and `alice-only` successfully
//!   - `bob` reads `shared` but is denied `alice-only`
//!
//! Run with:
//!
//! ```sh
//! cargo run -p mtls-identity-example
//! ```

use std::sync::Arc;

use connectrpc::ErrorCode;
use connectrpc::client::{ClientConfig, HttpClient};
use mtls_identity_example::{
    BoxError, GetSecretRequest, IdentityServiceClient, WhoAmIRequest, pki, serve,
};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // 1. PKI: a fresh CA, server cert, and two workload client certs.
    //    No PEM files on disk — see `pki::generate`.
    let pki = Arc::new(pki::generate(&["alice", "bob"]));

    // 2. Server: axum app behind connectrpc::axum::serve_tls.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, Arc::clone(&pki.server_config), async {
        shutdown_rx.await.ok();
    }));
    println!("IdentityService listening on https://{addr} (mTLS required)\n");

    // 3. Clients: one HttpClient per workload, each presenting its own
    //    client cert. The handler derives the identity from the cert SAN —
    //    the request bodies carry no credentials at all.
    let client_for = |workload: &str| {
        let http = HttpClient::with_tls(pki.client_config(workload));
        // The server cert's SAN is "localhost"; rustls verifies hostname.
        let cfg = ClientConfig::new(
            format!("https://localhost:{}", addr.port())
                .parse()
                .unwrap(),
        );
        IdentityServiceClient::new(http, cfg)
    };
    let alice = client_for("alice");
    let bob = client_for("bob");

    // WhoAmI: identity comes purely from the TLS layer.
    for (label, client) in [("alice", &alice), ("bob", &bob)] {
        let resp = client.who_am_i(WhoAmIRequest::default()).await?;
        let v = resp.view();
        println!(
            "[{label}] WhoAmI -> identity={:?} san={:?} from={:?}",
            v.identity.unwrap_or(""),
            v.san.unwrap_or(""),
            v.remote_addr.unwrap_or(""),
        );
    }
    println!();

    // GetSecret: the ACL is keyed on the cert-derived identity.
    for (label, client) in [("alice", &alice), ("bob", &bob)] {
        for name in ["shared", "alice-only"] {
            let req = GetSecretRequest {
                name: Some(name.into()),
                ..Default::default()
            };
            match client.get_secret(req).await {
                Ok(resp) => {
                    let served_by = resp
                        .trailers()
                        .get("x-served-by")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("?");
                    println!(
                        "[{label}] GetSecret({name:>10}) -> {:?} (x-served-by: {served_by})",
                        resp.view().value.unwrap_or("")
                    );
                }
                Err(err) => {
                    debug_assert_eq!(err.code, ErrorCode::PermissionDenied);
                    println!("[{label}] GetSecret({name:>10}) -> {err}");
                }
            }
        }
    }

    // 4. Graceful shutdown: stop accepting and drain in-flight connections.
    shutdown_tx.send(()).ok();
    server.await??;
    Ok(())
}
