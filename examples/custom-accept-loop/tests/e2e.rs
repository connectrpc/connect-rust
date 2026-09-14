//! End-to-end: the loop admits known workloads, serves `batch-*` identities on
//! the bulk runtime and everyone else on the accepting runtime, and turns away
//! a client whose certificate names no workload.

use std::sync::Arc;

use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::server::{AcceptConfig, Acceptor};
use connectrpc::{ErrorCode, Server};
use custom_accept_loop_example::{PlacementServiceClient, WhereAmIRequest, pki, router, serve};

#[test]
fn connections_are_placed_by_identity() {
    let interactive = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("interactive")
        .enable_all()
        .build()
        .unwrap();
    let bulk = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("bulk")
        .enable_all()
        .build()
        .unwrap();

    interactive.block_on(async {
        let pki = pki::generate(&["frontend", "batch-indexer", "stranger"]);
        let acceptor = Acceptor::bind(
            "127.0.0.1:0",
            AcceptConfig::new().with_tls(Arc::clone(&pki.server_config)),
        )
        .await
        .unwrap();
        let addr = acceptor.local_addr().unwrap();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let serving = tokio::spawn(serve(
            acceptor,
            Arc::new(Server::new(router())),
            bulk.handle().clone(),
            async {
                stop_rx.await.ok();
            },
        ));

        let call = |workload: &'static str| {
            let client = PlacementServiceClient::new(
                HttpClient::with_tls(pki.client_config(workload)),
                ClientConfig::new(
                    format!("https://localhost:{}", addr.port())
                        .parse()
                        .unwrap(),
                ),
            );
            async move { client.where_am_i(WhereAmIRequest::default()).await }
        };

        let resp = call("frontend").await.unwrap();
        assert_eq!(resp.view().identity, Some("frontend"));
        assert_eq!(resp.view().thread, Some("interactive"));

        let resp = call("batch-indexer").await.unwrap();
        assert_eq!(resp.view().identity, Some("batch-indexer"));
        assert_eq!(resp.view().thread, Some("bulk"));

        stop_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), serving)
            .await
            .expect("loop drains promptly")
            .unwrap()
            .unwrap();
    });
    bulk.shutdown_timeout(std::time::Duration::from_secs(5));
}

/// Admission happens before any HTTP is spoken: a verified certificate with no
/// workload SAN gets its connection dropped, which the client sees as a
/// transport failure.
#[tokio::test]
async fn unknown_workloads_are_not_served() {
    // `pki::generate` only issues workload SANs, so mint the outsider by
    // issuing a "workload" whose name contains a dot: it verifies, but is not
    // a direct child of the workload domain and so parses to no identity.
    let pki = pki::generate(&["not.a.workload"]);
    let acceptor = Acceptor::bind(
        "127.0.0.1:0",
        AcceptConfig::new().with_tls(Arc::clone(&pki.server_config)),
    )
    .await
    .unwrap();
    let addr = acceptor.local_addr().unwrap();
    let serving = tokio::spawn(serve(
        acceptor,
        Arc::new(Server::new(router())),
        tokio::runtime::Handle::current(),
        std::future::pending(),
    ));

    let client = PlacementServiceClient::new(
        HttpClient::with_tls(pki.client_config("not.a.workload")),
        ClientConfig::new(
            format!("https://localhost:{}", addr.port())
                .parse()
                .unwrap(),
        ),
    );
    let err = client
        .where_am_i(WhereAmIRequest::default())
        .await
        .expect_err("a certificate without a workload identity is turned away");
    assert_eq!(err.code, ErrorCode::Unavailable, "{err}");
    serving.abort();
}
