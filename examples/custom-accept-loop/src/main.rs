//! Identity-routed runtimes with a custom accept loop.
//!
//! Starts the loop from `lib.rs` on one TLS listener with two runtimes, then
//! calls it as two workloads and prints where each connection was served:
//! `frontend` on the main runtime, `batch-indexer` on the `bulk` runtime.
//!
//! ```sh
//! cargo run -p custom-accept-loop-example
//! ```

use std::sync::Arc;
use std::time::Duration;

use connectrpc::Server;
use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::server::{AcceptConfig, Acceptor};
use custom_accept_loop_example::{
    BoxError, PlacementServiceClient, WhereAmIRequest, pki, router, serve,
};

fn main() -> Result<(), BoxError> {
    let interactive = tokio::runtime::Builder::new_multi_thread()
        .thread_name("interactive")
        .enable_all()
        .build()?;
    let bulk = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("bulk")
        .enable_all()
        .build()?;

    interactive.block_on(async {
        let pki = pki::generate(&["frontend", "batch-indexer"]);
        let server =
            Arc::new(Server::new(router()).with_max_connection_age(Duration::from_secs(600)));
        let accept = AcceptConfig::new().with_tls(Arc::clone(&pki.server_config));
        let acceptor = Acceptor::bind("127.0.0.1:0", accept).await?;
        let addr = acceptor.local_addr()?;
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let serving = tokio::spawn(serve(acceptor, server, bulk.handle().clone(), async {
            stop_rx.await.ok();
        }));
        println!("PlacementService listening on https://{addr} (mTLS required)\n");

        for workload in ["frontend", "batch-indexer"] {
            let client = PlacementServiceClient::new(
                HttpClient::with_tls(pki.client_config(workload)),
                ClientConfig::new(format!("https://localhost:{}", addr.port()).parse()?),
            );
            let resp = client.where_am_i(WhereAmIRequest::default()).await?;
            let v = resp.view();
            println!(
                "[{workload}] served as {:?} on a {:?} thread",
                v.identity.unwrap_or(""),
                v.thread.unwrap_or("")
            );
        }

        stop_tx.send(()).ok();
        serving.await??;
        Ok::<_, BoxError>(())
    })?;
    bulk.shutdown_timeout(Duration::from_secs(5));
    Ok(())
}
