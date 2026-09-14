//! Layer 4: the default accept loop, composing the [`Acceptor`] (layer 3) with
//! [`serve_connection`] (layer 2).
//!
//! This is all the policy the built-in server has: spawn every connection on
//! the ambient tokio runtime, track it, fan the shutdown signal out, and wait
//! for the drain. [`Server`](super::Server) / [`BoundServer`](super::BoundServer)
//! and `connectrpc::axum::serve` are thin fronts for [`run`]; a server that
//! needs different placement or admission is this same short loop written by
//! hand against the two public layers, and keeps every lifecycle guarantee.

use std::future::Future;
use std::io;

use bytes::Bytes;
use tokio::sync::watch;
use tokio::task::JoinSet;

use super::Acceptor;
use super::ConnectionConfig;
use super::HandshakeError;
use super::connection;
use super::serve_connection;

/// Serve connections from `acceptor` with `service` until `shutdown`
/// resolves, then stop accepting, tell every live connection to drain, and
/// wait for all of them.
///
/// A fatal accept error detaches the live connections (they observe the
/// dropped drain signal and wind down on their own) and returns the error.
/// Dropping the future aborts every connection task.
pub(crate) async fn run<S, B, F>(
    acceptor: Acceptor,
    service: S,
    config: ConnectionConfig,
    shutdown: F,
) -> io::Result<()>
where
    S: tower::Service<http::Request<hyper::body::Incoming>, Response = http::Response<B>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    F: Future<Output = ()> + Send,
{
    config.lint();
    let mut shutdown = std::pin::pin!(shutdown);
    // Broadcasts "begin graceful shutdown" to every live connection. `watch`
    // gives a cloneable receiver per connection and a sticky value, so a
    // connection that registers after the signal still observes it.
    let (draining_tx, draining_rx) = watch::channel(false);
    let mut connections = JoinSet::new();

    loop {
        let accepted = tokio::select! {
            biased; // check shutdown first so we don't accept one more after signal

            () = &mut shutdown => {
                tracing::info!("Shutdown signal received; draining connections");
                break;
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                log_connection_task_result(result);
                continue;
            }
            accepted = acceptor.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(err) => {
                    connections.detach_all();
                    return Err(err);
                }
            },
        };

        let service = service.clone();
        let config = config.clone();
        let draining = connection::latched(draining_rx.clone());
        connections.spawn(async move {
            let (io, info) = match accepted.handshake().await {
                Ok(ready) => ready,
                Err(err) => {
                    log_handshake_error(&err);
                    return;
                }
            };
            serve_connection(io, info, service, config, draining).await;
        });
    }

    // Drop the listener (refuse new conns), then signal & drain existing ones.
    drop(acceptor);
    // Errors only if every connection already finished (no receivers left),
    // in which case there is nothing to drain.
    let _ = draining_tx.send(true);
    while let Some(result) = connections.join_next().await {
        log_connection_task_result(result);
    }
    tracing::info!("All connections drained; shutdown complete");
    Ok(())
}

/// Timeouts at `warn` (a slowloris signal worth surfacing); everything else
/// at `debug` (port scanners and plaintext clients are routine).
fn log_handshake_error(err: &HandshakeError) {
    let source = std::error::Error::source(err);
    if err.is_timeout() {
        tracing::warn!(error = ?source, "{err}");
    } else {
        tracing::debug!(error = ?source, "{err}");
    }
}

fn log_connection_task_result(result: Result<(), tokio::task::JoinError>) {
    if let Err(err) = result {
        tracing::warn!(error = %err, "Connection task ended unexpectedly");
    }
}
