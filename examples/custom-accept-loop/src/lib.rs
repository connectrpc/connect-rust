//! A custom accept loop: admit connections by client-certificate identity and
//! route each one to a tokio runtime chosen from that identity.
//!
//! The loop in [`serve`] is written against connectrpc's two public serving
//! layers — [`Acceptor`] (TCP accept + TLS handshake → stream + [`ConnectionInfo`])
//! and [`Server::serve_connection`] (one connection's whole HTTP lifecycle) —
//! so it decides *which* connections are served and *where*, and inherits
//! everything else from the library: HTTP/1.1 and HTTP/2 settings, the
//! header-read timeout, max connection age, graceful GOAWAY on shutdown, panic
//! isolation, and `PeerAddr` / `PeerCerts` / connection extensions on every
//! request. The identity is parsed once per connection, used for the routing
//! decision, and handed to handlers as a connection extension.

use std::future::Future;
use std::sync::Arc;

use connectrpc::server::Acceptor;
use connectrpc::{ConnectionInfo, RequestContext, Router, Server, ServiceRequest, ServiceResult};
use tokio::task::JoinSet;

pub mod proto {
    connectrpc::include_generated!();
}

pub use proto::anthropic::connectrpc::custom_accept_loop::v1::*;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// All clients carry a DNS SAN `<name>.workloads.example.com`.
pub const WORKLOAD_DOMAIN: &str = "workloads.example.com";

/// Who is on the other end of a connection, from its client certificate.
#[derive(Clone, Debug)]
pub struct Identity {
    /// Short workload name, e.g. `"batch-indexer"`.
    pub name: String,
}

/// Which runtime a connection is served on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    /// Latency-sensitive callers: served on the runtime running the loop.
    Interactive,
    /// `batch-*` workloads: served on a separate, smaller runtime so their
    /// bursts cannot starve interactive callers of worker threads.
    Bulk,
}

impl Identity {
    /// Parse the leaf certificate's workload SAN. `None` means "not one of
    /// ours": no certificate, or no SAN under [`WORKLOAD_DOMAIN`].
    pub fn from_connection(conn: &ConnectionInfo) -> Option<Self> {
        use x509_parser::extensions::GeneralName;
        use x509_parser::prelude::{FromDer, X509Certificate};

        let leaf = conn.peer_certs()?.first()?;
        let (_, cert) = X509Certificate::from_der(leaf.as_ref()).ok()?;
        let suffix = format!(".{WORKLOAD_DOMAIN}");
        cert.subject_alternative_name()
            .ok()??
            .value
            .general_names
            .iter()
            .find_map(|san| match san {
                GeneralName::DNSName(dns) => dns
                    .strip_suffix(suffix.as_str())
                    .filter(|name| !name.is_empty() && !name.contains('.'))
                    .map(|name| Identity {
                        name: name.to_owned(),
                    }),
                _ => None,
            })
    }

    pub fn lane(&self) -> Lane {
        if self.name.starts_with("batch-") {
            Lane::Bulk
        } else {
            Lane::Interactive
        }
    }
}

/// The accept loop. Compare with `Server::serve`: the only additions are the
/// admission check and the choice of runtime; everything a connection needs
/// after that comes from `serve_connection`.
///
/// Two rules the loop owns (the library cannot): it must outlive what it
/// spawned — so it tracks connections and drains them before returning — and
/// the runtime that accepted a socket must outlive the connection served on
/// it, because that is where the socket's IO is registered even when another
/// runtime runs the connection.
pub async fn serve(
    acceptor: Acceptor,
    server: Arc<Server>,
    bulk: tokio::runtime::Handle,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    let (drain_tx, drain_rx) = tokio::sync::watch::channel(false);
    let mut connections = JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let accepted = tokio::select! {
            biased; // a pending shutdown wins over one more accept
            () = &mut shutdown => break,
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                log_task_failure(joined);
                continue;
            }
            accepted = acceptor.accept() => match accepted {
                Ok(accepted) => accepted,
                // The listener is dead. Let live connections finish on their
                // own (they see `drain_tx` drop and wind down) instead of
                // aborting them with the `JoinSet`.
                Err(err) => {
                    connections.detach_all();
                    return Err(err);
                }
            },
        };
        let server = Arc::clone(&server);
        let bulk = bulk.clone();
        let mut drain = drain_rx.clone();
        connections.spawn(async move {
            // TLS handshake on the connection's own task, never on the loop.
            let (io, mut info) = match accepted.handshake().await {
                Ok(conn) => conn,
                Err(err) => {
                    // A stalled handshake is worth surfacing (slowloris);
                    // port scanners and plaintext clients are routine.
                    if err.is_timeout() {
                        eprintln!("{err}");
                    }
                    return;
                }
            };
            // Admission: only workloads we recognise get served at all.
            let Some(identity) = Identity::from_connection(&info) else {
                if let Some(peer) = info.peer_addr() {
                    eprintln!("refusing {peer}: no workload identity");
                }
                return;
            };
            // Parsed once here; handlers read it from the request extensions.
            let lane = identity.lane();
            info.extensions_mut().insert(identity);
            let connection = server.serve_connection(io, info, async move {
                let _ = drain.wait_for(|draining| *draining).await;
            });
            // Placement: the connection — its HTTP/2 streams, handlers and
            // timers — runs on whichever runtime polls this future.
            match lane {
                Lane::Interactive => connection.await,
                // If this loop is dropped, the `JoinSet` aborts this task but
                // not the task on `bulk`; that one still winds down, because
                // dropping the loop drops `drain_tx`.
                Lane::Bulk => log_task_failure(bulk.spawn(connection).await),
            }
        });
    }
    // Stop accepting, tell every connection to drain, wait for all of them.
    drop(acceptor);
    let _ = drain_tx.send(true);
    while let Some(joined) = connections.join_next().await {
        log_task_failure(joined);
    }
    Ok(())
}

fn log_task_failure(joined: Result<(), tokio::task::JoinError>) {
    if let Err(err) = joined {
        eprintln!("connection task failed: {err}");
    }
}

// ============================================================================
// PlacementService: reports who called and where the handler ran.
// ============================================================================

pub struct PlacementServiceImpl;

impl PlacementService for PlacementServiceImpl {
    async fn where_am_i(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, WhereAmIRequest>,
    ) -> ServiceResult<WhereAmIResponse> {
        let identity = ctx
            .extensions()
            .get::<Identity>()
            .map(|id| id.name.clone())
            .unwrap_or_default();
        let thread = std::thread::current().name().unwrap_or_default().to_owned();
        connectrpc::Response::ok(WhereAmIResponse {
            identity: Some(identity),
            thread: Some(thread),
            ..Default::default()
        })
    }
}

pub fn router() -> Router {
    Arc::new(PlacementServiceImpl).register(Router::new())
}

// ============================================================================
// In-memory PKI: one CA, one server leaf, one client leaf per workload.
// ============================================================================

pub mod pki {
    use std::collections::HashMap;
    use std::sync::Arc;

    use connectrpc::rustls;
    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair, SanType};
    use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    pub struct Pki {
        /// Requires and verifies client certificates signed by the demo CA.
        pub server_config: Arc<rustls::ServerConfig>,
        clients: HashMap<String, Arc<rustls::ClientConfig>>,
    }

    impl Pki {
        /// Client configuration presenting `workload`'s certificate.
        ///
        /// # Panics
        ///
        /// If `workload` was not passed to [`generate`].
        pub fn client_config(&self, workload: &str) -> Arc<rustls::ClientConfig> {
            Arc::clone(&self.clients[workload])
        }
    }

    /// A fresh CA, a server leaf for `localhost` (ALPN `h2`), and one client
    /// leaf per workload with SAN `<name>.workloads.example.com`.
    pub fn generate(workloads: &[&str]) -> Pki {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let ca_key = KeyPair::generate().expect("generate CA key");
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(ca_params, ca_key).expect("self-sign CA");
        let issue = |san: &str| -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
            let key = KeyPair::generate().expect("generate leaf key");
            let mut params = CertificateParams::default();
            params.subject_alt_names = vec![SanType::DnsName(san.try_into().expect("DNS SAN"))];
            let cert = params.signed_by(&key, &ca).expect("sign leaf");
            (
                CertificateDer::from(cert.der().to_vec()),
                PrivatePkcs8KeyDer::from(key.serialized_der().to_vec()).into(),
            )
        };

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(ca.der().to_vec()))
            .expect("add CA to roots");
        let roots = Arc::new(roots);

        let (server_cert, server_key) = issue("localhost");
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::clone(&roots))
            .build()
            .expect("client verifier");
        let mut server_config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![server_cert], server_key)
            .expect("server cert");
        server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        let clients = workloads
            .iter()
            .map(|name| {
                let (cert, key) = issue(&format!("{name}.{}", super::WORKLOAD_DOMAIN));
                let config = rustls::ClientConfig::builder()
                    .with_root_certificates(Arc::clone(&roots))
                    .with_client_auth_cert(vec![cert], key)
                    .expect("client cert");
                ((*name).to_owned(), Arc::new(config))
            })
            .collect();

        Pki {
            server_config: Arc::new(server_config),
            clients,
        }
    }
}
