//! mTLS cert-SAN identity for axum-hosted ConnectRPC services.
//!
//! Mirrors `examples/middleware/`, swapping bearer-token auth for mTLS:
//! instead of an `axum::middleware::from_fn` reading an `Authorization`
//! header, identity comes from the verified client certificate that
//! `connectrpc::axum::serve_tls` captures during the TLS handshake. The
//! function registered with
//! [`with_connection_extensions`](connectrpc::axum::Serve::with_connection_extensions)
//! parses the leaf cert's DNS SAN into a [`PeerIdentity`] once per
//! connection; every request on that connection carries it in its
//! extensions, and the handler reads it from [`RequestContext::extensions`]
//! to enforce an ACL. The raw chain is still available per request as
//! [`connectrpc::PeerCerts`].
//!
//! The same handler code works unchanged on the standalone
//! [`connectrpc::Server::with_tls`] or in a custom accept loop — the hosting
//! choice doesn't leak into authorization logic. The README says, for each
//! hosting path, where `PeerIdentity` enters the connection's extensions.

use std::collections::HashMap;
use std::sync::Arc;

use connectrpc::{
    ConnectError, ConnectionInfo, ErrorCode, RequestContext, Router, ServiceRequest, ServiceResult,
};

pub mod proto {
    connectrpc::include_generated!();
}

pub use proto::anthropic::connectrpc::mtls_identity::v1::*;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ============================================================================
// Identity: derive a workload name from the leaf cert's DNS SAN.
// ============================================================================

/// All clients in this demo carry a SAN under this domain. Anything else
/// is rejected as `Unauthenticated`.
pub const WORKLOAD_DOMAIN: &str = "workloads.example.com";

/// Caller identity, parsed from the leaf certificate's DNS SAN.
#[derive(Debug, Clone)]
pub struct Identity {
    /// Short workload name, e.g. `"alice"`.
    pub name: String,
    /// Full DNS SAN as presented, e.g. `"alice.workloads.example.com"`.
    pub san: String,
}

/// Parse a workload identity out of the leaf certificate's DNS SANs.
///
/// Returns the first SAN under [`WORKLOAD_DOMAIN`]. Returns
/// `Unauthenticated` when no client cert was presented (a non-mTLS
/// connection) or no SAN matches the expected domain.
///
/// In a real deployment you'd typically match a SPIFFE ID
/// (`spiffe://trust-domain/path`, a URI SAN) instead of a DNS SAN, or
/// delegate this whole step to an authorization framework. The shape is
/// the same: take the verified chain, parse the leaf, derive an identity —
/// once per connection via [`PeerIdentity::from_connection`].
pub fn extract_identity(
    certs: Option<&[rustls_pki_types::CertificateDer<'static>]>,
) -> Result<Identity, ConnectError> {
    use x509_parser::extensions::GeneralName;
    use x509_parser::prelude::{FromDer, X509Certificate};

    let leaf = certs.and_then(<[_]>::first).ok_or_else(|| {
        ConnectError::new(ErrorCode::Unauthenticated, "client certificate required")
    })?;

    let (_, parsed) = X509Certificate::from_der(leaf.as_ref()).map_err(|e| {
        ConnectError::new(ErrorCode::Unauthenticated, format!("bad client cert: {e}"))
    })?;

    let suffix = format!(".{WORKLOAD_DOMAIN}");
    parsed
        .subject_alternative_name()
        .ok()
        .flatten()
        .into_iter()
        .flat_map(|ext| ext.value.general_names.iter())
        .find_map(|gn| {
            // Only `<single-label>.workloads.example.com` is a workload SAN.
            // Reject `.workloads.example.com` and `a.b.workloads.example.com`:
            // we intend to accept only direct subdomains of the workload
            // domain.
            let GeneralName::DNSName(dns) = gn else {
                return None;
            };
            let prefix = dns.strip_suffix(&suffix)?;
            if prefix.is_empty() || prefix.contains('.') {
                return None;
            }
            Some(Identity {
                name: prefix.to_owned(),
                san: (*dns).to_owned(),
            })
        })
        .ok_or_else(|| {
            ConnectError::new(
                ErrorCode::Unauthenticated,
                format!("client cert has no workload SAN under {WORKLOAD_DOMAIN}"),
            )
        })
}

/// The caller's identity, parsed from its client certificate once per
/// connection and stamped into every request's extensions.
///
/// Holds the [`extract_identity`] outcome rather than just the success case
/// so a handler reports the same `Unauthenticated` detail it would have
/// produced by parsing the chain itself.
#[derive(Debug, Clone)]
pub struct PeerIdentity(pub Result<Identity, ConnectError>);

impl PeerIdentity {
    /// Parse the leaf certificate of `conn`; run it once per accepted TLS
    /// connection, not once per request.
    pub fn from_connection(conn: &ConnectionInfo) -> Self {
        Self(extract_identity(conn.peer_certs()))
    }

    /// The identity for this request, as parsed for its connection. A missing
    /// [`PeerIdentity`] means the hosting path never populated it — a server
    /// misconfiguration, reported as `Internal` rather than by silently
    /// re-parsing the chain per request.
    pub fn for_request(ctx: &RequestContext) -> Result<Identity, ConnectError> {
        let Some(PeerIdentity(parsed)) = ctx.extensions().get::<PeerIdentity>() else {
            return Err(ConnectError::new(
                ErrorCode::Internal,
                "no PeerIdentity on the request: populate it with with_connection_extensions",
            ));
        };
        parsed.clone()
    }
}

// ============================================================================
// IdentityService handler
// ============================================================================

/// Static secret store. Each secret declares which workloads may read it.
pub fn secret_store() -> HashMap<String, (String, Vec<&'static str>)> {
    HashMap::from([
        (
            "shared".into(),
            ("the value of teamwork".into(), vec!["alice", "bob"]),
        ),
        (
            "alice-only".into(),
            ("alice's diary entry".into(), vec!["alice"]),
        ),
    ])
}

pub struct IdentityServiceImpl {
    pub store: HashMap<String, (String, Vec<&'static str>)>,
}

impl IdentityService for IdentityServiceImpl {
    async fn who_am_i(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, WhoAmIRequest>,
    ) -> ServiceResult<WhoAmIResponse> {
        // PeerIdentity was parsed once for this connection by the
        // `with_connection_extensions` function; PeerAddr is stamped
        // alongside it. The dispatcher copies request extensions verbatim
        // into the request context.
        let id = PeerIdentity::for_request(&ctx)?;
        let remote = ctx.peer_addr().map(|a| a.to_string()).unwrap_or_default();
        connectrpc::Response::ok(WhoAmIResponse {
            identity: Some(id.name),
            san: Some(id.san),
            remote_addr: Some(remote),
            ..Default::default()
        })
    }

    async fn get_secret(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetSecretRequest>,
    ) -> ServiceResult<GetSecretResponse> {
        let id = PeerIdentity::for_request(&ctx)?;
        let name = request.name.unwrap_or("").to_owned();
        let (value, allowed) = self.store.get(&name).ok_or_else(|| {
            ConnectError::new(ErrorCode::NotFound, format!("no secret named {name:?}"))
        })?;
        if !allowed.iter().any(|w| *w == id.name) {
            return Err(ConnectError::new(
                ErrorCode::PermissionDenied,
                format!("workload {:?} ({}) cannot read {name:?}", id.name, id.san),
            ));
        }
        Ok(connectrpc::Response::new(GetSecretResponse {
            value: Some(value.clone()),
            ..Default::default()
        })
        .with_trailer("x-served-by", id.name))
    }
}

// ============================================================================
// Server hosting: axum app behind connectrpc::axum::serve_tls.
// ============================================================================

/// Build the axum app and serve it over TLS until `shutdown` resolves.
///
/// Two lines differ from a plaintext axum app: `connectrpc::axum::serve_tls`
/// instead of `axum::serve`, and `with_connection_extensions` parsing the
/// client certificate into a [`PeerIdentity`] once per connection.
pub async fn serve(
    listener: tokio::net::TcpListener,
    server_config: Arc<connectrpc::rustls::ServerConfig>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let svc = Arc::new(IdentityServiceImpl {
        store: secret_store(),
    });
    let connect_router = svc.register(Router::new());
    let app = axum::Router::new().fallback_service(connect_router.into_axum_service());

    connectrpc::axum::serve_tls(listener, app, server_config)
        // Once per connection, after the TLS handshake: parse the leaf cert
        // here so handlers don't re-parse DER on every request.
        .with_connection_extensions(|conn, ext| {
            ext.insert(PeerIdentity::from_connection(conn));
        })
        .with_graceful_shutdown(shutdown)
        .await
}

// ============================================================================
// In-memory PKI: one CA, one server leaf, N client leafs (DNS-SAN identities).
// ============================================================================

pub mod pki {
    use std::sync::Arc;

    use connectrpc::rustls;
    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair, SanType};
    use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    /// One client credential: a leaf cert (with a SAN under the workload
    /// domain) and its private key.
    pub struct ClientCredential {
        pub cert: CertificateDer<'static>,
        pub key: PrivateKeyDer<'static>,
    }

    /// In-memory PKI for the demo.
    pub struct Pki {
        /// Server [`rustls::ServerConfig`] requiring client certs signed
        /// by the demo CA.
        pub server_config: Arc<rustls::ServerConfig>,
        /// Trust roots containing only the demo CA, for building client
        /// configs.
        pub roots: Arc<rustls::RootCertStore>,
        /// Per-workload client credentials, keyed by short name.
        pub clients: std::collections::HashMap<String, ClientCredential>,
    }

    impl Pki {
        /// Build a [`rustls::ClientConfig`] that trusts the demo CA and
        /// presents the named workload's client cert during the handshake.
        ///
        /// # Panics
        ///
        /// Panics if `workload` isn't one of the names passed to [`generate`].
        pub fn client_config(&self, workload: &str) -> Arc<rustls::ClientConfig> {
            let cred = self
                .clients
                .get(workload)
                .unwrap_or_else(|| panic!("no credential for workload {workload:?}"));
            Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(Arc::clone(&self.roots))
                    .with_client_auth_cert(vec![cred.cert.clone()], cred.key.clone_key())
                    .expect("valid client cert"),
            )
        }
    }

    /// Generate a fresh CA, server leaf (`SAN = localhost`), and one client
    /// leaf per name in `workloads` (`SAN = <name>.workloads.example.com`).
    ///
    /// No PEM files touch disk: this is the same shape a deployment would
    /// load from a secret store, but generated in-process for the demo.
    pub fn generate(workloads: &[&str]) -> Pki {
        // Idempotent; err == already installed (tests share process state).
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let ca_key = KeyPair::generate().expect("generate CA key");
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(ca_params, ca_key).expect("self-sign CA");

        // Issue a leaf with the given DNS SANs, signed by the demo CA.
        let issue = |sans: &[&str]| -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
            let key = KeyPair::generate().expect("generate leaf key");
            let mut params = CertificateParams::default();
            params.subject_alt_names = sans
                .iter()
                .map(|s| SanType::DnsName((*s).try_into().expect("valid DNS SAN")))
                .collect();
            let cert = params.signed_by(&key, &ca).expect("sign leaf");
            (
                CertificateDer::from(cert.der().to_vec()),
                PrivatePkcs8KeyDer::from(key.serialized_der().to_vec()).into(),
            )
        };

        let (server_cert, server_key) = issue(&["localhost"]);
        let clients = workloads
            .iter()
            .map(|name| {
                let san = format!("{name}.{}", super::WORKLOAD_DOMAIN);
                let (cert, key) = issue(&[&san]);
                (name.to_string(), ClientCredential { cert, key })
            })
            .collect();

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(ca.der().to_vec()))
            .expect("add CA to roots");
        let roots = Arc::new(roots);

        // Require *and verify* client certs. WebPkiClientVerifier rejects
        // anything not chained to the demo CA before the request reaches
        // the handler, so PeerCerts is always a verified chain.
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::clone(&roots))
            .build()
            .expect("build client verifier");
        let server_config = Arc::new(
            rustls::ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(vec![server_cert], server_key)
                .expect("valid server cert"),
        );

        Pki {
            server_config,
            roots,
            clients,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, KeyPair, SanType};
    use rustls_pki_types::CertificateDer;

    /// Self-signed leaf with arbitrary DNS SANs, as a chain.
    fn peer_certs_with_dns_sans(sans: &[&str]) -> Vec<CertificateDer<'static>> {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::default();
        params.subject_alt_names = sans
            .iter()
            .map(|s| SanType::DnsName((*s).try_into().unwrap()))
            .collect();
        let cert = params.self_signed(&key).unwrap();
        vec![CertificateDer::from(cert.der().to_vec())]
    }

    #[test]
    fn extract_identity_rejects_no_cert() {
        assert_eq!(
            extract_identity(None).unwrap_err().code,
            ErrorCode::Unauthenticated
        );
    }

    #[test]
    fn extract_identity_parses_single_label_workload_san() {
        let certs =
            peer_certs_with_dns_sans(&["ignored.example.org", "alice.workloads.example.com"]);
        let id = extract_identity(Some(&certs)).unwrap();
        assert_eq!(id.name, "alice");
        assert_eq!(id.san, "alice.workloads.example.com");
    }

    #[test]
    fn extract_identity_rejects_empty_or_multi_label_prefix() {
        // Empty prefix: ".workloads.example.com" must not yield name = "".
        let empty = peer_certs_with_dns_sans(&[".workloads.example.com"]);
        assert_eq!(
            extract_identity(Some(&empty)).unwrap_err().code,
            ErrorCode::Unauthenticated
        );
        // Multi-label prefix: not a direct subdomain; reject it.
        let multi = peer_certs_with_dns_sans(&["a.b.workloads.example.com"]);
        assert_eq!(
            extract_identity(Some(&multi)).unwrap_err().code,
            ErrorCode::Unauthenticated
        );
    }

    #[test]
    fn extract_identity_rejects_unrelated_domain() {
        let certs = peer_certs_with_dns_sans(&["service.elsewhere.example.org"]);
        assert_eq!(
            extract_identity(Some(&certs)).unwrap_err().code,
            ErrorCode::Unauthenticated
        );
    }
}
