//! ELIZA ConnectRPC server example.
//!
//! This example implements the classic ELIZA psychotherapist chatbot as a
//! ConnectRPC service, matching the examples-go implementation from
//! <https://github.com/connectrpc/examples-go>.
//!
//! ## Running
//!
//! Plaintext (http://):
//! ```bash
//! cargo run -p eliza-example --bin eliza-server
//! ```
//!
//! TLS (https://) — requires `--features tls`:
//! ```bash
//! cargo run --bin eliza-server --features tls -- --cert server.pem --key server.key
//! ```
//!
//! mTLS (server verifies client certificates):
//! ```bash
//! cargo run --bin eliza-server --features tls -- \
//!   --cert server.pem --key server.key --client-ca client-ca.pem
//! ```
//!
//! See `--help` for all flags (addr, stream-delay).
//!
//! ## Testing with curl
//!
//! Unary (Say):
//! ```bash
//! curl -X POST http://localhost:8080/connectrpc.eliza.v1.ElizaService/Say \
//!   -H "Content-Type: application/json" \
//!   -d '{"sentence": "I feel happy"}'
//! ```

#[path = "generated/connect/mod.rs"]
mod connect;
mod eliza;
#[path = "generated/buffa/mod.rs"]
mod proto;

use connect::connectrpc::eliza::v1::*;
use proto::connectrpc::eliza::v1::*;

use connectrpc::{
    RequestContext, Response, Router as ConnectRouter, ServiceRequest, ServiceResult,
    ServiceStream, StreamMessage,
};
use std::sync::Arc;
use tokio::time::{Duration, sleep};

/// Our ELIZA service implementation.
struct ElizaServer {
    stream_delay: Duration,
}

impl ElizaServer {
    fn new(stream_delay: Duration) -> Self {
        Self { stream_delay }
    }
}

// `ServiceRequest<'_, Foo>` gives zero-copy borrowed access to request
// fields (e.g. `request.sentence: &str` points into the decoded buffer).
// The borrow can be held across `.await` points; anything that must outlive
// the call takes owned data via `request.to_owned_message()`.
impl ElizaService for ElizaServer {
    async fn say(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, SayRequest>,
    ) -> ServiceResult<SayResponse> {
        // `request.sentence` is a `&str` borrow into the decoded buffer.
        // No allocation, no copy — `eliza::reply` just reads it.
        let (reply, _end_session) = eliza::reply(request.sentence);
        Response::ok(SayResponse {
            sentence: reply,
            ..Default::default()
        })
    }

    async fn introduce(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, IntroduceRequest>,
    ) -> ServiceResult<ServiceStream<IntroduceResponse>> {
        let delay = self.stream_delay;

        // Zero-copy read of the name; fall back to a default for the empty case.
        let name = if request.name.is_empty() {
            "Anonymous User"
        } else {
            request.name
        };

        // `get_intro_responses` returns a `Vec<String>` — owned output,
        // so the request borrow is released before the stream is built.
        let intros = eliza::get_intro_responses(name);

        // Delay before each message, including the first — matches the
        // Go reference's time.Ticker semantics (examples-go/cmd/demoserver).
        let response_stream =
            futures::stream::unfold(intros.into_iter(), move |mut iter| async move {
                let sentence = iter.next()?;
                if delay > Duration::ZERO {
                    sleep(delay).await;
                }
                Some((
                    Ok(IntroduceResponse {
                        sentence,
                        ..Default::default()
                    }),
                    iter,
                ))
            });

        Response::stream_ok(response_stream)
    }

    async fn converse(
        &self,
        _ctx: RequestContext,
        requests: ServiceStream<StreamMessage<ConverseRequest>>,
    ) -> ServiceResult<ServiceStream<ConverseResponse>> {
        use futures::StreamExt;

        // Unfold over the request stream so we can end the response stream
        // early when Eliza detects a goodbye. The Go reference sends the
        // farewell reply and then breaks; same here by returning None on the
        // iteration after `end_session` was set.
        let response_stream = futures::stream::unfold(
            (requests, false),
            |(mut requests, session_ended)| async move {
                if session_ended {
                    return None;
                }
                match requests.next().await {
                    None => None, // Client closed its send side.
                    Some(Err(e)) => Some((Err(e), (requests, true))),
                    Some(Ok(req)) => {
                        // Each stream item is a `StreamMessage`. `req.sentence()`
                        // borrows the item's buffer; the borrow is released
                        // when `req` drops at the end of this arm, well
                        // before the next `.next().await`.
                        let (reply, end_session) = eliza::reply(req.sentence());
                        Some((
                            Ok(ConverseResponse {
                                sentence: reply,
                                ..Default::default()
                            }),
                            (requests, end_session),
                        ))
                    }
                }
            },
        );

        Response::stream_ok(response_stream)
    }
}

// ============================================================================
// CLI arguments
// ============================================================================

use clap::Parser;

/// ELIZA ConnectRPC server.
///
/// With `--features tls`, supports TLS (and mTLS via `--client-ca`).
/// Without TLS flags, serves plaintext http://.
#[derive(Parser)]
#[command(about)]
struct Args {
    /// Address to listen on. Use `0.0.0.0:8080` or `[::]:8080` to bind all
    /// interfaces (IPv4 / IPv6 respectively). On Linux, `[::]:8080` also
    /// accepts IPv4 via IPv4-mapped addresses by default.
    #[arg(long, default_value = "127.0.0.1:8080", env = "ADDR")]
    addr: String,

    /// Delay between server-stream responses (e.g. "100ms"). Default: no delay.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "0s")]
    stream_delay: Duration,

    /// TLS options (only present when built with --features tls).
    #[cfg(feature = "tls")]
    #[command(flatten)]
    tls: tls::TlsArgs,
}

// ============================================================================
// Main — builds the service, then dispatches to plaintext or TLS serve.
//
// All TLS complexity lives in the `tls` module below, gated by a single cfg.
// The rest of this file is cfg-free.
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let service = Arc::new(ElizaServer::new(args.stream_delay));
    let router = service.register(ConnectRouter::new());

    serve(&args, router).await
}

#[cfg(feature = "tls")]
async fn serve(
    args: &Args,
    router: ConnectRouter,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // If --cert/--key are provided, serve TLS. Otherwise fall through to
    // plaintext (same behavior as the non-tls build).
    if let (Some(cert), Some(key)) = (&args.tls.cert, &args.tls.key) {
        let cfg = tls::build_server_config(cert, key, args.tls.client_ca.as_deref())?;
        return tls::serve_tls(&args.addr, router, cfg, args.tls.client_ca.is_some()).await;
    }
    serve_plaintext(&args.addr, router).await
}

#[cfg(not(feature = "tls"))]
async fn serve(
    args: &Args,
    router: ConnectRouter,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    serve_plaintext(&args.addr, router).await
}

/// Plaintext http:// via axum. Includes a `/health` endpoint alongside
/// the ConnectRPC routes (the TLS path uses the built-in `Server` which
/// doesn't have per-route configuration — trade-off for simplicity).
async fn serve_plaintext(
    addr: &str,
    router: ConnectRouter,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = axum::Router::new()
        .route("/health", axum::routing::get(|| async { "OK" }))
        .fallback_service(router.into_axum_service())
        // Required for browser-based clients (e.g. wasm-client example).
        .layer(tower_http::cors::CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("ELIZA server listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

// ============================================================================
// TLS machinery (only compiled with --features tls)
//
// Everything that touches rustls, PEM loading, root stores, or client cert
// verification lives here behind a single cfg gate.
// ============================================================================

#[cfg(feature = "tls")]
mod tls {
    use super::ConnectRouter;
    use connectrpc::Server;
    use connectrpc::rustls;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::io::BufReader;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    #[derive(clap::Args)]
    pub struct TlsArgs {
        /// Server TLS certificate chain (PEM). Enables TLS when set with --key.
        #[arg(long, requires = "key")]
        pub cert: Option<PathBuf>,

        /// Server TLS private key (PEM). Enables TLS when set with --cert.
        #[arg(long, requires = "cert")]
        pub key: Option<PathBuf>,

        /// Client CA bundle (PEM) for mTLS. When set, the server REQUIRES and
        /// verifies client certificates against this CA. Implies --cert/--key.
        #[arg(long, requires_all = ["cert", "key"])]
        pub client_ca: Option<PathBuf>,
    }

    /// Build a rustls ServerConfig from PEM files.
    ///
    /// If `client_ca_path` is provided, the config REQUIRES and verifies
    /// client certificates against it (mTLS). Otherwise standard TLS with
    /// no client auth.
    ///
    /// ALPN is set to `["h2", "http/1.1"]` so Connect-over-h1 and
    /// gRPC-over-h2 clients can both connect.
    pub fn build_server_config(
        cert_path: &Path,
        key_path: &Path,
        client_ca_path: Option<&Path>,
    ) -> Result<Arc<rustls::ServerConfig>, BoxError> {
        let certs = load_cert_chain(cert_path, "server cert")?;
        let key = load_private_key(key_path, "server key")?;

        let mut config = match client_ca_path {
            None => rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| format!("build TLS config: {e}"))?,

            Some(ca_path) => {
                let verifier = build_client_verifier(ca_path)?;
                rustls::ServerConfig::builder()
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(certs, key)
                    .map_err(|e| format!("build mTLS config: {e}"))?
            }
        };

        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Arc::new(config))
    }

    /// Serve via `connectrpc::Server::with_tls`.
    pub async fn serve_tls(
        addr: &str,
        router: ConnectRouter,
        config: Arc<rustls::ServerConfig>,
        mtls: bool,
    ) -> Result<(), BoxError> {
        let bound = Server::bind(addr).await?;
        tracing::info!("ELIZA server listening on https://{addr} (mTLS: {mtls})");
        bound.with_tls(config).serve(router).await
    }

    /// Build a `WebPkiClientVerifier` that REQUIRES client certificates
    /// signed by the CA(s) in `ca_path`.
    fn build_client_verifier(
        ca_path: &Path,
    ) -> Result<Arc<dyn rustls::server::danger::ClientCertVerifier>, BoxError> {
        let mut roots = rustls::RootCertStore::empty();
        for ca in load_cert_chain(ca_path, "client-ca")? {
            roots
                .add(ca)
                .map_err(|e| format!("add client CA to root store: {e}"))?;
        }
        rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| format!("build client cert verifier: {e}").into())
    }

    // --- Primitive PEM loaders ---

    fn load_cert_chain(path: &Path, what: &str) -> Result<Vec<CertificateDer<'static>>, BoxError> {
        let file = std::fs::File::open(path)
            .map_err(|e| format!("open {what} {}: {e}", path.display()))?;
        rustls_pemfile::certs(&mut BufReader::new(file))
            .collect::<Result<_, _>>()
            .map_err(|e| format!("parse {what} {}: {e}", path.display()).into())
    }

    fn load_private_key(path: &Path, what: &str) -> Result<PrivateKeyDer<'static>, BoxError> {
        let file = std::fs::File::open(path)
            .map_err(|e| format!("open {what} {}: {e}", path.display()))?;
        rustls_pemfile::private_key(&mut BufReader::new(file))
            .map_err(|e| format!("parse {what} {}: {e}", path.display()))?
            .ok_or_else(|| format!("no private key found in {}", path.display()).into())
    }
}
