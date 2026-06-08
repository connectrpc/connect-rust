//! ELIZA ConnectRPC client — interactive terminal conversation.
//!
//! Connects to any `connectrpc.eliza.v1.ElizaService` endpoint. By default
//! targets the local server at `http://localhost:8080`. With the `tls`
//! feature enabled, also works against `https://` endpoints including the
//! hosted Go reference demo at `https://demo.connectrpc.com`.
//!
//! ## Running
//!
//! Against the local server (plaintext):
//! ```bash
//! cargo run -p eliza-example --bin eliza-server &
//! cargo run -p eliza-example --bin eliza-client
//! ```
//!
//! Against the hosted Go demo (proves cross-implementation interop):
//! ```bash
//! cargo run --bin eliza-client --features tls -- --url https://demo.connectrpc.com
//! ```
//!
//! With a custom CA bundle (e.g. self-signed server):
//! ```bash
//! cargo run --bin eliza-client --features tls -- \
//!   --url https://localhost:8443 --ca server-ca.pem
//! ```
//!
//! mTLS (client presents a certificate):
//! ```bash
//! cargo run --bin eliza-client --features tls -- \
//!   --url https://localhost:8443 --ca server-ca.pem \
//!   --cert client.pem --key client.key
//! ```
//!
//! Type sentences and press Enter. Say "bye", "quit", or "goodbye" and Eliza
//! will gracefully end the conversation (the server terminates the bidi stream
//! after sending her farewell).

#[path = "generated/connect/mod.rs"]
mod connect;
#[path = "generated/buffa/mod.rs"]
mod proto;

use connect::connectrpc::eliza::v1::*;
use proto::connectrpc::eliza::v1::*;

use clap::Parser;
use connectrpc::client::{BidiStream, ClientConfig, ClientTransport, HttpClient};
use std::io::{self, BufRead, Write};
use std::time::Duration;

type BoxError = Box<dyn std::error::Error>;

/// The concrete `BidiStream` type for the Converse RPC over an `HttpClient`.
/// Spelled out once here so helper signatures stay readable.
type ConvoStream = BidiStream<
    <HttpClient as ClientTransport>::ResponseBody,
    ConverseRequest,
    ConverseResponseView<'static>,
>;

// ============================================================================
// CLI arguments
// ============================================================================

#[derive(Parser)]
#[command(about)]
struct Args {
    /// Eliza server URL. http:// for plaintext, https:// requires --features tls.
    #[arg(long, default_value = "http://localhost:8080", env = "ELIZA_URL")]
    url: String,

    /// Your name (for Eliza's introduction).
    #[arg(long, env = "USER", default_value = "Anonymous User")]
    name: String,

    /// TLS options (only present when built with --features tls).
    #[cfg(feature = "tls")]
    #[command(flatten)]
    tls: tls::TlsArgs,
}

// ============================================================================
// Transport construction
//
// A thin scheme dispatcher delegates to the `tls` module (when built in)
// for https://. All feature-gated complexity lives in that one module;
// the rest of this file is cfg-free.
// ============================================================================

fn make_transport(args: &Args, uri: &http::Uri) -> Result<HttpClient, BoxError> {
    match uri.scheme_str() {
        Some("http") | None => Ok(HttpClient::plaintext()),
        Some("https") => make_https_transport(args),
        Some(other) => Err(format!("unsupported URI scheme: {other}").into()),
    }
}

#[cfg(feature = "tls")]
fn make_https_transport(args: &Args) -> Result<HttpClient, BoxError> {
    tls::make_https_transport(&args.tls)
}

#[cfg(not(feature = "tls"))]
fn make_https_transport(_args: &Args) -> Result<HttpClient, BoxError> {
    Err("https:// requires the 'tls' feature. Rebuild with:\n  \
         cargo run -p eliza-example --bin eliza-client --features tls"
        .into())
}

// ============================================================================
// TLS machinery (only compiled with --features tls)
//
// Everything that touches rustls, PEM loading, root stores, or client certs
// lives here behind a single cfg gate. The rest of the file doesn't know
// or care about any of it.
// ============================================================================

#[cfg(feature = "tls")]
mod tls {
    use super::{BoxError, HttpClient};
    use connectrpc::rustls;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::io::BufReader;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    #[derive(clap::Args)]
    pub struct TlsArgs {
        /// CA bundle (PEM) to trust for server verification. If not set,
        /// webpki-roots (the standard browser trust store) is used.
        #[arg(long)]
        pub ca: Option<PathBuf>,

        /// Client certificate chain (PEM) for mTLS. Requires --key.
        #[arg(long, requires = "key")]
        pub cert: Option<PathBuf>,

        /// Client private key (PEM) for mTLS. Requires --cert.
        #[arg(long, requires = "cert")]
        pub key: Option<PathBuf>,
    }

    /// Build an HttpClient::with_tls from the TLS args.
    ///
    /// Root store: custom `--ca` bundle if provided, else webpki-roots.
    /// Client auth: `--cert`/`--key` for mTLS if provided, else no client auth.
    pub fn make_https_transport(args: &TlsArgs) -> Result<HttpClient, BoxError> {
        let roots = build_root_store(args.ca.as_deref())?;
        let builder = rustls::ClientConfig::builder().with_root_certificates(roots);

        let config = match (args.cert.as_deref(), args.key.as_deref()) {
            (Some(cert), Some(key)) => {
                let (chain, key) = load_client_identity(cert, key)?;
                builder
                    .with_client_auth_cert(chain, key)
                    .map_err(|e| format!("set client cert for mTLS: {e}"))?
            }
            // clap's `requires` attribute ensures these are paired, so the
            // (Some, None) / (None, Some) cases won't happen in practice.
            _ => builder.with_no_client_auth(),
        };

        Ok(HttpClient::with_tls(Arc::new(config)))
    }

    /// Root store: custom CA bundle if provided, else webpki-roots (the
    /// standard browser trust store — validates public endpoints like
    /// demo.connectrpc.com out of the box).
    fn build_root_store(ca_path: Option<&Path>) -> Result<rustls::RootCertStore, BoxError> {
        let mut roots = rustls::RootCertStore::empty();
        match ca_path {
            Some(path) => {
                for ca in load_cert_chain(path, "CA bundle")? {
                    roots
                        .add(ca)
                        .map_err(|e| format!("add CA to root store: {e}"))?;
                }
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }
        Ok(roots)
    }

    /// Load client cert chain + private key for mTLS (`.with_client_auth_cert`).
    fn load_client_identity(
        cert_path: &Path,
        key_path: &Path,
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), BoxError> {
        let certs = load_cert_chain(cert_path, "client cert")?;
        let key = load_private_key(key_path, "client key")?;
        Ok((certs, key))
    }

    // --- Primitive PEM loaders (one file open + parse, with context in errors) ---

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

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let args = Args::parse();
    let base_uri: http::Uri = args.url.parse()?;

    let http = make_transport(&args, &base_uri)?;
    let config = ClientConfig::new(base_uri);
    let client = ElizaServiceClient::new(http, config);

    println!("Connecting to {}...\n", args.url);
    run_conversation(&client, &args.name).await
}

/// Run the Introduce → Converse flow against a connected client.
///
/// Uniform across plaintext and TLS — by this point the transport is set up
/// and we're just speaking the RPC protocol.
async fn run_conversation(
    client: &ElizaServiceClient<HttpClient>,
    name: &str,
) -> Result<(), BoxError> {
    // --- Introduce (server streaming) ---
    // Eliza sends a few introductory sentences; we print each as it arrives.
    let mut intro = client
        .introduce(IntroduceRequest {
            name: name.to_owned(),
            ..Default::default()
        })
        .await?;
    while let Some(msg) = intro.message().await? {
        println!("Eliza> {}", msg.reborrow().sentence);
    }
    if let Some(err) = intro.error() {
        return Err(err.clone().into());
    }

    // --- Converse (bidirectional streaming) ---
    println!("\n(Talking to Eliza as '{name}'. Type your messages; say 'bye' to end.)\n");

    let mut convo = client.converse().await?;
    let stdin = io::stdin();

    // Half-duplex interaction: prompt → read → send → receive → print.
    // (Full-duplex concurrent send/receive is also possible with BidiStream
    // but not needed for conversational eliza.)
    loop {
        print!("You> ");
        io::stdout().flush()?;

        let Some(sentence) = read_line_nonempty(&stdin)? else {
            // EOF (Ctrl-D). Close our send side and drain any final messages.
            convo.close_send();
            while let Some(msg) = convo.message().await? {
                println!("Eliza> {}", msg.reborrow().sentence);
            }
            break;
        };

        // Send can fail if Eliza already closed the stream (e.g. a previous
        // goodbye we didn't notice). Check the receive side for the real cause.
        if let Err(send_err) = convo
            .send(ConverseRequest {
                sentence,
                ..Default::default()
            })
            .await
        {
            // Drain whatever the server has already sent — likely a
            // farewell that arrived before (or racing with) our send. If the
            // stream then closes with no error, the server hung up gracefully
            // and our send just lost the race; treat it as a normal end.
            let mut got_reply = false;
            loop {
                match convo.message().await {
                    Ok(Some(msg)) => {
                        println!("Eliza> {}", msg.reborrow().sentence);
                        got_reply = true;
                    }
                    Ok(None) => {
                        if let Some(err) = convo.error() {
                            return Err(err.clone().into());
                        }
                        if got_reply {
                            // Server sent a farewell then closed cleanly.
                            // Our send just raced the close; swallow it.
                            println!("\n(Eliza has ended the session.)");
                            return Ok(());
                        }
                        // Stream closed with no reply and no stream error —
                        // surface the send error as the only diagnostic we have.
                        return Err(send_err.into());
                    }
                    Err(recv_err) => return Err(recv_err.into()),
                }
            }
        }

        // Receive the response; then peek for END_STREAM.
        match convo.message().await? {
            Some(msg) => {
                println!("Eliza> {}", msg.reborrow().sentence);
                if peek_stream_closed(&mut convo).await? {
                    println!("\n(Eliza has ended the session.)");
                    break;
                }
            }
            None => {
                // Stream ended before we got any response to this send.
                if let Some(err) = convo.error() {
                    return Err(err.clone().into());
                }
                println!("\n(Eliza has ended the session.)");
                break;
            }
        }
    }

    Ok(())
}

/// Read a trimmed line from stdin.
///
/// - `Ok(Some(line))` — non-empty line
/// - `Ok(None)` — EOF (Ctrl-D)
/// - Loops past empty lines, re-prompting `You> ` each time.
fn read_line_nonempty(stdin: &io::Stdin) -> Result<Option<String>, BoxError> {
    loop {
        let mut line = String::new();
        let n = stdin.lock().read_line(&mut line)?;
        if n == 0 {
            return Ok(None);
        }
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            return Ok(Some(trimmed.to_owned()));
        }
        // Empty line — re-prompt and keep reading.
        print!("You> ");
        io::stdout().flush()?;
    }
}

/// Peek for a trailing END_STREAM with a short timeout.
///
/// When the server ends the session (e.g. after a goodbye), it sends the
/// farewell DATA frame immediately followed by END_STREAM. Without this peek,
/// the caller would loop back to `read_line()` which blocks in interactive
/// mode — leaving the user at a `You>` prompt even though the server has
/// already hung up.
///
/// The 100ms window covers network RTT for the END_STREAM frame.
/// Returns `true` if the stream closed (session ended), `false` if it's still
/// open (timeout expired — normal ongoing conversation).
async fn peek_stream_closed(convo: &mut ConvoStream) -> Result<bool, BoxError> {
    match tokio::time::timeout(Duration::from_millis(100), convo.message()).await {
        // Timeout: stream still open.
        Err(_elapsed) => Ok(false),

        // Stream ended.
        Ok(Ok(None)) => {
            if let Some(err) = convo.error() {
                return Err(err.clone().into());
            }
            Ok(true)
        }

        // Server sent another message unprompted. Unusual for Eliza (she's
        // strictly 1:1) but valid for bidi streams in general. Print it and
        // report stream still open.
        Ok(Ok(Some(msg))) => {
            println!("Eliza> {}", msg.reborrow().sentence);
            Ok(false)
        }

        Ok(Err(e)) => Err(e.into()),
    }
}
