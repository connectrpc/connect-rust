//! Hyper-based HTTP server for ConnectRPC, in four layers you can also use
//! one at a time.
//!
//! | Layer | Item | Owns |
//! |---|---|---|
//! | 1. protocol | [`ConnectRpcService`](crate::ConnectRpcService) | RPC semantics: codecs, envelopes, compression, deadlines, limits, interceptors. A tower `Service`; knows nothing about sockets. |
//! | 2. connection | [`serve_connection`] + [`ConnectionConfig`] | One connection's HTTP lifecycle: HTTP/1.1 and HTTP/2 settings, keepalive, header-read timeout, max age / idle / request retirement, GOAWAY on shutdown, panic isolation, [`PeerAddr`] / `PeerCerts` / connection extensions on every request. A future; runs wherever it is polled. |
//! | 3. acceptor | [`Acceptor`] + [`AcceptConfig`] → [`ConnectionInfo`] | Turning a TCP listener into authenticated streams: accept, `TCP_NODELAY`, TLS handshake with its timeout, capture of peer facts. No HTTP. |
//! | 4. loop | [`Server`] / [`BoundServer`] (and `connectrpc::axum::serve`) | Placement and drain: hand each accepted stream (3) to the driver (2) on the ambient runtime, fan out the shutdown signal, wait. |
//!
//! Most services use layer 4 and never look further. The layers below are
//! public so that an accept-time or per-connection policy the built-in loop
//! does not have — admit or shed by client identity or address, cap
//! connections per tenant, serve different clients on different runtimes,
//! listen on something other than TCP — is a short loop of your own over
//! [`Acceptor`] and [`Server::serve_connection`] that keeps every guarantee of
//! layer 2, rather than a feature request or a from-scratch hyper server. See
//! `examples/custom-accept-loop`.
//!
//! # TLS Support
//!
//! With the `server-tls` feature, pass a `rustls::ServerConfig` to serve
//! over TLS; a verified client certificate chain reaches handlers as
//! `PeerCerts`:
//!
//! ```rust,ignore
//! let tls_config = Arc::new(rustls::ServerConfig::builder()
//!     .with_no_client_auth()
//!     .with_single_cert(certs, key)?);
//!
//! Server::new(router)
//!     .with_tls(tls_config)
//!     .serve(addr).await?;
//! ```
//!
//! # Graceful Shutdown
//!
//! Use [`BoundServer::serve_with_graceful_shutdown`] to stop accepting new
//! connections when a signal future resolves, then drain in-flight connections
//! before returning:
//!
//! ```rust,ignore
//! let bound = Server::bind("127.0.0.1:8080").await?;
//! bound
//!     .serve_with_graceful_shutdown(router, async {
//!         tokio::signal::ctrl_c().await.ok();
//!     })
//!     .await?;
//! ```
//!
//! # Connection Settings
//!
//! Every per-connection setting lives on [`ConnectionConfig`], a plain value
//! with a `Default` that `Server`, `BoundServer`, `connectrpc::axum::Serve`
//! and [`serve_connection`] all accept (`with_connection_config`); the
//! `with_*` setters on `Server` / `BoundServer` are shorthand for editing it.
//!
//! - **Retirement.** Behind a load balancer, retire long-lived connections so
//!   clients reconnect and traffic redistributes across restarts. Three
//!   independent triggers —
//!   [`with_max_connection_age`](ConnectionConfig::with_max_connection_age)
//!   (±10% jitter),
//!   [`with_max_connection_idle`](ConnectionConfig::with_max_connection_idle)
//!   (no in-flight requests for the duration; evaluated lazily, so retirement
//!   lands between one and two windows after the last activity) and
//!   [`with_max_requests_per_connection`](ConnectionConfig::with_max_requests_per_connection)
//!   — each send a GOAWAY and then force-close after the shared
//!   [`with_max_connection_age_grace`](ConnectionConfig::with_max_connection_age_grace);
//!   whichever fires first wins. Whole-server graceful shutdown still drains
//!   in-flight requests indefinitely, even inside a grace window.
//! - **HTTP/2.** [`with_max_concurrent_streams`](ConnectionConfig::with_max_concurrent_streams)
//!   bounds in-flight requests per connection (hyper's default is 200);
//!   [`with_http2_keepalive_interval`](ConnectionConfig::with_http2_keepalive_interval)
//!   / [`with_http2_keepalive_timeout`](ConnectionConfig::with_http2_keepalive_timeout)
//!   send PINGs and reclaim dead or half-open peers on long-lived streams;
//!   adaptive flow-control windows are on by default
//!   ([`DEFAULT_HTTP2_ADAPTIVE_WINDOW`]).
//! - **HTTP/1.1.** The header-read timeout ([`DEFAULT_HEADER_READ_TIMEOUT`])
//!   bounds slow or stalled request heads, including between keep-alive
//!   requests.
//!
//! # Connection-Scoped Extensions
//!
//! Every request carries the peer's address ([`PeerAddr`]) and, over mTLS,
//! its verified certificate chain (`PeerCerts`) in its extensions; handlers
//! read them with `ctx.peer_addr()` / `ctx.peer_certs()`. Both come from the
//! connection's [`ConnectionInfo`] and always reflect what the transport
//! observed. State derived from the connection once and reused by every
//! request on it — a parsed client-certificate identity, a tenant, a metrics
//! label — goes into [`ConnectionInfo::extensions_mut`] in a custom accept
//! loop before [`serve_connection`] is called, and is cloned into each
//! request's extensions to be read with `ctx.extensions().get::<T>()`.
//!
//! # Runtimes
//!
//! The connection driver is tokio-based (timers, IO traits, and hyper's
//! per-stream tasks use the runtime that polls the connection future); it
//! takes no executor parameter. A loop that wants a connection served on a
//! particular runtime spawns [`serve_connection`]'s future there. The runtime
//! that *accepted* a socket must outlive the connection, since that is where
//! the socket's IO is registered.

pub(crate) mod accept_loop;
mod acceptor;
mod config;
mod connection;
mod peer;
mod standalone;

pub use acceptor::Accepted;
pub use acceptor::Acceptor;
pub use acceptor::HandshakeError;
pub use acceptor::ServerIo;
pub use config::AcceptConfig;
pub use config::ConnectionConfig;
pub use config::DEFAULT_HEADER_READ_TIMEOUT;
pub use config::DEFAULT_HTTP2_ADAPTIVE_WINDOW;
pub use config::DEFAULT_HTTP2_KEEPALIVE_TIMEOUT;
#[cfg(feature = "server-tls")]
pub use config::DEFAULT_TLS_HANDSHAKE_TIMEOUT;
pub use connection::serve_connection;
pub use peer::ConnectionInfo;
pub use peer::PeerAddr;
#[cfg(feature = "server-tls")]
pub use peer::PeerCerts;
pub use standalone::BoundServer;
pub use standalone::Server;

#[cfg(test)]
pub(crate) mod tests;
