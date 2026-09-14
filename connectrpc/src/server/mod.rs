//! Hyper-based HTTP server for ConnectRPC.
//!
//! This module provides the HTTP server implementation that handles incoming
//! ConnectRPC requests and routes them to the appropriate handlers.
//!
//! # TLS Support
//!
//! When the `tls` feature is enabled, the server can be configured with a
//! [`rustls::ServerConfig`] to serve requests over TLS:
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
//! # Connection Retirement
//!
//! Retire long-lived connections proactively — recommended behind load
//! balancers so clients reconnect periodically and traffic redistributes
//! across restarts. Two independent triggers are available, and either, both,
//! or neither may be set:
//!
//! - [`Server::with_max_connection_age`] (or the [`BoundServer`] equivalent)
//!   retires by age: a connection is sent a GOAWAY once it reaches the
//!   configured age (with a ±10% jitter).
//! - [`Server::with_max_requests_per_connection`] retires by request count: a
//!   connection is sent a GOAWAY once it has dispatched the configured number
//!   of requests.
//!
//! When both are set, whichever trigger fires first retires the connection.
//! After a trigger fires the connection is force-closed once the shared grace
//! period ([`with_max_connection_age_grace`](BoundServer::with_max_connection_age_grace))
//! elapses. Retirement is independent of whole-server graceful shutdown, which
//! still drains in-flight requests indefinitely even while a connection is in
//! its grace window.
//!
//! # Maximum Concurrent Streams
//!
//! Use [`Server::with_max_concurrent_streams`] (or the [`BoundServer`]
//! equivalent) to bound the number of concurrent HTTP/2 streams (in-flight
//! requests) a single connection may have open. This maps to hyper's
//! `SETTINGS_MAX_CONCURRENT_STREAMS`; it is left at hyper's default (200)
//! when unset. Raise it for high-fan-in internal services, or lower it as a
//! cheap hardening measure against less-trusted clients.
//!
//! # HTTP/2 Keepalive
//!
//! Use [`Server::with_http2_keepalive_interval`] (or the [`BoundServer`]
//! equivalent) to make the server send HTTP/2 keepalive PING frames and
//! reclaim dead or half-open peers. Disabled by default. Once an interval is
//! set, an unacknowledged PING after
//! [`with_http2_keepalive_timeout`](BoundServer::with_http2_keepalive_timeout)
//! (20 seconds by default) closes the connection. This detects long-lived
//! server-streaming or bidirectional connections that have gone silent (NAT
//! timeout, client crash, network partition) instead of leaving them
//! half-open until the OS TCP timeout.
//!
//! # Maximum Connection Idle
//!
//! Use [`Server::with_max_connection_idle`] (or the [`BoundServer`] equivalent)
//! to reclaim connections that have gone quiet. A connection is idle when it
//! has no in-flight requests; once it stays idle for the configured duration it
//! is retired through the same GOAWAY-then-grace path as maximum age, draining
//! over the same grace period set by `with_max_connection_age_grace`. The idle
//! timer resets on activity, so a connection with steady traffic is never
//! retired. The window is evaluated lazily, so retirement happens between one
//! and two times the configured duration after the last activity. When both
//! limits are configured, whichever fires first wins.
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
//! For transport and HTTP/2 knobs that [`Server`] does not expose, drive
//! [`ConnectRpcService`](crate::ConnectRpcService) directly from a hyper accept loop. The crate guide's
//! "Advanced transport configuration" section shows the `hyper_util` pattern.

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
