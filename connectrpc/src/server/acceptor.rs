//! Layer 3: turn a TCP listener into authenticated byte streams.
//!
//! [`Acceptor::accept`] yields an [`Accepted`] connection; [`Accepted::handshake`]
//! (run on the connection's own task, so a slow peer never stalls the accept
//! loop) performs the optional TLS handshake and returns the [`ServerIo`] to
//! serve plus the [`ConnectionInfo`] describing the peer. No HTTP here.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use super::AcceptConfig;
use super::ConnectionInfo;

/// A bound TCP listener plus the [`AcceptConfig`] applied to what it accepts.
///
/// Owns `accept(2)`, transient-error retry, `TCP_NODELAY`, and — through
/// [`Accepted::handshake`] — TLS termination with its timeout and the capture
/// of peer facts into [`ConnectionInfo`]. TCP only.
#[derive(Debug)]
pub struct Acceptor {
    listener: TcpListener,
    #[cfg(feature = "server-tls")]
    tls: Option<TlsHandshake>,
}

/// How to terminate TLS on an accepted stream.
#[cfg(feature = "server-tls")]
#[derive(Clone)]
struct TlsHandshake {
    acceptor: tokio_rustls::TlsAcceptor,
    timeout: Duration,
}

#[cfg(feature = "server-tls")]
impl std::fmt::Debug for TlsHandshake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsHandshake")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl Acceptor {
    /// Accept from an already-bound listener — use this when socket options
    /// (`SO_REUSEPORT`, `IPV6_V6ONLY`) must be set before binding, or the
    /// listener is inherited.
    #[must_use]
    pub fn new(listener: TcpListener, config: AcceptConfig) -> Self {
        #[cfg(not(feature = "server-tls"))]
        let AcceptConfig { .. } = config; // no accept-time settings without TLS
        Self {
            listener,
            #[cfg(feature = "server-tls")]
            tls: config.tls().map(|server_config| TlsHandshake {
                acceptor: tokio_rustls::TlsAcceptor::from(std::sync::Arc::clone(server_config)),
                timeout: config.tls_handshake_timeout(),
            }),
        }
    }

    /// Bind `addr` (anything [`tokio::net::ToSocketAddrs`] accepts; the first
    /// address that binds wins) and accept from it.
    ///
    /// # Errors
    ///
    /// Returns the bind error.
    pub async fn bind(
        addr: impl tokio::net::ToSocketAddrs,
        config: AcceptConfig,
    ) -> io::Result<Self> {
        Ok(Self::new(TcpListener::bind(addr).await?, config))
    }

    /// The local address the listener is bound to.
    ///
    /// # Errors
    ///
    /// Returns the OS error from `getsockname(2)`.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Wait for the next TCP connection.
    ///
    /// Transient accept errors (`ECONNABORTED`, `EINTR`, resets) are logged
    /// at `warn` and retried — descriptor exhaustion (`EMFILE`/`ENFILE`)
    /// after a one-second pause — and only a fatal listener error is
    /// returned. `TCP_NODELAY` is set on the stream. No handshake happens
    /// here — call [`Accepted::handshake`] on the connection's own task.
    ///
    /// Cancel-safe: dropping the future loses no connection, so it may sit in
    /// a `select!` arm next to a shutdown signal.
    ///
    /// # Errors
    ///
    /// Returns a non-transient `accept(2)` error; the listener should be
    /// considered dead.
    pub async fn accept(&self) -> io::Result<Accepted> {
        loop {
            match self.listener.accept().await {
                Ok((stream, peer_addr)) => {
                    // Disable Nagle's algorithm to avoid latency from the
                    // interaction between Nagle buffering and delayed ACKs,
                    // which is especially problematic for HTTP/2's small
                    // control frames.
                    if let Err(err) = stream.set_nodelay(true) {
                        tracing::warn!("failed to set TCP_NODELAY: {err}");
                    }
                    return Ok(Accepted {
                        stream,
                        peer_addr,
                        #[cfg(feature = "server-tls")]
                        tls: self.tls.clone(),
                    });
                }
                Err(err) if is_transient_accept_error(&err) => {
                    tracing::warn!("Transient accept error (continuing): {err}");
                    if is_descriptor_exhaustion(&err) {
                        // accept(2) fails instantly until a descriptor frees;
                        // wait rather than spin.
                        tokio::time::sleep(ACCEPT_BACKOFF).await;
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }
}

/// One accepted TCP connection that has not yet completed its (optional) TLS
/// handshake. Cheap to move into the task that will serve it.
#[derive(Debug)]
pub struct Accepted {
    stream: TcpStream,
    peer_addr: SocketAddr,
    #[cfg(feature = "server-tls")]
    tls: Option<TlsHandshake>,
}

impl Accepted {
    /// Complete the connection: for plaintext, immediately; for TLS, run the
    /// handshake within the configured timeout and capture the verified client
    /// certificate chain, if any.
    ///
    /// # Errors
    ///
    /// [`HandshakeError`] if the TLS handshake fails or times out. The
    /// built-in loop logs it and drops that one connection.
    pub async fn handshake(self) -> Result<(ServerIo, ConnectionInfo), HandshakeError> {
        let info = ConnectionInfo::new().with_peer_addr(self.peer_addr);
        #[cfg(feature = "server-tls")]
        if let Some(TlsHandshake { acceptor, timeout }) = self.tls {
            let peer_addr = self.peer_addr;
            let stream = match tokio::time::timeout(timeout, acceptor.accept(self.stream)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(source)) => return Err(HandshakeError { peer_addr, source }),
                Err(_) => {
                    let source = io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("timed out after {timeout:?}"),
                    );
                    return Err(HandshakeError { peer_addr, source });
                }
            };
            // `into_owned()` detaches the chain from the session so it can
            // outlive the stream, which hyper is about to own.
            let info = match stream.get_ref().1.peer_certificates() {
                Some(chain) => {
                    info.with_peer_certs(chain.iter().map(|c| c.clone().into_owned()).collect())
                }
                None => info,
            };
            return Ok((ServerIo(Io::Tls(Box::new(stream))), info));
        }
        Ok((ServerIo(Io::Tcp(self.stream)), info))
    }
}

/// The byte stream of an accepted (and, if configured, TLS-terminated)
/// connection.
///
/// Opaque so the TLS implementation is not part of this crate's API.
#[derive(Debug)]
pub struct ServerIo(Io);

#[derive(Debug)]
enum Io {
    Tcp(TcpStream),
    #[cfg(feature = "server-tls")]
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

impl AsyncRead for ServerIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Io::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "server-tls")]
            Io::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ServerIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().0 {
            Io::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "server-tls")]
            Io::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().0 {
            Io::Tcp(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            #[cfg(feature = "server-tls")]
            Io::Tls(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match &self.0 {
            Io::Tcp(stream) => stream.is_write_vectored(),
            #[cfg(feature = "server-tls")]
            Io::Tls(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Io::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "server-tls")]
            Io::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            Io::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "server-tls")]
            Io::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// Why [`Accepted::handshake`] gave up on a connection: the TLS handshake
/// failed, or did not finish within
/// [`AcceptConfig::with_tls_handshake_timeout`] (the source is then of kind
/// [`io::ErrorKind::TimedOut`]). A plaintext handshake cannot fail.
#[derive(Debug)]
pub struct HandshakeError {
    peer_addr: SocketAddr,
    source: io::Error,
}

impl HandshakeError {
    /// The peer whose handshake failed.
    #[must_use]
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Whether the peer ran out the handshake timeout rather than failing the
    /// handshake outright. The built-in loop logs timeouts at `warn` (a
    /// slowloris signal worth surfacing) and other failures at `debug` (port
    /// scanners and plaintext clients are routine); a custom loop can apply
    /// the same split.
    #[must_use]
    pub fn is_timeout(&self) -> bool {
        self.source.kind() == io::ErrorKind::TimedOut
    }
}

/// Names the peer and the kind of failure; the underlying rustls or timeout
/// error is [`source`](std::error::Error::source), not repeated here.
impl std::fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let peer_addr = self.peer_addr;
        if self.is_timeout() {
            write!(f, "TLS handshake with {peer_addr} timed out")
        } else {
            write!(f, "TLS handshake with {peer_addr} failed")
        }
    }
}

impl std::error::Error for HandshakeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// How long [`Acceptor::accept`] waits after `EMFILE`/`ENFILE` before trying
/// again.
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

/// Whether an `accept(2)` error is transient and the listener still usable:
/// `EMFILE`/`ENFILE` (descriptor exhaustion), `ECONNABORTED`, `EINTR`,
/// `EAGAIN`, and resets.
pub(crate) fn is_transient_accept_error(err: &io::Error) -> bool {
    use std::io::ErrorKind;

    matches!(
        err.kind(),
        ErrorKind::WouldBlock
            | ErrorKind::Interrupted
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionReset
    ) || is_descriptor_exhaustion(err)
}

/// `EMFILE`/`ENFILE`; mapped to `Other`/`Uncategorized` on some platforms,
/// so matched by raw code.
fn is_descriptor_exhaustion(err: &io::Error) -> bool {
    err.raw_os_error()
        .is_some_and(|code| code == libc::EMFILE || code == libc::ENFILE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_accept_errors_are_classified() {
        use std::io::{Error, ErrorKind};
        for kind in [
            ErrorKind::WouldBlock,
            ErrorKind::Interrupted,
            ErrorKind::ConnectionAborted,
            ErrorKind::ConnectionReset,
        ] {
            assert!(is_transient_accept_error(&Error::from(kind)), "{kind:?}");
        }
        assert!(is_transient_accept_error(&Error::from_raw_os_error(
            libc::EMFILE
        )));
        assert!(is_transient_accept_error(&Error::from_raw_os_error(
            libc::ENFILE
        )));
        assert!(!is_transient_accept_error(&Error::from(
            ErrorKind::AddrInUse
        )));
        assert!(!is_transient_accept_error(&Error::from_raw_os_error(
            libc::EBADF
        )));
    }
}
