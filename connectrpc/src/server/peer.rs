//! What is known about a connection before any request is served on it.
//!
//! [`ConnectionInfo`] is the seam between whoever accepted the connection,
//! which fills it in, and the connection driver, which stamps it onto every
//! request as [`PeerAddr`] / `PeerCerts` plus the connection-scoped
//! extensions.

use std::net::SocketAddr;
#[cfg(feature = "server-tls")]
use std::sync::Arc;

/// Remote socket address of the connected peer.
///
/// Inserted into every request's extensions by the connection driver, so it
/// is present however the connection was accepted ([`Server`](super::Server),
/// `connectrpc::axum::serve`, or a custom loop) whenever
/// [`ConnectionInfo::peer_addr`] is known. Handlers read it via
/// [`RequestContext::peer_addr`](crate::RequestContext::peer_addr) (or
/// `ctx.extensions().get::<PeerAddr>()`).
///
/// Callers driving [`ConnectRpcService`](crate::ConnectRpcService) from
/// another HTTP stack can insert this same type from a tower layer so handlers
/// stay agnostic to the transport.
#[derive(Clone, Debug)]
pub struct PeerAddr(pub SocketAddr);

/// TLS client certificate chain presented by the peer (leaf first).
///
/// Captured at the TLS handshake when the [`rustls::ServerConfig`] requests
/// client authentication and the peer presents a chain rustls verifies, and
/// inserted into every request's extensions by the connection driver. Absent
/// on plaintext connections or when the client presents no certificate.
/// Handlers read it via
/// [`RequestContext::peer_certs`](crate::RequestContext::peer_certs) (or
/// `ctx.extensions().get::<PeerCerts>()`).
///
/// The `Arc` makes per-request insertion cheap: all requests on a
/// connection share one chain, so this is a refcount bump, not a copy.
#[cfg(feature = "server-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
#[derive(Clone, Debug)]
pub struct PeerCerts(pub Arc<[rustls::pki_types::CertificateDer<'static>]>);

/// What the server knows about one accepted connection before any request is
/// served on it: the remote address, over TLS the verified client certificate
/// chain, and the connection-scoped [`http::Extensions`] that every request on
/// the connection will carry.
///
/// The acceptor builds one per connection after the (optional) TLS handshake;
/// a loop on another transport builds its own with [`new`](Self::new) /
/// [`with_peer_addr`](Self::with_peer_addr), and code that owns the accept
/// step adds per-connection state through [`extensions_mut`](Self::extensions_mut)
/// before serving. The connection driver then stamps every request with those
/// extensions plus [`PeerAddr`] / `PeerCerts`, mirroring
/// [`RequestContext`](crate::RequestContext) at connection scope.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ConnectionInfo {
    peer_addr: Option<SocketAddr>,
    #[cfg(feature = "server-tls")]
    peer_certs: Option<Arc<[rustls::pki_types::CertificateDer<'static>]>>,
    extensions: http::Extensions,
}

impl ConnectionInfo {
    /// Describe a connection about which nothing is known yet.
    ///
    /// The acceptor constructs this for you; it is public so custom accept
    /// loops on transports other than TCP can describe their peers.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the peer's remote socket address.
    #[must_use]
    pub fn with_peer_addr(mut self, peer_addr: SocketAddr) -> Self {
        self.peer_addr = Some(peer_addr);
        self
    }

    /// Attach the TLS client certificate chain (leaf first) the peer
    /// presented.
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn with_peer_certs(
        mut self,
        certs: Arc<[rustls::pki_types::CertificateDer<'static>]>,
    ) -> Self {
        self.peer_certs = Some(certs);
        self
    }

    /// Remote socket address of the peer, or `None` when the transport has no
    /// meaningful address (in-memory streams, Unix sockets). Reaches handlers
    /// as [`PeerAddr`]; same shape as
    /// [`RequestContext::peer_addr`](crate::RequestContext::peer_addr).
    #[must_use]
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    /// TLS client certificate chain presented by the peer (leaf first), or
    /// `None` for plaintext connections and TLS connections without client
    /// authentication. Reaches handlers as [`PeerCerts`]; same shape as
    /// [`RequestContext::peer_certs`](crate::RequestContext::peer_certs).
    #[cfg(feature = "server-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "server-tls")))]
    #[must_use]
    pub fn peer_certs(&self) -> Option<&[rustls::pki_types::CertificateDer<'static>]> {
        self.peer_certs.as_deref()
    }

    /// Connection-scoped extensions: cloned into every request served on this
    /// connection, where handlers read them with `ctx.extensions().get::<T>()`.
    #[must_use]
    pub fn extensions(&self) -> &http::Extensions {
        &self.extensions
    }

    /// Mutable access to the connection-scoped extensions, for code that owns
    /// the accept loop and computes per-connection state before serving.
    pub fn extensions_mut(&mut self) -> &mut http::Extensions {
        &mut self.extensions
    }

    /// The extensions stamped into every request on this connection: the
    /// connection's own extensions, with [`PeerAddr`] / [`PeerCerts`] set from
    /// what the transport observed. Whatever a loop put under those
    /// two types is discarded first, so a request never reports a peer address
    /// or certificate chain the transport did not see.
    pub(crate) fn request_extensions(&self) -> http::Extensions {
        let mut ext = self.extensions.clone();
        ext.remove::<PeerAddr>();
        if let Some(peer_addr) = self.peer_addr {
            ext.insert(PeerAddr(peer_addr));
        }
        #[cfg(feature = "server-tls")]
        {
            ext.remove::<PeerCerts>();
            if let Some(certs) = &self.peer_certs {
                ext.insert(PeerCerts(Arc::clone(certs)));
            }
        }
        ext
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct ConnTag(usize);

    /// `request_extensions` sets the built-ins from the transport over the
    /// connection's own extensions: a loop can neither replace `PeerAddr` /
    /// `PeerCerts` nor supply them when the transport saw none, but its own
    /// values survive.
    #[test]
    fn builtins_are_authoritative_over_inserted_extensions() {
        let real: SocketAddr = "127.0.0.1:4242".parse().unwrap();
        let spoofed: SocketAddr = "10.0.0.1:1".parse().unwrap();
        let mut info = ConnectionInfo::new().with_peer_addr(real);
        info.extensions_mut().insert(PeerAddr(spoofed));
        info.extensions_mut().insert(ConnTag(7));
        let ext = info.request_extensions();
        assert_eq!(ext.get::<PeerAddr>().unwrap().0, real);
        assert_eq!(ext.get::<ConnTag>(), Some(&ConnTag(7)));

        // Nothing inserted: only the built-ins are present.
        let ext = ConnectionInfo::new()
            .with_peer_addr(real)
            .request_extensions();
        assert_eq!(ext.get::<PeerAddr>().unwrap().0, real);
        assert!(ext.get::<ConnTag>().is_none());
        #[cfg(feature = "server-tls")]
        assert!(ext.get::<PeerCerts>().is_none());

        // No address, no `PeerAddr` — even if something inserted one.
        let mut info = ConnectionInfo::new();
        info.extensions_mut().insert(PeerAddr(spoofed));
        assert!(info.request_extensions().get::<PeerAddr>().is_none());
    }

    /// On a connection without a verified client chain, a `PeerCerts` inserted
    /// into the extensions does not reach requests: `ctx.peer_certs()` only
    /// ever reports what the TLS layer saw.
    #[cfg(feature = "server-tls")]
    #[test]
    fn extensions_cannot_forge_peer_certs_on_plaintext() {
        let forged = PeerCerts(vec![rustls::pki_types::CertificateDer::from(vec![9u8])].into());
        let mut info = ConnectionInfo::new().with_peer_addr("127.0.0.1:1".parse().unwrap());
        info.extensions_mut().insert(forged);
        assert!(info.request_extensions().get::<PeerCerts>().is_none());
    }

    #[cfg(feature = "server-tls")]
    #[test]
    fn connection_info_carries_peer_certs() {
        let der = rustls::pki_types::CertificateDer::from(vec![1u8, 2, 3]);
        let info = ConnectionInfo::new().with_peer_certs(vec![der.clone()].into());
        assert_eq!(info.peer_certs().unwrap(), &[der.clone()][..]);
        let ext = info.request_extensions();
        assert_eq!(&ext.get::<PeerCerts>().unwrap().0[..], &[der][..]);
    }
}
