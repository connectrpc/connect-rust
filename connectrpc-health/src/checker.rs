//! The [`Checker`] trait that user code implements.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use connectrpc::ConnectError;
use futures::Stream;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;

use crate::Status;

/// Reports the health of services on this server.
///
/// # Example
///
/// ```no_run
/// use connectrpc::ConnectError;
/// use connectrpc_health::{Checker, Status};
///
/// struct MyChecker;
///
/// impl Checker for MyChecker {
///     async fn check(&self, service: &str) -> Result<Status, ConnectError> {
///         match service {
///             "" | "acme.user.v1.UserService" => Ok(Status::Serving),
///             _ => Err(ConnectError::not_found(format!("unknown service {service}"))),
///         }
///     }
/// }
/// ```
pub trait Checker: Send + Sync + 'static {
    /// Check the health of `service`. An empty `service` asks for the
    /// whole-process status.
    ///
    /// # Errors
    ///
    /// Return `Err(ConnectError::not_found(_))` for any service the
    /// implementation doesn't recognize.
    fn check(&self, service: &str) -> impl Future<Output = Result<Status, ConnectError>> + Send;

    /// Subscribe to status changes for `service`. The returned
    /// [`StatusStream`] yields the current status immediately, then a
    /// new value on every subsequent change. Updates may be coalesced.
    ///
    /// # Default body returns `Unimplemented`
    ///
    /// The provided default implementation reports
    /// [`Unimplemented`](::connectrpc::ErrorCode::Unimplemented), which is
    /// fine for Check-only deployments — kubelet's `grpc:` probe and
    /// `grpc_health_probe` only call Check. **If your callers stream
    /// Watch (service meshes, gRPC clients with health-based balancing),
    /// override this** or they will see every Watch RPC fail.
    /// [`StaticChecker`](crate::StaticChecker) provides a working
    /// `watch` for the common static-status case.
    ///
    /// # Errors
    ///
    /// Overrides should return `Err(ConnectError::not_found(_))` for
    /// unknown services.
    fn watch(
        &self,
        service: &str,
    ) -> impl Future<Output = Result<StatusStream, ConnectError>> + Send {
        let _ = service;
        async {
            Err(ConnectError::unimplemented(
                "watching health state is not supported",
            ))
        }
    }
}

/// A stream of [`Status`] updates produced by [`Checker::watch`].
pub struct StatusStream {
    inner: Pin<Box<dyn Stream<Item = Status> + Send + 'static>>,
}

impl StatusStream {
    /// Wrap an arbitrary [`Stream`] of status updates.
    #[must_use]
    pub fn new(stream: impl Stream<Item = Status> + Send + 'static) -> Self {
        Self {
            inner: Box::pin(stream),
        }
    }

    /// Wrap a [`tokio::sync::watch::Receiver`]. Preferred over
    /// [`new`](Self::new) when the checker already has a `watch::Sender`.
    #[must_use]
    pub fn from_watch(receiver: watch::Receiver<Status>) -> Self {
        Self::new(WatchStream::new(receiver))
    }
}

impl Stream for StatusStream {
    type Item = Status;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl std::fmt::Debug for StatusStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatusStream").finish_non_exhaustive()
    }
}
