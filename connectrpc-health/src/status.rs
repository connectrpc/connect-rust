//! The `Status` enum returned by checkers and streamed by watchers.
//!
//! `Status` is the Rust-side type for [`Checker`](crate::Checker) impls and
//! [`StaticChecker`](crate::StaticChecker); the server maps it to
//! [`wire::ServingStatus`](crate::wire::ServingStatus) automatically. Reach
//! for the wire enum only when decoding a raw `HealthCheckResponse` off the
//! network (e.g. in a probe loop). `SERVICE_UNKNOWN` is intentionally not
//! represented. Unknown services surface as `NotFound` instead.

use crate::proto::grpc::health::v1::health_check_response::ServingStatus;

/// Health status of a single service or of the whole server.
///
/// `Status::default()` is [`Status::Unknown`] (the proto wire default), not
/// [`Status::Serving`] — [`StaticChecker::with_services`] seeds new entries
/// with `Serving` because that's almost always what registering a service
/// means in practice.
///
/// [`StaticChecker::with_services`]: crate::StaticChecker::with_services
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Status {
    /// The implementation has not (yet) determined whether the service
    /// is healthy.
    #[default]
    Unknown,
    /// The service is ready to accept requests.
    Serving,
    /// The process is up but the service is intentionally not accepting
    /// requests (e.g. a dependency is down, or the service is draining
    /// in preparation for shutdown).
    NotServing,
}

impl Status {
    /// Lowercase string representation, useful for logging.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Serving => "serving",
            Self::NotServing => "not_serving",
        }
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<Status> for ServingStatus {
    fn from(value: Status) -> Self {
        match value {
            Status::Unknown => ServingStatus::UNKNOWN,
            Status::Serving => ServingStatus::SERVING,
            Status::NotServing => ServingStatus::NOT_SERVING,
        }
    }
}

impl From<ServingStatus> for Status {
    /// Map the wire enum back to [`Status`]. Useful when decoding a raw
    /// `HealthCheckResponse` off the network — e.g. a probe loop holding
    /// the response payload before re-encoding it.
    ///
    /// `SERVICE_UNKNOWN` maps to [`Status::Unknown`]: this crate
    /// surfaces unknown services as a transport-layer `NotFound`
    /// rather than as a wire status, so the round-trip is
    /// information-preserving for the three statuses [`Status`]
    /// models. The match is exhaustive — if the generated
    /// `grpc.health.v1` proto ever grows a fifth variant, this site
    /// fails to compile and forces an explicit mapping decision.
    fn from(value: ServingStatus) -> Self {
        match value {
            ServingStatus::SERVING => Status::Serving,
            ServingStatus::NOT_SERVING => Status::NotServing,
            ServingStatus::UNKNOWN | ServingStatus::SERVICE_UNKNOWN => Status::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_unknown() {
        assert_eq!(Status::default(), Status::Unknown);
    }

    #[test]
    fn as_str_lowercase() {
        assert_eq!(Status::Unknown.as_str(), "unknown");
        assert_eq!(Status::Serving.as_str(), "serving");
        assert_eq!(Status::NotServing.as_str(), "not_serving");
    }

    #[test]
    fn maps_to_serving_status() {
        assert_eq!(ServingStatus::from(Status::Unknown), ServingStatus::UNKNOWN);
        assert_eq!(ServingStatus::from(Status::Serving), ServingStatus::SERVING);
        assert_eq!(
            ServingStatus::from(Status::NotServing),
            ServingStatus::NOT_SERVING,
        );
    }

    #[test]
    fn maps_from_serving_status() {
        assert_eq!(Status::from(ServingStatus::SERVING), Status::Serving);
        assert_eq!(Status::from(ServingStatus::NOT_SERVING), Status::NotServing);
        assert_eq!(Status::from(ServingStatus::UNKNOWN), Status::Unknown);
        // SERVICE_UNKNOWN collapses to Unknown, not NotServing — the
        // wire signal is "we couldn't tell you", not "we're definitely
        // down". The match is exhaustive at the impl site, so a future
        // proto variant would force an explicit mapping decision there.
        assert_eq!(
            Status::from(ServingStatus::SERVICE_UNKNOWN),
            Status::Unknown
        );
    }

    #[test]
    fn round_trip_through_serving_status() {
        for status in [Status::Unknown, Status::Serving, Status::NotServing] {
            let wire = ServingStatus::from(status);
            assert_eq!(
                Status::from(wire),
                status,
                "round trip failed for {status:?}"
            );
        }
    }
}
