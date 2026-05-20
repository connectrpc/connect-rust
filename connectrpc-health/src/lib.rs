//! gRPC health-checking service for `connectrpc`.
//!
//! Wire-compatible with [`grpc.health.v1.Health`], so `grpc_health_probe`,
//! `grpcurl`, Kubernetes' gRPC liveness probes, and any other client of the
//! standard gRPC health protocol just work.
//!
//! Non-empty unregistered services return `Err(ConnectError::not_found(_))`
//! from both `Check` and `Watch`; the empty service auto-subscribes on
//! `Watch` and returns `Serving` on `Check` by default — see
//! [`HealthService`]'s `# Unknown services` section for how this relates
//! to the gRPC Health spec.
//!
//! # Cargo features
//!
//! * **`client`** (on by default) — re-exports the generated
//!   `HealthClient` for in-process probes, integration tests, and
//!   sidecar tooling. Pulls in `connectrpc`'s `client` feature (the
//!   HTTP/2 transport stack). Server-only deployments drop it with
//!   `connectrpc-health = { version = "0.6", default-features = false }`;
//!   `use connectrpc_health::HealthClient` then becomes an unresolved
//!   import (the type is gone), but the dependency graph loses
//!   `connectrpc/client`.
//!
//! # Writing a custom `Checker`
//!
//! [`StaticChecker`] covers most servers. If you implement [`Checker`]
//! yourself (e.g. report `NotServing` while a database connection is
//! down), note that the **default `watch` implementation returns
//! `Unimplemented`** — fine for kubelet / `grpc_health_probe` (they only
//! call `Check`), but service meshes (Linkerd, Istio) and gRPC clients
//! with health-based load balancing call `Watch` too. Override the
//! method if your probes need it. See [`Checker::watch`] for details.
//!
//! # Quick start
//!
//! ```no_run
//! use connectrpc::Router;
//! use connectrpc_health::{install_static, Status};
//!
//! // In real code, pass the generated `*_SERVICE_NAME` constant —
//! // the literal below is a stand-in.
//! let (router, health) = install_static(Router::new(), [
//!     "acme.user.v1.UserService",
//! ]);
//!
//! // Later, when something goes wrong. `set_status` errors on unknown
//! // names; here the name was just registered above, so `.expect`
//! // documents the invariant.
//! health
//!     .set_status("acme.user.v1.UserService", Status::NotServing)
//!     .expect("registered above");
//!
//! // ...and at shutdown. `shutdown()` flips every registered service,
//! // including the empty whole-process entry seeded on construction.
//! health.shutdown();
//! # drop(router);
//! ```
//!
//! For custom logic (probing a database, propagating dependency state),
//! implement [`Checker`] directly and wrap it in [`HealthService::new`]
//! / [`HealthService::from_arc`].
//!
//! [`grpc.health.v1.Health`]: https://github.com/grpc/grpc-proto/blob/master/grpc/health/v1/health.proto

mod checker;
mod service;
mod static_checker;
mod status;

#[path = "generated/connect/mod.rs"]
mod connect;
#[path = "generated/buffa/mod.rs"]
mod proto;

pub use checker::{Checker, StatusStream};
pub use service::{HealthService, install_static};
pub use static_checker::{StaticChecker, UnknownServiceError};
pub use status::Status;

/// Generated client for calling a `grpc.health.v1.Health` server.
///
/// Gated on the `client` Cargo feature (on by default). Server-only
/// deployments turn off default features to drop the `connectrpc/client`
/// transport stack from their dependency graph.
#[cfg(feature = "client")]
pub use connect::grpc::health::v1::HealthClient;

/// Generated extension trait that adds `.register(router)` to any
/// `Arc<S> where S: Health`. Import it to register a [`HealthService`].
pub use connect::grpc::health::v1::HealthExt;

/// Fully-qualified protobuf service name: `"grpc.health.v1.Health"`.
pub use connect::grpc::health::v1::HEALTH_SERVICE_NAME;

/// Re-exports of the generated `grpc.health.v1` wire types — request and
/// response messages, `ServingStatus`, the `*_SPEC` constants. Downstream
/// crates can build probe loops without regenerating the proto.
pub mod wire {
    pub use crate::connect::grpc::health::v1::{HEALTH_CHECK_SPEC, HEALTH_WATCH_SPEC};
    pub use crate::proto::grpc::health::v1::health_check_response::ServingStatus;
    pub use crate::proto::grpc::health::v1::{HealthCheckRequest, HealthCheckResponse};
}
