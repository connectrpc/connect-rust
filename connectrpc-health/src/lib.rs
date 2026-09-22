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
//!   `connectrpc-health = { version = "0.7", default-features = false }`;
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
//! / [`HealthService::from_arc`]; see the next section for the one extra
//! call that path needs.
//!
//! # Request limits
//!
//! A `HealthCheckRequest` is one service name, so the health routes do not
//! need the multi-megabyte request ceiling a `connectrpc` service allows by
//! default. This crate sizes `Check` and `Watch` to [`MAX_REQUEST_BYTES`]
//! (16 KiB) per request through per-route
//! [`Limits`](connectrpc::Limits) — see [`request_limits`] for the exact
//! profile — and a larger request is refused with `resource_exhausted`
//! before it reaches the [`Checker`]. The profile *replaces* the
//! service-wide limits on these two routes, whether those are looser or
//! tighter. Within that ceiling, [`StaticChecker`]'s `not_found` error for
//! an unregistered service echoes at most 128 bytes of the name, so the
//! error message is bounded by a constant rather than by the size of the
//! request. A custom [`Checker`] is responsible for bounding its own error
//! text.
//!
//! * [`install_static`] applies [`request_limits`] for you.
//! * Registering a [`HealthService`] any other way — the generated
//!   [`HealthExt::register`](HealthExt) or
//!   [`Router::add_service`](connectrpc::Router::add_service) — does not, so
//!   follow it with [`apply_request_limits`]`(router, `[`request_limits`]`())`.
//! * To tune the health routes specifically, call [`apply_request_limits`]
//!   with your own `Limits` after either path; the later call wins.
//!
//! ```no_run
//! use connectrpc::Router;
//! use connectrpc_health::{apply_request_limits, install_static, request_limits};
//!
//! let (router, health) = install_static(Router::new(), ["acme.user.v1.UserService"]);
//! // Optional: hold the health routes to 1 KiB instead of the bundled 16 KiB.
//! // Start from `request_limits()` so the rest of the profile carries over.
//! let router = apply_request_limits(
//!     router,
//!     request_limits()
//!         .with_max_request_body_size(1024)
//!         .with_max_message_size(1024),
//! );
//! # drop((router, health));
//! ```
//!
//! [`grpc.health.v1.Health`]: https://github.com/grpc/grpc-proto/blob/master/grpc/health/v1/health.proto
#![cfg_attr(docsrs, feature(doc_cfg))]

mod checker;
mod service;
mod static_checker;
mod status;

#[path = "generated/connect/mod.rs"]
mod connect;
#[path = "generated/buffa/mod.rs"]
mod proto;

pub use checker::{Checker, StatusStream};
pub use service::{
    HealthService, MAX_REQUEST_BYTES, apply_request_limits, install_static, request_limits,
};
pub use static_checker::{StaticChecker, UnknownServiceError};
pub use status::Status;

/// Generated client for calling a `grpc.health.v1.Health` server.
///
/// Gated on the `client` Cargo feature (on by default). Server-only
/// deployments turn off default features to drop the `connectrpc/client`
/// transport stack from their dependency graph.
#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
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
