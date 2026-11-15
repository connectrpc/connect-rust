//! Shared protobuf message types for `connectrpc`.
//!
//! Today that is [`google.rpc`](google::rpc), vendored from [googleapis]:
//! [`Status`](google::rpc::Status), the [`Code`](google::rpc::Code) enum,
//! and the `error_details.proto` payloads that go in error details —
//! [`BadRequest`](google::rpc::BadRequest),
//! [`RetryInfo`](google::rpc::RetryInfo),
//! [`ErrorInfo`](google::rpc::ErrorInfo),
//! [`QuotaFailure`](google::rpc::QuotaFailure),
//! [`PreconditionFailure`](google::rpc::PreconditionFailure),
//! [`ResourceInfo`](google::rpc::ResourceInfo),
//! [`RequestInfo`](google::rpc::RequestInfo), [`Help`](google::rpc::Help),
//! [`LocalizedMessage`](google::rpc::LocalizedMessage) and
//! [`DebugInfo`](google::rpc::DebugInfo).
//!
//! These are googleapis types, not well-known types, so `buffa-types` does
//! not carry them. Generating them into each service instead gives every
//! service its own `Status`: a different Rust type in each, so a `Status` or
//! `BadRequest` built by a shared library cannot be assigned to a field of a
//! message generated somewhere else. This crate is the one copy to share.
//! The `google.protobuf` types they use (`Any` in `Status.details`,
//! `Duration` in `RetryInfo`) are `buffa-types`' own.
//!
//! With the `views` feature, each message also has `*View` / `*OwnedView`
//! types: zero-copy decoders for reading a serialized message in place. Most
//! code wants the owned types.
//!
//! # Feature flags
//!
//! | Feature | Default | Adds | Turning it off saves |
//! |---|---|---|---|
//! | `json` | on | The `serde` impls with the protobuf JSON mapping, and `google::rpc::register_types` (see [JSON and `Any`](#json-and-any)) | The `serde` derives on these types, and the `json` features of `buffa` and `buffa-types` |
//! | `views` | on | The `*View` / `*OwnedView` types, and the `HasMessageView` impls that `connectrpc` service code requires | Compile time only |
//!
//! Code that only [attaches](#attaching-error-details) or
//! [reads](#reading-error-details) error details compiles with both off:
//!
//! ```toml
//! # Error details only.
//! connectrpc-types = { version = "0.9", default-features = false }
//! ```
//!
//! The crate then depends only on `buffa` and `buffa-types`. `serde` stays in
//! a build that also uses `connectrpc`, which always depends on it.
//!
//! A message of your own that has one of these types as a field
//! ([Using these types from generated code](#using-these-types-from-generated-code))
//! needs the features that match how it was generated:
//!
//! - `views`, unless the message is generated with `views=false`. Its view
//!   type has the view of each field as a field type, and `connectrpc` service
//!   code requires the view of each request and response message. Without the
//!   feature, the compiler reports ``cannot find `view` in `__buffa` ``.
//! - `json`, unless the message is generated without serde impls for a
//!   [proto-only build]. Its serde impls call the ones here. Without the
//!   feature, the compiler reports `Status: serde::Serialize` as not
//!   satisfied, which gives the trait and not the feature.
//!
//! ```toml
//! # Proto-only build with these types as fields.
//! connectrpc-types = { version = "0.9", default-features = false, features = ["views"] }
//! ```
//!
//! # Attaching error details
//!
//! [`ErrorDetail::from_message`] takes the message's bare fully-qualified
//! name, which every type here carries as
//! [`MessageName::FULL_NAME`](buffa::MessageName::FULL_NAME):
//!
//! ```
//! use buffa::MessageName;
//! use connectrpc::{ConnectError, ErrorDetail};
//! use connectrpc_types::google::rpc::{BadRequest, bad_request};
//!
//! let bad = BadRequest {
//!     field_violations: vec![bad_request::FieldViolation {
//!         field: "page_size".to_owned(),
//!         description: "must be between 1 and 100".to_owned(),
//!         ..Default::default()
//!     }],
//!     ..Default::default()
//! };
//!
//! let err = ConnectError::invalid_argument("invalid page_size")
//!     .with_detail(ErrorDetail::from_message(BadRequest::FULL_NAME, &bad));
//! # assert_eq!(err.details[0].type_url, "google.rpc.BadRequest");
//! # assert_ne!(BadRequest::FULL_NAME, BadRequest::TYPE_URL);
//! ```
//!
//! Use `FULL_NAME`, not the `TYPE_URL` constant that sits next to it. The
//! Connect protocol sends the bare name (`google.rpc.BadRequest`) in the
//! detail's `type` field, and `connectrpc` adds the `type.googleapis.com/`
//! prefix itself on the gRPC path. `TYPE_URL` compiles and works over gRPC,
//! but over Connect it sends a name peers will not recognise.
//!
//! # Reading error details
//!
//! [`ConnectError::detail`] returns the first detail of an error that decodes
//! as a given message type:
//!
//! ```
//! use connectrpc::ConnectError;
//! use connectrpc_types::google::rpc::RetryInfo;
//!
//! # use buffa::MessageName;
//! # use connectrpc::ErrorDetail;
//! # let retry = RetryInfo::default();
//! # let mut err = ConnectError::unavailable("draining")
//! #     .with_detail(ErrorDetail::from_message(RetryInfo::FULL_NAME, &retry));
//! # assert_eq!(err.detail::<RetryInfo>(), Some(retry.clone()));
//! # err.details[0].type_url = RetryInfo::TYPE_URL.to_owned(); // as decoded off gRPC
//! # assert_eq!(err.detail::<RetryInfo>(), Some(retry));
//! if let Some(retry) = err.detail::<RetryInfo>() {
//!     // back off for retry.retry_delay
//! #   let _ = retry;
//! }
//! ```
//!
//! A received detail's name is bare over Connect and
//! `type.googleapis.com/`-prefixed over gRPC and gRPC-Web, and its value may
//! be padded or unpadded base64; `detail` accepts either of each.
//! [`ErrorDetail::decode`] reads one detail and, unlike `detail`, reports a
//! detail that is named for the type but cannot be decoded.
//!
//! # Using these types from generated code
//!
//! A schema that imports `google/rpc/status.proto` (say, a batch response
//! carrying a `Status` per item) should name this crate rather than generate
//! the package again, so its fields hold these types. Map the package with
//! the buffa plugin's `extern_path` option, and keep this crate's
//! [features](#feature-flags) in line with the plugin's `views` and `json`
//! options:
//!
//! ```yaml
//! # buf.gen.yaml
//! - local: protoc-gen-buffa
//!   out: src/generated
//!   opt:
//!     - extern_path=.google.rpc=::connectrpc_types::google::rpc
//! ```
//!
//! With `protoc`, pass it through `--buffa_opt`. The value contains `:`, so
//! `--buffa_out=<opts>:<dir>` would cut it short:
//!
//! ```text
//! protoc --buffa_out=src/generated \
//!     --buffa_opt=extern_path=.google.rpc=::connectrpc_types::google::rpc ...
//! ```
//!
//! The imported `.proto` files still have to be on the compiler's include
//! path: depend on `buf.build/googleapis/googleapis` in `buf.yaml`, or vendor
//! googleapis' `google/rpc/*.proto`. `connectrpc-build` does not support
//! this mapping: its unified generation path ignores `extern_path`, so
//! schemas that embed these types need the plugin route.
//!
//! # JSON and `Any`
//!
//! With the `json` feature, every message implements `serde`'s `Serialize` /
//! `Deserialize` with the protobuf JSON mapping. To render the `details` of a
//! `Status` as expanded JSON objects rather than opaque `Any` values, register
//! these types in the process-wide buffa type registry once, at startup:
//!
//! ```
//! # #[cfg(feature = "json")] {
//! use buffa::type_registry::{TypeRegistry, set_type_registry};
//!
//! let mut registry = TypeRegistry::new();
//! buffa_types::register_wkt_types(&mut registry);
//! connectrpc_types::google::rpc::register_types(&mut registry);
//! set_type_registry(registry);
//! # }
//! ```
//!
//! Without the registry, a detail serializes as
//! `{"@type": ..., "value": <base64>}`, and an expanded detail from a peer
//! parses without an error into an `Any` whose `value` is empty.
//!
//! [googleapis]: https://github.com/googleapis/googleapis
//! [proto-only build]: https://github.com/connectrpc/connect-rust/blob/main/docs/guide.md#proto-only-no-json-builds
//! [`ErrorDetail::from_message`]: https://docs.rs/connectrpc/latest/connectrpc/error/struct.ErrorDetail.html#method.from_message
//! [`ErrorDetail::decode`]: https://docs.rs/connectrpc/latest/connectrpc/error/struct.ErrorDetail.html#method.decode
//! [`ConnectError::detail`]: https://docs.rs/connectrpc/latest/connectrpc/error/struct.ConnectError.html#method.detail

#![cfg_attr(docsrs, feature(doc_cfg))]

#[path = "generated/buffa/mod.rs"]
mod proto;

/// The generated types, mounted at their proto package path so a type is
/// where its fully-qualified name says it is: `google.rpc.Status` at
/// `connectrpc_types::google::rpc::Status`. That is also what lets an
/// `extern_path` be a single package-prefix mapping.
pub use proto::google;
