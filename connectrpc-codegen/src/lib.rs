//! connectrpc-codegen - library for generating ConnectRPC Rust bindings.
//!
//! This crate provides programmatic code generation from compiled proto
//! descriptors: buffa message types + ConnectRPC service traits, extension
//! traits, and typed clients.
//!
//! Most users will not use this crate directly. Use either:
//!
//! - **`protoc-gen-connect-rust`** - protoc/buf plugin binary (generates
//!   checked-in service stubs via `buf generate`)
//! - **`connectrpc-build`** - `build.rs` integration (generates unified
//!   message types + service stubs into `$OUT_DIR`)
//!
//! # Two generation modes
//!
//! [`codegen::generate_files`] - **unified** output. Buffa message types
//! and ConnectRPC service stubs in one file per proto, with
//! `super::`-relative type paths. Used by `connectrpc-build`.
//!
//! [`codegen::generate_services`] - **service stubs only**. Message types
//! are referenced via absolute paths configured via
//! [`codegen::CodeGenConfig::extern_paths`], so the output compiles standalone
//! against a separately-generated buffa module or crate. Used by the
//! `protoc-gen-connect-rust` plugin.
//!
//! # Library usage
//!
//! ```rust,ignore
//! use connectrpc_codegen::codegen::{generate_services, Options};
//!
//! let mut options = Options::default();
//! options.buffa.extern_paths.push((".".into(), "crate::proto".into()));
//! let files = generate_services(&descriptors, &files_to_generate, &options)?;
//! for f in files {
//!     std::fs::write(out_dir.join(&f.name), f.content)?;
//! }
//! ```

pub mod codegen;
mod comments;
pub mod plugin;

pub use codegen::CodeGenConfig;
