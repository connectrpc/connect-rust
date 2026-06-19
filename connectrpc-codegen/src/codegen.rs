//! Code generation logic for ConnectRPC Rust bindings.
//!
//! This module generates:
//! - Buffa message types (via buffa-codegen)
//! - ConnectRPC service traits and clients
//!
//! Code generation uses the `quote` crate for producing Rust code from
//! TokenStreams, which provides better syntax highlighting, type safety,
//! and maintainability compared to string-based generation.

use std::collections::HashMap;

use anyhow::Result;
use heck::ToSnakeCase;
use heck::ToUpperCamelCase;
use proc_macro2::{Ident, TokenStream};
use quote::format_ident;
use quote::quote;

use buffa_codegen::generated::descriptor::FileDescriptorProto;
use buffa_codegen::generated::descriptor::MethodDescriptorProto;
use buffa_codegen::generated::descriptor::ServiceDescriptorProto;
use buffa_codegen::generated::descriptor::SourceCodeInfo;
use buffa_codegen::generated::descriptor::method_options::IdempotencyLevel;
use buffa_codegen::idents::make_field_ident;
use buffa_codegen::idents::rust_path_to_tokens;

pub use buffa_codegen::generated::descriptor;
pub use buffa_codegen::{CodeGenConfig, GeneratedFile, GeneratedFileKind};

use crate::plugin::CodeGeneratorRequest;
use crate::plugin::CodeGeneratorResponse;
use crate::plugin::CodeGeneratorResponseFile;

/// Options for ConnectRPC code generation.
///
/// These control both the underlying buffa message generation and the
/// ConnectRPC service binding generation.
///
/// Construct via `Options::default()` then set fields on `buffa` directly
/// (the struct is `#[non_exhaustive]`, so struct-update syntax is
/// unavailable from outside this crate).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Options {
    /// The underlying buffa-codegen configuration. Set any
    /// [`CodeGenConfig`] field directly here; connectrpc passes it through
    /// verbatim except for [`CodeGenConfig::generate_views`], which is
    /// forced to `true` (service stubs require view types).
    ///
    /// [`Options::default()`] starts from buffa's defaults but enables
    /// `generate_json` (the Connect protocol's JSON codec needs it; buffa's
    /// own default is `false`).
    ///
    /// `buffa.extern_paths` is used by [`generate_services`] to bake
    /// absolute paths into service stubs (set a `(".", "crate::proto")`
    /// catch-all so every type resolves); it is ignored by
    /// [`generate_files`] (the unified `super::`-relative path).
    ///
    /// Every `extern_path` target must be buffa-generated code from
    /// buffa ≥ 0.7.0 with views enabled (and, if the crate feature-gates
    /// its generated impls, with that feature turned on): the service
    /// stubs rely on the `buffa::HasMessageView` impls and `FooOwnedView`
    /// wrappers emitted alongside each message, the same way they rely on
    /// the JSON/`Serialize` impls. `buffa-types` 0.7+ satisfies this for
    /// the well-known types. A crate generated without them fails to
    /// compile against the stubs (missing `HasMessageView` impl).
    pub buffa: CodeGenConfig,

    /// When `true`, prefix every emitted `FooClient<T>` struct and its
    /// `impl` block with `#[cfg(feature = "client")]`. Opt in when
    /// the consuming crate wants to give server-only deployments a way
    /// to drop the client transport stack from their dependency graph.
    pub gate_client_feature: bool,
}

impl Default for Options {
    fn default() -> Self {
        let mut buffa = CodeGenConfig::default();
        buffa.generate_json = true;
        Self {
            buffa,
            gate_client_feature: false,
        }
    }
}

impl Options {
    /// Clone the embedded buffa config and apply connectrpc's invariants
    /// (`generate_views = true` — service stubs reference view types).
    fn to_buffa_config(&self) -> CodeGenConfig {
        let mut config = self.buffa.clone();
        config.generate_views = true;
        config
    }
}

/// Emit one [`GeneratedFile`] per proto file in `file_to_generate` that
/// declares at least one `service`. Files with no services produce no output.
fn emit_service_files(
    proto_file: &[FileDescriptorProto],
    file_to_generate: &[String],
    resolver: &TypeResolver<'_>,
    gate_client_feature: bool,
) -> Result<Vec<GeneratedFile>> {
    let mut out = Vec::new();
    // Dedup state shared across the whole batch, not per file:
    // - output-type Encodable impls (else two files sharing an output
    //   type collide with E0119);
    // - OwnedFooView aliases keyed on (package, fqn) (else two files in
    //   the same package collide with E0428);
    // - colliding-alias detection (issue #75) needs full-batch visibility
    //   because the stitcher mounts sibling files into one module.
    let mut batch = BatchState {
        colliding_aliases: collect_alias_collisions(proto_file, file_to_generate),
        gate_client_feature,
        ..BatchState::default()
    };
    for file_name in file_to_generate {
        let file_desc = proto_file
            .iter()
            .find(|f| f.name.as_deref() == Some(file_name.as_str()));

        if let Some(file) = file_desc
            && !file.service.is_empty()
        {
            let service_tokens = generate_connect_services(file, resolver, &mut batch)?;
            let service_code = format_token_stream(&service_tokens)?;
            // Companion files are connect-rust's contribution alongside
            // buffa's per-proto outputs. The `.__connect.rs` suffix avoids
            // colliding with any of buffa's own filenames in the unified
            // path (`<stem>.rs`, `<stem>.__view.rs`, ...) per the
            // `apply_companions` contract; in the split path the plugin
            // writes to its own output directory so the suffix is just a
            // visible marker of the file's origin.
            out.push(GeneratedFile {
                name: format!(
                    "{}.__connect.rs",
                    buffa_codegen::proto_path_to_stem(file_name)
                ),
                package: file.package.clone().unwrap_or_default(),
                kind: GeneratedFileKind::Companion,
                content: service_code,
            });
        }
    }
    Ok(out)
}

/// Generate ConnectRPC service bindings + buffa message types from proto
/// descriptors.
///
/// Returns buffa's per-proto [`GeneratedFile`]s (Owned, View, Oneof,
/// ViewOneof, Ext, plus one PackageMod stitcher per package), with one
/// [`GeneratedFileKind::Companion`] file per service-declaring proto
/// (`<stem>.__connect.rs`) wired into the matching package stitcher via
/// [`buffa_codegen::apply_companions`]. Callers write every file to disk
/// and wire only the [`GeneratedFileKind::PackageMod`] entries into their
/// module tree (the stitchers `include!` the rest).
///
/// Under [`CodeGenConfig::file_per_package`] no `Companion` files are
/// emitted: the service stubs are inlined directly into buffa's single
/// `<dotted.pkg>.rs` `PackageMod` per package, mirroring how buffa
/// inlines its own ancillary content under that mode.
///
/// This is the **unified** path: service stubs reference message types via
/// `super::`-relative paths, so both must live in the same module tree.
/// [`CodeGenConfig::extern_paths`] is ignored.
///
/// # Errors
///
/// Returns an error if buffa-codegen fails (e.g. unsupported proto
/// feature) or if the generated service binding Rust does not parse
/// under `syn` (indicates a bug in this crate).
pub fn generate_files(
    proto_file: &[FileDescriptorProto],
    file_to_generate: &[String],
    options: &Options,
) -> Result<Vec<GeneratedFile>> {
    let config = options.to_buffa_config();

    let mut files = buffa_codegen::generate(proto_file, file_to_generate, &config)
        .map_err(|e| anyhow::anyhow!("buffa-codegen failed: {e}"))?;

    let resolver = TypeResolver::new(proto_file, file_to_generate, &config, false);
    let service_files = emit_service_files(
        proto_file,
        file_to_generate,
        &resolver,
        options.gate_client_feature,
    )?;

    if config.file_per_package {
        // Under `file_per_package` buffa emits one `<dotted.pkg>.rs`
        // (kind `PackageMod`) per package, inlining what the per-file
        // stitcher would otherwise `include!`. Inline the service stubs
        // into it directly so the output stays single-file-per-package —
        // a sibling `<stem>.__connect.rs` would defeat the layout's
        // purpose (BSR/`tonic`-style `lib.rs` synthesis from
        // `<dotted.package>.rs` filenames).
        inline_companions_into_package_mods(&mut files, service_files);
    } else {
        // Wire each `<stem>.__connect.rs` into the matching per-package
        // stitcher and append the companion files to the output set in one
        // pass. Every companion's package has a matching PackageMod here
        // because buffa unconditionally emits one for every package
        // containing a `file_to_generate` proto, so no companion is ever
        // orphaned.
        buffa_codegen::apply_companions(&mut files, service_files);

        // The orphaning safety above is a cross-crate invariant on buffa's
        // output shape; if a future buffa release stops emitting a
        // PackageMod for an empty package, `apply_companions` would
        // silently append the companion without any stitcher wiring it in.
        // Surface that early in debug builds rather than letting the
        // trait/client vanish at use-site.
        debug_assert!(
            files.iter().all(|f| {
                f.kind != GeneratedFileKind::Companion
                    || files.iter().any(|g| {
                        g.kind == GeneratedFileKind::PackageMod
                            && g.content.contains(&format!("include!(\"{}\")", f.name))
                    })
            }),
            "a companion service file was not wired into any package stitcher"
        );
    }

    Ok(files)
}

/// Append each companion's content directly to the matching `PackageMod`,
/// dropping the companion entries instead of `apply_companions`-ing them
/// as separate `include!`d siblings.
///
/// Used by [`generate_files`] under [`CodeGenConfig::file_per_package`],
/// where the `PackageMod` is the *only* per-package output file and a
/// sibling `<stem>.__connect.rs` would break the single-file convention
/// that BSR/`tonic`-style `lib.rs` synthesis depends on.
///
/// Companions whose package has no `PackageMod` are dropped — that does
/// not arise in [`generate_files`] (buffa unconditionally emits one per
/// `file_to_generate` package). Note this differs from `apply_companions`,
/// which appends-without-wiring (the dangling `.__connect.rs` lands on
/// disk as a debugging breadcrumb): here the orphan vanishes entirely.
/// Both paths yield a missing-symbol error at the consumer, but the
/// `debug_assert!` in [`generate_files`]'s default branch covers the
/// dangerous half (silent unwired siblings); this branch has no sibling
/// to leave dangling, so a vanished trait is the only signature.
fn inline_companions_into_package_mods(
    // Slice not Vec: this path mutates PackageMod content in place and
    // never appends — companions are consumed by the loop, not retained.
    files: &mut [GeneratedFile],
    companions: Vec<GeneratedFile>,
) {
    // Symmetric to the `debug_assert!` in `generate_files`'s default branch:
    // this branch leaves nothing on disk for an orphan, so the assertion is
    // the *only* signal if buffa's PackageMod-emission contract changes.
    debug_assert!(
        companions.iter().all(|c| files
            .iter()
            .any(|f| f.kind == GeneratedFileKind::PackageMod && f.package == c.package)),
        "a companion service file's package has no PackageMod to inline into"
    );
    for comp in companions {
        if let Some(pkg_mod) = files
            .iter_mut()
            .find(|f| f.kind == GeneratedFileKind::PackageMod && f.package == comp.package)
        {
            pkg_mod.content.push('\n');
            pkg_mod.content.push_str(&comp.content);
        }
    }
}

/// Generate **only** ConnectRPC service bindings from proto descriptors.
///
/// Returns one `<stem>.__connect.rs` `GeneratedFile` per proto file in
/// `file_to_generate` that declares at least one `service`, plus one
/// `<pkg>.mod.rs` stitcher per package. No message types.
///
/// Service files carry [`GeneratedFileKind::Companion`] for symmetry with
/// [`generate_files`], even though this path never calls
/// `apply_companions`: the split-path stitcher emitted here `include!`s
/// them directly. Build integrations filtering on kind should treat
/// `Companion` as "connect-rust service stub" in both modes.
///
/// Under [`CodeGenConfig::file_per_package`] the per-proto split is
/// collapsed: the output is exactly one `<dotted.pkg>.rs` (kind
/// [`GeneratedFileKind::PackageMod`]) per package with all service stubs
/// inlined, and no `<pkg>.mod.rs` stitcher. This matches the file layout
/// `protoc-gen-buffa` produces under the same option and the convention
/// that BSR cargo SDK generation and `tonic`-style build integrations
/// expect (one `<dotted.package>.rs` per package, module tree synthesised
/// from filenames). Route this output to its own directory — it shares
/// `protoc-gen-buffa`'s filename per package and would silently overwrite
/// in a shared one.
///
/// This is the **split** path: service stubs reference message types via
/// absolute Rust paths derived from [`CodeGenConfig::extern_paths`]. Callers must
/// set at least a `.` catch-all entry (e.g. `(".", "crate::proto")`) so
/// every type resolves; the auto-injected WKT mapping still takes priority
/// via longest-prefix-match. The generated code compiles standalone as long
/// as the extern paths point at a buffa-generated module tree.
///
/// # Errors
///
/// Errors if any method input/output type is not covered by an extern_path
/// mapping, or is absent from `proto_file` (missing import).
pub fn generate_services(
    proto_file: &[FileDescriptorProto],
    file_to_generate: &[String],
    options: &Options,
) -> Result<Vec<GeneratedFile>> {
    use std::collections::BTreeMap;

    let config = options.to_buffa_config();
    let resolver = TypeResolver::new(proto_file, file_to_generate, &config, true);
    let mut files = emit_service_files(
        proto_file,
        file_to_generate,
        &resolver,
        options.gate_client_feature,
    )?;

    if config.file_per_package {
        // Collapse the per-proto split into one `<dotted.pkg>.rs` per
        // package (kind `PackageMod`) with all service stubs inlined.
        // No stitcher — module tree wiring is the consumer's job (BSR
        // `lib.rs` synthesis, hand-written `mod.rs`, ...).
        let mut by_package: BTreeMap<String, String> = BTreeMap::new();
        for f in files {
            let entry = by_package.entry(f.package).or_insert_with(|| {
                String::from("// @generated by connectrpc-codegen. DO NOT EDIT.\n")
            });
            entry.push('\n');
            entry.push_str(&f.content);
        }
        return Ok(by_package
            .into_iter()
            .map(|(package, content)| GeneratedFile {
                name: buffa_codegen::package_to_filename(&package),
                package,
                kind: GeneratedFileKind::PackageMod,
                content,
            })
            .collect());
    }

    // Emit a per-package `<pkg>.mod.rs` stitcher for each package with at
    // least one service-declaring proto, so `protoc-gen-buffa-packaging`
    // can wire this output the same way it wires buffa's. The stitcher
    // here is trivial — just `include!("<stem>.__connect.rs")` per file;
    // there's no view/oneof ancillary tree for service stubs.
    let mut by_package: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for f in &files {
        by_package
            .entry(f.package.clone())
            .or_default()
            .push(f.name.clone());
    }
    for (package, names) in by_package {
        let mut content = String::from("// @generated by connectrpc-codegen. DO NOT EDIT.\n");
        for n in &names {
            // {:?} on the filename gives a quoted, escaped string literal.
            content.push_str(&format!("include!({n:?});\n"));
        }
        files.push(GeneratedFile {
            name: buffa_codegen::package_to_mod_filename(&package),
            package,
            kind: GeneratedFileKind::PackageMod,
            content,
        });
    }

    Ok(files)
}

/// Generate a `CodeGeneratorResponse` from a protoc `CodeGeneratorRequest`.
///
/// This is the entry point for the protoc plugin (`protoc-gen-connect-rust`).
/// It parses the comma-separated `request.parameter` into [`Options`] and
/// delegates to [`generate_services`] — service stubs only. Callers must
/// run `protoc-gen-buffa` (or equivalent) separately for message types.
///
/// # Output
///
/// Per proto with at least one `service`: a `<stem>.__connect.rs` content
/// file with the service stubs. Per package with at least one such proto:
/// a `<pkg>.mod.rs` stitcher that `include!`s the content files. The
/// stitcher filename intentionally matches `protoc-gen-buffa`'s, so run
/// this plugin into a separate output directory and use
/// `protoc-gen-buffa-packaging` to wire both trees, as shown in this
/// repo's `buf.gen.yaml` examples.
///
/// Under `file_per_package` the per-proto split is collapsed: one
/// `<dotted.pkg>.rs` per package with all service stubs inlined, no
/// per-proto content files, and no stitcher. **Drop the
/// `protoc-gen-buffa-packaging` invocations from your `buf.gen.yaml`
/// under this layout** — there are no per-file content files or
/// stitchers for it to wire, and leaving it in produces dead `mod.rs`
/// output without an error. Either let your downstream build tool
/// synthesise the module tree from `<dotted.package>.rs` filenames (BSR
/// cargo SDKs do this automatically) or hand-write the `mod.rs`. See
/// [`generate_services`].
///
/// A worked `file_per_package` `buf.gen.yaml`:
///
/// ```yaml
/// version: v2
/// plugins:
///   - local: protoc-gen-buffa
///     out: src/gen/buffa
///     opt: [file_per_package]
///   - local: protoc-gen-connect-rust
///     out: src/gen/connect
///     opt: [file_per_package, buffa_module=crate::gen::buffa]
/// ```
///
/// You then mount each tree with a hand-written `mod.rs` (or let BSR's
/// cargo SDK pipeline do it):
///
/// ```rust,ignore
/// pub mod buffa { /* one `pub mod <pkg> { include!("<pkg>.rs"); }` per package */ }
/// pub mod connect { /* same, pointing at src/gen/connect */ }
/// ```
///
/// # Recognized options
///
/// - `buffa_module=<rust_path>` — where you mounted the buffa-generated
///   module tree (e.g. `buffa_module=crate::proto`). Shorthand for
///   `extern_path=.=<rust_path>`. This is the option most local users want.
/// - `extern_path=<proto>=<rust>` — map a specific proto package prefix
///   to a Rust module path. Repeatable; longest-prefix-match wins.
///   `extern_path=.=<path>` is the catch-all (equivalent to `buffa_module`).
///   At least one catch-all mapping is required so every type resolves.
///   Every mapped path must point at buffa-generated code from
///   buffa ≥ 0.7.0 with views enabled — the stubs use the
///   `buffa::HasMessageView` impls and owned-view wrappers generated with
///   each message (`buffa-types` 0.7+ qualifies for the well-known types).
/// - `file_per_package` — emit one `<dotted.pkg>.rs` per proto package
///   instead of the per-proto split + stitcher. Set `protoc-gen-buffa`'s
///   own `file_per_package` option to the same value — the BSR/`tonic`
///   `lib.rs` synthesis assumes both plugins use the same filename
///   convention; mismatched settings produce a valid but asymmetric
///   layout you would have to wire by hand. Keep using a dedicated
///   output directory (the documented split-path setup already does
///   this) — the filename matches `protoc-gen-buffa`'s and would
///   silently overwrite in a shared one. See
///   [`CodeGenConfig::file_per_package`] for the `strategy: directory`
///   constraint.
/// - `strict_utf8_mapping` — see [`CodeGenConfig::strict_utf8_mapping`].
/// - `no_json` — disable `serde` derives on generated message types, for
///   proto-only builds. Pair it with `connectrpc`'s `default-features = false`
///   (the `json` cargo feature off) so the runtime drops its matching serde
///   bounds. Ignored in this plugin (no message types emitted); accepted for
///   compatibility with the unified path.
/// - `no_register_fn` — suppress the per-file
///   `register_types(&mut TypeRegistry)` aggregator. See
///   [`CodeGenConfig::emit_register_fn`]. Ignored in this plugin (no message
///   types emitted); accepted for compatibility with the unified path.
/// - `gate_client_feature` — prefix every emitted `FooClient<T>`
///   struct and its `impl` block with `#[cfg(feature = "client")]`.
///
/// # Client-side cfg gate
///
/// When `gate_client_feature` is set, the consumer crate must declare
/// a Cargo feature literally named `client`. Without it, the generated
/// `FooClient` items will be absent from the crate namespace.
///
/// Two consumer patterns:
///
/// 1. **Dep-forwarding** (`client = ["connectrpc/client"]`, with
///    `connectrpc = { ..., features = ["server"] }` and no `"client"`
///    in that dep's feature list): turns the gate into a real
///    server-only escape hatch. Disabling the feature drops
///    `connectrpc/client` (and its transport stack) from the
///    dependency graph entirely. This is the intended use; see
///    `connectrpc-health` for the minimal example.
///
/// 2. **Marker** (`client = []`, no forwarding): satisfies the gate
///    without slimming the dependency graph. Use only when you want
///    the cfg infrastructure in place but aren't ready to gate the
///    dep yet.
pub fn generate(request: &CodeGeneratorRequest) -> Result<CodeGeneratorResponse> {
    let mut options = Options::default();

    if let Some(ref param) = request.parameter {
        for opt in param.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            if let Some(value) = opt.strip_prefix("buffa_module=") {
                let rust = value.trim();
                if rust.is_empty() {
                    anyhow::bail!(
                        "buffa_module requires a non-empty path, \
                         e.g. buffa_module=crate::proto"
                    );
                }
                options
                    .buffa
                    .extern_paths
                    .push((".".into(), rust.to_string()));
            } else if let Some(value) = opt.strip_prefix("extern_path=") {
                // value is "<proto_path>=<rust_path>"
                let (proto, rust) = value.split_once('=').ok_or_else(|| {
                    anyhow::anyhow!(
                        "invalid extern_path format {value:?}, expected \
                         extern_path=.proto.pkg=::rust::path"
                    )
                })?;
                let proto = proto.trim();
                let rust = rust.trim();
                if proto.is_empty() || rust.is_empty() {
                    anyhow::bail!(
                        "invalid extern_path format {value:?}, expected \
                         extern_path=.proto.pkg=::rust::path (both sides non-empty)"
                    );
                }
                let mut proto = proto.to_string();
                if !proto.starts_with('.') {
                    proto.insert(0, '.');
                }
                options.buffa.extern_paths.push((proto, rust.to_string()));
            } else {
                match opt {
                    "file_per_package" => options.buffa.file_per_package = true,
                    "strict_utf8_mapping" => options.buffa.strict_utf8_mapping = true,
                    "no_json" => options.buffa.generate_json = false,
                    "no_register_fn" => options.buffa.emit_register_fn = false,
                    "gate_client_feature" => options.gate_client_feature = true,
                    _ => {
                        return Err(anyhow::anyhow!(
                            "unknown plugin option: {opt:?}. Supported: \
                             buffa_module=<rust_path>, extern_path=<proto>=<rust>, \
                             file_per_package, strict_utf8_mapping, no_json, \
                             no_register_fn, gate_client_feature"
                        ));
                    }
                }
            }
        }
    }

    let generated = generate_services(&request.proto_file, &request.file_to_generate, &options)?;

    let files: Vec<CodeGeneratorResponseFile> = generated
        .into_iter()
        .map(|g| CodeGeneratorResponseFile {
            name: Some(g.name),
            content: Some(g.content),
            ..Default::default()
        })
        .collect();

    Ok(CodeGeneratorResponse {
        supported_features: Some(feature_flags()),
        minimum_edition: Some(EDITION_2023),
        maximum_edition: Some(EDITION_2023),
        file: files,
        ..Default::default()
    })
}

/// Feature flags we support (bitmask). See
/// `google.protobuf.compiler.CodeGeneratorResponse.Feature`.
fn feature_flags() -> u64 {
    const FEATURE_PROTO3_OPTIONAL: u64 = 1;
    const FEATURE_SUPPORTS_EDITIONS: u64 = 2;
    FEATURE_PROTO3_OPTIONAL | FEATURE_SUPPORTS_EDITIONS
}

/// Edition 2023 numeric value. buffa-codegen handles proto2/proto3/edition-2023;
/// we declare 2023 as both min and max.
const EDITION_2023: i32 = 1000;

/// Format a TokenStream into a Rust source string via prettyplease.
fn format_token_stream(tokens: &TokenStream) -> Result<String> {
    let file = syn::parse2::<syn::File>(tokens.clone())
        .map_err(|e| anyhow::anyhow!("generated code failed to parse: {e}"))?;
    Ok(prettyplease::unparse(&file))
}

/// Emit `#[doc = " line"]` attributes for each line of `text`.
///
/// prettyplease renders `#[doc = "X"]` as `///X` verbatim (no space inserted);
/// to get `/// X` the string must already start with a space. This helper
/// prefixes each line with a space so the unparsed output matches hand-written
/// doc comment style.
///
/// Leaves blank lines as-is (→ `///`) so paragraph breaks render correctly.
fn doc_attrs(text: &str) -> TokenStream {
    let lines: Vec<String> = text
        .lines()
        .map(|l| {
            if l.is_empty() {
                String::new()
            } else {
                format!(" {l}")
            }
        })
        .collect();
    quote! { #(#[doc = #lines])* }
}

// ---------------------------------------------------------------------------
// Type path resolution
// ---------------------------------------------------------------------------

/// Resolves fully-qualified protobuf type names to Rust type-path tokens
/// relative to the current file's package module.
///
/// Wraps [`buffa_codegen::context::CodeGenContext`] via `for_generate()` so
/// service method input/output types resolve to the same paths buffa-codegen
/// emits for message fields — including cross-package (`super::foo::Bar`),
/// WKT extern paths (`::buffa_types::google::protobuf::Empty`), and nested
/// types (`outer::Inner`). Zero drift with buffa's own generation.
struct TypeResolver<'a> {
    ctx: buffa_codegen::context::CodeGenContext<'a>,
    /// When true, every resolved path must be absolute (`::foo` or
    /// `crate::foo`). Paths that would resolve to `super::`-relative or
    /// bare-ident forms produce an error instead. Used by
    /// [`generate_services`] to enforce that service stubs reference
    /// message types via `extern_path` only.
    require_extern: bool,
}

impl<'a> TypeResolver<'a> {
    fn new(
        proto_file: &'a [FileDescriptorProto],
        file_to_generate: &[String],
        config: &'a buffa_codegen::CodeGenConfig,
        require_extern: bool,
    ) -> Self {
        Self {
            ctx: buffa_codegen::context::CodeGenContext::for_generate(
                proto_file,
                file_to_generate,
                config,
            ),
            require_extern,
        }
    }

    /// Resolve a proto FQN (e.g. `.google.protobuf.Empty`) to a Rust type-path
    /// string relative to `current_package`.
    ///
    /// In `require_extern` mode, errors if the path is not absolute or the
    /// type is absent from the descriptor set. Otherwise falls back to the
    /// bare type name for unknown types (rustc will point at the use site).
    fn resolve_path(&self, proto_fqn: &str, current_package: &str) -> Result<String> {
        match self.ctx.rust_type_relative(proto_fqn, current_package, 0) {
            Some(path) => {
                self.check_extern_coverage(proto_fqn, &path)?;
                Ok(path)
            }
            None => self.fallback_unresolved(proto_fqn).map(str::to_string),
        }
    }

    /// In `require_extern` mode, fail if `path_prefix` isn't an absolute or
    /// crate-rooted path (i.e., the type wasn't covered by an extern_path
    /// mapping). No-op otherwise.
    fn check_extern_coverage(&self, proto_fqn: &str, path_prefix: &str) -> Result<()> {
        if self.require_extern
            && !path_prefix.starts_with("::")
            && !path_prefix.starts_with("crate::")
        {
            anyhow::bail!(
                "type {proto_fqn} is not covered by any extern_path mapping. \
                 Add extern_path=.=<your_buffa_module> (e.g. \
                 extern_path=.=crate::proto) to the plugin opts."
            );
        }
        Ok(())
    }

    /// Fallback when a FQN is absent from the descriptor set: error in
    /// `require_extern` mode, otherwise return the bare type name (rustc
    /// will point at the use site if it's wrong).
    fn fallback_unresolved<'f>(&self, proto_fqn: &'f str) -> Result<&'f str> {
        if self.require_extern {
            anyhow::bail!("type {proto_fqn} not found in descriptor set (missing proto import?)");
        }
        Ok(bare_type_name(proto_fqn))
    }

    /// Resolve a proto FQN to Rust type-path tokens.
    fn rust_type(&self, proto_fqn: &str, current_package: &str) -> Result<TokenStream> {
        let path = self.resolve_path(proto_fqn, current_package)?;
        Ok(rust_path_to_tokens(&path))
    }

    /// Resolve a proto FQN to its **view** Rust type-path tokens.
    ///
    /// Under buffa's `__buffa::` ancillary tree, view types live at
    /// `<to-package>::__buffa::view::<within-package>View`, so this uses
    /// `CodeGenContext::rust_type_relative_split` to find the package
    /// boundary and inserts the sentinel path between the two halves.
    fn rust_view_type(&self, proto_fqn: &str, current_package: &str) -> Result<TokenStream> {
        use buffa_codegen::context::SENTINEL_MOD;
        let (to_package, within) =
            match self
                .ctx
                .rust_type_relative_split(proto_fqn, current_package, 0)
            {
                Some(s) => {
                    self.check_extern_coverage(proto_fqn, &s.to_package)?;
                    (s.to_package, s.within_package)
                }
                None => (
                    String::new(),
                    self.fallback_unresolved(proto_fqn)?.to_string(),
                ),
            };
        let prefix = if to_package.is_empty() {
            format!("{SENTINEL_MOD}::view")
        } else {
            format!("{to_package}::{SENTINEL_MOD}::view")
        };
        Ok(rust_path_to_tokens(&format!("{prefix}::{within}View")))
    }
}

/// Last segment of a proto FQN, e.g. `.google.protobuf.Empty` → `"Empty"`.
/// Fallback for types absent from the resolver context.
fn bare_type_name(proto_fqn: &str) -> &str {
    proto_fqn
        .strip_prefix('.')
        .unwrap_or(proto_fqn)
        .rsplit('.')
        .next()
        .unwrap_or(proto_fqn)
}

// ---------------------------------------------------------------------------
// ConnectRPC service code generation
// ---------------------------------------------------------------------------

/// Generate ConnectRPC service bindings for a file.
/// Per-batch dedup state passed through the per-file emission loop.
#[derive(Default)]
struct BatchState {
    /// Proto FQNs of output types whose `Encodable<M>` view impls have
    /// already been emitted (global; impls are not module-scoped).
    encodable_seen: std::collections::BTreeSet<String>,
    /// `(package, proto FQN)` of input/output types whose
    /// `Owned#{Msg}View` alias has already been emitted (per package
    /// module; aliases are module-scoped).
    alias_seen: std::collections::BTreeSet<(String, String)>,
    /// `(package, alias_name)` pairs where two or more distinct FQNs would
    /// produce the same `Owned<Msg>View` alias in the same target Rust
    /// module — e.g. a service file that defines its own `MyMessage` and
    /// also references an imported `.api.v1.foo.bar.MyMessage` (issue
    /// [#75]). The alias is suppressed for every member of a colliding
    /// set; trait method signatures inline the
    /// `::buffa::view::OwnedView<…<'static>>` form for those types
    /// instead. Aliases for non-colliding types (the common case,
    /// including same-package and well-known types like
    /// `.google.protobuf.Empty`) are unaffected.
    ///
    /// [#75]: https://github.com/anthropics/connect-rust/issues/75
    colliding_aliases: std::collections::BTreeSet<(String, String)>,
    /// Mirrors [`Options::gate_client_feature`]. When `true`, prefix
    /// each emitted `FooClient<T>` struct + `impl` with
    /// `#[cfg(feature = "client")]`. Threaded here so it propagates
    /// through the per-file emission loop without changing every
    /// helper's signature.
    gate_client_feature: bool,
}

fn generate_connect_services(
    file: &FileDescriptorProto,
    resolver: &TypeResolver<'_>,
    batch: &mut BatchState,
) -> Result<TokenStream> {
    let mut tokens = TokenStream::new();

    // All types in generated code use fully qualified paths (e.g.
    // `::std::sync::Arc`, `::connectrpc::Context`) so that multiple service
    // files can be `include!`d into the same module without E0252 duplicate
    // import errors.

    // The view-family impls (`buffa::HasMessageView`) are emitted by buffa's
    // own codegen alongside each message's view and owned-view wrapper, so
    // nothing service-specific is needed here for `ServiceRequest` /
    // `StreamMessage` to be usable.
    tokens.extend(generate_owned_view_aliases(file, resolver, batch)?);
    tokens.extend(generate_encodable_view_impls(file, resolver, batch)?);

    for service in &file.service {
        tokens.extend(generate_service(file, service, resolver, batch)?);
    }

    Ok(tokens)
}

/// `Owned#{Msg}View` alias name for a proto FQN, e.g.
/// `.example.v1.Record` → `OwnedRecordView`.
fn owned_view_alias_ident(fqn: &str) -> Ident {
    format_ident!("Owned{}View", bare_type_name(fqn).to_upper_camel_case())
}

/// True iff emitting `Owned<Msg>View` for `proto_fqn` in `current_package`
/// would collide with another distinct FQN's alias in the same module
/// (issue [#75]). Cross-package types whose short name is unique in this
/// package's alias set keep their alias; only the colliding set is
/// suppressed in favour of the inlined `OwnedView<…<'static>>` form.
///
/// [#75]: https://github.com/anthropics/connect-rust/issues/75
fn alias_collides(batch: &BatchState, current_package: &str, proto_fqn: &str) -> bool {
    let alias = owned_view_alias_ident(proto_fqn).to_string();
    batch
        .colliding_aliases
        .contains(&(current_package.to_string(), alias))
}

/// Statement converting the Router-path `ServiceStream<OwnedView<…>>` into
/// `StreamMessage<Req>` items before calling the handler. Applies to every
/// input type, including ones mapped via `extern_path`: the backing
/// `buffa::HasMessageView` impl is emitted by buffa's codegen in the crate
/// that owns the type (`extern_path` targets are required to be generated
/// with buffa ≥ 0.7.0 and views enabled).
fn router_stream_items_tokens(
    resolver: &TypeResolver<'_>,
    method: &MethodDescriptorProto,
    package: &str,
) -> TokenStream {
    let input_fqn = method.input_type.as_deref().unwrap_or("");
    // Panic on resolver errors like the surrounding route-registration code
    // does. (Threading `Result` through the registration builder is a
    // follow-up.)
    let input_owned = resolver
        .rust_type(input_fqn, package)
        .expect("rust_type failed for streaming input type");
    quote! {
        let req = ::connectrpc::dispatcher::codegen::into_stream_messages::<#input_owned>(req);
    }
}

/// Doc lines describing the inbound stream item type on a client-streaming /
/// bidi trait method.
///
/// The yield-back sentence is only true when the method's input and output
/// types coincide (`StreamMessage<M>: Encodable<M>`), so it is emitted only
/// for echo-shaped methods.
fn stream_items_doc(method: &MethodDescriptorProto) -> TokenStream {
    let mut doc = quote! {
        #[doc = ""]
        #[doc = " Each `requests` item is a [`StreamMessage`](::connectrpc::StreamMessage):"]
        #[doc = " it owns its buffer, is `Send + 'static`, and exposes zero-copy"]
        #[doc = " accessor methods (`item.name()`), `.view()`, and"]
        #[doc = " `.to_owned_message()`."]
    };
    if method.input_type == method.output_type {
        doc.extend(quote! {
            #[doc = " Items can be yielded back unchanged"]
            #[doc = " (`StreamMessage<M>` implements `Encodable<M>`)."]
        });
    }
    doc
}

/// Inbound stream item type for a client-streaming / bidi RPC:
/// `StreamMessage<Req>` keyed by the owned message.
fn stream_item_arg(
    resolver: &TypeResolver<'_>,
    method: &MethodDescriptorProto,
    package: &str,
) -> Result<TokenStream> {
    let input_fqn = method.input_type.as_deref().unwrap_or("");
    let input_owned = resolver.rust_type(input_fqn, package)?;
    Ok(quote! { ::connectrpc::StreamMessage<#input_owned> })
}

/// Walk every service's method input/output FQNs across `file_to_generate`
/// and identify `(package, alias_ident)` pairs where two or more distinct
/// FQNs would produce the same `Owned<Msg>View` alias in the same target
/// Rust module. Caller stores the result in [`BatchState::colliding_aliases`].
///
/// This pre-pass is what makes the alias emission collision-aware: a
/// per-file walk can't see same-short-name FQNs from sibling files in the
/// same package, but the stitcher mounts both into one module so the
/// collision is real (issue [#75]).
///
/// [#75]: https://github.com/anthropics/connect-rust/issues/75
fn collect_alias_collisions(
    proto_file: &[FileDescriptorProto],
    file_to_generate: &[String],
) -> std::collections::BTreeSet<(String, String)> {
    use std::collections::BTreeMap;
    // (package, alias_name) -> first FQN seen; subsequent distinct FQNs
    // mark the key as colliding.
    let mut first_seen: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut colliding: std::collections::BTreeSet<(String, String)> =
        std::collections::BTreeSet::new();

    for file_name in file_to_generate {
        let Some(file) = proto_file
            .iter()
            .find(|f| f.name.as_deref() == Some(file_name.as_str()))
        else {
            continue;
        };
        let package = file.package.clone().unwrap_or_default();
        for service in &file.service {
            for m in &service.method {
                for fqn in [m.input_type.as_deref(), m.output_type.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    let alias = owned_view_alias_ident(fqn).to_string();
                    let key = (package.clone(), alias);
                    match first_seen.get(&key) {
                        Some(prev) if prev != fqn => {
                            colliding.insert(key);
                        }
                        Some(_) => {} // same FQN — fine, dedup catches it
                        None => {
                            first_seen.insert(key, fqn.to_string());
                        }
                    }
                }
            }
        }
    }
    colliding
}

/// Emit `pub type Owned#{Msg}View = OwnedView<#{Msg}View<'static>>;` for
/// every distinct RPC input/output type referenced by services in this
/// file. The alias names the owned-view form of a message in handler code
/// (e.g. an `OwnedOutView` response body or a decoded client response).
///
/// Aliases whose name would collide with another distinct type's alias
/// in the same target package (per [`BatchState::colliding_aliases`]) are
/// suppressed — users spell the inlined `OwnedView<…<'static>>` form for
/// those types instead. This is the issue [#75] fix; the non-colliding
/// common case (including well-known types like `.google.protobuf.Empty`)
/// keeps its alias.
///
/// Deduped on `(package, fqn)` across the batch so two files in the same
/// package don't both emit the alias (E0428).
///
/// [#75]: https://github.com/anthropics/connect-rust/issues/75
fn generate_owned_view_aliases(
    file: &FileDescriptorProto,
    resolver: &TypeResolver<'_>,
    batch: &mut BatchState,
) -> Result<TokenStream> {
    let package = file.package.as_deref().unwrap_or("");
    let mut out = TokenStream::new();
    for service in &file.service {
        for m in &service.method {
            for fqn in [m.input_type.as_deref(), m.output_type.as_deref()]
                .into_iter()
                .flatten()
            {
                if alias_collides(batch, package, fqn) {
                    continue;
                }
                if !batch
                    .alias_seen
                    .insert((package.to_string(), fqn.to_string()))
                {
                    continue;
                }
                let alias = owned_view_alias_ident(fqn);
                let view = resolver.rust_view_type(fqn, package)?;
                let doc = format!(
                    "Shorthand for `OwnedView<{}View<'static>>`.",
                    bare_type_name(fqn).to_upper_camel_case()
                );
                out.extend(quote! {
                    #[doc = #doc]
                    pub type #alias = ::buffa::view::OwnedView<#view<'static>>;
                });
            }
        }
    }
    Ok(out)
}

/// Emit `impl Encodable<M> for MView<'_>` and
/// `impl Encodable<M> for OwnedView<MView<'static>>` for every distinct
/// RPC output type not already in `batch.encodable_seen` (proto FQN).
///
/// These can't be runtime blankets (the `M: Message + Serialize` blanket
/// in `connectrpc::response` would conflict by coherence), so they're
/// emitted per concrete type. Orphan rules allow it because `M` (a local
/// type) appears in the trait parameters.
///
/// `batch.encodable_seen` is owned by the caller's batch loop so an
/// output type referenced from multiple input files only gets one impl
/// pair (the stitcher would otherwise hit E0119).
///
/// Skipped for output types that resolve to an absolute (`::`) extern
/// path, since those are foreign and would violate orphan rules.
fn generate_encodable_view_impls(
    file: &FileDescriptorProto,
    resolver: &TypeResolver<'_>,
    batch: &mut BatchState,
) -> Result<TokenStream> {
    let package = file.package.as_deref().unwrap_or("");
    let mut out = TokenStream::new();
    for service in &file.service {
        for m in &service.method {
            let fqn = m.output_type.as_deref().unwrap_or("");
            if !batch.encodable_seen.insert(fqn.to_string()) {
                continue;
            }
            let path = resolver.resolve_path(fqn, package)?;
            // Skip foreign types (extern_path → `::crate_name::...`): the
            // impl would be an orphan in the user's crate.
            if path.starts_with("::") {
                continue;
            }
            let owned = resolver.rust_type(fqn, package)?;
            let view = resolver.rust_view_type(fqn, package)?;
            out.extend(quote! {
                impl ::connectrpc::Encodable<#owned> for #view<'_> {
                    fn encode(&self, codec: ::connectrpc::CodecFormat)
                        -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError>
                    {
                        ::connectrpc::__codegen::encode_view_body(self, codec)
                    }
                }
                impl ::connectrpc::Encodable<#owned> for ::buffa::view::OwnedView<#view<'static>> {
                    fn encode(&self, codec: ::connectrpc::CodecFormat)
                        -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError>
                    {
                        ::connectrpc::__codegen::encode_view_body(self.reborrow(), codec)
                    }
                }
            });
        }
    }
    Ok(out)
}

/// Generate code for a single service.
/// Reject RPC method sets whose generated Rust identifiers collide.
///
/// Each proto method `Foo` produces both `foo` and `foo_with_options` on the
/// client. Two methods that normalize to the same snake_case name (e.g.
/// `GetFoo` and `get_foo`), or one whose snake form equals another's
/// `_with_options` form, would emit duplicate definitions and fail to
/// compile with an error pointing at generated code rather than the proto.
fn check_method_collisions(service_name: &str, service: &ServiceDescriptorProto) -> Result<()> {
    let mut seen: HashMap<String, String> = HashMap::new();
    for m in &service.method {
        let proto_name = m.name.as_deref().unwrap_or("");
        let snake = proto_name.to_snake_case();
        let with_opts = format!("{snake}_with_options");
        for ident in [snake.as_str(), with_opts.as_str()] {
            if let Some(prev) = seen.get(ident) {
                anyhow::bail!(
                    "service {service_name}: RPC methods {prev:?} and {proto_name:?} \
                     both generate Rust identifier `{ident}`; rename one in the proto"
                );
            }
        }
        seen.insert(snake, proto_name.to_string());
        seen.insert(with_opts, proto_name.to_string());
    }
    Ok(())
}

fn generate_service(
    file: &FileDescriptorProto,
    service: &ServiceDescriptorProto,
    resolver: &TypeResolver<'_>,
    batch: &BatchState,
) -> Result<TokenStream> {
    let package = file.package.as_deref().unwrap_or("");
    let service_name = service.name.as_deref().unwrap_or("");
    check_method_collisions(service_name, service)?;
    // Empty package is valid proto; the fully-qualified service name is just
    // `ServiceName`, not `.ServiceName` (which would break interop).
    let full_service_name = if package.is_empty() {
        service_name.to_string()
    } else {
        format!("{package}.{service_name}")
    };
    let service_upper = service_name.to_upper_camel_case();
    // `Self` is the only PascalCase Rust keyword, and cannot be a raw ident;
    // suffix it so `service Self {}` (accepted by protoc) generates a valid
    // trait. The suffixed derivatives below are already keyword-safe.
    let trait_name = if service_upper == "Self" {
        format_ident!("Self_")
    } else {
        format_ident!("{}", service_upper)
    };
    let ext_trait_name = format_ident!("{}Ext", service_upper);
    let client_name = format_ident!("{}Client", service_upper);
    let server_name = format_ident!("{}Server", service_upper);
    let service_name_const = format_ident!(
        "{}_SERVICE_NAME",
        service_name.to_snake_case().to_uppercase()
    );

    // Get service documentation and append async impl guidance
    let service_doc = get_service_comment(file, service).unwrap_or_default();
    let base_doc = if service_doc.is_empty() {
        format!("Server trait for {service_name}.")
    } else {
        service_doc
    };
    let full_doc = format!(
        "{base_doc}\n\n\
         # Implementing handlers\n\n\
         Implement methods with plain `async fn`; the returned future satisfies\n\
         the `Send` bound automatically.\n\n\
         **Unary and server-streaming requests** arrive as\n\
         [`ServiceRequest<'_, Req>`](::connectrpc::ServiceRequest): a zero-copy\n\
         view of the request plus its body, valid for the duration of the call.\n\
         Fields are read directly (`request.name` is a `&str` into the decoded\n\
         buffer) and the borrow may be held across `.await` points. Anything\n\
         that must outlive the call — `tokio::spawn`, channels, server state,\n\
         or data captured by a returned response stream — takes owned data:\n\
         call `request.to_owned_message()` (or copy the specific fields)\n\
         first.\n\n\
         **Client-streaming and bidi requests** arrive as\n\
         `ServiceStream<`[`StreamMessage<Req>`](::connectrpc::StreamMessage)`>`.\n\
         Each item owns its decoded buffer and is `Send + 'static`, so items\n\
         can be buffered or moved into spawned tasks; read fields zero-copy\n\
         through the generated accessor methods (`item.name()`) or `.view()`,\n\
         convert with `.to_owned_message()`, or yield an item back unchanged —\n\
         `StreamMessage<M>` implements `Encodable<M>`.\n\n\
         Request types resolved through `extern_path` (e.g. well-known types\n\
         from another crate) use the same wrappers; the crate that owns the\n\
         type must be generated with buffa ≥ 0.7.0 and views enabled so the\n\
         backing `HasMessageView` impl exists.\n\n\
         The `impl Encodable<Out>` return bound accepts the owned `Out`, the\n\
         generated `OutView<'_>` / `OwnedOutView`,\n\
         [`MaybeBorrowed`](::connectrpc::MaybeBorrowed), or\n\
         [`PreEncoded`](::connectrpc::PreEncoded) for handlers that encode a\n\
         non-`'static` view internally and pass the bytes across the handler\n\
         boundary. View bodies are not emitted for output types mapped via\n\
         `extern_path` (the impl would be an orphan); return owned for\n\
         WKT/extern outputs.\n\n\
         Server-streaming and bidi-streaming methods return\n\
         `ServiceStream<impl Encodable<Out> + Send + use<Self>>`. The\n\
         `use<Self>` precise-capturing clause excludes `&self`'s lifetime and\n\
         the request's lifetime (unary methods use `use<'a, Self>` and may\n\
         borrow from `&self`), so stream items must be `'static` and cannot\n\
         borrow from the request. To stream view-encoded data, encode each\n\
         item inside the stream body and yield\n\
         [`PreEncoded`](::connectrpc::PreEncoded) — see its `# Streaming\n\
         example` doc."
    );
    let service_doc_tokens = doc_attrs(&full_doc);

    // Generate trait methods
    let trait_methods: Vec<TokenStream> = service
        .method
        .iter()
        .map(|m| generate_trait_method(file, service, m, resolver, package))
        .collect::<Result<Vec<_>>>()?;

    // Generate route registrations for extension trait
    let route_registrations: Vec<TokenStream> = service
        .method
        .iter()
        .map(|m| {
            let method_name = m.name.as_deref().unwrap_or("");
            let method_snake = make_field_ident(&method_name.to_snake_case());
            // Attach the per-method `Spec` const so the dynamic `Router`
            // surfaces `RequestContext::spec()` exactly like the
            // monomorphic `FooServiceServer<T>` dispatcher does.
            let spec_const = method_spec_const_ident(service, method_name);

            let client_streaming = m.client_streaming.unwrap_or(false);
            let server_streaming = m.server_streaming.unwrap_or(false);

            let route_call = if server_streaming && !client_streaming {
                // Server streaming method. The trait method returns
                // `ServiceStream<impl Encodable<Out>>`; `Res = Out` is no
                // longer derivable from the opaque item type, so it must
                // be turbofished.
                let output_type = resolver
                    .rust_type(m.output_type.as_deref().unwrap_or(""), package)
                    .unwrap();
                let input_fqn = m.input_type.as_deref().unwrap_or("");
                let input_view = resolver.rust_view_type(input_fqn, package).unwrap();
                let input_owned = resolver.rust_type(input_fqn, package).unwrap();
                let call_handler = quote! {
                    let sreq = ::connectrpc::ServiceRequest::<#input_owned>::from_parts(req.reborrow(), req.bytes());
                    svc.#method_snake(ctx, sreq).await
                };
                quote! {
                    .route_view_server_stream::<_, _, #output_type>(
                        #service_name_const,
                        #method_name,
                        ::connectrpc::view_streaming_handler_fn({
                            let svc = ::std::sync::Arc::clone(&self);
                            move |ctx, req: ::buffa::view::OwnedView<#input_view<'static>>| {
                                let svc = ::std::sync::Arc::clone(&svc);
                                async move {
                                    // `req` (an OwnedView) is owned by this future; the
                                    // handler borrows from it until it returns the stream.
                                    #call_handler
                                }
                            }
                        }),
                    )
                }
            } else if client_streaming && !server_streaming {
                // Client streaming method
                let output_type = resolver
                    .rust_type(m.output_type.as_deref().unwrap_or(""), package)
                    .unwrap();
                let into_items = router_stream_items_tokens(resolver, m, package);
                quote! {
                    .route_view_client_stream(
                        #service_name_const,
                        #method_name,
                        ::connectrpc::view_client_streaming_handler_fn({
                            let svc = ::std::sync::Arc::clone(&self);
                            move |ctx, req, format| {
                                let svc = ::std::sync::Arc::clone(&svc);
                                async move {
                                    #into_items
                                    svc.#method_snake(ctx, req).await?.encode::<#output_type>(format)
                                }
                            }
                        }),
                    )
                }
            } else if client_streaming && server_streaming {
                // Bidi streaming method. Same turbofish need as server
                // streaming above.
                let output_type = resolver
                    .rust_type(m.output_type.as_deref().unwrap_or(""), package)
                    .unwrap();
                let into_items = router_stream_items_tokens(resolver, m, package);
                quote! {
                    .route_view_bidi_stream::<_, _, #output_type>(
                        #service_name_const,
                        #method_name,
                        ::connectrpc::view_bidi_streaming_handler_fn({
                            let svc = ::std::sync::Arc::clone(&self);
                            move |ctx, req| {
                                let svc = ::std::sync::Arc::clone(&svc);
                                async move {
                                    #into_items
                                    svc.#method_snake(ctx, req).await
                                }
                            }
                        }),
                    )
                }
            } else {
                // Unary method
                let is_idempotent = m
                    .options
                    .idempotency_level
                    .map(|level| level == IdempotencyLevel::NO_SIDE_EFFECTS)
                    .unwrap_or(false);

                let route_method = if is_idempotent {
                    quote! { route_view_idempotent }
                } else {
                    quote! { route_view }
                };
                let output_type = resolver
                    .rust_type(m.output_type.as_deref().unwrap_or(""), package)
                    .unwrap();
                // The closure parameter is annotated because the handler now
                // takes a borrowed request, so `ReqView` is no longer
                // inferable from the call alone.
                let input_fqn = m.input_type.as_deref().unwrap_or("");
                let input_view = resolver.rust_view_type(input_fqn, package).unwrap();
                let input_owned = resolver.rust_type(input_fqn, package).unwrap();
                let call_handler = quote! {
                    let sreq = ::connectrpc::ServiceRequest::<#input_owned>::from_parts(req.reborrow(), req.bytes());
                    svc.#method_snake(ctx, sreq).await?.encode::<#output_type>(format)
                };

                quote! {
                    .#route_method(
                        #service_name_const,
                        #method_name,
                        {
                            let svc = ::std::sync::Arc::clone(&self);
                            ::connectrpc::view_handler_fn(move |ctx, req: ::buffa::view::OwnedView<#input_view<'static>>, format| {
                                let svc = ::std::sync::Arc::clone(&svc);
                                async move {
                                    // `req` (an OwnedView) is owned by this future; the
                                    // handler borrows from it for the call.
                                    #call_handler
                                }
                            })
                        },
                    )
                }
            };

            quote! {
                #route_call
                .with_spec(#spec_const)
            }
        })
        .collect();

    // Generate client methods
    let client_methods: Vec<TokenStream> = service
        .method
        .iter()
        .map(|m| {
            generate_client_method(
                &service_name_const,
                &full_service_name,
                m,
                resolver,
                package,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // Generate monomorphic FooServiceServer<T> dispatcher.
    let service_server = generate_service_server(
        &full_service_name,
        &trait_name,
        &server_name,
        service,
        resolver,
        package,
    )?;

    // Example method name for client doc
    let example_method = service
        .method
        .first()
        .and_then(|m| m.name.as_deref())
        .map(|n| make_field_ident(&n.to_snake_case()).to_string())
        .unwrap_or_else(|| "method".to_string());

    // Build client doc comment with interpolated example method
    let client_name_str = client_name.to_string();
    let client_doc = format!(
        r#"Client for this service.

Generic over `T: ClientTransport`. For **gRPC** (HTTP/2), use
`Http2Connection` — it has honest `poll_ready` and composes with
`tower::balance` for multi-connection load balancing. For **Connect
over HTTP/1.1** (or unknown protocol), use `HttpClient`.

# Example (gRPC / HTTP/2)

```rust,ignore
use connectrpc::client::{{Http2Connection, ClientConfig}};
use connectrpc::Protocol;

let uri: http::Uri = "http://localhost:8080".parse()?;
let conn = Http2Connection::connect_plaintext(uri.clone()).await?.shared(1024);
let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);

let client = {client_name_str}::new(conn, config);
let response = client.{example_method}(request).await?;
```

# Example (Connect / HTTP/1.1 or ALPN)

```rust,ignore
use connectrpc::client::{{HttpClient, ClientConfig}};

let http = HttpClient::plaintext();  // cleartext http:// only
let config = ClientConfig::new("http://localhost:8080".parse()?);

let client = {client_name_str}::new(http, config);
let response = client.{example_method}(request).await?;
```

# Working with the response

Unary calls return [`UnaryResponse<OwnedView<FooView>>`](::connectrpc::client::UnaryResponse).
[`view()`](::connectrpc::client::UnaryResponse::view) borrows the response
message, so field access is zero-copy:

```rust,ignore
let resp = client.{example_method}(request).await?;
let name: &str = resp.view().name;  // borrow into the response buffer
```

If you need the owned struct (e.g. to store or pass by value), use
[`into_owned()`](::connectrpc::client::UnaryResponse::into_owned):

```rust,ignore
let owned = client.{example_method}(request).await?.into_owned();
```

[`into_view()`](::connectrpc::client::UnaryResponse::into_view) keeps the
zero-copy decoded body (an `OwnedView`) without copying; field access on it
goes through `.reborrow()`. Streaming responses yield one `OwnedView` per
received message from `.message().await` — bind `msg.reborrow()` for field
access, or convert with `.to_owned_message()`."#
    );
    let client_doc_tokens = doc_attrs(&client_doc);
    // Opt-in `#[cfg(feature = "client")]` on every client-side item.
    //
    // INVARIANT: any future emission referencing
    // `::connectrpc::client::*` (an additional `impl`, a free fn, a
    // sibling trait, …) must also be prefixed with `#client_cfg_attr`.
    // The `no_ungated_client_references` test enforces this by scanning
    // the formatted output under the opt-in path.
    let client_cfg_attr: TokenStream = if batch.gate_client_feature {
        quote! { #[cfg(feature = "client")] }
    } else {
        TokenStream::new()
    };

    // Per-method `Spec` constants. Stable, allocation-free metadata that the
    // dispatcher threads into `RequestContext::spec` and that user code can
    // reference directly (e.g. for tracing labels or routing tables).
    let spec_consts = generate_spec_consts(&full_service_name, service);

    Ok(quote! {
        // -----------------------------------------------------------------------------
        // #service_name
        // -----------------------------------------------------------------------------

        /// Full service name for this service.
        pub const #service_name_const: &str = #full_service_name;

        #(#spec_consts)*

        #service_doc_tokens
        #[allow(clippy::type_complexity)]
        pub trait #trait_name: Send + Sync + 'static {
            #(#trait_methods)*
        }

        /// Extension trait for registering a service implementation with a Router.
        ///
        /// This trait is automatically implemented for all types that implement the service trait.
        ///
        /// # Example
        ///
        /// ```rust,ignore
        /// use std::sync::Arc;
        ///
        /// let service = Arc::new(MyServiceImpl);
        /// let router = service.register(Router::new());
        /// ```
        pub trait #ext_trait_name: #trait_name {
            /// Register this service implementation with a Router.
            ///
            /// Takes ownership of the `Arc<Self>` and returns a new Router with
            /// this service's methods registered.
            fn register(self: ::std::sync::Arc<Self>, router: ::connectrpc::Router) -> ::connectrpc::Router;
        }

        impl<S: #trait_name> #ext_trait_name for S {
            fn register(self: ::std::sync::Arc<Self>, router: ::connectrpc::Router) -> ::connectrpc::Router {
                router
                    #(#route_registrations)*
            }
        }

        #service_server

        #client_doc_tokens
        #client_cfg_attr
        #[derive(Clone)]
        pub struct #client_name<T> {
            transport: T,
            config: ::connectrpc::client::ClientConfig,
        }

        #client_cfg_attr
        impl<T> #client_name<T>
        where
            T: ::connectrpc::client::ClientTransport,
            <T::ResponseBody as ::http_body::Body>::Error: ::std::fmt::Display,
        {
            /// Create a new client with the given transport and configuration.
            pub fn new(transport: T, config: ::connectrpc::client::ClientConfig) -> Self {
                Self { transport, config }
            }

            /// Get the client configuration.
            pub fn config(&self) -> &::connectrpc::client::ClientConfig {
                &self.config
            }

            /// Get a mutable reference to the client configuration.
            pub fn config_mut(&mut self) -> &mut ::connectrpc::client::ClientConfig {
                &mut self.config
            }

            #(#client_methods)*
        }
    })
}

/// Construct the identifier for a per-method `Spec` constant.
///
/// The name is derived from the service and method names, e.g.
/// `ELIZA_SERVICE_SAY_SPEC` for `ElizaService.Say`. Lives at module scope so
/// both the server dispatcher and (later) the generated client can reference
/// the same constant.
fn method_spec_const_ident(service: &ServiceDescriptorProto, method_name: &str) -> Ident {
    let service_name = service.name.as_deref().unwrap_or("");
    format_ident!(
        "{}_{}_SPEC",
        service_name.to_snake_case().to_uppercase(),
        method_name.to_snake_case().to_uppercase()
    )
}

/// Emit one `pub const … : ::connectrpc::Spec` per method.
///
/// Each constant captures the method's procedure path, stream type, and
/// idempotency level. Constructed via `Spec::server(...)` so
/// `Spec::origin == SpecOrigin::Server`; a future generated client will
/// emit a sibling constant via `Spec::client(...)`. The constants are
/// referenced by the generated `Dispatcher::lookup` impl and are also
/// stable public API for user code.
fn generate_spec_consts(
    full_service_name: &str,
    service: &ServiceDescriptorProto,
) -> Vec<TokenStream> {
    service
        .method
        .iter()
        .map(|m| {
            let method_name = m.name.as_deref().unwrap_or("");
            let spec_const = method_spec_const_ident(service, method_name);
            let procedure = format!("/{full_service_name}/{method_name}");
            let cs = m.client_streaming.unwrap_or(false);
            let ss = m.server_streaming.unwrap_or(false);
            let stream_type = match (cs, ss) {
                (true, true) => quote! { ::connectrpc::StreamType::BidiStream },
                (true, false) => quote! { ::connectrpc::StreamType::ClientStream },
                (false, true) => quote! { ::connectrpc::StreamType::ServerStream },
                (false, false) => quote! { ::connectrpc::StreamType::Unary },
            };
            let idempotency_level = match m.options.idempotency_level {
                Some(IdempotencyLevel::NO_SIDE_EFFECTS) => {
                    quote! { ::connectrpc::IdempotencyLevel::NoSideEffects }
                }
                Some(IdempotencyLevel::IDEMPOTENT) => {
                    quote! { ::connectrpc::IdempotencyLevel::Idempotent }
                }
                _ => quote! { ::connectrpc::IdempotencyLevel::Unknown },
            };
            let doc = format!(
                "Static [`Spec`](::connectrpc::Spec) for the server-side `{method_name}` RPC.\n\n\
                 The dispatcher surfaces this on\n\
                 [`RequestContext::spec`](::connectrpc::RequestContext::spec)."
            );
            let doc_tokens = doc_attrs(&doc);
            quote! {
                #doc_tokens
                pub const #spec_const: ::connectrpc::Spec =
                    ::connectrpc::Spec::server(#procedure, #stream_type)
                        .with_idempotency_level(#idempotency_level);
            }
        })
        .collect()
}

/// Generate a monomorphic `FooServiceServer<T>` struct and its `Dispatcher` impl.
///
/// This is the fast-path alternative to `FooServiceExt::register(Router)`: instead
/// of type-erasing each method behind `Arc<dyn ErasedHandler>` and looking them up
/// in a `HashMap`, this struct dispatches via a compile-time `match` on method name
/// with no trait objects or hash lookups in the hot path.
fn generate_service_server(
    full_service_name: &str,
    trait_name: &proc_macro2::Ident,
    server_name: &proc_macro2::Ident,
    service: &ServiceDescriptorProto,
    resolver: &TypeResolver<'_>,
    package: &str,
) -> Result<TokenStream> {
    // Path prefix matched by `dispatch` / `call_*`: "pkg.Service/"
    let path_prefix = format!("{full_service_name}/");

    // Per-method match arms for `lookup(path)`.
    let lookup_arms: Vec<TokenStream> = service
        .method
        .iter()
        .map(|m| {
            let method_name = m.name.as_deref().unwrap_or("");
            let client_streaming = m.client_streaming.unwrap_or(false);
            let server_streaming = m.server_streaming.unwrap_or(false);
            let is_idempotent = m
                .options
                .idempotency_level
                .map(|level| level == IdempotencyLevel::NO_SIDE_EFFECTS)
                .unwrap_or(false);
            let spec_const = method_spec_const_ident(service, method_name);

            let desc = if client_streaming && server_streaming {
                quote! { ::connectrpc::dispatcher::codegen::MethodDescriptor::bidi_streaming() }
            } else if client_streaming {
                quote! { ::connectrpc::dispatcher::codegen::MethodDescriptor::client_streaming() }
            } else if server_streaming {
                quote! { ::connectrpc::dispatcher::codegen::MethodDescriptor::server_streaming() }
            } else {
                quote! { ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(#is_idempotent) }
            };
            quote! { #method_name => Some(#desc.with_spec(#spec_const)), }
        })
        .collect();

    // Per-kind match arms for the four `call_*` methods.
    // Each `call_*` only includes arms for methods of the matching kind; other
    // paths fall through to `unimplemented_*` (the caller checked `lookup()`
    // first, so this is a defensive-only branch).
    let mut call_unary_arms: Vec<TokenStream> = Vec::new();
    let mut call_ss_arms: Vec<TokenStream> = Vec::new();
    let mut call_cs_arms: Vec<TokenStream> = Vec::new();
    let mut call_bidi_arms: Vec<TokenStream> = Vec::new();

    for m in &service.method {
        let method_name = m.name.as_deref().unwrap_or("");
        let method_snake = make_field_ident(&method_name.to_snake_case());
        let input_view = resolver.rust_view_type(m.input_type.as_deref().unwrap_or(""), package)?;
        let output_type = resolver.rust_type(m.output_type.as_deref().unwrap_or(""), package)?;
        let cs = m.client_streaming.unwrap_or(false);
        let ss = m.server_streaming.unwrap_or(false);

        // Inbound stream decoding for client-streaming / bidi: typed
        // `StreamMessage<Req>` items.
        let stream_decode = {
            let input_fqn = m.input_type.as_deref().unwrap_or("");
            let input_owned = resolver.rust_type(input_fqn, package)?;
            quote! { ::connectrpc::dispatcher::codegen::decode_message_request_stream::<#input_owned>(requests, format) }
        };

        if cs && ss {
            // Bidi streaming
            call_bidi_arms.push(quote! {
                #method_name => {
                    let svc = ::std::sync::Arc::clone(&self.inner);
                    Box::pin(async move {
                        let req_stream = #stream_decode;
                        let resp = svc.#method_snake(ctx, req_stream).await?;
                        Ok(resp.map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<#output_type, _, _>(s, format)))
                    })
                }
            });
        } else if cs {
            // Client streaming
            call_cs_arms.push(quote! {
                #method_name => {
                    let svc = ::std::sync::Arc::clone(&self.inner);
                    Box::pin(async move {
                        let req_stream = #stream_decode;
                        svc.#method_snake(ctx, req_stream).await?.encode::<#output_type>(format)
                    })
                }
            });
        } else if ss {
            // Server streaming
            let input_fqn = m.input_type.as_deref().unwrap_or("");
            let input_owned = resolver.rust_type(input_fqn, package)?;
            let call_handler = quote! {
                let req = ::connectrpc::ServiceRequest::<#input_owned>::from_parts(&req, &body);
                let resp = svc.#method_snake(ctx, req).await?;
            };
            call_ss_arms.push(quote! {
                #method_name => {
                    let svc = ::std::sync::Arc::clone(&self.inner);
                    Box::pin(async move {
                        // The normalized body is owned by this future; the handler
                        // borrows from it until it returns the response stream.
                        let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<#input_owned>(request, format)?;
                        let req: #input_view<'_> = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(&body)?;
                        #call_handler
                        Ok(resp.map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<#output_type, _, _>(s, format)))
                    })
                }
            });
        } else {
            // Unary
            let input_fqn = m.input_type.as_deref().unwrap_or("");
            let input_owned = resolver.rust_type(input_fqn, package)?;
            let call_handler = quote! {
                let req = ::connectrpc::ServiceRequest::<#input_owned>::from_parts(&req, &body);
                svc.#method_snake(ctx, req).await?.encode::<#output_type>(format)
            };
            call_unary_arms.push(quote! {
                #method_name => {
                    let svc = ::std::sync::Arc::clone(&self.inner);
                    Box::pin(async move {
                        // Generated handlers are view-based, so the owned-message
                        // cache an interceptor may have populated cannot be reused.
                        // `encoded()` returns the (post-replacement) wire bytes —
                        // a cheap `Bytes` clone for the common no-replacement case.
                        // The normalized body is owned by this future; the handler
                        // borrows from it for the duration of the call.
                        let body = ::connectrpc::dispatcher::codegen::request_proto_bytes::<#input_owned>(request.encoded()?, format)?;
                        let req: #input_view<'_> = ::connectrpc::dispatcher::codegen::decode_borrowed_request_view(&body)?;
                        #call_handler
                    })
                }
            });
        }
    }

    let server_doc = format!(
        "Monomorphic dispatcher for `{trait_name}`.\n\n\
         Unlike `.register(Router)` which type-erases each method into an \
         `Arc<dyn ErasedHandler>` stored in a `HashMap`, this struct dispatches \
         via a compile-time `match` on method name: no vtable, no hash lookup.\n\n\
         # Example\n\n\
         ```rust,ignore\n\
         use connectrpc::ConnectRpcService;\n\n\
         let server = {server_name}::new(MyImpl);\n\
         let service = ConnectRpcService::new(server);\n\
         // hand `service` to axum/hyper as a fallback_service\n\
         ```"
    );
    let server_doc_tokens = doc_attrs(&server_doc);

    Ok(quote! {
        #server_doc_tokens
        pub struct #server_name<T> {
            inner: ::std::sync::Arc<T>,
        }

        impl<T: #trait_name> #server_name<T> {
            /// Wrap a service implementation in a monomorphic dispatcher.
            pub fn new(service: T) -> Self {
                Self { inner: ::std::sync::Arc::new(service) }
            }

            /// Wrap an already-`Arc`'d service implementation.
            pub fn from_arc(inner: ::std::sync::Arc<T>) -> Self {
                Self { inner }
            }
        }

        impl<T> Clone for #server_name<T> {
            fn clone(&self) -> Self {
                Self { inner: ::std::sync::Arc::clone(&self.inner) }
            }
        }

        impl<T: #trait_name> ::connectrpc::Dispatcher for #server_name<T> {
            #[inline]
            fn lookup(&self, path: &str) -> Option<::connectrpc::dispatcher::codegen::MethodDescriptor> {
                let method = path.strip_prefix(#path_prefix)?;
                match method {
                    #(#lookup_arms)*
                    _ => None,
                }
            }

            fn call_unary(
                &self,
                path: &str,
                ctx: ::connectrpc::RequestContext,
                request: ::connectrpc::Payload,
                format: ::connectrpc::CodecFormat,
            ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
                let Some(method) = path.strip_prefix(#path_prefix) else {
                    return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
                };
                // Suppress unused warnings when this service has no unary methods.
                let _ = (&ctx, &request, &format);
                match method {
                    #(#call_unary_arms)*
                    _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
                }
            }

            fn call_server_streaming(
                &self,
                path: &str,
                ctx: ::connectrpc::RequestContext,
                request: ::buffa::bytes::Bytes,
                format: ::connectrpc::CodecFormat,
            ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
                let Some(method) = path.strip_prefix(#path_prefix) else {
                    return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
                };
                let _ = (&ctx, &request, &format);
                match method {
                    #(#call_ss_arms)*
                    _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
                }
            }

            fn call_client_streaming(
                &self,
                path: &str,
                ctx: ::connectrpc::RequestContext,
                requests: ::connectrpc::dispatcher::codegen::RequestStream,
                format: ::connectrpc::CodecFormat,
            ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
                let Some(method) = path.strip_prefix(#path_prefix) else {
                    return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
                };
                let _ = (&ctx, &requests, &format);
                match method {
                    #(#call_cs_arms)*
                    _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
                }
            }

            fn call_bidi_streaming(
                &self,
                path: &str,
                ctx: ::connectrpc::RequestContext,
                requests: ::connectrpc::dispatcher::codegen::RequestStream,
                format: ::connectrpc::CodecFormat,
            ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
                let Some(method) = path.strip_prefix(#path_prefix) else {
                    return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
                };
                let _ = (&ctx, &requests, &format);
                match method {
                    #(#call_bidi_arms)*
                    _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
                }
            }
        }
    })
}

/// Generate documentation comment tokens.
fn generate_doc_comment(doc: &str, default: &str) -> TokenStream {
    let comment = if doc.is_empty() { default } else { doc };
    doc_attrs(comment)
}

/// Generate a trait method for a service.
fn generate_trait_method(
    file: &FileDescriptorProto,
    service: &ServiceDescriptorProto,
    method: &MethodDescriptorProto,
    resolver: &TypeResolver<'_>,
    package: &str,
) -> Result<TokenStream> {
    let method_name = method.name.as_deref().unwrap_or("");
    let method_snake = make_field_ident(&method_name.to_snake_case());
    let output_type = resolver.rust_type(method.output_type.as_deref().unwrap_or(""), package)?;

    // Get method documentation
    let method_doc = get_method_comment(file, service, method).unwrap_or_default();
    let method_doc_tokens =
        generate_doc_comment(&method_doc, &format!("Handle the {method_name} RPC."));

    // Check for streaming
    let client_streaming = method.client_streaming.unwrap_or(false);
    let server_streaming = method.server_streaming.unwrap_or(false);

    let borrow_doc = quote! {
        #[doc = ""]
        #[doc = " `'a` lets the response body borrow from `&self` (e.g. server-resident state)."]
    };

    if server_streaming && !client_streaming {
        // Server streaming method. `impl Encodable<...>` lets the handler
        // yield `Res`, `PreEncoded`, or `MaybeBorrowed` items — same
        // flexibility as the unary `impl Encodable<...>` body bound.
        // `use<Self>` opts out of capturing `&self`'s lifetime (RPITITs in
        // trait methods otherwise capture it by default), since stream
        // items have to be `'static`. Without it, the generated route
        // registration's `Arc::clone` closures fail E0597. The borrowed
        // `ServiceRequest` lifetime is likewise excluded, so the returned
        // stream cannot borrow from the request — anything the stream needs
        // must be copied or converted to owned before returning it.
        let input_fqn = method.input_type.as_deref().unwrap_or("");
        let input_owned = resolver.rust_type(input_fqn, package)?;
        let request_param = quote! { ::connectrpc::ServiceRequest<'_, #input_owned> };
        let request_doc = quote! {
            #[doc = ""]
            #[doc = " `request` is borrowed from the request body and is valid for the"]
            #[doc = " duration of the call (until the response stream is returned);"]
            #[doc = " message fields are read directly on it (zero-copy). Data the"]
            #[doc = " returned stream needs must be copied out or converted via"]
            #[doc = " `.to_owned_message()`."]
        };
        Ok(quote! {
            #method_doc_tokens
            #request_doc
            fn #method_snake(
                &self,
                ctx: ::connectrpc::RequestContext,
                request: #request_param,
            ) -> impl ::std::future::Future<Output = ::connectrpc::ServiceResult<::connectrpc::ServiceStream<impl ::connectrpc::Encodable<#output_type> + Send + use<Self>>>> + Send;
        })
    } else if client_streaming && !server_streaming {
        // Client streaming method. Inbound items are `StreamMessage<Req>` —
        // each received message owns its decoded buffer (zero-copy reads via
        // `.view()`, conversion via `.to_owned_message()`, and — for
        // echo-shaped methods — items can be forwarded as-is since
        // `StreamMessage<M>: Encodable<M>`).
        let stream_item_arg = stream_item_arg(resolver, method, package)?;
        let items_doc = stream_items_doc(method);
        Ok(quote! {
            #method_doc_tokens
            #borrow_doc
            #items_doc
            fn #method_snake<'a>(
                &'a self,
                ctx: ::connectrpc::RequestContext,
                requests: ::connectrpc::ServiceStream<#stream_item_arg>,
            ) -> impl ::std::future::Future<Output = ::connectrpc::ServiceResult<impl ::connectrpc::Encodable<#output_type> + Send + use<'a, Self>>> + Send;
        })
    } else if client_streaming && server_streaming {
        // Bidi streaming method. Same `impl Encodable<...>` item type and
        // `use<Self>` capture clause as server streaming above; inbound items
        // are `StreamMessage<Req>` as for client streaming.
        let stream_item_arg = stream_item_arg(resolver, method, package)?;
        let items_doc = stream_items_doc(method);
        Ok(quote! {
            #method_doc_tokens
            #items_doc
            fn #method_snake(
                &self,
                ctx: ::connectrpc::RequestContext,
                requests: ::connectrpc::ServiceStream<#stream_item_arg>,
            ) -> impl ::std::future::Future<Output = ::connectrpc::ServiceResult<::connectrpc::ServiceStream<impl ::connectrpc::Encodable<#output_type> + Send + use<Self>>>> + Send;
        })
    } else {
        // Unary method. The request is *borrowed*: the generated dispatcher
        // owns the request body for the duration of the call and hands the
        // handler a `ServiceRequest<'_, Req>` (zero-copy view + raw body)
        // borrowed from it, so field access (`request.field`) is plain
        // borrow-checked access with no synthetic `'static` involved. The
        // handler future captures that borrow (RPITIT captures all in-scope
        // lifetimes), which is fine because the dispatcher awaits it while
        // still owning the body. The response's `use<'a, Self>` deliberately
        // excludes the request lifetime: the response must not borrow from
        // the request.
        let input_fqn = method.input_type.as_deref().unwrap_or("");
        let input_owned = resolver.rust_type(input_fqn, package)?;
        let request_param = quote! { ::connectrpc::ServiceRequest<'_, #input_owned> };
        let request_doc = quote! {
            #[doc = ""]
            #[doc = " `request` is borrowed from the request body and is valid for the"]
            #[doc = " duration of the call; message fields are read directly on it"]
            #[doc = " (zero-copy). The response cannot borrow from `request` — use"]
            #[doc = " `.to_owned_message()` (or copy the specific fields) for anything"]
            #[doc = " returned, stored, or moved into `tokio::spawn`."]
        };
        Ok(quote! {
            #method_doc_tokens
            #borrow_doc
            #request_doc
            fn #method_snake<'a>(
                &'a self,
                ctx: ::connectrpc::RequestContext,
                request: #request_param,
            ) -> impl ::std::future::Future<Output = ::connectrpc::ServiceResult<impl ::connectrpc::Encodable<#output_type> + Send + use<'a, Self>>> + Send;
        })
    }
}

/// Generate client method(s) for a service RPC.
///
/// Emits two methods per RPC:
///   - `<method_snake>(&self, ...)` — no-options convenience, delegates to `_with_options`
///   - `<method_snake>_with_options(&self, ..., options: CallOptions)` — explicit options
///
/// This gives callers an ergonomic default while still surfacing per-call
/// control. The library's `effective_options()` merges options over
/// ClientConfig defaults, so the no-options variant still picks up any
/// client-wide defaults the user configured.
fn generate_client_method(
    service_name_const: &Ident,
    full_service_name: &str,
    method: &MethodDescriptorProto,
    resolver: &TypeResolver<'_>,
    package: &str,
) -> Result<TokenStream> {
    let method_name = method.name.as_deref().unwrap_or("");
    let method_snake = make_field_ident(&method_name.to_snake_case());
    let method_with_opts = format_ident!("{}_with_options", method_name.to_snake_case());
    let input_type = resolver.rust_type(method.input_type.as_deref().unwrap_or(""), package)?;
    let output_view_type =
        resolver.rust_view_type(method.output_type.as_deref().unwrap_or(""), package)?;

    let client_streaming = method.client_streaming.unwrap_or(false);
    let server_streaming = method.server_streaming.unwrap_or(false);

    let doc = format!(
        " Call the {method_name} RPC. Sends a request to /{full_service_name}/{method_name}."
    );
    let doc_opts = format!(
        " Call the {method_name} RPC with explicit per-call options. \
         Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults."
    );

    // Return type is protocol-specific. Compute once.
    let ret_ty: TokenStream;
    let call_body: TokenStream;
    let short_args: TokenStream; // args to the no-opts convenience method
    let opts_args: TokenStream; // args to the _with_options method
    let short_delegate_args: TokenStream; // how short delegates to opts

    if client_streaming && !server_streaming {
        // Client-stream
        ret_ty = quote! {
            Result<
                ::connectrpc::client::UnaryResponse<::buffa::view::OwnedView<#output_view_type<'static>>>,
                ::connectrpc::ConnectError,
            >
        };
        call_body = quote! {
            ::connectrpc::client::call_client_stream(
                &self.transport, &self.config,
                #service_name_const, #method_name,
                requests, options,
            ).await
        };
        short_args = quote! { requests: impl IntoIterator<Item = #input_type> };
        opts_args = quote! { requests: impl IntoIterator<Item = #input_type>, options: ::connectrpc::client::CallOptions };
        short_delegate_args = quote! { requests, ::connectrpc::client::CallOptions::default() };
    } else if client_streaming && server_streaming {
        // Bidi
        ret_ty = quote! {
            Result<
                ::connectrpc::client::BidiStream<
                    T::ResponseBody, #input_type, #output_view_type<'static>
                >,
                ::connectrpc::ConnectError,
            >
        };
        call_body = quote! {
            ::connectrpc::client::call_bidi_stream(
                &self.transport, &self.config,
                #service_name_const, #method_name, options,
            ).await
        };
        short_args = quote! {};
        opts_args = quote! { options: ::connectrpc::client::CallOptions };
        short_delegate_args = quote! { ::connectrpc::client::CallOptions::default() };
    } else if server_streaming {
        // Server-stream
        ret_ty = quote! {
            Result<
                ::connectrpc::client::ServerStream<T::ResponseBody, #output_view_type<'static>>,
                ::connectrpc::ConnectError,
            >
        };
        call_body = quote! {
            ::connectrpc::client::call_server_stream(
                &self.transport, &self.config,
                #service_name_const, #method_name,
                request, options,
            ).await
        };
        short_args = quote! { request: #input_type };
        opts_args = quote! { request: #input_type, options: ::connectrpc::client::CallOptions };
        short_delegate_args = quote! { request, ::connectrpc::client::CallOptions::default() };
    } else {
        // Unary
        ret_ty = quote! {
            Result<
                ::connectrpc::client::UnaryResponse<::buffa::view::OwnedView<#output_view_type<'static>>>,
                ::connectrpc::ConnectError,
            >
        };
        call_body = quote! {
            ::connectrpc::client::call_unary(
                &self.transport, &self.config,
                #service_name_const, #method_name,
                request, options,
            ).await
        };
        short_args = quote! { request: #input_type };
        opts_args = quote! { request: #input_type, options: ::connectrpc::client::CallOptions };
        short_delegate_args = quote! { request, ::connectrpc::client::CallOptions::default() };
    }

    Ok(quote! {
        #[doc = #doc]
        pub async fn #method_snake(&self, #short_args) -> #ret_ty {
            self.#method_with_opts(#short_delegate_args).await
        }

        #[doc = #doc_opts]
        pub async fn #method_with_opts(&self, #opts_args) -> #ret_ty {
            #call_body
        }
    })
}

/// Get the documentation comment for a service.
fn get_service_comment(
    file: &FileDescriptorProto,
    service: &ServiceDescriptorProto,
) -> Option<String> {
    // MessageField derefs to default when unset; default has empty location vec
    let source_info: &SourceCodeInfo = &file.source_code_info;

    // Find service index
    let service_index = file.service.iter().position(|s| s.name == service.name)?;

    // Path for service: [6, service_index]
    // 6 = service field number in FileDescriptorProto
    let target_path = vec![6, service_index as i32];

    find_comment(source_info, &target_path)
}

/// Get the documentation comment for a method.
fn get_method_comment(
    file: &FileDescriptorProto,
    service: &ServiceDescriptorProto,
    method: &MethodDescriptorProto,
) -> Option<String> {
    let source_info: &SourceCodeInfo = &file.source_code_info;

    // Find service and method indices, matching on the parent service name
    // to avoid ambiguity when multiple services have methods with the same name.
    let (service_index, method_index) = file.service.iter().enumerate().find_map(|(si, s)| {
        if s.name != service.name {
            return None;
        }
        s.method
            .iter()
            .position(|m| m.name == method.name)
            .map(|mi| (si, mi))
    })?;

    // Path for method: [6, service_index, 2, method_index]
    // 6 = service field number in FileDescriptorProto
    // 2 = method field number in ServiceDescriptorProto
    let target_path = vec![6, service_index as i32, 2, method_index as i32];

    find_comment(source_info, &target_path)
}

/// Find a comment in source code info for the given path.
fn find_comment(source_info: &SourceCodeInfo, target_path: &[i32]) -> Option<String> {
    for location in &source_info.location {
        if location.path == target_path {
            let comment = location
                .leading_comments
                .as_ref()
                .or(location.trailing_comments.as_ref())?;

            // Trim each line; blank lines are dropped (protoc's convention
            // uses a leading space we don't need here — `doc_attrs` adds
            // its own uniform leading space for prettyplease rendering).
            let cleaned: String = comment
                .lines()
                .map(|line| line.trim())
                .filter(|line| !line.is_empty())
                .collect::<Vec<_>>()
                .join("\n");

            if !cleaned.is_empty() {
                return Some(cleaned);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffa_codegen::generated::descriptor::DescriptorProto;
    use quote::ToTokens;

    #[test]
    fn doc_attrs_prefixes_space_for_prettyplease() {
        // prettyplease emits `#[doc = "X"]` as `///X` verbatim. We prefix
        // each non-blank line with a space so the output is `/// X`.
        let ts = quote! {
            #[allow(dead_code)]
            mod m {}
        };
        let doc = doc_attrs("Hello.\n\nSecond paragraph.");
        let combined = quote! { #doc #ts };
        let file = syn::parse2::<syn::File>(combined).unwrap();
        let out = prettyplease::unparse(&file);
        // Each non-blank line should have a space after ///.
        assert!(out.contains("/// Hello."), "got: {out}");
        assert!(out.contains("/// Second paragraph."), "got: {out}");
        // Blank line becomes bare /// (paragraph break).
        assert!(out.contains("///\n"), "got: {out}");
        // Should NOT contain ///H (no space) or ///  H (double space).
        assert!(!out.contains("///Hello"), "got: {out}");
        assert!(!out.contains("///  Hello"), "got: {out}");
    }

    /// Build a minimal proto file with one message type and one service method.
    /// The service method's input/output types are fully-qualified proto names
    /// (e.g. `.example.v1.PingReq` or `.google.protobuf.Empty`) so the resolver
    /// can look them up.
    fn minimal_file(
        package: Option<&str>,
        input_type: &str,
        output_type: &str,
        local_messages: &[&str],
    ) -> FileDescriptorProto {
        minimal_file_with_method(package, "Ping", input_type, output_type, local_messages)
    }

    /// Like [`minimal_file`] but with a custom RPC method name, for testing
    /// keyword collisions and other name-derived behaviour.
    fn minimal_file_with_method(
        package: Option<&str>,
        method_name: &str,
        input_type: &str,
        output_type: &str,
        local_messages: &[&str],
    ) -> FileDescriptorProto {
        let method = MethodDescriptorProto {
            name: Some(method_name.into()),
            input_type: Some(input_type.into()),
            output_type: Some(output_type.into()),
            ..Default::default()
        };
        let service = ServiceDescriptorProto {
            name: Some("PingService".into()),
            method: vec![method],
            ..Default::default()
        };
        FileDescriptorProto {
            name: Some("ping.proto".into()),
            package: package.map(|p| p.into()),
            service: vec![service],
            message_type: local_messages
                .iter()
                .map(|name| DescriptorProto {
                    name: Some((*name).into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Build a minimal proto file with one service holding the given method
    /// names, all typed `Empty` -> `Empty`. Used for collision tests where
    /// the method *names* are what's under test.
    fn minimal_file_with_methods(package: &str, method_names: &[&str]) -> FileDescriptorProto {
        let methods = method_names
            .iter()
            .map(|n| MethodDescriptorProto {
                name: Some((*n).into()),
                input_type: Some(format!(".{package}.Empty")),
                output_type: Some(format!(".{package}.Empty")),
                ..Default::default()
            })
            .collect();
        let service = ServiceDescriptorProto {
            name: Some("PingService".into()),
            method: methods,
            ..Default::default()
        };
        FileDescriptorProto {
            name: Some("ping.proto".into()),
            package: Some(package.into()),
            service: vec![service],
            message_type: vec![DescriptorProto {
                name: Some("Empty".into()),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Generate service code for `files[target_idx]`. All files are visible
    /// to the resolver (as transitive deps via `--include_imports`), but
    /// only the target is in `file_to_generate` — mirroring real protoc use.
    ///
    /// `extern_paths` is wired into `CodeGenConfig.extern_paths` (which
    /// feeds the resolver's type_map via `effective_extern_paths`).
    /// `require_extern` selects unified (`false`, super::-relative) vs
    /// split (`true`, absolute-only) mode.
    fn gen_service(
        files: &[FileDescriptorProto],
        target_idx: usize,
        extern_paths: &[(String, String)],
        require_extern: bool,
    ) -> Result<String> {
        let mut config = buffa_codegen::CodeGenConfig::default();
        config.extern_paths = extern_paths.to_vec();
        let target_name = files[target_idx]
            .name
            .clone()
            .into_iter()
            .collect::<Vec<_>>();
        let resolver = TypeResolver::new(files, &target_name, &config, require_extern);
        let file = &files[target_idx];
        let service = &file.service[0];
        let batch = BatchState {
            colliding_aliases: collect_alias_collisions(files, &target_name),
            ..BatchState::default()
        };
        Ok(generate_service(file, service, &resolver, &batch)?.to_string())
    }

    /// Assert that `formatted` (a Rust source string) contains no `use`
    /// items at the file root. Parses with `syn` rather than string-matching
    /// so doc comments, string literals, and indented `use` statements in
    /// nested modules cannot trigger false positives.
    fn assert_no_top_level_use(formatted: &str, label: &str) {
        let parsed: syn::File = syn::parse_str(formatted).expect("formatted code parses");
        let offenders: Vec<String> = parsed
            .items
            .iter()
            .filter_map(|item| match item {
                syn::Item::Use(u) => Some(quote!(#u).to_string()),
                _ => None,
            })
            .collect();
        assert!(
            offenders.is_empty(),
            "{label} contains top-level use statement(s): {offenders:?}\nFull source:\n{formatted}"
        );
    }

    fn gen_file(
        files: &[FileDescriptorProto],
        target_idx: usize,
        extern_paths: &[(String, String)],
        require_extern: bool,
    ) -> Result<String> {
        let mut config = buffa_codegen::CodeGenConfig::default();
        config.extern_paths = extern_paths.to_vec();
        let target_name = files[target_idx]
            .name
            .clone()
            .into_iter()
            .collect::<Vec<_>>();
        let resolver = TypeResolver::new(files, &target_name, &config, require_extern);
        let mut batch = BatchState {
            colliding_aliases: collect_alias_collisions(files, &target_name),
            ..BatchState::default()
        };
        Ok(generate_connect_services(&files[target_idx], &resolver, &mut batch)?.to_string())
    }

    #[test]
    fn unary_response_body_captures_self_lifetime() {
        let file = minimal_file(
            Some("example.v1"),
            ".example.v1.PingReq",
            ".example.v1.PingResp",
            &["PingReq", "PingResp"],
        );
        let code = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap();
        assert!(code.contains("< 'a >"), "trait method missing 'a: {code}");
        assert!(code.contains("& 'a self"), "missing &'a self: {code}");
        assert!(
            code.contains("use < 'a , Self >"),
            "missing use<'a, Self> capture: {code}"
        );
        assert!(
            !code.contains("'static + use"),
            "'static bound on body should be dropped: {code}"
        );
    }

    #[test]
    fn owned_view_aliases_emitted_for_input_and_output() {
        let file = minimal_file(
            Some("example.v1"),
            ".example.v1.PingReq",
            ".example.v1.PingResp",
            &["PingReq", "PingResp"],
        );
        let code = gen_file(std::slice::from_ref(&file), 0, &[], false).unwrap();
        assert!(
            code.contains("pub type OwnedPingReqView = :: buffa :: view :: OwnedView"),
            "missing OwnedPingReqView alias: {code}"
        );
        assert!(
            code.contains("pub type OwnedPingRespView = :: buffa :: view :: OwnedView"),
            "missing OwnedPingRespView alias: {code}"
        );
        // Unary trait methods take a borrowed ServiceRequest; the alias is
        // still emitted (the natural spelling for pass-through response
        // bodies, e.g. `MaybeBorrowed<Ping, OwnedPingView>` holding a
        // `req.to_owned_view()`).
        assert!(
            code.contains("request : :: connectrpc :: ServiceRequest < '_"),
            "unary trait method should take request: ServiceRequest<'_, PingReq>: {code}"
        );
        // The view-family impls backing ServiceRequest come from buffa's own
        // codegen (alongside each message's view types), so connect-codegen
        // emits none of its own.
        assert!(
            !code.contains("impl :: connectrpc :: HasMessageView for"),
            "connect-codegen must not emit view-family impls (buffa does): {code}"
        );
    }

    #[test]
    fn cross_package_input_collision_suppresses_alias_for_both_sides() {
        // Regression test for #75. A service file that defines its own
        // `MyMessage` and also uses an imported `.api.v1.foo.bar.MyMessage`
        // as an RPC input previously emitted `pub type OwnedMyMessageView`
        // twice (once for the local output, once for the cross-package
        // input), failing to compile with E0428. The fix detects the
        // colliding alias name and inlines the `OwnedView<…<'static>>`
        // form for both members of the colliding set.
        let v1 = FileDescriptorProto {
            name: Some("api/v1/foo/bar/foobar.proto".into()),
            package: Some("api.v1.foo.bar".into()),
            message_type: vec![DescriptorProto {
                name: Some("MyMessage".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let v2 = minimal_file(
            Some("api.v2.foo.bar"),
            ".api.v1.foo.bar.MyMessage",
            ".api.v2.foo.bar.MyMessage",
            &["MyMessage"],
        );
        let code = gen_file(&[v1, v2], 1, &[], false).unwrap();

        // Neither side gets an alias because both would land at the same
        // identifier in the same module.
        let alias_count = code.matches("pub type OwnedMyMessageView").count();
        assert_eq!(
            alias_count, 0,
            "expected zero OwnedMyMessageView aliases when both sides collide; got {alias_count}: {code}"
        );

        // Both colliding sides reach the trait sig as the inlined
        // `OwnedView<…<'static>>` form.
        assert!(
            !code.contains("request : OwnedMyMessageView"),
            "colliding input must not reference the suppressed alias: {code}"
        );
        // The unary request is a borrowed ServiceRequest over the owned type,
        // so the alias collision only affects the (still-suppressed) aliases.
        assert!(
            code.contains("request : :: connectrpc :: ServiceRequest < '_"),
            "colliding unary input should still use ServiceRequest: {code}"
        );
    }

    #[test]
    fn cross_package_input_without_collision_keeps_alias() {
        // The #75 fix only suppresses aliases when two distinct FQNs in
        // the same target package would produce the same alias name. A
        // cross-package input with a unique short name (e.g. WKT inputs
        // like `.google.protobuf.Empty`) keeps its `OwnedEmptyView`
        // alias — generated handler code that previously read
        // `request: OwnedEmptyView` keeps working.
        let wkt = FileDescriptorProto {
            name: Some("google/protobuf/empty.proto".into()),
            package: Some("google.protobuf".into()),
            message_type: vec![DescriptorProto {
                name: Some("Empty".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let svc = minimal_file(
            Some("example.v1"),
            ".google.protobuf.Empty",
            ".example.v1.PingResp",
            &["PingResp"],
        );
        let code = gen_file(&[wkt, svc], 1, &[], false).unwrap();
        assert!(
            code.contains("pub type OwnedEmptyView = :: buffa :: view :: OwnedView"),
            "WKT cross-package input should keep its alias: {code}"
        );
        // `.google.protobuf.Empty` resolves through the default extern_path to
        // `::buffa_types::…`. extern_path targets are required to be
        // buffa ≥ 0.7.0 generated code with views enabled, so the unary input
        // uses the same `ServiceRequest<'_, Req>` form as local types — the
        // backing `buffa::HasMessageView` impl ships with buffa-types.
        assert!(
            code.contains(
                "request : :: connectrpc :: ServiceRequest < '_ , :: buffa_types :: google :: protobuf :: Empty >"
            ),
            "extern unary input should use ServiceRequest over the extern owned type: {code}"
        );
    }

    #[test]
    fn collision_inlines_in_all_streaming_method_shapes() {
        // The #75 fix substitutes `#input_arg` at four interpolation
        // sites in `generate_trait_method` (server-streaming, client-
        // streaming, bidi, unary). This drives all four shapes through
        // a colliding cross-package input to catch any regression that
        // accidentally drops the substitution from one branch.
        let v1 = FileDescriptorProto {
            name: Some("api/v1/foo/bar/foobar.proto".into()),
            package: Some("api.v1.foo.bar".into()),
            message_type: vec![DescriptorProto {
                name: Some("MyMessage".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let v2 = FileDescriptorProto {
            name: Some("api/v2/foo/bar/foobar.proto".into()),
            package: Some("api.v2.foo.bar".into()),
            message_type: vec![DescriptorProto {
                name: Some("MyMessage".into()),
                ..Default::default()
            }],
            service: vec![ServiceDescriptorProto {
                name: Some("FooBar".into()),
                method: vec![
                    MethodDescriptorProto {
                        name: Some("Unary".into()),
                        input_type: Some(".api.v1.foo.bar.MyMessage".into()),
                        output_type: Some(".api.v2.foo.bar.MyMessage".into()),
                        ..Default::default()
                    },
                    MethodDescriptorProto {
                        name: Some("ServerStream".into()),
                        input_type: Some(".api.v1.foo.bar.MyMessage".into()),
                        output_type: Some(".api.v2.foo.bar.MyMessage".into()),
                        server_streaming: Some(true),
                        ..Default::default()
                    },
                    MethodDescriptorProto {
                        name: Some("ClientStream".into()),
                        input_type: Some(".api.v1.foo.bar.MyMessage".into()),
                        output_type: Some(".api.v2.foo.bar.MyMessage".into()),
                        client_streaming: Some(true),
                        ..Default::default()
                    },
                    MethodDescriptorProto {
                        name: Some("Bidi".into()),
                        input_type: Some(".api.v1.foo.bar.MyMessage".into()),
                        output_type: Some(".api.v2.foo.bar.MyMessage".into()),
                        client_streaming: Some(true),
                        server_streaming: Some(true),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let code = gen_file(&[v1, v2], 1, &[], false).unwrap();

        // None of the four method shapes reference the suppressed alias.
        assert!(
            !code.contains("OwnedMyMessageView"),
            "no method shape should reference the suppressed alias: {code}"
        );

        // Unary and server-streaming both take the borrowed ServiceRequest
        // keyed by the owned message; the alias collision is irrelevant to it.
        assert!(
            code.matches("request : :: connectrpc :: ServiceRequest < '_")
                .count()
                >= 2,
            "unary and server-streaming should take the borrowed ServiceRequest form: {code}"
        );
        // Client-streaming and bidi inbound items are StreamMessage<Req> keyed
        // by the owned message — the alias collision is irrelevant to them.
        assert!(
            code.matches(
                "requests : :: connectrpc :: ServiceStream < :: connectrpc :: StreamMessage <"
            )
            .count()
                >= 2,
            "client-streaming and bidi should both take StreamMessage items: {code}"
        );
    }

    #[test]
    fn streaming_methods_use_encodable_item_type() {
        // Server-streaming and bidi methods should declare their stream
        // item type as `impl Encodable<Out> + Send + use<Self>` rather than
        // the bare `Out`, so handlers can return `PreEncoded` /
        // `MaybeBorrowed` items. The dispatcher and route-registration
        // arms must both turbofish `Res` since `Encodable<M>` for
        // `PreEncoded` is generic over `M` (so `Res` is no longer
        // derivable from the opaque item type).
        let file = FileDescriptorProto {
            name: Some("ex/v1/svc.proto".into()),
            package: Some("ex.v1".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Req".into()),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Resp".into()),
                    ..Default::default()
                },
            ],
            service: vec![ServiceDescriptorProto {
                name: Some("Svc".into()),
                method: vec![
                    MethodDescriptorProto {
                        name: Some("ServerStream".into()),
                        input_type: Some(".ex.v1.Req".into()),
                        output_type: Some(".ex.v1.Resp".into()),
                        server_streaming: Some(true),
                        ..Default::default()
                    },
                    MethodDescriptorProto {
                        name: Some("Bidi".into()),
                        input_type: Some(".ex.v1.Req".into()),
                        output_type: Some(".ex.v1.Resp".into()),
                        client_streaming: Some(true),
                        server_streaming: Some(true),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let code = gen_file(std::slice::from_ref(&file), 0, &[], false).unwrap();

        // Trait method declares `ServiceStream<impl Encodable<Resp> + ...>`.
        assert_eq!(
            code.matches(":: connectrpc :: ServiceStream < impl :: connectrpc :: Encodable < Resp > + Send + use < Self >>")
                .count(),
            2,
            "server-streaming and bidi should both use the Encodable item type: {code}"
        );

        // Dispatcher arms turbofish `Res` to encode_response_stream.
        assert_eq!(
            code.matches("encode_response_stream :: < Resp , _ , _ >")
                .count(),
            2,
            "dispatcher arms must turbofish Res to encode_response_stream: {code}"
        );

        // Route registrations turbofish `Res` to route_view_*_stream.
        assert!(
            code.contains("route_view_server_stream :: < _ , _ , Resp >"),
            "route_view_server_stream must turbofish Res: {code}"
        );
        assert!(
            code.contains("route_view_bidi_stream :: < _ , _ , Resp >"),
            "route_view_bidi_stream must turbofish Res: {code}"
        );
    }

    #[test]
    fn encodable_view_impls_emitted_per_output_type() {
        let file = minimal_file(
            Some("example.v1"),
            ".example.v1.PingReq",
            ".example.v1.PingResp",
            &["PingReq", "PingResp"],
        );
        let code = gen_file(std::slice::from_ref(&file), 0, &[], false).unwrap();
        assert!(
            code.contains(
                ":: connectrpc :: Encodable < PingResp > for __buffa :: view :: PingRespView"
            ),
            "missing Encodable<PingResp> for PingRespView: {code}"
        );
        assert!(
            code.contains(
                ":: connectrpc :: Encodable < PingResp > for :: buffa :: view :: OwnedView"
            ),
            "missing Encodable<PingResp> for OwnedView<PingRespView>: {code}"
        );
        // Input type should NOT get an impl (only output types).
        assert!(!code.contains("Encodable < PingReq >"), "got: {code}");
    }

    #[test]
    fn encodable_view_impls_skipped_for_extern_output() {
        // Output type resolves via the WKT extern_path → ::buffa_types::...
        // so the impl would be an orphan; verify it's skipped.
        let wkt = FileDescriptorProto {
            name: Some("google/protobuf/empty.proto".into()),
            package: Some("google.protobuf".into()),
            message_type: vec![DescriptorProto {
                name: Some("Empty".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let file = minimal_file(
            Some("example.v1"),
            ".example.v1.PingReq",
            ".google.protobuf.Empty",
            &["PingReq"],
        );
        let code = gen_file(&[wkt, file], 1, &[], false).unwrap();
        // The impl bodies call encode_view_body; the trait method's
        // `impl Encodable<M>` RPITIT bound doesn't.
        assert!(
            !code.contains("encode_view_body"),
            "extern output type must not get Encodable impl: {code}"
        );
    }

    #[test]
    fn encodable_view_impls_deduped_across_files() {
        // Two service files in different packages both return
        // `.common.v1.Reply`. The stitcher mounts both files into one
        // module tree, so the Encodable<Reply> impls must be emitted
        // exactly once across the batch (else E0119).
        let common = FileDescriptorProto {
            name: Some("common.proto".into()),
            package: Some("common.v1".into()),
            message_type: vec![DescriptorProto {
                name: Some("Reply".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let svc = |name: &str, pkg: &str| FileDescriptorProto {
            name: Some(name.into()),
            package: Some(pkg.into()),
            message_type: vec![DescriptorProto {
                name: Some("Req".into()),
                ..Default::default()
            }],
            service: vec![ServiceDescriptorProto {
                name: Some("S".into()),
                method: vec![MethodDescriptorProto {
                    name: Some("Call".into()),
                    input_type: Some(format!(".{pkg}.Req")),
                    output_type: Some(".common.v1.Reply".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let files = vec![common, svc("a.proto", "a.v1"), svc("b.proto", "b.v1")];

        let generated = generate_files(
            &files,
            &["a.proto".into(), "b.proto".into()],
            &Options::default(),
        )
        .unwrap();

        // Each service-declaring proto produces exactly one Companion file
        // named `<stem>.__connect.rs`, wired into its package stitcher.
        let companions: Vec<_> = generated
            .iter()
            .filter(|f| f.kind == GeneratedFileKind::Companion)
            .collect();
        let mut companion_names: Vec<&str> = companions.iter().map(|f| f.name.as_str()).collect();
        companion_names.sort_unstable();
        assert_eq!(companion_names, ["a.__connect.rs", "b.__connect.rs"]);
        for c in &companions {
            let stitcher = generated
                .iter()
                .find(|g| g.kind == GeneratedFileKind::PackageMod && g.package == c.package)
                .expect("each companion's package must have a stitcher");
            assert!(
                stitcher
                    .content
                    .contains(&format!("include!(\"{}\")", c.name)),
                "stitcher for {} must include companion {}",
                c.package,
                c.name
            );
        }

        let combined: String = companions.iter().map(|f| f.content.as_str()).collect();

        let view_impl = "impl ::connectrpc::Encodable<super::super::common::v1::Reply>\nfor super::super::common::v1::__buffa::view::ReplyView<'_>";
        let owned_view_impl = "impl ::connectrpc::Encodable<super::super::common::v1::Reply>\nfor ::buffa::view::OwnedView<";
        assert_eq!(
            combined.matches(view_impl).count(),
            1,
            "Encodable<Reply> for ReplyView<'_> must appear once: {combined}"
        );
        assert_eq!(
            combined.matches(owned_view_impl).count(),
            1,
            "Encodable<Reply> for OwnedView<ReplyView> must appear once: {combined}"
        );
    }

    /// Two service-declaring protos in the same package, plus one in a
    /// second package, with a shared dependency proto. Used by the
    /// `file_per_package` tests to exercise cross-file inlining and
    /// per-package grouping together.
    fn file_per_package_fixture() -> Vec<FileDescriptorProto> {
        let common = FileDescriptorProto {
            name: Some("common.proto".into()),
            package: Some("common.v1".into()),
            message_type: vec![DescriptorProto {
                name: Some("Reply".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        // Each service file declares its own request message — proto packages
        // can't have duplicate FQNs, so two same-package files with the same
        // message name would be an invalid descriptor set (and inlining both
        // into one `<dotted.pkg>.rs` under file_per_package would E0428).
        let svc = |proto_name: &str, pkg: &str, svc_name: &str, req: &str| FileDescriptorProto {
            name: Some(proto_name.into()),
            package: Some(pkg.into()),
            message_type: vec![DescriptorProto {
                name: Some(req.into()),
                ..Default::default()
            }],
            service: vec![ServiceDescriptorProto {
                name: Some(svc_name.into()),
                method: vec![MethodDescriptorProto {
                    name: Some("Call".into()),
                    input_type: Some(format!(".{pkg}.{req}")),
                    output_type: Some(".common.v1.Reply".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        vec![
            common,
            svc("a/x.proto", "a.v1", "XService", "XReq"),
            svc("a/y.proto", "a.v1", "YService", "YReq"),
            svc("b/z.proto", "b.v1", "ZService", "ZReq"),
        ]
    }

    #[test]
    fn generate_files_file_per_package_inlines_companions() {
        let files = file_per_package_fixture();
        let mut options = Options::default();
        options.buffa.file_per_package = true;

        let generated = generate_files(
            &files,
            &["a/x.proto".into(), "a/y.proto".into(), "b/z.proto".into()],
            &options,
        )
        .unwrap();

        // No Companion files survive — service stubs are inlined.
        assert!(
            !generated
                .iter()
                .any(|f| f.kind == GeneratedFileKind::Companion),
            "file_per_package must not emit sibling Companion files"
        );
        assert!(
            !generated.iter().any(|f| f.name.ends_with(".__connect.rs")),
            "file_per_package must not emit `<stem>.__connect.rs` files"
        );

        // Each service-declaring package's PackageMod inlines its services.
        let a = generated
            .iter()
            .find(|f| f.kind == GeneratedFileKind::PackageMod && f.package == "a.v1")
            .expect("a.v1 PackageMod must exist");
        assert!(
            a.content.contains("pub trait XService"),
            "a.v1 missing XService"
        );
        assert!(
            a.content.contains("pub trait YService"),
            "a.v1 missing YService"
        );
        assert!(
            !a.content.contains("pub trait ZService"),
            "a.v1 must not inline ZService"
        );
        assert!(
            !a.content.contains("__connect.rs"),
            "a.v1 PackageMod must not include! a connect file: {}",
            a.content
        );

        let b = generated
            .iter()
            .find(|f| f.kind == GeneratedFileKind::PackageMod && f.package == "b.v1")
            .expect("b.v1 PackageMod must exist");
        assert!(
            b.content.contains("pub trait ZService"),
            "b.v1 missing ZService"
        );
        assert!(
            !b.content.contains("pub trait XService"),
            "b.v1 must not inline XService"
        );

        // No PackageMod is emitted for the dependency-only package
        // `common.v1` — it is not in `file_to_generate`.
        let pkg_mods = generated
            .iter()
            .filter(|f| f.kind == GeneratedFileKind::PackageMod)
            .count();
        assert_eq!(
            pkg_mods, 2,
            "expected exactly two PackageMods: {generated:#?}"
        );

        // The cross-file Encodable<Reply> dedup must hold under
        // file_per_package exactly as it does under the per-proto split:
        // one impl pair across the whole batch (else E0119 at consumer
        // compile time). All three services return `.common.v1.Reply`.
        let combined: String = generated.iter().map(|f| f.content.as_str()).collect();
        assert_eq!(
            combined
                .matches("impl ::connectrpc::Encodable<super::super::common::v1::Reply>")
                .count(),
            2,
            "Encodable<Reply> impls must be deduplicated across packages \
             (1 for ReplyView, 1 for OwnedView<ReplyView>): {combined}"
        );
    }

    #[test]
    fn generate_services_file_per_package_emits_one_file_per_package() {
        let files = file_per_package_fixture();
        let mut options = Options::default();
        options.buffa.file_per_package = true;
        options
            .buffa
            .extern_paths
            .push((".".into(), "crate::proto".into()));

        let generated = generate_services(
            &files,
            &["a/x.proto".into(), "a/y.proto".into(), "b/z.proto".into()],
            &options,
        )
        .unwrap();

        // Output is exactly one PackageMod per service-declaring package
        // with all stubs inlined; no companions, no `<pkg>.mod.rs` stitchers.
        assert_eq!(
            generated.len(),
            2,
            "expected exactly two output files: {generated:#?}"
        );
        assert!(
            generated
                .iter()
                .all(|f| f.kind == GeneratedFileKind::PackageMod),
            "all output files must be PackageMod"
        );
        assert!(
            !generated.iter().any(|f| f.name.ends_with(".mod.rs")),
            "file_per_package must not emit a separate stitcher"
        );
        assert!(
            !generated.iter().any(|f| f.content.contains("include!")),
            "file_per_package output must not include! sibling files"
        );

        let mut names: Vec<&str> = generated.iter().map(|f| f.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            ["a.v1.rs", "b.v1.rs"],
            "filenames must be `<dotted.pkg>.rs` to match buffa's file_per_package convention"
        );

        let a = generated.iter().find(|f| f.package == "a.v1").unwrap();
        assert!(a.content.contains("pub trait XService"));
        assert!(a.content.contains("pub trait YService"));
        let b = generated.iter().find(|f| f.package == "b.v1").unwrap();
        assert!(b.content.contains("pub trait ZService"));
        assert!(!b.content.contains("pub trait XService"));
    }

    #[test]
    fn generate_services_file_per_package_default_layout_unchanged() {
        // Sanity: when the option is off, the existing per-proto + stitcher
        // layout is preserved (regression guard for the new branch).
        let files = file_per_package_fixture();
        let mut options = Options::default();
        options
            .buffa
            .extern_paths
            .push((".".into(), "crate::proto".into()));

        let generated = generate_services(
            &files,
            &["a/x.proto".into(), "a/y.proto".into(), "b/z.proto".into()],
            &options,
        )
        .unwrap();

        let mut companions: Vec<&str> = generated
            .iter()
            .filter(|f| f.kind == GeneratedFileKind::Companion)
            .map(|f| f.name.as_str())
            .collect();
        companions.sort_unstable();
        assert_eq!(
            companions,
            ["a.x.__connect.rs", "a.y.__connect.rs", "b.z.__connect.rs"],
            "default layout emits one companion per proto"
        );
        let mut stitchers: Vec<&str> = generated
            .iter()
            .filter(|f| f.kind == GeneratedFileKind::PackageMod)
            .map(|f| f.name.as_str())
            .collect();
        stitchers.sort_unstable();
        assert_eq!(
            stitchers,
            ["a.v1.mod.rs", "b.v1.mod.rs"],
            "default layout emits one stitcher per package"
        );
        // Each stitcher include!s its package's companions.
        let a_stitcher = generated.iter().find(|f| f.name == "a.v1.mod.rs").unwrap();
        assert!(
            a_stitcher
                .content
                .contains(r#"include!("a.x.__connect.rs");"#)
        );
        assert!(
            a_stitcher
                .content
                .contains(r#"include!("a.y.__connect.rs");"#)
        );
    }

    #[test]
    fn service_name_with_package() {
        let file = minimal_file(
            Some("example.v1"),
            ".example.v1.PingReq",
            ".example.v1.PingResp",
            &["PingReq", "PingResp"],
        );
        let code = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap();
        assert!(code.contains("\"example.v1.PingService\""), "got: {code}");
    }

    #[test]
    fn service_name_without_package() {
        // Empty package must produce "PingService", not ".PingService".
        let file = minimal_file(None, ".PingReq", ".PingResp", &["PingReq", "PingResp"]);
        let code = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap();
        assert!(code.contains("\"PingService\""), "got: {code}");
        assert!(
            !code.contains("\".PingService\""),
            "must not have leading dot: {code}"
        );
    }

    #[test]
    fn same_package_types_use_bare_names() {
        let file = minimal_file(
            Some("example.v1"),
            ".example.v1.PingReq",
            ".example.v1.PingResp",
            &["PingReq", "PingResp"],
        );
        let code = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap();
        // Same-package types resolve to bare identifiers.
        assert!(code.contains("PingReq"), "input type missing: {code}");
        assert!(code.contains("PingResp"), "output type missing: {code}");
        // No super:: prefix for same-package types.
        assert!(
            !code.contains("super :: PingReq"),
            "unexpected super: {code}"
        );
    }

    #[test]
    fn cross_package_types_use_relative_paths() {
        // Service in example.v1 references types from common.v1.
        // Must emit a super::-relative path matching buffa's module
        // layout, not bare `Shared` (which would fail to compile).
        let common = FileDescriptorProto {
            name: Some("common.proto".into()),
            package: Some("common.v1".into()),
            message_type: vec![DescriptorProto {
                name: Some("Shared".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let svc = minimal_file(
            Some("example.v1"),
            ".common.v1.Shared",
            ".example.v1.Out",
            &["Out"],
        );
        let code = gen_service(&[common, svc], 1, &[], false).unwrap();

        // example.v1 -> super::super -> common::v1::Shared
        // (token stream stringifies `::` with spaces, so match loosely)
        assert!(
            code.contains("super :: super :: common :: v1 :: Shared"),
            "cross-package path not emitted: {code}"
        );
        assert!(
            code.contains("super :: super :: common :: v1 :: __buffa :: view :: SharedView"),
            "cross-package view path not emitted: {code}"
        );
    }

    #[test]
    fn nested_message_view_type_mirrors_owned_module_nesting() {
        // Service in example.v1 references Outer.Inner (nested under Outer).
        // buffa lays out the view as __buffa::view::outer::InnerView, mirroring
        // the owned outer::Inner layout. rust_view_type must insert the
        // sentinel at the package boundary, not at the type boundary.
        let file = FileDescriptorProto {
            name: Some("nested.proto".into()),
            package: Some("example.v1".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Outer".into()),
                    nested_type: vec![DescriptorProto {
                        name: Some("Inner".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Out".into()),
                    ..Default::default()
                },
            ],
            service: vec![ServiceDescriptorProto {
                name: Some("NestedService".into()),
                method: vec![MethodDescriptorProto {
                    name: Some("Ping".into()),
                    input_type: Some(".example.v1.Outer.Inner".into()),
                    output_type: Some(".example.v1.Out".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let code = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap();

        assert!(
            code.contains("__buffa :: view :: outer :: InnerView"),
            "nested view path not emitted: {code}"
        );
        assert!(
            code.contains("outer :: Inner"),
            "nested owned path not emitted: {code}"
        );
    }

    #[test]
    fn wkt_types_use_buffa_types_extern_path() {
        // Service referencing google.protobuf.Empty as an input/output
        // type. WKT auto-injection maps it to ::buffa_types::..., same
        // path buffa-codegen emits for WKT message fields.
        let wkt = FileDescriptorProto {
            name: Some("google/protobuf/empty.proto".into()),
            package: Some("google.protobuf".into()),
            message_type: vec![DescriptorProto {
                name: Some("Empty".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let svc = minimal_file(
            Some("example.v1"),
            ".google.protobuf.Empty",
            ".example.v1.Out",
            &["Out"],
        );
        let code = gen_service(&[wkt, svc], 1, &[], false).unwrap();

        assert!(
            code.contains(":: buffa_types :: google :: protobuf :: Empty"),
            "WKT extern path not emitted: {code}"
        );
    }

    #[test]
    fn extern_catchall_uses_absolute_paths() {
        let file = minimal_file(
            Some("example.v1"),
            ".example.v1.PingReq",
            ".example.v1.PingResp",
            &["PingReq", "PingResp"],
        );
        let extern_paths = [(".".into(), "crate::proto".into())];
        let code = gen_service(std::slice::from_ref(&file), 0, &extern_paths, true).unwrap();
        assert!(
            code.contains("crate :: proto :: example :: v1 :: PingReq"),
            "owned type path missing: {code}"
        );
        assert!(
            code.contains("crate :: proto :: example :: v1 :: __buffa :: view :: PingReqView"),
            "view type path missing: {code}"
        );
    }

    #[test]
    fn extern_catchall_with_wkt_longest_wins() {
        // Auto-injected `.google.protobuf` mapping is more specific than
        // the `.` catch-all, so WKTs still route to ::buffa_types.
        let wkt = FileDescriptorProto {
            name: Some("google/protobuf/empty.proto".into()),
            package: Some("google.protobuf".into()),
            message_type: vec![DescriptorProto {
                name: Some("Empty".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let svc = minimal_file(
            Some("example.v1"),
            ".google.protobuf.Empty",
            ".example.v1.Out",
            &["Out"],
        );
        let extern_paths = [(".".into(), "crate::proto".into())];
        let code = gen_service(&[wkt, svc], 1, &extern_paths, true).unwrap();
        assert!(
            code.contains(":: buffa_types :: google :: protobuf :: Empty"),
            "WKT mapping lost to catch-all: {code}"
        );
        assert!(
            code.contains("crate :: proto :: example :: v1 :: Out"),
            "local type not routed through catch-all: {code}"
        );
    }

    #[test]
    fn missing_extern_path_errors() {
        let file = minimal_file(
            Some("example.v1"),
            ".example.v1.PingReq",
            ".example.v1.PingResp",
            &["PingReq", "PingResp"],
        );
        let err = gen_service(std::slice::from_ref(&file), 0, &[], true).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("extern_path"),
            "error message lacks hint: {msg}"
        );
    }

    #[test]
    fn keyword_package_escaped() {
        // `google.type` -> `google::r#type` via idents::rust_path_to_tokens.
        let file = minimal_file(
            Some("google.type"),
            ".google.type.LatLng",
            ".google.type.LatLng",
            &["LatLng"],
        );
        let extern_paths = [(".".into(), "crate::proto".into())];
        let code = gen_service(std::slice::from_ref(&file), 0, &extern_paths, true).unwrap();
        assert!(
            code.contains("crate :: proto :: google :: r#type :: LatLng"),
            "keyword segment not escaped: {code}"
        );
    }

    #[test]
    fn keyword_method_escaped() {
        // `rpc Move(...)` -> snake_case `move` is a Rust keyword; emit `r#move`
        // via idents::make_field_ident. Regression for issue #23.
        let file = minimal_file_with_method(
            Some("example.v1"),
            "Move",
            ".example.v1.Empty",
            ".example.v1.Empty",
            &["Empty"],
        );
        let code = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap();
        assert!(
            code.contains("fn r#move"),
            "keyword method not escaped: {code}"
        );
        assert!(
            code.contains("move_with_options"),
            "suffixed variant should not need escaping: {code}"
        );
        // Doc example should also use the escaped form so the snippet is valid.
        assert!(code.contains("client.r#move(request)"));
        syn::parse_str::<syn::File>(&code).expect("generated code parses");
    }

    #[test]
    fn path_keyword_method_suffixed() {
        // `self`/`super`/`Self`/`crate` cannot be raw identifiers; they are
        // suffixed with `_` instead (matching prost convention).
        let file = minimal_file_with_method(
            Some("example.v1"),
            "Self",
            ".example.v1.Empty",
            ".example.v1.Empty",
            &["Empty"],
        );
        let code = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap();
        assert!(
            code.contains("fn self_"),
            "path-keyword method not suffixed: {code}"
        );
        // The `_with_options` variant uses the unsuffixed snake name; the
        // suffix already de-keywords it, so we get `self_with_options`
        // (not `self__with_options`).
        assert!(code.contains("self_with_options"));
        syn::parse_str::<syn::File>(&code).expect("generated code parses");
    }

    #[test]
    fn service_name_keyword_suffixed() {
        // `service Self {}` is accepted by protoc but `Self` is a Rust keyword
        // that cannot be a raw ident; the bare trait name is suffixed `Self_`
        // while the derived `SelfExt`/`SelfClient`/`SelfServer` are already safe.
        let mut file = minimal_file(
            Some("example.v1"),
            ".example.v1.Empty",
            ".example.v1.Empty",
            &["Empty"],
        );
        file.service[0].name = Some("Self".into());
        let code = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap();
        assert!(code.contains("trait Self_ "), "trait not suffixed: {code}");
        assert!(code.contains("trait SelfExt"));
        assert!(code.contains("struct SelfClient"));
        assert!(code.contains("struct SelfServer"));
        syn::parse_str::<syn::File>(&code).expect("generated code parses");
    }

    #[test]
    fn method_snake_collision_errors() {
        // protoc accepts `GetFoo` and `get_foo` in the same service; both
        // snake-case to `get_foo`, which would emit duplicate Rust methods.
        let file = minimal_file_with_methods("example.v1", &["GetFoo", "get_foo"]);
        let err = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("PingService"), "missing service name: {msg}");
        assert!(msg.contains("\"GetFoo\""), "missing first method: {msg}");
        assert!(msg.contains("\"get_foo\""), "missing second method: {msg}");
        assert!(msg.contains("`get_foo`"), "missing rust ident: {msg}");
    }

    #[test]
    fn method_with_options_collision_errors() {
        // `Ping` generates client method `ping_with_options`; a proto method
        // `PingWithOptions` would generate the same base name.
        let file = minimal_file_with_methods("example.v1", &["Ping", "PingWithOptions"]);
        let err = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("\"Ping\""), "missing first method: {msg}");
        assert!(
            msg.contains("\"PingWithOptions\""),
            "missing second method: {msg}"
        );
        assert!(
            msg.contains("`ping_with_options`"),
            "missing rust ident: {msg}"
        );
    }

    #[test]
    fn distinct_methods_do_not_collide() {
        let file = minimal_file_with_methods("example.v1", &["GetFoo", "GetBar"]);
        let code = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap();
        syn::parse_str::<syn::File>(&code).expect("generated code parses");
    }

    #[test]
    fn options_default_buffa_config() {
        let cfg = Options::default().to_buffa_config();
        assert!(cfg.generate_json, "connectrpc enables JSON by default");
        assert!(cfg.generate_views);
        assert!(cfg.emit_register_fn);
        assert!(!cfg.strict_utf8_mapping);
    }

    #[test]
    fn options_buffa_passthrough_forces_views() {
        let mut opts = Options::default();
        opts.buffa.emit_register_fn = false;
        opts.buffa.generate_views = false;
        let cfg = opts.to_buffa_config();
        assert!(!cfg.emit_register_fn);
        assert!(cfg.generate_views, "generate_views must be forced on");
    }

    #[test]
    fn generate_files_emit_register_fn_false_suppresses_register_types() {
        // Build a file with a single message so buffa would normally emit
        // `pub fn register_types(&mut TypeRegistry)` aggregating it.
        let file = FileDescriptorProto {
            name: Some("ping.proto".into()),
            package: Some("example.v1".into()),
            message_type: vec![DescriptorProto {
                name: Some("PingReq".into()),
                ..Default::default()
            }],
            ..Default::default()
        };

        // `register_types` is emitted into the per-package stitcher, so
        // locate the PackageMod output and check that one.
        let stitcher = |files: &[GeneratedFile]| {
            files
                .iter()
                .find(|f| f.kind == GeneratedFileKind::PackageMod)
                .expect("PackageMod file emitted")
                .content
                .clone()
        };

        let with_fn = generate_files(
            std::slice::from_ref(&file),
            &["ping.proto".into()],
            &Options::default(),
        )
        .unwrap();
        let mod_rs = stitcher(&with_fn);
        assert!(
            mod_rs.contains("fn register_types"),
            "expected register_types in default output: {mod_rs}"
        );

        let mut opts = Options::default();
        opts.buffa.emit_register_fn = false;
        let without_fn =
            generate_files(std::slice::from_ref(&file), &["ping.proto".into()], &opts).unwrap();
        let mod_rs = stitcher(&without_fn);
        assert!(
            !mod_rs.contains("fn register_types"),
            "register_types should be suppressed: {mod_rs}"
        );
    }

    #[test]
    fn plugin_no_register_fn_parses() {
        let request = CodeGeneratorRequest {
            parameter: Some("buffa_module=crate::proto,no_register_fn".into()),
            file_to_generate: vec![],
            proto_file: vec![],
            ..Default::default()
        };
        // Plugin path emits services only, so we can't observe the buffa
        // config directly — just make sure the option parses without error.
        generate(&request).expect("no_register_fn should be a recognized plugin option");
    }

    /// Format `generate_service` output for a single-service file using
    /// the local `minimal_file` fixture. `gate_client_feature` selects
    /// whether the opt-in cfg attr is emitted; shared by the `*_client_*`
    /// tests below.
    fn format_minimal_service(gate_client_feature: bool) -> String {
        let file = minimal_file(
            Some("example.v1"),
            ".example.v1.PingReq",
            ".example.v1.PingResp",
            &["PingReq", "PingResp"],
        );
        let config = buffa_codegen::CodeGenConfig::default();
        let target = file.name.clone().into_iter().collect::<Vec<_>>();
        let resolver = TypeResolver::new(std::slice::from_ref(&file), &target, &config, false);
        let service = &file.service[0];
        let batch = BatchState {
            colliding_aliases: collect_alias_collisions(std::slice::from_ref(&file), &target),
            gate_client_feature,
            ..BatchState::default()
        };
        format_token_stream(&generate_service(&file, service, &resolver, &batch).unwrap()).unwrap()
    }

    #[test]
    fn default_emission_has_no_client_cfg() {
        // CRITICAL invariant: with the option unset, codegen emits zero
        // `#[cfg(feature = "client")]` attrs. External users with their
        // own protos must not be forced to declare a Cargo feature.
        let out = format_minimal_service(false);
        assert!(
            !out.contains("#[cfg(feature ="),
            "default emission must not emit any cfg attr — external \
             consumers should not need to declare a `client` Cargo \
             feature unless they explicitly opt in via the \
             `gate_client_feature` plugin option:\n{out}"
        );
    }

    #[test]
    fn client_items_gated_when_opt_in() {
        // When `gate_client_feature` is set, the `FooClient` struct +
        // impl carry `#[cfg(feature = "client")]`. Exactly two attrs:
        // one on the struct, one on the impl block. (All `_with_options`
        // methods live inside the impl and inherit the gate.)
        let out = format_minimal_service(true);
        let cfg_count = out.matches("#[cfg(feature = \"client\")]").count();
        assert_eq!(
            cfg_count, 2,
            "expected exactly two #[cfg(feature = \"client\")] attrs (one on \
             `pub struct PingServiceClient`, one on its `impl<T>` block); got \
             {cfg_count}:\n{out}"
        );
    }

    #[test]
    fn server_items_never_carry_client_cfg() {
        // The trait, ext trait, and monomorphic dispatcher live on the
        // server side; nothing about them should be feature-gated even
        // under the opt-in path.
        let out = format_minimal_service(true);
        for marker in [
            "pub trait PingService",
            "pub trait PingServiceExt",
            "pub struct PingServiceServer",
            "pub const PING_SERVICE_SERVICE_NAME",
        ] {
            let idx = out
                .find(marker)
                .unwrap_or_else(|| panic!("expected `{marker}` in output:\n{out}"));
            let prefix = &out[..idx];
            assert!(
                !prefix.trim_end().ends_with("#[cfg(feature = \"client\")]"),
                "`{marker}` must not be preceded by a client cfg attr — \
                 server-side items are always compiled in:\n{out}"
            );
        }
    }

    /// The strongest invariant: every reference to
    /// `::connectrpc::client::*` (or the unqualified `connectrpc::client::`
    /// — should not appear, but guard anyway) must live inside an item
    /// (or ancestor module/item) carrying `#[cfg(feature = "client")]`.
    /// Catches the missing-gate regression that a count-only test cannot
    /// detect: e.g. a future `impl<T> Default for FooClient<T>` that the
    /// contributor forgot to prefix.
    ///
    /// Walks recursively into `Item::Mod` bodies so a gate on a parent
    /// module implicitly covers its children — avoids false-positives
    /// where a wrapper `pub mod gated { #[cfg(...)] … }` would flag the
    /// outer module just because its rendered body mentions
    /// `::connectrpc::client::`.
    #[test]
    fn no_ungated_client_references() {
        // Only relevant under the opt-in path — that's where the
        // invariant ("every `::connectrpc::client::*` reference lives
        // inside a gated item") is meaningful.
        let out = format_minimal_service(true);
        let parsed: syn::File = syn::parse_str(&out).expect("output parses");

        let mut offenders: Vec<String> = Vec::new();
        scan_items_for_ungated_client_refs(&parsed.items, false, &mut offenders);
        assert!(
            offenders.is_empty(),
            "every item that mentions `::connectrpc::client::*` must be \
             prefixed with `#[cfg(feature = \"client\")]`. Offenders:\n{}\n\nFull output:\n{out}",
            offenders.join("\n")
        );
    }

    /// Predicate: is this attribute `#[cfg(feature = "client")]`?
    /// Stringifies the attr to avoid coupling to syn's parsed `Meta`
    /// shape across versions.
    fn is_client_feature_cfg(attr: &syn::Attribute) -> bool {
        attr.path().is_ident("cfg")
            && attr
                .to_token_stream()
                .to_string()
                .contains("feature = \"client\"")
    }

    /// Render `ts` through prettyplease (matching the spacing of the
    /// rest of the codegen test surface) and check for any reference
    /// to `::connectrpc::client::` or `connectrpc :: client ::` (the
    /// pre-prettyplease form, defensive).
    fn mentions_connectrpc_client(ts: TokenStream) -> bool {
        let rendered = format_token_stream(&ts).unwrap_or_default();
        rendered.contains("::connectrpc::client::") || rendered.contains("connectrpc :: client ::")
    }

    /// Recursive walker for `no_ungated_client_references`. For each
    /// item: if the item or any ancestor is `#[cfg(feature = "client")]`,
    /// it's gated and we skip. Otherwise, if its rendered tokens
    /// mention `::connectrpc::client::`, push an offender entry.
    /// `Item::Mod` recurses into its children so a parent-level gate
    /// implicitly covers them.
    ///
    /// Item kinds the codegen doesn't currently emit at top level
    /// (`Use`, `Static`, `Macro`, `ForeignMod`, `Union`, `TraitAlias`,
    /// `ExternCrate`, `Verbatim`, …) still go through the textual scan
    /// via the fallthrough arm — they're not gated by anything we can
    /// inspect, so if their token rendering mentions
    /// `::connectrpc::client::` they're flagged. This is the defensive
    /// shape: a future emission that introduces e.g. an ungated
    /// `use ::connectrpc::client::ClientConfig;` at module scope must
    /// not slip past the invariant test.
    fn scan_items_for_ungated_client_refs(
        items: &[syn::Item],
        ancestor_gated: bool,
        offenders: &mut Vec<String>,
    ) {
        for item in items {
            // Extract attrs for the kinds we explicitly model. For
            // everything else we treat the item as not self-gated and
            // fall through to the textual scan — better a false
            // positive on an exotic ungated emission than silently
            // missing a real one.
            let (attrs, ident): (&[syn::Attribute], String) = match item {
                syn::Item::Struct(s) => (&s.attrs, s.ident.to_string()),
                syn::Item::Impl(i) => (
                    &i.attrs,
                    format!("impl-block for {}", ToTokens::to_token_stream(&i.self_ty)),
                ),
                syn::Item::Fn(f) => (&f.attrs, f.sig.ident.to_string()),
                syn::Item::Trait(t) => (&t.attrs, t.ident.to_string()),
                syn::Item::Const(c) => (&c.attrs, c.ident.to_string()),
                syn::Item::Type(t) => (&t.attrs, t.ident.to_string()),
                syn::Item::Static(s) => (&s.attrs, s.ident.to_string()),
                syn::Item::Use(u) => (&u.attrs, "use-item".to_string()),
                syn::Item::ExternCrate(e) => (&e.attrs, e.ident.to_string()),
                syn::Item::Macro(m) => (
                    &m.attrs,
                    m.ident
                        .as_ref()
                        .map(syn::Ident::to_string)
                        .unwrap_or_else(|| "macro-item".to_string()),
                ),
                syn::Item::ForeignMod(f) => (&f.attrs, "extern-block".to_string()),
                syn::Item::Union(u) => (&u.attrs, u.ident.to_string()),
                syn::Item::TraitAlias(t) => (&t.attrs, t.ident.to_string()),
                syn::Item::Enum(e) => (&e.attrs, e.ident.to_string()),
                syn::Item::Mod(m) => {
                    let self_gated = m.attrs.iter().any(is_client_feature_cfg);
                    let gated = ancestor_gated || self_gated;
                    if let Some((_brace, children)) = &m.content {
                        scan_items_for_ungated_client_refs(children, gated, offenders);
                    }
                    // Don't fall through — the textual scan on a Mod's
                    // tokens would render its children too and double-count.
                    continue;
                }
                // `Item::Verbatim` and any future syn variant: we can't
                // inspect attrs, so assume not self-gated and let the
                // textual scan decide.
                _ => (&[][..], "<unrecognized item>".to_string()),
            };
            let self_gated = attrs.iter().any(is_client_feature_cfg);
            let gated = ancestor_gated || self_gated;
            if gated {
                continue;
            }
            if mentions_connectrpc_client(ToTokens::to_token_stream(item)) {
                offenders.push(format!(
                    "ungated reference to ::connectrpc::client in `{ident}`"
                ));
            }
        }
    }

    /// Verify the recursive scanner: a parent module gated on `client`
    /// covers its children (no false-positive); an ungated parent
    /// containing an ungated child gets flagged via the child, not the
    /// parent's textual rendering (no double-counting).
    #[test]
    fn ungated_scanner_handles_nested_modules() {
        // Case 1: gated parent + ungated-looking child → no offenders.
        let parsed: syn::File = syn::parse_str(
            r#"
            #[cfg(feature = "client")]
            pub mod gated_parent {
                pub struct WithClientRef {
                    field: ::connectrpc::client::ClientConfig,
                }
            }
            "#,
        )
        .unwrap();
        let mut offenders = Vec::new();
        scan_items_for_ungated_client_refs(&parsed.items, false, &mut offenders);
        assert!(
            offenders.is_empty(),
            "parent-level cfg must cover children: {offenders:?}"
        );

        // Case 2: ungated parent + ungated child referencing client → exactly
        // ONE offender (the inner struct), not two (parent + child).
        let parsed: syn::File = syn::parse_str(
            r#"
            pub mod ungated_parent {
                pub struct WithClientRef {
                    field: ::connectrpc::client::ClientConfig,
                }
            }
            "#,
        )
        .unwrap();
        let mut offenders = Vec::new();
        scan_items_for_ungated_client_refs(&parsed.items, false, &mut offenders);
        assert_eq!(
            offenders.len(),
            1,
            "exactly one offender expected (the inner struct), not the wrapping \
             module: {offenders:?}"
        );
        assert!(
            offenders[0].contains("WithClientRef"),
            "offender should name the inner struct: {:?}",
            offenders[0]
        );

        // Case 3: ungated parent containing a gated child → no offenders.
        let parsed: syn::File = syn::parse_str(
            r#"
            pub mod outer {
                #[cfg(feature = "client")]
                pub struct GatedClient {
                    field: ::connectrpc::client::ClientConfig,
                }
            }
            "#,
        )
        .unwrap();
        let mut offenders = Vec::new();
        scan_items_for_ungated_client_refs(&parsed.items, false, &mut offenders);
        assert!(
            offenders.is_empty(),
            "self-gating child inside ungated module must be OK: {offenders:?}"
        );
    }

    /// Regression: the scanner must not silently skip `syn::Item` variants
    /// the codegen doesn't currently emit. A future ungated
    /// `use ::connectrpc::client::ClientConfig;` or a `static`
    /// referencing the client module would have slipped past the
    /// earlier `_ => continue` catch-all; the expanded variant arms +
    /// fallthrough textual scan catch it now.
    #[test]
    fn ungated_scanner_catches_use_and_static_items() {
        // Item::Use, ungated → flagged.
        let parsed: syn::File = syn::parse_str("use ::connectrpc::client::ClientConfig;").unwrap();
        let mut offenders = Vec::new();
        scan_items_for_ungated_client_refs(&parsed.items, false, &mut offenders);
        assert_eq!(
            offenders.len(),
            1,
            "ungated `use ::connectrpc::client::*` must be flagged: {offenders:?}"
        );

        // Item::Use, gated → OK.
        let parsed: syn::File =
            syn::parse_str("#[cfg(feature = \"client\")] use ::connectrpc::client::ClientConfig;")
                .unwrap();
        let mut offenders = Vec::new();
        scan_items_for_ungated_client_refs(&parsed.items, false, &mut offenders);
        assert!(
            offenders.is_empty(),
            "gated `use ::connectrpc::client::*` must NOT be flagged: {offenders:?}"
        );

        // Item::Static, ungated, referencing client module → flagged.
        let parsed: syn::File =
            syn::parse_str("static FOO: &str = stringify!(::connectrpc::client::ClientConfig);")
                .unwrap();
        let mut offenders = Vec::new();
        scan_items_for_ungated_client_refs(&parsed.items, false, &mut offenders);
        assert_eq!(
            offenders.len(),
            1,
            "ungated `static FOO` mentioning ::connectrpc::client must be flagged: \
             {offenders:?}"
        );
    }

    #[test]
    fn client_cfg_round_trips_through_prettyplease() {
        // Sanity: prettyplease formats the cfg attr to exactly the
        // canonical spelling we grep for in the count test. If a future
        // formatting change reshapes the attribute (e.g. inserts spaces),
        // the count test would silently report zero matches — make sure
        // we'd notice.
        let out = format_minimal_service(true);
        // The exact rendered form prettyplease uses; if this assertion
        // ever fails we need to update the other test's grep pattern.
        assert!(
            out.contains("#[cfg(feature = \"client\")]"),
            "prettyplease no longer renders the cfg attr as expected; \
             update the grep pattern in client_items_always_gated:\n{out}"
        );
    }

    #[test]
    fn multi_service_in_one_file_each_client_is_gated() {
        // Two services in the same file → 4 cfg attrs (2 per FooClient).
        // Catches a regression where the cfg interpolation accidentally
        // moved outside the per-service token block.
        let make_service = |name: &str| ServiceDescriptorProto {
            name: Some(name.into()),
            method: vec![MethodDescriptorProto {
                name: Some("Ping".into()),
                input_type: Some(".example.v1.PingReq".into()),
                output_type: Some(".example.v1.PingResp".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let file = FileDescriptorProto {
            name: Some("two.proto".into()),
            package: Some("example.v1".into()),
            service: vec![make_service("Alpha"), make_service("Beta")],
            message_type: vec![
                DescriptorProto {
                    name: Some("PingReq".into()),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("PingResp".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let config = buffa_codegen::CodeGenConfig::default();
        let target = vec!["two.proto".to_string()];
        let resolver = TypeResolver::new(std::slice::from_ref(&file), &target, &config, false);
        let mut batch = BatchState {
            colliding_aliases: collect_alias_collisions(std::slice::from_ref(&file), &target),
            gate_client_feature: true,
            ..BatchState::default()
        };
        let ts = generate_connect_services(&file, &resolver, &mut batch).unwrap();
        let out = format_token_stream(&ts).unwrap();
        let cfg_count = out.matches("#[cfg(feature = \"client\")]").count();
        assert_eq!(
            cfg_count, 4,
            "expected 4 client cfg attrs (2 per service * 2 services); got \
             {cfg_count}:\n{out}"
        );
        // Both client structs are present, both gated.
        for client_struct in ["pub struct AlphaClient", "pub struct BetaClient"] {
            let idx = out
                .find(client_struct)
                .unwrap_or_else(|| panic!("expected `{client_struct}` in output:\n{out}"));
            let prefix = &out[..idx];
            assert!(
                prefix.trim_end().ends_with("#[derive(Clone)]")
                    || prefix.contains("#[cfg(feature = \"client\")]"),
                "`{client_struct}` must have a client cfg attr in its \
                 attribute cluster:\n{out}"
            );
        }
    }

    #[test]
    fn plugin_accepts_gate_client_feature_flag() {
        // The current option is a bare flag (no `=value`).
        let request = CodeGeneratorRequest {
            parameter: Some("buffa_module=crate::proto,gate_client_feature".into()),
            file_to_generate: vec![],
            proto_file: vec![],
            ..Default::default()
        };
        generate(&request).expect("gate_client_feature should be a recognized plugin option");
    }

    #[test]
    fn plugin_rejects_old_client_feature_value_form() {
        // The previous design used `client_feature=<name>` with an
        // arbitrary feature name. That option was renamed to the bare
        // flag `gate_client_feature` (the feature name is fixed as
        // `client`). A stale buf.gen.yaml using the old form must fail
        // loudly, not silently no-op.
        let request = CodeGeneratorRequest {
            parameter: Some("buffa_module=crate::proto,client_feature=client".into()),
            file_to_generate: vec![],
            proto_file: vec![],
            ..Default::default()
        };
        let err = generate(&request)
            .expect_err("legacy `client_feature=…` option must now fail as unknown");
        let msg = err.to_string();
        assert!(
            msg.contains("client_feature"),
            "error should name the offending option: {msg}"
        );
        assert!(
            msg.contains("unknown plugin option"),
            "error should say the option is unknown: {msg}"
        );
    }

    #[test]
    fn plugin_file_per_package_collapses_output() {
        // End-to-end through the protoc entry point: one `<dotted.pkg>.rs`
        // per package, no `<stem>.__connect.rs`, no `<pkg>.mod.rs`.
        let request = CodeGeneratorRequest {
            parameter: Some("buffa_module=crate::proto,file_per_package".into()),
            file_to_generate: vec!["a/x.proto".into(), "a/y.proto".into(), "b/z.proto".into()],
            proto_file: file_per_package_fixture(),
            ..Default::default()
        };
        let response = generate(&request).expect("file_per_package should parse and generate");
        let mut names: Vec<&str> = response
            .file
            .iter()
            .filter_map(|f| f.name.as_deref())
            .collect();
        names.sort_unstable();
        assert_eq!(
            names,
            ["a.v1.rs", "b.v1.rs"],
            "expected one file per package: {names:?}"
        );
        for f in &response.file {
            let content = f.content.as_deref().unwrap_or_default();
            assert!(
                !content.contains("include!"),
                "file_per_package output must be self-contained: {content}"
            );
        }
    }

    #[test]
    fn no_top_level_use_statements_in_generated_code() {
        // When multiple service files are `include!`d into the same module,
        // top-level `use` statements cause E0252 (duplicate imports). Verify
        // the generated code uses fully qualified paths instead.
        let file = minimal_file(
            Some("example.v1"),
            ".example.v1.PingReq",
            ".example.v1.PingResp",
            &["PingReq", "PingResp"],
        );
        let code = gen_service(std::slice::from_ref(&file), 0, &[], false).unwrap();
        let formatted = format_token_stream(&code.parse::<TokenStream>().unwrap()).unwrap();
        assert_no_top_level_use(&formatted, "generated code");
    }

    #[test]
    fn multi_service_include_no_e0252() {
        // Simulate `buffa-packaging` including two service files into one
        // module. Both files must parse together without duplicate imports.
        let file_a = {
            let method = MethodDescriptorProto {
                name: Some("Ping".into()),
                input_type: Some(".svc.v1.PingReq".into()),
                output_type: Some(".svc.v1.PingResp".into()),
                ..Default::default()
            };
            let service = ServiceDescriptorProto {
                name: Some("Alpha".into()),
                method: vec![method],
                ..Default::default()
            };
            FileDescriptorProto {
                name: Some("alpha.proto".into()),
                package: Some("svc.v1".into()),
                service: vec![service],
                message_type: vec![
                    DescriptorProto {
                        name: Some("PingReq".into()),
                        ..Default::default()
                    },
                    DescriptorProto {
                        name: Some("PingResp".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        };
        let file_b = {
            let method = MethodDescriptorProto {
                name: Some("Pong".into()),
                input_type: Some(".svc.v1.PongReq".into()),
                output_type: Some(".svc.v1.PongResp".into()),
                ..Default::default()
            };
            let service = ServiceDescriptorProto {
                name: Some("Beta".into()),
                method: vec![method],
                ..Default::default()
            };
            FileDescriptorProto {
                name: Some("beta.proto".into()),
                package: Some("svc.v1".into()),
                service: vec![service],
                message_type: vec![
                    DescriptorProto {
                        name: Some("PongReq".into()),
                        ..Default::default()
                    },
                    DescriptorProto {
                        name: Some("PongResp".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        };

        let files = vec![file_a, file_b];
        let config = buffa_codegen::CodeGenConfig::default();
        let targets = vec!["alpha.proto".to_string(), "beta.proto".to_string()];
        let resolver = TypeResolver::new(&files, &targets, &config, false);

        let mut batch = BatchState {
            colliding_aliases: collect_alias_collisions(&files, &targets),
            ..BatchState::default()
        };
        let code_a = generate_connect_services(&files[0], &resolver, &mut batch).unwrap();
        let code_b = generate_connect_services(&files[1], &resolver, &mut batch).unwrap();

        let formatted_a = format_token_stream(&code_a).unwrap();
        let formatted_b = format_token_stream(&code_b).unwrap();

        // Each file independently must parse.
        syn::parse_str::<syn::File>(&formatted_a).expect("service A should parse independently");
        syn::parse_str::<syn::File>(&formatted_b).expect("service B should parse independently");

        // Both files combined into one module must also parse (the E0252 scenario).
        let combined = format!("{formatted_a}\n{formatted_b}");
        syn::parse_str::<syn::File>(&combined)
            .expect("combined services should parse without E0252");

        // No top-level `use` in either file.
        assert_no_top_level_use(&formatted_a, "service A");
        assert_no_top_level_use(&formatted_b, "service B");
    }

    /// `generate_spec_consts` emits one `pub const … : Spec` per method,
    /// named `{SERVICE}_{METHOD}_SPEC`, with the right `StreamType`,
    /// `IdempotencyLevel`, and procedure path.
    #[test]
    fn generate_spec_consts_per_method() {
        use buffa_codegen::generated::descriptor::MethodOptions;

        let m = |name: &str, cs: bool, ss: bool, idem: Option<IdempotencyLevel>| {
            MethodDescriptorProto {
                name: Some(name.into()),
                input_type: Some(".pkg.Req".into()),
                output_type: Some(".pkg.Resp".into()),
                client_streaming: Some(cs),
                server_streaming: Some(ss),
                options: MethodOptions {
                    idempotency_level: idem,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }
        };
        let service = ServiceDescriptorProto {
            name: Some("EchoService".into()),
            method: vec![
                m("Say", false, false, Some(IdempotencyLevel::NO_SIDE_EFFECTS)),
                m("Subscribe", false, true, Some(IdempotencyLevel::IDEMPOTENT)),
                m("Upload", true, false, None),
                m("Chat", true, true, None),
            ],
            ..Default::default()
        };

        // The const names follow `{SERVICE}_{METHOD}_SPEC`.
        assert_eq!(
            method_spec_const_ident(&service, "Say").to_string(),
            "ECHO_SERVICE_SAY_SPEC"
        );

        let consts = generate_spec_consts("pkg.EchoService", &service);
        assert_eq!(consts.len(), 4, "one const per method");

        let render = |ts: &TokenStream| {
            let file = syn::parse2::<syn::File>(ts.clone()).expect("const should parse");
            prettyplease::unparse(&file)
        };
        let say = render(&consts[0]);
        assert!(say.contains("pub const ECHO_SERVICE_SAY_SPEC"), "{say}");
        assert!(say.contains(r#""/pkg.EchoService/Say""#), "{say}");
        assert!(say.contains("StreamType::Unary"), "{say}");
        assert!(say.contains("IdempotencyLevel::NoSideEffects"), "{say}");

        let subscribe = render(&consts[1]);
        assert!(
            subscribe.contains("StreamType::ServerStream"),
            "{subscribe}"
        );
        assert!(
            subscribe.contains("IdempotencyLevel::Idempotent"),
            "{subscribe}"
        );

        let upload = render(&consts[2]);
        assert!(upload.contains("StreamType::ClientStream"), "{upload}");
        assert!(upload.contains("IdempotencyLevel::Unknown"), "{upload}");

        let chat = render(&consts[3]);
        assert!(chat.contains("StreamType::BidiStream"), "{chat}");
    }
}
