//! Build-time integration for connectrpc.
//!
//! Use this crate in `build.rs` to compile `.proto` files into Rust code at
//! build time. It shells out to `protoc` (or `buf`, or reads a precompiled
//! `FileDescriptorSet`) to obtain descriptors, then runs
//! [`connectrpc_codegen`] to emit buffa message types plus ConnectRPC
//! service traits and clients into `$OUT_DIR`.
//!
//! # Example
//!
//! ```rust,ignore
//! // build.rs
//! fn main() {
//!     connectrpc_build::Config::new()
//!         .files(&["proto/my_service.proto"])
//!         .includes(&["proto/"])
//!         .include_file("_connectrpc.rs")
//!         .compile()
//!         .unwrap();
//! }
//! ```
//!
//! ```rust,ignore
//! // lib.rs
//! connectrpc::include_generated!();
//! ```
//!
//! # Requirements
//!
//! Requires `protoc` on `PATH` (or set via `PROTOC`). To use `buf` instead,
//! call [`Config::use_buf`]. To avoid both, precompile a `FileDescriptorSet`
//! once and ship it alongside your source via [`Config::descriptor_set`].

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use buffa::Message;
use buffa_codegen::generated::descriptor::FileDescriptorSet;
use connectrpc_codegen::codegen::{self, Options};

pub use connectrpc_codegen::codegen::CodeGenConfig;

/// How to acquire a `FileDescriptorSet` from `.proto` files.
#[derive(Debug, Clone, Default)]
enum DescriptorSource {
    /// Invoke `protoc` (default). Requires `protoc` on PATH or `PROTOC` env var.
    #[default]
    Protoc,
    /// Invoke `buf build --as-file-descriptor-set`. Requires `buf` on PATH.
    Buf,
    /// Read a pre-built `FileDescriptorSet` from a file.
    Precompiled(PathBuf),
}

/// Builder for configuring and running connectrpc code generation.
///
/// See the [crate-level docs](crate) for a worked example.
pub struct Config {
    files: Vec<PathBuf>,
    includes: Vec<PathBuf>,
    out_dir: Option<PathBuf>,
    descriptor_source: DescriptorSource,
    include_file: Option<String>,
    emit_descriptor_set: Option<String>,
    emit_rerun_directives: bool,
    options: Options,
}

impl Config {
    /// Create a new configuration with defaults.
    pub fn new() -> Self {
        Self {
            files: Vec::new(),
            includes: Vec::new(),
            out_dir: None,
            descriptor_source: DescriptorSource::default(),
            include_file: None,
            emit_descriptor_set: None,
            emit_rerun_directives: true,
            options: Options::default(),
        }
    }

    /// Add `.proto` files to compile.
    #[must_use]
    pub fn files(mut self, files: &[impl AsRef<Path>]) -> Self {
        self.files
            .extend(files.iter().map(|f| f.as_ref().to_path_buf()));
        self
    }

    /// Add include directories for protoc to search for imports.
    ///
    /// Ignored when using [`Config::use_buf`] (buf resolves imports via
    /// `buf.yaml`).
    #[must_use]
    pub fn includes(mut self, includes: &[impl AsRef<Path>]) -> Self {
        self.includes
            .extend(includes.iter().map(|i| i.as_ref().to_path_buf()));
        self
    }

    /// Set the output directory. Defaults to `$OUT_DIR`.
    #[must_use]
    pub fn out_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.out_dir = Some(dir.into());
        self
    }

    /// Emit `cargo:rerun-if-changed=` directives to stdout (default: `true`).
    ///
    /// Set to `false` when running outside a Cargo `build.rs` context (e.g.
    /// from a Bazel genrule or a standalone host tool) where the directives
    /// are noise on stdout rather than instructions to a build system.
    #[must_use]
    pub fn emit_rerun_directives(mut self, enabled: bool) -> Self {
        self.emit_rerun_directives = enabled;
        self
    }

    /// Honor `features.utf8_validation = NONE` by emitting `Vec<u8>`/`&[u8]`
    /// for such string fields. See [`CodeGenConfig::strict_utf8_mapping`].
    #[must_use]
    pub fn strict_utf8_mapping(mut self, enabled: bool) -> Self {
        self.options.buffa.strict_utf8_mapping = enabled;
        self
    }

    /// Emit `serde` derives and proto3 JSON helpers (default: true).
    ///
    /// Disable only for binary-only clients; the Connect protocol's JSON
    /// codec requires this. See [`CodeGenConfig::generate_json`].
    #[must_use]
    pub fn generate_json(mut self, enabled: bool) -> Self {
        self.options.buffa.generate_json = enabled;
        self
    }

    /// Emit the per-file `register_types(&mut TypeRegistry)` aggregator
    /// (default: true).
    ///
    /// Set to `false` when the generated files are `include!`d into the
    /// same module — the identically-named functions would otherwise
    /// collide. See [`CodeGenConfig::emit_register_fn`].
    #[must_use]
    pub fn emit_register_fn(mut self, enabled: bool) -> Self {
        self.options.buffa.emit_register_fn = enabled;
        self
    }

    /// Emit one `<dotted.pkg>.rs` per proto package instead of the
    /// per-proto split + per-package stitcher (default: `false`).
    ///
    /// Under this layout the connect service stubs are inlined directly
    /// into buffa's single `<dotted.pkg>.rs` `PackageMod` per package — no
    /// `<stem>.__connect.rs` companion files, no per-proto buffa content
    /// files, and no `<pkg>.mod.rs` stitchers are written. Combine with
    /// [`Config::include_file`] as usual: the include file wires
    /// `PackageMod` entries by `file.name`, so the new filename
    /// (`<dotted.pkg>.rs` instead of `<pkg>.mod.rs`) is picked up
    /// transparently — your `lib.rs` still reads
    /// `connectrpc::include_generated!()` with no change. If you instead
    /// `include!` or `#[path = ...]`-mount per-proto files directly,
    /// migrate to the include file or to the per-package filenames first;
    /// the per-proto files no longer exist under this layout.
    ///
    /// Match this to the `file_per_package` buf plugin option when
    /// generating Buf Schema Registry cargo SDKs or any consumer that
    /// synthesises a module tree from `<dotted.package>.rs` filenames
    /// (`tonic`'s convention). See [`CodeGenConfig::file_per_package`].
    #[must_use]
    pub fn file_per_package(mut self, enabled: bool) -> Self {
        self.options.buffa.file_per_package = enabled;
        self
    }

    /// Prefix every generated `FooClient<T>` struct and its `impl` block
    /// with `#[cfg(feature = "client")]` (default: `false`).
    ///
    /// Opt in when you want a server-only build of your crate to drop
    /// the `connectrpc/client` transport stack from its dependency
    /// graph. The consumer crate then declares a `client` Cargo feature
    /// that forwards to `connectrpc/client`; see the `# Client-side cfg
    /// gate` section in [`connectrpc_codegen::codegen::generate`]'s
    /// docs for the minimal pattern. With the option off (the default),
    /// generated client items are unconditional — external consumers
    /// don't have to declare any Cargo feature.
    #[must_use]
    pub fn gate_client_feature(mut self, enabled: bool) -> Self {
        self.options.gate_client_feature = enabled;
        self
    }

    /// Replace the underlying buffa [`CodeGenConfig`] wholesale.
    ///
    /// Any buffa knob not surfaced as a builder method here can be set this
    /// way. The convenience builders above remain available for the common
    /// cases. `generate_views` is forced to `true` regardless (service
    /// stubs require view types); see [`Options::buffa`].
    ///
    /// Calls to the convenience builders above made *before* this method
    /// are discarded; calls made *after* override individual fields in the
    /// supplied config.
    #[must_use]
    pub fn buffa_config(mut self, config: CodeGenConfig) -> Self {
        self.options.buffa = config;
        self
    }

    /// Invoke `buf build` instead of `protoc`.
    ///
    /// Requires `buf` on PATH. Uses buf's dependency resolution (BSR modules)
    /// and `buf.yaml` configuration; [`Config::includes`] is ignored. When
    /// using buf, [`Config::files`] must contain proto-relative names as
    /// they appear in the buf module (e.g. `"my/service.proto"`), not
    /// filesystem paths.
    #[must_use]
    pub fn use_buf(mut self) -> Self {
        self.descriptor_source = DescriptorSource::Buf;
        self
    }

    /// Read a precompiled `FileDescriptorSet` from disk instead of invoking
    /// a compiler.
    ///
    /// Produce the file once with `protoc --descriptor_set_out=... --include_imports`
    /// or `buf build --as-file-descriptor-set -o ...`, then ship it with
    /// your source.
    ///
    /// [`Config::files`] selects which files in the set to generate code for.
    /// **These must be the proto-relative names as they appear in the
    /// descriptor set** (e.g. `"my/service.proto"`), not filesystem paths.
    /// See the `.proto` file's `name` field in the descriptor, which protoc
    /// sets to the path relative to `--proto_path`.
    #[must_use]
    pub fn descriptor_set(mut self, path: impl Into<PathBuf>) -> Self {
        self.descriptor_source = DescriptorSource::Precompiled(path.into());
        self
    }

    /// Also write the input `FileDescriptorSet` (the full set handed to
    /// codegen, not just the files selected for generation) to
    /// `<out_dir>/<name>` as wire-format bytes. `name` must be a bare file
    /// name — no path separators.
    ///
    /// The set carries the full transitive import closure for every descriptor
    /// source (`protoc --include_imports`, `buf --as-file-descriptor-set`, or a
    /// precompiled set), so it is ready to back `grpc.reflection.v1.ServerReflection`
    /// for clients such as `grpcurl`. Pair it with `include_bytes!`:
    ///
    /// ```ignore
    /// // build.rs
    /// connectrpc_build::Config::new()
    ///     .files(&["proto/svc.proto"])
    ///     .includes(&["proto/"])
    ///     .emit_descriptor_set("svc_descriptor.bin")
    ///     .compile()?;
    /// // src/lib.rs
    /// pub const FILE_DESCRIPTOR_SET: &[u8] =
    ///     include_bytes!(concat!(env!("OUT_DIR"), "/svc_descriptor.bin"));
    /// ```
    ///
    /// The inverse of [`Config::descriptor_set`], which *reads* a precompiled
    /// set; this *writes* the one connectrpc-build already computed, so build
    /// scripts no longer need a second `protoc --descriptor_set_out` pass.
    #[must_use]
    pub fn emit_descriptor_set(mut self, name: impl Into<String>) -> Self {
        self.emit_descriptor_set = Some(name.into());
        self
    }

    /// Emit an `include!`-based module tree file alongside the per-file
    /// `.rs` outputs.
    ///
    /// The file contains nested `pub mod` blocks matching the proto package
    /// hierarchy, each `include!`-ing the relevant generated file. Include
    /// it from your crate root:
    ///
    /// ```rust,ignore
    /// connectrpc::include_generated!();
    /// ```
    #[must_use]
    pub fn include_file(mut self, name: impl Into<String>) -> Self {
        self.include_file = Some(name.into());
        self
    }

    /// Run code generation and write output files.
    ///
    /// # Errors
    ///
    /// - `$OUT_DIR` is unset and no `out_dir` was configured
    /// - `protoc` or `buf` is not on `PATH` (when using those sources)
    /// - the compiler exits non-zero (syntax error, missing import, ...)
    /// - a precompiled descriptor set cannot be read or decoded
    /// - codegen fails (unsupported proto feature)
    /// - the output directory cannot be created or written to
    /// - [`Config::emit_descriptor_set`] was given a name containing path
    ///   separators, or the descriptor set cannot be written
    pub fn compile(self) -> Result<()> {
        // When out_dir() is explicitly set, emit sibling-relative include!
        // paths — the include file lives next to the generated files and
        // is referenced as a module. When defaulted from $OUT_DIR, emit
        // the env!("OUT_DIR") form for the build.rs/include! workflow.
        let relative_includes = self.out_dir.is_some();
        let out_dir = match self.out_dir {
            Some(d) => d,
            None => std::env::var_os("OUT_DIR")
                .map(PathBuf::from)
                .context("OUT_DIR is not set and no out_dir() was configured")?,
        };

        // 1. Acquire descriptor bytes and resolve files_to_generate.
        //
        // `FileDescriptorProto.name` is the path relative to the include
        // directory, not the filesystem path. For the Protoc mode we strip
        // the longest matching include prefix to recover this name. For
        // Buf and Precompiled modes, the user must already provide
        // proto-relative names in .files() (see docs on use_buf() and
        // descriptor_set()) so we pass them through as-is.
        let (descriptor_bytes, files_to_generate) = match &self.descriptor_source {
            DescriptorSource::Protoc => {
                let bytes = run_protoc(&self.files, &self.includes)?;
                // Sort includes longest-first so nested prefixes like
                // ["proto/", "proto/vendor/"] strip the most specific one.
                let mut includes = self.includes.clone();
                includes.sort_by_key(|p| std::cmp::Reverse(p.as_os_str().len()));
                let files = self
                    .files
                    .iter()
                    .map(|f| strip_include_prefix(f, &includes))
                    .filter(|s| !s.is_empty())
                    .collect();
                (bytes, files)
            }
            DescriptorSource::Buf => {
                let bytes = run_buf(&self.files)?;
                (bytes, proto_relative_names(&self.files))
            }
            DescriptorSource::Precompiled(p) => {
                let bytes = std::fs::read(p)
                    .with_context(|| format!("failed to read descriptor set '{}'", p.display()))?;
                (bytes, proto_relative_names(&self.files))
            }
        };
        let fds = FileDescriptorSet::decode_from_slice(&descriptor_bytes)
            .map_err(|e| anyhow!("failed to decode FileDescriptorSet: {e}"))?;

        // 3. Generate.
        let generated = codegen::generate_files(&fds.file, &files_to_generate, &self.options)?;

        // 4. Write per-file outputs and collect (name, package) pairs for
        //    PackageMod files only — the per-package stitcher `include!`s
        //    the five content files itself, so the module tree only wires
        //    stitchers.
        std::fs::create_dir_all(&out_dir)
            .with_context(|| format!("failed to create out_dir '{}'", out_dir.display()))?;

        // Emit the parsed descriptor set for gRPC server reflection, if requested.
        // `descriptor_bytes` already carries the full import closure for every
        // descriptor source, so the written set is reflection-ready as-is.
        if let Some(name) = &self.emit_descriptor_set {
            // `<out_dir>/<name>` is the documented contract; a separator or
            // absolute path would silently escape it via `Path::join`.
            if Path::new(name).components().count() != 1 || Path::new(name).is_absolute() {
                bail!(
                    "emit_descriptor_set name must be a bare file name \
                     (no path separators), got {name:?}"
                );
            }
            let target = out_dir.join(name);
            write_if_changed(&target, &descriptor_bytes)
                .with_context(|| format!("failed to write descriptor set {}", target.display()))?;
        }

        let mut entries: Vec<(String, String)> = Vec::new();
        for file in &generated {
            let path = out_dir.join(&file.name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            write_if_changed(&path, file.content.as_bytes())?;
            if file.kind == codegen::GeneratedFileKind::PackageMod {
                entries.push((file.name.clone(), file.package.clone()));
            }
        }

        // 5. Optionally emit the module-tree include file.
        if let Some(ref include_name) = self.include_file {
            let include_src = generate_include_file(&entries, relative_includes);
            let include_path = out_dir.join(include_name);
            write_if_changed(&include_path, include_src.as_bytes())?;
        }

        // 6. Cargo re-run triggers. Skipped entirely for non-Cargo callers.
        // In Precompiled mode `self.files` holds proto-relative names (per
        // the docs on `descriptor_set()`), not on-disk paths; emitting
        // `rerun-if-changed` for them points cargo at missing files and
        // forces a rebuild on every invocation. The `.pb` path is the only
        // real input in that mode.
        if !self.emit_rerun_directives {
            return Ok(());
        }
        match &self.descriptor_source {
            DescriptorSource::Precompiled(p) => {
                println!("cargo:rerun-if-changed={}", p.display());
            }
            // Both Buf and Precompiled modes use proto-relative names (not
            // filesystem paths) in `.files()`. Emitting `rerun-if-changed`
            // for those would point cargo at non-existent files and force a
            // rebuild every invocation.
            DescriptorSource::Buf => {}
            DescriptorSource::Protoc => {
                for f in &self.files {
                    println!("cargo:rerun-if-changed={}", f.display());
                }
            }
        }

        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

/// Write `content` to `path` only if the file doesn't already exist with
/// identical content. Cargo's rebuild decision for `include!`-ed files is
/// mtime-based, so an unconditional write here would cascade into
/// recompiling every downstream crate whenever any `.proto` is touched.
fn write_if_changed(path: &Path, content: &[u8]) -> std::io::Result<()> {
    if let Ok(existing) = std::fs::read(path)
        && existing == content
    {
        return Ok(());
    }
    std::fs::write(path, content)
}

/// Run `protoc` and return the serialized `FileDescriptorSet`.
fn run_protoc(files: &[PathBuf], includes: &[PathBuf]) -> Result<Vec<u8>> {
    let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());

    let out = tempfile::NamedTempFile::new().context("failed to create tempfile for protoc")?;
    let out_path = out.path().to_path_buf();

    let mut cmd = Command::new(&protoc);
    cmd.arg("--include_imports");
    cmd.arg(format!("--descriptor_set_out={}", out_path.display()));
    for inc in includes {
        cmd.arg(format!("--proto_path={}", inc.display()));
    }
    for f in files {
        cmd.arg(f.as_os_str());
    }

    let output = cmd
        .output()
        .with_context(|| format!("failed to spawn protoc ('{protoc}')"))?;
    if !output.status.success() {
        bail!("protoc failed: {}", String::from_utf8_lossy(&output.stderr));
    }

    std::fs::read(&out_path).context("failed to read protoc descriptor output")
}

/// Run `buf build --as-file-descriptor-set` and return the serialized bytes.
///
/// Includes are intentionally NOT passed: buf's `--path` flag is a file
/// filter, not an import path like protoc's `--proto_path`. Passing include
/// directories as `--path` would restrict the output in unintended ways.
/// buf resolves imports via `buf.yaml`.
fn run_buf(files: &[PathBuf]) -> Result<Vec<u8>> {
    let out = tempfile::NamedTempFile::new().context("failed to create tempfile for buf")?;
    let out_path = out.path().to_path_buf();

    let mut cmd = Command::new("buf");
    cmd.arg("build")
        .arg("--as-file-descriptor-set")
        .arg("-o")
        .arg(&out_path);
    for f in files {
        cmd.arg("--path").arg(f.as_os_str());
    }

    let output = cmd.output().context("failed to spawn buf")?;
    if !output.status.success() {
        bail!(
            "buf build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    std::fs::read(&out_path).context("failed to read buf descriptor output")
}

/// Strip the longest matching include prefix from a filesystem path to
/// recover the proto-relative name protoc stores in `FileDescriptorProto.name`.
///
/// Falls back to the bare file name if no prefix matches. Callers must
/// pre-sort `includes` longest-first so nested include directories are
/// matched correctly.
fn strip_include_prefix(f: &Path, includes: &[PathBuf]) -> String {
    for inc in includes {
        if let Ok(rel) = f.strip_prefix(inc)
            && let Some(s) = rel.to_str()
        {
            return s.to_string();
        }
    }
    f.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .to_string()
}

/// Convert `.files()` paths to strings verbatim for Buf/Precompiled modes
/// where the user supplies proto-relative names directly (no include prefix
/// to strip).
fn proto_relative_names(files: &[PathBuf]) -> Vec<String> {
    files
        .iter()
        .filter_map(|f| f.to_str().map(str::to_string))
        .filter(|s| !s.is_empty())
        .collect()
}

/// Build an `include!`-based module-tree file.
///
/// Given `[("my.pkg.thing.rs", "my.pkg"), ...]`, produce nested
/// `pub mod my { pub mod pkg { include!(...); } }`.
///
/// When `relative` is false (the `$OUT_DIR` workflow), emit
/// `include!(concat!(env!("OUT_DIR"), "/my.pkg.thing.rs"))`.
/// When `relative` is true (explicit `out_dir()`), the include file and the
/// generated files are siblings, so emit `include!("my.pkg.thing.rs")` —
/// `include!` resolves relative to the including file.
fn generate_include_file(entries: &[(String, String)], relative: bool) -> String {
    use std::collections::BTreeMap;
    use std::fmt::Write as _;

    #[derive(Default)]
    struct Node {
        files: Vec<String>,
        children: BTreeMap<String, Node>,
    }

    let mut root = Node::default();
    for (file_name, package) in entries {
        let mut node = &mut root;
        if !package.is_empty() {
            for seg in package.split('.') {
                node = node.children.entry(seg.to_string()).or_default();
            }
        }
        node.files.push(file_name.clone());
    }

    fn emit(out: &mut String, node: &Node, depth: usize, relative: bool) {
        let indent = "    ".repeat(depth);
        for f in &node.files {
            if relative {
                writeln!(out, r#"{indent}include!("{f}");"#).unwrap();
            } else {
                writeln!(
                    out,
                    r#"{indent}include!(concat!(env!("OUT_DIR"), "/{f}"));"#
                )
                .unwrap();
            }
        }
        for (name, child) in &node.children {
            let ident = buffa_codegen::idents::escape_mod_ident(name);
            // The `pub mod <pkg>` tree wraps buffa's per-proto split
            // output (Owned/View/Oneof/Ext + the PackageMod stitcher)
            // plus our own `__connect.rs` companions. The per-proto
            // content files have no `#[allow(...)]` of their own —
            // buffa's `package_mod_allow_attr()` is scoped to `__buffa`
            // and `protoc-gen-buffa-packaging` covers the rest with an
            // inner `#![allow(...)]` that doesn't apply here — so the
            // suppression set must be the union of
            // `buffa_codegen::ALLOW_LINTS` and the lints connect-rust
            // output trips. Sourcing from `ALLOW_LINTS` keeps the two
            // in lockstep when buffa adds entries.
            //
            // `impl_trait_redundant_captures`: the `use<'a, Self>` precise-
            // capturing clause on trait method RPITs is required for
            // edition-2021 consumers (which capture only `'static` by
            // default) but redundant under edition 2024. Codegen targets
            // both editions and cannot know the consumer's at write time.
            let allow_lints = buffa_codegen::ALLOW_LINTS
                .iter()
                .copied()
                .chain(["impl_trait_redundant_captures"])
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(out, "{indent}#[allow({allow_lints})]").unwrap();
            writeln!(out, "{indent}pub mod {ident} {{").unwrap();
            writeln!(out, "{indent}    use super::*;").unwrap();
            emit(out, child, depth + 1, relative);
            writeln!(out, "{indent}}}").unwrap();
        }
    }

    let mut out = String::new();
    writeln!(out, "// @generated by connectrpc-build. DO NOT EDIT.").unwrap();
    writeln!(out).unwrap();
    emit(&mut out, &root, 0, relative);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn include_file_nests_packages() {
        let entries = vec![
            ("my.pkg.svc.rs".into(), "my.pkg".into()),
            ("my.other.rs".into(), "my".into()),
            ("root.rs".into(), String::new()),
        ];
        let out = generate_include_file(&entries, false);

        assert!(
            out.contains("// @generated by connectrpc-build"),
            "missing header: {out}"
        );
        // Root-level file has no wrapper.
        assert!(
            out.contains(r#"include!(concat!(env!("OUT_DIR"), "/root.rs"));"#),
            "missing root include: {out}"
        );
        // my.pkg.svc.rs is nested two levels deep.
        assert!(out.contains("pub mod my {"), "missing mod my: {out}");
        assert!(out.contains("pub mod pkg {"), "missing mod pkg: {out}");
        assert!(
            out.contains(r#"include!(concat!(env!("OUT_DIR"), "/my.pkg.svc.rs"));"#),
            "missing nested include: {out}"
        );
        // my.other.rs is one level deep (under mod my).
        assert!(
            out.contains(r#"include!(concat!(env!("OUT_DIR"), "/my.other.rs"));"#),
            "missing my.other include: {out}"
        );
    }

    #[test]
    fn include_file_relative_mode() {
        let entries = vec![
            ("my.pkg.svc.rs".into(), "my.pkg".into()),
            ("root.rs".into(), String::new()),
        ];
        let out = generate_include_file(&entries, true);

        // Relative mode uses bare sibling paths, no env!/concat!.
        assert!(
            out.contains(r#"include!("root.rs");"#),
            "missing relative root include: {out}"
        );
        assert!(
            out.contains(r#"include!("my.pkg.svc.rs");"#),
            "missing relative nested include: {out}"
        );
        assert!(
            !out.contains("env!"),
            "relative mode should not emit env!: {out}"
        );
        assert!(
            !out.contains("concat!"),
            "relative mode should not emit concat!: {out}"
        );
        // Module tree is the same regardless of include form.
        assert!(out.contains("pub mod my {"), "missing mod my: {out}");
        assert!(out.contains("pub mod pkg {"), "missing mod pkg: {out}");
    }

    #[test]
    fn include_file_escapes_keywords() {
        let entries = vec![("type.match.svc.rs".into(), "type.match".into())];
        let out = generate_include_file(&entries, false);
        assert!(out.contains("pub mod r#type {"), "expected r#type: {out}");
        assert!(out.contains("pub mod r#match {"), "expected r#match: {out}");
    }

    #[test]
    fn config_builder_chain() {
        let cfg = Config::new()
            .files(&["a.proto", "b.proto"])
            .includes(&["proto/"])
            .strict_utf8_mapping(true)
            .generate_json(false)
            .emit_register_fn(false)
            .gate_client_feature(true)
            .include_file("_inc.rs");
        assert_eq!(cfg.files.len(), 2);
        assert_eq!(cfg.includes.len(), 1);
        assert!(cfg.options.buffa.strict_utf8_mapping);
        assert!(!cfg.options.buffa.generate_json);
        assert!(!cfg.options.buffa.emit_register_fn);
        assert!(cfg.options.gate_client_feature);
        assert_eq!(cfg.include_file.as_deref(), Some("_inc.rs"));
    }

    #[test]
    fn config_default_options() {
        let cfg = Config::new();
        assert!(!cfg.options.buffa.strict_utf8_mapping);
        assert!(cfg.options.buffa.generate_json);
        assert!(cfg.options.buffa.emit_register_fn);
        // `gate_client_feature` defaults off — build.rs consumers don't
        // have to declare a `client` Cargo feature unless they opt in.
        assert!(!cfg.options.gate_client_feature);
        assert!(cfg.emit_rerun_directives);
        assert!(matches!(cfg.descriptor_source, DescriptorSource::Protoc));
    }

    /// End-to-end through `Config`: with `gate_client_feature(true)`,
    /// the generated `__connect.rs` contains `#[cfg(feature = "client")]`
    /// on the `EchoServiceClient` struct + impl. Without the opt-in, the
    /// cfg attr is absent. Uses the same `echo.fds.bin` fixture as
    /// [`compile_precompiled_descriptor_set`].
    #[test]
    fn compile_gate_client_feature_emits_cfg_attr() {
        let fixture = format!("{}/tests/fixtures/echo.fds.bin", env!("CARGO_MANIFEST_DIR"));

        // Opt-in: cfg attrs present on the client items.
        let out_with = tempfile::tempdir().unwrap();
        Config::new()
            .descriptor_set(&fixture)
            .files(&["echo.proto"])
            .out_dir(out_with.path())
            .gate_client_feature(true)
            .emit_rerun_directives(false)
            .compile()
            .expect("compile with gate_client_feature=true");
        let gated = std::fs::read_to_string(out_with.path().join("echo.__connect.rs"))
            .expect("read gated __connect.rs");
        let cfg_count = gated.matches("#[cfg(feature = \"client\")]").count();
        assert_eq!(
            cfg_count, 2,
            "expected exactly 2 cfg attrs (struct + impl) with \
             gate_client_feature=true; got {cfg_count}:\n{gated}"
        );
        // Sanity: the server-side trait + ext trait must not be gated.
        for marker in ["pub trait EchoService", "pub trait EchoServiceExt"] {
            let idx = gated
                .find(marker)
                .unwrap_or_else(|| panic!("expected `{marker}` in output:\n{gated}"));
            let prefix = &gated[..idx];
            assert!(
                !prefix.trim_end().ends_with("#[cfg(feature = \"client\")]"),
                "`{marker}` must not be gated:\n{gated}"
            );
        }

        // Opt-out (default): no cfg attrs anywhere in the same file.
        let out_without = tempfile::tempdir().unwrap();
        Config::new()
            .descriptor_set(&fixture)
            .files(&["echo.proto"])
            .out_dir(out_without.path())
            .emit_rerun_directives(false)
            .compile()
            .expect("compile with default options");
        let ungated = std::fs::read_to_string(out_without.path().join("echo.__connect.rs"))
            .expect("read default __connect.rs");
        assert!(
            !ungated.contains("#[cfg(feature ="),
            "default emission must not emit any cfg attr — external \
             consumers should not need to declare a `client` Cargo \
             feature unless they opt in. Got:\n{ungated}"
        );
    }

    #[test]
    fn config_emit_rerun_directives_toggle() {
        let cfg = Config::new().emit_rerun_directives(false);
        assert!(!cfg.emit_rerun_directives);
    }

    #[test]
    fn config_buffa_config_wholesale() {
        let mut buffa = CodeGenConfig::default();
        buffa.generate_text = true;
        let cfg = Config::new().buffa_config(buffa);
        assert!(cfg.options.buffa.generate_text);
    }

    #[test]
    fn config_descriptor_source_variants() {
        assert!(matches!(
            Config::new().use_buf().descriptor_source,
            DescriptorSource::Buf
        ));
        assert!(matches!(
            Config::new().descriptor_set("x.bin").descriptor_source,
            DescriptorSource::Precompiled(_)
        ));
    }

    #[test]
    fn config_emit_descriptor_set_toggle() {
        let cfg = Config::new().emit_descriptor_set("d.bin");
        assert_eq!(cfg.emit_descriptor_set.as_deref(), Some("d.bin"));
    }

    /// `emit_descriptor_set` writes the descriptor set used for codegen to
    /// `<out_dir>/<name>` as a wire-format `FileDescriptorSet` ready for gRPC
    /// server reflection. A precompiled source passes the bytes through
    /// unchanged, so the emitted file round-trips the input set.
    #[test]
    fn emit_descriptor_set_writes_reflection_bin() {
        let fixture = format!("{}/tests/fixtures/echo.fds.bin", env!("CARGO_MANIFEST_DIR"));
        let out = tempfile::tempdir().unwrap();

        Config::new()
            .descriptor_set(&fixture)
            .files(&["echo.proto"])
            .out_dir(out.path())
            .emit_descriptor_set("echo_descriptor.bin")
            .compile()
            .unwrap();

        let emitted = out.path().join("echo_descriptor.bin");
        assert!(emitted.exists(), "expected {emitted:?} to be written");

        let bytes = std::fs::read(&emitted).unwrap();
        let fds = FileDescriptorSet::decode_from_slice(&bytes)
            .expect("emitted descriptor set must decode");
        let names: Vec<_> = fds.file.iter().filter_map(|f| f.name.as_deref()).collect();
        assert_eq!(
            names,
            ["echo.proto"],
            "emitted set should contain the compiled file by name"
        );

        // Precompiled source passes bytes through unchanged → exact round-trip.
        let fixture_bytes = std::fs::read(&fixture).unwrap();
        assert_eq!(
            bytes, fixture_bytes,
            "emitted bytes must equal the source set"
        );
    }

    /// The emitted set carries the full transitive import closure, not just
    /// the files selected for generation: `imports.fds.bin` was built with
    /// `protoc --include_imports` from `uses_dep.proto` (which imports
    /// `dep.proto`), and both must appear in the emitted bytes — that is
    /// what makes the file servable via `grpc.reflection.v1.ServerReflection`.
    #[test]
    fn emit_descriptor_set_preserves_import_closure() {
        let fixture = format!(
            "{}/tests/fixtures/imports.fds.bin",
            env!("CARGO_MANIFEST_DIR")
        );
        let out = tempfile::tempdir().unwrap();

        Config::new()
            .descriptor_set(&fixture)
            .files(&["uses_dep.proto"])
            .out_dir(out.path())
            .emit_descriptor_set("fixture_descriptor.bin")
            .compile()
            .unwrap();

        let bytes = std::fs::read(out.path().join("fixture_descriptor.bin")).unwrap();
        let fds = FileDescriptorSet::decode_from_slice(&bytes)
            .expect("emitted descriptor set must decode");
        let names: Vec<_> = fds.file.iter().filter_map(|f| f.name.as_deref()).collect();
        assert!(
            names.contains(&"dep.proto") && names.contains(&"uses_dep.proto"),
            "emitted set must include the imported dependency, got {names:?}"
        );
    }

    /// `emit_descriptor_set` promises `<out_dir>/<name>`; a name with path
    /// separators (or an absolute path) would escape it via `Path::join`,
    /// so it is rejected.
    #[test]
    fn emit_descriptor_set_rejects_path_separators() {
        let fixture = format!("{}/tests/fixtures/echo.fds.bin", env!("CARGO_MANIFEST_DIR"));
        for name in ["sub/d.bin", "../d.bin", "/tmp/d.bin"] {
            let out = tempfile::tempdir().unwrap();
            let err = Config::new()
                .descriptor_set(&fixture)
                .files(&["echo.proto"])
                .out_dir(out.path())
                .emit_descriptor_set(name)
                .compile()
                .unwrap_err();
            assert!(
                err.to_string().contains("bare file name"),
                "expected bare-file-name error for {name:?}, got: {err}"
            );
        }
    }

    /// End-to-end: precompiled descriptor set → generated Rust in a tempdir.
    /// Verifies the file layout and that the service binding imports use
    /// `::connectrpc::` (absolute path).
    #[test]
    fn compile_precompiled_descriptor_set() {
        let fixture = format!("{}/tests/fixtures/echo.fds.bin", env!("CARGO_MANIFEST_DIR"));
        let out = tempfile::tempdir().unwrap();

        Config::new()
            .descriptor_set(&fixture)
            .files(&["echo.proto"])
            .out_dir(out.path())
            .include_file("_inc.rs")
            .compile()
            .unwrap();

        // Per-file output: buffa message types in `echo.rs`, connect-rust
        // service code in the `echo.__connect.rs` companion file.
        let echo_rs = out.path().join("echo.rs");
        assert!(echo_rs.exists(), "expected {echo_rs:?} to exist");
        let msg_content = std::fs::read_to_string(&echo_rs).unwrap();
        assert!(msg_content.contains("pub struct EchoRequest"));
        assert!(msg_content.contains("pub struct EchoResponse"));

        let connect_rs = out.path().join("echo.__connect.rs");
        assert!(connect_rs.exists(), "expected {connect_rs:?} to exist");
        let svc_content = std::fs::read_to_string(&connect_rs).unwrap();
        assert!(svc_content.contains("pub trait EchoService"));
        assert!(svc_content.contains("pub struct EchoServiceClient"));
        // Fully qualified paths (the module-collision fix): no top-level `use`
        // statements, all references are inline absolute paths like
        // `::connectrpc::Context`, `::std::sync::Arc`, etc.
        assert!(
            svc_content.contains("::connectrpc::"),
            "service code should use ::connectrpc:: fully qualified paths"
        );
        assert!(
            !svc_content.contains("\nuse "),
            "service code should not emit top-level use statements"
        );

        // Include file nests under test.echo.v1 and wires only the
        // per-package stitcher (the stitcher itself include!s the
        // per-proto content files). Because out_dir() was set explicitly
        // (not defaulted from $OUT_DIR), includes are sibling-relative.
        let inc = std::fs::read_to_string(out.path().join("_inc.rs")).unwrap();
        assert!(inc.contains("pub mod test {"));
        assert!(inc.contains("pub mod echo {"));
        assert!(inc.contains("pub mod v1 {"));
        assert!(inc.contains(r#"include!("test.echo.v1.mod.rs");"#));
        // Stitcher pulls in buffa's content files plus the connect-rust
        // companion (wired via `apply_companions`).
        let stitcher = std::fs::read_to_string(out.path().join("test.echo.v1.mod.rs")).unwrap();
        assert!(stitcher.contains(r#"include!("echo.rs");"#));
        assert!(
            stitcher.contains(r#"include!("echo.__connect.rs");"#),
            "stitcher should include the connect companion file (requires apply_companions, buffa >= 0.5)"
        );
        assert!(stitcher.contains("pub mod __buffa"));
    }

    #[test]
    fn compile_file_per_package_collapses_to_single_file() {
        let fixture = format!("{}/tests/fixtures/echo.fds.bin", env!("CARGO_MANIFEST_DIR"));
        let out = tempfile::tempdir().unwrap();

        Config::new()
            .descriptor_set(&fixture)
            .files(&["echo.proto"])
            .out_dir(out.path())
            .include_file("_inc.rs")
            .file_per_package(true)
            .compile()
            .unwrap();

        // No per-proto split, no companion siblings — everything lands in
        // the single per-package `<dotted.pkg>.rs` PackageMod.
        for stale in [
            "echo.rs",
            "echo.__connect.rs",
            "echo.__view.rs",
            "test.echo.v1.mod.rs",
        ] {
            assert!(
                !out.path().join(stale).exists(),
                "file_per_package must not emit {stale}"
            );
        }
        let pkg_rs = out.path().join("test.echo.v1.rs");
        assert!(pkg_rs.exists(), "expected {pkg_rs:?}");
        let content = std::fs::read_to_string(&pkg_rs).unwrap();
        assert!(
            content.contains("pub struct EchoRequest"),
            "missing message types"
        );
        assert!(
            content.contains("pub trait EchoService"),
            "missing service trait"
        );
        assert!(
            content.contains("pub struct EchoServiceClient"),
            "missing service client"
        );
        assert!(
            !content.contains("__connect.rs"),
            "single-file output must not include! a sibling: {content}"
        );

        // Include file wires the per-package PackageMod as before — the
        // `<dotted.pkg>.rs` filename replaces `<pkg>.mod.rs` and the
        // nested-mod wrapping (which `<dotted.pkg>.rs` doesn't carry
        // itself) is still synthesised here.
        let inc = std::fs::read_to_string(out.path().join("_inc.rs")).unwrap();
        assert!(inc.contains(r#"include!("test.echo.v1.rs");"#));
        assert_eq!(
            inc.matches("include!").count(),
            1,
            "include file must wire exactly one PackageMod: {inc}"
        );
        for m in ["pub mod test {", "pub mod echo {", "pub mod v1 {"] {
            assert!(
                inc.contains(m),
                "include file missing nested mod {m:?}: {inc}"
            );
        }
    }

    #[test]
    fn compile_rejects_unknown_file_names() {
        let fixture = format!("{}/tests/fixtures/echo.fds.bin", env!("CARGO_MANIFEST_DIR"));
        let out = tempfile::tempdir().unwrap();

        let err = Config::new()
            .descriptor_set(&fixture)
            .files(&["nonexistent.proto"])
            .out_dir(out.path())
            .compile()
            .unwrap_err();

        // buffa-codegen rejects unknown file_to_generate entries; verify that
        // error surfaces through compile() with the offending name.
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent.proto"),
            "error should name the missing file: {msg}"
        );
    }

    /// descriptor_set() with nested proto paths must preserve directory
    /// components — users provide proto-relative names like
    /// "my/pkg/ping.proto" which match what's inside the FDS. Stripping
    /// to file_name() would fail to find the descriptor.
    #[test]
    fn compile_precompiled_preserves_nested_paths() {
        let fixture = format!(
            "{}/tests/fixtures/nested.fds.bin",
            env!("CARGO_MANIFEST_DIR")
        );
        let out = tempfile::tempdir().unwrap();

        Config::new()
            .descriptor_set(&fixture)
            // nested path with directory components — before the fix,
            // this was stripped to "ping.proto" and failed to match
            .files(&["my/pkg/ping.proto"])
            .out_dir(out.path())
            .include_file("_inc.rs")
            .compile()
            .unwrap();

        // Output filenames derived from proto path with dots: buffa
        // message types in `<stem>.rs`, connect-rust service code in the
        // `<stem>.__connect.rs` companion file.
        let msg_rs = out.path().join("my.pkg.ping.rs");
        assert!(msg_rs.exists(), "expected {msg_rs:?}");
        assert!(
            std::fs::read_to_string(&msg_rs)
                .unwrap()
                .contains("pub struct PingRequest")
        );
        let svc_rs = out.path().join("my.pkg.ping.__connect.rs");
        assert!(svc_rs.exists(), "expected {svc_rs:?}");
        assert!(
            std::fs::read_to_string(&svc_rs)
                .unwrap()
                .contains("pub trait PingService")
        );

        // The dotted-stem stitcher must wire in the companion file by name;
        // this exercises `apply_companions` filename escaping for stems that
        // already contain `.` separators.
        let stitcher = std::fs::read_to_string(out.path().join("my.pkg.v1.mod.rs")).unwrap();
        assert!(
            stitcher.contains(r#"include!("my.pkg.ping.__connect.rs");"#),
            "stitcher should include the connect companion file (requires apply_companions, buffa >= 0.5)"
        );

        // Include file nests under my.pkg.v1.
        let inc = std::fs::read_to_string(out.path().join("_inc.rs")).unwrap();
        assert!(inc.contains("pub mod my {"));
        assert!(inc.contains("pub mod pkg {"));
        assert!(inc.contains("pub mod v1 {"));
    }

    #[test]
    fn strip_include_prefix_longest_first() {
        // With overlapping includes, the longest must win.
        let includes = vec![PathBuf::from("proto/vendor/"), PathBuf::from("proto/")];
        // Caller contract: sorted longest-first.
        let mut sorted = includes.clone();
        sorted.sort_by_key(|p| std::cmp::Reverse(p.as_os_str().len()));
        assert_eq!(sorted[0], PathBuf::from("proto/vendor/"));

        let f = PathBuf::from("proto/vendor/thing.proto");
        assert_eq!(strip_include_prefix(&f, &sorted), "thing.proto");

        let f = PathBuf::from("proto/my/svc.proto");
        assert_eq!(strip_include_prefix(&f, &sorted), "my/svc.proto");
    }

    #[test]
    fn strip_include_prefix_fallback_to_filename() {
        let f = PathBuf::from("unrelated/path/svc.proto");
        let includes = vec![PathBuf::from("proto/")];
        assert_eq!(strip_include_prefix(&f, &includes), "svc.proto");
    }

    #[test]
    fn proto_relative_names_verbatim() {
        let files = vec![
            PathBuf::from("my/pkg/svc.proto"),
            PathBuf::from("top.proto"),
        ];
        assert_eq!(
            proto_relative_names(&files),
            vec!["my/pkg/svc.proto".to_string(), "top.proto".to_string()]
        );
    }

    #[test]
    fn write_if_changed_creates_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new.rs");
        write_if_changed(&path, b"hello").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"hello");
    }

    #[test]
    fn write_if_changed_skips_identical_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("same.rs");
        std::fs::write(&path, b"content").unwrap();
        let mtime_before = std::fs::metadata(&path).unwrap().modified().unwrap();

        // Sleep briefly so a write would produce a distinguishable mtime.
        std::thread::sleep(std::time::Duration::from_millis(50));

        write_if_changed(&path, b"content").unwrap();
        let mtime_after = std::fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(mtime_before, mtime_after);
    }

    #[test]
    fn write_if_changed_overwrites_different_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("changed.rs");
        std::fs::write(&path, b"old").unwrap();

        write_if_changed(&path, b"new").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
    }
}
