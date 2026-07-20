# Contributing to connect-rust

See [`docs/guide.md`](docs/guide.md) for the user-facing handler/client API
and the [README](README.md) for the workspace layout. The current project
maintainers are listed in [MAINTAINERS.md](MAINTAINERS.md).

## Developer Certificate of Origin

All commits must be signed off to affirm compliance with the
[Developer Certificate of Origin](https://developercertificate.org/).
No Contributor License Agreement is required. Configure your git identity
to match your GitHub account, then use the `-s` flag when committing:

```console
$ git commit -s -m "your commit message"
```

## Prerequisites

- **Rust 1.88+** (MSRV; source of truth is `rust-version` in the workspace
  `Cargo.toml` — the codebase uses let-chains).
- **`protoc` v27+** — the test and example protos use editions syntax
  (`edition = "2023"`). Ubuntu's apt `protobuf-compiler` (v21) is too old;
  install from the [protobuf releases] page or via `arduino/setup-protoc`
  in CI. `task generate:all` and the `connectrpc-build` consumers need it.
- **`buf`** — used to drive regeneration of the checked-in code (the
  `buf.gen.yaml` files invoke locally-built plugins).
- **`task`** ([go-task]) — the repo's command runner. `task --list` shows
  everything.

[protobuf releases]: https://github.com/protocolbuffers/protobuf/releases
[go-task]: https://taskfile.dev

## Change Size

Keep each change to **≤ 250 lines net** (additions minus deletions,
excluding test files and `*/generated/*`) wherever possible. If a task
naturally exceeds that, split it into focused, self-contained PRs or
commits.

## Changelog

`CHANGELOG.md` is **generated** — do not edit it directly. Each change is
recorded as a small fragment file under `.changes/unreleased/`, so two PRs
never touch the same lines and the changelog never causes a merge conflict.

Add a fragment for any user-visible change:

```bash
task changelog-new          # prompts for kind (Added/Changed/Fixed/…) and a body
# or, non-interactively:
task changelog-new -- -k Fixed -b "One-line description of the change."
```

This writes `.changes/unreleased/<Kind>-<timestamp>.yaml`; commit it with your
change. Bodies may span multiple lines and use the same Markdown (`` `code` ``,
`[#NNN]` issue/PR references) as the existing entries. Skip the fragment only
for changes with no changelog impact (internal refactors, test-only edits, CI
tweaks).

The `check-changelog` CI job regenerates `CHANGELOG.md` with `changie merge`
and fails if it differs from what is committed, so a directly-edited or stale
`CHANGELOG.md` will be caught.

At release time the maintainer rolls the fragments into a version section:

```bash
task changelog-batch -- 0.8.0   # fragments → .changes/0.8.0.md (edit for prose)
task changelog-merge            # regenerate CHANGELOG.md
```

## Test Coverage

Every change must include unit tests. Target **≥ 80% line coverage** for
new code. Reaching 100% is not required when the remaining paths would
require artificial tests, but coverage gaps must be intentional and
justified. Tests live in `#[cfg(test)]` modules colocated with the code
they test; the `tests/streaming` crate is the in-workspace e2e consumer
of `connectrpc-build`.

## Rust Conventions

- Run `task lint` and `task test` before every commit (or `task ci` to
  also run docs, minimal-features, and the multiservice example).
- Public API items require doc comments (`///`); include `# Errors` /
  `# Panics` sections where applicable.
- Every `unsafe` block requires a `// SAFETY:` comment explaining the
  invariant.
- Avoid `.unwrap()` outside tests and provably-safe paths.
- `#[inline]` only with full-path profiling evidence.

## Code Generation (`connectrpc-codegen`)

All code generation uses `quote!` blocks rather than string manipulation;
`prettyplease` formats the final `TokenStream` into readable Rust source.
The pipeline mirrors buffa's: accumulate `TokenStream` → `syn::parse2` →
`prettyplease::unparse`.

Key rules:

- **Regular `//` comments** are not tokens and are silently dropped by
  `quote!`. Only doc comments (`///` → `#[doc = "..."]`) survive.
- **Identifiers** must go through `format_ident!` (or `Ident::new_raw`
  for Rust keywords). Never interpolate raw strings as identifiers.
- **Type paths** resolved from the descriptor context use
  `rust_path_to_tokens` (which wraps `syn::parse_str::<syn::Path>`).
- Generated code uses **fully-qualified paths everywhere**
  (`::connectrpc::...`, `::buffa::...`) — no `use` statements at module
  scope. This lets multiple generated files be `include!`d into the same
  Rust module without E0252 collisions.

## Conformance Tests

The Connect protocol [conformance suite] runs against both server and
client implementations. `task conformance:download` fetches the runner;
`task conformance:build` builds the binaries.

[conformance suite]: https://github.com/connectrpc/conformance

| Suite | Task | Tests |
|---|---|---:|
| Server default (all protocols) | `task conformance:test` | 3600 |
| Server Connect-only | `task conformance:test-connect-only` | 1192 |
| Server Connect+TLS | `task conformance:test-connect-tls` | 2396 |
| Client Connect | `task conformance:test-client-connect-only` | 2580 |
| Client gRPC | `task conformance:test-client-grpc-only` | 1454 |
| Client gRPC-Web | `task conformance:test-client-grpc-web-only` | 2838 |

A healthy run ends with `N passed, 0 failed`. CI runs the server-default
and the three client suites.

## Checked-In Generated Code

Six directories contain checked-in `buf generate` output and **must be
regenerated** whenever `connectrpc-codegen` output changes (or the buffa
dependency is bumped):

- `conformance/src/generated/`
- `examples/eliza/src/generated/`
- `examples/multiservice/src/generated/`
- `benches/rpc/src/generated/`
- `connectrpc-health/src/generated/`
- `connectrpc-reflection/src/generated/`

Regenerate all of them with:

```bash
task generate:all
```

This rebuilds `protoc-gen-connect-rust` (and the sibling buffa plugins
from `../buffa`) in release mode, then runs `buf generate` in each
directory. CI runs a separate generated-code check that fails when these
checked-in files are stale.

## Building Against a Local buffa Checkout

To test combined speculative changes across both repos:

```bash
task buffa:link     # writes .cargo/config.toml pointing at ../buffa
task buffa:unlink   # removes the override; reverts to crates.io / [patch]
```

The override is gitignored and never reaches CI or `cargo publish`.

## Continuous Integration

GitHub Actions CI (`.github/workflows/ci.yml`) runs on every push to
`main` and on all pull requests. Jobs:

- **Check** — `cargo check --workspace --all-features --all-targets`
  with `RUSTFLAGS=-Dwarnings`
- **Test** — `cargo test --workspace`
- **Clippy** — workspace, all targets, `-D warnings`, on a pinned toolchain
  (currently 1.95) so new-stable lints don't break CI unannounced
- **Format** — `cargo +nightly-2026-02-27 fmt --check`. Nightly is required
  because `format_generated_files = false` in `rustfmt.toml` is a
  nightly-only option; the date is pinned because rustfmt output drifts
  across releases. Use the same dated nightly locally
  (`rustup toolchain install nightly-2026-02-27 -c rustfmt`); bump it
  together with the `fmt` job in `.github/workflows/ci.yml`
- **Check generated code** — runs `task generate:all` and verifies the
  checked-in generated directories have no diff
- **Documentation** — `cargo doc` with broken-intra-doc-links denied
- **MSRV** — `cargo check` on the minimum toolchain, read from `rust-version`
  in the workspace `Cargo.toml` so the declaration and the check cannot drift
- **Examples** — builds and runs the example crates
- **Minimal features** — `cargo check` *and* `cargo test`, both
  `-p connectrpc --no-default-features`. Because the tests run, a test that
  needs `json`, `gzip`, `zstd` or `streaming` must be
  `#[cfg(feature = "...")]`-gated or it fails here while passing the
  default-feature suite
- **Wasm** — `wasm32-unknown-unknown` build of the client example
- **Conformance (server)** / **Conformance (client)** — full suites
