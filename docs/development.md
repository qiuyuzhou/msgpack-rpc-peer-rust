# Development guide

How CI, commit hooks, and local checks verify this crate.

## Continuous integration

The [GitHub Actions CI workflow](../.github/workflows/ci.yml) runs on pushes, pull
requests, and manual dispatch. It checks formatting, Clippy, and documentation on
stable Rust, runs tests including doctests on Linux and macOS, and checks all
targets with Rust 1.85 on Linux. Cargo commands use `--locked` to validate the
committed dependency versions.

## Commit checks

Install [pre-commit](https://pre-commit.com/#installation),
[actionlint](https://github.com/rhysd/actionlint#installation), and the Rust
components, then enable the hooks for this checkout:

```sh
rustup component add rustfmt clippy
pre-commit install
pre-commit run --all-files
```

The [pre-commit configuration](../.pre-commit-config.yaml) checks staged whitespace
and conflict markers on every commit. Rust source or manifest changes trigger
format checking; Rust source, dependency, toolchain, or Cargo configuration
changes trigger Clippy with warnings denied, including examples. Workflow changes
trigger actionlint. Hooks check files without fixing them; tests, documentation
builds, and MSRV checks stay in CI, and hooks do not run examples.

`pre-commit install` without `--overwrite` preserves existing hooks through
pre-commit's migration mode. During a commit, pre-commit temporarily hides
unstaged changes to tracked files, but Cargo still checks the whole crate and may
see untracked files. Manual `--all-files` runs check the working tree; the
staged-diff hook always checks the index.

## Local verification

Run the stable checks and examples locally:

```sh
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --all-features
cargo run --locked --example duplex
cargo run --locked --example bidirectional
cargo run --locked --example nested
cargo run --locked --example unix
cargo run --locked --example subprocess
```

The `unix` example runs only on Unix. Subprocess tests use a separate executable
harness so test output cannot corrupt RPC stdout. Tests cover real Unix sockets
and child processes, bounded queue saturation, cancellation, panic diagnostics,
and owned shutdown. Compiler and platform coverage beyond this validation
environment remains unverified.
