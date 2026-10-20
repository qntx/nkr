# AGENTS.md

- Do not preserve backward compatibility. Remove obsolete paths instead of adding compatibility layers, fallbacks, or migrations.
- Choose the simplest implementation that fully meets the current requirements. Avoid speculative abstractions, configuration, and indirection.
- Grow the system in layers. Start from the smallest version that works end to end, and add each new capability on top of a product that already works. Never trade a working product for unfinished complexity.
- Keep components modular and concerns clearly separated.
- Prefer established, well-maintained libraries when they reduce overall complexity or improve reliability. Do not reimplement common functionality without a clear reason.
- Lean on the dependencies already in the project before writing your own implementation or adding packages. Do not assume a library lacks a capability without checking its documentation and types.
- Make architectural decisions for the long term. Do not accept a stopgap that only works for now and is meant to be replaced later.

## Commands

Local gate before a pull request:

```bash
npx --yes @taplo/cli@0.7.0 fmt --check
cargo +nightly fmt --all -- --check
cargo +nightly clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo deny check
CC_wasm32_unknown_unknown="$(brew --prefix llvm)/bin/clang" \
    cargo check -p nosfry-core --target wasm32-unknown-unknown
```

Run the relay locally:

```bash
cargo run --release -p nosfry -- relay --config nosfry.example.toml
```

Crash-recovery testing (kill -9 cycles): the `crash` test defaults to
6 cycles; raise `NOSFRY_CRASH_CYCLES` for long runs before a release:

```bash
NOSFRY_CRASH_CYCLES=60 cargo test -p nosfry --test crash
```

TOML is formatted by taplo (`.taplo.toml`, aligned `=`); keep `taplo fmt --check` green.

Fuzz one target locally (needs a nightly toolchain and `cargo-fuzz`):

```bash
cargo +nightly fuzz run <target> -- -max_total_time=60
```

`docs/` is local-only and is never pushed: the whole directory is gitignored; never commit any file under `docs/`. Locally it is a Fumadocs tree: every page needs non-empty `title` and `description` frontmatter and must be listed in its folder's `meta.json`; headings start at `##`, every fenced code block has a language. Verify doc changes with `bunx oxfmt --ignore-path=/dev/null <files>` and `bun validate-docs-tree.ts docs --lint` from a checkout of `qntx/workflows/actions/validate-docs-tree`.
