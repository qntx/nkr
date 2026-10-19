<!-- markdownlint-disable MD033 MD041 -->

# nosfry

High-performance Nostr relay in Rust, for bare-metal servers and Cloudflare Workers.

Status: 0.1. The native relay serves NIP-01 and NIP-11; the strfry
feature-parity layer (NIP-09, 40, 42, 45, 70, 77, plugins, metrics) is
in progress. Linux and macOS are supported.

- `nosfry-core` — sans-IO protocol core (`wasm32-unknown-unknown` clean)
- `nosfry-lmdb` — synchronous LMDB event store (native only)
- `nosfry` — the relay crate and binary: WebSocket NIP-01 + NIP-11 over
  the store, on a writer/shard/reader thread topology (native only)

## Install

```bash
cargo install nosfry --locked
```

Prebuilt binaries for Linux (x86_64, aarch64) and macOS (aarch64) are
attached to every [GitHub release](https://github.com/qntx/nosfry/releases).

## Quick start

```bash
cp nosfry.example.toml nosfry.toml
nosfry relay --config nosfry.toml
```

From a checkout, `cargo run --release -p nosfry -- relay --config nosfry.toml`
does the same.

The relay listens on `127.0.0.1:7777` by default; every key in
`nosfry.example.toml` carries its default and a comment. `NOSFRY_CONFIG`
points at the config file too. `RUST_LOG` controls tracing;
`RUST_LOG_JSON=1` selects JSON logs. SIGINT/SIGTERM shut down
gracefully.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this project shall be dual-licensed as above, without any additional terms or conditions.

---

<div align="center">

A **[QuantX](https://qntx.org)** open-source project.

<a href="https://qntx.org"><img alt="QuantX" width="369" src="https://raw.githubusercontent.com/qntx/.github/main/profile/qntx.svg" /></a>

Code is law. We write both.

</div>
