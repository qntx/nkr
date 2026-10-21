//! `nosfry-sql` — the SQL event store for the `nosfry` Nostr relay's
//! Workers edition.
//!
//! The store runs on any synchronous [`Executor`]: a Cloudflare
//! Durable Object's `SqlStorage` in production, `rusqlite` in tests.
//! Write semantics are the native LMDB engine's: ephemeral kinds and
//! duplicate ids are skipped, NIP-09 blocks refuse re-publication and
//! delete their author's targets, replaceable/addressable winners
//! replace their predecessor, and every integer bound into SQL stays
//! inside JavaScript's exact-integer range (`2^53 - 1`).
//!
//! Scans page one cursor per index key newest-first, k-way merge the
//! heads, and post-filter on the packed record in Rust — the same
//! matcher the native engine and the live index use — so `ids`,
//! `authors`, `kinds`, tag conditions and time windows produce the
//! LMDB scan's exact order and dedup. `json` payloads are fetched
//! once, for the winners only.

#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        unused_crate_dependencies,
        reason = "dev-deps are exercised by integration tests"
    )
)]

mod error;
mod exec;
mod scan;
mod store;

#[cfg(all(feature = "rusqlite", not(target_arch = "wasm32")))]
pub mod rusqlite;

pub use error::StoreError;
pub use exec::{Executor, Param, Value};
pub use scan::{Count, Hit, Scan};
pub use store::{FORMAT_VERSION, SqlStore};
