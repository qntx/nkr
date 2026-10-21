//! `nosfry-worker` — the Workers edition's relay object.
//!
//! Host-testable plain Rust over [`nosfry_sql::Executor`] and a
//! [`Socket`] trait: the sans-IO `Session` and
//! `Monitors` of `nosfry-core`, the SQL store of `nosfry-sql`, and
//! the hibernation contract (per-socket attachments plus the `subs`
//! table) the Durable Object glue implements.

#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        unused_crate_dependencies,
        reason = "dev-deps are exercised by integration tests"
    )
)]

pub mod config;
pub mod relay;

pub use config::{Config, ConfigError};
pub use relay::{Attachment, Effects, Relay, RelayError, Socket};
