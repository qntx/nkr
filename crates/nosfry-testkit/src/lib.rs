//! `nosfry-testkit` — the shared reference model, event/filter
//! generators and the differential driver both event stores
//! (`nosfry-lmdb`, `nosfry-sql`) run against. Test support only;
//! never published.

#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "test support may panic on failure"
)]
#![cfg_attr(
    test,
    allow(
        unused_crate_dependencies,
        reason = "dev-deps are exercised by the store integration tests"
    )
)]

pub mod driver;
pub mod model;
pub mod spec;
