//! Criterion bench: live-monitor fan-out matching (`Monitors::matches`
//! clones one `SubKey` per hit).

#![allow(
    unused_crate_dependencies,
    reason = "each bench binary uses a subset of the workspace dependencies"
)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "bench fixtures may panic on failure"
)]

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use nosfry_core::event::{EventId, PublicKey};
use nosfry_core::filter::Filter;
use nosfry_core::kind::Kind;
use nosfry_core::message::SubId;
use nosfry_core::monitor::Monitors;
use nosfry_core::packed::PackedEvent;
use nosfry_core::session::SubKey;

fn monitors_matches(c: &mut Criterion) {
    let mut monitors = Monitors::<u64>::new();
    let filters: Arc<[Filter]> = Arc::from(vec![Filter::parse("{\"kinds\":[1]}").expect("filter")]);
    for conn in 0..1_000u64 {
        monitors.insert(
            conn,
            SubKey {
                id: SubId::parse(&format!("s{conn}")).expect("sub id"),
                generation: 0,
            },
            Arc::clone(&filters),
        );
    }
    let event = PackedEvent::build(
        &EventId([1; 32]),
        &PublicKey([2; 32]),
        1_700_000_000,
        Kind(1),
        0,
        Vec::<Vec<String>>::new(),
    );
    let packed = event.packed();
    let mut hits = Vec::new();
    c.bench_function("monitors_matches_1000_kinds_subs", |b| {
        b.iter(|| {
            hits.clear();
            monitors.matches(packed, &mut hits);
            std::hint::black_box(&hits);
        });
    });
}

criterion_group!(benches, monitors_matches);
criterion_main!(benches);
