#![no_main]

//! Parse arbitrary text as a NIP-01 filter and exercise every accessor
//! plus `matches` against one fixed packed event.

use std::sync::LazyLock;

use libfuzzer_sys::fuzz_target;
use nosfry_core::event::{EventId, PublicKey};
use nosfry_core::filter::Filter;
use nosfry_core::kind::Kind;
use nosfry_core::packed::PackedEvent;

static EVENT: LazyLock<PackedEvent> = LazyLock::new(|| {
    PackedEvent::build(
        &EventId([0x11; 32]),
        &PublicKey([0x22; 32]),
        1_700_000_000,
        Kind(1),
        0,
        [
            vec!["p".to_owned(), "33".repeat(32)],
            vec!["e".to_owned(), "44".repeat(32)],
            vec!["t".to_owned(), "nostr".to_owned()],
        ],
    )
});

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(filter) = Filter::parse(text) else {
        return;
    };
    let _ = filter.is_impossible();
    let _ = filter.tag_filter_count();
    let _ = filter.intrinsic_limit();
    let _ = filter.effective_limit(500);
    let _ = filter.matches(EVENT.packed());
});
