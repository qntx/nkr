#![no_main]

//! Validate arbitrary bytes as a packed index record; a `PackedRef`
//! must then walk its tag area byte-exactly.

use std::sync::LazyLock;

use libfuzzer_sys::fuzz_target;
use nosfry_core::auth::ReadGate;
use nosfry_core::filter::Filter;
use nosfry_core::packed::PackedRef;

static FILTERS: LazyLock<[Filter; 3]> = LazyLock::new(|| {
    [
        Filter::parse("{}").unwrap(),
        Filter::parse("{\"kinds\":[1]}").unwrap(),
        Filter::parse(&format!("{{\"#p\":[\"{}\"]}}", "ab".repeat(32))).unwrap(),
    ]
});

/// Byte offset where the tag entries start in a valid record.
const HEADER_LEN: usize = 82;

fuzz_target!(|data: &[u8]| {
    let Some(ev) = PackedRef::new(data) else {
        return;
    };
    let _ = ev.id();
    let _ = ev.pubkey();
    let _ = ev.created_at();
    let _ = ev.kind();
    let _ = ev.expiration();
    let _ = ev.d_tag();
    // `tags()` must consume exactly the bytes after the header.
    let consumed = ev.tags().fold(HEADER_LEN, |n, (_, v)| n + 2 + v.len());
    assert_eq!(consumed, ev.as_bytes().len());
    for filter in FILTERS.iter() {
        let _ = filter.matches(ev);
    }
    assert!(ReadGate::default().allows(ev));
});
