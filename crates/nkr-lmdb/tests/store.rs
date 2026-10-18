//! Store semantics: write outcomes, scans, snapshot bounds, persistence.

#![allow(
    unused_crate_dependencies,
    reason = "each test binary uses a subset of the workspace dependencies"
)]
#![allow(
    clippy::tests_outside_test_module,
    reason = "each file is a single integration test"
)]
#![allow(
    unsafe_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "tests may panic; the format-mismatch fixture opens LMDB directly"
)]

mod common;

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;

use common::{event, key};
use nkr::event::Event;
use nkr::filter::Filter;
use nkr_lmdb::{Progress, Scan, Store, StoreOptions, WriteOutcome};
use serde_json::json;

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
    (dir, store)
}

/// Run a scan to completion, collecting every emitted JSON string.
fn collect(store: &Store, filters: Arc<[Filter]>, snapshot: u64) -> Vec<String> {
    let mut scan = Scan::new(filters, snapshot, 500);
    let out = RefCell::new(Vec::new());
    loop {
        let progress = scan
            .step(store, Duration::ZERO, &mut |json| {
                out.borrow_mut().push(json.to_owned());
            })
            .unwrap();
        if progress == Progress::Done {
            break;
        }
    }
    out.into_inner()
}

#[test]
fn write_and_scan_all() {
    let (_dir, store) = store();
    let k = key(1);
    let e1 = event(&k, 1, 100, &json!([]), "a");
    let e2 = event(&k, 1, 200, &json!([]), "b");
    let out = store.write(&[Arc::clone(&e1), Arc::clone(&e2)]).unwrap();
    assert_eq!(
        out,
        vec![
            WriteOutcome::Stored { seq: 1 },
            WriteOutcome::Stored { seq: 2 }
        ]
    );
    assert_eq!(store.snapshot().unwrap(), 2);
    let got = collect(&store, Arc::from([Filter::parse("{}").unwrap()]), 2);
    assert_eq!(got, vec![e2.json().to_owned(), e1.json().to_owned()]);
}

#[test]
fn duplicate_and_ephemeral() {
    let (_dir, store) = store();
    let k = key(1);
    let e = event(&k, 1, 100, &json!([]), "a");
    let eph = event(&k, 20_001, 100, &json!([]), "gone");
    let out = store.write(&[Arc::clone(&e), Arc::clone(&e), eph]).unwrap();
    assert_eq!(
        out,
        vec![
            WriteOutcome::Stored { seq: 1 },
            WriteOutcome::Duplicate,
            WriteOutcome::Ephemeral
        ]
    );
    assert_eq!(store.snapshot().unwrap(), 1);
}

#[test]
fn replaceable_supersedes() {
    let (_dir, store) = store();
    let k = key(1);
    let old = event(&k, 0, 100, &json!([]), "old");
    let new = event(&k, 0, 200, &json!([]), "new");
    let stale = event(&k, 0, 50, &json!([]), "stale");
    let out = store
        .write(&[Arc::clone(&old), Arc::clone(&new), stale])
        .unwrap();
    assert_eq!(
        out,
        vec![
            WriteOutcome::Stored { seq: 1 },
            WriteOutcome::Stored { seq: 2 },
            WriteOutcome::Superseded
        ]
    );
    assert_eq!(store.snapshot().unwrap(), 2);
    let got = collect(
        &store,
        Arc::from([Filter::parse("{\"kinds\":[0]}").unwrap()]),
        2,
    );
    assert_eq!(got, vec![new.json().to_owned()]);
    let _ = old;
}

#[test]
fn replaceable_two_versions_one_batch() {
    let (_dir, store) = store();
    let k = key(1);
    // Older first: v2 must evict v1 inside the single write txn.
    let v1 = event(&k, 3, 100, &json!([]), "v1");
    let v2 = event(&k, 3, 300, &json!([]), "v2");
    let out = store.write(&[v1, Arc::clone(&v2)]).unwrap();
    assert_eq!(
        out,
        vec![
            WriteOutcome::Stored { seq: 1 },
            WriteOutcome::Stored { seq: 2 }
        ]
    );
    let got = collect(
        &store,
        Arc::from([Filter::parse("{}").unwrap()]),
        store.snapshot().unwrap(),
    );
    assert_eq!(got, vec![v2.json().to_owned()]);
}

#[test]
fn replaceable_tie_lower_id_wins() {
    let (_dir, store) = store();
    let k = key(1);
    // Same created_at: the event with the lower id wins. Craft by trying
    // contents until we find a pair with distinct id ordering.
    let a = event(&k, 0, 100, &json!([]), "tie-a");
    let b = event(&k, 0, 100, &json!([]), "tie-b");
    let (winner, loser) = if a.id() < b.id() { (a, b) } else { (b, a) };
    let out = store.write(&[loser, Arc::clone(&winner)]).unwrap();
    assert_eq!(
        out,
        vec![
            WriteOutcome::Stored { seq: 1 },
            WriteOutcome::Stored { seq: 2 }
        ]
    );
    let got = collect(
        &store,
        Arc::from([Filter::parse("{\"kinds\":[0]}").unwrap()]),
        2,
    );
    assert_eq!(got, vec![winner.json().to_owned()]);
}

#[test]
fn addressable_replaces_by_d_tag() {
    let (_dir, store) = store();
    let k = key(1);
    let v1 = event(&k, 30_001, 100, &json!([["d", "article"]]), "v1");
    let v2 = event(&k, 30_001, 200, &json!([["d", "article"]]), "v2");
    let other = event(&k, 30_001, 250, &json!([["d", "other"]]), "other");
    let out = store
        .write(&[v1, Arc::clone(&v2), Arc::clone(&other)])
        .unwrap();
    assert_eq!(
        out,
        vec![
            WriteOutcome::Stored { seq: 1 },
            WriteOutcome::Stored { seq: 2 },
            WriteOutcome::Stored { seq: 3 }
        ]
    );
    let got = collect(
        &store,
        Arc::from([Filter::parse("{\"kinds\":[30001]}").unwrap()]),
        3,
    );
    assert_eq!(got, vec![other.json().to_owned(), v2.json().to_owned()]);
}

#[test]
fn addressable_long_d_replaces() {
    let (_dir, store) = store();
    let k = key(1);
    let long_d = "d".repeat(300);
    let v1 = event(&k, 30_002, 100, &json!([["d", long_d]]), "v1");
    let v2 = event(&k, 30_002, 200, &json!([["d", long_d]]), "v2");
    let out = store.write(&[v1, Arc::clone(&v2)]).unwrap();
    assert_eq!(
        out,
        vec![
            WriteOutcome::Stored { seq: 1 },
            WriteOutcome::Stored { seq: 2 }
        ]
    );
    let got = collect(&store, Arc::from([Filter::parse("{}").unwrap()]), 2);
    assert_eq!(got, vec![v2.json().to_owned()]);
}

#[test]
fn different_authors_do_not_replace() {
    let (_dir, store) = store();
    let a = event(&key(1), 0, 100, &json!([]), "a");
    let b = event(&key(2), 0, 50, &json!([]), "b");
    let out = store.write(&[Arc::clone(&a), Arc::clone(&b)]).unwrap();
    assert_eq!(
        out,
        vec![
            WriteOutcome::Stored { seq: 1 },
            WriteOutcome::Stored { seq: 2 }
        ]
    );
    let _ = (a, b);
}

#[test]
fn scan_snapshot_bound() {
    let (_dir, store) = store();
    let k = key(1);
    let e1 = event(&k, 1, 100, &json!([]), "one");
    store.write(&[Arc::clone(&e1)]).unwrap();
    let snap = store.snapshot().unwrap();
    let e2 = event(&k, 1, 200, &json!([]), "two");
    store.write(&[e2]).unwrap();
    assert_eq!(store.snapshot().unwrap(), 2);
    let got = collect(&store, Arc::from([Filter::parse("{}").unwrap()]), snap);
    assert_eq!(got, vec![e1.json().to_owned()]);
}

#[test]
fn scan_writes_between_steps_invisible() {
    let (_dir, store) = store();
    let k = key(1);
    // 100 events so a ZERO-budget step stops mid-scan (64-candidate budget).
    let events: Vec<Arc<Event>> = (0..100)
        .map(|i| event(&k, 1, 1_000 + i, &json!([]), "x"))
        .collect();
    store.write(&events).unwrap();
    let snap = store.snapshot().unwrap();
    let mut scan = Scan::new(Arc::from([Filter::parse("{}").unwrap()]), snap, 500);
    let out = RefCell::new(Vec::new());
    let p1 = scan
        .step(&store, Duration::ZERO, &mut |j| {
            out.borrow_mut().push(j.to_owned());
        })
        .unwrap();
    // Write more events between steps — they exceed the snapshot.
    let later: Vec<Arc<Event>> = (0..10)
        .map(|i| event(&k, 1, 9_000 + i, &json!([]), "late"))
        .collect();
    store.write(&later).unwrap();
    loop {
        let p = scan
            .step(&store, Duration::ZERO, &mut |j| {
                out.borrow_mut().push(j.to_owned());
            })
            .unwrap();
        if p == Progress::Done {
            break;
        }
    }
    let _ = p1;
    let got = out.into_inner();
    assert_eq!(got.len(), 100);
    assert!(got.iter().all(|j| !j.contains("late")));
}

#[test]
fn scan_limit_and_ties() {
    let (_dir, store) = store();
    let k = key(1);
    let mut events = Vec::new();
    for i in 0..5u64 {
        events.push(event(&k, 1, 100, &json!([]), &format!("tie{i}")));
    }
    events.push(event(&k, 1, 50, &json!([]), "old"));
    store.write(&events).unwrap();
    let got = collect(
        &store,
        Arc::from([Filter::parse("{\"limit\":3}").unwrap()]),
        6,
    );
    assert_eq!(got.len(), 3);
}

#[test]
fn scan_cross_filter_dedup() {
    let (_dir, store) = store();
    let k = key(1);
    let e = event(&k, 1, 100, &json!([]), "a");
    store.write(&[Arc::clone(&e)]).unwrap();
    let id = e.id();
    let hex = format!("{id}");
    let filters: Arc<[Filter]> = Arc::from([
        Filter::parse(&format!("{{\"ids\":[\"{hex}\"]}}")).unwrap(),
        Filter::parse("{}").unwrap(),
    ]);
    let got = collect(&store, filters, 1);
    assert_eq!(got, vec![e.json().to_owned()]);
}

#[test]
fn scan_impossible_filter() {
    let (_dir, store) = store();
    let k = key(1);
    store.write(&[event(&k, 1, 100, &json!([]), "a")]).unwrap();
    let got = collect(
        &store,
        Arc::from([Filter::parse("{\"kinds\":[]}").unwrap()]),
        1,
    );
    assert!(got.is_empty(), "expected no emissions");
}

#[test]
fn scan_tag_and_author_indexes() {
    let (_dir, store) = store();
    let k = key(1);
    let tagged = event(&k, 1, 100, &json!([["t", "nostr"]]), "tagged");
    let plain = event(&k, 1, 200, &json!([]), "plain");
    store
        .write(&[Arc::clone(&tagged), Arc::clone(&plain)])
        .unwrap();
    let got = collect(
        &store,
        Arc::from([Filter::parse("{\"#t\":[\"nostr\"]}").unwrap()]),
        2,
    );
    assert_eq!(got, vec![tagged.json().to_owned()]);
    let pk = format!("{}", tagged.pubkey());
    let got_authors = collect(
        &store,
        Arc::from([Filter::parse(&format!("{{\"authors\":[\"{pk}\"]}}")).unwrap()]),
        2,
    );
    assert_eq!(got_authors.len(), 2); // both events share the author
    assert_eq!(got_authors[0], plain.json());
}

#[test]
fn persistence_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let k = key(1);
    let e1 = event(&k, 1, 100, &json!([]), "one");
    {
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
        store.write(&[Arc::clone(&e1)]).unwrap();
        assert_eq!(store.snapshot().unwrap(), 1);
    }
    let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
    let e2 = event(&k, 1, 200, &json!([]), "two");
    let out = store.write(&[Arc::clone(&e2)]).unwrap();
    assert_eq!(out, vec![WriteOutcome::Stored { seq: 2 }]);
    let got = collect(&store, Arc::from([Filter::parse("{}").unwrap()]), 2);
    assert_eq!(got, vec![e2.json().to_owned(), e1.json().to_owned()]);
}

#[test]
fn format_mismatch_rejected() {
    let dir = tempfile::tempdir().unwrap();
    // Hand-create an env whose meta.format is not 1.
    {
        use heed::types::Bytes;
        use heed::{EnvFlags, EnvOpenOptions};
        let mut options = EnvOpenOptions::new();
        // SAFETY: flag must be set before opening the env.
        unsafe { options.flags(EnvFlags::NO_READ_AHEAD) };
        options.map_size(1 << 20).max_dbs(16);
        // SAFETY: test-scoped env; the dir is private to this test.
        let env = unsafe { options.open(dir.path()).unwrap() };
        let mut wtxn = env.write_txn().unwrap();
        let meta = env
            .create_database::<Bytes, Bytes>(&mut wtxn, Some("meta"))
            .unwrap();
        meta.put(&mut wtxn, b"format", &9u32.to_be_bytes()).unwrap();
        wtxn.commit().unwrap();
    }
    let err = Store::open(dir.path(), &StoreOptions::default()).unwrap_err();
    assert!(matches!(
        err,
        nkr_lmdb::StoreError::FormatMismatch {
            found: 9,
            expected: 1
        }
    ));
}

#[test]
fn id_lookup_by_ids_filter() {
    let (_dir, store) = store();
    let k = key(1);
    let a = event(&k, 1, 100, &json!([]), "a");
    let b = event(&k, 1, 200, &json!([]), "b");
    store.write(&[Arc::clone(&a), Arc::clone(&b)]).unwrap();
    let hex = format!("{}", a.id());
    let got = collect(
        &store,
        Arc::from([Filter::parse(&format!("{{\"ids\":[\"{hex}\"]}}")).unwrap()]),
        2,
    );
    assert_eq!(got, vec![a.json().to_owned()]);
}

#[test]
fn scan_emits_first_group_before_step_ends() {
    let (_dir, store) = store();
    let k = key(1);
    let events: Vec<_> = (0..1_000u64)
        .map(|i| event(&k, 1, 1_000 + i, &json!([]), &format!("e{i}")))
        .collect();
    store.write(&events).unwrap();
    let snapshot = store.snapshot().unwrap();
    let filters: Arc<[Filter]> =
        Arc::from([Filter::parse("{\"kinds\":[1],\"limit\":500}").unwrap()]);
    let mut scan = Scan::new(filters, snapshot, 500);
    let mut emitted = 0usize;
    // A zero-budget step stops after BUDGET_CHECK_EVERY candidates; a
    // streaming scan has already flushed completed created_at groups.
    let progress = scan
        .step(&store, Duration::ZERO, &mut |_| {
            emitted += 1;
        })
        .unwrap();
    assert_eq!(progress, Progress::Pending);
    assert!(emitted > 0, "first step emitted nothing");
}
