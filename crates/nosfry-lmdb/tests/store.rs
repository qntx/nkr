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
use nosfry_core::event::Event;
use nosfry_core::filter::Filter;
use nosfry_lmdb::{FORMAT_VERSION, Progress, Scan, Store, StoreError, StoreOptions, WriteOutcome};
use serde_json::json;

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
    (dir, store)
}

/// Run a scan to completion, collecting every emitted JSON string.
/// `now = 0` disables the NIP-40 cutoff; pass a real `now` where
/// expiry visibility is under test.
fn collect_now(store: &Store, filters: Arc<[Filter]>, snapshot: u64, now: u64) -> Vec<String> {
    let mut scan = Scan::new(
        filters,
        nosfry_core::auth::ReadGate::default(),
        snapshot,
        500,
        now,
    );
    let out = RefCell::new(Vec::new());
    loop {
        let progress = scan
            .step(store, Duration::ZERO, &mut |_seq, json| {
                out.borrow_mut().push(json.to_owned());
            })
            .unwrap();
        if progress == Progress::Done {
            break;
        }
    }
    out.into_inner()
}

/// `collect_now` with the expiry cutoff disabled.
fn collect(store: &Store, filters: Arc<[Filter]>, snapshot: u64) -> Vec<String> {
    collect_now(store, filters, snapshot, 0)
}

#[test]
fn write_and_scan_all() {
    let (_dir, store) = store();
    let k = key(1);
    let e1 = event(&k, 1, 100, &json!([]), "a");
    let e2 = event(&k, 1, 200, &json!([]), "b");
    let out = store.write(&[Arc::clone(&e1), Arc::clone(&e2)]).unwrap();
    assert_eq!(
        out.outcomes,
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
        out.outcomes,
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
        out.outcomes,
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
        out.outcomes,
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
        out.outcomes,
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
        out.outcomes,
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
        out.outcomes,
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
        out.outcomes,
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
    let mut scan = Scan::new(
        Arc::from([Filter::parse("{}").unwrap()]),
        nosfry_core::auth::ReadGate::default(),
        snap,
        500,
        0,
    );
    let out = RefCell::new(Vec::new());
    let p1 = scan
        .step(&store, Duration::ZERO, &mut |_seq, j| {
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
            .step(&store, Duration::ZERO, &mut |_seq, j| {
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
    assert_eq!(out.outcomes, vec![WriteOutcome::Stored { seq: 2 }]);
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
        StoreError::FormatMismatch {
            found: 9,
            expected
        } if expected == FORMAT_VERSION
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
    let mut scan = Scan::new(
        filters,
        nosfry_core::auth::ReadGate::default(),
        snapshot,
        500,
        0,
    );
    let mut emitted = 0usize;
    // A zero-budget step stops after BUDGET_CHECK_EVERY candidates; a
    // streaming scan has already flushed completed created_at groups.
    let progress = scan
        .step(&store, Duration::ZERO, &mut |_, _| {
            emitted += 1;
        })
        .unwrap();
    assert_eq!(progress, Progress::Pending);
    assert!(emitted > 0, "first step emitted nothing");
}

/// Run an export, collecting every emitted JSON string.
fn export_lines(store: &Store, since: u64, until: u64, reverse: bool) -> Vec<String> {
    let lines = RefCell::new(Vec::new());
    store
        .export(since, until, reverse, &mut |json| {
            lines.borrow_mut().push(json.to_owned());
            Ok(())
        })
        .unwrap();
    lines.into_inner()
}

#[test]
fn export_orders_by_created_at_then_seq() {
    let (_dir, store) = store();
    let k = key(1);
    let e1 = event(&k, 1, 100, &json!([]), "a");
    let e2 = event(&k, 1, 100, &json!([]), "b");
    let e3 = event(&k, 1, 200, &json!([]), "c");
    store
        .write(&[Arc::clone(&e1), Arc::clone(&e2), Arc::clone(&e3)])
        .unwrap();

    let asc = export_lines(&store, 0, u64::MAX, false);
    assert_eq!(asc, [e1.json(), e2.json(), e3.json()], "ascending order");

    let desc = export_lines(&store, 0, u64::MAX, true);
    assert_eq!(
        desc,
        [e3.json(), e2.json(), e1.json()],
        "descending order; the created_at tie is seq-descending"
    );
}

#[test]
fn export_bounds_are_inclusive() {
    let (_dir, store) = store();
    let k = key(1);
    let e1 = event(&k, 1, 100, &json!([]), "a");
    let e2 = event(&k, 1, 200, &json!([]), "b");
    let e3 = event(&k, 1, 300, &json!([]), "c");
    store
        .write(&[Arc::clone(&e1), Arc::clone(&e2), Arc::clone(&e3)])
        .unwrap();

    let count = RefCell::new(0u64);
    let got = store
        .export(100, 200, false, &mut |json| {
            *count.borrow_mut() += 1;
            assert!(json.contains("\"a\"") || json.contains("\"b\""));
            Ok(())
        })
        .unwrap();
    assert_eq!(got, 2);
    assert_eq!(*count.borrow(), 2);

    assert_eq!(export_lines(&store, 0, 99, false), Vec::<String>::new());
    assert_eq!(
        export_lines(&store, 101, 300, false),
        [e2.json(), e3.json()]
    );
}

#[test]
fn export_propagates_emit_errors() {
    let (_dir, store) = store();
    let k = key(1);
    store.write(&[event(&k, 1, 100, &json!([]), "a")]).unwrap();
    let err = store
        .export(0, u64::MAX, false, &mut |_| {
            Err(std::io::Error::other("synthetic"))
        })
        .unwrap_err();
    assert!(matches!(err, StoreError::Io(e) if e.kind() == std::io::ErrorKind::Other));
}

#[test]
fn compact_to_round_trip() {
    let (_dir, store) = store();
    let k = key(1);
    let events: Vec<_> = (0..10u64)
        .map(|i| event(&k, 1, 100 + i, &json!([]), &format!("e{i}")))
        .collect();
    store.write(&events).unwrap();

    let staging = tempfile::tempdir().unwrap();
    let dest = staging.path().join("compact.mdb");
    store.compact_to(&dest).unwrap();
    assert!(dest.is_file(), "compact_to wrote a single file");

    let fresh = tempfile::tempdir().unwrap();
    std::fs::rename(&dest, fresh.path().join("data.mdb")).unwrap();
    let copy = Store::open(fresh.path(), &StoreOptions::default()).unwrap();

    assert_eq!(
        export_lines(&store, 0, u64::MAX, false),
        export_lines(&copy, 0, u64::MAX, false),
        "compacted copy exports identically"
    );
    assert_eq!(
        export_lines(&store, 0, u64::MAX, true),
        export_lines(&copy, 0, u64::MAX, true)
    );
}

#[test]
fn compact_to_refuses_existing_dest() {
    let (_dir, store) = store();
    let dest_dir = tempfile::tempdir().unwrap();
    let dest = dest_dir.path().join("data.mdb");
    std::fs::write(&dest, b"existing").unwrap();

    let err = store.compact_to(&dest).unwrap_err();
    assert!(
        matches!(err, StoreError::Io(ref e) if e.kind() == std::io::ErrorKind::AlreadyExists),
        "expected AlreadyExists, got {err}"
    );
    assert_eq!(std::fs::read(&dest).unwrap(), b"existing", "dest untouched");
}

/// A leftover `<dest>.tmp` — evidence of an interrupted compaction —
/// refuses the run instead of being silently reused or clobbered.
#[test]
fn compact_to_refuses_stale_tmp() {
    let (_dir, store) = store();
    let dest_dir = tempfile::tempdir().unwrap();
    let dest = dest_dir.path().join("data.mdb");
    let tmp = dest_dir.path().join("data.mdb.tmp");
    std::fs::write(&tmp, b"partial").unwrap();

    let err = store.compact_to(&dest).unwrap_err();
    assert!(
        matches!(err, StoreError::Io(ref e) if e.kind() == std::io::ErrorKind::AlreadyExists),
        "expected AlreadyExists for the tmp file, got {err}"
    );
    assert!(!dest.exists(), "dest must not be created");
    assert_eq!(std::fs::read(&tmp).unwrap(), b"partial", "tmp untouched");
}

/// The author's pubkey hex for `a` tag values.
fn pk_hex(keypair: &secp256k1::Keypair) -> String {
    format!("{}", keypair.x_only_public_key().0)
}

#[test]
fn deletion_by_e_removes_target_and_blocks_republish() {
    let (_dir, store) = store();
    let k = key(1);
    let note = event(&k, 1, 100, &json!([]), "gone");
    let kind5 = event(&k, 5, 200, &json!([["e", format!("{}", note.id())]]), "");
    assert_eq!(
        store.write(&[Arc::clone(&note)]).unwrap().outcomes,
        vec![WriteOutcome::Stored { seq: 1 }]
    );
    let written = store.write(&[Arc::clone(&kind5)]).unwrap();
    assert_eq!(written.outcomes, vec![WriteOutcome::Stored { seq: 2 }]);
    assert_eq!(written.deleted, vec![1]);
    let got = collect(&store, Arc::from([Filter::parse("{}").unwrap()]), 2);
    assert_eq!(got, vec![kind5.json().to_owned()]);
    // The id stays blocked for this author: republishing is Deleted.
    let again = store.write(&[Arc::clone(&note)]).unwrap();
    assert_eq!(again.outcomes, vec![WriteOutcome::Deleted]);
    assert_eq!(again.deleted, Vec::<u64>::new());
}

#[test]
fn deletion_kind5_targeting_kind5_has_no_effect() {
    let (_dir, store) = store();
    let k = key(1);
    let first = event(&k, 5, 100, &json!([]), "first");
    let second = event(
        &k,
        5,
        200,
        &json!([["e", format!("{}", first.id())]]),
        "second",
    );
    let written = store
        .write(&[Arc::clone(&first), Arc::clone(&second)])
        .unwrap();
    assert_eq!(written.deleted, Vec::<u64>::new());
    let got = collect(
        &store,
        Arc::from([Filter::parse("{\"kinds\":[5]}").unwrap()]),
        2,
    );
    assert_eq!(got.len(), 2, "a kind 5 cannot delete a kind 5");
}

#[test]
fn deletion_foreign_author_e_has_no_effect() {
    let (_dir, store) = store();
    let note = event(&key(1), 1, 100, &json!([]), "mine");
    let kind5 = event(
        &key(2),
        5,
        200,
        &json!([["e", format!("{}", note.id())]]),
        "",
    );
    let written = store
        .write(&[Arc::clone(&note), Arc::clone(&kind5)])
        .unwrap();
    assert_eq!(written.deleted, Vec::<u64>::new());
    let got = collect(&store, Arc::from([Filter::parse("{}").unwrap()]), 2);
    assert_eq!(got.len(), 2, "a foreign-author e target is ignored");
}

#[test]
fn deletion_by_a_removes_equal_and_older_versions() {
    let (_dir, store) = store();
    let k = key(1);
    let pk = pk_hex(&k);
    let v1 = event(&k, 30_023, 100, &json!([["d", "art"]]), "v1");
    let del = event(&k, 5, 200, &json!([["a", format!("30023:{pk}:art")]]), "");
    let written = store.write(&[Arc::clone(&v1), Arc::clone(&del)]).unwrap();
    assert_eq!(written.outcomes.len(), 2);
    assert_eq!(written.deleted, vec![1]);
    // Equal or older republishing is blocked; a newer version is accepted.
    let v_older = event(&k, 30_023, 50, &json!([["d", "art"]]), "older");
    let v_equal = event(&k, 30_023, 200, &json!([["d", "art"]]), "equal");
    let v_newer = event(&k, 30_023, 250, &json!([["d", "art"]]), "newer");
    let again = store
        .write(&[
            Arc::clone(&v1),
            Arc::clone(&v_older),
            Arc::clone(&v_equal),
            Arc::clone(&v_newer),
        ])
        .unwrap();
    assert_eq!(
        again.outcomes,
        vec![
            WriteOutcome::Deleted,
            WriteOutcome::Deleted,
            WriteOutcome::Deleted,
            WriteOutcome::Stored { seq: 3 }
        ]
    );
    let got = collect(
        &store,
        Arc::from([Filter::parse("{\"kinds\":[30023]}").unwrap()]),
        3,
    );
    assert_eq!(got, vec![v_newer.json().to_owned()]);
}

#[test]
fn deletion_by_a_replaceable_uses_empty_d() {
    let (_dir, store) = store();
    let k = key(1);
    let pk = pk_hex(&k);
    let profile = event(&k, 0, 100, &json!([]), "profile");
    let del = event(&k, 5, 200, &json!([["a", format!("0:{pk}:")]]), "");
    let written = store
        .write(&[Arc::clone(&profile), Arc::clone(&del)])
        .unwrap();
    assert_eq!(written.deleted, vec![1]);
    assert_eq!(
        store.write(&[Arc::clone(&profile)]).unwrap().outcomes,
        vec![WriteOutcome::Deleted]
    );
    let newer = event(&k, 0, 300, &json!([]), "new");
    assert_eq!(
        store.write(&[Arc::clone(&newer)]).unwrap().outcomes,
        vec![WriteOutcome::Stored { seq: 3 }]
    );
}

#[test]
fn deletion_foreign_a_has_no_effect() {
    let (_dir, store) = store();
    let k = key(1);
    let other = pk_hex(&key(2));
    let v1 = event(&k, 30_023, 100, &json!([["d", "art"]]), "v1");
    // The a value names another author's address: not a target.
    let del = event(
        &k,
        5,
        200,
        &json!([["a", format!("30023:{other}:art")]]),
        "",
    );
    let written = store.write(&[Arc::clone(&v1), Arc::clone(&del)]).unwrap();
    assert_eq!(written.deleted, Vec::<u64>::new());
    let got = collect(
        &store,
        Arc::from([Filter::parse("{\"kinds\":[30023]}").unwrap()]),
        2,
    );
    assert_eq!(got, vec![v1.json().to_owned()]);
}

/// A `d` longer than 255 bytes never fits the packed record; a delete
/// must still find and remove every index entry. Shared by the `e` and
/// `a` deletion tests.
fn assert_long_d_deletion(store: &Store, del: Arc<Event>) {
    let k = key(1);
    let long_d = "d".repeat(300);
    let v1 = event(&k, 30_002, 100, &json!([["d", long_d]]), "v1");
    let written = store.write(&[Arc::clone(&v1), del]).unwrap();
    assert_eq!(written.deleted, vec![1], "target must be deleted");
    let blocked = store.write(&[Arc::clone(&v1)]).unwrap();
    assert_eq!(
        blocked.outcomes,
        vec![WriteOutcome::Deleted],
        "republish must be blocked"
    );
    // A newer version is accepted.
    let v2 = event(&k, 30_002, 250, &json!([["d", long_d]]), "v2");
    let newest = store.write(&[Arc::clone(&v2)]).unwrap();
    assert_eq!(
        newest.outcomes,
        vec![WriteOutcome::Stored { seq: 3 }],
        "newer version must be stored"
    );
}

#[test]
fn deletion_long_d_by_e() {
    let (_dir, store) = store();
    let k = key(1);
    let v1 = event(&k, 30_002, 100, &json!([["d", "d".repeat(300)]]), "v1");
    let del = event(&k, 5, 200, &json!([["e", format!("{}", v1.id())]]), "");
    assert_long_d_deletion(&store, del);
}

#[test]
fn deletion_long_d_by_a() {
    let (_dir, store) = store();
    let k = key(1);
    let pk = pk_hex(&k);
    let long_d = "d".repeat(300);
    let del = event(
        &k,
        5,
        200,
        &json!([["a", format!("30002:{pk}:{long_d}")]]),
        "",
    );
    assert_long_d_deletion(&store, del);
}

#[test]
fn deletion_block_lifts_when_kind5_is_deleted() {
    let (_dir, store) = store();
    let k = key(1);
    let note = event(&k, 1, 100, &json!([]), "gone");
    let kind5 = event(&k, 5, 200, &json!([["e", format!("{}", note.id())]]), "");
    let written = store
        .write(&[Arc::clone(&note), Arc::clone(&kind5)])
        .unwrap();
    assert_eq!(written.deleted, vec![1]);
    assert_eq!(
        store.write(&[Arc::clone(&note)]).unwrap().outcomes,
        vec![WriteOutcome::Deleted]
    );
    // Deleting the kind 5 itself removes its index rows: republish works.
    assert_eq!(store.delete(&[2]).unwrap(), 1);
    assert_eq!(
        store.write(&[Arc::clone(&note)]).unwrap().outcomes,
        vec![WriteOutcome::Stored { seq: 3 }]
    );
}

#[test]
fn deleted_reported_within_one_batch() {
    let (_dir, store) = store();
    let k = key(1);
    // An event and its deletion in one batch.
    let note = event(&k, 1, 100, &json!([]), "gone");
    let del = event(&k, 5, 200, &json!([["e", format!("{}", note.id())]]), "");
    let written = store.write(&[Arc::clone(&note), Arc::clone(&del)]).unwrap();
    assert_eq!(
        written.outcomes,
        vec![
            WriteOutcome::Stored { seq: 1 },
            WriteOutcome::Stored { seq: 2 }
        ]
    );
    assert_eq!(written.deleted, vec![1]);
    // A replaceable pair in one batch: the loser lands in `deleted`.
    let v1 = event(&k, 0, 100, &json!([]), "v1");
    let v2 = event(&k, 0, 200, &json!([]), "v2");
    let pair = store.write(&[v1, Arc::clone(&v2)]).unwrap();
    assert_eq!(pair.deleted, vec![3]);
    let got = collect(
        &store,
        Arc::from([Filter::parse("{\"kinds\":[0]}").unwrap()]),
        4,
    );
    assert_eq!(got, vec![v2.json().to_owned()]);
}

#[test]
fn expire_removes_due_events_and_respects_limit() {
    let (_dir, store) = store();
    let k = key(1);
    let e1 = event(&k, 1, 100, &json!([["expiration", "150"]]), "a");
    let e2 = event(&k, 1, 100, &json!([["expiration", "250"]]), "b");
    let e3 = event(&k, 1, 100, &json!([]), "c");
    store
        .write(&[Arc::clone(&e1), Arc::clone(&e2), Arc::clone(&e3)])
        .unwrap();
    // Nothing due yet; limit respected; future events left alone.
    assert_eq!(store.expire(100, 10).unwrap(), 0);
    assert_eq!(store.expire(200, 1).unwrap(), 1);
    assert_eq!(store.expire(200, 10).unwrap(), 0);
    assert_eq!(store.expire(300, 10).unwrap(), 1);
    let got = collect(&store, Arc::from([Filter::parse("{}").unwrap()]), 3);
    assert_eq!(got, vec![e3.json().to_owned()]);
    assert_eq!(
        export_lines(&store, 0, u64::MAX, false),
        vec![e3.json().to_owned()]
    );
}

#[test]
fn scan_hides_expired_and_they_do_not_consume_limits() {
    let (_dir, store) = store();
    let k = key(1);
    let expired = event(&k, 1, 200, &json!([["expiration", "300"]]), "old");
    let live = event(&k, 1, 100, &json!([]), "live");
    let snap = store
        .write(&[Arc::clone(&expired), Arc::clone(&live)])
        .map(|_| store.snapshot().unwrap())
        .unwrap();
    // now = 0 disables the cutoff: both visible, newest first.
    let all = collect_now(&store, Arc::from([Filter::parse("{}").unwrap()]), snap, 0);
    assert_eq!(all, vec![expired.json().to_owned(), live.json().to_owned()]);
    // now >= expiration: the expired event is hidden even in storage.
    let at_now = collect_now(&store, Arc::from([Filter::parse("{}").unwrap()]), snap, 400);
    assert_eq!(at_now, vec![live.json().to_owned()]);
    // It does not consume the limit either: newest-first with limit 1
    // still serves the live event, not the expired one.
    let limited = collect_now(
        &store,
        Arc::from([Filter::parse("{\"limit\":1}").unwrap()]),
        snap,
        400,
    );
    assert_eq!(limited, vec![live.json().to_owned()]);
    // Export stays a raw dump: expired events are exported until swept.
    assert_eq!(export_lines(&store, 0, u64::MAX, false).len(), 2);
}

#[test]
fn scan_gate_hides_restricted_and_they_do_not_consume_limits() {
    let (_dir, store) = store();
    let k = key(1);
    let dm = event(&k, 4, 200, &json!([["p", pk_hex(&key(2))]]), "dm");
    let note = event(&k, 1, 100, &json!([]), "note");
    let snap = store
        .write(&[Arc::clone(&dm), Arc::clone(&note)])
        .map(|_| store.snapshot().unwrap())
        .unwrap();
    let rules = Arc::new(nosfry_core::auth::AuthRules::new("", &[4], true).unwrap());
    let collect_gated = |authed: &[nosfry_core::event::PublicKey], limit: &str| {
        let gate = nosfry_core::auth::ReadGate::new(Arc::clone(&rules), Arc::from(authed));
        let filters = Arc::from([Filter::parse(limit).unwrap()]);
        let mut scan = Scan::new(filters, gate, snap, 500, 0);
        let out = RefCell::new(Vec::new());
        loop {
            let progress = scan
                .step(&store, Duration::ZERO, &mut |_seq, json| {
                    out.borrow_mut().push(json.to_owned());
                })
                .unwrap();
            if progress == Progress::Done {
                break;
            }
        }
        out.into_inner()
    };
    // Unauthenticated: the kind-4 event is hidden and does not count
    // toward `limit` — newest-first with limit 1 still serves the note.
    let hidden = collect_gated(&[], "{}");
    assert_eq!(hidden, vec![note.json().to_owned()]);
    let limited = collect_gated(&[], "{\"limit\":1}");
    assert_eq!(limited, vec![note.json().to_owned()]);
    // The p-tagged recipient sees both.
    let involved = collect_gated(
        &[nosfry_core::event::PublicKey(
            key(2).x_only_public_key().0.to_byte_array(),
        )],
        "{}",
    );
    assert_eq!(involved, vec![dm.json().to_owned(), note.json().to_owned()]);
}
