//! `SqlStore` unit tests: schema versioning, write outcomes, NIP-09
//! blocks, NIP-40 expiry, and the scan budget.

#![cfg(feature = "rusqlite")]
#![allow(
    unused_crate_dependencies,
    reason = "each test binary uses a subset of the workspace dependencies"
)]
#![allow(
    clippy::tests_outside_test_module,
    reason = "each file is a single integration test"
)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "tests may panic on failure"
)]

use std::cell::Cell;
use std::ops::ControlFlow;
use std::sync::Arc;

use nosfry_core::auth::ReadGate;
use nosfry_core::event::Event;
use nosfry_core::filter::Filter;
use nosfry_core::sign::{event, key, keys};
use nosfry_core::store::WriteOutcome;
use nosfry_sql::rusqlite::Connection;
use nosfry_sql::{Executor, FORMAT_VERSION, Param, SqlStore, StoreError, Value};
use serde_json::json;

fn store() -> SqlStore<Connection> {
    SqlStore::open(Connection::open_in_memory().unwrap()).unwrap()
}

fn filter(json: &str) -> Filter {
    Filter::parse(json).unwrap()
}

/// A [`Connection`] that counts the index rows its queries stream.
/// Cursor pages select `packed`; payload lookups do not.
#[derive(Debug)]
struct Counting {
    conn: Connection,
    rows: Cell<usize>,
}

impl Counting {
    fn open() -> Self {
        Self {
            conn: Connection::open_in_memory().unwrap(),
            rows: Cell::new(0),
        }
    }

    /// The index rows read since the last call.
    const fn take_index_rows(&self) -> usize {
        self.rows.replace(0)
    }
}

impl Executor for Counting {
    type Error = ::rusqlite::Error;

    fn query(
        &self,
        sql: &str,
        params: &[Param<'_>],
        row: &mut dyn FnMut(Vec<Value>) -> ControlFlow<()>,
    ) -> Result<(), Self::Error> {
        if !sql.contains("packed") {
            return self.conn.query(sql, params, row);
        }
        self.conn.query(sql, params, &mut |values| {
            self.rows.set(self.rows.get() + 1);
            row(values)
        })
    }

    fn execute(&self, sql: &str, params: &[Param<'_>]) -> Result<(), Self::Error> {
        self.conn.execute(sql, params)
    }

    fn transaction<T>(&self, f: impl FnOnce() -> Result<T, StoreError>) -> Result<T, StoreError> {
        self.conn.transaction(f)
    }
}

/// `MAX(seq)` — the SQL store has no snapshot concept.
fn max_seq<E: Executor>(store: &SqlStore<E>) -> u64 {
    let mut max = 0u64;
    store
        .exec()
        .query("SELECT MAX(seq) FROM events", &[], &mut |row| {
            if let Some(Value::Int(v)) = row.first() {
                max = u64::try_from(*v).unwrap();
            }
            ControlFlow::Continue(())
        })
        .unwrap();
    max
}

fn seqs(store: &SqlStore<Connection>, json: &str, now: u64) -> Vec<u64> {
    store
        .query(
            &[filter(json)],
            &ReadGate::default(),
            now,
            u64::MAX,
            usize::MAX,
        )
        .unwrap()
        .hits
        .iter()
        .map(|hit| hit.seq)
        .collect()
}

#[test]
fn open_stamps_and_verifies_the_format_version() {
    let store = store();
    let mut version = None;
    store
        .exec()
        .query(
            "SELECT value FROM meta WHERE key = 'format'",
            &[],
            &mut |row| {
                if let Some(Value::Int(v)) = row.first() {
                    version = Some(*v);
                }
                ControlFlow::Continue(())
            },
        )
        .unwrap();
    assert_eq!(version, Some(FORMAT_VERSION));

    // An unknown version is refused as corrupt.
    store
        .exec()
        .execute("UPDATE meta SET value = 99 WHERE key = 'format'", &[])
        .unwrap();
    let err = SqlStore::open(store.into_exec()).unwrap_err();
    assert!(matches!(err, StoreError::Corrupt(_)));
}

#[test]
fn write_outcomes_and_scan_order() {
    let store = store();
    let k = key(1);
    let a = event(&k, 1, 100, &json!([]), "a");
    let b = event(&k, 1, 200, &json!([]), "b");
    let c = event(&k, 1, 300, &json!([]), "c");
    let outcomes = store
        .write(&[
            Arc::clone(&a),
            Arc::clone(&b),
            Arc::clone(&c),
            Arc::clone(&a),
        ])
        .unwrap();
    assert!(matches!(outcomes[0], WriteOutcome::Stored { seq: 1 }));
    assert!(matches!(outcomes[1], WriteOutcome::Stored { seq: 2 }));
    assert!(matches!(outcomes[2], WriteOutcome::Stored { seq: 3 }));
    assert_eq!(outcomes[3], WriteOutcome::Duplicate);

    let hits = store
        .query(
            &[filter("{\"kinds\":[1]}")],
            &ReadGate::default(),
            0,
            u64::MAX,
            usize::MAX,
        )
        .unwrap();
    assert!(!hits.budget_exhausted);
    let jsons: Vec<&str> = hits.hits.iter().map(|h| h.json.as_ref()).collect();
    assert_eq!(jsons, vec![c.json(), b.json(), a.json()]);
}

#[test]
fn replaceable_supersedes_and_loses() {
    let store = store();
    let k = key(2);
    let old = event(&k, 0, 100, &json!([]), "old");
    let new = event(&k, 0, 200, &json!([]), "new");
    let outcomes = store.write(&[Arc::clone(&old), Arc::clone(&new)]).unwrap();
    assert!(matches!(outcomes[1], WriteOutcome::Stored { seq: 2 }));
    // The loser is superseded, not stored.
    let late = store.write(&[old]).unwrap();
    assert_eq!(late[0], WriteOutcome::Superseded);
    // Only the winner serves.
    let hits = store
        .query(
            &[filter("{\"kinds\":[0]}")],
            &ReadGate::default(),
            0,
            u64::MAX,
            usize::MAX,
        )
        .unwrap();
    assert_eq!(hits.hits.len(), 1);
    assert_eq!(hits.hits[0].json.as_ref(), new.json());
}

#[test]
fn nip09_blocks_and_deletes() {
    let store = store();
    let k = key(3);
    let victim = event(&k, 1, 100, &json!([]), "victim");
    let kept = event(&k, 1, 200, &json!([]), "kept");
    store
        .write(&[Arc::clone(&victim), Arc::clone(&kept)])
        .unwrap();
    let del = event(&k, 5, 300, &json!([["e", format!("{}", victim.id())]]), "");
    let out = store.write(&[del]).unwrap();
    assert!(matches!(out[0], WriteOutcome::Stored { .. }));
    // The victim is gone and stays blocked.
    let hits = store
        .query(
            &[filter("{\"kinds\":[1]}")],
            &ReadGate::default(),
            0,
            u64::MAX,
            usize::MAX,
        )
        .unwrap();
    assert_eq!(hits.hits.len(), 1);
    assert_eq!(hits.hits[0].json.as_ref(), kept.json());
    let blocked = store.write(&[victim]).unwrap();
    assert_eq!(blocked[0], WriteOutcome::Deleted);
}

#[test]
fn expire_hides_then_sweeps() {
    let store = store();
    let k = key(4);
    let dying = event(&k, 1, 100, &json!([["expiration", "150"]]), "dying");
    let alive = event(&k, 1, 200, &json!([]), "alive");
    store.write(&[dying, alive]).unwrap();
    // `now = 0` disables the check; `now = 200` hides the event even
    // before the sweep.
    assert_eq!(seqs(&store, "{\"kinds\":[1]}", 0), vec![2, 1]);
    assert_eq!(seqs(&store, "{\"kinds\":[1]}", 200), vec![2]);
    assert_eq!(store.expire(200, 10).unwrap(), 1);
    assert_eq!(max_seq(&store), 2);
}

#[test]
fn row_budget_bounds_rows_read() {
    let store = SqlStore::open(Counting::open()).unwrap();
    let k = key(5);
    let events: Vec<Arc<Event>> = (0..20u64)
        .map(|i| event(&k, 1, 100 + i, &json!([]), "e"))
        .collect();
    store.write(&events).unwrap();
    store.exec().take_index_rows();

    let scan = store
        .query(
            &[filter("{\"kinds\":[1]}")],
            &ReadGate::default(),
            0,
            u64::MAX,
            5,
        )
        .unwrap();
    assert!(scan.budget_exhausted);
    assert!(!scan.hits.is_empty());
    // Results stay newest-first even when truncated by the budget.
    let first = scan.hits.first().unwrap().json.clone();
    assert_eq!(first.as_ref(), events[19].json());
    // `max_rows` bounds rows read: the budget may overshoot by at
    // most the page in flight (here the 16-row first page).
    let read = store.exec().take_index_rows();
    assert!(read <= 5 + 16, "read {read} index rows for a 5-row budget");
}

#[test]
fn first_page_scales_with_the_cursor_count() {
    let store = SqlStore::open(Counting::open()).unwrap();
    let ks = keys();
    let events: Vec<Arc<Event>> = (0..40u64)
        .map(|i| {
            event(
                &ks[usize::try_from(i).unwrap() % 4],
                1,
                100 + i,
                &json!([]),
                "e",
            )
        })
        .collect();
    store.write(&events).unwrap();
    let authors = (0..4)
        .map(nosfry_core::sign::pubkey_hex)
        .map(|pk| format!("\"{pk}\""))
        .collect::<Vec<_>>()
        .join(",");
    store.exec().take_index_rows();
    // limit 4 over 4 author cursors: first pages of 1 row each read
    // about `limit` rows, not 4 × 16.
    let scan = store
        .query(
            &[filter(&format!("{{\"authors\":[{authors}]}}"))],
            &ReadGate::default(),
            0,
            4,
            usize::MAX,
        )
        .unwrap();
    assert_eq!(scan.hits.len(), 4);
    let read = store.exec().take_index_rows();
    assert!(read <= 24, "read {read} index rows for limit 4");
}

#[test]
fn int_params_outside_the_js_range_are_rejected() {
    let store = store();
    let mut err = None;
    let result = store
        .exec()
        .query("SELECT ?1", &[Param::Int(i64::MAX)], &mut |_| {
            ControlFlow::Continue(())
        });
    if let Err(e) = result {
        err = Some(e);
    }
    let err = err.unwrap();
    assert!(err.to_string().contains("JavaScript-safe"), "{err}");
}
