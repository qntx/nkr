//! Schema, writes, expiry and the shared row helpers.

use std::ops::ControlFlow;
use std::sync::Arc;

use nosfry_core::auth::ReadGate;
use nosfry_core::deletion::DeletionTarget;
use nosfry_core::event::{Event, EventId, PublicKey, UnverifiedEvent, supersedes};
use nosfry_core::filter::Filter;
use nosfry_core::packed::PackedRef;
use nosfry_core::store::WriteOutcome;

use crate::exec::{Executor, Param, Value};
use crate::{Scan, StoreError};

/// The on-disk schema version `meta.format` must hold.
pub const FORMAT_VERSION: i64 = 1;

/// The largest integer JavaScript exchanges losslessly (`2^53 - 1`).
/// Every integer bound into SQL is clamped to it; a `created_at`
/// above it cannot be stored at all.
pub(crate) const JS_MAX: u64 = (1 << 53) - 1;

/// `v` as a SQL integer parameter, clamped to the JavaScript-safe
/// range: workerd refuses integers it cannot exchange losslessly.
pub(crate) fn int(v: u64) -> Param<'static> {
    Param::Int(i64::try_from(v.min(JS_MAX)).unwrap_or(i64::MAX))
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value INTEGER NOT NULL
    ) WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        id BLOB NOT NULL UNIQUE,
        pubkey BLOB NOT NULL,
        created_at INTEGER NOT NULL,
        kind INTEGER NOT NULL,
        address BLOB UNIQUE,
        expiration INTEGER,
        packed BLOB NOT NULL,
        json TEXT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS ix_time ON events (created_at)",
    "CREATE INDEX IF NOT EXISTS ix_pubkey ON events (pubkey, created_at)",
    "CREATE INDEX IF NOT EXISTS ix_kind ON events (kind, created_at)",
    "CREATE INDEX IF NOT EXISTS ix_pubkey_kind ON events (pubkey, kind, created_at)",
    "CREATE INDEX IF NOT EXISTS ix_expiration ON events (expiration) WHERE expiration IS NOT NULL",
    "CREATE TABLE IF NOT EXISTS tags (
        name INTEGER NOT NULL,
        value BLOB NOT NULL,
        created_at INTEGER NOT NULL,
        seq INTEGER NOT NULL,
        PRIMARY KEY (name, value, created_at, seq)
    ) WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS deletions (
        target BLOB NOT NULL,
        author BLOB NOT NULL,
        carrier INTEGER NOT NULL,
        PRIMARY KEY (target, author, carrier)
    ) WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS replace_deletions (
        address BLOB NOT NULL,
        deleted_at INTEGER NOT NULL,
        carrier INTEGER NOT NULL,
        PRIMARY KEY (address, deleted_at, carrier)
    ) WITHOUT ROWID",
];

/// The event store: writes, scans and expiry over an [`Executor`].
#[derive(Debug)]
pub struct SqlStore<E> {
    exec: E,
}

impl<E: Executor> SqlStore<E> {
    /// Open the store on `exec`, creating the schema when absent. An
    /// existing `meta.format` other than [`FORMAT_VERSION`] is
    /// refused; until the first release, schema changes replace the
    /// schema — there are no migrations.
    ///
    /// # Errors
    ///
    /// [`StoreError::Sql`] on executor failure,
    /// [`StoreError::Corrupt`] on an unknown format version.
    pub fn open(exec: E) -> Result<Self, StoreError> {
        let store = Self { exec };
        store.exec.transaction(|| {
            for statement in SCHEMA {
                store.exec_sql(statement, &[])?;
            }
            let mut versions = Vec::new();
            store.query_rows("SELECT value FROM meta WHERE key = 'format'", &[], |row| {
                versions.extend_from_slice(row);
                Ok(())
            })?;
            match versions.first() {
                None => store.exec_sql(
                    "INSERT INTO meta (key, value) VALUES ('format', ?1)",
                    &[Param::Int(FORMAT_VERSION)],
                ),
                Some(Value::Int(v)) if *v == FORMAT_VERSION => Ok(()),
                Some(_) => Err(StoreError::Corrupt("unknown store format version")),
            }
        })?;
        Ok(store)
    }

    /// Apply `events` in arrival order inside a single transaction,
    /// one [`WriteOutcome`] per input.
    ///
    /// # Errors
    ///
    /// [`StoreError::Sql`] on executor failure,
    /// [`StoreError::OutOfRange`] on a `created_at` above the
    /// JavaScript-safe integer range, [`StoreError::Corrupt`] on a
    /// broken index invariant.
    pub fn write(&self, events: &[Arc<Event>]) -> Result<Vec<WriteOutcome>, StoreError> {
        self.exec
            .transaction(|| events.iter().map(|event| self.apply(event)).collect())
    }

    /// Delete every event whose NIP-40 `expiration` is `<= now`, at
    /// most `limit` of them, in one transaction; returns the deleted
    /// count.
    ///
    /// # Errors
    ///
    /// [`StoreError::Sql`] on executor failure,
    /// [`StoreError::Corrupt`] on a broken index invariant.
    pub fn expire(&self, now: u64, limit: usize) -> Result<usize, StoreError> {
        self.exec.transaction(|| {
            let mut seqs = Vec::new();
            self.query_rows(
                "SELECT seq FROM events INDEXED BY ix_expiration
                 WHERE expiration <= ?1 ORDER BY expiration LIMIT ?2",
                &[int(now), int(limit as u64)],
                |row| {
                    seqs.push(uint(row, 0)?);
                    Ok(())
                },
            )?;
            for seq in &seqs {
                self.delete_seq(*seq)?;
            }
            Ok(seqs.len())
        })
    }

    /// The earliest stored NIP-40 `expiration`, or `None` when
    /// nothing expires — the next alarm time after
    /// [`SqlStore::expire`]. `ix_expiration` serves the `MIN`.
    ///
    /// # Errors
    ///
    /// [`StoreError::Sql`] on executor failure,
    /// [`StoreError::Corrupt`] on a broken row shape.
    pub fn next_expiration(&self) -> Result<Option<u64>, StoreError> {
        let row = self.row(
            "SELECT MIN(expiration) FROM events WHERE expiration IS NOT NULL",
            &[],
        )?;
        match row.as_deref() {
            // MIN over an empty set is NULL.
            Some([Value::Null] | []) | None => Ok(None),
            Some(row) => Ok(Some(uint(row, 0)?)),
        }
    }

    /// The underlying executor, for plan inspection and raw
    /// statements the store does not cover.
    pub const fn exec(&self) -> &E {
        &self.exec
    }

    /// Consume the store and return its executor.
    pub fn into_exec(self) -> E {
        self.exec
    }

    /// The stored events matching `filters`, per filter newest-first
    /// and deduplicated by seq across filters, fetched with at most
    /// `max_rows` index rows examined. See [`crate::Scan`] for the
    /// budget semantics.
    ///
    /// # Errors
    ///
    /// [`StoreError::Sql`] on executor failure,
    /// [`StoreError::Corrupt`] on a broken index invariant.
    pub fn query(
        &self,
        filters: &[Filter],
        gate: &ReadGate,
        now: u64,
        max_limit: u64,
        max_rows: usize,
    ) -> Result<Scan, StoreError> {
        crate::scan::scan(&self.exec, filters, gate, now, max_limit, max_rows)
    }

    /// Apply one event inside the write transaction: ephemeral and
    /// duplicate ids are skipped, a NIP-09-blocked event is `Deleted`,
    /// a losing replace is `Superseded`, a winner deletes its
    /// predecessor before the insert, and a stored kind 5 deletes its
    /// targets. Identical to the LMDB store minus NIP-62 vanish,
    /// which this slice does not implement.
    fn apply(&self, event: &Event) -> Result<WriteOutcome, StoreError> {
        if event.kind().is_ephemeral() {
            return Ok(WriteOutcome::Ephemeral);
        }
        if event.created_at() > JS_MAX {
            return Err(StoreError::OutOfRange(
                "created_at exceeds the JavaScript-safe integer range",
            ));
        }
        let id = event.id().0;
        if self.exists("SELECT 1 FROM events WHERE id = ?1", &[Param::Blob(&id)])? {
            return Ok(WriteOutcome::Duplicate);
        }
        // NIP-09: an id covered by a stored same-author kind 5 stays
        // deleted; a kind 5 itself is never blocked, and a kind 62
        // request to vanish is never blocked either (NIP-62).
        let pubkey = event.pubkey().0;
        if event.kind().0 != 5
            && event.kind().0 != 62
            && self.exists(
                "SELECT 1 FROM deletions WHERE target = ?1 AND author = ?2 LIMIT 1",
                &[Param::Blob(&id), Param::Blob(&pubkey)],
            )?
        {
            return Ok(WriteOutcome::Deleted);
        }
        // NIP-09: an `a` deletion blocks equal-or-older versions.
        let created = int(event.created_at());
        if let Some(address) = event.address()
            && self.exists(
                "SELECT 1 FROM replace_deletions
                 WHERE address = ?1 AND deleted_at >= ?2 LIMIT 1",
                &[Param::Blob(&address), created],
            )?
        {
            return Ok(WriteOutcome::Deleted);
        }
        if let Some(address) = event.address()
            && let Some(old) = self.row(
                "SELECT seq, packed FROM events WHERE address = ?1",
                &[Param::Blob(&address)],
            )?
        {
            let old_seq = uint(&old, 0)?;
            let packed = record(&old, 1)?;
            if !supersedes(
                (event.created_at(), &event.id()),
                (packed.created_at(), &packed.id()),
            ) {
                return Ok(WriteOutcome::Superseded);
            }
            self.delete_seq(old_seq)?;
        }
        let address = event.address();
        self.exec_sql(
            "INSERT INTO events (id, pubkey, created_at, kind, address, expiration, packed, json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            &[
                Param::Blob(&id),
                Param::Blob(&pubkey),
                created,
                Param::Int(i64::from(event.kind().0)),
                address
                    .as_ref()
                    .map_or(Param::Null, |a| Param::Blob(a.as_slice())),
                event.expiration().map_or(Param::Null, int),
                Param::Blob(event.packed().as_bytes()),
                Param::Text(event.json()),
            ],
        )?;
        let seq = self.last_rowid()?;
        for (name, value) in event.packed().tags() {
            self.exec_sql(
                "INSERT OR IGNORE INTO tags (name, value, created_at, seq)
                 VALUES (?1, ?2, ?3, ?4)",
                &[
                    Param::Int(i64::from(name)),
                    Param::Blob(value),
                    created,
                    int(seq),
                ],
            )?;
        }
        for target in event.deletion_targets() {
            match target {
                DeletionTarget::Event(target) => self.exec_sql(
                    "INSERT OR IGNORE INTO deletions (target, author, carrier)
                     VALUES (?1, ?2, ?3)",
                    &[Param::Blob(&target.0), Param::Blob(&pubkey), int(seq)],
                )?,
                DeletionTarget::Address(addr) => self.exec_sql(
                    "INSERT OR IGNORE INTO replace_deletions (address, deleted_at, carrier)
                     VALUES (?1, ?2, ?3)",
                    &[Param::Blob(addr), created, int(seq)],
                )?,
            }
        }
        if event.kind().0 == 5 {
            self.apply_deletions(event)?;
        }
        Ok(WriteOutcome::Stored { seq })
    }

    /// Apply every target of a stored kind-5 `event` (NIP-09). Targets
    /// are processed after the kind 5 itself is inserted.
    fn apply_deletions(&self, event: &Event) -> Result<(), StoreError> {
        for target in event.deletion_targets() {
            match target {
                DeletionTarget::Event(id) => self.delete_event_target(id, &event.pubkey())?,
                DeletionTarget::Address(address) => {
                    self.delete_address_target(address, event.created_at(), &event.pubkey())?;
                }
            }
        }
        Ok(())
    }

    /// Delete the `e`-tagged event `id` when it is `author`'s own and
    /// neither a kind 5 (NIP-09: a deletion request cannot delete a
    /// deletion request) nor a kind 62 (NIP-62: a request to vanish
    /// cannot be deleted).
    fn delete_event_target(&self, id: &EventId, author: &PublicKey) -> Result<(), StoreError> {
        let Some(row) = self.row(
            "SELECT seq, packed FROM events WHERE id = ?1",
            &[Param::Blob(&id.0)],
        )?
        else {
            return Ok(());
        };
        let seq = uint(&row, 0)?;
        let deletable = {
            let packed = record(&row, 1)?;
            packed.pubkey() == *author && packed.kind().0 != 5 && packed.kind().0 != 62
        };
        if deletable {
            self.delete_seq(seq)?;
        }
        Ok(())
    }

    /// Delete the stored version at `address` when it is `author`'s
    /// own and `created_at <= deleted_at` (the kind-5 timestamp).
    fn delete_address_target(
        &self,
        address: &[u8; 32],
        deleted_at: u64,
        author: &PublicKey,
    ) -> Result<(), StoreError> {
        let Some(row) = self.row(
            "SELECT seq, packed FROM events WHERE address = ?1",
            &[Param::Blob(address)],
        )?
        else {
            return Ok(());
        };
        let seq = uint(&row, 0)?;
        let deletable = {
            let packed = record(&row, 1)?;
            packed.pubkey() == *author && packed.created_at() <= deleted_at
        };
        if deletable {
            self.delete_seq(seq)?;
        }
        Ok(())
    }

    /// Remove every row `seq` carries; `false` when it is already
    /// gone. Tag keys are derived from the stored packed record; only
    /// a kind 5 carries deletion blocks, and only its payload needs
    /// the structural JSON parse (the packed record alone cannot
    /// reproduce an `e` or `a` value longer than 255 bytes).
    fn delete_seq(&self, seq: u64) -> Result<bool, StoreError> {
        let Some(row) = self.row(
            "SELECT packed, json FROM events WHERE seq = ?1",
            &[int(seq)],
        )?
        else {
            return Ok(false);
        };
        let packed = record(&row, 0)?;
        let created = int(packed.created_at());
        let author = packed.pubkey().0;
        let seqp = int(seq);
        for (name, value) in packed.tags() {
            self.exec_sql(
                "DELETE FROM tags
                 WHERE name = ?1 AND value = ?2 AND created_at = ?3 AND seq = ?4",
                &[
                    Param::Int(i64::from(name)),
                    Param::Blob(value),
                    created,
                    seqp,
                ],
            )?;
        }
        if packed.kind().0 == 5 {
            let unverified = UnverifiedEvent::parse_structural(text(&row, 1)?)
                .map_err(|_| StoreError::Corrupt("stored payload fails structural parse"))?;
            for target in unverified.deletion_targets() {
                match target {
                    DeletionTarget::Event(id) => self.exec_sql(
                        "DELETE FROM deletions
                         WHERE target = ?1 AND author = ?2 AND carrier = ?3",
                        &[Param::Blob(&id.0), Param::Blob(&author), seqp],
                    )?,
                    DeletionTarget::Address(addr) => self.exec_sql(
                        "DELETE FROM replace_deletions
                         WHERE address = ?1 AND deleted_at = ?2 AND carrier = ?3",
                        &[Param::Blob(&addr), created, seqp],
                    )?,
                }
            }
        }
        self.exec_sql("DELETE FROM events WHERE seq = ?1", &[seqp])?;
        Ok(true)
    }

    /// The seq `AUTOINCREMENT` assigned to the just-inserted event.
    fn last_rowid(&self) -> Result<u64, StoreError> {
        let row = self
            .row("SELECT last_insert_rowid()", &[])?
            .ok_or(StoreError::Corrupt("last_insert_rowid returned no row"))?;
        uint(&row, 0)
    }

    /// Execute one statement, boxing the executor error.
    fn exec_sql(&self, sql: &str, params: &[Param<'_>]) -> Result<(), StoreError> {
        self.exec.execute(sql, params).map_err(StoreError::sql)
    }

    /// Whether `sql` produces at least one row.
    fn exists(&self, sql: &str, params: &[Param<'_>]) -> Result<bool, StoreError> {
        let mut found = false;
        self.exec
            .query(sql, params, &mut |_| {
                found = true;
                ControlFlow::Break(())
            })
            .map_err(StoreError::sql)?;
        Ok(found)
    }

    /// The first row `sql` produces, if any.
    fn row(&self, sql: &str, params: &[Param<'_>]) -> Result<Option<Vec<Value>>, StoreError> {
        let mut first = None;
        self.exec
            .query(sql, params, &mut |row| {
                first = Some(row);
                ControlFlow::Break(())
            })
            .map_err(StoreError::sql)?;
        Ok(first)
    }

    /// Stream every row of `sql` through a fallible visitor.
    fn query_rows(
        &self,
        sql: &str,
        params: &[Param<'_>],
        mut visit: impl FnMut(&[Value]) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let mut failure = None;
        let result = self.exec.query(sql, params, &mut |row| match visit(&row) {
            Ok(()) => ControlFlow::Continue(()),
            Err(err) => {
                failure = Some(err);
                ControlFlow::Break(())
            }
        });
        match (result, failure) {
            (Err(err), _) => Err(StoreError::sql(err)),
            (Ok(()), Some(err)) => Err(err),
            (Ok(()), None) => Ok(()),
        }
    }
}

/// Column `i` of `row` as an unsigned integer.
pub(crate) fn uint(row: &[Value], i: usize) -> Result<u64, StoreError> {
    match row.get(i) {
        Some(Value::Int(v)) if *v >= 0 => Ok(u64::try_from(*v).unwrap_or_default()),
        _ => Err(StoreError::Corrupt("row column is not an integer")),
    }
}

/// Column `i` of `row` as a validated packed record.
pub(crate) fn record(row: &[Value], i: usize) -> Result<PackedRef<'_>, StoreError> {
    match row.get(i) {
        Some(Value::Blob(bytes)) => {
            PackedRef::new(bytes).ok_or(StoreError::Corrupt("bad packed record"))
        }
        _ => Err(StoreError::Corrupt("row column is not a blob")),
    }
}

/// Column `i` of `row` as text.
pub(crate) fn text(row: &[Value], i: usize) -> Result<&str, StoreError> {
    match row.get(i) {
        Some(Value::Text(s)) => Ok(s.as_str()),
        _ => Err(StoreError::Corrupt("row column is not text")),
    }
}
