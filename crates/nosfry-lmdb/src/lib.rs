//! `nosfry-lmdb` — synchronous LMDB event store for the `nosfry` protocol core.
//!
//! One writer at a time (the server owns the writer thread); readers take
//! TLS-less read transactions. Layout (all integers big-endian, `Bytes`
//! codec):
//!
//! | db | key | value |
//! |---|---|---|
//! | meta | `b"format"` / `b"next_seq"` | u32 / u64 |
//! | events | seq | packed record |
//! | payloads | seq | canonical JSON |
//! | ix_id | id | seq |
//! | ix_time | created_at ‖ seq | id |
//! | ix_pubkey | pubkey ‖ created_at ‖ seq | — |
//! | ix_kind | kind ‖ created_at ‖ seq | — |
//! | ix_pubkey_kind | pubkey ‖ kind ‖ created_at ‖ seq | — |
//! | ix_tag | name ‖ len ‖ packed value ‖ created_at ‖ seq | — |
//! | ix_replace | `Event::address` | seq |
#![cfg_attr(
    test,
    allow(
        unused_crate_dependencies,
        reason = "dev-deps are exercised by integration tests and benches"
    )
)]

use std::path::Path;
use std::sync::Arc;

use heed::types::Bytes;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RoTxn, RwTxn, WithoutTls};
use nosfry::event::{Event, supersedes};
use nosfry::packed::PackedRef;

mod scan;

pub use scan::{Progress, Scan};

/// On-disk format version, stored under the `format` meta key.
pub const FORMAT_VERSION: u32 = 1;

type BytesDb = Database<Bytes, Bytes>;
type Reader<'a> = RoTxn<'a, WithoutTls>;

/// LMDB environment options.
#[derive(Debug, Clone, Copy)]
pub struct StoreOptions {
    /// Memory map size in bytes.
    pub map_size: usize,
    /// Maximum reader slots.
    pub max_readers: u32,
    /// Disable OS read-ahead on the map (`MDB_NORDAHEAD`).
    pub no_read_ahead: bool,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            map_size: 10 << 40, // 10 TiB
            max_readers: 256,
            no_read_ahead: false,
        }
    }
}

/// Outcome of writing one event, in arrival order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// Stored under a fresh sequence number.
    Stored {
        /// The assigned sequence number.
        seq: u64,
    },
    /// The id was already stored.
    Duplicate,
    /// An equal-or-better replaceable/addressable version is stored.
    Superseded,
    /// Ephemeral kind; nothing written.
    Ephemeral,
}

/// Store failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// LMDB error.
    #[error("lmdb: {0}")]
    Heed(#[from] heed::Error),
    /// Filesystem error.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The on-disk format differs from [`FORMAT_VERSION`].
    #[error("format version mismatch: found {found}, expected {expected}")]
    FormatMismatch {
        /// The stored format version.
        found: u32,
        /// The version this build understands.
        expected: u32,
    },
    /// An internal invariant is broken.
    #[error("corrupt store: {0}")]
    Corrupt(&'static str),
}

/// Which index database an [`IndexEntry`] belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Index {
    /// `ix_id`: `id` → `seq`.
    Id,
    /// `ix_time`: `created_at` ‖ `seq` → `id`.
    Time,
    /// `ix_pubkey`: `pubkey` ‖ `created_at` ‖ `seq`.
    Pubkey,
    /// `ix_kind`: `kind` ‖ `created_at` ‖ `seq`.
    Kind,
    /// `ix_pubkey_kind`: `pubkey` ‖ `kind` ‖ `created_at` ‖ `seq`.
    PubkeyKind,
    /// `ix_tag`: `name` ‖ `len` ‖ `value` ‖ `created_at` ‖ `seq`.
    Tag,
    /// `ix_replace`: `address` → `seq`.
    Replace,
}

/// Derive every index entry of an event from its packed record and
/// sequence number. Insert and delete share this so they cannot diverge.
///
/// `address` is the event's replaceable/addressable key
/// ([`Event::address`]); on the delete path it is the address under which
/// the old event was found — the packed record alone cannot reproduce it
/// when the `d` value exceeds the 255-byte packed limit.
fn index_entries(
    packed: PackedRef<'_>,
    address: Option<[u8; 32]>,
    seq: u64,
    out: &mut Vec<(Index, Vec<u8>, Vec<u8>)>,
) {
    let ca = packed.created_at().to_be_bytes();
    let seqb = seq.to_be_bytes();
    let pk = packed.pubkey().0;
    let kind = packed.kind().0.to_be_bytes();

    out.push((Index::Id, packed.id().0.to_vec(), seqb.to_vec()));

    let mut time_key = Vec::with_capacity(16);
    time_key.extend_from_slice(&ca);
    time_key.extend_from_slice(&seqb);
    out.push((Index::Time, time_key, packed.id().0.to_vec()));

    let mut pubkey_key = Vec::with_capacity(48);
    pubkey_key.extend_from_slice(&pk);
    pubkey_key.extend_from_slice(&ca);
    pubkey_key.extend_from_slice(&seqb);
    out.push((Index::Pubkey, pubkey_key, Vec::new()));

    let mut kind_key = Vec::with_capacity(18);
    kind_key.extend_from_slice(&kind);
    kind_key.extend_from_slice(&ca);
    kind_key.extend_from_slice(&seqb);
    out.push((Index::Kind, kind_key, Vec::new()));

    let mut pk_kind_key = Vec::with_capacity(50);
    pk_kind_key.extend_from_slice(&pk);
    pk_kind_key.extend_from_slice(&kind);
    pk_kind_key.extend_from_slice(&ca);
    pk_kind_key.extend_from_slice(&seqb);
    out.push((Index::PubkeyKind, pk_kind_key, Vec::new()));

    for (name, value) in packed.tags() {
        let mut tag_key = Vec::with_capacity(18 + value.len());
        tag_key.push(name);
        tag_key.push(u8::try_from(value.len()).unwrap_or(u8::MAX));
        tag_key.extend_from_slice(value);
        tag_key.extend_from_slice(&ca);
        tag_key.extend_from_slice(&seqb);
        out.push((Index::Tag, tag_key, Vec::new()));
    }

    if let Some(address) = address {
        out.push((Index::Replace, address.to_vec(), seqb.to_vec()));
    }
}

/// The LMDB store.
#[derive(Debug)]
pub struct Store {
    env: Env<WithoutTls>,
    meta: BytesDb,
    pub(crate) events: BytesDb,
    pub(crate) payloads: BytesDb,
    ix_id: BytesDb,
    ix_time: BytesDb,
    ix_pubkey: BytesDb,
    ix_kind: BytesDb,
    ix_pubkey_kind: BytesDb,
    ix_tag: BytesDb,
    ix_replace: BytesDb,
}

impl Store {
    /// Open (or create) a store at `path`, checking the format version.
    ///
    /// # Errors
    ///
    /// [`StoreError::FormatMismatch`] when the on-disk format differs,
    /// [`StoreError::Io`] when the directory cannot be created, and
    /// [`StoreError::Heed`] for LMDB failures.
    #[expect(
        unsafe_code,
        reason = "heed maps the database file: the caller must not open the same env twice in one process nor let other code modify the files incompatibly"
    )]
    pub fn open(path: &Path, opts: &StoreOptions) -> Result<Self, StoreError> {
        std::fs::create_dir_all(path)?;
        let mut options = EnvOpenOptions::new().read_txn_without_tls();
        options
            .map_size(opts.map_size)
            .max_readers(opts.max_readers)
            .max_dbs(16);
        if opts.no_read_ahead {
            // SAFETY: heed requires env flags to be set before opening.
            unsafe { options.flags(EnvFlags::NO_READ_AHEAD) };
        }
        // SAFETY: the same env is not opened twice in one process, and the
        // map file is not modified incompatibly by other code.
        let env = unsafe { options.open(path)? };

        let mut wtxn = env.write_txn()?;
        let mut open_or_create = |name: &'static str| -> Result<BytesDb, StoreError> {
            match env.open_database::<Bytes, Bytes>(&wtxn, Some(name))? {
                Some(db) => Ok(db),
                None => Ok(env.create_database::<Bytes, Bytes>(&mut wtxn, Some(name))?),
            }
        };
        let meta = open_or_create("meta")?;
        let events = open_or_create("events")?;
        let payloads = open_or_create("payloads")?;
        let ix_id = open_or_create("ix_id")?;
        let ix_time = open_or_create("ix_time")?;
        let ix_pubkey = open_or_create("ix_pubkey")?;
        let ix_kind = open_or_create("ix_kind")?;
        let ix_pubkey_kind = open_or_create("ix_pubkey_kind")?;
        let ix_tag = open_or_create("ix_tag")?;
        let ix_replace = open_or_create("ix_replace")?;
        if let Some(bytes) = meta.get(&wtxn, b"format")? {
            let found = u32::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| StoreError::Corrupt("bad format value"))?,
            );
            if found != FORMAT_VERSION {
                return Err(StoreError::FormatMismatch {
                    found,
                    expected: FORMAT_VERSION,
                });
            }
        } else {
            meta.put(&mut wtxn, b"format", &FORMAT_VERSION.to_be_bytes())?;
            meta.put(&mut wtxn, b"next_seq", &1u64.to_be_bytes())?;
        }
        wtxn.commit()?;
        Ok(Self {
            env,
            meta,
            events,
            payloads,
            ix_id,
            ix_time,
            ix_pubkey,
            ix_kind,
            ix_pubkey_kind,
            ix_tag,
            ix_replace,
        })
    }

    /// Apply `events` in arrival order inside a single write transaction.
    ///
    /// # Errors
    ///
    /// [`StoreError::Heed`] on LMDB failure, [`StoreError::Corrupt`] on a
    /// broken index invariant.
    pub fn write(&self, events: &[Arc<Event>]) -> Result<Vec<WriteOutcome>, StoreError> {
        let mut txn = self.env.write_txn()?;
        let mut next_seq = self.next_seq(&txn)?;
        let mut outcomes = Vec::with_capacity(events.len());
        for event in events {
            outcomes.push(self.apply(&mut txn, event, &mut next_seq)?);
        }
        self.meta
            .put(&mut txn, b"next_seq", &next_seq.to_be_bytes())?;
        txn.commit()?;
        Ok(outcomes)
    }

    /// Apply one event inside the write transaction: ephemeral and
    /// duplicate ids are skipped, a losing replace is `Superseded`, and
    /// a winner deletes its predecessor before the insert.
    fn apply(
        &self,
        txn: &mut RwTxn<'_>,
        event: &Arc<Event>,
        next_seq: &mut u64,
    ) -> Result<WriteOutcome, StoreError> {
        if event.kind().is_ephemeral() {
            return Ok(WriteOutcome::Ephemeral);
        }
        if self.ix_id.get(txn, &event.id().0)?.is_some() {
            return Ok(WriteOutcome::Duplicate);
        }
        if let Some(address) = event.address()
            && let Some((old_seq, old_bytes)) = self.stored_version(txn, &address)?
        {
            let old = PackedRef::new(&old_bytes).ok_or(StoreError::Corrupt("bad packed record"))?;
            if !supersedes(
                (event.created_at(), &event.id()),
                (old.created_at(), &old.id()),
            ) {
                return Ok(WriteOutcome::Superseded);
            }
            self.delete(txn, old, old_seq, &address)?;
        }
        let seq = *next_seq;
        *next_seq = next_seq
            .checked_add(1)
            .ok_or(StoreError::Corrupt("seq overflow"))?;
        self.insert(txn, event, seq)?;
        Ok(WriteOutcome::Stored { seq })
    }

    /// The `(seq, packed bytes)` of the stored event `ix_replace` points
    /// at for `address`; the record load doubles as an index-consistency
    /// check.
    fn stored_version(
        &self,
        txn: &RwTxn<'_>,
        address: &[u8; 32],
    ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let Some(old_seq) = self.ix_replace.get(txn, address)? else {
            return Ok(None);
        };
        let old_seq = decode_seq(old_seq)?;
        let bytes = self
            .events
            .get(txn, &old_seq.to_be_bytes())?
            .ok_or(StoreError::Corrupt("ix_replace points at a missing event"))?
            .to_vec();
        Ok(Some((old_seq, bytes)))
    }

    /// The highest assigned sequence number (`0` when empty); the upper
    /// bound scans observe.
    ///
    /// # Errors
    ///
    /// [`StoreError::Heed`] on LMDB failure.
    pub fn snapshot(&self) -> Result<u64, StoreError> {
        let txn = self.env.read_txn()?;
        Ok(self.next_seq(&txn)?.saturating_sub(1))
    }

    /// A TLS-less read transaction; callers drop it before returning.
    pub(crate) fn read_txn(&self) -> Result<Reader<'_>, StoreError> {
        Ok(self.env.read_txn()?)
    }

    fn next_seq(&self, txn: &Reader<'_>) -> Result<u64, StoreError> {
        let bytes = self
            .meta
            .get(txn, b"next_seq")?
            .ok_or(StoreError::Corrupt("missing next_seq"))?;
        decode_seq(bytes)
    }

    pub(crate) const fn db(&self, index: Index) -> &BytesDb {
        match index {
            Index::Id => &self.ix_id,
            Index::Time => &self.ix_time,
            Index::Pubkey => &self.ix_pubkey,
            Index::Kind => &self.ix_kind,
            Index::PubkeyKind => &self.ix_pubkey_kind,
            Index::Tag => &self.ix_tag,
            Index::Replace => &self.ix_replace,
        }
    }

    fn insert(&self, txn: &mut RwTxn<'_>, event: &Event, seq: u64) -> Result<(), StoreError> {
        self.events
            .put(txn, &seq.to_be_bytes(), event.packed().as_bytes())?;
        self.payloads
            .put(txn, &seq.to_be_bytes(), event.json().as_bytes())?;
        let mut entries = Vec::new();
        index_entries(event.packed(), event.address(), seq, &mut entries);
        for (index, key, value) in entries {
            self.db(index).put(txn, &key, &value)?;
        }
        Ok(())
    }

    /// Remove every key belonging to `packed`/`seq`. `address` is the
    /// `ix_replace` key the old event was found under.
    fn delete(
        &self,
        txn: &mut RwTxn<'_>,
        packed: PackedRef<'_>,
        seq: u64,
        address: &[u8; 32],
    ) -> Result<(), StoreError> {
        let mut entries = Vec::new();
        index_entries(packed, Some(*address), seq, &mut entries);
        for (index, key, _) in entries {
            self.db(index).delete(txn, &key)?;
        }
        self.events.delete(txn, &seq.to_be_bytes())?;
        self.payloads.delete(txn, &seq.to_be_bytes())?;
        Ok(())
    }
}

fn decode_seq(bytes: &[u8]) -> Result<u64, StoreError> {
    let arr: &[u8; 8] = bytes
        .try_into()
        .map_err(|_| StoreError::Corrupt("bad seq value"))?;
    Ok(u64::from_be_bytes(*arr))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        reason = "unit tests may panic on failure"
    )]

    use std::sync::Arc;

    use nosfry::event::{EventLimits, UnverifiedEvent};
    use secp256k1::{Keypair, SecretKey, schnorr};
    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::*;

    fn key(n: u8) -> Keypair {
        let mut bytes = [0u8; 32];
        bytes[31] = n;
        Keypair::from_secret_key(&SecretKey::from_secret_bytes(bytes).unwrap())
    }

    fn event(
        keypair: &Keypair,
        kind: u16,
        created_at: u64,
        tags: &serde_json::Value,
        content: &str,
    ) -> Arc<Event> {
        let pubkey = faster_hex::hex_string(&keypair.x_only_public_key().0.to_byte_array());
        let preimage =
            serde_json::to_string(&json!([0, pubkey, created_at, kind, tags, content])).unwrap();
        let id = Sha256::digest(preimage.as_bytes());
        let sig = schnorr::sign_no_aux_rand(&id, keypair);
        let event_json = serde_json::to_string(&json!({
            "id": faster_hex::hex_string(&id),
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": kind,
            "tags": tags,
            "content": content,
            "sig": faster_hex::hex_string(&sig.to_byte_array()),
        }))
        .unwrap();
        Arc::new(
            UnverifiedEvent::parse(&event_json, &EventLimits::default())
                .unwrap()
                .verify(&EventLimits::default())
                .unwrap(),
        )
    }

    /// Deleting a superseded event must leave no index entry pointing at
    /// its sequence number.
    #[test]
    fn delete_removes_all_index_keys() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), &StoreOptions::default()).unwrap();
        let signer = key(1);
        let old = event(&signer, 0, 100, &json!([["t", "x"]]), "old");
        let new = event(&signer, 0, 200, &json!([["t", "x"]]), "new");
        let outcomes = store.write(&[Arc::clone(&old), new]).unwrap();
        assert_eq!(
            outcomes,
            vec![
                WriteOutcome::Stored { seq: 1 },
                WriteOutcome::Stored { seq: 2 }
            ]
        );
        let old_seq = 1u64.to_be_bytes();
        let txn = store.read_txn().unwrap();
        assert!(store.events.get(&txn, &old_seq).unwrap().is_none());
        assert!(store.payloads.get(&txn, &old_seq).unwrap().is_none());
        assert!(store.ix_id.get(&txn, &old.id().0).unwrap().is_none());
        assert!(
            store
                .ix_replace
                .get(&txn, &old.address().unwrap())
                .unwrap()
                .is_some_and(|v| v != old_seq)
        );
        for db in [
            store.ix_time,
            store.ix_pubkey,
            store.ix_kind,
            store.ix_pubkey_kind,
            store.ix_tag,
        ] {
            for entry in db.iter(&txn).unwrap() {
                let (key, _) = entry.unwrap();
                assert_ne!(key[key.len() - 8..], old_seq, "stale index key {key:?}");
            }
        }
    }
}
