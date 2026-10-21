//! NIP-77 negentropy shared by the editions: the storage a record
//! scan seals into, one reconcile round, and the wire limits.

pub use negentropy::NegentropyStorageVector;
use negentropy::{Id, Negentropy};

use crate::event::EventId;

/// Reconcile output cap per `NEG-MSG` (strfry `frameSizeLimit`).
pub const NEG_FRAME_LIMIT: u64 = 500_000;

/// Bytes charged per stored record against the sync memory budget:
/// the sealed vector's 40-byte `(created_at, id)` item.
pub const SYNC_RECORD_BYTES: usize = 40;

/// A negentropy storage build or reconcile failure; wire details
/// are for logs, never frames.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct SyncError(#[from] negentropy::Error);

/// Seal `records` into a `NegentropyStorageVector`.
///
/// # Errors
///
/// [`SyncError`] when `insert` or `seal` fails on a fresh storage —
/// an internal failure, never a wire detail.
pub fn build_storage(records: &[(u64, EventId)]) -> Result<NegentropyStorageVector, SyncError> {
    let mut storage = NegentropyStorageVector::with_capacity(records.len());
    for (created_at, id) in records {
        storage.insert(*created_at, Id::from_byte_array(id.0))?;
    }
    storage.seal()?;
    Ok(storage)
}

/// One reconcile round on a sealed storage — a fresh `Negentropy`
/// per message, like strfry. A well-formed message always yields at
/// least the protocol-version byte.
///
/// # Errors
///
/// [`SyncError`] when `msg` fails the negentropy protocol — the
/// caller answers `invalid: malformed negentropy message`.
pub fn reconcile(storage: &NegentropyStorageVector, msg: &[u8]) -> Result<Vec<u8>, SyncError> {
    Ok(Negentropy::borrowed(storage, NEG_FRAME_LIMIT)?.reconcile(msg)?)
}
