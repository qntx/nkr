//! Administration commands on the store: `export`, `import`, `scan`,
//! `delete`, `compact`. Each takes an opened [`Store`] plus explicit
//! inputs so tests can drive them directly; `main` only wires stdio.

use std::io::{BufRead, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use nosfry_core::auth::ReadGate;
use nosfry_core::event::{Event, EventLimits, UnverifiedEvent};
use nosfry_core::filter::{Filter, FilterError};
use nosfry_lmdb::{Progress, Scan, Store, StoreError, WriteOutcome};

/// Events per write transaction on import (strfry's import batch).
const IMPORT_BATCH: usize = 10_000;

/// Wall-clock budget per [`Scan::step`]; the read transaction is
/// released between steps so a running relay is not blocked.
const SCAN_STEP_BUDGET: Duration = Duration::from_secs(1);

/// Administration command failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AdminError {
    /// LMDB store failure.
    #[error("store: {0}")]
    Store(#[from] StoreError),
    /// I/O failure on stdin/stdout or the compact destination.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The scan filter JSON failed strict `Filter::parse`.
    #[error("filter: {0}")]
    Filter(#[from] FilterError),
}

/// Per-line tallies of an [`import`] run. `processed` counts every
/// non-blank line attempted, so `processed == added + duplicates +
/// superseded + ephemeral + rejected`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ImportStats {
    /// Non-blank input lines attempted.
    pub processed: u64,
    /// Newly stored events.
    pub added: u64,
    /// Lines whose event id was already stored.
    pub duplicates: u64,
    /// Replaceable/addressable events an equal-or-newer version exists for.
    pub superseded: u64,
    /// Ephemeral events (verified, never stored).
    pub ephemeral: u64,
    /// Events a stored same-author kind 5 deleted or blocks.
    pub blocked: u64,
    /// Rejected lines (too long, malformed, or failed verification).
    pub rejected: u64,
}

/// Write every stored event's canonical JSON to `out`, one per line,
/// ordered by `created_at` then seq (descending when `reverse`),
/// inclusively bounded by `since`/`until`. Returns the count written.
///
/// # Errors
///
/// [`AdminError::Store`] on store failure, [`AdminError::Io`] on a
/// failed write.
pub fn export(
    store: &Store,
    since: u64,
    until: u64,
    reverse: bool,
    out: &mut impl Write,
) -> Result<u64, AdminError> {
    Ok(store.export(since, until, reverse, &mut |json| writeln!(out, "{json}"))?)
}

/// Import newline-delimited event JSON from `input`.
///
/// Verified events go into one [`Store::write`] per `IMPORT_BATCH`
/// plus a final partial batch. There is no `created_at` time-window
/// check (strfry's `verifyTime=false`): the window is a session-level
/// policy, not an import policy. Lines longer than `max_line` and
/// lines failing parse or verification are rejected and counted;
/// blank lines are skipped entirely.
///
/// # Errors
///
/// [`AdminError::Io`] on a failed read, [`AdminError::Store`] on a
/// failed commit.
pub fn import(
    store: &Store,
    limits: &EventLimits,
    max_line: usize,
    mut input: impl BufRead,
    show_rejected: bool,
) -> Result<ImportStats, AdminError> {
    let mut importer = Importer {
        store,
        limits,
        max_line,
        show_rejected,
        batch: Vec::with_capacity(IMPORT_BATCH),
        stats: ImportStats::default(),
    };
    let mut buf = Vec::new();
    let mut line_no = 0u64;
    loop {
        buf.clear();
        if input.read_until(b'\n', &mut buf)? == 0 {
            break;
        }
        line_no += 1;
        importer.line(&buf, line_no)?;
    }
    importer.flush()?;
    tracing::info!(stats = ?importer.stats, "import done");
    Ok(importer.stats)
}

/// Line-oriented importer: verifies, tallies, and batches writes.
struct Importer<'a> {
    store: &'a Store,
    limits: &'a EventLimits,
    max_line: usize,
    show_rejected: bool,
    batch: Vec<Arc<Event>>,
    stats: ImportStats,
}

impl Importer<'_> {
    /// Handle one raw input line: skip blanks, reject or verify, batch.
    fn line(&mut self, buf: &[u8], line_no: u64) -> Result<(), AdminError> {
        let line = buf.strip_suffix(b"\n").unwrap_or(buf);
        let text = line.strip_suffix(b"\r").unwrap_or(line);
        if text.is_empty() {
            return Ok(());
        }
        self.stats.processed += 1;
        match self.check(buf, text) {
            Ok(event) => self.batch.push(event),
            Err(reason) => self.reject(line_no, &reason),
        }
        self.maybe_flush()
    }

    /// Enforce `max_line`, then parse and verify; `Err` is the reason.
    fn check(&self, buf: &[u8], text: &[u8]) -> Result<Arc<Event>, String> {
        if buf.len() > self.max_line {
            return Err("line too long".to_owned());
        }
        let text = std::str::from_utf8(text).map_err(|err| err.to_string())?;
        UnverifiedEvent::parse(text, self.limits)
            .and_then(|unverified| unverified.verify(self.limits))
            .map_err(|err| err.reason())
            .map(Arc::new)
    }

    /// Tally a rejection and log it when `show_rejected` is set.
    fn reject(&mut self, line_no: u64, reason: &str) {
        self.stats.rejected += 1;
        if self.show_rejected {
            tracing::warn!(line = line_no, %reason, "rejected");
        }
    }

    /// Commit a full batch and log running totals.
    fn maybe_flush(&mut self) -> Result<(), AdminError> {
        if self.batch.len() >= IMPORT_BATCH {
            self.flush()?;
            tracing::info!(stats = ?self.stats, "import progress");
        }
        Ok(())
    }

    /// Commit the batch, tally each outcome, and reset it.
    fn flush(&mut self) -> Result<(), AdminError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        for outcome in self.store.write(&self.batch)?.outcomes {
            match outcome {
                WriteOutcome::Stored { .. } => self.stats.added += 1,
                WriteOutcome::Duplicate => self.stats.duplicates += 1,
                WriteOutcome::Superseded => self.stats.superseded += 1,
                WriteOutcome::Ephemeral => self.stats.ephemeral += 1,
                WriteOutcome::Deleted => self.stats.blocked += 1,
            }
        }
        self.batch.clear();
        Ok(())
    }
}

/// Emit stored events matching `filter` like a `REQ`, newest first.
///
/// There is no relay `max_filter_limit` cap; the filter's own `limit`
/// still applies. Each event's canonical JSON goes to `out` on its own
/// line; with `count_only`, output is suppressed and only the count is
/// returned.
///
/// # Errors
///
/// [`AdminError::Store`] on store failure, [`AdminError::Io`] on a
/// failed write.
pub fn scan(
    store: &Store,
    filter: Filter,
    count_only: bool,
    out: &mut impl Write,
) -> Result<u64, AdminError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let mut scan = Scan::new(
        Arc::from([filter]),
        ReadGate::default(),
        store.snapshot()?,
        u64::MAX,
        now,
    );
    let mut count = 0u64;
    let mut write_err: Option<std::io::Error> = None;
    loop {
        let mut emit = |_seq: u64, json: &str| {
            if write_err.is_some() {
                return;
            }
            count += 1;
            if !count_only && let Err(err) = writeln!(out, "{json}") {
                write_err = Some(err);
            }
        };
        let progress = scan.step(store, SCAN_STEP_BUDGET, &mut emit)?;
        if let Some(err) = write_err.take() {
            return Err(err.into());
        }
        match progress {
            Progress::Pending => {}
            Progress::Done => break,
        }
    }
    Ok(count)
}

/// Delete stored events matching `filter`; with `dry_run`, count what
/// would be deleted without touching the store. Returns the deleted
/// (or would-be-deleted) count.
///
/// The scan runs with `now = 0`, so already-expired events stay
/// deletable; there is no relay `max_filter_limit` cap.
///
/// # Errors
///
/// [`AdminError::Store`] on store failure.
pub fn delete(store: &Store, filter: Filter, dry_run: bool) -> Result<u64, AdminError> {
    let mut scan = Scan::new(
        Arc::from([filter]),
        ReadGate::default(),
        store.snapshot()?,
        u64::MAX,
        0,
    );
    let mut count = 0u64;
    let mut seqs = Vec::new();
    loop {
        let progress = scan.step(store, SCAN_STEP_BUDGET, &mut |seq, _| {
            seqs.push(seq);
        })?;
        if dry_run {
            count += seqs.len() as u64;
        } else {
            count += store.delete(&seqs)?;
        }
        seqs.clear();
        if progress == Progress::Done {
            break;
        }
    }
    Ok(count)
}

/// Compact the store to a single file at `dest` (which must not exist).
///
/// # Errors
///
/// [`AdminError::Io`] when `dest` exists or cannot be created,
/// [`AdminError::Store`] on LMDB failure.
pub fn compact(store: &Store, dest: &Path) -> Result<(), AdminError> {
    store.compact_to(dest)?;
    Ok(())
}
