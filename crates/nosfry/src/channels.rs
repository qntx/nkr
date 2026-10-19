//! Message types exchanged between the connection tasks and the
//! writer, monitor shard, and reader threads.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use nosfry_core::event::Event;
use nosfry_core::filter::Filter;
use nosfry_core::message::SubId;
use nosfry_core::session::SubKey;
use tokio::sync::mpsc;

/// Connection identifier; also the shard/reader routing key.
pub(crate) type ConnId = u64;

/// Route `conn` to `conn % slots.len()`.
///
/// Every routing table is built from a `.max(1)` thread count at
/// server startup, so `slots` is never empty and `index` is in bounds.
pub(crate) fn route<T>(slots: &[T], conn: ConnId) -> &T {
    let index = usize::try_from(conn).unwrap_or(usize::MAX) % slots.len().max(1);
    let Some(slot) = slots.get(index) else {
        unreachable!("routing tables are never empty")
    };
    slot
}

/// One committed write batch: `(seq, event)` for `Stored` events only.
pub(crate) type Batch = Vec<(u64, Arc<Event>)>;

/// A publish request queued for the single writer thread.
#[derive(Debug)]
pub(crate) struct WriteRequest {
    /// The verified event.
    pub(crate) event: Arc<Event>,
    /// The connection's outbound queue; the writer pushes the `OK`
    /// frame onto it directly (one fewer task wake-up than a reply
    /// channel bouncing through the connection's `select!`). The `OK`
    /// is pushed only after the batch is queued on every shard, so a
    /// `REQ` issued after `OK` always snapshots the event.
    pub(crate) out: Outbound,
}

/// Inbox item for a monitor shard thread.
#[derive(Debug)]
pub(crate) enum ShardMsg {
    /// Register a subscription as pending; the shard forwards a
    /// [`ReaderMsg::Start`] carrying the current snapshot — the highest
    /// seq the shard has already received — so a `Cancel` it forwards
    /// later can never be overtaken by the `Start`.
    Register {
        /// Owning connection.
        conn: ConnId,
        /// Subscription key (with generation).
        sub: SubKey,
        /// Compiled filters.
        filters: Arc<[Filter]>,
        /// Where matches and scan results go.
        out: Outbound,
    },
    /// The historical scan finished: flush the pending buffer and mark
    /// the subscription active.
    Activate {
        /// Owning connection.
        conn: ConnId,
        /// Subscription key.
        sub: SubKey,
    },
    /// Close one subscription.
    Remove {
        /// Owning connection.
        conn: ConnId,
        /// Subscription id.
        sub_id: SubId,
    },
    /// Drop everything belonging to a connection.
    RemoveConn {
        /// The connection.
        conn: ConnId,
    },
    /// A committed write batch to match against live subscriptions.
    /// The writer queues it on every shard before sending any `OK`,
    /// so a `REQ` issued after `OK` always snapshots the event.
    Batch(Arc<Batch>),
    /// An ephemeral event to broadcast (never stored).
    Ephemeral(Arc<Event>),
}

/// Inbox item for a reader thread.
#[derive(Debug)]
pub(crate) enum ReaderMsg {
    /// Start a snapshot-bounded scan (boxed: it dwarfs the other variants).
    Start(Box<ScanStart>),
    /// Cancel one scan.
    Cancel {
        /// Owning connection.
        conn: ConnId,
        /// Subscription id.
        sub_id: SubId,
    },
    /// Drop everything belonging to a connection.
    CancelConn {
        /// The connection.
        conn: ConnId,
    },
}

/// Arguments for [`ReaderMsg::Start`].
#[derive(Debug)]
pub(crate) struct ScanStart {
    /// Owning connection.
    pub(crate) conn: ConnId,
    /// Subscription key (with generation).
    pub(crate) sub: SubKey,
    /// Compiled filters.
    pub(crate) filters: Arc<[Filter]>,
    /// Snapshot seq S assigned by the shard.
    pub(crate) snapshot: u64,
    /// Where results go.
    pub(crate) out: Outbound,
}

/// One outbound item for a connection's write loop.
#[derive(Debug)]
pub(crate) enum Out {
    /// A pre-serialized frame; `sub` carries the generation the
    /// connection checks before writing (`None` always goes out).
    Frame {
        /// Subscription tag for the `is_current` gate.
        sub: Option<SubKey>,
        /// The serialized frame.
        text: String,
    },
    /// A chunk of pre-serialized scan frames for one subscription:
    /// the reader packs a step's emits so the connection wakes once
    /// per chunk instead of once per event.
    Frames {
        /// Subscription key.
        sub: SubKey,
        /// Serialized frames in emit order.
        texts: Vec<String>,
    },
    /// A live match: one shared `Arc` per event, framed into
    /// `["EVENT",sub,json]` by the connection at write time.
    Event {
        /// Subscription key.
        sub: SubKey,
        /// The matched event.
        event: Arc<Event>,
    },
    /// The relay is closing the subscription: the connection closes it
    /// in its session and tears down the scan/monitor when current.
    Closed {
        /// Subscription key.
        sub: SubKey,
        /// The CLOSED reason.
        reason: String,
    },
}

impl Out {
    /// Accounted size in bytes for the outbound budget: exact for
    /// `Frame`, the serialized frame's size for `Event` (payload plus
    /// `["EVENT",<sub>,` overhead) and `Closed`.
    #[must_use]
    pub(crate) fn accounted_bytes(&self) -> usize {
        match self {
            Self::Frame { text, .. } => text.len(),
            Self::Frames { texts, .. } => texts.iter().map(String::len).sum(),
            Self::Event { sub, event } => event.json().len() + sub.id.json().len() + 12,
            Self::Closed { sub, reason } => sub.id.json().len() + reason.len() + 14,
        }
    }
}

/// Per-connection outbound queue: unbounded frames plus a bounded byte
/// budget tracked atomically.
#[derive(Debug)]
pub(crate) struct Outbound {
    tx: mpsc::UnboundedSender<Out>,
    pending: Arc<AtomicUsize>,
    dead: Arc<AtomicBool>,
    max_pending: usize,
}

impl Outbound {
    /// New queue; `max_pending` is the byte budget.
    #[must_use]
    pub(crate) fn new(max_pending: usize) -> (Self, mpsc::UnboundedReceiver<Out>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                tx,
                pending: Arc::new(AtomicUsize::new(0)),
                dead: Arc::new(AtomicBool::new(false)),
                max_pending,
            },
            rx,
        )
    }

    /// Queue a frame. Pushing pending bytes above `max_pending` marks
    /// the connection dead and drops the frame instead.
    pub(crate) fn push(&self, out: Out) {
        let len = out.accounted_bytes();
        let prev = self.pending.fetch_add(len, Ordering::Relaxed);
        if prev + len > self.max_pending {
            self.pending.fetch_sub(len, Ordering::Relaxed);
            self.dead.store(true, Ordering::Relaxed);
            return;
        }
        if self.tx.send(out).is_err() {
            self.pending.fetch_sub(len, Ordering::Relaxed);
        }
    }

    /// Account for a frame actually written to the socket.
    pub(crate) fn written(&self, bytes: usize) {
        self.pending.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Pending outbound bytes.
    #[must_use]
    pub(crate) fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    /// The byte budget was exceeded; the connection should close.
    #[must_use]
    pub(crate) fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Relaxed)
    }
}

impl Clone for Outbound {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            pending: Arc::clone(&self.pending),
            dead: Arc::clone(&self.dead),
            max_pending: self.max_pending,
        }
    }
}
