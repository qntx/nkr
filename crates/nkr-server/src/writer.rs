//! The single writer thread: group commits and batch broadcast.

use std::sync::Arc;

use nkr::message::ok_frame;
use nkr_lmdb::{Store, WriteOutcome};
use tokio::sync::mpsc;

use crate::channels::{Batch, Out, ShardMsg, WriteRequest};

/// Maximum events applied per write transaction.
const BATCH_CAP: usize = 1024;

/// Map one write outcome to the `OK` reply's `(accepted, message)` —
/// exhaustive so a new [`WriteOutcome`] variant cannot slip in
/// silently.
const fn ok_reply(outcome: WriteOutcome) -> (bool, &'static str) {
    match outcome {
        WriteOutcome::Stored { .. } | WriteOutcome::Ephemeral => (true, ""),
        WriteOutcome::Duplicate => (true, "duplicate: have this event"),
        WriteOutcome::Superseded => (false, "duplicate: have a newer version"),
    }
}

/// Writer thread main loop: block for the first request, drain up to
/// [`BATCH_CAP`], commit once, push each request's `OK` straight onto
/// its connection's outbound queue, and broadcast the batch to every
/// shard. Exits when every sender is dropped.
pub(crate) fn run(
    store: &Store,
    mut rx: mpsc::Receiver<WriteRequest>,
    shards: &[mpsc::Sender<ShardMsg>],
) {
    while let Some(first) = rx.blocking_recv() {
        let mut reqs = vec![first];
        while reqs.len() < BATCH_CAP {
            match rx.try_recv() {
                Ok(req) => reqs.push(req),
                Err(_) => break,
            }
        }
        let events: Vec<Arc<nkr::event::Event>> =
            reqs.iter().map(|r| Arc::clone(&r.event)).collect();
        let outcomes = match store.write(&events) {
            Ok(outcomes) => outcomes,
            Err(err) => {
                tracing::error!(%err, "store write failed");
                for req in reqs {
                    req.out.push(Out::Frame {
                        sub: None,
                        text: ok_frame(&req.event.id(), false, "error: internal error"),
                    });
                }
                continue;
            }
        };
        let mut batch: Batch = Vec::new();
        for (req, (outcome, event)) in reqs.into_iter().zip(outcomes.into_iter().zip(events)) {
            let (accepted, message) = ok_reply(outcome);
            req.out.push(Out::Frame {
                sub: None,
                text: ok_frame(&event.id(), accepted, message),
            });
            if let WriteOutcome::Stored { seq } = outcome {
                batch.push((seq, event));
            }
        }
        if !batch.is_empty() {
            let batch = Arc::new(batch);
            for shard in shards {
                drop(shard.blocking_send(ShardMsg::Batch(Arc::clone(&batch))));
            }
        }
    }
}
