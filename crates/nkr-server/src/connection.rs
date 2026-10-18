//! One connection task per WebSocket: owns the `nkr::session::Session`
//! and bridges it to the writer, the monitor shard, and the reader.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::SinkExt;
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use nkr::event::EventId;
use nkr::message::{closed_frame, event_frame, notice_frame, ok_frame};
use nkr::session::{Command, Session};
use nkr_lmdb::WriteOutcome;
use tokio::sync::{mpsc, oneshot, watch};
use yawc::close::CloseCode;
use yawc::{Frame, HttpWebSocket, OpCode};

use crate::channels::{ConnId, Out, Outbound, ReaderMsg, ShardMsg, WriteRequest};

/// What a writer reply resolves to: the published id and the outcome.
type OkReply = (EventId, Result<WriteOutcome, ()>);

/// Per-connection state handed down from the server.
#[derive(Debug, Clone)]
pub struct Handle {
    /// Writer inbox.
    pub writer: mpsc::Sender<WriteRequest>,
    /// Monitor shard inboxes, indexed by `conn % monitors`.
    pub shards: Arc<[mpsc::Sender<ShardMsg>]>,
    /// Reader inboxes, indexed by `conn % readers`.
    pub readers: Arc<[std::sync::mpsc::Sender<ReaderMsg>]>,
    /// Session limits.
    pub limits: Arc<nkr::limits::Limits>,
    /// Ping interval.
    pub ping_interval: Duration,
    /// Outbound byte budget.
    pub max_pending_outbound: usize,
    /// Broadcast shutdown signal.
    pub shutdown: watch::Receiver<bool>,
    /// Tracks every live connection task for the shutdown deadline.
    pub tracker: tokio_util::task::TaskTracker,
}

/// Global connection counter.
static NEXT_CONN: AtomicU64 = AtomicU64::new(1);

/// Frames batched into one socket flush after the first `Out` arrives.
const BATCH_MAX_FRAMES: usize = 64;
/// Byte cap for one batched flush.
const BATCH_MAX_BYTES: usize = 256 * 1024;
/// How long a best-effort close frame may block the shutdown path.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Feed one frame into the socket buffer; `false` when the write
/// failed or stalled past `timeout`.
async fn feed_timed(ws: &mut HttpWebSocket, frame: Frame, timeout: Duration, conn: ConnId) -> bool {
    match tokio::time::timeout(timeout, ws.feed(frame)).await {
        Ok(Ok(())) => true,
        Ok(Err(err)) => {
            tracing::debug!(%err, conn, "websocket send failed");
            false
        }
        Err(_) => {
            tracing::debug!(conn, "websocket write stalled");
            false
        }
    }
}

/// Run one WebSocket connection until close, error, or shutdown.
#[allow(
    clippy::too_many_lines,
    clippy::excessive_nesting,
    clippy::cognitive_complexity,
    reason = "one select loop; every branch stays in place"
)]
pub(crate) async fn run(ws: &mut HttpWebSocket, handle: &Handle) {
    let conn: ConnId = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
    let (out, mut out_rx) = Outbound::new(handle.max_pending_outbound);
    let mut session = Session::new(Arc::clone(&handle.limits));
    let mut commands: Vec<Command> = Vec::new();
    let mut ok_replies: FuturesUnordered<BoxFuture<'static, OkReply>> = FuturesUnordered::new();
    let mut ping = tokio::time::interval(handle.ping_interval);
    let mut last_inbound = Instant::now();
    let mut shutdown = handle.shutdown.clone();
    let mut batch: Vec<Out> = Vec::with_capacity(BATCH_MAX_FRAMES);
    let shard_idx = usize::try_from(conn).unwrap_or(usize::MAX) % handle.shards.len().max(1);
    let reader_idx = usize::try_from(conn).unwrap_or(usize::MAX) % handle.readers.len().max(1);
    let (Some(shard), Some(reader)) =
        (handle.shards.get(shard_idx), handle.readers.get(reader_idx))
    else {
        return;
    };
    let (shard, reader) = (shard.clone(), reader.clone());

    loop {
        tokio::select! {
            frame = ws.next() => {
                let Some(frame) = frame else { break };
                last_inbound = Instant::now();
                match frame.opcode() {
                    OpCode::Text => {
                        let Ok(text) = std::str::from_utf8(frame.payload())
                        else {
                            out.push(Out::Frame {
                                sub: None,
                                text: notice_frame("invalid: malformed utf-8"),
                            });
                            continue;
                        };
                        session.on_text(now(), text, &mut commands);
                        for cmd in std::mem::take(&mut commands) {
                            handle_command(
                                cmd, conn, handle, &shard, &reader,
                                &out, &mut ok_replies,
                            )
                            .await;
                        }
                    }
                    OpCode::Binary => out.push(Out::Frame {
                        sub: None,
                        text: notice_frame(
                            "invalid: binary frames are not supported",
                        ),
                    }),
                    OpCode::Close => break,
                    _ => {}
                }
            }
            out_msg = out_rx.recv() => {
                let Some(first) = out_msg else { break };
                // Batch: drain everything already queued so one socket
                // flush covers a burst of frames (e.g. a large REQ).
                batch.clear();
                let mut bytes = first.accounted_bytes();
                batch.push(first);
                while batch.len() < BATCH_MAX_FRAMES && bytes < BATCH_MAX_BYTES {
                    match out_rx.try_recv() {
                        Ok(msg) => {
                            bytes += msg.accounted_bytes();
                            batch.push(msg);
                        }
                        Err(_) => break,
                    }
                }
                let mut dead = false;
                #[allow(
                    clippy::iter_with_drain,
                    reason = "the Vec keeps its capacity for the next batch"
                )]
                for msg in batch.drain(..) {
                    out.written(msg.accounted_bytes());
                    match msg {
                        Out::Frame { sub, text } => {
                            if sub.as_ref().is_some_and(|s| !session.is_current(s)) {
                                continue;
                            }
                            dead = !feed_timed(
                                ws, Frame::text(text.into_bytes()),
                                handle.ping_interval, conn,
                            ).await;
                        }
                        Out::Event { sub, event } => {
                            if !session.is_current(&sub) {
                                continue;
                            }
                            let text = event_frame(&sub.id, event.json());
                            dead = !feed_timed(
                                ws, Frame::text(text.into_bytes()),
                                handle.ping_interval, conn,
                            ).await;
                        }
                        Out::Closed { sub, reason } => {
                            if !session.close_by_relay(&sub) {
                                continue;
                            }
                            drop(reader.send(ReaderMsg::Cancel {
                                conn,
                                sub_id: sub.id.clone(),
                            }));
                            drop(shard
                                .send(ShardMsg::Remove {
                                    conn,
                                    sub_id: sub.id.clone(),
                                })
                                .await);
                            let text = closed_frame(&sub.id, &reason);
                            dead = !feed_timed(
                                ws, Frame::text(text.into_bytes()),
                                handle.ping_interval, conn,
                            ).await;
                        }
                    }
                    if dead {
                        break;
                    }
                }
                if dead {
                    break;
                }
                match tokio::time::timeout(handle.ping_interval, ws.flush()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        tracing::debug!(%err, conn, "websocket flush failed");
                        break;
                    }
                    Err(_) => {
                        tracing::debug!(conn, "websocket flush stalled");
                        break;
                    }
                }
            }
            reply = ok_replies.next(), if !ok_replies.is_empty() => {
                let Some((id, outcome)) = reply else { continue };
                let (accepted, message) = match outcome {
                    Ok(WriteOutcome::Duplicate) => {
                        (true, "duplicate: have this event")
                    }
                    Ok(WriteOutcome::Superseded) => {
                        (false, "duplicate: have a newer version")
                    }
                    Ok(WriteOutcome::Stored { .. } | WriteOutcome::Ephemeral) => {
                        (true, "")
                    }
                    Ok(_) | Err(()) => {
                        tracing::error!(conn, "writer rejected event");
                        (false, "error: internal error")
                    }
                };
                out.push(Out::Frame {
                    sub: None,
                    text: ok_frame(&id, accepted, message),
                });
            }
            _ = ping.tick() => {
                if last_inbound.elapsed() > handle.ping_interval.saturating_mul(2) {
                    drop(tokio::time::timeout(
                        CLOSE_TIMEOUT,
                        ws.send(Frame::close(CloseCode::Away, &[][..])),
                    ).await);
                    break;
                }
                match tokio::time::timeout(
                    handle.ping_interval,
                    ws.send(Frame::ping(&[][..])),
                ).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        tracing::debug!(%err, conn, "ping failed");
                        break;
                    }
                    Err(_) => {
                        tracing::debug!(conn, "ping write stalled");
                        break;
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    drop(tokio::time::timeout(
                        CLOSE_TIMEOUT,
                        ws.send(Frame::close(CloseCode::Away, &[][..])),
                    ).await);
                    break;
                }
            }
        }
        if out.is_dead() {
            drop(
                tokio::time::timeout(
                    CLOSE_TIMEOUT,
                    ws.send(Frame::close(CloseCode::Policy, &b"slow consumer"[..])),
                )
                .await,
            );
            break;
        }
    }

    drop(reader.send(ReaderMsg::CancelConn { conn }));
    drop(shard.send(ShardMsg::RemoveConn { conn }).await);
}

/// Execute one session command.
#[allow(
    clippy::too_many_arguments,
    clippy::excessive_nesting,
    clippy::needless_pass_by_ref_mut,
    reason = "all handles are needed; FuturesUnordered is not Sync"
)]
async fn handle_command(
    cmd: Command,
    conn: ConnId,
    handle: &Handle,
    shard: &mpsc::Sender<ShardMsg>,
    reader: &std::sync::mpsc::Sender<ReaderMsg>,
    out: &Outbound,
    ok_replies: &mut FuturesUnordered<BoxFuture<'static, OkReply>>,
) {
    match cmd {
        Command::Publish(event) => {
            if event.kind().is_ephemeral() {
                for shard_tx in handle.shards.iter() {
                    if shard_tx
                        .send(ShardMsg::Ephemeral(Arc::clone(&event)))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                out.push(Out::Frame {
                    sub: None,
                    text: ok_frame(&event.id(), true, ""),
                });
            } else {
                let id = event.id();
                let (reply, rx) = oneshot::channel();
                if handle
                    .writer
                    .send(WriteRequest { event, reply })
                    .await
                    .is_err()
                {
                    out.push(Out::Frame {
                        sub: None,
                        text: ok_frame(&id, false, "error: internal error"),
                    });
                } else {
                    ok_replies.push(Box::pin(async move { (id, rx.await.unwrap_or(Err(()))) }));
                }
            }
        }
        Command::Subscribe { sub, filters } => {
            let (reply, rx) = oneshot::channel();
            let register = shard
                .send(ShardMsg::Register {
                    conn,
                    sub: sub.clone(),
                    filters: Arc::clone(&filters),
                    out: out.clone(),
                    reply,
                })
                .await;
            if register.is_err() {
                return;
            }
            let Ok(snapshot) = rx.await else { return };
            drop(reader.send(ReaderMsg::Start {
                conn,
                sub,
                filters,
                snapshot,
                out: out.clone(),
            }));
        }
        Command::Unsubscribe { sub_id } => {
            drop(reader.send(ReaderMsg::Cancel {
                conn,
                sub_id: sub_id.clone(),
            }));
            drop(shard.send(ShardMsg::Remove { conn, sub_id }).await);
        }
        Command::Send(text) => out.push(Out::Frame { sub: None, text }),
    }
}
