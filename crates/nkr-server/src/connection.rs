//! One connection task per WebSocket: owns the `nkr::session::Session`
//! and bridges it to the writer and its monitor shard (which forwards
//! scan control to the reader).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::SinkExt;
use futures_util::stream::StreamExt;
use nkr::filter::Filter;
use nkr::message::{SubId, closed_frame, event_frame, notice_frame, ok_frame};
use nkr::session::{Command, Session, SubKey};
use tokio::sync::{mpsc, watch};
use yawc::close::CloseCode;
use yawc::{Frame, HttpWebSocket, OpCode};

use crate::channels::{ConnId, Out, Outbound, ShardMsg, WriteRequest, route};

/// Per-connection state handed down from the server.
#[derive(Debug, Clone)]
pub struct Handle {
    /// Writer inbox.
    pub writer: mpsc::Sender<WriteRequest>,
    /// Monitor shard inboxes, indexed by `conn % shards.len()`; the
    /// shard owns the reader routing for scans.
    pub shards: Arc<[mpsc::Sender<ShardMsg>]>,
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

/// A batched socket write that failed.
#[derive(Debug)]
enum WriteError {
    /// `feed`/`flush` returned a socket error.
    Socket(yawc::WebSocketError),
    /// The batch exceeded its deadline.
    Stalled,
}

/// One connection's runtime state.
struct Connection<'a> {
    /// Connection id; the shard routing key.
    conn: ConnId,
    /// Server-wide state.
    handle: &'a Handle,
    /// This connection's monitor shard (register/remove plus the scan
    /// control the shard forwards to the reader).
    shard: mpsc::Sender<ShardMsg>,
    /// Protocol state machine.
    session: Session,
    /// This connection's outbound queue.
    out: Outbound,
    /// Reusable drained-batch buffer (`VecDeque` keeps FIFO order and
    /// its capacity across batches without a `drain` loop).
    batch: VecDeque<Out>,
}

impl<'a> Connection<'a> {
    fn new(handle: &'a Handle) -> (Self, mpsc::UnboundedReceiver<Out>) {
        let conn = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
        let (out, out_rx) = Outbound::new(handle.max_pending_outbound);
        (
            Self {
                conn,
                handle,
                shard: route(&handle.shards, conn).clone(),
                session: Session::new(Arc::clone(&handle.limits)),
                out,
                batch: VecDeque::new(),
            },
            out_rx,
        )
    }

    /// Handle one inbound frame; `false` means the connection ends.
    async fn inbound(&mut self, frame: Option<Frame>, last_inbound: &mut Instant) -> bool {
        let Some(frame) = frame else {
            return false;
        };
        *last_inbound = Instant::now();
        match frame.opcode() {
            OpCode::Text => {
                let Ok(text) = std::str::from_utf8(frame.payload()) else {
                    self.out.push(Out::Frame {
                        sub: None,
                        text: notice_frame("invalid: malformed utf-8"),
                    });
                    return true;
                };
                let mut commands = Vec::new();
                self.session.on_text(now(), text, &mut commands);
                for cmd in commands {
                    self.command(cmd).await;
                }
            }
            OpCode::Binary => self.out.push(Out::Frame {
                sub: None,
                text: notice_frame("invalid: binary frames are not supported"),
            }),
            OpCode::Close => return false,
            _ => {}
        }
        true
    }

    /// Execute one session command.
    async fn command(&self, cmd: Command) {
        match cmd {
            Command::Publish(event) => self.publish(event).await,
            Command::Subscribe { sub, filters } => self.subscribe(sub, filters).await,
            Command::Unsubscribe { sub_id } => self.unsubscribe(sub_id).await,
            Command::Send(text) => self.out.push(Out::Frame { sub: None, text }),
        }
    }

    /// Broadcast an ephemeral event or queue a stored write; the writer
    /// pushes the `OK` onto this connection's `Outbound` itself.
    async fn publish(&self, event: Arc<nkr::event::Event>) {
        if event.kind().is_ephemeral() {
            self.broadcast(&event).await;
            self.out.push(Out::Frame {
                sub: None,
                text: ok_frame(&event.id(), true, ""),
            });
            return;
        }
        let id = event.id();
        if self
            .handle
            .writer
            .send(WriteRequest {
                event,
                out: self.out.clone(),
            })
            .await
            .is_err()
        {
            self.out.push(Out::Frame {
                sub: None,
                text: ok_frame(&id, false, "error: internal error"),
            });
        }
    }

    /// Fan an ephemeral event out to every monitor shard.
    async fn broadcast(&self, event: &Arc<nkr::event::Event>) {
        for shard in self.handle.shards.iter() {
            if shard
                .send(ShardMsg::Ephemeral(Arc::clone(event)))
                .await
                .is_err()
            {
                break;
            }
        }
    }

    /// Register a subscription with this connection's shard; the shard
    /// starts the stored scan on the reader itself.
    async fn subscribe(&self, sub: SubKey, filters: Arc<[Filter]>) {
        drop(
            self.shard
                .send(ShardMsg::Register {
                    conn: self.conn,
                    sub,
                    filters,
                    out: self.out.clone(),
                })
                .await,
        );
    }

    /// Remove one subscription at the shard (which forwards the reader
    /// cancel itself).
    async fn unsubscribe(&self, sub_id: SubId) {
        drop(
            self.shard
                .send(ShardMsg::Remove {
                    conn: self.conn,
                    sub_id,
                })
                .await,
        );
    }

    /// Drain `out_rx` (bounded by `BATCH_MAX_*`), feed every surviving
    /// frame, then flush — one socket flush per batch.
    async fn write_batch(
        &mut self,
        ws: &mut HttpWebSocket,
        first: Out,
        out_rx: &mut mpsc::UnboundedReceiver<Out>,
    ) -> Result<(), WriteError> {
        let mut batch = std::mem::take(&mut self.batch);
        let mut bytes = first.accounted_bytes();
        batch.push_back(first);
        while batch.len() < BATCH_MAX_FRAMES && bytes < BATCH_MAX_BYTES {
            match out_rx.try_recv() {
                Ok(msg) => {
                    bytes += msg.accounted_bytes();
                    batch.push_back(msg);
                }
                Err(_) => break,
            }
        }
        // Account every drained frame up front: a timeout mid-batch
        // drops the rest, and their bytes must not stay charged to the
        // outbound queue.
        for msg in &batch {
            self.out.written(msg.accounted_bytes());
        }
        // One deadline covers every surviving feed plus the flush.
        let result = tokio::time::timeout(self.handle.ping_interval, async {
            while let Some(msg) = batch.pop_front() {
                self.feed(ws, msg).await.map_err(WriteError::Socket)?;
            }
            ws.flush().await.map_err(WriteError::Socket)
        })
        .await;
        self.batch = batch;
        result.map_err(|_| WriteError::Stalled)?
    }

    /// One drained batch: feed + flush; `false` means the connection
    /// ends (socket error or stalled write).
    async fn write_out(
        &mut self,
        ws: &mut HttpWebSocket,
        first: Option<Out>,
        out_rx: &mut mpsc::UnboundedReceiver<Out>,
    ) -> bool {
        let Some(first) = first else {
            return false;
        };
        match self.write_batch(ws, first, out_rx).await {
            Ok(()) => true,
            Err(WriteError::Socket(err)) => {
                tracing::debug!(%err, conn = self.conn, "websocket write failed");
                false
            }
            Err(WriteError::Stalled) => {
                tracing::debug!(conn = self.conn, "websocket write stalled");
                false
            }
        }
    }

    /// Shutdown watch tick: `true` (close sent) when the flag is raised.
    async fn shutdown_close(
        &self,
        ws: &mut HttpWebSocket,
        shutdown: &watch::Receiver<bool>,
    ) -> bool {
        if !*shutdown.borrow() {
            return false;
        }
        self.closing(ws, CloseCode::Away, &[]).await;
        true
    }

    /// Feed one outbound message; skipped subscriptions consume nothing.
    async fn feed(&mut self, ws: &mut HttpWebSocket, msg: Out) -> Result<(), yawc::WebSocketError> {
        match msg {
            Out::Frame { sub, text } => {
                if sub.as_ref().is_some_and(|s| !self.session.is_current(s)) {
                    return Ok(());
                }
                ws.feed(Frame::text(text.into_bytes())).await
            }
            Out::Frames { sub, texts } => {
                if !self.session.is_current(&sub) {
                    return Ok(());
                }
                for text in texts {
                    ws.feed(Frame::text(text.into_bytes())).await?;
                }
                Ok(())
            }
            Out::Event { sub, event } => {
                if !self.session.is_current(&sub) {
                    return Ok(());
                }
                let text = event_frame(&sub.id, event.json());
                ws.feed(Frame::text(text.into_bytes())).await
            }
            Out::Closed { sub, reason } => {
                if !self.session.close_by_relay(&sub) {
                    return Ok(());
                }
                // The shard removes the monitor and forwards the reader
                // cancel itself.
                drop(
                    self.shard
                        .send(ShardMsg::Remove {
                            conn: self.conn,
                            sub_id: sub.id.clone(),
                        })
                        .await,
                );
                let text = closed_frame(&sub.id, &reason);
                ws.feed(Frame::text(text.into_bytes())).await
            }
        }
    }

    /// Ping tick: close an idle connection (1001), else send a ping.
    /// `false` means the connection ends.
    async fn keepalive(&self, ws: &mut HttpWebSocket, last_inbound: Instant) -> bool {
        if last_inbound.elapsed() > self.handle.ping_interval.saturating_mul(2) {
            self.closing(ws, CloseCode::Away, &[]).await;
            return false;
        }
        match tokio::time::timeout(self.handle.ping_interval, ws.send(Frame::ping(&[][..]))).await {
            Ok(Ok(())) => true,
            Ok(Err(err)) => {
                tracing::debug!(%err, conn = self.conn, "ping failed");
                false
            }
            Err(_) => {
                tracing::debug!(conn = self.conn, "ping write stalled");
                false
            }
        }
    }

    /// Best-effort close frame bounded by `CLOSE_TIMEOUT`.
    async fn closing(&self, ws: &mut HttpWebSocket, code: CloseCode, reason: &[u8]) {
        drop(tokio::time::timeout(CLOSE_TIMEOUT, ws.send(Frame::close(code, reason))).await);
    }

    /// Drop everything this connection registered at the shard.
    async fn teardown(&self) {
        drop(
            self.shard
                .send(ShardMsg::RemoveConn { conn: self.conn })
                .await,
        );
    }
}

/// Run one WebSocket connection until close, error, or shutdown.
pub(crate) async fn run(ws: &mut HttpWebSocket, handle: &Handle) {
    let (mut conn, mut out_rx) = Connection::new(handle);
    let mut ping = tokio::time::interval(handle.ping_interval);
    let mut last_inbound = Instant::now();
    let mut shutdown = handle.shutdown.clone();

    loop {
        tokio::select! {
            frame = ws.next() => {
                if !conn.inbound(frame, &mut last_inbound).await {
                    break;
                }
            }
            first = out_rx.recv() => {
                if !conn.write_out(ws, first, &mut out_rx).await {
                    break;
                }
            }
            _ = ping.tick() => {
                if !conn.keepalive(ws, last_inbound).await {
                    break;
                }
            }
            _ = shutdown.changed() => {
                if conn.shutdown_close(ws, &shutdown).await {
                    break;
                }
            }
        }
        if conn.out.is_dead() {
            conn.closing(ws, CloseCode::Policy, b"slow consumer").await;
            break;
        }
    }

    conn.teardown().await;
}
