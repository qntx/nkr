//! nosfry: the native Nostr relay.
//!
//! Thread topology: one tokio multi-thread runtime for connection
//! tasks, one OS writer thread, M monitor shard threads, and N reader
//! threads. LMDB is only touched by the writer and reader threads;
//! everything else communicates over bounded channels.
#![cfg_attr(
    test,
    allow(
        unused_crate_dependencies,
        reason = "dev-deps are exercised by integration tests"
    )
)]

pub mod admin;
mod channels;
pub mod config;
mod connection;
mod http;
mod limiter;
mod metrics;
mod policy;
mod reader;
mod shard;
mod writer;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::thread::JoinHandle;
use std::time::Duration;

use axum::body::Body;
use axum::extract::ConnectInfo;
use clap as _;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use nosfry_lmdb::Store;
use rlimit as _;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tower::ServiceExt as _;
use tracing_subscriber as _;

use crate::channels::{ReaderMsg, ShardMsg, WriterMsg};
use crate::config::Config;

/// How many publish requests the writer queues before backpressuring
/// connections.
const WRITER_QUEUE: usize = 4096;

/// How many messages one monitor shard queues.
const SHARD_QUEUE: usize = 4096;

/// Fatal server errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ServerError {
    /// Filesystem/config IO failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// LMDB failure.
    #[error("store: {0}")]
    Store(#[from] nosfry_lmdb::StoreError),
    /// TOML parse failure.
    #[error("config: {0}")]
    Config(#[from] toml::de::Error),
    /// Invalid `[relay.auth]` configuration.
    #[error("config: relay.auth: {0}")]
    Auth(#[from] nosfry_core::auth::AuthRulesError),
    /// The worker shutdown task failed.
    #[error("worker shutdown: {0}")]
    Shutdown(#[source] tokio::task::JoinError),
    /// A worker thread failed to start.
    #[error("thread spawn failed: {0}")]
    Spawn(#[source] std::io::Error),
}

/// The live-reloadable slice of server state. Every reader clones the
/// `Arc` out of a `watch` borrow — never holding the borrow across an
/// `.await` — so a reload never blocks a request. Restart-only fields
/// (see [`Config::reloadable_from`]) stay frozen at startup values.
#[derive(Debug)]
pub(crate) struct Live {
    /// Effective configuration.
    pub(crate) config: Arc<Config>,
    /// Session limits assembled by [`Config::limits`]; new connections
    /// only — existing sessions keep the limits they started with.
    pub(crate) limits: Arc<nosfry_core::limits::Limits>,
    /// Pre-serialized NIP-11 document, rebuilt on each reload.
    pub(crate) nip11: axum::body::Bytes,
    /// Active write-policy plugin; read at each publish so a reload
    /// applies to the next event on every connection.
    pub(crate) policy: Option<policy::WritePolicy>,
}

/// Start the write-policy task unless the plugin command is empty.
fn start_policy(cfg: &config::WritePolicy) -> Option<policy::WritePolicy> {
    (!cfg.plugin.is_empty())
        .then(|| policy::WritePolicy::new(&cfg.plugin, Duration::from_secs(cfg.timeout_seconds)))
}

/// Apply reloaded configs until the `reload` channel closes, publishing
/// a fresh [`Live`] for every valid one (see [`reload_once`]).
async fn reload_loop(
    mut reload: watch::Receiver<Config>,
    live_tx: watch::Sender<Arc<Live>>,
    limiter: Arc<limiter::ConnectionLimiter>,
    mut live: Arc<Live>,
) {
    while reload.changed().await.is_ok() {
        let incoming = reload.borrow_and_update().clone();
        if let Some(next) = reload_once(incoming, &live, &limiter) {
            live = next;
            live_tx.send_replace(Arc::clone(&live));
        }
    }
}

/// Reader tunables derived from the startup config (restart-only:
/// readers hold them for their lifetime).
fn reader_tuning(config: &Config, sync_memory: &Arc<AtomicUsize>) -> reader::Tuning {
    reader::Tuning {
        budget: Duration::from_micros(config.relay.query_timeslice_budget_microseconds),
        high_water: config.relay.max_pending_outbound_bytes / 4,
        total_budget: config.relay.max_pending_outbound_bytes_total,
        max_filter_limit: config.relay.limits.max_filter_limit,
        max_filter_limit_count: config.relay.limits.max_filter_limit_count,
        max_sync_events: config.relay.limits.max_sync_events,
        max_sync_memory_bytes: config.relay.max_sync_memory_bytes,
        sync_memory: Arc::clone(sync_memory),
    }
}

/// One reload: merge, validate, then apply. Validation runs before any
/// side effect, so a rejected reload leaves the connection caps, the
/// write-policy task, and the limits exactly as they were (`None`).
fn reload_once(
    incoming: Config,
    live: &Live,
    limiter: &limiter::ConnectionLimiter,
) -> Option<Arc<Live>> {
    let (merged, ignored) = live.config.reloadable_from(incoming);
    if !ignored.is_empty() {
        tracing::warn!(?ignored, "restart-only keys ignored on config reload");
    }
    let limits = merged.limits().inspect_err(log_invalid_reload).ok()?;
    Some(Arc::new(apply_reload(merged, limits, live, limiter)))
}

fn log_invalid_reload(err: &ServerError) {
    tracing::error!(%err, "invalid reloaded config; keeping the running one");
}

/// Apply a validated reload: update the connection caps, replace the
/// write-policy task only when its command or timeout changed, and
/// build the next [`Live`].
fn apply_reload(
    merged: Config,
    limits: nosfry_core::limits::Limits,
    live: &Live,
    limiter: &limiter::ConnectionLimiter,
) -> Live {
    limiter.update_caps(
        merged.relay.max_connections,
        merged.relay.max_connections_per_ip,
        limits.rate,
    );
    let policy = if merged.relay.write_policy == live.config.relay.write_policy {
        live.policy.clone()
    } else {
        start_policy(&merged.relay.write_policy)
    };
    tracing::info!("configuration reloaded");
    Live {
        nip11: http::nip11(&merged).into(),
        config: Arc::new(merged),
        limits: Arc::new(limits),
        policy,
    }
}

/// A running server: router state plus the worker threads.
#[derive(Debug)]
pub struct Server {
    state: http::AppState,
    live_tx: watch::Sender<Arc<Live>>,
    stop_tx: watch::Sender<bool>,
    writer_tx: mpsc::Sender<WriterMsg>,
    /// Dropping this unblocks the expiry timer's `recv_timeout` at once.
    expire_stop: std::sync::mpsc::Sender<()>,
    shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    reader_txs: Arc<[std::sync::mpsc::Sender<ReaderMsg>]>,
    threads: Vec<JoinHandle<()>>,
}

impl Server {
    /// Start every worker thread and build the router state.
    ///
    /// # Errors
    ///
    /// [`ServerError::Store`] when the store fails to open,
    /// [`ServerError::Spawn`] when a worker thread cannot start.
    pub fn new(config: Config) -> Result<Self, ServerError> {
        let config = Arc::new(config);
        let store = Arc::new(Store::open(&config.db.path, &config.store_options())?);
        let monitors = config.relay.threads.monitors.max(1);
        let readers = config.relay.threads.readers.max(1);
        let metrics = Arc::new(metrics::Metrics::new());
        // Process-wide pending outbound bytes, shared by every
        // connection's `Outbound`; process-wide sealed-sync-storage
        // bytes, shared by every reader.
        let outbound_total = Arc::new(AtomicUsize::new(0));
        let sync_memory = Arc::new(AtomicUsize::new(0));
        let tuning = reader_tuning(&config, &sync_memory);
        let mut threads = Vec::with_capacity(monitors + readers + 1);
        let (stop_tx, stop_rx) = watch::channel(false);

        // All channels first: shards forward scan control into reader
        // inboxes, readers reply through shard inboxes.
        let mut shard_rxs = Vec::with_capacity(monitors);
        let mut shard_txs = Vec::with_capacity(monitors);
        for _ in 0..monitors {
            let (tx, rx) = mpsc::channel::<ShardMsg>(SHARD_QUEUE);
            shard_txs.push(tx);
            shard_rxs.push(rx);
        }
        let mut reader_rxs = Vec::with_capacity(readers);
        let mut reader_txs = Vec::with_capacity(readers);
        for _ in 0..readers {
            let (tx, rx) = std::sync::mpsc::channel::<ReaderMsg>();
            reader_txs.push(tx);
            reader_rxs.push(rx);
        }
        let reader_txs: Arc<[_]> = reader_txs.into();

        // Monitor shards.
        for rx in shard_rxs {
            let shard_store = Arc::clone(&store);
            let shard_readers = Arc::clone(&reader_txs);
            threads.push(
                std::thread::Builder::new()
                    .name("nosfry-monitor".into())
                    .spawn(move || shard::run(&shard_store, rx, shard_readers))
                    .map_err(ServerError::Spawn)?,
            );
        }

        // Readers.
        let tuning = Arc::new(tuning);
        for rx in reader_rxs {
            let reader_store = Arc::clone(&store);
            let reader_shards = shard_txs.clone();
            let reader_shutdown = stop_rx.clone();
            let reader_metrics = Arc::clone(&metrics);
            let reader_tuning = Arc::clone(&tuning);
            threads.push(
                std::thread::Builder::new()
                    .name("nosfry-reader".into())
                    .spawn(move || {
                        reader::run(
                            &reader_store,
                            &rx,
                            &reader_shards,
                            &reader_tuning,
                            &reader_metrics,
                            &reader_shutdown,
                        );
                    })
                    .map_err(ServerError::Spawn)?,
            );
        }

        // Writer.
        let (writer_tx, writer_rx) = mpsc::channel::<WriterMsg>(WRITER_QUEUE);
        let writer_shards = shard_txs.clone();
        let writer_metrics = Arc::clone(&metrics);
        threads.push(
            std::thread::Builder::new()
                .name("nosfry-writer".into())
                .spawn(move || writer::run(&store, writer_rx, &writer_shards, &writer_metrics))
                .map_err(ServerError::Spawn)?,
        );

        // Expiry timer: owns a writer sender and the stop receiver so
        // the writer channel closes when the thread exits.
        let (expire_stop, expire_stop_rx) = std::sync::mpsc::channel::<()>();
        {
            let expire_writer = writer_tx.clone();
            threads.push(
                std::thread::Builder::new()
                    .name("nosfry-expiry".into())
                    .spawn(move || writer::expire_timer(&expire_writer, &expire_stop_rx))
                    .map_err(ServerError::Spawn)?,
            );
        }
        let (live_tx, live_rx) = watch::channel(Arc::new(Live {
            config: Arc::clone(&config),
            limits: Arc::new(config.limits()?),
            nip11: http::nip11(&config).into(),
            policy: start_policy(&config.relay.write_policy),
        }));
        let conn = connection::Handle {
            writer: writer_tx.clone(),
            shards: Arc::from(shard_txs.as_slice()),
            live: live_rx.clone(),
            metrics: Arc::clone(&metrics),
            outbound_total,
            shutdown: stop_rx,
            tracker: tokio_util::task::TaskTracker::new(),
        };
        Ok(Self {
            state: http::AppState {
                live: live_rx,
                conn,
                sync_memory,
                metrics,
                limiter: Arc::new(limiter::ConnectionLimiter::new(
                    config.relay.max_connections,
                    config.relay.max_connections_per_ip,
                    config.relay.rate_limits,
                )),
            },
            live_tx,
            stop_tx,
            writer_tx,
            expire_stop,
            shard_txs,
            reader_txs,
            threads,
        })
    }

    /// The axum router, ready for `axum::serve`.
    pub fn router(&self) -> axum::Router {
        http::router(self.state.clone())
    }

    /// Tell every connection to close (1001 Going Away).
    pub fn signal_shutdown(&self) {
        self.stop_tx.send_replace(true);
    }

    /// Signal shutdown, drop every worker inbox, and join the threads.
    /// The writer drains and commits its remaining batch on the way out.
    pub fn stop(self) {
        self.signal_shutdown();
        drop(self.state);
        drop(self.writer_tx);
        drop(self.expire_stop);
        drop(self.shard_txs);
        drop(self.reader_txs);
        for thread in self.threads {
            drop(thread.join());
        }
    }
}

/// Read deadline for a complete HTTP request head: a client holding a
/// connection without finishing its headers is dropped (slowloris).
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Whether an `accept` error belongs to one dying connection and can be
/// skipped. Anything else (e.g. `EMFILE`) is logged and the accept loop
/// backs off for a second, as `axum::serve` does.
fn is_connection_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    )
}

/// Accept HTTP/1 connections until `shutdown` resolves, serving each
/// with a header-read deadline `axum::serve` cannot set. In-flight
/// connections get `graceful_shutdown` once `http_stop` fires.
async fn accept_loop(
    listener: &TcpListener,
    router: &axum::Router,
    shutdown: impl Future<Output = ()>,
    http_stop_rx: &watch::Receiver<()>,
    http_tracker: &tokio_util::task::TaskTracker,
) {
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let (tcp, peer) = tokio::select! {
            res = listener.accept() => match res {
                Ok(accepted) => accepted,
                Err(err) => {
                    if !is_connection_error(&err) {
                        tracing::error!(%err, "accept error");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    continue;
                }
            },
            () = &mut shutdown => break,
        };
        spawn_connection(tcp, peer, router, http_stop_rx, http_tracker);
    }
}

/// Serve one accepted socket with the header-read deadline, tracked by
/// `http_tracker` and gracefully shut down when `http_stop` fires.
fn spawn_connection(
    tcp: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    router: &axum::Router,
    http_stop_rx: &watch::Receiver<()>,
    http_tracker: &tokio_util::task::TaskTracker,
) {
    if let Err(err) = tcp.set_nodelay(true) {
        tracing::warn!(%err, "failed to set TCP_NODELAY");
    }
    let service = TowerToHyperService::new(router.clone().into_service().map_request(
        move |mut req: hyper::Request<Incoming>| {
            req.extensions_mut().insert(ConnectInfo(peer));
            req.map(Body::new)
        },
    ));
    let conn = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .serve_connection(TokioIo::new(tcp), service)
        .with_upgrades();
    let mut conn = Box::pin(conn);
    let mut conn_stop = http_stop_rx.clone();
    http_tracker.spawn(async move {
        tokio::select! {
            res = conn.as_mut() => {
                if let Err(err) = res {
                    tracing::debug!(%err, "connection error");
                }
                return;
            }
            _ = conn_stop.changed() => {}
        }
        conn.as_mut().graceful_shutdown();
        if let Err(err) = conn.as_mut().await {
            tracing::debug!(%err, "connection error");
        }
    });
}

/// Close a task tracker and await it with the shutdown deadline.
async fn wait_for(tracker: &tokio_util::task::TaskTracker, what: &str) {
    tracker.close();
    if tokio::time::timeout(Duration::from_secs(10), tracker.wait())
        .await
        .is_err()
    {
        tracing::warn!(
            remaining = tracker.len(),
            "shutdown deadline hit with {what}"
        );
    }
}

/// Run the relay until `shutdown` resolves. `reload` yields new
/// configurations (the binary sends on SIGHUP); each one is merged
/// through [`Config::reloadable_from`] and applied without
/// interrupting service.
///
/// # Errors
///
/// [`ServerError::Store`] or [`ServerError::Spawn`] on startup, or
/// [`ServerError::Shutdown`] when the worker-join task fails.
pub async fn serve(
    config: Config,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
    reload: watch::Receiver<Config>,
) -> Result<(), ServerError> {
    let server = Server::new(config)?;
    // The reload task ends when the sender side drops; it shares
    // neither the connection tracker nor the shutdown signal.
    tokio::spawn(reload_loop(
        reload,
        server.live_tx.clone(),
        Arc::clone(&server.state.limiter),
        server.live_tx.borrow().clone(),
    ));
    let stop = server.stop_tx.clone();
    let tracker = server.state.conn.tracker.clone();
    // HTTP/1 only, with a header-read deadline `axum::serve` cannot set.
    // `http_stop` asks every in-flight connection to shut down
    // gracefully; `http_tracker` awaits them.
    let (http_stop, http_stop_rx) = watch::channel(());
    let http_tracker = tokio_util::task::TaskTracker::new();
    let router = server.router();
    accept_loop(
        &listener,
        &router,
        async move {
            shutdown.await;
            stop.send_replace(true);
        },
        &http_stop_rx,
        &http_tracker,
    )
    .await;
    // The router holds an AppState (writer/shard senders): drop it so
    // `server.stop()`'s channel teardown can complete.
    drop(router);
    // Gracefully close in-flight HTTP connections (idle keep-alive and
    // mid-read conns end promptly), bounded by the shutdown deadline.
    drop(http_stop_rx);
    http_stop.send_replace(());
    wait_for(&http_tracker, "open HTTP connections").await;
    // Stop accepting new connection tasks, then give live ones a
    // deadline to close (the watch signal already went out).
    wait_for(&tracker, "open connections").await;
    // Joining worker threads blocks; keep it off the runtime.
    tokio::task::spawn_blocking(move || server.stop())
        .await
        .map_err(ServerError::Shutdown)?;
    Ok(())
}
