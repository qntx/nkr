//! nkr-server: the native nkr relay.
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

pub mod channels;
pub mod config;
pub mod connection;
pub mod http;
mod reader;
mod shard;
mod writer;

use std::future::Future;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use axum::serve::ListenerExt;
use clap as _;
use nkr_lmdb::Store;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tracing_subscriber as _;

use crate::channels::{ReaderMsg, ShardMsg, WriteRequest};
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
    Store(#[from] nkr_lmdb::StoreError),
    /// TOML parse failure.
    #[error("config: {0}")]
    Config(String),
    /// The HTTP listener failed.
    #[error("serve: {0}")]
    Serve(String),
    /// A worker thread failed to start.
    #[error("thread spawn failed")]
    Spawn,
}

/// A running server: router state plus the worker threads.
#[derive(Debug)]
pub struct Server {
    state: http::AppState,
    stop_tx: watch::Sender<bool>,
    writer_tx: mpsc::Sender<WriteRequest>,
    shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    reader_txs: Vec<std::sync::mpsc::Sender<ReaderMsg>>,
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
        let budget = Duration::from_micros(config.relay.query_timeslice_budget_microseconds);
        let high_water = config.relay.max_pending_outbound_bytes / 4;
        let max_filter_limit = config.relay.limits.max_filter_limit;

        let mut threads = Vec::with_capacity(monitors + readers + 1);

        // Monitor shards.
        let mut shard_txs = Vec::with_capacity(monitors);
        for _ in 0..monitors {
            let (tx, rx) = mpsc::channel::<ShardMsg>(SHARD_QUEUE);
            shard_txs.push(tx);
            let shard_store = Arc::clone(&store);
            threads.push(
                std::thread::Builder::new()
                    .name("nkr-monitor".into())
                    .spawn(move || shard::run(&shard_store, rx))
                    .map_err(|_| ServerError::Spawn)?,
            );
        }

        // Readers.
        let mut reader_txs = Vec::with_capacity(readers);
        for _ in 0..readers {
            let (tx, rx) = std::sync::mpsc::channel::<ReaderMsg>();
            reader_txs.push(tx);
            let reader_store = Arc::clone(&store);
            let reader_shards = shard_txs.clone();
            threads.push(
                std::thread::Builder::new()
                    .name("nkr-reader".into())
                    .spawn(move || {
                        reader::run(
                            &reader_store,
                            &rx,
                            &reader_shards,
                            monitors,
                            budget,
                            high_water,
                            max_filter_limit,
                        );
                    })
                    .map_err(|_| ServerError::Spawn)?,
            );
        }

        // Writer.
        let (writer_tx, writer_rx) = mpsc::channel::<WriteRequest>(WRITER_QUEUE);
        let writer_shards = shard_txs.clone();
        threads.push(
            std::thread::Builder::new()
                .name("nkr-writer".into())
                .spawn(move || writer::run(&store, writer_rx, &writer_shards))
                .map_err(|_| ServerError::Spawn)?,
        );

        let (stop_tx, stop_rx) = watch::channel(false);
        let conn = connection::Handle {
            writer: writer_tx.clone(),
            shards: Arc::from(shard_txs.as_slice()),
            readers: Arc::from(reader_txs.as_slice()),
            limits: Arc::new(config.limits()),
            ping_interval: Duration::from_secs(config.relay.auto_ping_seconds),
            max_pending_outbound: config.relay.max_pending_outbound_bytes,
            shutdown: stop_rx,
            tracker: tokio_util::task::TaskTracker::new(),
        };
        Ok(Self {
            state: http::AppState { config, conn },
            stop_tx,
            writer_tx,
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
        drop(self.shard_txs);
        drop(self.reader_txs);
        for thread in self.threads {
            drop(thread.join());
        }
    }
}

/// Run the relay until `shutdown` resolves.
///
/// # Errors
///
/// [`ServerError::Store`], [`ServerError::Spawn`], or
/// [`ServerError::Serve`] on listener/serve failure.
pub async fn serve(
    config: Config,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServerError> {
    let server = Server::new(config)?;
    let stop = server.stop_tx.clone();
    let tracker = server.state.conn.tracker.clone();
    axum::serve(
        listener.tap_io(|tcp: &mut tokio::net::TcpStream| {
            if let Err(err) = tcp.set_nodelay(true) {
                tracing::warn!(%err, "failed to set TCP_NODELAY");
            }
        }),
        server
            .router()
            .into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.await;
        stop.send_replace(true);
    })
    .await
    .map_err(|e| ServerError::Serve(e.to_string()))?;
    // Stop accepting new connection tasks, then give live ones a
    // deadline to close (the watch signal already went out).
    tracker.close();
    if tokio::time::timeout(Duration::from_secs(10), tracker.wait())
        .await
        .is_err()
    {
        tracing::warn!(
            remaining = tracker.len(),
            "shutdown deadline hit with open connections"
        );
    }
    // Joining worker threads blocks; keep it off the runtime.
    tokio::task::spawn_blocking(move || server.stop())
        .await
        .map_err(|e| ServerError::Serve(format!("stop join: {e}")))?;
    Ok(())
}
