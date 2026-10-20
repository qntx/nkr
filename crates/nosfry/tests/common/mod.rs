//! Shared test helpers: deterministic signing, client framing, server
//! bootstrap.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    missing_docs,
    unreachable_pub,
    reason = "test helpers may panic and are only partially used per binary"
)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use nosfry::config::Config;
use nosfry_core::event::{Event, EventLimits, UnverifiedEvent};
use secp256k1::{Keypair, SecretKey, schnorr};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, watch};

/// Fixed `n`-th test keypair.
pub fn key(n: u8) -> Keypair {
    let mut bytes = [0u8; 32];
    bytes[31] = n.max(1);
    Keypair::from_secret_key(&SecretKey::from_secret_bytes(bytes).unwrap())
}

/// Build a signed, verified event.
pub fn event(
    keypair: &Keypair,
    kind: u16,
    created_at: u64,
    tags: &Value,
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

/// The client EVENT wire message for an event.
pub fn event_wire(event: &Event) -> String {
    format!("[\"EVENT\",{}]", event.json())
}

/// A REQ wire message.
pub fn req_wire(sub_id: &str, filters: &str) -> String {
    format!("[\"REQ\",\"{sub_id}\",{filters}]")
}

/// A CLOSE wire message.
pub fn close_wire(sub_id: &str) -> String {
    format!("[\"CLOSE\",\"{sub_id}\"]")
}

/// Shared test config: rate limits and the per-IP connection cap are
/// disabled so traffic-heavy tests are not throttled; tests that need
/// them set the fields back explicitly.
pub fn config() -> Config {
    let mut config = Config::default();
    config.relay.rate_limits = nosfry_core::limits::RateLimits {
        events_per_second: 0,
        events_burst: 0,
        reqs_per_second: 0,
        reqs_burst: 0,
        ip_events_per_second: 0,
        ip_events_burst: 0,
        ip_reqs_per_second: 0,
        ip_reqs_burst: 0,
    };
    config.relay.max_connections_per_ip = 0;
    config
}

/// Running test relay.
pub struct Relay {
    /// Bound address.
    pub addr: SocketAddr,
    /// Push a config reload (the SIGHUP path's channel end).
    reload_tx: watch::Sender<Config>,
    stop: oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
    /// Keep the store directory alive.
    _dir: tempfile::TempDir,
}

impl Relay {
    /// Start a relay with the default config on a tempdir store.
    pub async fn start() -> Self {
        Self::start_with(config()).await
    }

    /// Start with a caller-adjusted config; the `db.path` inside it is
    /// overridden by the tempdir unless `db_path` is set.
    pub async fn start_with(mut config: Config) -> Self {
        let dir = tempfile::tempdir().unwrap();
        if config.db.path.as_os_str() == "./nosfry-db" {
            config.db.path = dir.path().to_path_buf();
        }
        let (tx, rx) = oneshot::channel();
        let (reload_tx, reload_rx) = watch::channel(config.clone());
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            nosfry::serve(
                config,
                listener,
                async {
                    rx.await.ok();
                },
                reload_rx,
            )
            .await
            .unwrap();
        });
        // Wait until the listener accepts.
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Self {
            addr,
            reload_tx,
            stop: tx,
            handle,
            _dir: dir,
        }
    }

    /// Send a config through the reload channel (what SIGHUP does in
    /// the binary).
    pub fn reload(&self, config: Config) {
        self.reload_tx.send(config).unwrap();
    }

    /// A websocket client connection.
    pub async fn connect(&self) -> yawc::TcpWebSocket {
        let url = format!("ws://{}/", self.addr);
        yawc::WebSocket::connect(url.parse().unwrap())
            .await
            .unwrap()
    }

    /// Graceful stop: drop the senders, drain, join.
    pub async fn stop(self) {
        self.stop.send(()).ok();
        self.handle.await.ok();
    }
}

/// Receive one text frame within `timeout`; panics on close/timeout.
pub async fn recv_text(ws: &mut yawc::TcpWebSocket, timeout: Duration) -> String {
    use futures_util::StreamExt;
    loop {
        let frame = tokio::time::timeout(timeout, ws.next())
            .await
            .expect("frame timeout")
            .expect("stream ended");
        if frame.opcode() == yawc::OpCode::Text {
            return std::str::from_utf8(frame.payload()).unwrap().to_owned();
        }
    }
}

/// Send one text frame.
pub async fn send_text(ws: &mut yawc::TcpWebSocket, text: &str) {
    use futures_util::SinkExt;
    ws.send(yawc::Frame::text(text.to_owned())).await.unwrap();
}
