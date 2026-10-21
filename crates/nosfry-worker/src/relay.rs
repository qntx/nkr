//! The relay object: connections, sessions, the live-subscription
//! index, and hibernation state over a [`SqlStore`].

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use nosfry_core::auth::{AUTH_KIND, ReadGate};
use nosfry_core::event::{Event, PublicKey};
use nosfry_core::filter::Filter;
use nosfry_core::hll::Hll;
use nosfry_core::limits::{AddressBuckets, Limits};
use nosfry_core::management::{self, Action, Request};
use nosfry_core::message::{
    SubId, closed_frame, count_frame, eose_frame, event_frame, neg_err_frame,
    neg_err_frame_with_limit, neg_msg_frame, ok_frame,
};
use nosfry_core::moderation::Moderation;
use nosfry_core::monitor::Monitors;
use nosfry_core::session::{Command, Session, SubKey};
use nosfry_core::store::WriteOutcome;
use nosfry_core::sync::{self, NegentropyStorageVector};
use nosfry_sql::{Executor, Param, SqlStore, StoreError, Value};
use serde::{Deserialize, Serialize};

use crate::config::{Config, ConfigError};

/// Connection ids must stay below 2^53: they are bound into SQL,
/// where integers are JavaScript doubles. The glue assigns them
/// (e.g. random 53-bit) and `accept`/`open` refuse larger ones.
const JS_MAX: u64 = (1 << 53) - 1;

/// A negentropy session idle this long is reaped with
/// `closed: idle timeout` — the native 60 seconds.
const NEG_IDLE_MS: u64 = 60_000;

/// A `u64` bound into SQL: ids stay under 2^53 already, so the
/// clamp is unreachable except for corrupt `subs` rows.
fn int(v: u64) -> Param<'static> {
    Param::Int(i64::try_from(v.min(JS_MAX)).unwrap_or(i64::MAX))
}

/// The `subs` schema the relay owns; created in [`Relay::open`].
const SUBS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS subs (
    conn INTEGER NOT NULL,
    sub_id TEXT NOT NULL,
    filters TEXT NOT NULL,
    authed INTEGER NOT NULL,
    PRIMARY KEY (conn, sub_id)
) WITHOUT ROWID";

/// What the platform does with a text frame and a close.
pub trait Socket {
    /// Send one text frame.
    fn send(&self, text: &str);
    /// Close the socket with a WebSocket code and reason.
    fn close(&self, code: u16, reason: &str);
}

/// Per-socket hibernation state, serialized into the platform's
/// WebSocket attachment (under 16 KiB at [`MAX_AUTHED_PUBKEYS`]).
///
/// [`MAX_AUTHED_PUBKEYS`]: nosfry_core::auth::MAX_AUTHED_PUBKEYS
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    /// Caller-chosen connection id (< 2^53, unique among live sockets).
    pub conn: u64,
    /// The client address the per-IP rate buckets key on.
    pub ip: Option<IpAddr>,
    /// The NIP-42 challenge issued to this connection.
    pub challenge: String,
    /// Whether the challenge was already sent.
    pub challenge_sent: bool,
    /// Authenticated pubkeys, lowercase hex.
    pub authed: Vec<String>,
}

/// Side effects one [`Relay::message`] call produced, beyond the
/// frames already written to sockets.
#[derive(Debug, Default)]
pub struct Effects {
    /// The connection's attachment changed (the challenge was sent or
    /// the authenticated set grew); the glue rewrites it on the socket.
    pub attachment: Option<Attachment>,
    /// The earliest expiration among events stored by this call; the
    /// glue moves the alarm forward when it precedes the scheduled one.
    pub expiration: Option<u64>,
    /// A stored (or duplicate) NIP-62 request to vanish queued
    /// deletion work; the glue schedules the alarm for now.
    pub vanish: bool,
}

/// The management RPC answer the glue turns into an HTTP response.
#[derive(Debug)]
pub struct Managed {
    /// The HTTP status.
    pub status: u16,
    /// The response body — a JSON object for 200/401 answers, plain
    /// text for 400/413 like the native axum handler.
    pub body: String,
}

/// [`Relay`] construction failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RelayError {
    /// The configuration is invalid.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The store failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A connection id exceeds the JavaScript-safe integer range.
    #[error("connection id exceeds the JavaScript-safe integer range")]
    ConnId,
}

/// One live connection.
#[derive(Debug)]
struct Conn<S> {
    /// The platform socket.
    socket: S,
    /// The sans-IO protocol state.
    session: Session,
    /// The client address the shared buckets key on.
    ip: Option<IpAddr>,
    /// The connection's NIP-42 challenge (the session also holds it).
    challenge: String,
}

/// One open negentropy sync: the sealed storage `NEG-MSG`s reconcile
/// against. Sessions live only in the object's memory — a Durable
/// Object wake drops them all and the next `NEG-MSG` answers
/// `closed: unknown subscription` (ADR 0007: sync state does not
/// hibernate).
#[derive(Debug)]
struct NegSession {
    /// The session generation the slot opened under.
    generation: u64,
    /// The sealed `(created_at, id)` records.
    storage: NegentropyStorageVector,
    /// The sync-memory reservation this session holds.
    reserved: usize,
    /// Last `NEG-OPEN`/`NEG-MSG` time; [`NEG_IDLE_MS`] reaps idle ones.
    last_active_ms: u64,
}

/// The relay object: the SQL store, the session per connection, and
/// the monitor index — everything but the platform's socket, timer,
/// and persistence calls.
#[derive(Debug)]
pub struct Relay<E: Executor, S: Socket> {
    store: SqlStore<E>,
    config: Config,
    limits: Arc<Limits>,
    /// Parsed `[relay.management] admins` — hex strings are validated
    /// in [`Config::parse`]; `open` parses them once.
    admins: Vec<PublicKey>,
    /// The NIP-86 snapshot hot paths read; `manage` refreshes it
    /// after every applied op.
    moderation: Moderation,
    conns: HashMap<u64, Conn<S>>,
    /// `AddressBuckets` shared by every connection of one client
    /// address; the entry dies with the last.
    buckets: HashMap<IpAddr, Arc<AddressBuckets>>,
    monitors: Monitors<u64>,
    /// Open negentropy sessions by `(conn, subscription id)`.
    negs: HashMap<(u64, SubId), NegSession>,
    /// Total bytes the open negentropy sessions have reserved against
    /// `[relay] max_sync_memory_bytes` (`0` = unlimited).
    sync_reserved: usize,
}

impl<E: Executor, S: Socket> Relay<E, S> {
    /// Open the object: create the `subs` table, resume one session
    /// per `(socket, attachment)` pair, reload their subscriptions
    /// into the monitor index, and drop `subs` rows whose connection
    /// is not live (a crash or deploy strands them).
    ///
    /// # Errors
    ///
    /// [`RelayError::Config`] on an invalid config,
    /// [`RelayError::ConnId`] on an attachment conn id above 2^53 − 1,
    /// [`RelayError::Store`] on SQL failures or corrupt `subs` rows.
    pub fn open(
        store: SqlStore<E>,
        config: Config,
        sockets: Vec<(S, Attachment)>,
    ) -> Result<Self, RelayError> {
        let limits = Arc::new(config.limits()?);
        store
            .exec()
            .execute(SUBS_SCHEMA, &[])
            .map_err(|err| StoreError::Sql(Box::new(err)))?;
        let moderation = store.moderation()?;
        let admins = config
            .relay
            .management
            .admin_keys()
            .map_err(ConfigError::Admins)?;
        let mut relay = Self {
            store,
            config,
            limits,
            admins,
            moderation,
            conns: HashMap::new(),
            buckets: HashMap::new(),
            monitors: Monitors::new(),
            negs: HashMap::new(),
            sync_reserved: 0,
        };
        for (socket, attachment) in sockets {
            if attachment.conn > JS_MAX {
                return Err(RelayError::ConnId);
            }
            let buckets = relay.buckets_for(attachment.ip);
            let authed: Arc<[PublicKey]> = attachment
                .authed
                .iter()
                .filter_map(|hex| PublicKey::from_hex(hex))
                .collect::<Vec<_>>()
                .into();
            let session = Session::resume(
                Arc::clone(&relay.limits),
                attachment.challenge.clone(),
                buckets,
                attachment.challenge_sent,
                authed,
            );
            relay.conns.insert(
                attachment.conn,
                Conn {
                    socket,
                    session,
                    ip: attachment.ip,
                    challenge: attachment.challenge,
                },
            );
        }
        relay.restore_subs()?;
        Ok(relay)
    }

    /// Whether `ip` is on the NIP-86 block list; the glue answers 403
    /// before upgrades, NIP-11, and the management RPC — the native
    /// order.
    #[must_use]
    pub fn ip_blocked(&self, ip: Option<IpAddr>) -> bool {
        ip.is_some_and(|ip| self.moderation.ip_blocked(&ip))
    }

    /// Whether `ip` may open another connection under the global and
    /// per-address caps; the glue answers 503 before upgrading.
    /// Address-less connections count toward the global cap only.
    #[must_use]
    pub fn admits(&self, ip: Option<IpAddr>) -> bool {
        let global = self.config.relay.max_connections;
        if global != 0 && self.conns.len() >= global {
            return false;
        }
        let per_ip = self.config.relay.max_connections_per_ip;
        ip.is_none_or(|ip| {
            per_ip == 0 || self.conns.values().filter(|c| c.ip == Some(ip)).count() < per_ip
        })
    }

    /// Register a new socket. `conn` is caller-chosen and must be
    /// unique among live connections and below 2^53 (it is bound into
    /// `subs` rows). Returns the initial attachment to persist on the
    /// socket.
    ///
    /// # Errors
    ///
    /// [`RelayError::ConnId`] when `conn` exceeds 2^53 − 1.
    pub fn accept(
        &mut self,
        socket: S,
        conn: u64,
        ip: Option<IpAddr>,
        challenge: String,
    ) -> Result<Attachment, RelayError> {
        if conn > JS_MAX {
            return Err(RelayError::ConnId);
        }
        let buckets = self.buckets_for(ip);
        let session = Session::new(Arc::clone(&self.limits), challenge.clone(), buckets);
        self.conns.insert(
            conn,
            Conn {
                socket,
                session,
                ip,
                challenge: challenge.clone(),
            },
        );
        Ok(Attachment {
            conn,
            ip,
            challenge,
            challenge_sent: false,
            authed: Vec::new(),
        })
    }

    /// Feed one inbound text frame, run the resulting commands in
    /// order, and report side effects. A text longer than
    /// `max_websocket_payload_size` closes the socket with 1009 and
    /// drops the connection exactly like [`Relay::close`].
    #[must_use]
    pub fn message(&mut self, conn: u64, now_ms: u64, text: &str) -> Effects {
        let mut effects = Effects::default();
        self.reap_negs(now_ms);
        if text.len() > self.config.relay.max_websocket_payload_size {
            if let Some(c) = self.conns.get(&conn) {
                c.socket.close(1009, "message too big");
            }
            self.close(conn);
            return effects;
        }
        let Some(c) = self.conns.get_mut(&conn) else {
            return effects;
        };
        let challenge_sent = c.session.challenge_sent();
        let authed = c.session.authenticated().len();
        let mut commands = Vec::new();
        // `Received` only carries metrics (verb, rate_limited); the
        // worker has no metrics.
        let _received = c.session.on_text(now_ms, text, &mut commands);
        for command in commands {
            self.command(conn, command, now_ms, &mut effects);
        }
        if let Some(live) = self.conns.get(&conn)
            && (live.session.challenge_sent() != challenge_sent
                || live.session.authenticated().len() != authed)
        {
            effects.attachment = Some(Attachment {
                conn,
                ip: live.ip,
                challenge: live.challenge.clone(),
                challenge_sent: live.session.challenge_sent(),
                authed: live
                    .session
                    .authenticated()
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            });
        }
        effects
    }

    /// Drop a connection: remove its subscriptions from the monitor
    /// index, delete its `subs` rows, release its negentropy
    /// reservations, and release its share of the address buckets.
    pub fn close(&mut self, conn: u64) {
        self.drop_conn_negs(conn);
        self.monitors.remove_connection(conn);
        self.exec("DELETE FROM subs WHERE conn = ?1", &[int(conn)]);
        let Some(c) = self.conns.remove(&conn) else {
            return;
        };
        // `c` still holds its session's bucket share: two references
        // left (the map's and the session's) means this was the last.
        if let Some(ip) = c.ip
            && self
                .buckets
                .get(&ip)
                .is_some_and(|b| Arc::strong_count(b) == 2)
        {
            self.buckets.remove(&ip);
        }
    }

    /// Run the NIP-40 and NIP-62 sweeps: delete up to 10,000 expired
    /// events, then drain one vanish step of up to 10,000 examined
    /// index rows. Answers the next alarm time — `Some(now)` while
    /// either backlog remains (run again immediately), else the next
    /// expiration or `None`.
    ///
    /// # Errors
    ///
    /// [`StoreError::Sql`] on executor failure.
    pub fn alarm(&self, now: u64) -> Result<Option<u64>, StoreError> {
        let expiring = self.store.expire(now, 10_000)? == 10_000;
        let step = self.store.vanish_step(10_000)?;
        if expiring || step.more {
            return Ok(Some(now));
        }
        self.store.next_expiration()
    }

    /// The alarm the object should wake at: `Some(now)` while vanish
    /// work is pending, else the next expiration — the value the
    /// glue reconciles against the stored alarm on wake so work
    /// committed just before a crash still runs.
    ///
    /// # Errors
    ///
    /// [`StoreError::Sql`] on executor failure.
    pub fn next_alarm(&self, now: u64) -> Result<Option<u64>, StoreError> {
        if self.store.vanish_pending()? {
            return Ok(Some(now));
        }
        self.store.next_expiration()
    }

    /// The NIP-86 management endpoint: `POST` with the
    /// `application/nostr+json+rpc` content type, NIP-98 authorized
    /// against `[relay.management] admins` — the native status
    /// table: 405 without either, 413 over [`management::BODY_LIMIT`]
    /// (the glue refuses earlier without buffering), 401 on
    /// authorization failure, 400 on invalid UTF-8/JSON, and method
    /// errors as `{"error": …}` at 200.
    #[must_use]
    pub fn manage(
        &mut self,
        content_type: Option<&str>,
        authorization: Option<&str>,
        body: &[u8],
        now: u64,
    ) -> Managed {
        let managed = |status, value: serde_json::Value| Managed {
            status,
            body: value.to_string(),
        };
        let rpc = content_type
            .and_then(|v| v.split(';').next())
            .is_some_and(|t| t.trim().eq_ignore_ascii_case(management::CONTENT_TYPE));
        if !rpc || self.admins.is_empty() {
            return Managed {
                status: 405,
                body: String::new(),
            };
        }
        if body.len() > management::BODY_LIMIT {
            return Managed {
                status: 413,
                body: "Failed to buffer the request body: length limit exceeded".to_owned(),
            };
        }
        if let Err(error) = management::authorize(
            authorization,
            body,
            now,
            &self.config.events,
            self.limits.auth.relay(),
            &self.admins,
        ) {
            return managed(
                401,
                serde_json::json!({"error": format!("unauthorized: {error}")}),
            );
        }
        let Ok(text) = std::str::from_utf8(body) else {
            return Managed {
                status: 400,
                body: "invalid utf-8".to_owned(),
            };
        };
        let Ok(request) = serde_json::from_str::<Request<'_>>(text) else {
            return Managed {
                status: 400,
                body: "invalid request json".to_owned(),
            };
        };
        match management::action(&request, &self.moderation) {
            Ok(Action::Respond(result)) => managed(200, serde_json::json!({"result": result})),
            Ok(Action::Apply(op)) => match self.store.moderate(op) {
                Ok(moderation) => {
                    self.moderation = moderation;
                    managed(200, serde_json::json!({"result": true}))
                }
                Err(err) => managed(200, serde_json::json!({"error": format!("error: {err}")})),
            },
            Err(error) => managed(200, serde_json::json!({"error": error})),
        }
    }

    /// The NIP-11 document: the implemented NIPs only — NIP-45 when
    /// COUNT is enabled, NIP-42 when `service_url` is set, NIP-62
    /// always, NIP-86 when management admins are configured. The
    /// moderation snapshot overrides `name`/`description`/`icon` like
    /// native.
    #[must_use]
    pub fn nip11(&self) -> String {
        let mut nips = vec![1, 9, 11, 40, 62, 70];
        if self.limits.req.max_filter_limit_count > 0 {
            nips.push(45);
        }
        if self.sync_cap() > 0 {
            nips.push(77);
        }
        if !self.config.relay.service_url.is_empty() {
            nips.push(42);
        }
        if !self.config.relay.management.admins.is_empty() {
            nips.push(86);
        }
        nips.sort_unstable();
        let relay = &self.config.relay;
        let info = nosfry_core::info::Info {
            name: self
                .moderation
                .name
                .as_deref()
                .unwrap_or(&relay.info.name)
                .to_owned(),
            description: self
                .moderation
                .description
                .as_deref()
                .unwrap_or(&relay.info.description)
                .to_owned(),
            pubkey: relay.info.pubkey.clone(),
            contact: relay.info.contact.clone(),
            icon: self
                .moderation
                .icon
                .as_deref()
                .unwrap_or(&relay.info.icon)
                .to_owned(),
        };
        nosfry_core::info::document(&info, &nips, &self.limits, relay.max_websocket_payload_size)
    }

    /// Consume the relay and return the store (tests reopen it with
    /// `into_exec`).
    pub fn into_store(self) -> SqlStore<E> {
        self.store
    }

    /// The shared `AddressBuckets` for `ip` (address-less connections
    /// get a disabled pair they never share).
    fn buckets_for(&mut self, ip: Option<IpAddr>) -> Arc<AddressBuckets> {
        match ip {
            Some(ip) => Arc::clone(
                self.buckets
                    .entry(ip)
                    .or_insert_with(|| Arc::new(AddressBuckets::new(&self.limits.rate))),
            ),
            None => Arc::new(AddressBuckets::default()),
        }
    }

    /// Run `sql` best-effort. These calls are cleanup deletes whose
    /// failure is unrecoverable anyway (the executor is broken), so
    /// the outcome is intentionally dropped rather than propagated.
    fn exec(&self, sql: &str, params: &[Param<'_>]) {
        drop(self.store.exec().execute(sql, params));
    }

    /// Send `text` on `conn`'s socket if it is still live.
    fn send(&self, conn: u64, text: &str) {
        if let Some(c) = self.conns.get(&conn) {
            c.socket.send(text);
        }
    }

    /// Execute one session command in order. `now_ms` is unix time in
    /// milliseconds; store queries take its seconds.
    fn command(&mut self, conn: u64, command: Command, now_ms: u64, effects: &mut Effects) {
        match command {
            Command::Send { text, .. } => self.send(conn, &text),
            Command::Publish { event, .. } => self.publish(conn, &event, effects),
            Command::Subscribe { sub, filters, gate } => {
                self.subscribe(conn, sub, filters, gate, now_ms / 1000);
            }
            Command::Unsubscribe { sub_id } => self.unsubscribe(conn, &sub_id),
            Command::Count { sub, filters, gate } => {
                self.count(conn, &sub, &filters, &gate, now_ms / 1000);
            }
            Command::NegOpen {
                sub,
                filters,
                gate,
                msg,
            } => self.neg_open(conn, &sub, &filters, &gate, &msg, now_ms),
            Command::NegMsg { sub, msg } => self.neg_msg(conn, &sub, &msg, now_ms),
            Command::NegClose { sub_id } => self.neg_close(conn, &sub_id),
        }
    }

    /// `min([relay.limits] max_sync_events, [worker] max_scan_rows)` —
    /// the record cap a `NEG-OPEN` answer obeys and NIP-11 advertises.
    fn sync_cap(&self) -> u64 {
        self.limits
            .req
            .max_sync_events
            .min(self.config.worker.max_scan_rows as u64)
    }

    /// Send `text` as the negentropy frame native sends, closing the
    /// session slot after it when `closes`. A stale generation drops
    /// the frame without sending, like native.
    fn neg_frame(&mut self, conn: u64, sub: &SubKey, text: &str, closes: bool) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        if !c.session.is_current_neg(sub) {
            return;
        }
        c.socket.send(text);
        if closes {
            c.session.close_neg_by_relay(sub);
        }
    }

    /// A `NEG-OPEN`: the records scan admits the session, over-cap or
    /// exhausted-budget scans are refused `blocked:` with the
    /// effective limit, and the memory budget refuses
    /// `blocked: sync capacity exhausted` — the native wording.
    /// Success always answers `NEG-MSG`, even an empty protocol round.
    fn neg_open(
        &mut self,
        conn: u64,
        sub: &SubKey,
        filters: &[Filter],
        gate: &ReadGate,
        msg: &[u8],
        now_ms: u64,
    ) {
        self.reap_negs(now_ms);
        let cap = self.sync_cap();
        let found = self.store.records(
            filters,
            gate,
            now_ms / 1000,
            cap.saturating_add(1),
            self.config.worker.max_scan_rows,
        );
        let records = match found {
            Err(_) => {
                self.neg_frame(
                    conn,
                    sub,
                    &neg_err_frame(&sub.id, "error: internal error"),
                    true,
                );
                return;
            }
            Ok(found) if found.budget_exhausted || found.records.len() as u64 > cap => {
                self.neg_frame(
                    conn,
                    sub,
                    &neg_err_frame_with_limit(&sub.id, "blocked: too many query results", cap),
                    true,
                );
                return;
            }
            Ok(found) => found.records,
        };
        let reserved = records.len() * sync::SYNC_RECORD_BYTES;
        let budget = self.config.relay.max_sync_memory_bytes;
        if budget != 0 && self.sync_reserved.saturating_add(reserved) > budget {
            self.neg_frame(
                conn,
                sub,
                &neg_err_frame(&sub.id, "blocked: sync capacity exhausted"),
                true,
            );
            return;
        }
        let opened = sync::build_storage(&records)
            .map_err(|_| ())
            .and_then(|storage| {
                sync::reconcile(&storage, msg)
                    .map(|reply| (storage, reply))
                    .map_err(|_| ())
            });
        match opened {
            Ok((storage, reply)) => {
                self.neg_frame(
                    conn,
                    sub,
                    &neg_msg_frame(&sub.id, &faster_hex::hex_string(&reply)),
                    false,
                );
                let live = self
                    .conns
                    .get(&conn)
                    .is_some_and(|c| c.session.is_current_neg(sub));
                if live {
                    self.sync_reserved += reserved;
                    self.negs.insert(
                        (conn, sub.id.clone()),
                        NegSession {
                            generation: sub.generation,
                            storage,
                            reserved,
                            last_active_ms: now_ms,
                        },
                    );
                }
            }
            Err(()) => {
                self.neg_frame(
                    conn,
                    sub,
                    &neg_err_frame(&sub.id, "invalid: malformed negentropy message"),
                    true,
                );
            }
        }
    }

    /// A `NEG-MSG`: reconcile against the stored records and answer
    /// `NEG-MSG` (even an empty round), or close with the native
    /// wording — `closed: unknown subscription` when no session
    /// answers, `invalid: malformed negentropy message` on a wire
    /// error. Both failures release the reservation.
    fn neg_msg(&mut self, conn: u64, sub: &SubKey, msg: &[u8], now_ms: u64) {
        let key = (conn, sub.id.clone());
        match self.negs.get_mut(&key) {
            Some(session) if session.generation == sub.generation => {
                if let Ok(reply) = sync::reconcile(&session.storage, msg) {
                    session.last_active_ms = now_ms;
                    self.neg_frame(
                        conn,
                        sub,
                        &neg_msg_frame(&sub.id, &faster_hex::hex_string(&reply)),
                        false,
                    );
                } else {
                    self.drop_neg(&key);
                    self.neg_frame(
                        conn,
                        sub,
                        &neg_err_frame(&sub.id, "invalid: malformed negentropy message"),
                        true,
                    );
                }
            }
            _ => self.neg_frame(
                conn,
                sub,
                &neg_err_frame(&sub.id, "closed: unknown subscription"),
                true,
            ),
        }
    }

    /// A `NEG-CLOSE`: drop the session and release its reservation
    /// (the session slot is already gone — `drop_neg` ran first).
    fn neg_close(&mut self, conn: u64, sub_id: &SubId) {
        self.drop_neg(&(conn, sub_id.clone()));
    }

    /// Release `key`'s session reservation and drop it.
    fn drop_neg(&mut self, key: &(u64, SubId)) {
        if let Some(session) = self.negs.remove(key) {
            self.sync_reserved -= session.reserved;
        }
    }

    /// Drop every negentropy session `conn` owns — socket close or
    /// message-too-big teardown.
    fn drop_conn_negs(&mut self, conn: u64) {
        let keys: Vec<(u64, SubId)> = self
            .negs
            .keys()
            .filter(|(c, _)| *c == conn)
            .cloned()
            .collect();
        for key in keys {
            self.drop_neg(&key);
        }
    }

    /// Reap sessions idle [`NEG_IDLE_MS`] or longer with
    /// `closed: idle timeout` — the lazy native reaper, run at the
    /// head of every `message()` and before admitting a session.
    fn reap_negs(&mut self, now_ms: u64) {
        if self.negs.is_empty() {
            return;
        }
        let dead: Vec<(u64, SubKey)> = self
            .negs
            .iter()
            .filter(|(_, s)| now_ms.saturating_sub(s.last_active_ms) >= NEG_IDLE_MS)
            .map(|((conn, id), s)| {
                (
                    *conn,
                    SubKey {
                        id: id.clone(),
                        generation: s.generation,
                    },
                )
            })
            .collect();
        for (conn, sub) in dead {
            self.drop_neg(&(conn, sub.id.clone()));
            self.neg_frame(
                conn,
                &sub,
                &neg_err_frame(&sub.id, "closed: idle timeout"),
                true,
            );
        }
    }

    /// A verified event: answer `OK`, persist non-ephemeral kinds,
    /// and deliver a `Stored` or accepted ephemeral event to every
    /// current subscription, the publisher's own included.
    fn publish(&mut self, conn: u64, event: &Arc<Event>, effects: &mut Effects) {
        // NIP-86: the moderation snapshot gates every publish,
        // ephemeral kinds included — the same position the native
        // connection checks. AUTH events and a relay-scoped request
        // to vanish are exempt (a banned author may still
        // authenticate or vanish).
        if event.kind() != AUTH_KIND
            && !event.requests_vanish(self.limits.auth.relay())
            && let Some(reason) = self.moderation.blocked_reason(event)
        {
            self.send(conn, &ok_frame(&event.id(), false, reason));
            return;
        }
        if event.kind().is_ephemeral() {
            self.send(conn, &ok_frame(&event.id(), true, ""));
            self.deliver(event);
            return;
        }
        match self
            .store
            .write(std::slice::from_ref(event), self.limits.auth.relay())
        {
            Ok(written) => {
                let Some(&outcome) = written.outcomes.first() else {
                    return;
                };
                effects.vanish |= written.vanish_pending;
                let (accepted, message) = outcome.ok_reply();
                self.send(conn, &ok_frame(&event.id(), accepted, message));
                if matches!(outcome, WriteOutcome::Stored { .. }) {
                    self.deliver(event);
                    effects.expiration = event.expiration();
                }
            }
            Err(_) => self.send(conn, &ok_frame(&event.id(), false, "error: internal error")),
        }
    }

    /// Fan `event` out to every matching current subscription.
    fn deliver(&mut self, event: &Event) {
        let mut hits = Vec::new();
        self.monitors.matches(event.packed(), &mut hits);
        for (conn, sub) in hits {
            if let Some(c) = self.conns.get(&conn)
                && c.session.is_current(&sub)
            {
                c.socket.send(&event_frame(&sub.id, event.json()));
            }
        }
    }

    /// A `REQ`: stream the stored hits then `EOSE`, register the
    /// monitor, and persist the subscription — only a subscription
    /// that survived its `EOSE` reaches the `subs` table.
    fn subscribe(
        &mut self,
        conn: u64,
        sub: SubKey,
        filters: Arc<[Filter]>,
        gate: ReadGate,
        now: u64,
    ) {
        let Ok(scan) = self.store.query(
            &filters,
            &gate,
            now,
            self.limits.req.max_filter_limit,
            self.config.worker.max_scan_rows,
        ) else {
            self.send(conn, &closed_frame(&sub.id, "error: internal error"));
            if let Some(c) = self.conns.get_mut(&conn) {
                c.session.close_by_relay(&sub);
            }
            return;
        };
        for hit in &scan.hits {
            self.send(conn, &event_frame(&sub.id, &hit.json));
        }
        self.send(conn, &eose_frame(&sub.id));
        let authed = self
            .conns
            .get(&conn)
            .map_or(0, |c| c.session.authenticated().len());
        let authed = u64::try_from(authed).unwrap_or(u64::MAX);
        let filters_json = format!(
            "[{}]",
            filters
                .iter()
                .map(Filter::to_json)
                .collect::<Vec<_>>()
                .join(",")
        );
        let persisted = self
            .store
            .exec()
            .execute(
                "INSERT INTO subs (conn, sub_id, filters, authed) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (conn, sub_id) DO UPDATE SET filters = excluded.filters,
                     authed = excluded.authed",
                &[
                    int(conn),
                    Param::Text(sub.id.as_str()),
                    Param::Text(&filters_json),
                    int(authed),
                ],
            )
            .is_ok();
        if persisted {
            self.monitors.insert(conn, sub, filters, gate);
        } else {
            self.send(conn, &closed_frame(&sub.id, "error: internal error"));
            if let Some(c) = self.conns.get_mut(&conn) {
                c.session.close_by_relay(&sub);
            }
        }
    }

    /// A `COUNT`: one frame over the stored union, then the relay
    /// closes the sub — a COUNT is a one-shot subscription and
    /// persists nothing to `subs`. The row budget is the smaller of
    /// the COUNT entry budget and the platform's scan-row cap;
    /// hitting it marks the answer `approximate`.
    fn count(&mut self, conn: u64, sub: &SubKey, filters: &[Filter], gate: &ReadGate, now: u64) {
        let budget = usize::try_from(
            self.limits
                .req
                .max_filter_limit_count
                .min(self.config.worker.max_scan_rows as u64),
        )
        .unwrap_or(usize::MAX);
        match self
            .store
            .count(filters, gate, now, budget, Hll::for_filters(filters))
        {
            Ok(result) => self.send(
                conn,
                &count_frame(
                    &sub.id,
                    result.count,
                    result.approximate,
                    result.hll.as_ref(),
                ),
            ),
            Err(_) => self.send(conn, &closed_frame(&sub.id, "error: internal error")),
        }
        if let Some(c) = self.conns.get_mut(&conn) {
            c.session.close_by_relay(sub);
        }
    }

    /// A `CLOSE` (or a relay-side unsubscribe): drop the monitor and
    /// the persisted row.
    fn unsubscribe(&mut self, conn: u64, sub_id: &SubId) {
        self.monitors.remove(conn, sub_id);
        self.exec(
            "DELETE FROM subs WHERE conn = ?1 AND sub_id = ?2",
            &[int(conn), Param::Text(sub_id.as_str())],
        );
    }

    /// Decode a live connection's `subs` row and re-register it in
    /// the session: the subscription comes back under a fresh
    /// generation and a REQ-time read gate (`authed` counts the
    /// pubkeys that had authenticated when the REQ arrived; pubkeys
    /// only append, so the prefix is exact). `None` on a corrupt row.
    fn decode_sub(&mut self, row: &[Value]) -> Option<(SubKey, Arc<[Filter]>, ReadGate)> {
        let uint = |i: usize| match row.get(i) {
            Some(Value::Int(v)) if *v >= 0 => u64::try_from(*v).ok(),
            _ => None,
        };
        let text = |i: usize| match row.get(i) {
            Some(Value::Text(s)) => Some(s.as_str()),
            _ => None,
        };
        let conn = uint(0)?;
        let sub_id = SubId::parse(text(1)?).ok()?;
        let mut parsed = Vec::new();
        for raw in serde_json::from_str::<Vec<&serde_json::value::RawValue>>(text(2)?).ok()? {
            parsed.push(Filter::from_raw(raw).ok()?);
        }
        let authed = uint(3)?;
        let c = self.conns.get_mut(&conn)?;
        let key = c.session.resume_sub(sub_id);
        let keys = c.session.authenticated();
        let n = usize::try_from(authed)
            .unwrap_or(usize::MAX)
            .min(keys.len());
        let gate = ReadGate::new(
            Arc::clone(&self.limits.auth),
            keys.get(..n).unwrap_or(&[]).into(),
        );
        Some((key, parsed.into(), gate))
    }

    /// Reload every `subs` row: live connections get their
    /// subscription back, stale and corrupt rows are deleted. Only
    /// executor failures abort the open.
    fn restore_subs(&mut self) -> Result<(), RelayError> {
        let mut rows = Vec::new();
        self.store
            .exec()
            .query(
                "SELECT conn, sub_id, filters, authed FROM subs",
                &[],
                &mut |row| {
                    rows.push(row);
                    core::ops::ControlFlow::Continue(())
                },
            )
            .map_err(|err| StoreError::Sql(Box::new(err)))?;
        for row in rows {
            // A row that cannot be decoded is corrupt state, not a
            // reason to brick the object: delete it and move on. Only
            // the executor failure above aborts the open.
            let Some((conn, sub_id)) = sub_pk(&row) else {
                continue;
            };
            let keep = self
                .conns
                .contains_key(&conn)
                .then(|| self.decode_sub(&row))
                .flatten();
            match keep {
                Some((key, filters, gate)) => self.monitors.insert(conn, key, filters, gate),
                None => self.exec(
                    "DELETE FROM subs WHERE conn = ?1 AND sub_id = ?2",
                    &[int(conn), Param::Text(&sub_id)],
                ),
            }
        }
        Ok(())
    }
}

/// Decode a `subs` row's primary key — the `(conn, sub_id)` pair
/// needed to delete the row. `None` when the columns are not their
/// declared types; such a row cannot even be addressed, so it is
/// left for the next wake's sweep.
fn sub_pk(row: &[Value]) -> Option<(u64, String)> {
    let conn = match row.first() {
        Some(Value::Int(v)) if *v >= 0 => u64::try_from(*v).ok()?,
        _ => return None,
    };
    let sub_id = match row.get(1) {
        Some(Value::Text(s)) => s.clone(),
        _ => return None,
    };
    Some((conn, sub_id))
}
