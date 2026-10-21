//! The relay object: connections, sessions, the live-subscription
//! index, and hibernation state over a [`SqlStore`].

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use nosfry_core::auth::ReadGate;
use nosfry_core::event::{Event, PublicKey};
use nosfry_core::filter::Filter;
use nosfry_core::limits::{AddressBuckets, Limits};
use nosfry_core::message::{SubId, closed_frame, eose_frame, event_frame, ok_frame};
use nosfry_core::monitor::Monitors;
use nosfry_core::session::{Command, Session, SubKey};
use nosfry_core::store::WriteOutcome;
use nosfry_sql::{Executor, Param, SqlStore, StoreError, Value};
use serde::{Deserialize, Serialize};

use crate::config::{Config, ConfigError};

/// Connection ids must stay below 2^53: they are bound into SQL,
/// where integers are JavaScript doubles. The glue assigns them
/// (e.g. random 53-bit) and `accept`/`open` refuse larger ones.
const JS_MAX: u64 = (1 << 53) - 1;

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

/// The relay object: the SQL store, the session per connection, and
/// the monitor index — everything but the platform's socket, timer,
/// and persistence calls.
#[derive(Debug)]
pub struct Relay<E: Executor, S: Socket> {
    store: SqlStore<E>,
    config: Config,
    limits: Arc<Limits>,
    conns: HashMap<u64, Conn<S>>,
    /// `AddressBuckets` shared by every connection of one client
    /// address; the entry dies with the last.
    buckets: HashMap<IpAddr, Arc<AddressBuckets>>,
    monitors: Monitors<u64>,
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
        let mut relay = Self {
            store,
            config,
            limits,
            conns: HashMap::new(),
            buckets: HashMap::new(),
            monitors: Monitors::new(),
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
            self.command(conn, command, now_ms / 1000, &mut effects);
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
    /// index, delete its `subs` rows, and release its share of the
    /// address buckets.
    pub fn close(&mut self, conn: u64) {
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

    /// Run the NIP-40 sweep: delete up to 10,000 expired events, then
    /// answer the next alarm time — `Some(now)` while the batch was
    /// full (run again immediately), else the next expiration or
    /// `None`.
    ///
    /// # Errors
    ///
    /// [`StoreError::Sql`] on executor failure.
    pub fn alarm(&self, now: u64) -> Result<Option<u64>, StoreError> {
        if self.store.expire(now, 10_000)? == 10_000 {
            return Ok(Some(now));
        }
        self.store.next_expiration()
    }

    /// The NIP-11 document: the implemented NIPs only — NIP-42 when
    /// `service_url` is set.
    #[must_use]
    pub fn nip11(&self) -> String {
        let mut nips = vec![1, 9, 11, 40, 70];
        if !self.config.relay.service_url.is_empty() {
            nips.push(42);
        }
        nips.sort_unstable();
        nosfry_core::info::document(
            &self.config.relay.info,
            &nips,
            &self.limits,
            self.config.relay.max_websocket_payload_size,
        )
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

    /// Execute one session command in order.
    fn command(&mut self, conn: u64, command: Command, now: u64, effects: &mut Effects) {
        match command {
            Command::Send { text, .. } => self.send(conn, &text),
            Command::Publish { event, .. } => self.publish(conn, &event, effects),
            Command::Subscribe { sub, filters, gate } => {
                self.subscribe(conn, sub, filters, gate, now);
            }
            Command::Unsubscribe { sub_id } => self.unsubscribe(conn, &sub_id),
            // COUNT and negentropy are refused by the session itself:
            // the worker's limits force max_filter_limit_count and
            // max_sync_events to 0, so these arms never fire.
            Command::Count { .. }
            | Command::NegOpen { .. }
            | Command::NegMsg { .. }
            | Command::NegClose { .. } => {}
        }
    }

    /// A verified event: answer `OK`, persist non-ephemeral kinds,
    /// and deliver a `Stored` or accepted ephemeral event to every
    /// current subscription, the publisher's own included.
    fn publish(&mut self, conn: u64, event: &Arc<Event>, effects: &mut Effects) {
        if event.kind().is_ephemeral() {
            self.send(conn, &ok_frame(&event.id(), true, ""));
            self.deliver(event);
            return;
        }
        match self.store.write(std::slice::from_ref(event)) {
            Ok(outcomes) => {
                let Some(&outcome) = outcomes.first() else {
                    return;
                };
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
