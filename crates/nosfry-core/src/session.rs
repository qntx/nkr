//! Per-connection session state machine (sans-IO): text in, `Command`s out.

use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::{AUTH_KIND, MAX_AUTHED_PUBKEYS, ReadGate};
use crate::event::{Event, EventId, PublicKey, UnverifiedEvent};
use crate::filter::Filter;
use crate::kind::Kind;
use crate::limits::{AddressBuckets, Limits, TokenBucket};
use crate::message::{
    ClientMessage, ClientVerb, MessageError, Reason, RelayVerb, SubId, auth_frame, closed_frame,
    neg_err_frame, notice_frame, ok_frame,
};

/// Identity of an open subscription: the id plus a generation counter that
/// changes on every re-subscribe, so the adapter can reject stale frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubKey {
    /// Subscription id.
    pub id: SubId,
    /// Increments each time the id is re-subscribed.
    pub generation: u64,
}

/// What the adapter must do next for this connection.
#[derive(Debug)]
pub enum Command {
    /// A verified event arrived. The adapter broadcasts it to live
    /// subscribers (ephemeral) or hands it to the writer, then answers OK.
    /// `authed` is the event author when authenticated, else the
    /// connection's first authenticated pubkey — the write-policy
    /// plugin's `authed` field.
    Publish {
        /// The verified event.
        event: Arc<Event>,
        /// Authenticated pubkey context for the write-policy plugin.
        authed: Option<PublicKey>,
    },
    /// Open a subscription: the adapter runs the stored-event scan and
    /// registers the live monitor.
    Subscribe {
        /// New subscription key (fresh generation).
        sub: SubKey,
        /// Compiled filters.
        filters: Arc<[Filter]>,
        /// Read visibility snapshot at REQ time.
        gate: ReadGate,
    },
    /// Close a subscription owned by this connection.
    Unsubscribe {
        /// Subscription id.
        sub_id: SubId,
    },
    /// A frame to send immediately.
    Send {
        /// The frame's verb (for relay-side metrics).
        verb: RelayVerb,
        /// The serialized frame.
        text: String,
    },
}

/// What the session made of one inbound text frame, so the transport can
/// account per-verb metrics without re-parsing.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Received {
    /// The recognized client verb — `Some` even when the message failed
    /// validation, `None` only for unparseable frames and unknown verbs.
    pub verb: Option<ClientVerb>,
    /// The message was refused by a rate-limit bucket.
    pub rate_limited: bool,
}

/// Per-connection protocol state.
#[derive(Debug)]
pub struct Session {
    limits: Arc<Limits>,
    /// The NIP-42 challenge for this connection (adapter-supplied).
    challenge: Box<str>,
    /// The challenge has been sent (at most once per connection).
    challenge_sent: bool,
    /// Pubkeys authenticated via AUTH on this connection.
    authed: Arc<[PublicKey]>,
    subs: HashMap<SubId, u64, foldhash::fast::RandomState>,
    next_generation: u64,
    /// EVENT token bucket (`None` when the rate limit is disabled).
    event_bucket: Option<TokenBucket>,
    /// REQ token bucket (`None` when the rate limit is disabled).
    req_bucket: Option<TokenBucket>,
    /// Buckets shared with every other connection of this client
    /// address (a session created without an address gets a fully
    /// disabled pair via [`AddressBuckets::default`]).
    address: Arc<AddressBuckets>,
}

impl Session {
    /// New session under the given limits with the adapter-supplied
    /// NIP-42 challenge (sent lazily, only when the connection needs
    /// it) and the client address's shared token buckets.
    #[must_use]
    pub fn new(limits: Arc<Limits>, challenge: String, address: Arc<AddressBuckets>) -> Self {
        let event_bucket = (limits.rate.events_per_second > 0)
            .then(|| TokenBucket::new(limits.rate.events_per_second, limits.rate.events_burst));
        let req_bucket = (limits.rate.reqs_per_second > 0)
            .then(|| TokenBucket::new(limits.rate.reqs_per_second, limits.rate.reqs_burst));
        Self {
            limits,
            challenge: challenge.into_boxed_str(),
            challenge_sent: false,
            authed: Arc::from([]),
            subs: HashMap::with_hasher(foldhash::fast::RandomState::default()),
            next_generation: 1,
            event_bucket,
            req_bucket,
            address,
        }
    }

    /// The pubkeys authenticated on this connection.
    #[must_use]
    pub fn authenticated(&self) -> &[PublicKey] {
        &self.authed
    }

    /// Whether `sub` still identifies the live subscription under its id —
    /// the adapter's final gate before sending EVENT/EOSE frames.
    #[must_use]
    pub fn is_current(&self, sub: &SubKey) -> bool {
        self.subs.get(&sub.id) == Some(&sub.generation)
    }

    /// The relay closes a subscription on its own initiative (delivery
    /// failure): removes it only when `sub` still identifies the live
    /// generation, so a stale close can never kill a re-subscribed id.
    /// Returns whether a subscription was removed.
    pub fn close_by_relay(&mut self, sub: &SubKey) -> bool {
        if self.subs.get(&sub.id) == Some(&sub.generation) {
            self.subs.remove(&sub.id);
            true
        } else {
            false
        }
    }

    /// Feed one text frame; append the resulting commands to `out`.
    /// `now_ms` is the current unix time in milliseconds: the session
    /// derives seconds for `created_at` checks and uses milliseconds
    /// for the rate-limit buckets.
    pub fn on_text(&mut self, now_ms: u64, text: &str, out: &mut Vec<Command>) -> Received {
        let message = match ClientMessage::parse(text) {
            Ok(message) => message,
            Err(error) => {
                let verb = error.verb();
                // A refused REQ closes an open subscription under the
                // same id: it must stop delivering before the CLOSED.
                if let MessageError::Filter {
                    verb: ClientVerb::Req,
                    sub,
                    ..
                } = &error
                {
                    self.drop_sub(sub, out);
                }
                out.push(rejection(error));
                return Received {
                    verb,
                    rate_limited: false,
                };
            }
        };
        let verb = message.verb();
        // Charge the rate-limit buckets right after a successful parse,
        // before any other work: an over-limit client is throttled no
        // matter what its message would have done.
        match message {
            ClientMessage::Event(unverified) | ClientMessage::Auth(unverified) => {
                // Charge the connection bucket first; only a pass
                // spends from the address-wide bucket.
                if !self
                    .event_bucket
                    .as_mut()
                    .is_none_or(|bucket| bucket.take(now_ms))
                    || !self.address.take_event(now_ms)
                {
                    out.push(Command::Send {
                        verb: RelayVerb::Ok,
                        text: ok_frame(
                            &unverified.id(),
                            false,
                            &Reason::RateLimited.message("slow down"),
                        ),
                    });
                    return Received {
                        verb: Some(verb),
                        rate_limited: true,
                    };
                }
                if verb == ClientVerb::Auth {
                    self.on_auth(now_ms / 1000, unverified, out);
                } else {
                    self.on_event(now_ms / 1000, unverified, out);
                }
            }
            ClientMessage::Req { sub_id, filters } => {
                if !self
                    .req_bucket
                    .as_mut()
                    .is_none_or(|bucket| bucket.take(now_ms))
                    || !self.address.take_req(now_ms)
                {
                    self.drop_sub(&sub_id, out);
                    out.push(Command::Send {
                        verb: RelayVerb::Closed,
                        text: closed_frame(&sub_id, &Reason::RateLimited.message("slow down")),
                    });
                    return Received {
                        verb: Some(verb),
                        rate_limited: true,
                    };
                }
                self.on_req(sub_id, filters, out);
            }
            ClientMessage::Close { sub_id } => {
                if self.subs.remove(&sub_id).is_some() {
                    out.push(Command::Unsubscribe { sub_id });
                }
            }
            ClientMessage::Count { sub_id, .. } => {
                out.push(Command::Send {
                    verb: RelayVerb::Closed,
                    text: closed_frame(&sub_id, &Reason::Unsupported.message("COUNT")),
                });
            }
            ClientMessage::NegOpen { sub_id, .. }
            | ClientMessage::NegMsg { sub_id, .. }
            | ClientMessage::NegClose { sub_id } => {
                out.push(Command::Send {
                    verb: RelayVerb::NegErr,
                    text: neg_err_frame(&sub_id, &Reason::Unsupported.message("negentropy sync")),
                });
            }
        }
        Received {
            verb: Some(verb),
            rate_limited: false,
        }
    }

    /// Send the NIP-42 challenge, at most once per connection (strfry's
    /// lazy challenge: never on connect).
    fn issue_challenge(&mut self, out: &mut Vec<Command>) {
        if self.challenge_sent {
            return;
        }
        self.challenge_sent = true;
        out.push(Command::Send {
            verb: RelayVerb::Auth,
            text: auth_frame(&self.challenge),
        });
    }

    /// Remove a subscription under `sub_id` and emit the adapter-side
    /// [`Command::Unsubscribe`] when it was open — every refusal that
    /// answers `CLOSED` for an existing id must stop the old
    /// subscription first.
    fn drop_sub(&mut self, sub_id: &SubId, out: &mut Vec<Command>) {
        if self.subs.remove(sub_id).is_some() {
            out.push(Command::Unsubscribe {
                sub_id: sub_id.clone(),
            });
        }
    }

    /// The shared `created_at`/expiration window checks for EVENT and
    /// AUTH payloads. `Some(reason)` rejects.
    fn check_time(
        &self,
        created_at: u64,
        kind: Kind,
        expiration: Option<u64>,
        now: u64,
    ) -> Option<&'static str> {
        let limits = &self.limits.event;
        if created_at > now.saturating_add(limits.reject_events_newer_than_seconds) {
            Some("created_at is too far in the future")
        } else if created_at < now.saturating_sub(limits.reject_events_older_than_seconds) {
            Some("created_at is too far in the past")
        } else if kind.is_ephemeral()
            && created_at < now.saturating_sub(limits.reject_ephemeral_events_older_than_seconds)
        {
            Some("ephemeral event created_at is too far in the past")
        } else if expiration.is_some_and(|expires| expires <= now) {
            Some("event has expired")
        } else {
            None
        }
    }

    fn on_event(&mut self, now: u64, unverified: UnverifiedEvent<'_>, out: &mut Vec<Command>) {
        let limits = &*self.limits;
        if let Err(error) = unverified.check_limits(&limits.event) {
            out.push(Command::Send {
                verb: RelayVerb::Ok,
                text: ok_frame(&unverified.id(), false, &error.reason()),
            });
            return;
        }
        if let Some(reason) = self.check_time(
            unverified.created_at(),
            unverified.kind(),
            unverified.expiration(),
            now,
        ) {
            out.push(Command::Send {
                verb: RelayVerb::Ok,
                text: ok_frame(&unverified.id(), false, &Reason::Invalid.message(reason)),
            });
            return;
        }
        // NIP-42 MUST: AUTH events are never broadcast or stored.
        if unverified.kind() == AUTH_KIND {
            out.push(Command::Send {
                verb: RelayVerb::Ok,
                text: ok_frame(
                    &unverified.id(),
                    false,
                    &Reason::Invalid.message("kind 22242 events are only accepted in AUTH"),
                ),
            });
            return;
        }
        if unverified.embeds_protected() {
            out.push(Command::Send {
                verb: RelayVerb::Ok,
                text: ok_frame(
                    &unverified.id(),
                    false,
                    &Reason::Blocked.message("reposts can't embed protected events"),
                ),
            });
            return;
        }
        if unverified.is_protected() {
            if !limits.auth.enabled() {
                out.push(Command::Send {
                    verb: RelayVerb::Ok,
                    text: ok_frame(
                        &unverified.id(),
                        false,
                        &Reason::Blocked.message("event marked as protected"),
                    ),
                });
                return;
            }
            if self.authed.is_empty() {
                self.issue_challenge(out);
                out.push(Command::Send {
                    verb: RelayVerb::Ok,
                    text: ok_frame(
                        &unverified.id(),
                        false,
                        &Reason::AuthRequired.message("event marked as protected"),
                    ),
                });
                return;
            }
            if !self.authed.contains(&unverified.pubkey()) {
                out.push(Command::Send {
                    verb: RelayVerb::Ok,
                    text: ok_frame(
                        &unverified.id(),
                        false,
                        &Reason::Restricted.message("must be published by the author"),
                    ),
                });
                return;
            }
        }
        let id = unverified.id();
        match unverified.verify(&limits.event) {
            Ok(event) => {
                let authed = if self.authed.contains(&event.pubkey()) {
                    Some(event.pubkey())
                } else {
                    self.authed.first().copied()
                };
                out.push(Command::Publish {
                    event: Arc::new(event),
                    authed,
                });
            }
            Err(error) => out.push(Command::Send {
                verb: RelayVerb::Ok,
                text: ok_frame(&id, false, &error.reason()),
            }),
        }
    }

    /// NIP-42 AUTH: verifies the event, then adds its pubkey to the
    /// connection's authenticated set. Every failure answers
    /// `OK <id> false <reason>`.
    fn on_auth(&mut self, now: u64, unverified: UnverifiedEvent<'_>, out: &mut Vec<Command>) {
        fn fail(out: &mut Vec<Command>, id: &EventId, reason: &str) {
            out.push(Command::Send {
                verb: RelayVerb::Ok,
                text: ok_frame(id, false, reason),
            });
        }
        let id = unverified.id();
        let auth = &self.limits.auth;
        if !auth.enabled() {
            return fail(
                out,
                &id,
                &Reason::Unsupported.message("authentication is disabled"),
            );
        }
        if let Err(error) = unverified.check_limits(&self.limits.event) {
            return fail(out, &id, &error.reason());
        }
        if unverified.kind() != AUTH_KIND {
            return fail(
                out,
                &id,
                &Reason::Invalid.message("AUTH event must be kind 22242"),
            );
        }
        if let Some(reason) = self.check_time(
            unverified.created_at(),
            unverified.kind(),
            unverified.expiration(),
            now,
        ) {
            return fail(out, &id, &Reason::Invalid.message(reason));
        }
        if !unverified
            .tag_values("challenge")
            .any(|value| value == &*self.challenge)
        {
            return fail(out, &id, &Reason::Invalid.message("challenge mismatch"));
        }
        if !unverified
            .tag_values("relay")
            .any(|value| auth.matches_relay(value))
        {
            return fail(out, &id, &Reason::Invalid.message("relay tag mismatch"));
        }
        let event = match unverified.verify(&self.limits.event) {
            Ok(event) => event,
            Err(error) => return fail(out, &id, &error.reason()),
        };
        let pubkey = event.pubkey();
        if self.authed.contains(&pubkey) {
            out.push(Command::Send {
                verb: RelayVerb::Ok,
                text: ok_frame(&id, true, ""),
            });
            return;
        }
        if self.authed.len() >= MAX_AUTHED_PUBKEYS {
            return fail(
                out,
                &id,
                &Reason::Restricted.message("too many authenticated pubkeys"),
            );
        }
        let mut authed = self.authed.to_vec();
        authed.push(pubkey);
        self.authed = authed.into();
        out.push(Command::Send {
            verb: RelayVerb::Ok,
            text: ok_frame(&id, true, ""),
        });
    }

    fn on_req(&mut self, sub_id: SubId, filters: Vec<Filter>, out: &mut Vec<Command>) {
        if filters.len() > self.limits.req.max_req_filter_size {
            self.drop_sub(&sub_id, out);
            out.push(Command::Send {
                verb: RelayVerb::Closed,
                text: closed_frame(
                    &sub_id,
                    &Reason::Invalid.message("too many filters in request"),
                ),
            });
            return;
        }
        if filters
            .iter()
            .any(|filter| filter.tag_filter_count() > self.limits.req.max_tags_per_filter)
        {
            self.drop_sub(&sub_id, out);
            out.push(Command::Send {
                verb: RelayVerb::Closed,
                text: closed_frame(
                    &sub_id,
                    &Reason::Invalid.message("filter has too many tag conditions"),
                ),
            });
            return;
        }
        if !self.subs.contains_key(&sub_id)
            && self.subs.len() >= self.limits.req.max_subs_per_connection
        {
            self.drop_sub(&sub_id, out);
            out.push(Command::Send {
                verb: RelayVerb::Closed,
                text: closed_frame(
                    &sub_id,
                    &Reason::Error.message("too many open subscriptions"),
                ),
            });
            return;
        }
        // A fully restricted filter group cannot return anything to an
        // unauthenticated client (strfry `isFilterGroupFullyRestricted`).
        if self.limits.auth.fully_restricts(&filters) && self.authed.is_empty() {
            if self.limits.auth.enabled() {
                self.issue_challenge(out);
                self.drop_sub(&sub_id, out);
                out.push(Command::Send {
                    verb: RelayVerb::Closed,
                    text: closed_frame(
                        &sub_id,
                        &Reason::AuthRequired.message("requested filter requires authentication"),
                    ),
                });
            } else {
                self.drop_sub(&sub_id, out);
                out.push(Command::Send {
                    verb: RelayVerb::Closed,
                    text: closed_frame(
                        &sub_id,
                        &Reason::Restricted.message("requested kinds are not served"),
                    ),
                });
            }
            return;
        }
        let generation = self.next_generation;
        self.next_generation += 1;
        if self.subs.insert(sub_id.clone(), generation).is_some() {
            out.push(Command::Unsubscribe {
                sub_id: sub_id.clone(),
            });
        }
        out.push(Command::Subscribe {
            sub: SubKey {
                id: sub_id,
                generation,
            },
            filters: filters.into(),
            gate: ReadGate {
                rules: Arc::clone(&self.limits.auth),
                authed: Arc::clone(&self.authed),
            },
        });
    }
}

/// Map a parse error to its rejection frame command: `OK` when a
/// claimed id exists, `NEG-ERR` for NIP-77 verbs, `CLOSED` for
/// subscription-bearing verbs, `NOTICE` otherwise.
fn rejection(error: MessageError) -> Command {
    match error {
        MessageError::Event { id, source, .. } => {
            let reason = source.reason();
            let (verb, text) = id.map_or_else(
                || (RelayVerb::Notice, notice_frame(&reason)),
                |id| (RelayVerb::Ok, ok_frame(&id, false, &reason)),
            );
            Command::Send { verb, text }
        }
        MessageError::Filter { verb, sub, source } => {
            let reason = source.reason();
            let (verb, text) = if verb.is_neg() {
                (RelayVerb::NegErr, neg_err_frame(&sub, &reason))
            } else {
                (RelayVerb::Closed, closed_frame(&sub, &reason))
            };
            Command::Send { verb, text }
        }
        error => Command::Send {
            verb: RelayVerb::Notice,
            text: notice_frame(&error.reason()),
        },
    }
}
