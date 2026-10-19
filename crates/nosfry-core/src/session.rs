//! Per-connection session state machine (sans-IO): text in, `Command`s out.

use std::collections::HashMap;
use std::sync::Arc;

use crate::event::{Event, UnverifiedEvent};
use crate::filter::Filter;
use crate::limits::Limits;
use crate::message::{
    ClientMessage, FilterVerb, MessageError, Reason, SubId, closed_frame, neg_err_frame,
    notice_frame, ok_frame,
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
    Publish(Arc<Event>),
    /// Open a subscription: the adapter runs the stored-event scan and
    /// registers the live monitor.
    Subscribe {
        /// New subscription key (fresh generation).
        sub: SubKey,
        /// Compiled filters.
        filters: Arc<[Filter]>,
    },
    /// Close a subscription owned by this connection.
    Unsubscribe {
        /// Subscription id.
        sub_id: SubId,
    },
    /// A frame to send immediately.
    Send(String),
}

/// Per-connection protocol state.
#[derive(Debug)]
pub struct Session {
    limits: Arc<Limits>,
    subs: HashMap<SubId, u64, foldhash::fast::RandomState>,
    next_generation: u64,
}

impl Session {
    /// New session under the given limits.
    #[must_use]
    pub fn new(limits: Arc<Limits>) -> Self {
        Self {
            limits,
            subs: HashMap::with_hasher(foldhash::fast::RandomState::default()),
            next_generation: 1,
        }
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
    pub fn on_text(&mut self, now: u64, text: &str, out: &mut Vec<Command>) {
        let message = match ClientMessage::parse(text) {
            Ok(message) => message,
            Err(MessageError::Event { id, source }) => {
                let reason = source.reason();
                let frame =
                    id.map_or_else(|| notice_frame(&reason), |id| ok_frame(&id, false, &reason));
                out.push(Command::Send(frame));
                return;
            }
            Err(MessageError::Filter { verb, sub, source }) => {
                let reason = source.reason();
                let frame = match verb {
                    FilterVerb::Req => closed_frame(&sub, &reason),
                    FilterVerb::Neg => neg_err_frame(&sub, &reason),
                };
                out.push(Command::Send(frame));
                return;
            }
            Err(error) => {
                out.push(Command::Send(notice_frame(&error.reason())));
                return;
            }
        };
        match message {
            ClientMessage::Event(unverified) => self.on_event(now, unverified, out),
            ClientMessage::Req { sub_id, filters } => self.on_req(sub_id, filters, out),
            ClientMessage::Close { sub_id } => {
                if self.subs.remove(&sub_id).is_some() {
                    out.push(Command::Unsubscribe { sub_id });
                }
            }
            ClientMessage::Count { sub_id, .. } => {
                out.push(Command::Send(closed_frame(
                    &sub_id,
                    &Reason::Unsupported.message("COUNT"),
                )));
            }
            ClientMessage::Auth(..) => {
                out.push(Command::Send(notice_frame(
                    &Reason::Unsupported.message("AUTH"),
                )));
            }
            ClientMessage::NegOpen { sub_id, .. }
            | ClientMessage::NegMsg { sub_id, .. }
            | ClientMessage::NegClose { sub_id } => {
                out.push(Command::Send(neg_err_frame(
                    &sub_id,
                    &Reason::Unsupported.message("negentropy sync"),
                )));
            }
        }
    }

    fn on_event(&self, now: u64, unverified: UnverifiedEvent<'_>, out: &mut Vec<Command>) {
        let limits = &*self.limits;
        if let Err(error) = unverified.check_limits(&limits.event) {
            out.push(Command::Send(ok_frame(
                &unverified.id(),
                false,
                &error.reason(),
            )));
            return;
        }
        let created_at = unverified.created_at();
        let reject = if created_at
            > now.saturating_add(limits.event.reject_events_newer_than_seconds)
        {
            Some("created_at is too far in the future")
        } else if created_at < now.saturating_sub(limits.event.reject_events_older_than_seconds) {
            Some("created_at is too far in the past")
        } else if unverified.kind().is_ephemeral()
            && created_at
                < now.saturating_sub(limits.event.reject_ephemeral_events_older_than_seconds)
        {
            Some("ephemeral event created_at is too far in the past")
        } else if unverified
            .expiration()
            .is_some_and(|expires| expires <= now)
        {
            Some("event has expired")
        } else {
            None
        };
        if let Some(reason) = reject {
            out.push(Command::Send(ok_frame(
                &unverified.id(),
                false,
                &Reason::Invalid.message(reason),
            )));
            return;
        }
        let id = unverified.id();
        match unverified.verify(&limits.event) {
            Ok(event) => out.push(Command::Publish(Arc::new(event))),
            Err(error) => out.push(Command::Send(ok_frame(&id, false, &error.reason()))),
        }
    }

    fn on_req(&mut self, sub_id: SubId, filters: Vec<Filter>, out: &mut Vec<Command>) {
        if filters.len() > self.limits.req.max_req_filter_size {
            out.push(Command::Send(closed_frame(
                &sub_id,
                &Reason::Invalid.message("too many filters in request"),
            )));
            return;
        }
        if filters
            .iter()
            .any(|filter| filter.tag_filter_count() > self.limits.req.max_tags_per_filter)
        {
            out.push(Command::Send(closed_frame(
                &sub_id,
                &Reason::Invalid.message("filter has too many tag conditions"),
            )));
            return;
        }
        if !self.subs.contains_key(&sub_id)
            && self.subs.len() >= self.limits.req.max_subs_per_connection
        {
            out.push(Command::Send(closed_frame(
                &sub_id,
                &Reason::Error.message("too many open subscriptions"),
            )));
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
        });
    }
}
