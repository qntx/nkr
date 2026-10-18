//! NIP-01 wire messages: client message parsing, relay frame builders.

use core::fmt;
use std::borrow::Cow;

use serde_json::value::RawValue;

use crate::event::{EventError, EventId, UnverifiedEvent};
use crate::filter::{Filter, FilterError};
use crate::hex;

/// Maximum subscription id length in chars (NIP-01).
const SUB_ID_MAX_CHARS: usize = 64;

/// A validated subscription id (1..=64 Unicode scalar values).
///
/// Stores the original string plus its pre-escaped JSON form so frames are
/// built without re-escaping. `Eq`/`Hash` compare the original only.
#[derive(Debug, Clone)]
pub struct SubId {
    raw: Box<str>,
    json: Box<str>,
}

impl SubId {
    /// Validate and pre-escape a subscription id.
    ///
    /// # Errors
    ///
    /// [`MessageError::Invalid`] when the id is empty or longer than 64
    /// Unicode scalar values.
    pub fn parse(raw: &str) -> Result<Self, MessageError> {
        let chars = raw.chars().count();
        if chars == 0 || chars > SUB_ID_MAX_CHARS {
            return Err(MessageError::Invalid(format!(
                "subscription id must be 1..={SUB_ID_MAX_CHARS} chars"
            )));
        }
        Ok(Self {
            raw: raw.into(),
            json: json_escape(raw).into(),
        })
    }

    /// The original string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// The JSON string literal form (with quotes), inserted verbatim in
    /// frames.
    #[must_use]
    pub fn json(&self) -> &str {
        &self.json
    }
}

impl PartialEq for SubId {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl Eq for SubId {}

impl core::hash::Hash for SubId {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.raw.hash(state);
    }
}

/// Verb a [`MessageError::Filter`] was raised while parsing; selects the
/// right error frame (`CLOSED` vs `NEG-ERR`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterVerb {
    /// `["REQ", ...]` or `["COUNT", ...]`.
    Req,
    /// `["NEG-OPEN", ...]`.
    Neg,
}

/// Errors from client message parsing.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    /// The input is not a well-formed message.
    #[error("invalid message: {0}")]
    Invalid(String),
    /// The event payload is not a structurally valid event; carries the
    /// best-effort claimed id so the session can answer `OK` when known.
    #[error("invalid event: {source}")]
    Event {
        /// The claimed event id, when the object carried a valid one.
        id: Option<EventId>,
        /// The structural error.
        source: EventError,
    },
    /// A filter inside a `REQ`/`COUNT`/`NEG-OPEN` failed; carries the
    /// subscription id so the session can answer `CLOSED`/`NEG-ERR`.
    #[error("invalid filter: {source}")]
    Filter {
        /// Which verb carried the bad filter.
        verb: FilterVerb,
        /// The (already validated) subscription id.
        sub: SubId,
        /// The filter error.
        source: FilterError,
    },
}

impl MessageError {
    /// The NIP-01 machine-readable rejection message.
    #[must_use]
    pub fn reason(&self) -> String {
        match self {
            Self::Invalid(message) => format!("invalid: {message}"),
            Self::Event { source, .. } => source.reason(),
            Self::Filter { source, .. } => source.reason(),
        }
    }
}

/// A parsed client→relay message. Event payloads stay borrowed as raw JSON;
/// `REQ`/`COUNT` filters are already compiled.
#[derive(Debug)]
pub enum ClientMessage<'a> {
    /// `["EVENT", <event>]` — structurally parsed, not yet verified.
    Event(UnverifiedEvent<'a>),
    /// `["REQ", <sub>, <filter>+]`.
    Req {
        /// Subscription id.
        sub_id: SubId,
        /// Compiled filters (NIP-01 OR semantics).
        filters: Vec<Filter>,
    },
    /// `["CLOSE", <sub>]`.
    Close {
        /// Subscription id.
        sub_id: SubId,
    },
    /// `["COUNT", <sub>, <filter>+]` (NIP-45).
    Count {
        /// Subscription id.
        sub_id: SubId,
        /// Compiled filters.
        filters: Vec<Filter>,
    },
    /// `["AUTH", <event>]` (NIP-42) — structurally parsed, not verified.
    Auth(UnverifiedEvent<'a>),
    /// `["NEG-OPEN", <sub>, <filter>, <hex>]` (NIP-77).
    NegOpen {
        /// Subscription id.
        sub_id: SubId,
        /// Compiled filter.
        filter: Box<Filter>,
        /// Hex-decoded payload.
        msg: Box<[u8]>,
    },
    /// `["NEG-MSG", <sub>, <hex>]` (NIP-77).
    NegMsg {
        /// Subscription id.
        sub_id: SubId,
        /// Hex-decoded payload.
        msg: Box<[u8]>,
    },
    /// `["NEG-CLOSE", <sub>]` (NIP-77).
    NegClose {
        /// Subscription id.
        sub_id: SubId,
    },
}

impl<'a> ClientMessage<'a> {
    /// Parse a client message frame.
    ///
    /// # Errors
    ///
    /// [`MessageError`] on malformed JSON, arity/type errors, invalid
    /// subscription ids, bad NEG hex, structurally invalid event payloads,
    /// or filter errors (with the sub id attached).
    pub fn parse(text: &'a str) -> Result<Self, MessageError> {
        let items: Vec<&RawValue> = serde_json::from_str(text)
            .map_err(|e| MessageError::Invalid(format!("not a JSON array: {e}")))?;
        let verb = items
            .first()
            .and_then(|raw| serde_json::from_str::<&str>(raw.get()).ok())
            .ok_or_else(|| MessageError::Invalid("expected [verb, ...] array".to_owned()))?;
        match verb {
            "EVENT" => {
                expect_arity(&items, 2, "EVENT")?;
                let event = required(&items, 1, "EVENT")?;
                let unverified = parse_event(event)?;
                Ok(Self::Event(unverified))
            }
            "REQ" => {
                let sub_id = parse_sub(&items, 1, "REQ")?;
                expect_min_arity(&items, 3, "REQ")?;
                let filters = parse_filters(&items, 2, FilterVerb::Req, &sub_id)?;
                Ok(Self::Req { sub_id, filters })
            }
            "CLOSE" => {
                expect_arity(&items, 2, "CLOSE")?;
                let sub_id = parse_sub(&items, 1, "CLOSE")?;
                Ok(Self::Close { sub_id })
            }
            "AUTH" => {
                expect_arity(&items, 2, "AUTH")?;
                let event = required(&items, 1, "AUTH")?;
                let unverified = parse_event(event)?;
                Ok(Self::Auth(unverified))
            }
            "COUNT" => {
                let sub_id = parse_sub(&items, 1, "COUNT")?;
                expect_min_arity(&items, 3, "COUNT")?;
                let filters = parse_filters(&items, 2, FilterVerb::Req, &sub_id)?;
                Ok(Self::Count { sub_id, filters })
            }
            "NEG-OPEN" => {
                expect_arity(&items, 4, "NEG-OPEN")?;
                let sub_id = parse_sub(&items, 1, "NEG-OPEN")?;
                let filter =
                    Filter::from_raw(required(&items, 2, "NEG-OPEN")?).map_err(|source| {
                        MessageError::Filter {
                            verb: FilterVerb::Neg,
                            sub: sub_id.clone(),
                            source,
                        }
                    })?;
                let msg = parse_neg_hex(items.get(3), "NEG-OPEN")?;
                Ok(Self::NegOpen {
                    sub_id,
                    filter: Box::new(filter),
                    msg,
                })
            }
            "NEG-MSG" => {
                expect_arity(&items, 3, "NEG-MSG")?;
                let sub_id = parse_sub(&items, 1, "NEG-MSG")?;
                let msg = parse_neg_hex(items.get(2), "NEG-MSG")?;
                Ok(Self::NegMsg { sub_id, msg })
            }
            "NEG-CLOSE" => {
                expect_arity(&items, 2, "NEG-CLOSE")?;
                let sub_id = parse_sub(&items, 1, "NEG-CLOSE")?;
                Ok(Self::NegClose { sub_id })
            }
            other => Err(MessageError::Invalid(format!("unknown verb `{other}`"))),
        }
    }
}

fn parse_event(raw: &RawValue) -> Result<UnverifiedEvent<'_>, MessageError> {
    UnverifiedEvent::parse_structural(raw.get()).map_err(|source| MessageError::Event {
        id: extract_event_id(raw),
        source,
    })
}

/// Best-effort `id` extraction for the rejection frame when structural
/// parse failed.
fn extract_event_id(raw: &RawValue) -> Option<EventId> {
    #[derive(serde::Deserialize)]
    struct OnlyId<'a> {
        #[serde(borrow)]
        id: Option<Cow<'a, str>>,
    }
    let parsed: OnlyId<'_> = serde_json::from_str(raw.get()).ok()?;
    parsed
        .id
        .and_then(|id| hex::decode_32(id.as_ref()))
        .map(EventId)
}

fn required<'a>(
    items: &[&'a RawValue],
    index: usize,
    verb: &str,
) -> Result<&'a RawValue, MessageError> {
    items
        .get(index)
        .copied()
        .ok_or_else(|| MessageError::Invalid(format!("{verb} is missing element {index}")))
}

fn expect_arity(items: &[&RawValue], arity: usize, verb: &str) -> Result<(), MessageError> {
    if items.len() != arity {
        return Err(MessageError::Invalid(format!(
            "{verb} takes {} element(s)",
            arity - 1
        )));
    }
    Ok(())
}

fn expect_min_arity(items: &[&RawValue], arity: usize, verb: &str) -> Result<(), MessageError> {
    if items.len() < arity {
        return Err(MessageError::Invalid(format!(
            "{verb} needs at least {} element(s)",
            arity - 1
        )));
    }
    Ok(())
}

fn parse_sub(items: &[&RawValue], index: usize, verb: &str) -> Result<SubId, MessageError> {
    let raw = items
        .get(index)
        .and_then(|raw| serde_json::from_str::<&str>(raw.get()).ok())
        .ok_or_else(|| MessageError::Invalid(format!("{verb} subscription id must be a string")))?;
    SubId::parse(raw)
}

fn parse_filters(
    items: &[&RawValue],
    from: usize,
    verb: FilterVerb,
    sub_id: &SubId,
) -> Result<Vec<Filter>, MessageError> {
    items
        .iter()
        .skip(from)
        .map(|raw| {
            Filter::from_raw(raw).map_err(|source| MessageError::Filter {
                verb,
                sub: sub_id.clone(),
                source,
            })
        })
        .collect()
}

fn parse_neg_hex(raw: Option<&&RawValue>, verb: &str) -> Result<Box<[u8]>, MessageError> {
    let text = raw
        .and_then(|raw| serde_json::from_str::<&str>(raw.get()).ok())
        .ok_or_else(|| MessageError::Invalid(format!("{verb} payload must be a hex string")))?;
    hex::decode_list(text)
        .map(Vec::into_boxed_slice)
        .ok_or_else(|| {
            MessageError::Invalid(format!("{verb} payload must be non-empty even-length hex"))
        })
}

/// `["EVENT", "<sub>", <json>]` — `json` is inserted verbatim.
#[must_use]
pub fn event_frame(sub: &SubId, json: &str) -> String {
    let mut out = String::with_capacity(12 + sub.json().len() + json.len());
    out.push_str("[\"EVENT\",");
    out.push_str(sub.json());
    out.push(',');
    out.push_str(json);
    out.push(']');
    out
}

/// `["OK", "<id>", <accepted>, "<message>"]`.
#[must_use]
pub fn ok_frame(id: &EventId, accepted: bool, message: &str) -> String {
    let mut out = String::with_capacity(16 + 64 + message.len());
    out.push_str("[\"OK\",\"");
    out.push_str(&id.to_string());
    out.push_str(if accepted { "\",true," } else { "\",false," });
    out.push_str(&json_escape(message));
    out.push(']');
    out
}

/// `["EOSE", "<sub>"]`.
#[must_use]
pub fn eose_frame(sub: &SubId) -> String {
    frame_with_sub("EOSE", sub, "]")
}

/// `["CLOSED", "<sub>", "<message>"]`.
#[must_use]
pub fn closed_frame(sub: &SubId, message: &str) -> String {
    let mut out = String::with_capacity(16 + sub.json().len() + message.len());
    out.push_str("[\"CLOSED\",");
    out.push_str(sub.json());
    out.push(',');
    out.push_str(&json_escape(message));
    out.push(']');
    out
}

/// `["NOTICE", "<message>"]`.
#[must_use]
pub fn notice_frame(message: &str) -> String {
    let mut out = String::with_capacity(12 + message.len());
    out.push_str("[\"NOTICE\",");
    out.push_str(&json_escape(message));
    out.push(']');
    out
}

/// `["NEG-ERR", "<sub>", "<message>"]`.
#[must_use]
pub fn neg_err_frame(sub: &SubId, message: &str) -> String {
    let mut out = String::with_capacity(16 + sub.json().len() + message.len());
    out.push_str("[\"NEG-ERR\",");
    out.push_str(sub.json());
    out.push(',');
    out.push_str(&json_escape(message));
    out.push(']');
    out
}

fn frame_with_sub(verb: &str, sub: &SubId, tail: &str) -> String {
    let mut out = String::with_capacity(4 + verb.len() + sub.json().len() + tail.len());
    out.push_str("[\"");
    out.push_str(verb);
    out.push_str("\",");
    out.push_str(sub.json());
    out.push_str(tail);
    out
}

fn json_escape(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_owned())
}

/// The NIP-01 machine-readable message prefixes.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reason {
    /// `duplicate:` — event already stored.
    Duplicate,
    /// `pow:` — insufficient proof of work.
    Pow,
    /// `blocked:` — author or content blocked by policy.
    Blocked,
    /// `rate-limited:` — connection exceeded limits.
    RateLimited,
    /// `invalid:` — malformed or unverifiable input.
    Invalid,
    /// `restricted:` — not allowed on this relay.
    Restricted,
    /// `mute:` — sender is muted.
    Mute,
    /// `error:` — internal error.
    Error,
    /// `auth-required:` — NIP-42 authentication needed.
    AuthRequired,
    /// `unsupported:` — feature not implemented.
    Unsupported,
}

impl Reason {
    /// The `xxx:` prefix.
    #[must_use]
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::Duplicate => "duplicate:",
            Self::Pow => "pow:",
            Self::Blocked => "blocked:",
            Self::RateLimited => "rate-limited:",
            Self::Invalid => "invalid:",
            Self::Restricted => "restricted:",
            Self::Mute => "mute:",
            Self::Error => "error:",
            Self::AuthRequired => "auth-required:",
            Self::Unsupported => "unsupported:",
        }
    }

    /// `"<prefix> <detail>"`.
    #[must_use]
    pub fn message(self, detail: &str) -> String {
        let mut out = String::with_capacity(self.prefix().len() + 1 + detail.len());
        out.push_str(self.prefix());
        out.push(' ');
        out.push_str(detail);
        out
    }
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.prefix())
    }
}
