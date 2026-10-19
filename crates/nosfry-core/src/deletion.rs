//! NIP-09 deletion targets derived from a kind-5 event's tags.

use std::borrow::Cow;

use crate::event::{EventId, PublicKey, address_hash};
use crate::hex;
use crate::kind::{Kind, KindClass};

/// One event or address a kind-5 event requests deleted (NIP-09).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DeletionTarget {
    /// An `e` tag's event id.
    Event(EventId),
    /// An `a` tag's address hash ([`address_hash`] of kind ‖ author ‖ `d`).
    Address([u8; 32]),
}

/// Every target of a kind-5 event by `pubkey`: each valid `e` id and
/// each `a` value `"<kind>:<author>:<d>"` whose kind is replaceable or
/// addressable. Malformed and foreign-author values are skipped —
/// NIP-09 only affects the author's own events. The result is sorted
/// and deduplicated; it is empty for non-kind-5 events.
#[must_use]
pub(crate) fn targets(
    kind: Kind,
    pubkey: &PublicKey,
    tags: &[Vec<Cow<'_, str>>],
) -> Vec<DeletionTarget> {
    if kind.0 != 5 {
        return Vec::new();
    }
    let mut out = Vec::new();
    for tag in tags {
        let Some(name) = tag.first() else {
            continue;
        };
        let Some(value) = tag.get(1) else {
            continue;
        };
        let target = match name.as_ref() {
            "e" => EventId::from_hex(value).map(DeletionTarget::Event),
            "a" => address_target(value, pubkey),
            _ => None,
        };
        if let Some(target) = target {
            out.push(target);
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// `"<kind>:<pubkey>:<d>"` → address target; `d` is everything after
/// the second colon (empty for replaceable kinds, may contain `:`).
fn address_target(value: &str, author: &PublicKey) -> Option<DeletionTarget> {
    let (kind, rest) = value.split_once(':')?;
    let (target_pubkey, d) = rest.split_once(':')?;
    let kind = kind.parse::<u16>().ok().map(Kind)?;
    if !matches!(
        kind.class(),
        KindClass::Replaceable | KindClass::Addressable
    ) || hex::decode_32(target_pubkey) != Some(author.0)
    {
        return None;
    }
    Some(DeletionTarget::Address(address_hash(
        kind,
        author,
        d.as_bytes(),
    )))
}
