//! Strict NIP-01 event ingest: borrowed parse, hash check, BIP-340 verify,
//! canonical JSON, packed record.

use core::cmp::Ordering;
use core::fmt;
use std::borrow::Cow;

use secp256k1::XOnlyPublicKey;
use secp256k1::schnorr::{self, Signature};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::hex;
use crate::kind::{Kind, KindClass};
use crate::packed::{PackedEvent, PackedRef};

/// A 32-byte event id. `Display` is lowercase hex.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(pub [u8; 32]);

impl EventId {
    /// Parse a 64-char lowercase hex id.
    #[must_use]
    pub fn from_hex(s: &str) -> Option<Self> {
        hex::decode_32(s).map(Self)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(&self.0))
    }
}

/// A 32-byte author public key. `Display` is lowercase hex.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicKey(pub [u8; 32]);

impl PublicKey {
    /// Parse a 64-char lowercase hex public key.
    #[must_use]
    pub fn from_hex(s: &str) -> Option<Self> {
        hex::decode_32(s).map(Self)
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(&self.0))
    }
}

/// Event acceptance limits: structure plus `created_at` windows, all
/// strfry defaults; unknown keys are rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EventLimits {
    /// Maximum size of the canonical event JSON, in bytes.
    pub max_event_size: usize,
    /// Maximum number of tags.
    pub max_num_tags: usize,
    /// Maximum size of a single tag element, in bytes.
    pub max_tag_val_size: usize,
    /// Reject events whose `created_at` is more than this far in the future.
    pub reject_events_newer_than_seconds: u64,
    /// Reject events whose `created_at` is older than this many seconds.
    pub reject_events_older_than_seconds: u64,
    /// Reject ephemeral events whose `created_at` is older than this many seconds.
    pub reject_ephemeral_events_older_than_seconds: u64,
}

impl Default for EventLimits {
    fn default() -> Self {
        Self {
            max_event_size: 65_536,
            max_num_tags: 2_000,
            max_tag_val_size: 1_024,
            reject_events_newer_than_seconds: 900,
            reject_events_older_than_seconds: 94_608_000,
            reject_ephemeral_events_older_than_seconds: 60,
        }
    }
}

/// Errors from event parse or verification.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum EventError {
    /// The input is not a well-formed event object.
    #[error("malformed JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// A field has a wrong shape (wrong type or bad hex).
    #[error("field `{0}` must be {1}")]
    Field(&'static str, &'static str),
    /// More tags than [`EventLimits::max_num_tags`].
    #[error("too many tags ({0} > {1})")]
    TooManyTags(usize, usize),
    /// A tag element larger than [`EventLimits::max_tag_val_size`].
    #[error("tag value too large ({0} > {1} bytes)")]
    TagValueTooLarge(usize, usize),
    /// The canonical JSON is larger than [`EventLimits::max_event_size`].
    #[error("event too large ({0} > {1} bytes)")]
    EventTooLarge(usize, usize),
    /// The claimed `id` does not match the serialized event hash.
    #[error("event id does not match computed hash")]
    IdMismatch,
    /// BIP-340 verification failed.
    #[error("signature verification failed")]
    Sig,
}

impl EventError {
    /// The NIP-01 machine-readable rejection message (`invalid: ...`).
    #[must_use]
    pub fn reason(&self) -> String {
        format!("invalid: {self}")
    }
}

/// Borrowed, structurally validated but not yet signature-verified event.
#[derive(Debug)]
pub struct UnverifiedEvent<'a> {
    id: EventId,
    pubkey: PublicKey,
    created_at: u64,
    kind: Kind,
    tags: Vec<Vec<Cow<'a, str>>>,
    content: Cow<'a, str>,
    sig: [u8; 64],
    pubkey_hex: Cow<'a, str>,
    sig_hex: Cow<'a, str>,
}

/// Serde view: all seven fields required, duplicates rejected, unknown keys
/// ignored (they never reach storage).
#[derive(Deserialize)]
struct RawEvent<'a> {
    #[serde(borrow)]
    id: Cow<'a, str>,
    #[serde(borrow)]
    pubkey: Cow<'a, str>,
    created_at: u64,
    kind: u16,
    #[serde(borrow)]
    tags: Vec<Vec<Cow<'a, str>>>,
    #[serde(borrow)]
    content: Cow<'a, str>,
    #[serde(borrow)]
    sig: Cow<'a, str>,
}

impl<'a> UnverifiedEvent<'a> {
    /// Structural parse + limit checks. No hashing happens here.
    ///
    /// # Errors
    ///
    /// [`EventError`] on any malformed field or exceeded limit.
    pub fn parse(json: &'a str, limits: &EventLimits) -> Result<Self, EventError> {
        let ev = Self::parse_structural(json)?;
        ev.check_limits(limits)?;
        Ok(ev)
    }

    /// Structural-only parse used by the client-message layer, where the
    /// limit policy is not yet in scope.
    pub(crate) fn parse_structural(json: &'a str) -> Result<Self, EventError> {
        let raw: RawEvent<'a> = serde_json::from_str(json)?;
        let id =
            hex::decode_32(&raw.id).ok_or(EventError::Field("id", "64 lowercase hex chars"))?;
        let pubkey = hex::decode_32(&raw.pubkey)
            .ok_or(EventError::Field("pubkey", "64 lowercase hex chars"))?;
        let sig =
            hex::decode_64(&raw.sig).ok_or(EventError::Field("sig", "128 lowercase hex chars"))?;
        if raw.created_at > 9_007_199_254_740_991 {
            return Err(EventError::Field("created_at", "integer <= 2^53 - 1"));
        }
        for tag in &raw.tags {
            let Some(name) = tag.first() else {
                return Err(EventError::Field(
                    "tags",
                    "arrays of non-empty string arrays",
                ));
            };
            if (name.as_ref() == "e" || name.as_ref() == "p")
                && let Some(value) = tag.get(1)
                && !hex::is_lower_hex(value, 64)
            {
                return Err(EventError::Field(
                    "tags",
                    "e/p first values of 64 lowercase hex chars",
                ));
            }
        }
        Ok(Self {
            id: EventId(id),
            pubkey: PublicKey(pubkey),
            created_at: raw.created_at,
            kind: Kind(raw.kind),
            tags: raw.tags,
            content: raw.content,
            sig,
            pubkey_hex: raw.pubkey,
            sig_hex: raw.sig,
        })
    }

    /// [`EventLimits::max_num_tags`] and
    /// [`EventLimits::max_tag_val_size`] checks.
    ///
    /// # Errors
    ///
    /// [`EventError::TooManyTags`] or [`EventError::TagValueTooLarge`].
    pub fn check_limits(&self, limits: &EventLimits) -> Result<(), EventError> {
        if self.tags.len() > limits.max_num_tags {
            return Err(EventError::TooManyTags(
                self.tags.len(),
                limits.max_num_tags,
            ));
        }
        for value in self.tags.iter().flatten() {
            if value.len() > limits.max_tag_val_size {
                return Err(EventError::TagValueTooLarge(
                    value.len(),
                    limits.max_tag_val_size,
                ));
            }
        }
        Ok(())
    }

    /// The claimed event id.
    #[must_use]
    pub const fn id(&self) -> EventId {
        self.id
    }

    /// The author public key.
    #[must_use]
    pub const fn pubkey(&self) -> PublicKey {
        self.pubkey
    }

    /// `created_at` seconds.
    #[must_use]
    pub const fn created_at(&self) -> u64 {
        self.created_at
    }

    /// Event kind.
    #[must_use]
    pub const fn kind(&self) -> Kind {
        self.kind
    }

    /// The event tags.
    pub fn tags(&self) -> impl Iterator<Item = &[Cow<'a, str>]> {
        self.tags.iter().map(Vec::as_slice)
    }

    /// Event content.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    /// The claimed 64-byte signature.
    #[must_use]
    pub const fn sig(&self) -> [u8; 64] {
        self.sig
    }

    /// Verify the id hash and the BIP-340 signature, then build the
    /// canonical JSON and the packed record. `limits` bounds the canonical
    /// JSON size.
    ///
    /// # Errors
    ///
    /// [`EventError::IdMismatch`], [`EventError::Sig`], or
    /// [`EventError::EventTooLarge`].
    pub fn verify(self, limits: &EventLimits) -> Result<Event, EventError> {
        let preimage = id_preimage(
            self.pubkey_hex.as_ref(),
            self.created_at,
            self.kind.0,
            &self.tags,
            self.content.as_ref(),
        );
        let computed: [u8; 32] = Sha256::digest(preimage.as_bytes()).into();
        if computed != self.id.0 {
            return Err(EventError::IdMismatch);
        }
        let sig = Signature::from_byte_array(self.sig);
        let pubkey = XOnlyPublicKey::from_byte_array(self.pubkey.0).map_err(|_| EventError::Sig)?;
        schnorr::verify(&sig, &computed, &pubkey).map_err(|_| EventError::Sig)?;

        let id_hex = self.id.to_string();
        let json = serde_json::to_string(&CanonicalEvent {
            id: &id_hex,
            pubkey: self.pubkey_hex.as_ref(),
            created_at: self.created_at,
            kind: self.kind.0,
            tags: &self.tags,
            content: self.content.as_ref(),
            sig: self.sig_hex.as_ref(),
        })?;
        if json.len() > limits.max_event_size {
            return Err(EventError::EventTooLarge(json.len(), limits.max_event_size));
        }

        let is_protected = self
            .tags
            .iter()
            .any(|tag| tag.first().is_some_and(|name| name.as_ref() == "-"));
        let address = match self.kind.class() {
            KindClass::Regular | KindClass::Ephemeral => None,
            KindClass::Replaceable => Some(address_hash(self.kind, &self.pubkey, &[])),
            KindClass::Addressable => {
                let d = self
                    .tags
                    .iter()
                    .find(|tag| tag.first().is_some_and(|name| name.as_ref() == "d"))
                    .and_then(|tag| tag.get(1))
                    .map_or(&[][..], |value| value.as_bytes());
                Some(address_hash(self.kind, &self.pubkey, d))
            }
        };
        let packed = PackedEvent::build(
            &self.id,
            &self.pubkey,
            self.created_at,
            self.kind,
            &self.tags,
        );
        Ok(Event {
            packed,
            json: json.into_boxed_str(),
            is_protected,
            address,
        })
    }
}

/// Canonical JSON field order; serde emits fields in declaration order.
#[derive(Serialize)]
struct CanonicalEvent<'a> {
    id: &'a str,
    pubkey: &'a str,
    created_at: u64,
    kind: u16,
    tags: &'a [Vec<Cow<'a, str>>],
    content: &'a str,
    sig: &'a str,
}

/// A fully verified, immutable event. Cheap to wrap in `Arc`.
#[derive(Debug)]
pub struct Event {
    packed: PackedEvent,
    json: Box<str>,
    is_protected: bool,
    address: Option<[u8; 32]>,
}

impl Event {
    /// Event id.
    #[must_use]
    pub fn id(&self) -> EventId {
        self.packed.packed().id()
    }

    /// Author public key.
    #[must_use]
    pub fn pubkey(&self) -> PublicKey {
        self.packed.packed().pubkey()
    }

    /// `created_at` seconds.
    #[must_use]
    pub fn created_at(&self) -> u64 {
        self.packed.packed().created_at()
    }

    /// Event kind.
    #[must_use]
    pub fn kind(&self) -> Kind {
        self.packed.packed().kind()
    }

    /// Canonical JSON, in fixed field order with `serde_json` string escaping.
    #[must_use]
    pub fn json(&self) -> &str {
        &self.json
    }

    /// Borrowed view over the packed index record.
    #[must_use]
    pub fn packed(&self) -> PackedRef<'_> {
        self.packed.packed()
    }

    /// NIP-70: the event carries a `["-"]` tag.
    #[must_use]
    pub const fn is_protected(&self) -> bool {
        self.is_protected
    }

    /// The fixed-size replaceable/addressable key:
    /// [`address_hash`] over an empty `d` for replaceable kinds, over the
    /// full first `d` value for addressable kinds, `None` otherwise.
    #[must_use]
    pub const fn address(&self) -> Option<[u8; 32]> {
        self.address
    }
}

/// The NIP-01 id preimage `[0, pubkey, created_at, kind, tags, content]`,
/// serialized exactly like `JSON.stringify` (`vectors/event-serialize.json`
/// is the oracle). `pubkey` is the lowercase hex string form.
fn id_preimage<T: Serialize + ?Sized>(
    pubkey: &str,
    created_at: u64,
    kind: u16,
    tags: &T,
    content: &str,
) -> String {
    serde_json::to_string(&(0u8, pubkey, created_at, kind, tags, content)).unwrap_or_default()
}

/// The NIP-01 id preimage, built for an unsigned event.
#[must_use]
pub fn serialize_for_id(
    pubkey: &PublicKey,
    created_at: u64,
    kind: Kind,
    tags: &[Vec<String>],
    content: &str,
) -> String {
    id_preimage(&pubkey.to_string(), created_at, kind.0, tags, content)
}

/// The NIP-01 event id: `SHA-256` of the [`serialize_for_id`] preimage.
#[must_use]
pub fn event_id(preimage: &[u8]) -> EventId {
    EventId(Sha256::digest(preimage).into())
}

/// The storage key for replaceable and addressable events:
/// `SHA-256(kind as u16 big-endian || pubkey || d)`.
#[must_use]
pub fn address_hash(kind: Kind, pubkey: &PublicKey, d: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(kind.0.to_be_bytes());
    hasher.update(pubkey.0);
    hasher.update(d);
    hasher.finalize().into()
}

/// Newest-first ordering: `created_at` descending, then id ascending.
#[must_use]
pub fn cmp_newest_first(a: (u64, &EventId), b: (u64, &EventId)) -> Ordering {
    b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1))
}

/// Whether `new` supersedes `old` as the stored replaceable/addressable
/// event: newer `created_at` wins, and on a tie the lower id wins.
#[must_use]
pub fn supersedes(new: (u64, &EventId), old: (u64, &EventId)) -> bool {
    match new.0.cmp(&old.0) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => new.1 < old.1,
    }
}

#[cfg(test)]
mod tests {
    use secp256k1::Keypair;
    use secp256k1::schnorr;

    use super::*;

    fn sign(kind: u16, created_at: u64, tags: &[Vec<String>], content: &str) -> Event {
        let keypair = Keypair::from_secret_bytes([7u8; 32]).expect("secret key");
        let pubkey = PublicKey(keypair.x_only_public_key().0.to_byte_array());
        let preimage = serialize_for_id(&pubkey, created_at, Kind(kind), tags, content);
        let id = event_id(preimage.as_bytes());
        let sig = schnorr::sign_no_aux_rand(&id.0, &keypair);
        let json = format!(
            "{{\"id\":\"{id}\",\"pubkey\":\"{pubkey}\",\"created_at\":{created_at},\
             \"kind\":{kind},\"tags\":{},\"content\":{},\"sig\":\"{}\"}}",
            serde_json::to_string(tags).expect("tags"),
            serde_json::to_string(content).expect("content"),
            hex::encode(&sig.to_byte_array()),
        );
        UnverifiedEvent::parse(&json, &EventLimits::default())
            .expect("parse")
            .verify(&EventLimits::default())
            .expect("verify")
    }

    fn pubkey() -> PublicKey {
        let keypair = Keypair::from_secret_bytes([7u8; 32]).expect("secret key");
        PublicKey(keypair.x_only_public_key().0.to_byte_array())
    }

    #[test]
    fn replaceable_address_uses_empty_d() {
        let event = sign(0, 1000, &[], "profile");
        assert_eq!(
            event.address(),
            Some(address_hash(Kind(0), &event.pubkey(), &[]))
        );
        let ignored = sign(10_000, 1000, &[vec!["d".into(), "ignored".into()]], "x");
        assert_eq!(
            ignored.address(),
            Some(address_hash(Kind(10_000), &ignored.pubkey(), &[])),
            "replaceable kinds hash an empty d even when a d tag exists"
        );
    }

    #[test]
    fn addressable_address_uses_first_d_value() {
        let event = sign(
            30_023,
            1000,
            &[
                vec!["d".into(), "post".into()],
                vec!["d".into(), "other".into()],
            ],
            "x",
        );
        assert_eq!(
            event.address(),
            Some(address_hash(Kind(30_023), &event.pubkey(), b"post"))
        );
        let no_d = sign(30_023, 1000, &[], "x");
        assert_eq!(
            no_d.address(),
            Some(address_hash(Kind(30_023), &no_d.pubkey(), &[]))
        );
    }

    #[test]
    fn addressable_address_keeps_full_d_beyond_packed_limit() {
        let d = "d".repeat(300);
        let event = sign(30_023, 1000, &[vec!["d".into(), d.clone()]], "x");
        // The 300-byte value is absent from the packed record...
        assert_eq!(event.packed().d_tag(), b"".as_slice());
        // ...but the address still covers all of it.
        assert_eq!(
            event.address(),
            Some(address_hash(Kind(30_023), &event.pubkey(), d.as_bytes()))
        );
    }

    #[test]
    fn regular_and_ephemeral_have_no_address() {
        assert_eq!(sign(1, 1000, &[], "gm").address(), None);
        assert_eq!(sign(20_000, 1000, &[], "gm").address(), None);
        assert_eq!(sign(65_535, 1000, &[], "gm").address(), None);
    }

    #[test]
    fn address_hash_layout() {
        let pk = pubkey();
        let hash = address_hash(Kind(30_023), &pk, b"post");
        let mut preimage = Vec::new();
        preimage.extend_from_slice(&30_023u16.to_be_bytes());
        preimage.extend_from_slice(&pk.0);
        preimage.extend_from_slice(b"post");
        assert_eq!(hash, <[u8; 32]>::from(Sha256::digest(&preimage)));
    }
}
