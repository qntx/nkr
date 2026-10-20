//! NIP-45 `HyperLogLog` register set for COUNT responses.
//!
//! Mirrors go-nostr's `nip45/hyperloglog`: 256 registers, a per-query
//! `offset` derived from the single tag condition of a single filter,
//! and register updates of the form `j = x[0]`,
//! `zero_bits = clz56(u64_be(x)) + 1` over `x = pubkey[offset..offset+8]`.

use std::str;

use sha2::{Digest, Sha256};

use crate::event::PublicKey;
use crate::filter::Filter;

/// A `HyperLogLog` register set for one COUNT query.
///
/// `offset` selects the 8-byte window into each counted author's
/// pubkey; it is derived from the query's tag value so clients can
/// combine sketches from relays that agree on the same rule.
#[derive(Clone, Debug)]
pub struct Hll {
    offset: usize,
    registers: [u8; 256],
}

impl Hll {
    /// Build the sketch for a COUNT filter set.
    ///
    /// Eligible iff exactly one filter carries exactly one tag
    /// condition holding exactly one value (NIP-45: "only one tag
    /// condition"). Anything else returns `None` and the relay omits
    /// the `hll` field.
    #[must_use]
    pub fn for_filters(filters: &[Filter]) -> Option<Self> {
        let [filter] = filters else { return None };
        let [(name, values)] = filter.tags() else {
            return None;
        };
        let [value] = values.as_ref() else {
            return None;
        };
        Some(Self {
            offset: offset_for(*name, value.as_bytes())?,
            registers: [0; 256],
        })
    }

    /// The byte offset into pubkeys this sketch was built with.
    #[must_use]
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// Add one author's pubkey, the go-nostr `AddBytes` update.
    pub fn add(&mut self, pubkey: &PublicKey) {
        let x: [u8; 8] = pubkey
            .0
            .get(self.offset..self.offset + 8)
            .and_then(|s| <&[u8; 8]>::try_from(s).ok())
            .copied()
            .unwrap_or_default();
        let j = usize::from(x[0]);
        // clz56: leading zeros of the low 56 bits; 1..=57.
        let low = u64::from_be_bytes(x) & ((1 << 56) - 1);
        let zero_bits = u8::try_from(low.leading_zeros().saturating_sub(7)).unwrap_or(u8::MAX);
        if let Some(register) = self.registers.get_mut(j) {
            *register = (*register).max(zero_bits);
        }
    }

    /// The 256 registers in order, hex-encoded on the wire.
    #[must_use]
    pub const fn registers(&self) -> &[u8; 256] {
        &self.registers
    }
}

/// The `offset` rule from NIP-45: obtain a 32-byte hex string from the
/// tag value, then `offset = hex_nibble(hex[32]) + 8`.
///
/// `e`/`p` tag values are stored packed as raw 32 bytes, so they are
/// hex-encoded back; every other value uses its string form: a
/// 64-char lowercase hex string as is, an `a`-style
/// `<kind>:<pubkey>:<d>` string via its pubkey part, anything else
/// sha256-hashed.
fn offset_for(name: u8, value: &[u8]) -> Option<usize> {
    let mut hex = [0u8; 64];
    if name == b'e' || name == b'p' {
        let bytes: &[u8; 32] = value.try_into().ok()?;
        hex.copy_from_slice(faster_hex::hex_string(bytes).as_bytes());
    } else {
        hex = derive_hex(str::from_utf8(value).ok()?);
    }
    Some(usize::from(hexdigit(*hex.get(32)?)?) + 8)
}

/// The 64-char hex the offset rule uses for a string value.
fn derive_hex(value: &str) -> [u8; 64] {
    let mut out = [0u8; 64];
    if is_hex64(value) {
        out.copy_from_slice(value.as_bytes());
        return out;
    }
    // <kind>:<pubkey>:<d> -> the pubkey part.
    let mut parts = value.splitn(3, ':');
    if let (Some(kind), Some(pk), Some(_)) = (parts.next(), parts.next(), parts.next())
        && !kind.is_empty()
        && kind.bytes().all(|b| b.is_ascii_digit())
        && is_hex64(pk)
    {
        out.copy_from_slice(pk.as_bytes());
        return out;
    }
    let digest = Sha256::digest(value.as_bytes());
    out.copy_from_slice(faster_hex::hex_string(&digest).as_bytes());
    out
}

/// All-lowercase 32-byte hex.
fn is_hex64(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `0-9a-f` -> `0..=15`.
const fn hexdigit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}
