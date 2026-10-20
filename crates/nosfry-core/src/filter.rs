//! Strict NIP-01 filter: compiled form and allocation-free matching.
//!
//! Conditions inside a filter are `AND`ed; missing lists are unconstrained;
//! empty `ids`/`authors`/`kinds` lists make the filter impossible (NIP-01).
//! Unknown keys are rejected (`unsupported:`), `search` (NIP-50) is
//! rejected, and `#` keys that are not exactly one ASCII letter are
//! rejected — a relay follows strfry and never silently widens a query.
//! `ids`, `authors`, `#e` and `#p` values are hex — accepted
//! case-insensitively, normalized to lowercase. Empty `#` value lists make
//! the filter impossible like empty `ids`/`authors`/`kinds` (NIP-01).

use std::borrow::Cow;

use serde::Deserialize;
use serde::de::{Deserializer as _, MapAccess, Visitor};
use serde_json::value::RawValue;

use crate::hex;
use crate::kind::Kind;
use crate::packed::{PackedRef, TagValueBuf, encode_tag_value};

/// Ordered `(key, value)` pairs of the raw filter object; duplicates are
/// preserved so [`Filter::compile`] can reject them like JSON.parse cannot.
struct Pairs<'a>(Vec<(Cow<'a, str>, &'a RawValue)>);

struct PairsVisitor;

impl<'de> Visitor<'de> for PairsVisitor {
    type Value = Pairs<'de>;

    fn expecting(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("a filter object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut pairs = Vec::with_capacity(map.size_hint().unwrap_or(0));
        while let Some((key, value)) = map.next_entry::<Cow<'de, str>, &'de RawValue>()? {
            pairs.push((key, value));
        }
        Ok(Pairs(pairs))
    }
}

/// Fields inside a filter are `AND`ed.
#[derive(Debug)]
pub struct Filter {
    ids: Option<Box<[[u8; 32]]>>,
    authors: Option<Box<[[u8; 32]]>>,
    kinds: Option<Box<[u16]>>,
    since: Option<u64>,
    until: Option<u64>,
    limit: Option<u64>,
    tags: Box<[(u8, Box<[TagValueBuf]>)]>,
    impossible: bool,
}

impl Filter {
    /// Parse a filter object from a JSON string.
    ///
    /// # Errors
    ///
    /// [`FilterError::Invalid`] on malformed values; duplicate keys are
    /// rejected (JSON.parse would take the last one — see the vector
    /// divergences).
    pub fn parse(json: &str) -> Result<Self, FilterError> {
        Self::from_raw(serde_json::from_str::<&RawValue>(json)?)
    }

    /// Parse a filter object from a borrowed raw JSON value.
    ///
    /// # Errors
    ///
    /// Same as [`Filter::parse`].
    pub fn from_raw(raw: &RawValue) -> Result<Self, FilterError> {
        let mut de = serde_json::Deserializer::from_str(raw.get());
        let Pairs(pairs) = de
            .deserialize_map(PairsVisitor)
            .map_err(|e| FilterError::Invalid(format!("filter: {e}")))?;
        de.end()
            .map_err(|e| FilterError::Invalid(format!("filter: {e}")))?;
        Self::compile(&pairs)
    }

    /// The `limit` value, if present.
    #[must_use]
    pub const fn limit(&self) -> Option<u64> {
        self.limit
    }

    /// The `since` value, if present.
    #[must_use]
    pub const fn since(&self) -> Option<u64> {
        self.since
    }

    /// The `until` value, if present.
    #[must_use]
    pub const fn until(&self) -> Option<u64> {
        self.until
    }

    /// Number of `#` conditions.
    #[must_use]
    pub fn tag_filter_count(&self) -> usize {
        self.tags.len()
    }

    /// Whether this filter can never match (empty `ids`/`authors`/`kinds`,
    /// or a `#` condition whose values can never be packed).
    #[must_use]
    pub const fn is_impossible(&self) -> bool {
        self.impossible
    }

    /// Sorted, deduplicated `ids`, if present.
    #[must_use]
    pub fn ids(&self) -> Option<&[[u8; 32]]> {
        self.ids.as_deref()
    }

    /// Sorted, deduplicated `authors`, if present.
    #[must_use]
    pub fn authors(&self) -> Option<&[[u8; 32]]> {
        self.authors.as_deref()
    }

    /// Sorted `kinds`, if present.
    #[must_use]
    pub fn kinds(&self) -> Option<&[u16]> {
        self.kinds.as_deref()
    }

    /// `#` conditions as `(letter, sorted values)`.
    #[must_use]
    pub fn tags(&self) -> &[(u8, Box<[TagValueBuf]>)] {
        &self.tags
    }

    /// Intrinsic result bound implied by the filter itself, independent of
    /// any external cap: empty ids/kinds/authors => `0`, deduplicated ids
    /// count, explicit `limit`, or `None` when unbounded
    /// (`vectors/filter-limit.json`).
    ///
    /// The kinds cardinality mirrors nostr-tools `getFilterLimit`
    /// (filter.ts @ `8ffc628`): `authors * kinds` only when every kind
    /// is replaceable (one live event per author+kind); `authors *
    /// kinds * #d` only when every kind is addressable *and* a `#d`
    /// condition is present (one live event per author+kind+d value).
    /// Addressable kinds without `#d`, and kinds mixing classes, are
    /// unbounded — an author may store any number of `d` addresses.
    #[must_use]
    pub fn intrinsic_limit(&self) -> Option<u64> {
        if self.impossible {
            return Some(0);
        }
        let mut bound = self.limit;
        if let Some(ids) = &self.ids {
            bound = Some(bound.map_or(ids.len() as u64, |b| b.min(ids.len() as u64)));
        }
        if let (Some(kinds), Some(authors)) = (&self.kinds, &self.authors)
            && !kinds.is_empty()
            && !authors.is_empty()
            && let Some(card) = self.kinds_cardinality(kinds, authors)
        {
            bound = Some(bound.map_or(card, |b| b.min(card)));
        }
        bound
    }

    /// The author+kind cardinality a filter can hold: `authors *
    /// kinds` when every kind is replaceable, `authors * kinds * #d`
    /// when every kind is addressable and a `#d` condition is present,
    /// else `None`.
    fn kinds_cardinality(&self, kinds: &[u16], authors: &[[u8; 32]]) -> Option<u64> {
        if kinds.iter().all(|k| Kind(*k).is_replaceable()) {
            return Some((kinds.len() as u64).saturating_mul(authors.len() as u64));
        }
        if !kinds.iter().all(|k| Kind(*k).is_addressable()) {
            return None;
        }
        self.tags
            .iter()
            .find(|(name, _)| *name == b'd')
            .filter(|(_, values)| !values.is_empty())
            .map(|(_, values)| {
                (kinds.len() as u64)
                    .saturating_mul(authors.len() as u64)
                    .saturating_mul(values.len() as u64)
            })
    }

    /// Result bound after applying `max` as the external cap.
    #[must_use]
    pub fn effective_limit(&self, max: u64) -> u64 {
        self.intrinsic_limit().unwrap_or(max).min(max)
    }

    /// Whether `ev` satisfies every present condition. Allocation-free.
    #[must_use]
    pub fn matches(&self, ev: PackedRef<'_>) -> bool {
        if self.impossible {
            return false;
        }
        if let Some(ids) = &self.ids
            && ids.binary_search(&ev.id().0).is_err()
        {
            return false;
        }
        if let Some(authors) = &self.authors
            && authors.binary_search(&ev.pubkey().0).is_err()
        {
            return false;
        }
        if let Some(kinds) = &self.kinds
            && kinds.binary_search(&ev.kind().0).is_err()
        {
            return false;
        }
        if let Some(since) = self.since
            && ev.created_at() < since
        {
            return false;
        }
        if let Some(until) = self.until
            && ev.created_at() > until
        {
            return false;
        }
        for (name, values) in &self.tags {
            let hit = ev.tags().any(|(n, v)| {
                n == *name && values.binary_search_by(|w| w.as_bytes().cmp(v)).is_ok()
            });
            if !hit {
                return false;
            }
        }
        true
    }

    fn compile(pairs: &[(Cow<'_, str>, &RawValue)]) -> Result<Self, FilterError> {
        let mut filter = Self {
            ids: None,
            authors: None,
            kinds: None,
            since: None,
            until: None,
            limit: None,
            tags: Box::new([]),
            impossible: false,
        };
        let mut tag_conditions: Vec<(u8, Box<[TagValueBuf]>)> = Vec::new();
        for (i, (key, raw)) in pairs.iter().enumerate() {
            let key = key.as_ref();
            if pairs.iter().take(i).any(|(prev, _)| prev.as_ref() == key) {
                return Err(FilterError::Invalid(format!("duplicate key `{key}`")));
            }
            match key {
                "ids" => filter.ids = Some(decode_hex_list(raw, "ids")?),
                "authors" => filter.authors = Some(decode_hex_list(raw, "authors")?),
                "kinds" => {
                    let mut kinds: Vec<u16> = parse_value(raw, "kinds")?;
                    kinds.sort_unstable();
                    kinds.dedup();
                    filter.kinds = Some(kinds.into_boxed_slice());
                }
                "since" => filter.since = Some(parse_value(raw, "since")?),
                "until" => filter.until = Some(parse_value(raw, "until")?),
                "limit" => filter.limit = Some(parse_value(raw, "limit")?),
                "search" => {
                    return Err(FilterError::Unsupported(
                        "search (NIP-50) is not supported".to_owned(),
                    ));
                }
                _ => compile_tag_condition(key, raw, &mut tag_conditions, &mut filter)?,
            }
        }
        if filter.ids.as_ref().is_some_and(|v| v.is_empty())
            || filter.authors.as_ref().is_some_and(|v| v.is_empty())
            || filter.kinds.as_ref().is_some_and(|v| v.is_empty())
        {
            filter.impossible = true;
        }
        tag_conditions.sort_by_key(|(name, _)| *name);
        filter.tags = tag_conditions.into_boxed_slice();
        Ok(filter)
    }
}

/// Compile one `#<letter>` condition. Unknown keys and `#` names that are
/// not exactly one ASCII letter are rejected (strfry relay policy); empty
/// value lists make the filter impossible (NIP-01); values that cannot be
/// packed can never match, so a non-empty list of them is impossible too.
fn compile_tag_condition(
    key: &str,
    raw: &RawValue,
    tag_conditions: &mut Vec<(u8, Box<[TagValueBuf]>)>,
    filter: &mut Filter,
) -> Result<(), FilterError> {
    let Some(name) = key.strip_prefix('#') else {
        return Err(FilterError::Unsupported(format!(
            "unrecognised filter item: {key}"
        )));
    };
    let mut chars = name.chars();
    let letter = chars.next().unwrap_or_default();
    if !letter.is_ascii_alphabetic() || chars.next().is_some() {
        return Err(FilterError::Unsupported(format!(
            "unindexed tag filter: {key}"
        )));
    }
    let values: Vec<Cow<'_, str>> = parse_value(raw, key)?;
    let mut encoded = Vec::with_capacity(values.len());
    for value in &values {
        let normalized;
        let mut value = value.as_ref();
        if letter == 'e' || letter == 'p' {
            if hex::decode_32_ci(value).is_none() {
                return Err(FilterError::Invalid(format!(
                    "`{key}` values must be 64 hex chars"
                )));
            }
            normalized = value.to_ascii_lowercase();
            value = &normalized;
        }
        if let Some(buf) = encode_tag_value(letter as u8, value) {
            encoded.push(buf);
        }
    }
    if encoded.is_empty() {
        filter.impossible = true;
        return Ok(());
    }
    encoded.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    encoded.dedup();
    tag_conditions.push((letter as u8, encoded.into_boxed_slice()));
    Ok(())
}

/// `ids`/`authors`: exactly 64 hex chars, case-insensitive, deduplicated.
fn decode_hex_list(raw: &RawValue, key: &str) -> Result<Box<[[u8; 32]]>, FilterError> {
    let values: Vec<Cow<'_, str>> = parse_value(raw, key)?;
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let bytes = hex::decode_32_ci(&value)
            .ok_or_else(|| FilterError::Invalid(format!("`{key}` values must be 64 hex chars")))?;
        out.push(bytes);
    }
    out.sort_unstable();
    out.dedup();
    Ok(out.into_boxed_slice())
}

fn parse_value<T: for<'de> Deserialize<'de>>(raw: &RawValue, key: &str) -> Result<T, FilterError> {
    serde_json::from_str(raw.get()).map_err(|e| FilterError::Invalid(format!("`{key}`: {e}")))
}

/// Filter validation failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FilterError {
    /// The value could not be parsed as JSON.
    #[error("malformed JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// A field was malformed; `invalid:` reason.
    #[error("invalid: {0}")]
    Invalid(String),
    /// A field the relay does not support; `unsupported:` reason.
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl FilterError {
    /// NIP-01 machine-readable reason string; always prefixed.
    #[must_use]
    pub fn reason(&self) -> String {
        match self {
            Self::Json(_) => format!("invalid: {self}"),
            Self::Invalid(_) | Self::Unsupported(_) => self.to_string(),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "unit tests may panic"
)]
mod tests {
    use super::*;

    fn encode_32(bytes: &[u8; 32]) -> String {
        use std::fmt::Write as _;
        bytes.iter().fold(String::with_capacity(64), |mut out, b| {
            write!(out, "{b:02x}").expect("write");
            out
        })
    }

    #[test]
    fn parses_plain_object() {
        let filter = Filter::parse(r#"{"kinds":[1,3]}"#).expect("parse");
        assert_eq!(filter.kinds(), Some(&[1u16, 3][..]));
        assert!(!filter.is_impossible());
    }

    #[test]
    fn parses_tag_conditions() {
        let filter = Filter::parse(r##"{"#t":["a","b"],"kinds":[1]}"##).expect("parse");
        assert_eq!(filter.tag_filter_count(), 1);
        let [(name, values)] = filter.tags() else {
            panic!("one tag condition");
        };
        assert_eq!(*name, b't');
        let values: Vec<&[u8]> = values.iter().map(TagValueBuf::as_bytes).collect();
        assert_eq!(values, [&b"a"[..], &b"b"[..]]);
    }

    #[test]
    fn normalizes_uppercase_hex() {
        let id = encode_32(&[0xAB; 32]);
        let filter =
            Filter::parse(&format!(r##"{{"ids":["{id}"],"#e":["{id}"]}}"##)).expect("parse");
        assert_eq!(filter.ids().map(<[_]>::len), Some(1));
        let [(name, values)] = filter.tags() else {
            panic!("one tag condition");
        };
        assert_eq!(*name, b'e');
        assert_eq!(
            values.first().map(TagValueBuf::as_bytes),
            Some(&[0xAB; 32][..])
        );
    }

    #[test]
    fn rejects_duplicate_keys() {
        let err = Filter::parse(r#"{"kinds":[9],"kinds":[1]}"#).expect_err("dup");
        assert!(err.reason().starts_with("invalid:"), "{}", err.reason());
    }

    #[test]
    fn rejects_unknown_keys() {
        for json in [r#"{"foo":1}"#, r#"{"search":"nostr"}"#, r#"{"search":42}"#] {
            let err = Filter::parse(json).expect_err(json);
            assert!(err.reason().starts_with("unsupported:"), "{}", err.reason());
        }
    }

    #[test]
    fn rejects_unindexed_tag_keys() {
        for json in [
            r##"{"#custom":["x"]}"##,
            r##"{"#1":["x"]}"##,
            r##"{"#":["x"]}"##,
        ] {
            let err = Filter::parse(json).expect_err(json);
            assert!(err.reason().starts_with("unsupported:"), "{}", err.reason());
        }
    }

    #[test]
    fn empty_tag_list_is_impossible() {
        let filter = Filter::parse(r##"{"#t":[]}"##).expect("parse");
        assert!(filter.is_impossible());
        assert_eq!(filter.intrinsic_limit(), Some(0));
    }

    #[test]
    fn empty_lists_are_impossible() {
        for json in [r#"{"ids":[]}"#, r#"{"authors":[]}"#, r#"{"kinds":[]}"#] {
            assert!(Filter::parse(json).expect(json).is_impossible(), "{json}");
        }
    }

    #[test]
    fn accepts_u64_beyond_js_safe_range() {
        let filter = Filter::parse(r#"{"since":9007199254740992,"limit":18446744073709551615}"#)
            .expect("parse");
        assert_eq!(filter.since(), Some(9_007_199_254_740_992));
        assert_eq!(filter.limit(), Some(u64::MAX));
    }

    #[test]
    fn every_reason_is_prefixed() {
        let errors = [
            Filter::parse("not json").expect_err("malformed"),
            Filter::parse(r#"{"kinds":"x"}"#).expect_err("invalid"),
            Filter::parse(r#"{"foo":1}"#).expect_err("unsupported"),
        ];
        let prefixes = [
            "duplicate:",
            "pow:",
            "blocked:",
            "rate-limited:",
            "invalid:",
            "restricted:",
            "mute:",
            "error:",
            "auth-required:",
            "unsupported:",
        ];
        for error in &errors {
            let reason = error.reason();
            assert!(
                prefixes.iter().any(|p| reason.starts_with(p)),
                "unprefixed reason: {reason}"
            );
        }
    }
}
