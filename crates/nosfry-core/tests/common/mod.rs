//! Shared helpers for the vendored vector runners.

#![allow(dead_code, reason = "helpers are shared across runners")]

use std::path::Path;

use nosfry_core::event::{EventLimits, UnverifiedEvent};
use nosfry_core::filter::Filter;
use nosfry_core::message::ClientMessage;
use nosfry_core::packed::TagValueBuf;
use serde::Deserialize;

/// Load `tests/vectors/<file>` and return its `cases` array.
pub(crate) fn cases<T: for<'de> Deserialize<'de>>(file: &str) -> Vec<T> {
    #[derive(Deserialize)]
    struct Vectors<T> {
        cases: Vec<T>,
    }
    let text = read(file);
    let parsed: Vectors<T> =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{file}: vector schema: {e}"));
    parsed.cases
}

/// Read `tests/vectors/<file>` to a string.
pub(crate) fn read(file: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/vectors")
        .join(file);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Case-insensitive hex decode, for the BIP-340 CSV only.
pub(crate) fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// A case-insensitive hex decode into a fixed array, for the BIP-340 CSV.
pub(crate) fn hex_decode_array<const N: usize>(s: &str) -> Option<[u8; N]> {
    hex_decode(s)?.try_into().ok()
}

/// Lowercase hex encode.
pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            write!(out, "{b:02x}").expect("write");
            out
        })
}

/// Canonical wire form of a compiled filter: `#` conditions first, then
/// `authors`, `ids`, `kinds`, `limit`, `since`, `until` (ASCII key order),
/// hex normalized to lowercase. Mirrors the SDK's `encoded` vectors.
pub(crate) fn filter_wire(filter: &Filter) -> String {
    let mut parts: Vec<String> = Vec::new();
    for (name, values) in filter.tags() {
        let list = values
            .iter()
            .map(|value| tag_wire_value(*name, value))
            .collect::<Vec<_>>()
            .join(",");
        parts.push(format!("\"#{}\":[{list}]", char::from(*name)));
    }
    if let Some(authors) = filter.authors() {
        parts.push(format!("\"authors\":[{}]", hex_list(authors)));
    }
    if let Some(ids) = filter.ids() {
        parts.push(format!("\"ids\":[{}]", hex_list(ids)));
    }
    if let Some(kinds) = filter.kinds() {
        let list = kinds
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(",");
        parts.push(format!("\"kinds\":[{list}]"));
    }
    if let Some(limit) = filter.limit() {
        parts.push(format!("\"limit\":{limit}"));
    }
    if let Some(since) = filter.since() {
        parts.push(format!("\"since\":{since}"));
    }
    if let Some(until) = filter.until() {
        parts.push(format!("\"until\":{until}"));
    }
    format!("{{{}}}", parts.join(","))
}

fn hex_list(ids: &[[u8; 32]]) -> String {
    ids.iter()
        .map(|id| format!("\"{}\"", hex_lower(id)))
        .collect::<Vec<_>>()
        .join(",")
}

fn tag_wire_value(name: u8, value: &TagValueBuf) -> String {
    let raw = if name == b'e' || name == b'p' {
        hex_lower(value.as_bytes())
    } else {
        String::from_utf8_lossy(value.as_bytes()).into_owned()
    };
    serde_json::to_string(raw.as_str()).unwrap_or_default()
}

/// Canonical client message wire form, rebuilt from the parsed message.
/// Used to compare `encoded` in `message-client.json`.
pub(crate) fn message_wire(message: ClientMessage<'_>) -> String {
    match message {
        ClientMessage::Event(ev) => format!("[\"EVENT\",{}]", event_wire(ev)),
        ClientMessage::Req { sub_id, filters } => {
            let filters = filters
                .iter()
                .map(filter_wire)
                .collect::<Vec<_>>()
                .join(",");
            format!("[\"REQ\",{},{filters}]", sub_id.json())
        }
        ClientMessage::Close { sub_id } => format!("[\"CLOSE\",{}]", sub_id.json()),
        ClientMessage::Auth(ev) => format!("[\"AUTH\",{}]", event_wire(ev)),
        ClientMessage::Count { sub_id, filters } => {
            let filters = filters
                .iter()
                .map(filter_wire)
                .collect::<Vec<_>>()
                .join(",");
            format!("[\"COUNT\",{},{filters}]", sub_id.json())
        }
        ClientMessage::NegOpen {
            sub_id,
            filter,
            msg,
        } => format!(
            "[\"NEG-OPEN\",{},{},\"{}\"]",
            sub_id.json(),
            filter_wire(&filter),
            hex_lower(&msg)
        ),
        ClientMessage::NegMsg { sub_id, msg } => {
            format!("[\"NEG-MSG\",{},\"{}\"]", sub_id.json(), hex_lower(&msg))
        }
        ClientMessage::NegClose { sub_id } => {
            format!("[\"NEG-CLOSE\",{}]", sub_id.json())
        }
    }
}

/// Canonical event JSON via the public verify path. `encoded` vectors
/// never carry an EVENT/AUTH payload, so an unverifiable event only
/// appears in a failure diagnostic.
fn event_wire(ev: UnverifiedEvent<'_>) -> String {
    match ev.verify(&EventLimits::default()) {
        Ok(event) => event.json().to_owned(),
        Err(error) => format!("\"<{}>\"", error.reason()),
    }
}
