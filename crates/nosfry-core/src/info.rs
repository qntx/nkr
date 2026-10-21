//! NIP-11 relay information: the `[relay.info]` configuration and
//! the document builder shared by the native and Workers editions.

use serde::Deserialize;
use serde_json::json;

use crate::limits::Limits;
use crate::message::SUB_ID_MAX_CHARS;
use crate::moderation::Moderation;

/// `[relay.info]` — NIP-11 document fields.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Info {
    /// Relay name.
    pub name: String,
    /// Plain-text description.
    pub description: String,
    /// Relay operator pubkey (hex).
    pub pubkey: String,
    /// Relay operator contact.
    pub contact: String,
    /// Relay icon URL.
    pub icon: String,
}

impl Default for Info {
    fn default() -> Self {
        Self {
            name: "nosfry".into(),
            description: String::new(),
            pubkey: String::new(),
            contact: String::new(),
            icon: String::new(),
        }
    }
}

impl Info {
    /// The effective relay info: the NIP-86 moderation snapshot's
    /// `name`/`description`/`icon` override the configured values
    /// where set.
    #[must_use]
    pub fn with_overrides(&self, moderation: &Moderation) -> Self {
        Self {
            name: moderation.name.as_deref().unwrap_or(&self.name).to_owned(),
            description: moderation
                .description
                .as_deref()
                .unwrap_or(&self.description)
                .to_owned(),
            pubkey: self.pubkey.clone(),
            contact: self.contact.clone(),
            icon: moderation.icon.as_deref().unwrap_or(&self.icon).to_owned(),
        }
    }
}

/// The NIP-11 relay information document. `description`, `pubkey`,
/// `contact`, and `icon` are optional and omitted when empty; `name`
/// stays unconditionally.
#[must_use]
pub fn document(info: &Info, nips: &[u16], limits: &Limits, max_message_length: usize) -> String {
    let mut doc = json!({
        "name": info.name.as_str(),
        "supported_nips": nips,
        "software": "https://github.com/qntx/nosfry",
        "version": env!("CARGO_PKG_VERSION"),
        "limitation": {
            "max_message_length": max_message_length,
            "max_subscriptions": limits.req.max_subs_per_connection,
            "max_limit": limits.req.max_filter_limit,
            "default_limit": limits.req.max_filter_limit,
            "max_subid_length": SUB_ID_MAX_CHARS,
            "max_event_tags": limits.event.max_num_tags,
            "created_at_lower_limit": limits.event.reject_events_older_than_seconds,
            "created_at_upper_limit": limits.event.reject_events_newer_than_seconds,
            "auth_required": false,
            "restricted_writes": false,
            "payment_required": false,
        },
    });
    if let Some(map) = doc.as_object_mut() {
        for (key, value) in [
            ("description", info.description.as_str()),
            ("pubkey", info.pubkey.as_str()),
            ("contact", info.contact.as_str()),
            ("icon", info.icon.as_str()),
        ] {
            if !value.is_empty() {
                map.insert(key.to_owned(), json!(value));
            }
        }
    }
    doc.to_string()
}
