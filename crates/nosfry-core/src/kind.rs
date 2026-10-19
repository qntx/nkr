//! NIP-01 event kind and its storage class.

/// A NIP-01 event kind (`0..=65535`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Kind(pub u16);

/// How a kind must be stored: the four NIP-01 ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KindClass {
    /// Stored and queried like ordinary history.
    Regular,
    /// Only the latest event per `(pubkey, kind)` is kept (`0`, `3`,
    /// `10000..20000`).
    Replaceable,
    /// Broadcast only, never stored (`20000..30000`).
    Ephemeral,
    /// Only the latest event per `(pubkey, kind, d)` is kept
    /// (`30000..40000`).
    Addressable,
}

impl Kind {
    /// `true` for kinds `0`, `3` and `10000..20000`.
    #[must_use]
    pub fn is_replaceable(self) -> bool {
        self.0 == 0 || self.0 == 3 || (10_000..20_000).contains(&self.0)
    }

    /// `true` for `20000..30000`.
    #[must_use]
    pub fn is_ephemeral(self) -> bool {
        (20_000..30_000).contains(&self.0)
    }

    /// `true` for `30000..40000`.
    #[must_use]
    pub fn is_addressable(self) -> bool {
        (30_000..40_000).contains(&self.0)
    }

    /// The NIP-01 `regular` flag: `k < 10000` except the replaceable kinds
    /// `0` and `3`. Note this is *not* the negation of the other flags:
    /// kinds `>= 40000` report `false` for every flag yet classify as
    /// [`KindClass::Regular`].
    #[must_use]
    pub fn is_regular(self) -> bool {
        self.0 < 10_000 && !self.is_replaceable()
    }

    /// The storage class of this kind.
    #[must_use]
    pub fn class(self) -> KindClass {
        if self.is_replaceable() {
            KindClass::Replaceable
        } else if self.is_ephemeral() {
            KindClass::Ephemeral
        } else if self.is_addressable() {
            KindClass::Addressable
        } else {
            KindClass::Regular
        }
    }
}

impl From<u16> for Kind {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<Kind> for u16 {
    fn from(value: Kind) -> Self {
        value.0
    }
}
