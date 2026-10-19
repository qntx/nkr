//! Connection caps: a global count plus a per-client-IP count.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// Per-IP bucket key: IPv4 as-is, IPv4-mapped IPv6 folded to its IPv4,
/// other IPv6 grouped by /64 — one subscriber typically owns a whole
/// /64, so per-address caps there are trivially bypassed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum IpKey {
    /// IPv4 address bits.
    V4(u32),
    /// First 64 bits of the IPv6 address.
    V6(u64),
}

fn ip_key(ip: IpAddr) -> IpKey {
    match ip {
        IpAddr::V4(v4) => IpKey::V4(u32::from(v4)),
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or_else(
            || IpKey::V6((u128::from(v6) >> 64) as u64),
            |v4| IpKey::V4(u32::from(v4)),
        ),
    }
}

/// Caps on simultaneous connections, shared by every accepted socket.
/// A cap of 0 means unlimited for that dimension.
#[derive(Debug)]
pub(crate) struct ConnectionLimiter {
    /// Live connections, counted even when `max_total` is unlimited.
    total: AtomicUsize,
    /// Live connections per IP key; populated only when `max_per_ip`
    /// is nonzero.
    per_ip: Mutex<HashMap<IpKey, usize, foldhash::fast::RandomState>>,
    /// Maximum `total` (0 = unlimited).
    max_total: usize,
    /// Maximum per `IpKey` (0 = unlimited and untracked).
    max_per_ip: usize,
}

impl ConnectionLimiter {
    /// A limiter with the given caps.
    pub(crate) fn new(max_total: usize, max_per_ip: usize) -> Self {
        Self {
            total: AtomicUsize::new(0),
            per_ip: Mutex::new(HashMap::with_hasher(foldhash::fast::RandomState::default())),
            max_total,
            max_per_ip,
        }
    }

    /// Live connections right now (the `nosfry_connections` gauge);
    /// counted even when `max_total` is unlimited.
    pub(crate) fn total(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }

    /// Count one new connection; `None` when a cap is reached. The
    /// mutex only ever guards a map update, never an `.await`.
    pub(crate) fn try_acquire(self: &Arc<Self>, ip: IpAddr) -> Option<ConnectionPermit> {
        let key = (self.max_per_ip != 0).then(|| ip_key(ip));
        if key.is_some_and(|key| !self.take_ip(key)) {
            return None;
        }
        if !self.take_total() {
            if let Some(key) = key {
                self.release_ip(key);
            }
            return None;
        }
        Some(ConnectionPermit {
            limiter: Arc::clone(self),
            key,
        })
    }

    /// Take one per-IP slot; `false` at the cap. The map is locked
    /// for a counter update only.
    fn take_ip(&self, key: IpKey) -> bool {
        let mut map = self.per_ip.lock().unwrap_or_else(PoisonError::into_inner);
        if map.get(&key).copied().unwrap_or(0) >= self.max_per_ip {
            return false;
        }
        *map.entry(key).or_default() += 1;
        true
    }

    /// Take one global slot; `false` at the cap.
    fn take_total(&self) -> bool {
        if self.max_total == 0 {
            self.total.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let mut current = self.total.load(Ordering::Relaxed);
        loop {
            if current >= self.max_total {
                return false;
            }
            match self.total.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(next) => current = next,
            }
        }
    }

    fn release_ip(&self, key: IpKey) {
        let mut map = self.per_ip.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = map.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                // Drop the entry so the map does not grow with every
                // IP ever seen.
                map.remove(&key);
            }
        }
    }
}

/// RAII lease on one connection slot; release happens on drop.
#[derive(Debug)]
pub(crate) struct ConnectionPermit {
    limiter: Arc<ConnectionLimiter>,
    key: Option<IpKey>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.limiter.total.fetch_sub(1, Ordering::Relaxed);
        if let Some(key) = self.key {
            self.limiter.release_ip(key);
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "unit tests may panic on failure"
)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn ip_key_groups_addresses() {
        let v4 = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        let mapped = IpAddr::V6("::ffff:192.0.2.1".parse::<Ipv6Addr>().unwrap());
        assert_eq!(ip_key(v4), ip_key(mapped), "v4-mapped v6 folds to v4");
        let a = IpAddr::V6("2001:db8::1".parse().unwrap());
        let b = IpAddr::V6("2001:db8::ffff".parse().unwrap());
        let c = IpAddr::V6("2001:db8:1::1".parse().unwrap());
        assert_eq!(ip_key(a), ip_key(b), "same /64 shares a key");
        assert_ne!(ip_key(a), ip_key(c), "different /64 differs");
        assert_ne!(ip_key(v4), ip_key(a), "v4 and v6 differ");
    }

    #[test]
    fn acquire_release_and_map_cleanup() {
        let limiter = Arc::new(ConnectionLimiter::new(0, 2));
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let first = limiter.try_acquire(ip).unwrap();
        let second = limiter.try_acquire(ip).unwrap();
        assert!(limiter.try_acquire(ip).is_none(), "third exceeds the cap");
        assert_eq!(limiter.per_ip.lock().unwrap().len(), 1);
        drop(first);
        let third = limiter.try_acquire(ip).expect("a freed slot opens");
        drop(second);
        assert_eq!(
            limiter.per_ip.lock().unwrap().len(),
            1,
            "the still-held slot keeps its entry"
        );
        drop(third);
    }

    #[test]
    fn empty_map_after_last_release() {
        let limiter = Arc::new(ConnectionLimiter::new(0, 2));
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        drop(limiter.try_acquire(ip).unwrap());
        assert!(
            limiter.per_ip.lock().unwrap().is_empty(),
            "released IPs leave no map entry"
        );
    }

    #[test]
    fn total_cap() {
        let limiter = Arc::new(ConnectionLimiter::new(1, 0));
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let any = limiter.try_acquire(ip).unwrap();
        let other = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        assert!(limiter.try_acquire(other).is_none(), "total cap reached");
        drop(any);
        assert!(limiter.try_acquire(other).is_some(), "total freed");
        // `max_per_ip = 0` tracks no map entries at all.
        assert!(limiter.per_ip.lock().unwrap().is_empty());
    }
}
