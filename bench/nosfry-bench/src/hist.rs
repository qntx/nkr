//! A thin wrapper over `hdrhistogram`: latencies go in as
//! microseconds, the JSON summary comes out in milliseconds, and the
//! whole histogram serializes to a base64 V2-deflate blob so separate
//! reps can be merged exactly during analysis.

use hdrhistogram::Histogram;
use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};
use serde_json::{Value, json};

/// Highest recordable value: 120 s in microseconds. Values above this
/// saturate `over` instead of the histogram.
const MAX_US: u64 = 120_000_000;

/// A latency histogram with 3 significant digits over `1µs..=120s`.
pub(crate) struct Hist {
    h: Histogram<u64>,
    /// Samples beyond the histogram's range; reported separately so an
    /// unbounded tail cannot silently fold into the max bucket.
    over: u64,
}

impl Hist {
    pub(crate) fn new() -> Self {
        Self {
            h: Histogram::new_with_bounds(1, MAX_US, 3).expect("static bounds are valid"),
            over: 0,
        }
    }

    /// Record one latency sample in microseconds.
    pub(crate) fn record_us(&mut self, us: u64) {
        if us > MAX_US {
            self.over += 1;
        } else {
            self.h.saturating_record(us.max(1));
        }
    }

    /// Fold another histogram's samples into this one.
    pub(crate) fn merge(&mut self, other: &Hist) {
        self.h.add(&other.h).expect("same bounds");
        self.over += other.over;
    }

    fn value_ms(&self, p: f64) -> f64 {
        self.h.value_at_percentile(p) as f64 / 1000.0
    }

    /// The standard latency summary plus `hist_b64`, the V2-deflate
    /// serialized histogram for exact offline merging.
    pub(crate) fn json(&self) -> Value {
        let mut buf = Vec::new();
        let hist_b64 = V2DeflateSerializer::new()
            .serialize(&self.h, &mut buf)
            .map(|_| crate::b64(&buf))
            .unwrap_or_default();
        let ms = |v: u64| v as f64 / 1000.0;
        json!({
            "count": self.h.len(),
            "over_range": self.over,
            "mean_ms": self.h.mean() / 1000.0,
            "p50_ms": self.value_ms(50.0),
            "p90_ms": self.value_ms(90.0),
            "p99_ms": self.value_ms(99.0),
            "p999_ms": self.value_ms(99.9),
            "p9999_ms": self.value_ms(99.99),
            "max_ms": ms(self.h.max()),
            "hist_b64": hist_b64,
        })
    }
}
