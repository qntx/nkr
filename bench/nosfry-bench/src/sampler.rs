//! Per-phase measurement plumbing shared by every load mode:
//!
//! - [`Sampler`]: a background task reading the relay process's
//!   `/proc` counters once per second over the measured phase, with a
//!   `ps` fallback off Linux.
//! - [`HarnessMeter`]: the harness's own CPU utilization over the
//!   phase, so a saturated load generator invalidates the run instead
//!   of silently under-driving the relay.
//! - [`probe_extensions`]: one raw HTTP/1.1 upgrade that records the
//!   relay's negotiated `Sec-WebSocket-Extensions` verbatim.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

/// Sampling cadence of the relay `/proc` reader.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
/// Harness CPU utilization above this marks the run as harness-bound.
const SATURATION: f64 = 0.70;

/// One relay reading: counters at `t_s` seconds into the phase.
#[derive(Clone, Copy)]
struct Sample {
    t_s: f64,
    cpu_s: f64,
    rss_kib: u64,
    rss_anon_kib: Option<u64>,
    threads: Option<u64>,
}

/// Take and store one reading of `pid`.
fn push_sample(shared: &Mutex<Vec<Sample>>, pid: u32, started: Instant) {
    if let Some(sample) = take_sample(pid, started) {
        shared.lock().expect("mutex").push(sample);
    }
}

/// One `/proc` reading of `pid`, `started` seconds in.
fn take_sample(pid: u32, started: Instant) -> Option<Sample> {
    let stats = crate::proc_stats(pid)?;
    Some(Sample {
        t_s: started.elapsed().as_secs_f64(),
        cpu_s: stats.cpu_secs,
        rss_kib: stats.rss_kib,
        rss_anon_kib: stats.rss_anon_kib,
        threads: stats.threads,
    })
}

/// Samples `pid` once per second until [`Sampler::finish`]. On
/// non-Linux hosts the reads degrade to `ps` and the JSON says so.
pub(crate) struct Sampler {
    samples: Arc<Mutex<Vec<Sample>>>,
    task: tokio::task::JoinHandle<()>,
    backend: &'static str,
    started: Instant,
}

impl Sampler {
    /// Start sampling `pid` (the relay process) in the background.
    pub(crate) fn start(pid: u32) -> Self {
        let samples = Arc::new(Mutex::new(Vec::new()));
        let shared = Arc::clone(&samples);
        let started = Instant::now();
        let task = tokio::spawn(async move {
            loop {
                push_sample(&shared, pid, started);
                tokio::time::sleep(SAMPLE_INTERVAL).await;
            }
        });
        Self {
            samples,
            task,
            backend: if cfg!(target_os = "linux") {
                "proc"
            } else {
                "ps"
            },
            started,
        }
    }

    /// Stop sampling and return the series plus phase summaries:
    /// relay CPU seconds spent and RssAnon at start / max / end.
    pub(crate) fn finish(self) -> Value {
        self.task.abort();
        let samples = std::mem::take(&mut *self.samples.lock().expect("mutex"));
        let series: Vec<Value> = samples
            .iter()
            .map(|s| {
                json!({
                    "t_s": s.t_s,
                    "cpu_s": s.cpu_s,
                    "rss_kib": s.rss_kib,
                    "rss_anon_kib": s.rss_anon_kib,
                    "threads": s.threads,
                })
            })
            .collect();
        let anon_max = samples.iter().filter_map(|s| s.rss_anon_kib).max();
        let (first, last) = (samples.first(), samples.last());
        json!({
            "backend": self.backend,
            "wall_s": self.started.elapsed().as_secs_f64(),
            "series": series,
            "relay_cpu_secs": match (first, last) {
                (Some(f), Some(l)) if samples.len() > 1 => json!(l.cpu_s - f.cpu_s),
                _ => Value::Null,
            },
            "rss_anon_kib_start": first.and_then(|s| s.rss_anon_kib),
            "rss_anon_kib_max": anon_max,
            "rss_anon_kib_end": last.and_then(|s| s.rss_anon_kib),
        })
    }
}

/// Harness self-utilization over one measured phase.
///
/// `harness_cpu_util` is the fraction of the process's CPU affinity
/// mask it burned; a run driving the relay past 70% of its own cores
/// measures the harness, not the relay.
pub(crate) struct HarnessMeter {
    cpu0: f64,
    cores: usize,
    started: Instant,
    backend: &'static str,
}

impl HarnessMeter {
    /// Snapshot the harness's own CPU usage and affinity at phase
    /// start.
    pub(crate) fn begin() -> Self {
        Self {
            cpu0: crate::proc_stats(std::process::id()).map_or(0.0, |s| s.cpu_secs),
            cores: affinity_cores(),
            started: Instant::now(),
            backend: if cfg!(target_os = "linux") {
                "proc"
            } else {
                "ps"
            },
        }
    }

    /// Close the phase and emit the utilization fields.
    pub(crate) fn finish(&self) -> Value {
        let cpu1 = crate::proc_stats(std::process::id()).map_or(0.0, |s| s.cpu_secs);
        let wall = self.started.elapsed().as_secs_f64();
        let util = if wall > 0.0 && self.cores > 0 {
            (cpu1 - self.cpu0) / (wall * self.cores as f64)
        } else {
            0.0
        };
        json!({
            "harness_backend": self.backend,
            "harness_cores": self.cores,
            "harness_cpu_s": cpu1 - self.cpu0,
            "harness_cpu_util": util,
            "harness_saturated": util > SATURATION,
        })
    }
}

/// The size of this process's CPU affinity mask: `sched_getaffinity`
/// under Linux, the available parallelism elsewhere.
fn affinity_cores() -> usize {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of_val(&set), &mut set) == 0 {
                let n = libc::CPU_COUNT(&set) as usize;
                if n > 0 {
                    return n;
                }
            }
        }
        1
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::thread::available_parallelism().map_or(1, |n| n.get())
    }
}

/// One raw HTTP/1.1 WebSocket upgrade against `url` to read back the
/// negotiated `Sec-WebSocket-Extensions` verbatim. `offer_deflate`
/// adds the `permessage-deflate` offer matching what `connect` sends
/// for `--deflate` runs. Blocking and small; callers should run it on
/// `spawn_blocking`. `ws://` only — for `wss://` the extension is
/// reported by the relay handshake the same way, but this probe
/// returns an error rather than reimplementing TLS.
pub(crate) fn probe_extensions(
    url: &url::Url,
    offer_deflate: bool,
) -> Result<Option<String>, String> {
    if url.scheme() != "ws" {
        return Err(format!(
            "extension probe only supports ws:// ({})",
            url.scheme()
        ));
    }
    let host = url.host_str().ok_or("url has no host")?;
    let port = url.port_or_known_default().unwrap_or(80);
    let mut stream = TcpStream::connect((host, port)).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| e.to_string())?;
    let path = if url.path().is_empty() {
        "/"
    } else {
        url.path()
    };
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\n\
         Connection: Upgrade\r\nSec-WebSocket-Key: cHJvYmVrZXkwMDAwMDAwMDA=\r\n\
         Sec-WebSocket-Version: 13\r\n"
    );
    if offer_deflate {
        req.push_str("Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n");
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut lines = BufReader::new(stream).lines();
    let status = lines
        .next()
        .ok_or("no response")?
        .map_err(|e| e.to_string())?;
    if !status.contains(" 101") {
        return Err(format!("upgrade refused: {status}"));
    }
    let mut extensions = None;
    for line in lines {
        let line = line.map_err(|e| e.to_string())?;
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("sec-websocket-extensions")
        {
            extensions = Some(value.trim().to_owned());
        }
    }
    Ok(extensions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn affinity_cores_is_positive() {
        assert!(affinity_cores() > 0);
    }
}
