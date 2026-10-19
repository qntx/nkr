//! strfry-compatible write-policy plugin bridge: one child process
//! (`/bin/sh -c <plugin>`) that receives each event as a JSON line on
//! stdin and answers `{"id","action","msg"}` on stdout.
//!
//! Protocol (strfry `PluginEventSifter` identically):
//!
//! - Request: `{"type":"new","event":<raw>,"receivedAt":<unix>,`
//!   `"sourceType":"IP4"|"IP6","sourceInfo":"<ip>"}` — IPv4-mapped
//!   IPv6 reports `IP4` with the v4 address.
//! - Response: `{"id":"<event id>","action":"accept"|"reject"|`
//!   `"shadowReject","msg":"..."}` (`msg` optional). Non-JSON lines
//!   are skipped with a warning so plugins may print noise; an id
//!   mismatch or unknown action is an error. Lines over
//!   [`MAX_RESPONSE`] bytes are an error.
//! - Any failure (spawn/write/read/timeout/EOF/bad response) rejects
//!   the event `error: internal error`, logs at `error`, and kills the
//!   child; the next event lazily spawns a fresh one.
//! - A plugin command without spaces is a path: its mtime is compared
//!   before every request and the child restarts when it changes.
//!
//! Requests are served strictly in order from a bounded channel —
//! plugins may rely on ordering (strfry is serial too). Queue-full
//! callers wait: backpressure applies to their connection only, never
//! to other connections' events.

use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nosfry_core::event::Event;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};

/// Request queue depth; extra callers wait in place.
const QUEUE: usize = 1024;
/// Response lines longer than this are an error (strfry's max
/// record size).
const MAX_RESPONSE: u64 = 8192;
/// The NIP-01 message for any plugin failure.
const INTERNAL: &str = "error: internal error";

/// The plugin's verdict on one event.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Continue the normal path.
    Accept,
    /// `OK false <msg>`; an empty `msg` gets a NIP-01 default at the
    /// caller.
    Reject(String),
    /// `OK true <msg>` without storing or broadcasting.
    ShadowReject(String),
}

/// A clonable handle to the single policy task. The task exits once
/// every clone is dropped.
#[derive(Debug, Clone)]
pub(crate) struct WritePolicy {
    tx: mpsc::Sender<Request>,
}

impl WritePolicy {
    /// Start the policy task serving `plugin` (`/bin/sh -c <plugin>`,
    /// lazily spawned on the first event).
    pub(crate) fn new(plugin: &str, timeout: Duration) -> Self {
        let (tx, rx) = mpsc::channel(QUEUE);
        tokio::spawn(run(plugin.to_owned(), timeout, rx));
        Self { tx }
    }

    /// Ask the plugin about one verified event.
    pub(crate) async fn check(&self, event: &Event, source: IpAddr) -> Decision {
        let received_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let (reply, rx) = oneshot::channel();
        let request = Request {
            line: request_line(event.json(), received_at, source),
            id: event.id().to_string(),
            reply,
        };
        if self.tx.send(request).await.is_err() {
            return Decision::Reject(INTERNAL.to_owned());
        }
        rx.await
            .unwrap_or_else(|_| Decision::Reject(INTERNAL.to_owned()))
    }
}

/// One queued request: the serialized input line plus the event id
/// the response must echo.
struct Request {
    /// The `{"type":"new",...}` line (no trailing newline).
    line: String,
    /// `event.id` in hex.
    id: String,
    /// Where the decision goes.
    reply: oneshot::Sender<Decision>,
}

/// The running child; `kill_on_drop` kills it when dropped.
struct Plugin {
    /// Process handle; kept only so drop kills the child.
    _child: Child,
    /// Request pipe.
    stdin: ChildStdin,
    /// Response pipe.
    stdout: BufReader<ChildStdout>,
    /// The plugin file's mtime when spawned (path commands only).
    mtime: Option<SystemTime>,
}

/// The policy task: owns the child and serves requests in order.
async fn run(plugin: String, timeout: Duration, mut rx: mpsc::Receiver<Request>) {
    let path_cmd = !plugin.contains(' ');
    let mut running: Option<Plugin> = None;
    while let Some(request) = rx.recv().await {
        let decision = serve(&plugin, path_cmd, timeout, &mut running, &request).await;
        drop(request.reply.send(decision));
    }
}

/// Serve one request; errors are logged, kill the child, and become
/// `error: internal error` rejections.
async fn serve(
    plugin: &str,
    path_cmd: bool,
    timeout: Duration,
    running: &mut Option<Plugin>,
    request: &Request,
) -> Decision {
    match try_serve(plugin, path_cmd, timeout, running, request).await {
        Ok(decision) => decision,
        Err(error) => {
            tracing::error!(%error, plugin, "write policy plugin failed");
            // Dropping the child (kill_on_drop) kills it; the next
            // request spawns a fresh process.
            *running = None;
            Decision::Reject(INTERNAL.to_owned())
        }
    }
}

async fn try_serve(
    plugin: &str,
    path_cmd: bool,
    timeout: Duration,
    running: &mut Option<Plugin>,
    request: &Request,
) -> Result<Decision, PolicyError> {
    // Path commands reload whenever the script's mtime changes.
    if path_cmd && running.is_some() {
        let mtime = std::fs::metadata(plugin)?.modified()?;
        if running.as_ref().is_some_and(|p| p.mtime != Some(mtime)) {
            *running = None;
        }
    }
    let child = match running {
        Some(child) => child,
        slot @ None => slot.insert(spawn(plugin, path_cmd)?),
    };

    timed(timeout, async {
        child.stdin.write_all(request.line.as_bytes()).await?;
        child.stdin.write_all(b"\n").await?;
        child.stdin.flush().await
    })
    .await?;

    // One deadline covers the whole response, noise lines included, so
    // a chatty plugin cannot hold the queue past the timeout.
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut line = Vec::new();
        let read = tokio::time::timeout_at(
            deadline,
            (&mut child.stdout)
                .take(MAX_RESPONSE + 1)
                .read_until(b'\n', &mut line),
        )
        .await
        .map_err(|_| PolicyError::Timeout)??;
        if read == 0 {
            return Err(PolicyError::Eof);
        }
        if u64::try_from(line.len()).unwrap_or(u64::MAX) > MAX_RESPONSE {
            return Err(PolicyError::TooLong);
        }
        match parse_response(&line, &request.id) {
            Ok(decision) => return Ok(decision),
            Err(ResponseError::Noise) => {
                tracing::warn!(
                    "unparseable line from write policy plugin: {}",
                    String::from_utf8_lossy(&line)
                );
            }
            Err(ResponseError::Fatal(error)) => return Err(error),
        }
    }
}

/// Spawn `/bin/sh -c <plugin>` with stdio piped and stderr inherited
/// so plugin logs reach the relay log. `kill_on_drop` cleans the
/// process up on error paths and shutdown.
fn spawn(plugin: &str, path_cmd: bool) -> Result<Plugin, PolicyError> {
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg(plugin)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let stdin = child.stdin.take().ok_or(PolicyError::Stdio)?;
    let stdout = child.stdout.take().ok_or(PolicyError::Stdio)?;
    let mtime = if path_cmd {
        Some(std::fs::metadata(plugin)?.modified()?)
    } else {
        None
    };
    Ok(Plugin {
        _child: child,
        stdin,
        stdout: BufReader::new(stdout),
        mtime,
    })
}

/// A future bounded by the configured timeout.
async fn timed<T, E: Into<PolicyError>>(
    timeout: Duration,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, PolicyError> {
    tokio::time::timeout(timeout, future).await.map_or_else(
        |_| Err(PolicyError::Timeout),
        |result| result.map_err(Into::into),
    )
}

/// The strfry input line for one event: minified JSON, the event
/// embedded raw (no re-serialization).
fn request_line(event_json: &str, received_at: u64, source: IpAddr) -> String {
    let (source_type, source_info) = source_parts(source);
    format!(
        "{{\"type\":\"new\",\"event\":{event_json},\"receivedAt\":{received_at},\
         \"sourceType\":\"{source_type}\",\"sourceInfo\":\"{source_info}\"}}"
    )
}

/// `sourceType`/`sourceInfo` for an address: IPv4-mapped IPv6 counts
/// as `IP4` with the v4 address (strfry's `renderIP`).
fn source_parts(ip: IpAddr) -> (&'static str, String) {
    match ip {
        IpAddr::V4(_) => ("IP4", ip.to_string()),
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map_or_else(|| ("IP6", ip.to_string()), |v4| ("IP4", v4.to_string())),
    }
}

/// Parse one response line; `Noise` is skipped with a warning, `Fatal`
/// kills the plugin and rejects the event.
fn parse_response(line: &[u8], id: &str) -> Result<Decision, ResponseError> {
    let Ok(response) = serde_json::from_slice::<Value>(line) else {
        return Err(ResponseError::Noise);
    };
    if response.get("id").and_then(Value::as_str) != Some(id) {
        return Err(ResponseError::Fatal(PolicyError::IdMismatch));
    }
    let Some(action) = response.get("action").and_then(Value::as_str) else {
        return Err(ResponseError::Fatal(PolicyError::Malformed));
    };
    let msg = response
        .get("msg")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    match action {
        "accept" => Ok(Decision::Accept),
        "reject" => Ok(Decision::Reject(msg)),
        "shadowReject" => Ok(Decision::ShadowReject(msg)),
        other => Err(ResponseError::Fatal(PolicyError::UnknownAction(
            other.to_owned(),
        ))),
    }
}

/// Ways a plugin or its transport can fail a request.
#[derive(Debug, thiserror::Error)]
enum PolicyError {
    /// Child stdio or filesystem error.
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// Write or read exceeded the configured timeout.
    #[error("timed out")]
    Timeout,
    /// The child closed stdout before answering.
    #[error("plugin closed its response stream")]
    Eof,
    /// A response line over [`MAX_RESPONSE`] bytes.
    #[error("response line exceeds {MAX_RESPONSE} bytes")]
    TooLong,
    /// The response's `id` differs from the request's event id.
    #[error("response id mismatch")]
    IdMismatch,
    /// The response lacks a string `id` or `action`.
    #[error("malformed response")]
    Malformed,
    /// The response's `action` is none of the known values.
    #[error("unknown action: {0}")]
    UnknownAction(String),
    /// The child spawned without its stdin/stdout.
    #[error("child stdio unavailable")]
    Stdio,
}

/// Parse-failure severity.
#[derive(Debug)]
enum ResponseError {
    /// Not JSON: skipped with a warning (plugins may print noise).
    Noise,
    /// Everything else rejects the event and kills the child.
    Fatal(PolicyError),
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "unit tests may panic on failure")]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    /// A plausible event JSON (field order intentionally
    /// non-alphabetical to prove raw embedding).
    const EVENT_JSON: &str = concat!(
        "{\"id\":\"0000000000000000000000000000000000000000000000000000000000000001\",",
        "\"pubkey\":\"0000000000000000000000000000000000000000000000000000000000000002\",",
        "\"created_at\":1700000000,\"kind\":1,\"tags\":[],\"content\":\"hi\",",
        "\"sig\":\"0000000000000000000000000000000000000000000000000000000000000003\"}",
    );

    /// The request line's wire shape, for assertions.
    #[derive(serde::Deserialize)]
    struct Req<'a> {
        #[serde(rename = "type")]
        kind: String,
        #[serde(borrow)]
        event: &'a serde_json::value::RawValue,
        #[serde(rename = "receivedAt")]
        received_at: u64,
        #[serde(rename = "sourceType")]
        source_type: String,
        #[serde(rename = "sourceInfo")]
        source_info: String,
    }

    #[test]
    fn request_line_shape() {
        let line = request_line(
            EVENT_JSON,
            1_700_000_000,
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 7)),
        );
        let request: Req<'_> = serde_json::from_str(&line).unwrap();
        assert_eq!(request.kind, "new");
        assert_eq!(request.received_at, 1_700_000_000);
        assert_eq!(request.source_type, "IP4");
        assert_eq!(request.source_info, "192.0.2.7");
        // The event is embedded raw: byte-identical, not reserialized.
        assert_eq!(request.event.get(), EVENT_JSON);
    }

    #[test]
    fn source_parts_maps_v4_mapped_v6() {
        assert_eq!(
            source_parts("::ffff:192.0.2.9".parse().unwrap()),
            ("IP4", "192.0.2.9".to_owned())
        );
        assert_eq!(
            source_parts("2001:db8::1".parse().unwrap()),
            ("IP6", "2001:db8::1".to_owned())
        );
    }

    #[test]
    fn parse_response_decisions() {
        let id = "a".repeat(64);
        assert_eq!(
            parse_response(
                format!("{{\"id\":\"{id}\",\"action\":\"accept\"}}").as_bytes(),
                &id
            )
            .ok(),
            Some(Decision::Accept)
        );
        assert_eq!(
            parse_response(
                format!("{{\"id\":\"{id}\",\"action\":\"reject\",\"msg\":\"blocked: spam\"}}")
                    .as_bytes(),
                &id
            )
            .ok(),
            Some(Decision::Reject("blocked: spam".to_owned()))
        );
        assert_eq!(
            parse_response(
                format!("{{\"id\":\"{id}\",\"action\":\"reject\"}}").as_bytes(),
                &id
            )
            .ok(),
            Some(Decision::Reject(String::new()))
        );
        assert_eq!(
            parse_response(
                format!("{{\"id\":\"{id}\",\"action\":\"shadowReject\",\"msg\":\"m\"}}").as_bytes(),
                &id
            )
            .ok(),
            Some(Decision::ShadowReject("m".to_owned()))
        );
    }

    #[test]
    fn parse_response_failures() {
        let id = "a".repeat(64);
        // Non-JSON is noise, not an error.
        assert!(matches!(
            parse_response(b"this is plugin noise", &id),
            Err(ResponseError::Noise)
        ));
        assert!(matches!(
            parse_response(
                format!("{{\"id\":\"{}\",\"action\":\"accept\"}}", "b".repeat(64)).as_bytes(),
                &id
            ),
            Err(ResponseError::Fatal(PolicyError::IdMismatch))
        ));
        assert!(matches!(
            parse_response(
                format!("{{\"id\":\"{id}\",\"action\":\"bogus\"}}").as_bytes(),
                &id
            ),
            Err(ResponseError::Fatal(PolicyError::UnknownAction(_)))
        ));
        // Missing action is a malformed response, not noise.
        assert!(matches!(
            parse_response(format!("{{\"id\":\"{id}\"}}").as_bytes(), &id),
            Err(ResponseError::Fatal(PolicyError::Malformed))
        ));
    }
}
