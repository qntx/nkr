//! Worker TOML configuration: defaults, unknown keys, and the
//! limits the session itself enforces.

#![allow(
    unused_crate_dependencies,
    reason = "each test binary uses a subset of the workspace dependencies"
)]
#![allow(
    clippy::tests_outside_test_module,
    reason = "each file is a single integration test"
)]
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::assert_is_empty,
    reason = "tests may panic on failure"
)]

use nosfry_worker::Config;

#[test]
fn defaults() {
    let config = Config::parse("").unwrap();
    assert_eq!(config.relay.max_websocket_payload_size, 131_072);
    assert_eq!(config.relay.max_connections, 5_000);
    assert_eq!(config.relay.max_connections_per_ip, 128);
    assert_eq!(config.worker.max_scan_rows, 100_000);
    assert!(config.relay.service_url.is_empty());
    assert!(config.worker.location_hint.is_empty());
}

#[test]
fn unknown_keys_are_rejected() {
    // A native-only key must fail rather than be ignored.
    assert!(Config::parse("[relay]\nbind = \"0.0.0.0\"\n").is_err());
    assert!(Config::parse("[db]\n").is_err());
    assert!(Config::parse("[worker]\nthreads = 4\n").is_err());
}

#[test]
fn limits_disable_negentropy_but_not_count() {
    let config = Config::parse("").unwrap();
    let limits = config.limits().unwrap();
    assert_eq!(
        limits.req.max_filter_limit_count,
        nosfry_core::limits::ReqLimits::default().max_filter_limit_count
    );
    assert_eq!(limits.req.max_sync_events, 0);
    assert_eq!(
        limits.req.max_filter_limit,
        nosfry_core::limits::ReqLimits::default().max_filter_limit
    );
}

#[test]
fn max_scan_rows_must_be_positive() {
    // A zero budget would make every REQ and COUNT return nothing.
    let err = Config::parse("[worker]\nmax_scan_rows = 0\n").unwrap_err();
    assert!(err.to_string().contains("max_scan_rows"), "got: {err}");
    assert!(Config::parse("[worker]\nmax_scan_rows = 1\n").is_ok());
}

#[test]
fn service_url_is_validated() {
    assert!(Config::parse("[relay]\nservice_url = \"http://nope\"\n").is_err());
    let config = Config::parse("[relay]\nservice_url = \"wss://relay.example.com\"\n").unwrap();
    assert!(config.limits().unwrap().auth.enabled());
    // An empty service URL means no AUTH.
    assert!(!Config::parse("").unwrap().limits().unwrap().auth.enabled());
}
