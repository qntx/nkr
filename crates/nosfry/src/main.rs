//! `nosfry` binary: the relay entry point and administration CLI.
#![allow(
    unused_crate_dependencies,
    reason = "the binary uses only the thin-cli subset of the package's dependencies"
)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use nosfry::config::Config;
use nosfry_lmdb::Store;

#[derive(Parser)]
#[command(name = "nosfry", version, about = "a Nostr relay")]
struct Cli {
    /// Config file path (env `NOSFRY_CONFIG`).
    #[arg(
        long,
        env = "NOSFRY_CONFIG",
        default_value = "nosfry.toml",
        global = true
    )]
    config: PathBuf,
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the relay.
    Relay,
    /// Export all events as JSON lines, oldest first.
    Export {
        /// Only events at or after this unix timestamp.
        #[arg(long, default_value_t = 0)]
        since: u64,
        /// Only events at or before this unix timestamp.
        #[arg(long, default_value_t = u64::MAX)]
        until: u64,
        /// Newest first instead.
        #[arg(long)]
        reverse: bool,
    },
    /// Import event JSON lines from stdin.
    Import {
        /// Log each rejected line with its reason.
        #[arg(long)]
        show_rejected: bool,
    },
    /// Emit stored events matching a REQ filter (JSON), newest first.
    Scan {
        /// Print only the match count.
        #[arg(long)]
        count: bool,
        /// Filter JSON, e.g. '{"kinds":[1]}'.
        filter: String,
    },
    /// Write a compacted copy of the database to a new file.
    Compact {
        /// Destination file; must not exist.
        dest: PathBuf,
    },
}

fn init_tracing() {
    use tracing_subscriber::fmt::format::FmtSpan;
    let env = std::env::var("RUST_LOG_JSON").is_ok_and(|v| v == "1" || v == "true");
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    if env {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_span_events(FmtSpan::NONE)
            .with_writer(std::io::stderr)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    }
}

/// Wait for SIGINT or SIGTERM.
#[cfg(unix)]
async fn shutdown_signal() {
    let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(term) => term,
        Err(err) => {
            tracing::warn!(%err, "SIGTERM handler unavailable");
            tokio::signal::ctrl_c().await.ok();
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

/// Wait for SIGINT (ctrl-c).
#[cfg(not(unix))]
async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
}

/// Open an existing database for a read-only command.
fn open_existing(config: &Config) -> Result<Store, Box<dyn std::error::Error>> {
    if !config.db.path.join("data.mdb").exists() {
        return Err(format!("no database at {}", config.db.path.display()).into());
    }
    Ok(Store::open(&config.db.path, &config.store_options())?)
}

/// A locked, buffered stdout for JSON-lines output.
fn stdout() -> std::io::BufWriter<std::io::StdoutLock<'static>> {
    std::io::BufWriter::new(std::io::stdout().lock())
}

/// The io error at the bottom of `err`'s source chain is a broken
/// pipe (the reader went away, e.g. `nosfry export | head`).
fn is_broken_pipe(mut err: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if let Some(io) = err.downcast_ref::<std::io::Error>()
            && io.kind() == std::io::ErrorKind::BrokenPipe
        {
            return true;
        }
        match err.source() {
            Some(source) => err = source,
            None => return false,
        }
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load(&cli.config)?;
    match cli.command {
        Cmd::Relay => relay(config).await,
        Cmd::Export {
            since,
            until,
            reverse,
        } => export(&config, since, until, reverse),
        Cmd::Import { show_rejected } => import(&config, show_rejected),
        Cmd::Scan { count, filter } => scan(&config, &filter, count),
        Cmd::Compact { dest } => compact(&config, &dest),
    }
}

async fn relay(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let addr = (config.relay.bind, config.relay.port);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|err| format!("bind {addr:?}: {err}"))?;
    tracing::info!(?addr, "listening");
    nosfry::serve(config, listener, shutdown_signal()).await?;
    Ok(())
}

fn export(
    config: &Config,
    since: u64,
    until: u64,
    reverse: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = open_existing(config)?;
    let mut out = stdout();
    let count = nosfry::admin::export(&store, since, until, reverse, &mut out)?;
    out.flush()?;
    tracing::info!(count, "export done");
    Ok(())
}

fn import(config: &Config, show_rejected: bool) -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::open(&config.db.path, &config.store_options())?;
    let stdin = std::io::stdin();
    nosfry::admin::import(
        &store,
        &config.events,
        config.relay.max_websocket_payload_size,
        stdin.lock(),
        show_rejected,
    )?;
    Ok(())
}

fn scan(config: &Config, filter: &str, count: bool) -> Result<(), Box<dyn std::error::Error>> {
    let filter = nosfry_core::filter::Filter::parse(filter)?;
    let store = open_existing(config)?;
    let mut out = stdout();
    let matched = nosfry::admin::scan(&store, filter, count, &mut out)?;
    if count {
        writeln!(out, "{matched}")?;
    }
    out.flush()?;
    Ok(())
}

fn compact(config: &Config, dest: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let store = open_existing(config)?;
    nosfry::admin::compact(&store, dest)?;
    tracing::info!(dest = %dest.display(), "compact done");
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) if is_broken_pipe(err.as_ref()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(%err, "fatal");
            ExitCode::FAILURE
        }
    }
}
