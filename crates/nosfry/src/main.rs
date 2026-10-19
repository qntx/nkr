//! `nosfry` binary: the relay entry point.
#![allow(
    unused_crate_dependencies,
    reason = "the binary uses only the thin-cli subset of the package's dependencies"
)]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use nosfry::config::Config;

#[derive(Parser)]
#[command(name = "nosfry", version, about = "a Nostr relay")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the relay.
    Relay {
        /// Config file path (env `NOSFRY_CONFIG`).
        #[arg(long, env = "NOSFRY_CONFIG", default_value = "nosfry.toml")]
        config: PathBuf,
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
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
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

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Cmd::Relay { config } => {
            let config = Config::load(&config)?;
            let addr = (config.relay.bind, config.relay.port);
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|err| format!("bind {addr:?}: {err}"))?;
            tracing::info!(?addr, "listening");
            nosfry::serve(config, listener, shutdown_signal()).await?;
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(%err, "fatal");
            ExitCode::FAILURE
        }
    }
}
