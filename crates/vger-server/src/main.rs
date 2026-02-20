mod config;
mod error;
mod handlers;
mod state;

use std::time::Duration;

use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;

use crate::config::{parse_size, ServerSection};
use crate::state::{write_unpoisoned, AppState};

#[derive(Parser)]
#[command(name = "vger-server", version, about = "vger backup server")]
struct Cli {
    /// Address to listen on
    #[arg(short, long, default_value = "127.0.0.1:8585")]
    listen: String,

    /// Root directory where repositories are stored
    #[arg(short, long, default_value = "/var/lib/vger")]
    data_dir: String,

    /// Reject DELETE and overwrite operations on pack files
    #[arg(long, default_value_t = false)]
    append_only: bool,

    /// Log output format: "json" or "pretty"
    #[arg(long, default_value = "pretty")]
    log_format: String,

    /// Per-repo storage quota (e.g. "500M", "10G", plain bytes). 0 = unlimited.
    #[arg(long, default_value = "0", value_parser = parse_size)]
    quota: u64,

    /// Lock TTL in seconds
    #[arg(long, default_value_t = 3600)]
    lock_ttl_seconds: u64,

    /// Maximum number of blocking threads for file I/O (minimum 1)
    #[arg(long, default_value_t = 6, value_parser = parse_blocking_threads)]
    max_blocking_threads: usize,
}

fn parse_blocking_threads(s: &str) -> Result<usize, String> {
    let n: usize = s.parse().map_err(|e| format!("{e}"))?;
    if n == 0 {
        return Err("value must be at least 1".into());
    }
    Ok(n)
}

fn main() {
    let cli = Cli::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(cli.max_blocking_threads)
        .build()
        .unwrap_or_else(|e| {
            eprintln!("Error: failed to build tokio runtime: {e}");
            std::process::exit(1);
        });

    runtime.block_on(async_main(cli));
}

async fn async_main(cli: Cli) {
    // Read token from environment
    let token = std::env::var("VGER_TOKEN").unwrap_or_default();
    if token.is_empty() {
        eprintln!("Error: VGER_TOKEN environment variable must be set");
        std::process::exit(1);
    }

    let config = ServerSection {
        listen: cli.listen,
        data_dir: cli.data_dir,
        token,
        append_only: cli.append_only,
        log_format: cli.log_format,
        quota_bytes: cli.quota,
        lock_ttl_seconds: cli.lock_ttl_seconds,
    };

    // Initialize tracing
    match config.log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt().json().init();
        }
        _ => {
            tracing_subscriber::fmt().init();
        }
    }

    // Ensure data directory exists
    std::fs::create_dir_all(&config.data_dir).unwrap_or_else(|e| {
        eprintln!(
            "Error: cannot create data directory '{}': {e}",
            config.data_dir
        );
        std::process::exit(1);
    });

    let listen_addr = config.listen.clone();
    let state = AppState::new(config);

    // Spawn lock cleanup background task
    let cleanup_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            cleanup_expired_locks(&cleanup_state);
        }
    });

    let app = handlers::router(state);

    info!("vger-server listening on {listen_addr}");
    let listener = TcpListener::bind(&listen_addr).await.unwrap_or_else(|e| {
        eprintln!("Error: cannot bind to {listen_addr}: {e}");
        std::process::exit(1);
    });
    axum::serve(listener, app).await.unwrap();
}

fn cleanup_expired_locks(state: &AppState) {
    let mut locks = write_unpoisoned(&state.inner.locks, "locks");
    for (_repo, repo_locks) in locks.iter_mut() {
        repo_locks.retain(|_id, info| !info.is_expired());
    }
    locks.retain(|_, repo_locks| !repo_locks.is_empty());
}
