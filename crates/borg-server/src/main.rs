mod config;
mod error;
mod handlers;
mod state;

use std::time::Duration;

use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;

use crate::config::ServerConfig;
use crate::state::AppState;

#[derive(Parser)]
#[command(name = "borg-rs-server", version, about = "borg-rs backup server")]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "borg-server.toml")]
    config: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Load config
    let config_str = std::fs::read_to_string(&cli.config).unwrap_or_else(|e| {
        eprintln!("Error: cannot read config file '{}': {e}", cli.config);
        std::process::exit(1);
    });
    let config: ServerConfig = toml::from_str(&config_str).unwrap_or_else(|e| {
        eprintln!("Error: invalid config file '{}': {e}", cli.config);
        std::process::exit(1);
    });

    if config.server.token.is_empty() {
        eprintln!("Error: server.token must be set in config");
        std::process::exit(1);
    }

    // Initialize tracing
    match config.server.log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt().json().init();
        }
        _ => {
            tracing_subscriber::fmt().init();
        }
    }

    // Ensure data directory exists
    std::fs::create_dir_all(&config.server.data_dir).unwrap_or_else(|e| {
        eprintln!(
            "Error: cannot create data directory '{}': {e}",
            config.server.data_dir
        );
        std::process::exit(1);
    });

    let listen_addr = config.server.listen.clone();
    let state = AppState::new(config.server);

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

    info!("borg-rs-server listening on {listen_addr}");
    let listener = TcpListener::bind(&listen_addr).await.unwrap_or_else(|e| {
        eprintln!("Error: cannot bind to {listen_addr}: {e}");
        std::process::exit(1);
    });
    axum::serve(listener, app).await.unwrap();
}

fn cleanup_expired_locks(state: &AppState) {
    let mut locks = state.inner.locks.write().unwrap();
    for (_repo, repo_locks) in locks.iter_mut() {
        repo_locks.retain(|_id, info| !info.is_expired());
    }
    locks.retain(|_, repo_locks| !repo_locks.is_empty());
}
