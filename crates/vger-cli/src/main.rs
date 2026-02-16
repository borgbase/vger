mod cli;
mod cmd;
mod config_gen;
mod dispatch;
mod format;
mod passphrase;
mod progress;
mod table;

use clap::Parser;

use vger_core::config::{self, ResolvedRepo};
use vger_core::hooks::{self, HookContext};

use cli::{Cli, Commands};
use config_gen::run_config_generate;
use dispatch::{dispatch_command, run_default_actions, warn_if_untrusted_rest};

fn main() {
    let cli = Cli::parse();

    // Initialize logging
    let filter = match cli.verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    // Handle `config` subcommand early — no config file needed
    if let Some(Commands::Config { dest }) = &cli.command {
        if let Err(e) = run_config_generate(dest.as_deref()) {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        return;
    }

    // Resolve config file
    let source = match config::resolve_config_path(cli.config.as_deref()) {
        Some(s) => s,
        None => {
            eprintln!("Error: no configuration file found.");
            eprintln!("Searched:");
            for (path, level) in config::default_config_search_paths() {
                eprintln!("  {} ({})", path.display(), level);
            }
            eprintln!();
            eprintln!("Run `vger config` to generate a starter config file.");
            std::process::exit(1);
        }
    };

    tracing::info!("Using config: {source}");

    let all_repos = match config::load_and_resolve(source.path()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    // Filter by --repo if provided on the subcommand
    let repo_selector = cli.command.as_ref().and_then(|cmd| cmd.repo());
    let repos: Vec<&ResolvedRepo> = if let Some(selector) = repo_selector {
        match config::select_repo(&all_repos, selector) {
            Some(r) => vec![r],
            None => {
                eprintln!("Error: no repository matching '{selector}'");
                eprintln!("Available repositories:");
                for r in &all_repos {
                    let label = r.label.as_deref().unwrap_or("-");
                    eprintln!("  {label:12} {}", r.config.repository.url);
                }
                std::process::exit(1);
            }
        }
    } else {
        all_repos.iter().collect()
    };

    let multi = repos.len() > 1;
    let mut had_error = false;

    for repo in &repos {
        if multi {
            let name = repo.label.as_deref().unwrap_or(&repo.config.repository.url);
            eprintln!("--- Repository: {name} ---");
        }

        let label = repo.label.as_deref();
        let cfg = &repo.config;
        warn_if_untrusted_rest(cfg, label);

        let has_hooks = !repo.global_hooks.is_empty() || !repo.repo_hooks.is_empty();

        let cmd_name = match &cli.command {
            Some(cmd) => cmd.name(),
            None => "run",
        };

        let run_action = || -> Result<(), Box<dyn std::error::Error>> {
            match &cli.command {
                Some(cmd) => dispatch_command(cmd, cfg, label, &repo.sources),
                None => run_default_actions(cfg, label, &repo.sources),
            }
        };

        let result = if has_hooks {
            let mut ctx = HookContext {
                command: cmd_name.to_string(),
                repository: cfg.repository.url.clone(),
                label: repo.label.clone(),
                error: None,
                source_label: None,
                source_paths: None,
            };
            hooks::run_with_hooks(&repo.global_hooks, &repo.repo_hooks, &mut ctx, run_action)
        } else {
            run_action()
        };

        if let Err(e) = result {
            eprintln!("Error: {e}");
            had_error = true;
            if multi {
                // Continue to next repo
                continue;
            } else {
                std::process::exit(1);
            }
        }
    }

    if had_error {
        std::process::exit(1);
    }
}
