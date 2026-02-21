mod cli;
mod cmd;
mod config_gen;
mod dispatch;
mod format;
mod hooks;
mod passphrase;
mod progress;
mod prompt;
mod table;

use clap::Parser;

use vger_core::config::{self, ResolvedRepo};

use crate::hooks::HookContext;

use cli::{Cli, Commands};
use config_gen::run_config_generate;
use dispatch::{dispatch_command, run_default_actions, warn_if_untrusted_rest};

fn main() {
    let cli = Cli::parse();

    // Initialize logging — auto-upgrade to info for daemon
    let filter = match cli.verbose {
        0 if matches!(&cli.command, Some(Commands::Daemon)) => "info",
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

    // Handle `daemon` subcommand early — operates on all repos at once
    if matches!(&cli.command, Some(Commands::Daemon)) {
        let runtime = vger_core::app::RuntimeConfig {
            source,
            repos: all_repos,
        };
        let schedule = runtime.schedule();
        let repo_refs: Vec<&ResolvedRepo> = runtime.repos.iter().collect();
        if let Err(e) = cmd::daemon::run_daemon(&repo_refs, &schedule) {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        return;
    }

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

        let result = match &cli.command {
            Some(cmd) => {
                let run_action = || dispatch_command(cmd, cfg, label, &repo.sources);
                if has_hooks {
                    let mut ctx = HookContext {
                        command: cmd.name().to_string(),
                        repository: cfg.repository.url.clone(),
                        label: repo.label.clone(),
                        error: None,
                        source_label: None,
                        source_paths: None,
                    };
                    hooks::run_with_hooks(
                        &repo.global_hooks,
                        &repo.repo_hooks,
                        &mut ctx,
                        run_action,
                    )
                } else {
                    run_action()
                }
            }
            None => run_default_actions(
                cfg,
                label,
                &repo.sources,
                &repo.global_hooks,
                &repo.repo_hooks,
                &repo.label,
                None,
            ),
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
