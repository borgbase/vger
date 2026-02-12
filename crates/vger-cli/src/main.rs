use clap::{Parser, Subcommand};
use comfy_table::{presets::UTF8_FULL_CONDENSED, Table};
use rand::RngCore;

use vger_core::commands;
use vger_core::compress::Compression;
use vger_core::config::{self, EncryptionModeConfig, ResolvedRepo, SourceEntry, VgerConfig};
use vger_core::hooks::{self, HookContext};

#[derive(Parser)]
#[command(
    name = "vger",
    version,
    about = "Fast, encrypted, deduplicated backups",
    after_help = "\
Configuration file lookup order:
  1. --config <path>             (explicit flag)
  2. $VGER_CONFIG                (environment variable)
  3. ./vger.yaml                 (project)
  4. $XDG_CONFIG_HOME/vger/config.yaml or ~/.config/vger/config.yaml (user)
  5. /etc/vger/config.yaml       (system)

Environment variables:
  VGER_CONFIG       Path to configuration file (overrides default search)
  VGER_PASSPHRASE   Repository passphrase (skips interactive prompt)"
)]
struct Cli {
    /// Path to configuration file (overrides VGER_CONFIG and default search)
    #[arg(short, long)]
    config: Option<String>,

    /// Select repository by label or path (operates on all repos if omitted)
    #[arg(short = 'R', long = "repo", global = true)]
    repo: Option<String>,

    /// Verbosity level (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new repository
    Init,

    /// Back up files to a new snapshot
    Backup {
        /// User-provided annotation for the snapshot
        #[arg(short = 'l', long)]
        label: Option<String>,

        /// Compression algorithm override (lz4, zstd, none)
        #[arg(long)]
        compression: Option<String>,

        /// Filter which configured sources to back up (by label)
        #[arg(short = 'S', long = "source")]
        source: Vec<String>,

        /// Ad-hoc paths to back up (grouped into a single snapshot)
        paths: Vec<String>,
    },

    /// List snapshots or snapshot contents
    List {
        /// Show contents of a specific snapshot
        #[arg(long)]
        snapshot: Option<String>,

        /// Filter displayed snapshots by source label
        #[arg(short = 'S', long = "source")]
        source: Vec<String>,

        /// Show only the N most recent snapshots
        #[arg(long)]
        last: Option<usize>,
    },

    /// Extract files from a snapshot
    Extract {
        /// Snapshot to extract from
        #[arg(long)]
        snapshot: String,

        /// Destination directory
        #[arg(long, default_value = ".")]
        dest: String,

        /// Only extract paths matching this glob pattern
        #[arg(long)]
        pattern: Option<String>,
    },

    /// Delete a specific snapshot
    Delete {
        /// Snapshot name to delete
        #[arg(long)]
        snapshot: String,

        /// Only show what would be deleted, don't actually delete
        #[arg(short = 'n', long)]
        dry_run: bool,
    },

    /// Prune snapshots according to retention policy
    Prune {
        /// Only show what would be pruned, don't actually prune
        #[arg(short = 'n', long)]
        dry_run: bool,

        /// Show detailed list of kept/pruned snapshots with reasons
        #[arg(long)]
        list: bool,

        /// Apply retention only to snapshots matching these source labels
        #[arg(short = 'S', long = "source")]
        source: Vec<String>,
    },

    /// Verify repository integrity
    Check {
        /// Read and verify all data chunks (slow but thorough)
        #[arg(long)]
        verify_data: bool,
    },

    /// Generate a minimal configuration file
    Config {
        /// Destination path for the config file (default: ./vger.yaml)
        #[arg(short, long, default_value = "vger.yaml")]
        dest: String,
    },

    /// Browse snapshots via a local WebDAV server
    Mount {
        /// Serve a single snapshot (omit for all snapshots)
        #[arg(long)]
        snapshot: Option<String>,

        /// Expose only snapshots matching these source labels
        #[arg(short = 'S', long = "source")]
        source: Vec<String>,

        /// Listen address (default: 127.0.0.1:8080)
        #[arg(long, default_value = "127.0.0.1:8080")]
        address: String,

        /// LRU chunk cache size in entries (default: 256)
        #[arg(long, default_value = "256")]
        cache_size: usize,
    },

    /// Free repository space by compacting pack files
    Compact {
        /// Minimum percentage of unused space to trigger repack (default: 10)
        #[arg(long, default_value = "10")]
        threshold: f64,

        /// Maximum total bytes to repack (e.g. 500M, 2G)
        #[arg(long)]
        max_repack_size: Option<String>,

        /// Only show what would be compacted, don't actually do it
        #[arg(short = 'n', long)]
        dry_run: bool,
    },
}

fn command_name(cmd: &Commands) -> &'static str {
    match cmd {
        Commands::Init => "init",
        Commands::Backup { .. } => "backup",
        Commands::List { .. } => "list",
        Commands::Extract { .. } => "extract",
        Commands::Delete { .. } => "delete",
        Commands::Prune { .. } => "prune",
        Commands::Check { .. } => "check",
        Commands::Mount { .. } => "mount",
        Commands::Compact { .. } => "compact",
        Commands::Config { .. } => "config",
    }
}

/// Generate an 8-character hex snapshot name (4 random bytes).
fn generate_snapshot_name() -> String {
    let mut buf = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

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
        if let Err(e) = run_config_generate(dest) {
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

    // Filter by --repo if provided
    let repos: Vec<&ResolvedRepo> = if let Some(ref selector) = cli.repo {
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

        let has_hooks = !repo.global_hooks.is_empty() || !repo.repo_hooks.is_empty();

        let cmd_name = match &cli.command {
            Some(cmd) => command_name(cmd),
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

enum StepResult {
    Ok,
    Failed(String),
    Skipped(&'static str),
}

fn run_default_actions(
    cfg: &VgerConfig,
    label: Option<&str>,
    sources: &[SourceEntry],
) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let mut steps: Vec<(&str, StepResult)> = Vec::new();

    // 1. Backup
    eprintln!("==> Starting backup");
    let backup_ok = match run_backup(cfg, label, None, None, vec![], sources, &[]) {
        Ok(()) => {
            steps.push(("backup", StepResult::Ok));
            true
        }
        Err(e) => {
            eprintln!("Error: {e}");
            steps.push(("backup", StepResult::Failed(e.to_string())));
            false
        }
    };

    // 2. Prune — skip if no retention rules configured
    let has_retention = cfg.retention.has_any_rule()
        || sources
            .iter()
            .any(|s| s.retention.as_ref().is_some_and(|r| r.has_any_rule()));

    if !has_retention {
        steps.push(("prune", StepResult::Skipped("no retention rules")));
    } else if !backup_ok {
        steps.push(("prune", StepResult::Skipped("backup failed")));
    } else {
        eprintln!("==> Starting prune");
        match run_prune(cfg, label, false, false, sources, &[]) {
            Ok(()) => steps.push(("prune", StepResult::Ok)),
            Err(e) => {
                eprintln!("Error: {e}");
                steps.push(("prune", StepResult::Failed(e.to_string())));
            }
        }
    }

    // 3. Compact
    if !backup_ok {
        steps.push(("compact", StepResult::Skipped("backup failed")));
    } else {
        eprintln!("==> Starting compact");
        match run_compact(cfg, label, 10.0, None, false) {
            Ok(()) => steps.push(("compact", StepResult::Ok)),
            Err(e) => {
                eprintln!("Error: {e}");
                steps.push(("compact", StepResult::Failed(e.to_string())));
            }
        }
    }

    // 4. Check (metadata-only)
    eprintln!("==> Starting check");
    match run_check(cfg, label, false) {
        Ok(()) => steps.push(("check", StepResult::Ok)),
        Err(e) => {
            eprintln!("Error: {e}");
            steps.push(("check", StepResult::Failed(e.to_string())));
        }
    }

    // Print summary
    let elapsed = start.elapsed();
    let mut had_failure = false;

    eprintln!();
    eprintln!("=== Summary ===");
    for (name, result) in &steps {
        match result {
            StepResult::Ok => eprintln!("  {name:<12} ok"),
            StepResult::Failed(e) => {
                had_failure = true;
                eprintln!("  {name:<12} FAILED: {e}");
            }
            StepResult::Skipped(reason) => eprintln!("  {name:<12} skipped ({reason})"),
        }
    }

    let secs = elapsed.as_secs();
    let mins = secs / 60;
    let secs = secs % 60;
    if mins > 0 {
        eprintln!("  Duration:    {mins}m {secs:02}s");
    } else {
        eprintln!("  Duration:    {secs}s");
    }

    if had_failure {
        Err("one or more steps failed".into())
    } else {
        Ok(())
    }
}

fn run_config_generate(dest: &str) -> Result<(), Box<dyn std::error::Error>> {
    let path = std::path::Path::new(dest);

    if path.exists() {
        return Err(format!("file already exists: {dest}").into());
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    std::fs::write(path, config::minimal_config_template())?;
    println!("Config written to: {dest}");
    println!("Edit it to set your repository path and source directories.");
    Ok(())
}

fn with_repo_passphrase<T>(
    config: &VgerConfig,
    label: Option<&str>,
    action: impl FnOnce(Option<&str>) -> Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config, label)?;
    action(passphrase.as_deref())
}

fn dispatch_command(
    command: &Commands,
    cfg: &VgerConfig,
    label: Option<&str>,
    sources: &[SourceEntry],
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Commands::Init => run_init(cfg, label),
        Commands::Backup {
            label: user_label,
            compression,
            source,
            paths,
        } => run_backup(
            cfg,
            label,
            user_label.clone(),
            compression.clone(),
            paths.clone(),
            sources,
            source,
        ),
        Commands::List {
            snapshot,
            source,
            last,
        } => run_list(cfg, label, snapshot.clone(), source, *last),
        Commands::Extract {
            snapshot,
            dest,
            pattern,
        } => run_extract(cfg, label, snapshot.clone(), dest.clone(), pattern.clone()),
        Commands::Delete { snapshot, dry_run } => {
            run_delete(cfg, label, snapshot.clone(), *dry_run)
        }
        Commands::Prune {
            dry_run,
            list,
            source,
        } => run_prune(cfg, label, *dry_run, *list, sources, source),
        Commands::Check { verify_data } => run_check(cfg, label, *verify_data),
        Commands::Mount {
            snapshot,
            source,
            address,
            cache_size,
        } => run_mount(
            cfg,
            label,
            snapshot.clone(),
            address.clone(),
            *cache_size,
            source,
        ),
        Commands::Compact {
            threshold,
            max_repack_size,
            dry_run,
        } => run_compact(cfg, label, *threshold, max_repack_size.clone(), *dry_run),
        Commands::Config { .. } => unreachable!(),
    }
}

fn get_passphrase(
    config: &VgerConfig,
    label: Option<&str>,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if config.encryption.mode == EncryptionModeConfig::None {
        return Ok(None);
    }

    if let Some(pass) = configured_passphrase(config)? {
        return Ok(Some(pass));
    }

    // Interactive prompt
    let prompt = match label {
        Some(l) => format!("Enter passphrase for '{l}': "),
        None => "Enter passphrase: ".to_string(),
    };
    let pass = rpassword::prompt_password(prompt)?;
    Ok(Some(pass))
}

fn configured_passphrase(
    config: &VgerConfig,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if let Some(ref p) = config.encryption.passphrase {
        return Ok(Some(p.clone()));
    }
    if let Some(ref cmd) = config.encryption.passcommand {
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "passcommand failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        let pass = String::from_utf8(output.stdout)?.trim().to_string();
        return Ok(Some(pass));
    }
    if let Ok(pass) = std::env::var("VGER_PASSPHRASE") {
        if !pass.is_empty() {
            return Ok(Some(pass));
        }
    }
    Ok(None)
}

fn get_init_passphrase(
    config: &VgerConfig,
    label: Option<&str>,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if config.encryption.mode == EncryptionModeConfig::None {
        return Ok(None);
    }
    if let Some(pass) = configured_passphrase(config)? {
        return Ok(Some(pass));
    }

    let suffix = label.map(|l| format!(" for '{l}'")).unwrap_or_default();
    let p1 = rpassword::prompt_password(format!("Enter new passphrase{suffix}: "))?;
    let p2 = rpassword::prompt_password(format!("Confirm passphrase{suffix}: "))?;
    if p1 != p2 {
        return Err("passphrases do not match".into());
    }
    Ok(Some(p1))
}

fn run_init(config: &VgerConfig, label: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_init_passphrase(config, label)?;

    commands::init::run(config, passphrase.as_deref())?;
    println!("Repository initialized at: {}", config.repository.url);
    Ok(())
}

fn run_backup(
    config: &VgerConfig,
    label: Option<&str>,
    user_label: Option<String>,
    compression_override: Option<String>,
    paths: Vec<String>,
    sources: &[SourceEntry],
    source_filter: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    with_repo_passphrase(config, label, |passphrase| {
        let user_label_str = user_label.as_deref().unwrap_or("");

        // Determine compression
        let compression = if let Some(ref algo) = compression_override {
            Compression::from_config(algo, config.compression.zstd_level)?
        } else {
            Compression::from_algorithm(config.compression.algorithm, config.compression.zstd_level)
        };

        if !source_filter.is_empty() && !paths.is_empty() {
            return Err("cannot combine --source with ad-hoc paths".into());
        }

        if !paths.is_empty() {
            // Ad-hoc paths mode: group all paths into a single snapshot
            let expanded: Vec<String> = paths.iter().map(|p| config::expand_tilde(p)).collect();
            let source_label = if expanded.len() == 1 {
                config::label_from_path(&expanded[0])
            } else {
                "adhoc".to_string()
            };
            let name = generate_snapshot_name();

            let stats = commands::backup::run(
                config,
                commands::backup::BackupRequest {
                    snapshot_name: &name,
                    passphrase,
                    source_paths: &expanded,
                    source_label: &source_label,
                    exclude_patterns: &config.exclude_patterns,
                    compression,
                    label: user_label_str,
                },
            )?;

            println!("Snapshot created: {name}");
            if !user_label_str.is_empty() {
                println!("  Label: {user_label_str}");
            }
            let paths_display = expanded.join(", ");
            println!("  Source: {paths_display} (label: {source_label})");
            println!(
                "  Files: {}, Original: {}, Compressed: {}, Deduplicated: {}",
                stats.nfiles,
                format_bytes(stats.original_size),
                format_bytes(stats.compressed_size),
                format_bytes(stats.deduplicated_size),
            );
        } else if sources.is_empty() {
            return Err("no sources configured and no paths specified".into());
        } else {
            // Filter sources by --source if specified
            let active_sources: Vec<&SourceEntry> = if source_filter.is_empty() {
                sources.iter().collect()
            } else {
                config::select_sources(sources, source_filter)
                    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?
            };

            for source in &active_sources {
                let name = generate_snapshot_name();

                let has_source_hooks = !source.hooks.before.is_empty()
                    || !source.hooks.after.is_empty()
                    || !source.hooks.failed.is_empty()
                    || !source.hooks.finally.is_empty();

                let backup_action = || -> Result<(), Box<dyn std::error::Error>> {
                    let stats = commands::backup::run(
                        config,
                        commands::backup::BackupRequest {
                            snapshot_name: &name,
                            passphrase,
                            source_paths: &source.paths,
                            source_label: &source.label,
                            exclude_patterns: &source.exclude,
                            compression,
                            label: user_label_str,
                        },
                    )?;

                    println!("Snapshot created: {name}");
                    if !user_label_str.is_empty() {
                        println!("  Label: {user_label_str}");
                    }
                    let paths_display = source.paths.join(", ");
                    println!("  Source: {paths_display} (label: {})", source.label);
                    println!(
                        "  Files: {}, Original: {}, Compressed: {}, Deduplicated: {}",
                        stats.nfiles,
                        format_bytes(stats.original_size),
                        format_bytes(stats.compressed_size),
                        format_bytes(stats.deduplicated_size),
                    );
                    Ok(())
                };

                if has_source_hooks {
                    let mut ctx = HookContext {
                        command: "backup".to_string(),
                        repository: config.repository.url.clone(),
                        label: label.map(|s| s.to_string()),
                        error: None,
                        source_label: Some(source.label.clone()),
                        source_paths: Some(source.paths.clone()),
                    };
                    hooks::run_source_hooks(&source.hooks, &mut ctx, backup_action)?;
                } else {
                    backup_action()?;
                }
            }
        }

        Ok(())
    })
}

fn run_list(
    config: &VgerConfig,
    label: Option<&str>,
    snapshot_name: Option<String>,
    source_filter: &[String],
    last: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    if snapshot_name.is_some() && !source_filter.is_empty() {
        return Err("cannot combine --source with --snapshot".into());
    }

    let result = with_repo_passphrase(config, label, |passphrase| {
        commands::list::run(config, passphrase, snapshot_name.as_deref())
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    match result {
        commands::list::ListResult::Snapshots(mut snapshots) => {
            // Filter by source label if requested
            if !source_filter.is_empty() {
                snapshots.retain(|e| source_filter.iter().any(|f| f == &e.source_label));
            }

            // Truncate to last N entries
            if let Some(n) = last {
                let len = snapshots.len();
                if n < len {
                    snapshots.drain(..len - n);
                }
            }
            if snapshots.is_empty() {
                println!("No snapshots found.");
                return Ok(());
            }

            let mut table = Table::new();
            table.load_preset(UTF8_FULL_CONDENSED);
            table.set_header(vec!["ID", "Source", "Label", "Date"]);

            for entry in &snapshots {
                table.add_row(vec![
                    entry.name.clone(),
                    if entry.source_label.is_empty() {
                        "-".to_string()
                    } else {
                        entry.source_label.clone()
                    },
                    if entry.label.is_empty() {
                        "-".to_string()
                    } else {
                        entry.label.clone()
                    },
                    entry.time.format("%Y-%m-%d %H:%M:%S").to_string(),
                ]);
            }
            println!("{table}");
        }
        commands::list::ListResult::Items(items) => {
            for item in &items {
                let type_char = match item.entry_type {
                    vger_core::snapshot::item::ItemType::Directory => "d",
                    vger_core::snapshot::item::ItemType::RegularFile => "-",
                    vger_core::snapshot::item::ItemType::Symlink => "l",
                };
                println!(
                    "{}{:o} {:>8} {}",
                    type_char,
                    item.mode & 0o7777,
                    item.size,
                    item.path
                );
            }
        }
    }

    Ok(())
}

fn run_extract(
    config: &VgerConfig,
    label: Option<&str>,
    snapshot_name: String,
    dest: String,
    pattern: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let stats = with_repo_passphrase(config, label, |passphrase| {
        commands::extract::run(
            config,
            passphrase,
            &snapshot_name,
            &dest,
            pattern.as_deref(),
        )
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    println!(
        "Extracted: {} files, {} dirs, {} symlinks ({})",
        stats.files,
        stats.dirs,
        stats.symlinks,
        format_bytes(stats.total_bytes),
    );

    Ok(())
}

fn run_delete(
    config: &VgerConfig,
    label: Option<&str>,
    snapshot_name: String,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let stats = with_repo_passphrase(config, label, |passphrase| {
        commands::delete::run(config, passphrase, &snapshot_name, dry_run)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    if dry_run {
        println!("Dry run: would delete snapshot '{}'", stats.snapshot_name);
        println!(
            "  Would free: {} chunks, {}",
            stats.chunks_deleted,
            format_bytes(stats.space_freed),
        );
    } else {
        println!("Deleted snapshot '{}'", stats.snapshot_name);
        println!(
            "  Freed: {} chunks, {}",
            stats.chunks_deleted,
            format_bytes(stats.space_freed),
        );
    }

    Ok(())
}

fn run_prune(
    config: &VgerConfig,
    label: Option<&str>,
    dry_run: bool,
    list: bool,
    sources: &[SourceEntry],
    source_filter: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let (stats, list_entries) = with_repo_passphrase(config, label, |passphrase| {
        commands::prune::run(config, passphrase, dry_run, list, sources, source_filter)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    if list || dry_run {
        for entry in &list_entries {
            if entry.reasons.is_empty() {
                println!("{:<6} {}", entry.action, entry.snapshot_name);
            } else {
                println!(
                    "{:<6} {}  [{}]",
                    entry.action,
                    entry.snapshot_name,
                    entry.reasons.join(", "),
                );
            }
        }
        println!();
    }

    if dry_run {
        println!(
            "Dry run: would keep {} and prune {} snapshots",
            stats.kept, stats.pruned,
        );
    } else {
        println!(
            "Pruned {} snapshots (kept {}), freed {} chunks ({})",
            stats.pruned,
            stats.kept,
            stats.chunks_deleted,
            format_bytes(stats.space_freed),
        );
    }

    Ok(())
}

fn run_check(
    config: &VgerConfig,
    label: Option<&str>,
    verify_data: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = with_repo_passphrase(config, label, |passphrase| {
        commands::check::run(config, passphrase, verify_data)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    if !result.errors.is_empty() {
        println!("Errors found:");
        for err in &result.errors {
            println!("  [{}] {}", err.context, err.message);
        }
        println!();
    }

    println!(
        "Check complete: {} snapshots, {} items, {} chunks existence-checked, {} chunks data-verified, {} errors",
        result.snapshots_checked,
        result.items_checked,
        result.chunks_existence_checked,
        result.chunks_data_verified,
        result.errors.len(),
    );

    if !result.errors.is_empty() {
        return Err(format!("check found {} error(s)", result.errors.len()).into());
    }

    Ok(())
}

fn run_mount(
    config: &VgerConfig,
    label: Option<&str>,
    snapshot_name: Option<String>,
    address: String,
    cache_size: usize,
    source_filter: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    with_repo_passphrase(config, label, |passphrase| {
        commands::mount::run(
            config,
            passphrase,
            snapshot_name.as_deref(),
            &address,
            cache_size,
            source_filter,
        )
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    Ok(())
}

fn run_compact(
    config: &VgerConfig,
    label: Option<&str>,
    threshold: f64,
    max_repack_size: Option<String>,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let max_bytes = max_repack_size.map(|s| parse_size(&s)).transpose()?;

    let stats = with_repo_passphrase(config, label, |passphrase| {
        commands::compact::run(config, passphrase, threshold, max_bytes, dry_run)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    if dry_run {
        println!(
            "Dry run: {} packs total, {} would be repacked, {} would be deleted (empty)",
            stats.packs_total, stats.packs_repacked, stats.packs_deleted_empty,
        );
        println!(
            "  {} live blobs, {} dead blobs, {} would be freed",
            stats.blobs_live,
            stats.blobs_dead,
            format_bytes(stats.space_freed),
        );
    } else {
        println!(
            "Compaction complete: {} packs repacked, {} empty packs deleted, {} freed",
            stats.packs_repacked,
            stats.packs_deleted_empty,
            format_bytes(stats.space_freed),
        );
    }

    Ok(())
}

/// Parse a human-readable size string like "500M", "2G", "1024K" into bytes.
fn parse_size(s: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty size string".into());
    }

    let (num_str, multiplier) = match s.as_bytes().last() {
        Some(b'K' | b'k') => (&s[..s.len() - 1], 1024u64),
        Some(b'M' | b'm') => (&s[..s.len() - 1], 1024 * 1024),
        Some(b'G' | b'g') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        Some(b'T' | b't') => (&s[..s.len() - 1], 1024 * 1024 * 1024 * 1024),
        _ => (s, 1u64),
    };

    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid size: '{s}'"))?;
    Ok((num * multiplier as f64) as u64)
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}
