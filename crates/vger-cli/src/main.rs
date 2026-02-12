use clap::{Parser, Subcommand};
use comfy_table::{Table, presets::UTF8_FULL_CONDENSED};

use vger_core::commands;
use vger_core::compress::Compression;
use vger_core::config::{
    self, ResolvedRepo, VgerConfig,
};

#[derive(Parser)]
#[command(name = "vger", version, about = "Fast, encrypted, deduplicated backups",
    after_help = "\
Configuration file lookup order:
  1. --config <path>             (explicit flag)
  2. $VGER_CONFIG                (environment variable)
  3. ./vger.yaml                 (project)
  4. $XDG_CONFIG_HOME/vger/config.yaml or ~/.config/vger/config.yaml (user)
  5. /etc/vger/config.yaml       (system)

Environment variables:
  VGER_CONFIG       Path to configuration file (overrides default search)
  VGER_PASSPHRASE   Repository passphrase (skips interactive prompt)")]
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
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new repository
    Init,

    /// Back up files to a new snapshot
    Backup {
        /// Snapshot name
        #[arg(long)]
        snapshot: Option<String>,

        /// Compression algorithm override (lz4, zstd, none)
        #[arg(long)]
        compression: Option<String>,

        /// Paths to back up (overrides config source_directories)
        paths: Vec<String>,
    },

    /// List snapshots or snapshot contents
    List {
        /// Show contents of a specific snapshot
        #[arg(long)]
        snapshot: Option<String>,
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
    if let Commands::Config { dest } = &cli.command {
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
                    eprintln!("  {label:12} {}", r.config.repository.path);
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
            let name = repo
                .label
                .as_deref()
                .unwrap_or(&repo.config.repository.path);
            eprintln!("--- Repository: {name} ---");
        }

        let label = repo.label.as_deref();
        let cfg = &repo.config;

        let result = match cli.command {
            Commands::Init => run_init(cfg, label),
            Commands::Backup {
                ref snapshot,
                ref compression,
                ref paths,
            } => run_backup(cfg, label, snapshot.clone(), compression.clone(), paths.clone()),
            Commands::List { ref snapshot } => run_list(cfg, label, snapshot.clone()),
            Commands::Extract {
                ref snapshot,
                ref dest,
                ref pattern,
            } => run_extract(cfg, label, snapshot.clone(), dest.clone(), pattern.clone()),
            Commands::Delete {
                ref snapshot,
                dry_run,
            } => run_delete(cfg, label, snapshot.clone(), dry_run),
            Commands::Prune { dry_run, list } => run_prune(cfg, label, dry_run, list),
            Commands::Check { verify_data } => run_check(cfg, label, verify_data),
            Commands::Compact {
                threshold,
                ref max_repack_size,
                dry_run,
            } => run_compact(cfg, label, threshold, max_repack_size.clone(), dry_run),
            Commands::Config { .. } => unreachable!(),
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

fn get_passphrase(config: &VgerConfig, label: Option<&str>) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if config.encryption.mode == "none" {
        return Ok(None);
    }

    // Check for inline passphrase
    if let Some(ref p) = config.encryption.passphrase {
        return Ok(Some(p.clone()));
    }

    // Check for passcommand
    if let Some(ref cmd) = config.encryption.passcommand {
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .output()?;
        if !output.status.success() {
            return Err(format!("passcommand failed: {}", String::from_utf8_lossy(&output.stderr)).into());
        }
        let pass = String::from_utf8(output.stdout)?.trim().to_string();
        return Ok(Some(pass));
    }

    // Check VGER_PASSPHRASE env var
    if let Ok(pass) = std::env::var("VGER_PASSPHRASE") {
        if !pass.is_empty() {
            return Ok(Some(pass));
        }
    }

    // Interactive prompt
    let prompt = match label {
        Some(l) => format!("Enter passphrase for '{l}': "),
        None => "Enter passphrase: ".to_string(),
    };
    let pass = rpassword::prompt_password(prompt)?;
    Ok(Some(pass))
}

fn run_init(config: &VgerConfig, label: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = if config.encryption.mode != "none" {
        // Try config passphrase/passcommand first, then env var, then interactive
        if let Some(p) = get_passphrase(config, label)? {
            Some(p)
        } else if let Ok(p) = std::env::var("VGER_PASSPHRASE") {
            if !p.is_empty() {
                Some(p)
            } else {
                let suffix = label.map(|l| format!(" for '{l}'")).unwrap_or_default();
                let p1 = rpassword::prompt_password(format!("Enter new passphrase{suffix}: "))?;
                let p2 = rpassword::prompt_password(format!("Confirm passphrase{suffix}: "))?;
                if p1 != p2 {
                    return Err("passphrases do not match".into());
                }
                Some(p1)
            }
        } else {
            let suffix = label.map(|l| format!(" for '{l}'")).unwrap_or_default();
            let p1 = rpassword::prompt_password(format!("Enter new passphrase{suffix}: "))?;
            let p2 = rpassword::prompt_password(format!("Confirm passphrase{suffix}: "))?;
            if p1 != p2 {
                return Err("passphrases do not match".into());
            }
            Some(p1)
        }
    } else {
        None
    };

    commands::init::run(config, passphrase.as_deref())?;
    println!("Repository initialized at: {}", config.repository.path);
    Ok(())
}

fn run_backup(
    config: &VgerConfig,
    label: Option<&str>,
    snapshot_name: Option<String>,
    compression_override: Option<String>,
    paths: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config, label)?;

    // Determine snapshot name
    let name = snapshot_name.unwrap_or_else(|| {
        let hostname = hostname::get()
            .map(|h: std::ffi::OsString| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".into());
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S");
        format!("{hostname}-{now}")
    });

    // Determine compression
    let compression = if let Some(ref algo) = compression_override {
        Compression::from_config(algo, config.compression.zstd_level)?
    } else {
        Compression::from_config(&config.compression.algorithm, config.compression.zstd_level)?
    };

    let stats = commands::backup::run(config, &name, passphrase.as_deref(), &paths, compression)?;

    println!("Snapshot created: {name}");
    println!(
        "  Files: {}, Original: {}, Compressed: {}, Deduplicated: {}",
        stats.nfiles,
        format_bytes(stats.original_size),
        format_bytes(stats.compressed_size),
        format_bytes(stats.deduplicated_size),
    );

    Ok(())
}

fn run_list(
    config: &VgerConfig,
    label: Option<&str>,
    snapshot_name: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config, label)?;

    let result = commands::list::run(config, passphrase.as_deref(), snapshot_name.as_deref())?;

    match result {
        commands::list::ListResult::Snapshots(snapshots) => {
            if snapshots.is_empty() {
                println!("No snapshots found.");
                return Ok(());
            }

            let mut table = Table::new();
            table.load_preset(UTF8_FULL_CONDENSED);
            table.set_header(vec!["Name", "Date", "ID"]);

            for entry in &snapshots {
                table.add_row(vec![
                    entry.name.clone(),
                    entry.time.format("%Y-%m-%d %H:%M:%S").to_string(),
                    hex::encode(&entry.id)[..16].to_string(),
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
                    type_char, item.mode & 0o7777, item.size, item.path
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
    let passphrase = get_passphrase(config, label)?;

    let stats = commands::extract::run(
        config,
        passphrase.as_deref(),
        &snapshot_name,
        &dest,
        pattern.as_deref(),
    )?;

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
    let passphrase = get_passphrase(config, label)?;

    let stats = commands::delete::run(config, passphrase.as_deref(), &snapshot_name, dry_run)?;

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
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config, label)?;

    let (stats, list_entries) =
        commands::prune::run(config, passphrase.as_deref(), dry_run, list)?;

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
    let passphrase = get_passphrase(config, label)?;

    let result = commands::check::run(config, passphrase.as_deref(), verify_data)?;

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
        std::process::exit(1);
    }

    Ok(())
}

fn run_compact(
    config: &VgerConfig,
    label: Option<&str>,
    threshold: f64,
    max_repack_size: Option<String>,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config, label)?;

    let max_bytes = max_repack_size
        .map(|s| parse_size(&s))
        .transpose()?;

    let stats = commands::compact::run(config, passphrase.as_deref(), threshold, max_bytes, dry_run)?;

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

    let num: f64 = num_str.parse().map_err(|_| format!("invalid size: '{s}'"))?;
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
