use clap::{Parser, Subcommand};
use comfy_table::{Table, presets::UTF8_FULL_CONDENSED};

use vger_core::commands;
use vger_core::compress::Compression;
use vger_core::config::VgerConfig;

#[derive(Parser)]
#[command(name = "vger", version, about = "Fast, encrypted, deduplicated backups")]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "vger.yaml")]
    config: String,

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

    /// Create a new backup archive
    Create {
        /// Archive name
        #[arg(long)]
        archive: Option<String>,

        /// Compression algorithm override (lz4, zstd, none)
        #[arg(long)]
        compression: Option<String>,

        /// Paths to back up (overrides config source_directories)
        paths: Vec<String>,
    },

    /// List archives or archive contents
    List {
        /// Show contents of a specific archive
        #[arg(long)]
        archive: Option<String>,
    },

    /// Extract files from an archive
    Extract {
        /// Archive to extract from
        #[arg(long)]
        archive: String,

        /// Destination directory
        #[arg(long, default_value = ".")]
        dest: String,

        /// Only extract paths matching this glob pattern
        #[arg(long)]
        pattern: Option<String>,
    },

    /// Delete a specific archive
    Delete {
        /// Archive name to delete
        #[arg(long)]
        archive: String,

        /// Only show what would be deleted, don't actually delete
        #[arg(short = 'n', long)]
        dry_run: bool,
    },

    /// Prune archives according to retention policy
    Prune {
        /// Only show what would be pruned, don't actually prune
        #[arg(short = 'n', long)]
        dry_run: bool,

        /// Show detailed list of kept/pruned archives with reasons
        #[arg(long)]
        list: bool,
    },

    /// Verify repository integrity
    Check {
        /// Read and verify all data chunks (slow but thorough)
        #[arg(long)]
        verify_data: bool,
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

    // Load config
    let config_str = match std::fs::read_to_string(&cli.config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: cannot read config file '{}': {e}", cli.config);
            std::process::exit(1);
        }
    };
    let config: VgerConfig = match serde_yaml::from_str(&config_str) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: invalid config file '{}': {e}", cli.config);
            std::process::exit(1);
        }
    };

    let result = match cli.command {
        Commands::Init => run_init(&config),
        Commands::Create {
            archive,
            compression,
            paths,
        } => run_create(&config, archive, compression, paths),
        Commands::List { archive } => run_list(&config, archive),
        Commands::Extract {
            archive,
            dest,
            pattern,
        } => run_extract(&config, archive, dest, pattern),
        Commands::Delete { archive, dry_run } => run_delete(&config, archive, dry_run),
        Commands::Prune { dry_run, list } => run_prune(&config, dry_run, list),
        Commands::Check { verify_data } => run_check(&config, verify_data),
        Commands::Compact {
            threshold,
            max_repack_size,
            dry_run,
        } => run_compact(&config, threshold, max_repack_size, dry_run),
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn get_passphrase(config: &VgerConfig) -> Result<Option<String>, Box<dyn std::error::Error>> {
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

    // Interactive prompt
    let pass = rpassword::prompt_password("Enter passphrase: ")?;
    Ok(Some(pass))
}

fn run_init(config: &VgerConfig) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = if config.encryption.mode != "none" {
        // Try config passphrase/passcommand first, fall back to interactive
        if let Some(p) = get_passphrase(config)? {
            Some(p)
        } else {
            let p1 = rpassword::prompt_password("Enter new passphrase: ")?;
            let p2 = rpassword::prompt_password("Confirm passphrase: ")?;
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

fn run_create(
    config: &VgerConfig,
    archive_name: Option<String>,
    compression_override: Option<String>,
    paths: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config)?;

    // Determine archive name
    let name = archive_name.unwrap_or_else(|| {
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

    let stats = commands::create::run(config, &name, passphrase.as_deref(), &paths, compression)?;

    println!("Archive created: {name}");
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
    archive_name: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config)?;

    let result = commands::list::run(config, passphrase.as_deref(), archive_name.as_deref())?;

    match result {
        commands::list::ListResult::Archives(archives) => {
            if archives.is_empty() {
                println!("No archives found.");
                return Ok(());
            }

            let mut table = Table::new();
            table.load_preset(UTF8_FULL_CONDENSED);
            table.set_header(vec!["Name", "Date", "ID"]);

            for entry in &archives {
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
                    vger_core::archive::item::ItemType::Directory => "d",
                    vger_core::archive::item::ItemType::RegularFile => "-",
                    vger_core::archive::item::ItemType::Symlink => "l",
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
    archive_name: String,
    dest: String,
    pattern: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config)?;

    let stats = commands::extract::run(
        config,
        passphrase.as_deref(),
        &archive_name,
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
    archive_name: String,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config)?;

    let stats = commands::delete::run(config, passphrase.as_deref(), &archive_name, dry_run)?;

    if dry_run {
        println!("Dry run: would delete archive '{}'", stats.archive_name);
        println!(
            "  Would free: {} chunks, {}",
            stats.chunks_deleted,
            format_bytes(stats.space_freed),
        );
    } else {
        println!("Deleted archive '{}'", stats.archive_name);
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
    dry_run: bool,
    list: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config)?;

    let (stats, list_entries) =
        commands::prune::run(config, passphrase.as_deref(), dry_run, list)?;

    if list || dry_run {
        for entry in &list_entries {
            if entry.reasons.is_empty() {
                println!("{:<6} {}", entry.action, entry.archive_name);
            } else {
                println!(
                    "{:<6} {}  [{}]",
                    entry.action,
                    entry.archive_name,
                    entry.reasons.join(", "),
                );
            }
        }
        println!();
    }

    if dry_run {
        println!(
            "Dry run: would keep {} and prune {} archives",
            stats.kept, stats.pruned,
        );
    } else {
        println!(
            "Pruned {} archives (kept {}), freed {} chunks ({})",
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
    verify_data: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config)?;

    let result = commands::check::run(config, passphrase.as_deref(), verify_data)?;

    if !result.errors.is_empty() {
        println!("Errors found:");
        for err in &result.errors {
            println!("  [{}] {}", err.context, err.message);
        }
        println!();
    }

    println!(
        "Check complete: {} archives, {} items, {} chunks existence-checked, {} chunks data-verified, {} errors",
        result.archives_checked,
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
    threshold: f64,
    max_repack_size: Option<String>,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config)?;

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
