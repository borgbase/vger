use std::io::{IsTerminal, Write};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use comfy_table::{presets::NOTHING, Attribute, Cell, Table};
use rand::RngCore;

use vger_core::commands;
use vger_core::compress::Compression;
use vger_core::config::{self, EncryptionModeConfig, ResolvedRepo, SourceEntry, VgerConfig};
use vger_core::hooks::{self, HookContext};
use vger_core::platform::shell;
use vger_core::storage::{parse_repo_url, ParsedUrl};

const PROGRESS_REDRAW_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_PROGRESS_COLUMNS: usize = 120;

struct BackupProgressRenderer {
    current_file: Option<String>,
    nfiles: u64,
    original_size: u64,
    compressed_size: u64,
    deduplicated_size: u64,
    last_draw: Instant,
    last_line_len: usize,
    rendered_any: bool,
}

impl BackupProgressRenderer {
    fn new() -> Self {
        Self {
            current_file: None,
            nfiles: 0,
            original_size: 0,
            compressed_size: 0,
            deduplicated_size: 0,
            last_draw: Instant::now(),
            last_line_len: 0,
            rendered_any: false,
        }
    }

    fn on_event(&mut self, event: commands::backup::BackupProgressEvent) {
        let should_render = match event {
            commands::backup::BackupProgressEvent::FileStarted { path } => {
                self.current_file = Some(path);
                true
            }
            commands::backup::BackupProgressEvent::StatsUpdated {
                nfiles,
                original_size,
                compressed_size,
                deduplicated_size,
                current_file,
            } => {
                self.nfiles = nfiles;
                self.original_size = original_size;
                self.compressed_size = compressed_size;
                self.deduplicated_size = deduplicated_size;
                if let Some(path) = current_file {
                    self.current_file = Some(path);
                }
                true
            }
            commands::backup::BackupProgressEvent::SourceStarted { .. }
            | commands::backup::BackupProgressEvent::SourceFinished { .. } => false,
        };

        if should_render {
            self.render(false);
        }
    }

    fn finish(&mut self) {
        if !self.rendered_any {
            return;
        }
        self.render(true);
        eprintln!();
        self.rendered_any = false;
        self.last_line_len = 0;
    }

    fn render(&mut self, force: bool) {
        if !force && self.rendered_any && self.last_draw.elapsed() < PROGRESS_REDRAW_INTERVAL {
            return;
        }
        self.last_draw = Instant::now();

        let file = self.current_file.as_deref().unwrap_or("-");
        let prefix = format!(
            "Files: {}, Original: {}, Compressed: {}, Deduplicated: {}, Current: ",
            self.nfiles,
            format_bytes(self.original_size),
            format_bytes(self.compressed_size),
            format_bytes(self.deduplicated_size),
        );

        let columns = terminal_columns();
        let available = columns.saturating_sub(prefix.chars().count());
        let current = truncate_with_ellipsis(file, available);
        let line = format!("{prefix}{current}");
        let line_len = line.chars().count();
        let pad_len = self.last_line_len.saturating_sub(line_len);

        eprint!("\r{line}{}", " ".repeat(pad_len));
        let _ = std::io::stderr().flush();

        self.last_line_len = line_len;
        self.rendered_any = true;
    }
}

fn terminal_columns() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_PROGRESS_COLUMNS)
}

fn truncate_with_ellipsis(input: &str, max_cols: usize) -> String {
    if max_cols == 0 {
        return String::new();
    }

    let input_len = input.chars().count();
    if input_len <= max_cols {
        return input.to_string();
    }

    if max_cols <= 3 {
        return ".".repeat(max_cols);
    }

    let keep = max_cols - 3;
    let tail: String = input
        .chars()
        .rev()
        .take(keep)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("...{tail}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CliTableTheme {
    use_unicode: bool,
    use_color: bool,
}

impl CliTableTheme {
    fn detect() -> Self {
        let is_tty = std::io::stdout().is_terminal();
        let no_color = std::env::var_os("NO_COLOR").is_some();
        resolve_table_theme(is_tty, no_color)
    }

    fn new_data_table(self, headers: &[&str]) -> Table {
        let mut table = Table::new();
        table.load_preset(NOTHING);
        let header_cells: Vec<Cell> = headers.iter().map(|h| self.header_cell(h)).collect();
        table.set_header(header_cells);
        table
    }

    fn new_kv_table(self) -> Table {
        let mut table = Table::new();
        table.load_preset(NOTHING);
        table
    }

    fn header_cell(self, text: &str) -> Cell {
        let mut cell = Cell::new(text);
        if self.use_color {
            cell = cell.add_attribute(Attribute::Bold);
        }
        cell
    }

    fn key_cell(self, text: &str) -> Cell {
        let mut cell = Cell::new(text);
        if self.use_color {
            cell = cell.add_attribute(Attribute::Bold);
        }
        cell
    }
}

fn resolve_table_theme(is_tty: bool, no_color: bool) -> CliTableTheme {
    CliTableTheme {
        use_unicode: is_tty,
        use_color: is_tty && !no_color,
    }
}

fn add_kv_row(table: &mut Table, theme: CliTableTheme, field: &str, value: impl ToString) {
    table.add_row(vec![theme.key_cell(field), Cell::new(value.to_string())]);
}

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
  4. Platform user config dir + /vger/config.yaml (e.g. ~/.config or %APPDATA%)
  5. Platform system config path (Unix: /etc/vger/config.yaml, Windows: %PROGRAMDATA%/vger/config.yaml)

Environment variables:
  VGER_CONFIG       Path to configuration file (overrides default search)
  VGER_PASSPHRASE   Repository passphrase (skips interactive prompt)"
)]
struct Cli {
    /// Path to configuration file (overrides VGER_CONFIG and default search)
    #[arg(short, long)]
    config: Option<String>,

    /// Verbosity level (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new repository
    Init {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,
    },

    /// Back up files to a new snapshot
    Backup {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Label for the snapshot (sets source_label for ad-hoc backups)
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

    /// List snapshots
    List {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Filter displayed snapshots by source label
        #[arg(short = 'S', long = "source")]
        source: Vec<String>,

        /// Show only the N most recent snapshots
        #[arg(long)]
        last: Option<usize>,
    },

    /// Inspect snapshot contents and metadata
    Snapshot {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        #[command(subcommand)]
        command: SnapshotCommand,
    },

    /// Restore files from a snapshot
    Restore {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Snapshot to restore from
        snapshot: String,

        /// Destination directory
        #[arg(long, default_value = ".")]
        dest: String,

        /// Only restore paths matching this glob pattern
        #[arg(long)]
        pattern: Option<String>,
    },

    /// Delete a specific snapshot
    Delete {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Snapshot name to delete
        snapshot: String,

        /// Only show what would be deleted, don't actually delete
        #[arg(short = 'n', long)]
        dry_run: bool,
    },

    /// Prune snapshots according to retention policy
    Prune {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Only show what would be pruned, don't actually prune
        #[arg(short = 'n', long)]
        dry_run: bool,

        /// Show detailed list of kept/pruned snapshots with reasons
        #[arg(long)]
        list: bool,

        /// Apply retention only to snapshots matching these source labels
        #[arg(short = 'S', long = "source")]
        source: Vec<String>,

        /// Run compact after pruning to reclaim space from orphaned blobs
        #[arg(long)]
        compact: bool,
    },

    /// Verify repository integrity
    Check {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

        /// Read and verify all data chunks (slow but thorough)
        #[arg(long)]
        verify_data: bool,
    },

    /// Show repository statistics and snapshot totals
    Info {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,
    },

    /// Generate a minimal configuration file
    Config {
        /// Destination path (skips interactive prompt)
        #[arg(short, long)]
        dest: Option<String>,
    },

    /// Browse snapshots via a local WebDAV server
    Mount {
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

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
        /// Select repository by label or path
        #[arg(short = 'R', long = "repo")]
        repo: Option<String>,

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

#[derive(Clone, clap::ValueEnum)]
enum SortField {
    Name,
    Size,
    Mtime,
}

#[derive(Subcommand)]
enum SnapshotCommand {
    /// Show contents of a snapshot
    List {
        /// Snapshot to inspect
        snapshot: String,
        /// Show only files under this subtree
        #[arg(long)]
        path: Option<String>,
        /// Show permissions, size, mtime
        #[arg(long)]
        long: bool,
        /// Sort output (default: name)
        #[arg(long, value_enum, default_value_t = SortField::Name)]
        sort: SortField,
    },
    /// Show metadata of a snapshot
    Info {
        /// Snapshot to inspect
        snapshot: String,
    },
    /// Find files across snapshots
    Find {
        /// Starting directory (default: root)
        path: Option<String>,
        /// Filter by source label
        #[arg(short = 'S', long = "source", help_heading = "Scope Options")]
        source: Option<String>,
        /// Search only the last N snapshots (must be >= 1)
        #[arg(
            long,
            value_parser = clap::value_parser!(u64).range(1..),
            help_heading = "Scope Options"
        )]
        last: Option<u64>,
        /// Match filename by glob pattern (case-sensitive)
        #[arg(long = "name", help_heading = "Filter Options")]
        name: Option<String>,
        /// Match filename by glob pattern (case-insensitive)
        #[arg(long = "iname", help_heading = "Filter Options")]
        iname: Option<String>,
        /// Filter by entry type: f (file), d (directory), l (symlink)
        #[arg(long = "type", value_name = "TYPE", help_heading = "Filter Options")]
        entry_type: Option<String>,
        /// Only include items modified within this time span (e.g. 24h, 7d, 2w)
        #[arg(long, help_heading = "Filter Options")]
        since: Option<String>,
        /// Only include items at least this size (e.g. 1M, 500K)
        #[arg(long, help_heading = "Filter Options")]
        larger: Option<String>,
        /// Only include items at most this size (e.g. 10M, 1G)
        #[arg(long, help_heading = "Filter Options")]
        smaller: Option<String>,
    },
}

impl Commands {
    fn repo(&self) -> Option<&str> {
        match self {
            Self::Init { repo, .. }
            | Self::Backup { repo, .. }
            | Self::List { repo, .. }
            | Self::Snapshot { repo, .. }
            | Self::Restore { repo, .. }
            | Self::Delete { repo, .. }
            | Self::Prune { repo, .. }
            | Self::Check { repo, .. }
            | Self::Info { repo, .. }
            | Self::Mount { repo, .. }
            | Self::Compact { repo, .. } => repo.as_deref(),
            Self::Config { .. } => None,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Init { .. } => "init",
            Self::Backup { .. } => "backup",
            Self::List { .. } => "list",
            Self::Restore { .. } => "restore",
            Self::Delete { .. } => "delete",
            Self::Prune { .. } => "prune",
            Self::Check { .. } => "check",
            Self::Info { .. } => "info",
            Self::Mount { .. } => "mount",
            Self::Compact { .. } => "compact",
            Self::Snapshot { .. } => "snapshot",
            Self::Config { .. } => "config",
        }
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

fn warn_if_untrusted_rest(config: &VgerConfig, label: Option<&str>) {
    let Ok(parsed) = parse_repo_url(&config.repository.url) else {
        return;
    };
    let ParsedUrl::Rest { url } = parsed else {
        return;
    };

    let repo_name = label.unwrap_or(&config.repository.url);
    if config.encryption.mode == EncryptionModeConfig::None {
        eprintln!(
            "Warning: repository '{repo_name}' uses REST with plaintext mode (encryption.mode=none)."
        );
    }
    if url.starts_with("http://") {
        eprintln!(
            "Warning: repository '{repo_name}' uses non-HTTPS REST URL '{url}'. Transport is not TLS-protected."
        );
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
        match run_prune(cfg, label, false, false, sources, &[], false) {
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

fn run_config_generate(dest: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let path = match dest {
        Some(d) => std::path::PathBuf::from(d),
        None => pick_config_location()?,
    };

    if path.exists() {
        return Err(format!("file already exists: {}", path.display()).into());
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    std::fs::write(&path, config::minimal_config_template())?;
    println!("Config written to: {}", path.display());
    println!("Edit it to set your repository path and source directories.");
    Ok(())
}

fn pick_config_location() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let bold = dialoguer::console::Style::new().bold();
    let search_paths = config::default_config_search_paths();

    let descriptions: &[&str] = &[
        "Best for: project-specific backups, version-controlled settings",
        "Best for: personal backups of your home directory",
        "Best for: server backups, runs as root or via systemd",
    ];

    let labels: &[&str] = &["Local directory", "User config", "System-wide"];

    let items: Vec<String> = search_paths
        .iter()
        .zip(labels.iter())
        .zip(descriptions.iter())
        .map(|(((path, _level), label), desc)| {
            format!(
                "{} {}\n  {desc}",
                bold.apply_to(label),
                bold.apply_to(path.display()),
            )
        })
        .collect();

    let selection = dialoguer::Select::new()
        .with_prompt("Where should the config file live?")
        .items(&items)
        .default(0)
        .interact()?;

    Ok(search_paths[selection].0.clone())
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
        Commands::Init { .. } => run_init(cfg, label),
        Commands::Backup {
            label: user_label,
            compression,
            source,
            paths,
            ..
        } => run_backup(
            cfg,
            label,
            user_label.clone(),
            compression.clone(),
            paths.clone(),
            sources,
            source,
        ),
        Commands::List { source, last, .. } => run_list(cfg, label, source, *last),
        Commands::Snapshot { command, .. } => run_snapshot_command(command, cfg, label),
        Commands::Restore {
            snapshot,
            dest,
            pattern,
            ..
        } => run_extract(cfg, label, snapshot.clone(), dest.clone(), pattern.clone()),
        Commands::Delete {
            snapshot, dry_run, ..
        } => run_delete(cfg, label, snapshot.clone(), *dry_run),
        Commands::Prune {
            dry_run,
            list,
            source,
            compact,
            ..
        } => run_prune(cfg, label, *dry_run, *list, sources, source, *compact),
        Commands::Check { verify_data, .. } => run_check(cfg, label, *verify_data),
        Commands::Info { .. } => run_info(cfg, label),
        Commands::Mount {
            snapshot,
            source,
            address,
            cache_size,
            ..
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
            ..
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
        let output = shell::run_script(cmd)?;
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

    let repo = commands::init::run(config, passphrase.as_deref())?;
    println!("Repository initialized at: {}", config.repository.url);
    println!("Encryption mode: {}", repo.config.encryption.as_str());
    Ok(())
}

fn run_backup_operation(
    config: &VgerConfig,
    req: commands::backup::BackupRequest<'_>,
    show_progress: bool,
) -> Result<vger_core::snapshot::SnapshotStats, Box<dyn std::error::Error>> {
    if !show_progress {
        return commands::backup::run(config, req)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) });
    }

    let mut renderer = BackupProgressRenderer::new();
    let mut on_progress = |event| renderer.on_event(event);
    let result = commands::backup::run_with_progress(config, req, Some(&mut on_progress));
    renderer.finish();

    result.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
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
        let show_progress = std::io::stderr().is_terminal();

        // Determine compression
        let compression = if let Some(ref algo) = compression_override {
            Compression::from_config(algo, config.compression.zstd_level)?
        } else {
            Compression::from_algorithm(config.compression.algorithm, config.compression.zstd_level)
        };

        if !source_filter.is_empty() && !paths.is_empty() {
            return Err("cannot combine --source with ad-hoc paths".into());
        }

        if user_label.is_some() && paths.is_empty() {
            return Err("--label can only be used with ad-hoc paths".into());
        }

        if !paths.is_empty() {
            // Ad-hoc paths mode: group all paths into a single snapshot
            let expanded: Vec<String> = paths.iter().map(|p| config::expand_tilde(p)).collect();
            let source_label = if !user_label_str.is_empty() {
                user_label_str.to_string()
            } else if expanded.len() == 1 {
                config::label_from_path(&expanded[0])
            } else {
                "adhoc".to_string()
            };
            let name = generate_snapshot_name();

            let stats = run_backup_operation(
                config,
                commands::backup::BackupRequest {
                    snapshot_name: &name,
                    passphrase,
                    source_paths: &expanded,
                    source_label: &source_label,
                    exclude_patterns: &config.exclude_patterns,
                    exclude_if_present: &config.exclude_if_present,
                    one_file_system: config.one_file_system,
                    git_ignore: config.git_ignore,
                    xattrs_enabled: config.xattrs.enabled,
                    compression,
                    command_dumps: &[],
                },
                show_progress,
            )?;

            println!("Snapshot created: {name}");
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
                    let stats = run_backup_operation(
                        config,
                        commands::backup::BackupRequest {
                            snapshot_name: &name,
                            passphrase,
                            source_paths: &source.paths,
                            source_label: &source.label,
                            exclude_patterns: &source.exclude,
                            exclude_if_present: &source.exclude_if_present,
                            one_file_system: source.one_file_system,
                            git_ignore: source.git_ignore,
                            xattrs_enabled: source.xattrs_enabled,
                            compression,
                            command_dumps: &source.command_dumps,
                        },
                        show_progress,
                    )?;

                    println!("Snapshot created: {name}");
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
    source_filter: &[String],
    last: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut snapshots = with_repo_passphrase(config, label, |passphrase| {
        commands::list::list_snapshots(config, passphrase)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

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

    let theme = CliTableTheme::detect();
    let mut table = theme.new_data_table(&["ID", "Source", "Label", "Date"]);

    for entry in &snapshots {
        let source_col = if !entry.source_paths.is_empty() {
            entry.source_paths.join("\n")
        } else if !entry.source_label.is_empty() {
            entry.source_label.clone()
        } else {
            "-".to_string()
        };
        let label_col = if !entry.label.is_empty() {
            entry.label.clone()
        } else if !entry.source_label.is_empty() {
            entry.source_label.clone()
        } else {
            "-".to_string()
        };
        table.add_row(vec![
            Cell::new(entry.name.clone()),
            Cell::new(source_col),
            Cell::new(label_col),
            Cell::new(entry.time.format("%Y-%m-%d %H:%M:%S").to_string()),
        ]);
    }
    println!("{table}");

    Ok(())
}

fn normalize_path_filter(raw: &str) -> String {
    let s = raw.strip_prefix("./").unwrap_or(raw);
    s.trim_end_matches('/').to_string()
}

fn path_matches_filter(item_path: &str, filter: &str) -> bool {
    item_path == filter || item_path.starts_with(&format!("{filter}/"))
}

fn run_snapshot_command(
    command: &SnapshotCommand,
    config: &VgerConfig,
    label: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        SnapshotCommand::List {
            snapshot,
            path,
            long,
            sort,
        } => {
            let mut items = with_repo_passphrase(config, label, |passphrase| {
                commands::list::list_snapshot_items(config, passphrase, snapshot)
                    .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
            })?;

            // Apply path filter (empty filter after normalization means "all items")
            if let Some(ref raw_path) = path {
                let filter = normalize_path_filter(raw_path);
                if !filter.is_empty() {
                    items.retain(|item| path_matches_filter(&item.path, &filter));
                }
            }

            // Apply sort
            match sort {
                SortField::Name => items.sort_by(|a, b| a.path.cmp(&b.path)),
                SortField::Size => items.sort_by(|a, b| b.size.cmp(&a.size)),
                SortField::Mtime => items.sort_by(|a, b| b.mtime.cmp(&a.mtime)),
            }

            if *long {
                for item in &items {
                    let type_char = match item.entry_type {
                        vger_core::snapshot::item::ItemType::Directory => "d",
                        vger_core::snapshot::item::ItemType::RegularFile => "-",
                        vger_core::snapshot::item::ItemType::Symlink => "l",
                    };
                    let secs = item.mtime / 1_000_000_000;
                    let nsecs = (item.mtime % 1_000_000_000) as u32;
                    let mtime = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| "-".to_string());
                    println!(
                        "{}{:04o} {:>10} {} {}",
                        type_char,
                        item.mode & 0o7777,
                        format_bytes(item.size),
                        mtime,
                        item.path,
                    );
                }
            } else {
                for item in &items {
                    println!("{}", item.path);
                }
            }
            Ok(())
        }
        SnapshotCommand::Find {
            path,
            source,
            last,
            name,
            iname,
            entry_type,
            since,
            larger,
            smaller,
        } => run_snapshot_find(
            config, label, path, source, last, name, iname, entry_type, since, larger, smaller,
        ),
        SnapshotCommand::Info { snapshot } => {
            let meta = with_repo_passphrase(config, label, |passphrase| {
                commands::list::get_snapshot_meta(config, passphrase, snapshot)
                    .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
            })?;

            let theme = CliTableTheme::detect();

            // Group 1: snapshot metadata
            let mut t1 = theme.new_kv_table();
            add_kv_row(&mut t1, theme, "Name", &meta.name);
            add_kv_row(&mut t1, theme, "Hostname", &meta.hostname);
            add_kv_row(&mut t1, theme, "Username", &meta.username);
            add_kv_row(
                &mut t1,
                theme,
                "Start time",
                meta.time.format("%Y-%m-%d %H:%M:%S UTC"),
            );
            add_kv_row(
                &mut t1,
                theme,
                "End time",
                meta.time_end.format("%Y-%m-%d %H:%M:%S UTC"),
            );
            let duration = meta.time_end.signed_duration_since(meta.time);
            let secs = duration.num_seconds();
            let duration_str = if secs >= 60 {
                format!("{}m {:02}s", secs / 60, secs % 60)
            } else {
                format!("{secs}s")
            };
            add_kv_row(&mut t1, theme, "Duration", duration_str);
            let effective_label = if meta.label.is_empty() {
                &meta.source_label
            } else {
                &meta.label
            };
            add_kv_row(&mut t1, theme, "Label", effective_label);
            add_kv_row(&mut t1, theme, "Source paths", meta.source_paths.join(", "));
            if !meta.comment.is_empty() {
                add_kv_row(&mut t1, theme, "Comment", &meta.comment);
            }
            println!("{t1}");
            println!();

            // Group 2: statistics
            let mut t2 = theme.new_kv_table();
            add_kv_row(&mut t2, theme, "Files", meta.stats.nfiles);
            add_kv_row(
                &mut t2,
                theme,
                "Original size",
                format_bytes(meta.stats.original_size),
            );
            add_kv_row(
                &mut t2,
                theme,
                "Compressed",
                format_bytes(meta.stats.compressed_size),
            );
            add_kv_row(
                &mut t2,
                theme,
                "Deduplicated",
                format_bytes(meta.stats.deduplicated_size),
            );
            println!("{t2}");
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_snapshot_find(
    config: &VgerConfig,
    label: Option<&str>,
    path: &Option<String>,
    source: &Option<String>,
    last: &Option<u64>,
    name: &Option<String>,
    iname: &Option<String>,
    entry_type: &Option<String>,
    since: &Option<String>,
    larger: &Option<String>,
    smaller: &Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    use commands::find::{FileStatus, FindFilter, FindScope};
    use vger_core::snapshot::item::ItemType;

    let scope = FindScope {
        source_label: source.clone(),
        last_n: last.map(|n| n as usize),
    };

    let path_prefix = path.as_deref().map(normalize_path_filter);

    let item_type: Option<ItemType> = entry_type
        .as_deref()
        .map(|t| match t {
            "f" => Ok(ItemType::RegularFile),
            "d" => Ok(ItemType::Directory),
            "l" => Ok(ItemType::Symlink),
            _ => Err(format!("unknown --type '{t}': use f, d, or l")),
        })
        .transpose()
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let since_dt = since.as_deref().map(parse_duration_span).transpose()?;

    let larger_than = larger.as_deref().map(|s| parse_size(s)).transpose()?;

    let smaller_than = smaller.as_deref().map(|s| parse_size(s)).transpose()?;

    let filter = FindFilter::build(
        path_prefix,
        name.as_deref(),
        iname.as_deref(),
        item_type,
        since_dt,
        larger_than,
        smaller_than,
    )
    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let timelines = with_repo_passphrase(config, label, |passphrase| {
        commands::find::run(config, passphrase, &scope, &filter)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    if timelines.is_empty() {
        println!("No matching files found.");
        return Ok(());
    }

    for timeline in &timelines {
        println!("{}", timeline.path);
        println!("  {:<12} {:<20} {:>10}  Status", "Snapshot", "Date", "Size");
        for ah in &timeline.hits {
            let date = ah.hit.snapshot_time.format("%Y-%m-%d %H:%M:%S").to_string();
            let size = format_bytes(ah.hit.size);
            let status = match ah.status {
                FileStatus::Added => "added",
                FileStatus::Modified => "modified",
                FileStatus::Unchanged => "unchanged",
            };
            println!(
                "  {:<12} {:<20} {:>10}  {}",
                ah.hit.snapshot_name, date, size, status
            );
        }
        println!();
    }

    Ok(())
}

fn parse_duration_span(
    s: &str,
) -> Result<chrono::DateTime<chrono::Utc>, Box<dyn std::error::Error>> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration string".into());
    }

    let (num_str, suffix) = match s.as_bytes().last() {
        Some(b'h' | b'H') => (&s[..s.len() - 1], 3600i64),
        Some(b'd' | b'D') => (&s[..s.len() - 1], 86400i64),
        Some(b'w' | b'W') => (&s[..s.len() - 1], 604800i64),
        _ => {
            return Err(format!(
                "invalid duration '{s}': use a suffix of h, d, or w (e.g. 24h, 7d, 2w)"
            )
            .into())
        }
    };

    let n: i64 = num_str
        .parse()
        .map_err(|_| format!("invalid duration number: '{num_str}'"))?;

    if n <= 0 {
        return Err(format!("--since duration must be positive (got '{s}')").into());
    }

    let seconds = n * suffix;
    let duration = chrono::Duration::seconds(seconds);
    Ok(chrono::Utc::now() - duration)
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
            config.xattrs.enabled,
        )
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    println!(
        "Restored: {} files, {} dirs, {} symlinks ({})",
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
    compact: bool,
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

    if compact {
        run_compact(config, label, 10.0, None, dry_run)?;
    }

    Ok(())
}

fn run_check(
    config: &VgerConfig,
    label: Option<&str>,
    verify_data: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = with_repo_passphrase(config, label, |passphrase| {
        let mut on_progress = |event: commands::check::CheckProgressEvent| match event {
            commands::check::CheckProgressEvent::SnapshotStarted {
                current,
                total,
                name,
            } => {
                eprintln!("[{current}/{total}] Checking snapshot '{name}'...");
            }
            commands::check::CheckProgressEvent::ChunksExistencePhaseStarted { total_chunks } => {
                eprintln!("Verifying existence of {total_chunks} chunks in pack files...");
            }
            commands::check::CheckProgressEvent::ChunksExistenceProgress {
                checked,
                total_chunks,
            } => {
                eprintln!("  existence: {checked}/{total_chunks}");
            }
            commands::check::CheckProgressEvent::ChunksDataPhaseStarted { total_chunks } => {
                eprintln!("Verifying data integrity of {total_chunks} chunks...");
            }
            commands::check::CheckProgressEvent::ChunksDataProgress {
                verified,
                total_chunks,
            } => {
                eprintln!("  verify-data: {verified}/{total_chunks}");
            }
        };

        commands::check::run_with_progress(config, passphrase, verify_data, Some(&mut on_progress))
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

fn run_info(config: &VgerConfig, label: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let stats = with_repo_passphrase(config, label, |passphrase| {
        commands::info::run(config, passphrase)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })?;

    let theme = CliTableTheme::detect();

    // Group 1: repository metadata
    let mut t1 = theme.new_kv_table();
    let repo_name = label.unwrap_or(&config.repository.url);
    add_kv_row(&mut t1, theme, "Repository", repo_name);
    add_kv_row(&mut t1, theme, "URL", config.repository.url.clone());
    add_kv_row(&mut t1, theme, "Encryption", stats.encryption.as_str());
    add_kv_row(
        &mut t1,
        theme,
        "Created",
        stats.repo_created.format("%Y-%m-%d %H:%M:%S UTC"),
    );
    println!("{t1}");
    println!();

    // Group 2: statistics
    let mut t2 = theme.new_kv_table();
    add_kv_row(&mut t2, theme, "Snapshots", stats.snapshot_count);
    let last_snapshot = stats
        .last_snapshot_time
        .map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "-".to_string());
    add_kv_row(&mut t2, theme, "Last snapshot", last_snapshot);
    add_kv_row(
        &mut t2,
        theme,
        "Raw size",
        format_size_with_bytes(stats.raw_size),
    );
    add_kv_row(
        &mut t2,
        theme,
        "Compressed",
        format_size_with_savings(stats.compressed_size, stats.raw_size, "ratio"),
    );
    add_kv_row(
        &mut t2,
        theme,
        "Deduplicated",
        format_size_with_savings(stats.deduplicated_size, stats.raw_size, "savings"),
    );
    add_kv_row(
        &mut t2,
        theme,
        "Unique stored",
        format_size_with_bytes(stats.unique_stored_size),
    );
    add_kv_row(
        &mut t2,
        theme,
        "Referenced",
        format_size_with_bytes(stats.referenced_stored_size),
    );
    add_kv_row(&mut t2, theme, "Unique chunks", stats.unique_chunks);
    println!("{t2}");
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

fn format_size_with_bytes(bytes: u64) -> String {
    format_bytes(bytes)
}

fn format_size_with_savings(bytes: u64, reference: u64, label: &str) -> String {
    if reference == 0 {
        return format_bytes(bytes);
    }
    let pct = (1.0 - bytes as f64 / reference as f64) * 100.0;
    format!("{}  ({:.1}% {label})", format_bytes(bytes), pct)
}

#[cfg(test)]
mod tests {
    use comfy_table::presets::NOTHING;

    use super::{resolve_table_theme, truncate_with_ellipsis};

    #[test]
    fn truncate_with_ellipsis_keeps_tail_when_needed() {
        let input = "/very/long/path/to/a/file.txt";
        let out = truncate_with_ellipsis(input, 16);
        assert_eq!(out, "...to/a/file.txt");
        assert_eq!(out.chars().count(), 16);
    }

    #[test]
    fn truncate_with_ellipsis_returns_original_when_short() {
        let input = "short.txt";
        assert_eq!(truncate_with_ellipsis(input, 32), input);
    }

    #[test]
    fn truncate_with_ellipsis_handles_tiny_widths() {
        assert_eq!(truncate_with_ellipsis("abcdef", 0), "");
        assert_eq!(truncate_with_ellipsis("abcdef", 1), ".");
        assert_eq!(truncate_with_ellipsis("abcdef", 2), "..");
        assert_eq!(truncate_with_ellipsis("abcdef", 3), "...");
    }

    #[test]
    fn resolve_table_theme_enables_unicode_and_color_for_tty() {
        let theme = resolve_table_theme(true, false);
        assert!(theme.use_unicode);
        assert!(theme.use_color);
    }

    #[test]
    fn resolve_table_theme_disables_color_when_no_color_is_set() {
        let theme = resolve_table_theme(true, true);
        assert!(theme.use_unicode);
        assert!(!theme.use_color);
    }

    #[test]
    fn resolve_table_theme_uses_plain_style_when_not_tty() {
        let theme = resolve_table_theme(false, false);
        assert!(!theme.use_unicode);
        assert!(!theme.use_color);
    }

    #[test]
    fn data_table_uses_nothing_preset() {
        let theme = resolve_table_theme(false, false);
        let mut table = theme.new_data_table(&["A", "B"]);
        assert_eq!(table.current_style_as_preset(), NOTHING);
    }

    #[test]
    fn kv_table_uses_nothing_preset() {
        let theme = resolve_table_theme(true, false);
        let mut table = theme.new_kv_table();
        table.add_row(vec!["key", "value"]);
        assert_eq!(table.current_style_as_preset(), NOTHING);
    }
}
