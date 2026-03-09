use std::io::IsTerminal;
use std::sync::atomic::AtomicBool;

use crate::hooks::{self, HookContext};
use crate::progress::BackupProgressRenderer;
use vykar_core::app::operations::{CycleEvent, CycleStep, FullCycleResult, StepOutcome};
use vykar_core::commands;
use vykar_core::config::{EncryptionModeConfig, HooksConfig, SourceEntry, VykarConfig};
use vykar_storage::{parse_repo_url, ParsedUrl};

use crate::cli::Commands;
use crate::cmd;
use crate::format::format_bytes;
use crate::passphrase::with_repo_passphrase;

pub(crate) fn warn_if_untrusted_rest(config: &VykarConfig, label: Option<&str>) {
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

fn make_hook_ctx(command: &str, cfg: &VykarConfig, repo_label: &Option<String>) -> HookContext {
    HookContext {
        command: command.to_string(),
        repository: cfg.repository.url.clone(),
        label: repo_label.clone(),
        error: None,
        source_label: None,
        source_paths: None,
    }
}

/// Returns `Ok(had_partial)` — `true` if backup had soft errors but still succeeded.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_default_actions(
    cfg: &VykarConfig,
    label: Option<&str>,
    sources: &[SourceEntry],
    global_hooks: &HooksConfig,
    repo_hooks: &HooksConfig,
    repo_label: &Option<String>,
    shutdown: Option<&AtomicBool>,
    verbose: u8,
) -> Result<bool, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();

    // Resolve passphrase once up front so all steps share it.
    let result = with_repo_passphrase(cfg, label, |passphrase| {
        let mut before_step = |step: CycleStep| -> std::result::Result<(), String> {
            let mut ctx = make_hook_ctx(step.command_name(), cfg, repo_label);
            hooks::run_before(global_hooks, repo_hooks, &mut ctx).map_err(|e| e.to_string())
        };

        let mut after_step = |step: CycleStep, outcome: &StepOutcome| {
            let mut ctx = make_hook_ctx(step.command_name(), cfg, repo_label);
            let success = outcome.is_success();
            if let Some(msg) = outcome.error_msg() {
                ctx.error = Some(msg.to_string());
            }
            hooks::run_after_or_failed(global_hooks, repo_hooks, &mut ctx, success);
            hooks::run_finally(global_hooks, repo_hooks, &mut ctx);
        };

        let is_tty = std::io::stderr().is_terminal();
        let show_progress = is_tty || verbose > 0;
        let mut backup_renderer: Option<BackupProgressRenderer> = None;

        let cycle_result = vykar_core::app::operations::run_full_cycle_for_repo(
            cfg,
            sources,
            passphrase,
            shutdown,
            &mut |event| match &event {
                CycleEvent::StepStarted(step) => {
                    eprintln!("==> Starting {}", step.command_name());
                    if matches!(step, CycleStep::Backup) && show_progress {
                        backup_renderer = Some(BackupProgressRenderer::new(verbose, is_tty));
                    }
                }
                CycleEvent::StepFinished(step, outcome) => {
                    if matches!(step, CycleStep::Backup) {
                        if let Some(ref mut r) = backup_renderer {
                            r.finish();
                        }
                        backup_renderer = None;
                    }
                    if matches!(outcome, StepOutcome::Failed(..)) {
                        if let StepOutcome::Failed(e) = outcome {
                            eprintln!("Error: {e}");
                        }
                    }
                }
                CycleEvent::Backup(evt) => {
                    if let Some(ref mut r) = backup_renderer {
                        r.on_event(evt.clone());
                    }
                }
                CycleEvent::Check(evt) => {
                    format_check_progress(evt);
                }
            },
            Some(&mut before_step),
            Some(&mut after_step),
        );

        // Ensure renderer is cleaned up if cycle ended abruptly (e.g. shutdown).
        if let Some(ref mut r) = backup_renderer {
            r.finish();
        }

        Ok(cycle_result)
    })?;

    print_step_details(&result);
    print_summary(&result.steps, start)?;

    if result.has_failures() {
        Err("one or more steps failed".into())
    } else {
        Ok(result.had_partial())
    }
}

/// Print the detailed per-step output (matching what individual CLI wrappers print).
fn print_step_details(result: &FullCycleResult) {
    // Backup summaries
    if let Some(ref report) = result.backup_report {
        for created in &report.created {
            let stats = &created.stats;
            let paths_display = created.source_paths.join(", ");
            println!("Snapshot created: {}", created.snapshot_name);
            println!(
                "  Source: {paths_display} (label: {})",
                created.source_label
            );
            if stats.errors > 0 {
                eprintln!(
                    "Warning: {} file(s) could not be read and were excluded from the snapshot",
                    stats.errors
                );
                println!(
                    "  Files: {}, Errors: {}, Original: {}, Compressed: {}, Deduplicated: {}",
                    stats.nfiles,
                    stats.errors,
                    format_bytes(stats.original_size),
                    format_bytes(stats.compressed_size),
                    format_bytes(stats.deduplicated_size),
                );
            } else {
                println!(
                    "  Files: {}, Original: {}, Compressed: {}, Deduplicated: {}",
                    stats.nfiles,
                    format_bytes(stats.original_size),
                    format_bytes(stats.compressed_size),
                    format_bytes(stats.deduplicated_size),
                );
            }
        }
    }

    // Prune stats
    if let Some(ref stats) = result.prune_stats {
        println!(
            "Pruned {} snapshots (kept {}), freed {} chunks ({})",
            stats.pruned,
            stats.kept,
            stats.chunks_deleted,
            format_bytes(stats.space_freed),
        );
    }

    // Compact stats
    if let Some(ref stats) = result.compact_stats {
        println!(
            "Compaction complete: {} packs repacked, {} empty packs deleted, {} freed",
            stats.packs_repacked,
            stats.packs_deleted_empty,
            format_bytes(stats.space_freed),
        );
        if stats.packs_corrupt > 0 {
            eprintln!(
                "  Warning: {} corrupt pack(s) found; run `vykar check --verify-data` for details",
                stats.packs_corrupt,
            );
        }
        if stats.packs_orphan > 0 {
            eprintln!(
                "  {} orphan pack(s) (present on disk but not in index)",
                stats.packs_orphan,
            );
        }
    }

    // Check results
    if let Some(ref result) = result.check_result {
        if !result.errors.is_empty() {
            println!("Errors found:");
            for err in &result.errors {
                println!("  [{}] {}", err.context, err.message);
            }
            println!();
        }
        println!(
            "Check complete: {} snapshots, {} items, {} packs existence-checked ({} chunks), {} chunks data-verified, {} errors",
            result.snapshots_checked,
            result.items_checked,
            result.packs_existence_checked,
            result.chunks_existence_checked,
            result.chunks_data_verified,
            result.errors.len(),
        );
    }
}

/// Prints the summary table.
fn print_summary(
    steps: &[(CycleStep, StepOutcome)],
    start: std::time::Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    let elapsed = start.elapsed();

    eprintln!();
    eprintln!("=== Summary ===");
    for (step, result) in steps {
        let name = step.command_name();
        match result {
            StepOutcome::Ok => eprintln!("  {name:<12} ok"),
            StepOutcome::Partial => eprintln!("  {name:<12} ok (partial)"),
            StepOutcome::Failed(e) => {
                eprintln!("  {name:<12} FAILED: {e}");
            }
            StepOutcome::Skipped(reason) => eprintln!("  {name:<12} skipped ({reason})"),
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

    Ok(())
}

fn format_check_progress(event: &commands::check::CheckProgressEvent) {
    match event {
        commands::check::CheckProgressEvent::SnapshotStarted {
            current,
            total,
            name,
        } => eprintln!("[{current}/{total}] Checking snapshot '{name}'..."),
        commands::check::CheckProgressEvent::PacksExistencePhaseStarted { total_packs } => {
            eprintln!("Verifying existence of {total_packs} packs in storage...");
        }
        commands::check::CheckProgressEvent::PacksExistenceProgress {
            checked,
            total_packs,
        } => eprintln!("  existence: {checked}/{total_packs} packs"),
        commands::check::CheckProgressEvent::ChunksDataPhaseStarted { total_chunks } => {
            eprintln!("Verifying data integrity of {total_chunks} chunks...");
        }
        commands::check::CheckProgressEvent::ChunksDataProgress {
            verified,
            total_chunks,
        } => eprintln!("  verify-data: {verified}/{total_chunks}"),
        commands::check::CheckProgressEvent::ServerVerifyPhaseStarted { total_packs } => {
            eprintln!("Server-side verification of {total_packs} packs...");
        }
        commands::check::CheckProgressEvent::ServerVerifyProgress {
            verified,
            total_packs,
        } => eprintln!("  server-verify: {verified}/{total_packs} packs"),
    }
}

/// Returns `Ok(had_partial)` — `true` if backup had soft errors but still succeeded.
pub(crate) fn dispatch_command(
    command: &Commands,
    cfg: &VykarConfig,
    label: Option<&str>,
    sources: &[SourceEntry],
    shutdown: Option<&AtomicBool>,
    verbose: u8,
) -> Result<bool, Box<dyn std::error::Error>> {
    match command {
        Commands::Init { .. } => cmd::init::run_init(cfg, label).map(|()| false),
        Commands::Backup {
            label: user_label,
            compression,
            connections,
            source,
            paths,
            ..
        } => cmd::backup::run_backup(
            cfg,
            label,
            user_label.clone(),
            compression.clone(),
            connections.map(|v| v as usize),
            paths.clone(),
            sources,
            source,
            shutdown,
            verbose,
        ),
        Commands::List { source, last, .. } => {
            cmd::list::run_list(cfg, label, source, *last).map(|()| false)
        }
        Commands::Snapshot { command, .. } => {
            cmd::snapshot::run_snapshot_command(command, cfg, label, shutdown).map(|()| false)
        }
        Commands::Restore {
            snapshot,
            dest,
            pattern,
            ..
        } => cmd::restore::run_restore(cfg, label, snapshot.clone(), dest.clone(), pattern.clone())
            .map(|()| false),
        Commands::Delete {
            yes_delete_this_repo,
            ..
        } => cmd::delete::run_delete_repo(cfg, label, *yes_delete_this_repo).map(|()| false),
        Commands::Prune {
            dry_run,
            list,
            source,
            compact,
            ..
        } => cmd::prune::run_prune(
            cfg, label, *dry_run, *list, sources, source, *compact, shutdown,
        )
        .map(|()| false),
        Commands::Check {
            verify_data,
            distrust_server,
            repair,
            dry_run,
            yes,
            ..
        } => cmd::check::run_check(
            cfg,
            label,
            *verify_data,
            *distrust_server,
            *repair,
            *dry_run,
            *yes,
        )
        .map(|()| false),
        Commands::Info { .. } => cmd::info::run_info(cfg, label).map(|()| false),
        Commands::Mount {
            snapshot,
            source,
            address,
            cache_size,
            ..
        } => cmd::mount::run_mount(
            cfg,
            label,
            snapshot.clone(),
            address.clone(),
            *cache_size,
            source,
        )
        .map(|()| false),
        Commands::BreakLock { sessions, .. } => {
            cmd::break_lock::run_break_lock(cfg, label, *sessions).map(|()| false)
        }
        Commands::Compact {
            threshold,
            max_repack_size,
            dry_run,
            ..
        } => {
            let t = threshold.unwrap_or(cfg.compact.threshold);
            cmd::compact::run_compact(cfg, label, t, max_repack_size.clone(), *dry_run, shutdown)
                .map(|()| false)
        }
        Commands::Config { .. } => {
            Err("'config' command should be handled before config resolution".into())
        }
        Commands::Daemon => {
            Err("'daemon' command should be handled before per-repo dispatch".into())
        }
    }
}
