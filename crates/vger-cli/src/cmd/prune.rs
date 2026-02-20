use vger_core::commands;
use vger_core::config::{SourceEntry, VgerConfig};

use crate::format::format_bytes;
use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_prune(
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
        super::compact::run_compact(config, label, config.compact.threshold, None, dry_run)?;
    }

    Ok(())
}
