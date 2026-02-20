use vger_core::commands;
use vger_core::config::VgerConfig;

use crate::format::{format_bytes, parse_size};
use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_compact(
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
            "  {} live blobs, {} would be freed",
            stats.blobs_live,
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

    if stats.packs_corrupt > 0 {
        println!(
            "  Warning: {} corrupt pack(s) found; run `vger check --verify-data` for details",
            stats.packs_corrupt,
        );
    }
    if stats.packs_orphan > 0 {
        println!(
            "  {} orphan pack(s) (present on disk but not in index)",
            stats.packs_orphan,
        );
    }

    Ok(())
}
