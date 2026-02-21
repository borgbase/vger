use std::io::IsTerminal;

use vger_core::commands;
use vger_core::config::VgerConfig;

use crate::format::format_bytes;
use crate::passphrase::with_repo_passphrase;

pub(crate) fn run_delete(
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

pub(crate) fn run_delete_repo(
    config: &VgerConfig,
    label: Option<&str>,
    yes_delete_this_repo: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Verify the repo exists before prompting
    let backend = vger_core::storage::backend_from_config(&config.repository)
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
    if !backend
        .exists("config")
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?
    {
        return Err(format!("no repository found at '{}'", config.repository.url).into());
    }
    drop(backend);

    if !yes_delete_this_repo {
        if !std::io::stdin().is_terminal() {
            return Err(
                "refusing to delete repository without confirmation in non-interactive mode; \
                 use --yes-delete-this-repo to skip the prompt"
                    .into(),
            );
        }

        let repo_name = label.unwrap_or(&config.repository.url);
        eprintln!(
            "WARNING: This will permanently delete the entire repository '{repo_name}' \
             and ALL its snapshots."
        );
        eprintln!();
        eprint!("Type 'delete' to confirm: ");
        std::io::Write::flush(&mut std::io::stderr())?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if input.trim() != "delete" {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    let stats = commands::delete_repo::run(config)
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

    let repo_name = label.unwrap_or(&config.repository.url);

    if stats.unknown_entries.is_empty() {
        println!(
            "Repository '{repo_name}' deleted ({} keys removed).",
            stats.keys_deleted
        );
    } else {
        println!(
            "Repository '{repo_name}' deleted ({} keys removed, {} unknown entries left).",
            stats.keys_deleted,
            stats.unknown_entries.len()
        );
        eprintln!();
        eprintln!(
            "Warning: {} unknown entries were not removed:",
            stats.unknown_entries.len()
        );
        for entry in &stats.unknown_entries {
            eprintln!("  {entry}");
        }
    }

    if stats.is_local && !stats.root_removed && stats.unknown_entries.is_empty() {
        eprintln!(
            "Note: repository directory '{}' could not be fully removed; \
             it may contain empty directories or other non-file entries.",
            config.repository.url
        );
    }

    Ok(())
}
