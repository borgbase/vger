use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use tracing::info;

use crate::archive::item::ItemType;
use crate::config::BorgConfig;
use crate::error::{BorgError, Result};
use crate::repo::Repository;
use crate::storage;

/// Run `borg-rs extract`.
pub fn run(
    config: &BorgConfig,
    passphrase: Option<&str>,
    archive_name: &str,
    dest: &str,
    pattern: Option<&str>,
) -> Result<ExtractStats> {
    let backend = storage::backend_from_config(&config.repository)?;
    let repo = Repository::open(backend, passphrase)?;

    let items = super::list::load_archive_items(&repo, archive_name)?;

    // Build optional filter pattern
    let filter = pattern.map(|p| {
        globset::GlobBuilder::new(p)
            .literal_separator(false)
            .build()
            .map(|g| g.compile_matcher())
    }).transpose()
    .map_err(|e| BorgError::Config(format!("invalid pattern: {e}")))?;

    let dest_path = Path::new(dest);
    std::fs::create_dir_all(dest_path)?;

    let mut stats = ExtractStats::default();

    // Sort: directories first so parents exist before children
    let mut sorted_items = items;
    sorted_items.sort_by(|a, b| {
        let type_ord = |t: &ItemType| match t {
            ItemType::Directory => 0,
            ItemType::Symlink => 1,
            ItemType::RegularFile => 2,
        };
        type_ord(&a.entry_type)
            .cmp(&type_ord(&b.entry_type))
            .then_with(|| a.path.cmp(&b.path))
    });

    for item in &sorted_items {
        // Apply filter
        if let Some(ref matcher) = filter {
            if !matcher.is_match(&item.path) {
                continue;
            }
        }

        let target = dest_path.join(&item.path);

        match item.entry_type {
            ItemType::Directory => {
                std::fs::create_dir_all(&target)?;
                // Set permissions (ignore errors on directories for now, re-set after)
                let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(item.mode));
                stats.dirs += 1;
            }
            ItemType::Symlink => {
                if let Some(ref link_target) = item.link_target {
                    // Remove existing if present
                    let _ = std::fs::remove_file(&target);
                    std::os::unix::fs::symlink(link_target, &target)?;
                    stats.symlinks += 1;
                }
            }
            ItemType::RegularFile => {
                // Ensure parent directory exists
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                let mut file = std::fs::File::create(&target)?;
                let mut total_bytes: u64 = 0;

                for chunk_ref in &item.chunks {
                    let chunk_data = repo.read_chunk(&chunk_ref.id)?;
                    std::io::Write::write_all(&mut file, &chunk_data)?;
                    total_bytes += chunk_data.len() as u64;
                }

                // Set permissions
                let _ = std::fs::set_permissions(
                    &target,
                    std::fs::Permissions::from_mode(item.mode),
                );

                // Set modification time
                let mtime_secs = item.mtime / 1_000_000_000;
                let mtime_nanos = (item.mtime % 1_000_000_000) as u32;
                let mtime = filetime::FileTime::from_unix_time(mtime_secs, mtime_nanos);
                let _ = filetime::set_file_mtime(&target, mtime);

                stats.files += 1;
                stats.total_bytes += total_bytes;
            }
        }
    }

    info!(
        "Extracted {} files, {} dirs, {} symlinks ({} bytes)",
        stats.files, stats.dirs, stats.symlinks, stats.total_bytes
    );

    Ok(stats)
}

#[derive(Debug, Default)]
pub struct ExtractStats {
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    pub total_bytes: u64,
}
