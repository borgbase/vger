use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use tracing::info;

use crate::config::VgerConfig;
use crate::error::{Result, VgerError};
use crate::platform::fs;
use crate::repo::Repository;
use crate::snapshot::item::ItemType;
use crate::storage;

/// Run `vger extract`.
pub fn run(
    config: &VgerConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    pattern: Option<&str>,
    xattrs_enabled: bool,
) -> Result<ExtractStats> {
    let backend = storage::backend_from_config(&config.repository, None)?;
    let repo = Repository::open(backend, passphrase)?;
    let xattrs_enabled = if xattrs_enabled && !fs::xattrs_supported() {
        tracing::warn!(
            "xattrs requested but not supported on this platform; continuing without xattrs"
        );
        false
    } else {
        xattrs_enabled
    };

    let items = super::list::load_snapshot_items(&repo, snapshot_name)?;

    // Build optional filter pattern
    let filter = pattern
        .map(|p| {
            globset::GlobBuilder::new(p)
                .literal_separator(false)
                .build()
                .map(|g| g.compile_matcher())
        })
        .transpose()
        .map_err(|e| VgerError::Config(format!("invalid pattern: {e}")))?;

    let dest_path = Path::new(dest);
    std::fs::create_dir_all(dest_path)?;
    let dest_root = dest_path
        .canonicalize()
        .map_err(|e| VgerError::Other(format!("invalid destination '{}': {e}", dest)))?;

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

        let rel = sanitize_item_path(&item.path)?;
        let target = dest_root.join(&rel);

        match item.entry_type {
            ItemType::Directory => {
                ensure_path_within_root(&target, &dest_root)?;
                std::fs::create_dir_all(&target)?;
                ensure_path_within_root(&target, &dest_root)?;
                // Set permissions (ignore errors on directories for now, re-set after)
                let _ = fs::apply_mode(&target, item.mode);
                if xattrs_enabled {
                    apply_item_xattrs(&target, item.xattrs.as_ref());
                }
                stats.dirs += 1;
            }
            ItemType::Symlink => {
                if let Some(ref link_target) = item.link_target {
                    ensure_parent_exists_within_root(&target, &dest_root)?;
                    // Remove existing if present
                    let _ = std::fs::remove_file(&target);
                    fs::create_symlink(Path::new(link_target), &target)?;
                    if xattrs_enabled {
                        apply_item_xattrs(&target, item.xattrs.as_ref());
                    }
                    stats.symlinks += 1;
                }
            }
            ItemType::RegularFile => {
                // Ensure parent directory exists
                ensure_parent_exists_within_root(&target, &dest_root)?;

                let mut file = std::fs::File::create(&target)?;
                let mut total_bytes: u64 = 0;

                for chunk_ref in &item.chunks {
                    let chunk_data = repo.read_chunk(&chunk_ref.id)?;
                    std::io::Write::write_all(&mut file, &chunk_data)?;
                    total_bytes += chunk_data.len() as u64;
                }

                // Set permissions
                let _ = fs::apply_mode(&target, item.mode);
                if xattrs_enabled {
                    apply_item_xattrs(&target, item.xattrs.as_ref());
                }

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

/// Run `vger extract` for a selected set of paths.
///
/// An item is included if its path exactly matches an entry in `selected_paths`,
/// or if any prefix of its path matches (i.e. a parent directory was selected).
pub fn run_selected(
    config: &VgerConfig,
    passphrase: Option<&str>,
    snapshot_name: &str,
    dest: &str,
    selected_paths: &HashSet<String>,
    xattrs_enabled: bool,
) -> Result<ExtractStats> {
    let backend = crate::storage::backend_from_config(&config.repository, None)?;
    let repo = Repository::open(backend, passphrase)?;
    let xattrs_enabled = if xattrs_enabled && !fs::xattrs_supported() {
        tracing::warn!(
            "xattrs requested but not supported on this platform; continuing without xattrs"
        );
        false
    } else {
        xattrs_enabled
    };

    let items = super::list::load_snapshot_items(&repo, snapshot_name)?;

    let dest_path = Path::new(dest);
    std::fs::create_dir_all(dest_path)?;
    let dest_root = dest_path
        .canonicalize()
        .map_err(|e| VgerError::Other(format!("invalid destination '{}': {e}", dest)))?;

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
        // Check if this item is selected or under a selected parent
        if !path_matches_selection(&item.path, selected_paths) {
            continue;
        }

        let rel = sanitize_item_path(&item.path)?;
        let target = dest_root.join(&rel);

        match item.entry_type {
            ItemType::Directory => {
                ensure_path_within_root(&target, &dest_root)?;
                std::fs::create_dir_all(&target)?;
                ensure_path_within_root(&target, &dest_root)?;
                let _ = fs::apply_mode(&target, item.mode);
                if xattrs_enabled {
                    apply_item_xattrs(&target, item.xattrs.as_ref());
                }
                stats.dirs += 1;
            }
            ItemType::Symlink => {
                if let Some(ref link_target) = item.link_target {
                    ensure_parent_exists_within_root(&target, &dest_root)?;
                    let _ = std::fs::remove_file(&target);
                    fs::create_symlink(Path::new(link_target), &target)?;
                    if xattrs_enabled {
                        apply_item_xattrs(&target, item.xattrs.as_ref());
                    }
                    stats.symlinks += 1;
                }
            }
            ItemType::RegularFile => {
                ensure_parent_exists_within_root(&target, &dest_root)?;

                let mut file = std::fs::File::create(&target)?;
                let mut total_bytes: u64 = 0;

                for chunk_ref in &item.chunks {
                    let chunk_data = repo.read_chunk(&chunk_ref.id)?;
                    std::io::Write::write_all(&mut file, &chunk_data)?;
                    total_bytes += chunk_data.len() as u64;
                }

                let _ = fs::apply_mode(&target, item.mode);
                if xattrs_enabled {
                    apply_item_xattrs(&target, item.xattrs.as_ref());
                }

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
        "Extracted {} files, {} dirs, {} symlinks ({} bytes) [selected]",
        stats.files, stats.dirs, stats.symlinks, stats.total_bytes
    );

    Ok(stats)
}

/// Check if a path matches the selection set.
/// A path matches if it's exactly in the set, or any prefix (ancestor) is in the set.
fn path_matches_selection(path: &str, selected: &HashSet<String>) -> bool {
    if selected.contains(path) {
        return true;
    }
    // Check if any ancestor is selected
    let mut current = path;
    while let Some(slash_idx) = current.rfind('/') {
        current = &current[..slash_idx];
        if selected.contains(current) {
            return true;
        }
    }
    false
}

fn apply_item_xattrs(target: &Path, xattrs: Option<&HashMap<String, Vec<u8>>>) {
    let Some(xattrs) = xattrs else {
        return;
    };

    let mut names: Vec<&str> = xattrs.keys().map(String::as_str).collect();
    names.sort_unstable();

    for name in names {
        let Some(value) = xattrs.get(name) else {
            continue;
        };

        #[cfg(unix)]
        if let Err(e) = xattr::set(target, name, value) {
            tracing::warn!(
                path = %target.display(),
                attr = %name,
                error = %e,
                "failed to restore extended attribute"
            );
        }
        #[cfg(not(unix))]
        {
            let _ = target;
            let _ = name;
            let _ = value;
        }
    }
}

#[derive(Debug, Default)]
pub struct ExtractStats {
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    pub total_bytes: u64,
}

fn sanitize_item_path(raw: &str) -> Result<PathBuf> {
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(VgerError::InvalidFormat(format!(
            "refusing to extract absolute path: {raw}"
        )));
    }
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(VgerError::InvalidFormat(format!(
                    "refusing to extract unsafe path: {raw}"
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(VgerError::InvalidFormat(format!(
            "refusing to extract empty path: {raw}"
        )));
    }
    Ok(out)
}

fn ensure_parent_exists_within_root(target: &Path, root: &Path) -> Result<()> {
    if let Some(parent) = target.parent() {
        ensure_path_within_root(parent, root)?;
        std::fs::create_dir_all(parent)?;
        ensure_path_within_root(parent, root)?;
    }
    Ok(())
}

fn ensure_path_within_root(path: &Path, root: &Path) -> Result<()> {
    let mut cursor = Some(path);
    while let Some(candidate) = cursor {
        if candidate.exists() {
            let canonical = candidate
                .canonicalize()
                .map_err(|e| VgerError::Other(format!("path check failed: {e}")))?;
            if !canonical.starts_with(root) {
                return Err(VgerError::InvalidFormat(format!(
                    "refusing to extract outside destination: {}",
                    path.display()
                )));
            }
            return Ok(());
        }
        cursor = candidate.parent();
    }
    Err(VgerError::InvalidFormat(format!(
        "invalid extraction target path: {}",
        path.display()
    )))
}
