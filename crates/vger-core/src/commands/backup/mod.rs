mod chunk_process;
mod command_dump;
mod commit;
mod concurrency;
pub(crate) mod pipeline;
mod sequential;
mod walk;

pub(crate) use chunk_process::WorkerChunk;

use chrono::Utc;
use tracing::{info, warn};

use super::util::with_repo_lock;
use crate::compress::Compression;
use crate::config::{ChunkerConfig, CommandDump, VgerConfig};
use crate::limits::{self, ByteRateLimiter};
use crate::platform::fs;
use crate::repo::file_cache::FileCache;
use crate::repo::format::{pack_object_with_context, ObjectType};
use crate::repo::manifest::SnapshotEntry;
use crate::repo::pack::PackType;
use crate::repo::Repository;
use crate::snapshot::item::Item;
use crate::snapshot::{SnapshotMeta, SnapshotStats};
use crate::storage;
use vger_types::chunk_id::ChunkId;
use vger_types::error::{Result, VgerError};
use vger_types::snapshot_id::SnapshotId;

use walk::items_chunker_config;

pub(crate) fn flush_item_stream_chunk(
    repo: &mut Repository,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    compression: Compression,
) -> Result<()> {
    if item_stream.is_empty() {
        return Ok(());
    }
    let chunk_data = std::mem::take(item_stream);
    let (chunk_id, _csize, _is_new) = repo.store_chunk(&chunk_data, compression, PackType::Tree)?;
    item_ptrs.push(chunk_id);
    Ok(())
}

pub(crate) fn append_item_to_stream(
    repo: &mut Repository,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    item: &Item,
    items_config: &ChunkerConfig,
    compression: Compression,
) -> Result<()> {
    rmp_serde::encode::write(item_stream, item)?;
    if item_stream.len() >= items_config.avg_size as usize {
        flush_item_stream_chunk(repo, item_stream, item_ptrs, compression)?;
    }
    Ok(())
}

/// Result of a backup run, containing stats and partial-success flag.
#[derive(Debug, Clone)]
pub struct BackupOutcome {
    pub stats: SnapshotStats,
    /// `true` when one or more files were skipped due to soft errors.
    pub is_partial: bool,
}

#[derive(Debug, Clone)]
pub enum BackupProgressEvent {
    SourceStarted {
        source_path: String,
    },
    SourceFinished {
        source_path: String,
    },
    FileStarted {
        path: String,
    },
    StatsUpdated {
        nfiles: u64,
        original_size: u64,
        compressed_size: u64,
        deduplicated_size: u64,
        errors: u64,
        current_file: Option<String>,
    },
}

pub(crate) fn emit_progress(
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    event: BackupProgressEvent,
) {
    if let Some(callback) = progress.as_deref_mut() {
        callback(event);
    }
}

pub(crate) fn emit_stats_progress(
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    stats: &SnapshotStats,
    current_file: Option<String>,
) {
    emit_progress(
        progress,
        BackupProgressEvent::StatsUpdated {
            nfiles: stats.nfiles,
            original_size: stats.original_size,
            compressed_size: stats.compressed_size,
            deduplicated_size: stats.deduplicated_size,
            errors: stats.errors,
            current_file,
        },
    );
}

/// Run `vger backup` for one or more source directories.
pub struct BackupRequest<'a> {
    pub snapshot_name: &'a str,
    pub passphrase: Option<&'a str>,
    pub source_paths: &'a [String],
    pub source_label: &'a str,
    pub exclude_patterns: &'a [String],
    pub exclude_if_present: &'a [String],
    pub one_file_system: bool,
    pub git_ignore: bool,
    pub xattrs_enabled: bool,
    pub compression: Compression,
    pub command_dumps: &'a [CommandDump],
}

pub fn run(config: &VgerConfig, req: BackupRequest<'_>) -> Result<BackupOutcome> {
    run_with_progress(config, req, None)
}

pub fn run_with_progress(
    config: &VgerConfig,
    req: BackupRequest<'_>,
    mut progress: Option<&mut dyn FnMut(BackupProgressEvent)>,
) -> Result<BackupOutcome> {
    let snapshot_name = req.snapshot_name;
    let passphrase = req.passphrase;
    let source_paths = req.source_paths;
    let source_label = req.source_label;
    let exclude_patterns = req.exclude_patterns;
    let exclude_if_present = req.exclude_if_present;
    let one_file_system = req.one_file_system;
    let git_ignore = req.git_ignore;
    let xattrs_enabled = if req.xattrs_enabled && !fs::xattrs_supported() {
        warn!("xattrs requested but not supported on this platform; continuing without xattrs");
        false
    } else {
        req.xattrs_enabled
    };
    let compression = req.compression;
    let command_dumps = req.command_dumps;

    if source_paths.is_empty() && command_dumps.is_empty() {
        return Err(VgerError::Other(
            "no source paths or command dumps specified".into(),
        ));
    }
    if one_file_system && !cfg!(unix) {
        warn!("one_file_system filtering has limited support on this platform");
    }

    let multi_path = source_paths.len() > 1;

    let _nice_guard = match limits::NiceGuard::apply(config.limits.cpu.nice) {
        Ok(guard) => guard,
        Err(e) => {
            warn!(
                "could not apply limits.cpu.nice={}: {e}",
                config.limits.cpu.nice
            );
            None
        }
    };
    let read_limiter = ByteRateLimiter::from_mib_per_sec(config.limits.io.read_mib_per_sec);
    let max_pending_transform_bytes = config.limits.cpu.transform_batch_bytes();
    let max_pending_file_actions = config.limits.cpu.max_pending_actions();
    let upload_concurrency = config.limits.cpu.upload_concurrency();
    let pipeline_depth = config.limits.cpu.effective_pipeline_depth();

    // Resolve effective worker count before building the rayon pool so we
    // can right-size it in pipeline mode (avoids 2× thread oversubscription).
    let num_workers = if config.limits.cpu.max_threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2)
    } else {
        config.limits.cpu.max_threads
    };

    let transform_pool = if pipeline_depth > 0 {
        // Pipeline mode doesn't need a rayon pool (no inline large-file processing).
        None
    } else {
        sequential::build_transform_pool(config.limits.cpu.max_threads)?
    };

    let backend = storage::backend_from_config(&config.repository)?;
    let backend =
        limits::wrap_backup_storage_backend(backend, &config.repository.url, &config.limits)?;
    let mut repo = Repository::open(
        backend,
        passphrase,
        super::util::cache_dir_from_config(config),
    )?;

    with_repo_lock(&mut repo, |repo| {
        // Check snapshot name is unique while holding the lock.
        if repo.manifest().find_snapshot(snapshot_name).is_some() {
            return Err(VgerError::SnapshotAlreadyExists(snapshot_name.into()));
        }

        // Recover chunk→pack mappings from a previous interrupted session.
        // Must happen before enable_tiered_dedup_mode() which drops the full
        // chunk index — recovered entries go into a separate map.
        match repo.recover_pending_index() {
            Ok(0) => {}
            Ok(n) => {
                info!(
                    recovered_chunks = n,
                    "recovered pending index from interrupted session"
                );
            }
            Err(e) => {
                warn!("failed to recover pending index: {e}");
            }
        }

        // Pre-sanitize stale file-cache entries whose chunks were pruned by
        // delete+compact. Must happen before enable_dedup_mode() which drops
        // the full chunk index. We temporarily take the cache to avoid
        // simultaneous mutable + immutable borrows of `repo`.
        //
        // INVARIANT: After this step, all remaining cache entries reference only
        // chunks present in the index. Cache-hit paths rely on this and skip
        // per-file existence checks for throughput.
        {
            let mut cache = repo.take_file_cache();
            let pruned = cache.prune_stale_entries(&|id| repo.chunk_exists(id));
            if pruned > 0 {
                info!(
                    pruned_entries = pruned,
                    "removed stale file cache entries referencing pruned chunks"
                );
            }
            repo.restore_file_cache(cache);
        }

        // Switch to tiered dedup mode to minimize memory during backup.
        // Uses mmap'd cache + xor filter when available, falls back to
        // DedupIndex HashMap on first backup or after index changes.
        // The full index is reloaded and updated at save_state time.
        repo.enable_tiered_dedup_mode();
        let dedup_filter = repo.dedup_filter();

        let time_start = Utc::now();
        let mut stats = SnapshotStats::default();
        let mut item_stream = Vec::new();
        let mut item_ptrs: Vec<ChunkId> = Vec::new();
        let items_config = items_chunker_config();
        let mut new_file_cache = FileCache::with_capacity(repo.file_cache().len());

        // Execute command dumps before walking filesystem
        command_dump::process_command_dumps(
            repo,
            command_dumps,
            compression,
            &items_config,
            &mut item_stream,
            &mut item_ptrs,
            &mut stats,
            &mut progress,
            time_start,
        )?;

        // Apply configurable upload concurrency.
        repo.set_max_in_flight_uploads(upload_concurrency);

        let pipeline_buffer_bytes = config.limits.cpu.pipeline_buffer_bytes();

        if pipeline_depth > 0 && !source_paths.is_empty() {
            // Parallel pipeline: walk → parallel_map (read+chunk+hash+compress+encrypt)
            // → readahead → sequential consumer (dedup + pack commit).
            let file_cache_snapshot = repo.take_file_cache();
            let crypto = std::sync::Arc::clone(&repo.crypto);
            let configured_segment = config.limits.cpu.segment_size_bytes();
            let segment_size = configured_segment.min(pipeline_buffer_bytes) as u64;
            if configured_segment > segment_size as usize {
                warn!(
                    configured = configured_segment,
                    clamped_to = %segment_size,
                    "segment_size clamped to pipeline_buffer_bytes"
                );
            }

            pipeline::run_parallel_pipeline(
                repo,
                source_paths,
                multi_path,
                exclude_patterns,
                exclude_if_present,
                one_file_system,
                git_ignore,
                xattrs_enabled,
                &file_cache_snapshot,
                &crypto,
                compression,
                read_limiter.as_deref(),
                num_workers,
                pipeline_depth,
                segment_size,
                &items_config,
                &mut item_stream,
                &mut item_ptrs,
                &mut stats,
                &mut new_file_cache,
                &mut progress,
                pipeline_buffer_bytes,
                dedup_filter.as_deref(),
            )?;
        } else {
            // Sequential fallback (pipeline_depth == 0 or no source paths).
            for source_path in source_paths {
                sequential::process_source_path(
                    repo,
                    source_path,
                    multi_path,
                    exclude_patterns,
                    exclude_if_present,
                    one_file_system,
                    git_ignore,
                    xattrs_enabled,
                    compression,
                    &items_config,
                    &mut item_stream,
                    &mut item_ptrs,
                    &mut stats,
                    &mut new_file_cache,
                    max_pending_transform_bytes,
                    max_pending_file_actions,
                    read_limiter.as_deref(),
                    transform_pool.as_ref(),
                    &mut progress,
                    dedup_filter.as_deref(),
                )?;
            }
        }
        flush_item_stream_chunk(repo, &mut item_stream, &mut item_ptrs, compression)?;

        let time_end = Utc::now();

        // Build snapshot metadata
        let hostname = crate::platform::hostname();
        let username = std::env::var("USER").unwrap_or_else(|_| "unknown".into());

        let snapshot_meta = SnapshotMeta {
            name: snapshot_name.to_string(),
            hostname,
            username,
            time: time_start,
            time_end,
            chunker_params: repo.config.chunker_params.clone(),
            comment: String::new(),
            item_ptrs,
            stats: stats.clone(),
            source_label: source_label.to_string(),
            source_paths: source_paths.to_vec(),
            label: String::new(),
        };

        // Generate snapshot ID and store
        let snapshot_id = SnapshotId::generate();

        let meta_bytes = rmp_serde::to_vec(&snapshot_meta)?;
        let meta_packed = pack_object_with_context(
            ObjectType::SnapshotMeta,
            snapshot_id.as_bytes(),
            &meta_bytes,
            repo.crypto.as_ref(),
        )?;
        repo.storage.put(&snapshot_id.storage_key(), &meta_packed)?;

        // Update manifest
        repo.manifest_mut().timestamp = Utc::now();
        repo.manifest_mut().snapshots.push(SnapshotEntry {
            name: snapshot_name.to_string(),
            id: snapshot_id,
            time: time_start,
            source_label: source_label.to_string(),
            label: String::new(),
            source_paths: source_paths.to_vec(),
        });

        // Replace file cache with the freshly-built one (drops stale entries).
        repo.set_file_cache(new_file_cache);

        // Save manifest, index, and file cache
        repo.save_state()?;

        // Clean up the pending_index now that all entries are in the persisted
        // index. Best-effort — a stale pending_index is harmless.
        repo.clear_pending_index();

        if stats.errors > 0 {
            info!(
                "Snapshot '{}' created: {} files, {} errors, {} original, {} compressed, {} deduplicated",
                snapshot_name,
                stats.nfiles,
                stats.errors,
                stats.original_size,
                stats.compressed_size,
                stats.deduplicated_size
            );
        } else {
            info!(
                "Snapshot '{}' created: {} files, {} original, {} compressed, {} deduplicated",
                snapshot_name,
                stats.nfiles,
                stats.original_size,
                stats.compressed_size,
                stats.deduplicated_size
            );
        }

        let is_partial = stats.errors > 0;
        Ok(BackupOutcome { stats, is_partial })
    })
}
