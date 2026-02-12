use chrono::Utc;
use vger_core::commands;
use vger_core::compress::Compression;
use vger_core::config::{
    ChunkerConfig, CompressionConfig, EncryptionConfig, EncryptionModeConfig, RepositoryConfig,
    ResourceLimitsConfig, RetentionConfig, RetryConfig, ScheduleConfig, VgerConfig, XattrsConfig,
};
use vger_core::repo::manifest::SnapshotEntry;
use vger_core::repo::pack::PackType;
use vger_core::repo::{EncryptionMode, Repository};
use vger_core::snapshot::item::ItemType;
use vger_core::storage::opendal_backend::OpendalBackend;

fn init_local_repo(dir: &std::path::Path) -> Repository {
    let storage = Box::new(OpendalBackend::local(dir.to_str().unwrap()).unwrap());
    Repository::init(
        storage,
        EncryptionMode::None,
        ChunkerConfig::default(),
        None,
        None,
    )
    .unwrap()
}

fn open_local_repo(dir: &std::path::Path) -> Repository {
    let storage = Box::new(OpendalBackend::local(dir.to_str().unwrap()).unwrap());
    Repository::open(storage, None).unwrap()
}

fn make_test_config(repo_dir: &std::path::Path) -> VgerConfig {
    VgerConfig {
        repository: RepositoryConfig {
            url: repo_dir.to_string_lossy().to_string(),
            region: None,
            access_key_id: None,
            secret_access_key: None,
            endpoint: None,
            sftp_key: None,
            rest_token: None,
            min_pack_size: 32 * 1024 * 1024,
            max_pack_size: 512 * 1024 * 1024,
            retry: RetryConfig::default(),
        },
        encryption: EncryptionConfig {
            mode: EncryptionModeConfig::None,
            passphrase: None,
            passcommand: None,
        },
        exclude_patterns: Vec::new(),
        exclude_if_present: Vec::new(),
        one_file_system: true,
        git_ignore: false,
        chunker: ChunkerConfig::default(),
        compression: CompressionConfig::default(),
        retention: RetentionConfig::default(),
        xattrs: XattrsConfig::default(),
        schedule: ScheduleConfig::default(),
        limits: ResourceLimitsConfig::default(),
    }
}

fn xattr_test_name() -> &'static str {
    "user.vger.test"
}

fn supports_xattrs(dir: &std::path::Path) -> bool {
    let probe = dir.join(".xattr-probe");
    if std::fs::write(&probe, b"probe").is_err() {
        return false;
    }

    let name = xattr_test_name();
    let supported = match xattr::set(&probe, name, b"1") {
        Ok(()) => true,
        Err(_) => false,
    };
    let _ = xattr::remove(&probe, name);
    let _ = std::fs::remove_file(&probe);
    supported
}

#[test]
fn init_store_reopen_read() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    // Init and store chunks
    let data1 = b"chunk one data for integration test";
    let data2 = b"chunk two data for integration test";
    let (id1, id2) = {
        let mut repo = init_local_repo(dir);
        let (id1, _, _) = repo
            .store_chunk(data1, Compression::Lz4, PackType::Data)
            .unwrap();
        let (id2, _, _) = repo
            .store_chunk(data2, Compression::Lz4, PackType::Data)
            .unwrap();
        repo.save_state().unwrap();
        (id1, id2)
    };

    // Reopen and verify
    let repo = open_local_repo(dir);
    assert_eq!(repo.chunk_index.len(), 2);
    let read1 = repo.read_chunk(&id1).unwrap();
    let read2 = repo.read_chunk(&id2).unwrap();
    assert_eq!(read1, data1);
    assert_eq!(read2, data2);
}

#[test]
fn manifest_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    // Init and add a snapshot entry to manifest
    {
        let mut repo = init_local_repo(dir);
        repo.manifest.snapshots.push(SnapshotEntry {
            name: "test-snapshot".to_string(),
            id: vec![0x42; 32],
            time: Utc::now(),
            source_label: String::new(),
            label: String::new(),
            source_paths: Vec::new(),
        });
        repo.save_state().unwrap();
    }

    // Reopen and check manifest
    let repo = open_local_repo(dir);
    assert_eq!(repo.manifest.snapshots.len(), 1);
    assert_eq!(repo.manifest.snapshots[0].name, "test-snapshot");
}

#[test]
fn init_auto_mode_persists_concrete_encryption_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();

    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Auto;

    let repo = commands::init::run(&config, Some("test-passphrase")).unwrap();
    let selected = repo.config.encryption.clone();
    assert!(matches!(
        selected,
        EncryptionMode::Aes256Gcm | EncryptionMode::Chacha20Poly1305
    ));
    drop(repo);

    let storage = Box::new(OpendalBackend::local(repo_dir.to_str().unwrap()).unwrap());
    let reopened = Repository::open(storage, Some("test-passphrase")).unwrap();
    assert_eq!(reopened.config.encryption, selected);
}

#[test]
fn backup_exclude_if_present_skips_marked_directories() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(source_dir.join("keep")).unwrap();
    std::fs::create_dir_all(source_dir.join("skip")).unwrap();
    std::fs::write(source_dir.join("keep").join("keep.txt"), b"keep").unwrap();
    std::fs::write(source_dir.join("skip").join("skip.txt"), b"skip").unwrap();
    std::fs::write(source_dir.join("skip").join(".nobackup"), b"").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present = vec![".nobackup".to_string()];
    let exclude_patterns: Vec<String> = Vec::new();

    let stats = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-marker",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            label: "",
        },
    )
    .unwrap();

    assert_eq!(stats.nfiles, 1);
}

#[test]
fn backup_git_ignore_respected_when_enabled() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(source_dir.join("target")).unwrap();
    std::fs::write(source_dir.join(".gitignore"), b"target/\n").unwrap();
    std::fs::write(source_dir.join("keep.txt"), b"keep").unwrap();
    std::fs::write(source_dir.join("target").join("ignored.txt"), b"ignore me").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let stats_without_gitignore = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-no-gitignore",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            label: "",
        },
    )
    .unwrap();

    let stats_with_gitignore = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-with-gitignore",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: true,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            label: "",
        },
    )
    .unwrap();

    assert_eq!(stats_without_gitignore.nfiles, 3);
    assert_eq!(stats_with_gitignore.nfiles, 2);
}

#[test]
fn backup_deduplicates_identical_files_and_extracts_correctly() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let payload: Vec<u8> = (0u32..512 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(source_dir.join("a.bin"), &payload).unwrap();
    std::fs::write(source_dir.join("b.bin"), &payload).unwrap();

    let mut config = make_test_config(&repo_dir);
    config.chunker = ChunkerConfig {
        min_size: 8 * 1024,
        avg_size: 16 * 1024,
        max_size: 64 * 1024,
    };

    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();
    let stats = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-dedup",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            label: "",
        },
    )
    .unwrap();

    assert_eq!(stats.nfiles, 2);
    assert!(stats.deduplicated_size > 0);
    assert!(stats.deduplicated_size < stats.compressed_size);

    let repo = open_local_repo(&repo_dir);
    let items = commands::list::load_snapshot_items(&repo, "snap-dedup").unwrap();
    let file_items: Vec<_> = items
        .iter()
        .filter(|item| item.entry_type == ItemType::RegularFile)
        .collect();
    assert_eq!(file_items.len(), 2);

    let first_ids: Vec<_> = file_items[0].chunks.iter().map(|c| c.id).collect();
    let second_ids: Vec<_> = file_items[1].chunks.iter().map(|c| c.id).collect();
    assert!(!first_ids.is_empty());
    assert_eq!(first_ids, second_ids);

    for chunk_id in first_ids {
        let entry = repo.chunk_index.get(&chunk_id).unwrap();
        assert_eq!(entry.refcount, 2);
    }

    let restore_dir = tmp.path().join("restore");
    let extract_stats = commands::extract::run(
        &config,
        None,
        "snap-dedup",
        restore_dir.to_str().unwrap(),
        None,
        config.xattrs.enabled,
    )
    .unwrap();
    assert_eq!(extract_stats.files, 2);

    assert_eq!(std::fs::read(restore_dir.join("a.bin")).unwrap(), payload);
    assert_eq!(std::fs::read(restore_dir.join("b.bin")).unwrap(), payload);
}

#[test]
fn backup_run_with_progress_emits_events_and_final_stats() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("one.txt"), b"one").unwrap();
    std::fs::write(source_dir.join("two.txt"), b"two").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    let mut events = Vec::new();
    let mut on_progress = |event| events.push(event);

    let stats = commands::backup::run_with_progress(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-progress",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            label: "",
        },
        Some(&mut on_progress),
    )
    .unwrap();

    let file_started_count = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                commands::backup::BackupProgressEvent::FileStarted { .. }
            )
        })
        .count();
    assert_eq!(file_started_count, 2);

    let final_stats_event = events
        .iter()
        .rev()
        .find_map(|event| match event {
            commands::backup::BackupProgressEvent::StatsUpdated {
                nfiles,
                original_size,
                compressed_size,
                deduplicated_size,
                ..
            } => Some((
                *nfiles,
                *original_size,
                *compressed_size,
                *deduplicated_size,
            )),
            _ => None,
        })
        .expect("expected at least one StatsUpdated event");

    assert_eq!(final_stats_event.0, stats.nfiles);
    assert_eq!(final_stats_event.1, stats.original_size);
    assert_eq!(final_stats_event.2, stats.compressed_size);
    assert_eq!(final_stats_event.3, stats.deduplicated_size);
}

#[test]
fn backup_and_restore_preserves_file_xattrs_when_enabled() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    if !supports_xattrs(&source_dir) {
        return;
    }

    let source_file = source_dir.join("file.txt");
    std::fs::write(&source_file, b"hello xattrs").unwrap();

    let attr_name = xattr_test_name();
    let attr_value = b"vger-value".to_vec();
    xattr::set(&source_file, attr_name, &attr_value).unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-xattrs",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: true,
            compression: Compression::None,
            label: "",
        },
    )
    .unwrap();

    let repo = open_local_repo(&repo_dir);
    let items = commands::list::load_snapshot_items(&repo, "snap-xattrs").unwrap();
    let item = items.iter().find(|i| i.path == "file.txt").unwrap();
    let stored = item
        .xattrs
        .as_ref()
        .and_then(|map| map.get(attr_name))
        .cloned();
    assert_eq!(stored, Some(attr_value.clone()));

    let restore_dir = tmp.path().join("restore");
    commands::extract::run(
        &config,
        None,
        "snap-xattrs",
        restore_dir.to_str().unwrap(),
        None,
        true,
    )
    .unwrap();

    let restored_file = restore_dir.join("file.txt");
    let restored = xattr::get(&restored_file, attr_name).unwrap();
    assert_eq!(restored, Some(attr_value));
}

#[test]
fn backup_skips_xattrs_when_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    if !supports_xattrs(&source_dir) {
        return;
    }

    let source_file = source_dir.join("file.txt");
    std::fs::write(&source_file, b"hello xattrs").unwrap();

    let attr_name = xattr_test_name();
    xattr::set(&source_file, attr_name, b"vger-value").unwrap();

    let config = make_test_config(&repo_dir);
    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-no-xattrs",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: false,
            compression: Compression::None,
            label: "",
        },
    )
    .unwrap();

    let repo = open_local_repo(&repo_dir);
    let items = commands::list::load_snapshot_items(&repo, "snap-no-xattrs").unwrap();
    let item = items.iter().find(|i| i.path == "file.txt").unwrap();
    assert!(item.xattrs.is_none());
}

#[test]
fn file_cache_persists_and_matches_snapshot_items() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let payload_a: Vec<u8> = (0u32..256 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(source_dir.join("a.bin"), &payload_a).unwrap();
    std::fs::write(source_dir.join("b.bin"), b"small file").unwrap();

    let mut config = make_test_config(&repo_dir);
    config.chunker = ChunkerConfig {
        min_size: 8 * 1024,
        avg_size: 16 * 1024,
        max_size: 64 * 1024,
    };

    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    // First backup — populates the file cache.
    let stats1 = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-1",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            label: "",
        },
    )
    .unwrap();
    assert_eq!(stats1.nfiles, 2);
    assert!(
        stats1.deduplicated_size > 0,
        "first backup should store new data"
    );

    // Verify file cache was persisted and its chunk_refs match the snapshot.
    {
        let repo = open_local_repo(&repo_dir);
        let items = commands::list::load_snapshot_items(&repo, "snap-1").unwrap();
        let files: Vec<_> = items
            .iter()
            .filter(|i| i.entry_type == ItemType::RegularFile)
            .collect();
        assert_eq!(files.len(), 2);

        // Each file's cache entry should contain chunk_refs identical to the snapshot.
        for file_item in &files {
            let abs_path = source_dir.join(&file_item.path);
            let cache_entry = repo
                .file_cache
                .get(abs_path.to_str().unwrap())
                .unwrap_or_else(|| panic!("cache should have entry for {}", file_item.path));
            let cached_ids: Vec<_> = cache_entry.chunk_refs.iter().map(|c| c.id).collect();
            let snap_ids: Vec<_> = file_item.chunks.iter().map(|c| c.id).collect();
            assert_eq!(
                cached_ids, snap_ids,
                "cache chunk_refs should match snapshot for {}",
                file_item.path
            );
        }
    }

    // Second backup — unchanged files. Produces identical snapshot.
    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-2",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            label: "",
        },
    )
    .unwrap();

    let repo = open_local_repo(&repo_dir);
    let items1 = commands::list::load_snapshot_items(&repo, "snap-1").unwrap();
    let items2 = commands::list::load_snapshot_items(&repo, "snap-2").unwrap();
    let files1: Vec<_> = items1
        .iter()
        .filter(|i| i.entry_type == ItemType::RegularFile)
        .collect();
    let files2: Vec<_> = items2
        .iter()
        .filter(|i| i.entry_type == ItemType::RegularFile)
        .collect();
    for (f1, f2) in files1.iter().zip(files2.iter()) {
        let ids1: Vec<_> = f1.chunks.iter().map(|c| c.id).collect();
        let ids2: Vec<_> = f2.chunks.iter().map(|c| c.id).collect();
        assert_eq!(
            ids1, ids2,
            "unchanged file {} should have same chunks",
            f1.path
        );
    }

    // Every chunk should have refcount 2 (one per snapshot).
    for file_item in &files1 {
        for cr in &file_item.chunks {
            let entry = repo.chunk_index.get(&cr.id).unwrap();
            assert_eq!(entry.refcount, 2, "chunk {} refcount", cr.id);
        }
    }
}

#[test]
fn file_cache_misses_on_modified_file() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let payload: Vec<u8> = (0u32..256 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(source_dir.join("unchanged.bin"), &payload).unwrap();
    std::fs::write(source_dir.join("modified.bin"), &payload).unwrap();

    let mut config = make_test_config(&repo_dir);
    config.chunker = ChunkerConfig {
        min_size: 8 * 1024,
        avg_size: 16 * 1024,
        max_size: 64 * 1024,
    };

    commands::init::run(&config, None).unwrap();

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();

    // First backup.
    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-1",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            label: "",
        },
    )
    .unwrap();

    // Collect chunk IDs from the first snapshot.
    let snap1_chunks: std::collections::HashMap<String, Vec<_>> = {
        let repo = open_local_repo(&repo_dir);
        let items = commands::list::load_snapshot_items(&repo, "snap-1").unwrap();
        items
            .iter()
            .filter(|i| i.entry_type == ItemType::RegularFile)
            .map(|i| (i.path.clone(), i.chunks.iter().map(|c| c.id).collect()))
            .collect()
    };

    // Modify one file with completely different content.
    let new_payload: Vec<u8> = (0u32..256 * 1024).map(|i| (i % 199) as u8).collect();
    std::fs::write(source_dir.join("modified.bin"), &new_payload).unwrap();

    // Second backup — cache should miss on modified.bin, hit on unchanged.bin.
    commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-2",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            label: "",
        },
    )
    .unwrap();

    let repo = open_local_repo(&repo_dir);
    let items2 = commands::list::load_snapshot_items(&repo, "snap-2").unwrap();
    let files2: std::collections::HashMap<String, Vec<_>> = items2
        .iter()
        .filter(|i| i.entry_type == ItemType::RegularFile)
        .map(|i| (i.path.clone(), i.chunks.iter().map(|c| c.id).collect()))
        .collect();

    // unchanged.bin should have the same chunks.
    assert_eq!(
        snap1_chunks["unchanged.bin"], files2["unchanged.bin"],
        "unchanged file should keep the same chunks"
    );

    // modified.bin should have different chunks.
    assert_ne!(
        snap1_chunks["modified.bin"], files2["modified.bin"],
        "modified file should have new chunks"
    );

    // The cache should now reflect the new content for modified.bin.
    let abs_modified = source_dir.join("modified.bin");
    let cache_entry = repo
        .file_cache
        .get(abs_modified.to_str().unwrap())
        .expect("cache should have entry for modified.bin after re-backup");
    let cached_ids: Vec<_> = cache_entry.chunk_refs.iter().map(|c| c.id).collect();
    assert_eq!(
        cached_ids, files2["modified.bin"],
        "cache should be updated with new chunks for modified.bin"
    );
}

#[test]
fn info_reports_repository_statistics() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();

    let payload: Vec<u8> = (0u32..256 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(source_dir.join("a.bin"), &payload).unwrap();
    std::fs::write(source_dir.join("b.bin"), &payload).unwrap();

    let mut config = make_test_config(&repo_dir);
    config.chunker = ChunkerConfig {
        min_size: 8 * 1024,
        avg_size: 16 * 1024,
        max_size: 64 * 1024,
    };

    commands::init::run(&config, None).unwrap();

    let empty = commands::info::run(&config, None).unwrap();
    assert_eq!(empty.snapshot_count, 0);
    assert!(empty.last_snapshot_time.is_none());
    assert_eq!(empty.raw_size, 0);
    assert_eq!(empty.compressed_size, 0);
    assert_eq!(empty.deduplicated_size, 0);
    assert_eq!(empty.unique_stored_size, 0);
    assert_eq!(empty.referenced_stored_size, 0);
    assert_eq!(empty.unique_chunks, 0);

    let source_paths = vec![source_dir.to_string_lossy().to_string()];
    let exclude_if_present: Vec<String> = Vec::new();
    let exclude_patterns: Vec<String> = Vec::new();
    let backup_stats = commands::backup::run(
        &config,
        commands::backup::BackupRequest {
            snapshot_name: "snap-info",
            passphrase: None,
            source_paths: &source_paths,
            source_label: "source",
            exclude_patterns: &exclude_patterns,
            exclude_if_present: &exclude_if_present,
            one_file_system: true,
            git_ignore: false,
            xattrs_enabled: config.xattrs.enabled,
            compression: Compression::None,
            label: "",
        },
    )
    .unwrap();

    let info = commands::info::run(&config, None).unwrap();
    assert_eq!(info.snapshot_count, 1);
    assert!(info.last_snapshot_time.is_some());
    assert_eq!(info.raw_size, backup_stats.original_size);
    assert_eq!(info.compressed_size, backup_stats.compressed_size);
    assert_eq!(info.deduplicated_size, backup_stats.deduplicated_size);
    assert!(info.unique_chunks > 0);
    assert!(info.unique_stored_size > 0);
    assert!(info.referenced_stored_size >= info.unique_stored_size);
}
