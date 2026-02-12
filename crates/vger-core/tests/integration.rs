use chrono::Utc;
use vger_core::commands;
use vger_core::compress::Compression;
use vger_core::config::{
    ChunkerConfig, CompressionConfig, EncryptionConfig, EncryptionModeConfig, RepositoryConfig,
    RetentionConfig, RetryConfig, VgerConfig,
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
    }
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
