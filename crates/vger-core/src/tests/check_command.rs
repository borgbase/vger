use crate::commands;
use crate::repo::Repository;
use crate::storage::local_backend::LocalBackend;

use super::helpers::{backup_single_source, init_repo, init_test_environment};

fn open_local_repo(repo_dir: &std::path::Path) -> Repository {
    init_test_environment();
    let storage = Box::new(LocalBackend::new(repo_dir.to_str().unwrap()).unwrap());
    Repository::open(storage, None).unwrap()
}

#[test]
fn check_verify_data_flag_controls_data_verification_counters() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"check-verify-data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-check-verify");

    let without_verify = commands::check::run(&config, None, false).unwrap();
    assert!(without_verify.errors.is_empty());
    assert_eq!(without_verify.chunks_data_verified, 0);
    assert!(without_verify.chunks_existence_checked > 0);

    let with_verify = commands::check::run(&config, None, true).unwrap();
    assert!(with_verify.errors.is_empty());
    assert!(with_verify.chunks_data_verified > 0);
    assert!(with_verify.chunks_existence_checked > 0);
}

#[test]
fn check_reports_missing_pack_file_in_storage() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"check-missing-pack").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-check-pack");

    let repo = open_local_repo(&repo_dir);
    let (_chunk_id, entry) = repo.chunk_index().iter().next().unwrap();
    let pack_key = entry.pack_id.storage_key();
    let pack_path = repo_dir.join(&pack_key);
    assert!(pack_path.exists());
    std::fs::remove_file(pack_path).unwrap();

    let result = commands::check::run(&config, None, false).unwrap();
    assert!(result.chunks_existence_checked > 0);
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.message.contains("missing from storage")),
        "expected missing pack error, got: {:?}",
        result
            .errors
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
    );
}

#[test]
fn check_reports_snapshot_metadata_load_failures() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"check-missing-meta").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-check-meta");

    let repo = open_local_repo(&repo_dir);
    let entry = repo.manifest().find_snapshot("snap-check-meta").unwrap();
    let snapshot_path = repo_dir.join("snapshots").join(hex::encode(&entry.id));
    assert!(snapshot_path.exists());
    std::fs::remove_file(snapshot_path).unwrap();

    let result = commands::check::run(&config, None, false).unwrap();
    assert_eq!(result.snapshots_checked, 0);
    assert!(
        result
            .errors
            .iter()
            .any(|e| e.message.contains("failed to load metadata")),
        "expected metadata load error, got: {:?}",
        result
            .errors
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
    );
}

#[test]
fn check_with_progress_emits_phase_events() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"check-progress").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-check-progress");

    let mut events = Vec::new();
    let mut on_progress = |event| events.push(event);

    let result =
        commands::check::run_with_progress(&config, None, false, Some(&mut on_progress)).unwrap();
    assert!(result.errors.is_empty());
    assert!(!events.is_empty());
    assert!(events.iter().any(|e| matches!(
        e,
        commands::check::CheckProgressEvent::SnapshotStarted { .. }
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        commands::check::CheckProgressEvent::ChunksExistencePhaseStarted { .. }
    )));
}
