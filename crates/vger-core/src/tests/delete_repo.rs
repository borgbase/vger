use crate::commands;

use super::helpers::{backup_single_source, init_repo};

#[test]
fn delete_repo_removes_local_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    let source_dir = tmp.path().join("source");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::write(source_dir.join("file.txt"), b"repo-delete-data").unwrap();

    let config = init_repo(&repo_dir);
    backup_single_source(&config, &source_dir, "src-a", "snap-del-repo");

    assert!(repo_dir.join("config").exists());

    let stats = commands::delete_repo::run(&config).unwrap();
    assert!(stats.keys_deleted > 0);
    assert!(!repo_dir.exists());
}

#[test]
fn delete_repo_rejects_nonexistent_repo() {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("empty-repo");
    std::fs::create_dir_all(&repo_dir).unwrap();

    let config = super::helpers::make_test_config(&repo_dir);

    let result = commands::delete_repo::run(&config);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("not found"), "got: {err}");
}
