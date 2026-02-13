use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use tempfile::TempDir;
use vger_core::repo::Repository;
use vger_core::storage::opendal_backend::OpendalBackend;

struct CliFixture {
    _tmp: TempDir,
    home_dir: PathBuf,
    cache_dir: PathBuf,
    repo_dir: PathBuf,
    source_a: PathBuf,
    source_b: PathBuf,
    config_path: PathBuf,
}

impl CliFixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let home_dir = tmp.path().join("home");
        let cache_dir = tmp.path().join("cache");
        let repo_dir = tmp.path().join("repo");
        let source_a = tmp.path().join("source-a");
        let source_b = tmp.path().join("source-b");
        let config_path = tmp.path().join("vger.yaml");

        std::fs::create_dir_all(&home_dir).unwrap();
        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::create_dir_all(&source_a).unwrap();
        std::fs::create_dir_all(&source_b).unwrap();

        Self {
            _tmp: tmp,
            home_dir,
            cache_dir,
            repo_dir,
            source_a,
            source_b,
            config_path,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut cmd = Command::new(vger_binary_path());
        cmd.args(args);
        cmd.env("HOME", &self.home_dir);
        cmd.env("XDG_CACHE_HOME", &self.cache_dir);
        cmd.env("NO_COLOR", "1");
        cmd.output().unwrap()
    }

    fn run_ok(&self, args: &[&str]) -> String {
        let output = self.run(args);
        if !output.status.success() {
            panic!(
                "command failed: {:?}\nstdout:\n{}\nstderr:\n{}",
                args,
                stdout(&output),
                stderr(&output)
            );
        }
        stdout(&output)
    }

    fn run_err(&self, args: &[&str]) -> (String, String) {
        let output = self.run(args);
        assert!(
            !output.status.success(),
            "command unexpectedly succeeded: {:?}\nstdout:\n{}\nstderr:\n{}",
            args,
            stdout(&output),
            stderr(&output)
        );
        (stdout(&output), stderr(&output))
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn vger_binary_path() -> PathBuf {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_vger") {
        return PathBuf::from(path);
    }

    let current_exe = std::env::current_exe().expect("failed to resolve current test binary path");
    let debug_dir = current_exe
        .parent()
        .and_then(|p| p.parent())
        .expect("unexpected test binary path layout");

    #[cfg(windows)]
    let candidate = debug_dir.join("vger.exe");
    #[cfg(not(windows))]
    let candidate = debug_dir.join("vger");

    assert!(
        candidate.exists(),
        "unable to locate vger binary at {:?}",
        candidate
    );
    candidate
}

fn yaml_quote_path(path: &Path) -> String {
    let raw = path.to_string_lossy();
    format!("\"{}\"", raw.replace('\\', "\\\\").replace('"', "\\\""))
}

fn write_plain_config(config_path: &Path, repo_dir: &Path) {
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nsources: []\n",
        yaml_quote_path(repo_dir)
    );
    std::fs::write(config_path, config).unwrap();
}

fn write_sources_config(
    config_path: &Path,
    repo_dir: &Path,
    source_a: &Path,
    source_b: &Path,
    keep_last: usize,
) {
    let config = format!(
        "repositories:\n  - url: {}\nencryption:\n  mode: none\nretention:\n  keep_last: {}\nsources:\n  - path: {}\n    label: src-a\n  - path: {}\n    label: src-b\n",
        yaml_quote_path(repo_dir),
        keep_last,
        yaml_quote_path(source_a),
        yaml_quote_path(source_b)
    );
    std::fs::write(config_path, config).unwrap();
}

fn parse_snapshot_name(output: &str) -> String {
    output
        .lines()
        .find_map(|line| line.strip_prefix("Snapshot created: "))
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| panic!("missing snapshot name in output:\n{output}"))
}

fn delete_pack_for_first_chunk(repo_dir: &Path) {
    let storage = Box::new(OpendalBackend::local(repo_dir.to_str().unwrap()).unwrap());
    let repo = Repository::open(storage, None).unwrap();
    let (_chunk_id, entry) = repo
        .chunk_index
        .iter()
        .next()
        .expect("repo must contain at least one chunk");
    let pack_path = repo_dir.join(entry.pack_id.storage_key());
    assert!(
        pack_path.exists(),
        "expected pack file to exist: {pack_path:?}"
    );
    std::fs::remove_file(pack_path).unwrap();
}

#[test]
fn cli_init_backup_list_restore_info_roundtrip() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("alpha.txt"), b"alpha file\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();
    let restore = fx._tmp.path().join("restore");
    let restore_str = restore.to_string_lossy().to_string();

    let init_out = fx.run_ok(&["--config", &cfg, "init"]);
    assert!(init_out.contains("Repository initialized at:"));

    let backup_out = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snapshot = parse_snapshot_name(&backup_out);

    let list_out = fx.run_ok(&["--config", &cfg, "list"]);
    assert!(list_out.contains(&snapshot));

    let restore_out = fx.run_ok(&[
        "--config",
        &cfg,
        "restore",
        "--snapshot",
        &snapshot,
        "--dest",
        &restore_str,
    ]);
    assert!(restore_out.contains("Restored:"));

    assert_eq!(
        std::fs::read_to_string(restore.join("alpha.txt")).unwrap(),
        "alpha file\n"
    );

    let info_out = fx.run_ok(&["--config", &cfg, "info"]);
    assert!(info_out.contains("Snapshots"));
    assert!(info_out.contains("Encryption"));
}

#[test]
fn cli_delete_dry_run_then_real_delete() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("delete.txt"), b"delete me\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    let backup_out = fx.run_ok(&["--config", &cfg, "backup", &source]);
    let snapshot = parse_snapshot_name(&backup_out);

    let dry_run_out = fx.run_ok(&["--config", &cfg, "delete", &snapshot, "--dry-run"]);
    assert!(dry_run_out.contains("Dry run: would delete snapshot"));
    assert!(dry_run_out.contains(&snapshot));

    let list_before = fx.run_ok(&["--config", &cfg, "list"]);
    assert!(list_before.contains(&snapshot));

    let delete_out = fx.run_ok(&["--config", &cfg, "delete", &snapshot]);
    assert!(delete_out.contains("Deleted snapshot"));
    assert!(delete_out.contains(&snapshot));

    let list_after = fx.run_ok(&["--config", &cfg, "list"]);
    assert!(list_after.contains("No snapshots found."));
}

#[test]
fn cli_prune_list_source_filter_and_mutation() {
    let fx = CliFixture::new();
    write_sources_config(&fx.config_path, &fx.repo_dir, &fx.source_a, &fx.source_b, 1);
    std::fs::write(fx.source_a.join("a.txt"), b"a-v1").unwrap();
    std::fs::write(fx.source_b.join("b.txt"), b"b-v1").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    fx.run_ok(&["--config", &cfg, "init"]);

    let a1_out = fx.run_ok(&["--config", &cfg, "backup", "--source", "src-a"]);
    let snap_a1 = parse_snapshot_name(&a1_out);
    std::thread::sleep(Duration::from_millis(2));

    std::fs::write(fx.source_a.join("a.txt"), b"a-v2").unwrap();
    let a2_out = fx.run_ok(&["--config", &cfg, "backup", "--source", "src-a"]);
    let snap_a2 = parse_snapshot_name(&a2_out);
    std::thread::sleep(Duration::from_millis(2));

    let b1_out = fx.run_ok(&["--config", &cfg, "backup", "--source", "src-b"]);
    let snap_b1 = parse_snapshot_name(&b1_out);

    let dry_out = fx.run_ok(&[
        "--config",
        &cfg,
        "prune",
        "--source",
        "src-a",
        "--list",
        "--dry-run",
    ]);
    assert!(dry_out.contains("keep"));
    assert!(dry_out.contains("prune"));

    let prune_out = fx.run_ok(&["--config", &cfg, "prune", "--source", "src-a", "--list"]);
    assert!(prune_out.contains("Pruned 1 snapshots"));

    let list_after = fx.run_ok(&["--config", &cfg, "list"]);
    assert!(!list_after.contains(&snap_a1));
    assert!(list_after.contains(&snap_a2));
    assert!(list_after.contains(&snap_b1));
}

#[test]
fn cli_check_verify_data_detects_tampered_pack() {
    let fx = CliFixture::new();
    write_plain_config(&fx.config_path, &fx.repo_dir);
    std::fs::write(fx.source_a.join("check.txt"), b"check data\n").unwrap();

    let cfg = fx.config_path.to_string_lossy().to_string();
    let source = fx.source_a.to_string_lossy().to_string();

    fx.run_ok(&["--config", &cfg, "init"]);
    fx.run_ok(&["--config", &cfg, "backup", &source]);

    let check_ok = fx.run_ok(&["--config", &cfg, "check", "--verify-data"]);
    assert!(check_ok.contains("0 errors"));

    delete_pack_for_first_chunk(&fx.repo_dir);
    let (stdout_err, _stderr_err) = fx.run_err(&["--config", &cfg, "check", "--verify-data"]);
    assert!(stdout_err.contains("Errors found:"));
    assert!(stdout_err.contains("missing from storage"));
}
