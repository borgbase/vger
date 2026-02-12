use chrono::Utc;

use crate::repo::manifest::{SnapshotEntry, Manifest};

fn make_entry(name: &str) -> SnapshotEntry {
    SnapshotEntry {
        name: name.to_string(),
        id: vec![0u8; 32],
        time: Utc::now(),
    }
}

#[test]
fn new_manifest_has_no_snapshots() {
    let m = Manifest::new();
    assert!(m.snapshots.is_empty());
    assert_eq!(m.version, 1);
}

#[test]
fn find_snapshot_returns_match() {
    let mut m = Manifest::new();
    m.snapshots.push(make_entry("backup-1"));
    m.snapshots.push(make_entry("backup-2"));
    let found = m.find_snapshot("backup-2");
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "backup-2");
}

#[test]
fn find_snapshot_returns_none() {
    let m = Manifest::new();
    assert!(m.find_snapshot("nonexistent").is_none());
}

#[test]
fn remove_snapshot_removes_and_returns() {
    let mut m = Manifest::new();
    m.snapshots.push(make_entry("backup-1"));
    m.snapshots.push(make_entry("backup-2"));
    let removed = m.remove_snapshot("backup-1");
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().name, "backup-1");
    assert_eq!(m.snapshots.len(), 1);
    assert!(m.find_snapshot("backup-1").is_none());
}

#[test]
fn remove_snapshot_returns_none() {
    let mut m = Manifest::new();
    m.snapshots.push(make_entry("backup-1"));
    let removed = m.remove_snapshot("nonexistent");
    assert!(removed.is_none());
    assert_eq!(m.snapshots.len(), 1);
}

#[test]
fn serde_roundtrip() {
    let mut m = Manifest::new();
    m.snapshots.push(make_entry("backup-1"));
    m.snapshots.push(make_entry("backup-2"));

    let serialized = rmp_serde::to_vec(&m).unwrap();
    let deserialized: Manifest = rmp_serde::from_slice(&serialized).unwrap();

    assert_eq!(deserialized.version, m.version);
    assert_eq!(deserialized.snapshots.len(), 2);
    assert_eq!(deserialized.snapshots[0].name, "backup-1");
    assert_eq!(deserialized.snapshots[1].name, "backup-2");
}
