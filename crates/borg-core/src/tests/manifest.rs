use chrono::Utc;

use crate::repo::manifest::{ArchiveEntry, Manifest};

fn make_entry(name: &str) -> ArchiveEntry {
    ArchiveEntry {
        name: name.to_string(),
        id: vec![0u8; 32],
        time: Utc::now(),
    }
}

#[test]
fn new_manifest_has_no_archives() {
    let m = Manifest::new();
    assert!(m.archives.is_empty());
    assert_eq!(m.version, 1);
}

#[test]
fn find_archive_returns_match() {
    let mut m = Manifest::new();
    m.archives.push(make_entry("backup-1"));
    m.archives.push(make_entry("backup-2"));
    let found = m.find_archive("backup-2");
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "backup-2");
}

#[test]
fn find_archive_returns_none() {
    let m = Manifest::new();
    assert!(m.find_archive("nonexistent").is_none());
}

#[test]
fn remove_archive_removes_and_returns() {
    let mut m = Manifest::new();
    m.archives.push(make_entry("backup-1"));
    m.archives.push(make_entry("backup-2"));
    let removed = m.remove_archive("backup-1");
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().name, "backup-1");
    assert_eq!(m.archives.len(), 1);
    assert!(m.find_archive("backup-1").is_none());
}

#[test]
fn remove_archive_returns_none() {
    let mut m = Manifest::new();
    m.archives.push(make_entry("backup-1"));
    let removed = m.remove_archive("nonexistent");
    assert!(removed.is_none());
    assert_eq!(m.archives.len(), 1);
}

#[test]
fn serde_roundtrip() {
    let mut m = Manifest::new();
    m.archives.push(make_entry("backup-1"));
    m.archives.push(make_entry("backup-2"));

    let serialized = rmp_serde::to_vec(&m).unwrap();
    let deserialized: Manifest = rmp_serde::from_slice(&serialized).unwrap();

    assert_eq!(deserialized.version, m.version);
    assert_eq!(deserialized.archives.len(), 2);
    assert_eq!(deserialized.archives[0].name, "backup-1");
    assert_eq!(deserialized.archives[1].name, "backup-2");
}
