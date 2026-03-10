#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_core::snapshot::SnapshotMeta;

fuzz_target!(|data: &[u8]| {
    let _ = rmp_serde::from_slice::<SnapshotMeta>(data);
});
