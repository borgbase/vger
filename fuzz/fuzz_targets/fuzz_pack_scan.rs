#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = vykar_core::repo::pack::scan_pack_blobs_bytes(data);
});
