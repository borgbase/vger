#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_core::index::IndexBlob;

fuzz_target!(|data: &[u8]| {
    let _ = rmp_serde::from_slice::<IndexBlob>(data);
});
