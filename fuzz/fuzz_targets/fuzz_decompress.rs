#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = vykar_core::compress::decompress(data);
    let _ = vykar_core::compress::decompress_metadata(data);
});
