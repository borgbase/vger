#![no_main]
use libfuzzer_sys::fuzz_target;
use vykar_core::repo::file_cache::FileCache;

fuzz_target!(|data: &[u8]| {
    let _ = FileCache::decode_from_plaintext(data);
});
