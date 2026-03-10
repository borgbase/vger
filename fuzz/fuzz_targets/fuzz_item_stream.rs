#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = vykar_core::commands::list::for_each_decoded_item(data, |_item| Ok(()));
});
