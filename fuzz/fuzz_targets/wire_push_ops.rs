#![no_main]

mod common;

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    for (format, message) in common::parse_raw_and_json_candidate(data)
        .into_iter()
        .flatten()
    {
        if common::is_push_ops(&message) {
            common::assert_roundtrip(format, &message);
        }
    }
});
