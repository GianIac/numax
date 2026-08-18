#![no_main]

mod common;

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use tokio::runtime::{Builder, Runtime};

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("Tokio runtime must initialize")
    })
}

fn read_with_production_framing(bytes: &[u8]) {
    let _ = runtime().block_on(nx_net::read_wire_message_for_fuzzing(
        bytes,
        common::MAX_FUZZ_MESSAGE_SIZE,
    ));
}

fuzz_target!(|data: &[u8]| {
    read_with_production_framing(data);

    if data.len() >= common::MAX_FUZZ_MESSAGE_SIZE {
        return;
    }

    let payload_len = data.len() + 1;
    let mut valid_length_frame = Vec::with_capacity(payload_len + 4);
    valid_length_frame.extend_from_slice(&(payload_len as u32).to_be_bytes());
    valid_length_frame.push(0x01);
    valid_length_frame.extend_from_slice(data);
    read_with_production_framing(&valid_length_frame);
});
