#![no_main]

mod common;

use libfuzzer_sys::fuzz_target;
use nx_net::Message;

fuzz_target!(|data: &[u8]| {
    let _ = Message::from_frame_bytes(data, common::MAX_FUZZ_MESSAGE_SIZE);

    if data.len() >= common::MAX_FUZZ_MESSAGE_SIZE {
        return;
    }

    let payload_len = data.len() + 1;
    let mut valid_length_frame = Vec::with_capacity(payload_len + 4);
    valid_length_frame.extend_from_slice(&(payload_len as u32).to_be_bytes());
    valid_length_frame.push(0x01);
    valid_length_frame.extend_from_slice(data);
    let _ = Message::from_frame_bytes(&valid_length_frame, common::MAX_FUZZ_MESSAGE_SIZE);
});
