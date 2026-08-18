#![allow(dead_code)]

use nx_net::{Message, MessageKind, SerializationFormat};

pub const MAX_FUZZ_MESSAGE_SIZE: usize = 1024 * 1024;

pub fn parse_raw_and_json_candidate(data: &[u8]) -> [Option<(SerializationFormat, Message)>; 2] {
    let raw = Message::from_bytes_with_format(data).ok();

    let json = if data.len() < MAX_FUZZ_MESSAGE_SIZE {
        let mut wire = Vec::with_capacity(data.len() + 1);
        wire.push(0x01);
        wire.extend_from_slice(data);
        Message::from_bytes_with_format(&wire).ok()
    } else {
        None
    };

    [raw, json]
}

pub fn assert_roundtrip(format: SerializationFormat, message: &Message) {
    let Ok(frame) = message.to_bytes_with_format(format) else {
        return;
    };
    if frame.len().saturating_sub(4) > MAX_FUZZ_MESSAGE_SIZE {
        return;
    }
    let (decoded_format, decoded) =
        Message::from_bytes_with_format(&frame[4..]).expect("a serialized message must decode");
    assert_eq!(decoded_format, format);
    assert_eq!(&decoded, message);
}

pub fn is_hello(message: &Message) -> bool {
    matches!(message.kind, MessageKind::Hello { .. })
}

pub fn is_push_ops(message: &Message) -> bool {
    matches!(message.kind, MessageKind::PushOps { .. })
}

pub fn is_pull_since(message: &Message) -> bool {
    matches!(message.kind, MessageKind::PullSince { .. })
}
