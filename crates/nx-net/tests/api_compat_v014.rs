use std::time::Duration;

use nx_net::{Message, MessageKind, NetError, NodeConfig, SerializationFormat, WireError};
use nx_sync::NodeId;

fn match_message_kind(kind: &MessageKind) {
    match kind {
        MessageKind::Hello { .. }
        | MessageKind::HelloAck { .. }
        | MessageKind::PushOps { .. }
        | MessageKind::PushOpsAck { .. }
        | MessageKind::PullSince { .. }
        | MessageKind::Ping
        | MessageKind::Pong
        | MessageKind::Error { .. } => {}
    }
}

fn match_wire_error(error: &WireError) {
    match error {
        WireError::ProtocolMismatch { .. }
        | WireError::OpRejected { .. }
        | WireError::RateLimited { .. }
        | WireError::NotAuthorized { .. }
        | WireError::Internal { .. } => {}
    }
}

fn match_net_error(error: &NetError) {
    match error {
        NetError::Io(_)
        | NetError::Serialization(_)
        | NetError::BinarySerialization(_)
        | NetError::BinaryDeserialization(_)
        | NetError::ConnectionFailed(_)
        | NetError::PeerDisconnected(_)
        | NetError::InvalidMessage(_)
        | NetError::Wire(_)
        | NetError::MessageTooLarge { .. }
        | NetError::Timeout
        | NetError::ChannelClosed
        | NetError::TlsError(_)
        | NetError::PeerNotAllowed(_)
        | NetError::PeerLimitReached(_)
        | NetError::NodeIdMismatch { .. } => {}
    }
}

#[test]
fn public_v014_surface_remains_source_compatible() {
    let config = NodeConfig {
        node_id: NodeId::new("compat"),
        listen_addr: "127.0.0.1:0".into(),
        initial_peers: Vec::new(),
        tls: None,
        max_peers: 8,
        max_message_size: 1024,
        socket_timeout: Duration::from_secs(1),
        serialization_format: SerializationFormat::Bincode,
        event_channel_capacity: 8,
    };
    let _node = nx_net::Node::new(config);

    let message = Message::ping();
    match_message_kind(&message.kind);
    match_wire_error(&WireError::Internal {
        reason: "compat".into(),
    });
    match_net_error(&NetError::Timeout);

    message.to_bytes().unwrap();
    message.to_json_bytes().unwrap();
}
