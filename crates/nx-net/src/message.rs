use nx_sync::{NodeId, Op};
use serde::{Deserialize, Serialize};

/// Protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Message type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageKind {
    /// Initial handshake.
    Hello { node_id: NodeId, version: u32 },

    /// Response to Hello.
    HelloAck { node_id: NodeId, version: u32 },

    /// Send CRDT operations.
    PushOps { ops: Vec<Op> },

    /// Acknowledge ops reception.
    PushOpsAck { received_count: usize },

    /// Request operations from a certain point.
    /// `since_op_id` is the last known op_id (None = I want everything).
    PullSince { since_op_id: Option<String> },

    /// Ping for keepalive.
    Ping,

    /// Response to Ping.
    Pong,
}

/// Complete message with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub kind: MessageKind,
}

impl Message {
    pub fn hello(node_id: NodeId) -> Self {
        Self {
            kind: MessageKind::Hello {
                node_id,
                version: PROTOCOL_VERSION,
            },
        }
    }

    pub fn hello_ack(node_id: NodeId) -> Self {
        Self {
            kind: MessageKind::HelloAck {
                node_id,
                version: PROTOCOL_VERSION,
            },
        }
    }

    pub fn push_ops(ops: Vec<Op>) -> Self {
        Self {
            kind: MessageKind::PushOps { ops },
        }
    }

    pub fn push_ops_ack(received_count: usize) -> Self {
        Self {
            kind: MessageKind::PushOpsAck { received_count },
        }
    }

    pub fn pull_since(since_op_id: Option<String>) -> Self {
        Self {
            kind: MessageKind::PullSince { since_op_id },
        }
    }

    pub fn ping() -> Self {
        Self {
            kind: MessageKind::Ping,
        }
    }

    pub fn pong() -> Self {
        Self {
            kind: MessageKind::Pong,
        }
    }

    /// Serialize to bytes (length-prefixed JSON).
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        let len = (json.len() as u32).to_be_bytes();
        let mut buf = Vec::with_capacity(4 + json.len());
        buf.extend_from_slice(&len);
        buf.extend_from_slice(&json);
        Ok(buf)
    }

    /// Deserialize from JSON bytes (without length prefix).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hello_message() {
        let node_id = NodeId::new("test-node");
        let msg = Message::hello(node_id.clone());

        match &msg.kind {
            MessageKind::Hello {
                node_id: id,
                version,
            } => {
                assert_eq!(id, &node_id);
                assert_eq!(*version, PROTOCOL_VERSION);
            }
            _ => panic!("wrong message kind"),
        }
    }

    #[test]
    fn test_message_roundtrip() {
        let node_id = NodeId::new("node-1");
        let msg = Message::hello(node_id);

        let bytes = msg.to_bytes().unwrap();

        // Skip 4-byte length prefix
        let parsed = Message::from_bytes(&bytes[4..]).unwrap();

        match (&msg.kind, &parsed.kind) {
            (MessageKind::Hello { node_id: id1, .. }, MessageKind::Hello { node_id: id2, .. }) => {
                assert_eq!(id1, id2);
            }
            _ => panic!("mismatch"),
        }
    }

    #[test]
    fn test_push_ops_message() {
        let node = NodeId::new("node-1");
        let op = Op::gcounter_increment(node, "counter:test", 5);
        let msg = Message::push_ops(vec![op]);

        match &msg.kind {
            MessageKind::PushOps { ops } => {
                assert_eq!(ops.len(), 1);
            }
            _ => panic!("wrong kind"),
        }
    }
}
