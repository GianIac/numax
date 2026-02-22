//! Messaggi del protocollo nx-net.
//!
//! Formato wire: lunghezza (4 bytes big-endian) + JSON payload.

use nx_sync::{NodeId, Op};
use serde::{Deserialize, Serialize};

/// Versione del protocollo.
pub const PROTOCOL_VERSION: u32 = 1;

/// Tipo di messaggio.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageKind {
    /// Handshake iniziale.
    Hello { node_id: NodeId, version: u32 },

    /// Risposta a Hello.
    HelloAck { node_id: NodeId, version: u32 },

    /// Invia operazioni CRDT.
    PushOps { ops: Vec<Op> },

    /// Conferma ricezione ops.
    PushOpsAck { received_count: usize },

    /// Richiedi operazioni da un certo punto.
    /// `since_op_id` è l'ultimo op_id conosciuto (None = voglio tutto).
    PullSince { since_op_id: Option<String> },

    /// Ping per keepalive.
    Ping,

    /// Risposta a Ping.
    Pong,
}

/// Messaggio completo con metadata.
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

    /// Serializza in bytes (length-prefixed JSON).
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        let len = (json.len() as u32).to_be_bytes();
        let mut buf = Vec::with_capacity(4 + json.len());
        buf.extend_from_slice(&len);
        buf.extend_from_slice(&json);
        Ok(buf)
    }

    /// Deserializza da JSON bytes (senza prefisso lunghezza).
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
