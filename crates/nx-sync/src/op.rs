use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

use crate::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OpId(String);

impl OpId {
    /// Generates a new unique OpId.
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Creates an OpId from an existing string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OpId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpKind {
    /// GCounter increment.
    GCounterIncrement { key: String, increment: u64 },
}

/// A complete CRDT operation, ready to be sent/received.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Op {
    /// Unique identifier of the operation.
    pub id: OpId,

    /// Node that originated the operation.
    pub origin: NodeId,

    /// Type and data of the operation.
    pub kind: OpKind,
}

impl Op {
    /// Creates a new GCounterIncrement operation.
    pub fn gcounter_increment(origin: NodeId, key: impl Into<String>, increment: u64) -> Self {
        Self {
            id: OpId::generate(),
            origin,
            kind: OpKind::GCounterIncrement {
                key: key.into(),
                increment,
            },
        }
    }

    /// Serializes the operation to JSON.
    ///
    /// TODO(phase4-serialization): add method for bincode/msgpack
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserializes an operation from JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serializes to bytes (JSON for now).
    ///
    /// TODO(phase4-serialization): when switching to bincode, change here.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserializes from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_op_id_unique() {
        let id1 = OpId::generate();
        let id2 = OpId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_op_gcounter_increment() {
        let node = NodeId::new("node-1");
        let op = Op::gcounter_increment(node.clone(), "counter:visits", 5);

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::GCounterIncrement { key, increment } => {
                assert_eq!(key, "counter:visits");
                assert_eq!(*increment, 5);
            }
        }
    }

    #[test]
    fn test_op_json_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::gcounter_increment(node, "counter:test", 42);

        let json = op.to_json().unwrap();
        println!("Op JSON: {}", json); // useful for debugging

        let parsed = Op::from_json(&json).unwrap();
        assert_eq!(op.origin, parsed.origin);
        assert_eq!(op.kind, parsed.kind);
        // id will only differ if we regenerate, but here it's the same
    }

    #[test]
    fn test_op_bytes_roundtrip() {
        let node = NodeId::new("test-node");
        let op = Op::gcounter_increment(node, "key", 100);

        let bytes = op.to_bytes().unwrap();
        let parsed = Op::from_bytes(&bytes).unwrap();

        assert_eq!(op, parsed);
    }
}