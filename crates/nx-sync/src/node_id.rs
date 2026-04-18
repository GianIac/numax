use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    /// Creates a new NodeId from a string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Generates a random NodeId (UUID v4).
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Returns the NodeId as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_generate_unique() {
        let id1 = NodeId::generate();
        let id2 = NodeId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_node_id_from_string() {
        let id = NodeId::new("node-1");
        assert_eq!(id.as_str(), "node-1");
    }

    #[test]
    fn test_node_id_serde_roundtrip() {
        let id = NodeId::new("test-node");
        let json = serde_json::to_string(&id).unwrap();
        let parsed: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, parsed);
    }
}