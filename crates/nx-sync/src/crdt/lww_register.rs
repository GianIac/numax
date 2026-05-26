use serde::{Deserialize, Serialize};

use crate::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LwwRegister {
    value: Vec<u8>,
    timestamp_ms: u64,
    writer: NodeId,
}

impl LwwRegister {
    /// Creates a register value written by `writer` at `timestamp_ms`.
    pub fn new(value: impl Into<Vec<u8>>, timestamp_ms: u64, writer: NodeId) -> Self {
        Self {
            value: value.into(),
            timestamp_ms,
            writer,
        }
    }

    /// Returns the current value.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns the current value as an owned buffer.
    pub fn value_bytes(&self) -> Vec<u8> {
        self.value.clone()
    }

    /// Returns the write timestamp in Unix epoch milliseconds.
    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    /// Returns the writer that produced the winning value.
    pub fn writer(&self) -> &NodeId {
        &self.writer
    }

    /// Replaces the register if the candidate wins the LWW ordering.
    pub fn assign(&mut self, value: impl Into<Vec<u8>>, timestamp_ms: u64, writer: NodeId) -> bool {
        let candidate = Self::new(value, timestamp_ms, writer);
        if candidate_wins(&candidate, self) {
            *self = candidate;
            true
        } else {
            false
        }
    }

    /// Merges another register into this one using deterministic LWW ordering.
    pub fn merge(&mut self, other: &LwwRegister) -> bool {
        if candidate_wins(other, self) {
            *self = other.clone();
            true
        } else {
            false
        }
    }

    /// Creates a merged register without mutating self.
    pub fn merged_with(&self, other: &LwwRegister) -> LwwRegister {
        let mut result = self.clone();
        result.merge(other);
        result
    }

    /// Serializes to JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserializes from JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

fn candidate_wins(candidate: &LwwRegister, current: &LwwRegister) -> bool {
    candidate.timestamp_ms > current.timestamp_ms
        || (candidate.timestamp_ms == current.timestamp_ms
            && candidate.writer.as_str() > current.writer.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lww_register_new_exposes_value_timestamp_and_writer() {
        let writer = NodeId::new("node-a");
        let register = LwwRegister::new(b"online".to_vec(), 100, writer.clone());

        assert_eq!(register.value(), b"online");
        assert_eq!(register.value_bytes(), b"online".to_vec());
        assert_eq!(register.timestamp_ms(), 100);
        assert_eq!(register.writer(), &writer);
    }

    #[test]
    fn merge_takes_newer_timestamp() {
        let mut left = LwwRegister::new(b"old".to_vec(), 100, NodeId::new("node-a"));
        let right = LwwRegister::new(b"new".to_vec(), 200, NodeId::new("node-b"));

        assert!(left.merge(&right));

        assert_eq!(left.value(), b"new");
        assert_eq!(left.timestamp_ms(), 200);
        assert_eq!(left.writer().as_str(), "node-b");
    }

    #[test]
    fn merge_ignores_older_timestamp() {
        let mut left = LwwRegister::new(b"new".to_vec(), 200, NodeId::new("node-a"));
        let right = LwwRegister::new(b"old".to_vec(), 100, NodeId::new("node-b"));

        assert!(!left.merge(&right));

        assert_eq!(left.value(), b"new");
        assert_eq!(left.timestamp_ms(), 200);
        assert_eq!(left.writer().as_str(), "node-a");
    }

    #[test]
    fn equal_timestamp_uses_writer_tie_breaker() {
        let mut left = LwwRegister::new(b"a".to_vec(), 100, NodeId::new("node-a"));
        let right = LwwRegister::new(b"b".to_vec(), 100, NodeId::new("node-b"));

        assert!(left.merge(&right));

        assert_eq!(left.value(), b"b");
        assert_eq!(left.writer().as_str(), "node-b");
    }

    #[test]
    fn equal_timestamp_losing_writer_is_ignored() {
        let mut left = LwwRegister::new(b"b".to_vec(), 100, NodeId::new("node-b"));
        let right = LwwRegister::new(b"a".to_vec(), 100, NodeId::new("node-a"));

        assert!(!left.merge(&right));

        assert_eq!(left.value(), b"b");
        assert_eq!(left.writer().as_str(), "node-b");
    }

    #[test]
    fn assign_uses_same_ordering() {
        let mut register = LwwRegister::new(b"old".to_vec(), 100, NodeId::new("node-a"));

        assert!(!register.assign(b"older".to_vec(), 99, NodeId::new("node-z")));
        assert!(register.assign(b"newer".to_vec(), 101, NodeId::new("node-a")));

        assert_eq!(register.value(), b"newer");
        assert_eq!(register.timestamp_ms(), 101);
    }

    #[test]
    fn merge_is_commutative_for_same_inputs() {
        let a = LwwRegister::new(b"a".to_vec(), 100, NodeId::new("node-a"));
        let b = LwwRegister::new(b"b".to_vec(), 200, NodeId::new("node-b"));

        let left = a.merged_with(&b);
        let right = b.merged_with(&a);

        assert_eq!(left, right);
        assert_eq!(left.value(), b"b");
    }

    #[test]
    fn merge_is_associative_for_same_inputs() {
        let a = LwwRegister::new(b"a".to_vec(), 100, NodeId::new("node-a"));
        let b = LwwRegister::new(b"b".to_vec(), 200, NodeId::new("node-b"));
        let c = LwwRegister::new(b"c".to_vec(), 200, NodeId::new("node-c"));

        let mut left = a.merged_with(&b);
        left.merge(&c);

        let right_inner = b.merged_with(&c);
        let right = a.merged_with(&right_inner);

        assert_eq!(left, right);
        assert_eq!(left.value(), b"c");
        assert_eq!(left.writer().as_str(), "node-c");
    }

    #[test]
    fn merge_is_idempotent() {
        let mut register = LwwRegister::new(b"value".to_vec(), 100, NodeId::new("node-a"));
        let before = register.clone();

        assert!(!register.merge(&before));

        assert_eq!(register, before);
    }

    #[test]
    fn json_roundtrip_preserves_register() {
        let register = LwwRegister::new(b"value".to_vec(), 100, NodeId::new("node-a"));

        let json = register.to_json().unwrap();
        let parsed = LwwRegister::from_json(&json).unwrap();

        assert_eq!(parsed, register);
    }
}
