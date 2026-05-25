use serde::{Deserialize, Serialize};

use crate::{GCounter, NodeId, Op, OpKind, SyncResult};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PNCounter {
    positive: GCounter,
    negative: GCounter,
}

impl PNCounter {
    /// Creates a new empty PNCounter.
    pub fn new() -> Self {
        Self {
            positive: GCounter::new(),
            negative: GCounter::new(),
        }
    }

    /// Returns the converged signed counter value.
    pub fn value(&self) -> i64 {
        let positive = self.positive.value() as i128;
        let negative = self.negative.value() as i128;
        clamp_i128_to_i64(positive - negative)
    }

    /// Returns the positive slot value for a specific node.
    pub fn positive_for(&self, node: &NodeId) -> u64 {
        self.positive.value_for(node)
    }

    /// Returns the negative slot value for a specific node.
    pub fn negative_for(&self, node: &NodeId) -> u64 {
        self.negative.value_for(node)
    }

    /// Increments the positive slot of the specified node.
    pub fn increment(&mut self, node: &NodeId, delta: u64) {
        self.positive.increment(node, delta);
    }

    /// Increments the negative slot of the specified node.
    pub fn decrement(&mut self, node: &NodeId, delta: u64) {
        self.negative.increment(node, delta);
    }

    /// Applies a PNCounter operation to this counter.
    pub fn apply_op(&mut self, op: &Op) -> SyncResult<bool> {
        match &op.kind {
            OpKind::PNCounterIncrement { key: _, increment } => {
                let before = self.positive_for(&op.origin);
                self.increment(&op.origin, *increment);
                Ok(self.positive_for(&op.origin) != before)
            }
            OpKind::PNCounterDecrement { key: _, decrement } => {
                let before = self.negative_for(&op.origin);
                self.decrement(&op.origin, *decrement);
                Ok(self.negative_for(&op.origin) != before)
            }
            OpKind::GCounterIncrement { .. } => Ok(false),
        }
    }

    /// Merges two PNCounters by merging their positive and negative GCounters.
    pub fn merge(&mut self, other: &PNCounter) {
        self.positive.merge(&other.positive);
        self.negative.merge(&other.negative);
    }

    /// Creates a merged PNCounter without mutating self.
    pub fn merged_with(&self, other: &PNCounter) -> PNCounter {
        let mut result = self.clone();
        result.merge(other);
        result
    }

    /// Returns all nodes that have contributed either positive or negative slots.
    pub fn nodes(&self) -> impl Iterator<Item = &str> {
        let mut nodes = self.positive.nodes().collect::<Vec<_>>();
        for node in self.negative.nodes() {
            if !nodes.contains(&node) {
                nodes.push(node);
            }
        }
        nodes.into_iter()
    }

    /// Returns the number of nodes that have contributed.
    pub fn node_count(&self) -> usize {
        self.nodes().count()
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

fn clamp_i128_to_i64(value: i128) -> i64 {
    value.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pncounter_new_is_zero() {
        let counter = PNCounter::new();
        assert_eq!(counter.value(), 0);
        assert_eq!(counter.node_count(), 0);
    }

    #[test]
    fn test_pncounter_increment_and_decrement() {
        let node = NodeId::new("node-a");
        let mut counter = PNCounter::new();

        counter.increment(&node, 10);
        counter.decrement(&node, 3);

        assert_eq!(counter.value(), 7);
        assert_eq!(counter.positive_for(&node), 10);
        assert_eq!(counter.negative_for(&node), 3);
    }

    #[test]
    fn test_pncounter_can_be_negative() {
        let node = NodeId::new("node-a");
        let mut counter = PNCounter::new();

        counter.decrement(&node, 5);

        assert_eq!(counter.value(), -5);
    }

    #[test]
    fn test_pncounter_multiple_nodes() {
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");
        let mut counter = PNCounter::new();

        counter.increment(&node_a, 10);
        counter.decrement(&node_b, 4);

        assert_eq!(counter.value(), 6);
        assert_eq!(counter.node_count(), 2);
        assert_eq!(counter.nodes().collect::<Vec<_>>().len(), 2);
    }

    #[test]
    fn test_pncounter_merge_takes_max_slots() {
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");
        let mut counter_a = PNCounter::new();
        let mut counter_b = PNCounter::new();

        counter_a.increment(&node_a, 10);
        counter_a.decrement(&node_b, 2);
        counter_b.increment(&node_a, 5);
        counter_b.decrement(&node_b, 8);

        counter_a.merge(&counter_b);

        assert_eq!(counter_a.positive_for(&node_a), 10);
        assert_eq!(counter_a.negative_for(&node_b), 8);
        assert_eq!(counter_a.value(), 2);
    }

    #[test]
    fn test_pncounter_merge_commutativity() {
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");
        let mut counter_a = PNCounter::new();
        let mut counter_b = PNCounter::new();

        counter_a.increment(&node_a, 10);
        counter_a.decrement(&node_b, 4);
        counter_b.increment(&node_b, 7);
        counter_b.decrement(&node_a, 2);

        let left = counter_a.merged_with(&counter_b);
        let right = counter_b.merged_with(&counter_a);

        assert_eq!(left.value(), right.value());
        assert_eq!(left.positive_for(&node_a), right.positive_for(&node_a));
        assert_eq!(left.positive_for(&node_b), right.positive_for(&node_b));
        assert_eq!(left.negative_for(&node_a), right.negative_for(&node_a));
        assert_eq!(left.negative_for(&node_b), right.negative_for(&node_b));
    }

    #[test]
    fn test_pncounter_merge_associativity() {
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");
        let node_c = NodeId::new("node-c");
        let mut counter_a = PNCounter::new();
        let mut counter_b = PNCounter::new();
        let mut counter_c = PNCounter::new();

        counter_a.increment(&node_a, 1);
        counter_b.decrement(&node_b, 2);
        counter_c.increment(&node_c, 3);

        let mut left = counter_a.merged_with(&counter_b);
        left.merge(&counter_c);

        let right_inner = counter_b.merged_with(&counter_c);
        let right = counter_a.merged_with(&right_inner);

        assert_eq!(left.value(), right.value());
        assert_eq!(left.node_count(), right.node_count());
    }

    #[test]
    fn test_pncounter_merge_idempotency() {
        let node = NodeId::new("node-a");
        let mut counter = PNCounter::new();

        counter.increment(&node, 10);
        counter.decrement(&node, 3);
        let before = counter.value();

        counter.merge(&counter.clone());

        assert_eq!(counter.value(), before);
    }

    #[test]
    fn test_pncounter_json_roundtrip() {
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");
        let mut counter = PNCounter::new();

        counter.increment(&node_a, 10);
        counter.decrement(&node_b, 4);

        let json = counter.to_json().unwrap();
        let parsed = PNCounter::from_json(&json).unwrap();

        assert_eq!(parsed.value(), 6);
        assert_eq!(parsed.positive_for(&node_a), 10);
        assert_eq!(parsed.negative_for(&node_b), 4);
    }

    #[test]
    fn test_pncounter_apply_increment_op() {
        let node = NodeId::new("node-a");
        let mut counter = PNCounter::new();
        let op = Op::pncounter_increment(node.clone(), "stock:sku-1", 10);

        let changed = counter.apply_op(&op).unwrap();

        assert!(changed);
        assert_eq!(counter.positive_for(&node), 10);
        assert_eq!(counter.value(), 10);
    }

    #[test]
    fn test_pncounter_apply_decrement_op() {
        let node = NodeId::new("node-a");
        let mut counter = PNCounter::new();
        let op = Op::pncounter_decrement(node.clone(), "stock:sku-1", 4);

        let changed = counter.apply_op(&op).unwrap();

        assert!(changed);
        assert_eq!(counter.negative_for(&node), 4);
        assert_eq!(counter.value(), -4);
    }

    #[test]
    fn test_pncounter_apply_ignores_other_op_kinds() {
        let node = NodeId::new("node-a");
        let mut counter = PNCounter::new();
        let op = Op::gcounter_increment(node, "counter:visits", 4);

        let changed = counter.apply_op(&op).unwrap();

        assert!(!changed);
        assert_eq!(counter.value(), 0);
    }

    #[test]
    fn test_pncounter_slot_overflow_saturates() {
        let node = NodeId::new("node-a");
        let mut counter = PNCounter::new();

        counter.increment(&node, u64::MAX);
        counter.increment(&node, 1);

        assert_eq!(counter.positive_for(&node), u64::MAX);
        assert_eq!(counter.value(), i64::MAX);
    }

    #[test]
    fn test_pncounter_value_clamps_to_i64_min() {
        let node = NodeId::new("node-a");
        let mut counter = PNCounter::new();

        counter.decrement(&node, u64::MAX);

        assert_eq!(counter.value(), i64::MIN);
    }
}
