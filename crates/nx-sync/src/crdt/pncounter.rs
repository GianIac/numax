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

    /// Returns the difference of the full slot totals, clamped to the i64 range.
    pub fn value(&self) -> i64 {
        let positive = self.positive.total();
        let negative = self.negative.total();
        if positive >= negative {
            (positive - negative).min(i64::MAX as u128) as i64
        } else {
            let magnitude = negative - positive;
            if magnitude > i64::MAX as u128 {
                i64::MIN
            } else {
                -(magnitude as i64)
            }
        }
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
            OpKind::GCounterIncrement { .. }
            | OpKind::LwwRegisterSet { .. }
            | OpKind::LwwMapSet { .. }
            | OpKind::LwwMapRemove { .. }
            | OpKind::ORSetAdd { .. }
            | OpKind::ORSetRemove { .. }
            | OpKind::RgaInsert { .. }
            | OpKind::RgaDelete { .. } => Ok(false),
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

    #[test]
    fn test_pncounter_cancels_totals_over_u64_before_clamping() {
        for (positive, negative, expected) in [
            (0, 0, 0),
            (1, 0, 1),
            (0, 1, -1),
            (i64::MAX as u64, 0, i64::MAX),
            (i64::MAX as u64 + 1, 0, i64::MAX),
            (0, i64::MAX as u64, -i64::MAX),
            (0, i64::MAX as u64 + 1, i64::MIN),
            (0, u64::MAX, i64::MIN),
            (u64::MAX, u64::MAX - 7, 7),
            (u64::MAX - 7, u64::MAX, -7),
        ] {
            let mut counter = PNCounter::new();
            for node in [NodeId::new("a"), NodeId::new("b")] {
                counter.increment(&node, u64::MAX);
                counter.decrement(&node, u64::MAX);
            }
            counter.increment(&NodeId::new("c"), positive);
            counter.decrement(&NodeId::new("d"), negative);
            assert_eq!(counter.value(), expected, "P={positive}, N={negative}");
            let restored = PNCounter::from_json(&counter.to_json().unwrap()).unwrap();
            assert_eq!(restored.value(), expected);
            assert_eq!(restored.positive_for(&NodeId::new("c")), positive);
            assert_eq!(restored.negative_for(&NodeId::new("d")), negative);
        }
    }

    #[test]
    fn test_pncounter_merge_and_apply_cancel_totals_over_u64() {
        let a = NodeId::new("a");
        let b = NodeId::new("b");
        let mut left = PNCounter::new();
        left.increment(&a, u64::MAX);
        left.decrement(&a, u64::MAX);
        let mut right = PNCounter::new();
        right.increment(&b, u64::MAX);
        right.decrement(&b, u64::MAX - 5);

        let mut merged = left.merged_with(&right);
        assert_eq!(merged.value(), 5);
        assert_eq!(
            serde_json::to_value(&merged).unwrap(),
            serde_json::to_value(right.merged_with(&left)).unwrap()
        );
        merged.merge(&right);
        assert_eq!(merged.value(), 5);
        assert_eq!(merged.positive_for(&b), u64::MAX);
        assert_eq!(merged.negative_for(&b), u64::MAX - 5);

        assert!(
            merged
                .apply_op(&Op::pncounter_decrement(b.clone(), "pn", 3))
                .unwrap()
        );
        assert_eq!(merged.value(), 2);
        assert!(
            merged
                .apply_op(&Op::pncounter_increment(NodeId::new("c"), "pn", 7))
                .unwrap()
        );
        assert_eq!(merged.value(), 9);
        assert!(
            !merged
                .apply_op(&Op::pncounter_increment(a, "pn", 1))
                .unwrap()
        );
        assert!(
            merged
                .apply_op(&Op::pncounter_decrement(b.clone(), "pn", 9))
                .unwrap()
        );
        assert_eq!(merged.negative_for(&b), u64::MAX);
        assert_eq!(merged.value(), 7);
        assert!(
            !merged
                .apply_op(&Op::pncounter_decrement(b, "pn", 1))
                .unwrap()
        );
    }
}
