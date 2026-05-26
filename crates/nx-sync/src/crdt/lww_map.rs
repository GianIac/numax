use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LwwMapEntry {
    value: Option<Vec<u8>>,
    timestamp_ms: u64,
    writer: NodeId,
}

impl LwwMapEntry {
    fn present(value: impl Into<Vec<u8>>, timestamp_ms: u64, writer: NodeId) -> Self {
        Self {
            value: Some(value.into()),
            timestamp_ms,
            writer,
        }
    }

    fn tombstone(timestamp_ms: u64, writer: NodeId) -> Self {
        Self {
            value: None,
            timestamp_ms,
            writer,
        }
    }

    /// Returns the visible value, or `None` when this entry is a tombstone.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Returns the visible value as an owned buffer.
    pub fn value_bytes(&self) -> Option<Vec<u8>> {
        self.value.clone()
    }

    /// Returns true when the entry currently has a visible value.
    pub fn is_visible(&self) -> bool {
        self.value.is_some()
    }

    /// Returns the write timestamp in Unix epoch milliseconds.
    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    /// Returns the writer that produced the winning entry.
    pub fn writer(&self) -> &NodeId {
        &self.writer
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LwwMap {
    entries: BTreeMap<String, LwwMapEntry>,
}

impl LwwMap {
    /// Creates an empty LWW map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a field if the write wins the field-level LWW ordering.
    pub fn set(
        &mut self,
        field: impl Into<String>,
        value: impl Into<Vec<u8>>,
        timestamp_ms: u64,
        writer: NodeId,
    ) -> bool {
        let field = field.into();
        let candidate = LwwMapEntry::present(value, timestamp_ms, writer);
        self.apply_entry(field, candidate)
    }

    /// Removes a field if the tombstone wins the field-level LWW ordering.
    pub fn remove(
        &mut self,
        field: impl Into<String>,
        timestamp_ms: u64,
        writer: NodeId,
    ) -> bool {
        let field = field.into();
        let candidate = LwwMapEntry::tombstone(timestamp_ms, writer);
        self.apply_entry(field, candidate)
    }

    /// Returns the visible value for a field.
    pub fn get(&self, field: &str) -> Option<&[u8]> {
        self.entries.get(field).and_then(LwwMapEntry::value)
    }

    /// Returns the visible value for a field as an owned buffer.
    pub fn get_bytes(&self, field: &str) -> Option<Vec<u8>> {
        self.entries.get(field).and_then(LwwMapEntry::value_bytes)
    }

    /// Returns true when a field currently has a visible value.
    pub fn contains(&self, field: &str) -> bool {
        self.get(field).is_some()
    }

    /// Returns the winning entry metadata for a field, including tombstones.
    pub fn entry(&self, field: &str) -> Option<&LwwMapEntry> {
        self.entries.get(field)
    }

    /// Returns visible entries in deterministic field order.
    pub fn entries(&self) -> Vec<(String, Vec<u8>)> {
        self.entries
            .iter()
            .filter_map(|(field, entry)| entry.value_bytes().map(|value| (field.clone(), value)))
            .collect()
    }

    /// Merges another map into this one using deterministic per-field LWW ordering.
    pub fn merge(&mut self, other: &LwwMap) -> bool {
        let mut changed = false;
        for (field, entry) in &other.entries {
            changed |= self.apply_entry(field.clone(), entry.clone());
        }
        changed
    }

    /// Creates a merged map without mutating self.
    pub fn merged_with(&self, other: &LwwMap) -> LwwMap {
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

    fn apply_entry(&mut self, field: String, candidate: LwwMapEntry) -> bool {
        match self.entries.get(&field) {
            Some(current) if !entry_wins(&candidate, current) => false,
            _ => {
                self.entries.insert(field, candidate);
                true
            }
        }
    }
}

fn entry_wins(candidate: &LwwMapEntry, current: &LwwMapEntry) -> bool {
    candidate.timestamp_ms > current.timestamp_ms
        || (candidate.timestamp_ms == current.timestamp_ms
            && candidate.writer.as_str() > current.writer.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_map_has_no_visible_entries() {
        let map = LwwMap::new();

        assert!(!map.contains("theme"));
        assert_eq!(map.entries(), Vec::<(String, Vec<u8>)>::new());
    }

    #[test]
    fn set_makes_field_visible() {
        let mut map = LwwMap::new();

        assert!(map.set("theme", b"dark".to_vec(), 100, NodeId::new("node-a")));

        assert!(map.contains("theme"));
        assert_eq!(map.get_bytes("theme"), Some(b"dark".to_vec()));
        assert_eq!(map.entry("theme").unwrap().timestamp_ms(), 100);
        assert_eq!(map.entry("theme").unwrap().writer().as_str(), "node-a");
    }

    #[test]
    fn newer_set_wins() {
        let mut map = LwwMap::new();
        map.set("theme", b"dark".to_vec(), 100, NodeId::new("node-a"));

        assert!(map.set("theme", b"light".to_vec(), 200, NodeId::new("node-b")));

        assert_eq!(map.get_bytes("theme"), Some(b"light".to_vec()));
        assert_eq!(map.entry("theme").unwrap().writer().as_str(), "node-b");
    }

    #[test]
    fn older_set_is_ignored() {
        let mut map = LwwMap::new();
        map.set("theme", b"light".to_vec(), 200, NodeId::new("node-b"));

        assert!(!map.set("theme", b"dark".to_vec(), 100, NodeId::new("node-a")));

        assert_eq!(map.get_bytes("theme"), Some(b"light".to_vec()));
    }

    #[test]
    fn equal_timestamp_uses_writer_tie_breaker() {
        let mut map = LwwMap::new();
        map.set("theme", b"dark".to_vec(), 100, NodeId::new("node-a"));

        assert!(map.set("theme", b"light".to_vec(), 100, NodeId::new("node-b")));

        assert_eq!(map.get_bytes("theme"), Some(b"light".to_vec()));
        assert_eq!(map.entry("theme").unwrap().writer().as_str(), "node-b");
    }

    #[test]
    fn remove_hides_field_and_keeps_tombstone_metadata() {
        let mut map = LwwMap::new();
        map.set("theme", b"dark".to_vec(), 100, NodeId::new("node-a"));

        assert!(map.remove("theme", 200, NodeId::new("node-b")));

        assert!(!map.contains("theme"));
        assert_eq!(map.get_bytes("theme"), None);
        assert!(!map.entry("theme").unwrap().is_visible());
        assert_eq!(map.entry("theme").unwrap().timestamp_ms(), 200);
    }

    #[test]
    fn older_remove_cannot_hide_newer_set() {
        let mut map = LwwMap::new();
        map.set("theme", b"dark".to_vec(), 200, NodeId::new("node-a"));

        assert!(!map.remove("theme", 100, NodeId::new("node-b")));

        assert_eq!(map.get_bytes("theme"), Some(b"dark".to_vec()));
    }

    #[test]
    fn newer_set_can_resurrect_removed_field() {
        let mut map = LwwMap::new();
        map.set("theme", b"dark".to_vec(), 100, NodeId::new("node-a"));
        map.remove("theme", 200, NodeId::new("node-b"));

        assert!(map.set("theme", b"light".to_vec(), 300, NodeId::new("node-a")));

        assert_eq!(map.get_bytes("theme"), Some(b"light".to_vec()));
    }

    #[test]
    fn entries_are_sorted_and_skip_tombstones() {
        let mut map = LwwMap::new();
        map.set("region", b"eu".to_vec(), 100, NodeId::new("node-a"));
        map.set("theme", b"dark".to_vec(), 100, NodeId::new("node-a"));
        map.set("feature", b"on".to_vec(), 100, NodeId::new("node-a"));
        map.remove("theme", 200, NodeId::new("node-a"));

        assert_eq!(
            map.entries(),
            vec![
                ("feature".to_string(), b"on".to_vec()),
                ("region".to_string(), b"eu".to_vec()),
            ]
        );
    }

    #[test]
    fn merge_is_commutative_for_same_inputs() {
        let mut a = LwwMap::new();
        a.set("theme", b"dark".to_vec(), 100, NodeId::new("node-a"));

        let mut b = LwwMap::new();
        b.set("theme", b"light".to_vec(), 200, NodeId::new("node-b"));
        b.set("region", b"eu".to_vec(), 100, NodeId::new("node-b"));

        let left = a.merged_with(&b);
        let right = b.merged_with(&a);

        assert_eq!(left, right);
        assert_eq!(left.get_bytes("theme"), Some(b"light".to_vec()));
        assert_eq!(left.get_bytes("region"), Some(b"eu".to_vec()));
    }

    #[test]
    fn merge_is_associative_for_same_inputs() {
        let mut a = LwwMap::new();
        a.set("theme", b"dark".to_vec(), 100, NodeId::new("node-a"));

        let mut b = LwwMap::new();
        b.set("theme", b"light".to_vec(), 200, NodeId::new("node-b"));

        let mut c = LwwMap::new();
        c.remove("theme", 300, NodeId::new("node-c"));

        let mut left = a.merged_with(&b);
        left.merge(&c);

        let right_inner = b.merged_with(&c);
        let right = a.merged_with(&right_inner);

        assert_eq!(left, right);
        assert!(!left.contains("theme"));
    }

    #[test]
    fn merge_is_idempotent() {
        let mut map = LwwMap::new();
        map.set("theme", b"dark".to_vec(), 100, NodeId::new("node-a"));
        let before = map.clone();

        assert!(!map.merge(&before));

        assert_eq!(map, before);
    }

    #[test]
    fn json_roundtrip_preserves_visible_entries_and_tombstones() {
        let mut map = LwwMap::new();
        map.set("theme", b"dark".to_vec(), 100, NodeId::new("node-a"));
        map.remove("region", 200, NodeId::new("node-b"));

        let json = map.to_json().unwrap();
        let parsed = LwwMap::from_json(&json).unwrap();

        assert_eq!(parsed, map);
        assert_eq!(parsed.get_bytes("theme"), Some(b"dark".to_vec()));
        assert!(!parsed.entry("region").unwrap().is_visible());
    }
}
