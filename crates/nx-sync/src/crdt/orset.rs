use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ORSet {
    adds: BTreeMap<String, BTreeSet<String>>,
    removes: BTreeMap<String, BTreeSet<String>>,
}

impl ORSet {
    /// Creates an empty observed-remove set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `element` with a unique add tag.
    ///
    /// Tags must be globally unique per add operation. The runtime will use
    /// the operation id as the tag when wiring this CRDT into sync.
    pub fn add(&mut self, element: impl Into<String>, tag: impl Into<String>) -> bool {
        self.adds
            .entry(element.into())
            .or_default()
            .insert(tag.into())
    }

    /// Applies a remote add operation.
    pub fn apply_add(&mut self, element: impl Into<String>, tag: impl Into<String>) -> bool {
        self.add(element, tag)
    }

    /// Removes all currently observed add tags for `element`.
    ///
    /// The returned tags must be carried by the remove operation. Concurrent
    /// adds that were not observed by this remove remain visible after merge.
    pub fn remove(&mut self, element: &str) -> Vec<String> {
        let observed_tags = self
            .adds
            .get(element)
            .map(|tags| tags.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        self.apply_remove(element, observed_tags.iter().cloned());
        observed_tags
    }

    /// Applies a remote observed-remove operation.
    pub fn apply_remove(
        &mut self,
        element: impl Into<String>,
        observed_tags: impl IntoIterator<Item = String>,
    ) -> bool {
        let tags = self.removes.entry(element.into()).or_default();
        let before = tags.len();
        tags.extend(observed_tags);
        tags.len() != before
    }

    /// Returns true when at least one add tag for `element` has not been removed.
    pub fn contains(&self, element: &str) -> bool {
        self.visible_tags(element).next().is_some()
    }

    /// Returns visible elements in deterministic order.
    pub fn elements(&self) -> Vec<String> {
        self.adds
            .keys()
            .filter(|element| self.contains(element))
            .cloned()
            .collect()
    }

    /// Returns observed add tags for `element` in deterministic order.
    pub fn observed_tags(&self, element: &str) -> Vec<String> {
        self.adds
            .get(element)
            .map(|tags| tags.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Merges another ORSet into this one by unioning adds and removals.
    pub fn merge(&mut self, other: &ORSet) -> bool {
        let mut changed = false;

        for (element, tags) in &other.adds {
            let local_tags = self.adds.entry(element.clone()).or_default();
            let before = local_tags.len();
            local_tags.extend(tags.iter().cloned());
            changed |= local_tags.len() != before;
        }

        for (element, tags) in &other.removes {
            let local_tags = self.removes.entry(element.clone()).or_default();
            let before = local_tags.len();
            local_tags.extend(tags.iter().cloned());
            changed |= local_tags.len() != before;
        }

        changed
    }

    /// Creates a merged set without mutating self.
    pub fn merged_with(&self, other: &ORSet) -> ORSet {
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

    fn visible_tags<'a>(&'a self, element: &'a str) -> impl Iterator<Item = &'a String> {
        let removed = self.removes.get(element);
        self.adds
            .get(element)
            .into_iter()
            .flatten()
            .filter(move |tag| removed.is_none_or(|removed| !removed.contains(*tag)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_set_is_empty() {
        let set = ORSet::new();

        assert!(set.elements().is_empty());
        assert!(!set.contains("blue"));
    }

    #[test]
    fn add_makes_element_visible() {
        let mut set = ORSet::new();

        assert!(set.add("blue", "add-1"));

        assert!(set.contains("blue"));
        assert_eq!(set.elements(), vec!["blue"]);
        assert_eq!(set.observed_tags("blue"), vec!["add-1"]);
    }

    #[test]
    fn duplicate_add_tag_is_idempotent() {
        let mut set = ORSet::new();

        assert!(set.add("blue", "add-1"));
        assert!(!set.add("blue", "add-1"));

        assert_eq!(set.elements(), vec!["blue"]);
        assert_eq!(set.observed_tags("blue"), vec!["add-1"]);
    }

    #[test]
    fn remove_hides_observed_tags() {
        let mut set = ORSet::new();
        set.add("blue", "add-1");
        set.add("blue", "add-2");

        let removed = set.remove("blue");

        assert_eq!(removed, vec!["add-1", "add-2"]);
        assert!(!set.contains("blue"));
        assert!(set.elements().is_empty());
    }

    #[test]
    fn remove_of_unseen_element_is_noop() {
        let mut set = ORSet::new();

        let removed = set.remove("blue");

        assert!(removed.is_empty());
        assert!(!set.contains("blue"));
    }

    #[test]
    fn concurrent_add_survives_observed_remove() {
        let mut left = ORSet::new();
        left.add("blue", "add-a");

        let mut right = left.clone();
        let removed = left.remove("blue");
        right.add("blue", "add-b");

        left.merge(&right);
        right.apply_remove("blue", removed);

        assert!(left.contains("blue"));
        assert!(right.contains("blue"));
        assert_eq!(left.elements(), right.elements());
    }

    #[test]
    fn remove_after_observing_all_tags_hides_element() {
        let mut left = ORSet::new();
        left.add("blue", "add-a");

        let mut right = ORSet::new();
        right.add("blue", "add-b");

        left.merge(&right);
        let removed = left.remove("blue");
        right.apply_remove("blue", removed);

        assert!(!left.contains("blue"));
        assert!(!right.contains("blue"));
    }

    #[test]
    fn merge_is_commutative_for_same_inputs() {
        let mut a = ORSet::new();
        a.add("blue", "add-a");
        let mut b = ORSet::new();
        b.add("red", "add-b");

        let left = a.merged_with(&b);
        let right = b.merged_with(&a);

        assert_eq!(left, right);
        assert_eq!(left.elements(), vec!["blue", "red"]);
    }

    #[test]
    fn merge_is_associative_for_same_inputs() {
        let mut a = ORSet::new();
        a.add("blue", "add-a");
        let mut b = ORSet::new();
        b.add("red", "add-b");
        let mut c = ORSet::new();
        c.add("green", "add-c");

        let mut left = a.merged_with(&b);
        left.merge(&c);

        let right_inner = b.merged_with(&c);
        let right = a.merged_with(&right_inner);

        assert_eq!(left, right);
        assert_eq!(left.elements(), vec!["blue", "green", "red"]);
    }

    #[test]
    fn merge_is_idempotent() {
        let mut set = ORSet::new();
        set.add("blue", "add-a");
        let before = set.clone();

        assert!(!set.merge(&before));

        assert_eq!(set, before);
    }

    #[test]
    fn json_roundtrip_preserves_set() {
        let mut set = ORSet::new();
        set.add("blue", "add-a");
        set.add("red", "add-b");
        set.remove("blue");

        let json = set.to_json().unwrap();
        let parsed = ORSet::from_json(&json).unwrap();

        assert_eq!(parsed, set);
        assert_eq!(parsed.elements(), vec!["red"]);
    }
}
