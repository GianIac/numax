use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RgaElementId(String);

impl RgaElementId {
    /// Creates an element id from an existing globally unique string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the element id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RgaElement {
    id: RgaElementId,
    parent: Option<RgaElementId>,
    value: Vec<u8>,
}

impl RgaElement {
    fn new(id: RgaElementId, parent: Option<RgaElementId>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            id,
            parent,
            value: value.into(),
        }
    }

    /// Returns the globally unique id of this element.
    pub fn id(&self) -> &RgaElementId {
        &self.id
    }

    /// Returns the parent element this element was inserted after.
    pub fn parent(&self) -> Option<&RgaElementId> {
        self.parent.as_ref()
    }

    /// Returns the element payload.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns the element payload as an owned buffer.
    pub fn value_bytes(&self) -> Vec<u8> {
        self.value.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Rga {
    elements: BTreeMap<RgaElementId, RgaElement>,
    tombstones: BTreeSet<RgaElementId>,
}

impl Rga {
    /// Creates an empty replicated growable array.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts an element after `parent`.
    ///
    /// `parent = None` inserts at the head. `id` must be globally unique; the
    /// runtime should use the operation id when wiring this CRDT into sync.
    pub fn insert(
        &mut self,
        id: impl Into<String>,
        parent: Option<impl Into<String>>,
        value: impl Into<Vec<u8>>,
    ) -> bool {
        let id = RgaElementId::new(id);
        if self.elements.contains_key(&id) {
            return false;
        }

        let parent = parent.map(RgaElementId::new);
        self.elements
            .insert(id.clone(), RgaElement::new(id, parent, value));
        true
    }

    /// Applies a remote insert operation.
    pub fn apply_insert(
        &mut self,
        id: impl Into<String>,
        parent: Option<impl Into<String>>,
        value: impl Into<Vec<u8>>,
    ) -> bool {
        self.insert(id, parent, value)
    }

    /// Deletes an element by tombstoning its id.
    pub fn delete(&mut self, id: impl Into<String>) -> bool {
        self.tombstones.insert(RgaElementId::new(id))
    }

    /// Applies a remote delete operation.
    pub fn apply_delete(&mut self, id: impl Into<String>) -> bool {
        self.delete(id)
    }

    /// Returns true when an element exists and is visible.
    pub fn contains(&self, id: &str) -> bool {
        let id = RgaElementId::new(id);
        self.elements.contains_key(&id) && !self.tombstones.contains(&id)
    }

    /// Returns the visible element payloads in deterministic sequence order.
    pub fn values(&self) -> Vec<Vec<u8>> {
        self.visible_elements()
            .into_iter()
            .map(|element| element.value_bytes())
            .collect()
    }

    /// Returns visible elements in deterministic sequence order.
    pub fn visible_elements(&self) -> Vec<RgaElement> {
        self.ordered_elements()
            .into_iter()
            .filter(|element| !self.tombstones.contains(element.id()))
            .collect()
    }

    /// Returns all element ids in deterministic sequence order, including tombstones.
    pub fn ordered_ids(&self) -> Vec<String> {
        self.ordered_elements()
            .into_iter()
            .map(|element| element.id().as_str().to_string())
            .collect()
    }

    /// Merges another RGA into this one by unioning elements and tombstones.
    pub fn merge(&mut self, other: &Rga) -> bool {
        let mut changed = false;

        for (id, element) in &other.elements {
            if !self.elements.contains_key(id) {
                self.elements.insert(id.clone(), element.clone());
                changed = true;
            }
        }

        let before = self.tombstones.len();
        self.tombstones.extend(other.tombstones.iter().cloned());
        changed |= self.tombstones.len() != before;

        changed
    }

    /// Creates a merged RGA without mutating self.
    pub fn merged_with(&self, other: &Rga) -> Rga {
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

    fn ordered_elements(&self) -> Vec<RgaElement> {
        let mut children = BTreeMap::<Option<RgaElementId>, Vec<RgaElementId>>::new();
        for element in self.elements.values() {
            let parent = match element.parent() {
                Some(parent) if self.elements.contains_key(parent) => Some(parent.clone()),
                _ => None,
            };
            children
                .entry(parent)
                .or_default()
                .push(element.id().clone());
        }

        let mut ordered = Vec::new();
        append_children(None, &children, &self.elements, &mut ordered);
        ordered
    }
}

fn append_children(
    parent: Option<RgaElementId>,
    children: &BTreeMap<Option<RgaElementId>, Vec<RgaElementId>>,
    elements: &BTreeMap<RgaElementId, RgaElement>,
    out: &mut Vec<RgaElement>,
) {
    let Some(child_ids) = children.get(&parent) else {
        return;
    };

    for child_id in child_ids {
        let Some(element) = elements.get(child_id) else {
            continue;
        };
        out.push(element.clone());
        append_children(Some(child_id.clone()), children, elements, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_rga_is_empty() {
        let rga = Rga::new();

        assert!(rga.values().is_empty());
        assert!(rga.ordered_ids().is_empty());
    }

    #[test]
    fn insert_at_head_makes_element_visible() {
        let mut rga = Rga::new();

        assert!(rga.insert("op-a", None::<String>, b"a".to_vec()));

        assert!(rga.contains("op-a"));
        assert_eq!(rga.values(), vec![b"a".to_vec()]);
        assert_eq!(rga.ordered_ids(), vec!["op-a"]);
    }

    #[test]
    fn insert_after_parent_orders_element_after_parent() {
        let mut rga = Rga::new();
        rga.insert("op-a", None::<String>, b"a".to_vec());

        assert!(rga.insert("op-b", Some("op-a"), b"b".to_vec()));

        assert_eq!(rga.values(), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(rga.ordered_ids(), vec!["op-a", "op-b"]);
    }

    #[test]
    fn concurrent_children_are_ordered_by_id() {
        let mut rga = Rga::new();
        rga.insert("op-root", None::<String>, b"root".to_vec());
        rga.insert("op-c", Some("op-root"), b"c".to_vec());
        rga.insert("op-b", Some("op-root"), b"b".to_vec());

        assert_eq!(
            rga.values(),
            vec![b"root".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(rga.ordered_ids(), vec!["op-root", "op-b", "op-c"]);
    }

    #[test]
    fn duplicate_insert_id_is_idempotent() {
        let mut rga = Rga::new();

        assert!(rga.insert("op-a", None::<String>, b"a".to_vec()));
        assert!(!rga.insert("op-a", None::<String>, b"other".to_vec()));

        assert_eq!(rga.values(), vec![b"a".to_vec()]);
    }

    #[test]
    fn delete_hides_element_but_keeps_children_visible() {
        let mut rga = Rga::new();
        rga.insert("op-a", None::<String>, b"a".to_vec());
        rga.insert("op-b", Some("op-a"), b"b".to_vec());

        assert!(rga.delete("op-a"));

        assert!(!rga.contains("op-a"));
        assert!(rga.contains("op-b"));
        assert_eq!(rga.values(), vec![b"b".to_vec()]);
        assert_eq!(rga.ordered_ids(), vec!["op-a", "op-b"]);
    }

    #[test]
    fn delete_before_insert_is_remembered() {
        let mut rga = Rga::new();

        assert!(rga.delete("op-a"));
        assert!(rga.insert("op-a", None::<String>, b"a".to_vec()));

        assert!(!rga.contains("op-a"));
        assert!(rga.values().is_empty());
    }

    #[test]
    fn missing_parent_is_treated_as_head_until_parent_arrives() {
        let mut rga = Rga::new();
        rga.insert("op-b", Some("op-a"), b"b".to_vec());

        assert_eq!(rga.values(), vec![b"b".to_vec()]);

        rga.insert("op-a", None::<String>, b"a".to_vec());

        assert_eq!(rga.values(), vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn merge_is_commutative_for_same_inputs() {
        let mut a = Rga::new();
        a.insert("op-a", None::<String>, b"a".to_vec());

        let mut b = Rga::new();
        b.insert("op-b", Some("op-a"), b"b".to_vec());

        let left = a.merged_with(&b);
        let right = b.merged_with(&a);

        assert_eq!(left, right);
        assert_eq!(left.values(), vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn merge_is_associative_for_same_inputs() {
        let mut a = Rga::new();
        a.insert("op-a", None::<String>, b"a".to_vec());
        let mut b = Rga::new();
        b.insert("op-b", Some("op-a"), b"b".to_vec());
        let mut c = Rga::new();
        c.insert("op-c", Some("op-b"), b"c".to_vec());

        let mut left = a.merged_with(&b);
        left.merge(&c);

        let right_inner = b.merged_with(&c);
        let right = a.merged_with(&right_inner);

        assert_eq!(left, right);
        assert_eq!(
            left.values(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn merge_is_idempotent() {
        let mut rga = Rga::new();
        rga.insert("op-a", None::<String>, b"a".to_vec());
        rga.delete("op-a");
        let before = rga.clone();

        assert!(!rga.merge(&before));

        assert_eq!(rga, before);
    }

    #[test]
    fn json_roundtrip_preserves_elements_and_tombstones() {
        let mut rga = Rga::new();
        rga.insert("op-a", None::<String>, b"a".to_vec());
        rga.delete("op-a");

        let json = rga.to_json().unwrap();
        let parsed = Rga::from_json(&json).unwrap();

        assert_eq!(parsed, rga);
        assert!(parsed.values().is_empty());
    }
}
