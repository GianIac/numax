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
    /// PNCounter positive-side increment.
    PNCounterIncrement { key: String, increment: u64 },
    /// PNCounter negative-side increment.
    PNCounterDecrement { key: String, decrement: u64 },
    /// LWW-Register assignment.
    LwwRegisterSet {
        key: String,
        value: Vec<u8>,
        timestamp_ms: u64,
    },
    /// LWW-Map field assignment.
    LwwMapSet {
        key: String,
        field: String,
        value: Vec<u8>,
        timestamp_ms: u64,
    },
    /// LWW-Map field removal.
    LwwMapRemove {
        key: String,
        field: String,
        timestamp_ms: u64,
    },
    /// ORSet observed add.
    ORSetAdd {
        key: String,
        element: String,
        tag: String,
    },
    /// ORSet observed remove.
    ORSetRemove {
        key: String,
        element: String,
        observed_tags: Vec<String>,
    },
    /// RGA insert.
    RgaInsert {
        key: String,
        id: String,
        parent: Option<String>,
        value: Vec<u8>,
    },
    /// RGA delete.
    RgaDelete { key: String, id: String },
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

    /// Creates a new PNCounterIncrement operation.
    pub fn pncounter_increment(origin: NodeId, key: impl Into<String>, increment: u64) -> Self {
        Self {
            id: OpId::generate(),
            origin,
            kind: OpKind::PNCounterIncrement {
                key: key.into(),
                increment,
            },
        }
    }

    /// Creates a new PNCounterDecrement operation.
    pub fn pncounter_decrement(origin: NodeId, key: impl Into<String>, decrement: u64) -> Self {
        Self {
            id: OpId::generate(),
            origin,
            kind: OpKind::PNCounterDecrement {
                key: key.into(),
                decrement,
            },
        }
    }

    /// Creates a new LwwRegisterSet operation.
    pub fn lww_register_set(
        origin: NodeId,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
        timestamp_ms: u64,
    ) -> Self {
        Self {
            id: OpId::generate(),
            origin,
            kind: OpKind::LwwRegisterSet {
                key: key.into(),
                value: value.into(),
                timestamp_ms,
            },
        }
    }

    /// Creates a new LwwMapSet operation.
    pub fn lww_map_set(
        origin: NodeId,
        key: impl Into<String>,
        field: impl Into<String>,
        value: impl Into<Vec<u8>>,
        timestamp_ms: u64,
    ) -> Self {
        Self {
            id: OpId::generate(),
            origin,
            kind: OpKind::LwwMapSet {
                key: key.into(),
                field: field.into(),
                value: value.into(),
                timestamp_ms,
            },
        }
    }

    /// Creates a new LwwMapRemove operation.
    pub fn lww_map_remove(
        origin: NodeId,
        key: impl Into<String>,
        field: impl Into<String>,
        timestamp_ms: u64,
    ) -> Self {
        Self {
            id: OpId::generate(),
            origin,
            kind: OpKind::LwwMapRemove {
                key: key.into(),
                field: field.into(),
                timestamp_ms,
            },
        }
    }

    /// Creates a new ORSetAdd operation with an explicit add tag.
    pub fn orset_add(
        origin: NodeId,
        key: impl Into<String>,
        element: impl Into<String>,
        tag: impl Into<String>,
    ) -> Self {
        Self {
            id: OpId::generate(),
            origin,
            kind: OpKind::ORSetAdd {
                key: key.into(),
                element: element.into(),
                tag: tag.into(),
            },
        }
    }

    /// Creates a new ORSetAdd operation using the generated OpId as add tag.
    pub fn orset_add_with_op_id_tag(
        origin: NodeId,
        key: impl Into<String>,
        element: impl Into<String>,
    ) -> Self {
        let id = OpId::generate();
        let tag = id.as_str().to_string();
        Self {
            id,
            origin,
            kind: OpKind::ORSetAdd {
                key: key.into(),
                element: element.into(),
                tag,
            },
        }
    }

    /// Creates a new ORSetRemove operation carrying the tags observed locally.
    pub fn orset_remove(
        origin: NodeId,
        key: impl Into<String>,
        element: impl Into<String>,
        observed_tags: impl Into<Vec<String>>,
    ) -> Self {
        Self {
            id: OpId::generate(),
            origin,
            kind: OpKind::ORSetRemove {
                key: key.into(),
                element: element.into(),
                observed_tags: observed_tags.into(),
            },
        }
    }

    /// Creates a new RgaInsert operation with an explicit element id.
    pub fn rga_insert(
        origin: NodeId,
        key: impl Into<String>,
        id: impl Into<String>,
        parent: Option<impl Into<String>>,
        value: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            id: OpId::generate(),
            origin,
            kind: OpKind::RgaInsert {
                key: key.into(),
                id: id.into(),
                parent: parent.map(Into::into),
                value: value.into(),
            },
        }
    }

    /// Creates a new RgaInsert operation using the generated OpId as element id.
    pub fn rga_insert_with_op_id(
        origin: NodeId,
        key: impl Into<String>,
        parent: Option<impl Into<String>>,
        value: impl Into<Vec<u8>>,
    ) -> Self {
        let id = OpId::generate();
        let element_id = id.as_str().to_string();
        Self {
            id,
            origin,
            kind: OpKind::RgaInsert {
                key: key.into(),
                id: element_id,
                parent: parent.map(Into::into),
                value: value.into(),
            },
        }
    }

    /// Creates a new RgaDelete operation.
    pub fn rga_delete(origin: NodeId, key: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            id: OpId::generate(),
            origin,
            kind: OpKind::RgaDelete {
                key: key.into(),
                id: id.into(),
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
            other => panic!("unexpected op kind: {other:?}"),
        }
    }

    #[test]
    fn test_op_pncounter_increment() {
        let node = NodeId::new("node-1");
        let op = Op::pncounter_increment(node.clone(), "stock:sku-1", 5);

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::PNCounterIncrement { key, increment } => {
                assert_eq!(key, "stock:sku-1");
                assert_eq!(*increment, 5);
            }
            other => panic!("unexpected op kind: {other:?}"),
        }
    }

    #[test]
    fn test_op_pncounter_decrement() {
        let node = NodeId::new("node-1");
        let op = Op::pncounter_decrement(node.clone(), "stock:sku-1", 3);

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::PNCounterDecrement { key, decrement } => {
                assert_eq!(key, "stock:sku-1");
                assert_eq!(*decrement, 3);
            }
            other => panic!("unexpected op kind: {other:?}"),
        }
    }

    #[test]
    fn test_op_lww_register_set() {
        let node = NodeId::new("node-1");
        let op = Op::lww_register_set(node.clone(), "status:user-1", b"online".to_vec(), 123);

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::LwwRegisterSet {
                key,
                value,
                timestamp_ms,
            } => {
                assert_eq!(key, "status:user-1");
                assert_eq!(value, b"online");
                assert_eq!(*timestamp_ms, 123);
            }
            other => panic!("unexpected op kind: {other:?}"),
        }
    }

    #[test]
    fn test_op_lww_map_set() {
        let node = NodeId::new("node-1");
        let op = Op::lww_map_set(
            node.clone(),
            "settings:service-a",
            "theme",
            b"dark".to_vec(),
            123,
        );

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::LwwMapSet {
                key,
                field,
                value,
                timestamp_ms,
            } => {
                assert_eq!(key, "settings:service-a");
                assert_eq!(field, "theme");
                assert_eq!(value, b"dark");
                assert_eq!(*timestamp_ms, 123);
            }
            other => panic!("unexpected op kind: {other:?}"),
        }
    }

    #[test]
    fn test_op_lww_map_remove() {
        let node = NodeId::new("node-1");
        let op = Op::lww_map_remove(node.clone(), "settings:service-a", "theme", 456);

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::LwwMapRemove {
                key,
                field,
                timestamp_ms,
            } => {
                assert_eq!(key, "settings:service-a");
                assert_eq!(field, "theme");
                assert_eq!(*timestamp_ms, 456);
            }
            other => panic!("unexpected op kind: {other:?}"),
        }
    }

    #[test]
    fn test_op_orset_add() {
        let node = NodeId::new("node-1");
        let op = Op::orset_add(node.clone(), "tags:item-1", "blue", "add-tag-1");

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::ORSetAdd { key, element, tag } => {
                assert_eq!(key, "tags:item-1");
                assert_eq!(element, "blue");
                assert_eq!(tag, "add-tag-1");
            }
            other => panic!("unexpected op kind: {other:?}"),
        }
    }

    #[test]
    fn test_op_orset_add_with_op_id_tag() {
        let node = NodeId::new("node-1");
        let op = Op::orset_add_with_op_id_tag(node.clone(), "tags:item-1", "blue");

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::ORSetAdd { key, element, tag } => {
                assert_eq!(key, "tags:item-1");
                assert_eq!(element, "blue");
                assert_eq!(tag, op.id.as_str());
            }
            other => panic!("unexpected op kind: {other:?}"),
        }
    }

    #[test]
    fn test_op_orset_remove() {
        let node = NodeId::new("node-1");
        let observed_tags = vec!["add-tag-1".to_string(), "add-tag-2".to_string()];
        let op = Op::orset_remove(node.clone(), "tags:item-1", "blue", observed_tags.clone());

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::ORSetRemove {
                key,
                element,
                observed_tags: tags,
            } => {
                assert_eq!(key, "tags:item-1");
                assert_eq!(element, "blue");
                assert_eq!(tags, &observed_tags);
            }
            other => panic!("unexpected op kind: {other:?}"),
        }
    }

    #[test]
    fn test_op_rga_insert() {
        let node = NodeId::new("node-1");
        let op = Op::rga_insert(
            node.clone(),
            "comments:doc-1",
            "op-a",
            Some("op-root"),
            b"hello".to_vec(),
        );

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::RgaInsert {
                key,
                id,
                parent,
                value,
            } => {
                assert_eq!(key, "comments:doc-1");
                assert_eq!(id, "op-a");
                assert_eq!(parent.as_deref(), Some("op-root"));
                assert_eq!(value, b"hello");
            }
            other => panic!("unexpected op kind: {other:?}"),
        }
    }

    #[test]
    fn test_op_rga_insert_with_op_id() {
        let node = NodeId::new("node-1");
        let op = Op::rga_insert_with_op_id(node.clone(), "comments:doc-1", None::<String>, b"a");

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::RgaInsert {
                key,
                id,
                parent,
                value,
            } => {
                assert_eq!(key, "comments:doc-1");
                assert_eq!(id, op.id.as_str());
                assert_eq!(parent, &None);
                assert_eq!(value, b"a");
            }
            other => panic!("unexpected op kind: {other:?}"),
        }
    }

    #[test]
    fn test_op_rga_delete() {
        let node = NodeId::new("node-1");
        let op = Op::rga_delete(node.clone(), "comments:doc-1", "op-a");

        assert_eq!(op.origin, node);
        match &op.kind {
            OpKind::RgaDelete { key, id } => {
                assert_eq!(key, "comments:doc-1");
                assert_eq!(id, "op-a");
            }
            other => panic!("unexpected op kind: {other:?}"),
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

    #[test]
    fn test_op_pncounter_json_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::pncounter_decrement(node, "stock:sku-1", 7);

        let json = op.to_json().unwrap();
        let parsed = Op::from_json(&json).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_pncounter_bytes_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::pncounter_increment(node, "stock:sku-1", 11);

        let bytes = op.to_bytes().unwrap();
        let parsed = Op::from_bytes(&bytes).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_lww_register_json_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::lww_register_set(node, "status:user-1", b"away".to_vec(), 456);

        let json = op.to_json().unwrap();
        let parsed = Op::from_json(&json).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_lww_register_bytes_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::lww_register_set(node, "status:user-1", b"busy".to_vec(), 789);

        let bytes = op.to_bytes().unwrap();
        let parsed = Op::from_bytes(&bytes).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_lww_map_set_json_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::lww_map_set(node, "settings:service-a", "theme", b"dark".to_vec(), 456);

        let json = op.to_json().unwrap();
        let parsed = Op::from_json(&json).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_lww_map_set_bytes_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::lww_map_set(node, "settings:service-a", "theme", b"light".to_vec(), 789);

        let bytes = op.to_bytes().unwrap();
        let parsed = Op::from_bytes(&bytes).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_lww_map_remove_json_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::lww_map_remove(node, "settings:service-a", "theme", 456);

        let json = op.to_json().unwrap();
        let parsed = Op::from_json(&json).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_lww_map_remove_bytes_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::lww_map_remove(node, "settings:service-a", "theme", 789);

        let bytes = op.to_bytes().unwrap();
        let parsed = Op::from_bytes(&bytes).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_orset_add_json_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::orset_add(node, "tags:item-1", "blue", "add-tag-1");

        let json = op.to_json().unwrap();
        let parsed = Op::from_json(&json).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_orset_add_bytes_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::orset_add_with_op_id_tag(node, "tags:item-1", "blue");

        let bytes = op.to_bytes().unwrap();
        let parsed = Op::from_bytes(&bytes).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_orset_remove_json_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::orset_remove(
            node,
            "tags:item-1",
            "blue",
            vec!["add-tag-1".to_string(), "add-tag-2".to_string()],
        );

        let json = op.to_json().unwrap();
        let parsed = Op::from_json(&json).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_orset_remove_bytes_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::orset_remove(node, "tags:item-1", "blue", vec!["add-tag-1".to_string()]);

        let bytes = op.to_bytes().unwrap();
        let parsed = Op::from_bytes(&bytes).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_rga_insert_json_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::rga_insert(
            node,
            "comments:doc-1",
            "op-a",
            Some("op-root"),
            b"hello".to_vec(),
        );

        let json = op.to_json().unwrap();
        let parsed = Op::from_json(&json).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_rga_insert_bytes_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::rga_insert_with_op_id(node, "comments:doc-1", None::<String>, b"hello");

        let bytes = op.to_bytes().unwrap();
        let parsed = Op::from_bytes(&bytes).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_rga_delete_json_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::rga_delete(node, "comments:doc-1", "op-a");

        let json = op.to_json().unwrap();
        let parsed = Op::from_json(&json).unwrap();

        assert_eq!(op, parsed);
    }

    #[test]
    fn test_op_rga_delete_bytes_roundtrip() {
        let node = NodeId::new("node-1");
        let op = Op::rga_delete(node, "comments:doc-1", "op-a");

        let bytes = op.to_bytes().unwrap();
        let parsed = Op::from_bytes(&bytes).unwrap();

        assert_eq!(op, parsed);
    }
}
