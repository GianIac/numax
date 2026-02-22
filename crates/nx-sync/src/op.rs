use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

use crate::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OpId(String);

impl OpId {
    /// Genera un nuovo OpId univoco.
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Crea un OpId da una stringa esistente.
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
    /// Incremento di un GCounter.
    /// Contiene: (chiave nel DB, incremento)
    GCounterIncrement { key: String, increment: u64 },
}

/// Un'operazione CRDT completa, pronta per essere inviata/ricevuta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Op {
    /// Identificatore univoco dell'operazione.
    pub id: OpId,

    /// Nodo che ha originato l'operazione.
    pub origin: NodeId,

    /// Tipo e dati dell'operazione.
    pub kind: OpKind,
}

impl Op {
    /// Crea una nuova operazione GCounterIncrement.
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

    /// Serializza l'operazione in JSON.
    ///
    /// TODO(phase4-serialization): aggiungere metodo per bincode/msgpack
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserializza un'operazione da JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serializza in bytes (JSON per ora).
    ///
    /// TODO(phase4-serialization): quando si passa a bincode, cambiare qui.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserializza da bytes.
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
        println!("Op JSON: {}", json); // utile per debug

        let parsed = Op::from_json(&json).unwrap();
        assert_eq!(op.origin, parsed.origin);
        assert_eq!(op.kind, parsed.kind);
        // id sarà diverso solo se ri-generiamo, ma qui è lo stesso
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
