//! GCounter - Grow-only Counter CRDT.
//!
//! Un GCounter è un contatore distribuito che supporta solo incrementi.
//! Ogni nodo mantiene il proprio "slot" e può incrementare solo quello.
//!
//! ## Proprietà CRDT
//! - **Commutatività**: merge(A, B) == merge(B, A)
//! - **Associatività**: merge(merge(A, B), C) == merge(A, merge(B, C))
//! - **Idempotenza**: merge(A, A) == A
//!
//! ## Esempio
//! ```
//! use nx_sync::{GCounter, NodeId};
//!
//! let mut counter = GCounter::new();
//! let node_a = NodeId::new("node-a");
//! let node_b = NodeId::new("node-b");
//!
//! counter.increment(&node_a, 5);
//! counter.increment(&node_b, 3);
//!
//! assert_eq!(counter.value(), 8);
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{NodeId, Op, OpKind, SyncResult};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GCounter {
    /// Mappa: NodeId -> valore locale di quel nodo.
    counts: HashMap<String, u64>,
}

impl GCounter {
    /// Crea un nuovo GCounter vuoto.
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
        }
    }

    /// Restituisce il valore totale del contatore (somma di tutti gli slot).
    pub fn value(&self) -> u64 {
        self.counts.values().sum()
    }

    /// Restituisce il valore dello slot di un nodo specifico.
    pub fn value_for(&self, node: &NodeId) -> u64 {
        self.counts.get(node.as_str()).copied().unwrap_or(0)
    }

    /// Incrementa lo slot del nodo specificato.
    ///
    /// Nota: in un sistema reale, un nodo dovrebbe incrementare
    /// solo il proprio slot. Questa funzione è "trusted".
    pub fn increment(&mut self, node: &NodeId, delta: u64) {
        let entry = self.counts.entry(node.as_str().to_string()).or_insert(0);
        *entry = entry.saturating_add(delta);
    }

    /// Applica un'operazione al GCounter.
    ///
    /// Restituisce `true` se lo stato è cambiato, `false` se era già aggiornato.
    pub fn apply_op(&mut self, op: &Op) -> SyncResult<bool> {
        match &op.kind {
            OpKind::GCounterIncrement { key: _, increment } => {
                let node_key = op.origin.as_str().to_string();
                let current = self.counts.get(&node_key).copied().unwrap_or(0);

                // In un GCounter, l'operazione contiene il delta.
                // Per idempotenza, dobbiamo tenere traccia delle op già applicate
                // oppure usare il valore assoluto. Qui usiamo delta semplice.
                //
                // NOTA: per vera idempotenza, servirebbe un set di OpId visti.
                // Per ora, assumiamo che nx-net deduplichi prima di chiamare apply_op.
                let new_value = current.saturating_add(*increment);
                if new_value != current {
                    self.counts.insert(node_key, new_value);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    /// Merge di due GCounter: prende il max di ogni slot.
    ///
    /// Questa è l'operazione fondamentale che garantisce convergenza.
    /// Dopo il merge, entrambi i nodi avranno lo stesso stato.
    pub fn merge(&mut self, other: &GCounter) {
        for (node, &value) in &other.counts {
            let entry = self.counts.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(value);
        }
    }

    /// Crea un nuovo GCounter risultato del merge (senza mutare self).
    pub fn merged_with(&self, other: &GCounter) -> GCounter {
        let mut result = self.clone();
        result.merge(other);
        result
    }

    /// Restituisce tutti i nodi che hanno contribuito al counter.
    pub fn nodes(&self) -> impl Iterator<Item = &str> {
        self.counts.keys().map(|s| s.as_str())
    }

    /// Restituisce il numero di nodi che hanno contribuito.
    pub fn node_count(&self) -> usize {
        self.counts.len()
    }

    /// Serializza in JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserializza da JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Crea un'operazione di incremento per questo GCounter.
///
/// Utility per generare Op da inviare ai peer.
pub fn create_increment_op(node: &NodeId, key: &str, increment: u64) -> Op {
    Op::gcounter_increment(node.clone(), key, increment)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcounter_new_is_zero() {
        let counter = GCounter::new();
        assert_eq!(counter.value(), 0);
    }

    #[test]
    fn test_gcounter_increment() {
        let mut counter = GCounter::new();
        let node = NodeId::new("node-1");

        counter.increment(&node, 5);
        assert_eq!(counter.value(), 5);
        assert_eq!(counter.value_for(&node), 5);

        counter.increment(&node, 3);
        assert_eq!(counter.value(), 8);
    }

    #[test]
    fn test_gcounter_multiple_nodes() {
        let mut counter = GCounter::new();
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");

        counter.increment(&node_a, 10);
        counter.increment(&node_b, 7);

        assert_eq!(counter.value(), 17);
        assert_eq!(counter.value_for(&node_a), 10);
        assert_eq!(counter.value_for(&node_b), 7);
    }

    #[test]
    fn test_gcounter_merge_takes_max() {
        // Simula due nodi con stati diversi
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");

        let mut counter1 = GCounter::new();
        counter1.increment(&node_a, 5);
        counter1.increment(&node_b, 3);

        let mut counter2 = GCounter::new();
        counter2.increment(&node_a, 2); // meno di counter1
        counter2.increment(&node_b, 7); // più di counter1

        // Merge counter2 into counter1
        counter1.merge(&counter2);

        // Deve prendere il max: A=5, B=7
        assert_eq!(counter1.value_for(&node_a), 5);
        assert_eq!(counter1.value_for(&node_b), 7);
        assert_eq!(counter1.value(), 12);
    }

    #[test]
    fn test_gcounter_merge_commutativity() {
        let node_a = NodeId::new("a");
        let node_b = NodeId::new("b");

        let mut c1 = GCounter::new();
        c1.increment(&node_a, 10);
        c1.increment(&node_b, 5);

        let mut c2 = GCounter::new();
        c2.increment(&node_a, 3);
        c2.increment(&node_b, 20);

        // merge(c1, c2)
        let merged_1_2 = c1.merged_with(&c2);

        // merge(c2, c1)
        let merged_2_1 = c2.merged_with(&c1);

        // Devono essere uguali (commutatività)
        assert_eq!(merged_1_2.value(), merged_2_1.value());
        assert_eq!(merged_1_2.value_for(&node_a), merged_2_1.value_for(&node_a));
        assert_eq!(merged_1_2.value_for(&node_b), merged_2_1.value_for(&node_b));
    }

    #[test]
    fn test_gcounter_merge_idempotency() {
        let node = NodeId::new("node-1");

        let mut counter = GCounter::new();
        counter.increment(&node, 42);

        let before = counter.value();

        // Merge con se stesso
        counter.merge(&counter.clone());

        // Il valore non deve cambiare (idempotenza)
        assert_eq!(counter.value(), before);
    }

    #[test]
    fn test_gcounter_merge_associativity() {
        let node_a = NodeId::new("a");
        let node_b = NodeId::new("b");
        let node_c = NodeId::new("c");

        let mut c1 = GCounter::new();
        c1.increment(&node_a, 1);

        let mut c2 = GCounter::new();
        c2.increment(&node_b, 2);

        let mut c3 = GCounter::new();
        c3.increment(&node_c, 3);

        // (c1 merge c2) merge c3
        let mut left = c1.merged_with(&c2);
        left.merge(&c3);

        // c1 merge (c2 merge c3)
        let right_inner = c2.merged_with(&c3);
        let right = c1.merged_with(&right_inner);

        // Devono essere uguali (associatività)
        assert_eq!(left.value(), right.value());
    }

    #[test]
    fn test_gcounter_apply_op() {
        let mut counter = GCounter::new();
        let node = NodeId::new("node-1");

        let op = Op::gcounter_increment(node.clone(), "counter:test", 10);

        let changed = counter.apply_op(&op).unwrap();
        assert!(changed);
        assert_eq!(counter.value(), 10);

        // Applicare di nuovo incrementa ancora (delta-based)
        // Per vera idempotenza servirebbero OpId tracking
        let changed2 = counter.apply_op(&op).unwrap();
        assert!(changed2);
        assert_eq!(counter.value(), 20);
    }

    #[test]
    fn test_gcounter_json_roundtrip() {
        let mut counter = GCounter::new();
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");

        counter.increment(&node_a, 100);
        counter.increment(&node_b, 50);

        let json = counter.to_json().unwrap();
        println!("GCounter JSON: {}", json);

        let parsed = GCounter::from_json(&json).unwrap();
        assert_eq!(counter.value(), parsed.value());
        assert_eq!(counter.value_for(&node_a), parsed.value_for(&node_a));
    }

    #[test]
    fn test_gcounter_overflow_protection() {
        let mut counter = GCounter::new();
        let node = NodeId::new("node");

        counter.increment(&node, u64::MAX);
        counter.increment(&node, 1); // dovrebbe saturare, non overflow

        assert_eq!(counter.value(), u64::MAX);
    }

    #[test]
    fn test_create_increment_op() {
        let node = NodeId::new("my-node");
        let op = create_increment_op(&node, "counter:visits", 1);

        assert_eq!(op.origin, node);
        match op.kind {
            OpKind::GCounterIncrement { key, increment } => {
                assert_eq!(key, "counter:visits");
                assert_eq!(increment, 1);
            }
        }
    }
}