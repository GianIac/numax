use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, atomic::AtomicU64};
use std::time::Duration;

use nx_net::Node;
use nx_store::Store as NxStore;
use nx_sync::{GCounter, LwwMap, LwwRegister, NodeId, ORSet, Op, PNCounter, Rga};
use tokio::sync::{RwLock, watch};

use crate::observability::RuntimeMetrics;

use super::peer::PeerHealth;
use super::replication::normalize_seen_ops_limit;

/// Upper bound on how many ops we coalesce into a single PushOps message.
pub(super) const BROADCAST_BATCH_MAX: usize = 1024;
pub(super) const BROADCAST_COALESCE_DELAY: Duration = Duration::from_millis(1);
pub(super) const GCOUNTER_STORE_PREFIX: &str = "__nx/crdt/gcounter/";
pub(super) const GCOUNTER_STATE_STORE_PREFIX: &str = "__nx/crdt/state/gcounter/";
pub(super) const PNCOUNTER_STORE_PREFIX: &str = "__nx/crdt/pncounter/";
pub(super) const PNCOUNTER_STATE_STORE_PREFIX: &str = "__nx/crdt/state/pncounter/";
pub(super) const LWW_REGISTER_STORE_PREFIX: &str = "__nx/crdt/lww-register/";
pub(super) const LWW_REGISTER_STATE_STORE_PREFIX: &str = "__nx/crdt/state/lww-register/";
pub(super) const LWW_MAP_STORE_PREFIX: &str = "__nx/crdt/lww-map/";
pub(super) const LWW_MAP_STATE_STORE_PREFIX: &str = "__nx/crdt/state/lww-map/";
pub(super) const ORSET_STORE_PREFIX: &str = "__nx/crdt/orset/";
pub(super) const ORSET_STATE_STORE_PREFIX: &str = "__nx/crdt/state/orset/";
pub(super) const RGA_STORE_PREFIX: &str = "__nx/crdt/rga/";
pub(super) const RGA_STATE_STORE_PREFIX: &str = "__nx/crdt/state/rga/";
pub(super) const SEEN_OP_STORE_PREFIX: &str = "__nx/crdt/seen-op/";
pub(super) const OP_LOG_STORE_PREFIX: &str = "__nx/crdt/op-log/";

#[derive(Debug, Clone, Copy)]
pub(super) enum CrdtStoreNamespace {
    Materialized,
    State,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum CrdtKind {
    GCounter,
    PNCounter,
    LwwRegister,
    LwwMap,
    ORSet,
    Rga,
}

impl CrdtKind {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::GCounter => "GCounter",
            Self::PNCounter => "PNCounter",
            Self::LwwRegister => "LWW-Register",
            Self::LwwMap => "LWW-Map",
            Self::ORSet => "ORSet",
            Self::Rga => "RGA",
        }
    }

    pub(super) fn prefix(self, namespace: CrdtStoreNamespace) -> &'static str {
        match (self, namespace) {
            (Self::GCounter, CrdtStoreNamespace::Materialized) => GCOUNTER_STORE_PREFIX,
            (Self::GCounter, CrdtStoreNamespace::State) => GCOUNTER_STATE_STORE_PREFIX,
            (Self::PNCounter, CrdtStoreNamespace::Materialized) => PNCOUNTER_STORE_PREFIX,
            (Self::PNCounter, CrdtStoreNamespace::State) => PNCOUNTER_STATE_STORE_PREFIX,
            (Self::LwwRegister, CrdtStoreNamespace::Materialized) => LWW_REGISTER_STORE_PREFIX,
            (Self::LwwRegister, CrdtStoreNamespace::State) => LWW_REGISTER_STATE_STORE_PREFIX,
            (Self::LwwMap, CrdtStoreNamespace::Materialized) => LWW_MAP_STORE_PREFIX,
            (Self::LwwMap, CrdtStoreNamespace::State) => LWW_MAP_STATE_STORE_PREFIX,
            (Self::ORSet, CrdtStoreNamespace::Materialized) => ORSET_STORE_PREFIX,
            (Self::ORSet, CrdtStoreNamespace::State) => ORSET_STATE_STORE_PREFIX,
            (Self::Rga, CrdtStoreNamespace::Materialized) => RGA_STORE_PREFIX,
            (Self::Rga, CrdtStoreNamespace::State) => RGA_STATE_STORE_PREFIX,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum SeenOpsInsert {
    AlreadySeen,
    Inserted { evicted: Vec<String> },
}

#[derive(Debug, Clone)]
pub(super) struct SeenOps {
    pub(super) ids: HashSet<String>,
    pub(super) order: VecDeque<String>,
    pub(super) limit: usize,
}

impl SeenOps {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            ids: HashSet::new(),
            order: VecDeque::new(),
            limit: normalize_seen_ops_limit(limit),
        }
    }

    #[cfg(test)]
    pub(super) fn contains(&self, op_id: &str) -> bool {
        self.ids.contains(op_id)
    }

    pub(super) fn insert(&mut self, op_id: impl Into<String>) -> SeenOpsInsert {
        let op_id = op_id.into();
        if !self.ids.insert(op_id.clone()) {
            return SeenOpsInsert::AlreadySeen;
        }

        self.order.push_back(op_id);
        let mut evicted_ids = Vec::new();
        while self.order.len() > self.limit {
            if let Some(evicted) = self.order.pop_front() {
                self.ids.remove(&evicted);
                evicted_ids.push(evicted);
            }
        }
        SeenOpsInsert::Inserted {
            evicted: evicted_ids,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.ids.len()
    }
}

pub(super) struct ReconnectLoopContext {
    pub(super) node: Arc<Node>,
    pub(super) peers: Vec<String>,
    pub(super) max_peers: usize,
    pub(super) initial_delay: Duration,
    pub(super) max_delay: Duration,
    pub(super) peer_dead_after_failures: u32,
    pub(super) shutdown_rx: watch::Receiver<bool>,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) peer_health: Arc<RwLock<HashMap<String, PeerHealth>>>,
}

pub(super) struct AntiEntropyLoopContext {
    pub(super) node: Arc<Node>,
    pub(super) peers: Vec<String>,
    pub(super) interval: Duration,
    pub(super) shutdown_rx: watch::Receiver<bool>,
    pub(super) metrics: Arc<RuntimeMetrics>,
}

pub(super) struct BroadcastLoopContext {
    pub(super) node: Arc<Node>,
    pub(super) seen_ops: Arc<RwLock<SeenOps>>,
    pub(super) seen_ops_next_sequence: Arc<AtomicU64>,
    pub(super) op_log: Arc<RwLock<Vec<Op>>>,
    pub(super) op_log_next_sequence: Arc<AtomicU64>,
    pub(super) op_log_limit: usize,
    pub(super) store: Arc<NxStore>,
    pub(super) metrics: Arc<RuntimeMetrics>,
}

pub(super) struct RemoteOpApplyContext<'a> {
    pub(super) counters: &'a Arc<RwLock<HashMap<String, GCounter>>>,
    pub(super) pncounters: &'a Arc<RwLock<HashMap<String, PNCounter>>>,
    pub(super) lww_registers: &'a Arc<RwLock<HashMap<String, LwwRegister>>>,
    pub(super) lww_maps: &'a Arc<RwLock<HashMap<String, LwwMap>>>,
    pub(super) orsets: &'a Arc<RwLock<HashMap<String, ORSet>>>,
    pub(super) rgas: &'a Arc<RwLock<HashMap<String, Rga>>>,
    pub(super) seen_ops: &'a Arc<RwLock<SeenOps>>,
    pub(super) seen_ops_next_sequence: &'a Arc<AtomicU64>,
    pub(super) op_log: &'a Arc<RwLock<Vec<Op>>>,
    pub(super) op_log_next_sequence: &'a Arc<AtomicU64>,
    pub(super) op_log_limit: usize,
    pub(super) store: &'a Arc<NxStore>,
    pub(super) metrics: &'a Arc<RuntimeMetrics>,
}

pub(super) struct RemoteCrdtRegistries<'a> {
    pub(super) counters: &'a HashMap<String, GCounter>,
    pub(super) pncounters: &'a HashMap<String, PNCounter>,
    pub(super) lww_registers: &'a HashMap<String, LwwRegister>,
    pub(super) lww_maps: &'a HashMap<String, LwwMap>,
    pub(super) orsets: &'a HashMap<String, ORSet>,
    pub(super) rgas: &'a HashMap<String, Rga>,
}

pub(super) struct RemoteCrdtUpdates<'a> {
    pub(super) counters: &'a mut HashMap<String, GCounter>,
    pub(super) pncounters: &'a mut HashMap<String, PNCounter>,
    pub(super) lww_registers: &'a mut HashMap<String, LwwRegister>,
    pub(super) lww_maps: &'a mut HashMap<String, LwwMap>,
    pub(super) orsets: &'a mut HashMap<String, ORSet>,
    pub(super) rgas: &'a mut HashMap<String, Rga>,
}

pub(super) struct RemoteCrdtUpdateBatch<'a> {
    pub(super) counters: &'a HashMap<String, GCounter>,
    pub(super) pncounters: &'a HashMap<String, PNCounter>,
    pub(super) lww_registers: &'a HashMap<String, LwwRegister>,
    pub(super) lww_maps: &'a HashMap<String, LwwMap>,
    pub(super) orsets: &'a HashMap<String, ORSet>,
    pub(super) rgas: &'a HashMap<String, Rga>,
}

pub(super) struct OpPersistencePlan {
    pub(super) op: Op,
    pub(super) seen_sequence: u64,
    pub(super) op_log_sequence: u64,
    pub(super) seen_evicted: Vec<String>,
    pub(super) op_log_evicted: Vec<Op>,
}

#[derive(Clone)]
pub(super) struct NodeEventContext {
    pub(super) counters: Arc<RwLock<HashMap<String, GCounter>>>,
    pub(super) pncounters: Arc<RwLock<HashMap<String, PNCounter>>>,
    pub(super) lww_registers: Arc<RwLock<HashMap<String, LwwRegister>>>,
    pub(super) lww_maps: Arc<RwLock<HashMap<String, LwwMap>>>,
    pub(super) orsets: Arc<RwLock<HashMap<String, ORSet>>>,
    pub(super) rgas: Arc<RwLock<HashMap<String, Rga>>>,
    pub(super) seen_ops: Arc<RwLock<SeenOps>>,
    pub(super) seen_ops_next_sequence: Arc<AtomicU64>,
    pub(super) op_log: Arc<RwLock<Vec<Op>>>,
    pub(super) op_log_next_sequence: Arc<AtomicU64>,
    pub(super) op_log_limit: usize,
    pub(super) store: Arc<NxStore>,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) node: Arc<Node>,
    pub(super) peer_health: Arc<RwLock<HashMap<String, PeerHealth>>>,
    pub(super) peer_node_ids: Arc<RwLock<HashMap<String, NodeId>>>,
    pub(super) anti_entropy_watermarks: Arc<RwLock<HashMap<NodeId, String>>>,
    pub(super) peer_dead_after_failures: u32,
}
