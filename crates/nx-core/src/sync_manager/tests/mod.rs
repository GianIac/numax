use super::*;
use crate::runtime::{Runtime, RuntimeConfig};
use crate::sync_manager::{apply::*, peer::*, replication::*, storage::*};
use nx_net::{ConnectionDirection, NodeEvent, PeerIdentityVerification};
use nx_sync::OpKind;
use std::sync::Mutex as StdMutex;
use std::time::Instant as StdInstant;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, Instant, sleep};

mod e2e;
mod support;

use support::*;

struct TestDynamicDiscovery {
    state: StdMutex<(u64, Vec<String>)>,
    events: tokio::sync::broadcast::Sender<crate::DiscoveryEvent>,
}

impl TestDynamicDiscovery {
    fn empty() -> Self {
        let (events, _) = tokio::sync::broadcast::channel(8);
        Self {
            state: StdMutex::new((0, Vec::new())),
            events,
        }
    }

    fn add(&self, endpoint: String) {
        let mut state = self.state.lock().unwrap();
        state.0 += 1;
        state.1.push(endpoint.clone());
        let _ = self.events.send(crate::DiscoveryEvent {
            revision: state.0,
            change: crate::DiscoveryChange::Added(endpoint),
        });
    }

    fn remove(&self, endpoint: &str) {
        let mut state = self.state.lock().unwrap();
        state.0 += 1;
        state.1.retain(|candidate| candidate != endpoint);
        let _ = self.events.send(crate::DiscoveryEvent {
            revision: state.0,
            change: crate::DiscoveryChange::Removed(endpoint.to_string()),
        });
    }
}

#[async_trait::async_trait]
impl crate::PeerDiscovery for TestDynamicDiscovery {
    async fn discover(&self) -> Result<crate::DiscoverySnapshot, crate::DiscoveryError> {
        let state = self.state.lock().unwrap();
        Ok(crate::DiscoverySnapshot::new(state.0, state.1.clone()))
    }

    async fn announce(
        &self,
        _announcement: &crate::PeerAnnouncement,
    ) -> Result<(), crate::DiscoveryError> {
        Err(crate::DiscoveryError::Unsupported {
            provider: "test-dynamic".to_string(),
            operation: "announcement",
        })
    }

    async fn watch(&self) -> Result<crate::DiscoveryWatch, crate::DiscoveryError> {
        let state = self.state.lock().unwrap();
        let events = self.events.subscribe();
        Ok(crate::DiscoveryWatch::new(
            crate::DiscoverySnapshot::new(state.0, state.1.clone()),
            events,
        ))
    }
}

#[test]
fn crdt_store_keys_roundtrip_through_generic_namespace_helpers() {
    let materialized = crdt_store_key(
        CrdtKind::GCounter,
        CrdtStoreNamespace::Materialized,
        "counter:visits",
    );
    let durable_state = crdt_store_key(
        CrdtKind::GCounter,
        CrdtStoreNamespace::State,
        "counter:visits",
    );

    assert_eq!(materialized, materialized_gcounter_key("counter:visits"));
    assert_eq!(durable_state, durable_gcounter_state_key("counter:visits"));
    assert_eq!(
        logical_crdt_key(
            &materialized,
            CrdtKind::GCounter,
            CrdtStoreNamespace::Materialized,
        )
        .unwrap(),
        "counter:visits"
    );
    assert_eq!(
        logical_crdt_key(
            &durable_state,
            CrdtKind::GCounter,
            CrdtStoreNamespace::State
        )
        .unwrap(),
        "counter:visits"
    );

    let pn_materialized = crdt_store_key(
        CrdtKind::PNCounter,
        CrdtStoreNamespace::Materialized,
        "stock:sku-1",
    );
    let pn_durable_state = crdt_store_key(
        CrdtKind::PNCounter,
        CrdtStoreNamespace::State,
        "stock:sku-1",
    );

    assert_eq!(pn_materialized, materialized_pncounter_key("stock:sku-1"));
    assert_eq!(pn_durable_state, durable_pncounter_state_key("stock:sku-1"));
    assert_eq!(
        logical_crdt_key(
            &pn_materialized,
            CrdtKind::PNCounter,
            CrdtStoreNamespace::Materialized,
        )
        .unwrap(),
        "stock:sku-1"
    );
    assert_eq!(
        logical_crdt_key(
            &pn_durable_state,
            CrdtKind::PNCounter,
            CrdtStoreNamespace::State
        )
        .unwrap(),
        "stock:sku-1"
    );

    let lww_materialized = crdt_store_key(
        CrdtKind::LwwRegister,
        CrdtStoreNamespace::Materialized,
        "status:user-1",
    );
    let lww_durable_state = crdt_store_key(
        CrdtKind::LwwRegister,
        CrdtStoreNamespace::State,
        "status:user-1",
    );

    assert_eq!(
        lww_materialized,
        materialized_lww_register_key("status:user-1")
    );
    assert_eq!(
        lww_durable_state,
        durable_lww_register_state_key("status:user-1")
    );
    assert_eq!(
        logical_crdt_key(
            &lww_materialized,
            CrdtKind::LwwRegister,
            CrdtStoreNamespace::Materialized,
        )
        .unwrap(),
        "status:user-1"
    );
    assert_eq!(
        logical_crdt_key(
            &lww_durable_state,
            CrdtKind::LwwRegister,
            CrdtStoreNamespace::State
        )
        .unwrap(),
        "status:user-1"
    );

    let lww_map_materialized = crdt_store_key(
        CrdtKind::LwwMap,
        CrdtStoreNamespace::Materialized,
        "settings:service-a",
    );
    let lww_map_durable_state = crdt_store_key(
        CrdtKind::LwwMap,
        CrdtStoreNamespace::State,
        "settings:service-a",
    );

    assert_eq!(
        lww_map_materialized,
        materialized_lww_map_key("settings:service-a")
    );
    assert_eq!(
        lww_map_durable_state,
        durable_lww_map_state_key("settings:service-a")
    );
    assert_eq!(
        logical_crdt_key(
            &lww_map_materialized,
            CrdtKind::LwwMap,
            CrdtStoreNamespace::Materialized,
        )
        .unwrap(),
        "settings:service-a"
    );
    assert_eq!(
        logical_crdt_key(
            &lww_map_durable_state,
            CrdtKind::LwwMap,
            CrdtStoreNamespace::State
        )
        .unwrap(),
        "settings:service-a"
    );

    let orset_materialized = crdt_store_key(
        CrdtKind::ORSet,
        CrdtStoreNamespace::Materialized,
        "tags:item-1",
    );
    let orset_durable_state =
        crdt_store_key(CrdtKind::ORSet, CrdtStoreNamespace::State, "tags:item-1");

    assert_eq!(orset_materialized, materialized_orset_key("tags:item-1"));
    assert_eq!(orset_durable_state, durable_orset_state_key("tags:item-1"));
    assert_eq!(
        logical_crdt_key(
            &orset_materialized,
            CrdtKind::ORSet,
            CrdtStoreNamespace::Materialized,
        )
        .unwrap(),
        "tags:item-1"
    );
    assert_eq!(
        logical_crdt_key(
            &orset_durable_state,
            CrdtKind::ORSet,
            CrdtStoreNamespace::State
        )
        .unwrap(),
        "tags:item-1"
    );

    let rga_materialized = crdt_store_key(
        CrdtKind::Rga,
        CrdtStoreNamespace::Materialized,
        "comments:doc-1",
    );
    let rga_durable_state =
        crdt_store_key(CrdtKind::Rga, CrdtStoreNamespace::State, "comments:doc-1");

    assert_eq!(rga_materialized, materialized_rga_key("comments:doc-1"));
    assert_eq!(rga_durable_state, durable_rga_state_key("comments:doc-1"));
    assert_eq!(
        logical_crdt_key(
            &rga_materialized,
            CrdtKind::Rga,
            CrdtStoreNamespace::Materialized,
        )
        .unwrap(),
        "comments:doc-1"
    );
    assert_eq!(
        logical_crdt_key(&rga_durable_state, CrdtKind::Rga, CrdtStoreNamespace::State).unwrap(),
        "comments:doc-1"
    );
}

#[test]
fn reconnect_backoff_doubles_until_max() {
    let max = Duration::from_secs(5);

    assert_eq!(
        next_reconnect_delay(Duration::from_millis(500), max),
        Duration::from_secs(1)
    );
    assert_eq!(
        next_reconnect_delay(Duration::from_secs(4), max),
        Duration::from_secs(5)
    );
    assert_eq!(
        next_reconnect_delay(Duration::from_secs(5), max),
        Duration::from_secs(5)
    );
}

#[test]
fn reconnect_delay_is_never_zero() {
    assert_eq!(
        normalize_reconnect_delay(Duration::ZERO),
        Duration::from_millis(1)
    );
}

#[test]
fn peer_reconnect_state_tracks_next_attempt_per_peer() {
    let now = StdInstant::now();
    let mut state = PeerReconnectState::new("peer-a".to_string(), Duration::from_millis(500), now);

    let first_delay = state.record_failure(Duration::from_secs(5), now);
    assert_eq!(first_delay, Some(Duration::from_millis(500)));
    assert_eq!(state.delay, Duration::from_secs(1));
    assert_eq!(state.next_attempt_at, now + Duration::from_millis(500));

    state.reset(Duration::from_millis(500), now);
    assert_eq!(state.delay, Duration::from_millis(500));
    assert_eq!(state.next_attempt_at, now);
}

#[test]
fn peer_reconnect_state_schedules_backoff_from_failure_time() {
    let started_at = StdInstant::now();
    let failed_at = started_at + Duration::from_secs(3);
    let mut state =
        PeerReconnectState::new("peer-a".to_string(), Duration::from_millis(500), started_at);

    state
        .record_failure(Duration::from_secs(5), failed_at)
        .unwrap();

    assert_eq!(
        state.next_attempt_at,
        failed_at + Duration::from_millis(500)
    );
}

#[tokio::test]
async fn op_log_since_returns_ops_after_known_id() {
    let op_a = Op::gcounter_increment(NodeId::new("node-a"), "counter:visits", 1);
    let op_b = Op::gcounter_increment(NodeId::new("node-b"), "counter:visits", 2);
    let op_c = Op::gcounter_increment(NodeId::new("node-c"), "counter:visits", 3);
    let op_log = Arc::new(RwLock::new(vec![op_a.clone(), op_b.clone(), op_c.clone()]));

    assert_eq!(
        op_log_since(&op_log, None).await,
        vec![op_a, op_b.clone(), op_c.clone()]
    );
    assert_eq!(
        op_log_since(&op_log, Some(op_b.id.as_str())).await,
        vec![op_c]
    );
}

#[tokio::test]
async fn remember_ops_prunes_op_log_to_configured_limit() {
    let store = temp_store();
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(10)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_a = Op::gcounter_increment(NodeId::new("node-a"), "counter:visits", 1);
    let op_b = Op::gcounter_increment(NodeId::new("node-b"), "counter:visits", 2);
    let op_c = Op::gcounter_increment(NodeId::new("node-c"), "counter:visits", 3);

    remember_ops(
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        2,
        &store,
        &[op_a.clone(), op_b.clone(), op_c.clone()],
    )
    .await
    .unwrap();

    assert_eq!(op_log.read().await.as_slice(), &[op_b, op_c]);
    assert!(seen_ops.read().await.contains(op_a.id.as_str()));
}

#[test]
fn op_log_limit_is_never_zero() {
    assert_eq!(normalize_op_log_limit(0), 1);
}

#[test]
fn seen_ops_limit_is_never_zero() {
    assert_eq!(normalize_seen_ops_limit(0), 1);
}

#[test]
fn seen_ops_eviction_keeps_recent_ids() {
    let mut seen = SeenOps::new(2);

    assert!(matches!(
        seen.insert("op-a"),
        SeenOpsInsert::Inserted { .. }
    ));
    assert!(matches!(
        seen.insert("op-b"),
        SeenOpsInsert::Inserted { .. }
    ));
    assert_eq!(
        seen.insert("op-c"),
        SeenOpsInsert::Inserted {
            evicted: vec!["op-a".to_string()]
        }
    );

    assert!(!seen.contains("op-a"));
    assert!(seen.contains("op-b"));
    assert!(seen.contains("op-c"));
    assert_eq!(seen.len(), 2);
}

#[test]
fn seen_ops_duplicate_does_not_refresh_position() {
    let mut seen = SeenOps::new(2);

    assert!(matches!(
        seen.insert("op-a"),
        SeenOpsInsert::Inserted { .. }
    ));
    assert!(matches!(
        seen.insert("op-b"),
        SeenOpsInsert::Inserted { .. }
    ));
    assert_eq!(seen.insert("op-a"), SeenOpsInsert::AlreadySeen);
    assert_eq!(
        seen.insert("op-c"),
        SeenOpsInsert::Inserted {
            evicted: vec!["op-a".to_string()]
        }
    );

    assert!(!seen.contains("op-a"));
    assert!(seen.contains("op-b"));
    assert!(seen.contains("op-c"));
}

#[test]
fn hydrate_seen_ops_restores_recent_ids_and_next_sequence() {
    let store = temp_store();

    persist_seen_op_batch(&store, "op-a", 7, &[]).unwrap();
    persist_seen_op_batch(&store, "op-b", 8, &[]).unwrap();

    let (seen, next_sequence) = hydrate_seen_ops(&store, 10).unwrap();

    assert!(seen.contains("op-a"));
    assert!(seen.contains("op-b"));
    assert_eq!(seen.len(), 2);
    assert_eq!(next_sequence, 9);
}

#[test]
fn hydrate_seen_ops_prunes_old_metadata_to_limit() {
    let store = temp_store();

    persist_seen_op_batch(&store, "op-a", 1, &[]).unwrap();
    persist_seen_op_batch(&store, "op-b", 2, &[]).unwrap();
    persist_seen_op_batch(&store, "op-c", 3, &[]).unwrap();

    let (seen, next_sequence) = hydrate_seen_ops(&store, 2).unwrap();

    assert!(!seen.contains("op-a"));
    assert!(seen.contains("op-b"));
    assert!(seen.contains("op-c"));
    assert_eq!(next_sequence, 4);
    assert!(!seen_op_exists(&store, "op-a"));
    assert!(seen_op_exists(&store, "op-b"));
    assert!(seen_op_exists(&store, "op-c"));
}

#[test]
fn hydrate_op_log_skips_key_value_op_id_mismatch() {
    let store = temp_store();
    let op_a = Op::gcounter_increment(NodeId::new("node-a"), "counter:visits", 1);
    let op_b = Op::gcounter_increment(NodeId::new("node-b"), "counter:visits", 1);

    write_durable_op_log_entry(&store, op_a.id.as_str(), 1, &op_a);
    write_durable_op_log_entry(&store, "different-op-id", 2, &op_b);

    let (op_log, next_sequence) = hydrate_op_log(&store, 10).unwrap();

    assert_eq!(op_log, vec![op_a]);
    assert_eq!(next_sequence, 2);
}

#[test]
fn peer_health_marks_suspect_then_dead_after_failures() {
    let mut peer_health = HashMap::new();

    record_peer_failure(&mut peer_health, "peer-a", 2);
    let health = peer_health.get("peer-a").unwrap();
    assert_eq!(health.state, PeerHealthState::Suspect);
    assert_eq!(health.consecutive_failures, 1);

    record_peer_failure(&mut peer_health, "peer-a", 2);
    let health = peer_health.get("peer-a").unwrap();
    assert_eq!(health.state, PeerHealthState::Dead);
    assert_eq!(health.consecutive_failures, 2);
}

#[test]
fn peer_health_resets_after_success() {
    let mut peer_health = HashMap::new();

    record_peer_failure(&mut peer_health, "peer-a", 1);
    assert_eq!(
        peer_health.get("peer-a").unwrap().state,
        PeerHealthState::Dead
    );

    record_peer_success(&mut peer_health, "peer-a");
    let health = peer_health.get("peer-a").unwrap();
    assert_eq!(health.state, PeerHealthState::Healthy);
    assert_eq!(health.consecutive_failures, 0);
}

#[test]
fn peer_dead_after_failures_is_never_zero() {
    assert_eq!(normalize_peer_dead_after_failures(0), 1);
}

#[tokio::test]
async fn peer_events_update_known_peer_health() {
    let peer = "127.0.0.1:42000";
    let peer_health = Arc::new(RwLock::new(HashMap::from([(
        peer.to_string(),
        PeerHealth::default(),
    )])));
    let counters = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let store = temp_store();
    let metrics = metrics();
    let context = test_event_context(
        Arc::clone(&counters),
        Arc::clone(&seen_ops),
        Arc::clone(&op_log),
        Arc::clone(&store),
        Arc::clone(&metrics),
        Arc::clone(&peer_health),
    );
    let node_id = NodeId::generate();

    handle_node_event(
        NodeEvent::PeerConnected {
            node_id: node_id.clone(),
            addr: peer.to_string(),
            peers_connected: 1,
        },
        &context,
    )
    .await;
    assert_eq!(
        peer_health.read().await.get(peer).unwrap().state,
        PeerHealthState::Healthy
    );

    handle_node_event(
        NodeEvent::PeerDisconnected {
            node_id,
            addr: peer.to_string(),
            peers_connected: 0,
        },
        &context,
    )
    .await;
    let health = *peer_health.read().await.get(peer).unwrap();
    assert_eq!(health.state, PeerHealthState::Suspect);
    assert_eq!(health.consecutive_failures, 1);
}

#[tokio::test]
async fn peer_events_ignore_unknown_peer_health() {
    let peer_health = Arc::new(RwLock::new(HashMap::new()));
    let counters = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let store = temp_store();
    let metrics = metrics();
    let context = test_event_context(
        Arc::clone(&counters),
        Arc::clone(&seen_ops),
        Arc::clone(&op_log),
        Arc::clone(&store),
        Arc::clone(&metrics),
        Arc::clone(&peer_health),
    );

    handle_node_event(
        NodeEvent::PeerConnected {
            node_id: NodeId::generate(),
            addr: "127.0.0.1:42001".to_string(),
            peers_connected: 1,
        },
        &context,
    )
    .await;

    assert!(peer_health.read().await.is_empty());
}

#[tokio::test]
async fn ops_received_updates_anti_entropy_watermark() {
    let peer = NodeId::new("peer-a");
    let peer_health = Arc::new(RwLock::new(HashMap::new()));
    let counters = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let store = temp_store();
    let metrics = metrics();
    let context = test_event_context(
        Arc::clone(&counters),
        Arc::clone(&seen_ops),
        Arc::clone(&op_log),
        Arc::clone(&store),
        Arc::clone(&metrics),
        Arc::clone(&peer_health),
    );
    let op_a = Op::gcounter_increment(peer.clone(), "counter:visits", 1);
    let op_b = Op::gcounter_increment(peer.clone(), "counter:visits", 1);
    let expected = op_b.id.as_str().to_string();

    handle_node_event(
        NodeEvent::OpsReceived {
            from: peer.clone(),
            ops: vec![op_a, op_b],
        },
        &context,
    )
    .await;

    assert_eq!(
        context
            .anti_entropy_watermarks
            .read()
            .await
            .get(&peer)
            .cloned(),
        Some(expected)
    );
}

#[tokio::test]
async fn apply_remote_op_materializes_counter_value() {
    let store = temp_store();
    let counters = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let origin = NodeId::new("remote-a");
    let op = Op::gcounter_increment(origin, "counter:visits", 3);

    apply_remote_op_for_test(
        &op,
        &counters,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    let key = materialized_gcounter_key("counter:visits");
    let bytes = store.get(&key).unwrap().unwrap();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes);

    assert_eq!(u64::from_le_bytes(buf), 3);
    let state = read_durable_gcounter_state(&store, "counter:visits");
    assert_eq!(state.value_for(&NodeId::new("remote-a")), 3);
}

#[tokio::test]
async fn apply_remote_op_duplicate_does_not_double_materialize() {
    let store = temp_store();
    let counters = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let origin = NodeId::new("remote-a");
    let op = Op::gcounter_increment(origin, "counter:visits", 3);

    apply_remote_op_for_test(
        &op,
        &counters,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_op_for_test(
        &op,
        &counters,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    let key = materialized_gcounter_key("counter:visits");
    let bytes = store.get(&key).unwrap().unwrap();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes);

    assert_eq!(u64::from_le_bytes(buf), 3);
}

#[tokio::test]
async fn apply_remote_pncounter_ops_materialize_signed_value() {
    let store = temp_store();
    let pncounters = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let origin = NodeId::new("remote-a");
    let increment = Op::pncounter_increment(origin.clone(), "stock:sku-1", 10);
    let decrement = Op::pncounter_decrement(origin.clone(), "stock:sku-1", 3);

    apply_remote_pncounter_op_for_test(
        &increment,
        &pncounters,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_pncounter_op_for_test(
        &decrement,
        &pncounters,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(read_materialized_pncounter(&store, "stock:sku-1"), 7);
    let state = read_durable_pncounter_state(&store, "stock:sku-1");
    assert_eq!(state.positive_for(&origin), 10);
    assert_eq!(state.negative_for(&origin), 3);
    assert_eq!(state.value(), 7);
}

#[tokio::test]
async fn apply_remote_pncounter_duplicate_does_not_double_materialize() {
    let store = temp_store();
    let pncounters = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let op = Op::pncounter_decrement(NodeId::new("remote-a"), "stock:sku-1", 3);

    apply_remote_pncounter_op_for_test(
        &op,
        &pncounters,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_pncounter_op_for_test(
        &op,
        &pncounters,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(read_materialized_pncounter(&store, "stock:sku-1"), -3);
}

#[tokio::test]
async fn apply_remote_lww_register_set_materializes_value() {
    let store = temp_store();
    let registers = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let origin = NodeId::new("remote-a");
    let op = Op::lww_register_set(origin.clone(), "status:user-1", b"online".to_vec(), 100);

    apply_remote_lww_register_op_for_test(
        &op,
        &registers,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(
        read_materialized_lww_register(&store, "status:user-1"),
        b"online".to_vec()
    );
    let state = read_durable_lww_register_state(&store, "status:user-1");
    assert_eq!(state.value(), b"online");
    assert_eq!(state.timestamp_ms(), 100);
    assert_eq!(state.writer(), &origin);
}

#[tokio::test]
async fn apply_remote_lww_register_older_set_does_not_overwrite() {
    let store = temp_store();
    let registers = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let newer = Op::lww_register_set(
        NodeId::new("remote-a"),
        "status:user-1",
        b"online".to_vec(),
        200,
    );
    let older = Op::lww_register_set(
        NodeId::new("remote-b"),
        "status:user-1",
        b"away".to_vec(),
        100,
    );

    apply_remote_lww_register_op_for_test(
        &newer,
        &registers,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_lww_register_op_for_test(
        &older,
        &registers,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(
        read_materialized_lww_register(&store, "status:user-1"),
        b"online".to_vec()
    );
    let state = read_durable_lww_register_state(&store, "status:user-1");
    assert_eq!(state.value(), b"online");
    assert_eq!(state.timestamp_ms(), 200);
}

#[tokio::test]
async fn apply_remote_lww_register_duplicate_does_not_double_apply() {
    let store = temp_store();
    let registers = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let op = Op::lww_register_set(
        NodeId::new("remote-a"),
        "status:user-1",
        b"online".to_vec(),
        100,
    );

    apply_remote_lww_register_op_for_test(
        &op,
        &registers,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_lww_register_op_for_test(
        &op,
        &registers,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(op_log.read().await.len(), 1);
    assert_eq!(
        read_materialized_lww_register(&store, "status:user-1"),
        b"online".to_vec()
    );
}

#[tokio::test]
async fn apply_remote_lww_map_set_materializes_visible_entries() {
    let store = temp_store();
    let maps = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let origin = NodeId::new("remote-a");
    let op = Op::lww_map_set(
        origin.clone(),
        "settings:service-a",
        "theme",
        b"dark".to_vec(),
        100,
    );

    apply_remote_lww_map_op_for_test(
        &op,
        &maps,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(
        read_materialized_lww_map(&store, "settings:service-a"),
        vec![("theme".to_string(), b"dark".to_vec())]
    );
    let state = read_durable_lww_map_state(&store, "settings:service-a");
    assert_eq!(state.get_bytes("theme"), Some(b"dark".to_vec()));
    assert_eq!(state.entry("theme").unwrap().timestamp_ms(), 100);
    assert_eq!(state.entry("theme").unwrap().writer(), &origin);
}

#[tokio::test]
async fn apply_remote_lww_map_remove_hides_field() {
    let store = temp_store();
    let maps = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let set = Op::lww_map_set(
        NodeId::new("remote-a"),
        "settings:service-a",
        "theme",
        b"dark".to_vec(),
        100,
    );
    let remove = Op::lww_map_remove(NodeId::new("remote-b"), "settings:service-a", "theme", 200);

    apply_remote_lww_map_op_for_test(
        &set,
        &maps,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_lww_map_op_for_test(
        &remove,
        &maps,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert!(read_materialized_lww_map(&store, "settings:service-a").is_empty());
    let state = read_durable_lww_map_state(&store, "settings:service-a");
    assert!(!state.entry("theme").unwrap().is_visible());
    assert_eq!(state.entry("theme").unwrap().timestamp_ms(), 200);
}

#[tokio::test]
async fn apply_remote_lww_map_older_set_does_not_resurrect_removed_field() {
    let store = temp_store();
    let maps = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let remove = Op::lww_map_remove(NodeId::new("remote-b"), "settings:service-a", "theme", 200);
    let older_set = Op::lww_map_set(
        NodeId::new("remote-a"),
        "settings:service-a",
        "theme",
        b"dark".to_vec(),
        100,
    );

    apply_remote_lww_map_op_for_test(
        &remove,
        &maps,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_lww_map_op_for_test(
        &older_set,
        &maps,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert!(read_materialized_lww_map(&store, "settings:service-a").is_empty());
    let state = read_durable_lww_map_state(&store, "settings:service-a");
    assert!(!state.entry("theme").unwrap().is_visible());
    assert_eq!(state.entry("theme").unwrap().timestamp_ms(), 200);
}

#[tokio::test]
async fn apply_remote_lww_map_duplicate_does_not_double_apply() {
    let store = temp_store();
    let maps = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let op = Op::lww_map_set(
        NodeId::new("remote-a"),
        "settings:service-a",
        "theme",
        b"dark".to_vec(),
        100,
    );

    apply_remote_lww_map_op_for_test(
        &op,
        &maps,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_lww_map_op_for_test(
        &op,
        &maps,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(op_log.read().await.len(), 1);
    let state = read_durable_lww_map_state(&store, "settings:service-a");
    assert_eq!(state.get_bytes("theme"), Some(b"dark".to_vec()));
}

#[tokio::test]
async fn apply_remote_orset_add_materializes_visible_element() {
    let store = temp_store();
    let sets = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let op = Op::orset_add(NodeId::new("remote-a"), "tags:item-1", "blue", "add-tag-1");

    apply_remote_orset_op_for_test(
        &op,
        &sets,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(
        read_materialized_orset(&store, "tags:item-1"),
        vec!["blue".to_string()]
    );
    let state = read_durable_orset_state(&store, "tags:item-1");
    assert!(state.contains("blue"));
    assert_eq!(state.observed_tags("blue"), vec!["add-tag-1".to_string()]);
}

#[tokio::test]
async fn apply_remote_orset_remove_hides_observed_element() {
    let store = temp_store();
    let sets = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let add = Op::orset_add(NodeId::new("remote-a"), "tags:item-1", "blue", "add-tag-1");
    let remove = Op::orset_remove(
        NodeId::new("remote-b"),
        "tags:item-1",
        "blue",
        vec!["add-tag-1".to_string()],
    );

    apply_remote_orset_op_for_test(
        &add,
        &sets,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_orset_op_for_test(
        &remove,
        &sets,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert!(read_materialized_orset(&store, "tags:item-1").is_empty());
    let state = read_durable_orset_state(&store, "tags:item-1");
    assert!(!state.contains("blue"));
}

#[tokio::test]
async fn apply_remote_orset_duplicate_does_not_double_apply() {
    let store = temp_store();
    let sets = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let op = Op::orset_add(NodeId::new("remote-a"), "tags:item-1", "blue", "add-tag-1");

    apply_remote_orset_op_for_test(
        &op,
        &sets,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_orset_op_for_test(
        &op,
        &sets,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(op_log.read().await.len(), 1);
    let state = read_durable_orset_state(&store, "tags:item-1");
    assert_eq!(state.observed_tags("blue"), vec!["add-tag-1".to_string()]);
}

#[tokio::test]
async fn apply_remote_rga_insert_materializes_visible_values() {
    let store = temp_store();
    let rgas = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let insert_a = Op::rga_insert(
        NodeId::new("remote-a"),
        "comments:doc-1",
        "op-a",
        None::<String>,
        b"a".to_vec(),
    );
    let insert_b = Op::rga_insert(
        NodeId::new("remote-b"),
        "comments:doc-1",
        "op-b",
        Some("op-a"),
        b"b".to_vec(),
    );

    apply_remote_rga_op_for_test(
        &insert_b,
        &rgas,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_rga_op_for_test(
        &insert_a,
        &rgas,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(
        read_materialized_rga(&store, "comments:doc-1"),
        vec![b"a".to_vec(), b"b".to_vec()]
    );
    let state = read_durable_rga_state(&store, "comments:doc-1");
    assert_eq!(state.ordered_ids(), vec!["op-a", "op-b"]);
}

#[tokio::test]
async fn apply_remote_rga_delete_hides_value_and_keeps_child_visible() {
    let store = temp_store();
    let rgas = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let insert_a = Op::rga_insert(
        NodeId::new("remote-a"),
        "comments:doc-1",
        "op-a",
        None::<String>,
        b"a".to_vec(),
    );
    let insert_b = Op::rga_insert(
        NodeId::new("remote-b"),
        "comments:doc-1",
        "op-b",
        Some("op-a"),
        b"b".to_vec(),
    );
    let delete_a = Op::rga_delete(NodeId::new("remote-c"), "comments:doc-1", "op-a");

    for op in [&insert_a, &insert_b, &delete_a] {
        apply_remote_rga_op_for_test(
            op,
            &rgas,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;
    }

    assert_eq!(
        read_materialized_rga(&store, "comments:doc-1"),
        vec![b"b".to_vec()]
    );
    let state = read_durable_rga_state(&store, "comments:doc-1");
    assert!(!state.contains("op-a"));
    assert!(state.contains("op-b"));
}

#[tokio::test]
async fn apply_remote_rga_duplicate_does_not_double_apply() {
    let store = temp_store();
    let rgas = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let op = Op::rga_insert(
        NodeId::new("remote-a"),
        "comments:doc-1",
        "op-a",
        None::<String>,
        b"a".to_vec(),
    );

    apply_remote_rga_op_for_test(
        &op,
        &rgas,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;
    apply_remote_rga_op_for_test(
        &op,
        &rgas,
        &seen_ops,
        &seen_ops_next_sequence,
        &op_log,
        &op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(op_log.read().await.len(), 1);
    let state = read_durable_rga_state(&store, "comments:doc-1");
    assert_eq!(state.values(), vec![b"a".to_vec()]);
}

#[tokio::test]
async fn duplicate_remote_op_after_restart_does_not_double_count() {
    let store = temp_store();
    let origin = NodeId::new("remote-a");
    let op = Op::gcounter_increment(origin, "counter:visits", 3);

    let first_manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );
    apply_remote_op_for_test(
        &op,
        &first_manager.counters,
        &first_manager.seen_ops,
        &first_manager.seen_ops_next_sequence,
        &first_manager.op_log,
        &first_manager.op_log_next_sequence,
        &store,
    )
    .await;

    let restarted_manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );
    apply_remote_op_for_test(
        &op,
        &restarted_manager.counters,
        &restarted_manager.seen_ops,
        &restarted_manager.seen_ops_next_sequence,
        &restarted_manager.op_log,
        &restarted_manager.op_log_next_sequence,
        &store,
    )
    .await;

    assert_eq!(
        restarted_manager.get_counter_value("counter:visits").await,
        3
    );
    assert_eq!(read_materialized(&store, "counter:visits"), 3);
}

#[tokio::test]
async fn disk_full_does_not_expose_partial_remote_state() {
    let store = temp_store();
    let counters = Arc::new(RwLock::new(HashMap::new()));
    let pncounters = Arc::new(RwLock::new(HashMap::new()));
    let lww_registers = Arc::new(RwLock::new(HashMap::new()));
    let lww_maps = Arc::new(RwLock::new(HashMap::new()));
    let orsets = Arc::new(RwLock::new(HashMap::new()));
    let rgas = Arc::new(RwLock::new(HashMap::new()));
    let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
    let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
    let op_log = Arc::new(RwLock::new(Vec::new()));
    let op_log_next_sequence = Arc::new(AtomicU64::new(0));
    let metrics = metrics();
    let op = Op::gcounter_increment(NodeId::new("remote-a"), "counter:visits", 3);
    let context = RemoteOpApplyContext {
        counters: &counters,
        pncounters: &pncounters,
        lww_registers: &lww_registers,
        lww_maps: &lww_maps,
        orsets: &orsets,
        rgas: &rgas,
        seen_ops: &seen_ops,
        seen_ops_next_sequence: &seen_ops_next_sequence,
        op_log: &op_log,
        op_log_next_sequence: &op_log_next_sequence,
        op_log_limit: 1024,
        store: &store,
        metrics: &metrics,
    };

    store.inject_disk_full_on_writes(true);
    let error = apply_remote_ops(std::slice::from_ref(&op), &context)
        .await
        .expect_err("disk-full persistence must reject the operation");

    assert!(error.to_string().contains("no space left on device"));
    assert!(counters.read().await.is_empty());
    assert_eq!(seen_ops.read().await.len(), 0);
    assert!(op_log.read().await.is_empty());
    assert_eq!(
        seen_ops_next_sequence.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        op_log_next_sequence.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(
        store
            .get(&durable_gcounter_state_key("counter:visits"))
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get(&op_log_store_key(op.id.as_str()))
            .unwrap()
            .is_none()
    );
    assert!(
        metrics
            .render_for_test(&store)
            .contains("numax_remote_op_apply_errors_total 1")
    );

    store.inject_disk_full_on_writes(false);
    apply_remote_ops(std::slice::from_ref(&op), &context)
        .await
        .expect("the runtime must accept writes after storage recovers");
    assert_eq!(counters.read().await["counter:visits"].value(), 3);
}

#[tokio::test]
async fn manager_hydrates_gcounter_registry_from_materialized_values() {
    let store = temp_store();
    materialize_gcounter_value(&store, "counter:visits", 42).unwrap();

    let node_id = NodeId::new("local-node");
    let config = SyncConfig::new();
    let manager = SyncManager::new(node_id.clone(), config, Arc::clone(&store), metrics());

    assert_eq!(manager.get_counter_value("counter:visits").await, 42);

    let counters = manager.counters.read().await;
    let counter = counters.get("counter:visits").unwrap();
    assert_eq!(counter.value_for(&node_id), 42);
}

#[tokio::test]
async fn manager_hydrates_gcounter_registry_from_durable_state() {
    let store = temp_store();
    let node_a = NodeId::new("node-a");
    let node_b = NodeId::new("node-b");
    let mut counter = GCounter::new();
    counter.increment(&node_a, 5);
    counter.increment(&node_b, 7);

    persist_gcounter_state(&store, "counter:visits", &counter).unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(manager.get_counter_value("counter:visits").await, 12);

    let counters = manager.counters.read().await;
    let hydrated = counters.get("counter:visits").unwrap();
    assert_eq!(hydrated.value_for(&node_a), 5);
    assert_eq!(hydrated.value_for(&node_b), 7);
}

#[test]
fn manager_rejects_corrupted_durable_crdt_state() {
    let cases = [
        (durable_gcounter_state_key("counter:visits"), "GCounter"),
        (durable_pncounter_state_key("counter:visits"), "PNCounter"),
        (
            durable_lww_register_state_key("counter:visits"),
            "LWW-Register",
        ),
        (durable_lww_map_state_key("counter:visits"), "LWW-Map"),
        (durable_orset_state_key("counter:visits"), "ORSet"),
        (durable_rga_state_key("counter:visits"), "RGA"),
    ];

    for (state_key, kind) in cases {
        let store = temp_store();
        store.set(&state_key, b"not-valid-crdt-json").unwrap();

        let result = SyncManager::try_new(
            NodeId::new("local-node"),
            SyncConfig::new(),
            Arc::clone(&store),
            metrics(),
        );

        let error = match result {
            Ok(_) => panic!("corrupted {kind} state must prevent startup"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains(&format!(
                "corrupted durable {kind} state for key `counter:visits`"
            )),
            "unexpected startup error for {kind}: {error:#}"
        );
    }
}

#[tokio::test]
async fn static_peer_lists_keep_their_historical_finite_size_at_startup() {
    let peers = (0..=nx_net::MAX_BOOTSTRAP_RESPONSE_CAPACITY)
        .map(|index| format!("peer-{index}.invalid:9000"))
        .collect::<Vec<_>>();
    // Disable outbound admission: this tests real candidate retention, not DNS/dials.
    let mut config = SyncConfig::new()
        .with_listen_addr("127.0.0.1:0")
        .with_max_peers(0);
    config.peers = peers.clone();

    let mut manager =
        SyncManager::try_new(NodeId::new("local-node"), config, temp_store(), metrics()).unwrap();

    assert_eq!(manager.discovery_config.max_candidates(), peers.len());
    manager.start().await.unwrap();
    assert_eq!(manager.peer_candidates(), peers);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn dynamic_provider_modes_accept_aggregate_capacity_above_bootstrap_response_limit() {
    let peers = vec![
        "peer-a.invalid:9000".to_string(),
        "peer-b.invalid:9000".to_string(),
    ];
    // Controlled snapshots isolate the manager contract from platform discovery I/O.
    for mode in ["mdns", "dns-srv", "file"] {
        let discovery = Arc::new(TestDynamicDiscovery::empty());
        discovery.state.lock().unwrap().1 = peers.clone();
        let mut manager = SyncManager::try_new_with_discovery(
            NodeId::new("local-node"),
            SyncConfig::new()
                .with_listen_addr("127.0.0.1:0")
                .with_max_peers(0),
            temp_store(),
            metrics(),
            vec![DiscoveryProvider::new(mode, discovery)],
            DiscoveryRuntimeConfig::default()
                .with_max_candidates(nx_net::MAX_BOOTSTRAP_RESPONSE_CAPACITY + 1),
        )
        .unwrap();

        manager.start().await.unwrap();
        assert_eq!(manager.peer_candidates(), peers, "{mode}");
        manager.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn bootstrap_responses_are_capped_without_reducing_the_aggregate_cache() {
    use nx_net::{BootstrapClient, BootstrapClientConfig, BootstrapRequest};
    let cap = nx_net::MAX_BOOTSTRAP_RESPONSE_CAPACITY;
    let discovery_config = DiscoveryRuntimeConfig::default().with_max_candidates(cap + 1);
    let cluster = discovery_config.cluster_id().to_string();
    let addr = free_addr();
    let mut manager = SyncManager::try_new_with_discovery(
        NodeId::new("bootstrap-seed"),
        SyncConfig::new().with_listen_addr(&addr),
        temp_store(),
        metrics(),
        vec![],
        discovery_config,
    )
    .unwrap();
    manager.start().await.unwrap();

    let client = |index: usize| {
        let mut config =
            BootstrapClientConfig::new(NodeId::new(format!("bootstrap-client-{index}")));
        config.max_response_candidates = cap;
        BootstrapClient::new(config).unwrap()
    };
    // Fill the actual server cache through one-shot handshakes, requesting only
    // one result while filling so the regression does not transfer quadratic data.
    for index in 0..=cap {
        client(index)
            .query(
                &addr,
                BootstrapRequest::new(&cluster, 1)
                    .with_advertised_endpoint(format!("peer-{index}.invalid:9000")),
            )
            .await
            .unwrap();
    }
    let response = client(cap + 1)
        .query(&addr, BootstrapRequest::new(&cluster, cap))
        .await
        .unwrap();
    assert_eq!(response.endpoints.len(), cap);
    assert!(
        !response
            .endpoints
            .contains(&format!("peer-{cap}.invalid:9000"))
    );

    // Free two earlier entries. The last contribution must emerge; a cache
    // incorrectly clamped to 4096 would have discarded it during admission.
    client(0)
        .query(&addr, BootstrapRequest::new(&cluster, 1))
        .await
        .unwrap();
    let response = client(1)
        .query(&addr, BootstrapRequest::new(&cluster, cap))
        .await
        .unwrap();
    assert_eq!(response.endpoints.len(), cap);
    assert!(
        response
            .endpoints
            .contains(&format!("peer-{cap}.invalid:9000"))
    );
    manager.shutdown().await.unwrap();
}

#[test]
fn manager_rejects_invalid_public_sync_config_before_channel_allocation() {
    let store = temp_store();
    for (field, config) in invalid_sync_configs() {
        let Err(error) = SyncManager::try_new_with_discovery(
            NodeId::new("local-node"),
            config,
            Arc::clone(&store),
            metrics(),
            vec![DiscoveryProvider::new(
                "untouched",
                Arc::new(UntouchedDiscovery),
            )],
            DiscoveryRuntimeConfig::default(),
        ) else {
            panic!("invalid {field} was accepted");
        };
        assert!(matches!(
            error.downcast_ref::<nx_net::NetError>(),
            Some(nx_net::NetError::InvalidConfig(_))
        ));
        assert!(error.to_string().contains(field), "{error}");
    }
}

#[tokio::test]
async fn manager_revalidates_timer_and_node_limits_before_touching_providers() {
    for (field, config) in invalid_sync_configs() {
        let mut manager = SyncManager::try_new_with_discovery(
            NodeId::new("local-node"),
            SyncConfig::new(),
            temp_store(),
            metrics(),
            vec![DiscoveryProvider::new(
                "untouched",
                Arc::new(UntouchedDiscovery),
            )],
            DiscoveryRuntimeConfig::default(),
        )
        .unwrap();
        manager.config = config.with_listen_addr("127.0.0.1:0");
        let error = manager.start().await.unwrap_err();
        assert!(error.to_string().contains(field), "{error}");
        assert!(manager.node.is_none());
        assert!(manager.discovery_coordinator.is_none());
        assert!(manager.op_rx.is_some());
        manager.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn manager_rejects_invalid_bootstrap_policy_before_touching_providers() {
    for discovery_config in [
        DiscoveryRuntimeConfig::default().with_cluster_id("x".repeat(256)),
        DiscoveryRuntimeConfig::default().with_max_candidates(0),
    ] {
        let mut manager = SyncManager::try_new_with_discovery(
            NodeId::new("local-node"),
            SyncConfig::new().with_listen_addr("127.0.0.1:0"),
            temp_store(),
            metrics(),
            vec![DiscoveryProvider::new(
                "untouched",
                Arc::new(UntouchedDiscovery),
            )],
            discovery_config,
        )
        .unwrap();
        assert!(manager.start().await.is_err());
        assert!(manager.node.is_none());
        assert!(manager.discovery_coordinator.is_none());
        assert!(manager.op_rx.is_some());
        manager.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn manager_hydrates_pncounter_registry_from_durable_state() {
    let store = temp_store();
    let node_a = NodeId::new("node-a");
    let node_b = NodeId::new("node-b");
    let mut counter = PNCounter::new();
    counter.increment(&node_a, 8);
    counter.decrement(&node_b, 11);

    persist_pncounter_state(&store, "stock:sku-1", &counter).unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(manager.get_pncounter_value("stock:sku-1").await, -3);

    let pncounters = manager.pncounters.read().await;
    let hydrated = pncounters.get("stock:sku-1").unwrap();
    assert_eq!(hydrated.positive_for(&node_a), 8);
    assert_eq!(hydrated.negative_for(&node_b), 11);
}

#[tokio::test]
async fn manager_hydrates_lww_register_registry_from_durable_state() {
    let store = temp_store();
    let writer = NodeId::new("node-a");
    let register = LwwRegister::new(b"online".to_vec(), 123, writer.clone());

    persist_lww_register_state(&store, "status:user-1", &register).unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(
        manager
            .get_lww_register_value("status:user-1")
            .await
            .unwrap(),
        b"online".to_vec()
    );

    let registers = manager.lww_registers.read().await;
    let hydrated = registers.get("status:user-1").unwrap();
    assert_eq!(hydrated.value(), b"online");
    assert_eq!(hydrated.timestamp_ms(), 123);
    assert_eq!(hydrated.writer(), &writer);
}

#[tokio::test]
async fn manager_hydrates_lww_map_registry_from_durable_state() {
    let store = temp_store();
    let writer = NodeId::new("node-a");
    let mut map = LwwMap::new();
    map.set("theme", b"dark".to_vec(), 123, writer.clone());
    map.set("region", b"eu".to_vec(), 124, writer.clone());
    map.remove("region", 125, writer.clone());

    persist_lww_map_state(&store, "settings:service-a", &map).unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(
        manager.get_lww_map_entries("settings:service-a").await,
        vec![("theme".to_string(), b"dark".to_vec())]
    );

    let maps = manager.lww_maps.read().await;
    let hydrated = maps.get("settings:service-a").unwrap();
    assert_eq!(hydrated.get_bytes("theme"), Some(b"dark".to_vec()));
    assert!(!hydrated.entry("region").unwrap().is_visible());
    assert_eq!(
        read_materialized_lww_map(&store, "settings:service-a").len(),
        1
    );
}

#[tokio::test]
async fn manager_hydrates_orset_registry_from_durable_state() {
    let store = temp_store();
    let mut set = ORSet::new();
    set.add("blue", "add-tag-1");
    set.add("red", "add-tag-2");
    set.remove("blue");

    persist_orset_state(&store, "tags:item-1", &set).unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(
        manager.get_orset_elements("tags:item-1").await,
        vec!["red".to_string()]
    );

    let sets = manager.orsets.read().await;
    let hydrated = sets.get("tags:item-1").unwrap();
    assert!(!hydrated.contains("blue"));
    assert!(hydrated.contains("red"));
    assert_eq!(read_materialized_orset(&store, "tags:item-1"), vec!["red"]);
}

#[tokio::test]
async fn manager_hydrates_rga_registry_from_durable_state() {
    let store = temp_store();
    let mut rga = Rga::new();
    rga.insert("op-a", None::<String>, b"a".to_vec());
    rga.insert("op-b", Some("op-a"), b"b".to_vec());
    rga.delete("op-a");

    persist_rga_state(&store, "comments:doc-1", &rga).unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(
        manager.get_rga_values("comments:doc-1").await,
        vec![b"b".to_vec()]
    );

    let rgas = manager.rgas.read().await;
    let hydrated = rgas.get("comments:doc-1").unwrap();
    assert!(!hydrated.contains("op-a"));
    assert!(hydrated.contains("op-b"));
    assert_eq!(
        read_materialized_rga(&store, "comments:doc-1"),
        vec![b"b".to_vec()]
    );
}

#[tokio::test]
async fn durable_lww_register_state_takes_precedence_over_materialized_value() {
    let store = temp_store();
    let register = LwwRegister::new(b"online".to_vec(), 123, NodeId::new("node-a"));

    persist_lww_register_state(&store, "status:user-1", &register).unwrap();
    materialize_lww_register_value(&store, "status:user-1", b"stale").unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(
        manager
            .get_lww_register_value("status:user-1")
            .await
            .unwrap(),
        b"online".to_vec()
    );
    assert_eq!(
        read_materialized_lww_register(&store, "status:user-1"),
        b"online".to_vec()
    );
}

#[tokio::test]
async fn durable_lww_map_state_takes_precedence_over_materialized_entries() {
    let store = temp_store();
    let mut map = LwwMap::new();
    map.set("theme", b"dark".to_vec(), 123, NodeId::new("node-a"));

    persist_lww_map_state(&store, "settings:service-a", &map).unwrap();
    materialize_lww_map_entries(
        &store,
        "settings:service-a",
        &[("theme".to_string(), b"stale".to_vec())],
    )
    .unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(
        manager.get_lww_map_entries("settings:service-a").await,
        vec![("theme".to_string(), b"dark".to_vec())]
    );
    assert_eq!(
        read_materialized_lww_map(&store, "settings:service-a"),
        vec![("theme".to_string(), b"dark".to_vec())]
    );
}

#[tokio::test]
async fn durable_orset_state_takes_precedence_over_materialized_elements() {
    let store = temp_store();
    let mut set = ORSet::new();
    set.add("fresh", "add-tag-1");

    persist_orset_state(&store, "tags:item-1", &set).unwrap();
    materialize_orset_elements(&store, "tags:item-1", &["stale".to_string()]).unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(
        manager.get_orset_elements("tags:item-1").await,
        vec!["fresh".to_string()]
    );
    assert_eq!(
        read_materialized_orset(&store, "tags:item-1"),
        vec!["fresh".to_string()]
    );
}

#[tokio::test]
async fn durable_rga_state_takes_precedence_over_materialized_values() {
    let store = temp_store();
    let mut rga = Rga::new();
    rga.insert("op-a", None::<String>, b"fresh".to_vec());

    persist_rga_state(&store, "comments:doc-1", &rga).unwrap();
    materialize_rga_values(&store, "comments:doc-1", &[b"stale".to_vec()]).unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(
        manager.get_rga_values("comments:doc-1").await,
        vec![b"fresh".to_vec()]
    );
    assert_eq!(
        read_materialized_rga(&store, "comments:doc-1"),
        vec![b"fresh".to_vec()]
    );
}

#[tokio::test]
async fn durable_pncounter_state_takes_precedence_over_materialized_total() {
    let store = temp_store();
    let node_a = NodeId::new("node-a");
    let node_b = NodeId::new("node-b");
    let mut counter = PNCounter::new();
    counter.increment(&node_a, 5);
    counter.decrement(&node_b, 2);

    persist_pncounter_state(&store, "stock:sku-1", &counter).unwrap();
    materialize_pncounter_value(&store, "stock:sku-1", -99).unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(manager.get_pncounter_value("stock:sku-1").await, 3);
    assert_eq!(read_materialized_pncounter(&store, "stock:sku-1"), 3);
}

#[tokio::test]
async fn durable_gcounter_state_takes_precedence_over_materialized_total() {
    let store = temp_store();
    let node_a = NodeId::new("node-a");
    let node_b = NodeId::new("node-b");
    let mut counter = GCounter::new();
    counter.increment(&node_a, 2);
    counter.increment(&node_b, 3);

    persist_gcounter_state(&store, "counter:visits", &counter).unwrap();
    materialize_gcounter_value(&store, "counter:visits", 99).unwrap();

    let manager = SyncManager::new(
        NodeId::new("local-node"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    );

    assert_eq!(manager.get_counter_value("counter:visits").await, 5);
    assert_eq!(read_materialized(&store, "counter:visits"), 5);
}

#[tokio::test]
async fn reconnect_loop_connects_configured_peer_that_starts_later() {
    let key = "visits";
    let addr_a = free_addr();
    let addr_b = free_addr();

    let config_a = SyncConfig::new()
        .with_listen_addr(addr_a)
        .with_peer(addr_b.clone())
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50));
    let (manager_a, handle_a, _store_a) = started_manager_with_config(config_a).await;

    let config_b = SyncConfig::new().with_listen_addr(addr_b);
    let (manager_b, _handle_b, store_b) = started_manager_with_config(config_b).await;

    wait_for_connected_peer(&manager_a).await;
    local_increment(&handle_a, key, 1).await;

    wait_for_counter(&manager_b, key, 1).await;
    assert_eq!(read_materialized(&store_b, key), 1);
}

#[tokio::test]
async fn dynamic_candidate_connects_after_startup_with_an_empty_snapshot() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let discovery = Arc::new(TestDynamicDiscovery::empty());
    let config_a = SyncConfig::new()
        .with_listen_addr(addr_a)
        .with_anti_entropy_interval(Duration::from_millis(10))
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50));
    let mut manager_a = SyncManager::try_new_with_discovery(
        NodeId::generate(),
        config_a,
        temp_store(),
        metrics(),
        vec![DiscoveryProvider::new("test-dynamic", discovery.clone())],
        DiscoveryRuntimeConfig::default(),
    )
    .unwrap();
    manager_a.start().await.unwrap();
    assert_eq!(manager_a.connected_peer_count().await, 0);

    let config_b = SyncConfig::new().with_listen_addr(addr_b.clone());
    let (mut manager_b, handle_b, _store_b) = started_manager_with_config(config_b).await;

    discovery.add(addr_b.clone());
    wait_for_connected_peer(&manager_a).await;

    let handle_a = manager_a.handle();
    let deadline = Instant::now() + Duration::from_secs(5);
    let connections = loop {
        let connections = handle_a.active_connections().await;
        if !connections.is_empty() {
            break connections;
        }
        assert!(
            Instant::now() < deadline,
            "connection metadata was not published"
        );
        sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(connections.len(), 1);
    assert_eq!(
        connections[0].dialed_endpoint.as_deref(),
        Some(addr_b.as_str())
    );
    assert_eq!(connections[0].direction, ConnectionDirection::Outbound);
    assert_eq!(
        connections[0].identity.verification,
        PeerIdentityVerification::Unverified
    );

    discovery.remove(&addr_b);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if manager_a.peer_candidates().is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "removed discovery candidate remained visible"
        );
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        manager_a.connected_peer_count().await,
        1,
        "removing a candidate must not terminate an admitted connection"
    );

    // The connection remains an anti-entropy target even after its discovery
    // contribution is removed. No broadcast can deliver this operation.
    dropped_local_increment(&manager_b, &handle_b, "removed-candidate", 7).await;
    wait_for_counter(&manager_a, "removed-candidate", 7).await;
    assert_eq!(read_materialized(&manager_a.store, "removed-candidate"), 7);

    manager_a.shutdown().await.unwrap();
    manager_b.shutdown().await.unwrap();
}

#[tokio::test]
async fn anti_entropy_recovers_missing_ops_during_continuous_candidate_churn() {
    let interval = Duration::from_millis(200);
    // A controlled peer never broadcasts or answers connection-time requests.
    let source_id = NodeId::generate();
    let mut source = Node::new(NodeConfig::new(source_id.clone(), "127.0.0.1:0"));
    let mut source_events = source.take_event_receiver().unwrap();
    let source_addr = source.start_listener().await.unwrap().to_string();
    let discovery = Arc::new(TestDynamicDiscovery::empty());
    discovery.add(source_addr);
    let mut target = SyncManager::try_new_with_discovery(
        NodeId::generate(),
        SyncConfig::new()
            .with_listen_addr("127.0.0.1:0")
            .with_max_peers(1)
            .with_anti_entropy_interval(interval),
        temp_store(),
        metrics(),
        vec![DiscoveryProvider::new("test-dynamic", discovery.clone())],
        DiscoveryRuntimeConfig::default(),
    )
    .unwrap();
    target.start().await.unwrap();
    wait_for_connected_peer(&target).await;

    // Reserve the unused endpoint; churn must not create additional connections.
    let unused = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unused_addr = unused.local_addr().unwrap().to_string();
    let mut candidates = target.discovery_coordinator.as_ref().unwrap().candidates();
    let (updates_tx, mut updates_rx) = tokio::sync::watch::channel(Instant::now());
    let churn = async {
        let mut cadence = tokio::time::interval(Duration::from_millis(10));
        cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut added = false;
        loop {
            cadence.tick().await;
            added = !added;
            if added {
                discovery.add(unused_addr.clone());
            } else {
                discovery.remove(&unused_addr);
            }
            // Observe each published change rather than just enqueueing events.
            candidates
                .wait_for(|snapshot| snapshot.contains(&unused_addr) == added)
                .await
                .unwrap();
            updates_tx.send_replace(Instant::now());
        }
    };
    let recovery = async {
        let mut ops = Vec::new();
        for expected in 1..=3 {
            ops.push(Op::gcounter_increment(source_id.clone(), "churn", 1));
            updates_rx.changed().await.unwrap();
            let first_change = *updates_rx.borrow_and_update();
            // Observe distinct published changes spanning a full anti-entropy
            // interval, rather than assuming a scheduler-dependent update count.
            let last_change = *updates_rx
                .wait_for(|changed_at| *changed_at >= first_change + interval)
                .await
                .unwrap();
            assert!(last_change.duration_since(first_change) >= interval);

            // Discard all earlier requests, including any eager startup pull.
            // Only a fresh periodic request may recover this missing operation.
            while let Ok(event) = source_events.try_recv() {
                assert!(matches!(
                    event,
                    NodeEvent::PeerConnected { .. } | NodeEvent::PullRequested { .. }
                ));
            }
            let (reply_addr, since) = wait_for_pull_request(&mut source_events).await;
            assert_eq!(since, None);
            let change_at_pull = *updates_rx.borrow_and_update();
            updates_rx.changed().await.unwrap();
            assert!(*updates_rx.borrow_and_update() > change_at_pull);
            assert_eq!(target.get_counter_value("churn").await, expected - 1);
            source
                .send_ops_to_addr(&reply_addr, ops.clone())
                .await
                .unwrap();

            // FIFO on this connection makes the reply to our pull an apply
            // barrier: the target must have processed the preceding ops first.
            source
                .send_pull_since_to_addr(&reply_addr, None)
                .await
                .unwrap();
            loop {
                match source_events
                    .recv()
                    .await
                    .expect("peer event channel closed")
                {
                    NodeEvent::OpsReceived { ops: received, .. } => {
                        assert_eq!(received, ops);
                        break;
                    }
                    NodeEvent::PullRequested { .. } => {}
                    event => panic!("unexpected event during recovery: {event:?}"),
                }
            }
            assert_eq!(target.get_counter_value("churn").await, expected);
        }
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = churn => unreachable!("churn must continue until recovery completes"),
            _ = recovery => {}
        }
    })
    .await
    .expect("anti-entropy did not recover missing ops while candidate updates continued");
    assert_eq!(target.connected_peer_count().await, 1);
    assert_eq!(read_materialized(&target.store, "churn"), 3);
    assert_eq!(target.op_log.read().await.len(), 3);
    assert_eq!(target.seen_ops.read().await.len(), 3);
    target.shutdown().await.unwrap();
    source.shutdown().await;
}

#[tokio::test]
async fn anti_entropy_inbound_only_max_peers_one_recovers_older_missing_op() {
    let addr = free_addr();
    let (mut target, _, store) = started_manager_with_config(
        SyncConfig::new()
            .with_listen_addr(addr.clone())
            .with_max_peers(1)
            .with_anti_entropy_interval(Duration::from_millis(10)),
    )
    .await;
    let source_id = NodeId::generate();
    let mut source = Node::new(NodeConfig::new(source_id.clone(), "127.0.0.1:0"));
    let mut source_events = source.take_event_receiver().unwrap();
    source.connect_to_peer(&addr).await.unwrap();
    wait_for_connected_peer(&target).await;
    assert!(target.peer_candidates().is_empty());
    assert_eq!(target.connected_peer_count().await, 1);

    let older = Op::gcounter_increment(source_id.clone(), "missing", 3);
    let newer = Op::gcounter_increment(source_id.clone(), "received", 7);
    source
        .send_ops_to_addr(&addr, vec![newer.clone()])
        .await
        .unwrap();
    wait_for_counter(&target, "received", 7).await;
    assert_eq!(target.get_counter_value("missing").await, 0);
    assert_eq!(
        target.anti_entropy_watermarks.read().await.get(&source_id),
        Some(&newer.id.as_str().to_string())
    );
    let connections = target.handle().active_connections().await;
    assert_eq!(connections.len(), 1);
    assert_eq!(connections[0].direction, ConnectionDirection::Inbound);
    assert!(connections[0].dialed_endpoint.is_none());

    // A newer received op must not become a causal frontier. The inbound
    // transport address (not an advertised candidate) is the only valid target.
    let (reply_addr, since) = wait_for_pull_request(&mut source_events).await;
    assert_eq!(since, None);
    source
        .send_ops_to_addr(&reply_addr, vec![older.clone(), newer.clone()])
        .await
        .unwrap();
    wait_for_counter(&target, "missing", 3).await;

    // Repeated full bounded-log pulls must still deduplicate previously seen ops.
    let (reply_addr, since) = wait_for_pull_request(&mut source_events).await;
    assert_eq!(since, None);
    let barrier = Op::gcounter_increment(source_id, "barrier", 1);
    source
        .send_ops_to_addr(&reply_addr, vec![older, newer, barrier])
        .await
        .unwrap();
    wait_for_counter(&target, "barrier", 1).await;
    assert_eq!(target.get_counter_value("missing").await, 3);
    assert_eq!(target.get_counter_value("received").await, 7);
    assert_eq!(read_materialized(&store, "missing"), 3);
    assert_eq!(read_durable_gcounter_state(&store, "missing").value(), 3);
    assert_eq!(target.op_log.read().await.len(), 3);
    assert_eq!(target.seen_ops.read().await.len(), 3);
    target.shutdown().await.unwrap();
    source.shutdown().await;
}

#[tokio::test]
async fn initial_unresponsive_candidates_do_not_block_startup_or_shutdown() {
    use tokio::io::AsyncReadExt;

    let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_addr = first.local_addr().unwrap().to_string();
    let second_addr = second.local_addr().unwrap().to_string();
    let mut manager = SyncManager::new(
        NodeId::generate(),
        SyncConfig::new()
            .with_listen_addr("127.0.0.1:0")
            .with_peer(first_addr.clone())
            .with_peer(second_addr.clone())
            .with_socket_timeout(Duration::from_secs(60)),
        temp_store(),
        metrics(),
    );
    tokio::time::timeout(Duration::from_secs(1), manager.start())
        .await
        .expect("startup waited for initial peer handshakes")
        .unwrap();
    assert!(manager.start().await.is_err(), "start remains one-shot");
    assert!(manager.event_task.is_some());
    assert!(manager.broadcast_task.is_some());
    assert!(manager.reconnect_task.is_some());
    assert!(manager.anti_entropy_task.is_some());

    let (mut stalled, _) = tokio::time::timeout(Duration::from_secs(1), first.accept())
        .await
        .expect("initial reconnect dial did not start")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), stalled.read_exact(&mut [0; 1]))
        .await
        .expect("initial dial did not send its Hello")
        .unwrap();
    let error = manager.connect_to_peer(&second_addr).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<nx_net::NetError>(),
        Some(nx_net::NetError::ConnectionAttemptLimitReached(1))
    ));
    assert_eq!(manager.connected_peer_count().await, 0);

    // Broadcast persistence and event processing are already running while the
    // first candidate has stalled and the second has not yet been attempted.
    local_increment(&manager.handle(), "startup", 1).await;
    tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
        .await
        .expect("shutdown waited for the in-flight dial timeout")
        .unwrap();
    assert_eq!(manager.op_log.read().await.len(), 1);
    assert!(manager.reconnect_task.is_none());
    assert!(manager.event_task.is_none());
    assert!(manager.broadcast_task.is_none());
    assert!(manager.anti_entropy_task.is_none());
    assert!(
        manager.start().await.is_err(),
        "shutdown must not allow restart"
    );
    tokio::time::timeout(Duration::from_secs(1), stalled.read_to_end(&mut Vec::new()))
        .await
        .expect("canceled dial kept its transport open")
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), second.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn immediate_shutdown_cancels_initial_dial_before_task_first_poll() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut manager, _, _) = started_manager_with_config(
        SyncConfig::new()
            .with_listen_addr("127.0.0.1:0")
            .with_peer(listener.local_addr().unwrap().to_string())
            .with_socket_timeout(Duration::from_secs(60)),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
        .await
        .expect("pre-signaled shutdown must not wait for an initial dial")
        .unwrap();
    assert!(manager.reconnect_task.is_none());
    assert_eq!(manager.connected_peer_count().await, 0);
}

#[tokio::test]
async fn anti_entropy_pull_converges_peer_that_missed_broadcast() {
    let key = "visits";
    let addr_a = free_addr();
    let addr_b = free_addr();

    let config_a = SyncConfig::new().with_listen_addr(addr_a.clone());
    let (manager_a, handle_a, _store_a) = started_manager_with_config(config_a).await;

    local_increment(&handle_a, key, 1).await;

    let config_b = SyncConfig::new()
        .with_listen_addr(addr_b)
        .with_peer(addr_a)
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (manager_b, _handle_b, store_b) = started_manager_with_config(config_b).await;

    wait_for_connected_peer(&manager_b).await;
    wait_for_counter(&manager_b, key, 1).await;
    assert_eq!(read_materialized(&store_b, key), 1);
    assert_eq!(manager_a.get_counter_value(key).await, 1);
}

#[tokio::test]
async fn intermittent_network_with_ten_percent_lost_pushes_converges() {
    let key = "visits";
    let addr_a = free_addr();
    let addr_b = free_addr();

    let config_a = SyncConfig::new()
        .with_listen_addr(addr_a.clone())
        .with_peer(addr_b.clone())
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (manager_a, handle_a, store_a) = started_manager_with_config(config_a).await;

    let config_b = SyncConfig::new()
        .with_listen_addr(addr_b)
        .with_peer(addr_a)
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (manager_b, _handle_b, store_b) = started_manager_with_config(config_b).await;

    wait_for_connected_peer(&manager_a).await;
    wait_for_connected_peer(&manager_b).await;

    for i in 0..100 {
        if i % 10 == 0 {
            dropped_local_increment(&manager_a, &handle_a, key, 1).await;
        } else {
            local_increment(&handle_a, key, 1).await;
        }
    }

    wait_for_counter(&manager_b, key, 100).await;
    assert_eq!(manager_a.get_counter_value(key).await, 100);
    assert_eq!(read_materialized(&store_a, key), 100);
    assert_eq!(read_materialized(&store_b, key), 100);
}

#[tokio::test]
async fn node_restart_reconnects_and_converges_from_durable_state() {
    let key = "visits";
    let addr_a = free_addr();
    let addr_b = free_addr();
    let store_b = temp_store();
    let node_b_id = NodeId::new("node-b");

    let config_a = SyncConfig::new()
        .with_listen_addr(addr_a.clone())
        .with_peer(addr_b.clone())
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (manager_a, handle_a, store_a) = started_manager_with_config(config_a).await;

    let config_b = SyncConfig::new()
        .with_listen_addr(addr_b.clone())
        .with_peer(addr_a.clone())
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (mut manager_b, _handle_b) =
        started_manager_with_store(node_b_id.clone(), config_b, Arc::clone(&store_b)).await;

    wait_for_connected_peer(&manager_a).await;
    local_increment(&handle_a, key, 1).await;
    wait_for_counter(&manager_b, key, 1).await;

    manager_b.shutdown().await.unwrap();
    local_increment(&handle_a, key, 1).await;

    let config_b_restart = SyncConfig::new()
        .with_listen_addr(addr_b)
        .with_peer(addr_a)
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (manager_b_restarted, _handle_b_restarted) =
        started_manager_with_store(node_b_id, config_b_restart, Arc::clone(&store_b)).await;

    wait_for_connected_peer(&manager_a).await;
    wait_for_connected_peer(&manager_b_restarted).await;
    wait_for_counter(&manager_b_restarted, key, 2).await;

    assert_eq!(manager_a.get_counter_value(key).await, 2);
    assert_eq!(read_materialized(&store_a, key), 2);
    assert_eq!(read_materialized(&store_b, key), 2);
}

#[tokio::test]
#[ignore = "chaos smoke: repeatedly restarts a node and uses local TCP timing"]
async fn chaos_node_restart_loop_converges() {
    let key = "visits";
    let addr_a = free_addr();
    let addr_b = free_addr();
    let addr_c = free_addr();
    let store_b = temp_store();
    let node_b_id = NodeId::new("node-b");

    let config_a = SyncConfig::new()
        .with_listen_addr(addr_a.clone())
        .with_peer(addr_b.clone())
        .with_peer(addr_c.clone())
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (manager_a, handle_a, store_a) = started_manager_with_config(config_a).await;

    let config_b = SyncConfig::new()
        .with_listen_addr(addr_b.clone())
        .with_peer(addr_a.clone())
        .with_peer(addr_c.clone())
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (mut manager_b, _handle_b) =
        started_manager_with_store(node_b_id.clone(), config_b.clone(), Arc::clone(&store_b)).await;

    let config_c = SyncConfig::new()
        .with_listen_addr(addr_c)
        .with_peer(addr_a.clone())
        .with_peer(addr_b.clone())
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (manager_c, handle_c, store_c) = started_manager_with_config(config_c).await;

    let mut expected = 0;
    for _ in 0..3 {
        local_increment(&handle_a, key, 1).await;
        local_increment(&handle_c, key, 1).await;
        expected += 2;
        wait_for_counter(&manager_b, key, expected).await;

        manager_b.shutdown().await.unwrap();

        local_increment(&handle_a, key, 1).await;
        local_increment(&handle_c, key, 1).await;
        expected += 2;

        let restarted =
            started_manager_with_store(node_b_id.clone(), config_b.clone(), Arc::clone(&store_b))
                .await;
        manager_b = restarted.0;

        wait_for_connected_peer(&manager_b).await;
        wait_for_counter(&manager_b, key, expected).await;
    }

    wait_for_counter(&manager_a, key, expected).await;
    wait_for_counter(&manager_c, key, expected).await;
    assert_eq!(read_materialized(&store_a, key), expected);
    assert_eq!(read_materialized(&store_b, key), expected);
    assert_eq!(read_materialized(&store_c, key), expected);
}

#[tokio::test]
async fn source_restart_before_anti_entropy_still_serves_missed_ops() {
    let key = "visits";
    let addr_a = free_addr();
    let addr_b = free_addr();
    let store_a = temp_store();
    let node_a_id = NodeId::new("node-a");

    let config_a = SyncConfig::new()
        .with_listen_addr(addr_a.clone())
        .with_peer(addr_b.clone())
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (mut manager_a, handle_a) =
        started_manager_with_store(node_a_id.clone(), config_a, Arc::clone(&store_a)).await;

    let config_b = SyncConfig::new()
        .with_listen_addr(addr_b)
        .with_peer(addr_a.clone())
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (manager_b, _handle_b, store_b) = started_manager_with_config(config_b).await;

    wait_for_connected_peer(&manager_a).await;
    wait_for_connected_peer(&manager_b).await;

    dropped_local_increment(&manager_a, &handle_a, key, 1).await;
    manager_a.shutdown().await.unwrap();

    let config_a_restart = SyncConfig::new()
        .with_listen_addr(addr_a)
        .with_peer(manager_b.config.listen_addr.clone().unwrap())
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
        .with_anti_entropy_interval(Duration::from_millis(10));
    let (manager_a_restarted, _handle_a_restarted) =
        started_manager_with_store(node_a_id, config_a_restart, Arc::clone(&store_a)).await;

    wait_for_connected_peer(&manager_a_restarted).await;
    wait_for_connected_peer(&manager_b).await;
    wait_for_counter(&manager_b, key, 1).await;

    assert_eq!(read_materialized(&store_a, key), 1);
    assert_eq!(read_materialized(&store_b, key), 1);
}

#[tokio::test]
async fn peer_health_marks_dead_then_healthy_when_peer_recovers() {
    let addr_a = free_addr();
    let addr_b = free_addr();

    let config_a = SyncConfig::new()
        .with_listen_addr(addr_a)
        .with_peer(addr_b.clone())
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20))
        .with_peer_dead_after_failures(2);
    let (manager_a, _handle_a, _store_a) = started_manager_with_config(config_a).await;

    wait_for_peer_health(&manager_a, &addr_b, PeerHealthState::Dead).await;

    let config_b = SyncConfig::new().with_listen_addr(addr_b.clone());
    let (_manager_b, _handle_b, _store_b) = started_manager_with_config(config_b).await;

    wait_for_connected_peer(&manager_a).await;
    wait_for_peer_health(&manager_a, &addr_b, PeerHealthState::Healthy).await;
}

#[tokio::test]
async fn peer_rotation_replaces_dead_peer_with_available_configured_peer() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let addr_c = free_addr();

    let (mut manager_b, _handle_b, _store_b) = started_manager(addr_b.clone()).await;
    let (_manager_c, _handle_c, _store_c) = started_manager(addr_c.clone()).await;

    let config_a = SyncConfig::new()
        .with_listen_addr(addr_a)
        .with_peer(addr_b.clone())
        .with_peer(addr_c.clone())
        .with_max_peers(1)
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20))
        .with_peer_dead_after_failures(1);
    let (manager_a, _handle_a, _store_a) = started_manager_with_config(config_a).await;

    wait_for_peer_health(&manager_a, &addr_b, PeerHealthState::Healthy).await;
    sleep(Duration::from_millis(50)).await;
    assert_eq!(
        manager_a.peer_health_state(&addr_c).await,
        Some(PeerHealthState::Suspect),
        "peer C should not be marked failed while peer slots are full"
    );

    manager_b.shutdown().await.unwrap();

    wait_for_peer_health(&manager_a, &addr_b, PeerHealthState::Dead).await;
    wait_for_peer_health(&manager_a, &addr_c, PeerHealthState::Healthy).await;
    assert_eq!(manager_a.connected_peer_count().await, 1);
}

#[tokio::test]
async fn shutdown_drains_queued_ops_before_closing_connections() {
    let key = "visits";
    let addr_a = free_addr();
    let addr_b = free_addr();
    let (mut manager_a, handle_a, _store_a) = started_manager(addr_a).await;
    let (manager_b, _handle_b, store_b) = started_manager(addr_b.clone()).await;

    manager_a.connect_to_peer(&addr_b).await.unwrap();

    let op = Op::gcounter_increment(handle_a.node_id().clone(), key, 1);
    handle_a.op_sender().send(op).await.unwrap();
    manager_a.shutdown().await.unwrap();

    wait_for_counter(&manager_b, key, 1).await;
    assert_eq!(read_materialized(&store_b, key), 1);
}

#[tokio::test]
async fn manager_uses_configured_queued_ops_limit() {
    let store = temp_store();
    let config = SyncConfig::new().with_queued_ops_limit(256);
    let manager = SyncManager::new(NodeId::generate(), config, Arc::clone(&store), metrics());

    assert_eq!(manager.op_sender().max_capacity(), 256);
}

#[tokio::test]
async fn manager_normalizes_empty_queued_ops_limit() {
    let store = temp_store();
    let config = SyncConfig::new().with_queued_ops_limit(0);
    let manager = SyncManager::new(NodeId::generate(), config, Arc::clone(&store), metrics());

    assert_eq!(manager.op_sender().max_capacity(), 1);
}

#[test]
fn sync_disabled_runtime_has_no_sync_handle() {
    let config = RuntimeConfig {
        datastore_path: std::env::temp_dir().join(format!(
            "numax-core-nosync-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )),
        ..RuntimeConfig::default()
    };

    let runtime = Runtime::new(config).unwrap();
    assert!(runtime.sync_handle().is_none());
}
